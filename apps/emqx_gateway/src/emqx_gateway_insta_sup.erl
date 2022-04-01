%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc The gateway runtime
-module(emqx_gateway_insta_sup).

-behaviour(gen_server).

-include("include/emqx_gateway.hrl").
-include_lib("emqx/include/logger.hrl").

%% APIs
-export([
    start_link/3,
    info/1,
    disable/1,
    enable/1,
    update/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    name :: gateway_name(),
    config :: emqx_config:config(),
    ctx :: emqx_gateway_ctx:context(),
    authns :: [emqx_authentication:chain_name()],
    status :: stopped | running,
    child_pids :: [pid()],
    gw_state :: emqx_gateway_impl:state() | undefined,
    created_at :: integer(),
    started_at :: integer() | undefined,
    stopped_at :: integer() | undefined
}).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Gateway, Ctx, GwDscrptr) ->
    gen_server:start_link(
        ?MODULE,
        [Gateway, Ctx, GwDscrptr],
        []
    ).

-spec info(pid()) -> gateway().
info(Pid) ->
    gen_server:call(Pid, info).

%% @doc Stop gateway
-spec disable(pid()) -> ok | {error, any()}.
disable(Pid) ->
    call(Pid, disable).

%% @doc Start gateway
-spec enable(pid()) -> ok | {error, any()}.
enable(Pid) ->
    call(Pid, enable).

%% @doc Update the gateway configurations
-spec update(pid(), emqx_config:config()) -> ok | {error, any()}.
update(Pid, Config) ->
    call(Pid, {update, Config}).

call(Pid, Req) ->
    gen_server:call(Pid, Req, 5000).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Gateway, Ctx, _GwDscrptr]) ->
    process_flag(trap_exit, true),
    #{name := GwName, config := Config} = Gateway,
    State = #state{
        ctx = Ctx,
        name = GwName,
        authns = [],
        config = Config,
        child_pids = [],
        status = stopped,
        created_at = erlang:system_time(millisecond)
    },
    case maps:get(enable, Config, true) of
        false ->
            ?SLOG(info, #{
                msg => "skip_to_start_gateway_due_to_disabled",
                gateway_name => GwName
            }),
            {ok, State};
        true ->
            case cb_gateway_load(State) of
                {error, Reason} ->
                    {stop, Reason};
                {ok, NState} ->
                    {ok, NState}
            end
    end.

handle_call(info, _From, State) ->
    {reply, detailed_gateway_info(State), State};
handle_call(disable, _From, State = #state{status = Status}) ->
    case Status of
        running ->
            case cb_gateway_unload(State) of
                {ok, NState} ->
                    {reply, ok, NState};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        _ ->
            {reply, {error, already_stopped}, State}
    end;
handle_call(enable, _From, State = #state{status = Status}) ->
    case Status of
        stopped ->
            case cb_gateway_load(State) of
                {error, Reason} ->
                    {reply, {error, Reason}, State};
                {ok, NState} ->
                    {reply, ok, NState}
            end;
        _ ->
            {reply, {error, already_started}, State}
    end;
handle_call({update, Config}, _From, State) ->
    case do_update_one_by_one(Config, State) of
        {ok, NState} ->
            {reply, ok, NState};
        {error, Reason} ->
            %% If something wrong, nothing to update
            {reply, {error, Reason}, State}
    end;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(
    {'EXIT', Pid, Reason},
    State = #state{
        name = Name,
        child_pids = Pids
    }
) ->
    case lists:member(Pid, Pids) of
        true ->
            ?SLOG(error, #{
                msg => "child_process_exited",
                child => Pid,
                reason => Reason
            }),
            case Pids -- [Pid] of
                [] ->
                    ?SLOG(error, #{
                        msg => "gateway_all_children_process_existed",
                        gateway_name => Name
                    }),
                    {noreply, State#state{
                        status = stopped,
                        child_pids = [],
                        gw_state = undefined
                    }};
                RemainPids ->
                    {noreply, State#state{child_pids = RemainPids}}
            end;
        _ ->
            ?SLOG(error, #{
                msg => "gateway_catch_a_unknown_process_exited",
                child => Pid,
                reason => Reason,
                gateway_name => Name
            }),
            {noreply, State}
    end;
handle_info(Info, State) ->
    ?SLOG(warning, #{
        msg => "unexcepted_info",
        info => Info
    }),
    {noreply, State}.

terminate(_Reason, State = #state{child_pids = Pids}) ->
    Pids /= [] andalso (_ = cb_gateway_unload(State)),
    _ = do_deinit_authn(State#state.authns),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

detailed_gateway_info(State) ->
    maps:filter(
        fun(_, V) -> V =/= undefined end,
        #{
            name => State#state.name,
            config => State#state.config,
            status => State#state.status,
            created_at => State#state.created_at,
            started_at => State#state.started_at,
            stopped_at => State#state.stopped_at
        }
    ).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

%% There are two layer authentication configs
%%       stomp.authn
%%           /                   \
%%   listeners.tcp.default.authn  *.ssl.default.authn
%%

init_authn(GwName, Config) ->
    Authns = authns(GwName, Config),
    try
        do_init_authn(Authns, [])
    catch
        throw:Reason = {badauth, _} ->
            do_deinit_authn(proplists:get_keys(Authns)),
            throw(Reason)
    end.

do_init_authn([], Names) ->
    Names;
do_init_authn([{_ChainName, _AuthConf = #{enable := false}} | More], Names) ->
    do_init_authn(More, Names);
do_init_authn([{ChainName, AuthConf} | More], Names) when is_map(AuthConf) ->
    _ = application:ensure_all_started(emqx_authn),
    do_create_authn_chain(ChainName, AuthConf),
    do_init_authn(More, [ChainName | Names]);
do_init_authn([_BadConf | More], Names) ->
    do_init_authn(More, Names).

authns(GwName, Config) ->
    Listeners = maps:to_list(maps:get(listeners, Config, #{})),
    lists:append(
        [
            [
                {emqx_gateway_utils:listener_chain(GwName, LisType, LisName), authn_conf(Opts)}
             || {LisName, Opts} <- maps:to_list(LisNames)
            ]
         || {LisType, LisNames} <- Listeners
        ]
    ) ++
        [{emqx_gateway_utils:global_chain(GwName), authn_conf(Config)}].

authn_conf(Conf) ->
    maps:get(authentication, Conf, #{enable => false}).

do_create_authn_chain(ChainName, AuthConf) ->
    case ensure_chain(ChainName) of
        ok ->
            case emqx_authentication:create_authenticator(ChainName, AuthConf) of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "failed_to_create_authenticator",
                        chain_name => ChainName,
                        reason => Reason,
                        config => AuthConf
                    }),
                    throw({badauth, Reason})
            end;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_create_authn_chanin",
                chain_name => ChainName,
                reason => Reason
            }),
            throw({badauth, Reason})
    end.

ensure_chain(ChainName) ->
    case emqx_authentication:create_chain(ChainName) of
        {ok, _ChainInfo} ->
            ok;
        {error, {already_exists, _}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

do_deinit_authn(Names) ->
    lists:foreach(
        fun(ChainName) ->
            case emqx_authentication:delete_chain(ChainName) of
                ok ->
                    ok;
                {error, {not_found, _}} ->
                    ok;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "failed_to_clean_authn_chain",
                        chain_name => ChainName,
                        reason => Reason
                    })
            end
        end,
        Names
    ).

do_update_one_by_one(
    NCfg,
    State = #state{
        name = GwName,
        config = OCfg,
        status = Status
    }
) ->
    NEnable = maps:get(enable, NCfg, true),

    OAuths = authns(GwName, OCfg),
    NAuths = authns(GwName, NCfg),

    case {Status, NEnable} of
        {stopped, true} ->
            NState = State#state{config = NCfg},
            cb_gateway_load(NState);
        {stopped, false} ->
            {ok, State#state{config = NCfg}};
        {running, true} ->
            NState =
                case NAuths == OAuths of
                    true ->
                        State;
                    false ->
                        %% Reset Authentication first
                        _ = do_deinit_authn(State#state.authns),
                        AuthnNames = init_authn(State#state.name, NCfg),
                        State#state{authns = AuthnNames}
                end,
            %% TODO: minimum impact update ???
            cb_gateway_update(NCfg, NState);
        {running, false} ->
            case cb_gateway_unload(State) of
                {ok, NState} -> {ok, NState#state{config = NCfg}};
                {error, Reason} -> {error, Reason}
            end;
        _ ->
            throw(nomatch)
    end.

cb_gateway_unload(
    State = #state{
        name = GwName,
        gw_state = GwState
    }
) ->
    Gateway = detailed_gateway_info(State),
    try
        #{cbkmod := CbMod} = emqx_gateway_registry:lookup(GwName),
        CbMod:on_gateway_unload(Gateway, GwState),
        {ok, State#state{
            child_pids = [],
            authns = [],
            status = stopped,
            gw_state = undefined,
            started_at = undefined,
            stopped_at = erlang:system_time(millisecond)
        }}
    catch
        Class:Reason:Stk ->
            ?SLOG(error, #{
                msg => "unload_gateway_crashed",
                gateway_name => GwName,
                inner_state => GwState,
                reason => {Class, Reason},
                stacktrace => Stk
            }),
            {error, Reason}
    after
        _ = do_deinit_authn(State#state.authns)
    end.

%% @doc 1. Create Authentcation Context
%%      2. Callback to Mod:on_gateway_load/2
%%
%% Notes: If failed, rollback
cb_gateway_load(
    State = #state{
        name = GwName,
        config = Config,
        ctx = Ctx
    }
) ->
    Gateway = detailed_gateway_info(State),
    try
        AuthnNames = init_authn(GwName, Config),
        NCtx = Ctx#{auth => AuthnNames},
        #{cbkmod := CbMod} = emqx_gateway_registry:lookup(GwName),
        case CbMod:on_gateway_load(Gateway, NCtx) of
            {error, Reason} ->
                do_deinit_authn(AuthnNames),
                {error, Reason};
            {ok, ChildPidOrSpecs, GwState} ->
                ChildPids = start_child_process(ChildPidOrSpecs),
                {ok, State#state{
                    ctx = NCtx,
                    authns = AuthnNames,
                    status = running,
                    child_pids = ChildPids,
                    gw_state = GwState,
                    stopped_at = undefined,
                    started_at = erlang:system_time(millisecond)
                }}
        end
    catch
        Class:Reason1:Stk ->
            ?SLOG(error, #{
                msg => "load_gateway_crashed",
                gateway_name => GwName,
                gateway => Gateway,
                ctx => Ctx,
                reason => {Class, Reason1},
                stacktrace => Stk
            }),
            {error, Reason1}
    end.

cb_gateway_update(
    Config,
    State = #state{
        name = GwName,
        gw_state = GwState
    }
) ->
    try
        #{cbkmod := CbMod} = emqx_gateway_registry:lookup(GwName),
        case CbMod:on_gateway_update(Config, detailed_gateway_info(State), GwState) of
            {error, Reason} ->
                {error, Reason};
            {ok, ChildPidOrSpecs, NGwState} ->
                ChildPids = start_child_process(ChildPidOrSpecs),
                {ok, State#state{
                    config = Config,
                    child_pids = ChildPids,
                    gw_state = NGwState
                }}
        end
    catch
        Class:Reason1:Stk ->
            ?SLOG(error, #{
                msg => "update_gateway_crashed",
                gateway_name => GwName,
                new_config => Config,
                reason => {Class, Reason1},
                stacktrace => Stk
            }),
            {error, Reason1}
    end.

start_child_process([]) ->
    [];
start_child_process([Indictor | _] = ChildPidOrSpecs) ->
    case erlang:is_pid(Indictor) of
        true ->
            ChildPidOrSpecs;
        _ ->
            do_start_child_process(ChildPidOrSpecs)
    end.

do_start_child_process(ChildSpecs) when is_list(ChildSpecs) ->
    lists:map(fun do_start_child_process/1, ChildSpecs);
do_start_child_process(_ChildSpec = #{start := {M, F, A}}) ->
    case erlang:apply(M, F, A) of
        {ok, Pid} ->
            Pid;
        {error, Reason} ->
            throw({start_child_process, Reason})
    end.
