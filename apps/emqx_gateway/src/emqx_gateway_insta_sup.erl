%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([ start_link/3
        , info/1
        , disable/1
        , enable/1
        , update/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
          name       :: gateway_name(),
          config     :: emqx_config:config(),
          ctx        :: emqx_gateway_ctx:context(),
          status     :: stopped | running,
          child_pids :: [pid()],
          gw_state   :: emqx_gateway_impl:state() | undefined,
          created_at :: integer(),
          started_at :: integer() | undefined,
          stopped_at :: integer() | undefined
         }).

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
    #{name := GwName, config := Config } = Gateway,
    State = #state{
               ctx = Ctx,
               name = GwName,
               config = Config,
               child_pids = [],
               status = stopped,
               created_at = erlang:system_time(millisecond)
              },
    case cb_gateway_load(State) of
        {error, Reason} ->
            {stop, {load_gateway_failure, Reason}};
        {ok, NState} ->
            {ok, NState}
    end.

handle_call(info, _From, State) ->
    {reply, detailed_gateway_info(State), State};

handle_call(disable, _From, State = #state{status = Status}) ->
    %% XXX: The `disable` opertaion is not persist to config database
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

handle_info({'EXIT', Pid, Reason}, State = #state{child_pids = Pids}) ->
    case lists:member(Pid, Pids) of
        true ->
            ?LOG(error, "Child process ~p exited: ~0p.", [Pid, Reason]),
            case Pids -- [Pid]of
                [] ->
                    ?LOG(error, "All child process exited!"),
                    {noreply, State#state{status = stopped,
                                          child_pids = [],
                                          gw_state = undefined}};
                RemainPids ->
                    {noreply, State#state{child_pids = RemainPids}}
            end;
        _ ->
            ?LOG(error, "Unknown process exited ~p:~0p", [Pid, Reason]),
            {noreply, State}
    end;

handle_info(Info, State) ->
    ?LOG(warning, "Unexcepted info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, State = #state{ctx = Ctx, child_pids = Pids}) ->
    Pids /= [] andalso (_ = cb_gateway_unload(State)),
    _ = do_deinit_authn(maps:get(auth, Ctx, undefined)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

detailed_gateway_info(State) ->
    #{name => State#state.name,
      config => State#state.config,
      status => State#state.status,
      created_at => State#state.created_at,
      started_at => State#state.started_at,
      stopped_at => State#state.stopped_at
     }.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

do_init_authn(GwName, Config) ->
    case maps:get(authentication, Config, #{enable => false}) of
        #{enable := false} -> undefined;
        AuthCfg when is_map(AuthCfg) ->
            case maps:get(enable, AuthCfg, true) of
                false ->
                    undefined;
                _ ->
                    %% TODO: Implement Authentication
                    GwName
                    %case emqx_authn:create_chain(#{id => ChainId}) of
                    %    {ok, _ChainInfo} ->
                    %        case emqx_authn:create_authenticator(ChainId, AuthCfg) of
                    %            {ok, _} -> ChainId;
                    %            {error, Reason} ->
                    %                ?LOG(error, "Failed to create authentication ~p", [Reason]),
                    %                throw({bad_authentication, Reason})
                    %        end;
                    %    {error, Reason} ->
                    %        ?LOG(error, "Failed to create authentication chain: ~p", [Reason]),
                    %        throw({bad_chain, {ChainId, Reason}})
                    %end.
            end;
        _ ->
            undefined
    end.

do_deinit_authn(undefined) ->
    ok;
do_deinit_authn(AuthnRef) ->
    %% TODO:
    ?LOG(error, "Failed to clean authn ~p, not suppported now", [AuthnRef]).
    %case emqx_authn:delete_chain(AuthnRef) of
    %    ok -> ok;
    %    {error, {not_found, _}} ->
    %        ?LOG(warning, "Failed to clean authentication chain: ~s, "
    %                      "reason: not_found", [AuthnRef]);
    %    {error, Reason} ->
    %        ?LOG(error, "Failed to clean authentication chain: ~s, "
    %                    "reason: ~p", [AuthnRef, Reason])
    %end.

do_update_one_by_one(NCfg0, State = #state{
                                      ctx = Ctx,
                                      config = OCfg,
                                      status = Status}) ->

    NCfg = emqx_map_lib:deep_merge(OCfg, NCfg0),

    OEnable = maps:get(enable, OCfg, true),
    NEnable = maps:get(enable, NCfg0, OEnable),

    OAuth = maps:get(authentication, OCfg, undefined),
    NAuth = maps:get(authentication, NCfg0, OAuth),

    if
        Status == stopped, NEnable == true ->
            NState = State#state{config = NCfg},
            cb_gateway_load(NState);
        Status == stopped, NEnable == false ->
            {ok, State#state{config = NCfg}};
        Status == running, NEnable == true ->
            NState = case NAuth == OAuth of
                         true -> State;
                         false ->
                             %% Reset Authentication first
                             _ = do_deinit_authn(maps:get(auth, Ctx, undefined)),
                             NCtx = Ctx#{
                                      auth => do_init_authn(
                                                State#state.name,
                                                NCfg
                                               )
                                     },
                             State#state{ctx = NCtx}
                     end,
            cb_gateway_update(NCfg, NState);
        Status == running, NEnable == false ->
            case cb_gateway_unload(State) of
                {ok, NState} -> {ok, NState#state{config = NCfg}};
                {error, Reason} -> {error, Reason}
            end;
        true ->
            throw(nomatch)
    end.

cb_gateway_unload(State = #state{name = GwName,
                                 gw_state = GwState}) ->
    Gateway = detailed_gateway_info(State),
    try
        #{cbkmod := CbMod} = emqx_gateway_registry:lookup(GwName),
        CbMod:on_gateway_unload(Gateway, GwState),
        {ok, State#state{child_pids = [],
                         status = stopped,
                         gw_state = undefined,
                         started_at = undefined,
                         stopped_at = erlang:system_time(millisecond)}}
    catch
        Class : Reason : Stk ->
            ?LOG(error, "Failed to unload gateway (~0p, ~0p) crashed: "
                        "{~p, ~p}, stacktrace: ~0p",
                         [GwName, GwState,
                          Class, Reason, Stk]),
            {error, {Class, Reason, Stk}}
    end.

%% @doc 1. Create Authentcation Context
%%      2. Callback to Mod:on_gateway_load/2
%%
%% Notes: If failed, rollback
cb_gateway_load(State = #state{name = GwName,
                               config = Config,
                               ctx = Ctx}) ->
    Gateway = detailed_gateway_info(State),
    try
        AuthnRef = do_init_authn(GwName, Config),
        NCtx = Ctx#{auth => AuthnRef},
        #{cbkmod := CbMod} = emqx_gateway_registry:lookup(GwName),
        case CbMod:on_gateway_load(Gateway, NCtx) of
            {error, Reason} ->
                do_deinit_authn(AuthnRef),
                throw({callback_return_error, Reason});
            {ok, ChildPidOrSpecs, GwState} ->
                ChildPids = start_child_process(ChildPidOrSpecs),
                {ok, State#state{
                       ctx = NCtx,
                       status = running,
                       child_pids = ChildPids,
                       gw_state = GwState,
                       stopped_at = undefined,
                       started_at = erlang:system_time(millisecond)
                      }}
        end
    catch
        Class : Reason1 : Stk ->
            ?LOG(error, "Failed to load ~s gateway (~0p, ~0p) crashed: "
                        "{~p, ~p}, stacktrace: ~0p",
                         [GwName, Gateway, Ctx,
                          Class, Reason1, Stk]),
            {error, {Class, Reason1, Stk}}
    end.

cb_gateway_update(Config,
                  State = #state{name = GwName,
                                 gw_state = GwState}) ->
    try
        #{cbkmod := CbMod} = emqx_gateway_registry:lookup(GwName),
        case CbMod:on_gateway_update(Config, detailed_gateway_info(State), GwState) of
            {error, Reason} -> throw({callback_return_error, Reason});
            {ok, ChildPidOrSpecs, NGwState} ->
                %% XXX: Hot-upgrade ???
                ChildPids = start_child_process(ChildPidOrSpecs),
                {ok, State#state{
                       config = Config,
                       child_pids = ChildPids,
                       gw_state = NGwState
                      }}
        end
    catch
        Class : Reason1 : Stk ->
            ?LOG(error, "Failed to update ~s gateway to config: ~0p crashed: "
                        "{~p, ~p}, stacktrace: ~0p",
                        [GwName, Config, Class, Reason1, Stk]),
            {error, {Class, Reason1, Stk}}
    end.

start_child_process([]) -> [];
start_child_process([Indictor|_] = ChildPidOrSpecs) ->
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
