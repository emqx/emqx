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
          gw     :: gateway(),
          ctx    :: emqx_gateway_ctx:context(),
          status :: stopped | running,
          child_pids :: [pid()],
          gw_state :: emqx_gateway_impl:state() | undefined
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
-spec update(pid(), gateway()) -> ok | {error, any()}.
update(Pid, NewGateway) ->
    call(Pid, {update, NewGateway}).

call(Pid, Req) ->
    gen_server:call(Pid, Req, 5000).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Gateway, Ctx0, _GwDscrptr]) ->
    process_flag(trap_exit, true),
    #{name := GwName, rawconf := RawConf} = Gateway,
    Ctx   = do_init_context(GwName, RawConf, Ctx0),
    State = #state{
               gw = Gateway,
               ctx   = Ctx,
               child_pids = [],
               status = stopped
              },
    case cb_gateway_load(State) of
        {error, Reason} ->
            do_deinit_context(Ctx),
            {stop, {load_gateway_failure, Reason}};
        {ok, NState} ->
            {ok, NState}
    end.

do_init_context(GwName, RawConf, Ctx) ->
    Auth = case maps:get(authentication, RawConf, #{enable => false}) of
               #{enable := true,
                 authenticators := AuthCfgs} when is_list(AuthCfgs) ->
                   create_authenticators_for_gateway_insta(GwName, AuthCfgs);
               _ ->
                   undefined
           end,
    Ctx#{auth => Auth}.

do_deinit_context(Ctx) ->
    cleanup_authenticators_for_gateway_insta(maps:get(auth, Ctx)),
    ok.

handle_call(info, _From, State = #state{gw = Gateway, status = Status}) ->
    {reply, Gateway#{status => Status}, State};

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

%% Stopped -> update
handle_call({update, NewGateway}, _From, State = #state{
                                                    gw = Gateway,
                                                    status = stopped}) ->
    case maps:get(name, NewGateway, undefined)
         == maps:get(name, Gateway, undefined) of
        true ->
            {reply, ok, State#state{gw = NewGateway}};
        false ->
            {reply, {error, gateway_name_not_match}, State}
    end;

%% Running -> update
handle_call({update, NewGateway}, _From, State = #state{gw = Gateway,
                                                      status = running}) ->
    case maps:get(name, NewGateway, undefined)
         == maps:get(name, Gateway, undefined) of
        true ->
            case cb_gateway_update(NewGateway, State) of
                {ok, NState} ->
                    {reply, ok, NState};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        false ->
            {reply, {error, gateway_name_not_match}, State}
    end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State = #state{child_pids = Pids}) ->
    case lists:member(Pid, Pids) of
        true ->
            logger:error("Child process ~p exited: ~0p.", [Pid, Reason]),
            case Pids -- [Pid]of
                [] ->
                    logger:error("All child process exited!"),
                    {noreply, State#state{status = stopped,
                                          child_pids = [],
                                          gw_state = undefined}};
                RemainPids ->
                    {noreply, State#state{child_pids = RemainPids}}
            end;
        _ ->
            logger:error("Unknown process exited ~p:~0p", [Pid, Reason]),
            {noreply, State}
    end;

handle_info(Info, State) ->
    logger:warning("Unexcepted info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, State = #state{ctx = Ctx, child_pids = Pids}) ->
    Pids /= [] andalso (_ = cb_gateway_unload(State)),
    _ = do_deinit_context(Ctx),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

%% @doc AuthCfgs is a array of authenticatior configurations,
%% see: emqx_authn_schema:authenticators/1
create_authenticators_for_gateway_insta(GwName, AuthCfgs) ->
    ChainId = atom_to_binary(GwName, utf8),
    case emqx_authn:create_chain(#{id => ChainId}) of
        {ok, _ChainInfo} ->
            Results = lists:map(fun(AuthCfg = #{name := Name}) ->
                         case emqx_authn:create_authenticator(
                                ChainId,
                                AuthCfg) of
                             {ok, _AuthInfo} -> ok;
                             {error, Reason} -> {Name, Reason}
                         end
                      end, AuthCfgs),
            NResults = [ E || E <- Results, E /= ok],
            NResults /= [] andalso begin
                logger:error("Failed to create authenticators: ~p", [NResults]),
                throw({bad_autheticators, NResults})
            end, ChainId;
        {error, Reason} ->
            logger:error("Failed to create authentication chain: ~p", [Reason]),
            throw({bad_chain, {ChainId, Reason}})
    end.

cleanup_authenticators_for_gateway_insta(undefined) ->
    ok;
cleanup_authenticators_for_gateway_insta(ChainId) ->
    case emqx_authn:delete_chain(ChainId) of
        ok -> ok;
        {error, {not_found, _}} ->
            logger:warning("Failed to clean authentication chain: ~s, "
                           "reason: not_found", [ChainId]);
        {error, Reason} ->
            logger:error("Failed to clean authentication chain: ~s, "
                         "reason: ~p", [ChainId, Reason])
    end.

cb_gateway_unload(State = #state{gw = Gateway = #{name := GwName},
                                 gw_state = GwState}) ->
    try
        #{cbkmod := CbMod} = emqx_gateway_registry:lookup(GwName),
        CbMod:on_gateway_unload(Gateway, GwState),
        {ok, State#state{child_pids = [],
                         gw_state = undefined,
                         status = stopped}}
    catch
        Class : Reason : Stk ->
            logger:error("Failed to unload gateway (~0p, ~0p) crashed: "
                         "{~p, ~p}, stacktrace: ~0p",
                         [Gateway, GwState,
                          Class, Reason, Stk]),
            {error, {Class, Reason, Stk}}
    end.

cb_gateway_load(State = #state{gw = Gateway = #{name := GwName},
                               ctx = Ctx}) ->
    try
        #{cbkmod := CbMod} = emqx_gateway_registry:lookup(GwName),
        case CbMod:on_gateway_load(Gateway, Ctx) of
            {error, Reason} -> throw({callback_return_error, Reason});
            {ok, ChildPidOrSpecs, GwState} ->
                ChildPids = start_child_process(ChildPidOrSpecs),
                {ok, State#state{
                       status = running,
                       child_pids = ChildPids,
                       gw_state = GwState
                      }}
        end
    catch
        Class : Reason1 : Stk ->
            logger:error("Failed to load ~s gateway (~0p, ~0p) crashed: "
                         "{~p, ~p}, stacktrace: ~0p",
                         [GwName, Gateway, Ctx,
                          Class, Reason1, Stk]),
            {error, {Class, Reason1, Stk}}
    end.

cb_gateway_update(NewGateway,
                State = #state{gw = Gateway = #{name := GwName},
                               ctx = Ctx,
                               gw_state = GwState}) ->
    try
        #{cbkmod := CbMod} = emqx_gateway_registry:lookup(GwName),
        case CbMod:on_gateway_update(NewGateway, Gateway, GwState) of
            {error, Reason} -> throw({callback_return_error, Reason});
            {ok, ChildPidOrSpecs, NGwState} ->
                %% XXX: Hot-upgrade ???
                ChildPids = start_child_process(ChildPidOrSpecs),
                {ok, State#state{
                       status = running,
                       child_pids = ChildPids,
                       gw_state = NGwState
                      }}
        end
    catch
        Class : Reason1 : Stk ->
            logger:error("Failed to update gateway (~0p, ~0p, ~0p) crashed: "
                         "{~p, ~p}, stacktrace: ~0p",
                         [NewGateway, Gateway, Ctx,
                          Class, Reason1, Stk]),
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
