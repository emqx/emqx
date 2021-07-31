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

%% @doc The gateway instance management
-module(emqx_gateway_insta_sup).

-behaviour(gen_server).

-include("include/emqx_gateway.hrl").

-logger_header("[PGW-Insta-Sup]").

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
          insta  :: instance(),
          ctx    :: emqx_gateway_ctx:context(),
          status :: stopped | running,
          child_pids :: [pid()],
          insta_state :: emqx_gateway_impl:state() | undefined
         }).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Insta, Ctx, GwDscrptr) ->
    gen_server:start_link(
      ?MODULE,
      [Insta, Ctx, GwDscrptr],
      []
     ).

-spec info(pid()) -> instance().
info(Pid) ->
    gen_server:call(Pid, info).

%% @doc Stop instance
-spec disable(pid()) -> ok | {error, any()}.
disable(Pid) ->
    call(Pid, disable).

%% @doc Start instance
-spec enable(pid()) -> ok | {error, any()}.
enable(Pid) ->
    call(Pid, enable).

%% @doc Update the gateway instance configurations
-spec update(pid(), instance()) -> ok | {error, any()}.
update(Pid, NewInsta) ->
    call(Pid, {update, NewInsta}).

call(Pid, Req) ->
    gen_server:call(Pid, Req, 5000).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Insta, Ctx0, _GwDscrptr]) ->
    process_flag(trap_exit, true),
    #{rawconf := RawConf} = Insta,
    Ctx   = do_init_context(RawConf, Ctx0),
    State = #state{
               insta = Insta,
               ctx   = Ctx,
               child_pids = [],
               status = stopped
              },
    case cb_insta_create(State) of
        {error, _Reason} ->
            do_deinit_context(Ctx),
            %% XXX: Return Reason??
            {stop, create_gateway_instance_failed};
        {ok, NState} ->
            {ok, NState}
    end.

do_init_context(RawConf, Ctx) ->
    Auth = case maps:get(authenticator, RawConf, allow_anonymous) of
               allow_anonymous -> allow_anonymous;
               Funcs when is_list(Funcs) ->
                   create_authenticator_for_gateway_insta(Funcs)
           end,
    Ctx#{auth => Auth}.

do_deinit_context(Ctx) ->
    cleanup_authenticator_for_gateway_insta(maps:get(auth, Ctx)),
    ok.

handle_call(info, _From, State = #state{insta = Insta}) ->
    {reply, Insta, State};

handle_call(disable, _From, State = #state{status = Status}) ->
    case Status of
        running ->
            case cb_insta_destroy(State) of
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
            case cb_insta_create(State) of
                {error, Reason} ->
                    {reply, {error, Reason}, State};
                {ok, NState} ->
                    {reply, ok, NState}
            end;
        _ ->
            {reply, {error, already_started}, State}
    end;

%% Stopped -> update
handle_call({update, NewInsta}, _From, State = #state{insta = Insta,
                                                      status = stopped}) ->
    case maps:get(id, NewInsta, undefined) == maps:get(id, Insta, undefined) of
        true ->
            {reply, ok, State#state{insta = NewInsta}};
        false ->
            {reply, {error, bad_instan_id}, State}
    end;

%% Running -> update
handle_call({update, NewInsta}, _From, State = #state{insta = Insta,
                                                      status = running}) ->
    case maps:get(id, NewInsta, undefined) == maps:get(id, Insta, undefined) of
        true ->
            case cb_insta_update(NewInsta, State) of
                {ok, NState} ->
                    {reply, ok, NState};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        false ->
            {reply, {error, bad_instan_id}, State}
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
                                          insta_state = undefined}};
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
    %% Cleanup instances
    %% Step1. Destory instance
    Pids /= [] andalso (_ = cb_insta_destroy(State)),
    %% Step2. Delete authenticator resources
    _ = do_deinit_context(Ctx),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

create_authenticator_for_gateway_insta(_Funcs) ->
    todo.

cleanup_authenticator_for_gateway_insta(allow_anonymouse) ->
    ok;
cleanup_authenticator_for_gateway_insta(_ChainId) ->
    todo.

cb_insta_destroy(State = #state{insta = Insta = #{type := Type},
                                insta_state = InstaState}) ->
    try
        #{cbkmod := CbMod,
          state := GwState} = emqx_gateway_registry:lookup(Type),
        CbMod:on_insta_destroy(Insta, InstaState, GwState),
        {ok, State#state{child_pids = [],
                         insta_state = undefined,
                         status = stopped}}
    catch
        Class : Reason : Stk ->
            logger:error("Destroy instance (~0p, ~0p, _) crashed: "
                         "{~p, ~p}, stacktrace: ~0p",
                         [Insta, InstaState,
                          Class, Reason, Stk]),
            {error, {Class, Reason, Stk}}
    end.

cb_insta_create(State = #state{insta = Insta = #{type := Type},
                               ctx   = Ctx}) ->
    try
        #{cbkmod := CbMod,
          state := GwState} = emqx_gateway_registry:lookup(Type),
        case CbMod:on_insta_create(Insta, Ctx, GwState) of
            {error, Reason} -> throw({callback_return_error, Reason});
            {ok, InstaPidOrSpecs, InstaState} ->
                ChildPids = start_child_process(InstaPidOrSpecs),
                {ok, State#state{
                       status = running,
                       child_pids = ChildPids,
                       insta_state = InstaState
                      }}
        end
    catch
        Class : Reason1 : Stk ->
            logger:error("Create instance (~0p, ~0p, _) crashed: "
                         "{~p, ~p}, stacktrace: ~0p",
                         [Insta, Ctx,
                          Class, Reason1, Stk]),
            {error, {Class, Reason1, Stk}}
    end.

cb_insta_update(NewInsta,
                State = #state{insta = Insta = #{type := Type},
                               ctx   = Ctx,
                               insta_state = GwInstaState}) ->
    try
        #{cbkmod := CbMod,
          state := GwState} = emqx_gateway_registry:lookup(Type),
        case CbMod:on_insta_update(NewInsta, Insta, GwInstaState, GwState) of
            {error, Reason} -> throw({callback_return_error, Reason});
            {ok, InstaPidOrSpecs, InstaState} ->
                %% XXX: Hot-upgrade ???
                ChildPids = start_child_process(InstaPidOrSpecs),
                {ok, State#state{
                       status = running,
                       child_pids = ChildPids,
                       insta_state = InstaState
                      }}
        end
    catch
        Class : Reason1 : Stk ->
            logger:error("Update instance (~0p, ~0p, ~0p, _) crashed: "
                         "{~p, ~p}, stacktrace: ~0p",
                         [NewInsta, Insta, Ctx,
                          Class, Reason1, Stk]),
            {error, {Class, Reason1, Stk}}
    end.

start_child_process([Indictor|_] = InstaPidOrSpecs) ->
    case erlang:is_pid(Indictor) of
        true ->
            InstaPidOrSpecs;
        _ ->
            do_start_child_process(InstaPidOrSpecs)
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
