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
%%       - ClientInfo Override? Translators ? Mountpoint ??
%%
%% Interface:
%%      
%%      -type clientinfo :: #{'$gateway': #{authenticators: allow_anonymouse | ChainId}
%%                           }
%%       - emqx_gateway:authenticate(Type, InstaId, ClientInfo)
%%       - emqx_gateway:register(Type, InstaId, ClientInfo)
%%

-module(emqx_gateway_insta_sup).

-behaviour(gen_server).

-include("include/emqx_gateway.hrl").

-logger_header("[PGW-Insta-Sup]").

%% APIs
-export([ start_link/3
        , info/1
        , disable/1
        , enable/1
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
          child_pids :: [pid()],
          insta_state :: emqx_gateway_impl:state()
         }).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Insta, Ctx, GwDscrptr) ->
    gen_server:start_link(
      {local, ?MODULE},
      ?MODULE,
      [Insta, Ctx, GwDscrptr],
      []
     ).

info(Pid) ->
    gen_server:call(Pid, info).

%% @doc Stop instance
disable(Pid) ->
    gen_server:call(Pid, disable).

%% @doc Start instance
enable(Pid) ->
    gen_server:call(Pid, enable).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Insta, Ctx0, _GwDscrptr]) ->
    process_flag(trap_exit, true),
    #{rawconf := RawConf} = Insta,
    Ctx   = do_init_context(RawConf, Ctx0),
    State = #state{
               insta = Insta,
               ctx   = Ctx
              },
    case cb_insta_create(State) of
        #state{child_pids = []} ->
            do_deinit_context(Ctx),
            {stop, create_gateway_instance_failed};
        NState ->
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

handle_call(disable, _From, State = #state{child_pids = ChildPids}) ->
    case ChildPids /= [] of
        true ->
            %% TODO: Report the error message
            {reply, ok, cb_insta_destroy(State)};
        _ ->
            {reply, ok, State}
    end;

handle_call(enable, _From, State = #state{child_pids = ChildPids}) ->
    case ChildPids /= [] of
        true ->
            {reply, {error, already_started}, State};
        _ ->
            %% TODO: Report the error message
            {reply, ok, cb_insta_create(State)}
    end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State = #state{child_pids = Pids}) ->
    case lists:member(Pid, Pids) of
        true ->
            logger:critical("Child process ~p exited: ~0p. "
                            "Try restart instance ~s now!", [Pid, Reason]),
            %% XXX: After process exited, do we need call destroy function??
            NState = cb_insta_create(cb_insta_destroy(State)),
            {noreply, NState};
        _ ->
            logger:info("Shutdown due to ~p:~0p", [Pid, Reason]),
            {stop, State}
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

cb_insta_destroy(State = #state{insta = Insta = #{gwid := GwId},
                                insta_state = InstaState}) ->
    try
        #{cbkmod := CbMod,
          state := GwState} = emqx_gateway_registry:lookup(GwId),
        CbMod:on_insta_destroy(Insta, InstaState, GwState)
    catch
        Class : Reason : Stk ->
            logger:error("Destroy instance (~0p, ~0p, _) crashed: "
                         "{~p, ~p}, stacktrace: ~0p",
                         [Insta, InstaState,
                          Class, Reason, Stk]),
            State#state{insta_state = undefined, child_pids = []}
    end.

cb_insta_create(State = #state{insta = Insta = #{gwid := GwId},
                               ctx   = Ctx}) ->
    try
        #{cbkmod := CbMod,
          state := GwState} = emqx_gateway_registry:lookup(GwId),
        case CbMod:on_insta_create(Insta, Ctx, GwState) of
            {error, Reason} -> throw({callback_return_error, Reason});
            {ok, InstaPidOrSpecs, InstaState} ->
                ChildPids = start_child_process(InstaPidOrSpecs),
                State#state{
                  child_pids = ChildPids,
                  insta_state = InstaState
                }
        end
    catch
        Class : Reason1 : Stk ->
            logger:error("Create instance (~0p, ~0p, _) crashed: "
                         "{~p, ~p}, stacktrace: ~0p",
                         [Insta, Ctx,
                          Class, Reason1, Stk]),
            State#state{
              child_pids = [],
              insta_state = undefined
             }
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
