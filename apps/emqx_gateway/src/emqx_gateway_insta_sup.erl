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
        , instance/1
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
          insta :: instance(),
          mrefs  :: [reference()],
          child_pids :: [pid()],
          auth :: allow_anonymouse | binary(),
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

instance(Pid) ->
    gen_server:call(Pid, instance).

%%--------------------------------------------------------------------
% gen_server callbacks
%%--------------------------------------------------------------------

init([Insta, Ctx0, GwDscrptr]) ->
    #instance{rawconf = RawConf} = Insta,
    #{cbkmod := CbMod,
      state  := GwState} = GwDscrptr,
    %% Create authenticators
    Auth = case maps:get(authenticator, RawConf, allow_anonymouse) of
               allow_anonymouse -> allow_anonymouse;
               Funcs when is_list(Funcs) ->
                   create_authenticator_for_gateway_insta(Funcs)
           end,
    Ctx = Ctx0#{auth => Auth},
    try
        case CbMod:on_insta_create(Insta, Ctx, GwState) of
            {error, Reason} -> throw({return_error, Reason});
            {ok, [Indictor|_] = InstaPidOrSpecs, GwInstaState} ->
                ChildPids = case erlang:is_pid(Indictor) of
                                true ->
                                    InstaPidOrSpecs;
                                _ ->
                                    start_child_process(InstaPidOrSpecs)
                            end,
                MRefs = lists:map(fun(Pid) ->
                            monitor(process, Pid)
                        end, ChildPids),
                State = #state{
                           insta = Insta,
                           mrefs = MRefs,
                           child_pids = ChildPids,
                           insta_state = GwInstaState
                          },
                {ok, State}
        end
    catch
        Class : Reason1 : Stk ->
            logger:error("Callback ~s:~s(~0p,~0p,~0p) crashed: "
                         "{~p, ~p}, stacktrace: ~0p",
                         [CbMod, on_insta_create, Insta, Ctx, GwState,
                          Class, Reason1, Stk]),
            {error, Reason1}
    after
        %% Clean authenticators
        %% TODO:
        cleanup_authenticator_for_gateway_insta(Auth)
    end.

    %% TODO: 2. ClientInfo Override Fun??

handle_call(instance, _From, State = #state{insta = Insta}) ->
    {reply, Insta, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State = #state{child_pids = _Pids}) ->
    logger:error("Child process ~p exited: ~0p", [Pid, Reason]),

    %% TODO: Restart It??

    %% Drain monitor message: 'DOWN'

    {noreply, State};

handle_info({'DOWN', MRef, process, Pid, Reason},
            State = #state{mrefs = MRefs, child_pids = Pids}) ->
    case lists:member(MRef, MRefs) andalso lists:member(Pid, Pids) of
        true ->
            logger:error("Child process ~p exited: ~0p", [Pid, Reason]),
            %% TODO: Restart It??
            {noreply, State#state{
                        mrefs = MRefs -- [MRef],
                        child_pids = Pids -- [Pid]
                       }
            };
        _ ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{insta = Insta = #instance{gwid = GwId},
                         insta_state = GwInstaState, child_pids = Pids}) ->
    %% Cleanup instances
    %% Step1. Destory instance
    Pids /= [] andalso
        try
            case emqx_gateway_registry:lookup(GwId) of
                undefined -> throw({notfound_gateway_in_registry, GwId});
                #{cbkmod := CbMod, state := GwState} ->
                    CbMod:on_insta_destroy(Insta, GwInstaState, GwState)
            end
        catch
            Class : Reason : Stk ->
                logger:error("Destory instance crashed: {~0p, ~0p}; "
                             "stacktrace: ~0p", [Class, Reason, Stk])
        end,

    %% Step2. Delete authenticator resources

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

start_child_process(ChildSpecs) when is_list(ChildSpecs) ->
    lists:map(fun start_child_process/1, ChildSpecs);

start_child_process(_ChildSpec = #{start := {M, F, A}}) ->
    case erlang:apply(M, F, A) of
        {ok, Pid} ->
            Pid;
        {error, Reason} ->
            throw({start_child_process, Reason})
    end.
