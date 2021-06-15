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
-export([start_link/1]).

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
          mref  :: reference(),
          child_pid :: pid(),
          auth :: allow_anonymouse | binary(),
          insta_state :: emqx_gateway_impl:state()
         }).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Insta) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Insta], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Insta, Ctx0, GwState]) ->
    #instance{
       type = CbMod,
       rawconf = RawConf} = Insta,

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
            {ok, InstaPidOrSpec, GwInstaState} ->
                ChildPid = case erlang:is_pid(InstaPidOrSpec) of
                               true ->
                                   InstaPidOrSpec;
                               _ ->
                                   start_child_process(InstaPidOrSpec)
                           end,
                MRef = monitor(process, ChildPid),
                State = #state{
                           insta = Insta,
                           mref = MRef,
                           child_pid = ChildPid,
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

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State = #state{child_pid = Pid}) ->
    logger:error("Child process ~p exited: ~0p", [Pid, Reason]),

    %% TODO: Restart It??

    %% Drain monitor message: 'DOWN'

    {noreply, State};

handle_info({'DOWN', MRef, process, Pid, Reason},
            State = #state{mref = MRef, child_pid = Pid}) ->
    logger:error("Child process ~p exited: ~0p", [Pid, Reason]),

    %% TODO: Restart It??

    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    %% Destory ctx
    %%  1. auth
    %%  2.
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

start_child_process(_ChildSpec = #{start := {M, F, A}}) ->
    case erlang:apply(M, F, A) of
        {ok, Pid} ->
            Pid;
        {error, Reason} ->
            throw({start_child_process, Reason})
    end.
