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

%% @doc The session router worker is responsible for buffering
%% messages for a persistent session while it is initializing.  If a
%% connection process exists for a persistent session, this process is
%% used for bridging the gap while the new connection process takes
%% over the persistent session, but if there is no such process this
%% worker takes it place.
%%
%% The workers are started on all nodes, and buffers all messages that
%% are persisted to the session message table. In the final stage of
%% the initialization, the messages are delivered and the worker is
%% terminated.

-module(emqx_session_router_worker).

-behaviour(gen_server).

%% API
-export([
    buffer/3,
    pendings/1,
    resume_end/3,
    start_link/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-record(state, {
    remote_pid :: pid(),
    session_id :: binary(),
    session_tab :: ets:table(),
    messages :: ets:table(),
    buffering :: boolean()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(SessionTab, #{} = Opts) ->
    gen_server:start_link(?MODULE, Opts#{session_tab => SessionTab}, []).

pendings(Pid) ->
    gen_server:call(Pid, pendings).

resume_end(RemotePid, Pid, _SessionID) ->
    case gen_server:call(Pid, {resume_end, RemotePid}) of
        {ok, EtsHandle} ->
            ?tp(ps_worker_call_ok, #{
                pid => Pid,
                remote_pid => RemotePid,
                sid => _SessionID
            }),
            {ok, ets:tab2list(EtsHandle)};
        {error, _} = Err ->
            ?tp(ps_worker_call_failed, #{
                pid => Pid,
                remote_pid => RemotePid,
                sid => _SessionID,
                reason => Err
            }),
            Err
    end.

buffer(Worker, STopic, Msg) ->
    Worker ! {buffer, STopic, Msg},
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(#{
    remote_pid := RemotePid,
    session_id := SessionID,
    session_tab := SessionTab
}) ->
    process_flag(trap_exit, true),
    erlang:monitor(process, RemotePid),
    ?tp(ps_worker_started, #{
        remote_pid => RemotePid,
        sid => SessionID
    }),
    {ok, #state{
        remote_pid = RemotePid,
        session_id = SessionID,
        session_tab = SessionTab,
        messages = ets:new(?MODULE, [protected, ordered_set]),
        buffering = true
    }}.

handle_call(pendings, _From, State) ->
    %% Debug API
    {reply, {State#state.messages, State#state.remote_pid}, State};
handle_call({resume_end, RemotePid}, _From, #state{remote_pid = RemotePid} = State) ->
    ?tp(ps_worker_resume_end, #{sid => State#state.session_id}),
    {reply, {ok, State#state.messages}, State#state{buffering = false}};
handle_call({resume_end, _RemotePid}, _From, State) ->
    ?tp(ps_worker_resume_end_error, #{sid => State#state.session_id}),
    {reply, {error, wrong_remote_pid}, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({buffer, _STopic, _Msg}, State) when not State#state.buffering ->
    ?tp(ps_worker_drop_deliver, #{
        sid => State#state.session_id,
        msg_id => emqx_message:id(_Msg)
    }),
    {noreply, State};
handle_info({buffer, STopic, Msg}, State) when State#state.buffering ->
    ?tp(ps_worker_deliver, #{
        sid => State#state.session_id,
        msg_id => emqx_message:id(Msg)
    }),
    ets:insert(State#state.messages, {{Msg, STopic}}),
    {noreply, State};
handle_info({'DOWN', _, process, RemotePid, _Reason}, #state{remote_pid = RemotePid} = State) ->
    ?tp(warning, ps_worker, #{
        event => worker_remote_died,
        sid => State#state.session_id,
        msg => "Remote pid died. Exiting."
    }),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(shutdown, _State) ->
    ?tp(ps_worker_shutdown, #{sid => _State#state.session_id}),
    ok;
terminate(_, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
