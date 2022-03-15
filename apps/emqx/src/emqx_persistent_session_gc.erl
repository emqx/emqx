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
-module(emqx_persistent_session_gc).

-behaviour(gen_server).

-include("emqx_persistent_session.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-ifdef(TEST).
-export([
    session_gc_worker/2,
    message_gc_worker/0
]).
-endif.

-define(SERVER, ?MODULE).
%% TODO: Maybe these should be configurable?
-define(MARKER_GRACE_PERIOD, 60000000).
-define(ABANDONED_GRACE_PERIOD, 300000000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    {ok, start_message_gc_timer(start_session_gc_timer(#{}))}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({timeout, Ref, session_gc_timeout}, State) ->
    State1 = session_gc_timeout(Ref, State),
    {noreply, State1};
handle_info({timeout, Ref, message_gc_timeout}, State) ->
    State1 = message_gc_timeout(Ref, State),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Session messages GC
%%--------------------------------------------------------------------

start_session_gc_timer(State) ->
    Interval = emqx_config:get([persistent_session_store, session_message_gc_interval]),
    State#{session_gc_timer => erlang:start_timer(Interval, self(), session_gc_timeout)}.

session_gc_timeout(Ref, #{session_gc_timer := R} = State) when R =:= Ref ->
    %% Prevent overlapping processes.
    GCPid = maps:get(session_gc_pid, State, undefined),
    case GCPid =/= undefined andalso erlang:is_process_alive(GCPid) of
        true ->
            start_session_gc_timer(State);
        false ->
            start_session_gc_timer(State#{
                session_gc_pid => proc_lib:spawn_link(fun session_gc_worker/0)
            })
    end;
session_gc_timeout(_Ref, State) ->
    State.

session_gc_worker() ->
    ok = emqx_persistent_session:gc_session_messages(fun session_gc_worker/2).

session_gc_worker(delete, Key) ->
    emqx_persistent_session:delete_session_message(Key);
session_gc_worker(marker, Key) ->
    TS = emqx_persistent_session:session_message_info(timestamp, Key),
    case TS + ?MARKER_GRACE_PERIOD < erlang:system_time(microsecond) of
        true -> emqx_persistent_session:delete_session_message(Key);
        false -> ok
    end;
session_gc_worker(abandoned, Key) ->
    TS = emqx_persistent_session:session_message_info(timestamp, Key),
    case TS + ?ABANDONED_GRACE_PERIOD < erlang:system_time(microsecond) of
        true -> emqx_persistent_session:delete_session_message(Key);
        false -> ok
    end.

%%--------------------------------------------------------------------
%% Message GC
%% --------------------------------------------------------------------
%% The message GC simply removes all messages older than the retain
%% period. A more exact GC would either involve treating the session
%% message table as root set, or some kind of reference counting.
%% We sacrifice space for simplicity at this point.
start_message_gc_timer(State) ->
    Interval = emqx_config:get([persistent_session_store, session_message_gc_interval]),
    State#{message_gc_timer => erlang:start_timer(Interval, self(), message_gc_timeout)}.

message_gc_timeout(Ref, #{message_gc_timer := R} = State) when R =:= Ref ->
    %% Prevent overlapping processes.
    GCPid = maps:get(message_gc_pid, State, undefined),
    case GCPid =/= undefined andalso erlang:is_process_alive(GCPid) of
        true ->
            start_message_gc_timer(State);
        false ->
            start_message_gc_timer(State#{
                message_gc_pid => proc_lib:spawn_link(fun message_gc_worker/0)
            })
    end;
message_gc_timeout(_Ref, State) ->
    State.

message_gc_worker() ->
    HighWaterMark = erlang:system_time(microsecond) - emqx_config:get(?msg_retain) * 1000,
    message_gc_worker(emqx_persistent_session:first_message_id(), HighWaterMark).

message_gc_worker('$end_of_table', _HighWaterMark) ->
    ok;
message_gc_worker(MsgId, HighWaterMark) ->
    case emqx_guid:timestamp(MsgId) < HighWaterMark of
        true ->
            emqx_persistent_session:delete_message(MsgId),
            message_gc_worker(emqx_persistent_session:next_message_id(MsgId), HighWaterMark);
        false ->
            ok
    end.
