%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module implements the global session registry history cleaner.
-module(emqx_cm_registry_keeper).
-behaviour(gen_server).

-export([
    start_link/0,
    count/1,
    purge/0
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

-include_lib("stdlib/include/ms_transform.hrl").
-include("emqx_cm.hrl").

-define(CACHE_COUNT_THRESHOLD, 1000).
-define(MIN_COUNT_INTERVAL_SECONDS, 5).
-define(CLEANUP_CHUNK_SIZE, 10000).

-define(IS_HIST_ENABLED(RETAIN), (RETAIN > 0)).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    case mria_config:whoami() =:= replicant of
        true ->
            %% Do not run delete loops on replicant nodes
            %% because the core nodes will do it anyway
            %% The process is started to serve the 'count' calls
            {ok, #{no_deletes => true}};
        false ->
            ok = send_delay_start(),
            {ok, #{next_clientid => undefined}}
    end.

%% @doc Count the number of sessions.
%% Include sessions which are expired since the given timestamp if `since' is greater than 0.
-spec count(non_neg_integer()) -> non_neg_integer().
count(Since) ->
    Retain = retain_duration(),
    Now = now_ts(),
    %% Get table size if hist is not enabled or
    %% Since is before the earliest possible retention time.
    IsCountAll = (not ?IS_HIST_ENABLED(Retain) orelse (Now - Retain >= Since)),
    case IsCountAll of
        true ->
            mnesia:table_info(?CHAN_REG_TAB, size);
        false ->
            %% make a gen call to avoid many callers doing the same concurrently
            gen_server:call(?MODULE, {count, Since}, infinity)
    end.

%% @doc Delete all retained history. Only for tests.
-spec purge() -> ok.
purge() ->
    purge_loop(undefined).

purge_loop(StartId) ->
    case cleanup_one_chunk(StartId, _IsPurge = true) of
        '$end_of_table' ->
            ok;
        NextId ->
            purge_loop(NextId)
    end.

handle_call({count, Since}, _From, State) ->
    {LastCountTime, LastCount} =
        case State of
            #{last_count_time := T, last_count := C} ->
                {T, C};
            _ ->
                {0, 0}
        end,
    Now = now_ts(),
    Total = mnesia:table_info(?CHAN_REG_TAB, size),
    %% Always count if the table is small enough
    %% or when the last count is too old
    IsTableSmall = (Total < ?CACHE_COUNT_THRESHOLD),
    IsLastCountOld = (Now - LastCountTime > ?MIN_COUNT_INTERVAL_SECONDS),
    case IsTableSmall orelse IsLastCountOld of
        true ->
            Count = do_count(Since),
            CountFinishedAt = now_ts(),
            {reply, Count, State#{last_count_time => CountFinishedAt, last_count => Count}};
        false ->
            {reply, LastCount, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(start, #{next_clientid := NextClientId} = State) ->
    case is_hist_enabled() of
        true ->
            NewNext =
                case cleanup_one_chunk(NextClientId) of
                    '$end_of_table' ->
                        ok = send_delay_start(),
                        undefined;
                    Id ->
                        _ = erlang:garbage_collect(),
                        Id
                end,
            {noreply, State#{next_clientid := NewNext}};
        false ->
            %% if not enabled, delay and check again
            %% because it might be enabled from online config change while waiting
            ok = send_delay_start(),
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

cleanup_one_chunk(NextClientId) ->
    cleanup_one_chunk(NextClientId, false).

cleanup_one_chunk(NextClientId, IsPurge) ->
    Retain = retain_duration(),
    Now = now_ts(),
    IsExpired = fun(#channel{pid = Ts}) ->
        IsPurge orelse (is_integer(Ts) andalso (Ts < Now - Retain))
    end,
    cleanup_loop(NextClientId, ?CLEANUP_CHUNK_SIZE, IsExpired).

cleanup_loop(ClientId, 0, _IsExpired) ->
    ClientId;
cleanup_loop('$end_of_table', _Count, _IsExpired) ->
    '$end_of_table';
cleanup_loop(undefined, Count, IsExpired) ->
    cleanup_loop(mnesia:dirty_first(?CHAN_REG_TAB), Count, IsExpired);
cleanup_loop(ClientId, Count, IsExpired) ->
    Records = mnesia:dirty_read(?CHAN_REG_TAB, ClientId),
    Next = mnesia:dirty_next(?CHAN_REG_TAB, ClientId),
    lists:foreach(
        fun(R) ->
            case IsExpired(R) of
                true ->
                    mria:dirty_delete_object(?CHAN_REG_TAB, R);
                false ->
                    ok
            end
        end,
        Records
    ),
    cleanup_loop(Next, Count - 1, IsExpired).

is_hist_enabled() ->
    retain_duration() > 0.

%% Return the session registration history retain duration in seconds.
-spec retain_duration() -> non_neg_integer().
retain_duration() ->
    emqx:get_config([broker, session_history_retain]).

cleanup_delay() ->
    Default = timer:minutes(2),
    case retain_duration() of
        0 ->
            %% prepare for online config change
            Default;
        RetainSeconds ->
            Min = max(timer:seconds(1), timer:seconds(RetainSeconds) div 4),
            min(Min, Default)
    end.

send_delay_start() ->
    Delay = cleanup_delay(),
    ok = send_delay_start(Delay).

send_delay_start(Delay) ->
    _ = erlang:send_after(Delay, self(), start),
    ok.

now_ts() ->
    erlang:system_time(seconds).

do_count(Since) ->
    Ms = ets:fun2ms(fun(#channel{pid = V}) ->
        is_pid(V) orelse (is_integer(V) andalso (V >= Since))
    end),
    ets:select_count(?CHAN_REG_TAB, Ms).
