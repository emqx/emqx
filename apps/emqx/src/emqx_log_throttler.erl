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

-module(emqx_log_throttler).

-behaviour(gen_server).

-include("logger.hrl").
-include("types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/0]).

%% throttler API
-export([allow/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SEQ_ID(Msg), {?MODULE, Msg}).
-define(NEW_SEQ, atomics:new(1, [{signed, false}])).
-define(GET_SEQ(Msg), persistent_term:get(?SEQ_ID(Msg), undefined)).
-define(ERASE_SEQ(Msg), persistent_term:erase(?SEQ_ID(Msg))).
-define(RESET_SEQ(SeqRef), atomics:put(SeqRef, 1, 0)).
-define(INC_SEQ(SeqRef), atomics:add(SeqRef, 1, 1)).
-define(GET_DROPPED(SeqRef), atomics:get(SeqRef, 1) - 1).
-define(IS_ALLOWED(SeqRef), atomics:add_get(SeqRef, 1, 1) =:= 1).

-define(MSGS_LIST, emqx:get_config([log, throttling, msgs], [])).
-define(TIME_WINDOW_MS, timer:seconds(emqx:get_config([log, throttling, time_window], 60))).

%% @doc Check if a throttled log message is allowed to pass down to the logger this time.
%% The Msg has to be an atom, and the second argument `UniqueKey' should be `undefined'
%% for predefined message IDs.
%% For relatively static resources created from configurations such as data integration
%% resource IDs `UniqueKey' should be of `binary()' type.
-spec allow(atom(), undefined | binary()) -> boolean().
allow(Msg, UniqueKey) when
    is_atom(Msg) andalso (is_binary(UniqueKey) orelse UniqueKey =:= undefined)
->
    case emqx_logger:get_primary_log_level() of
        debug ->
            true;
        _ ->
            do_allow(Msg, UniqueKey)
    end.

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    ok = lists:foreach(fun new_throttler/1, ?MSGS_LIST),
    CurrentPeriodMs = ?TIME_WINDOW_MS,
    TimerRef = schedule_refresh(CurrentPeriodMs),
    {ok, #{timer_ref => TimerRef, current_period_ms => CurrentPeriodMs}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(refresh, #{current_period_ms := PeriodMs} = State) ->
    Msgs = ?MSGS_LIST,
    DroppedStats = lists:foldl(
        fun(Msg, Acc) ->
            case ?GET_SEQ(Msg) of
                undefined ->
                    %% Should not happen, unless the static ids list is updated at run-time.
                    new_throttler(Msg),
                    ?tp(log_throttler_new_msg, #{throttled_msg => Msg}),
                    Acc;
                SeqMap when is_map(SeqMap) ->
                    maps:fold(
                        fun(Key, Ref, Acc0) ->
                            ID = iolist_to_binary([atom_to_binary(Msg), $:, Key]),
                            drop_stats(Ref, ID, Acc0)
                        end,
                        Acc,
                        SeqMap
                    );
                SeqRef ->
                    drop_stats(SeqRef, Msg, Acc)
            end
        end,
        #{},
        Msgs
    ),
    maybe_log_dropped(DroppedStats, PeriodMs),
    NewPeriodMs = ?TIME_WINDOW_MS,
    State1 = State#{
        timer_ref => schedule_refresh(NewPeriodMs),
        current_period_ms => NewPeriodMs
    },
    {noreply, State1};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unxpected_info", info => Info}),
    {noreply, State}.

drop_stats(SeqRef, Msg, Acc) ->
    Dropped = ?GET_DROPPED(SeqRef),
    ok = ?RESET_SEQ(SeqRef),
    ?tp(log_throttler_dropped, #{dropped_count => Dropped, throttled_msg => Msg}),
    maybe_add_dropped(Msg, Dropped, Acc).

terminate(_Reason, _State) ->
    %% atomics do not have delete/remove/release/deallocate API
    %% after the reference is garbage-collected the resource is released
    lists:foreach(fun(Msg) -> ?ERASE_SEQ(Msg) end, ?MSGS_LIST),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------

do_allow(Msg, UniqueKey) ->
    case persistent_term:get(?SEQ_ID(Msg), undefined) of
        undefined ->
            %% This is either a race condition (emqx_log_throttler is not started yet)
            %% or a developer mistake (msg used in ?SLOG_THROTTLE/2,3 macro is
            %% not added to the default value of `log.throttling.msgs`.
            ?SLOG(debug, #{
                msg => "log_throttle_disabled",
                throttled_msg => Msg
            }),
            true;
        %% e.g: unrecoverable msg throttle according resource_id
        SeqMap when is_map(SeqMap) ->
            case maps:find(UniqueKey, SeqMap) of
                {ok, SeqRef} ->
                    ?IS_ALLOWED(SeqRef);
                error ->
                    SeqRef = ?NEW_SEQ,
                    new_throttler(Msg, SeqMap#{UniqueKey => SeqRef}),
                    true
            end;
        SeqRef ->
            ?IS_ALLOWED(SeqRef)
    end.

maybe_add_dropped(Msg, Dropped, DroppedAcc) when Dropped > 0 ->
    DroppedAcc#{Msg => Dropped};
maybe_add_dropped(_Msg, _Dropped, DroppedAcc) ->
    DroppedAcc.

maybe_log_dropped(DroppedStats, PeriodMs) when map_size(DroppedStats) > 0 ->
    ?SLOG(warning, #{
        msg => "log_events_throttled_during_last_period",
        dropped => DroppedStats,
        period => emqx_utils_calendar:human_readable_duration_string(PeriodMs)
    });
maybe_log_dropped(_DroppedStats, _PeriodMs) ->
    ok.

schedule_refresh(PeriodMs) ->
    ?tp(log_throttler_sched_refresh, #{new_period_ms => PeriodMs}),
    erlang:send_after(PeriodMs, ?MODULE, refresh).

new_throttler(unrecoverable_resource_error = Msg) ->
    new_throttler(Msg, #{});
new_throttler(Msg) ->
    new_throttler(Msg, ?NEW_SEQ).

new_throttler(Msg, AtomicOrEmptyMap) ->
    persistent_term:put(?SEQ_ID(Msg), AtomicOrEmptyMap).
