%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_serial_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_mq_quota/emqx_mq_quota.hrl").

%% In these tests, a transaction with serial S has DS timestamp S * 100.
%% Limits: 100 bytes and 10 messages. Threshold: 20 bytes or 2 messages.

all() ->
    emqx_common_test_helpers:all(?MODULE).

-doc "A fence closes the full head, and buffered messages on both sides of the fence go to the right buckets".
t_fence_placement(_Config) ->
    {Index0, true} = update(new(), [add(1, 20, 2)], undefined),
    ?assertEqual(0, deadline(Index0)),
    %% Serial 9 committed before the fence at serial 10, serial 11 after it.
    {Index1, false} = update(Index0, [add(9, 10, 1), add(11, 10, 1)], fence(10)),
    ?assertEqual([bucket(10, 10, 1), bucket(0, 30, 3)], records(Index1)),
    %% Replacing the message with serial 9 subtracts from the bucket that holds it.
    {Index2, false} = update(Index1, [del(9, 10, 1)], undefined),
    ?assertEqual([bucket(10, 10, 1), bucket(0, 20, 2)], records(Index2)).

-doc "Without a fence, a full head stays open and the index asks for a fence again".
t_missing_fence(_Config) ->
    {Index0, true} = update(new(), [add(1, 50, 5)], undefined),
    {Index1, true} = update(Index0, [add(2, 50, 5)], undefined),
    ?assertEqual([bucket(0, 100, 10)], records(Index1)),
    ?assertEqual(0, deadline(Index1)).

-doc "A fence is ignored when the head is no longer full".
t_fence_ignored(_Config) ->
    {Index0, true} = update(new(), [add(1, 20, 2)], undefined),
    {Index1, false} = update(Index0, [del(1, 20, 2)], undefined),
    {Index2, false} = update(Index1, [], fence(10)),
    ?assertEqual([bucket(0, 0, 0)], records(Index2)).

-doc "A byte limit alone closes the head, and an index without limits never asks for a fence".
t_limits(_Config) ->
    {Index0, true} = update(new(), [add(1, 20, 0)], undefined),
    {Index1, false} = update(Index0, [], fence(10)),
    ?assertEqual([bucket(10, 0, 0), bucket(0, 20, 0)], records(Index1)),
    ?assertMatch(
        {_, false},
        emqx_mq_quota_index_serial:apply_updates(
            new(), [add(1, 1000, 1000)], undefined, #{threshold_percentage => 20}
        )
    ).

-doc "Two closed buckets merge only when their sum fits into the threshold for bytes and count".
t_merge(_Config) ->
    {Index0, true} = update(new(), [add(1, 30, 3)], undefined),
    {Index1, true} = update(Index0, [add(11, 30, 3)], fence(10)),
    {Index2, false} = update(Index1, [], fence(20)),
    ?assertEqual([bucket(20, 0, 0), bucket(10, 30, 3), bucket(0, 30, 3)], records(Index2)),
    %% After the deletes both closed buckets hold 10 bytes and 1 message, so they merge.
    {Index3, false} = update(Index2, [del(1, 20, 2), del(11, 20, 2)], undefined),
    ?assertEqual([bucket(20, 0, 0), bucket(0, 20, 2)], records(Index3)).

-doc "Old closed buckets are dropped, and updates older than the kept buckets are ignored".
t_drop_old(_Config) ->
    Opts = #{bytes => infinity, count => 4, threshold_percentage => 50},
    {Index0, true} = update(new(), [add(1, 20, 2)], undefined, Opts),
    {Index1, true} = update(Index0, [add(11, 20, 2)], fence(10), Opts),
    {Index2, true} = update(Index1, [add(21, 20, 2)], fence(20), Opts),
    {Index3, false} = update(Index2, [], fence(30), Opts),
    %% The two newest closed buckets reach the limit of 4, so the bucket at (0, 0) is dropped.
    ?assertEqual([bucket(30, 0, 0), bucket(20, 20, 2), bucket(10, 20, 2)], records(Index3)),
    ?assertEqual(ts(10), emqx_mq_quota_index_serial:deadline(Index3, Opts)),
    %% Messages from the dropped bucket do not change the index.
    {Index4, false} = update(Index3, [add(5, 10, 1), del(5, 10, 1)], undefined, Opts),
    ?assertEqual(records(Index3), records(Index4)).

-doc "The deadline is the start of the bucket at which the sum from the head reaches a limit".
t_deadline(_Config) ->
    {Index0, true} = update(new(), [add(1, 50, 5)], undefined),
    {Index1, true} = update(Index0, [add(11, 50, 5)], fence(10)),
    %% The limit is reached only at the bucket that starts at (0, 0).
    ?assertEqual(0, deadline(Index1)),
    {Index2, true} = update(Index1, [add(21, 50, 5)], fence(20)),
    %% The head and the bucket at serial 10 reach the limit together.
    ?assertEqual(ts(10), deadline(Index2)).

-doc "If one serial is shared by the fence and a later message, a replacement leaves +value and -value in two buckets".
t_shared_serial(_Config) ->
    ?check_trace(
        begin
            {Index0, true} = update(new(), [add(1, 30, 3)], undefined),
            %% Serial 10 is also the fence serial, so the message goes to the older bucket.
            {Index1, false} = update(Index0, [add(10, 10, 1), add(11, 0, 0)], fence(10)),
            %% It was stored after the fence, so its DS timestamp is in the newer bucket.
            {Index2, false} = update(Index1, [del_after_fence(10, 10, 1)], undefined),
            ?assertEqual([bucket(10, -10, -1), bucket(0, 40, 4)], records(Index2)),
            %% The totals stay correct.
            ?assertEqual(#{bytes => 30, count => 3}, totals(Index2))
        end,
        fun(Trace) ->
            %% The index reports the message with serial 10, but not the one with serial 11.
            ?assertMatch(
                [#{serial := 10, fence_ts_us := 1000, bytes := 10}],
                ?of_kind(mq_quota_index_shared_fence_serial, Trace)
            )
        end
    ).

-doc "With a shared serial, the deadline can move to an older boundary, never to a newer one".
t_shared_serial_deadline(_Config) ->
    {Index0, true} = update(new(), [add(1, 20, 2)], undefined),
    {Index1, true} = update(Index0, [add(10, 10, 1), add(11, 100, 10)], fence(10)),
    ?assertEqual(ts(10), deadline(Index1)),
    {Index2, true} = update(Index1, [del_after_fence(10, 10, 1)], undefined),
    ?assertEqual([bucket(10, 90, 9), bucket(0, 30, 3)], records(Index2)),
    ?assertEqual(0, deadline(Index2)).

-doc "With a shared serial, +value and -value cancel when the two buckets merge".
t_shared_serial_merge(_Config) ->
    {Index0, true} = update(new(), [add(1, 20, 2)], undefined),
    {Index1, true} = update(Index0, [add(10, 10, 1), add(11, 20, 2)], fence(10)),
    {Index2, false} = update(Index1, [del_after_fence(10, 10, 1), del(11, 20, 2)], fence(20)),
    ?assertEqual([bucket(20, 0, 0), bucket(0, 20, 2)], records(Index2)).

-doc """
With distinct serials and no delayed updates,
each kept bucket counts exactly the stored messages in its DS timestamp range.
""".
t_buckets_match_stored_messages(_Config) ->
    _ = lists:foldl(fun exact_step/2, {new(), undefined, #{}}, lists:seq(1, 300)),
    ok.

-doc """
With delayed updates and shared serials,
no sum of buckets from the head is larger than the stored data it covers.
""".
t_never_deletes_early(_Config) ->
    _ = rand:seed(exsss, {1, 2, 3}),
    State0 = #{
        index => new(),
        fence => undefined,
        last_serial => 0,
        last_ts_us => 0,
        last_fence_serial => 0,
        share_next_serial => false,
        stored => #{},
        counted => #{},
        pending => [],
        inexact_sums => 0
    },
    #{inexact_sums := InexactSums} = lists:foldl(
        fun(_Step, State) -> never_early_step(State) end, State0, lists:seq(1, 500)
    ),
    %% Delayed updates and shared serials actually made some sums smaller than the stored data.
    ?assert(InexactSums > 0).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

opts() ->
    #{bytes => 100, count => 10, threshold_percentage => 20}.

new() ->
    emqx_mq_quota_index_serial:new().

ts(Serial) ->
    Serial * 100.

add(Serial, Bytes, Count) ->
    #quota_add{pub_ts_us = 0, serial = Serial, bytes = Bytes, count = Count}.

del(Serial, Bytes, Count) ->
    #quota_del{pub_ts_us = 0, ds_ts_us = ts(Serial), bytes = Bytes, count = Count}.

%% Delete a message that shares the fence serial but was stored after the fence.
del_after_fence(Serial, Bytes, Count) ->
    #quota_del{pub_ts_us = 0, ds_ts_us = ts(Serial) + 1, bytes = Bytes, count = Count}.

fence(Serial) ->
    #{serial => Serial, ts_us => ts(Serial)}.

bucket(Serial, Bytes, Count) ->
    #{serial => Serial, ts_us => ts(Serial), bytes => Bytes, count => Count}.

update(Index, Updates, Fence) ->
    update(Index, Updates, Fence, opts()).

update(Index, Updates, Fence, Opts) ->
    emqx_mq_quota_index_serial:apply_updates(Index, Updates, Fence, Opts).

deadline(Index) ->
    emqx_mq_quota_index_serial:deadline(Index, opts()).

records(Index) ->
    #{records := Records} = emqx_mq_quota_index_serial:inspect(Index),
    Records.

totals(Index) ->
    #{total := Total} = emqx_mq_quota_index_serial:inspect(Index),
    Total.

%% Replace or add one of 17 keys per step, and compare each bucket with the stored messages.
exact_step(Step, {Index0, Fence0, Messages0}) ->
    Serial = Step * 2,
    %% Select a key to replace or add
    Key = Step rem 17,
    Bytes = 1 + Step rem 7,
    Deletes =
        case Messages0 of
            #{Key := #{serial := OldSerial, bytes := OldBytes}} -> [del(OldSerial, OldBytes, 1)];
            #{} -> []
        end,

    %% Message maintains true data (like DB)
    Messages = Messages0#{Key => #{serial => Serial, bytes => Bytes}},

    %% Update index and check that buckets keep valid data projections
    {Index, NeedFence} = update(Index0, Deletes ++ [add(Serial, Bytes, 1)], Fence0),
    _ = lists:foldl(
        fun(#{ts_us := Start, bytes := B, count := C}, End) ->
            Expected = [
                Size
             || #{serial := S, bytes := Size} <- maps:values(Messages), ts(S) >= Start, ts(S) < End
            ],
            ?assertEqual({lists:sum(Expected), length(Expected)}, {B, C}),
            Start
        end,
        infinity,
        records(Index)
    ),
    %% The fence is written by the flush transaction, after the messages of this step.
    Fence =
        case NeedFence of
            true -> fence(Serial + 1);
            false -> undefined
        end,
    {Index, Fence, Messages}.

%% One step of t_never_deletes_early:
%% write 1 to 3 messages, flush their updates except a random tail, and check the index.
never_early_step(State0) ->
    {Batches, State1} = write_messages(rand:uniform(3), State0, []),
    %% The updates of the last writes can reach the buffer after the flush has started.
    %% They are flushed one flush later, in the order they were written.
    {Now, Later} = lists:split(rand:uniform(length(Batches) + 1) - 1, Batches),
    #{pending := Pending} = State1,
    State2 = flush(Pending ++ Now, State1#{pending := Later}),
    check_never_early(State2).

write_messages(0, State, Batches) ->
    {lists:reverse(Batches), State};
write_messages(N, State0, Batches) ->
    {Batch, State} = write_message(State0),
    write_messages(N - 1, State, [Batch | Batches]).

%% Write one message to a random key. Replace the stored message of that key, if any.
%% The batch holds the index updates and the matching changes to the model of counted messages.
write_message(State) ->
    #{
        last_serial := LastSerial,
        last_ts_us := LastTsUs,
        share_next_serial := ShareSerial,
        stored := Stored
    } = State,
    Serial =
        case ShareSerial of
            true -> LastSerial;
            false -> LastSerial + 1
        end,
    Message = #{serial => Serial, ts_us => LastTsUs + 1, bytes => rand:uniform(7)},
    Key = rand:uniform(17),
    {Updates, Changes} =
        case Stored of
            #{Key := Old} -> {[del_message(Old)], [{remove, Key, Old}]};
            #{} -> {[], []}
        end,
    Batch = #{
        updates => Updates ++ [add_message(Message)],
        changes => Changes ++ [{put, Key, Message}]
    },
    {Batch, State#{
        last_serial := Serial,
        last_ts_us := LastTsUs + 1,
        share_next_serial := false,
        stored := Stored#{Key => Message}
    }}.

%% Apply the batches to the index. Write a fence if the index asks for one.
%% A fence can share its serial with the message written just before it or just after it.
flush(Batches, State) ->
    #{
        index := Index0,
        fence := Fence0,
        counted := Counted0,
        last_serial := LastSerial,
        last_ts_us := LastTsUs,
        last_fence_serial := LastFenceSerial
    } = State,
    Updates = lists:append([U || #{updates := U} <- Batches]),
    Changes = lists:append([C || #{changes := C} <- Batches]),
    {Index, NeedFence} = update(Index0, Updates, Fence0),
    Counted = lists:foldl(fun apply_change/2, Counted0, Changes),
    case NeedFence of
        true ->
            %% Two flushes never commit together, so fence serials always grow.
            FenceSerial =
                case LastSerial > LastFenceSerial andalso rand:uniform() < 0.3 of
                    true -> LastSerial;
                    false -> LastSerial + 1
                end,
            State#{
                index := Index,
                fence := #{serial => FenceSerial, ts_us => LastTsUs + 1},
                counted := Counted,
                last_serial := FenceSerial,
                last_ts_us := LastTsUs + 1,
                last_fence_serial := FenceSerial,
                share_next_serial := rand:uniform() < 0.3
            };
        false ->
            State#{index := Index, fence := undefined, counted := Counted}
    end.

apply_change({put, Key, Message}, Counted) ->
    Counted#{Key => Message};
apply_change({remove, Key, Message}, Counted) ->
    case Counted of
        #{Key := Message} -> maps:remove(Key, Counted);
        #{} -> Counted
    end.

%% For each bucket, sum the buckets from the head down to it.
%% The sum must not exceed the counted messages stored at or after the bucket start.
check_never_early(#{index := Index, counted := Counted, inexact_sums := Inexact0} = State) ->
    {_, _, Inexact} = lists:foldl(
        fun(#{ts_us := Start, bytes := B, count := C}, {Bytes0, Count0, InexactAcc}) ->
            Bytes = Bytes0 + B,
            Count = Count0 + C,
            Covered = [M || #{ts_us := TsUs} = M <- maps:values(Counted), TsUs >= Start],
            CoveredBytes = lists:sum([Size || #{bytes := Size} <- Covered]),
            ?assert(Bytes =< CoveredBytes),
            ?assert(Count =< length(Covered)),
            IsInexact = Bytes < CoveredBytes orelse Count < length(Covered),
            {Bytes, Count, InexactAcc + bool_to_int(IsInexact)}
        end,
        {0, 0, Inexact0},
        records(Index)
    ),
    State#{inexact_sums := Inexact}.

bool_to_int(true) -> 1;
bool_to_int(false) -> 0.

add_message(#{serial := Serial, bytes := Bytes}) ->
    #quota_add{pub_ts_us = 0, serial = Serial, bytes = Bytes, count = 1}.

del_message(#{ts_us := TsUs, bytes := Bytes}) ->
    #quota_del{pub_ts_us = 0, ds_ts_us = TsUs, bytes = Bytes, count = 1}.
