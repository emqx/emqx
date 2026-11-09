%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_serial).

-moduledoc """
The serial quota index counts the messages and bytes that one queue or stream stores in one DS slab.
Garbage collection asks it for a deadline: a DS timestamp.
Messages stored before the deadline can be deleted, and the newer messages still meet the limits.

This module is a pure data structure.
`emqx_mq_quota_index_tx` reads, writes, and deletes the index and its fences in DS.

## Buckets

The index is a list of buckets, newest first:

```
    newest                                              oldest
    [ head: (S2, T2) | closed: (S1, T1) | closed: (0, 0) ]
```

A bucket starts at a boundary: a DS serial and a DS timestamp.
It ends where the next newer bucket starts.
It stores the total bytes and the number of the messages written in that range.
The first bucket of a new index starts at (0, 0).
New messages normally go to the newest bucket, the head.

## Fences

Bucket boundaries come from fences.
A fence is a DS record that a flush transaction writes.
DS sets its value to the serial of that transaction, and its timestamp to the timestamp of that transaction.
So a message stored before the fence has a smaller or equal serial and an earlier DS timestamp.
A message stored after the fence has a greater or equal serial and a later DS timestamp.
The index relies on this property.
Serials can be equal because DS can give one serial to several transactions that commit together.
See "Accepted inaccuracy" for the effect.

## Updates

`apply_updates/4` applies one flush of buffered updates:

1. If a fence is given and the head is full, close the head at the fence.
   That is, start a new empty head at the fence boundary.
   If the head is no longer full, ignore the fence.
   This step runs before the new updates are applied.
2. Add each new message to the newest bucket that starts at a lower serial.
   Buffered messages can be older than the fence, so they can go to an older bucket.
3. Subtract each replaced message from the newest bucket that starts at or before its DS timestamp.
4. Ignore an update that is older than the oldest kept bucket.
   Its message is already before the deadline.
5. Merge small closed buckets and drop old ones. See below.
6. Return `NeedFence = true` when the head is full.
   The caller then writes a fence and passes it to the next call.

Publication timestamps are not used. Clock differences between nodes and publishers do not matter.

## Invariants

The caller must:

* Read and write the index inside the DS transaction that applies the updates.
  Then two concurrent flushes of the same index conflict, and DS retries one of them.
* Pass only the fence written by the previous update of this index.
  Delete that fence in the same transaction, whether or not the index used it.
* Pass the serial of the transaction that wrote each new message.
* Pass the DS timestamp at which each replaced message was stored.

The index then guarantees:

* Boundaries grow strictly in both serial and timestamp, from the oldest bucket to the head.
* Take a bucket that starts at (S0, T0), and the next newer bucket that starts at (S1, T1).
  A new message is added to the bucket when S0 < serial <= S1.
  A replaced message is subtracted from the bucket when T0 <= DS timestamp < T1.
  Both rules select the same bucket for a message, with one exception:
  a message that has serial S1 and was stored after the fence. See "Accepted inaccuracy".
* A boundary never moves. Later updates only change bucket counters.

## Accepted inaccuracy

The index is not strictly exact. These are the known errors:

* Bucket size. The deadline is always a bucket boundary.
  Up to one bucket of extra data stays. Bucket size is the threshold, a percentage of the limit.
* Waiting head. A full head keeps growing until the next flush provides its fence.
  On a busy queue one flush can add many messages to it, so a bucket can exceed the threshold.
* Lost updates. A node that stops without flushing loses its buffered updates.
  The index then counts fewer messages than are stored, and GC keeps more data.
* Rebuilt index. After an old node rewrites the index, all its data is in one bucket at (0, 0).
  That bucket cannot give a deadline, so GC keeps everything older than the next boundaries.
* Shared serial. A message can share the fence serial but be stored after the fence.
  It is added to the bucket before the fence, because its serial is not greater than the fence serial.
  When it is replaced, it is subtracted from the bucket after the fence, because of its DS timestamp.
  So the bucket before the fence counts too much, and the bucket after the fence counts too little.
  A counter can become negative. We do not clamp it: the sum of the two buckets is correct.
  A sum of buckets that ends at the bucket after the fence may be too small,
  so the deadline can be one bucket older than needed.
  The index cannot tell whether such a message was stored before or after the fence.
  It emits the debug trace event `mq_quota_index_shared_fence_serial` for each one.

Each of these errors either leaves extra data in a bucket, or makes a sum of buckets smaller than the stored data.
No error makes a sum larger than the stored data.
So the deadline can only move to an older timestamp: GC deletes messages later than needed, never earlier.

## Merging

After the updates, merge closed buckets to keep the index small:

* Walk the closed buckets from the newest to the oldest.
* If a bucket and the next older bucket together fit into the threshold for bytes and count,
  replace both with one bucket. It keeps the older start.
  Continue with the merged bucket.
* Never merge the head: the next fence must close the head, not a merged bucket.

A merge only removes a boundary. No message moves across a kept boundary, so the invariants still hold.
The deadline can only move to an older boundary, so GC can only keep more data.
Same as in the legacy index: a bucket is full when it reaches the threshold,
and two buckets merge when their sum does not exceed it.

## Dropping old buckets

Sum the closed buckets from the newest to the oldest.
When the sum of the newer buckets reaches a limit, drop the current bucket and all older ones.
Their messages are before the deadline.
The head is not in this sum, because its counters can still change a lot.

## Deadline

Sum all buckets, head included, from the newest to the oldest.
The deadline is the start timestamp of the bucket at which the sum first reaches a limit.
If no limit is reached, the deadline is 0 and nothing is deleted.
""".

%% ASN1-generated record field names are in camelCase.
-elvis([{elvis_style, atom_naming_convention, #{regex => "^[a-z]\\w*$"}}]).

-include_lib("../../gen_src/MessageQueue.hrl").
-include("emqx_mq_quota.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([new/0, new/2, apply_updates/4, deadline/2, inspect/1]).
-export_type([t/0, fence/0]).

-type t() :: nonempty_list(#'SerialIndexBucket'{}).
-type fence() :: #{serial := non_neg_integer(), ts_us := non_neg_integer()}.
-type opts() :: emqx_mq_quota_index:opts().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Create an empty index with one bucket at (0, 0)".
-spec new() -> t().
new() ->
    new(0, 0).

-doc "Create an index with one bucket at (0, 0) that already holds the given totals".
-spec new(integer(), integer()) -> t().
new(Bytes, Count) ->
    [#'SerialIndexBucket'{serial = 0, tsUs = 0, bytes = Bytes, count = Count}].

-doc "Apply one flush of updates. Return the new index and whether the caller must write a fence".
-spec apply_updates(t(), [emqx_mq_quota_index:update()], fence() | undefined, opts()) ->
    {t(), boolean()}.
apply_updates(Index0, Updates, Fence, Opts) ->
    Index1 = maybe_close_head(Index0, Fence, Opts),
    [Head | Closed0] = lists:foldl(fun apply_update/2, Index1, Updates),
    Closed1 = merge_closed(Closed0, Opts),
    Closed = drop_old(Closed1, Opts, 0, 0),
    NeedFence = is_full(Head, Opts),
    {[Head | Closed], NeedFence}.

-doc "Return the DS timestamp before which messages can be deleted, or 0".
-spec deadline(t(), opts()) -> non_neg_integer().
deadline(Index, Opts) ->
    deadline(Index, Opts, 0, 0).

-doc "Return buckets and totals in a readable form".
-spec inspect(t()) -> map().
inspect(Index) ->
    #{
        records => [
            #{serial => S, ts_us => T, bytes => B, count => C}
         || #'SerialIndexBucket'{serial = S, tsUs = T, bytes = B, count = C} <- Index
        ],
        total => #{
            bytes => lists:sum([B || #'SerialIndexBucket'{bytes = B} <- Index]),
            count => lists:sum([C || #'SerialIndexBucket'{count = C} <- Index])
        },
        size => length(Index)
    }.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

%% Start a new empty head at the fence if the current head is full.
maybe_close_head(Index, undefined, _Opts) ->
    Index;
maybe_close_head([Head | _] = Index, #{serial := Serial, ts_us := TsUs}, Opts) ->
    case is_full(Head, Opts) of
        true ->
            NewHead = #'SerialIndexBucket'{serial = Serial, tsUs = TsUs, bytes = 0, count = 0},
            [NewHead | Index];
        false ->
            Index
    end.

%% Walk from the newest bucket to the oldest and change the first bucket that holds the message.
apply_update(Update, Index) ->
    apply_update(Update, undefined, Index).

apply_update(_Update, _Newer, []) ->
    %% The message is older than every kept bucket.
    [];
apply_update(Update, Newer, [Bucket | Older]) ->
    case is_in_bucket(Update, Bucket) of
        true ->
            ok = trace_shared_fence_serial(Update, Newer),
            [change_bucket(Bucket, Update) | Older];
        false ->
            [Bucket | apply_update(Update, Bucket, Older)]
    end.

%% Report a new message that has the serial of the fence of the next newer bucket.
trace_shared_fence_serial(
    #quota_add{serial = Serial, bytes = Bytes},
    #'SerialIndexBucket'{serial = Serial, tsUs = FenceTsUs}
) ->
    ?tp(debug, mq_quota_index_shared_fence_serial, #{
        serial => Serial, fence_ts_us => FenceTsUs, bytes => Bytes
    }),
    ok;
trace_shared_fence_serial(_Update, _Newer) ->
    ok.

%% A new message is in a bucket when its serial is greater than the bucket start serial.
%% A replaced message is in a bucket when its DS timestamp is not before the bucket start timestamp.
is_in_bucket(#quota_add{serial = Serial}, #'SerialIndexBucket'{serial = StartSerial}) ->
    Serial > StartSerial;
is_in_bucket(#quota_del{ds_ts_us = TsUs}, #'SerialIndexBucket'{tsUs = StartTsUs}) ->
    TsUs >= StartTsUs.

change_bucket(Bucket, #quota_add{bytes = Bytes, count = Count}) ->
    add(Bucket, Bytes, Count);
change_bucket(Bucket, #quota_del{bytes = Bytes, count = Count}) ->
    add(Bucket, -Bytes, -Count).

add(#'SerialIndexBucket'{bytes = Bytes0, count = Count0} = Bucket, Bytes, Count) ->
    Bucket#'SerialIndexBucket'{bytes = Bytes0 + Bytes, count = Count0 + Count}.

%% Merge each closed bucket into the next older one while their sum fits into the threshold.
%% The merged bucket keeps the older start.
merge_closed([#'SerialIndexBucket'{bytes = Bytes, count = Count} = Newer, Older | Rest], Opts) ->
    Merged = add(Older, Bytes, Count),
    case is_within_threshold(Merged, Opts) of
        true -> merge_closed([Merged | Rest], Opts);
        false -> [Newer | merge_closed([Older | Rest], Opts)]
    end;
merge_closed(Buckets, _Opts) ->
    Buckets.

%% Keep closed buckets from the newest one until the sum of the kept buckets reaches a limit.
drop_old([], _Opts, _Bytes, _Count) ->
    [];
drop_old([#'SerialIndexBucket'{bytes = B, count = C} = Bucket | Older], Opts, Bytes, Count) ->
    case is_limit_reached(Bytes, Count, Opts) of
        true -> [];
        false -> [Bucket | drop_old(Older, Opts, Bytes + B, Count + C)]
    end.

%% Return the start of the bucket at which the sum of all newer buckets reaches a limit.
deadline([], _Opts, _Bytes, _Count) ->
    0;
deadline([#'SerialIndexBucket'{tsUs = TsUs, bytes = B, count = C} | Older], Opts, Bytes0, Count0) ->
    Bytes = Bytes0 + B,
    Count = Count0 + C,
    case is_limit_reached(Bytes, Count, Opts) of
        true -> TsUs;
        false -> deadline(Older, Opts, Bytes, Count)
    end.

is_full(#'SerialIndexBucket'{bytes = Bytes, count = Count}, Opts) ->
    Bytes >= threshold(bytes, Opts) orelse Count >= threshold(count, Opts).

is_within_threshold(#'SerialIndexBucket'{bytes = Bytes, count = Count}, Opts) ->
    Bytes =< threshold(bytes, Opts) andalso Count =< threshold(count, Opts).

%% The threshold is a percentage of the limit. An unset limit has no threshold.
threshold(Kind, #{threshold_percentage := Percentage} = Opts) ->
    case Opts of
        #{Kind := Max} when is_integer(Max) -> max(1, Max * Percentage div 100);
        _ -> infinity
    end.

is_limit_reached(Bytes, Count, Opts) ->
    is_kind_limit_reached(bytes, Bytes, Opts) orelse is_kind_limit_reached(count, Count, Opts).

is_kind_limit_reached(Kind, Value, Opts) ->
    case Opts of
        #{Kind := Max} when is_integer(Max) -> Value >= Max;
        _ -> false
    end.
