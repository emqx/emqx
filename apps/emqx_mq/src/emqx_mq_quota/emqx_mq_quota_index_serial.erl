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

A fence stays in DS until the index asks for a new one.
The index remembers the serial of the last fence it has seen.
A fence is used at most once: by the first update that sees it.
That update is the first update after the fence was written, because every update reads the fence.
So no message stored after the fence has been counted when the head closes at it.

A new or rebuilt index does not know which fences it has seen.
It treats a fence that is already in DS as seen, because that fence may be older than its counters.

## Updates

`apply_updates/4` applies one flush of buffered updates:

1. If the fence is not seen yet, mark it seen.
   If the head is also full, close the head at the fence.
   That is, start a new empty head at the fence boundary.
   This step runs before the new updates are applied.
2. Add each new message to the newest bucket that starts at a lower serial.
   Buffered messages can be older than the fence, so they can go to an older bucket.
3. Subtract each replaced message from the newest bucket that starts at or before its DS timestamp.
4. Ignore an update that is older than the oldest kept bucket.
   Its message is already before the deadline.
5. Merge small closed buckets and drop old ones. See below.
6. Return `NeedFence = true` when the head is full.
   After step 1 every fence is seen, so the head needs a new one.
   The caller then replaces the fence, and the next call gets the new fence.

Publication timestamps are not used. Clock differences between nodes and publishers do not matter.

## Invariants

The caller must:

* Read and write the index inside the DS transaction that applies the updates.
  Then two concurrent flushes of the same index conflict, and DS retries one of them.
* Pass the fence stored in DS, if any.
  Replace it only when the index returns `NeedFence = true`, or before a purge.
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

The index is not strictly exact. Its errors must keep this property:

* A bucket can count less than is stored. GC then keeps more data.
* A bucket can count more than is stored only in two cases:
  * Newer buckets count less by at least the same amount.
    Every sum from the head then stays at or below the stored data.
  * It is the tail: the oldest bucket that holds messages the queue must keep.
    The extra count of the tail can only move the deadline to the start of the tail,
    and make the index drop older buckets. Those buckets hold no messages to keep.

So GC can delete messages later than needed, but never earlier.
These are the known errors:

* Bucket size. The deadline is always a bucket boundary.
  Up to one bucket of extra data stays. Bucket size is the threshold, a percentage of the limit.
* Waiting head. A full head keeps growing until the next flush provides its fence.
  On a busy queue one flush can add many messages to it, so a bucket can exceed the threshold.
* Lost updates. A node that stops without flushing loses its buffered updates.
  The buckets count less than is stored.
* Rebuilt index. After an old node rewrites the index, all its data is in one bucket at (0, 0).
  That bucket cannot give a deadline, so GC keeps everything older than the next boundaries.
  The fence in DS is treated as seen, so the first boundary comes from the next fence.
* Shared serial. A message can share the fence serial but be stored after the fence.
  It is added to the bucket before the fence, because its serial is not greater than the fence serial.
  When it is replaced, it is subtracted from the bucket after the fence, because of its DS timestamp.
  So the bucket before the fence counts more than is stored, and the newer bucket after the fence counts less.
  A counter can become negative. We do not clamp it: the sum of the two buckets is correct.
  The index cannot tell whether such a message was stored before or after the fence.
  It emits the debug trace event `mq_quota_index_shared_fence_serial` for each one.
* Rotation at a seen fence. A flush can see the purge fence before the rotation and not close the head.
  It counts the messages stored after the fence in the old head.
  The rotation drops the old head, so the new head counts less than is stored.
* Deletion inside a bucket. Retention, the delivery cutoff, and a purge before a given time
  delete messages before a DS timestamp T that is not a bucket boundary.
  The bucket that contains T keeps counting the deleted messages, so it counts more than is stored.
  It is the tail: the older buckets hold only messages stored before T.

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
If no limit is reached, the deadline is the start timestamp of the oldest kept bucket.

The index counts no message stored before the oldest kept bucket.
Such messages were purged, or their buckets were dropped because the newer buckets reached a limit.
So GC deletes them too.
When the oldest kept bucket starts at (0, 0), the deadline is 0.

The caller applies retention and the delivery cutoff separately. See "Deletion inside a bucket".

## Purge

A purge first changes the index, then deletes the messages in separate requests.
GC deletes the purged messages too, also when the deletion requests fail.

* `truncate/2` is used to purge the messages stored before a DS timestamp T.
  It drops the buckets that end at or before T, and keeps the bucket that contains T.
  The caller then deletes the messages stored before T. See "Deletion inside a bucket".
* `rotate/2` is used to purge everything.
  The caller first writes a fresh fence. In the next transaction, it reads the fence (S, T) stored in DS,
  and passes it to `rotate/2`. A flush can replace the fence between the two transactions.
  * If a bucket starts at the fence, `rotate/2` keeps the buckets from it.
    A flush closed the head at the fence between the two transactions.
  * Otherwise `rotate/2` starts a new empty head at the fence and drops all older buckets.
    If a flush saw the fence without closing the head, the new head counts too little.
    See "Accepted inaccuracy".
  It returns the fence boundary (S, T). Messages stored before T can then be deleted.

Updates for purged messages are older than the kept buckets, so the index ignores them.
""".

%% ASN1-generated record field names are in camelCase.
-elvis([{elvis_style, atom_naming_convention, #{regex => "^[a-z]\\w*$"}}]).

-include_lib("../../gen_src/MessageQueue.hrl").
-include("emqx_mq_quota.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/0,
    new/2,
    from_storage/2,
    to_storage/1,
    apply_updates/4,
    rotate/2,
    truncate/2,
    deadline/2,
    inspect/1
]).
-export_type([t/0, fence/0, buckets/0, seen_fence_serial/0]).

-type buckets() :: nonempty_list(#'SerialIndexBucket'{}).
%% `undefined`: unknown, so a fence already in DS is treated as seen.
%% 0: no fence seen yet.
-type seen_fence_serial() :: non_neg_integer() | undefined.
-record(serial_index, {
    buckets :: buckets(),
    seen_fence_serial :: seen_fence_serial()
}).
-opaque t() :: #serial_index{}.
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
    #serial_index{
        buckets = [#'SerialIndexBucket'{serial = 0, tsUs = 0, bytes = Bytes, count = Count}],
        seen_fence_serial = undefined
    }.

-doc "Restore an index from its stored buckets and the serial of the last seen fence".
-spec from_storage(buckets(), seen_fence_serial()) -> t().
from_storage(Buckets, SeenFenceSerial) ->
    #serial_index{buckets = Buckets, seen_fence_serial = SeenFenceSerial}.

-doc "Return the buckets and the serial of the last seen fence for storage".
-spec to_storage(t()) -> {buckets(), seen_fence_serial()}.
to_storage(#serial_index{buckets = Buckets, seen_fence_serial = SeenFenceSerial}) ->
    {Buckets, SeenFenceSerial}.

-doc "Apply one flush of updates. Return the new index and whether the caller must write a fence".
-spec apply_updates(t(), [emqx_mq_quota_index:update()], fence() | undefined, opts()) ->
    {t(), boolean()}.
apply_updates(Index0, Updates, Fence, Opts) ->
    #serial_index{buckets = Buckets0} = Index = see_fence(Index0, Fence, Opts),
    [Head | Closed0] = lists:foldl(fun apply_update/2, Buckets0, Updates),
    Closed1 = merge_closed(Closed0, Opts),
    Closed = drop_old(Closed1, Opts, 0, 0),
    NeedFence = is_full(Head, Opts),
    {Index#serial_index{buckets = [Head | Closed]}, NeedFence}.

-doc """
Drop all buckets before the fence that is stored in DS.
Keep the bucket that starts at the fence, or start a new empty head there.
Return the fence boundary. Messages stored before its timestamp can be deleted.
""".
-spec rotate(t(), fence()) -> {fence(), t()}.
rotate(#serial_index{buckets = Buckets} = Index, #{serial := Serial, ts_us := TsUs} = Fence) ->
    case [B || #'SerialIndexBucket'{serial = S} = B <- Buckets, S >= Serial] of
        [_ | _] = Kept ->
            {Fence, Index#serial_index{buckets = Kept}};
        [] ->
            Head = #'SerialIndexBucket'{serial = Serial, tsUs = TsUs, bytes = 0, count = 0},
            {Fence, #serial_index{buckets = [Head], seen_fence_serial = Serial}}
    end.

-doc """
Drop the buckets that end at or before the given DS timestamp. Keep the bucket that contains it.
Keep everything when only the bucket at (0, 0) starts that early.
""".
-spec truncate(t(), non_neg_integer()) -> t().
truncate(#serial_index{buckets = Buckets} = Index, BeforeUs) ->
    case [TsUs || #'SerialIndexBucket'{tsUs = TsUs} <- Buckets, TsUs > 0, TsUs =< BeforeUs] of
        [BoundaryUs | _] ->
            Kept = [B || #'SerialIndexBucket'{tsUs = TsUs} = B <- Buckets, TsUs >= BoundaryUs],
            Index#serial_index{buckets = Kept};
        [] ->
            Index
    end.

-doc "Return the DS timestamp before which messages can be deleted, or 0".
-spec deadline(t(), opts()) -> non_neg_integer().
deadline(#serial_index{buckets = Buckets}, Opts) ->
    limits_deadline(Buckets, Opts, 0, 0).

-doc "Return buckets and totals in a readable form".
-spec inspect(t()) -> map().
inspect(#serial_index{buckets = Buckets, seen_fence_serial = SeenFenceSerial}) ->
    #{
        records => [
            #{serial => S, ts_us => T, bytes => B, count => C}
         || #'SerialIndexBucket'{serial = S, tsUs = T, bytes = B, count = C} <- Buckets
        ],
        total => #{
            bytes => lists:sum([B || #'SerialIndexBucket'{bytes = B} <- Buckets]),
            count => lists:sum([C || #'SerialIndexBucket'{count = C} <- Buckets])
        },
        size => length(Buckets),
        seen_fence_serial => SeenFenceSerial
    }.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

%% Mark the fence seen. If it was not seen yet and the head is full, start a new empty head at it.
see_fence(#serial_index{seen_fence_serial = undefined} = Index, undefined, _Opts) ->
    %% No fence in DS, so the next fence is written for this index.
    Index#serial_index{seen_fence_serial = 0};
see_fence(Index, undefined, _Opts) ->
    Index;
see_fence(#serial_index{seen_fence_serial = undefined} = Index, #{serial := Serial}, _Opts) ->
    %% The fence may be older than the counters of a new or rebuilt index.
    Index#serial_index{seen_fence_serial = Serial};
see_fence(#serial_index{buckets = [Head | _] = Buckets} = Index, Fence, Opts) ->
    #{serial := Serial, ts_us := TsUs} = Fence,
    case is_unseen(Index, Fence) andalso is_full(Head, Opts) of
        true ->
            NewHead = #'SerialIndexBucket'{serial = Serial, tsUs = TsUs, bytes = 0, count = 0},
            #serial_index{buckets = [NewHead | Buckets], seen_fence_serial = Serial};
        false ->
            Index#serial_index{seen_fence_serial = max_seen(Index, Serial)}
    end.

is_unseen(#serial_index{seen_fence_serial = SeenSerial}, #{serial := Serial}) ->
    Serial > SeenSerial.

max_seen(#serial_index{seen_fence_serial = SeenSerial}, Serial) ->
    max(SeenSerial, Serial).

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
%% If no limit is reached, return the start of the oldest bucket.
limits_deadline([#'SerialIndexBucket'{tsUs = TsUs}], _Opts, _Bytes, _Count) ->
    TsUs;
limits_deadline(
    [#'SerialIndexBucket'{tsUs = TsUs, bytes = B, count = C} | Older], Opts, Bytes0, Count0
) ->
    Bytes = Bytes0 + B,
    Count = Count0 + C,
    case is_limit_reached(Bytes, Count, Opts) of
        true -> TsUs;
        false -> limits_deadline(Older, Opts, Bytes, Count)
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
