%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index).

-moduledoc """
The quota index counts the messages and bytes that one limited queue or stream stores in one DS slab.
Garbage collection uses its deadline: messages stored before the deadline can be deleted,
and the newer messages still meet the count and byte limits.

The index does not keep an entry per message. It keeps a short list of buckets with totals.
So the deadline is approximate: GC can keep somewhat more data than the limits.

## Updates

The quota buffer collects updates and flushes them periodically.
`emqx_mq_quota_index_tx` applies a flush inside a DS transaction.

* `#quota_add{}`: a message was written. Its bytes and count are added.
* `#quota_del{}`: a last-value message was replaced. The old message's bytes and count are subtracted.

## Two index formats

This module stores two indices in one record and hides the difference from its callers.

* The serial index, `emqx_mq_quota_index_serial`, is the current one.
  It places messages by DS serial and DS timestamp. Read its documentation to see how buckets work.
  When serial index need to close a bucket, it asks to write a `fence` entry to DS,
  to read closing timestamp and serial values back from it on the next flush.
  On the next flush, these values are used to close the bucket.
* The legacy index, `emqx_mq_quota_index_legacy`, places messages by publication time.
  Older nodes understand only this index.

## Rolling upgrade

While some cluster node does not support the serial index, keep both indices up to date,
and use the legacy deadline. Older nodes use the same legacy data.
When every node supports the serial index (see `is_serial_index_supported/0`),
the next flush drops the legacy index, and the serial deadline is used from then on.
Stopped nodes count too. A node that left the cluster still counts until the core nodes restart,
so the legacy index is kept, and purge is refused, until then.

An older node keeps only the legacy index when it rewrites the record.
The next flush on a new node then rebuilds the serial index:
one bucket at (0, 0) with the totals that the legacy index keeps.

## Purge

`rotate/2` and `truncate/2` change the serial index before a purge deletes messages.
They require that the legacy index is dropped: a purge runs only when every node supports the serial index.
""".

%% ASN1-generated record field names are in camelCase.
-elvis([{elvis_style, atom_naming_convention, #{regex => "^[a-z]\\w*$"}}]).

-include_lib("../../gen_src/MessageQueue.hrl").
-include("emqx_mq_quota.hrl").

-export([
    new/3,
    decode/2,
    encode/1,
    apply_updates/3,
    rotate/2,
    truncate/2,
    drop_legacy/1,
    deadline/1,
    inspect/1,
    is_serial_index_supported/0
]).
-export_type([t/0, opts/0, update/0, timestamp_us/0]).

-type opts() :: emqx_mq_quota_index_legacy:opts().
-type update() :: #quota_add{} | #quota_del{}.
-type timestamp_us() :: non_neg_integer().
-record(index, {
    opts :: opts(),
    legacy :: emqx_mq_quota_index_legacy:t() | undefined,
    serial :: emqx_mq_quota_index_serial:t()
}).
-opaque t() :: #index{}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc """
Return true when every node supports the serial index, so the legacy index can be dropped.

Nodes that support the serial index announce version 2 of the `emqx_mq_consumer` BPAPI.
The cluster version of a BPAPI is the lowest version that the announcing nodes support.
Nodes keep their announcements when they stop, so a stopped older replicant keeps the legacy index.
""".
-spec is_serial_index_supported() -> boolean().
is_serial_index_supported() ->
    try emqx_bpapi:supported_version(emqx_mq_consumer) of
        Version -> Version >= 2
    catch
        error:badarg -> false
    end.

-doc """
Create an index. Add a legacy index only while some node needs it.
The legacy index starts at the given publication time.
""".
-spec new(opts(), timestamp_us(), _NeedLegacy :: boolean()) -> t().
new(Opts, TsUs, NeedLegacy) ->
    Legacy =
        case NeedLegacy of
            true -> emqx_mq_quota_index_legacy:new(Opts, TsUs);
            false -> undefined
        end,
    #index{opts = Opts, legacy = Legacy, serial = emqx_mq_quota_index_serial:new()}.

-doc "Decode a stored index. Rebuild the serial index if an older node removed it".
-spec decode(opts(), binary()) -> t().
decode(Opts, Bin) ->
    Storage = 'MessageQueue':decode('StorageIndex', Bin),
    Legacy = decode_legacy(Opts, Storage),
    Serial =
        case Storage of
            #'StorageIndex'{serialBuckets = asn1_NOVALUE} ->
                rebuild_serial(Storage, Legacy);
            #'StorageIndex'{serialBuckets = Buckets, seenFenceSerial = SeenFenceSerial} ->
                emqx_mq_quota_index_serial:from_storage(Buckets, from_asn1(SeenFenceSerial))
        end,
    #index{opts = Opts, legacy = Legacy, serial = Serial}.

-doc "Encode both indices. The legacy fields are left out after the legacy index is dropped".
-spec encode(t()) -> binary().
encode(#index{legacy = Legacy, serial = Serial}) ->
    Storage = legacy_storage(Legacy),
    {Buckets, SeenFenceSerial} = emqx_mq_quota_index_serial:to_storage(Serial),
    'MessageQueue':encode('StorageIndex', Storage#'StorageIndex'{
        serialBuckets = Buckets, seenFenceSerial = to_asn1(SeenFenceSerial)
    }).

-doc "Apply one flush of updates to both indices. Return the index and whether to write a fence".
-spec apply_updates(t(), [update()], emqx_mq_quota_index_serial:fence() | undefined) ->
    {t(), boolean()}.
apply_updates(#index{opts = Opts, serial = Serial0} = Index0, Updates, Fence) ->
    {Serial, NeedFence} = emqx_mq_quota_index_serial:apply_updates(Serial0, Updates, Fence, Opts),
    Index = apply_legacy_updates(Index0, Updates),
    {Index#index{serial = Serial}, NeedFence}.

-doc "Drop the buckets before the fence stored in DS. See `emqx_mq_quota_index_serial`".
-spec rotate(t(), emqx_mq_quota_index_serial:fence()) -> {emqx_mq_quota_index_serial:fence(), t()}.
rotate(#index{legacy = undefined, serial = Serial0} = Index, Fence) ->
    {Boundary, Serial} = emqx_mq_quota_index_serial:rotate(Serial0, Fence),
    {Boundary, Index#index{serial = Serial}}.

-doc "Drop the buckets that end at or before a DS timestamp. See `emqx_mq_quota_index_serial`".
-spec truncate(t(), timestamp_us()) -> t().
truncate(#index{legacy = undefined, serial = Serial0} = Index, BeforeUs) ->
    Index#index{serial = emqx_mq_quota_index_serial:truncate(Serial0, BeforeUs)}.

-doc "Drop the legacy index. Call only when every node supports the serial index".
-spec drop_legacy(t()) -> t().
drop_legacy(Index) ->
    Index#index{legacy = undefined}.

-doc "Return the GC deadline: from the legacy index while it exists, else from the serial index".
-spec deadline(t()) -> timestamp_us().
deadline(#index{legacy = undefined, serial = Serial, opts = Opts}) ->
    emqx_mq_quota_index_serial:deadline(Serial, Opts);
deadline(#index{legacy = Legacy}) ->
    emqx_mq_quota_index_legacy:deadline(Legacy).

-doc "Return both indices in a readable form. `legacy` is undefined after it is dropped".
-spec inspect(t()) -> map().
inspect(#index{opts = Opts, legacy = Legacy, serial = Serial}) ->
    #{
        opts => Opts,
        legacy => inspect_legacy(Legacy),
        serial => emqx_mq_quota_index_serial:inspect(Serial)
    }.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

apply_legacy_updates(#index{legacy = undefined} = Index, _Updates) ->
    Index;
apply_legacy_updates(#index{legacy = Legacy0} = Index, Updates) ->
    Legacy = emqx_mq_quota_index_legacy:apply_updates(Legacy0, Updates),
    Index#index{legacy = Legacy}.

decode_legacy(_Opts, #'StorageIndex'{bytes = asn1_NOVALUE, count = asn1_NOVALUE}) ->
    undefined;
decode_legacy(Opts, Storage) ->
    emqx_mq_quota_index_legacy:from_storage(Opts, Storage).

%% Put the totals of the legacy buckets that GC keeps into one serial bucket at (0, 0).
rebuild_serial(#'StorageIndex'{bytes = Bytes, count = Count}, Legacy) ->
    Deadline = emqx_mq_quota_index_legacy:deadline(Legacy),
    emqx_mq_quota_index_serial:new(kept_total(Bytes, Deadline), kept_total(Count, Deadline)).

kept_total(Records, Deadline) ->
    lists:sum([
        Value
     || #'StorageIndexRecord'{tsUs = TsUs, value = Value} <- Records, TsUs >= Deadline
    ]).

legacy_storage(undefined) ->
    #'StorageIndex'{maxTsUs = 0, bytes = asn1_NOVALUE, count = asn1_NOVALUE};
legacy_storage(Legacy) ->
    emqx_mq_quota_index_legacy:to_storage(Legacy).

inspect_legacy(undefined) ->
    undefined;
inspect_legacy(Legacy) ->
    emqx_mq_quota_index_legacy:inspect(Legacy).

from_asn1(asn1_NOVALUE) -> undefined;
from_asn1(Value) -> Value.

to_asn1(undefined) -> asn1_NOVALUE;
to_asn1(Value) -> Value.
