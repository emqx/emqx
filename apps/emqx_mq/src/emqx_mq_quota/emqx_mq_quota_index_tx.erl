%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_tx).

-moduledoc """
Read and write a stored quota index and its fence, inside the caller's DS transaction.

The index is stored in one DS record, the index topic.
Its fence, if any, is stored in a second DS record, the fence topic.
The caller provides both topics.

`update_tx/5` applies one flush of quota updates:

1. Reads the index. If there is none, creates one.
   The new index has a legacy part only while some node needs it.
2. Reads the fence, if any. The fence stays in DS.
3. Drops the legacy index once no node needs it.
4. Applies the updates, passing the fence to the index.
5. Replaces the fence if the index asks for a new one.
6. Writes the index.

A new fence is written with `?ds_tx_serial` as its value and `?ds_tx_ts_monotonic` as its timestamp.
DS replaces both at commit with the serial and the timestamp of this transaction.
See `emqx_mq_quota_index_serial` for why the index needs that, and how it uses a fence only once.

Every function here reads the index topic or the fence topic before writing it.
So concurrent transactions on the same index conflict, and DS retries one of them.

A purge uses the other functions:

* `write_fence_tx/1` writes a fresh fence.
* `rotate_tx/3`, in the next transaction, drops the buckets before the fence stored in DS.
* `truncate_tx/3` drops the buckets that end at or before a DS timestamp.

Quota release of reject queues uses `refresh_tx/3` to close a full head without a new flush,
then `truncate_tx/3`.
""".

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("../emqx_mq_internal.hrl").
-include("emqx_mq_quota.hrl").

-export([update_tx/5, write_fence_tx/1, rotate_tx/3, truncate_tx/3, refresh_tx/3]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc """
Close a full head at the stored fence without waiting for more publications.
Replace the fence if the index asks for it. The legacy index is dropped.
Return the index and whether a fresh fence was written, or `undefined` when there is no index.
A fresh fence closes the full head in the next refresh.
""".
-spec refresh_tx(emqx_ds:topic(), emqx_ds:topic(), emqx_mq_quota_index:opts()) ->
    {emqx_mq_quota_index:t(), _NeedFence :: boolean()} | undefined.
refresh_tx(IndexTopic, FenceTopic, QuotaIndexOpts) ->
    case emqx_ds:tx_read(IndexTopic) of
        [] ->
            undefined;
        [{_, ?QUOTA_INDEX_TS, IndexBin}] ->
            Index0 = emqx_mq_quota_index:drop_legacy(
                emqx_mq_quota_index:decode(QuotaIndexOpts, IndexBin)
            ),
            Fence = read_fence_tx(FenceTopic),
            {Index, NeedFence} = emqx_mq_quota_index:apply_updates(Index0, [], Fence),
            case NeedFence of
                true -> replace_fence_tx(FenceTopic, Fence);
                false -> ok
            end,
            case Index =:= Index0 of
                true -> ok;
                false -> write_index_tx(IndexTopic, Index)
            end,
            {Index, NeedFence}
    end.

-doc "Apply one flush of updates to the stored index. Replace the fence if the index asks for it".
-spec update_tx(
    emqx_ds:topic(),
    emqx_ds:topic(),
    emqx_mq_quota_index:opts(),
    [emqx_mq_quota_index:update()],
    _NeedLegacy :: boolean()
) -> ok.
update_tx(IndexTopic, FenceTopic, QuotaIndexOpts, Updates, NeedLegacy) ->
    Index0 = read_index_tx(IndexTopic, QuotaIndexOpts, Updates, NeedLegacy),
    Fence = read_fence_tx(FenceTopic),
    Index1 =
        case NeedLegacy of
            true -> Index0;
            false -> emqx_mq_quota_index:drop_legacy(Index0)
        end,
    {Index, NeedFence} = emqx_mq_quota_index:apply_updates(Index1, Updates, Fence),
    case NeedFence of
        true -> replace_fence_tx(FenceTopic, Fence);
        false -> ok
    end,
    write_index_tx(IndexTopic, Index).

-doc "Replace the fence with a fresh one. Its serial is the serial of this transaction".
-spec write_fence_tx(emqx_ds:topic()) -> ok.
write_fence_tx(FenceTopic) ->
    replace_fence_tx(FenceTopic, read_fence_tx(FenceTopic)).

-doc """
Drop the buckets before the fence stored in DS. See `emqx_mq_quota_index_serial:rotate/2`.
Create the index if there is none. The legacy index is dropped.
Return the fence boundary, or `{error, no_fence}` when DS has no fence.
""".
-spec rotate_tx(emqx_ds:topic(), emqx_ds:topic(), emqx_mq_quota_index:opts()) ->
    {ok, emqx_mq_quota_index_serial:fence()} | {error, no_fence}.
rotate_tx(IndexTopic, FenceTopic, QuotaIndexOpts) ->
    case read_fence_tx(FenceTopic) of
        undefined ->
            {error, no_fence};
        Fence ->
            Index0 =
                case emqx_ds:tx_read(IndexTopic) of
                    [] ->
                        emqx_mq_quota_index:new(QuotaIndexOpts, 0, false);
                    [{_, ?QUOTA_INDEX_TS, IndexBin}] ->
                        emqx_mq_quota_index:decode(QuotaIndexOpts, IndexBin)
                end,
            Index1 = emqx_mq_quota_index:drop_legacy(Index0),
            {Boundary, Index} = emqx_mq_quota_index:rotate(Index1, Fence),
            ok = write_index_tx(IndexTopic, Index),
            {ok, Boundary}
    end.

-doc """
Drop the buckets that end at or before the DS timestamp. The legacy index is dropped.
Do nothing when there is no index.
""".
-spec truncate_tx(emqx_ds:topic(), emqx_mq_quota_index:opts(), non_neg_integer()) -> ok.
truncate_tx(IndexTopic, QuotaIndexOpts, BeforeUs) ->
    case emqx_ds:tx_read(IndexTopic) of
        [] ->
            ok;
        [{_, ?QUOTA_INDEX_TS, IndexBin}] ->
            Index0 = emqx_mq_quota_index:drop_legacy(
                emqx_mq_quota_index:decode(QuotaIndexOpts, IndexBin)
            ),
            write_index_tx(IndexTopic, emqx_mq_quota_index:truncate(Index0, BeforeUs))
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

%% Read the fence, if any.
-spec read_fence_tx(emqx_ds:topic()) -> emqx_mq_quota_index_serial:fence() | undefined.
read_fence_tx(FenceTopic) ->
    case emqx_ds:tx_read(FenceTopic) of
        [] ->
            undefined;
        [{_, TsUs, SerialBin}] ->
            #{serial => binary:decode_unsigned(SerialBin), ts_us => TsUs}
    end.

read_index_tx(IndexTopic, QuotaIndexOpts, Updates, NeedLegacy) ->
    case emqx_ds:tx_read(IndexTopic) of
        [] ->
            %% A legacy index starts at the earliest publication time of the first flush.
            IndexStartTsUs = lists:min([pub_ts_us(Update) || Update <- Updates]),
            emqx_mq_quota_index:new(QuotaIndexOpts, IndexStartTsUs, NeedLegacy);
        [{_, ?QUOTA_INDEX_TS, IndexBin}] ->
            emqx_mq_quota_index:decode(QuotaIndexOpts, IndexBin)
    end.

write_index_tx(IndexTopic, Index) ->
    emqx_ds:tx_write({IndexTopic, ?QUOTA_INDEX_TS, emqx_mq_quota_index:encode(Index)}).

%% Delete the old fence and write a fresh one.
replace_fence_tx(FenceTopic, OldFence) ->
    case OldFence of
        undefined ->
            ok;
        #{ts_us := TsUs} ->
            %% Delete only up to the old fence. The new fence written in this
            %% transaction has a later timestamp and must stay.
            emqx_ds:tx_del_topic(FenceTopic, 0, TsUs + 1)
    end,
    emqx_ds:tx_write({FenceTopic, ?ds_tx_ts_monotonic, ?ds_tx_serial}).

pub_ts_us(#quota_add{pub_ts_us = TsUs}) -> TsUs;
pub_ts_us(#quota_del{pub_ts_us = TsUs}) -> TsUs.
