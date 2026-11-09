%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_tx).

-moduledoc """
Apply one flush of quota updates to a stored quota index, inside the caller's DS transaction.

The index is stored in one DS record, the index topic.
Its fence, if any, is stored in a second DS record, the fence topic.
The caller provides both topics.

Each call:

1. Reads the index. If there is none, creates one.
   The new index has a legacy part only while some node needs it.
2. Reads the fence written by the previous call, and deletes it.
3. Drops the legacy index once no node needs it.
4. Applies the updates, passing the fence to the index.
5. Writes a new fence if the index asks for one.
6. Writes the index.

A new fence is written with `?ds_tx_serial` as its value and `?ds_tx_ts_monotonic` as its timestamp.
DS replaces both at commit with the serial and the timestamp of this transaction.
See `emqx_mq_quota_index_serial` for why the index needs that.

The transaction reads the index topic, so concurrent flushes of the same index conflict,
and DS retries one of them. So a fence is used at most once.
""".

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("../emqx_mq_internal.hrl").
-include("emqx_mq_quota.hrl").

-export([update_tx/5]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Apply one flush of updates to the stored index and replace its fence".
-spec update_tx(
    emqx_ds:topic(),
    emqx_ds:topic(),
    emqx_mq_quota_index:opts(),
    [emqx_mq_quota_index:update()],
    _NeedLegacy :: boolean()
) -> ok.
update_tx(IndexTopic, FenceTopic, QuotaIndexOpts, Updates, NeedLegacy) ->
    Index0 = read_index_tx(IndexTopic, QuotaIndexOpts, Updates, NeedLegacy),
    Fence = take_fence_tx(FenceTopic),
    Index1 =
        case NeedLegacy of
            true -> Index0;
            false -> emqx_mq_quota_index:drop_legacy(Index0)
        end,
    {Index, NeedFence} = emqx_mq_quota_index:apply_updates(Index1, Updates, Fence),
    case NeedFence of
        true -> emqx_ds:tx_write({FenceTopic, ?ds_tx_ts_monotonic, ?ds_tx_serial});
        false -> ok
    end,
    emqx_ds:tx_write({IndexTopic, ?QUOTA_INDEX_TS, emqx_mq_quota_index:encode(Index)}).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

read_index_tx(IndexTopic, QuotaIndexOpts, Updates, NeedLegacy) ->
    case emqx_ds:tx_read(IndexTopic) of
        [] ->
            %% A legacy index starts at the earliest publication time of the first flush.
            IndexStartTsUs = lists:min([pub_ts_us(Update) || Update <- Updates]),
            emqx_mq_quota_index:new(QuotaIndexOpts, IndexStartTsUs, NeedLegacy);
        [{_, ?QUOTA_INDEX_TS, IndexBin}] ->
            emqx_mq_quota_index:decode(QuotaIndexOpts, IndexBin)
    end.

%% Read the fence and delete it.
take_fence_tx(FenceTopic) ->
    case emqx_ds:tx_read(FenceTopic) of
        [] ->
            undefined;
        [{_, TsUs, SerialBin}] ->
            %% Delete only up to the old fence. A new fence written in this
            %% transaction has a later timestamp and must stay.
            emqx_ds:tx_del_topic(FenceTopic, 0, TsUs + 1),
            #{serial => binary:decode_unsigned(SerialBin), ts_us => TsUs}
    end.

pub_ts_us(#quota_add{pub_ts_us = TsUs}) -> TsUs;
pub_ts_us(#quota_del{pub_ts_us = TsUs}) -> TsUs.
