%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_METRICS_HRL).
-define(EMQX_DS_METRICS_HRL, true).

%%%% Egress metrics:

%% Number of successfully flushed batches:
-define(DS_BUFFER_BATCHES, emqx_ds_buffer_batches).
%% Number of batch flush retries:
-define(DS_BUFFER_BATCHES_RETRY, emqx_ds_buffer_batches_retry).
%% Number of batches that weren't flushed due to unrecoverable errors:
-define(DS_BUFFER_BATCHES_FAILED, emqx_ds_buffer_batches_failed).
%% Total number of messages that were successfully committed to the storage:
-define(DS_BUFFER_MESSAGES, emqx_ds_buffer_messages).
%% Total size of payloads that were successfully committed to the storage:
-define(DS_BUFFER_BYTES, emqx_ds_buffer_bytes).
%% Sliding average of flush time (microseconds):
-define(DS_BUFFER_FLUSH_TIME, emqx_ds_buffer_flush_time).
%% Sliding average of latency induced by buffering (milliseconds):
-define(DS_BUFFER_LATENCY, emqx_ds_buffer_latency).

%%%% Storage layer metrics:
-define(DS_STORE_BATCH_TIME, emqx_ds_store_batch_time).
-define(DS_BUILTIN_NEXT_TIME, emqx_ds_builtin_next_time).

%%% Bitfield LTS Storage counters:

%% This counter is incremented when the iterator seeks to the next interval:
-define(DS_BITFIELD_LTS_SEEK_COUNTER, emqx_ds_storage_bitfield_lts_counter_seek).
%% This counter is incremented when the iterator proceeds to the next
%% key within the interval (this is is best case scenario):
-define(DS_BITFIELD_LTS_NEXT_COUNTER, emqx_ds_storage_bitfield_lts_counter_next).
%% This counter is incremented when the key passes bitmask check, but
%% the value is rejected by the subsequent post-processing:
-define(DS_BITFIELD_LTS_COLLISION_COUNTER, emqx_ds_storage_bitfield_lts_counter_collision).

%%% Skipstream LTS Storage counters:
-define(DS_SKIPSTREAM_LTS_SEEK, emqx_ds_storage_skipstream_lts_seek).
-define(DS_SKIPSTREAM_LTS_NEXT, emqx_ds_storage_skipstream_lts_next).
-define(DS_SKIPSTREAM_LTS_HASH_COLLISION, emqx_ds_storage_skipstream_lts_hash_collision).
-define(DS_SKIPSTREAM_LTS_HIT, emqx_ds_storage_skipstream_lts_hit).
-define(DS_SKIPSTREAM_LTS_MISS, emqx_ds_storage_skipstream_lts_miss).
-define(DS_SKIPSTREAM_LTS_FUTURE, emqx_ds_storage_skipstream_lts_future).
-define(DS_SKIPSTREAM_LTS_EOS, emqx_ds_storage_skipstream_lts_end_of_stream).

%%%% Metrics related to subscription:
%% Number of active subscriptions:
-define(DS_SUBS, emqx_ds_subs).
%% Total number of beams sent by all workers:
-define(DS_SUBS_BEAMS_SENT_TOTAL, emqx_ds_subs_beams_sent_total).
%% Measure of "beam coherence": average number of requests fulfilled
%% by a single beam:
-define(DS_SUBS_REQUEST_SHARING, emqx_ds_subs_request_sharing).
%% Number of subscription stuck/unstuck events:
-define(DS_SUBS_STUCK_TOTAL, emqsx_ds_subs_stuck_total).
-define(DS_SUBS_UNSTUCK_TOTAL, emqsx_ds_subs_unstuck_total).
%% Sliding average of time spent fulfilling requests per worker type (μs):
-define(DS_SUBS_FULFILL_TIME, emqx_ds_subs_fulfill_time).
%% Sliding average of time spent scanning the DB (μs):
-define(DS_SUBS_SCAN_TIME, emqx_ds_subs_scan_time).
%% Sliding average of time spent splitting the beam:
-define(DS_SUBS_FANOUT_TIME, emqx_ds_subs_fanout_time).

-endif.
