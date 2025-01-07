%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%%%% Poll metrics:
%% Total number of incoming poll requests:
-define(DS_POLL_REQUESTS, emqx_ds_poll_requests).
%% Number of fulfilled requests:
-define(DS_POLL_REQUESTS_FULFILLED, emqx_ds_poll_requests_fulfilled).
%% Number of requests dropped due to OLP:
-define(DS_POLL_REQUESTS_DROPPED, emqx_ds_poll_requests_dropped).
%% Number of requests that expired while waiting for new messages:
-define(DS_POLL_REQUESTS_EXPIRED, emqx_ds_poll_requests_expired).
%% Measure of "beam coherence": average number of requests fulfilled
%% by a single beam:
-define(DS_POLL_REQUEST_SHARING, emqx_ds_poll_request_sharing).

-define(DS_POLL_WAITING_QUEUE_LEN, emqx_ds_poll_waitq_len).
-define(DS_POLL_PENDING_QUEUE_LEN, emqx_ds_poll_pendingq_len).

-endif.
