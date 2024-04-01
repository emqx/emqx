%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-define(DS_EGRESS_BATCHES, emqx_ds_egress_batches).
%% Number of batch flush retries:
-define(DS_EGRESS_BATCHES_RETRY, emqx_ds_egress_batches_retry).
%% Number of batches that weren't flushed due to unrecoverable errors:
-define(DS_EGRESS_BATCHES_FAILED, emqx_ds_egress_batches_failed).
%% Total number of messages that were successfully committed to the storage:
-define(DS_EGRESS_MESSAGES, emqx_ds_egress_messages).
%% Total size of payloads that were successfully committed to the storage:
-define(DS_EGRESS_BYTES, emqx_ds_egress_bytes).
%% Sliding average of flush time (microseconds):
-define(DS_EGRESS_FLUSH_TIME, emqx_ds_egress_flush_time).

%%%% Storage layer metrics:
-define(DS_STORE_BATCH_TIME, emqx_ds_store_batch_time).
-define(DS_BUILTIN_NEXT_TIME, emqx_ds_builtin_next_time).

%%% LTS Storage counters:

%% This counter is incremented when the iterator seeks to the next interval:
-define(DS_LTS_SEEK_COUNTER, emqx_ds_storage_bitfield_lts_counter_seek).
%% This counter is incremented when the iterator proceeds to the next
%% key within the interval (this is is best case scenario):
-define(DS_LTS_NEXT_COUNTER, emqx_ds_storage_bitfield_lts_counter_next).
%% This counter is incremented when the key passes bitmask check, but
%% the value is rejected by the subsequent post-processing:
-define(DS_LTS_COLLISION_COUNTER, emqx_ds_storage_bitfield_lts_counter_collision).

-endif.
