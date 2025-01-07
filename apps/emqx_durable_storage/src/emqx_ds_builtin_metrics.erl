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
-module(emqx_ds_builtin_metrics).

%% DS-facing API:
-export([child_spec/0, init_for_db/1, shard_metric_id/2, init_for_shard/1]).

%% Prometheus-facing API:
-export([prometheus_meta/0, prometheus_collect/1]).

-export([
    inc_buffer_batches/1,
    inc_buffer_batches_retry/1,
    inc_buffer_batches_failed/1,
    inc_buffer_messages/2,
    inc_buffer_bytes/2,

    observe_buffer_flush_time/2,
    observe_buffer_latency/2,

    observe_store_batch_time/2,

    observe_next_time/2,

    observe_sharing/2,
    set_waitq_len/2,
    set_pendingq_len/2,
    inc_poll_requests/2,
    inc_poll_requests_fulfilled/2,
    inc_poll_requests_dropped/2,
    inc_poll_requests_expired/2,

    inc_lts_seek_counter/2,
    inc_lts_next_counter/2,
    inc_lts_collision_counter/2,

    collect_shard_counter/3
]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([shard_metrics_id/0]).

-include("emqx_ds_metrics.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(WORKER, ?MODULE).

-define(STORAGE_LAYER_METRICS, [
    {slide, ?DS_STORE_BATCH_TIME},
    {counter, ?DS_BITFIELD_LTS_SEEK_COUNTER},
    {counter, ?DS_BITFIELD_LTS_NEXT_COUNTER},
    {counter, ?DS_BITFIELD_LTS_COLLISION_COUNTER},
    {counter, ?DS_SKIPSTREAM_LTS_SEEK},
    {counter, ?DS_SKIPSTREAM_LTS_NEXT},
    {counter, ?DS_SKIPSTREAM_LTS_HASH_COLLISION},
    {counter, ?DS_SKIPSTREAM_LTS_HIT},
    {counter, ?DS_SKIPSTREAM_LTS_MISS},
    {counter, ?DS_SKIPSTREAM_LTS_FUTURE},
    {counter, ?DS_SKIPSTREAM_LTS_EOS}
]).

-define(FETCH_METRICS, [
    {slide, ?DS_BUILTIN_NEXT_TIME}
]).

-define(DB_METRICS, ?STORAGE_LAYER_METRICS ++ ?FETCH_METRICS).

-define(BUFFER_METRICS, [
    {counter, ?DS_BUFFER_BATCHES},
    {counter, ?DS_BUFFER_BATCHES_RETRY},
    {counter, ?DS_BUFFER_BATCHES_FAILED},
    {counter, ?DS_BUFFER_MESSAGES},
    {counter, ?DS_BUFFER_BYTES},
    {slide, ?DS_BUFFER_FLUSH_TIME},
    {slide, ?DS_BUFFER_LATENCY}
]).

-define(BEAMFORMER_METRICS, [
    {counter, ?DS_POLL_REQUESTS},
    {counter, ?DS_POLL_REQUESTS_FULFILLED},
    {counter, ?DS_POLL_REQUESTS_DROPPED},
    {counter, ?DS_POLL_REQUESTS_EXPIRED},
    {slide, ?DS_POLL_REQUEST_SHARING},
    {counter, ?DS_POLL_PENDING_QUEUE_LEN},
    {counter, ?DS_POLL_WAITING_QUEUE_LEN}
]).

-define(SHARD_METRICS, ?BEAMFORMER_METRICS ++ ?BUFFER_METRICS).

-type shard_metrics_id() :: binary().

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

%%================================================================================
%% API functions
%%================================================================================

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    emqx_metrics_worker:child_spec(?WORKER).

%% @doc Initialize metrics that are global for a DS database
-spec init_for_db(emqx_ds:db()) -> ok.
init_for_db(DB) ->
    emqx_metrics_worker:create_metrics(?WORKER, DB, ?DB_METRICS, []).

-spec shard_metric_id(emqx_ds:db(), binary()) -> shard_metrics_id().
shard_metric_id(DB, ShardId) ->
    iolist_to_binary([atom_to_list(DB), $/, ShardId]).

%% @doc Initialize metrics that are specific for the shard.
-spec init_for_shard(shard_metrics_id()) -> ok.
init_for_shard(ShardId) ->
    emqx_metrics_worker:create_metrics(?WORKER, ShardId, ?SHARD_METRICS, []).

%% @doc Increase the number of successfully flushed batches
-spec inc_buffer_batches(shard_metrics_id()) -> ok.
inc_buffer_batches(Id) ->
    catch emqx_metrics_worker:inc(?WORKER, Id, ?DS_BUFFER_BATCHES).

%% @doc Increase the number of time the buffer worker had to retry
%% flushing the batch
-spec inc_buffer_batches_retry(shard_metrics_id()) -> ok.
inc_buffer_batches_retry(Id) ->
    catch emqx_metrics_worker:inc(?WORKER, Id, ?DS_BUFFER_BATCHES_RETRY).

%% @doc Increase the number of time the buffer worker encountered an
%% unrecoverable error while trying to flush the batch
-spec inc_buffer_batches_failed(shard_metrics_id()) -> ok.
inc_buffer_batches_failed(Id) ->
    catch emqx_metrics_worker:inc(?WORKER, Id, ?DS_BUFFER_BATCHES_FAILED).

%% @doc Increase the number of messages successfully saved to the shard
-spec inc_buffer_messages(shard_metrics_id(), non_neg_integer()) -> ok.
inc_buffer_messages(Id, NMessages) ->
    catch emqx_metrics_worker:inc(?WORKER, Id, ?DS_BUFFER_MESSAGES, NMessages).

%% @doc Increase the number of messages successfully saved to the shard
-spec inc_buffer_bytes(shard_metrics_id(), non_neg_integer()) -> ok.
inc_buffer_bytes(Id, NMessages) ->
    catch emqx_metrics_worker:inc(?WORKER, Id, ?DS_BUFFER_BYTES, NMessages).

%% @doc Add a sample of elapsed time spent flushing the buffer to the
%% backend (in microseconds)
-spec observe_buffer_flush_time(shard_metrics_id(), non_neg_integer()) -> ok.
observe_buffer_flush_time(Id, FlushTime) ->
    catch emqx_metrics_worker:observe(?WORKER, Id, ?DS_BUFFER_FLUSH_TIME, FlushTime).

%% @doc Add a sample of latency induced by the buffer (milliseconds).
%% Latency is calculated as difference between timestamp of the oldest
%% message in the flushed batch and current time.
-spec observe_buffer_latency(shard_metrics_id(), non_neg_integer()) -> ok.
observe_buffer_latency(Id, FlushTime) ->
    catch emqx_metrics_worker:observe(?WORKER, Id, ?DS_BUFFER_LATENCY, FlushTime).

-spec observe_store_batch_time(emqx_ds_storage_layer:shard_id(), non_neg_integer()) -> ok.
observe_store_batch_time({DB, _}, StoreTime) ->
    catch emqx_metrics_worker:observe(?WORKER, DB, ?DS_STORE_BATCH_TIME, StoreTime).

%% @doc Add a sample of elapsed time spent waiting for a batch
%% `emqx_ds_replication_layer:next'
-spec observe_next_time(emqx_ds:db(), non_neg_integer()) -> ok.
observe_next_time(DB, NextTime) ->
    catch emqx_metrics_worker:observe(?WORKER, DB, ?DS_BUILTIN_NEXT_TIME, NextTime).

observe_sharing(Id, Sharing) ->
    catch emqx_metrics_worker:observe(?WORKER, Id, ?DS_POLL_REQUEST_SHARING, Sharing).

set_waitq_len(Id, Len) ->
    emqx_metrics_worker:set(?WORKER, Id, ?DS_POLL_WAITING_QUEUE_LEN, Len).

set_pendingq_len(Id, Len) ->
    emqx_metrics_worker:set(?WORKER, Id, ?DS_POLL_PENDING_QUEUE_LEN, Len).

inc_poll_requests(Id, NPolls) ->
    catch emqx_metrics_worker:inc(?WORKER, Id, ?DS_POLL_REQUESTS, NPolls).

inc_poll_requests_fulfilled(Id, NPolls) ->
    catch emqx_metrics_worker:inc(?WORKER, Id, ?DS_POLL_REQUESTS_FULFILLED, NPolls).

inc_poll_requests_expired(Id, NPolls) ->
    catch emqx_metrics_worker:inc(?WORKER, Id, ?DS_POLL_REQUESTS_EXPIRED, NPolls).

inc_poll_requests_dropped(Id, N) ->
    catch emqx_metrics_worker:inc(?WORKER, Id, ?DS_POLL_REQUESTS_DROPPED, N).

-spec inc_lts_seek_counter(emqx_ds_storage_layer:shard_id(), non_neg_integer()) -> ok.
inc_lts_seek_counter({DB, _}, Inc) ->
    catch emqx_metrics_worker:inc(?WORKER, DB, ?DS_BITFIELD_LTS_SEEK_COUNTER, Inc).

-spec inc_lts_next_counter(emqx_ds_storage_layer:shard_id(), non_neg_integer()) -> ok.
inc_lts_next_counter({DB, _}, Inc) ->
    catch emqx_metrics_worker:inc(?WORKER, DB, ?DS_BITFIELD_LTS_NEXT_COUNTER, Inc).

-spec inc_lts_collision_counter(emqx_ds_storage_layer:shard_id(), non_neg_integer()) -> ok.
inc_lts_collision_counter({DB, _}, Inc) ->
    catch emqx_metrics_worker:inc(?WORKER, DB, ?DS_BITFIELD_LTS_COLLISION_COUNTER, Inc).

-spec collect_shard_counter(emqx_ds_storage_layer:shard_id(), atom(), non_neg_integer()) -> ok.
collect_shard_counter({DB, _}, Key, Inc) ->
    catch emqx_metrics_worker:inc(?WORKER, DB, Key, Inc).

prometheus_meta() ->
    lists:map(
        fun
            ({counter, A}) ->
                {A, counter, A};
            ({slide, A}) ->
                {A, counter, A}
        end,
        ?DB_METRICS ++ ?SHARD_METRICS
    ).

prometheus_collect(NodeOrAggr) ->
    maps:merge(prometheus_per_db(NodeOrAggr), prometheus_per_shard(NodeOrAggr)).

prometheus_per_db(NodeOrAggr) ->
    lists:foldl(
        fun
            ({DB, Backend}, Acc) when Backend =:= builtin_local; Backend =:= builtin_raft ->
                prometheus_per_db(NodeOrAggr, DB, Acc);
            ({_, _}, Acc) ->
                Acc
        end,
        #{},
        emqx_ds:which_dbs()
    ).

%% This function returns the data in the following format:
%% ```
%% #{emqx_ds_store_batch_time =>
%%     [{[{db, messages}], 42}],
%%  ...
%% '''
%%
%% If `NodeOrAggr' = `aggr' then node name is appended to the list of
%% labels.
prometheus_per_db(NodeOrAggr, DB, Acc0) ->
    Labels = [
        {db, DB}
        | case NodeOrAggr of
            node -> [];
            _ -> [{node, node()}]
        end
    ],
    #{counters := CC, slides := SS} = emqx_metrics_worker:get_metrics(?WORKER, DB),
    %% Collect counters:
    Acc1 = maps:fold(
        fun(MetricId, Value, Acc1) ->
            append_to_key(MetricId, {Labels, Value}, Acc1)
        end,
        Acc0,
        CC
    ),
    %% Collect slides:
    maps:fold(
        fun(MetricId, Value, Acc2) ->
            Acc3 = append_to_key(MetricId, slide_value(current, Value, Labels), Acc2),
            append_to_key(MetricId, slide_value(last5m, Value, Labels), Acc3)
        end,
        Acc1,
        SS
    ).

%% This function returns the data in the following format:
%% ```
%% #{emqx_ds_buffer_batches =>
%%       [{[{db,messages},{shard,<<"1">>}],99408},
%%        {[{db,messages},{shard,<<"0">>}],99409}],
%%   emqx_ds_buffer_batches_retry =>
%%       [{[{db,messages},{shard,<<"1">>}],0},
%%        {[{db,messages},{shard,<<"0">>}],0}],
%%   emqx_ds_buffer_messages =>
%%        ...
%%  }
%% '''
%%
%% If `NodeOrAggr' = `node' then node name is appended to the list of
%% labels.
prometheus_per_shard(NodeOrAggr) ->
    prometheus_buffer_metrics(NodeOrAggr).

prometheus_buffer_metrics(NodeOrAggr) ->
    lists:foldl(
        fun({DB, Shard}, Acc) ->
            prometheus_per_shard(NodeOrAggr, DB, Shard, Acc)
        end,
        #{},
        emqx_ds_buffer:ls()
    ).

prometheus_per_shard(NodeOrAggr, DB, Shard, Acc0) ->
    Labels = [
        {db, DB},
        {shard, Shard}
        | case NodeOrAggr of
            node -> [];
            _ -> [{node, node()}]
        end
    ],
    #{counters := CC, slides := SS} = emqx_metrics_worker:get_metrics(
        ?WORKER, shard_metric_id(DB, Shard)
    ),
    %% Collect counters:
    Acc1 = maps:fold(
        fun(MetricId, Value, Acc1) ->
            append_to_key(MetricId, {Labels, Value}, Acc1)
        end,
        Acc0,
        CC
    ),
    %% Collect slides:
    maps:fold(
        fun(MetricId, Value, Acc2) ->
            Acc3 = append_to_key(MetricId, slide_value(current, Value, Labels), Acc2),
            append_to_key(MetricId, slide_value(last5m, Value, Labels), Acc3)
        end,
        Acc1,
        SS
    ).

-spec append_to_key(K, V, #{K => [V]}) -> #{K => [V]}.
append_to_key(Key, Value, Map) ->
    maps:update_with(
        Key,
        fun(L) ->
            [Value | L]
        end,
        [Value],
        Map
    ).

slide_value(Interval, Value, Labels0) ->
    Labels = [{interval, Interval} | Labels0],
    {Labels, maps:get(Interval, Value, 0)}.

%%================================================================================
%% Internal functions
%%================================================================================
