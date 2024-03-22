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
-module(emqx_ds_builtin_metrics).

%% DS-facing API:
-export([child_spec/0, init_for_db/1, shard_metric_id/2, init_for_shard/1]).

%% Prometheus-facing API:
-export([prometheus_meta/0, prometheus_collect/1]).

-export([
    inc_egress_batches/1,
    inc_egress_batches_retry/1,
    inc_egress_batches_failed/1,
    inc_egress_messages/2,
    inc_egress_bytes/2,
    observe_egress_flush_time/2
]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([shard_metrics_id/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(WORKER, ?MODULE).

-define(DB_METRICS, []).

-define(SHARD_METRICS, [
    {counter, 'emqx_ds_egress_batches'},
    {counter, 'emqx_ds_egress_batches_retry'},
    {counter, 'emqx_ds_egress_batches_failed'},
    {counter, 'emqx_ds_egress_messages'},
    {counter, 'emqx_ds_egress_bytes'},
    {slide, 'emqx_ds_egress_flush_time'}
]).

-type shard_metrics_id() :: binary().

%%================================================================================
%% API functions
%%================================================================================

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    emqx_metrics_worker:child_spec(?WORKER).

%% @doc Initialize metrics that are global for a DS database
-spec init_for_db(emqx_ds:db()) -> ok.
init_for_db(_DB) ->
    ok.

-spec shard_metric_id(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> shard_metrics_id().
shard_metric_id(DB, ShardId) ->
    iolist_to_binary([atom_to_list(DB), $/, ShardId]).

%% @doc Initialize metrics that are specific for the shard.
-spec init_for_shard(shard_metrics_id()) -> ok.
init_for_shard(ShardId) ->
    emqx_metrics_worker:create_metrics(?WORKER, ShardId, ?SHARD_METRICS, []).

%% @doc Increase the number of successfully flushed batches
-spec inc_egress_batches(shard_metrics_id()) -> ok.
inc_egress_batches(Id) ->
    emqx_metrics_worker:inc(?WORKER, Id, 'emqx_ds_egress_batches').

%% @doc Increase the number of time the egress worker had to retry
%% flushing the batch
-spec inc_egress_batches_retry(shard_metrics_id()) -> ok.
inc_egress_batches_retry(Id) ->
    emqx_metrics_worker:inc(?WORKER, Id, 'emqx_ds_egress_batches_retry').

%% @doc Increase the number of time the egress worker encountered an
%% unrecoverable error while trying to flush the batch
-spec inc_egress_batches_failed(shard_metrics_id()) -> ok.
inc_egress_batches_failed(Id) ->
    emqx_metrics_worker:inc(?WORKER, Id, 'emqx_ds_egress_batches_failed').

%% @doc Increase the number of messages successfully saved to the shard
-spec inc_egress_messages(shard_metrics_id(), non_neg_integer()) -> ok.
inc_egress_messages(Id, NMessages) ->
    emqx_metrics_worker:inc(?WORKER, Id, 'emqx_ds_egress_messages', NMessages).

%% @doc Increase the number of messages successfully saved to the shard
-spec inc_egress_bytes(shard_metrics_id(), non_neg_integer()) -> ok.
inc_egress_bytes(Id, NMessages) ->
    emqx_metrics_worker:inc(?WORKER, Id, 'emqx_ds_egress_bytes', NMessages).

%% @doc Add a sample of elapsed time spent flushing the egress to the
%% Raft log (in microseconds)
-spec observe_egress_flush_time(shard_metrics_id(), non_neg_integer()) -> ok.
observe_egress_flush_time(Id, FlushTime) ->
    emqx_metrics_worker:observe(?WORKER, Id, 'emqx_ds_egress_flush_time', FlushTime).

prometheus_meta() ->
    lists:map(
        fun
            ({counter, A}) ->
                {A, counter, A};
            ({slide, A}) ->
                {A, counter, A}
        end,
        ?SHARD_METRICS
    ).

prometheus_collect(NodeOrAggr) ->
    prometheus_per_shard(NodeOrAggr).

%% This function returns the data in the following format:
%% ```
%% #{emqx_ds_egress_batches =>
%%       [{[{db,emqx_persistent_message},{shard,<<"1">>}],99408},
%%        {[{db,emqx_persistent_message},{shard,<<"0">>}],99409}],
%%   emqx_ds_egress_batches_retry =>
%%       [{[{db,emqx_persistent_message},{shard,<<"1">>}],0},
%%        {[{db,emqx_persistent_message},{shard,<<"0">>}],0}],
%%   emqx_ds_egress_messages =>
%%        ...
%%  }
%% '''
%%
%% If `NodeOrAggr' = `node' then node name is appended to the list of
%% labels.
prometheus_per_shard(NodeOrAggr) ->
    lists:foldl(
        fun(DB, Acc0) ->
            lists:foldl(
                fun(Shard, Acc) ->
                    prometheus_per_shard(NodeOrAggr, DB, Shard, Acc)
                end,
                Acc0,
                emqx_ds_replication_layer_meta:shards(DB)
            )
        end,
        #{},
        emqx_ds_builtin_db_sup:which_dbs()
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
