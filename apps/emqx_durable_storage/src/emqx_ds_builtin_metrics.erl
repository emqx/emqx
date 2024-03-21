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

%% API:
-export([child_spec/0, init_for_db/1, shard_metric_id/2, init_for_shard/1]).
-export([
    inc_egress_batches/1,
    inc_egress_batches_retry/1,
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
    'egress.batches',
    'egress.batches.retry',
    'egress.messages',
    'egress.bytes',
    {slide, 'egress.flush_time'}
]).

-type shard_metrics_id() :: binary().

%%================================================================================
%% API functions
%%================================================================================

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    emqx_metrics_worker:child_spec(?WORKER).

-spec init_for_db(emqx_ds:db()) -> ok.
init_for_db(DB) ->
    emqx_metrics_worker:create_metrics(?WORKER, DB, ?DB_METRICS, []).

-spec shard_metric_id(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> shard_metrics_id().
shard_metric_id(DB, ShardId) ->
    iolist_to_binary([atom_to_list(DB), $/, ShardId]).

-spec init_for_shard(shard_metrics_id()) -> ok.
init_for_shard(ShardId) ->
    emqx_metrics_worker:create_metrics(?WORKER, ShardId, ?SHARD_METRICS, []).

%% @doc Increase the number of successfully flushed batches
-spec inc_egress_batches(shard_metrics_id()) -> ok.
inc_egress_batches(Id) ->
    emqx_metrics_worker:inc(?WORKER, Id, 'egress.batches').

%% @doc Increase the number of time the egress worker had to retry
%% flushing the batch
-spec inc_egress_batches_retry(shard_metrics_id()) -> ok.
inc_egress_batches_retry(Id) ->
    emqx_metrics_worker:inc(?WORKER, Id, 'egress.batches.retry').

%% @doc Increase the number of messages successfully saved to the shard
-spec inc_egress_messages(shard_metrics_id(), non_neg_integer()) -> ok.
inc_egress_messages(Id, NMessages) ->
    emqx_metrics_worker:inc(?WORKER, Id, 'egress.messages', NMessages).

%% @doc Increase the number of messages successfully saved to the shard
-spec inc_egress_bytes(shard_metrics_id(), non_neg_integer()) -> ok.
inc_egress_bytes(Id, NMessages) ->
    emqx_metrics_worker:inc(?WORKER, Id, 'egress.bytes', NMessages).

%% @doc Add a sample of time spent flushing the egress to the Raft log (in microseconds)
-spec observe_egress_flush_time(shard_metrics_id(), non_neg_integer()) -> ok.
observe_egress_flush_time(Id, FlushTime) ->
    emqx_metrics_worker:observe(?WORKER, Id, 'egress.flush_time', FlushTime).

%%================================================================================
%% Internal functions
%%================================================================================
