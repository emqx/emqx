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
-export([child_spec/0, init_for_db/1, init_for_shard/2]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(WORKER, ?MODULE).

-define(DB_METRICS,
        [

        ]).

-define(SHARD_METRICS,
        [
         'egress.bytes',
         'egress.batches',
         'egress.messages',
         {slide, 'egress.flush_time'}
        ]).

%%================================================================================
%% API functions
%%================================================================================

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    emqx_metrics_worker:child_spec(?WORKER).

-spec init_for_db(emqx_ds:db()) -> ok.
init_for_db(DB) ->
    emqx_metrics_worker:create_metrics(?WORKER, DB, ?DB_METRICS, []).

-spec init_for_shard(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> ok.
init_for_shard(DB, ShardId) ->
    Id = iolist_to_binary([atom_to_list(DB), $/, ShardId]),
    emqx_metrics_worker:create_metrics(?WORKER, Id, ?SHARD_METRICS, []).

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
