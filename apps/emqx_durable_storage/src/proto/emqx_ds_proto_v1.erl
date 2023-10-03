%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_proto_v1).

-behavior(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").
%% API:
-export([open_shard/3, get_streams/4, make_iterator/4, next/4]).

%% behavior callbacks:
-export([introduced_in/0]).

%%================================================================================
%% API funcions
%%================================================================================

-spec open_shard(node(), emqx_ds_replication_layer:shard(), emqx_ds:create_db_opts()) ->
    ok.
open_shard(Node, Shard, Opts) ->
    erpc:call(Node, emqx_ds_replication_layer, do_open_shard_v1, [Shard, Opts]).

-spec get_streams(
    node(), emqx_ds_replication_layer:shard(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [{integer(), emqx_ds_replication_layer:stream()}].
get_streams(Node, Shard, TopicFilter, Time) ->
    erpc:call(Node, emqx_ds_replication_layer, do_get_streams_v1, [Shard, TopicFilter, Time]).

-spec make_iterator(node(), emqx_ds_replication_layer:shard(), _Stream, emqx_ds:time()) ->
    {ok, emqx_ds_replication_layer:iterator()} | {error, _}.
make_iterator(Node, Shard, Stream, StartTime) ->
    erpc:call(Node, emqx_ds_replication_layer, do_make_iterator_v1, [Shard, Stream, StartTime]).

-spec next(
    node(), emqx_ds_replication_layer:shard(), emqx_ds_replication_layer:iterator(), pos_integer()
) ->
    {ok, emqx_ds_replication_layer:iterator(), [emqx_types:messages()]}
    | {ok, end_of_stream}
    | {error, _}.
next(Node, Shard, Iter, BatchSize) ->
    erpc:call(Node, emqx_ds_replication_layer, do_next_v1, [Shard, Iter, BatchSize]).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() ->
    %% FIXME
    "5.3.0".
