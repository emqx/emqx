%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_storage_layer_kv).

%% API:
-export([]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-callback create(
    emqx_ds_storage_layer:dbshard(),
    rocksdb:db_handle(),
    emqx_ds:generation(),
    Options :: map(),
    emqx_ds_storage_layer:generation_data() | undefined,
    emqx_ds:db_opts()
) ->
    {_Schema, emqx_ds_storage_layer:cf_refs()}.

%% Open the existing schema
-callback open(
    emqx_ds_storage_layer:dbshard(),
    rocksdb:db_handle(),
    emqx_ds:generation(),
    emqx_ds_storage_layer:cf_refs(),
    _Schema
) ->
    emqx_ds_storage_layer:generation_data().

%% Delete the schema and data
-callback drop(
    emqx_ds_storage_layer:dbshard(),
    rocksdb:db_handle(),
    emqx_ds:generation(),
    emqx_ds_storage_layer:cf_refs(),
    emqx_ds_storage_layer:generation_data()
) ->
    ok | {error, _Reason}.

-callback prepare_blob_tx(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    emqx_ds:tx_serial(),
    emqx_ds:blob_tx_ops(),
    emqx_ds_storage_layer:batch_prepare_opts()
) ->
    {ok, term()} | emqx_ds:error(_).

-callback commit_batch(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    _CookedBatch,
    emqx_ds_storage_layer:batch_store_opts()
) -> ok | emqx_ds:error(_).

-callback get_streams(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    emqx_ds:topic_filter(),
    _Time
) ->
    [_Stream].

-callback make_iterator(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    _Stream,
    emqx_ds:topic_filter(),
    beginning | emqx_ds:message_key()
) ->
    emqx_ds:make_iterator_result(_Iterator).

-callback next(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    Iter,
    pos_integer(),
    %% FIXME: remove
    _,
    _
) ->
    {ok, Iter, [{emqx_ds:key(), emqx_ds:kv_pair()}]} | emqx_ds:error(_).

-callback batch_events(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    _CookedBatch
) -> [_Stream].

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
