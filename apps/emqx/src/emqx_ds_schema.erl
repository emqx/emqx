%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Schema for EMQX_DS databases.
-module(emqx_ds_schema).

%% API:
-export([schema/0, translate_builtin/1]).

%% Behavior callbacks:
-export([fields/1, desc/1, namespace/0]).

-include("emqx_schema.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("hocon/include/hocon_types.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API
%%================================================================================

translate_builtin(#{
    backend := builtin,
    n_shards := NShards,
    replication_factor := ReplFactor,
    layout := Layout
}) ->
    Storage =
        case Layout of
            #{
                type := wildcard_optimized,
                bits_per_topic_level := BitsPerTopicLevel,
                epoch_bits := EpochBits,
                topic_index_bytes := TIBytes
            } ->
                {emqx_ds_storage_bitfield_lts, #{
                    bits_per_topic_level => BitsPerTopicLevel,
                    topic_index_bytes => TIBytes,
                    epoch_bits => EpochBits
                }};
            #{type := reference} ->
                {emqx_ds_storage_reference, #{}}
        end,
    #{
        backend => builtin,
        n_shards => NShards,
        replication_factor => ReplFactor,
        storage => Storage
    }.

%%================================================================================
%% Behavior callbacks
%%================================================================================

namespace() ->
    durable_storage.

schema() ->
    [
        {messages,
            ds_schema(#{
                default =>
                    #{
                        <<"backend">> => builtin
                    },
                importance => ?IMPORTANCE_MEDIUM,
                desc => ?DESC(messages)
            })}
    ].

fields(builtin) ->
    %% Schema for the builtin backend:
    [
        {backend,
            sc(
                builtin,
                #{
                    'readOnly' => true,
                    default => builtin,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_backend)
                }
            )},
        {'_config_handler',
            sc(
                {module(), atom()},
                #{
                    'readOnly' => true,
                    default => {?MODULE, translate_builtin},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {data_dir,
            sc(
                string(),
                #{
                    mapping => "emqx_durable_storage.db_data_dir",
                    required => false,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_data_dir)
                }
            )},
        {n_shards,
            sc(
                pos_integer(),
                #{
                    default => 16,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_n_shards)
                }
            )},
        {replication_factor,
            sc(
                pos_integer(),
                #{
                    default => 3,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {local_write_buffer,
            sc(
                ref(builtin_local_write_buffer),
                #{
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(builtin_local_write_buffer)
                }
            )},
        {layout,
            sc(
                hoconsc:union(builtin_layouts()),
                #{
                    desc => ?DESC(builtin_layout),
                    importance => ?IMPORTANCE_MEDIUM,
                    default =>
                        #{
                            <<"type">> => wildcard_optimized
                        }
                }
            )},
        {cache,
            sc(
                ref(cache),
                #{
                    desc => ?DESC(builtin_cache),
                    importance => ?IMPORTANCE_HIDDEN,
                    default => #{<<"enable">> => false}
                }
            )}
    ];
fields(builtin_local_write_buffer) ->
    [
        {max_items,
            sc(
                pos_integer(),
                #{
                    default => 1000,
                    mapping => "emqx_durable_storage.egress_batch_size",
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(builtin_local_write_buffer_max_items)
                }
            )},
        {flush_interval,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => 100,
                    mapping => "emqx_durable_storage.egress_flush_interval",
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(builtin_local_write_buffer_flush_interval)
                }
            )}
    ];
fields(layout_builtin_wildcard_optimized) ->
    [
        {type,
            sc(
                wildcard_optimized,
                #{
                    'readOnly' => true,
                    default => wildcard_optimized,
                    desc => ?DESC(layout_builtin_wildcard_optimized_type)
                }
            )},
        {bits_per_topic_level,
            sc(
                range(1, 64),
                #{
                    default => 64,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {epoch_bits,
            sc(
                range(0, 64),
                #{
                    default => 10,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(wildcard_optimized_epoch_bits)
                }
            )},
        {topic_index_bytes,
            sc(
                pos_integer(),
                #{
                    default => 4,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
fields(layout_builtin_reference) ->
    [
        {type,
            sc(
                reference,
                #{
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
fields(cache) ->
    [
        {enable,
            sc(
                boolean(),
                #{
                    default => false,
                    importance => ?IMPORTANCE_HIDDEN,
                    mapping => "emqx_durable_storage.cache_enabled",
                    'readOnly' => true,
                    desc => ?DESC(builtin_cache_enable)
                }
            )},
        {cache_prefetch_topic_filters,
            sc(
                hoconsc:array(emqx_ds:topic_filter()),
                #{
                    default => [],
                    %% This config is temporary: caching should be automatic and inferred
                    %% from usage.
                    %% TODO: while this is not automatic, relying on the application
                    %% schema instead of config handler callbacks implies that we'll need
                    %% to manually diff tracked topic filters and update the cache process
                    %% state.
                    importance => ?IMPORTANCE_HIDDEN,
                    mapping => "emqx_durable_storage.cache_prefetch_topic_filters",
                    desc => ?DESC(builtin_cache_prefetch_topic_filters)
                }
            )}
    ].

desc(builtin) ->
    ?DESC(builtin);
desc(builtin_local_write_buffer) ->
    ?DESC(builtin_local_write_buffer);
desc(layout_builtin_wildcard_optimized) ->
    ?DESC(layout_builtin_wildcard_optimized);
desc(_) ->
    undefined.

%%================================================================================
%% Internal functions
%%================================================================================

ds_schema(Options) ->
    sc(
        hoconsc:union([
            ref(builtin)
            | emqx_schema_hooks:injection_point('durable_storage.backends', [])
        ]),
        Options
    ).

-ifndef(TEST).
builtin_layouts() ->
    [ref(layout_builtin_wildcard_optimized)].
-else.
builtin_layouts() ->
    %% Reference layout stores everything in one stream, so it's not
    %% suitable for production use. However, it's very simple and
    %% produces a very predictabale replay order, which can be useful
    %% for testing and debugging:
    [ref(layout_builtin_wildcard_optimized), ref(layout_builtin_reference)].
-endif.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

ref(StructName) -> hoconsc:ref(?MODULE, StructName).
