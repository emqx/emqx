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
                desc => ?DESC(messages),
                importance => ?IMPORTANCE_HIDDEN,
                default =>
                    #{
                        <<"backend">> => builtin
                    }
            })}
    ].

fields(builtin) ->
    %% Schema for the builtin backend:
    [
        {backend,
            sc(
                builtin,
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    'readOnly' => true,
                    default => builtin,
                    desc => ?DESC(builtin)
                }
            )},
        {'_config_handler',
            sc(
                {module(), atom()},
                #{
                    importance => ?IMPORTANCE_HIDDEN,
                    'readOnly' => true,
                    default => {?MODULE, translate_builtin}
                }
            )},
        {data_dir,
            sc(
                string(),
                #{
                    desc => ?DESC(builtin_data_dir),
                    mapping => "emqx_durable_storage.db_data_dir",
                    required => false,
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {n_shards,
            sc(
                pos_integer(),
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_n_shards),
                    default => 16
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
        {egress,
            sc(
                ref(builtin_egress),
                #{
                    desc => ?DESC(builtin_egress),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {layout,
            sc(
                hoconsc:union([
                    ref(layout_builtin_wildcard_optimized), ref(layout_builtin_reference)
                ]),
                #{
                    desc => ?DESC(builtin_layout),
                    importance => ?IMPORTANCE_HIDDEN,
                    default =>
                        #{
                            <<"type">> => wildcard_optimized
                        }
                }
            )}
    ];
fields(builtin_egress) ->
    [
        {max_items,
            sc(
                pos_integer(),
                #{
                    default => 1000,
                    mapping => "emqx_durable_storage.egress_batch_size",
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {flush_interval,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => 100,
                    mapping => "emqx_durable_storage.egress_flush_interval",
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
fields(layout_builtin_wildcard_optimized) ->
    [
        {type,
            sc(
                wildcard_optimized,
                #{
                    desc => ?DESC(layout_wildcard_optimized),
                    'readOnly' => true,
                    default => wildcard_optimized
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
                    importance => ?IMPORTANCE_MEDIUM,
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
                #{'readOnly' => true}
            )}
    ].

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

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

ref(StructName) -> hoconsc:ref(?MODULE, StructName).
