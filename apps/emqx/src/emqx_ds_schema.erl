%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Schema for EMQX_DS databases.
-module(emqx_ds_schema).

%% API:
-export([schema/0, db_schema/1, db_schema/2]).
-export([db_config/1, session_config/0]).

%% Internal exports:
-export([translate_builtin_raft/1, translate_builtin_local/1]).

%% Behavior callbacks:
-export([fields/1, desc/1, namespace/0]).

-include("emqx_schema.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("hocon/include/hocon_types.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-ifndef(EMQX_RELEASE_EDITION).
-define(EMQX_RELEASE_EDITION, ce).
-endif.

-if(?EMQX_RELEASE_EDITION == ee).
-define(DEFAULT_BACKEND, builtin_raft).
-define(BUILTIN_BACKENDS, [builtin_raft, builtin_local]).
-else.
-define(DEFAULT_BACKEND, builtin_local).
-define(BUILTIN_BACKENDS, [builtin_local]).
-endif.

%%================================================================================
%% API
%%================================================================================

-spec db_config(emqx_config:runtime_config_key_path()) -> emqx_ds:create_db_opts().
db_config(Path) ->
    ConfigTree = #{backend := Backend} = emqx_config:get(Path),
    case Backend of
        builtin_local ->
            translate_builtin_local(ConfigTree);
        builtin_raft ->
            translate_builtin_raft(ConfigTree)
    end.

-spec session_config() -> emqx_ds:create_db_opts().
session_config() ->
    %% FIXME:
    #{
        n_shards => 8,
        n_sites => 1,
        replication_options => #{},
        replication_factor => 1
    }.

translate_builtin_raft(
    Backend = #{
        backend := builtin_raft,
        n_shards := NShards,
        n_sites := NSites,
        replication_factor := ReplFactor,
        transaction := Transaction
    }
) ->
    %% NOTE: Undefined if `basic` schema is in use.
    Layout = maps:get(layout, Backend, undefined),
    #{
        backend => builtin_raft,
        n_shards => NShards,
        n_sites => NSites,
        replication_factor => ReplFactor,
        replication_options => maps:get(replication_options, Backend, #{}),
        storage => emqx_maybe:apply(fun translate_layout/1, Layout),
        transaction => Transaction
    }.

translate_builtin_local(
    Backend = #{
        backend := builtin_local,
        n_shards := NShards,
        transaction := Transaction
    }
) ->
    %% NOTE: Undefined if `basic` schema is in use.
    Layout = maps:get(layout, Backend, undefined),
    NPollers = maps:get(poll_workers_per_shard, Backend, undefined),
    BatchSize = maps:get(poll_batch_size, Backend, undefined),
    #{
        backend => builtin_local,
        n_shards => NShards,
        storage => emqx_maybe:apply(fun translate_layout/1, Layout),
        poll_workers_per_shard => NPollers,
        poll_batch_size => BatchSize,
        transaction => Transaction
    }.

%%================================================================================
%% Behavior callbacks
%%================================================================================

namespace() ->
    durable_storage.

schema() ->
    [
        {messages,
            db_schema(#{
                importance => ?IMPORTANCE_MEDIUM,
                desc => ?DESC(messages)
            })}
    ] ++ emqx_schema_hooks:list_injection_point('durable_storage', []).

db_schema(ExtraOptions) ->
    db_schema(complete, ExtraOptions).

db_schema(Flavor, ExtraOptions) ->
    Options = #{
        default => #{<<"backend">> => ?DEFAULT_BACKEND}
    },
    BuiltinBackends = [backend_ref(Backend, Flavor) || Backend <- ?BUILTIN_BACKENDS],
    CustomBackends = emqx_schema_hooks:list_injection_point('durable_storage.backends', []),
    sc(
        hoconsc:union(BuiltinBackends ++ CustomBackends),
        maps:merge(Options, ExtraOptions)
    ).

-dialyzer({nowarn_function, backend_ref/2}).
backend_ref(Backend, complete) ->
    ref(Backend);
backend_ref(builtin_local, basic) ->
    ref(builtin_local_basic);
backend_ref(builtin_raft, basic) ->
    ref(builtin_raft_basic).

backend_fields(builtin_local, Flavor) ->
    %% Schema for the builtin_raft backend:
    [
        {backend,
            sc(
                builtin_local,
                #{
                    'readOnly' => true,
                    default => builtin_local,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(backend_type)
                }
            )}
        | common_builtin_fields(Flavor)
    ];
backend_fields(builtin_raft, Flavor) ->
    %% Schema for the builtin_raft backend:
    [
        {backend,
            sc(
                builtin_raft,
                #{
                    'readOnly' => true,
                    default => builtin_raft,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(backend_type)
                }
            )},
        {replication_factor,
            sc(
                pos_integer(),
                #{
                    default => 3,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_raft_replication_factor)
                }
            )},
        {n_sites,
            sc(
                pos_integer(),
                #{
                    default => 1,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(builtin_raft_n_sites)
                }
            )},
        %% TODO: Elaborate.
        {replication_options,
            sc(
                hoconsc:map(name, any()),
                #{
                    default => #{},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
        | common_builtin_fields(Flavor)
    ].

fields(builtin_local) ->
    backend_fields(builtin_local, complete);
fields(builtin_raft) ->
    backend_fields(builtin_raft, complete);
fields(builtin_local_basic) ->
    backend_fields(builtin_local, basic);
fields(builtin_raft_basic) ->
    backend_fields(builtin_raft, basic);
fields(builtin_write_buffer) ->
    [
        {max_items,
            sc(
                emqx_ds_buffer:size_limit(),
                #{
                    default => 1000,
                    mapping => "emqx_durable_storage.egress_batch_size",
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_write_buffer_max_items)
                }
            )},
        {max_bytes,
            sc(
                emqx_ds_buffer:size_limit(),
                #{
                    default => infinity,
                    mapping => "emqx_durable_storage.egress_batch_bytes",
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_write_buffer_max_items)
                }
            )},
        {flush_interval,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => 100,
                    mapping => "emqx_durable_storage.egress_flush_interval",
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(builtin_write_buffer_flush_interval)
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
                    default => 20,
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
fields(layout_builtin_wildcard_optimized_v2) ->
    [
        {type,
            sc(
                wildcard_optimized_v2,
                #{
                    'readOnly' => true,
                    default => wildcard_optimized_v2,
                    desc => ?DESC(layout_builtin_wildcard_optimized_type)
                }
            )},
        {bytes_per_topic_level,
            sc(
                range(1, 16),
                #{
                    default => 8,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {topic_index_bytes,
            sc(
                pos_integer(),
                #{
                    default => 8,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {serialization_schema,
            sc(
                emqx_ds_msg_serializer:schema(),
                #{
                    default => v1,
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
                    importance => ?IMPORTANCE_LOW,
                    default => reference,
                    desc => ?DESC(layout_builtin_reference_type)
                }
            )}
    ];
fields(optimistic_transaction) ->
    [
        {flush_interval,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => "5s",
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(otx_flush_interval)
                }
            )},
        {idle_flush_interval,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => "1ms",
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(otx_idle_flush_interval)
                }
            )},
        {conflict_window,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => "5s",
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(otx_conflict_window)
                }
            )}
    ].

common_builtin_fields(basic) ->
    [
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
        {local_write_buffer,
            sc(
                ref(builtin_write_buffer),
                #{
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(builtin_write_buffer)
                }
            )},
        {transaction,
            sc(
                ref(optimistic_transaction),
                #{
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(builtin_optimistic_transaction)
                }
            )}
    ];
common_builtin_fields(layout) ->
    [
        {layout,
            sc(
                hoconsc:union(builtin_layouts()),
                #{
                    desc => ?DESC(builtin_layout),
                    importance => ?IMPORTANCE_MEDIUM,
                    default =>
                        #{
                            <<"type">> => wildcard_optimized_v2
                        }
                }
            )}
    ];
common_builtin_fields(polling) ->
    [
        {poll_workers_per_shard,
            sc(
                pos_integer(),
                #{
                    default => 10,
                    importance => ?IMPORTANCE_HIDDEN,
                    mapping => "emqx_durable_storage.beamformer_workers_per_shard"
                }
            )},
        {poll_batch_size,
            sc(
                pos_integer(),
                #{
                    default => 100,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
common_builtin_fields(complete) ->
    common_builtin_fields(basic) ++
        common_builtin_fields(layout) ++
        common_builtin_fields(polling).

desc(builtin_raft) ->
    ?DESC(builtin_raft);
desc(builtin_local) ->
    ?DESC(builtin_local);
desc(builtin_write_buffer) ->
    ?DESC(builtin_write_buffer);
desc(layout_builtin_wildcard_optimized) ->
    ?DESC(layout_builtin_wildcard_optimized);
desc(layout_builtin_wildcard_optimized_v2) ->
    ?DESC(layout_builtin_wildcard_optimized);
desc(layout_builtin_reference) ->
    ?DESC(layout_builtin_reference);
desc(optimistic_transaction) ->
    ?DESC(optimistic_transaction);
desc(_) ->
    undefined.

%%================================================================================
%% Internal functions
%%================================================================================

translate_layout(
    #{
        type := wildcard_optimized_v2,
        bytes_per_topic_level := BytesPerTopicLevel,
        topic_index_bytes := TopicIndexBytes,
        serialization_schema := SSchema
    }
) ->
    {emqx_ds_storage_skipstream_lts, #{
        wildcard_hash_bytes => BytesPerTopicLevel,
        topic_index_bytes => TopicIndexBytes,
        serialization_schema => SSchema
    }};
translate_layout(
    #{
        type := wildcard_optimized,
        bits_per_topic_level := BitsPerTopicLevel,
        epoch_bits := EpochBits,
        topic_index_bytes := TIBytes
    }
) ->
    {emqx_ds_storage_bitfield_lts, #{
        bits_per_topic_level => BitsPerTopicLevel,
        topic_index_bytes => TIBytes,
        epoch_bits => EpochBits
    }};
translate_layout(#{type := reference}) ->
    {emqx_ds_storage_reference, #{}}.

builtin_layouts() ->
    %% Reference layout stores everything in one stream, so it's not
    %% suitable for production use. However, it's very simple and
    %% produces a very predictabale replay order, which can be useful
    %% for testing and debugging:
    [
        ref(layout_builtin_wildcard_optimized_v2),
        ref(layout_builtin_wildcard_optimized),
        ref(layout_builtin_reference)
    ].

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

ref(StructName) -> hoconsc:ref(?MODULE, StructName).
