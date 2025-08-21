%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Schema for EMQX_DS databases.
-module(emqx_ds_schema).

%% API:
-export([schema/0]).
-export([db_config_messages/0, db_config_sessions/0, db_config_timers/0, db_config_shared_subs/0]).

%% Behavior callbacks:
-export([fields/1, desc/1, namespace/0]).

-include("emqx_schema.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("hocon/include/hocon_types.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(DEFAULT_BACKEND, builtin_raft).

%%================================================================================
%% API
%%================================================================================

-spec db_config_messages() -> emqx_ds:create_db_opts().
db_config_messages() ->
    db_config([durable_storage, messages]).

db_config_sessions() ->
    db_config([durable_storage, sessions]).

db_config_timers() ->
    db_config([durable_storage, timers]).

db_config_shared_subs() ->
    db_config([durable_storage, queues]).

%%================================================================================
%% Behavior callbacks
%%================================================================================

namespace() ->
    durable_storage.

schema() ->
    [
        {n_sites,
            sc(
                pos_integer(),
                #{
                    default => 1,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_raft_n_sites),
                    mapping => "emqx_ds_builtin_raft.n_sites"
                }
            )},
        {messages,
            db_schema(
                [builtin_raft_messages, builtin_local_messages],
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(messages)
                }
            )},
        {sessions,
            db_schema(
                [builtin_raft_ttv, builtin_local_ttv],
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(sessions)
                }
            )},
        {timers,
            db_schema(
                [builtin_raft_ttv, builtin_local_ttv],
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(timers)
                }
            )},
        %% TODO: switch shared subs to use TTV and rename the DB to shared_sub
        {queues,
            db_schema(
                [builtin_raft_messages, builtin_local_messages],
                #{
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(shared_subs)
                }
            )}
    ] ++ emqx_schema_hooks:list_injection_point('durable_storage', []).

db_schema(Backends, ExtraOptions) ->
    Options = #{
        default => #{<<"backend">> => ?DEFAULT_BACKEND}
    },
    sc(
        hoconsc:union([ref(I) || I <- Backends]),
        maps:merge(Options, ExtraOptions)
    ).

fields(builtin_local_messages) ->
    make_local(false);
fields(builtin_raft_messages) ->
    make_raft(false);
fields(builtin_local_ttv) ->
    make_local(true);
fields(builtin_raft_ttv) ->
    make_raft(true);
fields(rocksdb_options) ->
    [
        {cache_size,
            sc(
                emqx_schema:bytesize(),
                #{
                    default => <<"8MB">>,
                    desc => ?DESC(rocksdb_cache_size)
                }
            )},
        {write_buffer_size,
            sc(
                emqx_schema:bytesize(),
                #{
                    default => <<"10MB">>,
                    desc => ?DESC(rocksdb_write_buffer_size)
                }
            )},
        {max_open_files,
            sc(
                hoconsc:union([pos_integer(), infinity]),
                #{
                    default => infinity,
                    desc => ?DESC(rocksdb_max_open_files)
                }
            )}
    ];
fields(builtin_write_buffer) ->
    %% TODO: this setting becomes obsolete after all DBs are switch to
    %% TTV style
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
                    default => asn1,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {wildcard_thresholds,
            sc(
                hoconsc:array(hoconsc:union([non_neg_integer(), infinity])),
                #{
                    default => [100, 10],
                    validator => fun validate_wildcard_thresholds/1,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(lts_wildcard_thresholds)
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
                    default => "10ms",
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
    ];
fields(subscriptions) ->
    [
        {batch_size,
            sc(
                pos_integer(),
                #{
                    default => 1000,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {n_workers_per_shard,
            sc(
                pos_integer(),
                #{
                    default => 10,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {housekeeping_interval,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => <<"1s">>,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ].

make_local(StoreTTV) ->
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
        | common_builtin_fields(StoreTTV)
    ].

make_raft(StoreTTV) ->
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
        %% TODO: Elaborate.
        {replication_options,
            sc(
                hoconsc:map(name, any()),
                #{
                    default => #{},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
        | common_builtin_fields(StoreTTV)
    ].

%% This function returns fields common for builtin_local and builtin_raft backends.
common_builtin_fields(StoreTTV) ->
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
        {rocksdb,
            sc(
                ref(rocksdb_options),
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_rocksdb_options)
                }
            )},
        {subscriptions,
            sc(
                ref(subscriptions),
                #{
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
        | case StoreTTV of
            true ->
                [
                    {transaction,
                        sc(
                            ref(optimistic_transaction),
                            #{
                                importance => ?IMPORTANCE_LOW,
                                desc => ?DESC(builtin_optimistic_transaction)
                            }
                        )}
                ];
            false ->
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
                ]
        end
    ].

desc(builtin_raft_ttv) ->
    ?DESC(builtin_raft);
desc(builtin_raft_messages) ->
    ?DESC(builtin_raft);
desc(builtin_local_ttv) ->
    ?DESC(builtin_local);
desc(builtin_local_messages) ->
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
desc(rocksdb_options) ->
    ?DESC(rocksdb_options);
desc(_) ->
    undefined.

%%================================================================================
%% Internal functions
%%================================================================================

db_config(SchemaKey) ->
    translate_backend(emqx_config:get(SchemaKey)).

translate_backend(
    #{
        backend := Backend,
        n_shards := NShards,
        rocksdb := RocksDBOptions,
        subscriptions := Subscriptions
    } = Input
) when Backend =:= builtin_local; Backend =:= builtin_raft ->
    Cfg1 = #{
        backend => Backend,
        n_shards => NShards,
        rocksdb => translate_rocksdb_options(RocksDBOptions),
        subscriptions => Subscriptions
    },
    Cfg2 =
        case Input of
            #{transaction := Transaction} ->
                Cfg1#{transaction => Transaction};
            #{} ->
                Cfg1
        end,
    Cfg =
        case Input of
            #{layout := Layout} ->
                Cfg2#{storage => translate_layout(Layout)};
            #{} ->
                Cfg2
        end,
    case Backend of
        builtin_raft ->
            #{replication_factor := ReplFactor, replication_options := ReplOptions} = Input,
            Cfg#{
                replication_factor => ReplFactor,
                replication_options => ReplOptions
            };
        builtin_local ->
            Cfg
    end.

translate_rocksdb_options(Input = #{max_open_files := MOF}) ->
    Input#{
        max_open_files :=
            case MOF of
                infinity -> -1;
                _ -> MOF
            end
    }.

translate_layout(
    #{
        type := wildcard_optimized_v2,
        bytes_per_topic_level := BytesPerTopicLevel,
        topic_index_bytes := TopicIndexBytes,
        serialization_schema := SSchema,
        wildcard_thresholds := WildcardThresholds
    }
) ->
    {emqx_ds_storage_skipstream_lts, #{
        wildcard_hash_bytes => BytesPerTopicLevel,
        topic_index_bytes => TopicIndexBytes,
        serialization_schema => SSchema,
        lts_threshold_spec => translate_lts_wildcard_thresholds(WildcardThresholds)
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

validate_wildcard_thresholds([]) ->
    {error, "List should not be empty."};
validate_wildcard_thresholds([_ | _]) ->
    ok.

translate_lts_wildcard_thresholds(L = [_ | _]) ->
    {simple, list_to_tuple(L)}.
