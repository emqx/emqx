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

-type backend() ::
    builtin_raft
    | builtin_local
    | builtin_raft_messages
    | builtin_local_messages.

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
    db_config([durable_storage, shared_subs]).

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
                ?IMPORTANCE_MEDIUM,
                ?DESC(messages),
                #{}
            )},
        {sessions,
            db_schema(
                [builtin_raft, builtin_local],
                ?IMPORTANCE_MEDIUM,
                ?DESC(sessions),
                #{}
            )},
        {timers,
            db_schema(
                [builtin_raft, builtin_local],
                ?IMPORTANCE_MEDIUM,
                ?DESC(timers),
                %% Latency for this DB should be low:
                #{
                    <<"transaction">> => #{
                        <<"idle_flush_interval">> => <<"1ms">>,
                        <<"flush_interval">> => <<"10ms">>
                    }
                }
            )},
        {shared_subs,
            db_schema(
                [builtin_raft, builtin_local],
                ?IMPORTANCE_MEDIUM,
                ?DESC(shared_subs),
                #{}
            )}
    ] ++ emqx_schema_hooks:list_injection_point('durable_storage', []).

-spec db_schema([backend()], _Importance, ?DESC(_), Defaults) ->
    #{type := _, _ => _}
when
    Defaults :: map().
db_schema(Backends, Importance, Desc, Defaults) ->
    sc(
        hoconsc:union([ref(I) || I <- Backends]),
        #{
            default => maps:merge(#{<<"backend">> => ?DEFAULT_BACKEND}, Defaults),
            importance => Importance,
            desc => Desc
        }
    ).

fields(builtin_local_messages) ->
    make_local(messages);
fields(builtin_raft_messages) ->
    make_raft(messages);
fields(builtin_local) ->
    make_local(generic);
fields(builtin_raft) ->
    make_raft(generic);
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
fields(layout_builtin_wildcard_optimized) ->
    %% Settings for `emqx_ds_storage_skipstream_lts_v2':
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
        {wildcard_thresholds,
            sc(
                hoconsc:array(hoconsc:union([non_neg_integer(), infinity])),
                #{
                    default => [100, 10],
                    validator => fun validate_wildcard_thresholds/1,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(lts_wildcard_thresholds)
                }
            )},
        {timestamp_bytes,
            sc(
                pos_integer(),
                #{
                    default => 8,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
fields(optimistic_transaction) ->
    %% `emqx_ds_optimistic_tx' settings:
    [
        {conflict_window,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => "5s",
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(otx_conflict_window)
                }
            )},
        {flush_interval,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => "10ms",
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(otx_flush_interval)
                }
            )},
        {idle_flush_interval,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => "1ms",
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(otx_idle_flush_interval)
                }
            )},
        {max_pending,
            sc(
                pos_integer(),
                #{
                    default => 10000,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(otx_max_pending)
                }
            )}
    ];
fields(subscriptions) ->
    %% Beamformer settings:
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

-spec make_local(generic | messages | queues) -> hocon_schema:fields().
make_local(Flavor) ->
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
    ].

-spec make_raft(generic | messages | queues) -> hocon_schema:fields().
make_raft(Flavor) ->
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
        | common_builtin_fields(Flavor)
    ].

%% This function returns fields common for builtin_local and builtin_raft backends.
common_builtin_fields(Flavor) ->
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
            )},
        {transaction,
            sc(
                ref(optimistic_transaction),
                #{
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(builtin_optimistic_transaction)
                }
            )}
        | case Flavor of
            generic ->
                %% Generic DBs use preconfigured storage layout
                [];
            messages ->
                %% `messages' DB lets user customize storage layout:
                [
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
                        )}
                ]
        end
    ].

desc(Backend) when
    Backend =:= builtin_raft; Backend =:= builtin_raft_messages
->
    ?DESC(builtin_raft);
desc(Backend) when
    Backend =:= builtin_local; Backend =:= builtin_local_messages
->
    ?DESC(builtin_local);
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
                Cfg1#{transaction => translate_otx_opts(Transaction)};
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

translate_otx_opts(#{
    conflict_window := CW,
    flush_interval := FI,
    idle_flush_interval := IFI,
    max_pending := MaxItems
}) ->
    #{
        conflict_window => CW,
        flush_interval => FI,
        idle_flush_interval => IFI,
        max_items => MaxItems
    }.

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
        type := wildcard_optimized,
        bytes_per_topic_level := BytesPerTopicLevel,
        topic_index_bytes := TopicIndexBytes,
        wildcard_thresholds := WildcardThresholds,
        timestamp_bytes := TSBytes
    }
) ->
    {emqx_ds_storage_skipstream_lts_v2, #{
        wildcard_hash_bytes => BytesPerTopicLevel,
        topic_index_bytes => TopicIndexBytes,
        lts_threshold_spec => translate_lts_wildcard_thresholds(WildcardThresholds),
        timestamp_bytes => TSBytes
    }}.

builtin_layouts() ->
    [
        ref(layout_builtin_wildcard_optimized)
    ].

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

ref(StructName) -> hoconsc:ref(?MODULE, StructName).

validate_wildcard_thresholds([]) ->
    {error, "List should not be empty."};
validate_wildcard_thresholds([_ | _]) ->
    ok.

translate_lts_wildcard_thresholds(L = [_ | _]) ->
    {simple, list_to_tuple(L)}.
