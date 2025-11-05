%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_schema).
-moduledoc """
Schema for EMQX_DS databases.
""".

-behaviour(emqx_config_handler).

%% API:
-export([add_handler/0]).
-export([
    db_config_messages/0,
    db_config_sessions/0,
    db_config_timers/0,
    db_config_shared_subs/0,
    db_config_mq_states/0,
    db_config_mq_messages/0,
    db_config_streams_messages/0,
    db_config_streams_states/0,

    setup_db_group/2,
    db_group_config/0
]).

%% Behavior callbacks:
-export([schema/0, fields/1, desc/1, namespace/0]).
-export([pre_config_update/3, post_config_update/6]).

%% Internal exports:
-export([to_size_limit/1, validate_config/1]).

-include("logger.hrl").
-include_lib("typerefl/include/types.hrl").
-include("emqx_schema.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("hocon/include/hocon_types.hrl").
-include_lib("emqx_utils/include/emqx_ds_dbs.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-define(DEFAULT_BACKEND, builtin_raft).

-type backend() :: builtin_raft | builtin_local.
-reflect_type([backend/0]).

-type db_backend_flavor() ::
    builtin_raft
    | builtin_local
    | builtin_raft_messages
    | builtin_local_messages.

-type size_limit() :: pos_integer() | infinity.

-reflect_type([size_limit/0]).
-typerefl_from_string({size_limit/0, ?MODULE, to_size_limit}).

%%================================================================================
%% API
%%================================================================================

add_handler() ->
    A = '?',
    [
        ok = emqx_config_handler:add_handler([namespace() | L], ?MODULE)
     || L <- [[A], [A, A], [A, A, A], [A, A, A, A]]
    ],
    [
        ok = emqx_config_handler:add_handler([durable_storage_groups | L], ?MODULE)
     || L <- [[A], [A, A]]
    ],
    ok.

-spec db_config_messages() -> emqx_ds:create_db_opts().
db_config_messages() ->
    db_config(?PERSISTENT_MESSAGE_DB).

db_config_sessions() ->
    db_config(?DURABLE_SESSION_STATE_DB).

db_config_timers() ->
    db_config(?DURABLE_TIMERS_DB).

db_config_shared_subs() ->
    db_config(?SHARED_SUBS_DB).

db_config_mq_states() ->
    db_config(?MQ_STATE_CONF_ROOT).

db_config_mq_messages() ->
    db_config(?MQ_MESSAGE_CONF_ROOT).

db_config_streams_messages() ->
    db_config(?STREAMS_MESSAGE_CONF_ROOT).

db_config_streams_states() ->
    db_config(?STREAMS_STATE_CONF_ROOT).

setup_db_group(Group, Config) ->
    case emqx_ds:setup_db_group(Group, Config) of
        ok ->
            ok;
        Err ->
            ?SLOG(error, #{
                msg => "invalid_db_group_config", group => Group, config => Config, reason => Err
            })
    end.

db_group_config() ->
    Raw = emqx_config:get([durable_storage, db_groups], #{}),
    maps:map(
        fun(_Key, Conf) ->
            translate_db_group(Conf)
        end,
        Raw
    ).

%% This callback validates global relationships between different
%% entities:
validate_config(Conf) ->
    DBs = maps:keys(Conf) -- [<<"n_sites">>, <<"db_groups">>],
    %% Verify that all DB groups explicitly assigned to the DBs exist
    %% and DB backends match with the group backends:
    try
        lists:foreach(
            fun(DB) ->
                {ok, Backend} = emqx_utils_maps:deep_find([DB, <<"backend">>], Conf),
                %% Find out name of the group associated with the DB
                %% and whether presence of the group is required:
                case emqx_utils_maps:deep_find([DB, <<"db_group">>], Conf) of
                    {not_found, _, _} ->
                        %% DB group is not specified. Group will be created explicitly:
                        Group = DB,
                        Required = false;
                    {ok, GroupAtom} ->
                        Group = atom_to_binary(GroupAtom),
                        Required = true
                end,
                case
                    emqx_utils_maps:deep_find(
                        [<<"db_groups">>, Group, <<"backend">>], Conf
                    )
                of
                    {ok, Backend} ->
                        ok;
                    {not_found, _, _} when not Required ->
                        ok;
                    {ok, Other} ->
                        throw(#{
                            reason => "backend of group and DB must be the same",
                            db => DB,
                            group => Group,
                            group_backend => Other,
                            db_backend => Backend
                        });
                    {not_found, _, _} ->
                        throw(#{reason => "unknown db_group", db => DB, group => Group})
                end
            end,
            DBs
        ),
        ok
    catch
        Err ->
            {error, Err}
    end.

%%================================================================================
%% HOCON schema callbacks
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
        {db_groups,
            sc(
                hoconsc:map(name, ref(db_group)),
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(db_groups_root)
                }
            )},
        {?PERSISTENT_MESSAGE_DB,
            db_schema(
                [builtin_raft_messages, builtin_local_messages],
                ?IMPORTANCE_MEDIUM,
                ?DESC(messages),
                #{}
            )},
        {?DURABLE_SESSION_STATE_DB,
            db_schema(
                [builtin_raft, builtin_local],
                ?IMPORTANCE_MEDIUM,
                ?DESC(sessions),
                #{
                    <<"transaction">> => #{
                        <<"idle_flush_interval">> => <<"0ms">>
                    },
                    <<"subscriptions">> => #{
                        <<"n_workers_per_shard">> => 0
                    }
                }
            )},
        {?DURABLE_TIMERS_DB,
            db_schema(
                [builtin_raft, builtin_local],
                ?IMPORTANCE_MEDIUM,
                ?DESC(timers),
                %% Latency for this DB should be low. Currently timers
                %% are mostly used for events that have 1s resolution
                %% (session expiration, will delay), so anything under
                %% this value should be ok.
                #{
                    <<"transaction">> => #{
                        <<"idle_flush_interval">> => <<"0ms">>
                    },
                    <<"subscriptions">> => #{
                        <<"n_workers_per_shard">> => 0
                    }
                }
            )},
        {?SHARED_SUBS_DB,
            db_schema(
                [builtin_raft, builtin_local],
                ?IMPORTANCE_MEDIUM,
                ?DESC(shared_subs),
                #{
                    <<"subscriptions">> => #{
                        <<"n_workers_per_shard">> => 0
                    }
                }
            )},
        {?MQ_STATE_CONF_ROOT,
            db_schema(
                [builtin_raft, builtin_local],
                ?IMPORTANCE_MEDIUM,
                ?DESC(mq_states),
                #{}
            )},
        {?MQ_MESSAGE_CONF_ROOT,
            db_schema(
                [builtin_raft, builtin_local],
                ?IMPORTANCE_MEDIUM,
                ?DESC(mq_messages),
                #{}
            )},
        {?STREAMS_MESSAGE_CONF_ROOT,
            db_schema(
                [builtin_raft, builtin_local],
                ?IMPORTANCE_MEDIUM,
                ?DESC(streams_messages),
                #{}
            )},
        {?STREAMS_STATE_CONF_ROOT,
            db_schema(
                [builtin_raft, builtin_local],
                ?IMPORTANCE_MEDIUM,
                ?DESC(streams_states),
                #{}
            )}
    ].

-spec db_schema([db_backend_flavor()], _Importance, ?DESC(_), Defaults) ->
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
                    deprecated => {since, "6.0.1"},
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
                non_neg_integer(),
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
    ];
fields(db_group) ->
    %% TODO: currently group settings for both raft and local backends
    %% are the same. Should they diverge, can we have a union here?
    [
        {name,
            sc(
                atom(),
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(db_group_name)
                }
            )},
        {backend,
            sc(
                backend(),
                #{
                    default => ?DEFAULT_BACKEND,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(db_group_backend)
                }
            )},
        {storage_quota,
            sc(
                size_limit(),
                #{
                    default => infinity,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(db_group_storage_quota)
                }
            )},
        {write_buffer_size,
            sc(
                size_limit(),
                #{
                    default => "256 MiB",
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(db_group_write_buffer_size)
                }
            )},
        {rocksdb_nthreads_high,
            sc(
                pos_integer(),
                #{
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(db_group_rocksdb_threads)
                }
            )},
        {rocksdb_nthreads_low,
            sc(
                pos_integer(),
                #{
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(db_group_rocksdb_threads)
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
            )},
        {max_retries,
            sc(
                non_neg_integer(),
                #{
                    default => 10,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_raft_max_retries)
                }
            )},
        {retry_interval,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => <<"10s">>,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(builtin_raft_retry_interval)
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
        {db_group,
            sc(
                atom(),
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(db_group)
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
desc(db_group) ->
    ?DESC(db_group_record);
desc(_) ->
    undefined.

%%================================================================================
%% emqx_config_handler callbacks
%%================================================================================

-spec post_config_update(
    [atom()],
    emqx_config:update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs(),
    emqx_config_handler:extra_context()
) ->
    ok | {error, _Reason}.
post_config_update(
    [durable_storage, db_groups, Group | _], _UpdateReq, _NewConf, _OldConf, _AppEnv, _Extra
) ->
    setup_db_group(Group, translate_db_group(emqx_config:get([durable_storage, db_groups, Group])));
post_config_update([durable_storage, Config | _], _UpdateReq, _NewConf, _OldConf, _AppEnv, _Extra) ->
    lists:foreach(
        fun(DB) ->
            update_db_config(DB, db_config(Config))
        end,
        config_root_to_dbs(Config)
    );
post_config_update(_, _, _, _, _, _) ->
    ok.

-spec pre_config_update(
    [atom()], emqx_config:update_request(), emqx_config:raw_config()
) ->
    ok | {error, term()}.
pre_config_update(_Root, _UpdateReq, _Conf) ->
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

translate_db_group(Conf) ->
    Conf.

to_size_limit(In) when is_integer(In), In > 0 ->
    {ok, In};
to_size_limit(In) when In =:= infinity; In =:= "infinity"; In =:= <<"infinity">> ->
    {ok, infinity};
to_size_limit(In) when is_list(In); is_binary(In) ->
    RE = "^([0-9]+)(\\.[0-9]{1,3})? *([kKMGTPEZYRQ]i?B)?$",
    case re:run(In, RE, [{capture, all_but_first, list}]) of
        {match, [Nstr]} ->
            {ok, list_to_integer(Nstr)};
        {match, [Nstr, FractionalPart, Prefix]} ->
            case Prefix of
                [PowerPrefix | "iB"] ->
                    Base = 1024;
                [PowerPrefix | "B"] ->
                    Base = 1000
            end,
            %% 1. Convert float to a precise integer by multiplying it
            %% to Base (later we'll account for that):
            N =
                case FractionalPart of
                    [] ->
                        list_to_integer(Nstr) * Base;
                    _ ->
                        round(list_to_float(Nstr ++ FractionalPart) * Base)
                end,
            Power =
                case PowerPrefix of
                    $k -> 0;
                    $K -> 0;
                    $M -> 1;
                    $G -> 2;
                    $T -> 3;
                    $P -> 4;
                    $E -> 5;
                    $Z -> 6;
                    $Y -> 7;
                    $R -> 8;
                    $Q -> 9
                end,
            Val = N * pow(Base, Power),
            {ok, Val};
        _ ->
            {error, "Invalid quota"}
    end;
to_size_limit(_) ->
    {error, "Invalid quota"}.

db_config(ConfRoot) ->
    translate_backend(emqx_config:get([namespace(), ConfRoot])).

translate_backend(
    #{
        backend := Backend,
        n_shards := NShards,
        rocksdb := RocksDBOptions,
        subscriptions := Subscriptions,
        transaction := Transaction
        %% group := DBGroup
    } = Input
) when Backend =:= builtin_local; Backend =:= builtin_raft ->
    Cfg0 = maps:with([data_dir, db_group], Input),
    Cfg1 = Cfg0#{
        backend => Backend,
        n_shards => NShards,
        rocksdb => translate_rocksdb_options(RocksDBOptions),
        subscriptions => Subscriptions,
        transactions => translate_otx_opts(Transaction)
    },
    Cfg =
        case Input of
            #{layout := Layout} ->
                Cfg1#{storage => translate_layout(Layout)};
            #{} ->
                Cfg1
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

update_db_config(DB, Conf) ->
    ?tp(debug, "ds_db_runtime_config_update", #{db => DB}),
    case emqx_ds:update_db_config(DB, Conf) of
        ok ->
            ok;
        {error, recoverable, db_is_closed} ->
            ok;
        Err ->
            ?tp(warning, "ds_db_runtime_config_update_failed", #{db => DB, reason => Err})
    end.

config_root_to_dbs(DB) when
    DB =:= ?DURABLE_TIMERS_DB;
    DB =:= ?PERSISTENT_MESSAGE_DB;
    DB =:= ?DURABLE_SESSION_STATE_DB;
    DB =:= ?SHARED_SUBS_DB
->
    [DB];
config_root_to_dbs(?MQ_STATE_CONF_ROOT) ->
    [?MQ_STATE_DB];
config_root_to_dbs(?MQ_MESSAGE_CONF_ROOT) ->
    [?MQ_MESSAGE_LASTVALUE_DB, ?MQ_MESSAGE_REGULAR_DB];
config_root_to_dbs(?STREAMS_MESSAGE_CONF_ROOT) ->
    [?STREAMS_MESSAGE_LASTVALUE_DB, ?STREAMS_MESSAGE_REGULAR_DB];
config_root_to_dbs(_) ->
    [].

%% Simple algorithm to multiply bigints (note: math module works with
%% floats only). A more efficient implementation exists, but it's
%% likely an overkill for our goals.
pow(_, 0) ->
    1;
pow(N, Power) when Power > 0 ->
    N * pow(N, Power - 1).

-ifdef(TEST).

quota_to_integer_literal_test() ->
    %% Integers are returned verbatim:
    ?assertEqual(
        {ok, 1},
        to_size_limit(1)
    ),
    ?assertEqual(
        {ok, 2},
        to_size_limit("2")
    ),
    ?assertEqual(
        {ok, 100_000_000},
        to_size_limit("100000000")
    ).

quota_to_integer_infinity_test() ->
    ?assertEqual(
        {ok, infinity},
        to_size_limit(infinity)
    ),
    ?assertEqual(
        {ok, infinity},
        to_size_limit("infinity")
    ),
    ?assertEqual(
        {ok, infinity},
        to_size_limit(<<"infinity">>)
    ).

quota_to_integer_deciaml_test() ->
    ?assertEqual(
        {ok, 1000},
        to_size_limit("1kB")
    ),
    ?assertEqual(
        {ok, 10_000},
        to_size_limit(<<"10kB">>)
    ),
    ?assertEqual(
        {ok, 11_000_000},
        to_size_limit(<<"11MB">>)
    ),
    ?assertEqual(
        {ok, 11_000_000_000},
        to_size_limit(<<"11GB">>)
    ),
    ?assertEqual(
        {ok, 11_000_000_000_000},
        to_size_limit(<<"11TB">>)
    ),
    ?assertEqual(
        {ok, 11_000_000_000_000_000},
        to_size_limit(<<"11PB">>)
    ),
    ?assertEqual(
        {ok, 11_000_000_000_000_000_000},
        to_size_limit(<<"11EB">>)
    ),
    ?assertEqual(
        {ok, 11_000_000_000_000_000_000_000},
        to_size_limit(<<"11ZB">>)
    ),
    ?assertEqual(
        {ok, 11_000_000_000_000_000_000_000_000},
        to_size_limit(<<"11YB">>)
    ),
    ?assertEqual(
        {ok, 11_000_000_000_000_000_000_000_000_000},
        to_size_limit(<<"11RB">>)
    ),
    ?assertEqual(
        {ok, 11_000_000_000_000_000_000_000_000_000_000},
        to_size_limit(<<"11QB">>)
    ),
    ?assertEqual(
        {ok, 1234},
        to_size_limit("1.234 kB")
    ),
    ?assertEqual(
        {ok, 1_234_000},
        to_size_limit("1.234MB")
    ).

quota_to_integer_binary_test() ->
    ?assertEqual(
        {ok, 1024},
        to_size_limit("1KiB")
    ),
    ?assertEqual(
        {ok, 1024 + 512},
        to_size_limit("1.5KiB")
    ),
    ?assertEqual(
        {ok, pow(1024, 2)},
        to_size_limit("1MiB")
    ),
    ?assertEqual(
        {ok, pow(1024, 3)},
        to_size_limit("1GiB")
    ),
    ?assertEqual(
        {ok, pow(1024, 4)},
        to_size_limit("1TiB")
    ),
    ?assertEqual(
        {ok, pow(1024, 5)},
        to_size_limit("1  PiB")
    ),
    ?assertEqual(
        {ok, pow(1024, 6)},
        to_size_limit("1 EiB")
    ),
    ?assertEqual(
        {ok, pow(1024, 7) + pow(1024, 7) div 2},
        to_size_limit("1.5 ZiB")
    ),
    ?assertEqual(
        {ok, pow(1024, 8)},
        to_size_limit("1 YiB")
    ),
    ?assertEqual(
        {ok, pow(1024, 9)},
        to_size_limit("1 RiB")
    ),
    ?assertEqual(
        {ok, pow(1024, 10)},
        to_size_limit("1 QiB")
    ).

quota_to_integer_invalid_test() ->
    %% Literal number should be >= 1:
    ?assertMatch(
        {error, _},
        to_size_limit(0)
    ),
    ?assertMatch(
        {error, _},
        to_size_limit(-1)
    ),
    %% Wrong type:
    ?assertMatch(
        {error, _},
        to_size_limit(test)
    ),
    %% Wrong value:
    ?assertMatch(
        {error, _},
        to_size_limit(" ")
    ),
    %% Negative numbers:
    ?assertMatch(
        {error, _},
        to_size_limit("-1TB")
    ),
    ?assertMatch(
        {error, _},
        to_size_limit("-1.112TB")
    ),
    %% Not natural:
    ?assertMatch(
        {error, _},
        to_size_limit("1.1")
    ),
    %% Fractional part is too precise (we support at most 3 digits):
    ?assertMatch(
        {error, _},
        to_size_limit("1.1234TB")
    ).

validate_conf_test() ->
    %% Happy cases:
    %%   Empty config:
    ?assertMatch(
        ok,
        validate_config(
            #{<<"n_sites">> => 16}
        )
    ),
    %%   It's possible to omit db_group:
    ?assertMatch(
        ok,
        validate_config(
            #{
                <<"n_sites">> => 16,
                <<"messages">> => #{<<"backend">> => builtin_raft}
            }
        )
    ),
    %%   Explicit group with matching backend:
    ?assertMatch(
        ok,
        validate_config(
            #{
                <<"messages">> => #{<<"backend">> => builtin_raft, <<"db_group">> => foo},
                <<"db_groups">> => #{<<"foo">> => #{<<"backend">> => builtin_raft}}
            }
        )
    ),
    %% Invalid configs:
    %%   Missing group config:
    ?assertMatch(
        {error, _},
        validate_config(
            #{
                <<"messages">> => #{<<"backend">> => builtin_raft, <<"db_group">> => foo}
            }
        )
    ),
    %%   Backend mismatch with explict group:
    ?assertMatch(
        {error, _},
        validate_config(
            #{
                <<"messages">> => #{<<"backend">> => builtin_raft, <<"db_group">> => foo},
                <<"db_groups">> => #{<<"foo">> => #{<<"backend">> => builtin_local}}
            }
        )
    ),
    %%   Backend mismatch with implicit group:
    ?assertMatch(
        {error, _},
        validate_config(
            #{
                <<"messages">> => #{<<"backend">> => builtin_raft},
                <<"db_groups">> => #{<<"messages">> => #{<<"backend">> => builtin_local}}
            }
        )
    ).

-endif.
