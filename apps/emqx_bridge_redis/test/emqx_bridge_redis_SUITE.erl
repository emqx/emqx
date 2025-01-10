%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("emqx_bridge/include/emqx_bridge.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

-define(KEYSHARDS, 3).
-define(KEYPREFIX, "MSGS").

-define(REDIS_TOXYPROXY_CONNECT_CONFIG, #{
    <<"server">> => <<"toxiproxy:6379">>,
    <<"redis_type">> => <<"single">>
}).

-define(COMMON_REDIS_OPTS, #{
    <<"password">> => <<"public">>,
    <<"command_template">> => [<<"RPUSH">>, <<?KEYPREFIX, "/${topic}">>, <<"${payload}">>],
    <<"local_topic">> => <<"local_topic/#">>
}).

-define(USERNAME_PASSWORD_AUTH_OPTS, #{
    <<"username">> => <<"test_user">>,
    <<"password">> => <<"test_passwd">>
}).

-define(BATCH_SIZE, 5).

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, "8474").

-define(WAIT(PATTERN, EXPRESSION, TIMEOUT),
    wait(
        fun() ->
            case EXPRESSION of
                PATTERN ->
                    ok;
                Other ->
                    ct:pal("ignored wait result: ~p", [Other]),
                    error
            end
        end,
        TIMEOUT
    )
).

all() -> [{group, transports}, {group, rest}].
suite() -> [{timetrap, {minutes, 20}}].

groups() ->
    ResourceSpecificTCs = [
        t_create_delete_bridge,
        t_create_via_http,
        t_start_stop
    ],
    TCs = emqx_common_test_helpers:all(?MODULE) -- ResourceSpecificTCs,
    TypeGroups = [
        {group, redis_single},
        {group, redis_sentinel},
        {group, redis_cluster}
    ],
    BatchGroups = [
        {group, batch_on},
        {group, batch_off}
    ],
    QueryModeGroups = [{group, async}, {group, sync}],
    [
        {rest, TCs},
        {transports, [
            {group, tcp},
            {group, tls}
        ]},
        {tcp, QueryModeGroups},
        {tls, QueryModeGroups},
        {async, TypeGroups},
        {sync, TypeGroups},
        {redis_single, BatchGroups},
        {redis_sentinel, BatchGroups},
        {redis_cluster, BatchGroups},
        {batch_on, ResourceSpecificTCs},
        {batch_off, ResourceSpecificTCs}
    ].

init_per_group(async, Config) ->
    [{query_mode, async} | Config];
init_per_group(sync, Config) ->
    [{query_mode, sync} | Config];
init_per_group(Group, Config) when
    Group =:= redis_single; Group =:= redis_sentinel; Group =:= redis_cluster
->
    [{connector_type, Group} | Config];
init_per_group(Group, Config) when
    Group =:= tcp; Group =:= tls
->
    [{transport, Group} | Config];
init_per_group(Group, Config) when
    Group =:= batch_on; Group =:= batch_off
->
    [{batch_mode, Group} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    wait_for_ci_redis(redis_checks(), Config).

wait_for_ci_redis(0, _Config) ->
    throw(no_redis);
wait_for_ci_redis(Checks, Config) ->
    timer:sleep(1000),
    TestHosts = all_test_hosts(),
    case emqx_common_test_helpers:is_all_tcp_servers_available(TestHosts) of
        true ->
            ProxyHost = os:getenv("PROXY_HOST", ?PROXY_HOST),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", ?PROXY_PORT)),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_resource,
                    emqx_bridge,
                    emqx_bridge_redis,
                    emqx_rule_engine,
                    emqx_management,
                    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            {ok, _Api} = emqx_common_test_http:create_default_app(),
            [
                {apps, Apps},
                {proxy_host, ProxyHost},
                {proxy_port, ProxyPort}
                | Config
            ];
        false ->
            wait_for_ci_redis(Checks - 1, Config)
    end.

redis_checks() ->
    case os:getenv("IS_CI") of
        "yes" ->
            10;
        _ ->
            1
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(Testcase, Config0) ->
    emqx_logger:set_log_level(debug),
    ok = delete_all_rules(),
    ok = emqx_bridge_v2_SUITE:delete_all_bridges_and_connectors(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(Testcase))/binary, UniqueNum/binary>>,
    Config = [{bridge_name, Name} | Config0],
    case {?config(connector_type, Config), ?config(batch_mode, Config)} of
        {undefined, _} ->
            Config;
        {redis_cluster, batch_on} ->
            {skip, "Batching is not supported by 'redis_cluster' bridge type"};
        {RedisType, BatchMode} ->
            Transport = ?config(transport, Config),
            QueryMode = ?config(query_mode, Config),
            #{RedisType := #{Transport := RedisConnConfig}} = redis_connect_configs(),
            #{BatchMode := ResourceConfig} = resource_configs(#{query_mode => QueryMode}),
            IsBatch = (BatchMode =:= batch_on),
            BridgeConfig0 = maps:merge(RedisConnConfig, ?COMMON_REDIS_OPTS),
            BridgeConfig1 = BridgeConfig0#{<<"resource_opts">> => ResourceConfig},
            [
                {bridge_type, RedisType},
                {bridge_config, BridgeConfig1},
                {is_batch, IsBatch}
                | Config
            ]
    end.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok = emqx_bridge_v2_SUITE:delete_all_bridges_and_connectors().

t_create_delete_bridge(Config) ->
    Pid = erlang:whereis(eredis_sentinel),
    ct:pal("t_create_detele_bridge:~p~n", [
        #{
            config => Config,
            sentinel => Pid,
            eredis_sentinel => Pid =/= undefined andalso erlang:process_info(Pid)
        }
    ]),
    Name = ?config(bridge_name, Config),
    Type = ?config(connector_type, Config),
    BridgeConfig = ?config(bridge_config, Config),
    IsBatch = ?config(is_batch, Config),
    ?assertMatch(
        {ok, _},
        emqx_bridge:create(Type, Name, BridgeConfig)
    ),
    ResourceId = emqx_bridge_resource:resource_id(Type, Name),
    ?WAIT(
        {ok, connected},
        emqx_resource:health_check(ResourceId),
        10
    ),

    RedisType = atom_to_binary(Type),
    Action = <<RedisType/binary, ":", Name/binary>>,

    RuleId = <<"my_rule_id">>,
    RuleConf = #{
        actions => [Action],
        description => <<>>,
        enable => true,
        id => RuleId,
        name => <<>>,
        sql => <<"SELECT * FROM \"t/#\"">>
    },

    %% check export by rule
    {ok, _} = emqx_rule_engine:create_rule(RuleConf),
    _ = check_resource_queries(ResourceId, <<"t/test">>, IsBatch),
    ok = emqx_rule_engine:delete_rule(RuleId),

    %% check export through local topic
    _ = check_resource_queries(ResourceId, <<"local_topic/test">>, IsBatch),

    ok = emqx_bridge:remove(Type, Name).

% check that we provide correct examples
t_check_values(_Config) ->
    lists:foreach(
        fun(Method) ->
            lists:foreach(
                fun({RedisType, #{value := Value}}) ->
                    MethodBin = atom_to_binary(Method),
                    Type = string:slice(RedisType, length("redis_")),
                    RefName = binary_to_list(<<MethodBin/binary, "_", Type/binary>>),
                    Schema = conf_schema(RefName),
                    ?assertMatch(
                        #{},
                        hocon_tconf:check_plain(Schema, #{<<"root">> => Value}, #{
                            atom_key => true,
                            required => false
                        })
                    )
                end,
                lists:flatmap(
                    fun maps:to_list/1,
                    emqx_bridge_redis:conn_bridge_examples(Method)
                )
            )
        end,
        [put, post, get]
    ).

t_check_replay(Config) ->
    Name = ?config(bridge_name, Config),
    Type = <<"redis_single">>,
    Topic = <<"local_topic/test">>,
    ProxyName = "redis_single_tcp",

    ?assertMatch(
        {ok, _},
        emqx_bridge:create(Type, Name, toxiproxy_redis_bridge_config())
    ),

    ResourceId = emqx_bridge_resource:resource_id(Type, Name),

    ?WAIT(
        {ok, connected},
        emqx_resource:health_check(ResourceId),
        5
    ),

    ?check_trace(
        ?wait_async_action(
            with_down_failure(Config, ProxyName, fun() ->
                {_, {ok, _}} =
                    ?wait_async_action(
                        lists:foreach(
                            fun(_) ->
                                _ = publish_message(Topic, <<"test_payload">>)
                            end,
                            lists:seq(1, ?BATCH_SIZE)
                        ),
                        #{
                            ?snk_kind := redis_bridge_connector_send_done,
                            batch := true,
                            result := {error, _}
                        },
                        10_000
                    )
            end),
            #{?snk_kind := redis_bridge_connector_send_done, batch := true, result := {ok, _}},
            10_000
        ),
        fun(Trace) ->
            ?assert(
                ?strict_causality(
                    #{?snk_kind := redis_bridge_connector_send_done, result := {error, _}},
                    #{?snk_kind := redis_bridge_connector_send_done, result := {ok, _}},
                    Trace
                )
            )
        end
    ),
    ok = emqx_bridge:remove(Type, Name).

t_permanent_error(_Config) ->
    Name = <<"invalid_command_bridge">>,
    Type = <<"redis_single">>,
    Topic = <<"local_topic/test">>,
    Payload = <<"payload for invalid redis command">>,

    ?assertMatch(
        {ok, _},
        emqx_bridge:create(Type, Name, invalid_command_bridge_config())
    ),

    ?check_trace(
        begin
            ?wait_async_action(
                publish_message(Topic, Payload),
                #{?snk_kind := redis_bridge_connector_send_done},
                10_000
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{result := {error, _}} | _],
                ?of_kind(redis_bridge_connector_send_done, Trace)
            )
        end
    ),
    ok = emqx_bridge:remove(Type, Name).

t_auth_username_password(Config) ->
    Name = ?config(bridge_name, Config),
    Type = <<"redis_single">>,
    BridgeConfig = username_password_redis_bridge_config(),
    ?assertMatch(
        {ok, _},
        emqx_bridge:create(Type, Name, BridgeConfig)
    ),
    ResourceId = emqx_bridge_resource:resource_id(Type, Name),
    ?WAIT(
        {ok, connected},
        emqx_resource:health_check(ResourceId),
        5
    ),
    ok = emqx_bridge:remove(Type, Name).

t_auth_error_username_password(Config) ->
    Name = ?config(bridge_name, Config),
    Type = <<"redis_single">>,
    BridgeConfig0 = username_password_redis_bridge_config(),
    BridgeConfig = maps:merge(BridgeConfig0, #{<<"password">> => <<"wrong_password">>}),
    ?assertMatch(
        {ok, _},
        emqx_bridge:create(Type, Name, BridgeConfig)
    ),
    ResourceId = emqx_bridge_resource:resource_id(Type, Name),
    ?WAIT(
        {ok, disconnected},
        emqx_resource:health_check(ResourceId),
        5
    ),
    ?assertMatch(
        {ok, _, #{error := {unhealthy_target, _Msg}}},
        emqx_resource_manager:lookup(ResourceId)
    ),
    ok = emqx_bridge:remove(Type, Name).

t_auth_error_password_only(Config) ->
    Name = ?config(bridge_name, Config),
    Type = <<"redis_single">>,
    BridgeConfig0 = toxiproxy_redis_bridge_config(),
    BridgeConfig = maps:merge(BridgeConfig0, #{<<"password">> => <<"wrong_password">>}),
    ?assertMatch(
        {ok, _},
        emqx_bridge:create(Type, Name, BridgeConfig)
    ),
    ResourceId = emqx_bridge_resource:resource_id(Type, Name),
    ?assertEqual(
        {ok, disconnected},
        emqx_resource:health_check(ResourceId)
    ),
    ?assertMatch(
        {ok, _, #{error := {unhealthy_target, _Msg}}},
        emqx_resource_manager:lookup(ResourceId)
    ),
    ok = emqx_bridge:remove(Type, Name).

t_create_disconnected(Config) ->
    Name = ?config(bridge_name, Config),
    Type = <<"redis_single">>,

    ?check_trace(
        with_down_failure(Config, "redis_single_tcp", fun() ->
            {ok, _} = emqx_bridge:create(
                Type, Name, toxiproxy_redis_bridge_config()
            )
        end),
        fun(Trace) ->
            ?assertMatch(
                [#{error := _} | _],
                ?of_kind(redis_bridge_connector_start_error, Trace)
            ),
            ok
        end
    ),
    ok = emqx_bridge:remove(Type, Name).

t_create_via_http(Config) ->
    ok = emqx_bridge_testlib:t_create_via_http(Config),
    ok.

t_start_stop(Config) ->
    ok = emqx_bridge_testlib:t_start_stop(Config, redis_bridge_stopped),
    ok.

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

with_down_failure(Config, Name, F) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    emqx_common_test_helpers:with_failure(down, Name, ProxyHost, ProxyPort, F).

check_resource_queries(ResourceId, BaseTopic, IsBatch) ->
    RandomPayload = rand:bytes(20),
    N =
        case IsBatch of
            true -> ?BATCH_SIZE;
            false -> 1
        end,
    ?check_trace(
        ?wait_async_action(
            lists:foreach(
                fun(I) ->
                    _ = publish_message(format_topic(BaseTopic, I), RandomPayload)
                end,
                lists:seq(1, N)
            ),
            #{?snk_kind := redis_bridge_connector_send_done, batch := IsBatch},
            5000
        ),
        fun(Trace) ->
            AddedMsgCount = length(added_msgs(ResourceId, BaseTopic, RandomPayload)),
            case IsBatch of
                true ->
                    ?assertMatch(
                        [#{result := {ok, _}, batch := true, batch_size := ?BATCH_SIZE} | _],
                        ?of_kind(redis_bridge_connector_send_done, Trace)
                    ),
                    ?assertEqual(?BATCH_SIZE, AddedMsgCount);
                false ->
                    ?assertMatch(
                        [#{result := {ok, _}, batch := false} | _],
                        ?of_kind(redis_bridge_connector_send_done, Trace)
                    ),
                    ?assertEqual(1, AddedMsgCount)
            end
        end
    ).

added_msgs(ResourceId, BaseTopic, Payload) ->
    lists:flatmap(
        fun(K) ->
            Message = {cmd, [<<"LRANGE">>, K, <<"0">>, <<"-1">>]},
            {ok, Results} = emqx_resource:simple_sync_query(ResourceId, Message),
            [El || El <- Results, El =:= Payload]
        end,
        [format_redis_key(BaseTopic, S) || S <- lists:seq(0, ?KEYSHARDS - 1)]
    ).

format_topic(Base, I) ->
    iolist_to_binary(io_lib:format("~s/~2..0B", [Base, I rem ?KEYSHARDS])).

format_redis_key(Base, I) ->
    iolist_to_binary([?KEYPREFIX, "/", format_topic(Base, I)]).

conf_schema(StructName) ->
    #{
        fields => #{},
        translations => #{},
        validations => [],
        namespace => undefined,
        roots => [{root, hoconsc:ref(emqx_bridge_redis, StructName)}]
    }.

delete_all_rules() ->
    lists:foreach(
        fun(#{id := RuleId}) ->
            emqx_rule_engine:delete_rule(RuleId)
        end,
        emqx_rule_engine:get_rules()
    ).

all_test_hosts() ->
    Confs = [
        ?REDIS_TOXYPROXY_CONNECT_CONFIG
        | lists:concat([
            maps:values(TypeConfs)
         || TypeConfs <- maps:values(redis_connect_configs())
        ])
    ],
    lists:flatmap(
        fun
            (#{<<"servers">> := ServersRaw}) ->
                parse_servers(ServersRaw);
            (#{<<"server">> := ServerRaw}) ->
                parse_servers(ServerRaw)
        end,
        Confs
    ).

parse_servers(Servers) ->
    lists:map(
        fun(#{hostname := Host, port := Port}) ->
            {Host, Port}
        end,
        emqx_schema:parse_servers(Servers, #{
            default_port => 6379
        })
    ).

redis_connect_ssl_opts(Type) ->
    maps:merge(
        client_ssl_cert_opts(Type),
        #{
            <<"enable">> => <<"true">>,
            <<"verify">> => <<"verify_none">>
        }
    ).

client_ssl_cert_opts(redis_single) ->
    emqx_authn_test_lib:client_ssl_cert_opts();
client_ssl_cert_opts(_) ->
    Dir = code:lib_dir(emqx),
    #{
        <<"keyfile">> => filename:join([Dir, <<"etc">>, <<"certs">>, <<"client-key.pem">>]),
        <<"certfile">> => filename:join([Dir, <<"etc">>, <<"certs">>, <<"client-cert.pem">>]),
        <<"cacertfile">> => filename:join([Dir, <<"etc">>, <<"certs">>, <<"cacert.pem">>])
    }.

redis_connect_configs() ->
    #{
        redis_single => #{
            tcp => #{
                <<"server">> => <<"redis:6379">>,
                <<"redis_type">> => <<"single">>
            },
            tls => #{
                <<"server">> => <<"redis-tls:6380">>,
                <<"ssl">> => redis_connect_ssl_opts(redis_single),
                <<"redis_type">> => <<"single">>
            }
        },
        redis_sentinel => #{
            tcp => #{
                <<"servers">> => <<"redis-sentinel:26379">>,
                <<"redis_type">> => <<"sentinel">>,
                <<"sentinel">> => <<"mytcpmaster">>
            },
            tls => #{
                <<"servers">> => <<"redis-sentinel-tls:26380">>,
                <<"redis_type">> => <<"sentinel">>,
                <<"sentinel">> => <<"mytlsmaster">>,
                <<"ssl">> => redis_connect_ssl_opts(redis_sentinel)
            }
        },
        redis_cluster => #{
            tcp => #{
                <<"servers">> =>
                    <<"redis-cluster-1:6379,redis-cluster-2:6379,redis-cluster-3:6379">>,
                <<"redis_type">> => <<"cluster">>
            },
            tls => #{
                <<"servers">> =>
                    <<"redis-cluster-tls-1:6389,redis-cluster-tls-2:6389,redis-cluster-tls-3:6389">>,
                <<"redis_type">> => <<"cluster">>,
                <<"ssl">> => redis_connect_ssl_opts(redis_cluster)
            }
        }
    }.

toxiproxy_redis_bridge_config() ->
    Conf0 = ?REDIS_TOXYPROXY_CONNECT_CONFIG#{
        <<"resource_opts">> => #{
            <<"query_mode">> => <<"sync">>,
            <<"worker_pool_size">> => <<"1">>,
            <<"batch_size">> => integer_to_binary(?BATCH_SIZE),
            <<"health_check_interval">> => <<"1s">>,
            <<"max_buffer_bytes">> => <<"256MB">>,
            <<"buffer_seg_bytes">> => <<"10MB">>,
            <<"request_ttl">> => <<"45s">>,
            <<"inflight_window">> => <<"100">>,
            <<"resume_interval">> => <<"1s">>,
            <<"metrics_flush_interval">> => <<"1s">>,
            <<"start_after_created">> => true,
            <<"start_timeout">> => <<"5s">>
        }
    },
    maps:merge(Conf0, ?COMMON_REDIS_OPTS).

username_password_redis_bridge_config() ->
    Conf0 = ?REDIS_TOXYPROXY_CONNECT_CONFIG#{
        <<"resource_opts">> => #{
            <<"query_mode">> => <<"sync">>,
            <<"worker_pool_size">> => <<"1">>,
            <<"batch_size">> => integer_to_binary(?BATCH_SIZE),
            <<"health_check_interval">> => <<"1s">>,
            <<"max_buffer_bytes">> => <<"256MB">>,
            <<"buffer_seg_bytes">> => <<"10MB">>,
            <<"request_ttl">> => <<"45s">>,
            <<"inflight_window">> => <<"100">>,
            <<"resume_interval">> => <<"15s">>,
            <<"metrics_flush_interval">> => <<"1s">>,
            <<"start_after_created">> => true,
            <<"start_timeout">> => <<"5s">>
        }
    },
    Conf1 = maps:merge(Conf0, ?COMMON_REDIS_OPTS),
    maps:merge(Conf1, ?USERNAME_PASSWORD_AUTH_OPTS).

invalid_command_bridge_config() ->
    #{redis_single := #{tcp := Conf0}} = redis_connect_configs(),
    Conf1 = maps:merge(Conf0, ?COMMON_REDIS_OPTS),
    Conf1#{
        <<"resource_opts">> => #{
            <<"query_mode">> => <<"sync">>,
            <<"worker_pool_size">> => <<"1">>,
            <<"start_timeout">> => <<"15s">>
        },
        <<"command_template">> => [<<"BAD">>, <<"COMMAND">>, <<"${payload}">>]
    }.

resource_configs(#{query_mode := QueryMode}) ->
    #{
        batch_off => #{
            <<"query_mode">> => atom_to_binary(QueryMode),
            <<"start_timeout">> => <<"15s">>
        },
        batch_on => #{
            <<"query_mode">> => atom_to_binary(QueryMode),
            <<"worker_pool_size">> => <<"1">>,
            <<"batch_size">> => integer_to_binary(?BATCH_SIZE),
            <<"start_timeout">> => <<"15s">>,
            <<"batch_time">> => <<"4s">>,
            <<"request_ttl">> => <<"30s">>
        }
    }.

publish_message(Topic, Payload) ->
    {ok, Client} = emqtt:start_link(),
    {ok, _} = emqtt:connect(Client),
    ok = emqtt:publish(Client, Topic, Payload),
    ok = emqtt:stop(Client).

wait(_F, 0) ->
    error(timeout);
wait(F, Attempt) ->
    case F() of
        ok ->
            ok;
        _ ->
            timer:sleep(1000),
            wait(F, Attempt - 1)
    end.
