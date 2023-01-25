%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("emqx_bridge/include/emqx_bridge.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

-define(REDIS_TOXYPROXY_CONNECT_CONFIG, #{
    <<"server">> => <<"toxiproxy:6379">>,
    <<"redis_type">> => <<"single">>
}).

-define(COMMON_REDIS_OPTS, #{
    <<"password">> => <<"public">>,
    <<"command_template">> => [<<"RPUSH">>, <<"MSGS">>, <<"${payload}">>],
    <<"local_topic">> => <<"local_topic/#">>
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

all() -> [{group, transport_types}, {group, rest}].

groups() ->
    ResourceSpecificTCs = [t_create_delete_bridge],
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
    [
        {rest, TCs},
        {transport_types, [
            {group, tcp},
            {group, tls}
        ]},
        {tcp, TypeGroups},
        {tls, TypeGroups},
        {redis_single, BatchGroups},
        {redis_sentinel, BatchGroups},
        {redis_cluster, BatchGroups},
        {batch_on, ResourceSpecificTCs},
        {batch_off, ResourceSpecificTCs}
    ].

init_per_group(Group, Config) when
    Group =:= redis_single; Group =:= redis_sentinel; Group =:= redis_cluster
->
    [{transport_type, Group} | Config];
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
            ok = emqx_common_test_helpers:start_apps([emqx_conf]),
            ok = emqx_connector_test_helpers:start_apps([
                emqx_resource, emqx_bridge, emqx_rule_engine
            ]),
            {ok, _} = application:ensure_all_started(emqx_connector),
            [
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

end_per_suite(_Config) ->
    ok = delete_all_bridges(),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_rule_engine, emqx_bridge, emqx_resource]),
    _ = application:stop(emqx_connector),
    ok.

init_per_testcase(_Testcase, Config) ->
    ok = delete_all_rules(),
    ok = delete_all_bridges(),
    case ?config(transport_type, Config) of
        undefined ->
            Config;
        RedisType ->
            Transport = ?config(transport, Config),
            BatchMode = ?config(batch_mode, Config),
            #{RedisType := #{Transport := RedisConnConfig}} = redis_connect_configs(),
            #{BatchMode := ResourceConfig} = resource_configs(),
            IsBatch = (BatchMode =:= batch_on),
            BridgeConfig0 = maps:merge(RedisConnConfig, ?COMMON_REDIS_OPTS),
            BridgeConfig1 = BridgeConfig0#{<<"resource_opts">> => ResourceConfig},
            [{bridge_config, BridgeConfig1}, {is_batch, IsBatch} | Config]
    end.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok = delete_all_bridges().

t_create_delete_bridge(Config) ->
    Name = <<"mybridge">>,
    Type = ?config(transport_type, Config),
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
        5
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

    {ok, _} = emqx_bridge:remove(Type, Name).

% check that we provide correct examples
t_check_values(_Config) ->
    lists:foreach(
        fun(Method) ->
            lists:foreach(
                fun({RedisType, #{value := Value0}}) ->
                    Value = maps:without(maps:keys(?METRICS_EXAMPLE), Value0),
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
                    emqx_ee_bridge_redis:conn_bridge_examples(Method)
                )
            )
        end,
        [put, post, get]
    ).

t_check_replay(Config) ->
    Name = <<"toxic_bridge">>,
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
                            ?snk_kind := redis_ee_connector_send_done,
                            batch := true,
                            result := {error, _}
                        },
                        10_000
                    )
            end),
            #{?snk_kind := redis_ee_connector_send_done, batch := true, result := {ok, _}},
            10_000
        ),
        fun(Trace) ->
            ?assert(
                ?strict_causality(
                    #{?snk_kind := redis_ee_connector_send_done, result := {error, _}},
                    #{?snk_kind := redis_ee_connector_send_done, result := {ok, _}},
                    Trace
                )
            )
        end
    ),
    {ok, _} = emqx_bridge:remove(Type, Name).

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
                #{?snk_kind := redis_ee_connector_send_done},
                10000
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{result := {error, _}} | _],
                ?of_kind(redis_ee_connector_send_done, Trace)
            )
        end
    ),
    {ok, _} = emqx_bridge:remove(Type, Name).

t_create_disconnected(Config) ->
    Name = <<"toxic_bridge">>,
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
                ?of_kind(redis_ee_connector_start_error, Trace)
            ),
            ok
        end
    ),
    {ok, _} = emqx_bridge:remove(Type, Name).

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
                    IBin = integer_to_binary(I),
                    Topic = <<BaseTopic/binary, "/", IBin/binary>>,
                    _ = publish_message(Topic, RandomPayload)
                end,
                lists:seq(1, N)
            ),
            #{?snk_kind := redis_ee_connector_send_done, batch := IsBatch},
            5000
        ),
        fun(Trace) ->
            AddedMsgCount = length(added_msgs(ResourceId, RandomPayload)),
            case IsBatch of
                true ->
                    ?assertMatch(
                        [#{result := {ok, _}, batch := true, batch_size := ?BATCH_SIZE} | _],
                        ?of_kind(redis_ee_connector_send_done, Trace)
                    ),
                    ?assertEqual(?BATCH_SIZE, AddedMsgCount);
                false ->
                    ?assertMatch(
                        [#{result := {ok, _}, batch := false} | _],
                        ?of_kind(redis_ee_connector_send_done, Trace)
                    ),
                    ?assertEqual(1, AddedMsgCount)
            end
        end
    ).

added_msgs(ResourceId, Payload) ->
    {ok, Results} = emqx_resource:simple_sync_query(
        ResourceId, {cmd, [<<"LRANGE">>, <<"MSGS">>, <<"0">>, <<"-1">>]}
    ),
    [El || El <- Results, El =:= Payload].

conf_schema(StructName) ->
    #{
        fields => #{},
        translations => #{},
        validations => [],
        namespace => undefined,
        roots => [{root, hoconsc:ref(emqx_ee_bridge_redis, StructName)}]
    }.

delete_all_rules() ->
    lists:foreach(
        fun(#{id := RuleId}) ->
            emqx_rule_engine:delete_rule(RuleId)
        end,
        emqx_rule_engine:get_rules()
    ).

delete_all_bridges() ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
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
    emqx_schema:parse_servers(Servers, #{
        default_port => 6379
    }).

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
    Dir = code:lib_dir(emqx, etc),
    #{
        <<"keyfile">> => filename:join([Dir, <<"certs">>, <<"client-key.pem">>]),
        <<"certfile">> => filename:join([Dir, <<"certs">>, <<"client-cert.pem">>]),
        <<"cacertfile">> => filename:join([Dir, <<"certs">>, <<"cacert.pem">>])
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
                <<"sentinel">> => <<"mymaster">>
            },
            tls => #{
                <<"servers">> => <<"redis-sentinel-tls:26380">>,
                <<"redis_type">> => <<"sentinel">>,
                <<"sentinel">> => <<"mymaster">>,
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
            <<"query_mode">> => <<"async">>,
            <<"worker_pool_size">> => <<"1">>,
            <<"batch_size">> => integer_to_binary(?BATCH_SIZE),
            <<"health_check_interval">> => <<"1s">>,
            <<"start_timeout">> => <<"15s">>
        }
    },
    maps:merge(Conf0, ?COMMON_REDIS_OPTS).

invalid_command_bridge_config() ->
    #{redis_single := #{tcp := Conf0}} = redis_connect_configs(),
    Conf1 = maps:merge(Conf0, ?COMMON_REDIS_OPTS),
    Conf1#{
        <<"resource_opts">> => #{
            <<"query_mode">> => <<"sync">>,
            <<"batch_size">> => <<"1">>,
            <<"worker_pool_size">> => <<"1">>,
            <<"start_timeout">> => <<"15s">>
        },
        <<"command_template">> => [<<"BAD">>, <<"COMMAND">>, <<"${payload}">>]
    }.

resource_configs() ->
    #{
        batch_off => #{
            <<"query_mode">> => <<"sync">>,
            <<"batch_size">> => <<"1">>,
            <<"start_timeout">> => <<"15s">>
        },
        batch_on => #{
            <<"query_mode">> => <<"async">>,
            <<"worker_pool_size">> => <<"1">>,
            <<"batch_size">> => integer_to_binary(?BATCH_SIZE),
            <<"start_timeout">> => <<"15s">>
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
