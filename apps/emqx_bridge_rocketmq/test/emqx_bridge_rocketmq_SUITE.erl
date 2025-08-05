%%--------------------------------------------------------------------
% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rocketmq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(CONNECTOR_TYPE, <<"rocketmq">>).
-define(ACTION_TYPE, <<"rocketmq">>).

% Bridge defaults
-define(TOPIC, "TopicTest").
-define(DENY_TOPIC, "DENY_TOPIC").
-define(ACCESS_KEY, "RocketMQ").
-define(SECRET_KEY, "12345678").
-define(BATCH_SIZE, 10).
-define(PAYLOAD, <<"HELLO">>).

-define(GET_CONFIG(KEY__, CFG__), proplists:get_value(KEY__, CFG__)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, async},
        {group, sync},
        {group, once}
    ].

groups() ->
    OnceTCs = once_testcases(),
    TCs = emqx_common_test_helpers:all(?MODULE) -- OnceTCs,
    BatchingGroups = [{group, with_batch}, {group, without_batch}],
    [
        {async, BatchingGroups},
        {sync, BatchingGroups},
        {with_batch, TCs},
        {without_batch, TCs},
        {once, OnceTCs}
    ].

once_testcases() ->
    [
        t_acl_deny,
        t_key_template_required,
        t_deprecated_templated_strategy
    ].

init_per_group(async, Config) ->
    [{query_mode, async} | Config];
init_per_group(sync, Config) ->
    [{query_mode, sync} | Config];
init_per_group(with_batch, Config0) ->
    Config = [{batch_size, ?BATCH_SIZE} | Config0],
    common_init(Config);
init_per_group(without_batch, Config0) ->
    Config = [{batch_size, 1} | Config0],
    common_init(Config);
init_per_group(once, Config0) ->
    Config = [{batch_size, 1}, {query_mode, sync} | Config0],
    common_init(Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= with_batch; Group =:= without_batch ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    Apps = ?config(apps, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ConnectorName = atom_to_binary(TestCase),
    ActionName = ConnectorName,
    [
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, connector_config(#{})},
        {bridge_kind, action},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, action_config(#{<<"connector">> => ConnectorName})}
        | Config
    ].

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok = snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

common_init(ConfigT) ->
    BridgeType = <<"rocketmq">>,
    Host = os:getenv("ROCKETMQ_HOST", "toxiproxy"),
    Port = list_to_integer(os:getenv("ROCKETMQ_PORT", "9876")),
    Config0 = [
        {host, Host},
        {port, Port},
        {proxy_name, "rocketmq"}
        | ConfigT
    ],
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            % Setup toxiproxy
            ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_connector,
                    emqx_bridge_rocketmq,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config0)}
            ),
            {Name, RocketMQConf} = rocketmq_config(BridgeType, Config0),
            RocketMQSSLConf = rocketmq_ssl_config(RocketMQConf, Config0),
            Config =
                [
                    {apps, Apps},
                    {rocketmq_config, RocketMQConf},
                    {rocketmq_config_ssl, RocketMQSSLConf},
                    {rocketmq_bridge_type, BridgeType},
                    {rocketmq_name, Name},
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort}
                    | Config0
                ],
            Config;
        false ->
            case os:getenv("IS_CI") of
                false ->
                    {skip, no_rocketmq};
                _ ->
                    throw(no_rocketmq)
            end
    end.

rocketmq_ssl_config(NonSSLConfig, _TCConfig) ->
    %% TODO: generate fixed files for server and client to actually test TLS...
    %% DataDir = ?config(data_dir, TCConfig),
    %% emqx_test_tls_certs_helper:generate_tls_certs(TCConfig),
    %% Keyfile = filename:join([DataDir, "client1.key"]),
    %% Certfile = filename:join([DataDir, "client1.pem"]),
    %% CACertfile = filename:join([DataDir, "intermediate1.pem"]),
    NonSSLConfig#{
        <<"servers">> => <<"rocketmq_namesrv_ssl:9876">>,
        <<"ssl">> => #{
            %% <<"keyfile">> => iolist_to_binary(Keyfile),
            %% <<"certfile">> => iolist_to_binary(Certfile),
            %% <<"cacertfile">> => iolist_to_binary(CACertfile),
            <<"enable">> => true,
            <<"verify">> => <<"verify_none">>
        }
    }.

rocketmq_config(BridgeType, Config) ->
    Port = integer_to_list(?GET_CONFIG(port, Config)),
    Server = ?GET_CONFIG(host, Config) ++ ":" ++ Port,
    Name = atom_to_binary(?MODULE),
    BatchSize = ?config(batch_size, Config),
    QueryMode = ?config(query_mode, Config),
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {\n"
            "  enable = true\n"
            "  servers = ~p\n"
            "  access_key = ~p\n"
            "  secret_key = ~p\n"
            "  topic = ~p\n"
            "  resource_opts = {\n"
            "    request_ttl = 1500ms\n"
            "    batch_size = ~b\n"
            "    query_mode = ~s\n"
            "  }\n"
            "}",
            [
                BridgeType,
                Name,
                Server,
                ?ACCESS_KEY,
                ?SECRET_KEY,
                ?TOPIC,
                BatchSize,
                QueryMode
            ]
        ),
    {Name, emqx_bridge_testlib:parse_and_check(BridgeType, Name, ConfigString)}.

connector_config(Overrides) ->
    Defaults = #{
        <<"description">> => <<"connector">>,
        <<"tags">> => [<<"a">>, <<"b">>],
        <<"servers">> => <<"toxiproxy:9876">>,
        <<"access_key">> => ?ACCESS_KEY,
        <<"secret_key">> => ?SECRET_KEY,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"connector">> => <<"please override">>,
        <<"parameters">> => #{
            <<"topic">> => ?TOPIC
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE, <<"x">>, InnerConfigMap).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

create_bridge(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    RocketMQConf = ?GET_CONFIG(rocketmq_config, Config),
    emqx_bridge_testlib:create_bridge_api(BridgeType, Name, RocketMQConf).

create_bridge_ssl(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    RocketMQConf = ?GET_CONFIG(rocketmq_config_ssl, Config),
    emqx_bridge_testlib:create_bridge_api([
        {bridge_type, BridgeType},
        {bridge_name, Name},
        {bridge_config, RocketMQConf}
    ]).

create_bridge_ssl_bad_ssl_opts(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    RocketMQConf0 = ?GET_CONFIG(rocketmq_config_ssl, Config),
    %% This config is wrong because we use verify_peer without
    %% a cert that can be used in the verification.
    RocketMQConf1 = maps:put(
        <<"ssl">>,
        #{
            <<"enable">> => true,
            <<"verify">> => verify_peer
        },
        RocketMQConf0
    ),
    emqx_bridge_testlib:create_bridge_api(BridgeType, Name, RocketMQConf1).

delete_bridge(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    emqx_bridge:remove(BridgeType, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} ->
            #{<<"type">> := Type, <<"name">> := Name} = Params,
            _ = emqx_bridge_v2_testlib:kickoff_action_health_check(Type, Name),
            {ok, emqx_utils_json:decode(Res)};
        Error ->
            Error
    end.

%% todo: messages should be sent via rules in tests...
send_message(Config, Payload) ->
    Name = ?GET_CONFIG(rocketmq_name, Config),
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    ActionId = id(BridgeType, Name),
    emqx_bridge_v2:query(?global_ns, BridgeType, Name, {ActionId, Payload}, #{}).

%% todo: messages should be sent via rules in tests...
query_resource(Config, Request) ->
    Name = ?GET_CONFIG(rocketmq_name, Config),
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    ID = id(BridgeType, Name),
    ResID = emqx_connector_resource:resource_id(BridgeType, Name),
    emqx_resource:query(ID, Request, #{timeout => 500, connector_resource_id => ResID}).

create_action_api(Type, Name, Conf) ->
    emqx_bridge_v2_testlib:create_kind_api([
        {bridge_kind, action},
        {action_type, Type},
        {action_name, Name},
        {action_config, Conf}
    ]).

health_check(Type, Name) ->
    emqx_bridge_v2_testlib:force_health_check(#{
        type => Type,
        name => Name,
        resource_namespace => ?global_ns,
        kind => action
    }).

id(Type, Name) ->
    emqx_bridge_v2_testlib:lookup_chan_id_in_conf(#{
        kind => action,
        type => Type,
        name => Name
    }).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    SentData = #{payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_setup_via_config_and_publish_ssl(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge_ssl(Config)
    ),
    SentData = #{payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

%% Check that we can not connect to the SSL only RocketMQ instance
%% with incorrect SSL options
t_setup_via_config_ssl_host_bad_ssl_opts(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge_ssl_bad_ssl_opts(Config)
    ),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),

    ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceID)),
    ?assertMatch(#{status := disconnected}, health_check(BridgeType, Name)),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    RocketMQConf = ?GET_CONFIG(rocketmq_config, Config),
    RocketMQConf2 = RocketMQConf#{
        <<"name">> => Name,
        <<"type">> => BridgeType
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(RocketMQConf2)
    ),
    SentData = #{payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_setup_two_actions_via_http_api_and_publish(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    RocketMQConf = ?GET_CONFIG(rocketmq_config, Config),
    RocketMQConf2 = RocketMQConf#{
        <<"name">> => Name,
        <<"type">> => BridgeType
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(RocketMQConf2)
    ),
    {ok, #{raw_config := ActionConf}} = emqx_bridge_v2:lookup(
        ?global_ns, actions, BridgeType, Name
    ),
    Topic2 = <<"Topic2">>,
    ActionConf2 = emqx_utils_maps:deep_force_put(
        [<<"parameters">>, <<"topic">>], ActionConf, Topic2
    ),
    Action2Name = atom_to_binary(?FUNCTION_NAME),
    {ok, _} = create_action_api(BridgeType, Action2Name, ActionConf2),
    SentData = #{payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    Config2 = proplists:delete(rocketmq_name, Config),
    Config3 = [{rocketmq_name, Action2Name} | Config2],
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config3, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_get_status(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),

    Name = ?GET_CONFIG(rocketmq_name, Config),
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),

    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceID)),
    ?assertMatch(#{status := connected}, health_check(BridgeType, Name)),
    ok.

t_simple_query(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Type = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    ActionId = id(Type, Name),
    Request = {ActionId, #{message => <<"Hello">>}},
    Result = query_resource(Config, Request),
    ?assertEqual(ok, Result),
    ok.

t_acl_deny(Config0) ->
    RocketCfg = ?GET_CONFIG(rocketmq_config, Config0),
    RocketCfg2 = RocketCfg#{<<"topic">> := ?DENY_TOPIC},
    Config = lists:keyreplace(rocketmq_config, 1, Config0, {rocketmq_config, RocketCfg2}),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    SentData = #{payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertMatch({error, #{<<"code">> := 1}}, send_message(Config, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{error := #{<<"code">> := 1}}], Trace),
            ok
        end
    ),
    ok.

-doc """
Smoke test for templating key and tag values.
""".
t_key_tag_templates(TCConfig) when is_list(TCConfig) ->
    BatchSize = emqx_bridge_v2_testlib:get_value(batch_size, TCConfig),
    Name = emqx_bridge_v2_testlib:get_value(action_name, TCConfig),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"key">> => <<"${.mykey}">>,
                <<"tag">> => <<"${.mytag}">>
            },
            <<"resource_opts">> => #{<<"batch_size">> => BatchSize}
        })
    ),
    ActionId = emqx_bridge_resource:bridge_id(?ACTION_TYPE, Name),
    RuleTopic = <<"t/keytag">>,
    {201, _} = emqx_bridge_v2_testlib:create_rule_api2(
        #{
            <<"id">> => atom_to_binary(?FUNCTION_NAME),
            <<"description">> => <<"">>,
            <<"sql">> =>
                emqx_bridge_v2_testlib:fmt(
                    <<
                        "select *, payload.tag as mytag, payload.key as mykey"
                        " from \"${t}\" "
                    >>,
                    #{t => RuleTopic}
                ),
            <<"actions">> => [ActionId]
        }
    ),
    {ok, C} = emqtt:start_link(#{proto_ver => v5}),
    {ok, _} = emqtt:connect(C),
    Payload = emqx_utils_json:encode(#{<<"key">> => <<"k1">>, <<"tag">> => <<"t1">>}),
    ct:timetrap({seconds, 10}),
    ok = snabbkaffe:start_trace(),
    {{ok, _}, {ok, #{data := Data}}} =
        ?wait_async_action(
            emqtt:publish(C, RuleTopic, Payload, [{qos, 2}]),
            #{?snk_kind := "rocketmq_rendered_data"}
        ),
    case BatchSize of
        1 ->
            ?assertMatch(
                {_Payload, #{
                    key := <<"k1">>,
                    tag := <<"t1">>
                }},
                Data
            );
        _ ->
            ?assertMatch(
                [
                    {_Payload, #{
                        key := <<"k1">>,
                        tag := <<"t1">>
                    }}
                    | _
                ],
                Data
            )
    end,
    ok.

-doc """
Checks that we require `key` to be set if `key_dispatch` strategy is used.
""".
t_key_template_required(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"must provide a key template if strategy is key dispatch">>
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"strategy">> => <<"key_dispatch">>
            }
        })
    ),
    ok.

-doc """
Checks that we emit a warning about using deprecated strategy which used templates in that
config key to imply key dispatch.
""".
t_deprecated_templated_strategy(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{})
    ),
    ct:timetrap({seconds, 5}),
    ok = snabbkaffe:start_trace(),
    ?assertMatch(
        {{201, #{<<"status">> := <<"connected">>}}, {ok, _}},
        ?wait_async_action(
            create_action_api(TCConfig, #{
                <<"parameters">> => #{
                    <<"strategy">> => <<"${some_template}">>
                }
            }),
            #{?snk_kind := "rocketmq_deprecated_placeholder_strategy"}
        )
    ),
    ok.
