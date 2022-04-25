%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_telemetry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(proplists, [get_value/2]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = meck:new(emqx_authz, [non_strict, passthrough, no_history, no_link]),
    meck:expect(
        emqx_authz,
        acl_conf_file,
        fun() ->
            emqx_common_test_helpers:deps_path(emqx_authz, "etc/acl.conf")
        end
    ),
    emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authn, emqx_authz, emqx_modules],
        fun set_special_configs/1
    ),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    emqx_common_test_helpers:stop_apps([emqx_conf, emqx_authn, emqx_authz, emqx_modules]),
    meck:unload(emqx_authz),
    ok.

init_per_testcase(t_get_telemetry, Config) ->
    DataDir = ?config(data_dir, Config),
    mock_httpc(),
    ok = meck:new(emqx_telemetry, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_telemetry,
        read_raw_build_info,
        fun() ->
            Path = filename:join([DataDir, "BUILD_INFO"]),
            {ok, Template} = file:read_file(Path),
            Vars0 = [
                {build_info_arch, "arch"},
                {build_info_wordsize, "64"},
                {build_info_os, "os"},
                {build_info_erlang, "erlang"},
                {build_info_elixir, "elixir"},
                {build_info_relform, "relform"}
            ],
            Vars = [
                {atom_to_list(K), iolist_to_binary(V)}
             || {K, V} <- Vars0
            ],
            Rendered = bbmustache:render(Template, Vars),
            {ok, Rendered}
        end
    ),
    Lwm2mDataDir = emqx_common_test_helpers:deps_path(
        emqx_gateway,
        "test/emqx_gateway_SUITE_data"
    ),
    ok = emqx_gateway_SUITE:setup_fake_usage_data(Lwm2mDataDir),
    {ok, _} = application:ensure_all_started(emqx_gateway),
    Config;
init_per_testcase(t_advanced_mqtt_features, Config) ->
    OldValues = emqx_modules:get_advanced_mqtt_features_in_use(),
    emqx_modules:set_advanced_mqtt_features_in_use(#{
        delayed => false,
        topic_rewrite => false,
        retained => false,
        auto_subscribe => false
    }),
    [{old_values, OldValues} | Config];
init_per_testcase(t_authn_authz_info, Config) ->
    mock_httpc(),
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    create_authn('mqtt:global', built_in_database),
    create_authn('tcp:default', redis),
    create_authn('ws:default', redis),
    create_authz(postgresql),
    Config;
init_per_testcase(t_enable, Config) ->
    ok = meck:new(emqx_telemetry, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(emqx_telemetry, official_version, fun(_) -> true end),
    mock_httpc(),
    Config;
init_per_testcase(t_send_after_enable, Config) ->
    ok = meck:new(emqx_telemetry, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(emqx_telemetry, official_version, fun(_) -> true end),
    mock_httpc(),
    Config;
init_per_testcase(t_rule_engine_and_data_bridge_info, Config) ->
    mock_httpc(),
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    {ok, _} = application:ensure_all_started(emqx_rule_engine),
    ok = application:start(emqx_bridge),
    ok = emqx_bridge_SUITE:setup_fake_telemetry_data(),
    ok = setup_fake_rule_engine_data(),
    Config;
init_per_testcase(t_exhook_info, Config) ->
    mock_httpc(),
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    ExhookConf =
        #{
            <<"exhook">> =>
                #{
                    <<"servers">> =>
                        [
                            #{
                                <<"name">> => "myhook",
                                <<"url">> => "http://127.0.0.1:9000"
                            }
                        ]
                }
        },
    {ok, _} = emqx_exhook_demo_svr:start(),
    ok = emqx_common_test_helpers:load_config(emqx_exhook_schema, ExhookConf),
    {ok, _} = application:ensure_all_started(emqx_exhook),
    Config;
init_per_testcase(_Testcase, Config) ->
    mock_httpc(),
    Config.

end_per_testcase(t_get_telemetry, _Config) ->
    meck:unload([httpc, emqx_telemetry]),
    application:stop(emqx_gateway),
    ok;
end_per_testcase(t_advanced_mqtt_features, Config) ->
    OldValues = ?config(old_values, Config),
    emqx_modules:set_advanced_mqtt_features_in_use(OldValues);
end_per_testcase(t_authn_authz_info, _Config) ->
    meck:unload([httpc]),
    emqx_authz:update({delete, postgresql}, #{}),
    lists:foreach(
        fun(ChainName) ->
            catch emqx_authn_test_lib:delete_authenticators(
                [authentication],
                ChainName
            )
        end,
        ['mqtt:global', 'tcp:default', 'ws:default']
    ),
    ok;
end_per_testcase(t_enable, _Config) ->
    meck:unload([httpc, emqx_telemetry]);
end_per_testcase(t_send_after_enable, _Config) ->
    meck:unload([httpc, emqx_telemetry]);
end_per_testcase(t_rule_engine_and_data_bridge_info, _Config) ->
    meck:unload(httpc),
    lists:foreach(
        fun(App) ->
            ok = application:stop(App)
        end,
        [
            emqx_bridge,
            emqx_rule_engine
        ]
    ),
    ok;
end_per_testcase(t_exhook_info, _Config) ->
    meck:unload(httpc),
    emqx_exhook_demo_svr:stop(),
    application:stop(emqx_exhook),
    ok;
end_per_testcase(_Testcase, _Config) ->
    meck:unload([httpc]),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

t_node_uuid(_) ->
    UUID = emqx_telemetry:generate_uuid(),
    Parts = binary:split(UUID, <<"-">>, [global, trim]),
    ?assertEqual(5, length(Parts)),
    {ok, NodeUUID2} = emqx_telemetry:get_node_uuid(),
    emqx_telemetry:disable(),
    emqx_telemetry:enable(),
    emqx_modules_conf:set_telemetry_status(false),
    emqx_modules_conf:set_telemetry_status(true),
    {ok, NodeUUID3} = emqx_telemetry:get_node_uuid(),
    {ok, NodeUUID4} = emqx_telemetry_proto_v1:get_node_uuid(node()),
    ?assertEqual(NodeUUID2, NodeUUID3),
    ?assertEqual(NodeUUID3, NodeUUID4),
    ?assertMatch({badrpc, nodedown}, emqx_telemetry_proto_v1:get_node_uuid('fake@node')).

t_cluster_uuid(_Config) ->
    {ok, ClusterUUID0} = emqx_telemetry:get_cluster_uuid(),
    {ok, ClusterUUID1} = emqx_telemetry_proto_v1:get_cluster_uuid(node()),
    ?assertEqual(ClusterUUID0, ClusterUUID1),
    {ok, NodeUUID0} = emqx_telemetry:get_node_uuid(),

    Node = start_slave(n1),
    try
        ok = setup_slave(Node),
        {ok, ClusterUUID2} = emqx_telemetry_proto_v1:get_cluster_uuid(Node),
        ?assertEqual(ClusterUUID0, ClusterUUID2),
        {ok, NodeUUID1} = emqx_telemetry_proto_v1:get_node_uuid(Node),
        ?assertNotEqual(NodeUUID0, NodeUUID1),
        ok
    after
        ok = stop_slave(Node)
    end,
    ok.

t_official_version(_) ->
    true = emqx_telemetry:official_version("0.0.0"),
    true = emqx_telemetry:official_version("1.1.1"),
    true = emqx_telemetry:official_version("10.10.10"),
    false = emqx_telemetry:official_version("0.0.0.0"),
    false = emqx_telemetry:official_version("1.1.a"),
    true = emqx_telemetry:official_version("0.0-alpha.1"),
    true = emqx_telemetry:official_version("1.1-alpha.1"),
    true = emqx_telemetry:official_version("10.10-alpha.10"),
    false = emqx_telemetry:official_version("1.1-alpha.0"),
    true = emqx_telemetry:official_version("1.1-beta.1"),
    true = emqx_telemetry:official_version("1.1-rc.1"),
    false = emqx_telemetry:official_version("1.1-alpha.a"),
    true = emqx_telemetry:official_version("5.0.0"),
    true = emqx_telemetry:official_version("5.0.0-alpha.1"),
    true = emqx_telemetry:official_version("5.0.0-beta.4"),
    true = emqx_telemetry:official_version("5.0-rc.1"),
    true = emqx_telemetry:official_version("5.0.0-rc.1"),
    false = emqx_telemetry:official_version("5.0.0-alpha.a"),
    false = emqx_telemetry:official_version("5.0.0-beta.a"),
    false = emqx_telemetry:official_version("5.0.0-rc.a"),
    false = emqx_telemetry:official_version("5.0.0-foo"),
    false = emqx_telemetry:official_version("5.0.0-rc.1-ccdf7920"),
    ok.

t_get_telemetry(_Config) ->
    {ok, TelemetryData} = emqx_telemetry:get_telemetry(),
    OTPVersion = bin(erlang:system_info(otp_release)),
    ?assertEqual(OTPVersion, get_value(otp_version, TelemetryData)),
    {ok, NodeUUID} = emqx_telemetry:get_node_uuid(),
    {ok, ClusterUUID} = emqx_telemetry:get_cluster_uuid(),
    ?assertEqual(NodeUUID, get_value(uuid, TelemetryData)),
    ?assertEqual(ClusterUUID, get_value(cluster_uuid, TelemetryData)),
    ?assertNotEqual(NodeUUID, ClusterUUID),
    ?assertEqual(0, get_value(num_clients, TelemetryData)),
    BuildInfo = get_value(build_info, TelemetryData),
    ?assertMatch(
        #{
            <<"arch">> := <<_/binary>>,
            <<"elixir">> := <<_/binary>>,
            <<"erlang">> := <<_/binary>>,
            <<"os">> := <<_/binary>>,
            <<"relform">> := <<_/binary>>,
            <<"wordsize">> := Wordsize
        } when is_integer(Wordsize),
        BuildInfo
    ),
    VMSpecs = get_value(vm_specs, TelemetryData),
    ?assert(is_integer(get_value(num_cpus, VMSpecs))),
    ?assert(0 =< get_value(num_cpus, VMSpecs)),
    ?assert(is_integer(get_value(total_memory, VMSpecs))),
    ?assert(0 =< get_value(total_memory, VMSpecs)),
    MQTTRTInsights = get_value(mqtt_runtime_insights, TelemetryData),
    ?assert(is_number(maps:get(messages_sent_rate, MQTTRTInsights))),
    ?assert(is_number(maps:get(messages_received_rate, MQTTRTInsights))),
    ?assert(is_integer(maps:get(num_topics, MQTTRTInsights))),
    ?assert(is_map(get_value(authn_authz, TelemetryData))),
    GatewayInfo = get_value(gateway, TelemetryData),
    ?assert(is_map(GatewayInfo)),
    lists:foreach(
        fun({GatewayType, GatewayData}) ->
            ?assertMatch(
                #{
                    authn := GwAuthn,
                    num_clients := NClients,
                    listeners := Ls
                } when
                    is_binary(GwAuthn) andalso
                        is_integer(NClients) andalso
                        is_list(Ls),
                GatewayData,
                #{gateway_type => GatewayType}
            ),
            ListenersData = maps:get(listeners, GatewayData),
            lists:foreach(
                fun(L) -> assert_gateway_listener_shape(L, GatewayType) end,
                ListenersData
            )
        end,
        maps:to_list(GatewayInfo)
    ),
    ok.

t_advanced_mqtt_features(_) ->
    {ok, TelemetryData} = emqx_telemetry:get_telemetry(),
    AdvFeats = get_value(advanced_mqtt_features, TelemetryData),
    ?assertEqual(
        #{
            retained => 0,
            topic_rewrite => 0,
            auto_subscribe => 0,
            delayed => 0
        },
        AdvFeats
    ),
    lists:foreach(
        fun(TelemetryKey) ->
            EnabledFeats = emqx_modules:get_advanced_mqtt_features_in_use(),
            emqx_modules:set_advanced_mqtt_features_in_use(EnabledFeats#{TelemetryKey => true}),
            {ok, Data} = emqx_telemetry:get_telemetry(),
            #{TelemetryKey := Value} = get_value(advanced_mqtt_features, Data),
            ?assertEqual(1, Value, #{key => TelemetryKey})
        end,
        [
            retained,
            topic_rewrite,
            auto_subscribe,
            delayed
        ]
    ),
    ok.

t_authn_authz_info(_) ->
    {ok, TelemetryData} = emqx_telemetry:get_telemetry(),
    AuthnAuthzInfo = get_value(authn_authz, TelemetryData),
    ?assertEqual(
        #{
            authn =>
                [
                    <<"password_based:built_in_database">>,
                    <<"password_based:redis">>
                ],
            authn_listener => #{<<"password_based:redis">> => 2},
            authz => [postgresql]
        },
        AuthnAuthzInfo
    ).

t_enable(_) ->
    ok = emqx_telemetry:enable(),
    ok = emqx_telemetry:disable().

t_send_after_enable(_) ->
    ok = emqx_telemetry:disable(),
    ok = snabbkaffe:start_trace(),
    try
        ok = emqx_telemetry:enable(),
        Timeout = 12_000,
        ?assertMatch(
            {ok, _},
            ?wait_async_action(
                ok = emqx_telemetry:enable(),
                #{?snk_kind := telemetry_data_reported},
                Timeout
            )
        ),
        receive
            {request, post, _URL, _Headers, Body} ->
                {ok, Decoded} = emqx_json:safe_decode(Body, [return_maps]),
                ?assertMatch(
                    #{
                        <<"uuid">> := _,
                        <<"messages_received">> := _,
                        <<"messages_sent">> := _,
                        <<"build_info">> := #{},
                        <<"vm_specs">> :=
                            #{
                                <<"num_cpus">> := _,
                                <<"total_memory">> := _
                            },
                        <<"mqtt_runtime_insights">> :=
                            #{
                                <<"messages_received_rate">> := _,
                                <<"messages_sent_rate">> := _,
                                <<"num_topics">> := _
                            },
                        <<"advanced_mqtt_features">> :=
                            #{
                                <<"retained">> := _,
                                <<"topic_rewrite">> := _,
                                <<"auto_subscribe">> := _,
                                <<"delayed">> := _
                            }
                    },
                    Decoded
                )
        after 2100 ->
            exit(telemetry_not_reported)
        end
    after
        ok = snabbkaffe:stop()
    end.

t_mqtt_runtime_insights(_) ->
    State0 = emqx_telemetry:empty_state(),
    {MQTTRTInsights1, State1} = emqx_telemetry:mqtt_runtime_insights(State0),
    ?assertEqual(
        #{
            messages_sent_rate => 0.0,
            messages_received_rate => 0.0,
            num_topics => 0
        },
        MQTTRTInsights1
    ),
    %% add some fake stats
    emqx_metrics:set('messages.sent', 10_000_000_000),
    emqx_metrics:set('messages.received', 20_000_000_000),
    emqx_stats:setstat('topics.count', 30_000),
    {MQTTRTInsights2, _State2} = emqx_telemetry:mqtt_runtime_insights(State1),
    assert_approximate(MQTTRTInsights2, messages_sent_rate, "16.53"),
    assert_approximate(MQTTRTInsights2, messages_received_rate, "33.07"),
    ?assertEqual(30_000, maps:get(num_topics, MQTTRTInsights2)),
    ok.

t_rule_engine_and_data_bridge_info(_Config) ->
    {ok, TelemetryData} = emqx_telemetry:get_telemetry(),
    RuleInfo = get_value(rule_engine, TelemetryData),
    BridgeInfo = get_value(bridge, TelemetryData),
    ?assertEqual(
        #{num_rules => 2},
        RuleInfo
    ),
    ?assertEqual(
        #{
            data_bridge =>
                #{
                    http => #{num => 1, num_linked_by_rules => 3},
                    mqtt => #{num => 1, num_linked_by_rules => 1}
                },
            num_data_bridges => 2
        },
        BridgeInfo
    ),
    ok.

t_exhook_info(_Config) ->
    {ok, TelemetryData} = emqx_telemetry:get_telemetry(),
    ExhookInfo = get_value(exhook, TelemetryData),
    ?assertEqual(1, maps:get(num_servers, ExhookInfo)),
    [Server] = maps:get(servers, ExhookInfo),
    ?assertEqual(grpc, maps:get(driver, Server)),
    Hooks = maps:get(hooks, Server),
    ?assertEqual(
        [
            'client.authenticate',
            'client.authorize',
            'client.connack',
            'client.connect',
            'client.connected',
            'client.disconnected',
            'client.subscribe',
            'client.unsubscribe',
            'message.acked',
            'message.delivered',
            'message.dropped',
            'message.publish',
            'session.created',
            'session.discarded',
            'session.resumed',
            'session.subscribed',
            'session.takenover',
            'session.terminated',
            'session.unsubscribed'
        ],
        lists:sort(Hooks)
    ),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

assert_approximate(Map, Key, Expected) ->
    Value = maps:get(Key, Map),
    ?assertEqual(Expected, float_to_list(Value, [{decimals, 2}])).

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.

mock_httpc() ->
    TestPID = self(),
    ok = meck:new(httpc, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(httpc, request, fun(
        Method, {URL, Headers, _ContentType, Body}, _HTTPOpts, _Opts
    ) ->
        TestPID ! {request, Method, URL, Headers, Body}
    end).

create_authn(ChainName, built_in_database) ->
    emqx_authentication:initialize_authentication(
        ChainName,
        [
            #{
                mechanism => password_based,
                backend => built_in_database,
                enable => true,
                user_id_type => username,
                password_hash_algorithm => #{
                    name => plain,
                    salt_position => suffix
                }
            }
        ]
    );
create_authn(ChainName, redis) ->
    emqx_authentication:initialize_authentication(
        ChainName,
        [
            #{
                mechanism => password_based,
                backend => redis,
                enable => true,
                user_id_type => username,
                cmd => "HMGET mqtt_user:${username} password_hash salt is_superuser",
                password_hash_algorithm => #{
                    name => plain,
                    salt_position => suffix
                }
            }
        ]
    ).

create_authz(postgresql) ->
    emqx_authz:update(
        append,
        #{
            <<"type">> => <<"postgresql">>,
            <<"enable">> => true,
            <<"server">> => <<"127.0.0.1:27017">>,
            <<"pool_size">> => 1,
            <<"database">> => <<"mqtt">>,
            <<"username">> => <<"xx">>,
            <<"password">> => <<"ee">>,
            <<"auto_reconnect">> => true,
            <<"ssl">> => #{<<"enable">> => false},
            <<"query">> => <<"abcb">>
        }
    ).

assert_gateway_listener_shape(ListenerData, GatewayType) ->
    ?assertMatch(
        #{type := LType, authn := LAuthn} when
            is_atom(LType) andalso is_binary(LAuthn),
        ListenerData,
        #{gateway_type => GatewayType}
    ).

setup_fake_rule_engine_data() ->
    {ok, _} =
        emqx_rule_engine:create_rule(
            #{
                id => <<"rule:t_get_basic_usage_info:1">>,
                sql => <<"select 1 from topic">>,
                outputs =>
                    [
                        #{function => <<"erlang:hibernate">>, args => #{}},
                        #{function => console},
                        <<"http:my_http_bridge">>,
                        <<"http:my_http_bridge">>
                    ]
            }
        ),
    {ok, _} =
        emqx_rule_engine:create_rule(
            #{
                id => <<"rule:t_get_basic_usage_info:2">>,
                sql => <<"select 1 from topic">>,
                outputs =>
                    [
                        <<"mqtt:my_mqtt_bridge">>,
                        <<"http:my_http_bridge">>
                    ]
            }
        ),
    ok.

set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config([authorization, sources], []),
    ok;
set_special_configs(_App) ->
    ok.

start_slave(Name) ->
    % We want VMs to only occupy a single core
    CommonBeamOpts = "+S 1:1 ",
    {ok, Node} = slave:start_link(host(), Name, CommonBeamOpts ++ ebin_path()),
    Node.

%% for some unknown reason, gen_rpc running locally or in CI might
%% start with different `port_discovery' modes, which means that'll
%% either be listening at the port in the config (`tcp_server_port',
%% 5369) if `manual', else it'll listen on 5370 if started as
%% `stateless'.
find_gen_rpc_port() ->
    [EPort] = [
        EPort
     || {links, Ls} <- process_info(whereis(gen_rpc_server_tcp)),
        EPort <- Ls,
        is_port(EPort)
    ],
    {ok, {_, Port}} = inet:sockname(EPort),
    Port.

setup_slave(Node) ->
    TestNode = node(),
    Port = find_gen_rpc_port(),
    [ok = rpc:call(Node, application, load, [App]) || App <- [gen_rpc, emqx]],
    ok = rpc:call(
        Node,
        application,
        set_env,
        [gen_rpc, tcp_server_port, 9002]
    ),
    ok = rpc:call(
        Node,
        application,
        set_env,
        [gen_rpc, client_config_per_node, {internal, #{TestNode => Port}}]
    ),
    ok = rpc:call(
        Node,
        application,
        set_env,
        [gen_rpc, port_discovery, manual]
    ),
    Handler =
        fun
            (emqx) ->
                application:set_env(
                    emqx,
                    boot_modules,
                    []
                ),
                ekka:join(TestNode),
                ok;
            (_) ->
                ok
        end,
    ok = rpc:call(
        Node,
        emqx_common_test_helpers,
        start_apps,
        [
            [emqx_conf, emqx_modules],
            Handler
        ]
    ),
    ok.

stop_slave(Node) ->
    slave:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    Host.

ebin_path() ->
    string:join(["-pa" | paths()], " ").

paths() ->
    [
        Path
     || Path <- code:get_path(),
        string:prefix(Path, code:lib_dir()) =:= nomatch,
        string:str(Path, "_build/default/plugins") =:= 0
    ].
