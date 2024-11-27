%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(TYPE, <<"pulsar">>).
-define(APPS, [emqx_conf, emqx_resource, emqx_bridge, emqx_rule_engine, emqx_bridge_pulsar]).
-define(RULE_TOPIC, "pulsar/rule").
-define(RULE_TOPIC_BIN, <<?RULE_TOPIC>>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_cases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.

groups() ->
    emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()).

matrix_cases() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        lists:flatten([
            ?APPS,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ]),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(plain = Type, Config) ->
    PulsarHost = os:getenv("PULSAR_PLAIN_HOST", "toxiproxy"),
    PulsarPort = list_to_integer(os:getenv("PULSAR_PLAIN_PORT", "6652")),
    ProxyName = "pulsar_plain",
    reset_proxy(),
    case emqx_common_test_helpers:is_tcp_server_available(PulsarHost, PulsarPort) of
        true ->
            Config1 = common_init_per_group(),
            [
                {proxy_name, ProxyName},
                {pulsar_host, PulsarHost},
                {pulsar_port, PulsarPort},
                {pulsar_type, Type},
                {use_tls, false}
                | Config1 ++ Config
            ];
        false ->
            maybe_skip_without_ci()
    end;
init_per_group(tls = Type, Config) ->
    PulsarHost = os:getenv("PULSAR_TLS_HOST", "toxiproxy"),
    PulsarPort = list_to_integer(os:getenv("PULSAR_TLS_PORT", "6653")),
    ProxyName = "pulsar_tls",
    reset_proxy(),
    case emqx_common_test_helpers:is_tcp_server_available(PulsarHost, PulsarPort) of
        true ->
            Config1 = common_init_per_group(),
            [
                {proxy_name, ProxyName},
                {pulsar_host, PulsarHost},
                {pulsar_port, PulsarPort},
                {pulsar_type, Type},
                {use_tls, true}
                | Config1 ++ Config
            ];
        false ->
            maybe_skip_without_ci()
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when
    Group =:= plain;
    Group =:= tls
->
    common_end_per_group(Config),
    ok;
end_per_group(_Group, _Config) ->
    ok.

common_init_per_group() ->
    reset_proxy(),
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    MQTTTopic = <<"mqtt/topic/", UniqueNum/binary>>,
    [
        {proxy_host, ProxyHost},
        {proxy_port, ProxyPort},
        {mqtt_topic, MQTTTopic}
    ].

reset_proxy() ->
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok.

common_end_per_group(Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok.

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

end_per_testcase(_Testcase, Config) ->
    ok = emqx_config:delete_override_conf_files(),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    stop_consumer(Config),
    %% in CI, apparently this needs more time since the
    %% machines struggle with all the containers running...
    emqx_common_test_helpers:call_janitor(60_000),
    ok = snabbkaffe:stop(),
    flush_consumed(),
    ok.

common_init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(60)),
    emqx_bridge_v2_testlib:delete_all_bridges(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    PulsarTopic =
        <<
            (atom_to_binary(TestCase))/binary,
            UniqueNum/binary
        >>,
    Config1 = [{pulsar_topic, PulsarTopic} | Config0],
    ConsumerConfig = start_consumer(TestCase, Config1),
    Config = ConsumerConfig ++ Config1,
    ok = snabbkaffe:start_trace(),
    Config.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

create_connector(Config) ->
    {201, _} = create_connector_api([
        {connector_type, ?TYPE},
        {connector_name, ?MODULE},
        {connector_config, connector_config(Config)}
    ]),
    ok.

delete_connector(Name) ->
    ok = emqx_connector:remove(?TYPE, Name).

create_action(Name, Config) ->
    Action = action_config(Config),
    {ok, _} = emqx_bridge_v2:create(actions, ?TYPE, Name, Action).

delete_action(Name) ->
    ok = emqx_bridge_v2:remove(actions, ?TYPE, Name).

connector_config(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    PulsarPort = ?config(pulsar_port, Config),
    UseTLS = proplists:get_value(use_tls, Config, false),
    Name = atom_to_binary(?MODULE),
    Prefix =
        case UseTLS of
            true -> <<"pulsar+ssl://">>;
            false -> <<"pulsar://">>
        end,
    ServerURL = iolist_to_binary([
        Prefix,
        PulsarHost,
        ":",
        integer_to_binary(PulsarPort)
    ]),
    InnerConfigMap = #{
        <<"enable">> => true,
        <<"ssl">> => #{
            <<"enable">> => UseTLS,
            <<"verify">> => <<"verify_none">>,
            <<"server_name_indication">> => <<"auto">>
        },
        <<"authentication">> => <<"none">>,
        <<"servers">> => ServerURL
    },
    emqx_bridge_v2_testlib:parse_and_check_connector(?TYPE, Name, InnerConfigMap).

action_config(Config) ->
    action_config(atom_to_binary(?MODULE), Config).

action_config(ConnectorName, Config) ->
    QueryMode = proplists:get_value(query_mode, Config, <<"sync">>),
    InnerConfigMap = #{
        <<"connector">> => ConnectorName,
        <<"enable">> => true,
        <<"parameters">> => #{
            <<"retention_period">> => <<"infinity">>,
            <<"max_batch_bytes">> => <<"1MB">>,
            <<"batch_size">> => 100,
            <<"strategy">> => <<"random">>,
            <<"buffer">> => #{
                <<"mode">> => <<"memory">>,
                <<"per_partition_limit">> => <<"10MB">>,
                <<"segment_bytes">> => <<"5MB">>,
                <<"memory_overload_protection">> => true
            },
            <<"message">> => #{
                <<"key">> => <<"${.clientid}">>,
                <<"value">> => <<"${.}">>
            },
            <<"pulsar_topic">> => ?config(pulsar_topic, Config)
        },
        <<"resource_opts">> => #{
            <<"query_mode">> => QueryMode,
            <<"request_ttl">> => <<"1s">>,
            <<"health_check_interval">> => <<"1s">>,
            <<"metrics_flush_interval">> => <<"300ms">>
        }
    },
    emqx_bridge_v2_testlib:parse_and_check(action, ?TYPE, <<"some_action">>, InnerConfigMap).

instance_id(Type, Name) ->
    ConnectorId = emqx_bridge_resource:resource_id(Type, ?TYPE, Name),
    BridgeId = emqx_bridge_resource:bridge_id(?TYPE, Name),
    TypeBin =
        case Type of
            sources -> <<"source:">>;
            actions -> <<"action:">>
        end,
    <<TypeBin/binary, BridgeId/binary, ":", ConnectorId/binary>>.

start_consumer(TestCase, Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    PulsarPort = ?config(pulsar_port, Config),
    PulsarTopic = ?config(pulsar_topic, Config),
    UseTLS = ?config(use_tls, Config),
    Scheme =
        case UseTLS of
            true -> <<"pulsar+ssl://">>;
            false -> <<"pulsar://">>
        end,
    URL =
        binary_to_list(
            <<Scheme/binary, (list_to_binary(PulsarHost))/binary, ":",
                (integer_to_binary(PulsarPort))/binary>>
        ),
    ConsumerClientId = list_to_atom(
        atom_to_list(TestCase) ++ integer_to_list(erlang:unique_integer())
    ),
    CertsPath = emqx_common_test_helpers:deps_path(emqx, "etc/certs"),
    SSLOpts = #{
        enable => UseTLS,
        keyfile => filename:join([CertsPath, "key.pem"]),
        certfile => filename:join([CertsPath, "cert.pem"]),
        cacertfile => filename:join([CertsPath, "cacert.pem"])
    },
    Opts = #{enable_ssl => UseTLS, ssl_opts => emqx_tls_lib:to_client_opts(SSLOpts)},
    {ok, _} = pulsar:ensure_supervised_client(ConsumerClientId, [URL], Opts),
    ConsumerOpts = Opts#{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => atom_to_list(TestCase) ++ integer_to_list(erlang:unique_integer()),
        max_consumer_num => 1,
        %% Note!  This must not coincide with the client
        %% id, or else weird bugs will happen, like the
        %% consumer never starts...
        name => list_to_atom("test_consumer" ++ integer_to_list(erlang:unique_integer())),
        consumer_id => 1
    },
    {ok, Consumer} = pulsar:ensure_supervised_consumers(
        ConsumerClientId,
        PulsarTopic,
        ConsumerOpts
    ),
    %% since connection is async, and there's currently no way to
    %% specify the subscription initial position as `Earliest', we
    %% need to wait until the consumer is connected to avoid
    %% flakiness.
    ok = wait_until_consumer_connected(Consumer),
    [
        {consumer_client_id, ConsumerClientId},
        {pulsar_consumer, Consumer}
    ].

stop_consumer(Config) ->
    ConsumerClientId = ?config(consumer_client_id, Config),
    Consumer = ?config(pulsar_consumer, Config),
    ok = pulsar:stop_and_delete_supervised_consumers(Consumer),
    ok = pulsar:stop_and_delete_supervised_client(ConsumerClientId),
    ok.

wait_until_consumer_connected(Consumer) ->
    ?retry(
        _Sleep = 300,
        _Attempts0 = 20,
        true = pulsar_consumers:all_connected(Consumer)
    ),
    ok.

wait_until_producer_connected() ->
    wait_until_connected(pulsar_producers_sup, pulsar_producer).

wait_until_connected(SupMod, Mod) ->
    Pids = get_pids(SupMod, Mod),
    ?retry(
        _Sleep = 300,
        _Attempts0 = 20,
        begin
            true = length(Pids) > 0,
            lists:foreach(fun(P) -> {connected, _} = sys:get_state(P) end, Pids)
        end
    ),
    ok.

get_pulsar_producers() ->
    get_pids(pulsar_producers_sup, pulsar_producer).

get_pids(SupMod, Mod) ->
    [
        P
     || {_Name, SupPid, _Type, _Mods} <- supervisor:which_children(SupMod),
        P <- element(2, process_info(SupPid, links)),
        case proc_lib:initial_call(P) of
            {Mod, init, _} -> true;
            _ -> false
        end
    ].

receive_consumed(Timeout) ->
    receive
        {pulsar_message, #{payloads := Payloads}} ->
            lists:map(fun try_decode_json/1, Payloads)
    after Timeout ->
        ct:pal("mailbox: ~p", [process_info(self(), messages)]),
        ct:fail("no message consumed")
    end.

flush_consumed() ->
    receive
        {pulsar_message, _} -> flush_consumed()
    after 0 -> ok
    end.

try_decode_json(Payload) ->
    case emqx_utils_json:safe_decode(Payload, [return_maps]) of
        {error, _} ->
            Payload;
        {ok, JSON} ->
            JSON
    end.

payload() ->
    #{<<"key">> => 42, <<"data">> => <<"pulsar">>, <<"timestamp">> => 10000}.

maybe_skip_without_ci() ->
    case os:getenv("IS_CI") of
        "yes" ->
            throw(no_pulsar);
        _ ->
            {skip, no_pulsar}
    end.

assert_status_api(Line, Type, Name, Status) ->
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, #{
                <<"status">> := Status,
                <<"node_status">> := [#{<<"status">> := Status}]
            }}},
        emqx_bridge_v2_testlib:get_bridge_api(Type, Name),
        #{line => Line, name => Name, expected_status => Status}
    ).
-define(assertStatusAPI(TYPE, NAME, STATUS), assert_status_api(?LINE, TYPE, NAME, STATUS)).

proplists_with(Keys, PList) ->
    lists:filter(fun({K, _}) -> lists:member(K, Keys) end, PList).

group_path(Config) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] ->
            undefined;
        Path ->
            Path
    end.

create_connector_api(Config) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config)
    ).

create_action_api(Config) ->
    create_action_api(Config, _Overrides = #{}).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_kind_api(Config, Overrides)
    ).

update_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_bridge_api(Config, Overrides)
    ).

get_combined_metrics(ActionResId, RuleId) ->
    Metrics = emqx_resource:get_metrics(ActionResId),
    RuleMetrics = emqx_metrics_worker:get_counters(rule_metrics, RuleId),
    Metrics#{rule => RuleMetrics}.

reset_combined_metrics(ActionResId, RuleId) ->
    #{
        kind := action,
        type := Type,
        name := Name
    } = emqx_bridge_v2:parse_id(ActionResId),
    ok = emqx_bridge_v2:reset_metrics(actions, Type, Name),
    ok = emqx_rule_engine:reset_metrics_for_rule(RuleId),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_action_probe(matrix) ->
    [[plain], [tls]];
t_action_probe(Config) when is_list(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_connector(Config),
    Action = action_config(Config),
    {ok, Res0} = emqx_bridge_v2_testlib:probe_bridge_api(action, ?TYPE, Name, Action),
    ?assertMatch({{_, 204, _}, _, _}, Res0),
    ok.

t_action(matrix) ->
    [
        [plain, async],
        [plain, sync],
        [tls, async]
    ];
t_action(Config) when is_list(Config) ->
    QueryMode =
        case group_path(Config) of
            [_, QM | _] -> atom_to_binary(QM);
            _ -> <<"async">>
        end,
    Name = atom_to_binary(?FUNCTION_NAME),
    create_connector(Config),
    create_action(Name, [{query_mode, QueryMode} | Config]),
    Actions = emqx_bridge_v2:list(actions),
    Any = fun(#{name := BName}) -> BName =:= Name end,
    ?assert(lists:any(Any, Actions), Actions),
    Topic = <<"lkadfdaction">>,
    {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
        #{
            sql => <<"select * from \"", Topic/binary, "\"">>,
            id => atom_to_binary(?FUNCTION_NAME),
            actions => [<<"pulsar:", Name/binary>>],
            description => <<"bridge_v2 send msg to pulsar action">>
        }
    ),
    on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),
    MQTTClientID = <<"pulsar_mqtt_clientid">>,
    {ok, C1} = emqtt:start_link([{clean_start, true}, {clientid, MQTTClientID}]),
    {ok, _} = emqtt:connect(C1),
    ReqPayload = payload(),
    ReqPayloadBin = emqx_utils_json:encode(ReqPayload),
    {ok, _} = emqtt:publish(C1, Topic, #{}, ReqPayloadBin, [{qos, 1}, {retain, false}]),
    [#{<<"clientid">> := ClientID, <<"payload">> := RespPayload}] = receive_consumed(5000),
    ?assertEqual(MQTTClientID, ClientID),
    ?assertEqual(ReqPayload, emqx_utils_json:decode(RespPayload)),
    ok = emqtt:disconnect(C1),
    InstanceId = instance_id(actions, Name),
    ?retry(
        100,
        20,
        ?assertMatch(
            #{
                counters := #{
                    dropped := 0,
                    success := 1,
                    matched := 1,
                    failed := 0,
                    received := 0
                }
            },
            emqx_resource:get_metrics(InstanceId)
        )
    ),
    ok = delete_action(Name),
    ActionsAfterDelete = emqx_bridge_v2:list(actions),
    ?assertNot(lists:any(Any, ActionsAfterDelete), ActionsAfterDelete),
    ok.

%% Tests that deleting/disabling an action that share the same Pulsar topic with other
%% actions do not disturb the latter.
t_multiple_actions_sharing_topic(matrix) ->
    [[plain], [tls]];
t_multiple_actions_sharing_topic(Config) when is_list(Config) ->
    Type = ?TYPE,
    ConnectorName = <<"c">>,
    ConnectorConfig = connector_config(Config),
    ActionConfig = action_config(ConnectorName, Config),
    ?check_trace(
        begin
            ConnectorParams = [
                {connector_config, ConnectorConfig},
                {connector_name, ConnectorName},
                {connector_type, Type}
            ],
            ActionName1 = <<"a1">>,
            ActionParams1 = [
                {action_config, ActionConfig},
                {action_name, ActionName1},
                {action_type, Type}
            ],
            ActionName2 = <<"a2">>,
            ActionParams2 = [
                {action_config, ActionConfig},
                {action_name, ActionName2},
                {action_type, Type}
            ],
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_connector_api(ConnectorParams),
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_action_api(ActionParams1),
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_action_api(ActionParams2),

            ?assertStatusAPI(Type, ActionName1, <<"connected">>),
            ?assertStatusAPI(Type, ActionName2, <<"connected">>),

            RuleTopic = <<"t/a2">>,
            {ok, _} = emqx_bridge_v2_testlib:create_rule_and_action_http(Type, RuleTopic, [
                {bridge_name, ActionName2}
            ]),
            {ok, C} = emqtt:start_link([]),
            {ok, _} = emqtt:connect(C),
            SendMessage = fun() ->
                ReqPayload = payload(),
                ReqPayloadBin = emqx_utils_json:encode(ReqPayload),
                {ok, _} = emqtt:publish(C, RuleTopic, #{}, ReqPayloadBin, [
                    {qos, 1}, {retain, false}
                ]),
                ok
            end,

            %% Disabling a1 shouldn't disturb a2.
            ?assertMatch(
                {204, _}, emqx_bridge_v2_testlib:disable_kind_api(action, Type, ActionName1)
            ),

            ?assertStatusAPI(Type, ActionName1, <<"disconnected">>),
            ?assertStatusAPI(Type, ActionName2, <<"connected">>),

            ?assertMatch(ok, SendMessage()),
            ?assertStatusAPI(Type, ActionName2, <<"connected">>),

            ?assertMatch(
                {204, _},
                emqx_bridge_v2_testlib:enable_kind_api(action, Type, ActionName1)
            ),
            ?assertStatusAPI(Type, ActionName1, <<"connected">>),
            ?assertStatusAPI(Type, ActionName2, <<"connected">>),
            ?assertMatch(ok, SendMessage()),

            %% Deleting also shouldn't disrupt a2.
            ?assertMatch(
                {204, _},
                emqx_bridge_v2_testlib:delete_kind_api(action, Type, ActionName1)
            ),
            ?assertStatusAPI(Type, ActionName2, <<"connected">>),
            ?assertMatch(ok, SendMessage()),

            ok
        end,
        []
    ),
    ok.

t_sync_query_down(matrix) ->
    [[plain]];
t_sync_query_down(Config0) when is_list(Config0) ->
    ct:timetrap({seconds, 15}),
    Payload = #{<<"x">> => <<"some data">>},
    PayloadBin = emqx_utils_json:encode(Payload),
    ClientId = <<"some_client">>,
    Opts = #{
        make_message_fn => fun(Topic) -> emqx_message:make(ClientId, Topic, PayloadBin) end,
        enter_tp_filter =>
            ?match_event(#{?snk_kind := "pulsar_producer_send"}),
        error_tp_filter =>
            ?match_event(#{?snk_kind := "resource_simple_sync_internal_buffer_query_timeout"}),
        success_tp_filter =>
            ?match_event(#{?snk_kind := pulsar_echo_consumer_message})
    },
    ConnectorName = atom_to_binary(?FUNCTION_NAME),
    Config = [
        {connector_type, ?TYPE},
        {connector_name, ConnectorName},
        {connector_config, connector_config(Config0)},
        {action_type, ?TYPE},
        {action_name, ?FUNCTION_NAME},
        {action_config, action_config(ConnectorName, Config0)}
        | proplists_with([proxy_name, proxy_host, proxy_port], Config0)
    ],
    emqx_bridge_v2_testlib:t_sync_query_down(Config, Opts),
    ok.

%% Checks that we correctly handle telemetry events emitted by pulsar.
t_telemetry_metrics(matrix) ->
    [[plain]];
t_telemetry_metrics(Config) when is_list(Config) ->
    ProxyName = ?config(proxy_name, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    Type = ?TYPE,
    ConnectorName = <<"c">>,
    ConnectorConfig = connector_config(Config),
    ActionConfig = action_config(ConnectorName, Config),
    ConnectorParams = [
        {connector_config, ConnectorConfig},
        {connector_name, ConnectorName},
        {connector_type, Type}
    ],
    ActionName1 = <<"a1">>,
    ActionParams1 = [
        {action_config, ActionConfig},
        {action_name, ActionName1},
        {action_type, Type}
    ],
    ActionName2 = <<"a2">>,
    ActionParams2 = [
        {action_config, ActionConfig},
        {action_name, ActionName2},
        {action_type, Type}
    ],
    ?check_trace(
        begin
            {201, _} =
                create_connector_api(ConnectorParams),
            {201, _} =
                create_action_api(
                    ActionParams1,
                    %% Initially, this will overflow on small messages
                    #{
                        <<"parameters">> => #{
                            <<"buffer">> => #{
                                <<"mode">> => <<"disk">>,
                                <<"per_partition_limit">> => <<"2B">>,
                                <<"segment_bytes">> => <<"1B">>
                            }
                        }
                    }
                ),
            {201, _} =
                create_action_api(ActionParams2),
            RuleTopic = <<"t/a2">>,
            {ok, #{<<"id">> := RuleId}} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(Type, RuleTopic, [
                    {bridge_name, ActionName1}
                ]),
            {ok, C} = emqtt:start_link([]),
            {ok, _} = emqtt:connect(C),
            SendMessage = fun() ->
                ReqPayload = payload(),
                ReqPayloadBin = emqx_utils_json:encode(ReqPayload),
                {ok, _} = emqtt:publish(C, RuleTopic, #{}, ReqPayloadBin, [
                    {qos, 1}, {retain, false}
                ]),
                ok
            end,
            SendMessage(),
            ActionResId1 = emqx_bridge_v2_testlib:bridge_id(ActionParams1),
            ActionResId2 = emqx_bridge_v2_testlib:bridge_id(ActionParams2),
            ?retry(
                100,
                10,
                ?assertMatch(
                    #{
                        counters := #{
                            'dropped.queue_full' := 1,
                            'dropped.expired' := 0,
                            success := 0,
                            matched := 1,
                            failed := 0,
                            received := 0
                        },
                        gauges := #{
                            inflight := 0,
                            queuing := 0,
                            queuing_bytes := 0
                        },
                        rule := #{
                            matched := 1,
                            %% todo: bump action failure count when dropped to mimic common
                            %% buffer worker behavior.
                            'actions.failed' := 0,
                            'actions.failed.unknown' := 0,
                            'actions.success' := 0
                        }
                    },
                    get_combined_metrics(ActionResId1, RuleId)
                )
            ),
            reset_combined_metrics(ActionResId1, RuleId),
            %% Now to make it drop expired messages
            {200, _} =
                update_action_api(ActionParams1, #{
                    <<"parameters">> => #{
                        <<"retention_period">> => <<"10ms">>
                    }
                }),
            emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
                SendMessage(),
                ?retry(
                    100,
                    10,
                    ?assertMatch(
                        #{
                            counters := #{
                                'dropped.queue_full' := 0,
                                'dropped.expired' := 0,
                                success := 0,
                                matched := 1,
                                failed := 0,
                                received := 0
                            },
                            gauges := #{
                                inflight := 0,
                                queuing := 1,
                                queuing_bytes := QueuingBytes1
                            }
                        } when QueuingBytes1 > 0,
                        get_combined_metrics(ActionResId1, RuleId)
                    )
                ),
                %% Other action is not affected by telemetry events for first action.
                ?assertMatch(
                    #{
                        counters := #{
                            'dropped.queue_full' := 0,
                            'dropped.expired' := 0,
                            success := 0,
                            matched := 0,
                            failed := 0,
                            received := 0
                        },
                        gauges := #{
                            inflight := 0,
                            queuing := 0,
                            queuing_bytes := 0
                        }
                    },
                    emqx_resource:get_metrics(ActionResId2)
                ),
                ct:sleep(20),
                ok
            end),
            %% After connection is restored, the request is already expired
            ?retry(
                500,
                20,
                ?assertMatch(
                    #{
                        counters := #{
                            'dropped.queue_full' := 0,
                            'dropped.expired' := 1,
                            success := 0,
                            matched := 1,
                            failed := 0,
                            received := 0
                        },
                        gauges := #{
                            inflight := 0,
                            queuing := 0,
                            queuing_bytes := 0
                        },
                        rule := #{
                            matched := 1,
                            %% todo: bump action failure count when dropped to mimic common
                            %% buffer worker behavior.
                            'actions.failed' := 0,
                            'actions.failed.unknown' := 0,
                            'actions.success' := 0
                        }
                    },
                    get_combined_metrics(ActionResId1, RuleId)
                )
            ),
            reset_combined_metrics(ActionResId1, RuleId),

            %% Now, a success.
            SendMessage(),
            ?retry(
                500,
                20,
                ?assertMatch(
                    #{
                        counters := #{
                            'dropped.queue_full' := 0,
                            'dropped.expired' := 0,
                            success := 1,
                            matched := 1,
                            failed := 0,
                            received := 0
                        },
                        gauges := #{
                            inflight := 0,
                            queuing := 0,
                            queuing_bytes := 0
                        },
                        rule := #{
                            matched := 1,
                            'actions.failed' := 0,
                            'actions.failed.unknown' := 0,
                            'actions.success' := 1
                        }
                    },
                    get_combined_metrics(ActionResId1, RuleId)
                )
            ),

            %% Other action is not affected by telemetry events for first action.
            ?retry(
                100,
                10,
                ?assertMatch(
                    #{
                        counters := #{
                            'dropped.queue_full' := 0,
                            'dropped.expired' := 0,
                            success := 0,
                            matched := 0,
                            failed := 0,
                            received := 0
                        },
                        gauges := #{
                            inflight := 0,
                            queuing := 0,
                            queuing_bytes := 0
                        }
                    },
                    emqx_resource:get_metrics(ActionResId2)
                )
            ),

            ok
        end,
        []
    ),
    ok.
