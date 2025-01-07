%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(BRIDGE_TYPE_BIN, <<"pulsar_producer">>).
-define(APPS, [emqx_conf, emqx_resource, emqx_bridge, emqx_rule_engine, emqx_bridge_pulsar]).
-define(RULE_TOPIC, "mqtt/rule").
-define(RULE_TOPIC_BIN, <<?RULE_TOPIC>>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, plain},
        {group, tls}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    OnlyOnceTCs = only_once_tests(),
    TCs = AllTCs -- OnlyOnceTCs,
    [
        {plain, AllTCs},
        {tls, TCs}
    ].

only_once_tests() ->
    [
        t_create_via_http,
        t_strategy_key_validation,
        t_start_when_down,
        t_send_when_down,
        t_send_when_timeout,
        t_failure_to_start_producer,
        t_producer_process_crash,
        t_resilience,
        t_resource_manager_crash_after_producers_started,
        t_resource_manager_crash_before_producers_started
    ].

init_per_suite(Config) ->
    %% Ensure enterprise bridge module is loaded
    _ = emqx_bridge_enterprise:module_info(),
    %% TODO
    %% This is needed to ensure that filenames generated deep inside pulsar/replayq
    %% will not exceed 256 characters, because replayq eventually turns them into atoms.
    %% The downside is increased risk of accidental name clashes / testsuite interference.
    {ok, Cwd} = file:get_cwd(),
    PrivDir = ?config(priv_dir, Config),
    WorkDir = emqx_utils_fs:find_relpath(filename:join(PrivDir, "ebp"), Cwd),
    Apps = emqx_cth_suite:start(
        lists:flatten([
            ?APPS,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ]),
        #{work_dir => WorkDir}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(plain = Type, Config) ->
    PulsarHost = os:getenv("PULSAR_PLAIN_HOST", "toxiproxy"),
    PulsarPort = list_to_integer(os:getenv("PULSAR_PLAIN_PORT", "6652")),
    ProxyName = "pulsar_plain",
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
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_pulsar);
                _ ->
                    {skip, no_pulsar}
            end
    end;
init_per_group(tls = Type, Config) ->
    PulsarHost = os:getenv("PULSAR_TLS_HOST", "toxiproxy"),
    PulsarPort = list_to_integer(os:getenv("PULSAR_TLS_PORT", "6653")),
    ProxyName = "pulsar_tls",
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
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_pulsar);
                _ ->
                    {skip, no_pulsar}
            end
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
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    MQTTTopic = <<"mqtt/topic/", UniqueNum/binary>>,
    [
        {proxy_host, ProxyHost},
        {proxy_port, ProxyPort},
        {mqtt_topic, MQTTTopic}
    ].

common_end_per_group(Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    delete_all_bridges(),
    ok.

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

end_per_testcase(_Testcase, Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            ok = emqx_config:delete_override_conf_files(),
            ProxyHost = ?config(proxy_host, Config),
            ProxyPort = ?config(proxy_port, Config),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            delete_all_bridges(),
            stop_consumer(Config),
            %% in CI, apparently this needs more time since the
            %% machines struggle with all the containers running...
            emqx_common_test_helpers:call_janitor(60_000),
            ok = snabbkaffe:stop(),
            flush_consumed(),
            ok
    end.

common_init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(60)),
    delete_all_bridges(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    PulsarTopic =
        <<
            (atom_to_binary(TestCase))/binary,
            UniqueNum/binary
        >>,
    PulsarType = ?config(pulsar_type, Config0),
    Config1 = [{pulsar_topic, PulsarTopic} | Config0],
    {Name, ConfigString, PulsarConfig} = pulsar_config(
        TestCase, PulsarType, Config1
    ),
    ConsumerConfig = start_consumer(TestCase, Config1),
    Config = ConsumerConfig ++ Config1,
    ok = snabbkaffe:start_trace(),
    [
        {pulsar_name, Name},
        {pulsar_config_string, ConfigString},
        {pulsar_config, PulsarConfig}
        | Config
    ].

delete_all_bridges() ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

pulsar_config(TestCase, _PulsarType, Config) ->
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    PulsarHost = ?config(pulsar_host, Config),
    PulsarPort = ?config(pulsar_port, Config),
    PulsarTopic = ?config(pulsar_topic, Config),
    AuthType = proplists:get_value(sasl_auth_mechanism, Config, none),
    UseTLS = proplists:get_value(use_tls, Config, false),
    Name = atom_to_binary(TestCase),
    MQTTTopic = proplists:get_value(mqtt_topic, Config, <<"mqtt/topic/", UniqueNum/binary>>),
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
    ConfigString =
        io_lib:format(
            "bridges.pulsar_producer.~s {\n"
            "  enable = true\n"
            "  servers = \"~s\"\n"
            "  sync_timeout = 5s\n"
            "  compression = no_compression\n"
            "  send_buffer = 1MB\n"
            "  retention_period = infinity\n"
            "  max_batch_bytes = 900KB\n"
            "  batch_size = 1\n"
            "  strategy = random\n"
            "  buffer {\n"
            "    mode = memory\n"
            "    per_partition_limit = 10MB\n"
            "    segment_bytes = 5MB\n"
            "    memory_overload_protection = true\n"
            "  }\n"
            "  message {\n"
            "    key = \"${.clientid}\"\n"
            "    value = \"${.}\"\n"
            "  }\n"
            "~s"
            "  ssl {\n"
            "    enable = ~p\n"
            "    verify = verify_none\n"
            "    server_name_indication = \"auto\"\n"
            "  }\n"
            "  pulsar_topic = \"~s\"\n"
            "  local_topic = \"~s\"\n"
            "}\n",
            [
                Name,
                ServerURL,
                authentication(AuthType),
                UseTLS,
                PulsarTopic,
                MQTTTopic
            ]
        ),
    {Name, ConfigString, parse_and_check(ConfigString, Name)}.

parse_and_check(ConfigString, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    TypeBin = ?BRIDGE_TYPE_BIN,
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{TypeBin := #{Name := Config}}} = RawConf,
    Config.

authentication(_) ->
    "  authentication = none\n".

resource_id(Config) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(pulsar_name, Config),
    emqx_bridge_resource:resource_id(Type, Name).

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(pulsar_name, Config),
    PulsarConfig0 = ?config(pulsar_config, Config),
    PulsarConfig = emqx_utils_maps:deep_merge(PulsarConfig0, Overrides),
    emqx_bridge:create(Type, Name, PulsarConfig).

delete_bridge(Config) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(pulsar_name, Config),
    emqx_bridge:remove(Type, Name).

create_bridge_api(Config) ->
    create_bridge_api(Config, _Overrides = #{}).

create_bridge_api(Config, Overrides) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(pulsar_name, Config),
    PulsarConfig0 = ?config(pulsar_config, Config),
    PulsarConfig = emqx_utils_maps:deep_merge(PulsarConfig0, Overrides),
    Params = PulsarConfig#{<<"type">> => TypeBin, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("creating bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params, Opts) of
            {ok, {Status, Headers, Body0}} ->
                {ok, {Status, Headers, emqx_utils_json:decode(Body0, [return_maps])}};
            {error, {Status, Headers, Body0}} ->
                {error, {Status, Headers, emqx_bridge_testlib:try_decode_error(Body0)}};
            Error ->
                Error
        end,
    ct:pal("bridge create result: ~p", [Res]),
    Res.

update_bridge_api(Config) ->
    update_bridge_api(Config, _Overrides = #{}).

update_bridge_api(Config, Overrides) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(pulsar_name, Config),
    PulsarConfig0 = ?config(pulsar_config, Config),
    PulsarConfig = emqx_utils_maps:deep_merge(PulsarConfig0, Overrides),
    BridgeId = emqx_bridge_resource:bridge_id(TypeBin, Name),
    Params = PulsarConfig#{<<"type">> => TypeBin, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("updating bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(put, Path, "", AuthHeader, Params, Opts) of
            {ok, {_Status, _Headers, Body0}} -> {ok, emqx_utils_json:decode(Body0, [return_maps])};
            Error -> Error
        end,
    ct:pal("bridge update result: ~p", [Res]),
    Res.

probe_bridge_api(Config) ->
    probe_bridge_api(Config, _Overrides = #{}).

probe_bridge_api(Config, Overrides) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(pulsar_name, Config),
    PulsarConfig = ?config(pulsar_config, Config),
    Params0 = PulsarConfig#{<<"type">> => TypeBin, <<"name">> => Name},
    Params = maps:merge(Params0, Overrides),
    Path = emqx_mgmt_api_test_util:api_path(["bridges_probe"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("probing bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params, Opts) of
            {ok, {{_, 204, _}, _Headers, _Body0} = Res0} ->
                {ok, Res0};
            {error, {Status, Headers, Body0}} ->
                {error, {Status, Headers, emqx_bridge_testlib:try_decode_error(Body0)}};
            Error ->
                Error
        end,
    ct:pal("bridge probe result: ~p", [Res]),
    Res.

start_consumer(TestCase, Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    PulsarPort = ?config(pulsar_port, Config),
    PulsarTopic = ?config(pulsar_topic, Config),
    UseTLS = ?config(use_tls, Config),
    %% FIXME: patch pulsar to accept binary urls...
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

create_rule_and_action_http(Config) ->
    PulsarName = ?config(pulsar_name, Config),
    BridgeId = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_BIN, PulsarName),
    Params = #{
        enable => true,
        sql => <<"SELECT * FROM \"", ?RULE_TOPIC, "\"">>,
        actions => [BridgeId]
    },
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    ct:pal("rule action params: ~p", [Params]),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

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

cluster(Config) ->
    Apps = [
        {emqx, #{override_env => [{boot_modules, [broker]}]}}
        | ?APPS
    ],
    Nodes = emqx_cth_cluster:start(
        [
            {emqx_bridge_pulsar_impl_producer1, #{apps => Apps}},
            {emqx_bridge_pulsar_impl_producer2, #{apps => Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    Nodes.

kill_resource_managers() ->
    ct:pal("gonna kill resource managers"),
    lists:foreach(
        fun({_, Pid, _, _}) ->
            ct:pal("terminating resource manager ~p", [Pid]),
            Ref = monitor(process, Pid),
            exit(Pid, kill),
            receive
                {'DOWN', Ref, process, Pid, killed} ->
                    ok
            after 500 ->
                ct:fail("pid ~p didn't die!", [Pid])
            end,
            ok
        end,
        supervisor:which_children(emqx_resource_manager_sup)
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_and_produce_ok(Config) ->
    MQTTTopic = ?config(mqtt_topic, Config),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    QoS = 0,
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    ?check_trace(
        begin
            ?assertMatch(
                {ok, _},
                create_bridge(Config)
            ),
            {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
            on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
            %% Publish using local topic.
            Message0 = emqx_message:make(ClientId, QoS, MQTTTopic, Payload),
            emqx:publish(Message0),
            %% Publish using rule engine.
            Message1 = emqx_message:make(ClientId, QoS, ?RULE_TOPIC_BIN, Payload),
            emqx:publish(Message1),

            #{rule_id => RuleId}
        end,
        fun(#{rule_id := RuleId}, _Trace) ->
            Data0 = receive_consumed(5_000),
            ?assertMatch(
                [
                    #{
                        <<"clientid">> := ClientId,
                        <<"event">> := <<"message.publish">>,
                        <<"payload">> := Payload,
                        <<"topic">> := MQTTTopic
                    }
                ],
                Data0
            ),
            Data1 = receive_consumed(5_000),
            ?assertMatch(
                [
                    #{
                        <<"clientid">> := ClientId,
                        <<"event">> := <<"message.publish">>,
                        <<"payload">> := Payload,
                        <<"topic">> := ?RULE_TOPIC_BIN
                    }
                ],
                Data1
            ),
            ?retry(
                _Sleep = 100,
                _Attempts0 = 20,
                begin
                    Id = get_channel_id(Config),
                    ?assertMatch(
                        #{
                            counters := #{
                                dropped := 0,
                                failed := 0,
                                late_reply := 0,
                                matched := 2,
                                received := 0,
                                retried := 0,
                                success := 2
                            }
                        },
                        emqx_resource:get_metrics(Id)
                    ),
                    ?assertEqual(
                        1, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.success')
                    ),
                    ?assertEqual(
                        0, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.failed')
                    ),
                    ok
                end
            ),
            ok
        end
    ),
    ok.

get_channel_id(Config) ->
    BridgeId = emqx_bridge_resource:bridge_id(
        <<"pulsar">>, ?config(pulsar_name, Config)
    ),
    ConnectorId = emqx_bridge_resource:resource_id(
        <<"pulsar">>, ?config(pulsar_name, Config)
    ),
    <<"action:", BridgeId/binary, ":", ConnectorId/binary>>.

%% Under normal operations, the bridge will be called async via
%% `simple_async_query'.
t_sync_query(Config) ->
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ResourceId = resource_id(Config),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            BridgeId = emqx_bridge_resource:bridge_id(<<"pulsar">>, ?config(pulsar_name, Config)),
            ConnectorId = emqx_bridge_resource:resource_id(
                <<"pulsar">>, ?config(pulsar_name, Config)
            ),
            Id = <<"action:", BridgeId/binary, ":", ConnectorId/binary>>,
            Message = {Id, #{payload => Payload}},
            ?assertMatch(
                {ok, #{sequence_id := _}}, emqx_resource:simple_sync_query(ResourceId, Message)
            ),
            ok
        end,
        []
    ),
    ok.

t_create_via_http(Config) ->
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),

            %% lightweight matrix testing some configs
            ?assertMatch(
                {ok, _},
                update_bridge_api(
                    Config,
                    #{
                        <<"buffer">> =>
                            #{<<"mode">> => <<"disk">>}
                    }
                )
            ),
            ?assertMatch(
                {ok, _},
                update_bridge_api(
                    Config,
                    #{
                        <<"buffer">> =>
                            #{
                                <<"mode">> => <<"hybrid">>,
                                <<"memory_overload_protection">> => true
                            }
                    }
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_start_stop(Config) ->
    PulsarName = ?config(pulsar_name, Config),
    ?check_trace(
        begin
            ?assertMatch(
                {ok, _},
                create_bridge(Config)
            ),
            ResourceId = resource_id(Config),
            %% Since the connection process is async, we give it some time to
            %% stabilize and avoid flakiness.
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),

            %% Check that the bridge probe API doesn't leak atoms.
            ProbeRes0 = probe_bridge_api(
                Config,
                #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}}
            ),
            ?assertMatch({ok, {{_, 204, _}, _Headers, _Body}}, ProbeRes0),
            AtomsBefore = erlang:system_info(atom_count),
            %% Probe again; shouldn't have created more atoms.
            ProbeRes1 = probe_bridge_api(
                Config,
                #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}}
            ),
            ?assertMatch({ok, {{_, 204, _}, _Headers, _Body}}, ProbeRes1),
            AtomsAfter = erlang:system_info(atom_count),
            ?assertEqual(AtomsBefore, AtomsAfter),

            %% Now stop the bridge.
            ?assertMatch(
                {{ok, _}, {ok, _}},
                ?wait_async_action(
                    emqx_bridge:disable_enable(disable, ?BRIDGE_TYPE_BIN, PulsarName),
                    #{?snk_kind := pulsar_bridge_stopped},
                    5_000
                )
            ),

            ok
        end,
        fun(Trace) ->
            %% one for each probe, one for real
            ?assertMatch([_, _, _], ?of_kind(pulsar_bridge_producer_stopped, Trace)),
            ?assertMatch([_, _, _], ?of_kind(pulsar_bridge_client_stopped, Trace)),
            ?assertMatch([_, _, _], ?of_kind(pulsar_bridge_stopped, Trace)),
            ok
        end
    ),
    ok.

t_on_get_status(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    ResourceId = resource_id(Config),
    %% Since the connection process is async, we give it some time to
    %% stabilize and avoid flakiness.
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ct:sleep(500),
        ?retry(
            _Sleep = 1_000,
            _Attempts = 20,
            ?assertEqual({ok, connecting}, emqx_resource_manager:health_check(ResourceId))
        )
    end),
    %% Check that it recovers itself.
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    ok.

t_start_when_down(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    ?check_trace(
        begin
            emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
                ?assertMatch(
                    {ok, _},
                    create_bridge(Config)
                ),
                ok
            end),
            ResourceId = resource_id(Config),
            %% Should recover given enough time.
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            ok
        end,
        []
    ),
    ok.

t_send_when_down(Config) ->
    do_t_send_with_failure(Config, down).

t_send_when_timeout(Config) ->
    do_t_send_with_failure(Config, timeout).

do_t_send_with_failure(Config, FailureType) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    MQTTTopic = ?config(mqtt_topic, Config),
    QoS = 0,
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    Message0 = emqx_message:make(ClientId, QoS, MQTTTopic, Payload),

    {{ok, _}, {ok, _}} =
        ?wait_async_action(
            create_bridge(Config),
            #{?snk_kind := pulsar_producer_bridge_started},
            10_000
        ),
    ?check_trace(
        begin
            emqx_common_test_helpers:with_failure(
                FailureType, ProxyName, ProxyHost, ProxyPort, fun() ->
                    {_, {ok, _}} =
                        ?wait_async_action(
                            emqx:publish(Message0),
                            #{
                                ?snk_kind := "pulsar_producer_query_enter",
                                mode := async,
                                ?snk_span := {complete, _}
                            },
                            5_000
                        ),
                    ok
                end
            ),
            ok
        end,
        fun(_Trace) ->
            %% Should recover given enough time.
            Data0 = receive_consumed(20_000),
            ?assertMatch(
                [
                    #{
                        <<"clientid">> := ClientId,
                        <<"event">> := <<"message.publish">>,
                        <<"payload">> := Payload,
                        <<"topic">> := MQTTTopic
                    }
                ],
                Data0
            ),
            ok
        end
    ),
    ok.

%% Check that we correctly terminate the pulsar client when the pulsar
%% producer processes fail to start for whatever reason.
t_failure_to_start_producer(Config) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := name_registered},
                #{?snk_kind := pulsar_producer_about_to_start_producers}
            ),
            spawn_link(fun() ->
                ?tp(will_register_name, #{}),
                {ok, #{producer_name := ProducerName}} = ?block_until(
                    #{?snk_kind := pulsar_producer_capture_name}, 10_000
                ),
                true = register(ProducerName, self()),
                ?tp(name_registered, #{name => ProducerName}),
                %% Just simulating another process so that starting the
                %% producers fail.  Currently it does a gen_server:call
                %% with `infinity' timeout, so this is just to avoid
                %% hanging.
                receive
                    {'$gen_call', From, _Request} ->
                        gen_server:reply(From, {error, im_not, your_producer})
                end,
                receive
                    die -> ok
                end
            end),
            {{ok, _}, {ok, _}} =
                ?wait_async_action(
                    create_bridge(Config),
                    #{?snk_kind := pulsar_bridge_producer_stopped},
                    20_000
                ),
            ok
        end,
        []
    ),
    ok.

%% Check the driver recovers itself if one of the producer processes
%% die for whatever reason.
t_producer_process_crash(Config) ->
    MQTTTopic = ?config(mqtt_topic, Config),
    QoS = 0,
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    Message0 = emqx_message:make(ClientId, QoS, MQTTTopic, Payload),
    ?check_trace(
        begin
            {{ok, _}, {ok, _}} =
                ?wait_async_action(
                    create_bridge(
                        Config,
                        #{<<"buffer">> => #{<<"mode">> => <<"disk">>}}
                    ),
                    #{?snk_kind := pulsar_producer_bridge_started},
                    10_000
                ),
            ResourceId = resource_id(Config),
            ChannelId = get_channel_id(Config),
            [ProducerPid | _] = [
                Pid
             || {_Name, PS, _Type, _Mods} <- supervisor:which_children(pulsar_producers_sup),
                Pid <- element(2, process_info(PS, links)),
                case proc_lib:initial_call(Pid) of
                    {pulsar_producer, init, _} -> true;
                    _ -> false
                end
            ],
            Ref = monitor(process, ProducerPid),
            exit(ProducerPid, kill),
            receive
                {'DOWN', Ref, process, ProducerPid, _Killed} ->
                    ok
            after 1_000 -> ct:fail("pid didn't die")
            end,
            ?retry(
                _Sleep0 = 50,
                _Attempts0 = 50,
                ?assertEqual(
                    #{error => <<"Not connected for unknown reason">>, status => connecting},
                    emqx_resource_manager:channel_health_check(ResourceId, ChannelId)
                )
            ),
            ?assertMatch({ok, connected}, emqx_resource_manager:health_check(ResourceId)),
            %% Should recover given enough time.
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual(
                    #{error => undefined, status => connected},
                    emqx_resource_manager:channel_health_check(ResourceId, ChannelId)
                )
            ),
            {_, {ok, _}} =
                ?wait_async_action(
                    emqx:publish(Message0),
                    #{
                        ?snk_kind := "pulsar_producer_query_enter",
                        mode := async,
                        ?snk_span := {complete, _}
                    },
                    5_000
                ),
            Data0 = receive_consumed(20_000),
            ?assertMatch(
                [
                    #{
                        <<"clientid">> := ClientId,
                        <<"event">> := <<"message.publish">>,
                        <<"payload">> := Payload,
                        <<"topic">> := MQTTTopic
                    }
                ],
                Data0
            ),
            ok
        end,
        []
    ),
    ok.

t_resource_manager_crash_after_producers_started(Config) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := pulsar_producer_producers_allocated},
                #{?snk_kind := will_kill_resource_manager}
            ),
            ?force_ordering(
                #{?snk_kind := resource_manager_killed},
                #{?snk_kind := pulsar_producer_bridge_started}
            ),
            spawn_link(fun() ->
                ?tp(will_kill_resource_manager, #{}),
                kill_resource_managers(),
                ?tp(resource_manager_killed, #{}),
                ok
            end),
            %% even if the resource manager is dead, we can still
            %% clear the allocated resources.
            {{error, {config_update_crashed, {killed, _}}}, {ok, _}} =
                ?wait_async_action(
                    create_bridge(Config),
                    #{?snk_kind := pulsar_bridge_stopped, instance_id := InstanceId} when
                        InstanceId =/= undefined,
                    10_000
                ),
            ?assertEqual([], get_pulsar_producers()),
            ?assertMatch({error, bridge_not_found}, delete_bridge(Config)),
            ok
        end,
        []
    ),
    ok.

t_resource_manager_crash_before_producers_started(Config) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := pulsar_producer_capture_name},
                #{?snk_kind := will_kill_resource_manager}
            ),
            ?force_ordering(
                #{?snk_kind := resource_manager_killed},
                #{?snk_kind := pulsar_producer_about_to_start_producers}
            ),
            spawn_link(fun() ->
                ?tp(will_kill_resource_manager, #{}),
                kill_resource_managers(),
                ?tp(resource_manager_killed, #{}),
                ok
            end),
            %% even if the resource manager is dead, we can still
            %% clear the allocated resources.
            {{error, {config_update_crashed, _}}, {ok, _}} =
                ?wait_async_action(
                    create_bridge(Config),
                    #{?snk_kind := pulsar_bridge_stopped},
                    10_000
                ),
            ?assertEqual([], get_pulsar_producers()),
            ?assertMatch({error, bridge_not_found}, delete_bridge(Config)),
            ok
        end,
        []
    ),
    ok.

t_strategy_key_validation(Config) ->
    ?assertMatch(
        {error,
            {{_, 400, _}, _, #{
                <<"message">> :=
                    #{
                        <<"kind">> := <<"validation_error">>,
                        <<"reason">> := <<"Message key cannot be empty", _/binary>>
                    }
            }}},
        create_bridge_api(
            Config,
            #{<<"strategy">> => <<"key_dispatch">>, <<"message">> => #{<<"key">> => <<>>}}
        )
    ),
    ?assertMatch(
        {error,
            {{_, 400, _}, _, #{
                <<"message">> :=
                    #{
                        <<"kind">> := <<"validation_error">>,
                        <<"reason">> := <<"Message key cannot be empty", _/binary>>
                    }
            }}},
        probe_bridge_api(
            Config,
            #{<<"strategy">> => <<"key_dispatch">>, <<"message">> => #{<<"key">> => <<>>}}
        )
    ),
    ok.

t_cluster(Config0) ->
    ct:timetrap({seconds, 120}),
    ?retrying(Config0, 3, fun do_t_cluster/1).

do_t_cluster(Config) ->
    ?check_trace(
        begin
            MQTTTopic = ?config(mqtt_topic, Config),
            Nodes = [N1, N2 | _] = cluster(Config),
            ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
            QoS = 0,
            Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
            {ok, SRef1} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := pulsar_producer_bridge_started}),
                length(Nodes),
                25_000
            ),
            {ok, _} = erpc:call(N1, fun() -> create_bridge(Config) end),
            {ok, _} = snabbkaffe:receive_events(SRef1),
            erpc:multicall(Nodes, fun wait_until_producer_connected/0),
            {ok, _} = snabbkaffe:block_until(
                ?match_n_events(
                    length(Nodes),
                    #{?snk_kind := bridge_post_config_update_done}
                ),
                25_000
            ),
            ResourceId = erpc:call(N1, ?MODULE, resource_id, [Config]),
            lists:foreach(
                fun(N) ->
                    ?retry(
                        _Sleep = 1_000,
                        _Attempts0 = 20,
                        ?assertEqual(
                            {ok, connected},
                            erpc:call(N, emqx_resource_manager, health_check, [ResourceId]),
                            #{node => N}
                        )
                    )
                end,
                Nodes
            ),
            Message0 = emqx_message:make(ClientId, QoS, MQTTTopic, Payload),
            ?tp(publishing_message, #{}),
            erpc:call(N2, emqx, publish, [Message0]),

            lists:foreach(
                fun(N) ->
                    ?assertEqual(
                        {ok, connected},
                        erpc:call(N, emqx_resource_manager, health_check, [ResourceId]),
                        #{node => N}
                    )
                end,
                Nodes
            ),

            Data0 = receive_consumed(30_000),
            ?assertMatch(
                [
                    #{
                        <<"clientid">> := ClientId,
                        <<"event">> := <<"message.publish">>,
                        <<"payload">> := Payload,
                        <<"topic">> := MQTTTopic
                    }
                ],
                Data0
            ),

            ok
        end,
        []
    ),
    ok.

t_resilience(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    ?check_trace(
        begin
            {ok, _} = create_bridge(Config),
            {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
            on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
            ResourceId = resource_id(Config),
            ?retry(
                _Sleep0 = 1_000,
                _Attempts0 = 20,
                ?assertEqual(
                    {ok, connected},
                    emqx_resource_manager:health_check(ResourceId)
                )
            ),

            {ok, C} = emqtt:start_link(),
            {ok, _} = emqtt:connect(C),
            ProduceInterval = 100,
            TestPid = self(),
            StartSequentialProducer =
                fun Go(SeqNo0) ->
                    receive
                        stop -> TestPid ! {done, SeqNo0}
                    after 0 ->
                        SeqNo = SeqNo0 + 1,
                        emqtt:publish(C, ?RULE_TOPIC_BIN, integer_to_binary(SeqNo)),
                        SeqNo rem 10 =:= 0 andalso (TestPid ! {sent, SeqNo}),
                        timer:sleep(ProduceInterval),
                        Go(SeqNo)
                    end
                end,
            SequentialProducer = spawn_link(fun() -> StartSequentialProducer(0) end),
            ct:sleep(2 * ProduceInterval),
            {ok, _} = emqx_common_test_helpers:enable_failure(
                down, ProxyName, ProxyHost, ProxyPort
            ),
            ?retry(
                _Sleep1 = 1_000,
                _Attempts1 = 20,
                ?assertNotEqual(
                    {ok, connected},
                    emqx_resource_manager:health_check(ResourceId)
                )
            ),
            %% Note: we don't check for timeouts here because:
            %%   a) If we do trigger auto reconnect, that means that the producers were
            %%   killed and the `receive_consumed' below will fail.
            %%   b) If there's a timeout, that's the correct path; we just need to give the
            %%   resource manager a chance to do so.
            ?block_until(#{?snk_kind := resource_auto_reconnect}, 5_000),
            {ok, _} = emqx_common_test_helpers:heal_failure(down, ProxyName, ProxyHost, ProxyPort),
            ?retry(
                _Sleep2 = 1_000,
                _Attempts2 = 20,
                ?assertEqual(
                    {ok, connected},
                    emqx_resource_manager:health_check(ResourceId)
                )
            ),
            SequentialProducer ! stop,
            NumProduced =
                receive
                    {done, SeqNo} -> SeqNo
                after 1_000 -> ct:fail("producer didn't stop!")
                end,
            Consumed = lists:flatmap(
                fun(_) -> receive_consumed(10_000) end, lists:seq(1, NumProduced)
            ),
            ?assertEqual(NumProduced, length(Consumed)),
            ExpectedPayloads = lists:map(fun integer_to_binary/1, lists:seq(1, NumProduced)),
            ?assertEqual(
                ExpectedPayloads, lists:map(fun(#{<<"payload">> := P}) -> P end, Consumed)
            ),
            ok
        end,
        []
    ),
    ok.

get_producers_config(ConnectorId, ChannelId) ->
    [
        #{
            state :=
                #{
                    channels :=
                        #{ChannelId := #{producers := Producers}}
                }
        }
    ] =
        lists:filter(
            fun(#{id := Id}) -> Id =:= ConnectorId end,
            emqx_resource_manager:list_all()
        ),
    Producers.
