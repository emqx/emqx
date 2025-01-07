%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_oracle_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(BRIDGE_TYPE_BIN, <<"oracle">>).
-define(SID, "XE").
-define(RULE_TOPIC, "mqtt/rule").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, plain}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {plain, AllTCs}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(plain = Type, Config) ->
    OracleHost = os:getenv("ORACLE_PLAIN_HOST", "toxiproxy.emqx.net"),
    OraclePort = list_to_integer(os:getenv("ORACLE_PLAIN_PORT", "1521")),
    ProxyName = "oracle",
    case emqx_common_test_helpers:is_tcp_server_available(OracleHost, OraclePort) of
        true ->
            Config1 = common_init_per_group(Config),
            [
                {proxy_name, ProxyName},
                {oracle_host, OracleHost},
                {oracle_port, OraclePort},
                {connection_type, Type}
                | Config1 ++ Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_oracle);
                _ ->
                    {skip, no_oracle}
            end
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when
    Group =:= plain
->
    common_end_per_group(Config),
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

common_init_per_group(Config) ->
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    %% Ensure enterprise bridge module is loaded
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_oracle,
            emqx_bridge_oracle,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    MQTTTopic = <<"mqtt/topic/", UniqueNum/binary>>,
    [
        {apps, Apps},
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
    common_end_per_testcase(_Testcase, Config).

common_init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(60)),
    delete_all_bridges(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    OracleTopic =
        <<
            (atom_to_binary(TestCase))/binary,
            UniqueNum/binary
        >>,
    ConnectionType = ?config(connection_type, Config0),
    Config = [{oracle_topic, OracleTopic} | Config0],
    {Name, ConfigString, OracleConfig} = oracle_config(
        TestCase, ConnectionType, Config
    ),
    ok = snabbkaffe:start_trace(),
    [
        {bridge_type, ?BRIDGE_TYPE_BIN},
        {bridge_name, Name},
        {oracle_name, Name},
        {oracle_config_string, ConfigString},
        {oracle_config, OracleConfig}
        | Config
    ].

common_end_per_testcase(_Testcase, Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            ProxyHost = ?config(proxy_host, Config),
            ProxyPort = ?config(proxy_port, Config),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            delete_all_bridges(),
            %% in CI, apparently this needs more time since the
            %% machines struggle with all the containers running...
            emqx_common_test_helpers:call_janitor(60_000),
            ok = snabbkaffe:stop(),
            ok
    end.

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
sql_insert_template_for_bridge() ->
    "INSERT INTO mqtt_test(topic, msgid, payload, retain) VALUES (${topic}, ${id}, ${payload}, ${retain})".

sql_insert_template_with_nested_token_for_bridge() ->
    "INSERT INTO mqtt_test(topic, msgid, payload, retain) VALUES (${topic}, ${id}, ${payload.msg}, ${retain})".

sql_insert_template_with_inconsistent_datatype() ->
    "INSERT INTO mqtt_test(topic, msgid, payload, retain) VALUES (${topic}, ${id}, ${payload}, ${flags})".

sql_create_table() ->
    "CREATE TABLE mqtt_test (topic VARCHAR2(255), msgid VARCHAR2(64), payload NCLOB, retain NUMBER(1))".

sql_drop_table() ->
    "BEGIN\n"
    "        EXECUTE IMMEDIATE 'DROP TABLE mqtt_test';\n"
    "     EXCEPTION\n"
    "        WHEN OTHERS THEN\n"
    "            IF SQLCODE = -942 THEN\n"
    "                NULL;\n"
    "            ELSE\n"
    "                RAISE;\n"
    "            END IF;\n"
    "     END;".

sql_check_table_exist() ->
    "SELECT COUNT(*) FROM user_tables WHERE table_name = 'MQTT_TEST'".

new_jamdb_connection(Config) ->
    JamdbOpts = [
        {host, ?config(oracle_host, Config)},
        {port, ?config(oracle_port, Config)},
        {user, "system"},
        {password, "oracle"},
        {sid, ?SID}
    ],
    jamdb_oracle:start(JamdbOpts).

close_jamdb_connection(Conn) ->
    jamdb_oracle:stop(Conn).

reset_table(Config) ->
    {ok, Conn} = new_jamdb_connection(Config),
    try
        ok = drop_table_if_exists(Conn),
        {ok, [{proc_result, 0, _}]} = jamdb_oracle:sql_query(Conn, sql_create_table())
    after
        close_jamdb_connection(Conn)
    end,
    ok.

drop_table_if_exists(Conn) when is_pid(Conn) ->
    {ok, [{proc_result, 0, _}]} = jamdb_oracle:sql_query(Conn, sql_drop_table()),
    ok;
drop_table_if_exists(Config) ->
    {ok, Conn} = new_jamdb_connection(Config),
    try
        ok = drop_table_if_exists(Conn)
    after
        close_jamdb_connection(Conn)
    end,
    ok.

oracle_config(TestCase, _ConnectionType, Config) ->
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    OracleHost = ?config(oracle_host, Config),
    OraclePort = ?config(oracle_port, Config),
    Name = <<
        (atom_to_binary(TestCase))/binary, UniqueNum/binary
    >>,
    ServerURL = iolist_to_binary([
        OracleHost,
        ":",
        integer_to_binary(OraclePort)
    ]),
    ConfigString =
        io_lib:format(
            "bridges.oracle.~s {\n"
            "  enable = true\n"
            "  sid = \"~s\"\n"
            "  server = \"~s\"\n"
            "  username = \"system\"\n"
            "  password = \"oracle\"\n"
            "  pool_size = 1\n"
            "  sql = \"~s\"\n"
            "  resource_opts = {\n"
            "     health_check_interval = \"15s\"\n"
            "     request_ttl = \"30s\"\n"
            "     query_mode = \"async\"\n"
            "     batch_size = 3\n"
            "     batch_time = \"3s\"\n"
            "     worker_pool_size = 1\n"
            "  }\n"
            "}\n",
            [
                Name,
                ?SID,
                ServerURL,
                sql_insert_template_for_bridge()
            ]
        ),
    {Name, ConfigString, parse_and_check(ConfigString, Name)}.

parse_and_check(ConfigString, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    TypeBin = ?BRIDGE_TYPE_BIN,
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{TypeBin := #{Name := Config}}} = RawConf,
    Config.

resource_id(Config) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(oracle_name, Config),
    <<"connector:", Type/binary, ":", Name/binary>>.

action_id(Config) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(oracle_name, Config),
    emqx_bridge_v2:id(Type, Name).

bridge_id(Config) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(oracle_name, Config),
    emqx_bridge_resource:bridge_id(Type, Name).

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(oracle_name, Config),
    OracleConfig0 = ?config(oracle_config, Config),
    OracleConfig = emqx_utils_maps:deep_merge(OracleConfig0, Overrides),
    emqx_bridge:create(Type, Name, OracleConfig).

create_bridge_api(Config) ->
    create_bridge_api(Config, _Overrides = #{}).

create_bridge_api(Config, Overrides) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(oracle_name, Config),
    OracleConfig0 = ?config(oracle_config, Config),
    OracleConfig = emqx_utils_maps:deep_merge(OracleConfig0, Overrides),
    Params = OracleConfig#{<<"type">> => TypeBin, <<"name">> => Name},
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
    Name = ?config(oracle_name, Config),
    OracleConfig0 = ?config(oracle_config, Config),
    OracleConfig = emqx_utils_maps:deep_merge(OracleConfig0, Overrides),
    BridgeId = emqx_bridge_resource:bridge_id(TypeBin, Name),
    Params = OracleConfig#{<<"type">> => TypeBin, <<"name">> => Name},
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
    Name = ?config(oracle_name, Config),
    OracleConfig0 = ?config(oracle_config, Config),
    OracleConfig = emqx_utils_maps:deep_merge(OracleConfig0, Overrides),
    case emqx_bridge_testlib:probe_bridge_api(TypeBin, Name, OracleConfig) of
        {ok, {{_, 204, _}, _Headers, _Body0} = Res0} ->
            {ok, Res0};
        {error, {Status, Headers, Body0}} ->
            {error, {Status, Headers, emqx_bridge_testlib:try_decode_error(Body0)}};
        Error ->
            Error
    end.

create_rule_and_action_http(Config) ->
    OracleName = ?config(oracle_name, Config),
    BridgeId = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_BIN, OracleName),
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

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_sync_query(Config) ->
    ResourceId = resource_id(Config),
    Name = ?config(oracle_name, Config),
    ?check_trace(
        begin
            reset_table(Config),
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            ?retry(
                _Sleep1 = 1_000,
                _Attempts1 = 30,
                ?assertMatch(
                    #{status := connected},
                    emqx_bridge_v2:health_check(
                        ?BRIDGE_TYPE_BIN,
                        Name
                    )
                )
            ),
            ActionId = action_id(Config),
            MsgId = erlang:unique_integer(),
            Params = #{
                topic => ?config(mqtt_topic, Config),
                id => MsgId,
                payload => ?config(oracle_name, Config),
                retain => true
            },
            Message = {ActionId, Params},
            ?assertEqual(
                {ok, [{affected_rows, 1}]}, emqx_resource:simple_sync_query(ResourceId, Message)
            ),
            ok
        end,
        []
    ),
    ok.

t_batch_sync_query(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    ResourceId = resource_id(Config),
    Name = ?config(oracle_name, Config),
    ?check_trace(
        begin
            reset_table(Config),
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 30,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 30,
                ?assertMatch(
                    #{status := connected},
                    emqx_bridge_v2:health_check(
                        ?BRIDGE_TYPE_BIN,
                        Name
                    )
                )
            ),
            MsgId = erlang:unique_integer(),
            Params = #{
                topic => ?config(mqtt_topic, Config),
                id => MsgId,
                payload => ?config(oracle_name, Config),
                retain => false
            },
            % Send 3 async messages while resource is down. When it comes back, these messages
            % will be delivered in sync way. If we try to send sync messages directly, it will
            % be sent async as callback_mode is set to async_if_possible.
            emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
                ct:sleep(1000),
                emqx_bridge_v2:send_message(?BRIDGE_TYPE_BIN, Name, Params, #{}),
                emqx_bridge_v2:send_message(?BRIDGE_TYPE_BIN, Name, Params, #{}),
                emqx_bridge_v2:send_message(?BRIDGE_TYPE_BIN, Name, Params, #{}),
                ok
            end),
            % Wait for reconnection.
            ?retry(
                _Sleep = 1_000,
                _Attempts = 30,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 30,
                ?assertMatch(
                    #{status := connected},
                    emqx_bridge_v2:health_check(
                        ?BRIDGE_TYPE_BIN,
                        Name
                    )
                )
            ),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 30,
                ?assertMatch(
                    {ok, [{result_set, _, _, [[{3}]]}]},
                    emqx_resource:simple_sync_query(
                        ResourceId, {query, "SELECT COUNT(*) FROM mqtt_test"}
                    )
                )
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
                        <<"resource_opts">> =>
                            #{<<"batch_size">> => 4}
                    }
                )
            ),
            ?assertMatch(
                {ok, _},
                update_bridge_api(
                    Config,
                    #{
                        <<"resource_opts">> =>
                            #{<<"batch_time">> => <<"4s">>}
                    }
                )
            ),
            ok
        end,
        []
    ),
    ResourceId = resource_id(Config),
    ?assertMatch(1, length(ecpool:workers(ResourceId))),
    ok.

t_start_stop(Config) ->
    OracleName = ?config(oracle_name, Config),
    ResourceId = resource_id(Config),
    ?check_trace(
        begin
            reset_table(Config),
            ?assertMatch({ok, _}, create_bridge(Config)),
            %% Since the connection process is async, we give it some time to
            %% stabilize and avoid flakiness.
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(
                    #{status := connected},
                    emqx_bridge_v2:health_check(
                        ?BRIDGE_TYPE_BIN,
                        OracleName
                    )
                )
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
                    emqx_bridge:disable_enable(disable, ?BRIDGE_TYPE_BIN, OracleName),
                    #{?snk_kind := oracle_bridge_stopped},
                    5_000
                )
            ),

            ok
        end,
        fun(Trace) ->
            %% one for each probe, one for real
            ?assertMatch([_, _, _], ?of_kind(oracle_bridge_stopped, Trace)),
            ok
        end
    ),
    ok.

t_probe_with_nested_tokens(Config) ->
    ProbeRes0 = probe_bridge_api(
        Config,
        #{<<"sql">> => sql_insert_template_with_nested_token_for_bridge()}
    ),
    ?assertMatch({ok, {{_, 204, _}, _Headers, _Body}}, ProbeRes0).

t_message_with_nested_tokens(Config) ->
    BridgeId = bridge_id(Config),
    ResourceId = resource_id(Config),
    Name = ?config(oracle_name, Config),
    reset_table(Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config, #{
            <<"sql">> => sql_insert_template_with_nested_token_for_bridge()
        })
    ),
    %% Since the connection process is async, we give it some time to
    %% stabilize and avoid flakiness.
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertMatch(
            #{status := connected},
            emqx_bridge_v2:health_check(
                ?BRIDGE_TYPE_BIN,
                Name
            )
        )
    ),
    MsgId = erlang:unique_integer(),
    Data = binary_to_list(?config(oracle_name, Config)),
    Params = #{
        topic => ?config(mqtt_topic, Config),
        id => MsgId,
        payload => emqx_utils_json:encode(#{<<"msg">> => Data}),
        retain => false
    },
    emqx_bridge:send_message(BridgeId, Params),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertMatch(
            {ok, [{result_set, [<<"PAYLOAD">>], _, [[Data]]}]},
            emqx_resource:simple_sync_query(
                ResourceId, {query, "SELECT payload FROM mqtt_test"}
            )
        )
    ),
    ok.

t_probe_with_inconsistent_datatype(Config) ->
    ProbeRes0 = probe_bridge_api(
        Config,
        #{<<"sql">> => sql_insert_template_with_inconsistent_datatype()}
    ),
    ?assertMatch({ok, {{_, 204, _}, _Headers, _Body}}, ProbeRes0).

t_on_get_status(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    Name = ?config(oracle_name, Config),
    ResourceId = resource_id(Config),
    reset_table(Config),
    ?assertMatch({ok, _}, create_bridge(Config)),
    %% Since the connection process is async, we give it some time to
    %% stabilize and avoid flakiness.
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ct:sleep(500),
        ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceId)),
        ?assertMatch(
            #{status := disconnected},
            emqx_bridge_v2:health_check(?BRIDGE_TYPE_BIN, Name)
        )
    end),
    %% Check that it recovers itself.
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        begin
            ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId)),
            ?assertMatch(
                #{status := connected},
                emqx_bridge_v2:health_check(?BRIDGE_TYPE_BIN, Name)
            )
        end
    ),
    ok.

t_no_sid_nor_service_name(Config0) ->
    OracleConfig0 = ?config(oracle_config, Config0),
    OracleConfig1 = maps:remove(<<"sid">>, OracleConfig0),
    OracleConfig = maps:remove(<<"service_name">>, OracleConfig1),
    NewOracleConfig = {oracle_config, OracleConfig},
    Config = lists:keyreplace(oracle_config, 1, Config0, NewOracleConfig),
    ?assertMatch(
        {error, #{kind := validation_error, reason := "neither SID nor Service Name was set"}},
        create_bridge(Config)
    ),
    ?assertMatch(
        {error,
            {{_, 400, _}, _, #{
                <<"message">> := #{
                    <<"kind">> := <<"validation_error">>,
                    <<"reason">> := <<"neither SID nor Service Name was set">>,
                    %% should be censored as it contains secrets
                    <<"value">> := #{<<"password">> := <<"******">>}
                }
            }}},
        create_bridge_api(Config)
    ),
    ?assertMatch(
        {error,
            {{_, 400, _}, _, #{
                <<"message">> := #{
                    <<"kind">> := <<"validation_error">>,
                    <<"reason">> := <<"neither SID nor Service Name was set">>,
                    %% should be censored as it contains secrets
                    <<"value">> := #{<<"password">> := <<"******">>}
                }
            }}},
        probe_bridge_api(Config)
    ),
    ok.

t_missing_table(Config) ->
    Name = ?config(bridge_name, Config),
    ?check_trace(
        begin
            drop_table_if_exists(Config),
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(
                    {ok, #{
                        <<"status">> := <<"disconnected">>,
                        <<"status_reason">> := <<"{unhealthy_target,", _/binary>>
                    }},
                    emqx_bridge_testlib:get_bridge_api(Config)
                )
            ),
            ?block_until(#{?snk_kind := oracle_undefined_table}),
            MsgId = erlang:unique_integer(),
            Params = #{
                topic => ?config(mqtt_topic, Config),
                id => MsgId,
                payload => ?config(oracle_name, Config),
                retain => true
            },
            ?assertMatch(
                {error, {resource_error, #{reason := unhealthy_target}}},
                emqx_bridge_v2:send_message(?BRIDGE_TYPE_BIN, Name, Params, _QueryOpts = #{})
            ),
            ok
        end,
        fun(Trace) ->
            ?assertNotMatch([], ?of_kind(oracle_undefined_table, Trace)),
            ok
        end
    ).

t_table_removed(Config) ->
    ResourceId = resource_id(Config),
    ?check_trace(
        begin
            reset_table(Config),
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ActionId = emqx_bridge_v2:id(?BRIDGE_TYPE_BIN, ?config(oracle_name, Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            drop_table_if_exists(Config),
            MsgId = erlang:unique_integer(),
            Params = #{
                topic => ?config(mqtt_topic, Config),
                id => MsgId,
                payload => ?config(oracle_name, Config),
                retain => true
            },
            Message = {ActionId, Params},
            ?assertEqual(
                {error, {unrecoverable_error, {942, "ORA-00942: table or view does not exist\n"}}},
                emqx_resource:simple_sync_query(ResourceId, Message)
            ),
            ok
        end,
        []
    ),
    ok.

t_update_with_invalid_prepare(Config) ->
    reset_table(Config),

    {ok, _} = create_bridge_api(Config),

    %% retainx is a bad column name
    BadSQL =
        <<"INSERT INTO mqtt_test(topic, msgid, payload, retainx) VALUES (${topic}, ${id}, ${payload}, ${retain})">>,

    Override = #{<<"sql">> => BadSQL},
    {ok, Body1} =
        update_bridge_api(Config, Override),

    ?assertMatch(#{<<"status">> := <<"disconnected">>}, Body1),
    Error1 = maps:get(<<"status_reason">>, Body1),
    case re:run(Error1, <<"unhealthy_target">>, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            ct:fail(#{
                expected_pattern => "undefined_column",
                got => Error1
            })
    end,

    %% assert that although there was an error returned, the invliad SQL is actually put
    BridgeName = ?config(oracle_name, Config),
    C1 = [{action_name, BridgeName}, {action_type, oracle} | Config],
    {ok, {{_, 200, "OK"}, _, Action}} = emqx_bridge_v2_testlib:get_action_api(C1),
    #{<<"parameters">> := #{<<"sql">> := FetchedSQL}} = Action,
    ?assertEqual(FetchedSQL, BadSQL),

    %% update again with the original sql
    {ok, Body2} = update_bridge_api(Config),
    %% the error should be gone now, and status should be 'connected'
    ?assertMatch(#{<<"status">> := <<"connected">>}, Body2),
    %% finally check if ecpool worker should have exactly one of reconnect callback
    ConnectorResId = <<"connector:oracle:", BridgeName/binary>>,
    Workers = ecpool:workers(ConnectorResId),
    [_ | _] = WorkerPids = lists:map(fun({_, Pid}) -> Pid end, Workers),
    lists:foreach(
        fun(Pid) ->
            [{emqx_oracle, prepare_sql_to_conn, Args}] =
                ecpool_worker:get_reconnect_callbacks(Pid),
            Sig = emqx_postgresql:get_reconnect_callback_signature(Args),
            BridgeResId = <<"action:oracle:", BridgeName/binary, $:, ConnectorResId/binary>>,
            ?assertEqual(BridgeResId, Sig)
        end,
        WorkerPids
    ),
    ok.
