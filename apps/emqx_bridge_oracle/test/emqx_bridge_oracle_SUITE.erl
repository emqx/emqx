%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_oracle_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(BRIDGE_TYPE_BIN, <<"oracle">>).
-define(APPS, [emqx_bridge, emqx_resource, emqx_rule_engine, emqx_oracle, emqx_bridge_oracle]).
-define(DATABASE, "XE").
-define(RULE_TOPIC, "mqtt/rule").
% -define(RULE_TOPIC_BIN, <<?RULE_TOPIC>>).

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

only_once_tests() ->
    [t_create_via_http].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps(lists:reverse(?APPS)),
    _ = application:stop(emqx_connector),
    ok.

init_per_group(plain = Type, Config) ->
    OracleHost = os:getenv("ORACLE_PLAIN_HOST", "toxiproxy.emqx.net"),
    OraclePort = list_to_integer(os:getenv("ORACLE_PLAIN_PORT", "1521")),
    ProxyName = "oracle",
    case emqx_common_test_helpers:is_tcp_server_available(OracleHost, OraclePort) of
        true ->
            Config1 = common_init_per_group(),
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
    ok;
end_per_group(_Group, _Config) ->
    ok.

common_init_per_group() ->
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    application:load(emqx_bridge),
    ok = emqx_common_test_helpers:start_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:start_apps(?APPS),
    {ok, _} = application:ensure_all_started(emqx_connector),
    emqx_mgmt_api_test_util:init_suite(),
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

sql_create_table() ->
    "CREATE TABLE mqtt_test (topic VARCHAR2(255), msgid VARCHAR2(64), payload NCLOB, retain NUMBER(1))".

sql_drop_table() ->
    "DROP TABLE mqtt_test".

reset_table(Config) ->
    ResourceId = resource_id(Config),
    _ = emqx_resource:simple_sync_query(ResourceId, {sql, sql_drop_table()}),
    {ok, [{proc_result, 0, _}]} = emqx_resource:simple_sync_query(
        ResourceId, {sql, sql_create_table()}
    ),
    ok.

drop_table(Config) ->
    ResourceId = resource_id(Config),
    emqx_resource:simple_sync_query(ResourceId, {query, sql_drop_table()}),
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
            "  database = \"~s\"\n"
            "  sid = \"~s\"\n"
            "  server = \"~s\"\n"
            "  username = \"system\"\n"
            "  password = \"oracle\"\n"
            "  pool_size = 1\n"
            "  sql = \"~s\"\n"
            "  resource_opts = {\n"
            "     auto_restart_interval = 5000\n"
            "     request_timeout = 30000\n"
            "     query_mode = \"async\"\n"
            "     enable_batch = true\n"
            "     batch_size = 3\n"
            "     batch_time = \"3s\"\n"
            "     worker_pool_size = 1\n"
            "  }\n"
            "}\n",
            [
                Name,
                ?DATABASE,
                ?DATABASE,
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
    emqx_bridge_resource:resource_id(Type, Name).

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

probe_bridge_api(Config, _Overrides) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(oracle_name, Config),
    OracleConfig = ?config(oracle_config, Config),
    Params = OracleConfig#{<<"type">> => TypeBin, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["bridges_probe"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("probing bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params, Opts) of
            {ok, {{_, 204, _}, _Headers, _Body0} = Res0} -> {ok, Res0};
            Error -> Error
        end,
    ct:pal("bridge probe result: ~p", [Res]),
    Res.

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

% Under normal operations, the bridge will be called async via
% `simple_async_query'.
t_sync_query(Config) ->
    ResourceId = resource_id(Config),
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            reset_table(Config),
            MsgId = erlang:unique_integer(),
            Params = #{
                topic => ?config(mqtt_topic, Config),
                id => MsgId,
                payload => ?config(oracle_name, Config),
                retain => true
            },
            Message = {send_message, Params},
            ?assertEqual(
                {ok, [{affected_rows, 1}]}, emqx_resource:simple_sync_query(ResourceId, Message)
            ),
            ok
        end,
        []
    ),
    ok.

t_async_query(Config) ->
    Overrides = #{
        <<"resource_opts">> => #{
            <<"enable_batch">> => <<"false">>,
            <<"batch_size">> => 1
        }
    },
    ResourceId = resource_id(Config),
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config, Overrides)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            reset_table(Config),
            MsgId = erlang:unique_integer(),
            Params = #{
                topic => ?config(mqtt_topic, Config),
                id => MsgId,
                payload => ?config(oracle_name, Config),
                retain => false
            },
            Message = {send_message, Params},
            ?assertMatch(
                {
                    ok,
                    {ok, #{result := {ok, [{affected_rows, 1}]}}}
                },
                ?wait_async_action(
                    emqx_resource:query(ResourceId, Message),
                    #{?snk_kind := oracle_query},
                    5_000
                )
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
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 30,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            reset_table(Config),
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
            Message = {send_message, Params},
            emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
                ct:sleep(1000),
                emqx_resource:query(ResourceId, Message),
                emqx_resource:query(ResourceId, Message),
                emqx_resource:query(ResourceId, Message)
            end),
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

t_batch_async_query(Config) ->
    ResourceId = resource_id(Config),
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            reset_table(Config),
            MsgId = erlang:unique_integer(),
            Params = #{
                topic => ?config(mqtt_topic, Config),
                id => MsgId,
                payload => ?config(oracle_name, Config),
                retain => false
            },
            Message = {send_message, Params},
            ?assertMatch(
                {
                    ok,
                    {ok, #{result := {ok, [{affected_rows, 1}]}}}
                },
                ?wait_async_action(
                    emqx_resource:query(ResourceId, Message),
                    #{?snk_kind := oracle_batch_query},
                    5_000
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
    ok.

t_start_stop(Config) ->
    OracleName = ?config(oracle_name, Config),
    ResourceId = resource_id(Config),
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge(Config)),
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

t_on_get_status(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    ResourceId = resource_id(Config),
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
        ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceId))
    end),
    %% Check that it recovers itself.
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    ok.
