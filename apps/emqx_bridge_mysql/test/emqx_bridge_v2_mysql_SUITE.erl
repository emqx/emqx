%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_bridge_v2_mysql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(BRIDGE_TYPE, mysql).
-define(BRIDGE_TYPE_BIN, <<"mysql">>).
-define(CONNECTOR_TYPE, mysql).
-define(CONNECTOR_TYPE_BIN, <<"mysql">>).

-import(emqx_common_test_helpers, [on_exit/1]).
-import(emqx_utils_conv, [bin/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    MysqlHost = os:getenv("MYSQL_TCP_HOST", "toxiproxy"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TCP_PORT", "3306")),
    case emqx_common_test_helpers:is_tcp_server_available(MysqlHost, MysqlPort) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_connector,
                    emqx_bridge,
                    emqx_bridge_mysql,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            NConfig = [
                {apps, Apps},
                {mysql_host, MysqlHost},
                {mysql_port, MysqlPort},
                {enable_tls, false},
                {mysql_host, MysqlHost},
                {mysql_port, MysqlPort}
                | Config
            ],
            emqx_bridge_mysql_SUITE:connect_and_drop_table(NConfig),
            NConfig;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_mysql);
                _ ->
                    {skip, no_mysql}
            end
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ct:timetrap(timer:seconds(60)),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_config:delete_override_conf_files(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = iolist_to_binary([atom_to_binary(TestCase), UniqueNum]),
    Username = <<"root">>,
    Password = <<"public">>,
    Passfile = filename:join(?config(priv_dir, Config), "passfile"),
    ok = file:write_file(Passfile, Password),
    NConfig = [
        {mysql_username, Username},
        {mysql_password, Password},
        {mysql_passfile, Passfile}
        | Config
    ],
    emqx_bridge_mysql_SUITE:connect_and_create_table(NConfig),
    ConnectorConfig = connector_config(Name, NConfig),
    BridgeConfig = bridge_config(Name, Name),
    ok = snabbkaffe:start_trace(),
    [
        {connector_type, proplists:get_value(connector_type, Config, ?CONNECTOR_TYPE)},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {action_type, proplists:get_value(action_type, Config, ?BRIDGE_TYPE)},
        {action_name, Name},
        {bridge_config, BridgeConfig}
        | NConfig
    ].

end_per_testcase(_Testcase, Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            emqx_bridge_mysql_SUITE:connect_and_drop_table(Config),
            emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
            emqx_common_test_helpers:call_janitor(60_000),
            ok = snabbkaffe:stop(),
            ok
    end.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, Config) ->
    MysqlHost = ?config(mysql_host, Config),
    MysqlPort = ?config(mysql_port, Config),
    Username = ?config(mysql_username, Config),
    PassFile = ?config(mysql_passfile, Config),
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"database">> => <<"mqtt">>,
            <<"server">> => iolist_to_binary([MysqlHost, ":", integer_to_binary(MysqlPort)]),
            <<"pool_size">> => 8,
            <<"username">> => Username,
            <<"password">> => iolist_to_binary(["file://", PassFile]),
            <<"resource_opts">> => #{
                <<"health_check_interval">> => <<"15s">>,
                <<"start_after_created">> => true,
                <<"start_timeout">> => <<"5s">>
            }
        },
    InnerConfigMap = serde_roundtrip(InnerConfigMap0),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, Name, InnerConfigMap).

default_sql() ->
    <<
        "INSERT INTO mqtt_test(payload, arrived) "
        "VALUES (${payload.value}, FROM_UNIXTIME(${timestamp}/1000))"
    >>.

bad_sql() ->
    <<
        "INSERT INTO mqtt_test(payload, arrivedx) "
        "VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))"
    >>.

bridge_config(Name, ConnectorId) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"connector">> => ConnectorId,
            <<"parameters">> =>
                #{<<"sql">> => default_sql()},
            <<"local_topic">> => <<"t/mysql">>,
            <<"resource_opts">> => #{
                <<"batch_size">> => 1,
                <<"batch_time">> => <<"0ms">>,
                <<"buffer_mode">> => <<"memory_only">>,
                <<"buffer_seg_bytes">> => <<"10MB">>,
                <<"health_check_interval">> => <<"15s">>,
                <<"inflight_window">> => 100,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"metrics_flush_interval">> => <<"1s">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"45s">>,
                <<"worker_pool_size">> => <<"1">>
            }
        },
    InnerConfigMap = serde_roundtrip(InnerConfigMap0),
    parse_and_check_bridge_config(InnerConfigMap, Name).

%% check it serializes correctly
serde_roundtrip(InnerConfigMap0) ->
    IOList = hocon_pp:do(InnerConfigMap0, #{}),
    {ok, InnerConfigMap} = hocon:binary(IOList),
    InnerConfigMap.

parse_and_check_bridge_config(InnerConfigMap, Name) ->
    emqx_bridge_v2_testlib:parse_and_check(?BRIDGE_TYPE_BIN, Name, InnerConfigMap).

make_message() ->
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    #{
        clientid => ClientId,
        payload => Payload,
        timestamp => 1668602148000
    }.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => connecting}),
    ok.

t_start_action_or_source_with_disabled_connector(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_action_or_source_with_disabled_connector(Config),
    ok.

t_update_with_invalid_prepare(Config) ->
    ConnectorName = ?config(connector_name, Config),
    BridgeName = ?config(action_name, Config),
    {ok, _} = emqx_bridge_v2_testlib:create_bridge_api(Config),
    %% arrivedx is a bad column name
    BadSQL = bad_sql(),
    Override = #{<<"parameters">> => #{<<"sql">> => BadSQL}},
    {ok, {{_, 200, "OK"}, _Headers1, Body1}} =
        emqx_bridge_v2_testlib:update_bridge_api(Config, Override),
    ?assertMatch(#{<<"status">> := <<"disconnected">>}, Body1),
    Error1 = maps:get(<<"error">>, Body1),
    case re:run(Error1, <<"Unknown column">>, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            ct:fail(#{
                expected_pattern => "undefined_column",
                got => Error1
            })
    end,
    %% assert that although there was an error returned, the invliad SQL is actually put
    C1 = [{action_name, BridgeName}, {action_type, mysql} | Config],
    {ok, {{_, 200, "OK"}, _, Action}} = emqx_bridge_v2_testlib:get_action_api(C1),
    #{<<"parameters">> := #{<<"sql">> := FetchedSQL}} = Action,
    ?assertEqual(FetchedSQL, BadSQL),

    %% update again with the original sql
    {ok, {{_, 200, "OK"}, _Headers2, Body2}} =
        emqx_bridge_v2_testlib:update_bridge_api(Config, #{}),
    %% the error should be gone now, and status should be 'connected'
    ?assertMatch(#{<<"error">> := <<>>, <<"status">> := <<"connected">>}, Body2),
    %% finally check if ecpool worker should have exactly one of reconnect callback
    ConnectorResId = <<"connector:mysql:", ConnectorName/binary>>,
    Workers = ecpool:workers(ConnectorResId),
    [_ | _] = WorkerPids = lists:map(fun({_, Pid}) -> Pid end, Workers),
    lists:foreach(
        fun(Pid) ->
            [{emqx_mysql, prepare_sql_to_conn, Args}] =
                ecpool_worker:get_reconnect_callbacks(Pid),
            Sig = emqx_mysql:get_reconnect_callback_signature(Args),
            BridgeResId = <<"action:mysql:", BridgeName/binary, $:, ConnectorResId/binary>>,
            ?assertEqual(BridgeResId, Sig)
        end,
        WorkerPids
    ),
    ok.
