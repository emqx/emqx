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
-module(emqx_bridge_v2_pgsql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BRIDGE_TYPE, pgsql).
-define(BRIDGE_TYPE_BIN, <<"pgsql">>).
-define(CONNECTOR_TYPE, pgsql).
-define(CONNECTOR_TYPE_BIN, <<"pgsql">>).

-import(emqx_common_test_helpers, [on_exit/1]).
-import(emqx_utils_conv, [bin/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    PostgresHost = os:getenv("PGSQL_TCP_HOST", "toxiproxy"),
    PostgresPort = list_to_integer(os:getenv("PGSQL_TCP_PORT", "5432")),
    case emqx_common_test_helpers:is_tcp_server_available(PostgresHost, PostgresPort) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_connector,
                    emqx_bridge,
                    emqx_bridge_pgsql,
                    emqx_rule_engine,
                    emqx_management,
                    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            {ok, Api} = emqx_common_test_http:create_default_app(),
            NConfig = [
                {apps, Apps},
                {api, Api},
                {pgsql_host, PostgresHost},
                {pgsql_port, PostgresPort},
                {enable_tls, false},
                {postgres_host, PostgresHost},
                {postgres_port, PostgresPort}
                | Config
            ],
            emqx_bridge_pgsql_SUITE:connect_and_create_table(NConfig),
            NConfig;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_postgres);
                _ ->
                    {skip, no_postgres}
            end
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

common_init_per_testcase(TestCase, Config) ->
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
        {postgres_username, Username},
        {postgres_password, Password},
        {postgres_passfile, Passfile}
        | Config
    ],
    ConnectorConfig = connector_config(Name, NConfig),
    BridgeConfig = bridge_config(Name, Name),
    ok = snabbkaffe:start_trace(),
    [
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {bridge_type, ?BRIDGE_TYPE},
        {bridge_name, Name},
        {bridge_config, BridgeConfig}
        | NConfig
    ].

end_per_testcase(_Testcase, Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            emqx_bridge_pgsql_SUITE:connect_and_clear_table(Config),
            emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
            emqx_common_test_helpers:call_janitor(60_000),
            ok = snabbkaffe:stop(),
            ok
    end.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, Config) ->
    PostgresHost = ?config(postgres_host, Config),
    PostgresPort = ?config(postgres_port, Config),
    Username = ?config(postgres_username, Config),
    PassFile = ?config(postgres_passfile, Config),
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"database">> => <<"mqtt">>,
            <<"server">> => iolist_to_binary([PostgresHost, ":", integer_to_binary(PostgresPort)]),
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
    parse_and_check_connector_config(InnerConfigMap, Name).

parse_and_check_connector_config(InnerConfigMap, Name) ->
    TypeBin = ?CONNECTOR_TYPE_BIN,
    RawConf = #{<<"connectors">> => #{TypeBin => #{Name => InnerConfigMap}}},
    #{<<"connectors">> := #{TypeBin := #{Name := Config}}} =
        hocon_tconf:check_plain(emqx_connector_schema, RawConf, #{
            required => false, atom_key => false
        }),
    ct:pal("parsed config: ~p", [Config]),
    InnerConfigMap.

bridge_config(Name, ConnectorId) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"connector">> => ConnectorId,
            <<"parameters">> =>
                #{<<"sql">> => emqx_bridge_pgsql_SUITE:default_sql()},
            <<"local_topic">> => <<"t/postgres">>,
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
                <<"resume_interval">> => <<"15s">>,
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

t_start_stop(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, postgres_stopped),
    ok.

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => connecting}),
    ok.

t_sync_query(Config) ->
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        fun make_message/0,
        fun(Res) -> ?assertMatch({ok, _}, Res) end,
        postgres_bridge_connector_on_query_return
    ),
    ok.
