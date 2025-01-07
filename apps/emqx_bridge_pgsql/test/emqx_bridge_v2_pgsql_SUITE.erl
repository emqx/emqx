%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_cases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.

matrix_cases() ->
    [
        t_disable_prepared_statements
    ].

groups() ->
    emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()).

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
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            NConfig = [
                {apps, Apps},
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

init_per_group(Group, Config) when
    Group =:= postgres;
    Group =:= timescale;
    Group =:= matrix
->
    [
        {bridge_type, group_to_type(Group)},
        {action_type, group_to_type(Group)},
        {connector_type, group_to_type(Group)}
        | Config
    ];
init_per_group(batch_enabled, Config) ->
    [
        {batch_size, 10},
        {batch_time, <<"10ms">>}
        | Config
    ];
init_per_group(batch_disabled, Config) ->
    [
        {batch_size, 1},
        {batch_time, <<"0ms">>}
        | Config
    ];
init_per_group(_Group, Config) ->
    Config.

group_to_type(postgres) -> pgsql;
group_to_type(Group) -> Group.

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
        {postgres_username, Username},
        {postgres_password, Password},
        {postgres_passfile, Passfile}
        | Config
    ],
    ConnectorConfig = connector_config(Name, NConfig),
    BridgeConfig = bridge_config(Name, Name),
    ok = snabbkaffe:start_trace(),
    [
        {connector_type, proplists:get_value(connector_type, Config, ?CONNECTOR_TYPE)},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {bridge_type, proplists:get_value(bridge_type, Config, ?BRIDGE_TYPE)},
        {action_type, proplists:get_value(action_type, Config, ?BRIDGE_TYPE)},
        {bridge_name, Name},
        {action_name, Name},
        {action_config, BridgeConfig},
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
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, Name, InnerConfigMap).

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

create_connector_api(Config) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

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

t_start_action_or_source_with_disabled_connector(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_action_or_source_with_disabled_connector(Config),
    ok.

t_disable_prepared_statements(matrix) ->
    [
        [postgres, batch_disabled],
        [postgres, batch_enabled],
        [timescale, batch_disabled],
        [timescale, batch_enabled],
        [matrix, batch_disabled],
        [matrix, batch_enabled]
    ];
t_disable_prepared_statements(Config0) ->
    BatchSize = ?config(batch_size, Config0),
    BatchTime = ?config(batch_time, Config0),
    ConnectorConfig0 = ?config(connector_config, Config0),
    ConnectorConfig = maps:merge(ConnectorConfig0, #{<<"disable_prepared_statements">> => true}),
    BridgeConfig0 = ?config(action_config, Config0),
    BridgeConfig = emqx_utils_maps:deep_merge(
        BridgeConfig0,
        #{
            <<"resource_opts">> => #{
                <<"batch_size">> => BatchSize,
                <<"batch_time">> => BatchTime,
                <<"query_mode">> => <<"async">>
            }
        }
    ),
    Config1 = lists:keyreplace(connector_config, 1, Config0, {connector_config, ConnectorConfig}),
    Config = lists:keyreplace(action_config, 1, Config1, {action_config, BridgeConfig}),
    ?check_trace(
        #{timetrap => 5_000},
        begin
            ?assertMatch({ok, _}, emqx_bridge_v2_testlib:create_bridge_api(Config)),
            RuleTopic = <<"t/postgres">>,
            Type = ?config(bridge_type, Config),
            {ok, _} = emqx_bridge_v2_testlib:create_rule_and_action_http(Type, RuleTopic, Config),
            ResourceId = emqx_bridge_v2_testlib:resource_id(Config),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            {ok, C} = emqtt:start_link(),
            {ok, _} = emqtt:connect(C),
            lists:foreach(
                fun(N) ->
                    emqtt:publish(C, RuleTopic, integer_to_binary(N))
                end,
                lists:seq(1, BatchSize)
            ),
            case BatchSize > 1 of
                true ->
                    ?block_until(#{
                        ?snk_kind := "postgres_success_batch_result",
                        row_count := BatchSize
                    }),
                    ok;
                false ->
                    ok
            end,
            ok
        end,
        []
    ),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok = emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => connecting}),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_update_with_invalid_prepare(Config) ->
    ConnectorName = ?config(connector_name, Config),
    BridgeName = ?config(bridge_name, Config),
    {ok, _} = emqx_bridge_v2_testlib:create_bridge_api(Config),
    %% arrivedx is a bad column name
    BadSQL = <<
        "INSERT INTO mqtt_test(payload, arrivedx) "
        "VALUES (${payload}, TO_TIMESTAMP((${timestamp} :: bigint)/1000))"
    >>,
    Override = #{<<"parameters">> => #{<<"sql">> => BadSQL}},
    {ok, {{_, 200, "OK"}, _Headers1, Body1}} =
        emqx_bridge_v2_testlib:update_bridge_api(Config, Override),
    ?assertMatch(#{<<"status">> := <<"disconnected">>}, Body1),
    Error1 = maps:get(<<"error">>, Body1),
    case re:run(Error1, <<"undefined_column">>, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            ct:fail(#{
                expected_pattern => "undefined_column",
                got => Error1
            })
    end,
    %% assert that although there was an error returned, the invliad SQL is actually put
    C1 = [{action_name, BridgeName}, {action_type, pgsql} | Config],
    {ok, {{_, 200, "OK"}, _, Action}} = emqx_bridge_v2_testlib:get_action_api(C1),
    #{<<"parameters">> := #{<<"sql">> := FetchedSQL}} = Action,
    ?assertEqual(FetchedSQL, BadSQL),

    %% update again with the original sql
    {ok, {{_, 200, "OK"}, _Headers2, Body2}} =
        emqx_bridge_v2_testlib:update_bridge_api(Config, #{}),
    %% the error should be gone now, and status should be 'connected'
    ?assertMatch(#{<<"error">> := <<>>, <<"status">> := <<"connected">>}, Body2),
    %% finally check if ecpool worker should have exactly one of reconnect callback
    ConnectorResId = <<"connector:pgsql:", ConnectorName/binary>>,
    Workers = ecpool:workers(ConnectorResId),
    [_ | _] = WorkerPids = lists:map(fun({_, Pid}) -> Pid end, Workers),
    lists:foreach(
        fun(Pid) ->
            [{emqx_postgresql, prepare_sql_to_conn, Args}] =
                ecpool_worker:get_reconnect_callbacks(Pid),
            Sig = emqx_postgresql:get_reconnect_callback_signature(Args),
            BridgeResId = <<"action:pgsql:", BridgeName/binary, $:, ConnectorResId/binary>>,
            ?assertEqual(BridgeResId, Sig)
        end,
        WorkerPids
    ),
    ok.

%% Checks that furnishing `epgsql' a value that cannot be encoded to a timestamp results
%% in a pretty error instead of a crash.
t_bad_datetime_param(Config) ->
    {201, _} = create_connector_api(Config),
    {201, _} = create_action_api(Config, #{
        <<"parameters">> => #{
            <<"sql">> => <<
                "INSERT INTO mqtt_test(payload, arrived) "
                "VALUES (${payload}, ${payload})"
            >>
        }
    }),
    RuleTopic = <<"bad/timestamp">>,
    {ok, _} = emqx_bridge_v2_testlib:create_rule_and_action_http(
        ?config(action_type, Config), RuleTopic, Config, #{}
    ),
    Payload = <<"2024-06-30 00:10:00">>,
    ?assertMatch(
        {_,
            {ok, #{
                context := #{
                    reason := bad_param,
                    type := timestamp,
                    index := 1,
                    value := Payload
                }
            }}},
        ?wait_async_action(
            emqx:publish(emqx_message:make(RuleTopic, Payload)),
            #{?snk_kind := "postgres_bad_param_error"}
        )
    ),
    ok.
