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
-module(emqx_bridge_v2_mongodb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ACTION_TYPE, mongodb).
-define(ACTION_TYPE_BIN, <<"mongodb">>).
-define(CONNECTOR_TYPE, mongodb).
-define(CONNECTOR_TYPE_BIN, <<"mongodb">>).

-import(emqx_common_test_helpers, [on_exit/1]).
-import(emqx_utils_conv, [bin/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    MongoHost = os:getenv("MONGO_SINGLE_HOST", "toxiproxy"),
    MongoPort = list_to_integer(os:getenv("MONGO_SINGLE_PORT", "27017")),
    ProxyHost = "toxiproxy",
    ProxyPort = 8474,
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    case emqx_common_test_helpers:is_tcp_server_available(MongoHost, MongoPort) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_connector,
                    emqx_bridge,
                    emqx_bridge_mongodb,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [
                {proxy_name, "mongo_single_tcp"},
                {proxy_host, ProxyHost},
                {proxy_port, ProxyPort},
                {apps, Apps},
                {mongo_host, MongoHost},
                {mongo_port, MongoPort}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_mongo);
                _ ->
                    {skip, no_mongo}
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
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_config:delete_override_conf_files(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = iolist_to_binary([atom_to_binary(TestCase), UniqueNum]),
    AuthSource = bin(os:getenv("MONGO_AUTHSOURCE", "admin")),
    Username = bin(os:getenv("MONGO_USERNAME", "")),
    Password = bin(os:getenv("MONGO_PASSWORD", "")),
    Passfile = filename:join(?config(priv_dir, Config), "passfile"),
    ok = file:write_file(Passfile, Password),
    NConfig = [
        {mongo_authsource, AuthSource},
        {mongo_username, Username},
        {mongo_password, Password},
        {mongo_passfile, Passfile}
        | Config
    ],
    ConnectorConfig = connector_config(Name, NConfig),
    BridgeConfig = action_config(Name, Name),
    ok = snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, Name},
        {action_config, BridgeConfig}
        | NConfig
    ].

end_per_testcase(_Testcase, Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
            emqx_common_test_helpers:call_janitor(60_000),
            ok = snabbkaffe:stop(),
            ok
    end.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, Config) ->
    MongoHost = ?config(mongo_host, Config),
    MongoPort = ?config(mongo_port, Config),
    AuthSource = ?config(mongo_authsource, Config),
    Username = ?config(mongo_username, Config),
    PassFile = ?config(mongo_passfile, Config),
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"database">> => <<"mqtt">>,
            <<"parameters">> =>
                #{
                    <<"mongo_type">> => <<"single">>,
                    <<"server">> => iolist_to_binary([MongoHost, ":", integer_to_binary(MongoPort)]),
                    <<"w_mode">> => <<"safe">>
                },
            <<"pool_size">> => 8,
            <<"srv_record">> => false,
            <<"username">> => Username,
            <<"password">> => iolist_to_binary(["file://", PassFile]),
            <<"auth_source">> => AuthSource,
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

action_config(Name, ConnectorId) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"connector">> => ConnectorId,
            <<"parameters">> =>
                #{},
            <<"local_topic">> => <<"t/mongo">>,
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
    emqx_bridge_v2_testlib:parse_and_check(?ACTION_TYPE_BIN, Name, InnerConfigMap).

shared_secret_path() ->
    os:getenv("CI_SHARED_SECRET_PATH", "/var/lib/secret").

shared_secret(client_keyfile) ->
    filename:join([shared_secret_path(), "client.key"]);
shared_secret(client_certfile) ->
    filename:join([shared_secret_path(), "client.crt"]);
shared_secret(client_cacertfile) ->
    filename:join([shared_secret_path(), "ca.crt"]);
shared_secret(rig_keytab) ->
    filename:join([shared_secret_path(), "rig.keytab"]).

make_message() ->
    Time = erlang:unique_integer(),
    BinTime = integer_to_binary(Time),
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    #{
        clientid => BinTime,
        payload => Payload,
        timestamp => Time
    }.

create_bridge_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_bridge_api(Config, Overrides)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_kind_api(Config, Overrides)
    ).

get_connector_api(Config) ->
    ConnectorName = ?config(connector_name, Config),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(
            ?CONNECTOR_TYPE,
            ConnectorName
        )
    ).

get_action_api(Config) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_action_api(
            Config
        )
    ).

delete_action_api(Config) ->
    emqx_bridge_v2_testlib:delete_kind_api(
        action,
        ?ACTION_TYPE,
        ?config(action_name, Config)
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, mongodb_stopped),
    ok.

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config),
    ok.

t_sync_query(Config) ->
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        fun make_message/0,
        fun(Res) -> ?assertEqual(ok, Res) end,
        mongo_bridge_connector_on_query_return
    ),
    ok.

%% Checks that we don't mangle the connector state if the underlying connector
%% implementation returns a tuple with new state.
%% See also: https://emqx.atlassian.net/browse/EMQX-13496.
t_timeout_during_connector_health_check(Config0) ->
    ProxyName = ?config(proxy_name, Config0),
    ProxyHost = ?config(proxy_host, Config0),
    ProxyPort = ?config(proxy_port, Config0),
    ConnectorName = ?config(connector_name, Config0),
    Overrides = #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"700ms">>}},
    Config = emqx_bridge_v2_testlib:proplist_update(
        Config0,
        connector_config,
        fun(Cfg) ->
            emqx_utils_maps:deep_merge(Cfg, Overrides)
        end
    ),
    ?check_trace(
        begin
            {201, _} = create_bridge_api(Config, Overrides),

            %% emqx_common_test_helpers:with_failure(timeout, ProxyName, ProxyHost, ProxyPort, fun() ->
            %%   ?retry(500, 10, ?assertEqual(<<"disconnected">>, GetConnectorStatus())),
            %%   ok
            %% end),
            %% Wait until it's disconnected
            emqx_common_test_helpers:with_mock(
                emqx_resource_pool,
                health_check_workers,
                fun(_Pool, _Fun, _Timeout, _Opts) -> {error, timeout} end,
                fun() ->
                    ?retry(
                        1_000,
                        10,
                        ?assertMatch(
                            {200, #{<<"status">> := <<"disconnected">>}},
                            get_connector_api(Config)
                        )
                    ),
                    ?retry(
                        1_000,
                        10,
                        ?assertMatch(
                            {200, #{<<"status">> := <<"disconnected">>}},
                            get_action_api(Config)
                        )
                    ),
                    ok
                end
            ),

            %% Wait for recovery
            ?retry(
                1_000,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(Config)
                )
            ),

            RuleTopic = <<"t/timeout">>,
            {ok, _} = emqx_bridge_v2_testlib:create_rule_and_action_http(
                ?ACTION_TYPE, RuleTopic, Config
            ),

            %% Action should be fine, including its metrics.
            ?retry(
                1_000,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_action_api(Config)
                )
            ),
            emqx:publish(emqx_message:make(RuleTopic, <<"hey">>)),
            ?retry(
                1_000,
                10,
                ?assertMatch(
                    {200, #{<<"metrics">> := #{<<"matched">> := 1}}},
                    emqx_bridge_v2_testlib:get_action_metrics_api(Config)
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind("health_check_exception", Trace)),
            ?assertEqual([], ?of_kind("remove_channel_failed", Trace)),
            ok
        end
    ),
    ok.
