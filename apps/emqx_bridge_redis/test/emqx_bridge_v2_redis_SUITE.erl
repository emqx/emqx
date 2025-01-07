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
-module(emqx_bridge_v2_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(BRIDGE_TYPE, redis).
-define(BRIDGE_TYPE_BIN, <<"redis">>).
-define(CONNECTOR_TYPE, redis).
-define(CONNECTOR_TYPE_BIN, <<"redis">>).

-import(emqx_common_test_helpers, [on_exit/1]).
-import(emqx_utils_conv, [bin/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_testcases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.

groups() ->
    emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_testcases()).

matrix_testcases() ->
    [
        t_start_stop,
        t_create_via_http,
        t_on_get_status,
        t_on_get_status_no_username_pass,
        t_sync_query,
        t_map_to_redis_hset_args
    ].

init_per_suite(Config) ->
    TestHosts = [
        {"redis", 6379},
        {"redis-tls", 6380},
        {"redis-sentinel", 26379},
        {"redis-sentinel-tls", 26380},
        {"redis-cluster-1", 6379},
        {"redis-cluster-2", 6379},
        {"redis-cluster-3", 6379},
        {"redis-cluster-tls-1", 6389},
        {"redis-cluster-tls-2", 6389},
        {"redis-cluster-tls-3", 6389}
    ],
    case emqx_common_test_helpers:is_all_tcp_servers_available(TestHosts) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_connector,
                    emqx_bridge_redis,
                    emqx_bridge,
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
                {enable_tls, false}
                | Config
            ],
            NConfig;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_redis);
                _ ->
                    {skip, no_redis}
            end
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(Group, Config) when
    Group =:= single;
    Group =:= sentinel;
    Group =:= cluster
->
    [{redis_type, Group} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

common_init_per_testcase(TestCase, Config) ->
    ct:timetrap(timer:seconds(60)),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_config:delete_override_conf_files(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = iolist_to_binary([atom_to_binary(TestCase), UniqueNum]),
    Username = <<"test_user">>,
    Password = <<"test_passwd">>,
    Passfile = filename:join(?config(priv_dir, Config), "passfile"),
    ok = file:write_file(Passfile, Password),
    NConfig = [
        {redis_username, Username},
        {redis_password, Password},
        {redis_passfile, Passfile}
        | Config
    ],
    Path = group_path(Config),
    ct:comment(Path),
    ConnectorConfig = connector_config(Name, Path, NConfig),
    BridgeConfig = action_config(Name, Path, Name, TestCase),
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
            emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
            emqx_common_test_helpers:call_janitor(60_000),
            ok = snabbkaffe:stop(),
            ok
    end.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, Path, Config) ->
    [RedisType, _Transport | _] = Path,
    Username = ?config(redis_username, Config),
    PassFile = ?config(redis_passfile, Config),
    CommonCfg = #{
        <<"enable">> => true,
        <<"description">> => <<"redis connector">>,
        <<"parameters">> => #{
            <<"password">> => iolist_to_binary(["file://", PassFile]),
            <<"pool_size">> => 8,
            <<"username">> => Username
        },
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"15s">>,
            <<"start_after_created">> => true,
            <<"start_timeout">> => <<"5s">>
        }
    },
    PerTypeCfg = per_type_connector_config(RedisType),
    InnerConfigMap0 = emqx_utils_maps:deep_merge(CommonCfg, PerTypeCfg),
    InnerConfigMap = serde_roundtrip(InnerConfigMap0),
    parse_and_check_connector_config(InnerConfigMap, Name).

per_type_connector_config(single) ->
    #{
        <<"parameters">> =>
            #{
                <<"database">> => <<"0">>,
                <<"server">> => <<"redis:6379">>,
                <<"redis_type">> => <<"single">>
            }
    };
per_type_connector_config(sentinel) ->
    #{
        <<"parameters">> =>
            #{
                <<"database">> => <<"0">>,
                <<"servers">> => <<"redis-sentinel:26379">>,
                <<"sentinel">> => <<"mytcpmaster">>,
                <<"redis_type">> => <<"sentinel">>
            }
    };
per_type_connector_config(cluster) ->
    #{
        <<"parameters">> =>
            #{
                <<"servers">> =>
                    <<"redis-cluster-1:6379,redis-cluster-2:6379,redis-cluster-3:6379">>,
                <<"redis_type">> => <<"cluster">>
            }
    }.

parse_and_check_connector_config(InnerConfigMap, Name) ->
    TypeBin = ?CONNECTOR_TYPE_BIN,
    RawConf = #{<<"connectors">> => #{TypeBin => #{Name => InnerConfigMap}}},
    #{<<"connectors">> := #{TypeBin := #{Name := Config}}} =
        hocon_tconf:check_plain(emqx_connector_schema, RawConf, #{
            required => false, atom_key => false
        }),
    ct:pal("parsed config: ~p", [Config]),
    InnerConfigMap.

action_config(Name, Path, ConnectorId, TestCase) ->
    Template =
        try
            ?MODULE:TestCase(command_template)
        catch
            _:_ ->
                [<<"RPUSH">>, <<"MSGS/${topic}">>, <<"${payload}">>]
        end,
    [RedisType, _Transport | _] = Path,
    CommonCfg =
        #{
            <<"enable">> => true,
            <<"connector">> => ConnectorId,
            <<"parameters">> =>
                #{
                    <<"command_template">> => Template,
                    <<"redis_type">> => atom_to_binary(RedisType)
                },
            <<"local_topic">> => <<"t/redis">>,
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
    InnerConfigMap = serde_roundtrip(CommonCfg),
    parse_and_check_bridge_config(InnerConfigMap, Name).

%% check it serializes correctly
serde_roundtrip(InnerConfigMap0) ->
    IOList = hocon_pp:do(InnerConfigMap0, #{}),
    {ok, InnerConfigMap} = hocon:binary(IOList),
    InnerConfigMap.

parse_and_check_bridge_config(InnerConfigMap, Name) ->
    emqx_bridge_v2_testlib:parse_and_check(?BRIDGE_TYPE_BIN, Name, InnerConfigMap).

make_message() ->
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    make_message_with_payload(Payload).

make_message_with_payload(Payload) ->
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    #{
        clientid => ClientId,
        payload => Payload,
        timestamp => 1668602148000
    }.

%% return the path (reverse of the stack) of the test groups.
%% root group is discarded.
group_path(Config) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] ->
            undefined;
        Path ->
            tl(Path)
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(matrix) ->
    {start_stop, [
        [single, tcp],
        [sentinel, tcp],
        [cluster, tcp]
    ]};
t_start_stop(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, redis_bridge_stopped),
    ok.

t_create_via_http(matrix) ->
    {create_via_http, [
        [single, tcp],
        [sentinel, tcp],
        [cluster, tcp]
    ]};
t_create_via_http(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(matrix) ->
    {on_get_status, [
        [single, tcp],
        [sentinel, tcp],
        [cluster, tcp]
    ]};
t_on_get_status(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => connecting}),
    ok.

t_on_get_status_no_username_pass(matrix) ->
    {on_get_status, [
        [single, tcp],
        [cluster, tcp],
        [sentinel, tcp]
    ]};
t_on_get_status_no_username_pass(Config0) when is_list(Config0) ->
    ConnectorConfig0 = ?config(connector_config, Config0),
    ConnectorConfig1 = emqx_utils_maps:deep_put(
        [<<"parameters">>, <<"password">>], ConnectorConfig0, <<"">>
    ),
    ConnectorConfig2 = emqx_utils_maps:deep_put(
        [<<"parameters">>, <<"username">>], ConnectorConfig1, <<"">>
    ),
    Config1 = proplists:delete(connector_config, Config0),
    Config2 = [{connector_config, ConnectorConfig2} | Config1],
    ?check_trace(
        emqx_bridge_v2_testlib:t_on_get_status(
            Config2,
            #{
                failure_status => disconnected,
                normal_status => disconnected
            }
        ),
        fun(ok, Trace) ->
            case ?config(redis_type, Config2) of
                single ->
                    ?assertMatch([_ | _], ?of_kind(emqx_redis_auth_required_error, Trace));
                sentinel ->
                    ?assertMatch([_ | _], ?of_kind(emqx_redis_auth_required_error, Trace));
                cluster ->
                    ok
            end
        end
    ),
    ok.

t_sync_query(matrix) ->
    {sync_query, [
        [single, tcp],
        [sentinel, tcp],
        [cluster, tcp]
    ]};
t_sync_query(Config) when is_list(Config) ->
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        fun make_message/0,
        fun(Res) -> ?assertMatch({ok, _}, Res) end,
        redis_bridge_connector_send_done
    ),
    ok.

t_map_to_redis_hset_args(matrix) ->
    {map_to_redis_hset_args, [
        [single, tcp],
        [sentinel, tcp],
        [cluster, tcp]
    ]};
t_map_to_redis_hset_args(command_template) ->
    [<<"HMSET">>, <<"t_map_to_redis_hset_args">>, <<"${payload}">>];
t_map_to_redis_hset_args(Config) when is_list(Config) ->
    Payload = emqx_rule_funcs:map_to_redis_hset_args(#{<<"a">> => 1, <<"b">> => <<"2">>}),
    MsgFn = fun() -> make_message_with_payload(Payload) end,
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        MsgFn,
        fun(Res) -> ?assertMatch({ok, _}, Res) end,
        redis_bridge_connector_send_done
    ),
    ok.
