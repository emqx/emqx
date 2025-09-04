%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mongodb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ACTION_TYPE, mongodb).
-define(ACTION_TYPE_BIN, <<"mongodb">>).
-define(CONNECTOR_TYPE, mongodb).
-define(CONNECTOR_TYPE_BIN, <<"mongodb">>).

%% Only for single mode
-define(PROXY_NAME, "mongo_single_tcp").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(rs, rs).
-define(sharded, sharded).
-define(single, single).

-define(sync, sync).
-define(async, async).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_mongodb,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?rs, TCConfig) ->
    Host = os:getenv("MONGO_RS_HOST", "mongo1"),
    Port = list_to_integer(os:getenv("MONGO_RS_PORT", "27017")),
    [
        {mongo_host, Host},
        {mongo_port, Port},
        {mongo_type, ?rs}
        | TCConfig
    ];
init_per_group(?sharded, TCConfig) ->
    Host = os:getenv("MONGO_SHARDED_HOST", "mongosharded3"),
    Port = list_to_integer(os:getenv("MONGO_SHARDED_PORT", "27017")),
    [
        {mongo_host, Host},
        {mongo_port, Port},
        {mongo_type, ?sharded}
        | TCConfig
    ];
init_per_group(?single, TCConfig) ->
    Host = os:getenv("MONGO_SINGLE_HOST", "mongo"),
    Port = list_to_integer(os:getenv("MONGO_SINGLE_PORT", "27017")),
    [
        {mongo_host, Host},
        {mongo_port, Port},
        {mongo_type, ?single},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME}
        | TCConfig
    ];
init_per_group(?async, TCConfig) ->
    [{query_mode, async} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, sync} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnTypeConfig =
        case get_config(mongo_type, TCConfig, ?single) of
            ?single -> single_connector_config();
            ?sharded -> sharded_connector_config();
            ?rs -> rs_connector_config()
        end,
    ConnectorConfig = connector_config(ConnTypeConfig),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"query_mode">> => get_config(query_mode, TCConfig, <<"sync">>)
        }
    }),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    reset_proxy(),
    snabbkaffe:stop(),
    clear_db(TCConfig),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"srv_record">> => false,
        <<"pool_size">> => <<"8">>,
        <<"use_legacy_protocol">> => <<"auto">>,
        <<"database">> => <<"mqtt">>,
        <<"topology">> => #{
            <<"max_overflow">> => 0,
            <<"overflow_ttl">> => <<"1s">>,
            <<"overflow_check_period">> => <<"1s">>,
            <<"local_threshold_ms">> => <<"1s">>,
            <<"connect_timeout_ms">> => <<"1s">>,
            <<"socket_timeout_ms">> => <<"1s">>,
            <<"server_selection_timeout_ms">> => <<"1s">>,
            <<"wait_queue_timeout_ms">> => <<"1s">>,
            <<"heartbeat_frequency_ms">> => <<"200s">>,
            <<"min_heartbeat_frequency_ms">> => <<"1s">>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

rs_connector_config() ->
    #{
        <<"parameters">> => #{
            <<"mongo_type">> => <<"rs">>,
            <<"servers">> => <<"mongo1:27017">>,
            <<"w_mode">> => <<"unsafe">>,
            <<"r_mode">> => <<"master">>,
            <<"replica_set_name">> => <<"rs0">>
        }
    }.

sharded_connector_config() ->
    #{
        <<"parameters">> => #{
            <<"mongo_type">> => <<"sharded">>,
            <<"servers">> => <<"mongosharded3:27017">>,
            <<"w_mode">> => <<"unsafe">>
        }
    }.

single_connector_config() ->
    #{
        <<"parameters">> => #{
            <<"mongo_type">> => <<"single">>,
            <<"server">> => <<"toxiproxy:27017">>,
            <<"w_mode">> => <<"unsafe">>
        },
        %% NOTE: `mongo-single` has auth enabled, see `credentials.env`.
        <<"username">> => <<"emqx">>,
        <<"password">> => <<"passw0rd">>,
        <<"auth_source">> => <<"admin">>
    }.

merge_maps(Maps) ->
    lists:foldl(fun maps:merge/2, #{}, Maps).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"collection">> => <<"mycol">>,
            <<"payload_template">> => <<"${.}">>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

mongo_type(TCConfig) ->
    case get_config(mongo_type, TCConfig, ?single) of
        ?rs ->
            ConnectorConfig = get_config(connector_config, TCConfig),
            RSName = emqx_utils_maps:deep_get(
                [<<"parameters">>, <<"replica_set_name">>],
                ConnectorConfig
            ),
            {rs, RSName};
        ?sharded ->
            sharded;
        ?single ->
            single
    end.

clear_db(TCConfig) ->
    #{<<"parameters">> := #{<<"collection">> := Collection}} =
        get_config(action_config, TCConfig),
    {ok, Client} = mk_mongo_connection(TCConfig),
    {true, _} = mongo_api:delete(Client, Collection, _Selector = #{}),
    mongo_api:disconnect(Client),
    ?retry(200, 10, ?assertMatch([], find_all(TCConfig))),
    ok.

mk_mongo_connection(TCConfig) ->
    ConnectorConfig = get_config(connector_config, TCConfig),
    Server = emqx_utils_maps:deep_get(
        [<<"parameters">>, <<"servers">>],
        ConnectorConfig,
        emqx_utils_maps:deep_get(
            [<<"parameters">>, <<"server">>],
            ConnectorConfig,
            undefined
        )
    ),
    {true, Server} = {is_binary(Server), Server},
    #{<<"database">> := Database} = ConnectorConfig,
    #{<<"parameters">> := #{<<"collection">> := Collection}} =
        get_config(action_config, TCConfig),
    AuthOpts =
        case get_config(mongo_type, TCConfig, ?single) of
            ?single ->
                [
                    {login, <<"emqx">>},
                    {password, <<"passw0rd">>},
                    {auth_source, <<"admin">>}
                ];
            _ ->
                []
        end,
    WorkerOpts = [
        {database, Database},
        {w_mode, safe}
        | AuthOpts
    ],
    ct:pal("db: ~p\ncoll: ~p", [Database, Collection]),
    MongoType = mongo_type(TCConfig),
    mongo_api:connect(MongoType, [Server], [], WorkerOpts).

find_all(TCConfig) ->
    #{<<"parameters">> := #{<<"collection">> := Collection}} =
        get_config(action_config, TCConfig),
    find_all(Collection, TCConfig).

find_all(Collection, TCConfig) ->
    {ok, Client} = mk_mongo_connection(TCConfig),
    Filter = #{},
    Projector = #{},
    Skip = 0,
    BatchSize = 100,
    case mongo_api:find(Client, Collection, Filter, Projector, Skip, BatchSize) of
        {ok, Cursor} when is_pid(Cursor) ->
            Limit = 1_000,
            Res = mc_cursor:take(Cursor, Limit),
            mongo_api:disconnect(Client),
            {ok, Res};
        Res ->
            mongo_api:disconnect(Client),
            Res
    end.

full_matrix() ->
    [
        [Type, Sync]
     || Type <- [?single, ?rs, ?sharded],
        Sync <- [?sync, ?async]
    ].

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

probe_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:probe_connector_api2(TCConfig, Overrides).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_action_api(
            TCConfig
        )
    ).

get_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

update_connector_api(TCConfig, Overrides) ->
    #{
        connector_type := Type,
        connector_name := Name,
        connector_config := Cfg0
    } =
        emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    Cfg = emqx_utils_maps:deep_merge(Cfg0, Overrides),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_connector_api(Name, Type, Cfg)
    ).

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

get_worker_pids(TCConfig) ->
    ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    %% abusing health check api a bit...
    GetWorkerPid = fun(TopologyPid) ->
        mongoc:transaction_query(TopologyPid, fun(#{pool := WorkerPid}) -> WorkerPid end)
    end,
    {ok, WorkerPids = [_ | _]} =
        emqx_resource_pool:health_check_workers(
            ConnResId,
            GetWorkerPid,
            5_000,
            #{return_values => true}
        ),
    WorkerPids.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [
        [Type, ?sync]
     || Type <- [?single, ?rs, ?sharded]
    ];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "mongodb_connector_stop").

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [
        [Type, ?sync]
     || Type <- [?single, ?rs, ?sharded]
    ];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    full_matrix();
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?retry(
            200,
            10,
            ?assertMatch(
                {ok, [#{<<"payload">> := Payload}]},
                find_all(TCConfig),
                #{payload => Payload}
            )
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_payload_template(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"payload_template">> => <<"{\"foo\": \"${payload}\"}">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = unique_payload(),
    {ok, {ok, _}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, Payload),
            #{?snk_kind := mongo_bridge_connector_on_query_return},
            5_000
        ),
    ?assertMatch(
        {ok, [#{<<"foo">> := Payload}]},
        find_all(TCConfig)
    ),
    ok.

t_collection_template(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"payload_template">> => <<"{\"foo\": \"${payload}\"}">>,
            <<"collection">> => <<"${payload}">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = unique_payload(),
    {ok, {ok, _}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, Payload),
            #{?snk_kind := mongo_bridge_connector_on_query_return},
            5_000
        ),
    ?assertMatch(
        {ok, [#{<<"foo">> := Payload}]},
        find_all(Payload, TCConfig)
    ),
    ok.

t_mongo_date_rule_engine_functions(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"payload_template">> =>
                <<"{\"date_0\": ${date_0}, \"date_1\": ${date_1}, \"date_2\": ${date_2}}">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(
        <<
            "SELECT mongo_date() as date_0,"
            " mongo_date(1000) as date_1,"
            " mongo_date(1, 'second') as date_2 "
            " FROM \"${t}\" "
        >>,
        TCConfig
    ),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hey">>),
    ?retry(
        200,
        10,
        ?assertMatch(
            {ok, [
                #{
                    <<"date_0">> := {_, _, _},
                    <<"date_1">> := {0, 1, 0},
                    <<"date_2">> := {0, 1, 0}
                }
            ]},
            find_all(TCConfig)
        )
    ),
    ok.

t_get_status_server_selection_too_short(TCConfig) ->
    ?assertMatch(
        {400, #{<<"message">> := <<"timeout">>}},
        probe_connector_api(
            TCConfig,
            #{
                <<"topology">> => #{<<"server_selection_timeout_ms">> => <<"1ms">>}
            }
        )
    ),
    ok.

t_use_legacy_protocol_option(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(TCConfig, #{<<"use_legacy_protocol">> => true}),
    WorkerPids0 = get_worker_pids(TCConfig),
    Expected0 = maps:from_keys(WorkerPids0, true),
    LegacyOptions0 = maps:from_list([{Pid, mc_utils:use_legacy_protocol(Pid)} || Pid <- WorkerPids0]),
    ?assertEqual(Expected0, LegacyOptions0),

    {200, #{<<"status">> := <<"connected">>}} =
        update_connector_api(TCConfig, #{<<"use_legacy_protocol">> => false}),
    WorkerPids1 = get_worker_pids(TCConfig),
    Expected1 = maps:from_keys(WorkerPids1, false),
    LegacyOptions1 = maps:from_list([{Pid, mc_utils:use_legacy_protocol(Pid)} || Pid <- WorkerPids1]),
    ?assertEqual(Expected1, LegacyOptions1),

    ok.

%% Checks that we don't mangle the connector state if the underlying connector
%% implementation returns a tuple with new state.
%% See also: https://emqx.atlassian.net/browse/EMQX-13496.
t_timeout_during_connector_health_check(TCConfig) ->
    Overrides = #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"700ms">>}},
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, Overrides),
            {201, _} = create_action_api(TCConfig, Overrides),

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
                            get_connector_api(TCConfig)
                        )
                    ),
                    ?retry(
                        1_000,
                        10,
                        ?assertMatch(
                            {200, #{<<"status">> := <<"disconnected">>}},
                            get_action_api(TCConfig)
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
                    get_connector_api(TCConfig)
                )
            ),

            RuleTopic = <<"t/timeout">>,
            {ok, _} = emqx_bridge_v2_testlib:create_rule_and_action_http(
                ?ACTION_TYPE, RuleTopic, TCConfig
            ),

            %% Action should be fine, including its metrics.
            ?retry(
                1_000,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_action_api(TCConfig)
                )
            ),
            emqx:publish(emqx_message:make(RuleTopic, <<"hey">>)),
            ?retry(
                1_000,
                10,
                ?assertMatch(
                    {200, #{<<"metrics">> := #{<<"matched">> := 1}}},
                    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig)
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

-doc """
Currently, we try to find documents in a collection called "foo" to do connector health
checks.

If the connector's user does not have permissions to find stuff in such collection, it
should not render the status `?status_disconnected`.
""".
t_connector_health_check_permission_denied(TCConfig) when is_list(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{
            <<"username">> => <<"user1">>,
            <<"password">> => <<"abc123">>,
            <<"auth_source">> => <<"mqtt">>
        })
    ),
    ok.
