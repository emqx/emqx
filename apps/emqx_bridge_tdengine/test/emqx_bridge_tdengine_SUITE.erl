%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_tdengine_SUITE).

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

-define(CONNECTOR_TYPE, tdengine).
-define(CONNECTOR_TYPE_BIN, <<"tdengine">>).
-define(ACTION_TYPE, tdengine).
-define(ACTION_TYPE_BIN, <<"tdengine">>).

-define(PROXY_NAME, "tdengine_restful").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(TOKEN, <<"token_1234567890">>).
-define(MOCK_SERVER_PORT, 8080).

-define(cloud, cloud).
-define(not_cloud, not_cloud).
-define(sync, sync).
-define(async, async).
-define(without_batch, without_batch).
-define(with_batch, with_batch).

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
            emqx_bridge_tdengine,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?not_cloud, TCConfig) ->
    [{is_cloud, false} | TCConfig];
init_per_group(?cloud, TCConfig) ->
    emqx_tdengine_cloud_svr:start(
        ?MOCK_SERVER_PORT, ?TOKEN, direct_conn_opts()
    ),
    [{is_cloud, true} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, ?sync} | TCConfig];
init_per_group(?async, TCConfig) ->
    [{query_mode, ?async} | TCConfig];
init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?cloud, _TCConfig) ->
    emqx_tdengine_cloud_svr:stop(),
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ServerConfig =
        case get_config(is_cloud, TCConfig, false) of
            true ->
                #{
                    <<"server">> => fmt(<<"localhost:${p}">>, #{p => ?MOCK_SERVER_PORT}),
                    <<"token">> => ?TOKEN
                };
            false ->
                #{
                    <<"server">> => <<"toxiproxy:6041">>,
                    <<"username">> => <<"root">>,
                    <<"password">> => <<"taosdata">>
                }
        end,
    ConnectorConfig = connector_config(ServerConfig),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"query_mode">> => bin(get_config(query_mode, TCConfig, ?sync)),
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>)
        }
    }),
    connect_and_create_table(),
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

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    reset_proxy(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

fmt(Fmt, Args) -> emqx_bridge_v2_testlib:fmt(Fmt, Args).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"database">> => <<"mqtt">>,
            <<"sql">> => <<
                "insert into t_mqtt_msg(ts, payload) values (${payload.timestamp}, '${payload.data}')"
                "t_mqtt_msg(ts, payload) values (${payload.second_ts}, '${payload.data}')"
            >>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

bin(X) -> emqx_utils_conv:bin(X).
str(X) -> emqx_utils_conv:str(X).

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

connect_and_get_payload() ->
    connect_and_query("SELECT payload FROM t_mqtt_msg").

direct_conn_opts() ->
    [
        {host, <<"toxiproxy">>},
        {port, 6041},
        {username, <<"root">>},
        {password, <<"taosdata">>},
        {pool_size, 8}
    ].

connect_direct_tdengine() ->
    {ok, Conn} = tdengine:start_link(direct_conn_opts()),
    Conn.

with_conn(Fn) ->
    Conn = connect_direct_tdengine(),
    try
        Fn(Conn)
    after
        ok = tdengine:stop(Conn)
    end.

directly_query(Conn, Query) ->
    directly_query(Conn, Query, [{db_name, "mqtt"}]).

directly_query(Conn, Query, QueryOpts) ->
    tdengine:insert(Conn, Query, QueryOpts).

connect_and_create_table() ->
    with_conn(fun(Conn) ->
        _ = directly_query(Conn, "DROP TABLE t_mqtt_msg"),
        _ = directly_query(Conn, "DROP STABLE s_tab"),
        {ok, _} = directly_query(Conn, "CREATE DATABASE IF NOT EXISTS mqtt; USE mqtt;", []),
        {ok, _} = directly_query(
            Conn,
            "CREATE TABLE t_mqtt_msg (\n"
            "  ts timestamp,\n"
            "  payload BINARY(1024)\n"
            ");"
        ),
        {ok, _} = directly_query(
            Conn,
            "CREATE STABLE s_tab (\n"
            "  ts timestamp,\n"
            "  payload BINARY(1024)\n"
            ") TAGS (clientid BINARY(128));"
        )
    end).

connect_and_query(Query) ->
    with_conn(fun(Conn) ->
        {ok, #{<<"code">> := 0, <<"data">> := Result}} = directly_query(Conn, Query),
        Result
    end).

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

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

dummy_payload_bin() ->
    dummy_payload_bin(_OverrideFn = fun(X) -> X end).

dummy_payload_bin(OverrideFn) ->
    emqx_utils_json:encode(
        OverrideFn(
            #{
                <<"data">> => <<"HELLO">>,
                <<"timestamp">> => 1668602148000,
                <<"second_ts">> => 1668602148010
            }
        )
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, tdengine_connector_stop).

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [Cloud, Sync, Batch]
     || Cloud <- [?not_cloud, ?cloud],
        Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PayloadFn = fun dummy_payload_bin/0,
    PostPublishFn = fun(_Context) ->
        ?retry(200, 10, ?assertMatch([[<<"HELLO">>], [<<"HELLO">>]], connect_and_get_payload()))
    end,
    Opts = #{
        payload_fn => PayloadFn,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_simple_insert_undefined(TCConfig) ->
    PayloadFn = fun() ->
        dummy_payload_bin(fun(X) -> maps:remove(<<"data">>, X) end)
    end,
    PostPublishFn = fun(_Context) ->
        %% the old behavior without undefined_vars_as_null
        ?retry(
            200,
            10,
            ?assertMatch(
                [[<<"undefined">>], [<<"undefined">>]],
                connect_and_get_payload()
            )
        )
    end,
    Opts = #{
        payload_fn => PayloadFn,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_undefined_vars_as_null(TCConfig) ->
    PayloadFn = fun() ->
        dummy_payload_bin(fun(X) -> maps:remove(<<"data">>, X) end)
    end,
    PostPublishFn = fun(_Context) ->
        %% the old behavior without undefined_vars_as_null
        ?retry(
            200,
            10,
            ?assertMatch(
                [[<<"null">>], [<<"null">>]],
                connect_and_get_payload()
            )
        )
    end,
    ActionOverrides = #{
        <<"parameters">> => #{<<"undefined_vars_as_null">> => true}
    },
    Opts = #{
        payload_fn => PayloadFn,
        post_publish_fn => PostPublishFn,
        action_overrides => ActionOverrides
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_batch_insert(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Size = 5,
    Ts = erlang:system_time(millisecond),
    {_, {ok, #{result := _Result}}} =
        ?wait_async_action(
            emqx_utils:pforeach(
                fun(Idx) ->
                    SentData = #{
                        payload => <<"HELLO">>,
                        timestamp => Ts + Idx,
                        second_ts => Ts + Idx + 5000
                    },
                    Payload = emqx_utils_json:encode(SentData),
                    emqx:publish(emqx_message:make(Topic, Payload))
                end,
                lists:seq(1, Size)
            ),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    DoubleSize = Size * 2,
    ?retry(
        _Sleep = 50,
        _Attempts = 30,
        ?assertMatch(
            [[DoubleSize]],
            connect_and_query("SELECT COUNT(1) FROM t_mqtt_msg")
        )
    ),
    ok.

t_invalid_data(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {_, {ok, #{error := #{<<"code">> := 534}}}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, <<"{}">>, [{qos, 1}]),
            #{?snk_kind := tdengine_connector_query_return},
            10_000
        ),
    ok.

t_auto_create_simple_insert(TCConfig) ->
    ClientId = str(?FUNCTION_NAME),
    PayloadFn = fun() ->
        dummy_payload_bin(fun(X) ->
            X#{<<"clientid">> => ClientId}
        end)
    end,
    PostPublishFn = fun(_Context) ->
        ?assertMatch(
            [[<<"HELLO">>]],
            connect_and_query("SELECT payload FROM " ++ ClientId)
        ),
        ?assertMatch(
            [[<<"HELLO">>]],
            connect_and_query("SELECT payload FROM test_" ++ ClientId)
        ),
        ?assertMatch(
            [[0]],
            connect_and_query("DROP TABLE " ++ ClientId)
        ),
        ?assertMatch(
            [[0]],
            connect_and_query("DROP TABLE test_" ++ ClientId)
        )
    end,
    ActionOverrides = #{
        <<"parameters">> => #{
            <<"sql">> =>
                <<
                    "insert into ${payload.clientid}"
                    " USING s_tab TAGS ('${payload.clientid}')"
                    " values (${payload.timestamp}, '${payload.data}')"
                    "test_${payload.clientid} USING s_tab"
                    " TAGS ('${payload.clientid}') values (${payload.second_ts}, '${payload.data}')"
                >>
        }
    },
    Opts = #{
        payload_fn => PayloadFn,
        post_publish_fn => PostPublishFn,
        action_overrides => ActionOverrides
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_auto_create_batch_insert(TCConfig) ->
    ClientId1 = "client1",
    ClientId2 = "client2",
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"sql">> =>
                <<
                    "insert into ${payload.clientid}"
                    " USING s_tab TAGS ('${payload.clientid}')"
                    " values (${payload.timestamp}, '${payload.data}')"
                    "test_${payload.clientid} USING s_tab"
                    " TAGS ('${payload.clientid}') values (${payload.second_ts}, '${payload.data}')"
                >>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Size1 = 2,
    Size2 = 3,
    Ts = erlang:system_time(millisecond),
    {_, {ok, #{result := _Result}}} =
        ?wait_async_action(
            emqx_utils:pforeach(
                fun({Offset, ClientId, Size}) ->
                    lists:foreach(
                        fun(Idx) ->
                            SentData = #{
                                payload => <<"HELLO">>,
                                timestamp => Ts + Idx + Offset,
                                second_ts => Ts + Idx + Offset + 5000,
                                clientid => ClientId
                            },
                            Payload = emqx_utils_json:encode(SentData),
                            emqx:publish(emqx_message:make(Topic, Payload))
                        end,
                        lists:seq(1, Size)
                    )
                end,
                [{0, ClientId1, Size1}, {100, ClientId2, Size2}]
            ),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    ?retry(
        _Sleep = 50,
        _Attempts = 30,
        lists:foreach(
            fun({Table, Size}) ->
                ?assertMatch(
                    [[Size]],
                    connect_and_query("SELECT COUNT(1) FROM " ++ Table)
                )
            end,
            lists:zip(
                [ClientId1, "test_" ++ ClientId1, ClientId2, "test_" ++ ClientId2],
                [Size1, Size1, Size2, Size2]
            )
        )
    ),
    lists:foreach(
        fun(E) ->
            ?assertMatch(
                [[0]],
                connect_and_query("DROP TABLE " ++ E)
            )
        end,
        [ClientId1, ClientId2, "test_" ++ ClientId1, "test_" ++ ClientId2]
    ),
    ok.
