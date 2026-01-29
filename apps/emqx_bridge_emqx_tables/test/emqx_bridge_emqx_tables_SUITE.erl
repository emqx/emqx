%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_emqx_tables_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_bridge_emqx_tables.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(PROXY_NAME_GRPC, "greptimedb_grpc").
-define(PROXY_NAME_TLS_GRPC, "greptimedb_tls_grpc").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(tcp, tcp).
-define(tls, tls).
-define(async, async).
-define(sync, sync).
-define(with_batch, with_batch).
-define(without_batch, without_batch).

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
            emqx_bridge_emqx_tables,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    EHttpcPoolName = <<(atom_to_binary(?MODULE))/binary, "_http">>,
    [
        {apps, Apps},
        {ehttpc_pool_name, EHttpcPoolName}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?tcp, TCConfig) ->
    [
        {query_server, <<"toxiproxy:4000">>},
        {server, <<"toxiproxy:4001">>},
        {use_tls, false},
        {proxy_name, ?PROXY_NAME_GRPC},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    [
        {query_server, <<"toxiproxy:4002">>},
        {server, <<"toxiproxy:4003">>},
        {use_tls, true},
        {proxy_name, ?PROXY_NAME_TLS_GRPC},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT}
        | TCConfig
    ];
init_per_group(?async, TCConfig) ->
    [{query_mode, async} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, sync} | TCConfig];
init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{
        <<"server">> => get_config(server, TCConfig, <<"toxiproxy:4001">>),
        <<"ssl">> => tls_opts(TCConfig)
    }),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>),
            <<"query_mode">> => get_config(query_mode, TCConfig, <<"sync">>)
        }
    }),
    ok = start_ehttpc_pool(TCConfig),
    drop_table(TCConfig),
    {ok, _} = create_simple_table(<<"mqtt">>, TCConfig),
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
    snabbkaffe:stop(),
    reset_proxy(),
    drop_table(TCConfig),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    stop_ehttpc_pool(TCConfig),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    emqx_bridge_schema_testlib:greptimedb_connector_config(Overrides).

action_config(Overrides) ->
    emqx_bridge_schema_testlib:greptimedb_action_config(Overrides).

tls_opts(TCConfig) ->
    case get_config(use_tls, TCConfig, false) of
        true ->
            Root = emqx_common_test_helpers:proj_root(),
            CertsPath = filename:join([Root, ".ci", "docker-compose-file", "greptimedb", "certs"]),
            #{
                <<"enable">> => true,
                <<"keyfile">> => bin(filename:join([CertsPath, "server.key"])),
                <<"certfile">> => bin(filename:join([CertsPath, "server.crt"])),
                %% note: the driver imposes that cacertfile is required when using tls...
                <<"cacertfile">> => bin(filename:join([CertsPath, "ca.crt"]))
            };
        false ->
            #{<<"enable">> => false}
    end.

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

bin(X) -> emqx_utils_conv:bin(X).

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

query_mode(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?sync, ?async], ?sync).

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, TCConfig, Fn) ->
    ProxyName = get_config(proxy_name, TCConfig),
    emqx_common_test_helpers:with_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT, Fn).

start_ehttpc_pool(TCConfig) ->
    emqx_bridge_greptimedb_SUITE:start_ehttpc_pool(TCConfig).

stop_ehttpc_pool(TCConfig) ->
    emqx_bridge_greptimedb_SUITE:stop_ehttpc_pool(TCConfig).

create_simple_table(TableName, TCConfig) ->
    SQL = iolist_to_binary([
        [<<"create table if not exists ">>, TableName, <<"(">>],
        <<" ts timestamp time index,">>,
        <<" clientid string,">>,
        <<" payload string,">>,
        <<" primary key (clientid)">>,
        <<")">>
    ]),
    query_by_sql(SQL, TCConfig).

drop_table(TCConfig) ->
    drop_table(<<"mqtt">>, TCConfig).

drop_table(TableName, TCConfig) ->
    query_by_sql(<<"drop table ", TableName/binary>>, TCConfig).

query_by_clientid(ClientId, TCConfig) ->
    emqx_bridge_greptimedb_SUITE:query_by_clientid(ClientId, TCConfig).

query_by_sql(SQL, TCConfig) ->
    emqx_bridge_greptimedb_SUITE:query_by_sql(SQL, TCConfig).

make_row(Schema, Rows) ->
    emqx_bridge_greptimedb_SUITE:make_row(Schema, Rows).

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

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

json_encode(X) ->
    emqx_utils_json:encode(X).

simple_write_syntax() ->
    <<"${.payload.t} payload=${.payload.p}">>.

maybe_with_forced_sync_query_mode(TCConfig, Fn) ->
    case query_mode(TCConfig) of
        ?sync ->
            emqx_bridge_v2_testlib:with_forced_sync_callback_mode(?CONNECTOR_TYPE, Fn);
        ?async ->
            Fn()
    end.

run_multiple_tables_scenario(TCConfig, Opts) ->
    #{
        tables_to_create := TablesToCreate,
        tables_to_query := Tables
    } = Opts,
    maybe_with_forced_sync_query_mode(TCConfig, fun() ->
        {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
        {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
            <<"parameters">> => #{<<"write_syntax">> => simple_write_syntax()},
            <<"resource_opts">> => #{
                %% So that all messages are buffered by the same worker
                <<"worker_pool_size">> => 1,
                %% So that we don't block the publishing client, even though we set
                %% this in the test case setup.  We force the callback mode above.
                <<"query_mode">> => <<"async">>
            }
        }),
        #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
        C = start_client(),
        lists:foreach(fun(T) -> drop_table(T, TCConfig) end, Tables),
        lists:foreach(
            fun(T) -> {ok, _} = create_simple_table(T, TCConfig) end,
            TablesToCreate
        ),
        {ok, {ok, _}} =
            ?wait_async_action(
                lists:foreach(
                    fun(Table) ->
                        Payload = json_encode(#{t => Table, p => Table}),
                        emqtt:publish(C, RuleTopic, Payload, [{qos, 0}])
                    end,
                    Tables
                ),
                #{?snk_kind := Kind} when
                    Kind == handle_async_reply orelse Kind == "greptime_rs_sync_batch_reply",
                5_000
            ),
        ok
    end).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [
        [Conn, ?sync, ?without_batch]
     || Conn <- [?tcp, ?tls]
    ];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "greptimedb_rs_connector_stop").

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [
        [Conn, ?sync, ?without_batch]
     || Conn <- [?tcp, ?tls]
    ];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [Conn, Sync, Batch]
     || Conn <- [?tcp, ?tls],
        Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    StartClientOpts = #{
        clean_start => true,
        proto_ver => v5,
        clientid => ClientId
    },
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?retry(
            200,
            10,
            ?assertMatch(
                #{<<"payload">> := Payload},
                query_by_clientid(ClientId, TCConfig)
            )
        ),
        ?retry(
            500,
            5,
            ?assertMatch(
                {200, #{
                    <<"metrics">> := #{
                        <<"matched">> := 1,
                        <<"success">> := 1,
                        <<"failed">> := 0
                    }
                }},
                emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig)
            )
        ),
        ok
    end,
    Opts = #{
        start_client_opts => StartClientOpts,
        post_publish_fn => PostPublishFn
    },
    maybe_with_forced_sync_query_mode(TCConfig, fun() ->
        emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts)
    end).

-doc """
Smoke test to exercise the code path where a batch with more than 1 record is handled.
""".
t_batch() ->
    [{matrix, true}].
t_batch(matrix) ->
    [
        [?tcp, Sync, ?with_batch]
     || Sync <- [?sync, ?async]
    ];
t_batch(TCConfig) ->
    NumMsgs = 3,
    ActionOverrides = #{
        <<"resource_opts">> => #{<<"worker_pool_size">> => 1}
    },
    PublishFn = fun(#{rule_topic := RuleTopic, payload_fn := PayloadFnIn} = Context) ->
        Payload = PayloadFnIn(),
        emqx_utils:pforeach(
            fun(_) ->
                C = start_client(),
                emqtt:publish(C, RuleTopic, Payload, [{qos, 0}]),
                ok = emqtt:stop(C)
            end,
            lists:seq(1, NumMsgs)
        ),
        Context#{payload => Payload}
    end,
    PostPublishFn = fun(_Context) ->
        ?retry(
            500,
            5,
            ?assertMatch(
                {200, #{
                    <<"metrics">> := #{
                        <<"matched">> := 3,
                        <<"success">> := 3,
                        <<"failed">> := 0
                    }
                }},
                emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig)
            )
        ),
        ok
    end,
    TraceChecker = fun(Trace) ->
        ?assertMatch(
            [#{batch_or_query := [_, _ | _]}],
            ?of_kind(buffer_worker_flush_ack, Trace)
        ),
        ok
    end,
    Opts = #{
        action_overrides => ActionOverrides,
        publish_fn => PublishFn,
        post_publish_fn => PostPublishFn,
        trace_checkers => [TraceChecker]
    },
    maybe_with_forced_sync_query_mode(TCConfig, fun() ->
        emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts)
    end).

-doc """
Verifies case where batch resolves to multiple tables, and all succeed.
""".
t_multiple_tables_success() ->
    [{matrix, true}].
t_multiple_tables_success(matrix) ->
    [
        [?tcp, Sync, ?with_batch]
     || Sync <- [?sync, ?async]
    ];
t_multiple_tables_success(TCConfig) when is_list(TCConfig) ->
    Tables = [<<"t1">>, <<"t2">>, <<"t3">>],
    ?check_trace(
        run_multiple_tables_scenario(TCConfig, #{
            tables_to_query => Tables,
            tables_to_create => Tables
        }),
        fun(Trace) ->
            case query_mode(TCConfig) of
                ?async ->
                    ?assertMatch(
                        [
                            #{
                                result := [
                                    {ok, _},
                                    {ok, _},
                                    {ok, _}
                                ]
                            }
                        ],
                        ?of_kind(handle_async_reply, Trace)
                    );
                ?sync ->
                    ?assertMatch(
                        [
                            #{
                                results := [
                                    {ok, _},
                                    {ok, _},
                                    {ok, _}
                                ]
                            }
                        ],
                        ?of_kind("greptime_rs_sync_batch_reply", Trace)
                    )
            end
        end
    ),
    ok.

-doc """
Verifies case where batch resolves to multiple tables, and one in the middle fails.

Checks that we report which tables succeeded and which did not.
""".
t_multiple_tables_failure_in_the_middle() ->
    [{matrix, true}].
t_multiple_tables_failure_in_the_middle(matrix) ->
    [
        [?tcp, Sync, ?with_batch]
     || Sync <- [?sync, ?async]
    ];
t_multiple_tables_failure_in_the_middle(TCConfig) when is_list(TCConfig) ->
    Tables = [<<"t1">>, <<"t2">>, <<"t3">>],
    ?check_trace(
        run_multiple_tables_scenario(TCConfig, #{
            tables_to_query => Tables,
            tables_to_create => Tables -- [<<"t2">>]
        }),
        fun(Trace) ->
            case query_mode(TCConfig) of
                ?async ->
                    ?assertMatch(
                        [
                            #{
                                result := [
                                    {ok, _},
                                    {ok, _},
                                    {ok, _}
                                ]
                            }
                        ],
                        ?of_kind(handle_async_reply, Trace)
                    );
                ?sync ->
                    ?assertMatch(
                        [
                            #{
                                results := [
                                    {ok, _},
                                    {ok, _},
                                    {ok, _}
                                ]
                            }
                        ],
                        ?of_kind("greptime_rs_sync_batch_reply", Trace)
                    )
            end
        end
    ),
    ok.

-doc """
Verifies case where batch resolves to multiple tables, and the last one fails.
""".
t_multiple_tables_failure_in_the_end() ->
    [{matrix, true}].
t_multiple_tables_failure_in_the_end(matrix) ->
    [
        [?tcp, Sync, ?with_batch]
     || Sync <- [?sync, ?async]
    ];
t_multiple_tables_failure_in_the_end(TCConfig) when is_list(TCConfig) ->
    Tables = [<<"t1">>, <<"t2">>, <<"t3">>],
    ?check_trace(
        run_multiple_tables_scenario(TCConfig, #{
            tables_to_query => Tables,
            tables_to_create => Tables -- [<<"t3">>]
        }),
        fun(Trace) ->
            case query_mode(TCConfig) of
                ?async ->
                    ?assertMatch(
                        [
                            #{
                                result := [
                                    {ok, _},
                                    {ok, _},
                                    {ok, _}
                                ]
                            }
                        ],
                        ?of_kind(handle_async_reply, Trace)
                    );
                ?sync ->
                    ?assertMatch(
                        [
                            #{
                                results := [
                                    {ok, _},
                                    {ok, _},
                                    {ok, _}
                                ]
                            }
                        ],
                        ?of_kind("greptime_rs_sync_batch_reply", Trace)
                    )
            end
        end
    ),
    ok.

-doc """
Checks that we treat port 4001 as the default port when the port is omitted in the server
field, similar to greptimedb connector.
""".
t_default_port() ->
    [{matrix, true}].
t_default_port(matrix) ->
    [[?tcp, ?sync, ?without_batch]];
t_default_port(TCConfig) when is_list(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{
            <<"server">> => <<"toxiproxy">>
        })
    ),
    ok.
