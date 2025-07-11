%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_bigquery_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_bridge_bigquery.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(PROXY_NAME, "bigquery").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).
-define(PORT, 9050).

-define(PREPARED_REQUEST(METHOD, PATH, BODY),
    {prepared_request, {METHOD, PATH, BODY}, #{request_ttl => 1_000}}
).

-define(batching, batching).
-define(not_batching, not_batching).
-define(sync, sync).
-define(async, async).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_cases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.

matrix_cases() ->
    lists:filter(
        fun(TestCase) ->
            get_tc_prop(TestCase, matrix, false)
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

groups() ->
    emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()).

init_per_suite(TCConfig) ->
    Server = render_str("${host}:${port}", #{host => ?PROXY_HOST, port => ?PORT}),
    os:putenv("BIGQUERY_EMULATOR_HOST", Server),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_bigquery,
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

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    os:unsetenv("BIGQUERY_EMULATOR_HOST"),
    ok.

init_per_testcase(TestCase, TCConfig0) ->
    reset_proxy(),
    Path = group_path(TCConfig0, no_groups),
    ct:print(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    UniqueNum = integer_to_binary(
        erlang:monotonic_time() band 16#ffff
    ),
    %% The currently used emulator is extremely buggy, and does not like tables that have
    %% the same name as datasets....
    Dataset = render_bin(<<"ds_${t}${n}">>, #{t => TestCase, n => UniqueNum}),
    Table = render_bin(<<"tab_${t}${n}">>, #{t => TestCase, n => UniqueNum}),
    ConnectorName = atom_to_binary(TestCase),
    ServiceAccountJSON =
        #{<<"project_id">> := ProjectId} =
        emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ConnectorConfig = connector_config(ServiceAccountJSON),
    ActionName = ConnectorName,
    ActionConfig = action_config(
        #{
            <<"connector">> => ConnectorName,
            <<"parameters">> => #{
                <<"dataset">> => Dataset,
                <<"table">> => Table
            }
        },
        TCConfig0
    ),
    Client = start_control_client(),
    TCConfig = [
        {client, Client},
        {project_id, ProjectId},
        {service_account_json, ServiceAccountJSON}
        | TCConfig0
    ],
    snabbkaffe:start_trace(),
    create_dataset_and_table(Dataset, Table, TCConfig),
    maybe
        ?sync ?= query_mode(TCConfig),
        force_sync_callback_mode()
    end,
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {dataset, Dataset},
        {table, Table}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    reset_proxy(),
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    stop_control_client(TCConfig),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(ServiceAccountJSON) ->
    connector_config(ServiceAccountJSON, _Overrides = #{}).

connector_config(ServiceAccountJSON, Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"connect_timeout">> => <<"15s">>,
        <<"max_inactive">> => <<"10s">>,
        <<"max_retries">> => 2,
        <<"pipelining">> => 100,
        <<"pool_size">> => 8,
        <<"service_account_json">> => emqx_utils_json:encode(ServiceAccountJSON),
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides, TCConfig) ->
    Defaults = #{
        <<"parameters">> => #{
            <<"dataset">> => <<"please override">>,
            <<"table">> => <<"please override">>
        },
        <<"resource_opts">> =>
            merge_many([
                emqx_bridge_v2_testlib:common_action_resource_opts(),
                batching_opts_of(TCConfig),
                query_mode_opts_of(TCConfig)
            ])
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
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

get_matrix_prop(TCConfig, Alternatives, Default) ->
    GroupPath = group_path(TCConfig, [Default]),
    case lists:filter(fun(G) -> lists:member(G, Alternatives) end, GroupPath) of
        [] ->
            Default;
        [Opt] ->
            Opt
    end.

is_batching(TCConfig) ->
    get_matrix_prop(TCConfig, [?not_batching, ?batching], ?not_batching).

batching_opts_of(TCConfig) ->
    case is_batching(TCConfig) of
        ?batching ->
            #{
                <<"batch_size">> => 10,
                <<"batch_time">> => <<"100ms">>
            };
        ?not_batching ->
            #{
                <<"batch_size">> => 1,
                <<"batch_time">> => <<"0ms">>
            }
    end.

query_mode(TCConfig) ->
    get_matrix_prop(TCConfig, [?sync, ?async], ?sync).

query_mode_opts_of(TCConfig) ->
    case query_mode(TCConfig) of
        ?sync ->
            #{<<"query_mode">> => <<"sync">>};
        ?async ->
            #{<<"query_mode">> => <<"async">>}
    end.

force_sync_callback_mode() ->
    on_exit(fun() ->
        persistent_term:erase({emqx_bridge_bigquery_impl, callback_mode})
    end),
    persistent_term:put({emqx_bridge_bigquery_impl, callback_mode}, always_sync).

merge_many(Maps) ->
    lists:foldr(fun maps:merge/2, #{}, Maps).

get_value(Key, TCConfig) ->
    emqx_bridge_v2_testlib:get_value(Key, TCConfig).

start_control_client() ->
    RawServiceAccount = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ClientConfig =
        #{
            connect_timeout => 5_000,
            max_retries => 0,
            pool_size => 1,
            service_account_json => RawServiceAccount,
            jwt_opts => #{aud => <<"https://bigquery.googleapis.com/">>},
            transport => tcp,
            host => "bigquery",
            port => ?PORT
        },
    PoolName = <<"control_connector">>,
    {ok, Client} = emqx_bridge_gcp_pubsub_client:start(PoolName, ClientConfig),
    Client.

stop_control_client(TCConfig) ->
    Client = get_value(client, TCConfig),
    ok = emqx_bridge_gcp_pubsub_client:stop(Client),
    ok.

render(TemplateStr, Context) ->
    Template = emqx_template:parse(TemplateStr),
    emqx_template:render_strict(Template, Context).

render_bin(TemplateStr, Context) ->
    iolist_to_binary(render(TemplateStr, Context)).

render_str(TemplateStr, Context) ->
    binary_to_list(render_bin(TemplateStr, Context)).

do_create_dataset(Dataset, TCConfig) ->
    ProjectId = get_value(project_id, TCConfig),
    Client = get_value(client, TCConfig),
    Method = post,
    Path = render(<<"/bigquery/v2/projects/${project_id}/datasets">>, #{
        project_id => ProjectId
    }),
    Body = emqx_utils_json:encode(#{
        <<"datasetReference">> => #{<<"datasetId">> => Dataset}
    }),
    on_exit(fun() -> do_delete_dataset(Dataset, TCConfig) end),
    {ok, _} = emqx_bridge_gcp_pubsub_client:query_sync(
        ?PREPARED_REQUEST(Method, Path, Body),
        Client
    ),
    ct:pal("dataset ~s created", [Dataset]),
    ok.

do_delete_dataset(Dataset, TCConfig) ->
    ProjectId = ?config(project_id, TCConfig),
    Client = ?config(client, TCConfig),
    Method = delete,
    Path = render(<<"/bigquery/v2/projects/${project_id}/datasets/${dataset}">>, #{
        project_id => ProjectId,
        dataset => Dataset
    }),
    Body = <<"">>,
    {ok, _} = emqx_bridge_gcp_pubsub_client:query_sync(
        ?PREPARED_REQUEST(Method, Path, Body),
        Client
    ),
    ct:pal("dataset ~s deleted", [Dataset]),
    ok.

do_create_table(Dataset, Table, TCConfig) ->
    ProjectId = get_value(project_id, TCConfig),
    Client = get_value(client, TCConfig),
    Method = post,
    Path = render(<<"/bigquery/v2/projects/${project_id}/datasets/${dataset}/tables">>, #{
        project_id => ProjectId,
        dataset => Dataset
    }),
    Body = emqx_utils_json:encode(#{
        <<"tableReference">> => #{
            <<"tableId">> => Table
        },
        <<"schema">> => #{
            <<"fields">> => [
                #{<<"name">> => <<"clientid">>, <<"type">> => <<"STRING">>},
                #{<<"name">> => <<"payload">>, <<"type">> => <<"BYTES">>},
                #{<<"name">> => <<"topic">>, <<"type">> => <<"STRING">>},
                #{<<"name">> => <<"publish_received_at">>, <<"type">> => <<"TIMESTAMP">>}
            ]
        }
    }),
    on_exit(fun() -> do_delete_table(Dataset, Table, TCConfig) end),
    {ok, _} = emqx_bridge_gcp_pubsub_client:query_sync(
        ?PREPARED_REQUEST(Method, Path, Body),
        Client
    ),
    ct:pal("table ~s created", [Table]),
    ok.

do_delete_table(Dataset, Table, TCConfig) ->
    ProjectId = get_value(project_id, TCConfig),
    Client = get_value(client, TCConfig),
    Method = delete,
    Path = render(<<"/bigquery/v2/projects/${project_id}/datasets/${dataset}/tables/${table}">>, #{
        project_id => ProjectId,
        dataset => Dataset,
        table => Table
    }),
    Body = <<"">>,
    Res = emqx_bridge_gcp_pubsub_client:query_sync(
        ?PREPARED_REQUEST(Method, Path, Body),
        Client
    ),
    case Res of
        {ok, _} ->
            ok;
        {error, #{status_code := 404}} ->
            ok
    end,
    ct:pal("table ~s deleted", [Table]),
    ok.

create_dataset_and_table(Dataset, Table, TCConfig) ->
    ok = do_create_dataset(Dataset, TCConfig),
    ok = do_create_table(Dataset, Table, TCConfig),
    ok.

scan_table(TCConfig) ->
    Dataset = get_value(dataset, TCConfig),
    Table = get_value(table, TCConfig),
    scan_table(Dataset, Table, TCConfig).

scan_table(Dataset, Table, TCConfig) ->
    ProjectId = get_value(project_id, TCConfig),
    Client = get_value(client, TCConfig),
    Method = get,
    Path = render(
        <<"/bigquery/v2/projects/${project_id}/datasets/${dataset}/tables/${table}/data">>, #{
            project_id => ProjectId,
            dataset => Dataset,
            table => Table
        }
    ),
    Body = <<"">>,
    {ok, #{body := RespBodyRaw}} = emqx_bridge_gcp_pubsub_client:query_sync(
        ?PREPARED_REQUEST(Method, Path, Body),
        Client
    ),
    #{<<"rows">> := Rows} = emqx_utils_json:decode(RespBodyRaw),
    lists:map(
        fun(#{<<"f">> := Row}) ->
            lists:map(fun(#{<<"v">> := V}) -> V end, Row)
        end,
        Rows
    ).

set_insert_all_mock(MockFn) ->
    on_exit(fun meck:unload/0),
    ok = meck:new(ehttpc, [passthrough, no_history]),
    ok = meck:expect(ehttpc, request, fun(PoolName, Method, Request, RequestTTL, MaxRetries) ->
        Path = element(1, Request),
        case re:run(Path, <<"/insertAll$">>, [{capture, none}]) of
            match ->
                MockFn(#{});
            nomatch ->
                meck:passthrough([PoolName, Method, Request, RequestTTL, MaxRetries])
        end
    end),
    ok = meck:expect(ehttpc, request_async, fun(
        PoolName, Method, Request, RequestTTL, ReplyFnAndArgs
    ) ->
        Path = element(1, Request),
        case re:run(Path, <<"/insertAll$">>, [{capture, none}]) of
            match ->
                Res = MockFn(#{}),
                spawn_link(fun() ->
                    {F, Args} = ReplyFnAndArgs,
                    apply(F, Args ++ [Res])
                end),
                {ok, self()};
            nomatch ->
                meck:passthrough([PoolName, Method, Request, RequestTTL, ReplyFnAndArgs])
        end
    end),
    ok.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_action_api(TCConfig)
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "bigquery_connector_stop").

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [?sync, ?not_batching],
        [?sync, ?batching],
        [?async, ?not_batching],
        [?async, ?batching]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload, rule_topic := Topic} = Context,
        Payload64 = base64:encode(Payload),
        ?retry(
            200,
            10,
            ?assertMatch(
                [[_ClientId, Payload64, Topic, _PublishedAt]],
                scan_table(TCConfig)
            )
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

%% Checks that we mark the resource as unhealthy if the dataset does not exist at channel
%% creation time.
t_inexistent_dataset(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ExpectedReason = iolist_to_binary(
        io_lib:format(
            "~p",
            [{unhealthy_target, <<"Table or dataset does not exist">>}]
        )
    ),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := ExpectedReason
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{<<"dataset">> => <<"idontexist">>}
        }),
        #{expected_reason => ExpectedReason}
    ),
    ok.

%% Checks that we mark the resource as unhealthy if the table does not exist at channel
%% creation time.
t_inexistent_table(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ExpectedReason = iolist_to_binary(
        io_lib:format(
            "~p",
            [{unhealthy_target, <<"Table or dataset does not exist">>}]
        )
    ),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := ExpectedReason
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{<<"table">> => <<"idontexist">>}
        }),
        #{expected_reason => ExpectedReason}
    ),
    ok.

%% Verifies that we check for insertion errors when inserting.
t_invalid_data() ->
    [{matrix, true}].
t_invalid_data(matrix) ->
    [[?sync], [?async]];
t_invalid_data(TCConfig) when is_list(TCConfig) ->
    %% The currently emulator doesn't seem to care about required fields missing, or
    %% unknown fields....  So we need to mock here.
    %% Apparently, BigQuery doesn't seem to respect REQUIRED fields either?!
    set_insert_all_mock(fun(_Ctx) ->
        Body = #{
            <<"insertErrors">> => [
                #{
                    <<"errors">> => [
                        #{
                            <<"debugInfo">> => <<"">>,
                            <<"location">> => <<"unknown_column">>,
                            <<"message">> => <<"no such field: unknown_column.">>,
                            <<"reason">> => <<"invalid">>
                        }
                    ]
                }
            ]
        },
        {ok, 200, [], emqx_utils_json:encode(Body)}
    end),
    PostPublishFn = fun(_Context) ->
        ?retry(
            750,
            10,
            ?assertMatch(
                {200, #{
                    <<"metrics">> := #{
                        <<"matched">> := 1,
                        <<"success">> := 0,
                        <<"failed">> := 1
                    }
                }},
                emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig)
            )
        )
    end,
    TraceChecker = fun(Trace) ->
        ?assertMatch([_ | _], ?of_kind("bigquery_insert_errors", Trace)),
        ok
    end,
    Opts = #{
        post_publish_fn => PostPublishFn,
        trace_checkers => [TraceChecker]
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

%% Verifies that we check for 403 errors when inserting.
t_forbidden() ->
    [{matrix, true}].
t_forbidden(matrix) ->
    [[?sync], [?async]];
t_forbidden(TCConfig) when is_list(TCConfig) ->
    set_insert_all_mock(fun(_Ctx) ->
        Body = #{
            <<"error">> => #{
                <<"code">> => 403,
                <<"errors">> => [
                    #{
                        <<"domain">> => <<"global">>,
                        <<"message">> => <<
                            "Access Denied: Dataset myproject:test_tmg: "
                            "Permission bigquery.datasets.get denied on dataset "
                            "myproject:test_tmg (or it may not exist)."
                        >>,
                        <<"reason">> => <<"accessDenied">>
                    }
                ],
                <<"message">> => <<
                    "Access Denied: Dataset myproject:test_tmg: Permission"
                    "bigquery.datasets.get denied on dataset myproject:test_tmg (or it may not exist)."
                >>,
                <<"status">> => <<"PERMISSION_DENIED">>
            }
        },
        {ok, 403, [], emqx_utils_json:encode(Body)}
    end),
    PostPublishFn = fun(_Context) ->
        ?retry(
            750,
            10,
            ?assertMatch(
                {200, #{
                    <<"metrics">> := #{
                        <<"matched">> := 1,
                        <<"success">> := 0,
                        <<"failed">> := 1
                    }
                }},
                emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig)
            )
        )
    end,
    TraceChecker = fun(Trace) ->
        ?assertMatch([_ | _], ?of_kind("bigquery_insert_access_denied", Trace)),
        ok
    end,
    Opts = #{
        post_publish_fn => PostPublishFn,
        trace_checkers => [TraceChecker]
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

-doc """
Verifies that we check for table existence during action health checks, and mark the
action as `?status_disconnected` if the table ceases to exist.
""".
t_health_check_drop_table(TCConfig) when is_list(TCConfig) ->
    Dataset = get_value(dataset, TCConfig),
    Table = get_value(table, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    ok = do_delete_table(Dataset, Table, TCConfig),
    ExpectedReason = iolist_to_binary(
        io_lib:format(
            "~p",
            [{unhealthy_target, <<"Table or dataset does not exist">>}]
        )
    ),
    ?retry(
        750,
        5,
        ?assertMatch(
            {200, #{
                <<"status">> := <<"disconnected">>,
                <<"status_reason">> := ExpectedReason
            }},
            get_action_api(TCConfig)
        )
    ),
    ok.
