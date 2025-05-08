%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_s3tables_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_bridge_s3tables.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(V1, "v1").
-define(ACCESS_KEY_ID, <<"admin">>).
-define(SECRET_ACCESS_KEY, <<"password">>).
-define(BUCKET, <<"testbucket">>).
-define(BASE_ENDPOINT, <<"http://iceberg-rest-proxy">>).
-define(QUERY_ENDPOINT, <<"http://query:8090">>).
%% -define(S3_HOST, <<"minio">>).
%% -define(S3_PORT, 9000).
-define(S3_HOST, <<"toxiproxy">>).
-define(S3_PORT, 19000).
-define(PROXY_NAME, "iceberg_rest").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

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
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_s3tables,
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

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:print(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(),
    ActionName = ConnectorName,
    #{ns := Ns, table := Table} = simple_setup_table(TestCase, TCConfig),
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"parameters">> => #{
            <<"namespace">> => Ns,
            <<"table">> => Table
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
        {action_config, ActionConfig},
        {namespace, Ns},
        {table, Table}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    connector_config_s3t(Overrides).

connector_config_s3t(Overrides) ->
    Defaults = #{
        <<"parameters">> => #{
            <<"account_id">> => <<"1234567890">>,
            <<"access_key_id">> => ?ACCESS_KEY_ID,
            <<"secret_access_key">> => ?SECRET_ACCESS_KEY,
            <<"base_endpoint">> => ?BASE_ENDPOINT,
            <<"bucket">> => ?BUCKET,
            <<"s3tables_arn">> => iolist_to_binary([
                <<"arn:aws:s3tables:sa-east-1:123456789012:bucket/">>,
                ?BUCKET
            ]),
            <<"request_timeout">> => <<"10s">>,
            <<"s3_client">> => #{
                <<"host">> => ?S3_HOST,
                <<"port">> => ?S3_PORT
            }
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"parameters">> => #{
            <<"aggregation">> => #{
                <<"time_interval">> => <<"1s">>,
                <<"max_records">> => 3
            },
            <<"s3">> => #{
                <<"min_part_size">> => <<"5mb">>,
                <<"max_part_size">> => <<"10mb">>
            }
        },
        <<"resource_opts">> =>
            maps:merge(
                emqx_bridge_v2_testlib:common_action_resource_opts(),
                #{
                    <<"batch_size">> => 10,
                    <<"batch_time">> => <<"100ms">>
                }
            )
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

make_client() ->
    Params = #{
        account_id => <<"1234567890">>,
        access_key_id => ?ACCESS_KEY_ID,
        secret_access_key => ?SECRET_ACCESS_KEY,
        base_endpoint => ?BASE_ENDPOINT,
        bucket => ?BUCKET
    },
    {ok, Client} = emqx_bridge_s3tables_client_s3t:new(Params),
    Client.

%% Note: namespace is a list of strings.
create_namespace(Client, Namespace) ->
    Payload = #{
        <<"namespace">> => Namespace,
        <<"properties">> => #{}
    },
    Context = #{
        method => post,
        path_parts => [?V1, arn, "namespaces"],
        headers => [{"content-type", "application/json"}],
        query_params => [],
        payload => emqx_utils_json:encode(Payload)
    },
    emqx_bridge_s3tables_client_s3t:do_request(Client, Context).

%% Note: namespace is a list of strings.
delete_namespace(Client, Namespace) ->
    Context = #{
        method => delete,
        path_parts => [?V1, arn, "namespaces", Namespace],
        headers => [],
        query_params => [],
        payload => <<"">>
    },
    emqx_bridge_s3tables_client_s3t:do_request(Client, Context).

ensure_namespace_created(Client, Namespace) ->
    on_exit(fun() -> ensure_namespace_deleted(Client, Namespace) end),
    case create_namespace(Client, Namespace) of
        {ok, _} ->
            ct:pal("namespace ~p created", [Namespace]),
            ok;
        {error, {http_error, 409, _, _, _}} ->
            ct:pal("namespace ~p already exists", [Namespace]),
            ok;
        {error, Reason} ->
            error(Reason)
    end.

ensure_namespace_deleted(Client, Namespace) ->
    case delete_namespace(Client, Namespace) of
        {ok, _} ->
            ct:pal("namespace ~p deleted", [Namespace]),
            ok;
        {error, {http_error, 404, _, _, _}} ->
            ct:pal("namespace ~p already gone", [Namespace]),
            ok;
        {error, Reason} ->
            error(Reason)
    end.

join_ns(Namespace) ->
    iolist_to_binary(lists:join(".", Namespace)).

create_table(Client, Namespace, Table, Schema, ExtraOpts) ->
    Payload0 = #{
        <<"name">> => Table,
        <<"schema">> => Schema,
        <<"stage-create">> => false
    },
    Payload = maps:merge(Payload0, ExtraOpts),
    Context = #{
        method => post,
        path_parts => [?V1, arn, "namespaces", join_ns(Namespace), "tables"],
        headers => [{"content-type", "application/json"}],
        query_params => [],
        payload => emqx_utils_json:encode(Payload)
    },
    emqx_bridge_s3tables_client_s3t:do_request(Client, Context).

delete_table(Client, Namespace, Table) ->
    Context = #{
        method => delete,
        path_parts => [?V1, arn, "namespaces", join_ns(Namespace), "tables", Table],
        headers => [],
        query_params => [],
        payload => <<"">>
    },
    emqx_bridge_s3tables_client_s3t:do_request(Client, Context).

ensure_table_created(Client, Namespace, Table, Schema, Opts) ->
    on_exit(fun() -> ensure_table_deleted(Client, Namespace, Table) end),
    case create_table(Client, Namespace, Table, Schema, Opts) of
        {ok, _} ->
            ct:pal("table ~p.~p created", [Namespace, Table]),
            ok;
        {error, {http_error, 409, _, _, _}} ->
            ct:pal("table ~p.~p already exists", [Namespace, Table]),
            ok;
        {error, Reason} ->
            error(Reason)
    end.

ensure_table_deleted(Client, Namespace, Table) ->
    case delete_table(Client, Namespace, Table) of
        {ok, _} ->
            ct:pal("table ~p.~p deleted", [Namespace, Table]),
            ok;
        {error, {http_error, 404, _, _, _}} ->
            ct:pal("table ~p.~p already gone", [Namespace, Table]),
            ok;
        {error, Reason} ->
            error(Reason)
    end.

simple_schema1() ->
    #{
        <<"type">> => <<"struct">>,
        <<"fields">> => [
            #{
                <<"id">> => 1,
                <<"name">> => <<"col_str">>,
                <<"type">> => <<"string">>,
                <<"required">> => true
            },
            #{
                <<"id">> => 2,
                <<"name">> => <<"col_long">>,
                <<"type">> => <<"long">>,
                <<"required">> => false
            },
            #{
                <<"id">> => 3,
                <<"name">> => <<"col_fixed">>,
                <<"type">> => <<"fixed[16]">>,
                <<"required">> => false
            },
            #{
                <<"id">> => 4,
                <<"name">> => <<"col_decimal">>,
                <<"type">> => <<"decimal(8,2)">>,
                <<"required">> => false
            },
            #{
                <<"id">> => 5,
                <<"name">> => <<"col_int">>,
                <<"type">> => <<"int">>,
                <<"required">> => false
            }
        ]
    }.

simple_schema1_partition_spec1() ->
    #{
        <<"spec-id">> => 1,
        <<"fields">> => [
            #{
                <<"field-id">> => 1001,
                <<"source-id">> => 1,
                <<"name">> => <<"str">>,
                <<"transform">> => <<"identity">>
            },
            #{
                <<"field-id">> => 1002,
                <<"source-id">> => 5,
                <<"name">> => <<"int">>,
                <<"transform">> => <<"identity">>
            }
        ]
    }.

simple_payload_all() ->
    simple_payload_all(_Overrides = #{}).

simple_payload_all(Overrides) ->
    Defaults = #{
        <<"str">> => emqx_guid:to_hexstr(emqx_guid:gen()),
        <<"long">> => 12345678910,
        <<"int">> => 4321,
        %% Those become ints in binary form, and the byte size comes from
        %% `emqx_bridge_s3tables_logic:decimal_required_bytes(Precision)`.
        %% ex: 1.234567E+9 (Precision=7) -> <<1234567:(4*8)/signed-big>>
        <<"decimal">> => base64:encode(<<123456_78:(4 * 8)/signed-big>>),
        <<"fixed">> => <<"123456789ABCDEF0">>
    },
    maps:merge(Defaults, Overrides).

simple_payload_null() ->
    simple_payload_null(_Overrides = #{}).

%% N.B.: rule engine always adds the keys defined in the select statement to the
%% resulting map, and assigns `undefined` for missing values.  In turn, `erlavro`
%% does not interpret `undefined` as `null`...  So we have to explicitly set those
%% fields to `null` here to avoid it failing to encode....
simple_payload_null(Overrides) ->
    Defaults = #{
        <<"str">> => null,
        <<"long">> => null,
        <<"int">> => null,
        <<"decimal">> => null,
        <<"fixed">> => null
    },
    maps:merge(Defaults, Overrides).

simple_setup_table(TestCase, TCConfig) ->
    Client = make_client(),
    TestCaseBin = atom_to_binary(TestCase),
    N = erlang:unique_integer([positive]),
    Name = <<TestCaseBin/binary, "_", (integer_to_binary(N))/binary>>,
    Namespace = [Name],
    Table = Name,
    Schema = simple_schema1(),
    ok = ensure_namespace_created(Client, Namespace),
    ExtraOptsFn = get_tc_prop(TestCase, table_extra_opts_fn, fun(_) -> #{} end),
    ExtraOpts = ExtraOptsFn(TCConfig),
    ok = ensure_table_created(Client, Namespace, Table, Schema, ExtraOpts),
    #{ns => join_ns(Namespace), table => Table}.

scan_table(Namespace, Table) ->
    Method = get,
    URI = iolist_to_binary(
        lists:join(
            "/",
            [
                ?QUERY_ENDPOINT,
                "scan",
                Namespace,
                Table
            ]
        )
    ),
    {ok, {{_, 200, _}, _, Body}} = httpc:request(Method, {URI, []}, [], [{body_format, binary}]),
    emqx_utils_json:decode(Body).

get_table_partitions(Namespace, Table) ->
    Method = get,
    URI = iolist_to_binary(
        lists:join(
            "/",
            [
                ?QUERY_ENDPOINT,
                "partitions",
                Namespace,
                Table
            ]
        )
    ),
    {ok, {{_, 200, _}, _, Body}} = httpc:request(Method, {URI, []}, [], [{body_format, binary}]),
    Res0 = emqx_utils_json:decode(Body),
    Res1 = maps:update_with(<<"from-data">>, fun lists:sort/1, Res0),
    maps:update_with(<<"from-meta">>, fun lists:sort/1, Res1).

spark_sql(SQL) ->
    Method = post,
    URI = iolist_to_binary(lists:join("/", [?QUERY_ENDPOINT, "sql"])),
    {ok, {{_, 200, _}, _, Body}} = httpc:request(
        Method,
        {URI, [], "application/sql", SQL},
        [],
        [{body_format, binary}]
    ),
    emqx_utils_json:decode(Body).

render(Template, Context) ->
    Parsed = emqx_template:parse(Template),
    iolist_to_binary(emqx_template:render_strict(Parsed, Context)).

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

delete_action_api(Config) ->
    Name = emqx_bridge_v2_testlib:get_value(action_name, Config),
    emqx_bridge_v2_testlib:delete_kind_api(action, ?ACTION_TYPE, Name).

rule_sql_for_schema1(RuleTopic) ->
    render(
        <<
            "SELECT"
            "  payload.str as col_str, "
            "  payload.long as col_long, "
            "  payload.fixed as col_fixed, "
            "  case when payload.decimal = 'null' then payload.decimal "
            "       else base64_decode(payload.decimal) "
            "       end as col_decimal, "
            "  payload.int as col_int "
            " FROM \"${rule_topic}\" "
        >>,
        #{
            rule_topic => RuleTopic
        }
    ).

create_rule_for_schema1(TCConfig, RuleTopic) ->
    Opts = #{
        sql => rule_sql_for_schema1(RuleTopic)
    },
    emqx_bridge_v2_testlib:create_rule_and_action_http(?ACTION_TYPE, RuleTopic, TCConfig, Opts).

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

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, "s3tables_connector_stop").

t_rule_action() ->
    TableExtraOptsFn = fun(TCConfig) ->
        case group_path(TCConfig, [batched, not_partitioned]) of
            [_, not_partitioned] ->
                #{};
            [_, partitioned] ->
                #{<<"partition-spec">> => simple_schema1_partition_spec1()}
        end
    end,
    [
        {matrix, true},
        {table_extra_opts_fn, TableExtraOptsFn}
    ].
t_rule_action(matrix) ->
    [
        [batched, not_partitioned],
        [not_batched, not_partitioned],
        [batched, partitioned]
    ];
t_rule_action(Config) when is_list(Config) ->
    ct:timetrap({seconds, 15}),
    [IsBatched, IsPartitioned] = group_path(Config, [batched, not_partitioned]),
    Ns = emqx_bridge_v2_testlib:get_value(namespace, Config),
    Table = emqx_bridge_v2_testlib:get_value(table, Config),
    RuleTopic = atom_to_binary(?FUNCTION_NAME),
    NumPayloads =
        case IsBatched of
            batched ->
                3;
            not_batched ->
                1
        end,
    PublishFn = fun(Context) ->
        Payloads = lists:map(fun(_) -> simple_payload_all() end, lists:seq(1, NumPayloads)),
        ct:pal("publishing payloads"),
        emqx_utils:pforeach(
            fun(P0) ->
                {ok, C} = emqtt:start_link(#{clean_start => true, proto_ver => v5}),
                {ok, _} = emqtt:connect(C),
                P = emqx_utils_json:encode(P0),
                ?assertMatch({ok, _}, emqtt:publish(C, RuleTopic, P, [{qos, 2}])),
                ok = emqtt:stop(C)
            end,
            Payloads
        ),
        ct:pal("published payloads"),
        Context#{payloads => Payloads}
    end,
    PostPublishFn = fun(Context) ->
        #{payloads := Payloads0} = Context,
        Payloads = lists:sort(lists:map(fun(#{<<"str">> := S}) -> S end, Payloads0)),
        ct:pal("waiting for delivery to complete"),
        ?block_until(
            #{?snk_kind := connector_aggreg_delivery_completed, transfer := T} when
                T /= empty
        ),
        ct:pal("scanning table"),
        Rows0 = scan_table(Ns, Table),
        Rows = lists:sort(fun(#{<<"col_str">> := A}, #{<<"col_str">> := B}) -> A =< B end, Rows0),
        ExpectedRows = lists:sort(
            lists:map(
                fun(#{<<"str">> := S}) ->
                    #{
                        <<"col_fixed">> => "123456789ABCDEF0",
                        <<"col_int">> => 4321,
                        <<"col_long">> => 12345678910,
                        <<"col_decimal">> => 123456.78,
                        <<"col_str">> => S
                    }
                end,
                Payloads0
            )
        ),
        ?assertMatch(
            ExpectedRows,
            Rows,
            #{payloads => Payloads}
        ),
        case IsPartitioned of
            not_partitioned ->
                ok;
            partitioned ->
                ExpectedPartitions0 = lists:map(
                    fun(P) ->
                        #{<<"str">> => P, <<"int">> => 4321}
                    end,
                    Payloads
                ),
                ExpectedPartitions = lists:sort(ExpectedPartitions0),
                ?assertMatch(
                    #{
                        <<"from-data">> := ExpectedPartitions,
                        <<"from-meta">> := ExpectedPartitions
                    },
                    get_table_partitions(Ns, Table)
                )
        end,

        ok
    end,
    CreateBridgeFn = fun() ->
        ?assertMatch(
            {ok, _},
            emqx_bridge_v2_testlib:create_bridge_api(
                Config,
                #{
                    <<"resource_opts">> => #{
                        <<"batch_size">> => NumPayloads
                    }
                }
            )
        )
    end,
    Opts = #{
        create_bridge_fn => CreateBridgeFn,
        post_publish_fn => PostPublishFn,
        publish_fn => PublishFn,
        rule_topic => RuleTopic,
        sql => rule_sql_for_schema1(RuleTopic)
    },
    emqx_bridge_v2_testlib:t_rule_action(Config, Opts),
    ok.

t_consecutive_writes(Config) ->
    ct:timetrap({seconds, 15}),
    Ns = get_config(namespace, Config),
    Table = get_config(table, Config),
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_action_api(Config, #{}),
    RuleTopic = <<"consecutive/writes">>,
    {ok, _} = create_rule_for_schema1(Config, RuleTopic),

    {ok, C} = emqtt:start_link(#{clean_start => true, proto_ver => v5}),
    {ok, _} = emqtt:connect(C),

    Publish = fun(N) ->
        P = emqx_utils_json:encode(simple_payload_null(#{<<"str">> => integer_to_binary(N)})),
        {ok, _} = emqtt:publish(C, RuleTopic, P, [{qos, 2}])
    end,
    {ok, {ok, _}} =
        ?wait_async_action(
            lists:foreach(Publish, lists:seq(1, 3)),
            #{?snk_kind := connector_aggreg_delivery_completed, transfer := T} when
                T /= empty
        ),
    {ok, {ok, _}} =
        ?wait_async_action(
            lists:foreach(Publish, lists:seq(4, 6)),
            #{?snk_kind := connector_aggreg_delivery_completed, transfer := T} when
                T /= empty
        ),

    ?retry(200, 10, begin
        Rows0 = scan_table(Ns, Table),
        Rows = lists:sort(fun(#{<<"col_str">> := A}, #{<<"col_str">> := B}) -> A =< B end, Rows0),
        ?assertMatch(
            [
                #{<<"col_str">> := <<"1">>},
                #{<<"col_str">> := <<"2">>},
                #{<<"col_str">> := <<"3">>},
                #{<<"col_str">> := <<"4">>},
                #{<<"col_str">> := <<"5">>},
                #{<<"col_str">> := <<"6">>}
            ],
            Rows
        ),
        ok
    end),
    ok.

%% Checks that we report the action as an "unhealthy target" when either the table or the
%% namespace doesn't exist during channel creation.
t_inexistent_table(Config) ->
    {201, _} = create_connector_api(Config, #{}),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> :=
                <<"{unhealthy_target,<<\"Namespace or table does not exist", _/binary>>
        }},
        create_action_api(Config, #{
            <<"parameters">> => #{<<"table">> => <<"i_dont_exist">>}
        })
    ),
    {204, _} = delete_action_api(Config),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> :=
                <<"{unhealthy_target,<<\"Namespace or table does not exist", _/binary>>
        }},
        create_action_api(Config, #{
            <<"parameters">> => #{<<"namespace">> => <<"i_dont_exist">>}
        })
    ),
    ok.

%% Tests the upload behavior when a conflict occurs during commit.
t_conflicting_transactions() ->
    [{matrix, true}].
t_conflicting_transactions(matrix) ->
    [
        [second_commit]
        %% N.B.: the apache iceberg-rest fixture container is extremely buggy, and tends
        %% to crash and enter a corrupt state that does not recover without restarting the
        %% service/container when running this case...
        %% , [first_commit]
    ];
t_conflicting_transactions(Config) ->
    ct:timetrap({seconds, 15}),
    [Scenario] = group_path(Config, [second_commit]),
    Ns = get_config(namespace, Config),
    Table = get_config(table, Config),
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_action_api(Config, #{}),
    RuleTopic = <<"consecutive/writes">>,
    {ok, _} = create_rule_for_schema1(Config, RuleTopic),

    {ok, C} = emqtt:start_link(#{clean_start => true, proto_ver => v5}),
    {ok, _} = emqtt:connect(C),

    Publish = fun(N) ->
        P = emqx_utils_json:encode(simple_payload_null(#{<<"str">> => integer_to_binary(N)})),
        {ok, _} = emqtt:publish(C, RuleTopic, P, [{qos, 2}])
    end,

    case Scenario of
        first_commit ->
            ok;
        second_commit ->
            {ok, {ok, _}} =
                ?wait_async_action(
                    lists:foreach(Publish, lists:seq(1, 3)),
                    #{?snk_kind := connector_aggreg_delivery_completed, transfer := T} when
                        T /= empty
                )
    end,

    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := "concurrent_transactions_in_place"},
                #{?snk_kind := "s3tables_about_to_commit"}
            ),
            TestPid = self(),
            spawn_link(fun() ->
                {ok, SRef0} =
                    snabbkaffe:subscribe(
                        ?match_event(#{?snk_kind := "s3tables_upload_manifests_enter"}),
                        2,
                        infinity
                    ),
                TestPid ! subscribed,
                {ok, _} = snabbkaffe:receive_events(SRef0),
                ?tp("concurrent_transactions_in_place", #{})
            end),
            ?assertReceive(subscribed),

            lists:foreach(Publish, lists:seq(4, 9)),

            ?retry(1_000, 5, begin
                Rows0 = scan_table(Ns, Table),
                Rows = lists:sort(
                    fun(#{<<"col_str">> := A}, #{<<"col_str">> := B}) -> A =< B end, Rows0
                ),
                case Scenario of
                    first_commit ->
                        ?assertMatch(
                            [
                                #{<<"col_str">> := <<"4">>},
                                #{<<"col_str">> := <<"5">>},
                                #{<<"col_str">> := <<"6">>},
                                #{<<"col_str">> := <<"7">>},
                                #{<<"col_str">> := <<"8">>},
                                #{<<"col_str">> := <<"9">>}
                            ],
                            Rows
                        );
                    second_commit ->
                        ?assertMatch(
                            [
                                #{<<"col_str">> := <<"1">>},
                                #{<<"col_str">> := <<"2">>},
                                #{<<"col_str">> := <<"3">>},
                                #{<<"col_str">> := <<"4">>},
                                #{<<"col_str">> := <<"5">>},
                                #{<<"col_str">> := <<"6">>},
                                #{<<"col_str">> := <<"7">>},
                                #{<<"col_str">> := <<"8">>},
                                #{<<"col_str">> := <<"9">>}
                            ],
                            Rows
                        )
                end,
                ok
            end),
            ok
        end,
        []
    ),
    ok.

%% Checks that the action is marked as an "unhealthy target" when attempting to add an
%% action pointing to a table containing an unsupported data type.
t_unsupported_type(Config) ->
    {201, _} = create_connector_api(Config, #{}),
    emqx_common_test_helpers:with_mock(
        emqx_bridge_s3tables_client_s3t,
        load_table,
        fun(Client, Namespace, Table) ->
            {ok, Res} = meck:passthrough([Client, Namespace, Table]),
            #{<<"metadata">> := #{<<"schemas">> := [#{<<"fields">> := [F0 | Fs0]} = Sc0]} = M0} =
                Res,
            F = F0#{<<"type">> := <<"foobar">>},
            BadRes = Res#{
                <<"metadata">> := M0#{<<"schemas">> := [Sc0#{<<"fields">> := [F | Fs0]}]}
            },
            {ok, BadRes}
        end,
        fun() ->
            ?assertMatch(
                {201, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> :=
                        <<"{unhealthy_target,<<\"Schema contains unsupported data type: foobar\">>}">>
                }},
                create_action_api(Config, #{})
            )
        end
    ),
    ok.

%% Checks that the action is marked as an "unhealthy target" when attempting to add an
%% action whose table metadata is corrupt and we cannot find the current schema.
t_schema_not_found(Config) ->
    {201, _} = create_connector_api(Config, #{}),
    emqx_common_test_helpers:with_mock(
        emqx_bridge_s3tables_client_s3t,
        load_table,
        fun(Client, Namespace, Table) ->
            {ok, Res} = meck:passthrough([Client, Namespace, Table]),
            #{<<"metadata">> := M0} = Res,
            BadRes = Res#{<<"metadata">> := M0#{<<"current-schema-id">> := -999}},
            {ok, BadRes}
        end,
        fun() ->
            ?assertMatch(
                {201, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> :=
                        <<"{unhealthy_target,<<\"Current schema could not be found\">>}">>
                }},
                create_action_api(Config, #{})
            )
        end
    ),
    ok.

%% Checks that the action is marked as an "unhealthy target" when attempting to add an
%% action whose table metadata is corrupt and we cannot find the current partition spec.
t_partition_spec_not_found(Config) ->
    {201, _} = create_connector_api(Config, #{}),
    emqx_common_test_helpers:with_mock(
        emqx_bridge_s3tables_client_s3t,
        load_table,
        fun(Client, Namespace, Table) ->
            {ok, Res} = meck:passthrough([Client, Namespace, Table]),
            #{<<"metadata">> := M0} = Res,
            BadRes = Res#{<<"metadata">> := M0#{<<"default-spec-id">> := -999}},
            {ok, BadRes}
        end,
        fun() ->
            ?assertMatch(
                {201, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> :=
                        <<"{unhealthy_target,<<\"Current partition spec could not be found\">>}">>
                }},
                create_action_api(Config, #{})
            )
        end
    ),
    ok.

%% Checks that the action is marked as an "unhealthy target" when attempting to add an
%% action whose table metadata is corrupt and the current partition spec is invalid.
t_invalid_spec(Config) ->
    {201, _} = create_connector_api(Config, #{}),
    emqx_common_test_helpers:with_mock(
        emqx_bridge_s3tables_client_s3t,
        load_table,
        fun(Client, Namespace, Table) ->
            {ok, Res} = meck:passthrough([Client, Namespace, Table]),
            #{<<"metadata">> := #{<<"partition-specs">> := [PS0]} = M0} = Res,
            BadPS = maps:with([<<"spec-id">>], PS0),
            BadRes = Res#{<<"metadata">> := M0#{<<"partition-specs">> := [BadPS]}},
            {ok, BadRes}
        end,
        fun() ->
            ?assertMatch(
                {201, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> :=
                        <<"{unhealthy_target,<<\"Partition spec is invalid\">>}">>
                }},
                create_action_api(Config, #{})
            )
        end
    ),
    ok.

%% Checks the status reason for other errors that may occur while loading the table.
%%   * timeout
%%   * connection refused
t_error_loading_table_while_adding_channel(Config) ->
    {201, _} = create_connector_api(Config, #{
        <<"parameters">> => #{
            <<"base_endpoint">> => <<"http://toxiproxy:8181/v1">>,
            <<"request_timeout">> => <<"700ms">>
        }
    }),
    with_failure(timeout, fun() ->
        ?assertMatch(
            {201, #{
                <<"status">> := <<"disconnected">>,
                <<"status_reason">> := <<"Timeout loading table">>
            }},
            create_action_api(Config, #{})
        )
    end),

    {204, _} = delete_action_api(Config),
    with_failure(down, fun() ->
        ?assertMatch(
            {201, #{
                <<"status">> := <<"disconnected">>,
                <<"status_reason">> := <<"Connection refused">>
            }},
            create_action_api(Config, #{})
        )
    end),

    {204, _} = delete_action_api(Config),

    ok.

%% Checks that the action is marked as an "unhealthy target" when attempting to add an
%% action whose table metadata format version is unsupported (currently, only 2 is
%% supported).
t_unsupported_format_version(Config) ->
    {201, _} = create_connector_api(Config, #{}),

    TestWithVsn = fun(Vsn) ->
        emqx_common_test_helpers:with_mock(
            emqx_bridge_s3tables_client_s3t,
            load_table,
            fun(Client, Namespace, Table) ->
                {ok, Res} = meck:passthrough([Client, Namespace, Table]),
                #{<<"metadata">> := M0} = Res,
                BadM = M0#{<<"format-version">> := Vsn},
                BadRes = Res#{<<"metadata">> := BadM},
                {ok, BadRes}
            end,
            fun() ->
                ExpectedInnerMsg = iolist_to_binary(
                    io_lib:format(
                        "Table uses unsupported Iceberg format version: ~p",
                        [Vsn]
                    )
                ),
                ExpectedMsg = iolist_to_binary(
                    io_lib:format(
                        "~0p",
                        [{unhealthy_target, ExpectedInnerMsg}]
                    )
                ),
                ?assertMatch(
                    {201, #{
                        <<"status">> := <<"disconnected">>,
                        <<"status_reason">> := ExpectedMsg
                    }},
                    create_action_api(Config, #{}),
                    #{expected_msg => ExpectedMsg}
                )
            end
        )
    end,
    TestWithVsn(1),
    {204, _} = delete_action_api(Config),
    TestWithVsn(3),

    ok.

%% Checks that we correctly append new rows after a delete operation.
t_write_after_delete(Config) ->
    Ns = get_config(namespace, Config),
    Table = get_config(table, Config),

    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_action_api(Config, #{}),
    RuleTopic = <<"after/delete">>,
    {ok, _} = create_rule_for_schema1(Config, RuleTopic),

    Publish = fun(N) ->
        {ok, C} = emqtt:start_link(#{clean_start => true, proto_ver => v5}),
        {ok, _} = emqtt:connect(C),
        P = emqx_utils_json:encode(simple_payload_null(#{<<"str">> => integer_to_binary(N)})),
        {ok, _} = emqtt:publish(C, RuleTopic, P, [{qos, 2}]),
        emqtt:stop(C)
    end,

    %% Write some rows to be deleted
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_utils:pforeach(Publish, lists:seq(1, 3)),
            #{?snk_kind := connector_aggreg_delivery_completed, transfer := T} when
                T /= empty
        ),
    [] = spark_sql(
        render(<<"delete from ${ns}.${table}">>, #{
            ns => Ns,
            table => Table
        })
    ),

    ?retry(200, 10, ?assertMatch([], scan_table(Ns, Table))),

    %% New writes
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_utils:pforeach(Publish, lists:seq(10, 13)),
            #{?snk_kind := connector_aggreg_delivery_completed, transfer := T} when
                T /= empty
        ),

    ?retry(200, 10, begin
        Rows0 = scan_table(Ns, Table),
        Rows = lists:sort(fun(#{<<"col_str">> := A}, #{<<"col_str">> := B}) -> A =< B end, Rows0),
        ?assertMatch(
            [
                #{<<"col_str">> := <<"10">>},
                #{<<"col_str">> := <<"11">>},
                #{<<"col_str">> := <<"12">>},
                #{<<"col_str">> := <<"13">>}
            ],
            Rows
        ),
        ok
    end),

    ok.

%% More test ideas:
%%   * Concurrent schema change during upload.
%%   * Timeout/connection error when loading schema for the first time or when retrying
%%     commit.
