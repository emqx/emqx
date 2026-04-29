%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_bigtable_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_bridge_bigtable.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(PROXY_NAME, "gcp_emulator_bigtable").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(QUERY_SERVER, "http://bigtable-query:8090").
-define(SUP, emqx_bridge_bigtable_sup).

-define(batching, batching).
-define(not_batching, not_batching).
-define(sync, sync).
-define(async, async).
-define(service_account_json, service_account_json).
-define(wif_oidc, wif_oidc).
-define(attached_service_account, attached_service_account).

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
            emqx_bridge_bigtable,
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

init_per_group(?sync, TCConfig) ->
    [{query_mode, sync} | TCConfig];
init_per_group(?async, TCConfig) ->
    [{query_mode, async} | TCConfig];
init_per_group(?batching, TCConfig) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig];
init_per_group(?not_batching, TCConfig) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig0) ->
    reset_proxy(),
    Path = group_path(TCConfig0, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ServiceAccountJSON =
        #{<<"project_id">> := _ProjectId} =
        emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    Authentication =
        case auth_of(TCConfig0) of
            ?service_account_json ->
                #{
                    <<"type">> => <<"service_account_json">>,
                    <<"service_account_json">> => emqx_utils_json:encode(ServiceAccountJSON)
                };
            ?wif_oidc ->
                wif_oidc_auth();
            ?attached_service_account ->
                attached_service_account_auth()
        end,
    ConnectorConfig = connector_config(#{
        <<"authentication">> => Authentication
    }),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"parameters">> => #{
            <<"table_id">> => ActionName
        },
        <<"resource_opts">> => #{
            <<"query_mode">> => get_config(query_mode, TCConfig0, <<"sync">>),
            <<"batch_size">> => get_config(batch_size, TCConfig0, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig0, <<"0ms">>)
        }
    }),
    delete_table(ActionName),
    create_simple_table(ActionName),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig0
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

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"url">> => <<"http://toxiproxy:8186">>,
        <<"connect_timeout">> => <<"1s">>,
        <<"pool_size">> => 4,
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
            <<"instance_id">> => <<"emqxinst">>,
            <<"table_id">> => <<"please override">>,
            <<"row_key">> => <<"clientid">>,
            <<"mutations">> => [
                #{
                    <<"type">> => <<"set_cell">>,
                    <<"family_name">> => <<"'_'">>,
                    <<"column_qualifier">> => <<"''">>,
                    <<"timestamp_micros">> => <<"publish_received_at">>,
                    <<"value">> => <<"payload">>
                }
            ]
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

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

auth_of(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(
        TCConfig,
        [?service_account_json, ?wif_oidc, ?attached_service_account],
        ?service_account_json
    ).

wif_oidc_auth() ->
    #{
        <<"type">> => <<"wif">>,
        <<"gcp_project_id">> => <<"myproject">>,
        <<"gcp_project_number">> => <<"123456789012">>,
        <<"gcp_wif_pool_id">> => <<"my-wif">>,
        <<"gcp_wif_pool_provider_id">> => <<"my-wif-provider">>,
        <<"service_account_email">> => <<"sa@myproject.iam.gserviceaccount.com">>,
        <<"initial_token">> => #{
            <<"type">> => <<"oidc_client_credentials">>,
            <<"client_id">> => <<"5e870489-067f-4a0d-aa4d-295563d8b2e9">>,
            <<"client_secret">> => <<"super oidc secret">>,
            <<"endpoint_uri">> => <<"https://my.oidc.provider/oauth2/token/uri">>,
            <<"scope">> => <<"api://03e6cfaa-bf6d-4078-b748-cb73834e37f3/.default">>
        }
    }.

attached_service_account_auth() ->
    #{
        <<"type">> => <<"attached_service_account">>
    }.

mock_wif_auth_calls() ->
    Mod = emqx_bridge_gcp_pubsub_auth_wif_worker,
    on_exit(fun meck:unload/0),
    meck:new(Mod, [passthrough]),
    meck:expect(Mod, request, fun(_Method, URL, _Headers, _Body, _ReqOpts) ->
        case URL of
            <<"https://my.oidc.provider/oauth2/token/uri">> ->
                simple_token_reply(<<"access_token">>, <<"initial_token">>);
            <<"https://sts.googleapis.com/v1/token">> ->
                simple_token_reply(<<"access_token">>, <<"gcp_access_token">>);
            <<"https://iamcredentials.googleapis.com/v1/", _/binary>> ->
                simple_token_reply(<<"accessToken">>, <<"sa_impersonation_token">>)
        end
    end),
    ok.

mock_attached_service_account_auth_calls() ->
    Mod = emqx_bridge_gcp_pubsub_client,
    on_exit(fun meck:unload/0),
    meck:new(Mod, [passthrough]),
    meck:expect(Mod, do_metadata_request, fun(#{url := URL}) ->
        case URL of
            <<
                "http://metadata.google.internal/computeMetadata"
                "/v1/project/project-id"
            >> ->
                {ok, 200, [{<<"Content-Type">>, <<"application/text">>}], <<"myproject">>};
            <<
                "http://metadata.google.internal/computeMetadata"
                "/v1/instance/service-accounts/default/token"
            >> ->
                NowS = erlang:system_time(seconds),
                ExpiresInS = NowS + 3600,
                simple_token_reply(#{
                    <<"access_token">> => <<"attached_sa_token">>,
                    <<"expires_in">> => ExpiresInS
                })
        end
    end),
    ok.

simple_token_reply(Key, Token) ->
    simple_token_reply(#{Key => Token}).

simple_token_reply(Body) ->
    {ok, 200, [{<<"Content-Type">>, <<"application/json">>}], emqx_utils_json:encode(Body)}.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

query_mode(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?sync, ?async], ?sync).

maybe_with_forced_sync_query_mode(TCConfig, Fn) ->
    case query_mode(TCConfig) of
        ?sync ->
            emqx_bridge_v2_testlib:with_forced_sync_callback_mode(?CONNECTOR_TYPE, Fn);
        ?async ->
            Fn()
    end.

create_simple_table(Name) ->
    Req = #{
        name => Name,
        column_families => [<<"_">>]
    },
    create_table(Req).

create_table(Req) ->
    #{name := Name} = Req,
    on_exit(fun() -> delete_table(Name) end),
    URL = ?QUERY_SERVER ++ "/table",
    Headers = [],
    Body = emqx_utils_json:encode(Req),
    {ok, {{_, 204, _}, _, _}} = httpc:request(
        post, {URL, Headers, "application/json", Body}, [], []
    ),
    ok.

delete_table(Name0) ->
    Name = str(Name0),
    URL = ?QUERY_SERVER ++ "/table/" ++ Name,
    Headers = [],
    {ok, {{_, _, _}, _, _}} = httpc:request(delete, {URL, Headers}, [], []),
    ok.

read_rows(Name0) ->
    Name = str(Name0),
    URL = ?QUERY_SERVER ++ "/table/" ++ Name,
    Headers = [],
    {ok, {{_, 200, _}, _, Body}} = httpc:request(get, {URL, Headers}, [], []),
    emqx_utils_json:decode(Body).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

simple_create_rule_api(TCConfig) ->
    SQL = <<"select *, publish_received_at * 1000 as publish_received_at from \"${t}\" ">>,
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts) ->
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

%% * Worker process is removed from supervisor.
%% * Token is deleted from table.
ensure_wif_token_resources_cleared() ->
    ?assertMatch(
        [],
        [
            Child
         || Child = {Id, _, _, _} <- supervisor:which_children(?SUP),
            Id /= emqx_bridge_bigtable_token_cache
        ]
    ),
    ?assertMatch([], ets:tab2list(?TOKEN_TAB)),
    ok.

ensure_attached_service_account_token_resources_cleared() ->
    ?assertMatch([], ets:tab2list(?SA_TOKEN_RESP_TAB)),
    ok.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "bigtable_connector_stop").

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?not_batching, ?batching]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    RuleTopic = <<"testbigtable">>,
    SQL = <<"select *, publish_received_at * 1000 as publish_received_at from ", RuleTopic/binary>>,
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        Name = get_config(action_name, TCConfig),
        ?retry(
            200,
            10,
            begin
                Rows = read_rows(Name),
                ct:pal("rows:\n  ~p", [Rows]),
                ?assertMatch([#{<<"">> := Payload}], Rows),
                ok
            end
        ),
        ?retry(
            200,
            10,
            ?assertMatch(
                {200, #{
                    <<"metrics">> := #{
                        <<"matched">> := 1,
                        <<"success">> := 1,
                        <<"failed">> := 0
                    }
                }},
                get_action_metrics_api(TCConfig)
            )
        ),

        ok
    end,
    Opts = #{
        sql => SQL,
        rule_topic => RuleTopic,
        post_publish_fn => PostPublishFn
    },
    maybe_with_forced_sync_query_mode(TCConfig, fun() ->
        emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts)
    end).

t_rule_test_trace() ->
    [{matrix, true}].
t_rule_test_trace(matrix) ->
    [
        [Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?not_batching, ?batching]
    ];
t_rule_test_trace(TCConfig) when is_list(TCConfig) ->
    SQL = <<"select *, publish_received_at * 1000 as publish_received_at from \"${t}\" ">>,
    Opts = #{rule_sql => SQL},
    emqx_bridge_v2_testlib:t_rule_test_trace(TCConfig, Opts).

-doc """
Simple smoke test for using WIF (workload identity federation) authentication.

It cannot really emulate the real GCP IAM authentication process, but it does emulate the
calls to get a token and use the stored token.
""".
t_wif_auth() ->
    [{matrix, true}].
t_wif_auth(matrix) ->
    [[?wif_oidc]];
t_wif_auth(TCConfig) when is_list(TCConfig) ->
    mock_wif_auth_calls(),
    %% Sanity check
    ensure_wif_token_resources_cleared(),

    ?assertMatch(
        {201, #{
            <<"status">> := <<"connected">>,
            <<"authentication">> := #{<<"type">> := <<"wif">>}
        }},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{})
    ),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = <<"payload">>,
    emqtt:publish(C, Topic, Payload),
    Name = get_config(action_name, TCConfig),
    ?retry(
        200,
        10,
        begin
            Rows = read_rows(Name),
            ct:pal("rows:\n  ~p", [Rows]),
            ?assertMatch([#{<<"">> := Payload}], Rows),
            ok
        end
    ),

    %% Verify resource cleanup
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ensure_wif_token_resources_cleared(),
    ok.

-doc """
Simple smoke test for using attached service account authentication.

It cannot really emulate the real GCP IAM authentication process, but it does emulate the
calls to get a token and use the stored token.
""".
t_attached_service_account_auth() ->
    [{matrix, true}].
t_attached_service_account_auth(matrix) ->
    [[?attached_service_account]];
t_attached_service_account_auth(TCConfig) ->
    mock_attached_service_account_auth_calls(),
    %% Sanity check
    ensure_attached_service_account_token_resources_cleared(),

    ?assertMatch(
        {201, #{
            <<"status">> := <<"connected">>,
            <<"authentication">> := #{<<"type">> := <<"attached_service_account">>}
        }},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{})
    ),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = <<"payload">>,
    emqtt:publish(C, Topic, Payload),
    Name = get_config(action_name, TCConfig),
    ?retry(
        200,
        10,
        begin
            Rows = read_rows(Name),
            ct:pal("rows:\n  ~p", [Rows]),
            ?assertMatch([#{<<"">> := Payload}], Rows),
            ok
        end
    ),

    %% Verify resource cleanup
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ensure_attached_service_account_token_resources_cleared(),

    ok.

-doc """
Verifies that the implementation returns one result for each incoming message in a batch.
""".
t_partial_batch_failure() ->
    [{matrix, true}].
t_partial_batch_failure(matrix) ->
    [
        [Sync, ?batching]
     || Sync <- [?sync, ?async]
    ];
t_partial_batch_failure(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"row_key">> => <<"pub_props.User-Property.rk">>
        },
        <<"resource_opts">> => #{<<"worker_pool_size">> => 1}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Payload = <<"hello">>,
    %% need individual clients for each request for the sync case
    UserProps = [
        %% bad payload: undfined value; should fail to be rendered and sent in the
        %% batch
        [],
        %% payload ok: should appear in the table
        [{<<"rk">>, <<"mycol">>}],
        %% bad payload: renders ok, but will be rejected by bigtable ("row keys
        %% must be non empty")
        [{<<"rk">>, <<"">>}]
    ],
    ?check_trace(
        begin
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_utils:pforeach(
                        fun(UP) ->
                            C = start_client(#{proto_ver => v5}),
                            emqtt:publish(C, Topic, #{'User-Property' => UP}, Payload, [{qos, 0}]),
                            emqtt:stop(C)
                        end,
                        UserProps
                    ),
                    #{?snk_kind := "bigtable_result"},
                    5_000
                ),
            Name = get_config(action_name, TCConfig),
            ?retry(
                200,
                10,
                begin
                    Rows = read_rows(Name),
                    ct:pal("rows:\n  ~p", [Rows]),
                    ?assertMatch([#{<<"">> := Payload}], Rows),
                    ok
                end
            ),
            ok
        end,
        fun(Trace) ->
            SubTrace = ?of_kind(["bigtable_result"], Trace),
            ?assertMatch(
                [#{results := [_, _, _]}],
                SubTrace
            ),
            [#{results := Results}] = SubTrace,
            ?assertMatch(
                [
                    {error, {unrecoverable_error, {internal, _}}},
                    {error, {unrecoverable_error, #{reason := var_unbound}}},
                    {ok, _}
                ],
                lists:sort(Results)
            ),
            ok
        end
    ),

    ok.

-doc """
Verifies the case where the resulting batch is empty due to failure to render the payload.
""".
t_single_message_render_failure() ->
    [{matrix, true}].
t_single_message_render_failure(matrix) ->
    [
        [Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?not_batching, ?batching]
    ];
t_single_message_render_failure(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"row_key">> => <<"pub_props.User-Property.rk">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(#{proto_ver => v5}),
    Payload = <<"hello">>,
    ?check_trace(
        begin
            {ok, {ok, _}} =
                ?wait_async_action(
                    %% row key var will be unbound and fail to render
                    emqtt:publish(C, Topic, #{'User-Property' => []}, Payload, [{qos, 0}]),
                    #{?snk_kind := "bigtable_result"},
                    5_000
                ),
            Name = get_config(action_name, TCConfig),
            Rows = read_rows(Name),
            ?assertMatch([], Rows),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        results := [
                            {error, {unrecoverable_error, #{reason := var_unbound}}}
                        ]
                    }
                ],
                ?of_kind(["bigtable_result"], Trace)
            ),
            ok
        end
    ),
    ok.

-doc """
Asserts that we reject empty mutation arrays in the schema validation phase.
""".
t_non_empty_mutation_array(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {400, _},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"mutations">> => []
            }
        })
    ),
    ok.
