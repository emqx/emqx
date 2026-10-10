%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_prometheus_api_2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("emqx_prometheus.hrl").

-define(with_auth_header(HEADER, BODY), with_auth_header(HEADER, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:start_trace(),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

get_data_integration(Mode, Opts) ->
    URL = emqx_mgmt_api_test_util:api_path(["prometheus", "data_integration"]),
    get_prometheus(URL, Mode, Opts).

get_namespaced_stats(Mode, Opts) ->
    URL = emqx_mgmt_api_test_util:api_path(["prometheus", "namespaced_stats"]),
    get_prometheus(URL, Mode, Opts).

get_topic_metrics(Mode, Opts) ->
    URL = topic_metrics_url(),
    get_prometheus(URL, Mode, Opts).

get_topic_metrics_raw(Mode, Opts) ->
    prometheus_request(topic_metrics_url(), Mode, Opts).

topic_metrics_url() ->
    emqx_mgmt_api_test_util:api_path(["prometheus", "topic_metrics"]).

get_prometheus(URL, Mode, Opts) ->
    {Status, Response} = prometheus_request(URL, Mode, Opts),
    case Status of
        200 ->
            {Status, parse_prometheus(Response)};
        _ ->
            {Status, Response}
    end.

prometheus_request(URL, Mode, Opts) ->
    Ns = maps:get(ns, Opts, undefined),
    OnlyGlobal = maps:get(only_global, Opts, undefined),
    ExtraHeaders = maps:get(extra_headers, Opts, []),
    QueryString = uri_string:compose_query(
        lists:flatten([
            {"mode", atom_to_binary(Mode)},
            [{"ns", Ns} || Ns /= undefined],
            [{"only_global", OnlyGlobal} || OnlyGlobal /= undefined]
        ])
    ),
    AuthHeader = maps:get(auth_header, Opts, {"no", "auth"}),
    emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => URL,
        extra_headers => ExtraHeaders,
        query_params => QueryString,
        auth_header => AuthHeader
    }).

parse_prometheus(RawData) ->
    lists:foldl(
        fun
            (<<"#", _/binary>>, Acc) ->
                Acc;
            (Line, Acc) ->
                {Name, Labels, Value} = parse_prometheus_line(Line),
                maps:update_with(
                    Name,
                    fun(Old) -> Old#{Labels => Value} end,
                    #{Labels => Value},
                    Acc
                )
        end,
        #{},
        binary:split(iolist_to_binary(RawData), <<"\n">>, [global, trim_all])
    ).

parse_prometheus_line(Line) ->
    RE = <<"(?<name>[a-z0-9A-Z_]+)(\\{(?<labels>[^)]*)\\})? *(?<value>[0-9]+(\\.[0-9]+)?)">>,
    {match, [Name, Labels0, Value0]} = re:run(
        Line, RE, [{capture, [<<"name">>, <<"labels">>, <<"value">>], binary}]
    ),
    Labels = parse_prometheus_labels(Labels0),
    Value =
        try
            binary_to_float(Value0)
        catch
            error:badarg ->
                binary_to_integer(Value0)
        end,
    {Name, Labels, Value}.

parse_prometheus_labels(<<"">>) ->
    #{};
parse_prometheus_labels(Labels) ->
    lists:foldl(
        fun(Label, Acc) ->
            [K, V0] = binary:split(Label, <<"=">>),
            V = binary:replace(V0, <<"\"">>, <<"">>, [global]),
            Acc#{K => V}
        end,
        #{},
        binary:split(Labels, <<",">>, [global])
    ).

%% Series keys as `{MetricName, Labels}'.  `parse_prometheus/1' stores
%% samples in a map keyed by labels, so a duplicate series would be
%% silently collapsed; the raw exposition is needed to catch that.
series_keys(RawData) ->
    [
        {Name, Labels}
     || Line <- binary:split(iolist_to_binary(RawData), <<"\n">>, [global, trim_all]),
        Line =/= <<>>,
        binary:first(Line) =/= $#,
        {Name, Labels, _Value} <- [parse_prometheus_line(Line)]
    ].

assert_unique_series(RawData) ->
    Keys = series_keys(RawData),
    ?assertEqual(lists:usort(Keys), lists:sort(Keys)).

%% Names of the collections present in one metric family of the raw
%% exposition, sorted.
collection_names(MetricName, RawData) ->
    lists:sort([
        maps:get(<<"name">>, Labels)
     || {Name, Labels} <- series_keys(RawData), Name =:= MetricName
    ]).

%% A dashboard user scoped to one namespace, holding `Role' there.
%% Usernames only accept letters, digits and underscores.
namespaced_auth_header(Ns, Role) ->
    create_namespaced_user_auth_header(#{
        params => #{
            <<"username">> => <<Ns/binary, "_", Role/binary>>,
            <<"role">> => <<"ns:", Ns/binary, "::", Role/binary>>
        }
    }).

%% Publish a message over the public broker path, attributed to a
%% namespace through `client_attrs.tns', so the topic-metric counters are
%% produced by the same code path as a real client publish.
publish_as(Namespace, Topic) ->
    Msg = emqx_message:set_headers(
        #{client_attrs => #{?CLIENT_ATTR_NAME_TNS => Namespace}},
        emqx_message:make(<<"prometheus-test">>, Topic, <<>>)
    ),
    _ = emqx_broker:publish(Msg),
    ok.

%% Expected parsed exposition for the given collections, each
%% `{BinName, TopicFilter, OwnerNs, MessagesIn, BytesIn, MessagesDropped}';
%% `OwnerNs = undefined' is a global-owned collection, which carries no
%% namespace label.  `BytesIn' is the summed message size of the matching
%% publishes.  Every other counter stays at zero: the fixture has no
%% subscriber, so nothing is delivered (`messages.out').
topic_metrics_expected(Mode, Collections) ->
    NodeLabels =
        case Mode of
            ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED ->
                #{<<"node">> => atom_to_binary(node(), utf8)};
            _ ->
                #{}
        end,
    lists:foldl(
        fun({BinName, TopicFilter, OwnerNs, MessagesIn, BytesIn, MessagesDropped}, Acc0) ->
            Labels0 = #{<<"name">> => BinName, <<"topic_filter">> => TopicFilter},
            Labels1 =
                case OwnerNs of
                    undefined -> Labels0;
                    _ -> Labels0#{<<"namespace">> => OwnerNs}
                end,
            Labels = maps:merge(Labels1, NodeLabels),
            lists:foldl(
                fun({MetricName, Value}, Acc) ->
                    maps:update_with(
                        MetricName,
                        fun(Series) -> Series#{Labels => Value} end,
                        #{Labels => Value},
                        Acc
                    )
                end,
                Acc0,
                [
                    {<<"emqx_topic_metric_messages_in_count">>, MessagesIn},
                    {<<"emqx_topic_metric_messages_out_count">>, 0},
                    {<<"emqx_topic_metric_messages_dropped_count">>, MessagesDropped},
                    {<<"emqx_topic_metric_bytes_in">>, BytesIn},
                    {<<"emqx_topic_metric_bytes_out">>, 0}
                ]
            )
        end,
        #{},
        Collections
    ).

start_local(TestCase, TCConfig, Opts) ->
    ExtraApps = maps:get(extra_apps, Opts, []),
    AppSpecs =
        [
            emqx,
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
            {emqx_prometheus, "prometheus.namespaced_metrics_limiter.rate = infinity"}
        ] ++ ExtraApps,
    Apps = emqx_cth_suite:start(AppSpecs, #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}),
    on_exit(fun() -> emqx_cth_suite:stop(Apps) end),
    Apps.

create_namespaced_user_auth_header(Opts) ->
    Token = emqx_bridge_v2_testlib:create_namespaced_user_and_token(Opts),
    {"Authorization", <<"Bearer ", Token/binary>>}.

global_admin_auth_header() ->
    emqx_mgmt_api_test_util:auth_header_().

create_connector_api(TCConfig, Overrides) ->
    on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

simple_create_rule_api(Opts0, TCConfig) ->
    Opts = maps:merge(#{sql => auto}, Opts0),
    emqx_bridge_v2_testlib:simple_create_rule_api(Opts, TCConfig).

with_auth_header(AuthHeader, Fn) ->
    try
        AuthFn = fun() -> AuthHeader end,
        emqx_bridge_v2_testlib:set_auth_header_getter(AuthFn),
        Fn()
    after
        emqx_bridge_v2_testlib:clear_auth_header_getter()
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Checks that global admins may observe namespaced metrics from all namespaces, and
namespaced admins only see their own namespace.
""".
t_namespaced_stats(TCConfig) ->
    start_local(?FUNCTION_NAME, TCConfig, #{
        extra_apps => [
            emqx_auth_mnesia,
            emqx_auth,
            emqx_mt
        ]
    }),

    GetLabels = fun(Key, Res) -> lists:sort(maps:keys(maps:get(Key, Res))) end,
    Ns1 = <<"ns1">>,
    ok = emqx_mt_config:create_managed_ns(Ns1),
    Ns2 = <<"ns2">>,
    ok = emqx_mt_config:create_managed_ns(Ns2),

    %% without authn enabled, there's nothing much we can do...  all namespaces are returned
    {ok, _} = emqx:update_config([prometheus, enable_basic_auth], false),
    #{started := Started0} = emqx_dashboard:listeners_status(),
    ok = emqx_dashboard_dispatch:regenerate_dispatch(Started0),

    {200, NoAuthRes} = get_namespaced_stats(?PROM_DATA_MODE__NODE, #{}),
    ?assertMatch(
        [
            #{<<"namespace">> := Ns1},
            #{<<"namespace">> := Ns2}
        ],
        GetLabels(<<"emqx_bytes_received">>, NoAuthRes)
    ),

    {ok, _} = emqx:update_config([prometheus, enable_basic_auth], true),
    #{started := Started1} = emqx_dashboard:listeners_status(),
    ok = emqx_dashboard_dispatch:regenerate_dispatch(Started1),

    %% sanity check: auth should be enabled
    ?assertMatch({401, _}, get_namespaced_stats(?PROM_DATA_MODE__NODE, #{})),

    Ns1AuthHeader = create_namespaced_user_auth_header(#{
        params => #{
            <<"username">> => Ns1,
            <<"role">> => <<"ns:", Ns1/binary, "::administrator">>
        }
    }),
    Ns2AuthHeader = create_namespaced_user_auth_header(#{
        params => #{
            <<"username">> => Ns2,
            <<"role">> => <<"ns:", Ns2/binary, "::administrator">>
        }
    }),
    GlobalAuthHeader = global_admin_auth_header(),

    lists:foreach(
        fun(Mode) ->
            ct:pal("mode ~s", [Mode]),

            %% global admin sees metrics from all namespaces (except global; there's
            %% another endpoint for that)
            {200, GlobalNodeRes1} = get_namespaced_stats(Mode, #{
                auth_header => GlobalAuthHeader
            }),
            ?assertMatch(
                [
                    #{<<"namespace">> := Ns1},
                    #{<<"namespace">> := Ns2}
                ],
                GetLabels(<<"emqx_bytes_received">>, GlobalNodeRes1)
            ),
            ?assertMatch(
                [
                    #{<<"namespace">> := Ns1},
                    #{<<"namespace">> := Ns2}
                ],
                GetLabels(<<"emqx_sessions_count">>, GlobalNodeRes1)
            ),
            %% possible to filter one specific namespace
            {200, GlobalNodeRes2} = get_namespaced_stats(Mode, #{
                auth_header => GlobalAuthHeader,
                ns => Ns2
            }),
            ?assertMatch(
                [
                    #{<<"namespace">> := Ns2}
                ],
                GetLabels(<<"emqx_bytes_received">>, GlobalNodeRes2)
            ),

            %% namespaced admin can only filter its own namespace
            {200, NsNodeRes1} = get_namespaced_stats(Mode, #{
                auth_header => Ns1AuthHeader
            }),
            ?assertMatch(
                [#{<<"namespace">> := Ns1}],
                GetLabels(<<"emqx_bytes_received">>, NsNodeRes1)
            ),
            {200, NsNodeRes2} = get_namespaced_stats(Mode, #{
                auth_header => Ns1AuthHeader,
                ns => Ns1
            }),
            ?assertMatch(
                [#{<<"namespace">> := Ns1}],
                GetLabels(<<"emqx_bytes_received">>, NsNodeRes2)
            ),
            ?assertMatch(
                {403, _},
                get_namespaced_stats(Mode, #{
                    auth_header => Ns1AuthHeader,
                    ns => Ns2
                })
            ),

            {200, NsNodeRes3} = get_namespaced_stats(Mode, #{
                auth_header => Ns2AuthHeader
            }),
            ?assertMatch(
                [#{<<"namespace">> := Ns2}],
                GetLabels(<<"emqx_bytes_received">>, NsNodeRes3)
            ),

            ok
        end,
        ?PROM_DATA_MODES
    ),

    ok.

-doc """
Checks that the namespaced stats endpoint responds successfully when the `emqx_mt' and
`emqx_auth_mnesia' applications are not running: their tables do not exist, and the
namespaced session/authz/authn metrics are simply omitted instead of crashing the
collector.
""".
t_namespaced_stats_mt_not_started(TCConfig) ->
    start_local(?FUNCTION_NAME, TCConfig, #{}),
    AuthHeader = global_admin_auth_header(),
    lists:foreach(
        fun(Mode) ->
            ct:pal("mode ~s", [Mode]),
            {200, Res} = get_namespaced_stats(Mode, #{auth_header => AuthHeader}),
            ?assertNot(is_map_key(<<"emqx_sessions_count">>, Res), Res),
            ?assertNot(is_map_key(<<"emqx_authz_builtin_record_count">>, Res), Res),
            ?assertNot(is_map_key(<<"emqx_authn_builtin_record_count">>, Res), Res)
        end,
        ?PROM_DATA_MODES
    ),
    ok.

-doc """
Checks that requesting stats for a specific namespace while `emqx_mt' is not running (its
tables do not exist, so no namespace is known) responds successfully with the namespaced
metrics omitted, instead of fabricating zero-valued samples for a namespace that does not
exist.
""".
t_namespaced_stats_unknown_ns_mt_not_started(TCConfig) ->
    start_local(?FUNCTION_NAME, TCConfig, #{}),
    AuthHeader = global_admin_auth_header(),
    lists:foreach(
        fun(Mode) ->
            ct:pal("mode ~s", [Mode]),
            {200, Res} = get_namespaced_stats(Mode, #{
                auth_header => AuthHeader,
                ns => <<"unknown_ns">>
            }),
            ?assertNot(is_map_key(<<"emqx_sessions_count">>, Res), Res),
            ?assertNot(is_map_key(<<"emqx_authz_builtin_record_count">>, Res), Res),
            ?assertNot(is_map_key(<<"emqx_authn_builtin_record_count">>, Res), Res),
            ?assertNot(is_map_key(<<"emqx_bytes_received">>, Res), Res)
        end,
        ?PROM_DATA_MODES
    ),
    ok.

-doc """
Checks that requesting stats for a namespace name that was never created responds
successfully with the namespaced metrics omitted, while metrics for a known namespace are
still reported.
""".
t_namespaced_stats_unknown_ns(TCConfig) ->
    start_local(?FUNCTION_NAME, TCConfig, #{
        extra_apps => [
            emqx_auth_mnesia,
            emqx_auth,
            emqx_mt
        ]
    }),
    GetLabels = fun(Key, Res) -> lists:sort(maps:keys(maps:get(Key, Res, #{}))) end,
    Ns1 = <<"ns1">>,
    ok = emqx_mt_config:create_managed_ns(Ns1),
    AuthHeader = global_admin_auth_header(),
    lists:foreach(
        fun(Mode) ->
            ct:pal("mode ~s", [Mode]),
            {200, UnknownRes} = get_namespaced_stats(Mode, #{
                auth_header => AuthHeader,
                ns => <<"unknown_ns">>
            }),
            ?assertNot(is_map_key(<<"emqx_sessions_count">>, UnknownRes), UnknownRes),
            ?assertNot(
                is_map_key(<<"emqx_authz_builtin_record_count">>, UnknownRes), UnknownRes
            ),
            ?assertNot(
                is_map_key(<<"emqx_authn_builtin_record_count">>, UnknownRes), UnknownRes
            ),
            ?assertNot(is_map_key(<<"emqx_bytes_received">>, UnknownRes), UnknownRes),
            %% The known namespace is still reported.
            {200, KnownRes} = get_namespaced_stats(Mode, #{
                auth_header => AuthHeader,
                ns => Ns1
            }),
            ?assertMatch(
                [#{<<"namespace">> := Ns1}],
                GetLabels(<<"emqx_sessions_count">>, KnownRes)
            ),
            ?assertMatch(
                [#{<<"namespace">> := Ns1}],
                GetLabels(<<"emqx_authz_builtin_record_count">>, KnownRes)
            ),
            ?assertMatch(
                [#{<<"namespace">> := Ns1}],
                GetLabels(<<"emqx_authn_builtin_record_count">>, KnownRes)
            ),
            ?assertMatch(
                [#{<<"namespace">> := Ns1}],
                GetLabels(<<"emqx_bytes_received">>, KnownRes)
            )
        end,
        ?PROM_DATA_MODES
    ),
    ok.

-doc """
Regression test for the case where there is an action whose connector does not exist.

This may arise if someone manually edits the configuration and starts up the node like that.
""".
t_action_without_connector(TCConfig) ->
    ActionConfig = emqx_bridge_schema_testlib:mqtt_action_config(#{
        <<"connector">> => <<"a">>
    }),
    start_local(?FUNCTION_NAME, TCConfig, #{
        extra_apps => [
            emqx_bridge_mqtt,
            {emqx_bridge, #{
                config =>
                    #{
                        <<"actions">> =>
                            #{
                                <<"mqtt">> =>
                                    #{<<"a">> => ActionConfig}
                            }
                    }
            }},
            emqx_rule_engine
        ]
    }),
    AuthHeader = global_admin_auth_header(),
    lists:foreach(
        fun(Mode) ->
            ?assertMatch(
                {200, _},
                get_data_integration(Mode, #{auth_header => AuthHeader}),
                #{mode => Mode}
            )
        end,
        ?PROM_DATA_MODES
    ),
    ok.

t_namespaced_data_integration(TCConfig) ->
    start_local(?FUNCTION_NAME, TCConfig, #{
        extra_apps => [
            emqx_bridge_mqtt,
            emqx_bridge,
            emqx_rule_engine,
            emqx_mt
        ]
    }),
    %% sanity check: auth should be enabled
    ?assertMatch({401, _}, get_data_integration(?PROM_DATA_MODE__NODE, #{})),

    MkBridgeConfig = fun(Name) ->
        [
            {bridge_kind, action},
            {connector_type, <<"mqtt">>},
            {connector_name, Name},
            {connector_config, emqx_bridge_schema_testlib:mqtt_connector_config(#{})},
            {action_type, <<"mqtt">>},
            {action_name, Name},
            {action_config,
                emqx_bridge_schema_testlib:mqtt_action_config(#{<<"connector">> => Name})}
        ]
    end,
    Ns = <<"ns1">>,
    ok = emqx_mt_config:create_managed_ns(Ns),
    NsAuthHeader = create_namespaced_user_auth_header(#{}),
    ?with_auth_header(NsAuthHeader, begin
        Cfg = MkBridgeConfig(Ns),
        {201, _} = create_connector_api(Cfg, #{}),
        {201, _} = create_action_api(Cfg, #{}),
        simple_create_rule_api(#{id => Ns}, Cfg),
        ok
    end),
    GlobalAuthHeader = global_admin_auth_header(),
    ?with_auth_header(GlobalAuthHeader, begin
        Cfg = MkBridgeConfig(<<"global">>),
        {201, _} = create_connector_api(Cfg, #{}),
        {201, _} = create_action_api(Cfg, #{}),
        simple_create_rule_api(#{id => <<"global">>}, Cfg),
        ok
    end),

    GetLabels = fun(Key, Res) -> lists:sort(maps:keys(maps:get(Key, Res))) end,

    lists:foreach(
        fun(Mode) ->
            ct:pal("mode ~s", [Mode]),

            %% namespaced admin cannot see other namespaces
            ?assertMatch(
                {403, _},
                get_data_integration(Mode, #{
                    auth_header => NsAuthHeader,
                    ns => <<"other_ns">>
                })
            ),

            {200, NsNodeRes1} = get_data_integration(Mode, #{
                auth_header => NsAuthHeader
            }),
            ?assertNotMatch(
                [#{<<"id">> := <<"mqtt:global">>}],
                GetLabels(<<"emqx_connector_status">>, NsNodeRes1)
            ),
            ?assertNotMatch(
                [#{<<"id">> := <<"mqtt:global">>}], GetLabels(<<"emqx_action_enable">>, NsNodeRes1)
            ),
            ?assertNotMatch(
                [#{<<"id">> := <<"mqtt:global">>}], GetLabels(<<"emqx_rule_enable">>, NsNodeRes1)
            ),
            ?assertMatch(
                [
                    #{
                        <<"id">> := <<"mqtt:ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_connector_status">>, NsNodeRes1)
            ),
            ?assertMatch(
                [
                    #{
                        <<"id">> := <<"mqtt:ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_action_enable">>, NsNodeRes1)
            ),
            ?assertMatch(
                [
                    #{
                        <<"id">> := <<"ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_rule_enable">>, NsNodeRes1)
            ),

            %% without specifying `only_global`, global admin sees metrics from all namespaces
            {200, GlobalNodeRes1} = get_data_integration(Mode, #{
                auth_header => GlobalAuthHeader
            }),
            ?assertMatch(
                [
                    #{<<"id">> := <<"mqtt:global">>},
                    #{
                        <<"id">> := <<"mqtt:ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_connector_status">>, GlobalNodeRes1)
            ),
            ?assertMatch(
                [
                    #{<<"id">> := <<"mqtt:global">>},
                    #{
                        <<"id">> := <<"mqtt:ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_action_enable">>, GlobalNodeRes1)
            ),
            ?assertMatch(
                [
                    #{<<"id">> := <<"global">>},
                    #{
                        <<"id">> := <<"ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_rule_enable">>, GlobalNodeRes1)
            ),
            %% with `only_global`, global admin sees metrics only from global ns
            {200, GlobalNodeRes2} = get_data_integration(Mode, #{
                auth_header => GlobalAuthHeader,
                only_global => true
            }),
            ?assertMatch(
                [
                    #{<<"id">> := <<"mqtt:global">>}
                ],
                GetLabels(<<"emqx_connector_status">>, GlobalNodeRes2)
            ),
            ?assertMatch(
                [
                    #{<<"id">> := <<"mqtt:global">>}
                ],
                GetLabels(<<"emqx_action_enable">>, GlobalNodeRes2)
            ),
            ?assertMatch(
                [
                    #{<<"id">> := <<"global">>}
                ],
                GetLabels(<<"emqx_rule_enable">>, GlobalNodeRes2)
            ),

            %% namespaced admin can filter specific namespaces
            {200, GlobalNodeRes3} = get_data_integration(Mode, #{
                auth_header => GlobalAuthHeader,
                ns => Ns
            }),
            ?assertMatch(
                [
                    #{
                        <<"id">> := <<"mqtt:ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_connector_status">>, GlobalNodeRes3)
            ),
            ?assertMatch(
                [
                    #{
                        <<"id">> := <<"mqtt:ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_action_enable">>, GlobalNodeRes3)
            ),
            ?assertMatch(
                [
                    #{
                        <<"id">> := <<"ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_rule_enable">>, GlobalNodeRes3)
            ),

            ok
        end,
        ?PROM_DATA_MODES
    ),

    %% if auth is disabled, there's not much we can do.  we treat the request as if coming
    %% from a global admin.
    {ok, _} = emqx:update_config([prometheus, enable_basic_auth], false),
    #{started := Started} = emqx_dashboard:listeners_status(),
    ok = emqx_dashboard_dispatch:regenerate_dispatch(Started),
    lists:foreach(
        fun(Mode) ->
            ct:pal("mode ~s", [Mode]),
            %% without specifying `only_global`, global admin sees metrics from all namespaces
            {200, GlobalNodeRes1} = get_data_integration(Mode, #{}),
            ?assertMatch(
                [
                    #{<<"id">> := <<"mqtt:global">>},
                    #{
                        <<"id">> := <<"mqtt:ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_connector_status">>, GlobalNodeRes1)
            ),
            ?assertMatch(
                [
                    #{<<"id">> := <<"mqtt:global">>},
                    #{
                        <<"id">> := <<"mqtt:ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_action_enable">>, GlobalNodeRes1)
            ),
            ?assertMatch(
                [
                    #{<<"id">> := <<"global">>},
                    #{
                        <<"id">> := <<"ns1">>,
                        <<"namespace">> := Ns
                    }
                ],
                GetLabels(<<"emqx_rule_enable">>, GlobalNodeRes1)
            ),
            %% with `only_global`, global admin sees metrics only from global ns
            {200, GlobalNodeRes2} = get_data_integration(Mode, #{
                only_global => true
            }),
            ?assertMatch(
                [
                    #{<<"id">> := <<"mqtt:global">>}
                ],
                GetLabels(<<"emqx_connector_status">>, GlobalNodeRes2)
            ),
            ?assertMatch(
                [
                    #{<<"id">> := <<"mqtt:global">>}
                ],
                GetLabels(<<"emqx_action_enable">>, GlobalNodeRes2)
            ),
            ?assertMatch(
                [
                    #{<<"id">> := <<"global">>}
                ],
                GetLabels(<<"emqx_rule_enable">>, GlobalNodeRes2)
            ),
            ok
        end,
        ?PROM_DATA_MODES
    ),

    ok.

-doc """
Checks that `/prometheus/topic_metrics' only exposes collections owned by the caller's
namespace in every `mode': a namespaced administrator or viewer sees exactly its own rows, a
global administrator or viewer sees every namespace, and a namespaced actor cannot address
another namespace.  Two namespaces own collections with the same bin-name and topic filter, so
the owner namespace is the only thing that tells the two series apart.
""".
t_topic_metrics_namespace(TCConfig) ->
    start_local(?FUNCTION_NAME, TCConfig, #{
        extra_apps => [emqx_auth_mnesia, emqx_auth, emqx_mt]
    }),
    Ns1 = <<"ns1">>,
    Ns2 = <<"ns2">>,
    ok = emqx_mt_config:create_managed_ns(Ns1),
    ok = emqx_mt_config:create_managed_ns(Ns2),
    ok = emqx_topic_metrics2:register(<<"metric-a">>, <<"tenant/a/#">>, Ns1),
    ok = emqx_topic_metrics2:register(<<"metric-b">>, <<"tenant/b/#">>, Ns2),
    ok = emqx_topic_metrics2:register(<<"same">>, <<"same/#">>, Ns1),
    ok = emqx_topic_metrics2:register(<<"same">>, <<"same/#">>, Ns2),
    ok = emqx_topic_metrics2:register(<<"metric-global">>, <<"global/#">>, ?global_ns),

    publish_as(Ns1, <<"tenant/a/1">>),
    publish_as(Ns1, <<"tenant/a/1">>),
    publish_as(Ns2, <<"tenant/b/1">>),
    publish_as(Ns1, <<"same/1">>),
    publish_as(Ns2, <<"same/1">>),
    publish_as(Ns2, <<"same/1">>),
    publish_as(Ns1, <<"global/1">>),

    Ns1Admin = namespaced_auth_header(Ns1, <<"administrator">>),
    Ns1Viewer = namespaced_auth_header(Ns1, <<"viewer">>),
    GlobalAdmin = global_admin_auth_header(),
    GlobalViewer = create_namespaced_user_auth_header(#{
        params => #{<<"username">> => <<"global_viewer">>, <<"role">> => <<"viewer">>}
    }),

    %% The fixture has no subscriber: each publish is counted as
    %% `messages.in' (plus its size as `bytes.in') and then as dropped.
    Ns1Rows = [
        {<<"metric-a">>, <<"tenant/a/#">>, Ns1, 2, 20, 2},
        {<<"same">>, <<"same/#">>, Ns1, 1, 6, 1}
    ],
    Ns2Rows = [
        {<<"metric-b">>, <<"tenant/b/#">>, Ns2, 1, 10, 1},
        {<<"same">>, <<"same/#">>, Ns2, 2, 12, 2}
    ],
    GlobalRows = [{<<"metric-global">>, <<"global/#">>, undefined, 1, 8, 1}],

    lists:foreach(
        fun(Mode) ->
            ct:pal("mode ~s", [Mode]),
            Ns1Expected = topic_metrics_expected(Mode, Ns1Rows),
            AllExpected = topic_metrics_expected(Mode, Ns1Rows ++ Ns2Rows ++ GlobalRows),

            %% A namespaced administrator and a namespaced viewer see the
            %% same rows: the role does not widen the namespace scope.
            {200, Ns1AdminRes} = get_topic_metrics(Mode, #{auth_header => Ns1Admin}),
            ?assertEqual(Ns1Expected, Ns1AdminRes),
            {200, Ns1ViewerRes} = get_topic_metrics(Mode, #{auth_header => Ns1Viewer}),
            ?assertEqual(Ns1Expected, Ns1ViewerRes),

            %% The parsed map cannot represent duplicate label sets, so
            %% check the raw exposition of the scoped view as well.
            {200, Ns1Raw} = get_topic_metrics_raw(Mode, #{auth_header => Ns1Admin}),
            assert_unique_series(Ns1Raw),
            ?assertEqual(
                [<<"metric-a">>, <<"same">>],
                collection_names(<<"emqx_topic_metric_messages_in_count">>, Ns1Raw)
            ),

            %% A global administrator and a global viewer see every
            %% namespace, and may narrow the request to one namespace.
            {200, GlobalRes} = get_topic_metrics(Mode, #{auth_header => GlobalAdmin}),
            ?assertEqual(AllExpected, GlobalRes),
            %% The global view holds both `same' collections, so its raw
            %% exposition must keep two distinct series; the parsed map
            %% would silently collapse duplicate label sets.
            {200, GlobalRaw} = get_topic_metrics_raw(Mode, #{auth_header => GlobalAdmin}),
            assert_unique_series(GlobalRaw),
            ?assertEqual(
                [<<"metric-a">>, <<"metric-b">>, <<"metric-global">>, <<"same">>, <<"same">>],
                collection_names(<<"emqx_topic_metric_messages_in_count">>, GlobalRaw)
            ),
            {200, GlobalViewerRes} = get_topic_metrics(Mode, #{auth_header => GlobalViewer}),
            ?assertEqual(AllExpected, GlobalViewerRes),
            {200, Ns2Res} = get_topic_metrics(Mode, #{
                auth_header => GlobalAdmin, ns => Ns2
            }),
            ?assertEqual(topic_metrics_expected(Mode, Ns2Rows), Ns2Res),

            %% A namespaced actor stays on its own namespace.
            {200, Ns1Own} = get_topic_metrics(Mode, #{auth_header => Ns1Admin, ns => Ns1}),
            ?assertEqual(Ns1Expected, Ns1Own),
            ?assertMatch(
                {403, _},
                get_topic_metrics(Mode, #{auth_header => Ns1Admin, ns => Ns2})
            )
        end,
        ?PROM_DATA_MODES
    ),
    ok.

-doc """
Checks the topic-metrics endpoint's authentication, JSON rejection, and namespace values that
select no collection.
""".
t_topic_metrics_namespace_errors(TCConfig) ->
    start_local(?FUNCTION_NAME, TCConfig, #{
        extra_apps => [emqx_auth_mnesia, emqx_auth, emqx_mt]
    }),
    Ns1 = <<"ns1">>,
    ok = emqx_mt_config:create_managed_ns(Ns1),
    Ns1Admin = namespaced_auth_header(Ns1, <<"administrator">>),
    GlobalAdmin = global_admin_auth_header(),

    %% No credentials: rejected before any collection is rendered.
    ?assertMatch({401, _}, get_topic_metrics(?PROM_DATA_MODE__NODE, #{})),

    %% The JSON representation is not supported.
    ?assertMatch(
        {400, _},
        get_topic_metrics(?PROM_DATA_MODE__NODE, #{
            auth_header => GlobalAdmin,
            extra_headers => [{"accept", "application/json"}]
        })
    ),

    %% No collections yet: metric family headers only, no series.
    {200, NoCollections} = get_topic_metrics(?PROM_DATA_MODE__NODE, #{
        auth_header => GlobalAdmin
    }),
    ?assertEqual(#{}, NoCollections),

    ok = emqx_topic_metrics2:register(<<"metric-a">>, <<"tenant/a/#">>, Ns1),
    publish_as(Ns1, <<"tenant/a/1">>),

    %% A namespace that owns no collection selects no rows, for a global
    %% administrator and for an unrelated namespaced one.
    lists:foreach(
        fun(Ns) ->
            {200, Empty} = get_topic_metrics(?PROM_DATA_MODE__NODE, #{
                auth_header => GlobalAdmin, ns => Ns
            }),
            ?assertEqual(#{}, Empty)
        end,
        [<<"unknown_ns">>, <<"ns2">>, <<"global">>, <<>>]
    ),

    %% The namespaced actor may name its own namespace, but nothing else.
    {200, Own} = get_topic_metrics(?PROM_DATA_MODE__NODE, #{
        auth_header => Ns1Admin, ns => Ns1
    }),
    ?assertEqual(
        topic_metrics_expected(?PROM_DATA_MODE__NODE, [
            {<<"metric-a">>, <<"tenant/a/#">>, Ns1, 1, 10, 1}
        ]),
        Own
    ),
    lists:foreach(
        fun(Ns) ->
            ?assertMatch(
                {403, _},
                get_topic_metrics(?PROM_DATA_MODE__NODE, #{auth_header => Ns1Admin, ns => Ns})
            )
        end,
        [<<"ns2">>, <<>>]
    ),

    %% A second namespace, plus a global-owned collection, makes the
    %% unauthenticated view distinguishable from a namespace-scoped one.
    ok = emqx_topic_metrics2:register(<<"metric-b">>, <<"tenant/b/#">>, <<"ns2">>),
    ok = emqx_topic_metrics2:register(<<"metric-g">>, <<"global/#">>, ?global_ns),
    publish_as(<<"ns2">>, <<"tenant/b/1">>),
    publish_as(<<"ns1">>, <<"global/1">>),

    %% With basic auth disabled there is no auth_meta at all: the request
    %% behaves like a global administrator and sees every namespace,
    %% including the global-owned collection.
    {ok, _} = emqx:update_config([prometheus, enable_basic_auth], false),
    #{started := Started} = emqx_dashboard:listeners_status(),
    ok = emqx_dashboard_dispatch:regenerate_dispatch(Started),
    {200, NoAuth} = get_topic_metrics(?PROM_DATA_MODE__NODE, #{}),
    ?assertEqual(
        topic_metrics_expected(?PROM_DATA_MODE__NODE, [
            {<<"metric-a">>, <<"tenant/a/#">>, Ns1, 1, 10, 1},
            {<<"metric-b">>, <<"tenant/b/#">>, <<"ns2">>, 1, 10, 1},
            {<<"metric-g">>, <<"global/#">>, undefined, 1, 8, 1}
        ]),
        NoAuth
    ),
    ok.

-doc """
Checks that a scoped render leaves no namespace scope behind, including when rendering raises:
a later context-free `prometheus_text_format:format/1' (the path the push gateway uses) still
renders every collection.
""".
t_topic_metrics_render_context(TCConfig) ->
    start_local(?FUNCTION_NAME, TCConfig, #{
        extra_apps => [emqx_auth_mnesia, emqx_auth, emqx_mt]
    }),
    ok = emqx_topic_metrics2:register(<<"metric-a">>, <<"tenant/a/#">>, <<"ns1">>),
    ok = emqx_topic_metrics2:register(<<"metric-b">>, <<"tenant/b/#">>, <<"ns2">>),
    Registry = emqx_prometheus_topic_metrics:registry(),
    Metric = <<"emqx_topic_metric_messages_in_count">>,
    AllNames = [<<"metric-a">>, <<"metric-b">>],

    Ns1Body = emqx_prometheus_topic_metrics:collect_ns(<<"ns1">>, ?PROM_DATA_MODE__NODE),
    ?assertEqual([<<"metric-a">>], collection_names(Metric, Ns1Body)),
    ?assertEqual(AllNames, collection_names(Metric, prometheus_text_format:format(Registry))),

    %% A render failure must not leave the scope behind either.
    ok = meck:new(prometheus_text_format, [passthrough]),
    try
        ok = meck:expect(prometheus_text_format, format, fun(_) -> error(render_failed) end),
        ?assertError(
            render_failed,
            emqx_prometheus_topic_metrics:collect_ns(<<"ns1">>, ?PROM_DATA_MODE__NODE)
        )
    after
        meck:unload(prometheus_text_format)
    end,
    ?assertEqual(AllNames, collection_names(Metric, prometheus_text_format:format(Registry))),
    ok.

-doc """
Checks the failure path of the namespace-aware fan-out in both fan-out modes: a node that
cannot answer is skipped, the answering node's series are served in full, an all-nodes failure
still returns a well-formed empty exposition, and every skipped node is reported with a
bounded warning (node and failure class only, no collection, topic filter or credential data).
Covers the documented OTP 28 `erpc:multicall/5` result shapes for a missing callback, a
timeout, a lost connection and a generic runtime error.
""".
t_topic_metrics_node_skipped(TCConfig) ->
    start_local(?FUNCTION_NAME, TCConfig, #{
        extra_apps => [emqx_auth_mnesia, emqx_auth, emqx_mt]
    }),
    Ns = <<"ns1">>,
    ok = emqx_topic_metrics2:register(<<"metric-a">>, <<"tenant/a/#">>, Ns),
    publish_as(Ns, <<"tenant/a/1">>),
    publish_as(Ns, <<"tenant/a/1">>),
    GlobalAdmin = global_admin_auth_header(),
    Rows = [{<<"metric-a">>, <<"tenant/a/#">>, Ns, 2, 20, 2}],
    Peer = 'old-release@127.0.0.1',

    %% The exact result shape erpc returns for a peer that does not export
    %% the callback, which is how an older release looks.  The node name is
    %% positional in `erpc:multicall/5', not part of the term.
    [{error, UndefReason}] = emqx_prometheus_proto_v3:raw_prom_data(
        [node()], emqx_prometheus_topic_metrics, not_exported, [], 5_000
    ),
    Cases = [
        {unsupported, {error, UndefReason}},
        {timeout, {error, {erpc, timeout}}},
        {unreachable, {error, {erpc, noconnection}}},
        {error, {error, {exception, badarith, [{erlang, '+', [1, a], []}]}}}
    ],

    ok = meck:new(emqx_bpapi, [passthrough, no_link]),
    ok = meck:new(emqx_prometheus_proto_v3, [passthrough, no_link]),
    try
        %% Two nodes: the local one answers, the peer fails with each of
        %% the result shapes above.
        ok = meck:expect(emqx_bpapi, nodes_supporting_bpapi_version, fun(_Api, _Vsn) ->
            [node(), Peer]
        end),
        lists:foreach(
            fun(Mode) ->
                lists:foreach(
                    fun({Class, ErrorResult}) ->
                        %% Partial failure: the answering node's series are
                        %% served in full and only the peer is reported.
                        ok = meck:expect(
                            emqx_prometheus_proto_v3,
                            raw_prom_data,
                            fun(Nodes0, M, F, Args, _T) ->
                                [node_result(N, M, F, Args, ErrorResult) || N <- Nodes0]
                            end
                        ),
                        {PartialRes, PartialLogs} = capture_topic_metrics(warning, Mode, #{
                            auth_header => GlobalAdmin
                        }),
                        {200, Partial} = PartialRes,
                        ?assertEqual(topic_metrics_expected(Mode, Rows), Partial),
                        ?assertEqual(
                            [skipped_node_log(Peer, Class)], skipped_node_logs(PartialLogs)
                        ),

                        %% No node answers: the whole parsed result is empty
                        %% and each skipped node gets exactly one report.
                        ok = meck:expect(
                            emqx_prometheus_proto_v3,
                            raw_prom_data,
                            fun(Nodes0, _M, _F, _Args, _T) ->
                                [ErrorResult || _ <- Nodes0]
                            end
                        ),
                        {AllFailedRes, AllFailedLogs} = capture_topic_metrics(warning, Mode, #{
                            auth_header => GlobalAdmin
                        }),
                        {200, AllFailed} = AllFailedRes,
                        ?assertEqual(#{}, AllFailed),
                        ?assertEqual(
                            [skipped_node_log(node(), Class), skipped_node_log(Peer, Class)],
                            skipped_node_logs(AllFailedLogs)
                        )
                    end,
                    Cases
                )
            end,
            [?PROM_DATA_MODE__ALL_NODES_AGGREGATED, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED]
        )
    after
        meck:unload(emqx_prometheus_proto_v3),
        meck:unload(emqx_bpapi)
    end,
    ok.

%% The local node answers; the simulated peer fails.
node_result(N, M, F, Args, ErrorResult) ->
    case N =:= node() of
        true -> {ok, apply(M, F, Args)};
        false -> ErrorResult
    end.

-doc """
Checks that `/prometheus/topic_metrics' is exempt from the all-namespaces limiter: with a
finite rate, repeated all-namespaces scrapes still succeed, while the sibling endpoints
sharing that limiter keep being limited.
""".
t_topic_metrics_not_rate_limited(TCConfig) ->
    start_local(?FUNCTION_NAME, TCConfig, #{
        extra_apps => [emqx_auth_mnesia, emqx_auth, emqx_mt]
    }),
    ok = emqx_topic_metrics2:register(<<"metric-a">>, <<"tenant/a/#">>, <<"ns1">>),
    GlobalAdmin = global_admin_auth_header(),

    %% The suite starts with the limiter disabled; turn on a finite rate
    %% through the public config API (the whole `prometheus' root, as the
    %% limiter is propagated by its post-config-update hook).
    Raw0 = emqx_config:get_raw([prometheus]),
    Raw = emqx_utils_maps:deep_put(
        [<<"namespaced_metrics_limiter">>, <<"rate">>], Raw0, <<"1/s">>
    ),
    {{ok, _}, {ok, _}} = ?wait_async_action(
        emqx_conf:update([prometheus], Raw, #{override_to => cluster}),
        #{?snk_kind := "prometheus_api_limiter_updated"},
        5_000
    ),

    %% Repeated all-namespaces scrapes are not limited.
    lists:foreach(
        fun(_) ->
            ?assertMatch(
                {200, _},
                get_topic_metrics(?PROM_DATA_MODE__NODE, #{auth_header => GlobalAdmin})
            )
        end,
        lists:seq(1, 5)
    ),

    %% The sibling endpoint shares the limiter: its first all-namespaces
    %% request still succeeds (the scrapes above consumed no quota), then
    %% it gets limited again.
    Statuses = [
        element(1, get_namespaced_stats(?PROM_DATA_MODE__NODE, #{auth_header => GlobalAdmin}))
     || _ <- lists:seq(1, 10)
    ],
    ?assertEqual(200, hd(Statuses)),
    ?assert(lists:member(429, Statuses), Statuses),
    ok.

%% `emqx_cth_log_capture:capture/2' returns the captured reports only, so
%% pass the request result back through a message.
capture_topic_metrics(Level, Mode, Opts) ->
    Self = self(),
    Logs = emqx_cth_log_capture:capture(Level, fun() ->
        Self ! {captured_request, get_topic_metrics(Mode, Opts)},
        ok
    end),
    receive
        {captured_request, Result} -> {Result, Logs}
    end.

skipped_node_logs(Logs) ->
    [
        Log
     || Log <- Logs,
        maps:get(msg, Log, undefined) =:= "prometheus_namespaced_metrics_node_skipped"
    ].

%% The bounded report holds the node and the failure class only.
skipped_node_log(Node, Class) ->
    #{
        msg => "prometheus_namespaced_metrics_node_skipped",
        collector => emqx_prometheus_topic_metrics,
        node => Node,
        class => Class
    }.
