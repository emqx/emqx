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

get_prometheus(URL, Mode, Opts) ->
    Ns = maps:get(ns, Opts, undefined),
    OnlyGlobal = maps:get(only_global, Opts, undefined),
    QueryString = uri_string:compose_query(
        lists:flatten([
            {"mode", atom_to_binary(Mode)},
            [{"ns", Ns} || Ns /= undefined],
            [{"only_global", OnlyGlobal} || OnlyGlobal /= undefined]
        ])
    ),
    AuthHeader = maps:get(auth_header, Opts, {"no", "auth"}),
    {Status, Response} = emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => URL,
        extra_headers => [],
        query_params => QueryString,
        auth_header => AuthHeader
    }),
    case Status of
        200 ->
            {Status, parse_prometheus(Response)};
        _ ->
            {Status, Response}
    end.

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
