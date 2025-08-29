%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rocketmq_SUITE).

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

-define(CONNECTOR_TYPE, rocketmq).
-define(CONNECTOR_TYPE_BIN, <<"rocketmq">>).
-define(ACTION_TYPE, rocketmq).
-define(ACTION_TYPE_BIN, <<"rocketmq">>).

-define(PROXY_NAME, "rocketmq").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(tcp, tcp).
-define(tls, tls).
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
            emqx_bridge_rocketmq,
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

init_per_group(?tcp, TCConfig) ->
    [
        {servers, <<"toxiproxy:9876">>},
        {enable_tls, false}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    [
        {servers, <<"rocketmq_namesrv_ssl:9876">>},
        {enable_tls, true}
        | TCConfig
    ];
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

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{
        <<"servers">> => get_config(servers, TCConfig, <<"toxiproxy:9876">>),
        <<"ssl">> => #{
            <<"enable">> => get_config(enable_tls, TCConfig, false),
            <<"verify">> => <<"verify_none">>
        }
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
        <<"servers">> => <<"toxiproxy:9876">>,
        <<"access_key">> => <<"RocketMQ">>,
        <<"secret_key">> => <<"12345678">>,
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
            <<"topic">> => <<"TopicTest">>
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

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [[?tcp], [?tls]];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "rocketmq_connector_stop").

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{failure_status => ?status_connecting}).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [?tcp, ?sync, ?without_batch],
        [?tcp, ?sync, ?with_batch],
        [?tcp, ?async, ?without_batch],
        [?tcp, ?async, ?with_batch],
        [?tls, ?sync, ?without_batch]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    TraceChecker = fun(Trace) ->
        ?assertMatch([#{result := ok}], ?of_kind(rocketmq_connector_query_return, Trace)),
        ok
    end,
    PostPublishFn = fun(_Context) ->
        {ok, _} = ?block_until(#{?snk_kind := rocketmq_connector_query_return}, 10_000)
    end,
    Opts = #{
        trace_checkers => [TraceChecker],
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

%% Check that we can not connect to the SSL only RocketMQ instance
%% with incorrect SSL options
t_setup_via_config_ssl_host_bad_ssl_opts() ->
    [{matrix, true}].
t_setup_via_config_ssl_host_bad_ssl_opts(matrix) ->
    [[?tls]];
t_setup_via_config_ssl_host_bad_ssl_opts(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"disconnected">>}},
        create_connector_api(TCConfig, #{
            <<"ssl">> => #{<<"verify">> => <<"verify_peer">>}
        })
    ),
    ok.

t_setup_two_actions_via_http_api_and_publish(TCConfig) ->
    ActionName1 = <<"action1">>,
    ActionName2 = <<"action2">>,
    TCConfigAction1 = [{action_name, ActionName1} | TCConfig],
    TCConfigAction2 = [{action_name, ActionName2} | TCConfig],
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfigAction1, #{}),
    {201, _} = create_action_api(TCConfigAction2, #{
        <<"parameters">> => #{<<"topic">> => <<"Topic2">>}
    }),
    #{topic := Topic1} = simple_create_rule_api(TCConfigAction1),
    #{topic := Topic2} = simple_create_rule_api(TCConfigAction2),
    C = start_client(),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic1, <<"hey">>),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic2, <<"hey">>),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_acl_deny(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"topic">> => <<"DENY_TOPIC">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := #{<<"code">> := 1}}],
                ?of_kind(rocketmq_connector_query_return, Trace)
            ),
            ok
        end
    ),
    ok.

-doc """
Smoke test for templating key and tag values.
""".
t_key_tag_templates() ->
    [{matrix, true}].
t_key_tag_templates(matrix) ->
    [[?tcp, ?without_batch], [?tcp, ?with_batch]];
t_key_tag_templates(TCConfig) when is_list(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"key">> => <<"${.mykey}">>,
                <<"tag">> => <<"${.mytag}">>
            },
            <<"resource_opts">> => #{<<"batch_size">> => BatchSize}
        })
    ),
    #{topic := Topic} = simple_create_rule_api(
        <<
            "select *, payload.tag as mytag, payload.key as mykey"
            " from \"${t}\" "
        >>,
        TCConfig
    ),
    C = start_client(),
    Payload = emqx_utils_json:encode(#{<<"key">> => <<"k1">>, <<"tag">> => <<"t1">>}),
    ct:timetrap({seconds, 10}),
    ok = snabbkaffe:start_trace(),
    {{ok, _}, {ok, #{data := Data}}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, Payload, [{qos, 2}]),
            #{?snk_kind := "rocketmq_rendered_data"}
        ),
    case BatchSize of
        1 ->
            ?assertMatch(
                {_Payload, #{
                    key := <<"k1">>,
                    tag := <<"t1">>
                }},
                Data
            );
        _ ->
            ?assertMatch(
                [
                    {_Payload, #{
                        key := <<"k1">>,
                        tag := <<"t1">>
                    }}
                    | _
                ],
                Data
            )
    end,
    ok.

-doc """
Checks that we require `key` to be set if `key_dispatch` strategy is used.
""".
t_key_template_required(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"must provide a key template if strategy is key dispatch">>
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"strategy">> => <<"key_dispatch">>
            }
        })
    ),
    ok.

-doc """
Checks that we emit a warning about using deprecated strategy which used templates in that
config key to imply key dispatch.
""".
t_deprecated_templated_strategy(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{})
    ),
    ct:timetrap({seconds, 5}),
    ok = snabbkaffe:start_trace(),
    ?assertMatch(
        {{201, #{<<"status">> := <<"connected">>}}, {ok, _}},
        ?wait_async_action(
            create_action_api(TCConfig, #{
                <<"parameters">> => #{
                    <<"strategy">> => <<"${some_template}">>
                }
            }),
            #{?snk_kind := "rocketmq_deprecated_placeholder_strategy"}
        )
    ),
    ok.
