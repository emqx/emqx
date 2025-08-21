%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_opents_SUITE).

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

-define(CONNECTOR_TYPE, opents).
-define(CONNECTOR_TYPE_BIN, <<"opents">>).
-define(ACTION_TYPE, opents).
-define(ACTION_TYPE_BIN, <<"opents">>).

-define(PROXY_NAME, "opents").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

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
            emqx_bridge_opents,
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
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
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
        <<"server">> => <<"http://toxiproxy.emqx.net:4242">>,
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
            <<"data">> => []
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

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

opentsdb_query(TCConfig) ->
    ?retry(200, 10, begin
        {ok, {{_, 200, _}, _, IoTDBResult}} = do_opentsdb_query(TCConfig),
        emqx_utils_json:decode(IoTDBResult)
    end).

do_opentsdb_query(TCConfig) ->
    Metric = get_config(action_name, TCConfig),
    Path = <<"/api/query">>,
    Opts = #{return_all => true},
    Body = #{
        start => <<"1h-ago">>,
        queries => [
            #{
                aggregator => <<"last">>,
                metric => Metric,
                tags => #{
                    host => <<"*">>
                }
            }
        ],
        showTSUID => false,
        showQuery => false,
        delete => false
    },
    opentsdb_request(Path, Body, Opts).

opentsdb_request(Path, Body) ->
    opentsdb_request(Path, Body, #{}).

opentsdb_request(Path, Body, Opts) ->
    ServerURL = <<"http://toxiproxy.emqx.net:4242">>,
    URL = <<ServerURL/binary, Path/binary>>,
    emqx_mgmt_api_test_util:request_api(post, URL, [], [], Body, Opts).

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

update_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:update_bridge_api2(Config, Overrides).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

default_rule_sql() ->
    <<
        "select payload.metric as metric,"
        " payload.tags as tags,"
        " payload.value as value"
        " from \"${t}\" "
    >>.

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, opents_bridge_stopped).

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{
        failure_status => ?status_connecting
    }).

t_rule_action(TCConfig) when is_list(TCConfig) ->
    Value = 12,
    PayloadFn = fun() ->
        emqx_utils_json:encode(#{
            <<"metric">> => atom_to_binary(?FUNCTION_NAME),
            <<"tags">> => #{<<"host">> => <<"serverA">>},
            <<"value">> => Value
        })
    end,
    PostPublishFn = fun(_Context) ->
        ?retry(200, 10, begin
            QResult = opentsdb_query(TCConfig),
            ?assertMatch(
                [
                    #{
                        <<"metric">> := _,
                        <<"dps">> := _
                    }
                ],
                QResult
            ),
            [#{<<"dps">> := Dps}] = QResult,
            ?assertMatch([Value | _], maps:values(Dps))
        end)
    end,
    RuleTopic = atom_to_binary(?FUNCTION_NAME),
    RuleSQL = emqx_bridge_v2_testlib:fmt(default_rule_sql(), #{t => RuleTopic}),
    Opts = #{
        rule_topic => RuleTopic,
        sql => RuleSQL,
        payload_fn => PayloadFn,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_query_invalid_data(TCConfig) ->
    %% Missing `value` key
    Payload = emqx_utils_json:encode(#{
        <<"metric">> => atom_to_binary(?FUNCTION_NAME),
        <<"tags">> => #{<<"host">> => <<"serverA">>}
    }),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(default_rule_sql(), TCConfig),
    C = start_client(),
    {_, {ok, _}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, Payload),
            #{?snk_kind := opents_connector_query_return},
            5_000
        ),
    ok.

t_tags_validator(TCConfig) ->
    %% Create without data  configured
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    ?assertMatch(
        {200, _},
        update_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"data">> => [
                    #{
                        <<"metric">> => <<"${metric}">>,
                        <<"tags">> => <<"${tags}">>,
                        <<"value">> => <<"${payload.value}">>
                    }
                ]
            }
        })
    ),
    ?assertMatch(
        {400, _},
        update_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"data">> => [
                    #{
                        <<"metric">> => <<"${metric}">>,
                        <<"tags">> => <<"text">>,
                        <<"value">> => <<"${payload.value}">>
                    }
                ]
            }
        })
    ),
    ok.

t_list_tags(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"data">> => [
                #{
                    <<"metric">> => <<"${metric}">>,
                    <<"tags">> => #{<<"host">> => <<"valueA">>},
                    <<"value">> => <<"${value}">>
                }
            ]
        }
    }),
    #{topic := Topic} = simple_create_rule_api(default_rule_sql(), TCConfig),
    C = start_client(),
    Metric = atom_to_binary(?FUNCTION_NAME),
    Payload = emqx_utils_json:encode(#{
        <<"value">> => 12,
        <<"metric">> => Metric,
        <<"tags">> => #{<<"host">> => <<"serverA">>}
    }),
    emqtt:publish(C, Topic, Payload),
    ?retry(
        200,
        10,
        ?assertMatch(
            [
                #{
                    <<"metric">> := Metric,
                    <<"tags">> := #{<<"host">> := <<"valueA">>}
                }
            ],
            opentsdb_query(TCConfig)
        )
    ),
    ok.

t_raw_int_value(TCConfig) ->
    raw_value_test(42, TCConfig).

t_raw_float_value(TCConfig) ->
    raw_value_test(42.5, TCConfig).

raw_value_test(RawValue, TCConfig) ->
    Metric = get_config(action_name, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"data">> => [
                #{
                    <<"metric">> => <<"${metric}">>,
                    <<"tags">> => <<"${tags}">>,
                    <<"value">> => RawValue
                }
            ]
        }
    }),
    #{topic := Topic} = simple_create_rule_api(default_rule_sql(), TCConfig),
    C = start_client(),
    Value = 12,
    Payload = emqx_utils_json:encode(#{
        <<"value">> => Value,
        <<"metric">> => Metric,
        <<"tags">> => #{<<"host">> => <<"serverA">>}
    }),
    emqtt:publish(C, Topic, Payload),
    ?retry(200, 10, begin
        QResult = opentsdb_query(TCConfig),
        ?assertMatch(
            [
                #{
                    <<"metric">> := Metric,
                    <<"dps">> := _
                }
            ],
            QResult
        ),
        [#{<<"dps">> := Dps}] = QResult,
        ?assertMatch([RawValue | _], maps:values(Dps))
    end),
    ok.

t_list_tags_with_var(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"data">> => [
                #{
                    <<"metric">> => <<"${metric}">>,
                    <<"tags">> => #{<<"host">> => <<"${value}">>},
                    <<"value">> => <<"${value}">>
                }
            ]
        }
    }),
    #{topic := Topic} = simple_create_rule_api(default_rule_sql(), TCConfig),
    C = start_client(),
    Metric = atom_to_binary(?FUNCTION_NAME),
    Payload = emqx_utils_json:encode(#{
        <<"value">> => 12,
        <<"metric">> => Metric,
        <<"tags">> => #{<<"host">> => <<"serverA">>}
    }),
    emqtt:publish(C, Topic, Payload),
    ?retry(
        200,
        10,
        ?assertMatch(
            [
                #{
                    <<"metric">> := Metric,
                    <<"tags">> := #{<<"host">> := <<"12">>}
                }
            ],
            opentsdb_query(TCConfig)
        )
    ),
    ok.
