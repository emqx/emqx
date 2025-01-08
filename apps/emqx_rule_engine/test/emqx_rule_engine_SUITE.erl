%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%-define(PROPTEST(M,F), true = proper:quickcheck(M:F())).
-define(TMP_RULEID, atom_to_binary(?FUNCTION_NAME)).

all() ->
    [
        {group, engine},
        {group, funcs},
        {group, registry},
        {group, runtime},
        {group, events},
        {group, telemetry},
        {group, bugs},
        {group, metrics},
        {group, metrics_simple},
        {group, metrics_fail},
        {group, metrics_fail_simple},
        {group, tracing},
        {group, command_line}
    ].

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 30}}].

groups() ->
    [
        {engine, [sequence], [t_create_rule]},
        {funcs, [], [t_kv_store]},
        {registry, [sequence], [
            t_add_get_remove_rule,
            t_add_get_remove_rules,
            t_create_existing_rule,
            t_get_rules_for_topic,
            t_get_rules_for_topic_2,
            t_get_rules_with_same_event,
            t_get_rule_ids_by_action,
            t_ensure_action_removed
        ]},
        {runtime, [], [
            t_match_atom_and_binary,
            t_sqlselect_0,
            t_sqlselect_00,
            t_sqlselect_with_3rd_party_impl,
            t_sqlselect_with_3rd_party_impl2,
            t_sqlselect_with_3rd_party_funcs_unknown,
            t_sqlselect_001,
            t_sqlselect_002,
            t_sqlselect_inject_props,
            t_sqlselect_01,
            t_sqlselect_02,
            t_sqlselect_03,
            t_sqlselect_1,
            t_sqlselect_2,
            t_sqlselect_3,
            t_direct_dispatch,
            t_sqlselect_message_publish_event_keep_original_props_1,
            t_sqlselect_message_publish_event_keep_original_props_2,
            t_sqlselect_missing_template_vars_render_as_undefined,
            t_sqlparse_event_1,
            t_sqlparse_event_2,
            t_sqlparse_event_3,
            t_sqlparse_foreach_1,
            t_sqlparse_foreach_2,
            t_sqlparse_foreach_3,
            t_sqlparse_foreach_4,
            t_sqlparse_foreach_5,
            t_sqlparse_foreach_6,
            t_sqlparse_foreach_7,
            t_sqlparse_foreach_8,
            t_sqlparse_foreach_9,
            t_sqlparse_case_when_1,
            t_sqlparse_case_when_2,
            t_sqlparse_case_when_3,
            t_sqlparse_array_index_1,
            t_sqlparse_array_index_2,
            t_sqlparse_array_index_3,
            t_sqlparse_array_index_4,
            t_sqlparse_array_index_5,
            t_sqlparse_array_with_expressions,
            t_sqlparse_select_matadata_1,
            t_sqlparse_array_range_1,
            t_sqlparse_array_range_2,
            t_sqlparse_true_false,
            t_sqlparse_undefined_variable,
            t_sqlparse_new_map,
            t_sqlparse_invalid_json,
            t_sqlselect_as_put,
            t_sqlselect_client_attr
        ]},
        {events, [], [
            t_events,
            t_event_client_disconnected_normal,
            t_event_client_disconnected_kicked,
            t_event_client_disconnected_discarded,
            t_event_client_disconnected_takenover,
            t_event_client_disconnected_takenover_2
        ]},
        {telemetry, [], [
            t_get_basic_usage_info_0,
            t_get_basic_usage_info_1
        ]},
        {bugs, [], [
            t_sqlparse_payload_as,
            t_sqlparse_nested_get
        ]},
        {metrics, [], [
            t_rule_metrics_sync,
            t_rule_metrics_async
        ]},
        {metrics_simple, [], [
            t_rule_metrics_sync,
            t_rule_metrics_async
        ]},
        {metrics_fail, [], [
            t_rule_metrics_sync_fail,
            t_rule_metrics_async_fail,
            t_failed_rule_metrics
        ]},
        {metrics_fail_simple, [], [
            t_rule_metrics_sync_fail,
            t_rule_metrics_async_fail
        ]},
        {tracing, [], [
            t_trace_rule_id
        ]},
        {command_line, [], [
            t_command_line_list_print_rule
        ]}
    ].

%%------------------------------------------------------------------------------
%% Overall setup/teardown
%%------------------------------------------------------------------------------

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        lists:flatten([
            emqx,
            emqx_conf,
            emqx_rule_engine,
            emqx_auth,
            emqx_bridge,
            [
                {emqx_schema_validation, #{
                    config => #{
                        <<"schema_validation">> => #{
                            <<"validations">> => [
                                #{
                                    <<"name">> => <<"v1">>,
                                    <<"topics">> => [<<"sv/fail">>],
                                    <<"strategy">> => <<"all_pass">>,
                                    <<"failure_action">> => <<"drop">>,
                                    <<"checks">> => [
                                        #{
                                            <<"type">> => <<"sql">>,
                                            <<"sql">> => <<"select 1 where false">>
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }}
             || is_ee()
            ],
            [
                {emqx_message_transformation, #{
                    config => #{
                        <<"message_transformation">> => #{
                            <<"transformations">> => [
                                #{
                                    <<"name">> => <<"t1">>,
                                    <<"topics">> => <<"mt/fail">>,
                                    <<"failure_action">> => <<"drop">>,
                                    <<"payload_decoder">> => #{<<"type">> => <<"json">>},
                                    <<"payload_encoder">> => #{<<"type">> => <<"json">>},
                                    <<"operations">> => []
                                }
                            ]
                        }
                    }
                }}
             || is_ee()
            ]
        ]),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

set_special_configs(emqx_auth) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => atom_to_binary(allow),
            <<"cache">> => #{<<"enable">> => atom_to_binary(true)},
            <<"sources">> => []
        }
    ),
    ok;
set_special_configs(_) ->
    ok.
on_resource_create(_id, _) -> #{}.
on_resource_destroy(_id, _) -> ok.
on_get_resource_status(_id, _) -> #{}.

%%------------------------------------------------------------------------------
%% Group specific setup/teardown
%%------------------------------------------------------------------------------

group(_Groupname) ->
    [].

-define(BRIDGE_IMPL, emqx_bridge_mqtt_connector).
init_per_group(registry, Config) ->
    Config;
init_per_group(metrics_fail, Config) ->
    meck:expect(?BRIDGE_IMPL, on_query, 3, {error, {unrecoverable_error, mecked_failure}}),
    meck:expect(?BRIDGE_IMPL, on_query_async, 4, {error, {unrecoverable_error, mecked_failure}}),
    [{mecked, [?BRIDGE_IMPL]} | Config];
init_per_group(metrics_simple, Config) ->
    meck:new(?BRIDGE_IMPL, [non_strict, no_link, passthrough]),
    meck:expect(?BRIDGE_IMPL, query_mode, fun
        (#{resource_opts := #{query_mode := sync}}) -> simple_sync;
        (_) -> simple_async
    end),
    [{mecked, [?BRIDGE_IMPL]} | Config];
init_per_group(metrics_fail_simple, Config) ->
    meck:new(?BRIDGE_IMPL, [non_strict, no_link, passthrough]),
    meck:expect(?BRIDGE_IMPL, query_mode, fun
        (#{resource_opts := #{query_mode := sync}}) -> simple_sync;
        (_) -> simple_async
    end),
    meck:expect(?BRIDGE_IMPL, on_query, 3, {error, {unrecoverable_error, mecked_failure}}),
    meck:expect(?BRIDGE_IMPL, on_query_async, fun(_, _, {_ReplyFun, _Args}, _) ->
        {error, {unrecoverable_error, mecked_failure}}
    end),
    [{mecked, [?BRIDGE_IMPL]} | Config];
init_per_group(_Groupname, Config) ->
    Config.

end_per_group(_Groupname, Config) ->
    case ?config(mecked, Config) of
        undefined -> ok;
        Mecked -> meck:unload(Mecked)
    end.
%%------------------------------------------------------------------------------
%% Testcase specific setup/teardown
%%------------------------------------------------------------------------------

init_per_testcase(t_events, Config) ->
    init_events_counters(),
    SQL =
        "SELECT * FROM \"$events/client_connected\", "
        "\"$events/client_disconnected\", "
        "\"$events/client_connack\", "
        "\"$events/client_check_authz_complete\", "
        "\"$events/client_check_authn_complete\", "
        "\"$events/session_subscribed\", "
        "\"$events/session_unsubscribed\", "
        "\"$events/message_acked\", "
        "\"$events/message_delivered\", "
        "\"$events/message_dropped\", "
        "\"$events/delivery_dropped\", "
        "\"$events/schema_validation_failed\", "
        "\"$events/message_transformation_failed\", "
        "\"t1\"",
    {ok, Rule} = emqx_rule_engine:create_rule(
        #{
            id => <<"rule:t_events">>,
            sql => SQL,
            actions => [
                #{
                    function => <<"emqx_rule_engine_SUITE:action_record_triggered_events">>,
                    args => #{}
                }
            ],
            description => <<"to console and record triggered events">>
        }
    ),
    ?assertMatch(#{id := <<"rule:t_events">>}, Rule),
    [{hook_points_rules, Rule} | Config];
init_per_testcase(t_get_basic_usage_info_1, Config) ->
    meck:new(emqx_bridge, [passthrough, no_link, no_history]),
    meck:expect(emqx_bridge, lookup, fun(_Type, _Name) -> {ok, #{mocked => true}} end),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_events, Config) ->
    ets:delete(events_record_tab),
    ok = delete_rule(?config(hook_points_rules, Config)),
    emqx_common_test_helpers:call_janitor(),
    ok;
end_per_testcase(t_get_basic_usage_info_1, _Config) ->
    meck:unload(),
    emqx_common_test_helpers:call_janitor(),
    ok;
end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Test cases for rule engine
%%------------------------------------------------------------------------------
t_create_rule(_Config) ->
    {ok, #{id := Id}} = emqx_rule_engine:create_rule(
        #{
            sql => <<"select * from \"t/a\"">>,
            id => <<"t_create_rule">>,
            actions => [#{function => console}],
            description => <<"debug rule">>
        }
    ),
    ct:pal("======== emqx_rule_engine:get_rules :~p", [emqx_rule_engine:get_rules()]),
    ?assertMatch(
        {ok, #{id := Id, from := [<<"t/a">>]}},
        emqx_rule_engine:get_rule(Id)
    ),
    delete_rule(Id),
    ok.

%%------------------------------------------------------------------------------
%% Test cases for rule funcs
%%------------------------------------------------------------------------------

t_kv_store(_) ->
    undefined = emqx_rule_funcs:kv_store_get(<<"abc">>),
    <<"not_found">> = emqx_rule_funcs:kv_store_get(<<"abc">>, <<"not_found">>),
    emqx_rule_funcs:kv_store_put(<<"abc">>, 1),
    1 = emqx_rule_funcs:kv_store_get(<<"abc">>),
    emqx_rule_funcs:kv_store_del(<<"abc">>),
    undefined = emqx_rule_funcs:kv_store_get(<<"abc">>).

t_function_clause_errors(_Config) ->
    SQL0 = <<"select upper(xxxx) from \"t/a\"">>,
    Payload = <<"{}">>,
    ?assertMatch(
        {error,
            {select_and_transform_error,
                {throw,
                    #{
                        arguments := [undefined],
                        reason := bad_sql_function_argument,
                        function_name := upper
                    },
                    _Stack}}},
        emqx_rule_sqltester:test(
            #{
                sql => SQL0,
                context => #{payload => Payload, topic => <<"t/a">>}
            }
        )
    ),
    SQL1 = <<"foreach xs as x do upper(xxxx) from \"t/a\"">>,
    ?assertMatch(
        {error, {
            {doeach_error,
                {throw,
                    #{
                        arguments := [undefined],
                        reason := bad_sql_function_argument,
                        function_name := upper
                    },
                    _Stack0}},
            _Stack1
        }},
        emqx_rule_sqltester:test(
            #{
                sql => SQL1,
                context => #{payload => Payload, xs => [1, 2, 3], topic => <<"t/a">>}
            }
        )
    ),
    SQL2 = <<"foreach upper(xxxx) as x from \"t/a\"">>,
    ?assertMatch(
        {error,
            {select_and_collect_error,
                {throw,
                    #{
                        arguments := [undefined],
                        reason := bad_sql_function_argument,
                        function_name := upper
                    },
                    _Stack}}},
        emqx_rule_sqltester:test(
            #{
                sql => SQL2,
                context => #{payload => Payload, topic => <<"t/a">>}
            }
        )
    ),
    ok.

%%------------------------------------------------------------------------------
%% Test cases for rule registry
%%------------------------------------------------------------------------------

t_add_get_remove_rule(_Config) ->
    RuleId0 = <<"rule-debug-0">>,
    ok = create_rule(make_simple_rule(RuleId0)),
    ?assertMatch({ok, #{id := RuleId0}}, emqx_rule_engine:get_rule(RuleId0)),
    ok = delete_rule(RuleId0),
    ?assertEqual(not_found, emqx_rule_engine:get_rule(RuleId0)),

    RuleId1 = <<"rule-debug-1">>,
    Rule1 = make_simple_rule(RuleId1),
    ok = create_rule(Rule1),
    ?assertMatch({ok, #{id := RuleId1}}, emqx_rule_engine:get_rule(RuleId1)),
    ok = delete_rule(Rule1),
    ?assertEqual(not_found, emqx_rule_engine:get_rule(RuleId1)),
    ok.

t_add_get_remove_rules(_Config) ->
    delete_rules_by_ids([Id || #{id := Id} <- emqx_rule_engine:get_rules()]),
    ok = create_rules(
        [
            make_simple_rule(<<"rule-debug-1">>),
            make_simple_rule(<<"rule-debug-2">>)
        ]
    ),
    ?assertEqual(2, length(emqx_rule_engine:get_rules())),
    ok = delete_rules_by_ids([<<"rule-debug-1">>, <<"rule-debug-2">>]),
    ?assertEqual([], emqx_rule_engine:get_rules()),
    ok.

t_create_existing_rule(_Config) ->
    %% create a rule using given rule id
    {ok, _} = emqx_rule_engine:create_rule(
        #{
            id => <<"an_existing_rule">>,
            sql => <<"select * from \"t/#\"">>,
            actions => [#{function => console}]
        }
    ),
    {ok, #{sql := SQL}} = emqx_rule_engine:get_rule(<<"an_existing_rule">>),
    ?assertEqual(<<"select * from \"t/#\"">>, SQL),

    ok = delete_rule(<<"an_existing_rule">>),
    ?assertEqual(not_found, emqx_rule_engine:get_rule(<<"an_existing_rule">>)),
    ok.

t_get_rules_for_topic(_Config) ->
    Len0 = length(emqx_rule_engine:get_rules_for_topic(<<"simple/topic">>)),
    ok = create_rules(
        [
            make_simple_rule(<<"rule-debug-1">>),
            make_simple_rule(<<"rule-debug-2">>)
        ]
    ),
    ?assertEqual(Len0 + 2, length(emqx_rule_engine:get_rules_for_topic(<<"simple/topic">>))),
    ok = delete_rules_by_ids([<<"rule-debug-1">>, <<"rule-debug-2">>]),
    ok.

t_get_rules_ordered_by_ts(_Config) ->
    Now = erlang:system_time(microsecond),
    ok = create_rules(
        [
            make_simple_rule_with_ts(<<"rule-debug-0">>, Now + 1),
            make_simple_rule_with_ts(<<"rule-debug-1">>, Now + 2),
            make_simple_rule_with_ts(<<"rule-debug-2">>, Now + 3)
        ]
    ),
    ?assertMatch(
        [
            #{id := <<"rule-debug-0">>},
            #{id := <<"rule-debug-1">>},
            #{id := <<"rule-debug-2">>}
        ],
        emqx_rule_engine:get_rules_ordered_by_ts()
    ).

t_get_rules_for_topic_2(_Config) ->
    Len0 = length(emqx_rule_engine:get_rules_for_topic(<<"simple/1">>)),
    ok = create_rules(
        [
            make_simple_rule(<<"rule-debug-1">>, _1 = <<"select * from \"simple/#\"">>),
            make_simple_rule(<<"rule-debug-2">>, _2 = <<"select * from \"simple/+\"">>),
            make_simple_rule(<<"rule-debug-3">>, <<"select * from \"simple/+/1\"">>),
            make_simple_rule(<<"rule-debug-4">>, _3 = <<"select * from \"simple/1\"">>),
            make_simple_rule(
                <<"rule-debug-5">>,
                _4 = <<"select * from \"simple/2\", \"simple/+\", \"simple/3\"">>
            ),
            make_simple_rule(
                <<"rule-debug-6">>,
                <<"select * from \"simple/2\", \"simple/3\", \"simple/4\"">>
            )
        ]
    ),
    ?assertEqual(Len0 + 4, length(emqx_rule_engine:get_rules_for_topic(<<"simple/1">>))),
    ok = delete_rules_by_ids([
        <<"rule-debug-1">>,
        <<"rule-debug-2">>,
        <<"rule-debug-3">>,
        <<"rule-debug-4">>,
        <<"rule-debug-5">>,
        <<"rule-debug-6">>
    ]),
    ok.

t_get_rules_with_same_event(_Config) ->
    PubT = <<"simple/1">>,
    PubN = length(emqx_rule_engine:get_rules_with_same_event(PubT)),
    ?assertEqual([], emqx_rule_engine:get_rules_with_same_event(<<"$events/client_connected">>)),
    ?assertEqual([], emqx_rule_engine:get_rules_with_same_event(<<"$events/client_disconnected">>)),
    ?assertEqual([], emqx_rule_engine:get_rules_with_same_event(<<"$events/session_subscribed">>)),
    ?assertEqual(
        [], emqx_rule_engine:get_rules_with_same_event(<<"$events/session_unsubscribed">>)
    ),
    ?assertEqual([], emqx_rule_engine:get_rules_with_same_event(<<"$events/message_delivered">>)),
    ?assertEqual([], emqx_rule_engine:get_rules_with_same_event(<<"$events/message_acked">>)),
    ?assertEqual([], emqx_rule_engine:get_rules_with_same_event(<<"$events/message_dropped">>)),
    ok = create_rules(
        [
            make_simple_rule(<<"r1">>, <<"select * from \"simple/#\"">>),
            make_simple_rule(<<"r2">>, <<"select * from \"abc/+\"">>),
            make_simple_rule(
                <<"r3">>,
                <<"select * from \"$events/client_connected\"">>
            ),
            make_simple_rule(
                <<"r4">>,
                <<"select * from \"$events/client_disconnected\"">>
            ),
            make_simple_rule(
                <<"r5">>,
                <<"select * from \"$events/session_subscribed\"">>
            ),
            make_simple_rule(
                <<"r6">>,
                <<"select * from \"$events/session_unsubscribed\"">>
            ),
            make_simple_rule(
                <<"r7">>,
                <<"select * from \"$events/message_delivered\"">>
            ),
            make_simple_rule(
                <<"r8">>,
                <<"select * from \"$events/message_acked\"">>
            ),
            make_simple_rule(
                <<"r9">>,
                <<"select * from \"$events/message_dropped\"">>
            ),
            make_simple_rule(
                <<"r10">>,
                <<
                    "select * from \"t/1\", "
                    "\"$events/session_subscribed\", \"$events/client_connected\""
                >>
            )
        ]
    ),
    ?assertEqual(PubN + 3, length(emqx_rule_engine:get_rules_with_same_event(PubT))),
    ?assertEqual(
        2, length(emqx_rule_engine:get_rules_with_same_event(<<"$events/client_connected">>))
    ),
    ?assertEqual(
        1, length(emqx_rule_engine:get_rules_with_same_event(<<"$events/client_disconnected">>))
    ),
    ?assertEqual(
        2, length(emqx_rule_engine:get_rules_with_same_event(<<"$events/session_subscribed">>))
    ),
    ?assertEqual(
        1, length(emqx_rule_engine:get_rules_with_same_event(<<"$events/session_unsubscribed">>))
    ),
    ?assertEqual(
        1, length(emqx_rule_engine:get_rules_with_same_event(<<"$events/message_delivered">>))
    ),
    ?assertEqual(
        1, length(emqx_rule_engine:get_rules_with_same_event(<<"$events/message_acked">>))
    ),
    ?assertEqual(
        1, length(emqx_rule_engine:get_rules_with_same_event(<<"$events/message_dropped">>))
    ),
    ok = delete_rules_by_ids([
        <<"r1">>,
        <<"r2">>,
        <<"r3">>,
        <<"r4">>,
        <<"r5">>,
        <<"r6">>,
        <<"r7">>,
        <<"r8">>,
        <<"r9">>,
        <<"r10">>
    ]),
    ok.

t_get_rule_ids_by_action(_) ->
    ID = <<"t_get_rule_ids_by_action">>,
    Rule1 = #{
        id => ID,
        sql => <<"SELECT * FROM \"t\"">>,
        actions => [
            #{function => console, args => #{}},
            #{function => republish, args => #{}},
            <<"mqtt:my_mqtt_bridge">>,
            <<"mysql:foo">>
        ],
        description => ID,
        created_at => erlang:system_time(millisecond)
    },
    ok = create_rules([Rule1]),
    ?assertMatch(
        [ID],
        emqx_rule_engine:get_rule_ids_by_action(#{function => <<"emqx_rule_actions:console">>})
    ),
    ?assertMatch(
        [ID],
        emqx_rule_engine:get_rule_ids_by_action(#{function => <<"emqx_rule_actions:republish">>})
    ),
    ?assertEqual([], emqx_rule_engine:get_rule_ids_by_action(#{function => <<"some_mod:fun">>})),
    ?assertMatch([ID], emqx_rule_engine:get_rule_ids_by_action(<<"mysql:foo">>)),
    ?assertEqual([], emqx_rule_engine:get_rule_ids_by_action(<<"mysql:not_exists">>)),
    ok = delete_rules_by_ids([<<"t_get_rule_ids_by_action">>]).

%% Check that command line interface don't crash when listing and showing rules
t_command_line_list_print_rule(_) ->
    ID = <<"t_command_line">>,
    Rule1 = #{
        id => ID,
        sql => <<"SELECT * FROM \"t\"">>,
        actions => [
            #{function => console, args => #{}},
            #{function => republish, args => #{}},
            <<"mqtt:my_mqtt_bridge">>,
            <<"mysql:foo">>
        ],
        description => ID,
        created_at => erlang:system_time(millisecond)
    },
    ok = create_rules([Rule1]),
    ok = emqx_rule_engine_cli:cmd(["list"]),
    ok = emqx_rule_engine_cli:cmd(["show", binary_to_list(ID)]),
    ok = delete_rules_by_ids([ID]).

t_ensure_action_removed(_) ->
    Id = <<"t_ensure_action_removed">>,
    GetSelectedData = <<"emqx_rule_sqltester:get_selected_data">>,
    {ok, _} = emqx:update_config(
        [rule_engine, rules, Id],
        #{
            <<"actions">> => [
                #{<<"function">> => GetSelectedData},
                #{<<"function">> => <<"console">>},
                #{
                    <<"function">> => <<"republish">>,
                    <<"args">> => #{<<"topic">> => <<"some/topic">>}
                },
                <<"mysql:foo">>,
                <<"mqtt:bar">>
            ],
            <<"description">> => <<"">>,
            <<"sql">> => <<"SELECT * FROM \"t/#\"">>
        }
    ),
    ?assertMatch(
        #{
            <<"actions">> := [
                #{<<"function">> := GetSelectedData},
                #{<<"function">> := <<"console">>},
                #{<<"function">> := <<"republish">>},
                <<"mysql:foo">>,
                <<"mqtt:bar">>
            ]
        },
        emqx:get_raw_config([rule_engine, rules, Id])
    ),
    ok = emqx_rule_engine:ensure_action_removed(Id, #{function => <<"console">>}),
    ?assertMatch(
        #{
            <<"actions">> := [
                #{<<"function">> := GetSelectedData},
                #{<<"function">> := <<"republish">>},
                <<"mysql:foo">>,
                <<"mqtt:bar">>
            ]
        },
        emqx:get_raw_config([rule_engine, rules, Id])
    ),
    ok = emqx_rule_engine:ensure_action_removed(Id, <<"mysql:foo">>),
    ?assertMatch(
        #{
            <<"actions">> := [
                #{<<"function">> := GetSelectedData},
                #{<<"function">> := <<"republish">>},
                <<"mqtt:bar">>
            ]
        },
        emqx:get_raw_config([rule_engine, rules, Id])
    ),
    ok = emqx_rule_engine:ensure_action_removed(Id, #{function => GetSelectedData}),
    ?assertMatch(
        #{
            <<"actions">> := [
                #{<<"function">> := <<"republish">>},
                <<"mqtt:bar">>
            ]
        },
        emqx:get_raw_config([rule_engine, rules, Id])
    ),
    emqx:remove_config([rule_engine, rules, Id]).

%%------------------------------------------------------------------------------
%% Test cases for rule runtime
%%------------------------------------------------------------------------------

t_json_payload_decoding(_Config) ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),

    Cases =
        [
            #{
                select_fields =>
                    <<"payload.measurement, payload.data_type, payload.value, payload.device_id">>,
                payload => emqx_utils_json:encode(#{
                    measurement => <<"temp">>,
                    data_type => <<"FLOAT">>,
                    value => <<"32.12">>,
                    device_id => <<"devid">>
                }),
                expected => #{
                    payload => #{
                        <<"measurement">> => <<"temp">>,
                        <<"data_type">> => <<"FLOAT">>,
                        <<"value">> => <<"32.12">>,
                        <<"device_id">> => <<"devid">>
                    }
                }
            },
            %% "last write wins" examples
            #{
                select_fields => <<"payload as p, payload.f as p.answer">>,
                payload => emqx_utils_json:encode(#{f => 42, keep => <<"that?">>}),
                expected => #{
                    <<"p">> => #{
                        <<"answer">> => 42
                    }
                }
            },
            #{
                select_fields => <<"payload as p, payload.f as p.jsonlike.f">>,
                payload => emqx_utils_json:encode(#{
                    jsonlike => emqx_utils_json:encode(#{a => 0}),
                    f => <<"huh">>
                }),
                %% behavior from 4.4: jsonlike gets wiped without preserving old "keys"
                %% here we overwrite it since we don't explicitly decode it
                expected => #{
                    <<"p">> => #{
                        <<"jsonlike">> => #{<<"f">> => <<"huh">>}
                    }
                }
            },
            #{
                select_fields =>
                    <<"payload as p, 42 as p, payload.measurement as p.measurement, 51 as p">>,
                payload => emqx_utils_json:encode(#{
                    measurement => <<"temp">>,
                    data_type => <<"FLOAT">>,
                    value => <<"32.12">>,
                    device_id => <<"devid">>
                }),
                expected => #{
                    <<"p">> => 51
                }
            },
            %% if selected field is already structured, new values are inserted into it
            #{
                select_fields =>
                    <<"json_decode(payload) as p, payload.a as p.z">>,
                payload => emqx_utils_json:encode(#{
                    a => 1,
                    b => <<"2">>
                }),
                expected => #{
                    <<"p">> => #{
                        <<"a">> => 1,
                        <<"b">> => <<"2">>,
                        <<"z">> => 1
                    }
                }
            }
        ],
    ActionFn = <<(atom_to_binary(?MODULE))/binary, ":action_response">>,
    Topic = <<"some/topic">>,

    ok = snabbkaffe:start_trace(),
    on_exit(fun() -> snabbkaffe:stop() end),
    on_exit(fun() -> delete_rule(?TMP_RULEID) end),
    lists:foreach(
        fun(#{select_fields := Fs, payload := P, expected := E} = Case) ->
            ct:pal("testing case ~p", [Case]),
            SQL = <<"select ", Fs/binary, " from \"", Topic/binary, "\"">>,
            delete_rule(?TMP_RULEID),
            {ok, _Rule} = emqx_rule_engine:create_rule(
                #{
                    sql => SQL,
                    id => ?TMP_RULEID,
                    actions => [#{function => ActionFn}]
                }
            ),
            {_, {ok, Event}} =
                ?wait_async_action(
                    emqtt:publish(C, Topic, P, 0),
                    #{?snk_kind := action_response},
                    5_000
                ),
            ?assertMatch(
                #{selected := E},
                Event,
                #{payload => P, fields => Fs, expected => E}
            ),
            ok
        end,
        Cases
    ),
    snabbkaffe:stop(),

    ok.

t_events(_Config) ->
    {ok, Client} = emqtt:start_link(
        [
            {username, <<"u_event">>},
            {clientid, <<"c_event">>},
            {proto_ver, v5},
            {properties, #{'Session-Expiry-Interval' => 60}}
        ]
    ),
    {ok, Client2} = emqtt:start_link(
        [
            {username, <<"u_event2">>},
            {clientid, <<"c_event2">>},
            {proto_ver, v5},
            {properties, #{'Session-Expiry-Interval' => 60}}
        ]
    ),
    ct:pal("====== verify $events/client_connected, $events/client_connack"),
    client_connected(Client, Client2),
    ct:pal("====== verify $events/message_dropped"),
    message_dropped(Client),
    ct:pal("====== verify $events/session_subscribed"),
    session_subscribed(Client2),
    ct:pal("====== verify t1"),
    message_publish(Client),
    is_ee() andalso
        begin
            ct:pal("====== verify $events/schema_validation_failed"),
            schema_validation_failed(Client),
            ct:pal("====== verify $events/message_transformation_failed"),
            message_transformation_failed(Client)
        end,
    ct:pal("====== verify $events/delivery_dropped"),
    delivery_dropped(Client),
    ct:pal("====== verify $events/message_delivered"),
    message_delivered(Client),
    ct:pal("====== verify $events/message_acked"),
    message_acked(Client),
    ct:pal("====== verify $events/session_unsubscribed"),
    session_unsubscribed(Client2),
    ct:pal("====== verify $events/client_disconnected"),
    client_disconnected(Client, Client2),
    ct:pal("====== verify $events/client_connack"),
    client_connack_failed(),
    ok.

t_event_client_disconnected_normal(_Config) ->
    SQL =
        "select * "
        "from \"$events/client_disconnected\" ",
    RepubT = <<"repub/to/disconnected/normal">>,

    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [republish_action(RepubT, <<>>)]
        }
    ),

    {ok, Client} = emqtt:start_link([{clientid, <<"get_repub_client">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, RepubT, 0),
    ct:sleep(200),
    {ok, Client1} = emqtt:start_link([{clientid, <<"emqx">>}, {username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client1),
    emqtt:disconnect(Client1),

    receive
        {publish, #{topic := T, payload := Payload}} ->
            ?assertEqual(RepubT, T),
            ?assertMatch(
                #{<<"reason">> := <<"normal">>}, emqx_utils_json:decode(Payload, [return_maps])
            )
    after 1000 ->
        ct:fail(wait_for_repub_disconnected_normal)
    end,
    emqtt:stop(Client),

    delete_rule(TopicRule).

t_event_client_disconnected_kicked(_Config) ->
    SQL =
        "select * "
        "from \"$events/client_disconnected\" ",
    RepubT = <<"repub/to/disconnected/kicked">>,

    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [republish_action(RepubT, <<>>)]
        }
    ),

    {ok, Client} = emqtt:start_link([{clientid, <<"get_repub_client">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, RepubT, 0),
    ct:sleep(200),

    {ok, Client1} = emqtt:start_link([{clientid, <<"emqx">>}, {username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client1),
    %% the process will receive {'EXIT',{shutdown,tcp_closed}}
    unlink(Client1),

    emqx_cm:kick_session(<<"emqx">>),

    receive
        {publish, #{topic := T, payload := Payload}} ->
            ?assertEqual(RepubT, T),
            ?assertMatch(
                #{<<"reason">> := <<"kicked">>}, emqx_utils_json:decode(Payload, [return_maps])
            )
    after 1000 ->
        ct:fail(wait_for_repub_disconnected_kicked)
    end,

    emqtt:stop(Client),
    delete_rule(TopicRule).

t_event_client_disconnected_discarded(_Config) ->
    SQL =
        "select * "
        "from \"$events/client_disconnected\" ",
    RepubT = <<"repub/to/disconnected/discarded">>,

    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [republish_action(RepubT, <<>>)]
        }
    ),

    {ok, Client} = emqtt:start_link([{clientid, <<"get_repub_client">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, RepubT, 0),
    ct:sleep(200),

    {ok, Client1} = emqtt:start_link([{clientid, <<"emqx">>}, {username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client1),
    %% the process will receive {'EXIT',{shutdown,tcp_closed}}
    unlink(Client1),

    {ok, Client2} = emqtt:start_link([
        {clientid, <<"emqx">>}, {username, <<"emqx">>}, {clean_start, true}
    ]),
    {ok, _} = emqtt:connect(Client2),

    receive
        {publish, #{topic := T, payload := Payload}} ->
            ?assertEqual(RepubT, T),
            ?assertMatch(
                #{<<"reason">> := <<"discarded">>}, emqx_utils_json:decode(Payload, [return_maps])
            )
    after 1000 ->
        ct:fail(wait_for_repub_disconnected_discarded)
    end,
    emqtt:stop(Client),
    emqtt:stop(Client2),

    delete_rule(TopicRule).

t_event_client_disconnected_takenover(_Config) ->
    SQL =
        "select * "
        "from \"$events/client_disconnected\" ",
    RepubT = <<"repub/to/disconnected/takenover">>,

    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [republish_action(RepubT, <<>>)]
        }
    ),

    {ok, ClientRecv} = emqtt:start_link([
        {clientid, <<"get_repub_client">>}, {username, <<"emqx0">>}
    ]),
    {ok, _} = emqtt:connect(ClientRecv),
    {ok, _, _} = emqtt:subscribe(ClientRecv, RepubT, 0),
    ct:sleep(200),

    {ok, Client1} = emqtt:start_link([{clientid, <<"emqx">>}, {username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client1),
    %% the process will receive {'EXIT',{shutdown,tcp_closed}}
    unlink(Client1),

    {ok, Client2} = emqtt:start_link([
        {clientid, <<"emqx">>}, {username, <<"emqx">>}, {clean_start, false}
    ]),
    {ok, _} = emqtt:connect(Client2),

    receive
        {publish, #{topic := T, payload := Payload}} ->
            ?assertEqual(RepubT, T),
            ?assertMatch(
                #{<<"reason">> := <<"takenover">>}, emqx_utils_json:decode(Payload, [return_maps])
            )
    after 1000 ->
        ct:fail(wait_for_repub_disconnected_discarded)
    end,

    emqtt:stop(ClientRecv),
    emqtt:stop(Client2),

    delete_rule(TopicRule).

t_event_client_disconnected_takenover_2(_Config) ->
    SQL =
        "select * "
        "from \"$events/client_disconnected\" ",
    RepubT = <<"repub/to/disconnected/takenover">>,

    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [republish_action(RepubT, <<>>)]
        }
    ),

    {ok, ClientRecv} = emqtt:start_link([
        {clientid, <<"get_repub_client">>}, {username, <<"emqx0">>}
    ]),
    {ok, _} = emqtt:connect(ClientRecv),
    {ok, _, _} = emqtt:subscribe(ClientRecv, RepubT, 0),
    ct:sleep(200),

    {ok, Client1} = emqtt:start_link([
        {clientid, <<"emqx">>}, {username, <<"emqx">>}, {clean_start, false}
    ]),
    {ok, _} = emqtt:connect(Client1),
    ok = emqtt:disconnect(Client1),

    %% receive the normal disconnected event
    receive
        {publish, #{topic := T, payload := Payload}} ->
            ?assertEqual(RepubT, T),
            ?assertMatch(
                #{<<"reason">> := <<"normal">>}, emqx_utils_json:decode(Payload, [return_maps])
            )
    after 1000 ->
        ct:fail(wait_for_repub_disconnected_discarded)
    end,

    {ok, Client2} = emqtt:start_link([
        {clientid, <<"emqx">>}, {username, <<"emqx">>}, {clean_start, false}
    ]),
    {ok, _} = emqtt:connect(Client2),

    %% should not receive the takenoverdisconnected event
    receive
        {publish, #{topic := T1, payload := Payload1}} ->
            ?assertEqual(RepubT, T1),
            ?assertMatch(
                #{<<"reason">> := <<"takenover">>}, emqx_utils_json:decode(Payload1, [return_maps])
            ),
            ct:fail(wait_for_repub_disconnected_discarded)
    after 1000 ->
        ok
    end,

    emqtt:stop(ClientRecv),
    emqtt:stop(Client2),

    delete_rule(TopicRule).

client_connack_failed() ->
    {ok, Client} = emqtt:start_link(
        [
            {username, <<"u_event3">>},
            {clientid, <<"c_event3">>},
            {proto_ver, v5},
            {properties, #{'Session-Expiry-Interval' => 60}}
        ]
    ),
    try
        meck:new(emqx_access_control, [non_strict, passthrough]),
        meck:expect(
            emqx_access_control,
            authenticate,
            fun(_) -> {error, bad_username_or_password} end
        ),
        process_flag(trap_exit, true),
        ?assertMatch({error, _}, emqtt:connect(Client)),
        timer:sleep(300),
        verify_event('client.connack')
    after
        meck:unload(emqx_access_control)
    end,
    ok.

message_publish(Client) ->
    emqtt:publish(
        Client,
        <<"t1">>,
        #{'Message-Expiry-Interval' => 60},
        <<"{\"id\": 1, \"name\": \"ha\"}">>,
        [{qos, 1}]
    ),
    verify_event('message.publish'),
    ok.
client_connected(Client, Client2) ->
    {ok, _} = emqtt:connect(Client),
    {ok, _} = emqtt:connect(Client2),
    verify_event('client.connack'),
    verify_event('client.connected'),
    verify_event('client.check_authn_complete'),
    ok.
client_disconnected(Client, Client2) ->
    ok = emqtt:disconnect(Client, 0, #{'User-Property' => {<<"reason">>, <<"normal">>}}),
    ok = emqtt:disconnect(Client2, 0, #{'User-Property' => {<<"reason">>, <<"normal">>}}),
    verify_event('client.disconnected'),
    ok.
session_subscribed(Client2) ->
    {ok, _, _} = emqtt:subscribe(
        Client2, #{'User-Property' => {<<"topic_name">>, <<"t1">>}}, <<"t1">>, 1
    ),
    verify_event('session.subscribed'),
    verify_event('client.check_authz_complete'),
    ok.
session_unsubscribed(Client2) ->
    {ok, _, _} = emqtt:unsubscribe(
        Client2, #{'User-Property' => {<<"topic_name">>, <<"t1">>}}, <<"t1">>
    ),
    verify_event('session.unsubscribed'),
    ok.

message_delivered(_Client) ->
    verify_event('message.delivered'),
    ok.
delivery_dropped(Client) ->
    %% subscribe "t1" and then publish to "t1", the message will not be received by itself
    %% because we have set the subscribe flag 'nl' = true
    {ok, _, _} = emqtt:subscribe(Client, #{}, <<"t1">>, [{nl, true}, {qos, 1}]),
    ct:sleep(50),
    message_publish(Client),
    verify_event('delivery.dropped'),
    ok.
message_dropped(Client) ->
    message_publish(Client),
    verify_event('message.dropped'),
    ok.
message_acked(_Client) ->
    verify_event('message.acked'),
    ok.
schema_validation_failed(Client) ->
    {ok, _} = emqtt:publish(Client, <<"sv/fail">>, <<"">>, [{qos, 1}]),
    ct:sleep(100),
    verify_event('schema.validation_failed'),
    ok.
message_transformation_failed(Client) ->
    {ok, _} = emqtt:publish(Client, <<"mt/fail">>, <<"will fail to { parse">>, [{qos, 1}]),
    ct:sleep(100),
    verify_event('message.transformation_failed'),
    ok.

t_match_atom_and_binary(_Config) ->
    SQL =
        "SELECT connected_at as ts, * "
        "FROM \"$events/client_connected\" "
        "WHERE username = 'emqx2' ",
    Repub = republish_action(<<"t2">>, <<"user:${ts}">>),
    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    {ok, Client} = emqtt:start_link([{username, <<"emqx1">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    ct:sleep(100),
    {ok, Client2} = emqtt:start_link([{username, <<"emqx2">>}]),
    {ok, _} = emqtt:connect(Client2),
    receive
        {publish, #{topic := T, payload := Payload}} ->
            ?assertEqual(<<"t2">>, T),
            <<"user:", ConnAt/binary>> = Payload,
            _ = binary_to_integer(ConnAt)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:stop(Client),
    delete_rule(TopicRule).

t_sqlselect_0(_Config) ->
    %% Verify SELECT with and without 'AS'
    Sql =
        "select * "
        "from \"t/#\" "
        "where payload.cmd.info = 'tt'",
    ?assertMatch(
        {ok, #{payload := <<"{\"cmd\": {\"info\":\"tt\"}}">>}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload =>
                            <<"{\"cmd\": {\"info\":\"tt\"}}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql2 =
        "select payload.cmd as cmd "
        "from \"t/#\" "
        "where cmd.info = 'tt'",
    ?assertMatch(
        {ok, #{<<"cmd">> := #{<<"info">> := <<"tt">>}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context =>
                    #{
                        payload =>
                            <<"{\"cmd\": {\"info\":\"tt\"}}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql3 =
        "select payload.cmd as cmd, cmd.info as info "
        "from \"t/#\" "
        "where cmd.info = 'tt' and info = 'tt'",
    ?assertMatch(
        {ok, #{
            <<"cmd">> := #{<<"info">> := <<"tt">>},
            <<"info">> := <<"tt">>
        }},
        emqx_rule_sqltester:test(
            #{
                sql => Sql3,
                context =>
                    #{
                        payload =>
                            <<"{\"cmd\": {\"info\":\"tt\"}}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    %% cascaded as
    Sql4 =
        "select payload.cmd as cmd, cmd.info as meta.info "
        "from \"t/#\" "
        "where cmd.info = 'tt' and meta.info = 'tt'",
    ?assertMatch(
        {ok, #{
            <<"cmd">> := #{<<"info">> := <<"tt">>},
            <<"meta">> := #{<<"info">> := <<"tt">>}
        }},
        emqx_rule_sqltester:test(
            #{
                sql => Sql4,
                context =>
                    #{
                        payload =>
                            <<"{\"cmd\": {\"info\":\"tt\"}}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ).

t_sqlselect_00(_Config) ->
    %% Verify plus/subtract and unary_add_or_subtract
    Sql =
        "select 1-1 as a "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"a">> := 0}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload => <<"">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql1 =
        "select -1 + 1 as a "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"a">> := 0}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql1,
                context =>
                    #{
                        payload => <<"">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql2 =
        "select 1 + 1 as a "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"a">> := 2}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context =>
                    #{
                        payload => <<"">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql3 =
        "select +1 as a "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"a">> := 1}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql3,
                context =>
                    #{
                        payload => <<"">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ).

t_sqlselect_with_3rd_party_impl(_Config) ->
    Sql =
        "select * from \"t/#\" where emqx_rule_funcs_demo.is_my_topic(topic)",
    T = fun(Topic) ->
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload => #{<<"what">> => 0},
                        topic => Topic
                    }
            }
        )
    end,
    ?assertMatch({ok, _}, T(<<"t/2/3/4/5">>)),
    ?assertMatch({error, nomatch}, T(<<"t/1">>)).

t_sqlselect_with_3rd_party_impl2(_Config) ->
    Sql = fun(N) ->
        "select emqx_rule_funcs_demo.duplicate_payload(payload," ++ integer_to_list(N) ++
            ") as payload_list from \"t/#\""
    end,
    T = fun(Payload, N) ->
        emqx_rule_sqltester:test(
            #{
                sql => Sql(N),
                context =>
                    #{
                        payload => Payload,
                        topic => <<"t/a">>
                    }
            }
        )
    end,
    ?assertMatch({ok, #{<<"payload_list">> := [_, _]}}, T(<<"payload1">>, 2)),
    ?assertMatch({ok, #{<<"payload_list">> := [_, _, _]}}, T(<<"payload1">>, 3)),
    %% crash
    ?assertMatch({error, {select_and_transform_error, _}}, T(<<"payload1">>, 4)).

t_sqlselect_with_3rd_party_funcs_unknown(_Config) ->
    Sql = "select emqx_rule_funcs_demo_no_such_module.foo(payload) from \"t/#\"",
    ?assertMatch(
        {error,
            {select_and_transform_error,
                {throw, #{reason := sql_function_provider_module_not_loaded}, _}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{payload => <<"a">>, topic => <<"t/a">>}
            }
        )
    ).

t_sqlselect_001(_Config) ->
    %% Verify that the jq function can be called from SQL
    Sql =
        "select jq('.what + .what', payload) as ans "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"ans">> := [8]}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload => #{<<"what">> => 4},
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql2 =
        "SELECT jq('.a|.[]', "
        "'{\"a\": [{\"b\": 1}, {\"b\": 2}, {\"b\": 3}]}') "
        "as jq_action, "
        "   jq_action[1].b as first_b from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"first_b">> := 1}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context =>
                    #{
                        payload => #{<<"what">> => 4},
                        topic => <<"t/a">>
                    }
            }
        )
    ).

t_sqlselect_002(_Config) ->
    %% Verify that the div and mod can be used both as infix operations and as
    %% function calls
    Sql =
        ""
        "select 2 mod 2   as mod1,\n"
        "                  mod(3, 2) as mod2,\n"
        "                  4 div 2   as div1,\n"
        "                  div(7, 2) as div2\n"
        "           from \"t/#\" "
        "",
    ?assertMatch(
        {ok, #{
            <<"mod1">> := 0,
            <<"mod2">> := 1,
            <<"div1">> := 2,
            <<"div2">> := 3
        }},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload => #{<<"what">> => 4},
                        topic => <<"t/a">>
                    }
            }
        )
    ).

t_sqlselect_inject_props(_Config) ->
    SQL =
        "SELECT json_decode(payload) as p, payload, "
        "map_put('inject_key', 'inject_val', user_properties) as user_properties "
        "FROM \"t3/#\", \"t1\" "
        "WHERE p.x = 1",
    Repub = republish_action(<<"t2">>),
    {ok, TopicRule1} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, #{}, <<"{\"x\":1}">>, [{qos, 0}]),
    receive
        {publish, #{topic := T, payload := Payload, properties := Props}} ->
            ?assertEqual(user_properties(#{<<"inject_key">> => <<"inject_val">>}), Props),
            ?assertEqual(<<"t2">>, T),
            ?assertEqual(<<"{\"x\":1}">>, Payload)
    after 2000 ->
        ct:fail(wait_for_t2)
    end,
    emqtt:stop(Client),
    delete_rule(TopicRule1).

t_sqlselect_01(_Config) ->
    SQL =
        "SELECT json_decode(payload) as p, payload "
        "FROM \"t3/#\", \"t1\" "
        "WHERE p.x = 1",
    Repub = republish_action(<<"t2">>, <<"${payload}">>, <<"${pub_props.'User-Property'}">>),
    {ok, TopicRule1} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    Props = user_properties(#{<<"mykey">> => <<"myval">>}),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, Props, <<"{\"x\":1}">>, [{qos, 0}]),
    receive
        {publish, #{topic := T, payload := Payload}} ->
            ?assertEqual(<<"t2">>, T),
            ?assertEqual(<<"{\"x\":1}">>, Payload)
    after 2000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:publish(Client, <<"t1">>, Props, <<"{\"x\":2}">>, [{qos, 0}]),
    receive
        {publish, #{topic := <<"t2">>, payload := _}} ->
            ct:fail(unexpected_t2)
    after 2000 ->
        ok
    end,

    emqtt:publish(Client, <<"t3/a">>, Props, <<"{\"x\":1}">>, [{qos, 0}]),
    receive
        {publish, #{topic := T3, payload := Payload3, properties := Props2}} ->
            ?assertEqual(Props, Props2),
            ?assertEqual(<<"t2">>, T3),
            ?assertEqual(<<"{\"x\":1}">>, Payload3)
    after 2000 ->
        ct:fail(wait_for_t3)
    end,

    emqtt:stop(Client),
    delete_rule(TopicRule1).

t_sqlselect_02(_Config) ->
    SQL =
        "SELECT * "
        "FROM \"t3/#\", \"t1\" "
        "WHERE payload.x = 1",
    Repub = republish_action(<<"t2">>),
    {ok, TopicRule1} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1}">>, 0),
    ct:sleep(100),
    receive
        {publish, #{topic := T, payload := Payload}} ->
            ?assertEqual(<<"t2">>, T),
            ?assertEqual(<<"{\"x\":1}">>, Payload)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:publish(Client, <<"t1">>, <<"{\"x\":2}">>, 0),
    receive
        {publish, #{topic := <<"t2">>, payload := Payload0}} ->
            ct:fail({unexpected_t2, Payload0})
    after 1000 ->
        ok
    end,

    emqtt:publish(Client, <<"t3/a">>, <<"{\"x\":1}">>, 0),
    receive
        {publish, #{topic := T3, payload := Payload3}} ->
            ?assertEqual(<<"t2">>, T3),
            ?assertEqual(<<"{\"x\":1}">>, Payload3)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:stop(Client),
    delete_rule(TopicRule1).

t_sqlselect_03(_Config) ->
    init_events_counters(),
    SQL = "SELECT * FROM \"t/r\" ",
    Repub = republish_action(
        <<"t/republish">>,
        <<"${.}">>,
        <<"${pub_props.'User-Property'}">>,
        #{
            <<"Payload-Format-Indicator">> => <<"${.payload.pfi}">>,
            <<"Message-Expiry-Interval">> => <<"${.payload.mei}">>,
            <<"Content-Type">> => <<"${.payload.ct}">>,
            <<"Response-Topic">> => <<"${.payload.rt}">>,
            <<"Correlation-Data">> => <<"${.payload.cd}">>
        }
    ),
    RepubRaw = emqx_utils_maps:binary_key_map(Repub#{function => <<"republish">>}),
    ct:pal("republish action raw:\n  ~p", [RepubRaw]),
    RuleRaw = #{
        <<"sql">> => SQL,
        <<"actions">> => [RepubRaw]
    },
    {ok, _} = emqx_conf:update([rule_engine, rules, ?TMP_RULEID], RuleRaw, #{}),
    on_exit(fun() -> emqx_rule_engine:delete_rule(?TMP_RULEID) end),
    %% to check what republish is actually producing without loss of information
    SQL1 = "select * from \"t/republish\" ",
    RuleId0 = ?TMP_RULEID,
    RuleId1 = <<RuleId0/binary, "2">>,
    {ok, _} = emqx_rule_engine:create_rule(
        #{
            sql => SQL1,
            id => RuleId1,
            actions => [
                #{
                    function => <<"emqx_rule_engine_SUITE:action_record_triggered_events">>,
                    args => #{}
                }
            ]
        }
    ),
    on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId1) end),

    UserProps = maps:to_list(#{<<"mykey">> => <<"myval">>}),
    Payload =
        emqx_utils_json:encode(
            #{
                pfi => 1,
                mei => 2,
                ct => <<"3">>,
                rt => <<"4">>,
                cd => <<"5">>
            }
        ),
    {ok, Client} = emqtt:start_link([
        {username, <<"emqx">>},
        {proto_ver, v5},
        {properties, #{'Topic-Alias-Maximum' => 100}}
    ]),
    on_exit(fun() -> emqtt:stop(Client) end),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/republish">>, 0),
    PubProps = #{'User-Property' => UserProps},
    ExpectedMQTTProps0 = #{
        'Payload-Format-Indicator' => 1,
        'Message-Expiry-Interval' => 2,
        'Content-Type' => <<"3">>,
        'Response-Topic' => <<"4">>,
        'Correlation-Data' => <<"5">>,
        %% currently, `Topic-Alias' is dropped `emqx_message:filter_pub_props',
        %% so the channel controls those aliases on its own, starting from 1.
        'Topic-Alias' => 1,
        'User-Property' => UserProps
    },
    emqtt:publish(Client, <<"t/r">>, PubProps, Payload, [{qos, 0}]),
    receive
        {publish, #{topic := <<"t/republish">>, properties := Props1}} ->
            ?assertEqual(ExpectedMQTTProps0, Props1),
            ok
    after 2000 ->
        ct:pal("mailbox:\n  ~p", [?drainMailbox()]),
        ct:fail("message not republished (l. ~b)", [?LINE])
    end,
    ExpectedMQTTProps1 = #{
        'Payload-Format-Indicator' => 1,
        'Message-Expiry-Interval' => 2,
        'Content-Type' => <<"3">>,
        'Response-Topic' => <<"4">>,
        'Correlation-Data' => <<"5">>,
        'User-Property' => maps:from_list(UserProps),
        'User-Property-Pairs' => [
            #{key => K, value => V}
         || {K, V} <- UserProps
        ]
    },
    ?assertMatch(
        [
            {'message.publish', #{
                topic := <<"t/republish">>,
                pub_props := ExpectedMQTTProps1
            }}
        ],
        ets:lookup(events_record_tab, 'message.publish'),
        #{expected_props => ExpectedMQTTProps1}
    ),

    ct:pal("testing payload that is not a json object"),
    emqtt:publish(Client, <<"t/r">>, PubProps, <<"not-a-map">>, [{qos, 0}]),
    ExpectedMQTTProps2 = #{
        'Content-Type' => <<"undefined">>,
        'Correlation-Data' => <<"undefined">>,
        'Response-Topic' => <<"undefined">>,
        %% currently, `Topic-Alias' is dropped `emqx_message:filter_pub_props',
        %% so the channel controls those aliases on its own, starting from 1.
        'Topic-Alias' => 1,
        'User-Property' => UserProps
    },
    receive
        {publish, #{topic := T1, properties := Props2}} ->
            ?assertEqual(ExpectedMQTTProps2, Props2),
            %% empty this time, due to topic alias set before
            ?assertEqual(<<>>, T1),
            ok
    after 2000 ->
        ct:pal("mailbox:\n  ~p", [?drainMailbox()]),
        ct:fail("message not republished (l. ~b)", [?LINE])
    end,

    ct:pal("testing payload with some uncoercible keys"),
    ets:delete_all_objects(events_record_tab),
    Payload1 =
        emqx_utils_json:encode(#{
            pfi => <<"bad_value1">>,
            mei => <<"bad_value2">>,
            ct => <<"some_value3">>,
            rt => <<"some_value4">>,
            cd => <<"some_value5">>
        }),
    emqtt:publish(Client, <<"t/r">>, PubProps, Payload1, [{qos, 0}]),
    ExpectedMQTTProps3 = #{
        %% currently, `Topic-Alias' is dropped `emqx_message:filter_pub_props',
        %% so the channel controls those aliases on its own, starting from 1.
        'Topic-Alias' => 1,
        'Content-Type' => <<"some_value3">>,
        'Response-Topic' => <<"some_value4">>,
        'Correlation-Data' => <<"some_value5">>,
        'User-Property' => UserProps
    },
    receive
        {publish, #{topic := T2, properties := Props3}} ->
            ?assertEqual(ExpectedMQTTProps3, Props3),
            %% empty this time, due to topic alias set before
            ?assertEqual(<<>>, T2),
            ok
    after 2000 ->
        ct:pal("mailbox:\n  ~p", [?drainMailbox()]),
        ct:fail("message not republished (l. ~b)", [?LINE])
    end,
    ExpectedMQTTProps4 = #{
        'Content-Type' => <<"some_value3">>,
        'Response-Topic' => <<"some_value4">>,
        'Correlation-Data' => <<"some_value5">>,
        'User-Property' => maps:from_list(UserProps),
        'User-Property-Pairs' => [
            #{key => K, value => V}
         || {K, V} <- UserProps
        ]
    },
    ?assertMatch(
        [
            {'message.publish', #{
                topic := <<"t/republish">>,
                pub_props := ExpectedMQTTProps4
            }}
        ],
        ets:lookup(events_record_tab, 'message.publish'),
        #{expected_props => ExpectedMQTTProps4}
    ),

    ct:pal("testing a payload with a more complex placeholder"),
    Repub1 = republish_action(
        <<"t/republish">>,
        <<"${.}">>,
        <<"${pub_props.'User-Property'}">>,
        #{
            %% Note: `Payload-Format-Indicator' is capped at 225.
            <<"Payload-Format-Indicator">> => <<"1${.payload.pfi}3">>,
            <<"Message-Expiry-Interval">> => <<"9${.payload.mei}6">>
        }
    ),
    RepubRaw1 = emqx_utils_maps:binary_key_map(Repub1#{function => <<"republish">>}),
    ct:pal("republish action raw:\n  ~p", [RepubRaw1]),
    RuleRaw1 = #{
        <<"sql">> => SQL,
        <<"actions">> => [RepubRaw1]
    },
    {ok, _} = emqx_conf:update([rule_engine, rules, ?TMP_RULEID], RuleRaw1, #{}),

    Payload2 =
        emqx_utils_json:encode(#{
            pfi => <<"2">>,
            mei => <<"87">>
        }),
    emqtt:publish(Client, <<"t/r">>, PubProps, Payload2, [{qos, 0}]),
    ExpectedMQTTProps5 = #{
        %% Note: PFI should be 0 or 1 according to spec, but we don't validate this when
        %% serializing nor parsing...
        'Payload-Format-Indicator' => 123,
        'Message-Expiry-Interval' => 9876,
        %% currently, `Topic-Alias' is dropped `emqx_message:filter_pub_props',
        %% so the channel controls those aliases on its own, starting from 1.
        'Topic-Alias' => 1,
        'User-Property' => UserProps
    },
    receive
        {publish, #{topic := T3, properties := Props4}} ->
            ?assertEqual(ExpectedMQTTProps5, Props4),
            %% empty this time, due to topic alias set before
            ?assertEqual(<<>>, T3),
            ok
    after 2000 ->
        ct:pal("mailbox:\n  ~p", [?drainMailbox()]),
        ct:fail("message not republished (l. ~b)", [?LINE])
    end,

    ct:pal("testing payload-format-indicator cap"),
    Payload3 =
        emqx_utils_json:encode(#{
            pfi => <<"999999">>,
            mei => <<"87">>
        }),
    emqtt:publish(Client, <<"t/r">>, PubProps, Payload3, [{qos, 0}]),
    ExpectedMQTTProps6 = #{
        %% Note: PFI should be 0 or 1 according to spec, but we don't validate this when
        %% serializing nor parsing...
        %% Note: PFI is capped at 16#FF
        'Payload-Format-Indicator' => 16#FF band 19999993,
        'Message-Expiry-Interval' => 9876,
        %% currently, `Topic-Alias' is dropped `emqx_message:filter_pub_props',
        %% so the channel controls those aliases on its own, starting from 1.
        'Topic-Alias' => 1,
        'User-Property' => UserProps
    },
    receive
        {publish, #{topic := T4, properties := Props5}} ->
            ?assertEqual(ExpectedMQTTProps6, Props5),
            %% empty this time, due to topic alias set before
            ?assertEqual(<<>>, T4),
            ok
    after 2000 ->
        ct:pal("mailbox:\n  ~p", [?drainMailbox()]),
        ct:fail("message not republished (l. ~b)", [?LINE])
    end,

    ok.

t_sqlselect_1(_Config) ->
    SQL =
        "SELECT json_decode(payload) as p, payload "
        "FROM \"t1\" "
        "WHERE p.x = 1 and p.y = 2",
    Repub = republish_action(<<"t2">>),
    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1,\"y\":2}">>, 0),
    receive
        {publish, #{topic := T, payload := Payload}} ->
            ?assertEqual(<<"t2">>, T),
            ?assertEqual(<<"{\"x\":1,\"y\":2}">>, Payload)
    after 2000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1,\"y\":1}">>, 0),
    receive
        {publish, #{topic := <<"t2">>, payload := _}} ->
            ct:fail(unexpected_t2)
    after 1000 ->
        ok
    end,

    emqtt:stop(Client),
    delete_rule(TopicRule).

t_sqlselect_2(_Config) ->
    %% recursively republish to t2
    SQL = "SELECT * FROM \"t2\" ",
    Repub = republish_action(<<"t2">>),
    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),

    emqtt:publish(Client, <<"t2">>, <<"{\"x\":1,\"y\":144}">>, 0),
    Fun = fun() ->
        receive
            {publish, #{topic := <<"t2">>, payload := _}} ->
                received_t2
        after 500 ->
            received_nothing
        end
    end,
    received_t2 = Fun(),
    received_t2 = Fun(),
    received_nothing = Fun(),

    emqtt:stop(Client),
    delete_rule(TopicRule).

t_sqlselect_3(_Config) ->
    %% republish the client.connected msg
    SQL =
        "SELECT * "
        "FROM \"$events/client_connected\" "
        "WHERE username = 'emqx1'",
    Repub = republish_action(<<"t2">>, <<"clientid=${clientid}">>),
    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    {ok, Client} = emqtt:start_link([{clientid, <<"emqx0">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    {ok, Client1} = emqtt:start_link([{clientid, <<"c_emqx1">>}, {username, <<"emqx1">>}]),
    {ok, _} = emqtt:connect(Client1),
    receive
        {publish, #{topic := T, payload := Payload}} ->
            ?assertEqual(<<"t2">>, T),
            ?assertEqual(<<"clientid=c_emqx1">>, Payload)
    after 2000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1,\"y\":1}">>, 0),
    receive
        {publish, #{topic := <<"t2">>, payload := _}} ->
            ct:fail(unexpected_t2)
    after 1000 ->
        ok
    end,

    emqtt:stop(Client),
    delete_rule(TopicRule).

%% select from t/1, republish to t/1, no dead-loop expected
%% i.e. payload is mutated once and only once
t_direct_dispatch(_Config) ->
    SQL = "SELECT * FROM \"t/1\"",
    Repub = republish_action(
        <<"t/1">>,
        <<"republished: ${payload}">>,
        <<"${user_properties}">>,
        #{},
        true
    ),
    {ok, Rule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    {ok, Pub} = emqtt:start_link([{clientid, <<"pubclient">>}]),
    {ok, _} = emqtt:connect(Pub),
    {ok, Sub} = emqtt:start_link([{clientid, <<"subclient">>}]),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, _} = emqtt:subscribe(Sub, <<"t/1">>, 0),
    Payload = base64:encode(crypto:strong_rand_bytes(12)),
    emqtt:publish(Pub, <<"t/1">>, Payload, 0),
    receive
        {publish, #{topic := T, payload := Payload1}} ->
            ?assertEqual(<<"t/1">>, T),
            ?assertEqual(<<"republished: ", Payload/binary>>, Payload1)
    after 2000 ->
        ct:fail(wait_for_t2)
    end,
    emqtt:stop(Pub),
    emqtt:stop(Sub),
    delete_rule(Rule).

t_sqlselect_message_publish_event_keep_original_props_1(_Config) ->
    %% republish the client.connected msg
    Topic = <<"foo/bar/1">>,
    SQL = <<
        "SELECT clientid "
        "FROM \"$events/message_dropped\" "
    >>,

    %"WHERE topic = \"", Topic/binary, "\"">>,
    Repub = republish_action(
        <<"t2">>,
        <<"clientid=${clientid}">>,
        <<"${pub_props.'User-Property'}">>
    ),
    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    {ok, Client1} = emqtt:start_link([{clientid, <<"sub-01">>}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client1),
    {ok, _, _} = emqtt:subscribe(Client1, <<"t2">>, 1),
    {ok, Client2} = emqtt:start_link([{clientid, <<"pub-02">>}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client2),
    Props = user_properties(#{<<"mykey">> => <<"111111">>}),
    emqtt:publish(Client2, Topic, Props, <<"{\"x\":1}">>, [{qos, 1}]),
    receive
        {publish, #{topic := T, payload := Payload, properties := Props1}} ->
            ?assertEqual(Props1, Props),
            ?assertEqual(<<"t2">>, T),
            ?assertEqual(<<"clientid=pub-02">>, Payload)
    after 2000 ->
        ct:fail(wait_for_t2)
    end,
    emqtt:stop(Client2),
    emqtt:stop(Client1),
    delete_rule(TopicRule).

t_sqlselect_message_publish_event_keep_original_props_2(_Config) ->
    %% republish the client.connected msg
    Topic = <<"foo/bar/1">>,
    SQL = <<
        "SELECT clientid, pub_props.'User-Property' as user_properties "
        "FROM \"$events/message_dropped\" "
    >>,

    %"WHERE topic = \"", Topic/binary, "\"">>,
    Repub = republish_action(<<"t2">>, <<"clientid=${clientid}">>),
    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    {ok, Client1} = emqtt:start_link([{clientid, <<"sub-01">>}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client1),
    {ok, _, _} = emqtt:subscribe(Client1, <<"t2">>, 1),
    {ok, Client2} = emqtt:start_link([{clientid, <<"pub-02">>}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client2),
    Props = user_properties(#{<<"mykey">> => <<"222222222222">>}),
    emqtt:publish(Client2, Topic, Props, <<"{\"x\":1}">>, [{qos, 1}]),
    receive
        {publish, #{topic := T, payload := Payload, properties := Props1}} ->
            ?assertEqual(Props1, Props),
            ?assertEqual(<<"t2">>, T),
            ?assertEqual(<<"clientid=pub-02">>, Payload)
    after 2000 ->
        ct:fail(wait_for_t2)
    end,
    emqtt:stop(Client2),
    emqtt:stop(Client1),
    delete_rule(TopicRule).

t_sqlselect_as_put(_Config) ->
    %% Verify SELECT with 'AS' to update the payload
    Sql =
        "select payload, "
        "'STEVE' as payload.data[1].name "
        "from \"t/#\" ",
    PayloadMap = #{
        <<"f1">> => <<"f1">>,
        <<"f2">> => <<"f2">>,
        <<"data">> => [
            #{<<"name">> => <<"n1">>, <<"idx">> => 1},
            #{<<"name">> => <<"n2">>, <<"idx">> => 2}
        ]
    },
    PayloadBin = emqx_utils_json:encode(PayloadMap),
    SqlResult = emqx_rule_sqltester:test(
        #{
            sql => Sql,
            context =>
                #{
                    payload => PayloadBin,
                    topic => <<"t/a">>
                }
        }
    ),
    ?assertMatch({ok, #{<<"payload">> := _}}, SqlResult),
    {ok, #{<<"payload">> := PayloadMap2}} = SqlResult,
    ?assertMatch(
        #{
            <<"f1">> := <<"f1">>,
            <<"f2">> := <<"f2">>,
            <<"data">> := [
                #{<<"name">> := <<"STEVE">>, <<"idx">> := 1},
                #{<<"name">> := <<"n2">>, <<"idx">> := 2}
            ]
        },
        PayloadMap2
    ).

t_sqlselect_missing_template_vars_render_as_undefined(_Config) ->
    SQL = <<"SELECT * FROM \"$events/client_connected\"">>,
    Repub = republish_action(<<"t2">>, <<"${clientid}:${missing.var}">>),
    {ok, TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),
    {ok, Client1} = emqtt:start_link([{clientid, <<"sub-01">>}]),
    {ok, _} = emqtt:connect(Client1),
    {ok, _, _} = emqtt:subscribe(Client1, <<"t2">>),
    {ok, Client2} = emqtt:start_link([{clientid, <<"pub-02">>}]),
    {ok, _} = emqtt:connect(Client2),
    emqtt:publish(Client2, <<"foo/bar/1">>, <<>>),
    receive
        {publish, Msg} ->
            ?assertMatch(#{topic := <<"t2">>, payload := <<"pub-02:undefined">>}, Msg)
    after 2000 ->
        ct:fail(wait_for_t2)
    end,
    emqtt:stop(Client2),
    emqtt:stop(Client1),
    delete_rule(TopicRule).

t_sqlparse_event_1(_Config) ->
    Sql =
        "select topic as tp "
        "from \"$events/session_subscribed\" ",
    ?assertMatch(
        {ok, #{<<"tp">> := <<"t/tt">>}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    topic => <<"t/tt">>,
                    event => 'session.subscribed'
                }
            }
        )
    ).

t_sqlparse_event_2(_Config) ->
    Sql =
        "select clientid "
        "from \"$events/client_connected\" ",
    ?assertMatch(
        {ok, #{<<"clientid">> := <<"abc">>}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    clientid => <<"abc">>,
                    event => 'client.connected'
                }
            }
        )
    ).

t_sqlparse_event_3(_Config) ->
    Sql =
        "select clientid, topic as tp "
        "from \"t/tt\", \"$events/client_connected\" ",
    ?assertMatch(
        {ok, #{<<"clientid">> := <<"abc">>, <<"tp">> := <<"t/tt">>}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{clientid => <<"abc">>, topic => <<"t/tt">>}
            }
        )
    ).

t_sqlparse_foreach_1(_Config) ->
    %% Verify foreach with and without 'AS'
    Sql =
        "foreach payload.sensors as s "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, [#{<<"s">> := 1}, #{<<"s">> := 2}]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"sensors\": [1, 2]}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    Sql2 =
        "foreach payload.sensors "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, [#{item := 1}, #{item := 2}]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context => #{
                    payload => <<"{\"sensors\": [1, 2]}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    Sql3 =
        "foreach payload.sensors "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, [
            #{item := #{<<"cmd">> := <<"1">>}, clientid := <<"c_a">>},
            #{item := #{<<"cmd">> := <<"2">>, <<"name">> := <<"ct">>}, clientid := <<"c_a">>}
        ]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql3,
                context => #{
                    payload =>
                        <<"{\"sensors\": [{\"cmd\":\"1\"}, {\"cmd\":\"2\",\"name\":\"ct\"}]}">>,
                    clientid => <<"c_a">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    Sql4 =
        "foreach payload.sensors "
        "from \"t/#\" ",
    {ok, [
        #{metadata := #{rule_id := TRuleId}},
        #{metadata := #{rule_id := TRuleId}}
    ]} =
        emqx_rule_sqltester:test(
            #{
                sql => Sql4,
                context => #{
                    payload => <<"{\"sensors\": [1, 2]}">>,
                    topic => <<"t/a">>
                }
            }
        ),

    Sql5 =
        "foreach payload.sensors "
        "from \"t/#\" ",
    {ok, [
        #{payload := #{<<"sensors">> := _}},
        #{payload := #{<<"sensors">> := _}}
    ]} =
        emqx_rule_sqltester:test(
            #{
                sql => Sql5,
                context => #{
                    payload => <<"{\"sensors\": [1, 2]}">>,
                    topic => <<"t/a">>
                }
            }
        ),

    try
        meck:new(emqx_rule_runtime, [non_strict, passthrough]),
        meck:expect(
            emqx_rule_runtime,
            apply_rule,
            fun(Rule, #{payload := Payload} = Columns, Env) ->
                Columns2 = maps:put(<<"payload">>, Payload, maps:without([payload], Columns)),
                meck:passthrough([Rule, Columns2, Env])
            end
        ),

        Sql6 =
            "foreach payload.sensors "
            "from \"t/#\" ",
        {ok, [
            #{<<"payload">> := #{<<"sensors">> := _}},
            #{<<"payload">> := #{<<"sensors">> := _}}
        ]} =
            emqx_rule_sqltester:test(
                #{
                    sql => Sql6,
                    context => #{
                        <<"payload">> => <<"{\"sensors\": [1, 2]}">>,
                        topic => <<"t/a">>
                    }
                }
            ),

        Sql7 =
            "foreach payload.sensors "
            "from \"t/#\" ",
        ?assertNotMatch(
            {ok, [
                #{<<"payload">> := _, payload := _},
                #{<<"payload">> := _, payload := _}
            ]},
            emqx_rule_sqltester:test(
                #{
                    sql => Sql7,
                    context => #{
                        <<"payload">> => <<"{\"sensors\": [1, 2]}">>,
                        topic => <<"t/a">>
                    }
                }
            )
        )
    after
        meck:unload(emqx_rule_runtime)
    end,
    ?assert(is_binary(TRuleId)).

t_sqlparse_foreach_2(_Config) ->
    %% Verify foreach-do with and without 'AS'
    Sql =
        "foreach payload.sensors as s "
        "do s.cmd as msg_type "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, [#{<<"msg_type">> := <<"1">>}, #{<<"msg_type">> := <<"2">>}]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload =>
                            <<"{\"sensors\": [{\"cmd\":\"1\"}, {\"cmd\":\"2\"}]}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql2 =
        "foreach payload.sensors "
        "do item.cmd as msg_type "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, [#{<<"msg_type">> := <<"1">>}, #{<<"msg_type">> := <<"2">>}]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context =>
                    #{
                        payload =>
                            <<"{\"sensors\": [{\"cmd\":\"1\"}, {\"cmd\":\"2\"}]}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql3 =
        "foreach payload.sensors "
        "do item as item "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, [#{<<"item">> := 1}, #{<<"item">> := 2}]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql3,
                context =>
                    #{
                        payload =>
                            <<"{\"sensors\": [1, 2]}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ).

t_sqlparse_foreach_3(_Config) ->
    %% Verify foreach-incase with and without 'AS'
    Sql =
        "foreach payload.sensors as s "
        "incase s.cmd != 1 "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, [
            #{<<"s">> := #{<<"cmd">> := 2}},
            #{<<"s">> := #{<<"cmd">> := 3}}
        ]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload =>
                            <<"{\"sensors\": [{\"cmd\":1}, {\"cmd\":2}, {\"cmd\":3}]}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql2 =
        "foreach payload.sensors "
        "incase item.cmd != 1 "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, [
            #{item := #{<<"cmd">> := 2}},
            #{item := #{<<"cmd">> := 3}}
        ]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context =>
                    #{
                        payload =>
                            <<"{\"sensors\": [{\"cmd\":1}, {\"cmd\":2}, {\"cmd\":3}]}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ).

t_sqlparse_foreach_4(_Config) ->
    %% Verify foreach-do-incase
    Sql =
        "foreach payload.sensors as s "
        "do s.cmd as msg_type, s.name as name "
        "incase is_not_null(s.cmd) "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, [#{<<"msg_type">> := <<"1">>}, #{<<"msg_type">> := <<"2">>}]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload =>
                            <<"{\"sensors\": [{\"cmd\":\"1\"}, {\"cmd\":\"2\"}]}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    ?assertMatch(
        {ok, [#{<<"msg_type">> := <<"1">>, <<"name">> := <<"n1">>}, #{<<"msg_type">> := <<"2">>}]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload =>
                            <<"{\"sensors\": [{\"cmd\":\"1\", \"name\":\"n1\"}, {\"cmd\":\"2\"}, {\"name\":\"n3\"}]}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    ?assertMatch(
        {ok, []},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload => <<"{\"sensors\": [1, 2]}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ).

t_sqlparse_foreach_5(_Config) ->
    %% Verify foreach on a empty-list or non-list variable
    Sql =
        "foreach payload.sensors as s "
        "do s.cmd as msg_type, s.name as name "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, []},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload => <<"{\"sensors\": 1}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    ?assertMatch(
        {ok, []},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload => <<"{\"sensors\": []}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql2 =
        "foreach payload.sensors "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, []},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context =>
                    #{
                        payload => <<"{\"sensors\": 1}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ).

t_sqlparse_foreach_6(_Config) ->
    %% Verify foreach on a empty-list or non-list variable
    Sql =
        "foreach json_decode(payload) "
        "do item.id as zid, timestamp as t "
        "from \"t/#\" ",
    {ok, Res} = emqx_rule_sqltester:test(
        #{
            sql => Sql,
            context =>
                #{
                    payload => <<"[{\"id\": 5},{\"id\": 15}]">>,
                    topic => <<"t/a">>
                }
        }
    ),
    [
        #{<<"t">> := Ts1, <<"zid">> := Zid1},
        #{<<"t">> := Ts2, <<"zid">> := Zid2}
    ] = Res,
    ?assertEqual(true, is_integer(Ts1)),
    ?assertEqual(true, is_integer(Ts2)),
    ?assert(Zid1 == 5 orelse Zid1 == 15),
    ?assert(Zid2 == 5 orelse Zid2 == 15).

t_sqlparse_foreach_7(_Config) ->
    %% Verify foreach-do-incase and cascaded AS
    Sql =
        "foreach json_decode(payload) as p, p.sensors as s, s.collection as c, c.info as info "
        "do info.cmd as msg_type, info.name as name "
        "incase is_not_null(info.cmd) "
        "from \"t/#\" "
        "where s.page = '2' ",
    Payload = <<
        "{\"sensors\": {\"page\": 2, \"collection\": "
        "{\"info\":[{\"name\":\"cmd1\", \"cmd\":\"1\"}, {\"cmd\":\"2\"}]} } }"
    >>,
    ?assertMatch(
        {ok, [#{<<"name">> := <<"cmd1">>, <<"msg_type">> := <<"1">>}, #{<<"msg_type">> := <<"2">>}]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload => Payload,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    Sql2 =
        "foreach json_decode(payload) as p, p.sensors as s, s.collection as c, c.info as info "
        "do info.cmd as msg_type, info.name as name "
        "incase is_not_null(info.cmd) "
        "from \"t/#\" "
        "where s.page = '3' ",
    ?assertMatch(
        {error, nomatch},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context =>
                    #{
                        payload => Payload,
                        topic => <<"t/a">>
                    }
            }
        )
    ).

-define(COLL, #{<<"info">> := [<<"haha">>, #{<<"name">> := <<"cmd1">>, <<"cmd">> := <<"1">>}]}).
t_sqlparse_foreach_8(_Config) ->
    %% Verify foreach-do-incase and cascaded AS
    Sql =
        "foreach json_decode(payload) as p, p.sensors as s, s.collection as c, c.info as info "
        "do info.cmd as msg_type, info.name as name, s, c "
        "incase is_map(info) "
        "from \"t/#\" "
        "where s.page = '2' ",
    Payload = <<
        "{\"sensors\": {\"page\": 2, \"collection\": "
        "{\"info\":[\"haha\", {\"name\":\"cmd1\", \"cmd\":\"1\"}]} } }"
    >>,
    ?assertMatch(
        {ok, [
            #{
                <<"name">> := <<"cmd1">>,
                <<"msg_type">> := <<"1">>,
                <<"s">> := #{<<"page">> := 2, <<"collection">> := ?COLL},
                <<"c">> := ?COLL
            }
        ]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context =>
                    #{
                        payload => Payload,
                        topic => <<"t/a">>
                    }
            }
        )
    ),

    Sql3 =
        "foreach json_decode(payload) as p, p.sensors as s, s.collection as c, sublist(2,1,c.info) as info "
        "do info.cmd as msg_type, info.name as name "
        "from \"t/#\" "
        "where s.page = '2' ",
    [
        ?assertMatch(
            {ok, [#{<<"name">> := <<"cmd1">>, <<"msg_type">> := <<"1">>}]},
            emqx_rule_sqltester:test(
                #{
                    sql => SqlN,
                    context =>
                        #{
                            payload => Payload,
                            topic => <<"t/a">>
                        }
                }
            )
        )
     || SqlN <- [Sql3]
    ].

t_sqlparse_foreach_9(_Config) ->
    Sql1 =
        "foreach json_decode(payload) as p "
        "do p.ts as ts "
        "from \"t/#\" ",
    Context = #{
        payload =>
            emqx_utils_json:encode(
                [
                    #{
                        <<"ts">> => 1451649600512,
                        <<"values">> =>
                            #{
                                <<"respiratoryrate">> => 20,
                                <<"heartrate">> => 130,
                                <<"systolic">> => 50
                            }
                    }
                ]
            ),
        topic => <<"t/a">>
    },
    ?assertMatch(
        {ok, [#{<<"ts">> := 1451649600512}]},
        emqx_rule_sqltester:test(
            #{
                sql => Sql1,
                context => Context
            }
        )
    ),
    %% doesn't work if we don't decode it first
    Sql2 =
        "foreach payload as p "
        "do p.ts as ts "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, []},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context => Context
            }
        )
    ),
    ok.

t_sqlparse_case_when_1(_Config) ->
    %% case-when-else clause
    Sql =
        "select "
        "  case when payload.x < 0 then 0 "
        "       when payload.x > 7 then 7 "
        "       else payload.x "
        "  end as y "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"y">> := 1}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 1}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{<<"y">> := 0}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 0}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{<<"y">> := 0}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": -1}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{<<"y">> := 7}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 7}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{<<"y">> := 7}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 8}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ok.

t_sqlparse_case_when_2(_Config) ->
    % switch clause
    Sql =
        "select "
        "  case payload.x when 1 then 2 "
        "                 when 2 then 3 "
        "                 else 4 "
        "  end as y "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"y">> := 2}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 1}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{<<"y">> := 3}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 2}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{<<"y">> := 4}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 4}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{<<"y">> := 4}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 7}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{<<"y">> := 4}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 8}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ).

t_sqlparse_case_when_3(_Config) ->
    %% case-when clause
    Sql =
        "select "
        "  case when payload.x < 0 then 0 "
        "       when payload.x > 7 then 7 "
        "  end as y "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 1}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 5}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 0}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{<<"y">> := 0}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": -1}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 7}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{<<"y">> := 7}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 8}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ok.

t_sqlparse_array_index_1(_Config) ->
    %% index get
    Sql =
        "select "
        "  json_decode(payload) as p, "
        "  p[1] as a "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"a">> := #{<<"x">> := 1}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"[{\"x\": 1}]">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    ?assertMatch(
        {ok, #{}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"{\"x\": 1}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    %% index get without 'as'
    Sql2 =
        "select "
        "  payload.x[2] "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{payload := #{<<"x">> := [3]}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context => #{
                    payload => #{<<"x">> => [1, 3, 4]},
                    topic => <<"t/a">>
                }
            }
        )
    ),
    %% index get without 'as' again
    Sql3 =
        "select "
        "  payload.x[2].y "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{payload := #{<<"x">> := [#{<<"y">> := 3}]}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql3,
                context => #{
                    payload => #{<<"x">> => [1, #{y => 3}, 4]},
                    topic => <<"t/a">>
                }
            }
        )
    ),

    %% index get with 'as'
    Sql4 =
        "select "
        "  payload.x[2].y as b "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"b">> := 3}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql4,
                context => #{
                    payload => #{<<"x">> => [1, #{y => 3}, 4]},
                    topic => <<"t/a">>
                }
            }
        )
    ).

t_sqlparse_array_index_2(_Config) ->
    %% array get with negative index
    Sql1 =
        "select "
        "  payload.x[-2].y as b "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"b">> := 3}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql1,
                context => #{
                    payload => #{<<"x">> => [1, #{y => 3}, 4]},
                    topic => <<"t/a">>
                }
            }
        )
    ),
    %% array append to head or tail of a list:
    Sql2 =
        "select "
        "  payload.x as b, "
        "  1 as c[-0], "
        "  2 as c[-0], "
        "  b as c[0] "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"b">> := 0, <<"c">> := [0, 1, 2]}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context => #{
                    payload => #{<<"x">> => 0},
                    topic => <<"t/a">>
                }
            }
        )
    ),
    %% construct an empty list:
    Sql3 =
        "select "
        "  [] as c, "
        "  1 as c[-0], "
        "  2 as c[-0], "
        "  0 as c[0] "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"c">> := [0, 1, 2]}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql3,
                context => #{
                    payload => <<"">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    %% construct a list:
    Sql4 =
        "select "
        "  [payload.a, \"topic\", 'c'] as c, "
        "  1 as c[-0], "
        "  2 as c[-0], "
        "  0 as c[0] "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"c">> := [0, 11, <<"t/a">>, <<"c">>, 1, 2]}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql4,
                context => #{
                    payload => <<"{\"a\":11}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ).

t_sqlparse_array_index_3(_Config) ->
    %% array with json string payload:
    Sql0 =
        "select "
        "payload,"
        "payload.x[2].y "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"payload">> := #{<<"x">> := [1, #{<<"y">> := [1, 2]}, 3]}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql0,
                context => #{
                    payload => <<"{\"x\": [1,{\"y\": [1,2]},3]}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    %% same as above but don't select payload:
    Sql1 =
        "select "
        "payload.x[2].y as b "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"b">> := [1, 2]}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql1,
                context => #{
                    payload => <<"{\"x\": [1,{\"y\": [1,2]},3]}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    %% same as above but add 'as' clause:
    Sql2 =
        "select "
        "payload.x[2].y as b.c "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"b">> := #{<<"c">> := [1, 2]}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context => #{
                    payload => <<"{\"x\": [1,{\"y\": [1,2]},3]}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ).

t_sqlparse_array_index_4(_Config) ->
    %% array with json string payload:
    Sql0 =
        "select "
        "0 as payload.x[2].y "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"payload">> := #{<<"x">> := [#{<<"y">> := 0}]}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql0,
                context => #{
                    payload => <<"{\"x\": [1,{\"y\": [1,2]},3]}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    %% array with json string payload, and also select payload.x:
    Sql1 =
        "select "
        "payload.x, "
        "0 as payload.x[2].y "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{payload := #{<<"x">> := [1, #{<<"y">> := 0}, 3]}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql1,
                context => #{
                    payload => <<"{\"x\": [1,{\"y\": [1,2]},3]}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ).

t_sqlparse_array_index_5(_Config) ->
    Sql00 =
        "select "
        "  [1,2,3,4] "
        "from \"t/#\" ",
    {ok, Res00} =
        emqx_rule_sqltester:test(
            #{
                sql => Sql00,
                context => #{
                    payload => <<"">>,
                    topic => <<"t/a">>
                }
            }
        ),
    ?assert(
        lists:any(
            fun({_K, V}) ->
                V =:= [1, 2, 3, 4]
            end,
            maps:to_list(Res00)
        )
    ).

t_sqlparse_array_with_expressions(_Config) ->
    Sql =
        "select "
        "  [21 + 21, abs(-abs(-2)), [1 + 1], 4] "
        "from \"t/#\" ",
    {ok, Res} =
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    payload => <<"">>,
                    topic => <<"t/a">>
                }
            }
        ),
    ?assert(
        lists:any(
            fun({_K, V}) ->
                V =:= [42, 2, [2], 4]
            end,
            maps:to_list(Res)
        )
    ).

t_sqlparse_select_matadata_1(_Config) ->
    %% array with json string payload:
    Sql0 =
        "select "
        "payload "
        "from \"t/#\" ",
    ?assertNotMatch(
        {ok, #{<<"payload">> := <<"abc">>, metadata := _}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql0,
                context => #{
                    payload => <<"abc">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    Sql1 =
        "select "
        "payload, metadata "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"payload">> := <<"abc">>, <<"metadata">> := _}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql1,
                context => #{
                    payload => <<"abc">>,
                    topic => <<"t/a">>
                }
            }
        )
    ).

t_sqlparse_array_range_1(_Config) ->
    %% get a range of list
    Sql0 =
        "select "
        "  payload.a[1..4] as c "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"c">> := [0, 1, 2, 3]}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql0,
                context => #{
                    payload => <<"{\"a\":[0,1,2,3,4,5]}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ),
    %% get a range from non-list data
    Sql02 =
        "select "
        "  payload.a[1..4] as c "
        "from \"t/#\" ",
    ?assertMatch(
        {error, {select_and_transform_error, {error, {range_get, non_list_data}, _}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql02,
                context =>
                    #{
                        payload => <<"{\"x\":[0,1,2,3,4,5]}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),
    %% construct a range:
    Sql1 =
        "select "
        "  [1..4] as c, "
        "  5 as c[-0], "
        "  6 as c[-0], "
        "  0 as c[0] "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"c">> := [0, 1, 2, 3, 4, 5, 6]}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql1,
                context => #{
                    payload => <<"">>,
                    topic => <<"t/a">>
                }
            }
        )
    ).

t_sqlparse_array_range_2(_Config) ->
    %% construct a range without 'as'
    Sql00 =
        "select "
        "  [1..4] "
        "from \"t/#\" ",
    {ok, Res00} =
        emqx_rule_sqltester:test(
            #{
                sql => Sql00,
                context => #{
                    payload => <<"">>,
                    topic => <<"t/a">>
                }
            }
        ),
    ?assert(
        lists:any(
            fun({_K, V}) ->
                V =:= [1, 2, 3, 4]
            end,
            maps:to_list(Res00)
        )
    ),
    %% construct a range without 'as'
    Sql01 =
        "select "
        "  a[2..4] "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"a">> := [2, 3, 4]}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql01,
                context => #{
                    <<"a">> => [1, 2, 3, 4, 5],
                    topic => <<"t/a">>
                }
            }
        )
    ),
    %% get a range of list without 'as'
    Sql02 =
        "select "
        "  payload.a[1..4] "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{payload := #{<<"a">> := [0, 1, 2, 3]}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql02,
                context => #{
                    payload => <<"{\"a\":[0,1,2,3,4,5]}">>,
                    topic => <<"t/a">>
                }
            }
        )
    ).

t_sqlparse_true_false(_Config) ->
    %% construct a range without 'as'
    Sql00 =
        "select "
        " true as a, false as b, "
        " false as x.y, true as c[-0] "
        "from \"t/#\" ",
    {ok, Res00} =
        emqx_rule_sqltester:test(
            #{
                sql => Sql00,
                context => #{
                    payload => <<"">>,
                    topic => <<"t/a">>
                }
            }
        ),
    ?assertMatch(
        #{
            <<"a">> := true,
            <<"b">> := false,
            <<"x">> := #{<<"y">> := false},
            <<"c">> := [true]
        },
        Res00
    ).

t_sqlparse_undefined_variable(_Config) ->
    %% undefined == undefined
    Sql00 =
        "select "
        "a, b "
        "from \"t/#\" "
        "where a = b",

    {ok, Res00} = emqx_rule_sqltester:test(
        #{sql => Sql00, context => #{payload => <<"">>, topic => <<"t/a">>}}
    ),
    ?assertEqual(#{<<"a">> => undefined, <<"b">> => undefined}, Res00),
    ?assertEqual(2, map_size(Res00)),
    %% undefined compare to non-undefined variables should return false
    Sql01 =
        "select "
        "a, b "
        "from \"t/#\" "
        "where a > b",

    {error, nomatch} = emqx_rule_sqltester:test(
        #{
            sql => Sql01,
            context => #{payload => <<"{\"b\":1}">>, topic => <<"t/a">>}
        }
    ),
    Sql02 =
        "select "
        "a < b as c "
        "from \"t/#\" ",

    {ok, Res02} = emqx_rule_sqltester:test(
        #{
            sql => Sql02,
            context => #{payload => <<"{\"b\":1}">>, topic => <<"t/a">>}
        }
    ),
    ?assertMatch(#{<<"c">> := false}, Res02).

t_sqlparse_new_map(_Config) ->
    %% construct a range without 'as'
    Sql00 =
        "select "
        " map_new() as a, map_new() as b, "
        " map_new() as x.y, map_new() as c[-0] "
        "from \"t/#\" ",
    {ok, Res00} =
        emqx_rule_sqltester:test(
            #{
                sql => Sql00,
                context => #{
                    payload => <<"">>,
                    topic => <<"t/a">>
                }
            }
        ),
    ?assertMatch(
        #{
            <<"a">> := #{},
            <<"b">> := #{},
            <<"x">> := #{<<"y">> := #{}},
            <<"c">> := [#{}]
        },
        Res00
    ).

t_sqlparse_payload_as(_Config) ->
    %% https://github.com/emqx/emqx/issues/3866
    Sql00 =
        "SELECT "
        " payload, map_get('engineWorkTime', payload.params, -1) as payload.params.engineWorkTime, "
        " map_get('hydOilTem', payload.params, -1) as payload.params.hydOilTem "
        "FROM \"t/#\" ",
    Payload1 =
        <<"{ \"msgId\": 1002, \"params\": { \"convertTemp\": 20, \"engineSpeed\": 42, \"hydOilTem\": 30 } }">>,
    {ok, Res01} = emqx_rule_sqltester:test(
        #{
            sql => Sql00,
            context => #{
                payload => Payload1,
                topic => <<"t/a">>
            }
        }
    ),
    ?assertMatch(
        #{
            <<"payload">> := #{
                <<"params">> := #{
                    <<"convertTemp">> := 20,
                    <<"engineSpeed">> := 42,
                    <<"engineWorkTime">> := -1,
                    <<"hydOilTem">> := 30
                }
            }
        },
        Res01
    ),

    Payload2 = <<"{ \"msgId\": 1002, \"params\": { \"convertTemp\": 20, \"engineSpeed\": 42 } }">>,
    {ok, Res02} = emqx_rule_sqltester:test(
        #{
            sql => Sql00,
            context => #{
                payload => Payload2,
                topic => <<"t/a">>
            }
        }
    ),
    ?assertMatch(
        #{
            <<"payload">> := #{
                <<"params">> := #{
                    <<"convertTemp">> := 20,
                    <<"engineSpeed">> := 42,
                    <<"engineWorkTime">> := -1,
                    <<"hydOilTem">> := -1
                }
            }
        },
        Res02
    ).

t_sqlparse_nested_get(_Config) ->
    Sql =
        "select payload as p, p.a.b as c "
        "from \"t/#\" ",
    ?assertMatch(
        {ok, #{<<"c">> := 0}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{
                    topic => <<"t/1">>,
                    payload => <<"{\"a\": {\"b\": 0}}">>
                }
            }
        )
    ).

t_sqlparse_invalid_json(_Config) ->
    Sql02 =
        "select "
        "  payload.a[1..4] as c "
        "from \"t/#\" ",
    ?assertMatch(
        {error, {select_and_transform_error, {error, {decode_json_failed, _}, _}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql02,
                context =>
                    #{
                        payload => <<"{\"x\":[0,1,2,3,}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ),

    Sql2 =
        "foreach payload.sensors "
        "do item.cmd as msg_type "
        "from \"t/#\" ",
    ?assertMatch(
        {error, {select_and_collect_error, {error, {decode_json_failed, _}, _}}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql2,
                context =>
                    #{
                        payload =>
                            <<"{\"sensors\": [{\"cmd\":\"1\"} {\"cmd\":}]}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ).

t_sqlparse_both_string_types_in_from(_Config) ->
    %% Here is an SQL select statement with both string types in the FROM clause
    SqlSelect =
        "select clientid, topic as tp "
        "from 't/tt', \"$events/client_connected\" ",
    ?assertMatch(
        {ok, #{<<"clientid">> := <<"abc">>, <<"tp">> := <<"t/tt">>}},
        emqx_rule_sqltester:test(
            #{
                sql => SqlSelect,
                context => #{clientid => <<"abc">>, topic => <<"t/tt">>}
            }
        )
    ),
    %% Here is an SQL foreach statement with both string types in the FROM clause
    SqlForeach =
        "foreach payload.sensors "
        "from 't/#', \"$events/client_connected\" ",
    ?assertMatch(
        {ok, []},
        emqx_rule_sqltester:test(
            #{
                sql => SqlForeach,
                context =>
                    #{
                        payload => <<"{\"sensors\": 1}">>,
                        topic => <<"t/a">>
                    }
            }
        )
    ).

%%------------------------------------------------------------------------------
%% Test cases for telemetry functions
%%------------------------------------------------------------------------------

t_get_basic_usage_info_0(_Config) ->
    ?assertEqual(
        #{
            num_rules => 0,
            referenced_bridges => #{}
        },
        emqx_rule_engine:get_basic_usage_info()
    ),
    ok.

t_get_basic_usage_info_1(_Config) ->
    {ok, _} =
        emqx_rule_engine:create_rule(
            #{
                id => <<"rule:t_get_basic_usage_info:1">>,
                sql => <<"select 1 from topic">>,
                actions =>
                    [
                        #{function => <<"erlang:hibernate">>, args => #{}},
                        #{function => console},
                        <<"webhook:my_webhook">>,
                        <<"webhook:my_webhook">>
                    ]
            }
        ),
    {ok, _} =
        emqx_rule_engine:create_rule(
            #{
                id => <<"rule:t_get_basic_usage_info:2">>,
                sql => <<"select 1 from topic">>,
                actions =>
                    [
                        <<"mqtt:my_mqtt_bridge">>,
                        <<"webhook:my_webhook">>
                    ]
            }
        ),
    ?assertEqual(
        #{
            num_rules => 2,
            referenced_bridges =>
                #{
                    mqtt => 1,
                    http => 3
                }
        },
        emqx_rule_engine:get_basic_usage_info()
    ),
    ok.

t_get_rule_ids_by_action_reference_ingress_bridge(_Config) ->
    BridgeId = <<"mqtt:ingress">>,
    RuleId = <<"rule:ingress_bridge_referenced">>,
    {ok, _} =
        emqx_rule_engine:create_rule(
            #{
                id => RuleId,
                sql => <<"select 1 from \"$bridges/", BridgeId/binary, "\"">>,
                actions => [#{function => console}]
            }
        ),
    on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),
    ?assertMatch(
        [RuleId],
        emqx_rule_engine:get_rule_ids_by_action(BridgeId)
    ),
    ok.

%%------------------------------------------------------------------------------
%% Test cases for rule metrics
%%------------------------------------------------------------------------------

-define(BRIDGE_TYPE, <<"mqtt">>).
-define(BRIDGE_NAME, <<"bridge_over_troubled_water">>).
-define(BRIDGE_CONFIG(QMODE), #{
    <<"server">> => <<"127.0.0.1:1883">>,
    <<"username">> => <<"user1">>,
    <<"password">> => <<"">>,
    <<"proto_ver">> => <<"v4">>,
    <<"ssl">> => #{<<"enable">> => false},
    <<"egress">> =>
        #{
            <<"local">> =>
                #{
                    <<"topic">> => <<"foo/#">>
                },
            <<"remote">> =>
                #{
                    <<"topic">> => <<"bar/${topic}">>,
                    <<"payload">> => <<"${payload}">>,
                    <<"qos">> => <<"${qos}">>,
                    <<"retain">> => <<"${retain}">>
                }
        },
    <<"resource_opts">> =>
        #{
            <<"health_check_interval">> => <<"5s">>,
            <<"query_mode">> => QMODE,
            <<"request_ttl">> => <<"3s">>,
            <<"worker_pool_size">> => 1
        }
}).

-define(SUCCESSS_METRICS, #{
    matched := 1,
    'actions.total' := 1,
    'actions.failed' := 0,
    'actions.success' := 1,
    'actions.discarded' := 0
}).
-define(FAIL_METRICS, #{
    matched := 1,
    'actions.total' := 1,
    'actions.failed' := 1,
    'actions.success' := 0,
    'actions.discarded' := 0
}).

t_rule_metrics_sync(_Config) ->
    do_test_rule_metrics_success(<<"sync">>).

t_rule_metrics_async(_Config) ->
    do_test_rule_metrics_success(<<"async">>).

t_rule_metrics_sync_fail(_Config) ->
    do_test_rule_metrics_fail(<<"sync">>).

t_rule_metrics_async_fail(_Config) ->
    do_test_rule_metrics_fail(<<"async">>).

do_test_rule_metrics_success(QMode) ->
    ?assertMatch(
        ?SUCCESSS_METRICS,
        do_test_rule_metrics(QMode)
    ).

do_test_rule_metrics_fail(QMode) ->
    ?assertMatch(
        ?FAIL_METRICS,
        do_test_rule_metrics(QMode)
    ).

do_test_rule_metrics(QMode) ->
    BridgeId = create_bridge(?BRIDGE_TYPE, ?BRIDGE_NAME, ?BRIDGE_CONFIG(QMode)),
    RuleId = <<"rule:test_metrics_bridge_action">>,
    {ok, #{id := RuleId}} =
        emqx_rule_engine:create_rule(
            #{
                id => RuleId,
                sql => <<"SELECT * FROM \"topic/#\"">>,
                actions => [BridgeId]
            }
        ),
    timer:sleep(100),
    ?assertMatch(
        #{
            matched := 0,
            'actions.total' := 0,
            'actions.failed' := 0,
            'actions.success' := 0
        },
        emqx_metrics_worker:get_counters(rule_metrics, RuleId)
    ),
    MsgId = emqx_guid:gen(),
    emqx:publish(#message{id = MsgId, topic = <<"topic/test">>, payload = <<"hello">>}),
    timer:sleep(100),
    on_exit(
        fun() ->
            emqx_rule_engine:delete_rule(RuleId),
            emqx_bridge:remove(?BRIDGE_TYPE, ?BRIDGE_NAME)
        end
    ),
    emqx_metrics_worker:get_counters(rule_metrics, RuleId).

create_bridge(Type, Name, Config) ->
    {ok, _Bridge} = emqx_bridge:create(Type, Name, Config),
    emqx_bridge_resource:bridge_id(Type, Name).

create_rule(Name, SQL) ->
    Rule = emqx_rule_engine_SUITE:make_simple_rule(Name, SQL),
    {ok, _} = emqx_rule_engine:create_rule(Rule).

emqtt_client_config() ->
    [
        {host, "localhost"},
        {clientid, <<"client">>},
        {username, <<"testuser">>},
        {password, <<"pass">>}
    ].

filesync(Name, Type) ->
    ct:sleep(50),
    filesync(Name, Type, 5).

%% sometime the handler process is not started yet.
filesync(Name, Type, 0) ->
    ct:fail("Handler process not started ~p ~p", [Name, Type]);
filesync(Name0, Type, Retry) ->
    Name =
        case is_binary(Name0) of
            true -> Name0;
            false -> list_to_binary(Name0)
        end,
    try
        Handler = binary_to_atom(<<"trace_", (atom_to_binary(Type))/binary, "_", Name/binary>>),
        ok = logger_disk_log_h:filesync(Handler)
    catch
        E:R ->
            ct:pal("Filesync error:~p ~p~n", [{Name, Type, Retry}, {E, R}]),
            ct:sleep(100),
            filesync(Name, Type, Retry - 1)
    end.

t_trace_rule_id(_Config) ->
    %% Start MQTT Client
    emqx_trace_SUITE:reload(),
    {ok, T} = emqtt:start_link(emqtt_client_config()),
    emqtt:connect(T),
    %% Create rules
    create_rule(
        <<"test_rule_id_1">>,
        <<"select 1 as rule_number from \"rule_1_topic\"">>
    ),
    create_rule(
        <<"test_rule_id_2">>,
        <<"select 2 as rule_number from \"rule_2_topic\"">>
    ),
    %% Start tracing
    ok = emqx_trace_handler:install(
        "CLI-RULE-1", ruleid, <<"test_rule_id_1">>, all, "tmp/rule_trace_1.log"
    ),
    ok = emqx_trace_handler:install(
        "CLI-RULE-2", ruleid, <<"test_rule_id_2">>, all, "tmp/rule_trace_2.log"
    ),
    emqx_trace:check(),
    ok = filesync("CLI-RULE-1", ruleid),
    ok = filesync("CLI-RULE-2", ruleid),

    %% Verify the tracing file exits
    ?assert(filelib:is_regular("tmp/rule_trace_1.log")),
    ?assert(filelib:is_regular("tmp/rule_trace_2.log")),

    %% Get current traces
    ?assertMatch(
        [
            #{
                type := ruleid,
                filter := <<"test_rule_id_1">>,
                level := debug,
                dst := "tmp/rule_trace_1.log",
                name := <<"CLI-RULE-1">>
            },
            #{
                type := ruleid,
                filter := <<"test_rule_id_2">>,
                name := <<"CLI-RULE-2">>,
                level := debug,
                dst := "tmp/rule_trace_2.log"
            }
        ],
        emqx_trace_handler:running()
    ),

    %% Trigger rule
    emqtt:publish(T, <<"rule_1_topic">>, <<"my_traced_message">>),
    ?retry(
        100,
        5,
        begin
            ok = filesync("CLI-RULE-1", ruleid),
            {ok, Bin} = file:read_file("tmp/rule_trace_1.log"),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"my_traced_message">>]))
        end
    ),
    ok = filesync("CLI-RULE-2", ruleid),
    ?assert(filelib:file_size("tmp/rule_trace_2.log") =:= 0),

    %% Stop tracing
    ok = emqx_trace_handler:uninstall(ruleid, <<"CLI-RULE-1">>),
    ok = emqx_trace_handler:uninstall(ruleid, <<"CLI-RULE-2">>),
    ?assertEqual([], emqx_trace_handler:running()),
    emqtt:disconnect(T).

t_sqlselect_client_attr(_) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, Compiled} = emqx_variform:compile("user_property.group"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            set_as_attr => <<"group">>
        },
        #{
            expression => Compiled,
            set_as_attr => <<"group2">>
        }
    ]),

    SQL =
        "SELECT client_attrs as payload FROM \"t/1\" ",
    Repub = republish_action(<<"t/2">>),
    {ok, _TopicRule} = emqx_rule_engine:create_rule(
        #{
            sql => SQL,
            id => ?TMP_RULEID,
            actions => [Repub]
        }
    ),

    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'User-Property' => [{<<"group">>, <<"g1">>}]}}
    ]),
    {ok, _} = emqtt:connect(Client),

    {ok, _, _} = emqtt:subscribe(Client, <<"t/2">>, 0),
    ct:sleep(100),
    emqtt:publish(Client, <<"t/1">>, <<"Hello">>),

    receive
        {publish, #{topic := Topic, payload := Payload}} ->
            ?assertEqual(<<"t/2">>, Topic),
            ?assertMatch(
                #{<<"group">> := <<"g1">>, <<"group2">> := <<"g1">>},
                emqx_utils_json:decode(Payload)
            )
    after 1000 ->
        ct:fail(wait_for_t_2)
    end,

    emqtt:disconnect(Client),
    emqx_rule_engine:delete_rule(?TMP_RULEID),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], []).

%% Checks that we bump both `failed' and one of its sub-counters when failures occur while
%% evaluating rule SQL.
t_failed_rule_metrics(_Config) ->
    {ok, C} = emqtt:start_link(emqtt_client_config()),
    emqtt:connect(C),
    RuleId = atom_to_binary(?FUNCTION_NAME),
    %% Never matches
    create_rule(
        RuleId,
        <<"select 1 from \"t\" where false">>
    ),
    {ok, _} = emqtt:publish(C, <<"t">>, <<"hi">>, [{qos, 2}]),
    ?assertMatch(
        #{
            'matched' := 1,
            'failed' := 1,
            'failed.no_result' := 1,
            'failed.exception' := 0
        },
        get_counters(RuleId)
    ),
    %% Exception in select clause
    delete_rule(RuleId),
    create_rule(
        RuleId,
        <<"select map_get(1, 1) from \"t\"">>
    ),
    {ok, _} = emqtt:publish(C, <<"t">>, <<"hi">>, [{qos, 2}]),
    ?assertMatch(
        #{
            'matched' := 1,
            'failed' := 1,
            'failed.no_result' := 0,
            'failed.exception' := 1
        },
        get_counters(RuleId)
    ),
    delete_rule(RuleId),
    emqtt:stop(C),
    ok.

%%------------------------------------------------------------------------------
%% Internal helpers
%%------------------------------------------------------------------------------

is_ee() ->
    emqx_release:edition() == ee.

get_counters(RuleId) ->
    emqx_metrics_worker:get_counters(rule_metrics, RuleId).

republish_action(Topic) ->
    republish_action(Topic, <<"${payload}">>).

republish_action(Topic, Payload) ->
    republish_action(Topic, Payload, <<"${user_properties}">>).

republish_action(Topic, Payload, UserProperties) ->
    republish_action(Topic, Payload, UserProperties, _MQTTProperties = #{}).

republish_action(Topic, Payload, UserProperties, MQTTProperties) ->
    republish_action(Topic, Payload, UserProperties, MQTTProperties, false).

republish_action(Topic, Payload, UserProperties, MQTTProperties, DirectDispatch) ->
    #{
        function => republish,
        args => #{
            payload => Payload,
            topic => Topic,
            qos => 0,
            retain => false,
            mqtt_properties => MQTTProperties,
            user_properties => UserProperties,
            direct_dispatch => DirectDispatch
        }
    }.

action_response(Selected, Envs, Args) ->
    ?tp(action_response, #{
        selected => Selected,
        envs => Envs,
        args => Args
    }),
    ok.

make_simple_rule_with_ts(RuleId, Ts) when is_binary(RuleId) ->
    SQL = <<"select * from \"simple/topic\"">>,
    make_simple_rule(RuleId, SQL, Ts).

make_simple_rule(RuleId) when is_binary(RuleId) ->
    SQL = <<"select * from \"simple/topic\"">>,
    make_simple_rule(RuleId, SQL).

make_simple_rule(RuleId, SQL) when is_binary(RuleId) ->
    make_simple_rule(RuleId, SQL, erlang:system_time(millisecond)).

make_simple_rule(RuleId, SQL, Ts) when is_binary(RuleId) ->
    #{
        id => RuleId,
        sql => SQL,
        actions => [#{function => console, args => #{}}],
        description => <<"simple rule">>,
        created_at => Ts
    }.

action_record_triggered_events(Data = #{event := EventName}, _Envs, _Args) ->
    ct:pal("applying action_record_triggered_events: ~p", [Data]),
    ets:insert(events_record_tab, {EventName, Data}).

verify_event(EventName) ->
    ct:sleep(50),
    case ets:lookup(events_record_tab, EventName) of
        [] ->
            ct:fail({no_such_event, EventName, ets:tab2list(events_record_tab)});
        Records ->
            [
                begin
                    %% verify fields can be formatted to JSON string
                    _ = emqx_utils_json:encode(Fields),
                    %% verify metadata fields
                    verify_metadata_fields(EventName, Fields),
                    %% verify available fields for each event name
                    verify_event_fields(EventName, Fields)
                end
             || {_Name, Fields} <- Records
            ]
    end.

verify_metadata_fields(_EventName, #{metadata := Metadata}) ->
    ?assertMatch(
        #{rule_id := <<"rule:t_events">>},
        Metadata
    ).

verify_event_fields('message.publish', Fields) ->
    #{
        id := ID,
        clientid := ClientId,
        username := Username,
        payload := Payload,
        peerhost := PeerHost,
        peername := PeerName,
        topic := Topic,
        qos := QoS,
        flags := Flags,
        pub_props := Properties,
        timestamp := Timestamp,
        publish_received_at := EventAt,
        client_attrs := ClientAttrs
    } = Fields,
    Now = erlang:system_time(millisecond),
    TimestampElapse = Now - Timestamp,
    RcvdAtElapse = Now - EventAt,
    ?assert(is_binary(ID)),
    ?assertEqual(<<"c_event">>, ClientId),
    ?assertEqual(<<"u_event">>, Username),
    ?assertEqual(<<"{\"id\": 1, \"name\": \"ha\"}">>, Payload),
    verify_peername(PeerName),
    verify_ipaddr(PeerHost),
    ?assertEqual(<<"t1">>, Topic),
    ?assertEqual(1, QoS),
    ?assert(is_map(Flags)),
    ?assertMatch(#{'Message-Expiry-Interval' := 60}, Properties),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60 * 1000),
    ?assert(EventAt =< Timestamp),
    ?assert(is_map(ClientAttrs));
verify_event_fields('client.connected', Fields) ->
    #{
        clientid := ClientId,
        username := Username,
        mountpoint := MountPoint,
        peername := PeerName,
        sockname := SockName,
        proto_name := ProtoName,
        proto_ver := ProtoVer,
        keepalive := Keepalive,
        clean_start := CleanStart,
        expiry_interval := ExpiryInterval,
        is_bridge := IsBridge,
        conn_props := Properties,
        timestamp := Timestamp,
        connected_at := EventAt,
        client_attrs := ClientAttrs
    } = Fields,
    Now = erlang:system_time(millisecond),
    TimestampElapse = Now - Timestamp,
    RcvdAtElapse = Now - EventAt,
    ?assert(is_binary(MountPoint) orelse MountPoint == undefined),
    ?assert(lists:member(ClientId, [<<"c_event">>, <<"c_event2">>])),
    ?assert(lists:member(Username, [<<"u_event">>, <<"u_event2">>])),
    verify_peername(PeerName),
    verify_peername(SockName),
    ?assertEqual(<<"MQTT">>, ProtoName),
    ?assertEqual(5, ProtoVer),
    ?assert(is_integer(Keepalive)),
    ?assert(is_boolean(CleanStart)),
    ?assertEqual(60, ExpiryInterval),
    ?assertEqual(false, IsBridge),
    ?assertMatch(#{'Session-Expiry-Interval' := 60}, Properties),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60 * 1000),
    ?assert(EventAt =< Timestamp),
    ?assert(is_map(ClientAttrs));
verify_event_fields('client.disconnected', Fields) ->
    #{
        reason := Reason,
        clientid := ClientId,
        username := Username,
        peername := PeerName,
        sockname := SockName,
        disconn_props := Properties,
        timestamp := Timestamp,
        disconnected_at := EventAt,
        client_attrs := ClientAttrs
    } = Fields,
    Now = erlang:system_time(millisecond),
    TimestampElapse = Now - Timestamp,
    RcvdAtElapse = Now - EventAt,
    ?assert(is_atom(Reason)),
    ?assert(lists:member(ClientId, [<<"c_event">>, <<"c_event2">>])),
    ?assert(lists:member(Username, [<<"u_event">>, <<"u_event2">>])),
    verify_peername(PeerName),
    verify_peername(SockName),
    ?assertMatch(#{'User-Property' := #{<<"reason">> := <<"normal">>}}, Properties),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60 * 1000),
    ?assert(EventAt =< Timestamp),
    ?assert(is_map(ClientAttrs));
verify_event_fields(SubUnsub, Fields) when
    SubUnsub == 'session.subscribed';
    SubUnsub == 'session.unsubscribed'
->
    #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost,
        peername := PeerName,
        topic := Topic,
        qos := QoS,
        timestamp := Timestamp,
        client_attrs := ClientAttrs
    } = Fields,
    Now = erlang:system_time(millisecond),
    TimestampElapse = Now - Timestamp,
    ?assert(is_atom(reason)),
    ?assertEqual(<<"c_event2">>, ClientId),
    ?assertEqual(<<"u_event2">>, Username),
    verify_peername(PeerName),
    verify_ipaddr(PeerHost),
    ?assertEqual(<<"t1">>, Topic),
    ?assertEqual(1, QoS),
    PropKey =
        case SubUnsub of
            'session.subscribed' -> sub_props;
            'session.unsubscribed' -> unsub_props
        end,
    ?assertMatch(
        #{'User-Property' := #{<<"topic_name">> := <<"t1">>}},
        maps:get(PropKey, Fields)
    ),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000),
    ?assert(is_map(ClientAttrs));
verify_event_fields('delivery.dropped', Fields) ->
    #{
        event := 'delivery.dropped',
        id := ID,
        metadata := #{rule_id := RuleId},
        reason := Reason,
        clientid := ClientId,
        username := Username,
        from_clientid := FromClientId,
        from_username := FromUsername,
        node := Node,
        payload := Payload,
        peerhost := PeerHost,
        peername := PeerName,
        pub_props := Properties,
        publish_received_at := EventAt,
        qos := QoS,
        flags := Flags,
        timestamp := Timestamp,
        topic := Topic
    } = Fields,
    Now = erlang:system_time(millisecond),
    TimestampElapse = Now - Timestamp,
    RcvdAtElapse = Now - EventAt,
    ?assert(is_binary(ID)),
    ?assertEqual(<<"rule:t_events">>, RuleId),
    ?assertEqual(no_local, Reason),
    ?assertEqual(node(), Node),
    ?assertEqual(<<"c_event">>, ClientId),
    ?assertEqual(<<"u_event">>, Username),
    ?assertEqual(<<"c_event">>, FromClientId),
    ?assertEqual(<<"u_event">>, FromUsername),
    ?assertEqual(<<"{\"id\": 1, \"name\": \"ha\"}">>, Payload),
    verify_peername(PeerName),
    verify_ipaddr(PeerHost),
    ?assertEqual(<<"t1">>, Topic),
    ?assertEqual(1, QoS),
    ?assert(is_map(Flags)),
    ?assertMatch(#{'Message-Expiry-Interval' := 60}, Properties),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60 * 1000),
    ?assert(EventAt =< Timestamp);
verify_event_fields('message.dropped', Fields) ->
    #{
        id := ID,
        reason := Reason,
        clientid := ClientId,
        username := Username,
        payload := Payload,
        peerhost := PeerHost,
        peername := PeerName,
        topic := Topic,
        qos := QoS,
        flags := Flags,
        pub_props := Properties,
        timestamp := Timestamp,
        publish_received_at := EventAt
    } = Fields,
    Now = erlang:system_time(millisecond),
    TimestampElapse = Now - Timestamp,
    RcvdAtElapse = Now - EventAt,
    ?assert(is_binary(ID)),
    ?assert(is_atom(Reason)),
    ?assertEqual(<<"c_event">>, ClientId),
    ?assertEqual(<<"u_event">>, Username),
    ?assertEqual(<<"{\"id\": 1, \"name\": \"ha\"}">>, Payload),
    verify_peername(PeerName),
    verify_ipaddr(PeerHost),
    ?assertEqual(<<"t1">>, Topic),
    ?assertEqual(1, QoS),
    ?assert(is_map(Flags)),
    ?assertMatch(#{'Message-Expiry-Interval' := 60}, Properties),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60 * 1000),
    ?assert(EventAt =< Timestamp);
verify_event_fields('message.delivered', Fields) ->
    #{
        id := ID,
        clientid := ClientId,
        username := Username,
        from_clientid := FromClientId,
        from_username := FromUsername,
        payload := Payload,
        peerhost := PeerHost,
        peername := PeerName,
        topic := Topic,
        qos := QoS,
        flags := Flags,
        pub_props := Properties,
        timestamp := Timestamp,
        publish_received_at := EventAt
    } = Fields,
    Now = erlang:system_time(millisecond),
    TimestampElapse = Now - Timestamp,
    RcvdAtElapse = Now - EventAt,
    ?assert(is_binary(ID)),
    ?assertEqual(<<"c_event2">>, ClientId),
    ?assertEqual(<<"u_event2">>, Username),
    ?assertEqual(<<"c_event">>, FromClientId),
    ?assertEqual(<<"u_event">>, FromUsername),
    ?assertEqual(<<"{\"id\": 1, \"name\": \"ha\"}">>, Payload),
    verify_peername(PeerName),
    verify_ipaddr(PeerHost),
    ?assertEqual(<<"t1">>, Topic),
    ?assertEqual(1, QoS),
    ?assert(is_map(Flags)),
    ?assertMatch(#{'Message-Expiry-Interval' := 60}, Properties),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60 * 1000),
    ?assert(EventAt =< Timestamp);
verify_event_fields('message.acked', Fields) ->
    #{
        id := ID,
        clientid := ClientId,
        username := Username,
        from_clientid := FromClientId,
        from_username := FromUsername,
        payload := Payload,
        peerhost := PeerHost,
        peername := PeerName,
        topic := Topic,
        qos := QoS,
        flags := Flags,
        pub_props := PubProps,
        puback_props := PubAckProps,
        timestamp := Timestamp,
        publish_received_at := EventAt
    } = Fields,
    Now = erlang:system_time(millisecond),
    TimestampElapse = Now - Timestamp,
    RcvdAtElapse = Now - EventAt,
    ?assert(is_binary(ID)),
    ?assertEqual(<<"c_event2">>, ClientId),
    ?assertEqual(<<"u_event2">>, Username),
    ?assertEqual(<<"c_event">>, FromClientId),
    ?assertEqual(<<"u_event">>, FromUsername),
    ?assertEqual(<<"{\"id\": 1, \"name\": \"ha\"}">>, Payload),
    verify_peername(PeerName),
    verify_ipaddr(PeerHost),
    ?assertEqual(<<"t1">>, Topic),
    ?assertEqual(1, QoS),
    ?assert(is_map(Flags)),
    ?assertMatch(#{'Message-Expiry-Interval' := 60}, PubProps),
    ?assert(is_map(PubAckProps)),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60 * 1000),
    ?assert(EventAt =< Timestamp);
verify_event_fields('client.connack', Fields) ->
    #{
        clientid := ClientId,
        clean_start := CleanStart,
        username := Username,
        peername := PeerName,
        sockname := SockName,
        proto_name := ProtoName,
        proto_ver := ProtoVer,
        keepalive := Keepalive,
        expiry_interval := ExpiryInterval,
        conn_props := Properties,
        reason_code := Reason,
        timestamp := Timestamp
    } = Fields,
    Now = erlang:system_time(millisecond),
    TimestampElapse = Now - Timestamp,
    ?assert(lists:member(Reason, [success, bad_username_or_password])),
    ?assert(lists:member(ClientId, [<<"c_event">>, <<"c_event2">>, <<"c_event3">>])),
    ?assert(lists:member(Username, [<<"u_event">>, <<"u_event2">>, <<"u_event3">>])),
    verify_peername(PeerName),
    verify_peername(SockName),
    ?assertEqual(<<"MQTT">>, ProtoName),
    ?assertEqual(5, ProtoVer),
    ?assert(is_integer(Keepalive)),
    ?assert(is_boolean(CleanStart)),
    ?assertEqual(60000, ExpiryInterval),
    ?assertMatch(#{'Session-Expiry-Interval' := 60}, Properties),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000);
verify_event_fields('client.check_authz_complete', Fields) ->
    #{
        clientid := ClientId,
        action := Action,
        result := Result,
        peername := PeerName,
        topic := Topic,
        authz_source := AuthzSource,
        username := Username,
        client_attrs := ClientAttrs
    } = Fields,
    ?assertEqual(<<"t1">>, Topic),
    ?assert(lists:member(Action, [subscribe, publish])),
    ?assert(lists:member(Result, [allow, deny])),
    verify_peername(PeerName),
    ?assert(
        lists:member(AuthzSource, [
            cache,
            default,
            file,
            http,
            mongodb,
            mysql,
            redis,
            postgresql,
            built_in_database
        ])
    ),
    ?assert(lists:member(ClientId, [<<"c_event">>, <<"c_event2">>])),
    ?assert(lists:member(Username, [<<"u_event">>, <<"u_event2">>])),
    ?assert(is_map(ClientAttrs));
verify_event_fields('client.check_authn_complete', Fields) ->
    #{
        clientid := ClientId,
        peername := PeerName,
        username := Username,
        is_anonymous := IsAnonymous,
        is_superuser := IsSuperuser,
        client_attrs := ClientAttrs
    } = Fields,
    verify_peername(PeerName),
    ?assert(lists:member(ClientId, [<<"c_event">>, <<"c_event2">>])),
    ?assert(lists:member(Username, [<<"u_event">>, <<"u_event2">>])),
    ?assert(erlang:is_boolean(IsAnonymous)),
    ?assert(erlang:is_boolean(IsSuperuser)),
    ?assert(is_map(ClientAttrs));
verify_event_fields('schema.validation_failed', Fields) ->
    #{
        validation := ValidationName,
        clientid := ClientId,
        username := Username,
        payload := _Payload,
        peername := PeerName,
        qos := _QoS,
        topic := _Topic,
        flags := _Flags,
        pub_props := _PubProps,
        publish_received_at := _PublishReceivedAt
    } = Fields,
    ?assertEqual(<<"v1">>, ValidationName),
    verify_peername(PeerName),
    ?assert(lists:member(ClientId, [<<"c_event">>, <<"c_event2">>])),
    ?assert(lists:member(Username, [<<"u_event">>, <<"u_event2">>])),
    ok;
verify_event_fields('message.transformation_failed', Fields) ->
    #{
        transformation := TransformationName,
        clientid := ClientId,
        username := Username,
        payload := _Payload,
        peername := PeerName,
        qos := _QoS,
        topic := _Topic,
        flags := _Flags,
        pub_props := _PubProps,
        publish_received_at := _PublishReceivedAt
    } = Fields,
    ?assertEqual(<<"t1">>, TransformationName),
    verify_peername(PeerName),
    ?assert(lists:member(ClientId, [<<"c_event">>, <<"c_event2">>])),
    ?assert(lists:member(Username, [<<"u_event">>, <<"u_event2">>])),
    ok.

verify_peername(PeerName) ->
    case string:split(PeerName, ":") of
        [IPAddrS, PortS] ->
            verify_ipaddr(IPAddrS),
            _ = binary_to_integer(PortS);
        _ ->
            ct:fail({invalid_peername, PeerName})
    end.

verify_ipaddr(IPAddrS) ->
    ?assertMatch({ok, _}, inet:parse_address(binary_to_list(IPAddrS))).

init_events_counters() ->
    ets:new(events_record_tab, [named_table, bag, public]).

user_properties(PairsMap) ->
    #{'User-Property' => maps:to_list(PairsMap)}.

%%------------------------------------------------------------------------------
%% Start Apps
%%------------------------------------------------------------------------------
deps_path(App, RelativePath) ->
    Path0 = code:lib_dir(App),
    Path =
        case file:read_link(Path0) of
            {ok, Resolved} -> Resolved;
            {error, _} -> Path0
        end,
    filename:join([Path, RelativePath]).

local_path(RelativePath) ->
    deps_path(emqx_rule_engine, RelativePath).

create_rules(Rules) ->
    lists:foreach(fun create_rule/1, Rules).

create_rule(Rule) ->
    {ok, _} = emqx_rule_engine:create_rule(Rule),
    ok.

delete_rules_by_ids(Ids) ->
    lists:foreach(
        fun(Id) ->
            ok = emqx_rule_engine:delete_rule(Id)
        end,
        Ids
    ).

delete_rule(#{id := Id}) ->
    ok = emqx_rule_engine:delete_rule(Id);
delete_rule(Id) when is_binary(Id) ->
    ok = emqx_rule_engine:delete_rule(Id).
