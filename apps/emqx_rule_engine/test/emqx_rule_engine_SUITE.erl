%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx_rule_engine/include/rule_engine.hrl").
-include_lib("emqx/include/emqx.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

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
        {group, bugs}
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
            t_sqlselect_1,
            t_sqlselect_2,
            t_sqlselect_3,
            t_sqlselect_message_publish_event_keep_original_props_1,
            t_sqlselect_message_publish_event_keep_original_props_2,
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
            t_sqlparse_case_when_1,
            t_sqlparse_case_when_2,
            t_sqlparse_case_when_3,
            t_sqlparse_array_index_1,
            t_sqlparse_array_index_2,
            t_sqlparse_array_index_3,
            t_sqlparse_array_index_4,
            t_sqlparse_array_index_5,
            t_sqlparse_select_matadata_1,
            t_sqlparse_array_range_1,
            t_sqlparse_array_range_2,
            t_sqlparse_true_false,
            t_sqlparse_undefined_variable,
            t_sqlparse_new_map,
            t_sqlparse_invalid_json
        ]},
        {events, [], [
            t_events,
            t_event_client_disconnected_normal,
            t_event_client_disconnected_kicked,
            t_event_client_disconnected_discarded,
            t_event_client_disconnected_takenover
        ]},
        {telemetry, [], [
            t_get_basic_usage_info_0,
            t_get_basic_usage_info_1
        ]},
        {bugs, [], [
            t_sqlparse_payload_as,
            t_sqlparse_nested_get
        ]}
    ].

%%------------------------------------------------------------------------------
%% Overall setup/teardown
%%------------------------------------------------------------------------------

init_per_suite(Config) ->
    %% ensure module loaded
    emqx_rule_funcs_demo:module_info(),
    application:load(emqx_conf),
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_rule_engine, emqx_authz],
        fun set_special_configs/1
    ),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_conf, emqx_rule_engine]),
    ok.

set_special_configs(emqx_authz) ->
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

init_per_group(registry, Config) ->
    Config;
init_per_group(_Groupname, Config) ->
    Config.

end_per_group(_Groupname, _Config) ->
    ok.

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
        "\"$events/session_subscribed\", "
        "\"$events/session_unsubscribed\", "
        "\"$events/message_acked\", "
        "\"$events/message_delivered\", "
        "\"$events/message_dropped\", "
        "\"$events/delivery_dropped\", "
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
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_events, Config) ->
    ets:delete(events_record_tab),
    ok = delete_rule(?config(hook_points_rules, Config)),
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

t_ensure_action_removed(_) ->
    Id = <<"t_ensure_action_removed">>,
    GetSelectedData = <<"emqx_rule_sqltester:get_selected_data">>,
    emqx:update_config(
        [rule_engine, rules, Id],
        #{
            <<"actions">> => [
                #{<<"function">> => GetSelectedData},
                #{<<"function">> => <<"console">>},
                #{<<"function">> => <<"republish">>},
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
    ct:pal("--- current emqx hooks: ~p", [ets:tab2list(emqx_hooks)]),
    verify_event('delivery.dropped'),
    ok.
message_dropped(Client) ->
    message_publish(Client),
    verify_event('message.dropped'),
    ok.
message_acked(_Client) ->
    verify_event('message.acked'),
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
    Props = user_properties(#{<<"inject_key">> => <<"inject_val">>}),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, #{}, <<"{\"x\":1}">>, [{qos, 0}]),
    receive
        {publish, #{topic := T, payload := Payload, properties := Props2}} ->
            ?assertEqual(Props, Props2),
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

t_sqlparse_event_1(_Config) ->
    Sql =
        "select topic as tp "
        "from \"$events/session_subscribed\" ",
    ?assertMatch(
        {ok, #{<<"tp">> := <<"t/tt">>}},
        emqx_rule_sqltester:test(
            #{
                sql => Sql,
                context => #{topic => <<"t/tt">>}
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
                context => #{clientid => <<"abc">>}
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
        {ok, #{<<"payload">> := #{<<"x">> := [3]}}},
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
        {ok, #{<<"payload">> := #{<<"x">> := [#{<<"y">> := 3}]}}},
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
        {ok, #{<<"payload">> := #{<<"x">> := [1, #{<<"y">> := 0}, 3]}}},
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
        {ok, #{<<"payload">> := #{<<"a">> := [0, 1, 2, 3]}}},
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
                    webhook => 3
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
%% Internal helpers
%%------------------------------------------------------------------------------

republish_action(Topic) ->
    republish_action(Topic, <<"${payload}">>).

republish_action(Topic, Payload) ->
    republish_action(Topic, Payload, <<"${user_properties}">>).

republish_action(Topic, Payload, UserProperties) ->
    #{
        function => republish,
        args => #{
            payload => Payload,
            topic => Topic,
            qos => 0,
            retain => false,
            user_properties => UserProperties
        }
    }.

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
    ?assertEqual(<<"c_event">>, ClientId),
    ?assertEqual(<<"u_event">>, Username),
    ?assertEqual(<<"{\"id\": 1, \"name\": \"ha\"}">>, Payload),
    verify_ipaddr(PeerHost),
    ?assertEqual(<<"t1">>, Topic),
    ?assertEqual(1, QoS),
    ?assert(is_map(Flags)),
    ?assertMatch(#{'Message-Expiry-Interval' := 60}, Properties),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60 * 1000),
    ?assert(EventAt =< Timestamp);
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
        connected_at := EventAt
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
    ?assert(EventAt =< Timestamp);
verify_event_fields('client.disconnected', Fields) ->
    #{
        reason := Reason,
        clientid := ClientId,
        username := Username,
        peername := PeerName,
        sockname := SockName,
        disconn_props := Properties,
        timestamp := Timestamp,
        disconnected_at := EventAt
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
    ?assert(EventAt =< Timestamp);
verify_event_fields(SubUnsub, Fields) when
    SubUnsub == 'session.subscribed';
    SubUnsub == 'session.unsubscribed'
->
    #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost,
        topic := Topic,
        qos := QoS,
        timestamp := Timestamp
    } = Fields,
    Now = erlang:system_time(millisecond),
    TimestampElapse = Now - Timestamp,
    ?assert(is_atom(reason)),
    ?assertEqual(<<"c_event2">>, ClientId),
    ?assertEqual(<<"u_event2">>, Username),
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
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60 * 1000);
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
        topic := Topic,
        authz_source := AuthzSource,
        username := Username
    } = Fields,
    ?assertEqual(<<"t1">>, Topic),
    ?assert(lists:member(Action, [subscribe, publish])),
    ?assert(lists:member(Result, [allow, deny])),
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
    ?assert(lists:member(Username, [<<"u_event">>, <<"u_event2">>])).

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
