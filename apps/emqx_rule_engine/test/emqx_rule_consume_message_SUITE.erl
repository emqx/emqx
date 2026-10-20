%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_consume_message_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
A QoS 1 message that no client subscribes to and that a rule with
`consume_message` selects gets PUBACK 0x00, is not counted as dropped, does not
run the `message.dropped` hook, and is counted in `messages.consumed`.
""".
t_qos1_consumed(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    _ = create_rule(Topic, #{<<"consume_message">> => true}),
    ok = watch_dropped(),
    C = connect(v5),
    Before = counters(),
    ?assertMatch({ok, #{reason_code := ?RC_SUCCESS}}, emqtt:publish(C, Topic, <<"m">>, 1)),
    ?assertEqual(consumed_deltas(), deltas(Before)),
    ?assertEqual([], dropped_topics()).

-doc """
Same as `t_qos1_consumed`, for QoS 2: PUBREC carries 0x00.
""".
t_qos2_consumed(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    _ = create_rule(Topic, #{<<"consume_message">> => true}),
    ok = watch_dropped(),
    C = connect(v5),
    Before = counters(),
    ?assertMatch({ok, #{reason_code := ?RC_SUCCESS}}, emqtt:publish(C, Topic, <<"m">>, 2)),
    ?assertEqual(consumed_deltas(), deltas(Before)),
    ?assertEqual([], dropped_topics()).

-doc """
A rule created without `consume_message` reports `false` and leaves a QoS 1
message without subscribers dropped with PUBACK 0x10.
""".
t_qos1_not_consumed_by_default(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    RuleId = create_rule(Topic, #{}),
    ?assertMatch({200, #{<<"consume_message">> := false}}, get_rule(RuleId)),
    ok = watch_dropped(),
    C = connect(v5),
    Before = counters(),
    ?assertMatch(
        {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}}, emqtt:publish(C, Topic, <<"m">>, 1)
    ),
    ?assertEqual(dropped_deltas(), deltas(Before)),
    ?assertEqual([Topic], dropped_topics()).

-doc """
Same as `t_qos1_not_consumed_by_default`, for QoS 2: PUBREC carries 0x10.
""".
t_qos2_not_consumed_by_default(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    _ = create_rule(Topic, #{}),
    ok = watch_dropped(),
    C = connect(v5),
    Before = counters(),
    ?assertMatch(
        {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}}, emqtt:publish(C, Topic, <<"m">>, 2)
    ),
    ?assertEqual(dropped_deltas(), deltas(Before)),
    ?assertEqual([Topic], dropped_topics()).

-doc """
A rule with `consume_message` whose `WHERE` clause is false does not mark the
message: PUBACK 0x10 and the message is counted as dropped.
""".
t_where_false_not_consumed(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    Sql = <<"SELECT * FROM \"", Topic/binary, "\" WHERE payload = 'yes'">>,
    _ = create_rule(Topic, #{<<"sql">> => Sql, <<"consume_message">> => true}),
    ok = watch_dropped(),
    C = connect(v5),
    Before = counters(),
    ?assertMatch(
        {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}}, emqtt:publish(C, Topic, <<"no">>, 1)
    ),
    ?assertEqual(dropped_deltas(), deltas(Before)),
    ?assertEqual([Topic], dropped_topics()).

-doc """
A `FOREACH` rule with `consume_message` whose collection is empty produces no
output and does not mark the message.
""".
t_foreach_empty_not_consumed(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    Sql = <<"FOREACH payload.items FROM \"", Topic/binary, "\"">>,
    _ = create_rule(Topic, #{<<"sql">> => Sql, <<"consume_message">> => true}),
    ok = watch_dropped(),
    C = connect(v5),
    Before = counters(),
    ?assertMatch(
        {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}},
        emqtt:publish(C, Topic, <<"{\"items\": []}">>, 1)
    ),
    ?assertEqual(dropped_deltas(), deltas(Before)),
    ?assertEqual([Topic], dropped_topics()).

-doc """
A `FOREACH` rule with `consume_message` whose collection is not empty marks the
message.
""".
t_foreach_consumed(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    Sql = <<"FOREACH payload.items FROM \"", Topic/binary, "\"">>,
    _ = create_rule(Topic, #{<<"sql">> => Sql, <<"consume_message">> => true}),
    ok = watch_dropped(),
    C = connect(v5),
    Before = counters(),
    ?assertMatch(
        {ok, #{reason_code := ?RC_SUCCESS}},
        emqtt:publish(C, Topic, <<"{\"items\": [1]}">>, 1)
    ),
    ?assertEqual(consumed_deltas(), deltas(Before)),
    ?assertEqual([], dropped_topics()).

-doc """
A consumed message is still delivered to a subscriber. It is neither dropped
nor counted in `messages.consumed`, which counts only messages without
subscribers.
""".
t_subscriber_still_receives(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    _ = create_rule(Topic, #{<<"consume_message">> => true}),
    ok = watch_dropped(),
    Sub = connect(v5),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Sub, Topic, 1),
    Pub = connect(v5),
    Before = counters(),
    ?assertMatch({ok, #{reason_code := ?RC_SUCCESS}}, emqtt:publish(Pub, Topic, <<"m">>, 1)),
    ?assertReceive({publish, #{topic := Topic, payload := <<"m">>}}),
    ?assertEqual(#{dropped => 0, no_subscribers => 0, consumed => 0}, deltas(Before)),
    ?assertEqual([], dropped_topics()).

-doc """
MQTT 3.1.1 PUBACK has no reason code, so only the counters and the
`message.dropped` hook show that a QoS 1 message was consumed.
""".
t_mqtt311_consumed(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    _ = create_rule(Topic, #{<<"consume_message">> => true}),
    ok = watch_dropped(),
    C = connect(v4),
    Before = counters(),
    ?assertMatch({ok, _}, emqtt:publish(C, Topic, <<"m">>, 1)),
    ?assertEqual(consumed_deltas(), deltas(Before)),
    ?assertEqual([], dropped_topics()).

-doc """
QoS 0 has no acknowledgement, so only the counters and the `message.dropped`
hook show that the message was consumed.
""".
t_qos0_consumed(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    _ = create_rule(Topic, #{<<"consume_message">> => true}),
    ok = watch_dropped(),
    C = connect(v5),
    Before = counters(),
    ok = emqtt:publish(C, Topic, <<"m">>, 0),
    %% A QoS 1 publish on a topic of no rule orders the QoS 0 one before the check.
    {ok, _} = emqtt:publish(C, <<"sync/", Topic/binary>>, <<>>, 1),
    ?assertEqual(
        #{dropped => 1, no_subscribers => 1, consumed => 1},
        deltas(Before)
    ),
    ?assertEqual([<<"sync/", Topic/binary>>], dropped_topics()).

-doc """
Two matching rules, one with `consume_message` and one without: the message is
marked whatever the order the rules run in.
""".
t_two_rules_one_consuming(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    _ = create_rule(Topic, #{<<"consume_message">> => false}),
    _ = create_rule(Topic, #{<<"consume_message">> => true}),
    _ = create_rule(Topic, #{<<"consume_message">> => false}),
    ok = watch_dropped(),
    C = connect(v5),
    Before = counters(),
    ?assertMatch({ok, #{reason_code := ?RC_SUCCESS}}, emqtt:publish(C, Topic, <<"m">>, 1)),
    ?assertEqual(consumed_deltas(), deltas(Before)),
    ?assertEqual([], dropped_topics()).

-doc """
A disabled rule with `consume_message` does not mark the message.
""".
t_disabled_rule_not_consumed(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    _ = create_rule(Topic, #{<<"consume_message">> => true, <<"enable">> => false}),
    C = connect(v5),
    ?assertMatch(
        {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}}, emqtt:publish(C, Topic, <<"m">>, 1)
    ).

-doc """
`PUT /rules/:id` turns `consume_message` on and off, and `GET` reports it.
""".
t_update_rule(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    RuleId = create_rule(Topic, #{<<"consume_message">> => true}),
    ?assertMatch({200, #{<<"consume_message">> := true}}, get_rule(RuleId)),
    C = connect(v5),
    ?assertMatch({ok, #{reason_code := ?RC_SUCCESS}}, emqtt:publish(C, Topic, <<"m">>, 1)),
    ?assertMatch(
        {200, #{<<"consume_message">> := false}},
        update_rule(RuleId, #{<<"sql">> => sql(Topic), <<"consume_message">> => false})
    ),
    ?assertMatch({200, #{<<"consume_message">> := false}}, get_rule(RuleId)),
    ?assertMatch(
        {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}}, emqtt:publish(C, Topic, <<"m">>, 1)
    ).

-doc """
`POST /publish` answers 200 instead of 202 for a consumed message without
subscribers.
""".
t_http_publish_consumed(_Config) ->
    Topic = topic(?FUNCTION_NAME),
    _ = create_rule(Topic, #{<<"consume_message">> => true}),
    Before = counters(),
    ?assertMatch(
        {200, #{<<"id">> := _}},
        emqx_mgmt_api_test_util:simple_request(
            post,
            emqx_mgmt_api_test_util:api_path(["publish"]),
            #{<<"topic">> => Topic, <<"payload">> => <<"m">>, <<"qos">> => 1}
        )
    ),
    ?assertEqual(consumed_deltas(), deltas(Before)).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

topic(TestCase) ->
    <<"consume/", (atom_to_binary(TestCase))/binary>>.

sql(Topic) ->
    <<"SELECT * FROM \"", Topic/binary, "\"">>.

create_rule(Topic, Overrides) ->
    Params = maps:merge(#{<<"sql">> => sql(Topic), <<"actions">> => []}, Overrides),
    {201, #{<<"id">> := RuleId} = Rule} = emqx_mgmt_api_test_util:simple_request(
        post, emqx_mgmt_api_test_util:api_path(["rules"]), Params
    ),
    ?assertEqual(
        maps:get(<<"consume_message">>, Overrides, false), maps:get(<<"consume_message">>, Rule)
    ),
    on_exit(fun() ->
        {204, _} = emqx_mgmt_api_test_util:simple_request(#{
            method => delete, url => emqx_mgmt_api_test_util:api_path(["rules", RuleId])
        })
    end),
    RuleId.

get_rule(RuleId) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => get, url => emqx_mgmt_api_test_util:api_path(["rules", RuleId])
    }).

update_rule(RuleId, Params) ->
    emqx_mgmt_api_test_util:simple_request(
        put, emqx_mgmt_api_test_util:api_path(["rules", RuleId]), Params
    ).

connect(ProtoVer) ->
    {ok, C} = emqtt:start_link(#{proto_ver => ProtoVer}),
    {ok, _} = emqtt:connect(C),
    on_exit(fun() -> catch emqtt:stop(C) end),
    C.

counters() ->
    #{
        dropped => emqx_metrics:val_global('messages.dropped'),
        no_subscribers => emqx_metrics:val_global('messages.dropped.no_subscribers'),
        consumed => emqx_metrics:val_global('messages.consumed')
    }.

deltas(Before) ->
    maps:map(fun(K, V) -> V - maps:get(K, Before) end, counters()).

consumed_deltas() ->
    #{dropped => 0, no_subscribers => 0, consumed => 1}.

dropped_deltas() ->
    #{dropped => 1, no_subscribers => 1, consumed => 0}.

%% Record the topics of the messages for which the `message.dropped' hook runs.
watch_dropped() ->
    Hook = {?MODULE, on_message_dropped, [self()]},
    ok = emqx_hooks:put('message.dropped', Hook, ?HP_HIGHEST),
    on_exit(fun() -> emqx_hooks:del('message.dropped', {?MODULE, on_message_dropped}) end).

on_message_dropped(Msg, _By, Reason, TestPid) ->
    emqx_message:is_sys(Msg) orelse (TestPid ! {message_dropped, emqx_message:topic(Msg), Reason}),
    ok.

%% The hook runs in the publisher's channel before it sends PUBACK or PUBREC,
%% so a dropped notice is already in the mailbox when the publish returns.
dropped_topics() ->
    receive
        {message_dropped, Topic, no_subscribers} -> [Topic | dropped_topics()]
    after 0 -> []
    end.
