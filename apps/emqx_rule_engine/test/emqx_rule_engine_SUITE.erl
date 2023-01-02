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

-import(emqx_rule_test_lib,
        [ request_api/4
        , request_api/5
        , auth_header_/0
        , api_path/1
        , stop_apps/0
        , start_apps/0
        , create_simple_repub_rule/2
        , create_simple_repub_rule/3
        , make_simple_debug_resource_type/0
        , make_simple_resource_type/1
        , make_bad_resource_type/0
        , init_events_counters/0
        ]).

%%-define(PROPTEST(M,F), true = proper:quickcheck(M:F())).

all() ->
    [ {group, engine}
    , {group, actions}
    , {group, api}
    , {group, cli}
    , {group, funcs}
    , {group, registry}
    , {group, runtime}
    , {group, events}
    , {group, multi_actions}
    , {group, bugs}
    , {group, rule_metrics}
    ].

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 30}}].

groups() ->
    [{engine, [sequence],
      [t_register_provider,
       t_unregister_provider,
       t_create_rule,
       t_reset_metrics,
       t_reset_metrics_fallbacks,
       t_create_resource,
       t_create_rule_with_resource,
       t_clean_resource_alarms
      ]},
     {actions, [],
      [t_inspect_action
      ,t_republish_action
      ]},
     {api, [],
      [t_crud_rule_api,
       t_rule_api_unicode_ids,
       t_list_actions_api,
       t_show_action_api,
       t_crud_resources_api,
       t_list_resource_types_api,
       t_show_resource_type_api,
       t_list_rule_api
       ]},
     {cli, [],
      [t_rules_cli,
       t_actions_cli,
       t_resources_cli,
       t_resource_types_cli
      ]},
     {funcs, [],
      [t_topic_func,
       t_kv_store
      ]},
     {registry, [sequence],
      [t_add_get_remove_rule,
       t_add_get_remove_rules,
       t_create_existing_rule,
       t_update_rule,
       t_disable_rule,
       t_get_rules_for,
       t_get_rules_for_2,
       t_get_rules_with_same_event,
       t_add_get_remove_action,
       t_add_get_remove_actions,
       t_remove_actions_of,
       t_get_resources,
       t_add_get_remove_resource,
       t_resource_types
      ]},
     {runtime, [],
      [t_match_atom_and_binary
      ]},
     {rule_metrics, [],
      [t_metrics,
       t_metrics1
      ]},
     {events, [],
      [t_events
      ]},
     {bugs, [],
      [t_sqlparse_payload_as,
       t_sqlparse_nested_get
      ]},
     {multi_actions, [],
      [t_sqlselect_multi_actoins_1,
       t_sqlselect_multi_actoins_1_1,
       t_sqlselect_multi_actoins_2,
       t_sqlselect_multi_actoins_3,
       t_sqlselect_multi_actoins_3_1,
       t_sqlselect_multi_actoins_4
      ]}
    ].

%%------------------------------------------------------------------------------
%% Overall setup/teardown
%%------------------------------------------------------------------------------

init_per_suite(Config) ->
    ok = ekka_mnesia:start(),
    ok = emqx_rule_registry:mnesia(boot),
    start_apps(),
    Config.

end_per_suite(_Config) ->
    stop_apps(),
    ok.

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
    ok = emqx_rule_engine:load_providers(),
    init_events_counters(),
    ok = emqx_rule_registry:register_resource_types(
           [make_simple_resource_type(simple_resource_type)]),
    ok = emqx_rule_registry:add_action(
            #action{name = 'hook-metrics-action', app = ?APP,
                    module = ?MODULE, on_create = hook_metrics_action,
                    types=[], params_spec = #{},
                    title = #{en => <<"Hook metrics action">>},
                    description = #{en => <<"Hook metrics action">>}}),
    SQL = "SELECT * FROM \"$events/client_connected\", "
                        "\"$events/client_disconnected\", "
                        "\"$events/client_connack\", "
                        "\"$events/client_check_acl_complete\", "
                        "\"$events/session_subscribed\", "
                        "\"$events/session_unsubscribed\", "
                        "\"$events/message_acked\", "
                        "\"$events/message_delivered\", "
                        "\"$events/message_dropped\", "
                        "\"t1\"",
    {ok, Rule} = emqx_rule_engine:create_rule(
                    #{id => <<"rule:t_events">>,
                      rawsql => SQL,
                      actions => [#{id => <<"action:inspect">>, name => 'inspect', args => #{}},
                                  #{ id => <<"action:hook-metrics-action">>
                                   , name => 'hook-metrics-action'
                                   , args => #{}
                                   }],
                      description => <<"Debug rule">>}),
    ?assertMatch(#rule{id = <<"rule:t_events">>}, Rule),
    [{hook_points_rules, Rule} | Config];
init_per_testcase(Test, Config)
        when Test =:= t_sqlselect_multi_actoins_1
            ;Test =:= t_sqlselect_multi_actoins_1_1
            ;Test =:= t_sqlselect_multi_actoins_2
            ;Test =:= t_sqlselect_multi_actoins_3
            ;Test =:= t_sqlselect_multi_actoins_3_1
            ;Test =:= t_sqlselect_multi_actoins_4
        ->
    ok = emqx_rule_engine:load_providers(),
    ok = emqx_rule_registry:add_action(
            #action{name = 'crash_action', app = ?APP,
                    module = ?MODULE, on_create = crash_action,
                    types=[], params_spec = #{},
                    title = #{en => <<"Crash Action">>},
                    description = #{en => <<"This action will always fail!">>}}),
    ok = emqx_rule_registry:add_action(
            #action{name = 'failure_action', app = ?APP,
                    module = ?MODULE, on_create = failure_action,
                    types=[], params_spec = #{},
                    title = #{en => <<"Crash Action">>},
                    description = #{en => <<"This action will always fail!">>}}),
    ok = emqx_rule_registry:add_action(
            #action{name = 'plus_by_one', app = ?APP,
                    module = ?MODULE, on_create = plus_by_one_action,
                    types=[], params_spec = #{},
                    title = #{en => <<"Plus an integer by 1">>}
                    }),
    init_plus_by_one_action(),
    SQL = "SELECT * "
          "FROM \"$events/client_connected\" "
          "WHERE username = 'emqx1'",
    {ok, SubClient} = emqtt:start_link([{clientid, <<"emqx0">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(SubClient),
    {ok, _, _} = emqtt:subscribe(SubClient, <<"t2">>, 0),
    ct:sleep(100),

    {ok, ConnClient} = emqtt:start_link([{clientid, <<"c_emqx1">>}, {username, <<"emqx1">>}]),
    TriggerConnEvent = fun() ->
        {ok, _} = emqtt:connect(ConnClient)
    end,
    [{subclient, SubClient},
     {connclient, ConnClient},
     {conn_event, TriggerConnEvent},
     {connsql, SQL}
    | Config];
init_per_testcase(t_rule_api_unicode_ids, Config) ->
    ok = emqx_dashboard_admin:mnesia(boot),
    ok = emqx_mgmt_auth:mnesia(boot),
    emqx_ct_helpers:start_apps([emqx_management, emqx_dashboard]),
    Config;
init_per_testcase(_TestCase, Config) ->
    ok = emqx_rule_registry:register_resource_types(
            [make_simple_debug_resource_type()]),
    %ct:pal("============ ~p", [ets:tab2list(emqx_resource_type)]),
    Config.


end_per_testcase(t_events, Config) ->
    ets:delete(events_record_tab),
    ok = emqx_rule_registry:remove_rule(?config(hook_points_rules, Config)),
    ok = emqx_rule_registry:remove_action('hook-metrics-action');
end_per_testcase(Test, Config)
        when Test =:= t_sqlselect_multi_actoins_1
            ;Test =:= t_sqlselect_multi_actoins_1_1
            ;Test =:= t_sqlselect_multi_actoins_2
            ;Test =:= t_sqlselect_multi_actoins_3
            ;Test =:= t_sqlselect_multi_actoins_3_1
            ;Test =:= t_sqlselect_multi_actoins_4
            ->
    emqtt:stop(?config(subclient, Config)),
    emqtt:stop(?config(connclient, Config)),
    Config;
end_per_testcase(t_rule_api_unicode_ids, _Config) ->
    application:stop(emqx_dashboard),
    application:stop(emqx_management),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Test cases for rule engine
%%------------------------------------------------------------------------------

t_register_provider(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    ?assert(length(emqx_rule_registry:get_actions()) >= 2),
    ok.

t_unregister_provider(_Config) ->
    ok = emqx_rule_engine:unload_providers(),
    ?assert(length(emqx_rule_registry:get_actions()) == 0),
    ok.

t_create_rule(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    {ok, #rule{id = Id}} = emqx_rule_engine:create_rule(
            #{rawsql => <<"select * from \"t/a\"">>,
              actions => [#{name => 'inspect', args => #{arg1 => 1}}],
              description => <<"debug rule">>}),
    %ct:pal("======== emqx_rule_registry:get_rules :~p", [emqx_rule_registry:get_rules()]),
    ?assertMatch({ok,#rule{id = Id, for = [<<"t/a">>]}}, emqx_rule_registry:get_rule(Id)),
    ok = emqx_rule_engine:unload_providers(),
    emqx_rule_registry:remove_rule(Id),
    ok.

t_create_resource(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    {ok, #resource{id = ResId}} = emqx_rule_engine:create_resource(
            #{type => built_in,
              config => #{},
              description => <<"debug resource">>}),
    ?assert(true, is_binary(ResId)),
    ok = emqx_rule_engine:unload_providers(),
    emqx_rule_registry:remove_resource(ResId),
    ok.

t_create_rule_with_resource(_) ->
    emqx_rule_registry:register_resource_types([make_bad_resource_type()]),
    ok = emqx_rule_engine:load_providers(),
    {ok, #resource{id = BadResId}} = emqx_rule_engine:create_resource(
            #{type => bad_resource,
              config => #{},
              description => <<"bad resource">>}),
    {ok, #resource{id = GoodResId}} = emqx_rule_engine:create_resource(
            #{type => built_in,
              config => #{},
              description => <<"bad resource">>}),

    {ok, #rule{id = BadRuleId, enabled = Enabled}} = emqx_rule_engine:create_rule(
                #{rawsql => "select clientid as c, username as u "
                            "from \"t1\" ",
                  actions => [#{name => 'inspect',
                                args => #{<<"$resource">> => BadResId, a=>1, b=>2}}],
                  description => <<"Inspect rule">>
                }),
    ?assertEqual(false, Enabled),

    {ok, #rule{id = GoodRuleId, enabled = Enabled1}} = emqx_rule_engine:create_rule(
                #{rawsql => "select clientid as c, username as u "
                            "from \"t1\" ",
                  actions => [#{name => 'inspect',
                                args => #{<<"$resource">> => GoodResId, a=>1, b=>2}}],
                  description => <<"Inspect rule">>
                }),
    ?assertEqual(true, Enabled1),
    emqx_rule_registry:remove_rule(BadRuleId),
    emqx_rule_registry:remove_rule(GoodRuleId),
    emqx_rule_registry:remove_resource(BadResId),
    emqx_rule_registry:remove_resource(GoodResId),
    ok.

t_clean_resource_alarms(_Config) ->
    lists:foreach(fun(ResId) ->
            clean_resource_alarms(ResId)
        end, [<<"abc">>, <<"哈喽"/utf8>>]).

clean_resource_alarms(ResId) ->
    emqx_rule_registry:register_resource_types(
            [make_simple_debug_resource_type()]),
    ok = emqx_rule_engine:load_providers(),
    {ok, #resource{id = ResId}} = emqx_rule_engine:create_resource(
            #{id => ResId,
              type => built_in,
              config => #{},
              description => <<"debug resource">>}),
    Name = emqx_rule_engine:alarm_name_of_resource_down(ResId, built_in),
    _ = emqx_alarm:activate(Name, #{id => ResId, type => built_in}),
    AlarmExist = fun(#{name := AName}) -> AName == Name end,
    Len = length(lists:filter(AlarmExist, emqx_alarm:get_alarms(activated))),
    ?assertEqual(1, Len),
    emqx_rule_engine:ensure_resource_deleted(ResId),
    emqx_alarm:deactivate(Name),
    LenAfterRemove = length(lists:filter(AlarmExist, emqx_alarm:get_alarms(activated))),
    ?assertEqual(0, LenAfterRemove),
    ok = emqx_rule_engine:unload_providers(),
    ok.

%%------------------------------------------------------------------------------
%% Test cases for rule actions
%%------------------------------------------------------------------------------

t_inspect_action(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    {ok, #resource{id = ResId}} = emqx_rule_engine:create_resource(
            #{type => built_in,
              config => #{},
              description => <<"debug resource">>}),
    {ok, #rule{id = Id}} = emqx_rule_engine:create_rule(
                #{rawsql => "select clientid as c, username as u "
                            "from \"t1\" ",
                  actions => [#{name => 'inspect',
                                args => #{<<"$resource">> => ResId, a=>1, b=>2}}],
                  description => <<"Inspect rule">>
                  }),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    emqtt:publish(Client, <<"t1">>, <<"{\"id\": 1, \"name\": \"ha\"}">>, 0),
    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(Id),
    emqx_rule_registry:remove_resource(ResId),
    ok.

t_reset_metrics(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    {ok, #rule{id = Id, actions = [#action_instance{id = ActId0}]}} =
        emqx_rule_engine:create_rule(
            #{rawsql => "select clientid as c, username as u "
            "from \"t1\" ",
            actions => [#{name => 'inspect', args => #{a=>1, b=>2}}],
            description => <<"Inspect rule">>
            }),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    [ begin
          emqtt:publish(Client, <<"t1">>, <<"{\"id\": 1, \"name\": \"ha\"}">>, 0),
          timer:sleep(100)
      end
      || _ <- lists:seq(1,10)],
    ?assertMatch(#{exception := 0, failed := 0,
                   matched := 10, no_result := 0, passed := 10},
                 emqx_rule_metrics:get_rule_metrics(Id)),
    ?assertMatch(#{failed := 0, success := 10, taken := 10},
                   emqx_rule_metrics:get_action_metrics(ActId0)),
    emqx_rule_metrics:reset_metrics(Id),
    ?assertEqual(#{exception => 0,failed => 0,
                   matched => 0,no_result => 0,passed => 0,
                   speed => 0.0,speed_last5m => 0.0,speed_max => 0.0},
                 emqx_rule_metrics:get_rule_metrics(Id)),
    ?assertEqual(#{failed => 0, success => 0, taken => 0},
                   emqx_rule_metrics:get_action_metrics(ActId0)),
    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(Id),
    ok.

t_reset_metrics_fallbacks(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    ok = emqx_rule_registry:add_action(
            #action{name = 'crash_action', app = ?APP,
                    module = ?MODULE, on_create = crash_action,
                    types=[], params_spec = #{},
                    title = #{en => <<"Crash Action">>},
                    description = #{en => <<"This action will always fail!">>}}),
    {ok, #rule{id = Id, actions = [#action_instance{id = ActId0, fallbacks = [
            #action_instance{id = ActId1},
            #action_instance{id = ActId2}
        ]}]}} =
        emqx_rule_engine:create_rule(
            #{rawsql => "select clientid as c, username as u "
            "from \"t1\" ",
            actions => [#{name => 'crash_action', args => #{a=>1, b=>2}, fallbacks => [
                              #{name => 'inspect', args => #{}, fallbacks => []},
                              #{name => 'inspect', args => #{}, fallbacks => []}
                          ]}],
            description => <<"Inspect rule">>
            }),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    [ begin
          emqtt:publish(Client, <<"t1">>, <<"{\"id\": 1, \"name\": \"ha\"}">>, 0),
          timer:sleep(100)
      end
      || _ <- lists:seq(1,10)],
    ?assertMatch(#{exception := 0, failed := 0,
                   matched := 10, no_result := 0, passed := 10},
                 emqx_rule_metrics:get_rule_metrics(Id)),
    [?assertMatch(#{failed := 10, success := 0, taken := 10},
                   emqx_rule_metrics:get_action_metrics(AId)) || AId <- [ActId0]],
    [?assertMatch(#{failed := 0, success := 10, taken := 10},
                   emqx_rule_metrics:get_action_metrics(AId)) || AId <- [ ActId1, ActId2]],
    emqx_rule_metrics:reset_metrics(Id),
    ?assertEqual(#{exception => 0,failed => 0,
                   matched => 0,no_result => 0,passed => 0,
                   speed => 0.0,speed_last5m => 0.0,speed_max => 0.0},
                 emqx_rule_metrics:get_rule_metrics(Id)),
    [?assertEqual(#{failed => 0, success => 0, taken => 0},
                   emqx_rule_metrics:get_action_metrics(AId)) || AId <- [ActId0, ActId1, ActId2]],
    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(Id),
    ok = emqx_rule_registry:remove_action('crash_action'),
    ok.

t_republish_action(_Config) ->
    TargetQoSList = [-1, 0, 1, 2, <<"${qos}">>],
    TargetRetainList = [true, false, <<"${flags.retain}">>],
    [[republish_action_test(TargetQoS, TargetRetain) || TargetRetain <- TargetRetainList]
        || TargetQoS <- TargetQoSList],
    ok.

republish_action_test(TargetQoS, TargetRetain) ->
    {QoSReceivedMetricsName, PubQoS} =
        case TargetQoS of
            <<"${qos}">> -> {'messages.qos0.received', 0};
            -1 -> {'messages.qos0.received', 0};
            0 -> {'messages.qos0.received', 0};
            1 -> {'messages.qos1.received', 1};
            2 -> {'messages.qos2.received', 2}
        end,
    QosReceived = emqx_metrics:val(QoSReceivedMetricsName),
    Received = emqx_metrics:val('messages.received'),
    ok = emqx_rule_engine:load_providers(),
    {ok, #rule{id = Id, for = [<<"t1">>]}} =
        emqx_rule_engine:create_rule(
                    #{rawsql => <<"select * from \"t1\"">>,
                      actions => [#{name => 'republish',
                                    args => #{<<"target_topic">> => <<"t2">>,
                                              <<"target_qos">> => TargetQoS,
                                              <<"target_retain">> => TargetRetain,
                                              <<"payload_tmpl">> => <<"${payload}">>}}],
                      description => <<"builtin-republish-rule">>}),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),

    Msg = <<"{\"id\": 1, \"name\": \"ha\"}">>,
    emqtt:publish(Client, <<"t1">>, Msg, PubQoS),
    receive {publish, #{topic := <<"t2">>, payload := Payload}} ->
        ?assertEqual(Msg, Payload)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,
    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(Id),
    ?assertEqual(2, emqx_metrics:val(QoSReceivedMetricsName) - QosReceived),
    ?assertEqual(2, emqx_metrics:val('messages.received') - Received),
    ok.

%%------------------------------------------------------------------------------
%% Test cases for rule engine api
%%------------------------------------------------------------------------------

t_crud_rule_api(_Config) ->
    {ok, #{code := 0, data := Rule}} =
        emqx_rule_engine_api:create_rule(#{},
                [{<<"name">>, <<"debug-rule">>},
                 {<<"rawsql">>, <<"select * from \"t/a\"">>},
                 {<<"actions">>, [[{<<"name">>,<<"inspect">>},
                                   {<<"params">>,[{<<"arg1">>,1}]}]]},
                 {<<"description">>, <<"debug rule">>}]),
    RuleID = maps:get(id, Rule),
    {ok, #{code := 400, message := <<"Already Exists">>}} =
        emqx_rule_engine_api:create_rule(#{},
                [{<<"name">>, <<"debug-rule">>},
                 {<<"id">>, RuleID},
                 {<<"rawsql">>, <<"select * from \"t/a\"">>},
                 {<<"actions">>, [[{<<"name">>,<<"inspect">>},
                                   {<<"params">>,[{<<"arg1">>,1}]}]]},
                 {<<"description">>, <<"debug rule">>}]),
    %ct:pal("RCreated : ~p", [Rule]),

    {ok, #{code := 0, data := Rules}} = emqx_rule_engine_api:list_rules(#{}, []),
    %ct:pal("RList : ~p", [Rules]),
    ?assert(length(Rules) > 0),

    {ok, #{code := 0, data := Rule1}} = emqx_rule_engine_api:show_rule(#{id => RuleID}, []),
    %ct:pal("RShow : ~p", [Rule1]),
    ?assertEqual(Rule, Rule1),

    {ok, #{code := 0, data := Rule2}} = emqx_rule_engine_api:update_rule(#{id => RuleID},
                [{<<"rawsql">>, <<"select * from \"t/b\"">>}]),

    {ok, #{code := 0, data := Rule3}} = emqx_rule_engine_api:show_rule(#{id => RuleID}, []),
    %ct:pal("RShow : ~p", [Rule1]),
    ?assertEqual(Rule3, Rule2),
    ?assertEqual(<<"select * from \"t/b\"">>, maps:get(rawsql, Rule3)),

    {ok, #{code := 0, data := Rule4}} = emqx_rule_engine_api:update_rule(#{id => RuleID},
                [{<<"actions">>,
                    [[
                        {<<"name">>,<<"republish">>},
                        {<<"params">>,[
                            {<<"arg1">>,1},
                            {<<"target_topic">>, <<"t2">>},
                            {<<"target_qos">>, 0},
                            {<<"payload_tmpl">>, <<"${payload}">>}
                        ]}
                    ]]
                 }]),

    {ok, #{code := 0, data := Rule5}} = emqx_rule_engine_api:show_rule(#{id => RuleID}, []),
    %ct:pal("RShow : ~p", [Rule1]),
    ?assertEqual(Rule5, Rule4),
    ?assertMatch([#{name := republish }], maps:get(actions, Rule5)),

    ?assertMatch({ok, #{code := 0}}, emqx_rule_engine_api:delete_rule(#{id => RuleID}, [])),

    NotFound = emqx_rule_engine_api:show_rule(#{id => RuleID}, []),
    %ct:pal("Show After Deleted: ~p", [NotFound]),
    ?assertMatch({ok, #{code := 404, message := _Message}}, NotFound),
    ok.

-define(PRED(Elem), fun(Elem) -> true; (_) -> false end).

t_rule_api_unicode_ids(_Config) ->
    UData =
        fun(Description) ->
                #{<<"name">> => <<"debug-rule">>,
                  <<"rawsql">> => <<"select * from \"t/a\"">>,
                  <<"actions">> => [#{<<"name">> => <<"do_nothing">>,
                                      <<"params">> => []}
                                   ],
                  <<"description">> => Description}
        end,
    CData = fun(Id, Description) ->  maps:put(<<"id">>, Id, UData(Description)) end,

    CDes = <<"Creating rules description">>,
    UDes = <<"Updating rules description">>,

    %% create rule
    CFun = fun(Id) -> {ok, Return} = request_api(post, api_path(["rules"]), [], auth_header_(), CData(Id, CDes)), Return end,
    %% update rule
    UFun = fun(Id) -> {ok, Return} = request_api(put, api_path(["rules", cow_uri:urlencode(Id)]), [], auth_header_(), UData(UDes)), Return end,
    %% show rule
    SFun = fun(Id) -> {ok, Return} = request_api(get, api_path(["rules", cow_uri:urlencode(Id)]), [], auth_header_()), Return end,
    %% delete rule
    DFun = fun(Id) -> {ok, Return} = request_api(delete, api_path(["rules", cow_uri:urlencode(Id)]), [], auth_header_()), Return end,

    Ids = [unicode:characters_to_binary([Char]) || Char <- lists:seq(0, 1000) -- [46]] ++ [<<"%2e">>],

    Ress = [begin
                {?assertMatch(#{<<"code">> := 0, <<"data">> := #{<<"description">> := CDes}}, decode_to_map(CFun(Id))),
                 ?assertMatch(#{<<"code">> := 0}, decode_to_map(UFun(Id))),
                 ?assertMatch(#{<<"code">> := 0, <<"data">> := #{<<"description">> := UDes}}, decode_to_map(SFun(Id))),
                 ?assertMatch(#{<<"code">> := 0}, decode_to_map(DFun(Id)))}
           end || Id <- Ids],

    ?assertEqual(true, lists:all(?PRED(true), [?PRED({ok, ok, ok, ok})(Res) || Res <- Ress ])).

decode_to_map(ResponseBody) ->
    jiffy:decode(list_to_binary(ResponseBody), [return_maps]).

t_list_rule_api(_Config) ->
    AddIds =
        lists:map(fun(Seq) ->
            SeqBin = integer_to_binary(Seq),
            {ok, #{code := 0, data := #{id := Id}}} =
                emqx_rule_engine_api:create_rule(#{},
                    [{<<"name">>, <<"debug-rule-", SeqBin/binary>>},
                        {<<"rawsql">>, <<"select * from \"t/a/", SeqBin/binary, "\"">>},
                        {<<"actions">>, [[{<<"name">>,<<"inspect">>}, {<<"params">>,[{<<"arg1">>,1}]}]]},
                        {<<"description">>, <<"debug rule desc ", SeqBin/binary>>}]),
            Id
                  end, lists:seq(1, 20)),

    {ok, #{code := 0, data := Rules11}} = emqx_rule_engine_api:list_rules(#{},
        [{<<"_limit">>,<<"10">>}, {<<"_page">>, <<"1">>}, {<<"enable_paging">>, true}]),
    ?assertEqual(10, length(Rules11)),
    {ok, #{code := 0, data := Rules12}} = emqx_rule_engine_api:list_rules(#{},
        [{<<"_limit">>,<<"10">>}, {<<"_page">>, <<"2">>}, {<<"enable_paging">>, true}]),
    ?assertEqual(10, length(Rules12)),
    Rules1 = Rules11 ++ Rules12,

    [RuleID | _] = AddIds,
    {ok, #{code := 0}} = emqx_rule_engine_api:update_rule(#{id => RuleID},
        [{<<"enabled">>, false}]),
    Params1 = [{<<"enabled">>,<<"true">>}, {<<"enable_paging">>, true}],
    {ok, #{code := 0, data := Rules2}} = emqx_rule_engine_api:list_rules(#{}, Params1),
    ?assert(lists:all(fun(#{id := ID}) -> ID =/= RuleID end, Rules2)),

    Params2 = [{<<"for">>, RuleID}, {<<"enable_paging">>, true}],
    {ok, #{code := 0, data := Rules3}} = emqx_rule_engine_api:list_rules(#{}, Params2),
    ?assert(lists:all(fun(#{id := ID}) -> ID =:= RuleID end, Rules3)),

    Params3 = [{<<"_like_id">>,<<"rule:">>}, {<<"enable_paging">>, true}],
    {ok, #{code := 0, data := Rules4}} = emqx_rule_engine_api:list_rules(#{}, Params3),
    ?assertEqual(length(Rules1), length(Rules4)),

    Params4 = [{<<"_like_for">>,<<"t/a/">>}, {<<"enable_paging">>, true}],
    {ok, #{code := 0, data := Rules5}} = emqx_rule_engine_api:list_rules(#{}, Params4),
    ?assertEqual(length(Rules1), length(Rules5)),
    {ok, #{code := 0}} = emqx_rule_engine_api:update_rule(#{id => RuleID},
        [{<<"rawsql">>, <<"select * from \"t/b/c\"">>}]),
    {ok, #{code := 0, data := Rules6}} = emqx_rule_engine_api:list_rules(#{}, Params4),
    ?assert(lists:all(fun(#{id := ID}) -> ID =/= RuleID end, Rules6)),
    ?assertEqual(1, length(Rules1) - length(Rules6)),

    Params5 = [{<<"_match_for">>,<<"t/+/+">>}, {<<"enable_paging">>, true}],
    {ok, #{code := 0, data := Rules7}} = emqx_rule_engine_api:list_rules(#{}, Params5),
    ?assertEqual(length(Rules1), length(Rules7)),
    {ok, #{code := 0}} = emqx_rule_engine_api:update_rule(#{id => RuleID},
        [{<<"rawsql">>, <<"select * from \"t1/b\"">>}]),
    {ok, #{code := 0, data := Rules8}} = emqx_rule_engine_api:list_rules(#{}, Params5),
    ?assert(lists:all(fun(#{id := ID}) -> ID =/= RuleID end, Rules8)),
    ?assertEqual(1, length(Rules1) - length(Rules8)),

    Params6 = [{<<"_like_description">>,<<"rule">>}, {<<"enable_paging">>, true}],
    {ok, #{code := 0, data := Rules9}} = emqx_rule_engine_api:list_rules(#{}, Params6),
    ?assertEqual(length(Rules1), length(Rules9)),
    {ok, #{code := 0}} = emqx_rule_engine_api:update_rule(#{id => RuleID},
        [{<<"description">>, <<"not me">>}]),
    {ok, #{code := 0, data := Rules10}} = emqx_rule_engine_api:list_rules(#{}, Params6),
    ?assert(lists:all(fun(#{id := ID}) -> ID =/= RuleID end, Rules10)),
    ?assertEqual(1, length(Rules1) - length(Rules10)),

    lists:foreach(fun(ID) ->
        ?assertMatch({ok, #{code := 0}}, emqx_rule_engine_api:delete_rule(#{id => ID}, []))
                  end, AddIds),
    ok.

t_list_actions_api(_Config) ->
    {ok, #{code := 0, data := Actions}} = emqx_rule_engine_api:list_actions(#{}, []),
    %ct:pal("RList : ~p", [Actions]),
    ?assert(length(Actions) > 0),
    ok.

t_show_action_api(_Config) ->
    {ok, #{code := 0, data := Actions}} =
        emqx_rule_engine_api:show_action(#{name => 'inspect'}, []),
    ?assertEqual('inspect', maps:get(name, Actions)),
    ok.

t_crud_resources_api(_Config) ->
    ResParams = [
        {<<"name">>, <<"Simple Resource">>},
        {<<"type">>, <<"built_in">>},
        {<<"config">>, [{<<"a">>, 1}]},
        {<<"description">>, <<"Simple Resource">>}
    ],
    {ok, #{code := 0, data := Resources1}} =
        emqx_rule_engine_api:create_resource(#{}, ResParams),
    ResId = maps:get(id, Resources1),
    %% create again using given resource id returns error
    {ok, #{code := 400, message := <<"Already Exists">>}} =
        emqx_rule_engine_api:create_resource(#{}, [{<<"id">>, ResId} | ResParams]),
    {ok, #{code := 0, data := Resources}} = emqx_rule_engine_api:list_resources(#{}, []),
    ?assert(length(Resources) > 0),
    {ok, #{code := 0, data := Resources2}} = emqx_rule_engine_api:show_resource(#{id => ResId}, []),
    ?assertEqual(ResId, maps:get(id, Resources2)),
    %
    {ok, #{code := 0}} = emqx_rule_engine_api:update_resource(#{id => ResId},
                                                              [{<<"config">>, [{<<"a">>, 2}]},
                                                               {<<"description">>, <<"2">>}]),
    {ok, #{code := 0, data := Resources3}} = emqx_rule_engine_api:show_resource(#{id => ResId}, []),
    ?assertEqual(ResId, maps:get(id, Resources3)),
    ?assertEqual(#{<<"a">> => 2}, maps:get(config, Resources3)),
    ?assertEqual(<<"2">>, maps:get(description, Resources3)),
    %
    {ok, #{code := 0}} = emqx_rule_engine_api:update_resource(#{id => ResId},
                                                              [{<<"config">>, [{<<"a">>, 3}]}]),
    {ok, #{code := 0, data := Resources4}} = emqx_rule_engine_api:show_resource(#{id => ResId}, []),
    ?assertEqual(ResId, maps:get(id, Resources4)),
    ?assertEqual(#{<<"a">> => 3}, maps:get(config, Resources4)),
    ?assertEqual(<<"2">>, maps:get(description, Resources4)),
    % Only config
    {ok, #{code := 0}} = emqx_rule_engine_api:update_resource(#{id => ResId},
                                                              [{<<"config">>, [{<<"a">>, 1},
                                                                               {<<"b">>, 2},
                                                                               {<<"c">>, 3}]}]),
    {ok, #{code := 0, data := Resources5}} = emqx_rule_engine_api:show_resource(#{id => ResId}, []),
    ?assertEqual(ResId, maps:get(id, Resources5)),
    ?assertEqual(#{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3}, maps:get(config, Resources5)),
    ?assertEqual(<<"2">>, maps:get(description, Resources5)),
    % Only description
    {ok, #{code := 0}} = emqx_rule_engine_api:update_resource(#{id => ResId},
                                                              [{<<"description">>, <<"new5">>}]),
    {ok, #{code := 0, data := Resources6}} = emqx_rule_engine_api:show_resource(#{id => ResId}, []),
    ?assertEqual(ResId, maps:get(id, Resources6)),
    ?assertEqual(#{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3}, maps:get(config, Resources6)),
    ?assertEqual(<<"new5">>, maps:get(description, Resources6)),
    % None
    {ok, #{code := 0}} = emqx_rule_engine_api:update_resource(#{id => ResId}, []),
    {ok, #{code := 0, data := Resources7}} = emqx_rule_engine_api:show_resource(#{id => ResId}, []),
    ?assertEqual(ResId, maps:get(id, Resources7)),
    ?assertEqual(#{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3}, maps:get(config, Resources7)),
    ?assertEqual(<<"new5">>, maps:get(description, Resources7)),
    %
    ?assertMatch({ok, #{code := 0}}, emqx_rule_engine_api:delete_resource(#{id => ResId},#{})),
    ?assertMatch({ok, #{code := 404}}, emqx_rule_engine_api:show_resource(#{id => ResId}, [])),
    ok.

t_list_resource_types_api(_Config) ->
    {ok, #{code := 0, data := ResourceTypes}} = emqx_rule_engine_api:list_resource_types(#{}, []),
    ?assert(length(ResourceTypes) > 0),
    ok.

t_show_resource_type_api(_Config) ->
    {ok, #{code := 0, data := RShow}} =
        emqx_rule_engine_api:show_resource_type(#{name => 'built_in'}, []),
    %ct:pal("RShow : ~p", [RShow]),
    ?assertEqual(built_in, maps:get(name, RShow)),
    ok.

%%------------------------------------------------------------------------------
%% Test cases for rule engine cli
%%------------------------------------------------------------------------------

t_rules_cli(_Config) ->
    mock_print(),
    RCreate = emqx_rule_engine_cli:rules(["create",
                                          "select * from \"t1\" where topic='t1'",
                                          "[{\"name\":\"inspect\", \"params\": {\"arg1\": 1}}]",
                                          "-d", "Debug Rule"]),
    %ct:pal("Result : ~p", [RCreate]),
    ?assertMatch({match, _}, re:run(RCreate, "created")),

    RuleId = re:replace(
               re:replace( RCreate
                         , "Rule\s"
                         , ""
                         , [{return, list}]
                         ),
               "\screated\n",
               "",
               [{return, list}]),

    RList = emqx_rule_engine_cli:rules(["list"]),
    ?assertMatch({match, _}, re:run(RList, RuleId)),
    %ct:pal("RList : ~p", [RList]),
    %ct:pal("table action params: ~p", [ets:tab2list(emqx_action_instance_params)]),

    RShow = emqx_rule_engine_cli:rules(["show", RuleId]),
    ?assertMatch({match, _}, re:run(RShow, RuleId)),
    %ct:pal("RShow : ~p", [RShow]),

    RUpdate = emqx_rule_engine_cli:rules(["update",
                                          RuleId,
                                          "-s", "select * from \"t2\""]),
    ?assertMatch({match, _}, re:run(RUpdate, "updated")),

    RDelete = emqx_rule_engine_cli:rules(["delete", RuleId]),
    ?assertEqual("ok\n", RDelete),
    %ct:pal("RDelete : ~p", [RDelete]),
    %ct:pal("table action params after deleted: ~p", [ets:tab2list(emqx_action_instance_params)]),

    RShow2 = emqx_rule_engine_cli:rules(["show", RuleId]),
    ?assertMatch({match, _}, re:run(RShow2, "Cannot found")),
    %ct:pal("RShow2 : ~p", [RShow2]),
    unmock_print(),
    ok.

t_actions_cli(_Config) ->
    mock_print(),
    RList = emqx_rule_engine_cli:actions(["list"]),
    ?assertMatch({match, _}, re:run(RList, "inspect")),
    %ct:pal("RList : ~p", [RList]),

    RShow = emqx_rule_engine_cli:actions(["show", "inspect"]),
    ?assertMatch({match, _}, re:run(RShow, "inspect")),
    %ct:pal("RShow : ~p", [RShow]),
    unmock_print(),
    ok.

t_resources_cli(_Config) ->
    mock_print(),
    RCreate = emqx_rule_engine_cli:resources(
                ["create", "built_in", "{\"a\" : 1}", "-d", "test resource"]),
    ResId = re:replace(
              re:replace( RCreate
                        , "Resource\s"
                        , ""
                        , [{return, list}]
                        ),
              "\screated\n",
              "",
              [{return, list}]),

    RList = emqx_rule_engine_cli:resources(["list"]),
    ?assertMatch({match, _}, re:run(RList, "test resource")),
    %ct:pal("RList : ~p", [RList]),

    RListT = emqx_rule_engine_cli:resources(["list", "-t", "built_in"]),
    ?assertMatch({match, _}, re:run(RListT, "test resource")),
    %ct:pal("RListT : ~p", [RListT]),

    RShow = emqx_rule_engine_cli:resources(["show", ResId]),
    ?assertMatch({match, _}, re:run(RShow, "test resource")),
    %ct:pal("RShow : ~p", [RShow]),

    RDelete = emqx_rule_engine_cli:resources(["delete", ResId]),
    ?assertEqual("ok\n", RDelete),

    RShow2 = emqx_rule_engine_cli:resources(["show", ResId]),
    ?assertMatch({match, _}, re:run(RShow2, "Cannot found")),
    %ct:pal("RShow2 : ~p", [RShow2]),
    unmock_print(),
    ok.

t_resource_types_cli(_Config) ->
    mock_print(),
    RList = emqx_rule_engine_cli:resource_types(["list"]),
    ?assertMatch({match, _}, re:run(RList, "built_in")),
    %ct:pal("RList : ~p", [RList]),

    RShow = emqx_rule_engine_cli:resource_types(["show", "inspect"]),
    ?assertMatch({match, _}, re:run(RShow, "inspect")),
    %ct:pal("RShow : ~p", [RShow]),
    unmock_print(),
    ok.

%%------------------------------------------------------------------------------
%% Test cases for rule funcs
%%------------------------------------------------------------------------------

t_topic_func(_Config) ->
    %%TODO:
    ok.

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
    mock_print(),
    RuleId0 = <<"rule-debug-0">>,
    ok = emqx_rule_registry:add_rule(make_simple_rule(RuleId0)),
    ?assertMatch({ok, #rule{id = RuleId0}}, emqx_rule_registry:get_rule(RuleId0)),
    ok = emqx_rule_registry:remove_rule(RuleId0),
    ?assertEqual(not_found, emqx_rule_registry:get_rule(RuleId0)),

    RuleId1 = <<"rule-debug-1">>,
    Rule1 = make_simple_rule(RuleId1),
    ok = emqx_rule_registry:add_rule(Rule1),
    ?assertMatch({ok, #rule{id = RuleId1}}, emqx_rule_registry:get_rule(RuleId1)),
    ok = emqx_rule_registry:remove_rule(Rule1),
    ?assertEqual(not_found, emqx_rule_registry:get_rule(RuleId1)),
    unmock_print(),
    ok.

t_add_get_remove_rules(_Config) ->
    emqx_rule_registry:remove_rules(emqx_rule_registry:get_rules()),
    ok = emqx_rule_registry:add_rules(
            [make_simple_rule(<<"rule-debug-1">>),
             make_simple_rule(<<"rule-debug-2">>)]),
    ?assertEqual(2, length(emqx_rule_registry:get_rules())),
    ok = emqx_rule_registry:remove_rules([<<"rule-debug-1">>, <<"rule-debug-2">>]),
    ?assertEqual([], emqx_rule_registry:get_rules()),

    Rule3 = make_simple_rule(<<"rule-debug-3">>),
    Rule4 = make_simple_rule(<<"rule-debug-4">>),
    ok = emqx_rule_registry:add_rules([Rule3, Rule4]),
    ?assertEqual(2, length(emqx_rule_registry:get_rules())),
    ok = emqx_rule_registry:remove_rules([Rule3, Rule4]),
    ?assertEqual([], emqx_rule_registry:get_rules()),
    ok.

t_create_existing_rule(_Config) ->
    %% create a rule using given rule id
    {ok, _} = emqx_rule_engine:create_rule(
                    #{id => <<"an_existing_rule">>,
                      rawsql => <<"select * from \"t/#\"">>,
                      actions => [
                          #{name => 'inspect', args => #{}}
                      ]
                     }),
    {ok, #rule{rawsql = SQL}} = emqx_rule_registry:get_rule(<<"an_existing_rule">>),
    ?assertEqual(<<"select * from \"t/#\"">>, SQL),

    ok = emqx_rule_engine:delete_rule(<<"an_existing_rule">>),
    ?assertEqual(not_found, emqx_rule_registry:get_rule(<<"an_existing_rule">>)),
    ok.

t_update_rule(_Config) ->
    {ok, #rule{actions = [#action_instance{id = ActInsId0}]}} = emqx_rule_engine:create_rule(
                    #{id => <<"an_existing_rule">>,
                      rawsql => <<"select * from \"t/#\"">>,
                      actions => [
                          #{name => 'inspect', args => #{}}
                      ]
                     }),
    ?assertMatch({ok, #action_instance_params{apply = _}},
                 emqx_rule_registry:get_action_instance_params(ActInsId0)),
    %% update the rule and verify the old action instances has been cleared
    %% and the new action instances has been created.
    emqx_rule_engine:update_rule(#{ id => <<"an_existing_rule">>,
                                    actions => [
                                        #{name => 'do_nothing', args => #{}}
                                    ]}),

    {ok, #rule{actions = [#action_instance{id = ActInsId1}]}}
        = emqx_rule_registry:get_rule(<<"an_existing_rule">>),

    ?assertMatch(not_found,
                 emqx_rule_registry:get_action_instance_params(ActInsId0)),

    ?assertMatch({ok, #action_instance_params{apply = _}},
                 emqx_rule_registry:get_action_instance_params(ActInsId1)),

    ok = emqx_rule_engine:delete_rule(<<"an_existing_rule">>),
    ?assertEqual(not_found, emqx_rule_registry:get_rule(<<"an_existing_rule">>)),
    ok.

t_disable_rule(_Config) ->
    ets:new(simple_action_2, [named_table, set, public]),
    ets:insert(simple_action_2, {created, 0}),
    ets:insert(simple_action_2, {destroyed, 0}),
    Now = erlang:timestamp(),
    emqx_rule_registry:add_action(
        #action{name = 'simple_action_2', app = ?APP,
                module = ?MODULE,
                on_create = simple_action_2_create,
                on_destroy = simple_action_2_destroy,
                types=[], params_spec = #{},
                title = #{en => <<"Simple Action">>},
                description = #{en => <<"Simple Action">>}}),
    {ok, #rule{actions = [#action_instance{}]}} = emqx_rule_engine:create_rule(
        #{id => <<"simple_rule_2">>,
          rawsql => <<"select * from \"t/#\"">>,
          actions => [#{name => 'simple_action_2', args => #{}}]
        }),
    [{_, CAt}] = ets:lookup(simple_action_2, created),
    ?assert(CAt > Now),
    [{_, DAt}] = ets:lookup(simple_action_2, destroyed),
    ?assert(DAt < Now),

    %% disable the rule and verify the old action instances has been cleared
    Now2 = erlang:timestamp(),
    emqx_rule_engine:update_rule(#{ id => <<"simple_rule_2">>,
                                    enabled => false}),
    [{_, CAt2}] = ets:lookup(simple_action_2, created),
    ?assert(CAt2 < Now2),
    [{_, DAt2}] = ets:lookup(simple_action_2, destroyed),
    ?assert(DAt2 > Now2),

    %% enable the rule again and verify the action instances has been created
    Now3 = erlang:timestamp(),
    emqx_rule_engine:update_rule(#{ id => <<"simple_rule_2">>,
                                    enabled => true}),
    [{_, CAt3}] = ets:lookup(simple_action_2, created),
    ?assert(CAt3 > Now3),
    [{_, DAt3}] = ets:lookup(simple_action_2, destroyed),
    ?assert(DAt3 < Now3),
    ok = emqx_rule_engine:delete_rule(<<"simple_rule_2">>).

t_get_rules_for(_Config) ->
    Len0 = length(emqx_rule_registry:get_rules_for(<<"simple/topic">>)),
    ok = emqx_rule_registry:add_rules(
            [make_simple_rule(<<"rule-debug-1">>),
             make_simple_rule(<<"rule-debug-2">>)]),
    ?assertEqual(Len0+2, length(emqx_rule_registry:get_rules_for(<<"simple/topic">>))),
    ok = emqx_rule_registry:remove_rules([<<"rule-debug-1">>, <<"rule-debug-2">>]),
    ok.

t_get_rules_ordered_by_ts(_Config) ->
    Now = fun() -> erlang:system_time(nanosecond) end,
    ok = emqx_rule_registry:add_rules(
            [make_simple_rule_with_ts(<<"rule-debug-0">>, Now()),
             make_simple_rule_with_ts(<<"rule-debug-1">>, Now()),
             make_simple_rule_with_ts(<<"rule-debug-2">>, Now())
             ]),
    ?assertMatch([
        #rule{id = <<"rule-debug-0">>},
        #rule{id = <<"rule-debug-1">>},
        #rule{id = <<"rule-debug-2">>}
    ], emqx_rule_registry:get_rules_ordered_by_ts()).

t_get_rules_for_2(_Config) ->
    Len0 = length(emqx_rule_registry:get_rules_for(<<"simple/1">>)),
    ok = emqx_rule_registry:add_rules(
            [make_simple_rule(
               <<"rule-debug-1">>,
               <<"select * from \"simple/#\"">>,
               [<<"simple/#">>]),
             make_simple_rule(
               <<"rule-debug-2">>,
               <<"select * from \"simple/+\"">>,
               [<<"simple/+">>]),
             make_simple_rule(
               <<"rule-debug-3">>,
               <<"select * from \"simple/+/1\"">>,
               [<<"simple/+/1">>]),
             make_simple_rule(
               <<"rule-debug-4">>,
               <<"select * from \"simple/1\"">>,
               [<<"simple/1">>]),
             make_simple_rule(
               <<"rule-debug-5">>,
               <<"select * from \"simple/2,simple/+,simple/3\"">>,
               [<<"simple/2">>,<<"simple/+">>, <<"simple/3">>]),
             make_simple_rule(
               <<"rule-debug-6">>,
               <<"select * from \"simple/2,simple/3,simple/4\"">>,
               [<<"simple/2">>,<<"simple/3">>, <<"simple/4">>])
             ]),
    ?assertEqual(Len0+4, length(emqx_rule_registry:get_rules_for(<<"simple/1">>))),
    ok = emqx_rule_registry:remove_rules(
           [ <<"rule-debug-1">>
           , <<"rule-debug-2">>
           , <<"rule-debug-3">>
           , <<"rule-debug-4">>
           , <<"rule-debug-5">>
           , <<"rule-debug-6">>
           ]),
    ok.

t_get_rules_with_same_event(_Config) ->
    PubT = <<"simple/1">>,
    PubN = length(emqx_rule_registry:get_rules_with_same_event(PubT)),
    ?assertEqual([], emqx_rule_registry:get_rules_with_same_event(<<"$events/client_connected">>)),
    ?assertEqual(
       [],
       emqx_rule_registry:get_rules_with_same_event(<<"$events/client_disconnected">>)),
    ?assertEqual(
       [],
       emqx_rule_registry:get_rules_with_same_event(<<"$events/session_subscribed">>)),
    ?assertEqual(
       [],
       emqx_rule_registry:get_rules_with_same_event(<<"$events/session_unsubscribed">>)),
    ?assertEqual([], emqx_rule_registry:get_rules_with_same_event(<<"$events/message_delivered">>)),
    ?assertEqual([], emqx_rule_registry:get_rules_with_same_event(<<"$events/message_acked">>)),
    ?assertEqual([], emqx_rule_registry:get_rules_with_same_event(<<"$events/message_dropped">>)),
    ok = emqx_rule_registry:add_rules(
            [make_simple_rule(<<"r1">>, <<"select * from \"simple/#\"">>, [<<"simple/#">>]),
             make_simple_rule(<<"r2">>, <<"select * from \"abc/+\"">>, [<<"abc/+">>]),
             make_simple_rule(
               <<"r3">>,
               <<"select * from \"$events/client_connected\"">>,
               [<<"$events/client_connected">>]),
             make_simple_rule(
               <<"r4">>,
               <<"select * from \"$events/client_disconnected\"">>,
               [<<"$events/client_disconnected">>]),
             make_simple_rule(
               <<"r5">>,
               <<"select * from \"$events/session_subscribed\"">>,
               [<<"$events/session_subscribed">>]),
             make_simple_rule(
               <<"r6">>,
               <<"select * from \"$events/session_unsubscribed\"">>,
               [<<"$events/session_unsubscribed">>]),
             make_simple_rule(
               <<"r7">>,
               <<"select * from \"$events/message_delivered\"">>,
               [<<"$events/message_delivered">>]),
             make_simple_rule(
               <<"r8">>,
               <<"select * from \"$events/message_acked\"">>,
               [<<"$events/message_acked">>]),
             make_simple_rule(
               <<"r9">>,
               <<"select * from \"$events/message_dropped\"">>,
               [<<"$events/message_dropped">>]),
             make_simple_rule(
               <<"r10">>,
               <<"select * from \"t/1, $events/session_subscribed,"
                 " $events/client_connected\"">>,
               [<<"t/1">>, <<"$events/session_subscribed">>, <<"$events/client_connected">>])
             ]),
    ?assertEqual(PubN + 3, length(emqx_rule_registry:get_rules_with_same_event(PubT))),
    ?assertEqual(
       2,
       length(emqx_rule_registry:get_rules_with_same_event(<<"$events/client_connected">>))),
    ?assertEqual(
       1,
       length(emqx_rule_registry:get_rules_with_same_event(<<"$events/client_disconnected">>))),
    ?assertEqual(
       2,
       length(emqx_rule_registry:get_rules_with_same_event(<<"$events/session_subscribed">>))),
    ?assertEqual(
       1,
       length(emqx_rule_registry:get_rules_with_same_event(<<"$events/session_unsubscribed">>))),
    ?assertEqual(
       1,
       length(emqx_rule_registry:get_rules_with_same_event(<<"$events/message_delivered">>))),
    ?assertEqual(
       1,
       length(emqx_rule_registry:get_rules_with_same_event(<<"$events/message_acked">>))),
    ?assertEqual(
       1,
       length(emqx_rule_registry:get_rules_with_same_event(<<"$events/message_dropped">>))),
    ok = emqx_rule_registry:remove_rules(
           [<<"r1">>, <<"r2">>,<<"r3">>,
            <<"r4">>,<<"r5">>, <<"r6">>,
            <<"r7">>, <<"r8">>, <<"r9">>, <<"r10">>]),
    ok.

t_add_get_remove_action(_Config) ->
    ActionName0 = 'action-debug-0',
    Action0 = make_simple_action(ActionName0),
    ok = emqx_rule_registry:add_action(Action0),
    ?assertMatch({ok, #action{name = ActionName0}}, emqx_rule_registry:find_action(ActionName0)),
    ok = emqx_rule_registry:remove_action(ActionName0),
    ?assertMatch(not_found, emqx_rule_registry:find_action(ActionName0)),

    ok = emqx_rule_registry:add_action(Action0),
    ?assertMatch({ok, #action{name = ActionName0}}, emqx_rule_registry:find_action(ActionName0)),
    ok = emqx_rule_registry:remove_action(Action0),
    ?assertMatch(not_found, emqx_rule_registry:find_action(ActionName0)),
    ok.

t_add_get_remove_actions(_Config) ->
    InitActionLen = length(emqx_rule_registry:get_actions()),
    ActionName1 = 'action-debug-1',
    ActionName2 = 'action-debug-2',
    Action1 = make_simple_action(ActionName1),
    Action2 = make_simple_action(ActionName2),
    ok = emqx_rule_registry:add_actions([Action1, Action2]),
    ?assertMatch(2, length(emqx_rule_registry:get_actions()) - InitActionLen),
    ok = emqx_rule_registry:remove_actions([ActionName1, ActionName2]),
    ?assertMatch(InitActionLen, length(emqx_rule_registry:get_actions())),

    ok = emqx_rule_registry:add_actions([Action1, Action2]),
    ?assertMatch(2, length(emqx_rule_registry:get_actions()) - InitActionLen),
    ok = emqx_rule_registry:remove_actions([Action1, Action2]),
    ?assertMatch(InitActionLen, length(emqx_rule_registry:get_actions())),
     ok.

t_remove_actions_of(_Config) ->
    ok = emqx_rule_registry:add_actions([make_simple_action('action-debug-1'),
                                         make_simple_action('action-debug-2')]),
    Len1 = length(emqx_rule_registry:get_actions()),
    ?assert(Len1 >= 2),
    ok = emqx_rule_registry:remove_actions_of(?APP),
    ?assert((Len1 - length(emqx_rule_registry:get_actions())) >= 2),
    ok.

t_add_get_remove_resource(_Config) ->
    ResId = <<"resource-debug">>,
    Res = make_simple_resource(ResId),
    ok = emqx_rule_registry:add_resource(Res),
    ?assertMatch({ok, #resource{id = ResId}}, emqx_rule_registry:find_resource(ResId)),
    ok = emqx_rule_registry:remove_resource(ResId),
    ?assertEqual(not_found, emqx_rule_registry:find_resource(ResId)),
    ok = emqx_rule_registry:add_resource(Res),
    ?assertMatch({ok, #resource{id = ResId}}, emqx_rule_registry:find_resource(ResId)),
    ok = emqx_rule_registry:remove_resource(Res),
    ?assertEqual(not_found, emqx_rule_registry:find_resource(ResId)),
    ok.
t_get_resources(_Config) ->
    Len0 = length(emqx_rule_registry:get_resources()),
    Res1 = make_simple_resource(<<"resource-debug-1">>),
    Res2 = make_simple_resource(<<"resource-debug-2">>),
    ok = emqx_rule_registry:add_resource(Res1),
    ok = emqx_rule_registry:add_resource(Res2),
    ?assertEqual(Len0+2, length(emqx_rule_registry:get_resources())),
    ok.

t_resource_types(_Config) ->
    register_resource_types(),
    get_resource_type(),
    get_resource_types(),
    unregister_resource_types_of().

register_resource_types() ->
    ResType1 = make_simple_resource_type(<<"resource-type-debug-1">>),
    ResType2 = make_simple_resource_type(<<"resource-type-debug-2">>),
    emqx_rule_registry:register_resource_types([ResType1,ResType2]),
    ok.
get_resource_type() ->
    ?assertMatch( { ok
                  , #resource_type{name = <<"resource-type-debug-1">>}
                  }
                , emqx_rule_registry:find_resource_type(<<"resource-type-debug-1">>)
                ),
    ok.
get_resource_types() ->
    ResTypes = emqx_rule_registry:get_resource_types(),
    ct:pal("resource types now: ~p", [ResTypes]),
    ?assert(length(ResTypes) > 0),
    ok.
unregister_resource_types_of() ->
    NumOld = length(emqx_rule_registry:get_resource_types()),
    ok = emqx_rule_registry:unregister_resource_types_of(?APP),
    NumNow = length(emqx_rule_registry:get_resource_types()),
    ?assert((NumOld - NumNow) >= 2),
    ok.

%%------------------------------------------------------------------------------
%% Test cases for rule runtime
%%------------------------------------------------------------------------------

t_events(_Config) ->
    {ok, Client} = emqtt:start_link(
        [ {username, <<"u_event">>}
        , {clientid, <<"c_event">>}
        , {proto_ver, v5}
        , {properties, #{'Session-Expiry-Interval' => 60}}
        ]),
    {ok, Client2} = emqtt:start_link(
        [ {username, <<"u_event2">>}
        , {clientid, <<"c_event2">>}
        , {proto_ver, v5}
        , {properties, #{'Session-Expiry-Interval' => 60}}
        ]),

    ct:pal("====== verify $events/client_connected, $events/client_connack"),
    client_connected(Client, Client2),
    ct:pal("====== verify $events/session_subscribed, $events/client_check_acl_complete"),
    session_subscribed(Client2),
    ct:pal("====== verify t1"),
    message_publish(Client),
    ct:pal("====== verify $events/message_delivered"),
    message_delivered(Client),
    ct:pal("====== verify $events/message_acked"),
    message_acked(Client),
    ct:pal("====== verify $events/session_unsubscribed"),
    session_unsubscribed(Client2),
    ct:pal("====== verify $events/message_dropped"),
    message_dropped(Client),
    ct:pal("====== verify $events/client_disconnected"),
    client_disconnected(Client, Client2),
    ct:pal("====== verify $events/client_connack"),
    client_connack_failed(),
    ok.

message_publish(Client) ->
    emqtt:publish(Client, <<"t1">>, #{'Message-Expiry-Interval' => 60},
        <<"{\"id\": 1, \"name\": \"ha\"}">>, [{qos, 1}]),
    verify_event('message.publish'),
    ok.

client_connected(Client, Client2) ->
    {ok, _} = emqtt:connect(Client),
    {ok, _} = emqtt:connect(Client2),
    verify_event('client.connack'),
    verify_event('client.connected'),
    ok.

client_connack_failed() ->
    {ok, Client} = emqtt:start_link(
                      [ {username, <<"u_event3">>}
                      , {clientid, <<"c_event3">>}
                      , {proto_ver, v5}
                      , {properties, #{'Session-Expiry-Interval' => 60}}
                      ]),
    try
        meck:new(emqx_access_control, [non_strict, passthrough]),
        meck:expect(emqx_access_control, authenticate,
                    fun(_) -> {error, bad_username_or_password} end),
        process_flag(trap_exit, true),
        ?assertMatch({error, _}, emqtt:connect(Client)),
        timer:sleep(300),
        verify_event('client.connack')
    after
        meck:unload(emqx_access_control)
    end,
    ok.
client_disconnected(Client, Client2) ->
    ok = emqtt:disconnect(Client, 0, #{'User-Property' => {<<"reason">>, <<"normal">>}}),
    ok = emqtt:disconnect(Client2, 0, #{'User-Property' => {<<"reason">>, <<"normal">>}}),
    verify_event('client.disconnected'),
    ok.
session_subscribed(Client2) ->
    {ok, _, _} = emqtt:subscribe( Client2
                                , #{'User-Property' => {<<"topic_name">>, <<"t1">>}}
                                , <<"t1">>
                                , 1
                                ),
    verify_event('session.subscribed'),
    verify_event('client.check_acl_complete'),
    ok.
session_unsubscribed(Client2) ->
    {ok, _, _} = emqtt:unsubscribe( Client2
                                  , #{'User-Property' => {<<"topic_name">>, <<"t1">>}}
                                  , <<"t1">>
                                  ),
    verify_event('session.unsubscribed'),
    ok.

message_delivered(_Client) ->
    verify_event('message.delivered'),
    ok.
message_dropped(Client) ->
    message_publish(Client),
    verify_event('message.dropped'),
    ok.
message_acked(_Client) ->
    verify_event('message.acked'),
    ok.

t_mfa_action(_Config) ->
    ok = emqx_rule_registry:add_action(
            #action{name = 'mfa-action', app = ?APP,
                    module = ?MODULE, on_create = mfa_action,
                    types=[], params_spec = #{},
                    title = #{en => <<"MFA callback action">>},
                    description = #{en => <<"MFA callback action">>}}),
    SQL = "SELECT * FROM \"t1\"",
    {ok, #rule{id = Id}} = emqx_rule_engine:create_rule(
                    #{id => <<"rule:t_mfa_action">>,
                      rawsql => SQL,
                      actions => [#{ id => <<"action:mfa-test">>
                                   , name => 'mfa-action'
                                   , args => #{}
                                   }],
                      description => <<"Debug rule">>}),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    emqtt:publish(Client, <<"t1">>, <<"{\"id\": 1, \"name\": \"ha\"}">>, 0),
    emqtt:stop(Client),
    ct:sleep(500),
    ?assertEqual(1, persistent_term:get(<<"action:mfa-test">>, 0)),
    emqx_rule_registry:remove_rule(Id),
    emqx_rule_registry:remove_action('mfa-action'),
    ok.

t_match_atom_and_binary(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    TopicRule = create_simple_repub_rule(
                    <<"t2">>,
                    "SELECT connected_at as ts, * "
                    "FROM \"$events/client_connected\" "
                    "WHERE username = 'emqx2' ",
                    <<"user:${ts}">>),
    {ok, Client} = emqtt:start_link([{username, <<"emqx1">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    ct:sleep(100),
    {ok, Client1} = emqtt:start_link([{username, <<"emqx2">>}]),
    {ok, _} = emqtt:connect(Client1),
    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(<<"t2">>, T),
        <<"user:", ConnAt/binary>> = Payload,
        _ = binary_to_integer(ConnAt)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:stop(Client), emqtt:stop(Client1),
    emqx_rule_registry:remove_rule(TopicRule).

t_metrics(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    TopicRule = create_simple_repub_rule(
                    <<"t2">>,
                    "SELECT payload.msg as msg, payload.idx as idx "
                    "FROM \"t1\" "
                    "WHERE msg = 'hello' and idx + 1 > 2 "),
    #rule{id = RuleId} = TopicRule,
    ?assertEqual(0, emqx_rule_metrics:get_rules_matched(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_passed(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_failed(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_exception(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_no_result(RuleId)),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    ct:sleep(200),
    PublishMoreTimes = fun(SomeMessage, Times) ->
        [begin
            emqtt:publish(Client, <<"t1">>, SomeMessage, 0),
            ct:sleep(200)
         end || _ <- lists:seq(1, Times)] end,
    PublishMoreTimes(<<"{\"msg\":\"hello\", \"idx\":5}">>, 10),
    ?assertEqual(10, emqx_rule_metrics:get_rules_matched(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_passed(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_failed(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_exception(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_no_result(RuleId)),
    PublishMoreTimes(<<"{\"msg\":\"hello\", \"idx\":0}">>, 10),
    ?assertEqual(20, emqx_rule_metrics:get_rules_matched(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_passed(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_failed(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_exception(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_no_result(RuleId)),
    PublishMoreTimes(<<"{\"msg\":\"hello\", \"idx\":\"somevalue\"}">>, 10),
    ?assertEqual(30, emqx_rule_metrics:get_rules_matched(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_passed(RuleId)),
    ?assertEqual(20, emqx_rule_metrics:get_rules_failed(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_exception(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_no_result(RuleId)),
    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(TopicRule).

t_metrics1(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    TopicRule = create_simple_repub_rule(
                    <<"t2">>,
                "FOREACH payload.sensors "
                "DO clientid,item.name as name, item.idx + 1 as idx "
                "INCASE item.idx >= 1 "
                "FROM \"t1\" "),
    #rule{id = RuleId} = TopicRule,
    ?assertEqual(0, emqx_rule_metrics:get_rules_matched(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_passed(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_failed(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_exception(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_no_result(RuleId)),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    ct:sleep(200),
    PublishMoreTimes = fun(SomeMessage, Times) ->
        [begin
            emqtt:publish(Client, <<"t1">>, SomeMessage, 0),
            ct:sleep(200)
         end || _ <- lists:seq(1, Times)] end,
    Message =  <<"{\"date\": \"2020-04-24\",
                   \"sensors\": [
                       {\"name\": \"a\", \"idx\":0},
                       {\"name\": \"b\", \"idx\":1},
                       {\"name\": \"c\", \"idx\":2}
                   ]}">>,
    PublishMoreTimes(Message, 10),
    ?assertEqual(10, emqx_rule_metrics:get_rules_matched(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_passed(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_failed(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_exception(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_no_result(RuleId)),
    Message1 =  <<"{\"date\": \"2020-04-24\",
                   \"sensors\": [
                       {\"name\": \"a\", \"idx\":0},
                       {\"name\": \"b\", \"idx\":0},
                       {\"name\": \"c\", \"idx\":0}
                   ]}">>,
    PublishMoreTimes(Message1, 10),
    ?assertEqual(20, emqx_rule_metrics:get_rules_matched(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_passed(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_failed(RuleId)),
    ?assertEqual(0, emqx_rule_metrics:get_rules_exception(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_no_result(RuleId)),
    Message2 =  <<"{\"date\": \"2020-04-24\",
                   \"sensors\": [
                       {\"name\": \"a\", \"idx\":0},
                       {\"name\": \"b\", \"idx\":1},
                       {\"name\": \"c\", \"idx\":\"some string\"}
                   ]}">>,
    PublishMoreTimes(Message2, 10),
    ?assertEqual(30, emqx_rule_metrics:get_rules_matched(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_passed(RuleId)),
    ?assertEqual(20, emqx_rule_metrics:get_rules_failed(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_exception(RuleId)),
    ?assertEqual(10, emqx_rule_metrics:get_rules_no_result(RuleId)),

    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(TopicRule).

t_sqlselect_multi_actoins_1(Config) ->
    %% We create 2 actions in the same rule:
    %% The first will fail and we need to make sure the
    %% second one can still execute as the on_action_failed
    %% defaults to 'continue'
    {ok, Rule} = emqx_rule_engine:create_rule(
                    #{rawsql => ?config(connsql, Config),
                      actions => [
                          #{name => 'crash_action', args => #{}, fallbacks => []},
                          #{name => 'republish',
                            args => #{<<"target_topic">> => <<"t2">>,
                                      <<"target_qos">> => -1,
                                      <<"payload_tmpl">> => <<"clientid=${clientid}">>
                                     },
                            fallbacks => []}
                      ]
                     }),

    (?config(conn_event, Config))(),
    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(<<"t2">>, T),
        ?assertEqual(<<"clientid=c_emqx1">>, Payload)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqx_rule_registry:remove_rule(Rule).

t_sqlselect_multi_actoins_1_1(Config) ->
    %% Try again but set on_action_failed = 'continue' explicitly
    {ok, Rule2} = emqx_rule_engine:create_rule(
                    #{rawsql => ?config(connsql, Config),
                      on_action_failed => 'continue',
                      actions => [
                          #{name => 'crash_action', args => #{}, fallbacks => []},
                          #{name => 'republish',
                            args => #{<<"target_topic">> => <<"t2">>,
                                      <<"target_qos">> => -1,
                                      <<"payload_tmpl">> => <<"clientid=${clientid}">>
                                    },
                            fallbacks => []}
                      ]
                     }),

    (?config(conn_event, Config))(),
    receive {publish, #{topic := T2, payload := Payload2}} ->
        ?assertEqual(<<"t2">>, T2),
        ?assertEqual(<<"clientid=c_emqx1">>, Payload2)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqx_rule_registry:remove_rule(Rule2).

t_sqlselect_multi_actoins_2(Config) ->
    %% We create 2 actions in the same rule:
    %% The first will fail and we need to make sure the
    %% second one cannot execute as we've set the on_action_failed = 'stop'
    {ok, Rule} = emqx_rule_engine:create_rule(
                    #{rawsql => ?config(connsql, Config),
                      on_action_failed => stop,
                      actions => [
                          #{name => 'crash_action', args => #{}, fallbacks => []},
                          #{name => 'republish',
                            args => #{<<"target_topic">> => <<"t2">>,
                                      <<"target_qos">> => -1,
                                      <<"payload_tmpl">> => <<"clientid=${clientid}">>
                                    },
                            fallbacks => []}
                      ]
                     }),

    (?config(conn_event, Config))(),
    receive {publish, #{topic := <<"t2">>}} ->
        ct:fail(unexpected_t2)
    after 1000 ->
        ok
    end,

    emqx_rule_registry:remove_rule(Rule).

t_sqlselect_multi_actoins_3(Config) ->
    %% We create 2 actions in the same rule (on_action_failed = continue):
    %% The first will fail and we need to make sure the
    %% fallback actions can be executed, and the next actoins
    %% will be run without influence
    {ok, Rule} = emqx_rule_engine:create_rule(
                    #{rawsql => ?config(connsql, Config),
                      on_action_failed => continue,
                      actions => [
                          #{name => 'crash_action', args => #{}, fallbacks =>[
                              #{name => 'plus_by_one', args => #{}, fallbacks =>[]},
                              #{name => 'plus_by_one', args => #{}, fallbacks =>[]}
                          ]},
                          #{name => 'republish',
                            args => #{<<"target_topic">> => <<"t2">>,
                                      <<"target_qos">> => -1,
                                      <<"payload_tmpl">> => <<"clientid=${clientid}">>
                                    },
                            fallbacks => []}
                      ]
                     }),

    (?config(conn_event, Config))(),
    timer:sleep(100),

    %% verfiy the fallback actions has been run
    ?assertEqual(2, ets:lookup_element(plus_by_one_action, num, 2)),

    %% verfiy the next actions can be run
    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(<<"t2">>, T),
        ?assertEqual(<<"clientid=c_emqx1">>, Payload)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqx_rule_registry:remove_rule(Rule).

t_sqlselect_multi_actoins_3_1(Config) ->
    %% We create 2 actions in the same rule (on_action_failed = continue):
    %% The first will fail (with a 'badact' return) and we need to make sure the
    %% fallback actions can be executed, and the next actoins
    %% will be run without influence
    {ok, Rule} = emqx_rule_engine:create_rule(
                    #{rawsql => ?config(connsql, Config),
                      on_action_failed => continue,
                      actions => [
                          #{name => 'failure_action', args => #{}, fallbacks =>[
                              #{name => 'plus_by_one', args => #{}, fallbacks =>[]},
                              #{name => 'plus_by_one', args => #{}, fallbacks =>[]}
                          ]},
                          #{name => 'republish',
                            args => #{<<"target_topic">> => <<"t2">>,
                                      <<"target_qos">> => -1,
                                      <<"payload_tmpl">> => <<"clientid=${clientid}">>
                                    },
                            fallbacks => []}
                      ]
                     }),

    (?config(conn_event, Config))(),
    timer:sleep(100),

    %% verfiy the fallback actions has been run
    ?assertEqual(2, ets:lookup_element(plus_by_one_action, num, 2)),

    %% verfiy the next actions can be run
    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(<<"t2">>, T),
        ?assertEqual(<<"clientid=c_emqx1">>, Payload)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqx_rule_registry:remove_rule(Rule).

t_sqlselect_multi_actoins_4(Config) ->
    %% We create 2 actions in the same rule (on_action_failed = continue):
    %% The first will fail and we need to make sure the
    %% fallback actions can be executed, and the next actoins
    %% will be run without influence
    {ok, Rule} = emqx_rule_engine:create_rule(
                    #{rawsql => ?config(connsql, Config),
                      on_action_failed => continue,
                      actions => [
                          #{name => 'crash_action', args => #{}, fallbacks => [
                              #{name =>'plus_by_one', args => #{}, fallbacks =>[]},
                              #{name =>'crash_action', args => #{}, fallbacks =>[]},
                              #{name =>'plus_by_one', args => #{}, fallbacks =>[]}
                          ]},
                          #{name => 'republish',
                            args => #{<<"target_topic">> => <<"t2">>,
                                      <<"target_qos">> => -1,
                                      <<"payload_tmpl">> => <<"clientid=${clientid}">>
                                    },
                            fallbacks => []}
                      ]
                     }),

    (?config(conn_event, Config))(),
    timer:sleep(100),

    %% verfiy all the fallback actions were run, even if the second
    %% fallback action crashed
    ?assertEqual(2, ets:lookup_element(plus_by_one_action, num, 2)),

    %% verfiy the next actions can be run
    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(<<"t2">>, T),
        ?assertEqual(<<"clientid=c_emqx1">>, Payload)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqx_rule_registry:remove_rule(Rule).

t_sqlparse_payload_as(_Config) ->
    %% https://github.com/emqx/emqx/issues/3866
    Sql00 = "SELECT "
            " payload, map_get('engineWorkTime', payload.params, -1) "
            "as payload.params.engineWorkTime, "
            " map_get('hydOilTem', payload.params, -1) as payload.params.hydOilTem "
            "FROM \"t/#\" ",
    Payload1 = <<"{ \"msgId\": 1002, \"params\": "
                 "{ \"convertTemp\": 20, \"engineSpeed\": 42, \"hydOilTem\": 30 } }">>,
    {ok, Res01} = emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql00,
                      <<"ctx">> => #{<<"payload">> => Payload1,
                                     <<"topic">> => <<"t/a">>}}),
    ?assertMatch(#{
        <<"payload">> := #{
            <<"params">> := #{
                <<"convertTemp">> := 20,
                <<"engineSpeed">> := 42,
                <<"engineWorkTime">> := -1,
                <<"hydOilTem">> := 30
            }
        }
    }, Res01),

    Payload2 = <<"{ \"msgId\": 1002, \"params\": { \"convertTemp\": 20, \"engineSpeed\": 42 } }">>,
    {ok, Res02} = emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql00,
                      <<"ctx">> => #{<<"payload">> => Payload2,
                                     <<"topic">> => <<"t/a">>}}),
    ?assertMatch(#{
        <<"payload">> := #{
            <<"params">> := #{
                <<"convertTemp">> := 20,
                <<"engineSpeed">> := 42,
                <<"engineWorkTime">> := -1,
                <<"hydOilTem">> := -1
            }
        }
    }, Res02).

t_sqlparse_nested_get(_Config) ->
    Sql = "select payload as p, p.a.b as c "
          "from \"t/#\" ",
    ?assertMatch({ok,#{<<"c">> := 0}},
        emqx_rule_sqltester:test(
        #{<<"rawsql">> => Sql,
          <<"ctx">> => #{
              <<"topic">> => <<"t/1">>,
              <<"payload">> => <<"{\"a\": {\"b\": 0}}">>
          }})).

%%------------------------------------------------------------------------------
%% Internal helpers
%%------------------------------------------------------------------------------

make_simple_rule(RuleId) when is_binary(RuleId) ->
    #rule{id = RuleId,
          rawsql = <<"select * from \"simple/topic\"">>,
          for = [<<"simple/topic">>],
          fields = [<<"*">>],
          is_foreach = false,
          conditions = {},
          actions = [{'inspect', #{}}],
          description = <<"simple rule">>}.

make_simple_rule_with_ts(RuleId, Ts) when is_binary(RuleId) ->
    #rule{id = RuleId,
          rawsql = <<"select * from \"simple/topic\"">>,
          for = [<<"simple/topic">>],
          fields = [<<"*">>],
          is_foreach = false,
          conditions = {},
          actions = [{'inspect', #{}}],
          created_at = Ts,
          description = <<"simple rule">>}.

make_simple_rule(RuleId, SQL, ForTopics) when is_binary(RuleId) ->
    #rule{id = RuleId,
          rawsql = SQL,
          for = ForTopics,
          fields = [<<"*">>],
          is_foreach = false,
          conditions = {},
          actions = [{'inspect', #{}}],
          description = <<"simple rule">>}.

make_simple_action(ActionName) when is_atom(ActionName) ->
    #action{name = ActionName, app = ?APP,
            module = ?MODULE, on_create = simple_action_inspect, params_spec = #{},
            title = #{en => <<"Simple inspect action">>},
            description = #{en => <<"Simple inspect action">>}}.
make_simple_action(ActionName, Hook) when is_atom(ActionName) ->
    #action{name = ActionName, app = ?APP, for = Hook,
            module = ?MODULE, on_create = simple_action_inspect, params_spec = #{},
            title = #{en => <<"Simple inspect action">>},
            description = #{en => <<"Simple inspect action with hook">>}}.

simple_action_inspect(Params) ->
    fun(Data) ->
        io:format("Action InputData: ~p, Action InitParams: ~p~n", [Data, Params])
    end.

make_simple_resource(ResId) ->
    #resource{id = ResId,
              type = simple_resource_type,
              config = #{},
              description = <<"Simple Resource">>}.

hook_metrics_action(_Id, _Params) ->
    fun(Data = #{event := EventName}, _Envs) ->
        ct:pal("applying hook_metrics_action: ~p", [Data]),
        ets:insert(events_record_tab, {EventName, Data})
    end.

mfa_action(Id, _Params) ->
    persistent_term:put(Id, 0),
    {?MODULE, mfa_action_do, [Id]}.

mfa_action_do(_Data, _Envs, K) ->
    persistent_term:put(K, 1).

failure_action(_Id, _Params) ->
    fun(Data, _Envs) ->
        ct:pal("applying crash action, Data: ~p", [Data]),
        {badact, intentional_failure}
    end.

crash_action(_Id, _Params) ->
    fun(Data, _Envs) ->
        ct:pal("applying crash action, Data: ~p", [Data]),
        error(crash)
    end.

simple_action_2_create(_Id, _Params) ->
    ets:insert(simple_action_2, {created, erlang:timestamp()}),
    fun(_Data, _Envs) -> ok end.

simple_action_2_destroy(_Id, _Params) ->
    ets:insert(simple_action_2, {destroyed, erlang:timestamp()}),
    fun(_Data, _Envs) -> ok end.

init_plus_by_one_action() ->
    ets:new(plus_by_one_action, [named_table, set, public]),
    ets:insert(plus_by_one_action, {num, 0}).

plus_by_one_action(_Id, #{}) ->
    fun(Data, _Envs) ->
        ct:pal("applying plus_by_one_action, Data: ~p", [Data]),
        Num = ets:lookup_element(plus_by_one_action, num, 2),
        ets:insert(plus_by_one_action, {num, Num + 1})
    end.

verify_event(EventName) ->
    ct:sleep(50),
    case ets:lookup(events_record_tab, EventName) of
        [] ->
            ct:fail({no_such_event, EventName, ets:tab2list(events_record_tab)});
        Records ->
            [begin
                %% verify fields can be formatted to JSON string
                _ = emqx_json:encode(Fields),
                %% verify metadata fields
                verify_metadata_fields(EventName, Fields),
                %% verify available fields for each event name
                verify_event_fields(EventName, Fields)
            end || {_Name, Fields} <- Records]
    end.

verify_metadata_fields(_EventName, #{metadata := Metadata}) ->
    ?assertMatch(
        #{rule_id := <<"rule:t_events">>},
        Metadata).

verify_event_fields('message.publish', Fields) ->
    #{id := ID,
      clientid := ClientId,
      username := Username,
      payload := Payload,
      peerhost := PeerHost,
      topic := Topic,
      qos := QoS,
      flags := Flags,
      headers := Headers,
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
    ?assert(is_map(Headers)),
    ?assertMatch(#{'Message-Expiry-Interval' := 60}, Properties),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60*1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60*1000),
    ?assert(EventAt =< Timestamp);

verify_event_fields('client.connected', Fields) ->
    #{clientid := ClientId,
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
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60*1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60*1000),
    ?assert(EventAt =< Timestamp);

verify_event_fields('client.disconnected', Fields) ->
    #{reason := Reason,
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
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60*1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60*1000),
    ?assert(EventAt =< Timestamp);

verify_event_fields('client.connack', Fields) ->
    #{clientid := ClientId,
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
    ?assertEqual(60, ExpiryInterval),
    ?assertMatch(#{'Session-Expiry-Interval' := 60}, Properties),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60*1000);

verify_event_fields(SubUnsub, Fields) when SubUnsub == 'session.subscribed'
                                         ; SubUnsub == 'session.unsubscribed' ->
    #{clientid := ClientId,
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
    ?assertMatch(#{'User-Property' := #{<<"topic_name">> := <<"t1">>}},
                 maps:get(PropKey, Fields)),
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60*1000);

verify_event_fields('message.dropped', Fields) ->
    #{id := ID,
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
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60*1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60*1000),
    ?assert(EventAt =< Timestamp);

verify_event_fields('message.delivered', Fields) ->
    #{id := ID,
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
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60*1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60*1000),
    ?assert(EventAt =< Timestamp);

verify_event_fields('message.acked', Fields) ->
    #{id := ID,
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
    ?assert(0 =< TimestampElapse andalso TimestampElapse =< 60*1000),
    ?assert(0 =< RcvdAtElapse andalso RcvdAtElapse =< 60*1000),
    ?assert(EventAt =< Timestamp);

verify_event_fields('client.check_acl_complete', Fields) ->
    #{clientid := ClientId,
      action := Action,
      result := Result,
      topic := Topic,
      is_cache := IsCache,
      username := Username
    } = Fields,
    ?assertEqual(<<"t1">>, Topic),
    ?assert(lists:member(Action, [subscribe, publish])),
    ?assert(lists:member(Result, [allow, deny])),
    ?assert(lists:member(IsCache, [true, false])),
    ?assert(lists:member(ClientId, [<<"c_event">>, <<"c_event2">>])),
    ?assert(lists:member(Username, [<<"u_event">>, <<"u_event2">>])).

verify_peername(PeerName) ->
    case string:split(PeerName, ":") of
        [IPAddrS, PortS] ->
            verify_ipaddr(IPAddrS),
            _ = binary_to_integer(PortS);
        _ -> ct:fail({invalid_peername, PeerName})
    end.

verify_ipaddr(IPAddrS) ->
    ?assertMatch({ok, _}, inet:parse_address(binary_to_list(IPAddrS))).

%%------------------------------------------------------------------------------
%% Mock funcs
%%------------------------------------------------------------------------------

mock_print() ->
    catch meck:unload(emqx_ctl),
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg, []) end),
    meck:expect(emqx_ctl, print, fun(Msg, Arg) -> emqx_ctl:format(Msg, Arg) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> emqx_ctl:format_usage(Usages) end),
    meck:expect(emqx_ctl, usage, fun(Cmd, Descr) -> emqx_ctl:format_usage(Cmd, Descr) end).

unmock_print() ->
    meck:unload(emqx_ctl).

t_load_providers(_) ->
    error('TODO').

t_unload_providers(_) ->
    error('TODO').

t_delete_rule(_) ->
    error('TODO').

t_start_resource(_) ->
    error('TODO').

t_test_resource(_) ->
    error('TODO').

t_get_resource_status(_) ->
    error('TODO').

t_get_resource_params(_) ->
    error('TODO').

t_delete_resource(_) ->
    error('TODO').

t_refresh_resources(_) ->
    error('TODO').

t_refresh_rules(_) ->
    error('TODO').

t_refresh_resource_status(_) ->
    error('TODO').

t_init_resource(_) ->
    error('TODO').

t_init_action(_) ->
    error('TODO').

t_clear_resource(_) ->
    error('TODO').

t_clear_action(_) ->
    error('TODO').
