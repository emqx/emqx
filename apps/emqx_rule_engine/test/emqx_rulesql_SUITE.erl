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

-module(emqx_rulesql_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_rule_engine/include/rule_engine.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_rule_test_lib,
        [ stop_apps/0
        , start_apps/0
        , create_simple_repub_rule/2
        , create_simple_repub_rule/3
        , make_simple_debug_resource_type/0
        , init_events_counters/0
        ]).

all() ->
    [ {group, rulesql_select}
    , {group, rulesql_select_events}
    , {group, rulesql_select_metadata}
    , {group, rulesql_foreach}
    , {group, rulesql_case_when}
    , {group, rulesql_array_index}
    , {group, rulesql_array_range}
    , {group, rulesql_compare}
    , {group, rulesql_boolean}
    , {group, rulesql_others}
    ].

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 30}}].

groups() ->
    [{rulesql_select, [],
      [ t_sqlselect_0
      , t_sqlselect_00
      , t_sqlselect_01
      , t_sqlselect_02
      , t_sqlselect_1
      , t_sqlselect_2
      ]},
     {rulesql_select_events, [],
      [ t_sqlparse_event_client_connected_01
      , t_sqlparse_event_client_connected_02
      , t_sqlparse_event_client_disconnected_normal
      , t_sqlparse_event_client_disconnected_kicked
      , t_sqlparse_event_client_disconnected_discarded
      , t_sqlparse_event_client_disconnected_takeovered
      , t_sqlparse_event_session_subscribed
      , t_sqlparse_event_session_unsubscribed
      , t_sqlparse_event_message_delivered
      , t_sqlparse_event_message_acked
      , t_sqlparse_event_message_dropped
      ]},
     {rulesql_select_metadata, [],
      [ t_sqlparse_select_matadata_1
      ]},
     {rulesql_foreach, [],
      [ t_sqlparse_foreach_1
      , t_sqlparse_foreach_2
      , t_sqlparse_foreach_3
      , t_sqlparse_foreach_4
      , t_sqlparse_foreach_5
      , t_sqlparse_foreach_6
      , t_sqlparse_foreach_7
      , t_sqlparse_foreach_8
      ]},
     {rulesql_case_when, [],
      [ t_sqlparse_case_when_1
      , t_sqlparse_case_when_2
      , t_sqlparse_case_when_3
      ]},
     {rulesql_array_index, [],
      [ t_sqlparse_array_index_1
      , t_sqlparse_array_index_2
      , t_sqlparse_array_index_3
      , t_sqlparse_array_index_4
      , t_sqlparse_array_index_5
      ]},
     {rulesql_array_range, [],
      [ t_sqlparse_array_range_1
      , t_sqlparse_array_range_2
      ]},
     {rulesql_compare, [],
      [ t_sqlparse_compare_undefined
      , t_sqlparse_compare_null_null
      , t_sqlparse_compare_null_notnull
      , t_sqlparse_compare_notnull_null
      , t_sqlparse_compare
      ]},
     {rulesql_boolean, [],
      [ t_sqlparse_true_false
      ]},
     {rulesql_others, [],
      [ t_sqlparse_new_map
      , t_sqlparse_invalid_json
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

on_resource_create(_id, _) -> #{}.
on_resource_destroy(_id, _) -> ok.
on_get_resource_status(_id, _) -> #{is_alive => true}.

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

init_per_testcase(t_sqlparse_event_client_disconnected_discarded, Config) ->
    application:set_env(emqx, client_disconnect_discarded, true),
    Config;
init_per_testcase(t_sqlparse_event_client_disconnected_takeovered, Config) ->
    application:set_env(emqx, client_disconnect_takeovered, true),
    Config;
init_per_testcase(_TestCase, Config) ->
    init_events_counters(),
    ok = emqx_rule_registry:register_resource_types(
            [make_simple_debug_resource_type()]),
    %ct:pal("============ ~p", [ets:tab2list(emqx_resource_type)]),
    Config.

end_per_testcase(t_sqlparse_event_client_disconnected_takeovered, Config) ->
    application:set_env(emqx, client_disconnect_takeovered, false), %% back to default
    Config;
end_per_testcase(t_sqlparse_event_client_disconnected_discarded, Config) ->
    application:set_env(emqx, client_disconnect_discarded, false), %% back to default
    Config;
end_per_testcase(_TestCase, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Test cases for `select`
%%------------------------------------------------------------------------------

t_sqlselect_0(_Config) ->
    %% Verify SELECT with and without 'AS'

    Sql1 = "SELECT * "
        "FROM \"t/#\" "
        "WHERE payload.cmd.info = 'tt'",
    %% all available fields by `select` from message publish, selecte other key will be `undefined`
    Topic = <<"t/a">>,
    Payload = <<"{\"cmd\": {\"info\":\"tt\"}}">>,
    {ok, #{username := <<"u_emqx">>,
           topic := Topic,
           timestamp := TimeStamp,
           qos := 1,
           publish_received_at := TimeStamp,
           peerhost := <<"192.168.0.10">>,
           payload := Payload,
           node := 'test@127.0.0.1',
           metadata := #{rule_id := TestRuleId},
           id := MsgId,
           flags := #{},
           clientid := <<"c_emqx">>
          }
    } = emqx_rule_sqltester:test(
          #{<<"rawsql">> => Sql1,
            <<"ctx">> =>
                #{<<"payload">> => Payload,
                  <<"topic">> => Topic}}),
    ?assert(is_binary(TestRuleId)),
    ?assert(is_binary(MsgId)),
    ?assert(is_integer(TimeStamp)),

    Sql2 = "select payload.cmd as cmd "
           "from \"t/#\" "
           "where cmd.info = 'tt'",
    ?assertMatch({ok,#{<<"cmd">> := #{<<"info">> := <<"tt">>}}},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql2,
                      <<"ctx">> =>
                        #{<<"payload">> =>
                            <<"{\"cmd\": {\"info\":\"tt\"}}">>,
                          <<"topic">> => <<"t/a">>}})),

    Sql3 = "select payload.cmd as cmd, cmd.info as info "
           "from \"t/#\" "
           "where cmd.info = 'tt' and info = 'tt'",
    ?assertMatch({ok,#{<<"cmd">> := #{<<"info">> := <<"tt">>},
                       <<"info">> := <<"tt">>}},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql3,
                      <<"ctx">> =>
                        #{<<"payload">> =>
                            <<"{\"cmd\": {\"info\":\"tt\"}}">>,
                          <<"topic">> => <<"t/a">>}})),
    %% cascaded as
    Sql4 = "select payload.cmd as cmd, cmd.info as meta.info "
           "from \"t/#\" "
           "where cmd.info = 'tt' and meta.info = 'tt'",
    ?assertMatch({ok,#{<<"cmd">> := #{<<"info">> := <<"tt">>},
                       <<"meta">> := #{<<"info">> := <<"tt">>}}},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql4,
                      <<"ctx">> =>
                        #{<<"payload">> =>
                            <<"{\"cmd\": {\"info\":\"tt\"}}">>,
                          <<"topic">> => <<"t/a">>}})).

t_sqlselect_00(_Config) ->
    %% Verify plus/subtract and unary_add_or_subtract
    Sql0 = "select 1 - 1 as a "
          "from \"t/#\" ",
    ?assertMatch({ok,#{<<"a">> := 0}},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql0,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"">>,
                          <<"topic">> => <<"t/a">>}})),

    Sql1 = "select -1 + 1 as a "
           "from \"t/#\" ",
    ?assertMatch({ok,#{<<"a">> := 0}},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql1,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"">>,
                          <<"topic">> => <<"t/a">>}})),

    Sql2 = "select 1 + 1 as a "
           "from \"t/#\" ",
    ?assertMatch({ok,#{<<"a">> := 2}},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql2,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"">>,
                          <<"topic">> => <<"t/a">>}})),

    Sql3 = "select +1 as a "
           "from \"t/#\" ",
    ?assertMatch({ok,#{<<"a">> := 1}},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql3,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"">>,
                          <<"topic">> => <<"t/a">>}})),

    Sql4 = "select payload.msg1 + payload.msg2 as msg "
           "from \"t/#\" ",
    ?assertMatch({ok,#{<<"msg">> := <<"hello world">>}},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql4,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"{\"msg1\": \"hello\", \"msg2\": \" world\"}">>,
                          <<"topic">> => <<"t/1">>}})),

    Sql5 = "select payload.msg1 + payload.msg2 as msg "
           "from \"t/#\" ",
    ?assertMatch({error, {select_and_transform_error, {error, unsupported_type_implicit_conversion, _ST}}},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql5,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"{\"msg1\": \"hello\", \"msg2\": 1}">>,
                          <<"topic">> => <<"t/1">>}})).

%% Verify SELECT with and with 'WHERE'
t_sqlselect_01(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    TopicRule1 = create_simple_repub_rule(
                    <<"t2">>,
                    "SELECT json_decode(payload) as p, payload "
                    "FROM \"t3/#\", \"t1\" "
                    "WHERE p.x = 1"),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1}">>, 0),
    ct:sleep(100),
    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(<<"t2">>, T),
        ?assertEqual(<<"{\"x\":1}">>, Payload)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:publish(Client, <<"t1">>, <<"{\"x\":2}">>, 0),
    receive {publish, #{topic := <<"t2">>, payload := _}} ->
        ct:fail(unexpected_t2)
    after 1000 ->
        ok
    end,

    emqtt:publish(Client, <<"t3/a">>, <<"{\"x\":1}">>, 0),
    receive {publish, #{topic := T3, payload := Payload3}} ->
        ?assertEqual(<<"t2">>, T3),
        ?assertEqual(<<"{\"x\":1}">>, Payload3)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(TopicRule1).

t_sqlselect_02(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    TopicRule1 = create_simple_repub_rule(
                    <<"t2">>,
                    "SELECT * "
                    "FROM \"t3/#\", \"t1\" "
                    "WHERE payload.x = 1"),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1}">>, 0),
    ct:sleep(100),
    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(<<"t2">>, T),
        ?assertEqual(<<"{\"x\":1}">>, Payload)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:publish(Client, <<"t1">>, <<"{\"x\":2}">>, 0),
    receive {publish, #{topic := <<"t2">>, payload := Payload0}} ->
        ct:fail({unexpected_t2, Payload0})
    after 1000 ->
        ok
    end,

    emqtt:publish(Client, <<"t3/a">>, <<"{\"x\":1}">>, 0),
    receive {publish, #{topic := T3, payload := Payload3}} ->
        ?assertEqual(<<"t2">>, T3),
        ?assertEqual(<<"{\"x\":1}">>, Payload3)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(TopicRule1).

t_sqlselect_03(_Config) ->
    %% Verify SELECT with and with 'WHERE' and `+` `=` and `or` condition in 'WHERE' clause
    ok = emqx_rule_engine:load_providers(),
    TopicRule1 = create_simple_repub_rule(
                    <<"t2">>,
                    "SELECT * "
                    "FROM \"t3/#\", \"t1\" "
                    "WHERE payload.x + payload.y = 2 or payload.x + payload.y = \"11\""),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1, \"y\":1}">>, 0),
    ct:sleep(100),
    receive {publish, #{topic := T1, payload := Payload0}} ->
        ?assertEqual(<<"t2">>, T1),
        ?assertEqual(<<"{\"x\":1, \"y\":1}">>, Payload0)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    receive {publish, #{topic := T2, payload := Payload1}} ->
        ?assertEqual(<<"t2">>, T2),
        ?assertEqual(<<"{\"x\":\"1\", \"y\":\"1\"}">>, Payload1)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1, \"y\":2}">>, 0),
    receive {publish, #{topic := <<"t2">>, payload := Payload2}} ->
        ct:fail({unexpected_t2, Payload2})
    after 1000 ->
        ok
    end,

    emqtt:publish(Client, <<"t3/a">>, <<"{\"x\":1, \"y\":1}">>, 0),
    receive {publish, #{topic := T3, payload := Payload3}} ->
        ?assertEqual(<<"t2">>, T3),
        ?assertEqual(<<"{\"x\":1, \"y\":1}">>, Payload3)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(TopicRule1).

t_sqlselect_1(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    TopicRule = create_simple_repub_rule(
                    <<"t2">>,
                    "SELECT json_decode(payload) as p, payload "
                    "FROM \"t1\" "
                    "WHERE p.x = 1 and p.y = 2"),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    ct:sleep(200),
    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1,\"y\":2}">>, 0),
    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(<<"t2">>, T),
        ?assertEqual(<<"{\"x\":1,\"y\":2}">>, Payload)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1,\"y\":1}">>, 0),
    receive {publish, #{topic := <<"t2">>, payload := _}} ->
        ct:fail(unexpected_t2)
    after 1000 ->
        ok
    end,

    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(TopicRule).

t_sqlselect_2(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    %% recursively republish to t2
    TopicRule = create_simple_repub_rule(
                    <<"t2">>,
                    "SELECT * "
                    "FROM \"t2\" "),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),

    emqtt:publish(Client, <<"t2">>, <<"{\"x\":1,\"y\":144}">>, 0),
    Fun = fun() ->
            receive {publish, #{topic := <<"t2">>, payload := _}} ->
                received_t2
            after 500 ->
                received_nothing
            end
          end,
    received_t2 = Fun(),
    received_t2 = Fun(),
    received_nothing = Fun(),

    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(TopicRule).

%%------------------------------------------------------------------------------
%% Test cases for events
%%------------------------------------------------------------------------------

%% FROM $events/client_connected
t_sqlparse_event_client_connected_01(_Config) ->
    Sql = "select *"
          "from \"$events/client_connected\" ",

    %% all available fields by `select` from message publish, selecte other key will be `undefined`
    {ok, #{clientid := <<"c_emqx">>,
           username := <<"u_emqx">>,
           timestamp := TimeStamp,
           connected_at := TimeStamp,
           peername := <<"127.0.0.1:12345">>,
           metadata := #{rule_id := RuleId},
           %% default value
           node := 'test@127.0.0.1',
           sockname := <<"0.0.0.0:1883">>,
           proto_name := <<"MQTT">>,
           proto_ver := 5,
           mountpoint := undefined,
           keepalive := 60,
           is_bridge := false,
           expiry_interval := 3600,
           event := 'client.connected',
           clean_start := true
          }
    } = emqx_rule_sqltester:test(
          #{<<"rawsql">> => Sql,
            <<"ctx">> => #{<<"clientid">> => <<"c_emqx">>, <<"peername">> => <<"127.0.0.1:12345">>, <<"username">> => <<"u_emqx">>}}),
    ?assert(is_binary(RuleId)),
    ?assert(is_integer(TimeStamp)).

t_sqlparse_event_client_connected_02(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    %% republish the client.connected msg
    TopicRule = create_simple_repub_rule(
                    <<"repub/to/connected">>,
                    "SELECT * "
                    "FROM \"$events/client_connected\" "
                    "WHERE username = 'emqx1'",
                    <<"{clientid: ${clientid}}">>),
    {ok, Client} = emqtt:start_link([{clientid, <<"emqx0">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"repub/to/connected">>, 0),
    ct:sleep(200),
    {ok, Client1} = emqtt:start_link([{clientid, <<"c_emqx1">>}, {username, <<"emqx1">>}]),
    {ok, _} = emqtt:connect(Client1),
    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(<<"repub/to/connected">>, T),
        ?assertEqual(<<"{clientid: c_emqx1}">>, Payload)
    after 1000 ->
        ct:fail(wait_for_t2)
    end,

    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1,\"y\":1}">>, 0),
    receive {publish, #{topic := <<"t2">>, payload := _}} ->
        ct:fail(unexpected_t2)
    after 1000 ->
        ok
    end,

    emqtt:stop(Client), emqtt:stop(Client1),
    emqx_rule_registry:remove_rule(TopicRule).

%% FROM $events/client_disconnected
t_sqlparse_event_client_disconnected_normal(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    Sql = "select * "
          "from \"$events/client_disconnected\" ",
    RepubT = <<"repub/to/disconnected/normal">>,

    TopicRule = create_simple_repub_rule(RepubT, Sql, <<>>),

    {ok, Client} = emqtt:start_link([{clientid, <<"get_repub_client">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, RepubT, 0),
    ct:sleep(200),
    {ok, Client1} = emqtt:start_link([{clientid, <<"emqx">>}, {username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client1),
    emqtt:disconnect(Client1),

    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(RepubT, T),
        ?assertMatch(#{<<"reason">> := <<"normal">>}, emqx_json:decode(Payload, [return_maps]))
    after 1000 ->
        ct:fail(wait_for_repub_disconnected_normal)
    end,
    emqtt:stop(Client),

    emqx_rule_registry:remove_rule(TopicRule).

t_sqlparse_event_client_disconnected_kicked(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    Sql = "select * "
          "from \"$events/client_disconnected\" ",
    RepubT = <<"repub/to/disconnected/kicked">>,

    TopicRule = create_simple_repub_rule(RepubT, Sql, <<>>),

    {ok, ClientRecvRepub} = emqtt:start_link([{clientid, <<"get_repub_client">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(ClientRecvRepub),
    {ok, _, _} = emqtt:subscribe(ClientRecvRepub, RepubT, 0),
    ct:sleep(200),

    {ok, Client1} = emqtt:start_link([{clientid, <<"emqx">>}, {username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client1),
    emqx_cm:kick_session(<<"emqx">>),
    unlink(Client1), %% the process will receive {'EXIT',{shutdown,tcp_closed}}

    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(RepubT, T),
        ?assertMatch(#{<<"reason">> := <<"kicked">>}, emqx_json:decode(Payload, [return_maps]))
    after 1000 ->
        ct:fail(wait_for_repub_disconnected_kicked)
    end,
    emqtt:stop(ClientRecvRepub),
    emqx_rule_registry:remove_rule(TopicRule).

t_sqlparse_event_client_disconnected_discarded(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    Sql = "select * "
          "from \"$events/client_disconnected\" ",
    RepubT = <<"repub/to/disconnected/discarded">>,

    TopicRule = create_simple_repub_rule(RepubT, Sql, <<>>),

    {ok, ClientRecvRepub} = emqtt:start_link([{clientid, <<"get_repub_client">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(ClientRecvRepub),
    {ok, _, _} = emqtt:subscribe(ClientRecvRepub, RepubT, 0),
    ct:sleep(200),

    {ok, Client1} = emqtt:start_link([{clientid, <<"emqx">>}, {username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client1),
    unlink(Client1), %% the process will receive {'EXIT',{shutdown,tcp_closed}}

    {ok, Client2} = emqtt:start_link([{clientid, <<"emqx">>}, {username, <<"emqx">>}, {clean_start, true}]),
    {ok, _} = emqtt:connect(Client2),

    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(RepubT, T),
        ?assertMatch(#{<<"reason">> := <<"discarded">>}, emqx_json:decode(Payload, [return_maps]))
    after 1000 ->
        ct:fail(wait_for_repub_disconnected_discarded)
    end,

    emqtt:stop(ClientRecvRepub), emqtt:stop(Client2),
    emqx_rule_registry:remove_rule(TopicRule).

t_sqlparse_event_client_disconnected_takeovered(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    Sql = "select * "
          "from \"$events/client_disconnected\" ",
    RepubT = <<"repub/to/disconnected/takeovered">>,

    TopicRule = create_simple_repub_rule(RepubT, Sql, <<>>),

    {ok, ClientRecvRepub} = emqtt:start_link([{clientid, <<"get_repub_client">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(ClientRecvRepub),
    {ok, _, _} = emqtt:subscribe(ClientRecvRepub, RepubT, 0),
    ct:sleep(200),

    {ok, Client1} = emqtt:start_link([{clientid, <<"emqx">>}, {username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client1),
    unlink(Client1), %% the process will receive {'EXIT',{shutdown,tcp_closed}}

    {ok, Client2} = emqtt:start_link([{clientid, <<"emqx">>}, {username, <<"emqx">>}, {clean_start, false}]),
    {ok, _} = emqtt:connect(Client2),

    receive {publish, #{topic := T, payload := Payload}} ->
        ?assertEqual(RepubT, T),
        ?assertMatch(#{<<"reason">> := <<"takeovered">>}, emqx_json:decode(Payload, [return_maps]))
    after 1000 ->
        ct:fail(wait_for_repub_disconnected_discarded)
    end,

    emqtt:stop(ClientRecvRepub), emqtt:stop(Client2),
    emqx_rule_registry:remove_rule(TopicRule).

%% FROM $events/session_subscribed
t_sqlparse_event_session_subscribed(_Config) ->
    Sql = "select topic as tp "
          "from \"$events/session_subscribed\" ",
    ?assertMatch({ok,#{<<"tp">> := <<"t/tt">>}},
        emqx_rule_sqltester:test(
        #{<<"rawsql">> => Sql,
          <<"ctx">> => #{<<"topic">> => <<"t/tt">>}})).

%% FROM $events/session_unsubscribed
t_sqlparse_event_session_unsubscribed(_Config) ->
    %% TODO
    ok.

%% FROM $events/message_delivered
t_sqlparse_event_message_delivered(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    %% recursively republish to t2, if the msg delivered
    TopicRule = create_simple_repub_rule(
                    <<"t2">>,
                    "SELECT * "
                    "FROM \"$events/message_delivered\" "),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t2">>, <<"{\"x\":1,\"y\":144}">>, 0),
    Fun = fun() ->
            receive {publish, #{topic := <<"t2">>, payload := _}} ->
                received_t2
            after 500 ->
                received_nothing
            end
          end,
    received_t2 = Fun(),
    received_t2 = Fun(),
    received_nothing = Fun(),

    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(TopicRule).

%% FROM $events/message_acked
t_sqlparse_event_message_acked(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    %% republish to `repub/if/acked`, if the msg acked
    TopicRule = create_simple_repub_rule(
                    <<"repub/if/acked">>,
                    "SELECT * "
                    "FROM \"$events/message_acked\" "),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"repub/if/acked">>, ?QOS_1),

    Fun = fun() ->
            receive {publish, #{topic := <<"repub/if/acked">>, payload := _}} ->
                received_acked
            after 500 ->
                received_nothing
            end
          end,

    %% sub with max qos1 to generate ack packet
    {ok, _, _} = emqtt:subscribe(Client, <<"any/topic">>, ?QOS_1),

    Payload = <<"{\"x\":1,\"y\":144}">>,

    %% even sub with qos1, but publish with qos0, no ack
    emqtt:publish(Client, <<"any/topic">>, Payload, ?QOS_0),
    received_nothing = Fun(),

    %% sub and pub both are qos1, acked
    emqtt:publish(Client, <<"any/topic">>, Payload, ?QOS_1),
    received_acked = Fun(),
    received_nothing = Fun(),

    %% pub with qos2 but subscribed with qos1, acked
    emqtt:publish(Client, <<"any/topic">>, Payload, ?QOS_2),
    received_acked = Fun(),
    received_nothing = Fun(),

    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(TopicRule).

%% FROM $events/message_dropped
t_sqlparse_event_message_dropped(_Config) ->
    ok = emqx_rule_engine:load_providers(),
    %% republish to `repub/if/dropped`, if any msg dropped
    TopicRule = create_simple_repub_rule(
                    <<"repub/if/dropped">>,
                    "SELECT * "
                    "FROM \"$events/message_dropped\" "),
    {ok, Client} = emqtt:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqtt:connect(Client),

    %% this message will be dropped and then repub to `repub/if/dropped`
    emqtt:publish(Client, <<"any/topic/1">>, <<"{\"x\":1,\"y\":144}">>, 0),

    Fun_t2 = fun() ->
               receive {publish, #{topic := <<"repub/if/dropped">>, payload := _}} ->
                   received_repub
               after 500 ->
                   received_nothing
               end
             end,

    %% No client subscribed `any/topic`, triggered repub rule.
    %% But havn't sub `repub/if/dropped`, so the repub message will also be dropped with recursively republish.
    received_nothing = Fun_t2(),

    {ok, _, _} = emqtt:subscribe(Client, <<"repub/if/dropped">>, 0),

    %% this message will be dropped and then repub to `repub/to/t`
    emqtt:publish(Client, <<"any/topic/2">>, <<"{\"x\":1,\"y\":144}">>, 0),

    %% received subscribed `repub/to/t`
    received_repub = Fun_t2(),

    emqtt:stop(Client),
    emqx_rule_registry:remove_rule(TopicRule).

%%------------------------------------------------------------------------------
%% Test cases for `foreach`
%%------------------------------------------------------------------------------

t_sqlparse_foreach_1(_Config) ->
    %% Verify foreach with and without 'AS'
    Sql = "foreach payload.sensors as s "
          "from \"t/#\" ",
    ?assertMatch({ok,[#{<<"s">> := 1}, #{<<"s">> := 2}]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"sensors\": [1, 2]}">>,
                                     <<"topic">> => <<"t/a">>}})),
    Sql2 = "foreach payload.sensors "
          "from \"t/#\" ",
    ?assertMatch({ok,[#{item := 1}, #{item := 2}]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql2,
                      <<"ctx">> => #{<<"payload">> => <<"{\"sensors\": [1, 2]}">>,
                                     <<"topic">> => <<"t/a">>}})),
    Sql3 = "foreach payload.sensors "
          "from \"t/#\" ",
    ?assertMatch({ok,[#{item := #{<<"cmd">> := <<"1">>}, clientid := <<"c_a">>},
                       #{item := #{ <<"cmd">> := <<"2">>
                                  , <<"name">> := <<"ct">>
                                  }
                        , clientid := <<"c_a">>}]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql3,
                      <<"ctx">> =>
                          #{ <<"payload">> => <<"{\"sensors\": [{\"cmd\":\"1\"}, "
                                                "{\"cmd\":\"2\",\"name\":\"ct\"}]}">>
                           , <<"clientid">> => <<"c_a">>
                           , <<"topic">> => <<"t/a">>
                           }})),
    Sql4 = "foreach payload.sensors "
          "from \"t/#\" ",
    {ok,[#{metadata := #{rule_id := TRuleId}},
         #{metadata := #{rule_id := TRuleId}}]} =
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql4,
                      <<"ctx">> => #{
                          <<"payload">> => <<"{\"sensors\": [1, 2]}">>,
                          <<"topic">> => <<"t/a">>}}),
    ?assert(is_binary(TRuleId)).

t_sqlparse_foreach_2(_Config) ->
    %% Verify foreach-do with and without 'AS'
    Sql = "foreach payload.sensors as s "
          "do s.cmd as msg_type "
          "from \"t/#\" ",
    ?assertMatch({ok,[#{<<"msg_type">> := <<"1">>},#{<<"msg_type">> := <<"2">>}]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> =>
                        #{<<"payload">> =>
                            <<"{\"sensors\": [{\"cmd\":\"1\"}, {\"cmd\":\"2\"}]}">>,
                          <<"topic">> => <<"t/a">>}})),
    Sql2 = "foreach payload.sensors "
          "do item.cmd as msg_type "
          "from \"t/#\" ",
    ?assertMatch({ok,[#{<<"msg_type">> := <<"1">>},#{<<"msg_type">> := <<"2">>}]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql2,
                      <<"ctx">> =>
                        #{<<"payload">> =>
                            <<"{\"sensors\": [{\"cmd\":\"1\"}, {\"cmd\":\"2\"}]}">>,
                          <<"topic">> => <<"t/a">>}})),
    Sql3 = "foreach payload.sensors "
           "do item as item "
           "from \"t/#\" ",
    ?assertMatch({ok,[#{<<"item">> := 1},#{<<"item">> := 2}]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql3,
                      <<"ctx">> =>
                        #{<<"payload">> =>
                            <<"{\"sensors\": [1, 2]}">>,
                          <<"topic">> => <<"t/a">>}})).

t_sqlparse_foreach_3(_Config) ->
    %% Verify foreach-incase with and without 'AS'
    Sql = "foreach payload.sensors as s "
          "incase s.cmd != 1 "
          "from \"t/#\" ",
    ?assertMatch({ok,[#{<<"s">> := #{<<"cmd">> := 2}},
                      #{<<"s">> := #{<<"cmd">> := 3}}
                      ]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> =>
                        #{<<"payload">> =>
                            <<"{\"sensors\": [{\"cmd\":1}, {\"cmd\":2}, {\"cmd\":3}]}">>,
                          <<"topic">> => <<"t/a">>}})),
    Sql2 = "foreach payload.sensors "
          "incase item.cmd != 1 "
          "from \"t/#\" ",
    ?assertMatch({ok,[#{item := #{<<"cmd">> := 2}},
                      #{item := #{<<"cmd">> := 3}}
                      ]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql2,
                      <<"ctx">> =>
                        #{<<"payload">> =>
                            <<"{\"sensors\": [{\"cmd\":1}, {\"cmd\":2}, {\"cmd\":3}]}">>,
                          <<"topic">> => <<"t/a">>}})).

t_sqlparse_foreach_4(_Config) ->
    %% Verify foreach-do-incase
    Sql = "foreach payload.sensors as s "
          "do s.cmd as msg_type, s.name as name "
          "incase is_not_null(s.cmd) "
          "from \"t/#\" ",
    ?assertMatch({ok,[#{<<"msg_type">> := <<"1">>},#{<<"msg_type">> := <<"2">>}]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> =>
                        #{<<"payload">> =>
                            <<"{\"sensors\": [{\"cmd\":\"1\"}, {\"cmd\":\"2\"}]}">>,
                          <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ ok
                 , [ #{<<"msg_type">> := <<"1">>, <<"name">> := <<"n1">>}
                   , #{<<"msg_type">> := <<"2">>}
                   ]
                 },
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> =>
                        #{<<"payload">> =>
                            <<"{\"sensors\": [{\"cmd\":\"1\", \"name\":\"n1\"}, "
                              "{\"cmd\":\"2\"}, {\"name\":\"n3\"}]}">>,
                          <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok,[]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"{\"sensors\": [1, 2]}">>,
                          <<"topic">> => <<"t/a">>}})).

t_sqlparse_foreach_5(_Config) ->
    %% Verify foreach on a empty-list or non-list variable
    Sql = "foreach payload.sensors as s "
          "do s.cmd as msg_type, s.name as name "
          "from \"t/#\" ",
    ?assertMatch({ok,[]}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"{\"sensors\": 1}">>,
                          <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok,[]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"{\"sensors\": []}">>,
                          <<"topic">> => <<"t/a">>}})),
    Sql2 = "foreach payload.sensors "
          "from \"t/#\" ",
    ?assertMatch({ok,[]}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql2,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"{\"sensors\": 1}">>,
                          <<"topic">> => <<"t/a">>}})).

t_sqlparse_foreach_6(_Config) ->
    %% Verify foreach on a empty-list or non-list variable
    Sql = "foreach json_decode(payload) "
          "do item.id as zid, timestamp as t "
          "from \"t/#\" ",
    {ok, Res} = emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> =>
                        #{<<"payload">> => <<"[{\"id\": 5},{\"id\": 15}]">>,
                          <<"topic">> => <<"t/a">>}}),
    [#{<<"t">> := Ts1, <<"zid">> := Zid1},
     #{<<"t">> := Ts2, <<"zid">> := Zid2}] = Res,
    ?assertEqual(true, is_integer(Ts1)),
    ?assertEqual(true, is_integer(Ts2)),
    ?assert(Zid1 == 5 orelse Zid1 == 15),
    ?assert(Zid2 == 5 orelse Zid2 == 15).

t_sqlparse_foreach_7(_Config) ->
    %% Verify foreach-do-incase and cascaded AS
    Sql = "foreach json_decode(payload) as p, p.sensors as s, s.collection as c, c.info as info "
          "do info.cmd as msg_type, info.name as name "
          "incase is_not_null(info.cmd) "
          "from \"t/#\" "
          "where s.page = '2' ",
    Payload  = <<"{\"sensors\": {\"page\": 2, \"collection\": "
                 "{\"info\":[{\"name\":\"cmd1\", \"cmd\":\"1\"}, {\"cmd\":\"2\"}]} } }">>,
    ?assertMatch({ ok
                 , [ #{<<"name">> := <<"cmd1">>, <<"msg_type">> := <<"1">>}
                   , #{<<"msg_type">> := <<"2">>}
                   ]
                 },
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> =>
                        #{<<"payload">> => Payload,
                          <<"topic">> => <<"t/a">>}})),
    Sql2 = "foreach json_decode(payload) as p, p.sensors as s, s.collection as c, c.info as info "
          "do info.cmd as msg_type, info.name as name "
          "incase is_not_null(info.cmd) "
          "from \"t/#\" "
          "where s.page = '3' ",
    ?assertMatch({error, nomatch},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql2,
                      <<"ctx">> =>
                        #{<<"payload">> => Payload,
                          <<"topic">> => <<"t/a">>}})).

-define(COLL, #{<<"info">> := [<<"haha">>, #{<<"name">> := <<"cmd1">>, <<"cmd">> := <<"1">>}]}).
t_sqlparse_foreach_8(_Config) ->
    %% Verify foreach-do-incase and cascaded AS
    Sql = "foreach json_decode(payload) as p, p.sensors as s, s.collection as c, c.info as info "
          "do info.cmd as msg_type, info.name as name, s, c "
          "incase is_map(info) "
          "from \"t/#\" "
          "where s.page = '2' ",
    Payload  = <<"{\"sensors\": {\"page\": 2, \"collection\": "
                 "{\"info\":[\"haha\", {\"name\":\"cmd1\", \"cmd\":\"1\"}]} } }">>,
    ?assertMatch({ok,[#{<<"name">> := <<"cmd1">>, <<"msg_type">> := <<"1">>,
                        <<"s">> := #{<<"page">> := 2, <<"collection">> := ?COLL},
                        <<"c">> := ?COLL}]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> =>
                        #{<<"payload">> => Payload,
                          <<"topic">> => <<"t/a">>}})),

    Sql3 = "foreach json_decode(payload) as p, p.sensors as s,"
           " s.collection as c, sublist(2,1,c.info) as info "
           "do info.cmd as msg_type, info.name as name "
           "from \"t/#\" "
           "where s.page = '2' ",
    [?assertMatch({ok,[#{<<"name">> := <<"cmd1">>, <<"msg_type">> := <<"1">>}]},
                 emqx_rule_sqltester:test(
                    #{<<"rawsql">> => SqlN,
                      <<"ctx">> =>
                        #{<<"payload">> => Payload,
                          <<"topic">> => <<"t/a">>}}))
     || SqlN <- [Sql3]].

%%------------------------------------------------------------------------------
%% Test cases for `case..when..`
%%------------------------------------------------------------------------------

t_sqlparse_case_when_1(_Config) ->
    %% case-when-else clause
    Sql = "select "
          "  case when payload.x < 0 then 0 "
          "       when payload.x > 7 then 7 "
          "       else payload.x "
          "  end as y "
          "from \"t/#\" ",
    ?assertMatch({ok, #{<<"y">> := 1}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 1}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{<<"y">> := 0}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 0}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{<<"y">> := 0}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": -1}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{<<"y">> := 7}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 7}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{<<"y">> := 7}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 8}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ok.

t_sqlparse_case_when_2(_Config) ->
    % switch clause
    Sql = "select "
          "  case payload.x when 1 then 2 "
          "                 when 2 then 3 "
          "                 else 4 "
          "  end as y "
          "from \"t/#\" ",
    ?assertMatch({ok, #{<<"y">> := 2}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 1}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{<<"y">> := 3}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 2}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{<<"y">> := 4}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 4}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{<<"y">> := 4}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 7}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{<<"y">> := 4}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 8}">>,
                                     <<"topic">> => <<"t/a">>}})).

t_sqlparse_case_when_3(_Config) ->
    %% case-when clause
    Sql = "select "
          "  case when payload.x < 0 then 0 "
          "       when payload.x > 7 then 7 "
          "  end as y "
          "from \"t/#\" ",
    ?assertMatch({ok, #{}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 1}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 5}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 0}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{<<"y">> := 0}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": -1}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 7}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{<<"y">> := 7}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 8}">>,
                                     <<"topic">> => <<"t/a">>}})),
    ok.

%%------------------------------------------------------------------------------
%% Test cases for array index
%%------------------------------------------------------------------------------

t_sqlparse_array_index_1(_Config) ->
    %% index get
    Sql = "select "
          "  json_decode(payload) as p, "
          "  p[1] as a "
          "from \"t/#\" ",
    ?assertMatch({ok, #{<<"a">> := #{<<"x">> := 1}}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"[{\"x\": 1}]">>,
                                     <<"topic">> => <<"t/a">>}})),
    ?assertMatch({ok, #{}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": 1}">>,
                                     <<"topic">> => <<"t/a">>}})),
    %% index get without 'as'
    Sql2 = "select "
           "  payload.x[2] "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"payload">> := #{<<"x">> := [3]}}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql2,
                      <<"ctx">> => #{<<"payload">> => #{<<"x">> => [1,3,4]},
                                     <<"topic">> => <<"t/a">>}})),
    %% index get without 'as' again
    Sql3 = "select "
           "  payload.x[2].y "
           "from \"t/#\" ",
    ?assertMatch( {ok, #{<<"payload">> := #{<<"x">> := [#{<<"y">> := 3}]}}}
                , emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql3,
                      <<"ctx">> => #{<<"payload">> => #{<<"x">> => [1,#{y => 3},4]},
                                     <<"topic">> => <<"t/a">>}})
                ),

    %% index get with 'as'
    Sql4 = "select "
           "  payload.x[2].y as b "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"b">> := 3}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql4,
                      <<"ctx">> => #{<<"payload">> => #{<<"x">> => [1,#{y => 3},4]},
                                     <<"topic">> => <<"t/a">>}})).

t_sqlparse_array_index_2(_Config) ->
    %% array get with negative index
    Sql1 = "select "
           "  payload.x[-2].y as b "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"b">> := 3}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql1,
                      <<"ctx">> => #{<<"payload">> => #{<<"x">> => [1,#{y => 3},4]},
                                     <<"topic">> => <<"t/a">>}})),
    %% array append to head or tail of a list:
    Sql2 = "select "
           "  payload.x as b, "
           "  1 as c[-0], "
           "  2 as c[-0], "
           "  b as c[0] "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"b">> := 0, <<"c">> := [0,1,2]}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql2,
                      <<"ctx">> => #{<<"payload">> => #{<<"x">> => 0},
                                     <<"topic">> => <<"t/a">>}})),
    %% construct an empty list:
    Sql3 = "select "
           "  [] as c, "
           "  1 as c[-0], "
           "  2 as c[-0], "
           "  0 as c[0] "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"c">> := [0,1,2]}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql3,
                      <<"ctx">> => #{<<"payload">> => <<"">>,
                                     <<"topic">> => <<"t/a">>}})),
    %% construct a list:
    Sql4 = "select "
           "  [payload.a, \"topic\", 'c'] as c, "
           "  1 as c[-0], "
           "  2 as c[-0], "
           "  0 as c[0] "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"c">> := [0,11,<<"t/a">>,<<"c">>,1,2]}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql4,
                      <<"ctx">> => #{<<"payload">> => <<"{\"a\":11}">>,
                                     <<"topic">> => <<"t/a">>
                                     }})).

t_sqlparse_array_index_3(_Config) ->
    %% array with json string payload:
    Sql0 = "select "
           "payload,"
           "payload.x[2].y "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"payload">> := #{<<"x">> := [1, #{<<"y">> := [1,2]}, 3]}}},
            emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql0,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": [1,{\"y\": [1,2]},3]}">>,
                                     <<"topic">> => <<"t/a">>}})),
    %% same as above but don't select payload:
    Sql1 = "select "
           "payload.x[2].y as b "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"b">> := [1,2]}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql1,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": [1,{\"y\": [1,2]},3]}">>,
                                     <<"topic">> => <<"t/a">>}})),
    %% same as above but add 'as' clause:
    Sql2 = "select "
           "payload.x[2].y as b.c "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"b">> := #{<<"c">> := [1,2]}}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql2,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": [1,{\"y\": [1,2]},3]}">>,
                                     <<"topic">> => <<"t/a">>}})).

t_sqlparse_array_index_4(_Config) ->
    %% array with json string payload:
    Sql0 = "select "
           "0 as payload.x[2].y "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"payload">> := #{<<"x">> := [#{<<"y">> := 0}]}}},
            emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql0,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": [1,{\"y\": [1,2]},3]}">>,
                                     <<"topic">> => <<"t/a">>}})),
    %% array with json string payload, and also select payload.x:
    Sql1 = "select "
           "payload.x, "
           "0 as payload.x[2].y "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"payload">> := #{<<"x">> := [1, #{<<"y">> := 0}, 3]}}},
            emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql1,
                      <<"ctx">> => #{<<"payload">> => <<"{\"x\": [1,{\"y\": [1,2]},3]}">>,
                                     <<"topic">> => <<"t/a">>}})).

t_sqlparse_array_index_5(_Config) ->
    Sql00 = "select "
            "  [1,2,3,4] "
            "from \"t/#\" ",
    {ok, Res00} =
        emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql00,
                      <<"ctx">> => #{<<"payload">> => <<"">>,
                                     <<"topic">> => <<"t/a">>}}),
    ?assert(lists:any(fun({_K, V}) ->
            V =:= [1,2,3,4]
        end, maps:to_list(Res00))).

%%------------------------------------------------------------------------------
%% Test cases for rule metadata
%%------------------------------------------------------------------------------

t_sqlparse_select_matadata_1(_Config) ->
    %% array with json string payload:
    Sql0 = "select "
           "payload "
           "from \"t/#\" ",
    ?assertNotMatch({ok, #{<<"payload">> := <<"abc">>, metadata := _}},
            emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql0,
                      <<"ctx">> => #{<<"payload">> => <<"abc">>,
                                     <<"topic">> => <<"t/a">>}})),
    Sql1 = "select "
           "payload, metadata "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"payload">> := <<"abc">>, <<"metadata">> := _}},
            emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql1,
                      <<"ctx">> => #{<<"payload">> => <<"abc">>,
                                     <<"topic">> => <<"t/a">>}})).

%%------------------------------------------------------------------------------
%% Test cases for array range
%%------------------------------------------------------------------------------

t_sqlparse_array_range_1(_Config) ->
    %% get a range of list
    Sql0 = "select "
           "  payload.a[1..4] as c "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"c">> := [0,1,2,3]}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql0,
                      <<"ctx">> => #{<<"payload">> => <<"{\"a\":[0,1,2,3,4,5]}">>,
                                     <<"topic">> => <<"t/a">>}})),
    %% get a range from non-list data
    Sql02 = "select "
           "  payload.a[1..4] as c "
           "from \"t/#\" ",
    ?assertMatch({error, {select_and_transform_error, {error,{range_get,non_list_data},_}}},
        emqx_rule_sqltester:test(
            #{<<"rawsql">> => Sql02,
                <<"ctx">> =>
                    #{<<"payload">> => <<"{\"x\":[0,1,2,3,4,5]}">>,
                      <<"topic">> => <<"t/a">>}})),

    %% construct a range:
    Sql1 = "select "
           "  [1..4] as c, "
           "  5 as c[-0], "
           "  6 as c[-0], "
           "  0 as c[0] "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"c">> := [0,1,2,3,4,5,6]}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql1,
                      <<"ctx">> => #{<<"payload">> => <<"">>,
                                     <<"topic">> => <<"t/a">>}})).

t_sqlparse_array_range_2(_Config) ->
    %% construct a range without 'as'
    Sql00 = "select "
            "  [1..4] "
            "from \"t/#\" ",
    {ok, Res00} =
        emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql00,
                      <<"ctx">> => #{<<"payload">> => <<"">>,
                                     <<"topic">> => <<"t/a">>}}),
    ?assert(lists:any(fun({_K, V}) ->
            V =:= [1,2,3,4]
        end, maps:to_list(Res00))),
    %% construct a range without 'as'
    Sql01 = "select "
            "  a[2..4] "
            "from \"t/#\" ",
    ?assertMatch({ok, #{<<"a">> := [2,3,4]}},
        emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql01,
                      <<"ctx">> => #{<<"a">> => [1,2,3,4,5],
                                     <<"topic">> => <<"t/a">>}})),
    %% get a range of list without 'as'
    Sql02 = "select "
           "  payload.a[1..4] "
           "from \"t/#\" ",
    ?assertMatch({ok, #{<<"payload">> := #{<<"a">> := [0,1,2,3]}}}, emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql02,
                      <<"ctx">> => #{<<"payload">> => <<"{\"a\":[0,1,2,3,4,5]}">>,
                                     <<"topic">> => <<"t/a">>}})).

%%------------------------------------------------------------------------------
%% Test cases for boolean
%%------------------------------------------------------------------------------

t_sqlparse_true_false(_Config) ->
    %% construct a range without 'as'
    Sql00 = "select "
            " true as a, false as b, "
            " false as x.y, true as c[-0] "
            "from \"t/#\" ",
    {ok, Res00} =
        emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql00,
                      <<"ctx">> => #{<<"payload">> => <<"">>,
                                     <<"topic">> => <<"t/a">>}}),
    ?assertMatch(#{<<"a">> := true, <<"b">> := false,
                   <<"x">> := #{<<"y">> := false},
                   <<"c">> := [true]
                   }, Res00).

%%------------------------------------------------------------------------------
%% Test cases for compare
%%------------------------------------------------------------------------------

-define(TEST_SQL(SQL),
    emqx_rule_sqltester:test(
        #{<<"rawsql">> => SQL,
          <<"ctx">> => #{<<"payload">> => <<"{}">>,
                         <<"topic">> => <<"t/a">>}})).

t_sqlparse_compare_undefined(_Config) ->
    Sql00 = "select "
            " * "
            "from \"t/#\" "
            "where dev != undefined ",
    %% no match
    ?assertMatch({error, nomatch}, ?TEST_SQL(Sql00)),

    Sql01 = "select "
            " 'd' as dev "
            "from \"t/#\" "
            "where dev != undefined ",
    {ok, Res01} = ?TEST_SQL(Sql01),
    %% pass
    ?assertMatch(#{}, Res01),

    Sql02 = "select "
            " * "
            "from \"t/#\" "
            "where dev != 'undefined' ",
    {ok, Res02} = ?TEST_SQL(Sql02),
    %% pass
    ?assertMatch(#{}, Res02).

t_sqlparse_compare_null_null(_Config) ->
    %% test undefined == undefined
    Sql00 = "select "
            " a = b as c "
            "from \"t/#\" ",
    {ok, Res00} = ?TEST_SQL(Sql00),
    ?assertMatch(#{<<"c">> := true
                   }, Res00),

    %% test undefined != undefined
    Sql01 = "select "
            " a != b as c "
            "from \"t/#\" ",
    {ok, Res01} = ?TEST_SQL(Sql01),
    ?assertMatch(#{<<"c">> := false
                   }, Res01),

    %% test undefined > undefined
    Sql02 = "select "
            " a > b as c "
            "from \"t/#\" ",
    {ok, Res02} = ?TEST_SQL(Sql02),
    ?assertMatch(#{<<"c">> := false
                   }, Res02),

    %% test undefined < undefined
    Sql03 = "select "
            " a < b as c "
            "from \"t/#\" ",
    {ok, Res03} = ?TEST_SQL(Sql03),
    ?assertMatch(#{<<"c">> := false
                   }, Res03),

    %% test undefined <= undefined
    Sql04 = "select "
            " a <= b as c "
            "from \"t/#\" ",
    {ok, Res04} = ?TEST_SQL(Sql04),
    ?assertMatch(#{<<"c">> := true
                   }, Res04),

    %% test undefined >= undefined
    Sql05 = "select "
            " a >= b as c "
            "from \"t/#\" ",
    {ok, Res05} = ?TEST_SQL(Sql05),
    ?assertMatch(#{<<"c">> := true
                   }, Res05).

t_sqlparse_compare_null_notnull(_Config) ->
    %% test undefined == b
    Sql00 = "select "
            " 'b' as b, a = b as c "
            "from \"t/#\" ",
    {ok, Res00} = ?TEST_SQL(Sql00),
    ?assertMatch(#{<<"c">> := false
                   }, Res00),

    %% test undefined != b
    Sql01 = "select "
            " 'b' as b, a != b as c "
            "from \"t/#\" ",
    {ok, Res01} = ?TEST_SQL(Sql01),
    ?assertMatch(#{<<"c">> := true
                   }, Res01),

    %% test undefined > b
    Sql02 = "select "
            " 'b' as b, a > b as c "
            "from \"t/#\" ",
    {ok, Res02} = ?TEST_SQL(Sql02),
    ?assertMatch(#{<<"c">> := false
                   }, Res02),

    %% test undefined < b
    Sql03 = "select "
            " 'b' as b, a < b as c "
            "from \"t/#\" ",
    {ok, Res03} = ?TEST_SQL(Sql03),
    ?assertMatch(#{<<"c">> := false
                   }, Res03),

    %% test undefined <= b
    Sql04 = "select "
            " 'b' as b, a <= b as c "
            "from \"t/#\" ",
    {ok, Res04} = ?TEST_SQL(Sql04),
    ?assertMatch(#{<<"c">> := false
                   }, Res04),

    %% test undefined >= b
    Sql05 = "select "
            " 'b' as b, a >= b as c "
            "from \"t/#\" ",
    {ok, Res05} = ?TEST_SQL(Sql05),
    ?assertMatch(#{<<"c">> := false
                   }, Res05).

t_sqlparse_compare_notnull_null(_Config) ->
    %% test 'a' == undefined
    Sql00 = "select "
            " 'a' as a, a = b as c "
            "from \"t/#\" ",
    {ok, Res00} = ?TEST_SQL(Sql00),
    ?assertMatch(#{<<"c">> := false
                   }, Res00),

    %% test 'a' != undefined
    Sql01 = "select "
            " 'a' as a, a != b as c "
            "from \"t/#\" ",
    {ok, Res01} = ?TEST_SQL(Sql01),
    ?assertMatch(#{<<"c">> := true
                   }, Res01),

    %% test 'a' > undefined
    Sql02 = "select "
            " 'a' as a, a > b as c "
            "from \"t/#\" ",
    {ok, Res02} = ?TEST_SQL(Sql02),
    ?assertMatch(#{<<"c">> := false
                   }, Res02),

    %% test 'a' < undefined
    Sql03 = "select "
            " 'a' as a, a < b as c "
            "from \"t/#\" ",
    {ok, Res03} = ?TEST_SQL(Sql03),
    ?assertMatch(#{<<"c">> := false
                   }, Res03),

    %% test 'a' <= undefined
    Sql04 = "select "
            " 'a' as a, a <= b as c "
            "from \"t/#\" ",
    {ok, Res04} = ?TEST_SQL(Sql04),
    ?assertMatch(#{<<"c">> := false
                   }, Res04),

    %% test 'a' >= undefined
    Sql05 = "select "
            " 'a' as a, a >= b as c "
            "from \"t/#\" ",
    {ok, Res05} = ?TEST_SQL(Sql05),
    ?assertMatch(#{<<"c">> := false
                   }, Res05).

t_sqlparse_compare(_Config) ->
    Sql00 = "select "
            " 'a' as a, 'a' as b, a = b as c "
            "from \"t/#\" ",
    {ok, Res00} = ?TEST_SQL(Sql00),
    ?assertMatch(#{<<"c">> := true
                   }, Res00),

    Sql01 = "select "
            " is_null(a) as c "
            "from \"t/#\" ",
    {ok, Res01} = ?TEST_SQL(Sql01),
    ?assertMatch(#{<<"c">> := true
                   }, Res01),

    Sql02 = "select "
            " 1 as a, 2 as b, a < b as c "
            "from \"t/#\" ",
    {ok, Res02} = ?TEST_SQL(Sql02),
    ?assertMatch(#{<<"c">> := true
                   }, Res02),

    Sql03 = "select "
            " 1 as a, 2 as b, a > b as c "
            "from \"t/#\" ",
    {ok, Res03} = ?TEST_SQL(Sql03),
    ?assertMatch(#{<<"c">> := false
                   }, Res03),

    Sql04 = "select "
            " 1 as a, 2 as b, a = b as c "
            "from \"t/#\" ",
    {ok, Res04} = ?TEST_SQL(Sql04),
    ?assertMatch(#{<<"c">> := false
                   }, Res04),

    %% test 'a' >= undefined
    Sql05 = "select "
            " 1 as a, 2 as b, a >= b as c "
            "from \"t/#\" ",
    {ok, Res05} = ?TEST_SQL(Sql05),
    ?assertMatch(#{<<"c">> := false
                   }, Res05),

    %% test 'a' >= undefined
    Sql06 = "select "
            " 1 as a, 2 as b, a <= b as c "
            "from \"t/#\" ",
    {ok, Res06} = ?TEST_SQL(Sql06),
    ?assertMatch(#{<<"c">> := true
                   }, Res06).



t_sqlparse_new_map(_Config) ->
    %% construct a range without 'as'
    Sql00 = "select "
            " map_new() as a, map_new() as b, "
            " map_new() as x.y, map_new() as c[-0] "
            "from \"t/#\" ",
    {ok, Res00} =
        emqx_rule_sqltester:test(
                    #{<<"rawsql">> => Sql00,
                      <<"ctx">> => #{<<"payload">> => <<"">>,
                                     <<"topic">> => <<"t/a">>}}),
    ?assertMatch(#{<<"a">> := #{}, <<"b">> := #{},
                   <<"x">> := #{<<"y">> := #{}},
                   <<"c">> := [#{}]
                   }, Res00).


t_sqlparse_invalid_json(_Config) ->
    Sql02 = "select "
           "  payload.a[1..4] as c "
           "from \"t/#\" ",
    ?assertMatch({error, {select_and_transform_error, {error,{decode_json_failed,_},_}}},
        emqx_rule_sqltester:test(
            #{<<"rawsql">> => Sql02,
                <<"ctx">> =>
                    #{<<"payload">> => <<"{\"x\":[0,1,2,3,}">>,
                      <<"topic">> => <<"t/a">>}})),


    Sql2 = "foreach payload.sensors "
        "do item.cmd as msg_type "
        "from \"t/#\" ",
    ?assertMatch({error, {select_and_collect_error, {error,{decode_json_failed,_},_}}},
                 emqx_rule_sqltester:test(
                   #{<<"rawsql">> => Sql2,
                     <<"ctx">> =>
                         #{<<"payload">> =>
                               <<"{\"sensors\": [{\"cmd\":\"1\"} {\"cmd\":}]}">>,
                           <<"topic">> => <<"t/a">>}})).
