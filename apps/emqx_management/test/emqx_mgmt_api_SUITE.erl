%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_management/include/emqx_mgmt.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

-define(HOST, "http://127.0.0.1:8081/").

-define(API_VERSION, "v4").

-define(BASE_PATH, "api").

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_management], fun set_special_configs/1),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_management]),
    Config.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

set_special_configs(emqx_management) ->
    emqx_config:put([emqx_management], #{listeners => [#{protocol => http, port => 8081}],
                                         applications =>[#{id => "admin", secret => "public"}]}),
    ok;
set_special_configs(_App) ->
    ok.

get(Key, ResponseBody) ->
   maps:get(Key, jiffy:decode(list_to_binary(ResponseBody), [return_maps])).

lookup_alarm(Name, [#{<<"name">> := Name} | _More]) ->
    true;
lookup_alarm(Name, [_Alarm | More]) ->
    lookup_alarm(Name, More);
lookup_alarm(_Name, []) ->
    false.

is_existing(Name, [#{name := Name} | _More]) ->
    true;
is_existing(Name, [_Alarm | More]) ->
    is_existing(Name, More);
is_existing(_Name, []) ->
    false.

t_alarms(_) ->
    emqx_alarm:activate(alarm1),
    emqx_alarm:activate(alarm2),

    ?assert(is_existing(alarm1, emqx_alarm:get_alarms(activated))),
    ?assert(is_existing(alarm2, emqx_alarm:get_alarms(activated))),

    {ok, Return1} = request_api(get, api_path(["alarms/activated"]), auth_header_()),
    ?assert(lookup_alarm(<<"alarm1">>, maps:get(<<"alarms">>, lists:nth(1, get(<<"data">>, Return1))))),
    ?assert(lookup_alarm(<<"alarm2">>, maps:get(<<"alarms">>, lists:nth(1, get(<<"data">>, Return1))))),

    emqx_alarm:deactivate(alarm1),

    {ok, Return2} = request_api(get, api_path(["alarms"]), auth_header_()),
    ?assert(lookup_alarm(<<"alarm1">>, maps:get(<<"alarms">>, lists:nth(1, get(<<"data">>, Return2))))),
    ?assert(lookup_alarm(<<"alarm2">>, maps:get(<<"alarms">>, lists:nth(1, get(<<"data">>, Return2))))),

    {ok, Return3} = request_api(get, api_path(["alarms/deactivated"]), auth_header_()),
    ?assert(lookup_alarm(<<"alarm1">>, maps:get(<<"alarms">>, lists:nth(1, get(<<"data">>, Return3))))),
    ?assertNot(lookup_alarm(<<"alarm2">>, maps:get(<<"alarms">>, lists:nth(1, get(<<"data">>, Return3))))),

    emqx_alarm:deactivate(alarm2),

    {ok, Return4} = request_api(get, api_path(["alarms/deactivated"]), auth_header_()),
    ?assert(lookup_alarm(<<"alarm1">>, maps:get(<<"alarms">>, lists:nth(1, get(<<"data">>, Return4))))),
    ?assert(lookup_alarm(<<"alarm2">>, maps:get(<<"alarms">>, lists:nth(1, get(<<"data">>, Return4))))),

    {ok, _} = request_api(delete, api_path(["alarms/deactivated"]), auth_header_()),

    {ok, Return5} = request_api(get, api_path(["alarms/deactivated"]), auth_header_()),
    ?assertNot(lookup_alarm(<<"alarm1">>, maps:get(<<"alarms">>, lists:nth(1, get(<<"data">>, Return5))))),
    ?assertNot(lookup_alarm(<<"alarm2">>, maps:get(<<"alarms">>, lists:nth(1, get(<<"data">>, Return5))))).

t_apps(_) ->
    AppId = <<"123456">>,
    meck:new(emqx_mgmt_auth, [passthrough, no_history]),
    meck:expect(emqx_mgmt_auth, add_app, 6, fun(_, _, _, _, _, _) -> {error, undefined} end),
    {ok, Error1} = request_api(post, api_path(["apps"]), [],
                               auth_header_(), #{<<"app_id">> => AppId,
                                                 <<"name">> => <<"test">>,
                                                 <<"status">> => true}),
    ?assertMatch(<<"undefined">>, get(<<"message">>, Error1)),

    meck:expect(emqx_mgmt_auth, del_app, 1, fun(_) -> {error, undefined} end),
    {ok, Error2} = request_api(delete, api_path(["apps", binary_to_list(AppId)]), auth_header_()),
    ?assertMatch(<<"undefined">>, get(<<"message">>, Error2)),
    meck:unload(emqx_mgmt_auth),

    {ok, NoApp} = request_api(get, api_path(["apps", binary_to_list(AppId)]), auth_header_()),
    ?assertEqual(0, maps:size(get(<<"data">>, NoApp))),
    {ok, NotFound} = request_api(put, api_path(["apps", binary_to_list(AppId)]), [],
                                 auth_header_(), #{<<"name">> => <<"test 2">>,
                                                   <<"status">> => true}),
    ?assertEqual(<<"not_found">>, get(<<"message">>, NotFound)),

    {ok, _} = request_api(post, api_path(["apps"]), [],
                          auth_header_(), #{<<"app_id">> => AppId,
                                            <<"name">> => <<"test">>,
                                            <<"status">> => true}),
    {ok, _} = request_api(get, api_path(["apps"]), auth_header_()),
    {ok, _} = request_api(get, api_path(["apps", binary_to_list(AppId)]), auth_header_()),
    {ok, _} = request_api(put, api_path(["apps", binary_to_list(AppId)]), [],
                          auth_header_(), #{<<"name">> => <<"test 2">>,
                                            <<"status">> => true}),
    {ok, AppInfo} = request_api(get, api_path(["apps", binary_to_list(AppId)]), auth_header_()),
    ?assertEqual(<<"test 2">>, maps:get(<<"name">>, get(<<"data">>, AppInfo))),
    {ok, _} = request_api(delete, api_path(["apps", binary_to_list(AppId)]), auth_header_()),
    {ok, Result} = request_api(get, api_path(["apps"]), auth_header_()),
    [App] = get(<<"data">>, Result),
    ?assertEqual(<<"admin">>, maps:get(<<"app_id">>, App)).

t_banned(_) ->
    Who = <<"myclient">>,
    {ok, _} = request_api(post, api_path(["banned"]), [],
                          auth_header_(), #{<<"who">> => Who,
                                            <<"as">> => <<"clientid">>,
                                            <<"reason">> => <<"test">>,
                                            <<"by">> => <<"dashboard">>,
                                            <<"at">> => erlang:system_time(second),
                                            <<"until">> => erlang:system_time(second) + 10}),

    {ok, Result} = request_api(get, api_path(["banned"]), auth_header_()),
    [Banned] = get(<<"data">>, Result),
    ?assertEqual(Who, maps:get(<<"who">>, Banned)),

    {ok, _} = request_api(delete, api_path(["banned", "clientid", binary_to_list(Who)]), auth_header_()),
    {ok, Result2} = request_api(get, api_path(["banned"]), auth_header_()),
    ?assertEqual([], get(<<"data">>, Result2)).

t_brokers(_) ->
    {ok, _} = request_api(get, api_path(["brokers"]), auth_header_()),
    {ok, _} = request_api(get, api_path(["brokers", atom_to_list(node())]), auth_header_()),
    meck:new(emqx_mgmt, [passthrough, no_history]),
    meck:expect(emqx_mgmt, lookup_broker, 1, fun(_) -> {error, undefined} end),
    {ok, Error} = request_api(get, api_path(["brokers", atom_to_list(node())]), auth_header_()),
    ?assertEqual(<<"undefined">>, get(<<"message">>, Error)),
    meck:unload(emqx_mgmt).

t_clients(_) ->
    process_flag(trap_exit, true),
    Username1 = <<"user1">>,
    Username2 = <<"user2">>,
    ClientId1 = <<"client1">>,
    ClientId2 = <<"client2">>,
    {ok, C1} = emqtt:start_link(#{username => Username1, clientid => ClientId1}),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link(#{username => Username2, clientid => ClientId2}),
    {ok, _} = emqtt:connect(C2),

    timer:sleep(300),

    {ok, Clients1} = request_api(get, api_path(["clients", binary_to_list(ClientId1)])
                               , auth_header_()),
    ?assertEqual(<<"client1">>, maps:get(<<"clientid">>, lists:nth(1, get(<<"data">>, Clients1)))),

    {ok, Clients2} = request_api(get, api_path(["nodes", atom_to_list(node()),
                                                "clients", binary_to_list(ClientId2)])
                                 , auth_header_()),
    ?assertEqual(<<"client2">>, maps:get(<<"clientid">>, lists:nth(1, get(<<"data">>, Clients2)))),

    {ok, Clients3} = request_api(get, api_path(["clients",
                                                "username", binary_to_list(Username1)]),
                                 auth_header_()),
    ?assertEqual(<<"client1">>, maps:get(<<"clientid">>, lists:nth(1, get(<<"data">>, Clients3)))),

    {ok, Clients4} = request_api(get, api_path(["nodes", atom_to_list(node()),
                                                "clients",
                                                "username", binary_to_list(Username2)])
                                 , auth_header_()),
    ?assertEqual(<<"client2">>, maps:get(<<"clientid">>, lists:nth(1, get(<<"data">>, Clients4)))),

    {ok, Clients5} = request_api(get, api_path(["clients"]), "_limit=100&_page=1", auth_header_()),
    ?assertEqual(2, maps:get(<<"count">>, get(<<"meta">>, Clients5))),

    meck:new(emqx_mgmt, [passthrough, no_history]),
    meck:expect(emqx_mgmt, kickout_client, 1, fun(_) -> {error, undefined} end),

    {ok, MeckRet1} = request_api(delete, api_path(["clients", binary_to_list(ClientId1)]), auth_header_()),
    ?assertEqual(?ERROR1, get(<<"code">>, MeckRet1)),

    meck:expect(emqx_mgmt, clean_acl_cache, 1, fun(_) -> {error, undefined} end),
    {ok, MeckRet2} = request_api(delete, api_path(["clients", binary_to_list(ClientId1), "acl_cache"]), auth_header_()),
    ?assertEqual(?ERROR1, get(<<"code">>, MeckRet2)),

    meck:expect(emqx_mgmt, list_acl_cache, 1, fun(_) -> {error, undefined} end),
    {ok, MeckRet3} = request_api(get, api_path(["clients", binary_to_list(ClientId2), "acl_cache"]), auth_header_()),
    ?assertEqual(?ERROR1, get(<<"code">>, MeckRet3)),

    meck:unload(emqx_mgmt),

    {ok, Ok} = request_api(delete, api_path(["clients", binary_to_list(ClientId1)]), auth_header_()),
    ?assertEqual(?SUCCESS, get(<<"code">>, Ok)),

    timer:sleep(300),

    {ok, NotFound0} = request_api(delete, api_path(["clients", binary_to_list(ClientId1)]), auth_header_()),
    ?assertEqual(?ERROR12, get(<<"code">>, NotFound0)),

    {ok, Clients6} = request_api(get, api_path(["clients"]), "_limit=100&_page=1", auth_header_()),
    ?assertEqual(1, maps:get(<<"count">>, get(<<"meta">>, Clients6))),

    {ok, NotFound1} = request_api(get, api_path(["clients", binary_to_list(ClientId1), "acl_cache"]), auth_header_()),
    ?assertEqual(?ERROR12, get(<<"code">>, NotFound1)),

    {ok, NotFound2} = request_api(delete, api_path(["clients", binary_to_list(ClientId1), "acl_cache"]), auth_header_()),
    ?assertEqual(?ERROR12, get(<<"code">>, NotFound2)),

    {ok, EmptyAclCache} = request_api(get, api_path(["clients", binary_to_list(ClientId2), "acl_cache"]), auth_header_()),
    ?assertEqual(0, length(get(<<"data">>, EmptyAclCache))),

    {ok, Ok1} = request_api(delete, api_path(["clients", binary_to_list(ClientId2), "acl_cache"]), auth_header_()),
    ?assertEqual(?SUCCESS, get(<<"code">>, Ok1)).

receive_exit(0) ->
    ok;
receive_exit(Count) ->
    receive
        {'EXIT', Client, {shutdown, tcp_closed}} ->
            ct:log("receive exit signal, Client: ~p", [Client]),
            receive_exit(Count - 1);
        {'EXIT', Client, _Reason} ->
            ct:log("receive exit signal, Client: ~p", [Client]),
            receive_exit(Count - 1)
    after 1000 ->
            ct:log("timeout")
    end.

t_listeners(_) ->
    {ok, _} = request_api(get, api_path(["listeners"]), auth_header_()),
    {ok, _} = request_api(get, api_path(["nodes", atom_to_list(node()), "listeners"]), auth_header_()),
    meck:new(emqx_mgmt, [passthrough, no_history]),
    meck:expect(emqx_mgmt, list_listeners, 0, fun() -> [{node(), {error, undefined}}] end),
    {ok, Return} = request_api(get, api_path(["listeners"]), auth_header_()),
    [Error] = get(<<"data">>, Return),
    ?assertEqual(<<"undefined">>,
                 maps:get(<<"error">>, maps:get(<<"listeners">>, Error))),
    meck:unload(emqx_mgmt).

t_metrics(_) ->
    {ok, _} = request_api(get, api_path(["metrics"]), auth_header_()),
    {ok, _} = request_api(get, api_path(["nodes", atom_to_list(node()), "metrics"]), auth_header_()),
    meck:new(emqx_mgmt, [passthrough, no_history]),
    meck:expect(emqx_mgmt, get_metrics, 1, fun(_) -> {error, undefined} end),
    {ok, "{\"message\":\"undefined\"}"} = request_api(get, api_path(["nodes", atom_to_list(node()), "metrics"]), auth_header_()),
    meck:unload(emqx_mgmt).

t_nodes(_) ->
    {ok, _} = request_api(get, api_path(["nodes"]), auth_header_()),
    {ok, _} = request_api(get, api_path(["nodes", atom_to_list(node())]), auth_header_()),
    meck:new(emqx_mgmt, [passthrough, no_history]),
    meck:expect(emqx_mgmt, list_nodes, 0, fun() -> [{node(), {error, undefined}}] end),
    {ok, Return} = request_api(get, api_path(["nodes"]), auth_header_()),
    [Error] = get(<<"data">>, Return),
    ?assertEqual(<<"undefined">>, maps:get(<<"error">>, Error)),
    meck:unload(emqx_mgmt).

% t_plugins(_) ->
%     application:ensure_all_started(emqx_retainer),
%     {ok, Plugins1} = request_api(get, api_path(["plugins"]), auth_header_()),
%     [Plugins11] = filter(get(<<"data">>, Plugins1), <<"node">>, atom_to_binary(node(), utf8)),
%     [Plugin1] = filter(maps:get(<<"plugins">>, Plugins11), <<"name">>, <<"emqx_retainer">>),
%     ?assertEqual(<<"emqx_retainer">>, maps:get(<<"name">>, Plugin1)),
%     ?assertEqual(true, maps:get(<<"active">>, Plugin1)),
% 
%     {ok, _} = request_api(put,
%                           api_path(["plugins",
%                                     atom_to_list(emqx_retainer),
%                                     "unload"]),
%                           auth_header_()),
%     {ok, Error1} = request_api(put,
%                                api_path(["plugins",
%                                          atom_to_list(emqx_retainer),
%                                          "unload"]),
%                                auth_header_()),
%     ?assertEqual(<<"not_started">>, get(<<"message">>, Error1)),
%     {ok, Plugins2} = request_api(get,
%                                  api_path(["nodes", atom_to_list(node()), "plugins"]),
%                                  auth_header_()),
%     [Plugin2] = filter(get(<<"data">>, Plugins2), <<"name">>, <<"emqx_retainer">>),
%     ?assertEqual(<<"emqx_retainer">>, maps:get(<<"name">>, Plugin2)),
%     ?assertEqual(false, maps:get(<<"active">>, Plugin2)),
% 
%     {ok, _} = request_api(put,
%                           api_path(["nodes",
%                                     atom_to_list(node()),
%                                     "plugins",
%                                     atom_to_list(emqx_retainer),
%                                     "load"]),
%                           auth_header_()),
%     {ok, Plugins3} = request_api(get,
%                                  api_path(["nodes", atom_to_list(node()), "plugins"]),
%                                  auth_header_()),
%     [Plugin3] = filter(get(<<"data">>, Plugins3), <<"name">>, <<"emqx_retainer">>),
%     ?assertEqual(<<"emqx_retainer">>, maps:get(<<"name">>, Plugin3)),
%     ?assertEqual(true, maps:get(<<"active">>, Plugin3)),
% 
%     {ok, _} = request_api(put,
%                           api_path(["nodes",
%                                     atom_to_list(node()),
%                                     "plugins",
%                                     atom_to_list(emqx_retainer),
%                                     "unload"]),
%                           auth_header_()),
%     {ok, Error2} = request_api(put,
%                                api_path(["nodes",
%                                          atom_to_list(node()),
%                                          "plugins",
%                                          atom_to_list(emqx_retainer),
%                                          "unload"]),
%                                auth_header_()),
%     ?assertEqual(<<"not_started">>, get(<<"message">>, Error2)),
%     application:stop(emqx_retainer).

t_acl_cache(_) ->
    ClientId = <<"client1">>,
    Topic = <<"mytopic">>,
    {ok, C1} = emqtt:start_link(#{clientid => ClientId}),
    {ok, _} = emqtt:connect(C1),
    {ok, _, _} = emqtt:subscribe(C1, Topic, 2),
    %% get acl cache, should not be empty
    {ok, Result} = request_api(get, api_path(["clients", binary_to_list(ClientId), "acl_cache"]), [], auth_header_()),
    #{<<"code">> := 0, <<"data">> := Caches} = jiffy:decode(list_to_binary(Result), [return_maps]),
    ?assert(length(Caches) > 0),
    ?assertMatch(#{<<"access">> := <<"subscribe">>,
                   <<"topic">> := Topic,
                   <<"result">> := <<"allow">>,
                   <<"updated_time">> := _}, hd(Caches)),
    %% clear acl cache
    {ok, Result2} = request_api(delete, api_path(["clients", binary_to_list(ClientId), "acl_cache"]), [], auth_header_()),
    ?assertMatch(#{<<"code">> := 0}, jiffy:decode(list_to_binary(Result2), [return_maps])),
    %% get acl cache again, after the acl cache is cleared
    {ok, Result3} = request_api(get, api_path(["clients", binary_to_list(ClientId), "acl_cache"]), [], auth_header_()),
    #{<<"code">> := 0, <<"data">> := Caches3} = jiffy:decode(list_to_binary(Result3), [return_maps]),
    ?assertEqual(0, length(Caches3)),
    ok = emqtt:disconnect(C1).

t_pubsub(_) ->
    Qos1Received = emqx_metrics:val('messages.qos1.received'),
    Qos2Received = emqx_metrics:val('messages.qos2.received'),
    Received = emqx_metrics:val('messages.received'),

    ClientId = <<"client1">>,
    Options = #{clientid => ClientId,
                proto_ver => 5},
    Topic = <<"mytopic">>,
    {ok, C1} = emqtt:start_link(Options),
    {ok, _} = emqtt:connect(C1),

    meck:new(emqx_mgmt, [passthrough, no_history]),
    meck:expect(emqx_mgmt, subscribe, 2, fun(_, _) -> {error, undefined} end),
    {ok, NotFound1} = request_api(post, api_path(["mqtt/subscribe"]), [], auth_header_(),
                                 #{<<"clientid">> => ClientId,
                                   <<"topic">> => Topic,
                                   <<"qos">> => 2}),
    ?assertEqual(?ERROR12, get(<<"code">>, NotFound1)),
    meck:unload(emqx_mgmt),

    {ok, BadTopic1} = request_api(post, api_path(["mqtt/subscribe"]), [], auth_header_(),
                                 #{<<"clientid">> => ClientId,
                                   <<"topics">> => <<"">>,
                                   <<"qos">> => 2}),
    ?assertEqual(?ERROR15, get(<<"code">>, BadTopic1)),

    {ok, BadTopic2} = request_api(post, api_path(["mqtt/publish"]), [], auth_header_(),
                                 #{<<"clientid">> => ClientId,
                                   <<"topics">> => <<"">>,
                                   <<"qos">> => 1,
                                   <<"payload">> => <<"hello">>}),
    ?assertEqual(?ERROR15, get(<<"code">>, BadTopic2)),

    {ok, BadTopic3} = request_api(post, api_path(["mqtt/unsubscribe"]), [], auth_header_(),
                                 #{<<"clientid">> => ClientId,
                                   <<"topic">> => <<"">>}),
    ?assertEqual(?ERROR15, get(<<"code">>, BadTopic3)),

    meck:new(emqx_mgmt, [passthrough, no_history]),
    meck:expect(emqx_mgmt, unsubscribe, 2, fun(_, _) -> {error, undefined} end),
    {ok, NotFound2} = request_api(post, api_path(["mqtt/unsubscribe"]), [], auth_header_(),
                                 #{<<"clientid">> => ClientId,
                                   <<"topic">> => Topic}),
    ?assertEqual(?ERROR12, get(<<"code">>, NotFound2)),
    meck:unload(emqx_mgmt),

    {ok, Code} = request_api(post, api_path(["mqtt/subscribe"]), [], auth_header_(),
                             #{<<"clientid">> => ClientId,
                               <<"topic">> => Topic,
                               <<"qos">> => 2}),
    ?assertEqual(?SUCCESS, get(<<"code">>, Code)),
    {ok, Code} = request_api(post, api_path(["mqtt/publish"]), [], auth_header_(),
                             #{<<"clientid">> => ClientId,
                               <<"topic">> => <<"mytopic">>,
                               <<"qos">> => 1,
                               <<"payload">> => <<"hello">>}),
    ?assert(receive
                {publish, #{payload := <<"hello">>}} ->
                    true
            after 100 ->
                    false
            end),
    %% json payload
    {ok, Code} = request_api(post, api_path(["mqtt/publish"]), [], auth_header_(),
                             #{<<"clientid">> => ClientId,
                               <<"topic">> => <<"mytopic">>,
                               <<"qos">> => 1,
                               <<"payload">> => #{body => "hello world"}}),
    Payload = emqx_json:encode(#{body => "hello world"}),
    ?assert(receive
                {publish, #{payload := Payload}} ->
                    true
            after 100 ->
                    false
            end),

    {ok, Code} = request_api(post, api_path(["mqtt/unsubscribe"]), [], auth_header_(),
                             #{<<"clientid">> => ClientId,
                              <<"topic">> => Topic}),

    %% tests subscribe_batch
    Topic_list = [<<"mytopic1">>, <<"mytopic2">>],
    [ {ok, _, [2]} = emqtt:subscribe(C1, Topics, 2) || Topics <- Topic_list],

    Body1 = [ #{<<"clientid">> => ClientId, <<"topic">> => Topics, <<"qos">> => 2} || Topics <- Topic_list],
    {ok, Data1} = request_api(post, api_path(["mqtt/subscribe_batch"]), [], auth_header_(), Body1),
    loop(maps:get(<<"data">>, jiffy:decode(list_to_binary(Data1), [return_maps]))),

    %% tests publish_batch
    Body2 = [ #{<<"clientid">> => ClientId, <<"topic">> => Topics, <<"qos">> => 2, <<"retain">> => <<"false">>, <<"payload">> => #{body => "hello world"}} || Topics <- Topic_list ],
    {ok, Data2} = request_api(post, api_path(["mqtt/publish_batch"]), [], auth_header_(), Body2),
    loop(maps:get(<<"data">>, jiffy:decode(list_to_binary(Data2), [return_maps]))),
    [ ?assert(receive
                    {publish, #{topic := Topics}} ->
                        true
                    after 100 ->
                        false
                    end) || Topics <- Topic_list ],

    %% tests unsubscribe_batch
    Body3 = [#{<<"clientid">> => ClientId, <<"topic">> => Topics} || Topics <- Topic_list],
    {ok, Data3} = request_api(post, api_path(["mqtt/unsubscribe_batch"]), [], auth_header_(), Body3),
    loop(maps:get(<<"data">>, jiffy:decode(list_to_binary(Data3), [return_maps]))),

    ok = emqtt:disconnect(C1),

    ?assertEqual(2, emqx_metrics:val('messages.qos1.received') - Qos1Received),
    ?assertEqual(2, emqx_metrics:val('messages.qos2.received') - Qos2Received),
    ?assertEqual(4, emqx_metrics:val('messages.received') - Received).

loop([]) -> [];

loop(Data) ->
    [H | T] = Data,
    ct:pal("H: ~p~n", [H]),
    ?assertEqual(0, maps:get(<<"code">>, H)),
    loop(T).

t_routes_and_subscriptions(_) ->
    ClientId = <<"myclient">>,
    Topic = <<"mytopic">>,
    {ok, NonRoute} = request_api(get, api_path(["routes"]), auth_header_()),
    ?assertEqual([], get(<<"data">>, NonRoute)),
    {ok, NonSubscription} = request_api(get, api_path(["subscriptions"]), auth_header_()),
    ?assertEqual([], get(<<"data">>, NonSubscription)),
    {ok, NonSubscription1} = request_api(get, api_path(["nodes", atom_to_list(node()), "subscriptions"]), auth_header_()),
    ?assertEqual([], get(<<"data">>, NonSubscription1)),
    {ok, NonSubscription2} = request_api(get,
                                         api_path(["subscriptions", binary_to_list(ClientId)]),
                                         auth_header_()),
    ?assertEqual([], get(<<"data">>, NonSubscription2)),
    {ok, NonSubscription3} = request_api(get, api_path(["nodes",
                                                        atom_to_list(node()),
                                                        "subscriptions",
                                                        binary_to_list(ClientId)])
                                         , auth_header_()),
    ?assertEqual([], get(<<"data">>, NonSubscription3)),
    {ok, C1} = emqtt:start_link(#{clean_start => true,
                                  clientid    => ClientId,
                                  proto_ver   => ?MQTT_PROTO_V5}),
    {ok, _} = emqtt:connect(C1),
    {ok, _, [2]} = emqtt:subscribe(C1, Topic, qos2),
    {ok, Result} = request_api(get, api_path(["routes"]), auth_header_()),
    [Route] = get(<<"data">>, Result),
    ?assertEqual(Topic, maps:get(<<"topic">>, Route)),

    {ok, Result2} = request_api(get, api_path(["routes", binary_to_list(Topic)]), auth_header_()),
    [Route] = get(<<"data">>, Result2),

    {ok, Result3} = request_api(get, api_path(["subscriptions"]), auth_header_()),
    [Subscription] = get(<<"data">>, Result3),
    ?assertEqual(Topic, maps:get(<<"topic">>, Subscription)),
    ?assertEqual(ClientId, maps:get(<<"clientid">>, Subscription)),

    {ok, Result3} = request_api(get, api_path(["nodes", atom_to_list(node()), "subscriptions"]), auth_header_()),

    {ok, Result4} = request_api(get, api_path(["subscriptions", binary_to_list(ClientId)]), auth_header_()),
    [Subscription] = get(<<"data">>, Result4),
    {ok, Result4} = request_api(get, api_path(["nodes", atom_to_list(node()), "subscriptions", binary_to_list(ClientId)])
                               , auth_header_()),

    ok = emqtt:disconnect(C1).

t_stats(_) ->
    {ok, _} = request_api(get, api_path(["stats"]), auth_header_()),
    {ok, _} = request_api(get, api_path(["nodes", atom_to_list(node()), "stats"]), auth_header_()),
    meck:new(emqx_mgmt, [passthrough, no_history]),
    meck:expect(emqx_mgmt, get_stats, 1, fun(_) -> {error, undefined} end),
    {ok, Return} = request_api(get, api_path(["nodes", atom_to_list(node()), "stats"]), auth_header_()),
    ?assertEqual(<<"undefined">>, get(<<"message">>, Return)),
    meck:unload(emqx_mgmt).

request_api(Method, Url, Auth) ->
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth) ->
    request_api(Method, Url, QueryParams, Auth, []).

request_api(Method, Url, QueryParams, Auth, []) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth]});
request_api(Method, Url, QueryParams, Auth, Body) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_json:encode(Body)}).

do_request_api(Method, Request)->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return} }
            when Code =:= 200 orelse Code =:= 201 ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    AppId = <<"admin">>,
    AppSecret = <<"public">>,
    auth_header_(binary_to_list(AppId), binary_to_list(AppSecret)).

auth_header_(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Pass])),
    {"Authorization","Basic " ++ Encoded}.

api_path(Parts)->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION] ++ Parts).

filter(List, Key, Value) ->
    lists:filter(fun(Item) ->
        maps:get(Key, Item) == Value
    end, List).
