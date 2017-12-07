%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqx_SUITE).

-compile(export_all).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-define(CONTENT_TYPE, "application/json").

-define(MQTT_SSL_TWOWAY, [{cacertfile, "certs/cacert.pem"},
                          {verify, verify_peer},
                          {fail_if_no_peer_cert, true}]).

-define(MQTT_SSL_CLIENT, [{keyfile, "certs/client-key.pem"},
                          {cacertfile, "certs/cacert.pem"},
                          {certfile, "certs/client-cert.pem"}]).

-define(URL, "http://localhost:8080/api/v2/").

-define(APPL_JSON, "application/json").

-define(PRINT(PATH), lists:flatten(io_lib:format(PATH, [atom_to_list(node())]))).

-define(GET_API, ["management/nodes",
                  ?PRINT("management/nodes/~s"),
                  "monitoring/nodes",
                  ?PRINT("monitoring/nodes/~s"),
                  "monitoring/listeners",
                  ?PRINT("monitoring/listeners/~s"),
                  "monitoring/metrics",
                  ?PRINT("monitoring/metrics/~s"),
                  "monitoring/stats",
                  ?PRINT("monitoring/stats/~s"),
                  ?PRINT("nodes/~s/clients"),
                  "routes"]).

all() ->
    [{group, protocol},
     {group, pubsub},
     {group, session},
     {group, broker},
     {group, metrics},
     {group, stats},
     {group, hook},
     {group, http},
     {group, alarms},
     {group, cli},
     {group, cleanSession}].

groups() ->
    [{protocol, [sequence],
      [mqtt_connect,
       mqtt_ssl_oneway,
       mqtt_ssl_twoway]},
     {pubsub, [sequence],
      [subscribe_unsubscribe,
       publish, pubsub,
       t_local_subscribe,
       t_shared_subscribe,
       'pubsub#', 'pubsub+']},
     {session, [sequence],
      [start_session]},
     {broker, [sequence],
      [hook_unhook]},
     {metrics, [sequence],
      [inc_dec_metric]},
     {stats, [sequence],
      [set_get_stat]},
     {hook, [sequence],
      [add_delete_hook,
       run_hooks]},
    {http, [sequence], 
     [request_status,
      request_publish,
      get_api_lists
     % websocket_test
     ]},
     {alarms, [sequence], 
     [set_alarms]
     },
     {cli, [sequence],
      [ctl_register_cmd,
       cli_status,
       cli_broker,
       cli_clients,
       cli_sessions,
       cli_routes,
       cli_topics,
       cli_subscriptions,
       cli_bridges,
       cli_plugins,
       {listeners, [sequence],
        [cli_listeners,
         conflict_listeners
         ]},
       cli_vm]},
     {cleanSession, [sequence],
      [cleanSession_validate,
       cleanSession_validate1]
     }
    ].

init_per_suite(Config) ->
    NewConfig = generate_config(),
    lists:foreach(fun set_app_env/1, NewConfig),
    Apps = application:ensure_all_started(?APP),
    ct:log("Apps:~p", [Apps]),
    Config.

end_per_suite(_Config) ->
    emqx:shutdown().

%%--------------------------------------------------------------------
%% Protocol Test
%%--------------------------------------------------------------------

mqtt_connect(_) ->
    %% Issue #599
    %% Empty clientId and clean_session = false
    ?assertEqual(<<32,2,0,2>>, connect_broker_(<<16,12,0,4,77,81,84,84,4,0,0,90,0,0>>, 4)),
    %% Empty clientId and clean_session = true
    ?assertEqual(<<32,2,0,0>>, connect_broker_(<<16,12,0,4,77,81,84,84,4,2,0,90,0,0>>, 4)).

connect_broker_(Packet, RecvSize) ->
    {ok, Sock} = gen_tcp:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}]),
    gen_tcp:send(Sock, Packet),
    {ok, Data} = gen_tcp:recv(Sock, RecvSize, 3000),
    gen_tcp:close(Sock),
    Data.

mqtt_ssl_oneway(_) ->
    emqx:stop(),
    change_opts(ssl_oneway),
    emqx:start(),
    {ok, SslOneWay} = emqttc:start_link([{host, "localhost"},
                                         {port, 8883},
                                         {logger, debug},
                                         {client_id, <<"ssloneway">>}, ssl]),
    timer:sleep(100),
    emqttc:subscribe(SslOneWay, <<"topic">>, qos1),
    {ok, Pub} = emqttc:start_link([{host, "localhost"},
                                   {client_id, <<"pub">>}]),
    emqttc:publish(Pub, <<"topic">>, <<"SSL oneWay test">>, [{qos, 1}]),
    timer:sleep(100),
    receive {publish, _Topic, RM} ->
        ?assertEqual(<<"SSL oneWay test">>, RM)
    after 1000 -> false
    end,
    timer:sleep(100),
    emqttc:disconnect(SslOneWay),
    emqttc:disconnect(Pub).

mqtt_ssl_twoway(_Config) ->
    emqx:stop(),
    change_opts(ssl_twoway),
    emqx:start(),
    timer:sleep(3000),
    ClientSSl = [{Key, local_path(["etc", File])} || {Key, File} <- ?MQTT_SSL_CLIENT],
    {ok, SslTwoWay} = emqttc:start_link([{host, "localhost"},
                                         {port, 8883},
                                         {client_id, <<"ssltwoway">>},
                                         {ssl, ClientSSl}]),
    {ok, Sub} = emqttc:start_link([{host, "localhost"},
                                   {client_id, <<"sub">>}]),
    emqttc:subscribe(Sub, <<"topic">>, qos1),
    emqttc:publish(SslTwoWay, <<"topic">>, <<"ssl client pub message">>, [{qos, 1}]),
    timer:sleep(10),
    receive {publish, _Topic, RM} ->
        ?assertEqual(<<"ssl client pub message">>, RM)
    after 1000 -> false
    end,
    emqttc:disconnect(SslTwoWay),
    emqttc:disconnect(Sub).

%%--------------------------------------------------------------------
%% PubSub Test
%%--------------------------------------------------------------------

subscribe_unsubscribe(_) ->
    ok = emqx:subscribe(<<"topic">>, <<"clientId">>),
    ok = emqx:subscribe(<<"topic/1">>, <<"clientId">>, [{qos, 1}]),
    ok = emqx:subscribe(<<"topic/2">>, <<"clientId">>, [{qos, 2}]),
    ok = emqx:unsubscribe(<<"topic">>, <<"clientId">>),
    ok = emqx:unsubscribe(<<"topic/1">>, <<"clientId">>),
    ok = emqx:unsubscribe(<<"topic/2">>, <<"clientId">>).

publish(_) ->
    Msg = emqx_message:make(ct, <<"test/pubsub">>, <<"hello">>),
    ok = emqx:subscribe(<<"test/+">>),
    timer:sleep(10),
    emqx:publish(Msg),
    ?assert(receive {dispatch, <<"test/+">>, Msg} -> true after 5 -> false end).

pubsub(_) ->
    Self = self(),
    ok = emqx:subscribe(<<"a/b/c">>, Self, [{qos, 1}]),
    ?assertMatch({error, _}, emqx:subscribe(<<"a/b/c">>, Self, [{qos, 2}])),
    timer:sleep(10),
    [{Self, <<"a/b/c">>}] = ets:lookup(mqtt_subscription, Self),
    [{<<"a/b/c">>, Self}] = ets:lookup(mqtt_subscriber, <<"a/b/c">>),
    emqx:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/b/c">>, _} -> true after 2 -> false end),
    spawn(fun() ->
            emqx:subscribe(<<"a/b/c">>),
            emqx:subscribe(<<"c/d/e">>),
            timer:sleep(10),
            emqx:unsubscribe(<<"a/b/c">>)
          end),
    timer:sleep(20),
    emqx:unsubscribe(<<"a/b/c">>).

t_local_subscribe(_) ->
    ok = emqx:subscribe("$local/topic0"),
    ok = emqx:subscribe("$local/topic1", <<"x">>),
    ok = emqx:subscribe("$local/topic2", <<"x">>, [{qos, 2}]),
    timer:sleep(10),
    ?assertEqual([self()], emqx:subscribers("$local/topic0")),
    ?assertEqual([{<<"x">>, self()}], emqx:subscribers("$local/topic1")),
    ?assertEqual([{{<<"x">>, self()}, <<"$local/topic1">>, []},
                  {{<<"x">>, self()}, <<"$local/topic2">>, [{qos,2}]}],
                 emqx:subscriptions(<<"x">>)),
    ?assertEqual(ok, emqx:unsubscribe("$local/topic0")),
    ?assertMatch({error, {subscription_not_found, _}}, emqx:unsubscribe("$local/topic0")),
    ?assertEqual(ok, emqx:unsubscribe("$local/topic1", <<"x">>)),
    ?assertEqual(ok, emqx:unsubscribe("$local/topic2", <<"x">>)),
    ?assertEqual([], emqx:subscribers("topic1")),
    ?assertEqual([], emqx:subscriptions(<<"x">>)).

t_shared_subscribe(_) ->
    emqx:subscribe("$local/$share/group1/topic1"),
    emqx:subscribe("$share/group2/topic2"),
    emqx:subscribe("$queue/topic3"),
    timer:sleep(10),
    ?assertEqual([self()], emqx:subscribers(<<"$local/$share/group1/topic1">>)),
    ?assertEqual([{self(), <<"$local/$share/group1/topic1">>, []},
                  {self(), <<"$queue/topic3">>, []},
                  {self(), <<"$share/group2/topic2">>, []}],
                 lists:sort(emqx:subscriptions(self()))),
    emqx:unsubscribe("$local/$share/group1/topic1"),
    emqx:unsubscribe("$share/group2/topic2"),
    emqx:unsubscribe("$queue/topic3"),
    ?assertEqual([], lists:sort(emqx:subscriptions(self()))).

'pubsub#'(_) ->
    emqx:subscribe(<<"a/#">>),
    timer:sleep(10),
    emqx:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/#">>, _} -> true after 2 -> false end),
    emqx:unsubscribe(<<"a/#">>).

'pubsub+'(_) ->
    emqx:subscribe(<<"a/+/+">>),
    timer:sleep(10),
    emqx:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/+/+">>, _} -> true after 1 -> false end),
    emqx:unsubscribe(<<"a/+/+">>).

loop_recv(Topic, Timeout) ->
    loop_recv(Topic, Timeout, []).

loop_recv(Topic, Timeout, Acc) ->
    receive
        {dispatch, Topic, Msg} ->
            loop_recv(Topic, Timeout, [Msg|Acc])
    after
        Timeout -> {ok, Acc}
    end.

recv_loop(Msgs) ->
    receive
        {dispatch, _Topic, Msg} ->
            recv_loop([Msg|Msgs])
        after
            100 -> lists:reverse(Msgs)
    end.

%%--------------------------------------------------------------------
%% Session Group
%%--------------------------------------------------------------------

start_session(_) ->
    {ok, ClientPid} = emqx_mock_client:start_link(<<"clientId">>),
    {ok, SessPid} = emqx_mock_client:start_session(ClientPid),
    Message = emqx_message:make(<<"clientId">>, 2, <<"topic">>, <<"hello">>),
    Message1 = Message#mqtt_message{pktid = 1},
    emqx_session:publish(SessPid, Message1),
    emqx_session:pubrel(SessPid, 1),
    emqx_session:subscribe(SessPid, [{<<"topic/session">>, [{qos, 2}]}]),
    Message2 = emqx_message:make(<<"clientId">>, 1, <<"topic/session">>, <<"test">>),
    emqx_session:publish(SessPid, Message2),
    emqx_session:unsubscribe(SessPid, [{<<"topic/session">>, []}]),
    emqx_mock_client:stop(ClientPid).

%%--------------------------------------------------------------------
%% Broker Group
%%--------------------------------------------------------------------
hook_unhook(_) ->
    ok.

%%--------------------------------------------------------------------
%% Metric Group
%%--------------------------------------------------------------------
inc_dec_metric(_) ->
    emqx_metrics:inc(gauge, 'messages/retained', 10),
    emqx_metrics:dec(gauge, 'messages/retained', 10).

%%--------------------------------------------------------------------
%% Stats Group
%%--------------------------------------------------------------------
set_get_stat(_) ->
    emqx_stats:setstat('retained/max', 99),
    99 = emqx_stats:getstat('retained/max').

%%--------------------------------------------------------------------
%% Hook Test
%%--------------------------------------------------------------------

add_delete_hook(_) ->
    ok = emqx:hook(test_hook, fun ?MODULE:hook_fun1/1, []),
    ok = emqx:hook(test_hook, {tag, fun ?MODULE:hook_fun2/1}, []),
    {error, already_hooked} = emqx:hook(test_hook, {tag, fun ?MODULE:hook_fun2/1}, []),
    Callbacks = [{callback, undefined, fun ?MODULE:hook_fun1/1, [], 0},
                 {callback, tag, fun ?MODULE:hook_fun2/1, [], 0}],
    Callbacks = emqx_hooks:lookup(test_hook),
    ok = emqx:unhook(test_hook, fun ?MODULE:hook_fun1/1),
    ct:print("Callbacks: ~p~n", [emqx_hooks:lookup(test_hook)]),
    ok = emqx:unhook(test_hook, {tag, fun ?MODULE:hook_fun2/1}),
    {error, not_found} = emqx:unhook(test_hook1, {tag, fun ?MODULE:hook_fun2/1}),
    [] = emqx_hooks:lookup(test_hook),

    ok = emqx:hook(emqx_hook, fun ?MODULE:hook_fun1/1, [], 9),
    ok = emqx:hook(emqx_hook, {"tag", fun ?MODULE:hook_fun2/1}, [], 8),
    Callbacks2 = [{callback, "tag", fun ?MODULE:hook_fun2/1, [], 8},
                  {callback, undefined, fun ?MODULE:hook_fun1/1, [], 9}],
    Callbacks2 = emqx_hooks:lookup(emqx_hook),
    ok = emqx:unhook(emqx_hook, fun ?MODULE:hook_fun1/1),
    ok = emqx:unhook(emqx_hook, {"tag", fun ?MODULE:hook_fun2/1}),
    [] = emqx_hooks:lookup(emqx_hook).

run_hooks(_) ->
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun3/4, [init]),
    ok = emqx:hook(foldl_hook, {tag, fun ?MODULE:hook_fun3/4}, [init]),
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun4/4, [init]),
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun5/4, [init]),
    {stop, [r3, r2]} = emqx:run_hooks(foldl_hook, [arg1, arg2], []),
    {ok, []} = emqx:run_hooks(unknown_hook, [], []),

    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun6/2, [initArg]),
    ok = emqx:hook(foreach_hook, {tag, fun ?MODULE:hook_fun6/2}, [initArg]),
    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun7/2, [initArg]),
    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun8/2, [initArg]),
    stop = emqx:run_hooks(foreach_hook, [arg]).

hook_fun1([]) -> ok.
hook_fun2([]) -> {ok, []}.

hook_fun3(arg1, arg2, _Acc, init) -> ok.
hook_fun4(arg1, arg2, Acc, init)  -> {ok, [r2 | Acc]}.
hook_fun5(arg1, arg2, Acc, init)  -> {stop, [r3 | Acc]}.

hook_fun6(arg, initArg) -> ok.
hook_fun7(arg, initArg) -> any.
hook_fun8(arg, initArg) -> stop.

%%--------------------------------------------------------------------
%% HTTP Request Test
%%--------------------------------------------------------------------

request_status(_) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    AppStatus =
    case lists:keysearch(?APP, 1, application:which_applications()) of
        false         -> not_running;
        {value, _Val} -> running
    end,
    Status = iolist_to_binary(io_lib:format("Node ~s is ~s~nemqx is ~s",
            [node(), InternalStatus, AppStatus])),
    Url = "http://127.0.0.1:8080/status",
    {ok, {{"HTTP/1.1", 200, "OK"}, _, Return}} =
    httpc:request(get, {Url, []}, [], []),
    ?assertEqual(binary_to_list(Status), Return).

request_publish(_) ->
    application:start(emq_dashboard),
    emqttc:start_link([{host, "localhost"},
                       {port, 1883},
                       {client_id, <<"random">>},
                       {clean_sess, false}]),
    SubParams = "{\"qos\":1, \"topic\" : \"a\/b\/c\", \"client_id\" :\"random\"}",
    ?assert(connect_emqx_pubsub_(post, "api/v2/mqtt/subscribe", SubParams, auth_header_("admin", "public"))),
    ok = emqx:subscribe(<<"a/b/c">>, self(), [{qos, 1}]),
    Params = "{\"qos\":1, \"retain\":false, \"topic\" : \"a\/b\/c\", \"messages\" :\"hello\"}",
    ?assert(connect_emqx_pubsub_(post, "api/v2/mqtt/publish", Params, auth_header_("admin", "public"))),
    ?assert(receive {dispatch, <<"a/b/c">>, _} -> true after 2 -> false end),

    UnSubParams = "{\"topic\" : \"a\/b\/c\", \"client_id\" :\"random\"}",
    ?assert(connect_emqx_pubsub_(post, "api/v2/mqtt/unsubscribe", UnSubParams, auth_header_("admin", "public"))).

connect_emqx_pubsub_(Method, Api, Params, Auth) ->
    Url = "http://127.0.0.1:8080/" ++ Api,
    case httpc:request(Method, {Url, [Auth], ?CONTENT_TYPE, Params}, [], []) of
    {error, socket_closed_remotely} ->
        false;
    {ok, {{"HTTP/1.1", 200, "OK"}, _, _Return} }  ->
        true;
    {ok, {{"HTTP/1.1", 400, _}, _, []}} ->
        false;
    {ok, {{"HTTP/1.1", 404, _}, _, []}} ->
        false
    end.

request(Path) ->
    http_get(get, Path).

http_get(Method, Path) ->
    req(Method, Path, []).

http_put(Method, Path, Params) ->
    req(Method, Path, format_for_upload(Params)).

http_post(Method, Path, Params) ->
    req(Method, Path, format_for_upload(Params)).

req(Method, Path, Body) ->
   Url = ?URL ++ Path,
   Headers = auth_header_("", ""),
   case httpc:request(Method, {Url, [Headers]}, [], []) of
   {error, R} ->
       ct:log("R:~p~n", [R]),
       false;
   {ok, {{"HTTP/1.1", 200, "OK"}, _, _Return} }  ->
       true;
   {ok, {{"HTTP/1.1", 400, _}, _, []}} ->
       false;
    {ok, {{"HTTP/1.1", 404, _}, _, []}} ->
        false
    end.

format_for_upload(none) ->
    <<"">>;
format_for_upload(List) ->
    iolist_to_binary(mochijson2:encode(List)).

connect_emqx_publish_(Method, Api, Params, Auth) ->
    Url = "http://127.0.0.1:8080/" ++ Api,
    case httpc:request(Method, {Url, [Auth], ?CONTENT_TYPE, Params}, [], []) of
    {error, socket_closed_remotely} ->
        false;
    {ok, {{"HTTP/1.1", 200, "OK"}, _, _Return} }  ->
        true;
    {ok, {{"HTTP/1.1", 400, _}, _, []}} ->
        false;
    {ok, {{"HTTP/1.1", 404, _}, _, []}} ->
        false
    end.
	
auth_header_(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Pass])),
    {"Authorization","Basic " ++ Encoded}.

get_api_lists(_Config) ->
    lists:foreach(fun request/1, ?GET_API).

websocket_test(_) ->
    Conn = esockd_connection:new(esockd_transport, nil, []),
    Req = mochiweb_request:new(Conn, 'GET', "/mqtt", {1, 1},
                                mochiweb_headers:make([{"Sec-WebSocket-Key","Xn3fdKyc3qEXPuj2A3O+ZA=="}])),

    ct:log("Req:~p", [Req]).
    %%emqx_http:handle_request(Req).

set_alarms(_) ->
    AlarmTest = #mqtt_alarm{id = <<"1">>, severity = error, title="alarm title", summary="alarm summary"},
    emqx_alarm:set_alarm(AlarmTest),
    Alarms = emqx_alarm:get_alarms(),
    ?assertEqual(1, length(Alarms)),
    emqx_alarm:clear_alarm(<<"1">>),
    [] = emqx_alarm:get_alarms().

%%--------------------------------------------------------------------
%% Cli group
%%--------------------------------------------------------------------

ctl_register_cmd(_) ->
    emqx_ctl:register_cmd(test_cmd, {?MODULE, test_cmd}),
    erlang:yield(),
    timer:sleep(5),
    [{?MODULE, test_cmd}] = emqx_ctl:lookup(test_cmd),
    emqx_ctl:run(["test_cmd", "arg1", "arg2"]),
    emqx_ctl:unregister_cmd(test_cmd).

test_cmd(["arg1", "arg2"]) ->
    ct:print("test_cmd is called");

test_cmd([]) ->
    io:format("test command").

cli_status(_) ->
    emqx_cli:status([]).

cli_broker(_) ->
    emqx_cli:broker([]),
    emqx_cli:broker(["stats"]),
    emqx_cli:broker(["metrics"]),
    emqx_cli:broker(["pubsub"]).

cli_clients(_) ->
    emqx_cli:clients(["list"]),
    emqx_cli:clients(["show", "clientId"]),
    emqx_cli:clients(["kick", "clientId"]).

cli_sessions(_) ->
    emqx_cli:sessions(["list"]),
    emqx_cli:sessions(["list", "persistent"]),
    emqx_cli:sessions(["list", "transient"]),
    emqx_cli:sessions(["show", "clientId"]).

cli_routes(_) ->
    emqx:subscribe(<<"topic/route">>),
    emqx_cli:routes(["list"]),
    emqx_cli:routes(["show", "topic/route"]),
    emqx:unsubscribe(<<"topic/route">>).

cli_topics(_) ->
    emqx:subscribe(<<"topic">>),
    emqx_cli:topics(["list"]),
    emqx_cli:topics(["show", "topic"]),
    emqx:unsubscribe(<<"topic">>).

cli_subscriptions(_) ->
    emqx_cli:subscriptions(["list"]),
    emqx_cli:subscriptions(["show", "clientId"]),
    emqx_cli:subscriptions(["add", "clientId", "topic", "2"]),
    emqx_cli:subscriptions(["del", "clientId", "topic"]).

cli_plugins(_) ->
    emqx_cli:plugins(["list"]),
    emqx_cli:plugins(["load", "emqx_plugin_template"]),
    emqx_cli:plugins(["unload", "emqx_plugin_template"]).

cli_bridges(_) ->
    emqx_cli:bridges(["list"]),
    emqx_cli:bridges(["start", "a@127.0.0.1", "topic"]),
    emqx_cli:bridges(["stop", "a@127.0.0.1", "topic"]).

cli_listeners(_) ->
    emqx_cli:listeners([]).

conflict_listeners(_) ->
    F =
    fun() ->
    process_flag(trap_exit, true),
    emqttc:start_link([{host, "localhost"},
                       {port, 1883},
                       {client_id, <<"c1">>},
                       {clean_sess, false}])
    end,
    spawn_link(F),

    {ok, C2} = emqttc:start_link([{host, "localhost"},
                                  {port, 1883},
                                  {client_id, <<"c1">>},
                                  {clean_sess, false}]),
    timer:sleep(100),

    Listeners =
    lists:map(fun({{Protocol, ListenOn}, Pid}) ->
        Key = atom_to_list(Protocol) ++ ":" ++ esockd:to_string(ListenOn),
        {Key, [{acceptors, esockd:get_acceptors(Pid)},
               {max_clients, esockd:get_max_clients(Pid)},
               {current_clients, esockd:get_current_clients(Pid)},
               {shutdown_count, esockd:get_shutdown_count(Pid)}]}
              end, esockd:listeners()),
    L = proplists:get_value("mqtt:tcp:0.0.0.0:1883", Listeners),
    ?assertEqual(1, proplists:get_value(current_clients, L)),
    ?assertEqual(1, proplists:get_value(conflict, proplists:get_value(shutdown_count, L))),
    timer:sleep(100),
    emqttc:disconnect(C2).

cli_vm(_) ->
    emqx_cli:vm([]),
    emqx_cli:vm(["ports"]).

cleanSession_validate(_) ->
    {ok, C1} = emqttc:start_link([{host, "localhost"},
                                         {port, 1883},
                                         {client_id, <<"c1">>},
                                         {clean_sess, false}]),
    timer:sleep(10),
    emqttc:subscribe(C1, <<"topic">>, qos0),
    ok = emqx_cli:sessions(["list", "persistent"]),
    emqttc:disconnect(C1),
    {ok, Pub} = emqttc:start_link([{host, "localhost"},
                                         {port, 1883},
                                         {client_id, <<"pub">>}]),

    emqttc:publish(Pub, <<"topic">>, <<"m1">>, [{qos, 0}]),
    timer:sleep(10),
    {ok, C11} = emqttc:start_link([{host, "localhost"},
                                   {port, 1883},
                                   {client_id, <<"c1">>},
                                   {clean_sess, false}]),
    timer:sleep(100),
    Metrics = emqx_metrics:all(),
    ct:log("Metrics:~p~n", [Metrics]),
    ?assertEqual(1, proplists:get_value('messages/qos0/sent', Metrics)),
    ?assertEqual(1, proplists:get_value('messages/qos0/received', Metrics)),
    emqttc:disconnect(Pub),
    emqttc:disconnect(C11).

cleanSession_validate1(_) ->
    {ok, C1} = emqttc:start_link([{host, "localhost"},
                                         {port, 1883},
                                         {client_id, <<"c1">>},
                                         {clean_sess, true}]),
    timer:sleep(10),
    emqttc:subscribe(C1, <<"topic">>, qos1),
    ok = emqx_cli:sessions(["list", "transient"]),
    emqttc:disconnect(C1),
    {ok, Pub} = emqttc:start_link([{host, "localhost"},
                                         {port, 1883},
                                         {client_id, <<"pub">>}]),

    emqttc:publish(Pub, <<"topic">>, <<"m1">>, [{qos, 1}]),
    timer:sleep(10),
    {ok, C11} = emqttc:start_link([{host, "localhost"},
                                         {port, 1883},
                                         {client_id, <<"c1">>},
                                         {clean_sess, false}]),
    timer:sleep(100),
    Metrics = emqx_metrics:all(),
    ?assertEqual(0, proplists:get_value('messages/qos1/sent', Metrics)),
    ?assertEqual(1, proplists:get_value('messages/qos1/received', Metrics)),
    emqttc:disconnect(Pub),
    emqttc:disconnect(C11).

ensure_ok(ok) -> ok;
ensure_ok({error, {already_started, _}}) -> ok.

host() -> ct:print("!!!! Node: ~p~n", [node()]), [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

wait_running(Node) ->
    wait_running(Node, 30000).

wait_running(Node, Timeout) when Timeout < 0 ->
    throw({wait_timeout, Node});

wait_running(Node, Timeout) ->
    case rpc:call(Node, emqx, is_running, [Node]) of
        true  -> ok;
        false -> timer:sleep(100),
                 wait_running(Node, Timeout - 100)
    end.

slave(emqx, Node) ->
    {ok, Slave} = slave:start(host(), Node, "-config ../../test/emqx_SUITE_data/slave.config " ++ ensure_slave()),
    ct:log("Slave:~p~n", [Slave]),
    rpc:call(Slave, application, ensure_all_started, [emqx]),
    Slave;

slave(node, Node) ->
    {ok, N} = slave:start(host(), Node, ensure_slave()),
    N.

ensure_slave() ->
    EbinDir = local_path(["ebin"]),
    DepsDir = local_path(["deps", "*", "ebin"]),
    RpcDir = local_path(["deps", "gen_rpc", "_build", "dev", "lib", "*", "ebin"]),
    "-pa " ++ EbinDir ++ " -pa " ++ DepsDir ++  " -pa " ++ RpcDir.

change_opts(SslType) ->
    {ok, Listeners} = application:get_env(?APP, listeners),
    NewListeners =
    lists:foldl(fun({Protocol, Port, Opts} = Listener, Acc) ->
    case Protocol of
    ssl ->
            SslOpts = proplists:get_value(sslopts, Opts),
            Keyfile = local_path(["etc/certs", "key.pem"]),
            Certfile = local_path(["etc/certs", "cert.pem"]),
            TupleList1 = lists:keyreplace(keyfile, 1, SslOpts, {keyfile, Keyfile}),
            TupleList2 = lists:keyreplace(certfile, 1, TupleList1, {certfile, Certfile}),
            TupleList3 =
            case SslType of
            ssl_twoway->
                CAfile = local_path(["etc", proplists:get_value(cacertfile, ?MQTT_SSL_TWOWAY)]),
                MutSslList = lists:keyreplace(cacertfile, 1, ?MQTT_SSL_TWOWAY, {cacertfile, CAfile}),
                lists:merge(TupleList2, MutSslList);
            _ ->
                lists:filter(fun ({cacertfile, _}) -> false;
                                 ({verify, _}) -> false;
                                 ({fail_if_no_peer_cert, _}) -> false;
                                 (_) -> true
                             end, TupleList2)
            end,
            [{Protocol, Port, lists:keyreplace(sslopts, 1, Opts, {sslopts, TupleList3})} | Acc];
        _ ->
            [Listener | Acc]
    end
    end, [], Listeners),
    application:set_env(?APP, listeners, NewListeners).

generate_config() ->
    Schema = cuttlefish_schema:files([local_path(["priv", "emqx.schema"])]),
    Conf = conf_parse:file([local_path(["etc", "emqx.conf"])]),
    cuttlefish_generator:map(Schema, Conf).

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

get_base_dir() ->
    get_base_dir(?MODULE).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

local_path(Components) ->
    local_path(Components, ?MODULE).

set_app_env({App, Lists}) ->
    lists:foreach(fun({Par, Var}) ->
                  application:set_env(App, Par, Var)
                  end, Lists).

