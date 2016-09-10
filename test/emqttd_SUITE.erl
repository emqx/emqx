%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_SUITE).

-compile(export_all).

-include("emqttd.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

all() ->
    [{group, protocol},
     {group, pubsub},
     {group, router},
     {group, session},
     %%{group, retainer},
     {group, broker},
     {group, metrics},
     {group, stats},
     {group, hook},
     {group, http},
     %%{group, backend},
     {group, cli}].

groups() ->
    [{protocol, [sequence],
      [mqtt_connect]},
     {pubsub, [sequence],
      [subscribe_unsubscribe,
       publish, pubsub,
       'pubsub#', 'pubsub+']},
     {router, [sequence],
      [router_add_del,
       router_print,
       router_unused]},
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
     {backend, [sequence],
      []},
    {http, [sequence], 
     [request_status,
      request_publish
     ]},
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
       cli_listeners,
       cli_vm]}].

init_per_suite(Config) ->
    application:start(lager),
    DataDir = proplists:get_value(data_dir, Config),
    application:set_env(emqttd, conf, filename:join([DataDir, "emqttd.conf"])),
    application:ensure_all_started(emqttd),
    Config.

end_per_suite(_Config) ->
    application:stop(emqttd),
    application:stop(esockd),
    application:stop(gproc),
    emqttd_mnesia:ensure_stopped().

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

%%--------------------------------------------------------------------
%% PubSub Test
%%--------------------------------------------------------------------

subscribe_unsubscribe(_) ->
    ok = emqttd:subscribe(<<"topic">>, <<"clientId">>),
    ok = emqttd:subscribe(<<"topic/1">>, <<"clientId">>, [{qos, 1}]),
    ok = emqttd:subscribe(<<"topic/2">>, <<"clientId">>, [{qos, 2}]),
    ok = emqttd:unsubscribe(<<"topic">>, <<"clientId">>),
    ok = emqttd:unsubscribe(<<"topic/1">>, <<"clientId">>),
    ok = emqttd:unsubscribe(<<"topic/2">>, <<"clientId">>).

publish(_) ->
    Msg = emqttd_message:make(ct, <<"test/pubsub">>, <<"hello">>),
    ok = emqttd:subscribe(<<"test/+">>),
    timer:sleep(10),
    emqttd:publish(Msg),
    ?assert(receive {dispatch, <<"test/+">>, Msg} -> true after 5 -> false end).

pubsub(_) ->
    Self = self(),
    ok = emqttd:subscribe(<<"a/b/c">>, Self, [{qos, 1}]),
    ?assertMatch({error, _}, emqttd:subscribe(<<"a/b/c">>, Self, [{qos, 2}])),
    timer:sleep(10),
    [{Self, <<"a/b/c">>}] = ets:lookup(mqtt_subscription, Self),
    [{<<"a/b/c">>, Self}] = ets:lookup(mqtt_subscriber, <<"a/b/c">>),
    emqttd:publish(emqttd_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/b/c">>, _} -> true after 2 -> false end),
    spawn(fun() ->
            emqttd:subscribe(<<"a/b/c">>),
            emqttd:subscribe(<<"c/d/e">>),
            timer:sleep(10),
            emqttd:unsubscribe(<<"a/b/c">>)
          end),
    timer:sleep(20),
    emqttd:unsubscribe(<<"a/b/c">>).

'pubsub#'(_) ->
    emqttd:subscribe(<<"a/#">>),
    timer:sleep(10),
    emqttd:publish(emqttd_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/#">>, _} -> true after 2 -> false end),
    emqttd:unsubscribe(<<"a/#">>).

'pubsub+'(_) ->
    emqttd:subscribe(<<"a/+/+">>),
    timer:sleep(10),
    emqttd:publish(emqttd_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/+/+">>, _} -> true after 1 -> false end),
    emqttd:unsubscribe(<<"a/+/+">>).

loop_recv(Topic, Timeout) ->
    loop_recv(Topic, Timeout, []).

loop_recv(Topic, Timeout, Acc) ->
    receive
        {dispatch, Topic, Msg} ->
            loop_recv(Topic, Timeout, [Msg|Acc])
    after
        Timeout -> {ok, Acc}
    end.

%%--------------------------------------------------------------------
%% Router Test
%%--------------------------------------------------------------------

router_add_del(_) ->
    %% Add
    emqttd_router:add_route(<<"#">>),
    emqttd_router:add_route(<<"a/b/c">>),
    emqttd_router:add_route(<<"+/#">>, node()),
    Routes = [R1, R2 | _] = [
            #mqtt_route{topic = <<"#">>,     node = node()},
            #mqtt_route{topic = <<"+/#">>,   node = node()},
            #mqtt_route{topic = <<"a/b/c">>, node = node()}],
    Routes = lists:sort(emqttd_router:match(<<"a/b/c">>)),

    %% Batch Add
    emqttd_router:add_routes(Routes),
    Routes = lists:sort(emqttd_router:match(<<"a/b/c">>)),

    %% Del
    emqttd_router:del_route(<<"a/b/c">>),
    [R1, R2] = lists:sort(emqttd_router:match(<<"a/b/c">>)),
    {atomic, []} = mnesia:transaction(fun emqttd_trie:lookup/1, [<<"a/b/c">>]),

    %% Batch Del
    R3 = #mqtt_route{topic = <<"#">>, node = 'a@127.0.0.1'},
    emqttd_router:add_route(R3),
    emqttd_router:del_routes([R1, R2]),
    emqttd_router:del_route(R3),
    [] = lists:sort(emqttd_router:match(<<"a/b/c">>)).

router_print(_) ->
    Routes = [#mqtt_route{topic = <<"a/b/c">>, node = node()},
              #mqtt_route{topic = <<"#">>,     node = node()},
              #mqtt_route{topic = <<"+/#">>,   node = node()}],
    emqttd_router:add_routes(Routes),
    emqttd_router:print(<<"a/b/c">>).

router_unused(_) ->
    gen_server:call(emqttd_router, bad_call),
    gen_server:cast(emqttd_router, bad_msg),
    emqttd_router ! bad_info.

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
    {ok, ClientPid} = emqttd_mock_client:start_link(<<"clientId">>),
    {ok, SessPid} = emqttd_mock_client:start_session(ClientPid),
    Message = emqttd_message:make(<<"clientId">>, 2, <<"topic">>, <<"hello">>),
    Message1 = Message#mqtt_message{pktid = 1},
    emqttd_session:publish(SessPid, Message1),
    emqttd_session:pubrel(SessPid, 1),
    emqttd_session:subscribe(SessPid, [{<<"topic/session">>, [{qos, 2}]}]),
    Message2 = emqttd_message:make(<<"clientId">>, 1, <<"topic/session">>, <<"test">>),
    emqttd_session:publish(SessPid, Message2),
    emqttd_session:unsubscribe(SessPid, [{<<"topic/session">>, []}]),
    emqttd_mock_client:stop(ClientPid).

%%--------------------------------------------------------------------
%% Broker Group
%%--------------------------------------------------------------------
hook_unhook(_) ->
    ok.

%%--------------------------------------------------------------------
%% Metric Group
%%--------------------------------------------------------------------
inc_dec_metric(_) ->
    emqttd_metrics:inc(gauge, 'messages/retained', 10),
    emqttd_metrics:dec(gauge, 'messages/retained', 10).

%%--------------------------------------------------------------------
%% Stats Group
%%--------------------------------------------------------------------
set_get_stat(_) ->
    emqttd_stats:setstat('retained/max', 99),
    99 = emqttd_stats:getstat('retained/max').

%%--------------------------------------------------------------------
%% Hook Test
%%--------------------------------------------------------------------

add_delete_hook(_) ->
    emqttd:hook(test_hook, fun ?MODULE:hook_fun1/1, []),
    emqttd:hook(test_hook, fun ?MODULE:hook_fun2/1, []),
    {error, already_hooked} = emqttd:hook(test_hook, fun ?MODULE:hook_fun2/1, []),
    Callbacks = [{callback, fun ?MODULE:hook_fun1/1, [], 0},
                 {callback, fun ?MODULE:hook_fun2/1, [], 0}],
    Callbacks = emqttd_hook:lookup(test_hook),
    emqttd:unhook(test_hook, fun ?MODULE:hook_fun1/1),
    emqttd:unhook(test_hook, fun ?MODULE:hook_fun2/1),
    ok = emqttd:unhook(test_hook, fun ?MODULE:hook_fun2/1),
    {error, not_found} = emqttd:unhook(test_hook1, fun ?MODULE:hook_fun2/1),
    [] = emqttd_hook:lookup(test_hook),

    emqttd:hook(emqttd_hook, fun ?MODULE:hook_fun1/1, [], 9),
    emqttd:hook(emqttd_hook, fun ?MODULE:hook_fun2/1, [], 8),
    Callbacks2 = [{callback, fun ?MODULE:hook_fun2/1, [], 8},
                  {callback, fun ?MODULE:hook_fun1/1, [], 9}],
    Callbacks2 = emqttd_hook:lookup(emqttd_hook),
    emqttd:unhook(emqttd_hook, fun ?MODULE:hook_fun1/1),
    emqttd:unhook(emqttd_hook, fun ?MODULE:hook_fun2/1),
    [] = emqttd_hook:lookup(emqttd_hook).

run_hooks(_) ->
    emqttd:hook(test_hook, fun ?MODULE:hook_fun3/4, [init]),
    emqttd:hook(test_hook, fun ?MODULE:hook_fun4/4, [init]),
    emqttd:hook(test_hook, fun ?MODULE:hook_fun5/4, [init]),
    {stop, [r3, r2]} = emqttd:run_hooks(test_hook, [arg1, arg2], []),
    {ok, []} = emqttd:run_hooks(unknown_hook, [], []).

hook_fun1([]) -> ok.
hook_fun2([]) -> {ok, []}.

hook_fun3(arg1, arg2, _Acc, init) -> ok.
hook_fun4(arg1, arg2, Acc, init)  -> {ok, [r2 | Acc]}.
hook_fun5(arg1, arg2, Acc, init)  -> {stop, [r3 | Acc]}.

%%--------------------------------------------------------------------
%% HTTP Request Test
%%--------------------------------------------------------------------

request_status(_) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    AppStatus =
    case lists:keysearch(emqttd, 1, application:which_applications()) of
        false         -> not_running;
        {value, _Val} -> running
    end,
    Status = iolist_to_binary(io_lib:format("Node ~s is ~s~nemqttd is ~s",
            [node(), InternalStatus, AppStatus])),
    Url = "http://127.0.0.1:8083/status",
    {ok, {{"HTTP/1.1", 200, "OK"}, _, Return}} =
    httpc:request(get, {Url, []}, [], []),
    ?assertEqual(binary_to_list(Status), Return).

request_publish(_) ->
    ok = emqttd:subscribe(<<"a/b/c">>, self(), [{qos, 1}]),
    Params = "qos=1&retain=0&topic=a/b/c&message=hello",
    ?assert(connect_emqttd_publish_(post, "mqtt/publish", Params, auth_header_("", ""))),
    ?assert(receive {dispatch, <<"a/b/c">>, _} -> true after 2 -> false end),
    emqttd:unsubscribe(<<"a/b/c">>).

connect_emqttd_publish_(Method, Api, Params, Auth) ->
    Url = "http://127.0.0.1:8083/" ++ Api,
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

%%--------------------------------------------------------------------
%% Cli group
%%--------------------------------------------------------------------

ctl_register_cmd(_) ->
    emqttd_ctl:register_cmd(test_cmd, {?MODULE, test_cmd}),
    erlang:yield(),
    timer:sleep(5),
    [{?MODULE, test_cmd}] = emqttd_ctl:lookup(test_cmd),
    emqttd_ctl:run(["test_cmd", "arg1", "arg2"]),
    emqttd_ctl:unregister_cmd(test_cmd).

test_cmd(["arg1", "arg2"]) ->
    ct:print("test_cmd is called");

test_cmd([]) ->
    io:format("test command").

cli_status(_) ->
    emqttd_cli:status([]).

cli_broker(_) ->
    emqttd_cli:broker([]),
    emqttd_cli:broker(["stats"]),
    emqttd_cli:broker(["metrics"]),
    emqttd_cli:broker(["pubsub"]).

cli_clients(_) ->
    emqttd_cli:clients(["list"]),
    emqttd_cli:clients(["show", "clientId"]),
    emqttd_cli:clients(["kick", "clientId"]).

cli_sessions(_) ->
    emqttd_cli:sessions(["list"]),
    emqttd_cli:sessions(["list", "persistent"]),
    emqttd_cli:sessions(["list", "transient"]),
    emqttd_cli:sessions(["show", "clientId"]).

cli_routes(_) ->
    emqttd:subscribe(<<"topic/route">>),
    emqttd_cli:routes(["list"]),
    emqttd_cli:routes(["show", "topic/route"]),
    emqttd:unsubscribe(<<"topic/route">>).

cli_topics(_) ->
    emqttd:subscribe(<<"topic">>),
    emqttd_cli:topics(["list"]),
    emqttd_cli:topics(["show", "topic"]),
    emqttd:unsubscribe(<<"topic">>).

cli_subscriptions(_) ->
    emqttd_cli:subscriptions(["list"]),
    emqttd_cli:subscriptions(["show", "clientId"]),
    emqttd_cli:subscriptions(["add", "clientId", "topic", "2"]),
    emqttd_cli:subscriptions(["del", "clientId", "topic"]).

cli_plugins(_) ->
    emqttd_cli:plugins(["list"]),
    emqttd_cli:plugins(["load", "emqttd_plugin_template"]),
    emqttd_cli:plugins(["unload", "emqttd_plugin_template"]).

cli_bridges(_) ->
    emqttd_cli:bridges(["list"]),
    emqttd_cli:bridges(["start", "a@127.0.0.1", "topic"]),
    emqttd_cli:bridges(["stop", "a@127.0.0.1", "topic"]).

cli_listeners(_) ->
    emqttd_cli:listeners([]).

cli_vm(_) ->
    emqttd_cli:vm([]),
    emqttd_cli:vm(["ports"]).

