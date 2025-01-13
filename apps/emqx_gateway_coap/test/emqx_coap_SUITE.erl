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

-module(emqx_coap_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_gateway_test_utils,
    [
        request/2,
        request/3
    ]
).

-include_lib("er_coap_client/include/coap.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, <<
    "\n"
    "gateway.coap\n"
    "{\n"
    "    idle_timeout = 30s\n"
    "    enable_stats = false\n"
    "    mountpoint = \"\"\n"
    "    notify_type = qos\n"
    "    connection_required = true\n"
    "    subscribe_qos = qos1\n"
    "    publish_qos = qos1\n"
    "\n"
    "    listeners.udp.default\n"
    "    {bind = 5683}\n"
    "}\n"
>>).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).
-define(PS_PREFIX, "coap://127.0.0.1/ps").
-define(MQTT_PREFIX, "coap://127.0.0.1/mqtt").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_gateway_coap),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, ?CONF_DEFAULT},
            emqx_gateway,
            emqx_auth,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

init_per_testcase(t_connection_with_authn_failed, Config) ->
    ok = meck:new(emqx_access_control, [passthrough]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) -> {error, bad_username_or_password} end
    ),
    Config;
init_per_testcase(t_connection_with_expire, Config) ->
    ok = meck:new(emqx_access_control, [passthrough, no_history]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) ->
            {ok, #{is_superuser => false, expire_at => erlang:system_time(millisecond) + 100}}
        end
    ),
    snabbkaffe:start_trace(),
    Config;
init_per_testcase(t_heartbeat, Config) ->
    NewHeartbeat = 800,
    OldConf = emqx:get_raw_config([gateway, coap]),
    {ok, _} = emqx_gateway_conf:update_gateway(
        coap,
        OldConf#{<<"heartbeat">> => <<"1s">>}
    ),
    [
        {old_conf, OldConf},
        {new_heartbeat, NewHeartbeat}
        | Config
    ];
init_per_testcase(_, Config) ->
    ok = meck:new(emqx_access_control, [passthrough]),
    Config.

end_per_testcase(t_heartbeat, Config) ->
    OldConf = ?config(old_conf, Config),
    {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf),
    ok;
end_per_testcase(t_connection_with_expire, Config) ->
    snabbkaffe:stop(),
    meck:unload(emqx_access_control),
    Config;
end_per_testcase(_, Config) ->
    ok = meck:unload(emqx_access_control),
    Config.

default_config() ->
    ?CONF_DEFAULT.

mqtt_prefix() ->
    ?MQTT_PREFIX.

ps_prefix() ->
    ?PS_PREFIX.

restart_coap_with_connection_mode(Bool) ->
    Conf = emqx:get_raw_config([gateway, coap]),
    emqx_gateway_conf:update_gateway(
        coap,
        Conf#{<<"connection_required">> => atom_to_binary(Bool)}
    ).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connection(_) ->
    Action = fun(Channel) ->
        emqx_gateway_test_utils:meck_emqx_hook_calls(),

        %% connection
        Token = connection(Channel),

        timer:sleep(100),
        ?assertNotEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),

        ?assertMatch(
            ['client.connect' | _],
            emqx_gateway_test_utils:collect_emqx_hooks_calls()
        ),

        %% heartbeat
        {ok, changed, _} = send_heartbeat(Token),

        disconnection(Channel, Token),

        timer:sleep(100),
        ?assertEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        )
    end,
    do(Action),
    ok.

t_connection_with_short_param_name(_) ->
    Action = fun(Channel) ->
        %% connection
        Token = connection(Channel, true),

        timer:sleep(100),
        ?assertNotEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),

        %% heartbeat
        {ok, changed, _} = send_heartbeat(Token, true),

        disconnection(Channel, Token, true),

        timer:sleep(100),
        ?assertEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        )
    end,
    do(Action).

t_heartbeat(Config) ->
    Heartbeat = ?config(new_heartbeat, Config),
    Action = fun(Channel) ->
        Token = connection(Channel),

        timer:sleep(100),
        ?assertNotEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),

        %% must keep client connection alive
        Delay = Heartbeat div 2,
        lists:foreach(
            fun(_) ->
                ?assertMatch({ok, changed, _}, send_heartbeat(Token)),
                timer:sleep(Delay)
            end,
            lists:seq(1, 5)
        ),

        ?assertNotEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),
        %% The minimum timeout time is 1 second.
        %% 1.5 * Heartbeat + 0.5 * Heartbeat(< 1s) = 1.5 * 1 + 1 = 2.5
        timer:sleep(Heartbeat * 2 + 1000),
        ?assertEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),

        disconnection(Channel, Token),

        timer:sleep(100),
        ?assertEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        )
    end,
    do(Action).

t_connection_optional_params(_) ->
    UsernamePasswordAreOptional =
        fun(Channel) ->
            URI =
                ?MQTT_PREFIX ++
                    "/connection?clientid=client1",
            Req = make_req(post),
            {ok, created, Data} = do_request(Channel, URI, Req),
            #coap_content{payload = Token0} = Data,
            Token = binary_to_list(Token0),

            timer:sleep(100),
            ?assertNotEqual(
                [],
                emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
            ),

            disconnection(Channel, Token),

            timer:sleep(100),
            ?assertEqual(
                [],
                emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
            )
        end,
    ClientIdIsRequired =
        fun(Channel) ->
            URI =
                ?MQTT_PREFIX ++
                    "/connection",
            Req = make_req(post),
            {error, bad_request, _} = do_request(Channel, URI, Req)
        end,
    do(UsernamePasswordAreOptional),
    do(ClientIdIsRequired).

t_connection_with_authn_failed(_) ->
    ChId = {{127, 0, 0, 1}, 5683},
    {ok, Sock} = er_coap_udp_socket:start_link(),
    {ok, Channel} = er_coap_udp_socket:get_channel(Sock, ChId),
    URI =
        ?MQTT_PREFIX ++
            "/connection?clientid=client1&username=admin&password=public",
    Req = make_req(post),
    ?assertMatch({error, bad_request, _}, do_request(Channel, URI, Req)),

    timer:sleep(100),
    ?assertEqual(
        [],
        emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
    ),
    ok.

t_connection_with_expire(_) ->
    ChId = {{127, 0, 0, 1}, 5683},
    {ok, Sock} = er_coap_udp_socket:start_link(),
    {ok, Channel} = er_coap_udp_socket:get_channel(Sock, ChId),

    URI = ?MQTT_PREFIX ++ "/connection?clientid=client1",

    ?assertWaitEvent(
        begin
            Req = make_req(post),
            {ok, created, _Data} = do_request(Channel, URI, Req)
        end,
        #{
            ?snk_kind := conn_process_terminated,
            clientid := <<"client1">>,
            reason := {shutdown, expired}
        },
        5000
    ).

t_publish(_) ->
    %% can publish to a normal topic
    Topics = [
        <<"abc">>,
        %% can publish to a `/` leading topic
        <<"/abc">>
    ],
    Action = fun(Topic, Channel, Token) ->
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic), Token),

        %% Sub topic first
        emqx:subscribe(Topic),

        Req = make_req(post, Payload),
        {ok, changed, _} = do_request(Channel, URI, Req),

        receive
            {deliver, Topic, Msg} ->
                ?assertEqual(Topic, Msg#message.topic),
                ?assertEqual(Payload, Msg#message.payload)
        after 500 ->
            ?assert(false)
        end,
        true
    end,
    with_connection(Topics, Action).

t_publish_with_retain_qos_expiry(_) ->
    _ = meck:expect(
        emqx_access_control,
        authorize,
        fun(_, #{action_type := publish, qos := 1, retain := true}, _) ->
            allow
        end
    ),

    Topics = [<<"abc">>],
    Action = fun(Topic, Channel, Token) ->
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic), Token) ++ "&retain=true&qos=1&expiry=60",

        %% Sub topic first
        emqx:subscribe(Topic),

        Req = make_req(post, Payload),
        {ok, changed, _} = do_request(Channel, URI, Req),

        receive
            {deliver, Topic, Msg} ->
                ?assertEqual(Topic, Msg#message.topic),
                ?assertEqual(Payload, Msg#message.payload)
        after 500 ->
            ?assert(false)
        end,

        true
    end,
    with_connection(Topics, Action),

    _ = meck:validate(emqx_access_control).

t_subscribe(_) ->
    %% can subscribe to a normal topic
    Topics = [
        <<"abc">>,
        %% can subscribe to a `/` leading topic
        <<"/abc">>
    ],
    Fun = fun(Topic, Channel, Token) ->
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic), Token),
        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        %% ensure subscribe succeed
        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        %% publish a message
        emqx:publish(emqx_message:make(Topic, Payload)),
        {ok, content, Notify} = with_response(Channel),
        ?LOGT("observer get Notif=~p", [Notify]),

        #coap_content{payload = PayloadRecv} = Notify,

        ?assertEqual(Payload, PayloadRecv),
        true
    end,

    with_connection(Topics, Fun),

    %% subscription removed if coap client disconnected
    timer:sleep(100),
    lists:foreach(
        fun(Topic) ->
            ?assertEqual([], emqx:subscribers(Topic))
        end,
        Topics
    ).

t_subscribe_with_qos_opt(_) ->
    Topics = [
        {<<"abc">>, 0},
        {<<"/abc">>, 1},
        {<<"abc/d">>, 2}
    ],
    Fun = fun({Topic, Qos}, Channel, Token) ->
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic), Token) ++ "&qos=" ++ integer_to_list(Qos),
        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        %% ensure subscribe succeed
        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),
        ?assertEqual(Qos, maps:get(qos, emqx_broker:get_subopts(SubPid, Topic))),
        %% publish a message
        emqx:publish(emqx_message:make(Topic, Payload)),
        {ok, content, Notify} = with_response(Channel),
        ?LOGT("observer get Notif=~p", [Notify]),

        #coap_content{payload = PayloadRecv} = Notify,

        ?assertEqual(Payload, PayloadRecv),
        true
    end,

    with_connection(Topics, Fun),

    %% subscription removed if coap client disconnected
    timer:sleep(100),
    lists:foreach(
        fun({Topic, _Qos}) ->
            ?assertEqual([], emqx:subscribers(Topic))
        end,
        Topics
    ).

t_un_subscribe(_) ->
    %% can unsubscribe to a normal topic
    Topics = [
        <<"abc">>,
        %% can unsubscribe to a `/` leading topic
        <<"/abc">>
    ],
    Fun = fun(Topic, Channel, Token) ->
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic), Token),

        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        UnReq = make_req(get, Payload, [{observe, 1}]),
        {ok, nocontent, _} = do_request(Channel, URI, UnReq),
        ?LOGT("un observer topic:~ts~n", [Topic]),
        timer:sleep(100),
        ?assertEqual([], emqx:subscribers(Topic)),
        true
    end,

    with_connection(Topics, Fun).

t_observe_wildcard(_) ->
    Fun = fun(Channel, Token) ->
        %% resolve_url can't process wildcard with #
        Topic = <<"abc/+">>,
        Payload = <<"123">>,

        URI = pubsub_uri(binary_to_list(Topic), Token),
        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        %% Publish a message
        PubTopic = <<"abc/def">>,
        emqx:publish(emqx_message:make(PubTopic, Payload)),
        {ok, content, Notify} = with_response(Channel),

        ?LOGT("observer get Notif=~p", [Notify]),

        #coap_content{payload = PayloadRecv} = Notify,

        ?assertEqual(Payload, PayloadRecv),
        true
    end,

    with_connection(Fun).

t_clients_api(_) ->
    Fun = fun(_Channel, _Token) ->
        ClientId = <<"client1">>,
        %% list
        {200, #{data := [Client1]}} = request(get, "/gateways/coap/clients"),
        #{clientid := ClientId} = Client1,
        %% searching
        {200, #{data := [Client2]}} =
            request(
                get,
                "/gateways/coap/clients",
                [{<<"clientid">>, ClientId}]
            ),
        {200, #{data := [Client3]}} =
            request(
                get,
                "/gateways/coap/clients",
                [{<<"like_clientid">>, <<"cli">>}]
            ),
        %% lookup
        {200, Client4} =
            request(get, "/gateways/coap/clients/client1"),
        %% assert
        Client1 = Client2 = Client3 = Client4,
        %% assert keepalive
        ?assertEqual(15, maps:get(keepalive, Client4)),
        %% kickout
        {204, _} =
            request(delete, "/gateways/coap/clients/client1"),
        timer:sleep(200),
        {200, #{data := []}} = request(get, "/gateways/coap/clients"),
        false
    end,
    with_connection(Fun).

t_clients_subscription_api(_) ->
    Fun = fun(_Channel, _Token) ->
        Path = "/gateways/coap/clients/client1/subscriptions",
        %% list
        {200, []} = request(get, Path),
        %% create
        SubReq = #{
            topic => <<"tx">>,
            qos => 0,
            nl => 0,
            rap => 0,
            rh => 0
        },

        {201, SubsResp} = request(post, Path, SubReq),
        {200, [SubsResp2]} = request(get, Path),
        ?assertEqual(
            maps:get(topic, SubsResp),
            maps:get(topic, SubsResp2)
        ),

        %% check subscription_cnt
        {200, #{subscriptions_cnt := 1}} = request(get, "/gateways/coap/clients/client1"),

        {204, _} = request(delete, Path ++ "/tx"),

        {200, []} = request(get, Path),
        true
    end,
    with_connection(Fun).

t_clients_get_subscription_api(_) ->
    Fun = fun(Channel, Token) ->
        Path = "/gateways/coap/clients/client1/subscriptions",
        %% list
        {200, []} = request(get, Path),

        observe(Channel, Token, true),

        {200, [Subs]} = request(get, Path),

        ?assertEqual(<<"coap/observe">>, maps:get(topic, Subs)),

        observe(Channel, Token, false),

        {200, []} = request(get, Path),
        true
    end,
    with_connection(Fun).

t_on_offline_event(_) ->
    Fun = fun(Channel) ->
        emqx_hooks:add('client.connected', {emqx_sys, on_client_connected, []}, 1000),
        emqx_hooks:add('client.disconnected', {emqx_sys, on_client_disconnected, []}, 1000),

        ConnectedSub = <<"$SYS/brokers/+/gateway/coap/clients/+/connected">>,
        emqx_broker:subscribe(ConnectedSub),
        timer:sleep(100),

        Token = connection(Channel),
        ?assertMatch(#message{}, receive_deliver(500)),

        DisconnectedSub = <<"$SYS/brokers/+/gateway/coap/clients/+/disconnected">>,
        emqx_broker:subscribe(DisconnectedSub),
        timer:sleep(100),

        disconnection(Channel, Token),

        ?assertMatch(#message{}, receive_deliver(500)),

        emqx_broker:unsubscribe(ConnectedSub),
        emqx_broker:unsubscribe(DisconnectedSub),

        emqx_hooks:del('client.connected', {emqx_sys, on_client_connected}),
        emqx_hooks:del('client.disconnected', {emqx_sys, on_client_disconnected}),
        timer:sleep(500)
    end,
    do(Fun).

t_connectionless_pubsub(_) ->
    restart_coap_with_connection_mode(false),
    Fun = fun(Channel) ->
        Topic = <<"t/a">>,
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic)),
        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        %% ensure subscribe succeed
        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        %% publish a message
        Req2 = make_req(post, Payload),
        {ok, changed, _} = do_request(Channel, URI, Req2),

        {ok, content, Notify} = with_response(Channel),
        ?LOGT("observer get Notif=~p", [Notify]),

        #coap_content{payload = PayloadRecv} = Notify,

        ?assertEqual(Payload, PayloadRecv)
    end,
    do(Fun),
    restart_coap_with_connection_mode(true).

%%--------------------------------------------------------------------
%% helpers

send_heartbeat(Token) ->
    send_heartbeat(Token, false).

send_heartbeat(Token, ShortenParamName) ->
    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    ?LOGT("send heartbeat request:~ts~n", [URI]),
    er_coap_client:request(put, URI).

connection(Channel) ->
    connection(Channel, false).

connection(Channel, ShortenParamName) ->
    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries = #{
        "clientid" => <<"client1">>,
        "username" => <<"admin">>,
        "password" => <<"public">>
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    Req = make_req(post),
    {ok, created, Data} = do_request(Channel, URI, Req),
    #coap_content{payload = BinToken} = Data,
    binary_to_list(BinToken).

disconnection(Channel, Token) ->
    disconnection(Channel, Token, false).

disconnection(Channel, Token, ShortenParamName) ->
    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    Req = make_req(delete),
    {ok, deleted, _} = do_request(Channel, URI, Req).

observe(Channel, Token, Observe) ->
    observe(Channel, Token, Observe, false).

observe(Channel, Token, true, ShortenParamName) ->
    Prefix = ?PS_PREFIX ++ "/coap/observe",
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    Req = make_req(get, <<>>, [{observe, 0}]),
    {ok, content, _Data} = do_request(Channel, URI, Req),
    ok;
observe(Channel, Token, false, ShortenParamName) ->
    Prefix = ?PS_PREFIX ++ "/coap/observe",
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    Req = make_req(get, <<>>, [{observe, 1}]),
    {ok, nocontent, _Data} = do_request(Channel, URI, Req),
    ok.

pubsub_uri(Topic) when is_list(Topic) ->
    ?PS_PREFIX ++ "/" ++ Topic.

pubsub_uri(Topic, Token) ->
    pubsub_uri(Topic, Token, false).

pubsub_uri(Topic, Token, ShortenParamName) when is_list(Topic), is_list(Token) ->
    Prefix = ?PS_PREFIX ++ "/" ++ Topic,
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    compose_uri(Prefix, Queries, ShortenParamName).

make_req(Method) ->
    make_req(Method, <<>>).

make_req(Method, Payload) ->
    make_req(Method, Payload, []).

make_req(Method, Payload, Opts) ->
    er_coap_message:request(con, Method, Payload, Opts).

do_request(Channel, URI, #coap_message{options = Opts} = Req) ->
    {_, _, Path, Query} = er_coap_client:resolve_uri(URI),
    Opts2 = [{uri_path, Path}, {uri_query, Query} | Opts],
    Req2 = Req#coap_message{options = Opts2},
    ?LOGT("send request:~ts~nReq:~p~n", [URI, Req2]),

    {ok, _} = er_coap_channel:send(Channel, Req2),
    with_response(Channel).

with_response(Channel) ->
    receive
        {coap_response, _ChId, Channel, _Ref, Message = #coap_message{method = Code}} ->
            return_response(Code, Message);
        {coap_error, _ChId, Channel, _Ref, reset} ->
            {error, reset}
    after 2000 ->
        {error, timeout}
    end.

return_response({ok, Code}, Message) ->
    {ok, Code, er_coap_message:get_content(Message)};
return_response({error, Code}, #coap_message{payload = <<>>}) ->
    {error, Code};
return_response({error, Code}, Message) ->
    {error, Code, er_coap_message:get_content(Message)}.

do(Fun) ->
    ChId = {{127, 0, 0, 1}, 5683},
    {ok, Sock} = er_coap_udp_socket:start_link(),
    {ok, Channel} = er_coap_udp_socket:get_channel(Sock, ChId),
    %% send and receive
    Res = Fun(Channel),
    %% terminate the processes
    er_coap_channel:close(Channel),
    er_coap_udp_socket:close(Sock),
    Res.

with_connection(Action) ->
    Fun = fun(Channel) ->
        Token = connection(Channel),
        timer:sleep(100),
        _ = Action(Channel, Token) andalso disconnection(Channel, Token),
        timer:sleep(100)
    end,
    do(Fun).

with_connection(Checks, Action) ->
    Fun = fun(Channel) ->
        Token = connection(Channel),
        timer:sleep(100),
        lists:foreach(fun(E) -> Action(E, Channel, Token) end, Checks),
        disconnection(Channel, Token),
        timer:sleep(100)
    end,
    do(Fun).

receive_deliver(Wait) ->
    receive
        {deliver, _, Msg} ->
            Msg
    after Wait ->
        {error, timeout}
    end.

get_field(type, #coap_message{type = Type}) ->
    Type;
get_field(method, #coap_message{method = Method}) ->
    Method.

compose_uri(URI, Queries, ShortenParamName) ->
    Queries1 = shorten_param_name(ShortenParamName, Queries),
    case maps:size(Queries1) of
        0 ->
            URI;
        _ ->
            URI ++ "?" ++ uri_string:compose_query(maps:to_list(Queries1))
    end.

shorten_param_name(false, Queries) ->
    Queries;
shorten_param_name(true, Queries) ->
    lists:foldl(
        fun({Short, Long}, Acc) ->
            case maps:take(Long, Acc) of
                error ->
                    Acc;
                {Value, Acc1} ->
                    maps:put(Short, Value, Acc1)
            end
        end,
        Queries,
        emqx_coap_message:query_params_mapping_table()
    ).
