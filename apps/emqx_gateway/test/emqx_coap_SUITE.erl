%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    ok = emqx_common_test_helpers:load_config(emqx_gateway_schema, ?CONF_DEFAULT),
    emqx_mgmt_api_test_util:init_suite([emqx_authn, emqx_gateway]),
    ok = meck:new(emqx_access_control, [passthrough, no_history, no_link]),
    Config.

end_per_suite(_) ->
    meck:unload(emqx_access_control),
    {ok, _} = emqx:remove_config([<<"gateway">>, <<"coap">>]),
    emqx_mgmt_api_test_util:end_suite([emqx_gateway, emqx_authn]).

init_per_testcase(t_connection_with_authn_failed, Config) ->
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) -> {error, bad_username_or_password} end
    ),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(t_connection_with_authn_failed, Config) ->
    ok = meck:delete(emqx_access_control, authenticate, 1),
    Config;
end_per_testcase(_, Config) ->
    Config.

default_config() ->
    ?CONF_DEFAULT.

mqtt_prefix() ->
    ?MQTT_PREFIX.

ps_prefix() ->
    ?PS_PREFIX.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connection(_) ->
    Action = fun(Channel) ->
        %% connection
        Token = connection(Channel),

        timer:sleep(100),
        ?assertNotEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),

        %% heartbeat
        HeartURI =
            ?MQTT_PREFIX ++
                "/connection?clientid=client1&token=" ++
                Token,

        ?LOGT("send heartbeat request:~ts~n", [HeartURI]),
        {ok, changed, _} = er_coap_client:request(put, HeartURI),

        disconnection(Channel, Token),

        timer:sleep(100),
        ?assertEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        )
    end,
    do(Action).

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

t_publish(_) ->
    Action = fun(Channel, Token) ->
        Topic = <<"/abc">>,
        Payload = <<"123">>,

        TopicStr = binary_to_list(Topic),
        URI = ?PS_PREFIX ++ TopicStr ++ "?clientid=client1&token=" ++ Token,

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
        end
    end,
    with_connection(Action).

t_subscribe(_) ->
    Topic = <<"/abc">>,
    Fun = fun(Channel, Token) ->
        TopicStr = binary_to_list(Topic),
        Payload = <<"123">>,

        URI = ?PS_PREFIX ++ TopicStr ++ "?clientid=client1&token=" ++ Token,
        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        %% Publish a message
        emqx:publish(emqx_message:make(Topic, Payload)),
        {ok, content, Notify} = with_response(Channel),
        ?LOGT("observer get Notif=~p", [Notify]),

        #coap_content{payload = PayloadRecv} = Notify,

        ?assertEqual(Payload, PayloadRecv)
    end,

    with_connection(Fun),
    timer:sleep(100),

    ?assertEqual([], emqx:subscribers(Topic)).

t_un_subscribe(_) ->
    Topic = <<"/abc">>,
    Fun = fun(Channel, Token) ->
        TopicStr = binary_to_list(Topic),
        Payload = <<"123">>,

        URI = ?PS_PREFIX ++ TopicStr ++ "?clientid=client1&token=" ++ Token,

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
        ?assertEqual([], emqx:subscribers(Topic))
    end,

    with_connection(Fun).

t_observe_wildcard(_) ->
    Fun = fun(Channel, Token) ->
        %% resolve_url can't process wildcard with #
        Topic = <<"/abc/+">>,
        TopicStr = binary_to_list(Topic),
        Payload = <<"123">>,

        URI = ?PS_PREFIX ++ TopicStr ++ "?clientid=client1&token=" ++ Token,
        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        %% Publish a message
        PubTopic = <<"/abc/def">>,
        emqx:publish(emqx_message:make(PubTopic, Payload)),
        {ok, content, Notify} = with_response(Channel),

        ?LOGT("observer get Notif=~p", [Notify]),

        #coap_content{payload = PayloadRecv} = Notify,

        ?assertEqual(Payload, PayloadRecv)
    end,

    with_connection(Fun).

t_clients_api(_) ->
    Fun = fun(_Channel, _Token) ->
        ClientId = <<"client1">>,
        %% list
        {200, #{data := [Client1]}} = request(get, "/gateway/coap/clients"),
        #{clientid := ClientId} = Client1,
        %% searching
        {200, #{data := [Client2]}} =
            request(
                get,
                "/gateway/coap/clients",
                [{<<"clientid">>, ClientId}]
            ),
        {200, #{data := [Client3]}} =
            request(
                get,
                "/gateway/coap/clients",
                [{<<"like_clientid">>, <<"cli">>}]
            ),
        %% lookup
        {200, Client4} =
            request(get, "/gateway/coap/clients/client1"),
        %% assert
        Client1 = Client2 = Client3 = Client4,
        %% kickout
        {204, _} =
            request(delete, "/gateway/coap/clients/client1"),
        timer:sleep(200),
        {200, #{data := []}} = request(get, "/gateway/coap/clients")
    end,
    with_connection(Fun).

t_clients_subscription_api(_) ->
    Fun = fun(_Channel, _Token) ->
        Path = "/gateway/coap/clients/client1/subscriptions",
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

        {204, _} = request(delete, Path ++ "/tx"),

        {200, []} = request(get, Path)
    end,
    with_connection(Fun).

t_clients_get_subscription_api(_) ->
    Fun = fun(Channel, Token) ->
        Path = "/gateway/coap/clients/client1/subscriptions",
        %% list
        {200, []} = request(get, Path),

        observe(Channel, Token, true),

        {200, [Subs]} = request(get, Path),

        ?assertEqual(<<"/coap/observe">>, maps:get(topic, Subs)),

        observe(Channel, Token, false),

        {200, []} = request(get, Path)
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

%%--------------------------------------------------------------------
%% helpers

connection(Channel) ->
    URI =
        ?MQTT_PREFIX ++
            "/connection?clientid=client1&username=admin&password=public",
    Req = make_req(post),
    {ok, created, Data} = do_request(Channel, URI, Req),
    #coap_content{payload = BinToken} = Data,
    binary_to_list(BinToken).

disconnection(Channel, Token) ->
    %% delete
    URI = ?MQTT_PREFIX ++ "/connection?clientid=client1&token=" ++ Token,
    Req = make_req(delete),
    {ok, deleted, _} = do_request(Channel, URI, Req).

observe(Channel, Token, true) ->
    URI = ?PS_PREFIX ++ "/coap/observe?clientid=client1&token=" ++ Token,
    Req = make_req(get, <<>>, [{observe, 0}]),
    {ok, content, _Data} = do_request(Channel, URI, Req),
    ok;
observe(Channel, Token, false) ->
    URI = ?PS_PREFIX ++ "/coap/observe?clientid=client1&token=" ++ Token,
    Req = make_req(get, <<>>, [{observe, 1}]),
    {ok, nocontent, _Data} = do_request(Channel, URI, Req),
    ok.

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
        Action(Channel, Token),
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
