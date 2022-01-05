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

-include_lib("er_coap_client/include/coap.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, <<"
gateway.coap
{
    idle_timeout = 30s
    enable_stats = false
    mountpoint = \"\"
    notify_type = qos
    connection_required = true
    subscribe_qos = qos1
    publish_qos = qos1

    listeners.udp.default
    {bind = 5683}
}
">>).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).
-define(PS_PREFIX, "coap://127.0.0.1/ps").
-define(MQTT_PREFIX, "coap://127.0.0.1/mqtt").


all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([emqx_gateway], fun set_special_cfg/1),
    Config.

set_special_cfg(emqx_gateway) ->
    ok = emqx_config:init_load(emqx_gateway_schema, ?CONF_DEFAULT);

set_special_cfg(_) ->
    ok.

end_per_suite(Config) ->
    {ok, _} = emqx:remove_config([<<"gateway">>,<<"coap">>]),
    emqx_common_test_helpers:stop_apps([emqx_gateway]),
    Config.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------
t_connection(_Config) ->
    Action = fun(Channel) ->
                     %% connection
                     Token = connection(Channel),

                     timer:sleep(100),
                     ?assertNotEqual([], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)),

                     %% heartbeat
                     HeartURI = ?MQTT_PREFIX ++ "/connection?clientid=client1&token=" ++ Token,
                     ?LOGT("send heartbeat request:~ts~n", [HeartURI]),
                     {ok, changed, _} = er_coap_client:request(put, HeartURI),

                     disconnection(Channel, Token),

                     timer:sleep(100),
                     ?assertEqual([], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>))
             end,
    do(Action).


t_publish(_Config) ->
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
                     after
                         500 ->
                             ?assert(false)
                     end
             end,

    with_connection(Action).


%t_publish_authz_deny(_Config) ->
%    Action = fun(Channel, Token) ->
%                     Topic = <<"/abc">>,
%                     Payload = <<"123">>,
%                     InvalidToken = lists:reverse(Token),
%
%                     TopicStr = binary_to_list(Topic),
%                     URI = ?PS_PREFIX ++ TopicStr ++ "?clientid=client1&token=" ++ InvalidToken,
%
%                     %% Sub topic first
%                     emqx:subscribe(Topic),
%
%                     Req = make_req(post, Payload),
%                     Result = do_request(Channel, URI, Req),
%                     ?assertEqual({error, reset}, Result)
%             end,
%
%    with_connection(Action).

t_subscribe(_Config) ->
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


t_un_subscribe(_Config) ->
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

t_observe_wildcard(_Config) ->
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

connection(Channel) ->
    URI = ?MQTT_PREFIX ++ "/connection?clientid=client1&username=admin&password=public",
    Req = make_req(post),
    {ok, created, Data} = do_request(Channel, URI, Req),
    #coap_content{payload = BinToken} = Data,
    binary_to_list(BinToken).

disconnection(Channel, Token) ->
    %% delete
    URI = ?MQTT_PREFIX ++ "/connection?clientid=client1&token=" ++ Token,
    Req = make_req(delete),
    {ok, deleted, _} = do_request(Channel, URI, Req).

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
        {coap_response, _ChId, Channel, _Ref, Message=#coap_message{method=Code}} ->
            return_response(Code, Message);
        {coap_error, _ChId, Channel, _Ref, reset} ->
            {error, reset}
    after 2000 ->
            {error, timeout}
    end.

return_response({ok, Code}, Message) ->
    {ok, Code, er_coap_message:get_content(Message)};
return_response({error, Code}, #coap_message{payload= <<>>}) ->
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
