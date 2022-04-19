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
-module(emqx_mgmt_api_publish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(CLIENTID, <<"api_clientid">>).
-define(USERNAME, <<"api_username">>).

-define(TOPIC1, <<"api_topic1">>).
-define(TOPIC2, <<"api_topic2">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_publish_api(_) ->
    {ok, Client} = emqtt:start_link(#{
        username => <<"api_username">>, clientid => <<"api_clientid">>
    }),
    {ok, _} = emqtt:connect(Client),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC1),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC2),
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = #{topic => ?TOPIC1, payload => Payload},
    {ok, _} = emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body),
    ?assertEqual(receive_assert(?TOPIC1, 0, Payload), ok),
    emqtt:disconnect(Client).

t_publish_bulk_api(_) ->
    {ok, Client} = emqtt:start_link(#{
        username => <<"api_username">>, clientid => <<"api_clientid">>
    }),
    {ok, _} = emqtt:connect(Client),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC1),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC2),
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = [#{topic => ?TOPIC1, payload => Payload}, #{topic => ?TOPIC2, payload => Payload}],
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body),
    ResponseMap = emqx_json:decode(Response, [return_maps]),
    ?assertEqual(2, erlang:length(ResponseMap)),
    ?assertEqual(receive_assert(?TOPIC1, 0, Payload), ok),
    ?assertEqual(receive_assert(?TOPIC2, 0, Payload), ok),
    emqtt:disconnect(Client).

receive_assert(Topic, Qos, Payload) ->
    receive
        {publish, Message} ->
            ReceiveTopic = maps:get(topic, Message),
            ReceiveQos = maps:get(qos, Message),
            ReceivePayload = maps:get(payload, Message),
            ?assertEqual(ReceiveTopic, Topic),
            ?assertEqual(ReceiveQos, Qos),
            ?assertEqual(ReceivePayload, Payload),
            ok
    after 5000 ->
        timeout
    end.
