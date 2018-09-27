
%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_client_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

all() ->
    [
        request_response_test
    ].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

request_response_test(_Config) ->
    {ok, Requester1, _} = emqx_client:start_link([{proto_ver, v5},
                                {properties, #{ 'Request-Response-Information' => 1}}]),
    {ok, Responser1, _} = emqx_client:start_link([{proto_ver, v5},
                                {properties, #{ 'Request-Response-Information' => 1}}]),
    {ok, ResponseTopic1} = emqx_client:sub_response_topic(Responser1, false, ?QOS_2, <<"request_response_test">>),
    ct:log("ResponseTopic: ~p",[ResponseTopic1]),
    ok = emqx_client:def_response(Responser1, <<"ResponseTest">>),
    {ok, <<"ResponseTest">>} = emqx_client:request(Requester1, <<"request_response_test">>, <<"request_payload">>, ?QOS_2),
    ok = emqx_client:def_response(Responser1, fun(<<"request_payload">>) ->
                                                     <<"ResponseFunctionTest">>;
                                                (_) ->
                                                     <<"404">>
                                             end),
    {ok, <<"ResponseFunctionTest">>} = emqx_client:request(Requester1, <<"request_response_test">>, <<"request_payload">>, ?QOS_2),
    {ok, <<"404">>} = emqx_client:request(Requester1, <<"request_response_test">>, <<"invalid_request">>, ?QOS_2),
    ok = emqx_client:disconnect(Responser1),
    ok = emqx_client:disconnect(Requester1),

    {ok, Requester2, _} = emqx_client:start_link([{proto_ver, v5},
                                {properties, #{ 'Request-Response-Information' => 1}}]),
    {ok, Responser2, _} = emqx_client:start_link([{proto_ver, v5},
                                {properties, #{ 'Request-Response-Information' => 1}}]),
    {ok, ResponseTopic2} = emqx_client:sub_response_topic(Responser2, false, ?QOS_1, <<"request_response_test">>),
    ct:log("ResponseTopic: ~p",[ResponseTopic2]),
    ok = emqx_client:def_response(Responser2, <<"ResponseTest">>),
    {ok, <<"ResponseTest">>} = emqx_client:request(Requester2, <<"request_response_test">>, <<"request_payload">>, ?QOS_1),
    ok = emqx_client:def_response(Responser2, fun(<<"request_payload">>) ->
                                                     <<"ResponseFunctionTest">>;
                                                (_) ->
                                                     <<"404">>
                                             end),
    {ok, <<"ResponseFunctionTest">>} = emqx_client:request(Requester2, <<"request_response_test">>, <<"request_payload">>, ?QOS_1),
    {ok, <<"404">>} = emqx_client:request(Requester2, <<"request_response_test">>, <<"invalid_request">>, ?QOS_1),
    ok = emqx_client:disconnect(Responser2),
    ok = emqx_client:disconnect(Requester2),

    {ok, Requester3, _} = emqx_client:start_link([{proto_ver, v5},
                                {properties, #{ 'Request-Response-Information' => 1}}]),
    {ok, Responser3, _} = emqx_client:start_link([{proto_ver, v5},
                                {properties, #{ 'Request-Response-Information' => 1}}]),
    {ok, ResponseTopic3} = emqx_client:sub_response_topic(Responser3, false, ?QOS_0, <<"request_response_test">>),
    ct:log("ResponseTopic: ~p",[ResponseTopic3]),
    ok = emqx_client:def_response(Responser3, <<"ResponseTest">>),
    {ok, <<"ResponseTest">>} = emqx_client:request(Requester3, <<"request_response_test">>, <<"request_payload">>, ?QOS_0),
    ok = emqx_client:def_response(Responser3, fun(<<"request_payload">>) ->
                                                     <<"ResponseFunctionTest">>;
                                                (_) ->
                                                     <<"404">>
                                             end),
    {ok, <<"ResponseFunctionTest">>} = emqx_client:request(Requester3, <<"request_response_test">>, <<"request_payload">>, ?QOS_0),
    {ok, <<"404">>} = emqx_client:request(Requester3, <<"request_response_test">>, <<"invalid_request">>, ?QOS_0),
    ok = emqx_client:disconnect(Responser3),
    ok = emqx_client:disconnect(Requester3).
