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

receive_messages(Count) ->
    receive_messages(Count, []).
receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count-1, [Msg|Msgs]);
        Other ->
            ct:log("~p~n", [Other]),
            receive_messages(Count, Msgs)
    after 10 ->
        Msgs
    end.

request_response_test(_Config) ->
    dbg:start(),
    dbg:tracer(),
    dbg:p(all, c),

    {ok, Requester, _} =
        emqx_client:start_request({shared, false},[{client_id, <<"requestclient1">>},
            {request_qos, ?QOS_2}]),
    {ok, Responser} = emqx_client:start_response({shared, false},<<"requestclient1">>, ?QOS_2,
        [{client_id, <<"responseclient2">>}]),
    ok = emqx_client:set_response(Responser, <<"ResponseInfomation">>),
        emqx_client:request(Requester, <<"requestclient1">>, <<"Request">>, ?QOS_2),
    ?assertEqual(2,length(receive_messages(4))),
    ok = emqx_client:disconnect(Responser),
    ok = emqx_client:disconnect(Requester).
