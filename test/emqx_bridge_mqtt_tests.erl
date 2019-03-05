%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_mqtt_tests).
-include_lib("eunit/include/eunit.hrl").
-include("emqx_mqtt.hrl").

send_and_ack_test() ->
    %% delegate from gen_rpc to rpc for unit test
    meck:new(emqx_client, [passthrough, no_history]),
    meck:expect(emqx_client, start_link, 1,
                fun(#{msg_handler := Hdlr}) ->
                        {ok, spawn_link(fun() -> fake_client(Hdlr) end)}
                end),
    meck:expect(emqx_client, connect, 1, {ok, dummy}),
    meck:expect(emqx_client, stop, 1,
                fun(Pid) -> Pid ! stop end),
    meck:expect(emqx_client, publish, 2,
                fun(Client, Msg) ->
                        Client ! {publish, Msg},
                        {ok, Msg} %% as packet id
                end),
    try
        Max = 100,
        Batch = lists:seq(1, Max),
        {ok, Ref, Conn} = emqx_bridge_mqtt:start(#{address => "127.0.0.1:1883"}),
        %% return last packet id as batch reference
        {ok, AckRef} = emqx_bridge_mqtt:send(Conn, Batch),
        %% expect batch ack
        receive {batch_ack, AckRef} -> ok end,
        ok = emqx_bridge_mqtt:stop(Ref, Conn)
    after
        meck:unload(emqx_client)
    end.

fake_client(#{puback := PubAckCallback} = Hdlr) ->
    receive
        {publish, PktId} ->
            PubAckCallback(#{packet_id => PktId, reason_code => ?RC_SUCCESS}),
            fake_client(Hdlr);
        stop ->
            exit(normal)
    end.
