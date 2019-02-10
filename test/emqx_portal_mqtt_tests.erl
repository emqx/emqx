%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_portal_mqtt_tests).
-include_lib("eunit/include/eunit.hrl").

send_and_ack_test() ->
    %% delegate from gen_rpc to rpc for unit test
    Tester = self(),
    meck:new(emqx_client, [passthrough, no_history]),
    meck:expect(emqx_client, start_link, 1,
                fun(#{msg_handler := Hdlr}) -> {ok, Hdlr} end),
    meck:expect(emqx_client, connect, 1, {ok, dummy}),
    meck:expect(emqx_client, stop, 1, ok),
    meck:expect(emqx_client, publish, 2,
                fun(_Conn, Msg) ->
                        case rand:uniform(100) of
                            1 ->
                                {error, {dummy, inflight_full}};
                            _ ->
                                Tester ! {published, Msg},
                                {ok, Msg}
                        end
                end),
    try
        Max = 100,
        Batch = lists:seq(1, Max),
        {ok, Ref, Conn} = emqx_portal_mqtt:start(#{}),
        %% return last packet id as batch reference
        {ok, AckRef} = emqx_portal_mqtt:send(Conn, Batch),
        %% expect batch ack
        {ok, LastId} = collect_acks(Conn, Batch),
        %% asset received ack matches the batch ref returned in send API
        ?assertEqual(AckRef, LastId),
        ok = emqx_portal_mqtt:stop(Ref, Conn)
    after
        meck:unload(emqx_client)
    end.

collect_acks(_Conn, []) ->
    receive {batch_ack, Id} -> {ok, Id} end;
collect_acks(#{client_pid := Client} = Conn, [Id | Rest]) ->
    %% mocked for testing, should be a pid() at runtime
    #{puback := PubAckCallback} = Client,
    receive
        {published, Id} ->
            PubAckCallback(#{packet_id => Id, reason_code => dummy}),
            collect_acks(Conn, Rest)
    end.


