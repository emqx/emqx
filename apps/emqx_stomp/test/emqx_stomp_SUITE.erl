%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_stomp_SUITE).

-include_lib("emqx_stomp/include/emqx_stomp.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(HEARTBEAT, <<$\n>>).

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_stomp]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_stomp]).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connect(_) ->
    %% Connect should be succeed
    with_connection(fun(Sock) ->
                        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                                     [{<<"accept-version">>, ?STOMP_VER},
                                                      {<<"host">>, <<"127.0.0.1:61613">>},
                                                      {<<"login">>, <<"guest">>},
                                                      {<<"passcode">>, <<"guest">>},
                                                      {<<"heart-beat">>, <<"1000,2000">>}])),
                        {ok, Data} = gen_tcp:recv(Sock, 0),
                        {ok, Frame = #stomp_frame{command = <<"CONNECTED">>,
                                                  headers = _,
                                                  body    = _}, _} = parse(Data),
                        <<"2000,1000">> = proplists:get_value(<<"heart-beat">>, Frame#stomp_frame.headers),

                        gen_tcp:send(Sock, serialize(<<"DISCONNECT">>,
                                                     [{<<"receipt">>, <<"12345">>}])),

                        {ok, Data1} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"RECEIPT">>,
                                          headers = [{<<"receipt-id">>, <<"12345">>}],
                                          body    = _}, _} = parse(Data1)
                    end),

    %% Connect will be failed, because of bad login or passcode
    with_connection(fun(Sock) ->
                        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                                     [{<<"accept-version">>, ?STOMP_VER},
                                                      {<<"host">>, <<"127.0.0.1:61613">>},
                                                      {<<"login">>, <<"admin">>},
                                                      {<<"passcode">>, <<"admin">>},
                                                      {<<"heart-beat">>, <<"1000,2000">>}])),
                        {ok, Data} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"ERROR">>,
                                          headers = _,
                                          body    = <<"Login or passcode error!">>}, _} = parse(Data)
                    end),

    %% Connect will be failed, because of bad version
    with_connection(fun(Sock) ->
                        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                                     [{<<"accept-version">>, <<"2.0,2.1">>},
                                                      {<<"host">>, <<"127.0.0.1:61613">>},
                                                      {<<"login">>, <<"guest">>},
                                                      {<<"passcode">>, <<"guest">>},
                                                      {<<"heart-beat">>, <<"1000,2000">>}])),
                        {ok, Data} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"ERROR">>,
                                          headers = _,
                                          body    = <<"Supported protocol versions < 1.2">>}, _} = parse(Data)
                    end).

t_heartbeat(_) ->
    %% Test heart beat
    with_connection(fun(Sock) ->
                        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                                    [{<<"accept-version">>, ?STOMP_VER},
                                                     {<<"host">>, <<"127.0.0.1:61613">>},
                                                     {<<"login">>, <<"guest">>},
                                                     {<<"passcode">>, <<"guest">>},
                                                     {<<"heart-beat">>, <<"1000,2000">>}])),
                        {ok, Data} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"CONNECTED">>,
                                          headers = _,
                                          body    = _}, _} = parse(Data),

                        {ok, ?HEARTBEAT} = gen_tcp:recv(Sock, 0),
                        %% Server will close the connection because never receive the heart beat from client
                        {error, closed} = gen_tcp:recv(Sock, 0)
                    end).

t_subscribe(_) ->
    with_connection(fun(Sock) ->
                        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                                    [{<<"accept-version">>, ?STOMP_VER},
                                                     {<<"host">>, <<"127.0.0.1:61613">>},
                                                     {<<"login">>, <<"guest">>},
                                                     {<<"passcode">>, <<"guest">>},
                                                     {<<"heart-beat">>, <<"0,0">>}])),
                        {ok, Data} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"CONNECTED">>,
                                          headers = _,
                                          body    = _}, _} = parse(Data),

                        %% Subscribe
                        gen_tcp:send(Sock, serialize(<<"SUBSCRIBE">>,
                                                    [{<<"id">>, 0},
                                                     {<<"destination">>, <<"/queue/foo">>},
                                                     {<<"ack">>, <<"auto">>}])),

                        %% 'user-defined' header will be retain
                        gen_tcp:send(Sock, serialize(<<"SEND">>,
                                                    [{<<"destination">>, <<"/queue/foo">>},
                                                     {<<"user-defined">>, <<"emq">>}],
                                                    <<"hello">>)),

                        {ok, Data1} = gen_tcp:recv(Sock, 0, 1000),
                        {ok, Frame = #stomp_frame{command = <<"MESSAGE">>,
                                                  headers = _,
                                                  body    = <<"hello">>}, _} = parse(Data1),
                        lists:foreach(fun({Key, Val}) ->
                                          Val = proplists:get_value(Key, Frame#stomp_frame.headers)
                                      end, [{<<"destination">>,  <<"/queue/foo">>},
                                            {<<"subscription">>, <<"0">>},
                                            {<<"user-defined">>, <<"emq">>}]),

                        %% Unsubscribe
                        gen_tcp:send(Sock, serialize(<<"UNSUBSCRIBE">>,
                                                    [{<<"id">>, 0},
                                                     {<<"receipt">>, <<"12345">>}])),

                        {ok, Data2} = gen_tcp:recv(Sock, 0, 1000),

                        {ok, #stomp_frame{command = <<"RECEIPT">>,
                                          headers = [{<<"receipt-id">>, <<"12345">>}],
                                          body    = _}, _} = parse(Data2),

                        gen_tcp:send(Sock, serialize(<<"SEND">>,
                                                    [{<<"destination">>, <<"/queue/foo">>}],
                                                    <<"You will not receive this msg">>)),

                        {error, timeout} = gen_tcp:recv(Sock, 0, 500)
                    end).

t_transaction(_) ->
    with_connection(fun(Sock) ->
                        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                                     [{<<"accept-version">>, ?STOMP_VER},
                                                      {<<"host">>, <<"127.0.0.1:61613">>},
                                                      {<<"login">>, <<"guest">>},
                                                      {<<"passcode">>, <<"guest">>},
                                                      {<<"heart-beat">>, <<"0,0">>}])),
                        {ok, Data} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"CONNECTED">>,
                                                  headers = _,
                                                  body    = _}, _} = parse(Data),

                        %% Subscribe
                        gen_tcp:send(Sock, serialize(<<"SUBSCRIBE">>,
                                                    [{<<"id">>, 0},
                                                     {<<"destination">>, <<"/queue/foo">>},
                                                     {<<"ack">>, <<"auto">>}])),

                        %% Transaction: tx1
                        gen_tcp:send(Sock, serialize(<<"BEGIN">>,
                                                    [{<<"transaction">>, <<"tx1">>}])),

                        gen_tcp:send(Sock, serialize(<<"SEND">>,
                                                    [{<<"destination">>, <<"/queue/foo">>},
                                                     {<<"transaction">>, <<"tx1">>}],
                                                    <<"hello">>)),

                        %% You will not receive any messages
                        {error, timeout} = gen_tcp:recv(Sock, 0, 1000),

                        gen_tcp:send(Sock, serialize(<<"SEND">>,
                                                    [{<<"destination">>, <<"/queue/foo">>},
                                                     {<<"transaction">>, <<"tx1">>}],
                                                    <<"hello again">>)),

                        gen_tcp:send(Sock, serialize(<<"COMMIT">>,
                                                    [{<<"transaction">>, <<"tx1">>}])),

                        ct:sleep(1000),
                        {ok, Data1} = gen_tcp:recv(Sock, 0, 500),

                        {ok, #stomp_frame{command = <<"MESSAGE">>,
                                          headers = _,
                                          body    = <<"hello">>}, Rest1} = parse(Data1),

                        %{ok, Data2} = gen_tcp:recv(Sock, 0, 500),
                        {ok, #stomp_frame{command = <<"MESSAGE">>,
                                          headers = _,
                                          body    = <<"hello again">>}, _Rest2} = parse(Rest1),

                        %% Transaction: tx2
                        gen_tcp:send(Sock, serialize(<<"BEGIN">>,
                                                    [{<<"transaction">>, <<"tx2">>}])),

                        gen_tcp:send(Sock, serialize(<<"SEND">>,
                                                    [{<<"destination">>, <<"/queue/foo">>},
                                                     {<<"transaction">>, <<"tx2">>}],
                                                    <<"hello">>)),

                        gen_tcp:send(Sock, serialize(<<"ABORT">>,
                                                    [{<<"transaction">>, <<"tx2">>}])),

                        %% You will not receive any messages
                        {error, timeout} = gen_tcp:recv(Sock, 0, 1000),

                        gen_tcp:send(Sock, serialize(<<"DISCONNECT">>,
                                                     [{<<"receipt">>, <<"12345">>}])),

                        {ok, Data3} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"RECEIPT">>,
                                          headers = [{<<"receipt-id">>, <<"12345">>}],
                                          body    = _}, _} = parse(Data3)
                    end).

t_receipt_in_error(_) ->
    with_connection(fun(Sock) ->
                        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                                     [{<<"accept-version">>, ?STOMP_VER},
                                                      {<<"host">>, <<"127.0.0.1:61613">>},
                                                      {<<"login">>, <<"guest">>},
                                                      {<<"passcode">>, <<"guest">>},
                                                      {<<"heart-beat">>, <<"0,0">>}])),
                        {ok, Data} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"CONNECTED">>,
                                          headers = _,
                                          body    = _}, _} = parse(Data),

                        gen_tcp:send(Sock, serialize(<<"ABORT">>,
                                                    [{<<"transaction">>, <<"tx1">>},
                                                     {<<"receipt">>, <<"12345">>}])),

                        {ok, Data1} = gen_tcp:recv(Sock, 0),
                        {ok, Frame = #stomp_frame{command = <<"ERROR">>,
                                          headers = _,
                                          body    = <<"Transaction tx1 not found">>}, _} = parse(Data1),

                         <<"12345">> = proplists:get_value(<<"receipt-id">>, Frame#stomp_frame.headers)
                    end).

t_ack(_) ->
    with_connection(fun(Sock) ->
                        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                                     [{<<"accept-version">>, ?STOMP_VER},
                                                      {<<"host">>, <<"127.0.0.1:61613">>},
                                                      {<<"login">>, <<"guest">>},
                                                      {<<"passcode">>, <<"guest">>},
                                                      {<<"heart-beat">>, <<"0,0">>}])),
                        {ok, Data} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"CONNECTED">>,
                                          headers = _,
                                          body    = _}, _} = parse(Data),

                        %% Subscribe
                        gen_tcp:send(Sock, serialize(<<"SUBSCRIBE">>,
                                                    [{<<"id">>, 0},
                                                     {<<"destination">>, <<"/queue/foo">>},
                                                     {<<"ack">>, <<"client">>}])),

                        gen_tcp:send(Sock, serialize(<<"SEND">>,
                                                    [{<<"destination">>, <<"/queue/foo">>}],
                                                    <<"ack test">>)),

                        {ok, Data1} = gen_tcp:recv(Sock, 0),
                        {ok, Frame = #stomp_frame{command = <<"MESSAGE">>,
                                                  headers = _,
                                                  body    = <<"ack test">>}, _} = parse(Data1),

                        AckId = proplists:get_value(<<"ack">>, Frame#stomp_frame.headers),

                        gen_tcp:send(Sock, serialize(<<"ACK">>,
                                                    [{<<"id">>, AckId},
                                                     {<<"receipt">>, <<"12345">>}])),

                        {ok, Data2} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"RECEIPT">>,
                                                  headers = [{<<"receipt-id">>, <<"12345">>}],
                                                  body    = _}, _} = parse(Data2),

                        gen_tcp:send(Sock, serialize(<<"SEND">>,
                                                    [{<<"destination">>, <<"/queue/foo">>}],
                                                    <<"nack test">>)),

                        {ok, Data3} = gen_tcp:recv(Sock, 0),
                        {ok, Frame1 = #stomp_frame{command = <<"MESSAGE">>,
                                                  headers = _,
                                                  body    = <<"nack test">>}, _} = parse(Data3),

                        AckId1 = proplists:get_value(<<"ack">>, Frame1#stomp_frame.headers),

                        gen_tcp:send(Sock, serialize(<<"NACK">>,
                                                    [{<<"id">>, AckId1},
                                                     {<<"receipt">>, <<"12345">>}])),

                        {ok, Data4} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"RECEIPT">>,
                                                  headers = [{<<"receipt-id">>, <<"12345">>}],
                                                  body    = _}, _} = parse(Data4)
                    end).

t_1000_msg_send(_) ->
    with_connection(fun(Sock) ->
        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                     [{<<"accept-version">>, ?STOMP_VER},
                                      {<<"host">>, <<"127.0.0.1:61613">>},
                                      {<<"login">>, <<"guest">>},
                                      {<<"passcode">>, <<"guest">>},
                                      {<<"heart-beat">>, <<"0,0">>}])),
        {ok, Data} = gen_tcp:recv(Sock, 0),
        {ok, #stomp_frame{command = <<"CONNECTED">>,
                          headers = _,
                          body    = _}, _} = parse(Data),

        Topic = <<"/queue/foo">>,
        SendFun = fun() ->
            gen_tcp:send(Sock, serialize(<<"SEND">>,
                                        [{<<"destination">>, Topic}],
                                        <<"msgtest">>))
        end,

        RecvFun = fun() ->
            receive
                {deliver, Topic, _Msg}->
                    ok
            after 5000 ->
                      ?assert(false, "waiting message timeout")
            end
        end,

        emqx:subscribe(Topic),
        lists:foreach(fun(_) -> SendFun() end, lists:seq(1, 1000)),
        lists:foreach(fun(_) -> RecvFun() end, lists:seq(1, 1000))
    end).

t_sticky_packets_truncate_after_headers(_) ->
    with_connection(fun(Sock) ->
        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                     [{<<"accept-version">>, ?STOMP_VER},
                                      {<<"host">>, <<"127.0.0.1:61613">>},
                                      {<<"login">>, <<"guest">>},
                                      {<<"passcode">>, <<"guest">>},
                                      {<<"heart-beat">>, <<"0,0">>}])),
        {ok, Data} = gen_tcp:recv(Sock, 0),
        {ok, #stomp_frame{command = <<"CONNECTED">>,
                          headers = _,
                          body    = _}, _} = parse(Data),

        Topic = <<"/queue/foo">>,

        emqx:subscribe(Topic),
        gen_tcp:send(Sock, ["SEND\n",
                            "content-length:3\n",
                            "destination:/queue/foo\n"]),
        timer:sleep(300),
        gen_tcp:send(Sock, ["\nfoo",0]),
        receive
            {deliver, Topic, _Msg}->
                ok
        after 100 ->
                  ?assert(false, "waiting message timeout")
        end
    end).

with_connection(DoFun) ->
    {ok, Sock} = gen_tcp:connect({127, 0, 0, 1},
                                 61613,
                                 [binary, {packet, raw}, {active, false}],
                                 3000),
    try
        DoFun(Sock)
    after
        gen_tcp:close(Sock)
    end.

serialize(Command, Headers) ->
    emqx_stomp_frame:serialize(emqx_stomp_frame:make(Command, Headers)).

serialize(Command, Headers, Body) ->
    emqx_stomp_frame:serialize(emqx_stomp_frame:make(Command, Headers, Body)).

parse(Data) ->
    ProtoEnv = [{max_headers, 10},
                {max_header_length, 1024},
                {max_body_length, 8192}],
    Parser = emqx_stomp_frame:init_parer_state(ProtoEnv),
    emqx_stomp_frame:parse(Data, Parser).
