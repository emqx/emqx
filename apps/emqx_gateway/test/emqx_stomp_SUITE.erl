%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx_gateway/src/stomp/include/emqx_stomp.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(HEARTBEAT, <<$\n>>).

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

init_per_suite(Cfg) ->
    emqx_ct_helpers:start_apps([emqx_gateway], fun set_special_configs/1),
    Cfg.

end_per_suite(_Cfg) ->
    emqx_ct_helpers:stop_apps([emqx_gateway]),
    ok.

set_special_configs(emqx_gateway) ->
    emqx_config:put(
      [emqx_gateway],
      #{stomp =>
        #{'1' =>
          #{authenticator => allow_anonymous,
            clientinfo_override =>
                #{password => "${Packet.headers.passcode}",
                  username => "${Packet.headers.login}"},
            frame =>
                #{max_body_length => 8192,
                  max_headers => 10,
                  max_headers_length => 1024},
            listener =>
                #{tcp =>
                  #{'1' =>
                    #{acceptors => 16,active_n => 100,backlog => 1024,
                      bind => 61613,high_watermark => 1048576,
                      max_conn_rate => 1000,max_connections => 1024000,
                      send_timeout => 15000,send_timeout_close => true}}}}}}),
    ok;
set_special_configs(_) ->
    ok.

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
                                                  body    = _}, _, _} = parse(Data),
                        <<"2000,1000">> = proplists:get_value(<<"heart-beat">>, Frame#stomp_frame.headers),

                        gen_tcp:send(Sock, serialize(<<"DISCONNECT">>,
                                                     [{<<"receipt">>, <<"12345">>}])),

                        {ok, Data1} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"RECEIPT">>,
                                          headers = [{<<"receipt-id">>, <<"12345">>}],
                                          body    = _}, _, _} = parse(Data1)
                    end),

    %% Connect will be failed, because of bad login or passcode
    %% FIXME: Waiting for authentication works
    %with_connection(fun(Sock) ->
    %                    gen_tcp:send(Sock, serialize(<<"CONNECT">>,
    %                                                 [{<<"accept-version">>, ?STOMP_VER},
    %                                                  {<<"host">>, <<"127.0.0.1:61613">>},
    %                                                  {<<"login">>, <<"admin">>},
    %                                                  {<<"passcode">>, <<"admin">>},
    %                                                  {<<"heart-beat">>, <<"1000,2000">>}])),
    %                    {ok, Data} = gen_tcp:recv(Sock, 0),
    %                    {ok, #stomp_frame{command = <<"ERROR">>,
    %                                      headers = _,
    %                                      body    = <<"Login or passcode error!">>}, _, _} = parse(Data)
    %                end),

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
                                          body    = <<"Login Failed: Supported protocol versions < 1.2">>}, _, _} = parse(Data)
                    end).

t_heartbeat(_) ->
    %% Test heart beat
    with_connection(fun(Sock) ->
                        gen_tcp:send(Sock, serialize(<<"CONNECT">>,
                                                    [{<<"accept-version">>, ?STOMP_VER},
                                                     {<<"host">>, <<"127.0.0.1:61613">>},
                                                     {<<"login">>, <<"guest">>},
                                                     {<<"passcode">>, <<"guest">>},
                                                     {<<"heart-beat">>, <<"1000,800">>}])),
                        {ok, Data} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"CONNECTED">>,
                                          headers = _,
                                          body    = _}, _, _} = parse(Data),

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
                                          body    = _}, _, _} = parse(Data),

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
                                                  body    = <<"hello">>}, _, _} = parse(Data1),
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
                                          body    = _}, _, _} = parse(Data2),

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
                                                  body    = _}, _, _} = parse(Data),

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
                                          body    = <<"hello">>}, Rest1, _} = parse(Data1),

                        %{ok, Data2} = gen_tcp:recv(Sock, 0, 500),
                        {ok, #stomp_frame{command = <<"MESSAGE">>,
                                          headers = _,
                                          body    = <<"hello again">>}, _Rest2, _} = parse(Rest1),

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
                                          body    = _}, _, _} = parse(Data3)
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
                                          body    = _}, _, _} = parse(Data),

                        gen_tcp:send(Sock, serialize(<<"ABORT">>,
                                                    [{<<"transaction">>, <<"tx1">>},
                                                     {<<"receipt">>, <<"12345">>}])),

                        {ok, Data1} = gen_tcp:recv(Sock, 0),
                        {ok, Frame = #stomp_frame{command = <<"ERROR">>,
                                          headers = _,
                                          body    = <<"Transaction tx1 not found">>}, _, _} = parse(Data1),

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
                                          body    = _}, _, _} = parse(Data),

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
                                                  body    = <<"ack test">>}, _, _} = parse(Data1),

                        AckId = proplists:get_value(<<"ack">>, Frame#stomp_frame.headers),

                        gen_tcp:send(Sock, serialize(<<"ACK">>,
                                                    [{<<"id">>, AckId},
                                                     {<<"receipt">>, <<"12345">>}])),

                        {ok, Data2} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"RECEIPT">>,
                                                  headers = [{<<"receipt-id">>, <<"12345">>}],
                                                  body    = _}, _, _} = parse(Data2),

                        gen_tcp:send(Sock, serialize(<<"SEND">>,
                                                    [{<<"destination">>, <<"/queue/foo">>}],
                                                    <<"nack test">>)),

                        {ok, Data3} = gen_tcp:recv(Sock, 0),
                        {ok, Frame1 = #stomp_frame{command = <<"MESSAGE">>,
                                                  headers = _,
                                                  body    = <<"nack test">>}, _, _} = parse(Data3),

                        AckId1 = proplists:get_value(<<"ack">>, Frame1#stomp_frame.headers),

                        gen_tcp:send(Sock, serialize(<<"NACK">>,
                                                    [{<<"id">>, AckId1},
                                                     {<<"receipt">>, <<"12345">>}])),

                        {ok, Data4} = gen_tcp:recv(Sock, 0),
                        {ok, #stomp_frame{command = <<"RECEIPT">>,
                                                  headers = [{<<"receipt-id">>, <<"12345">>}],
                                                  body    = _}, _, _} = parse(Data4)
                    end).

%% TODO: Mountpoint, AuthChain, ACL + Mountpoint, ClientInfoOverride,
%%       Listeners, Metrics, Stats, ClientInfo
%%
%% TODO: Start/Stop, List Instace
%%
%% TODO: RateLimit, OOM, 

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
    emqx_stomp_frame:serialize_pkt(emqx_stomp_frame:make(Command, Headers), #{}).

serialize(Command, Headers, Body) ->
    emqx_stomp_frame:serialize_pkt(emqx_stomp_frame:make(Command, Headers, Body), #{}).

parse(Data) ->
    ProtoEnv = #{max_headers => 10,
                 max_header_length => 1024,
                 max_body_length => 8192
                },
    Parser = emqx_stomp_frame:initial_parse_state(ProtoEnv),
    emqx_stomp_frame:parse(Data, Parser).
