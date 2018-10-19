%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqttd_ws).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd_protocol.hrl").

-import(proplists, [get_value/3]).

-export([handle_request/1, ws_loop/3]).

%% WebSocket Loop State
-record(wsocket_state, {peername, client_pid, max_packet_size, parser}).

-define(WSLOG(Level, Format, Args, State),
              lager:Level("WsClient(~s): " ++ Format,
                          [esockd_net:format(State#wsocket_state.peername) | Args])).


handle_request(Req) ->
    handle_request(Req:get(method), Req:get(path), Req).

%%--------------------------------------------------------------------
%% MQTT Over WebSocket
%%--------------------------------------------------------------------

handle_request('GET', "/ready", Req) ->
		lager:debug("WebSocket Health Check: ~s", [Req:get(peer)]),
		Req:respond({200, [], <<"I'm alive!">>;

handle_request('GET', "/mqtt", Req) ->
    lager:debug("WebSocket Connection from: ~s", [Req:get(peer)]),
    Upgrade = Req:get_header_value("Upgrade"),
    Proto   = check_protocol_header(Req),
    case {is_websocket(Upgrade), Proto} of
        {true, "mqtt" ++ _Vsn} ->
            case Req:get(peername) of
                {ok, Peername} ->
                    {ok, ProtoEnv} = emqttd:env(protocol),
                    PacketSize = get_value(max_packet_size, ProtoEnv, ?MAX_PACKET_SIZE),
                    Parser = emqttd_parser:initial_state(PacketSize),
                    %% Upgrade WebSocket.
                    {ReentryWs, ReplyChannel} = mochiweb_websocket:upgrade_connection(Req, fun ?MODULE:ws_loop/3),
                    {ok, ClientPid} = emqttd_ws_client_sup:start_client(self(), Req, ReplyChannel),
                    ReentryWs(#wsocket_state{peername = Peername,
                                             parser = Parser,
                                             max_packet_size = PacketSize,
                                             client_pid = ClientPid});
                {error, Reason} ->
                    lager:error("Get peername with error ~s", [Reason]),
                    Req:respond({400, [], <<"Bad Request">>})
            end;
        {false, _} ->
            lager:error("Not WebSocket: Upgrade = ~s", [Upgrade]),
            Req:respond({400, [], <<"Bad Request">>});
        {_, Proto} ->
            lager:error("WebSocket with error Protocol: ~s", [Proto]),
            Req:respond({400, [], <<"Bad WebSocket Protocol">>})
    end;

handle_request(Method, Path, Req) ->
    lager:error("Unexpected WS Request: ~s ~s", [Method, Path]),
    Req:not_found().

is_websocket(Upgrade) ->
    Upgrade =/= undefined andalso string:to_lower(Upgrade) =:= "websocket".

check_protocol_header(Req) ->
    case emqttd:env(websocket_protocol_header, false) of
        true  -> get_protocol_header(Req);
        false -> "mqtt-v3.1.1"
    end.

get_protocol_header(Req) ->
    case Req:get_header_value("EMQ-WebSocket-Protocol") of
        undefined -> Req:get_header_value("Sec-WebSocket-Protocol");
        Proto     -> Proto
    end.

%%--------------------------------------------------------------------
%% Receive Loop
%%--------------------------------------------------------------------

%% @doc WebSocket frame receive loop.
ws_loop(<<>>, State, _ReplyChannel) ->
    State;
ws_loop([<<>>], State, _ReplyChannel) ->
    State;
ws_loop(Data, State = #wsocket_state{client_pid = ClientPid, parser = Parser}, ReplyChannel) ->
    ?WSLOG(debug, "RECV ~p", [Data], State),
    emqttd_metrics:inc('bytes/received', iolist_size(Data)),
    case catch emqttd_parser:parse(iolist_to_binary(Data), Parser) of
        {more, NewParser} ->
            State#wsocket_state{parser = NewParser};
        {ok, Packet, Rest} ->
            gen_server:cast(ClientPid, {received, Packet}),
            ws_loop(Rest, reset_parser(State), ReplyChannel);
        {error, Error} ->
            ?WSLOG(error, "Frame error: ~p", [Error], State),
            exit({shutdown, Error});
        {'EXIT', Reason} ->
            ?WSLOG(error, "Frame error: ~p", [Reason], State),
            ?WSLOG(error, "Error data: ~p", [Data], State),
            exit({shutdown, parser_error})
    end.

reset_parser(State = #wsocket_state{max_packet_size = PacketSize}) ->
    State#wsocket_state{parser = emqttd_parser:initial_state(PacketSize)}.

