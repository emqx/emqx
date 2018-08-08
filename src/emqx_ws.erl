%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_ws).

-include("emqx_mqtt.hrl").

-import(proplists, [get_value/3]).

%% WebSocket Loop State
-record(wsocket_state, {req, peername, client_pid, max_packet_size, parser}).

-define(WSLOG(Level, Format, Args, State),
              lager:Level("WsClient(~s): " ++ Format,
                          [esockd_net:format(State#wsocket_state.peername) | Args])).

-export([init/2]).
-export([websocket_init/1]).
-export([websocket_handle/2]).
-export([websocket_info/2]).

init(Req0, State) ->
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req0) of
        undefined ->
            {cowboy_websocket, Req0, #wsocket_state{}};
        Subprotocols ->
            case lists:member(<<"mqtt">>, Subprotocols) of
                true ->
                    Peername = cowboy_req:peer(Req0),
                    Req = cowboy_req:set_resp_header(<<"sec-websocket-protocol">>, <<"mqtt">>, Req0),
                    {cowboy_websocket, Req, #wsocket_state{req = Req, peername = Peername}, #{idle_timeout => 86400000}};
                false ->
                    Req = cowboy_req:reply(400, Req0),
                    {ok, Req, #wsocket_state{}}
            end
    end.

websocket_init(State = #wsocket_state{req = Req}) ->
    case emqx_ws_connection_sup:start_connection(self(), Req) of
        {ok, ClientPid} ->
            {ok, ProtoEnv} = emqx_config:get_env(protocol),
            PacketSize = get_value(max_packet_size, ProtoEnv, ?MAX_PACKET_SIZE),
            Parser = emqx_frame:initial_state(#{max_packet_size => PacketSize}),
            NewState = State#wsocket_state{parser = Parser,
                                           max_packet_size = PacketSize,
                                           client_pid = ClientPid},
            {ok, NewState};
        Error ->
            ?WSLOG(error, "Start client fail: ~p", [Error], State),
            {stop, State}
    end.

websocket_handle({binary, <<>>}, State) ->
    {ok, State};
websocket_handle({binary, [<<>>]}, State) ->
    {ok, State};

websocket_handle({binary, Data}, State = #wsocket_state{client_pid = ClientPid, parser = Parser}) ->
    ?WSLOG(debug, "RECV ~p", [Data], State),
    BinSize = iolist_size(Data),
    emqx_metrics:inc('bytes/received', BinSize),
    case catch emqx_frame:parse(iolist_to_binary(Data), Parser) of
        {more, NewParser} ->
            {ok, State#wsocket_state{parser = NewParser}};
        {ok, Packet, Rest} ->
            gen_server:cast(ClientPid, {received, Packet, BinSize}),
            websocket_handle({binary, Rest}, reset_parser(State));
        {error, Error} ->
            ?WSLOG(error, "Frame error: ~p", [Error], State),
            {stop, State};
        {'EXIT', Reason} ->
            ?WSLOG(error, "Frame error: ~p", [Reason], State),
            ?WSLOG(error, "Error data: ~p", [Data], State),
            {stop, State}
    end.

websocket_info({binary, Data}, State) ->
    {reply, {binary, Data}, State};

websocket_info({'EXIT', _Pid, {shutdown, kick}}, State) ->
    {stop, State};

websocket_info(_Info, State) ->
    {ok, State}.

reset_parser(State = #wsocket_state{max_packet_size = PacketSize}) ->
    State#wsocket_state{parser = emqx_frame:initial_state(#{max_packet_size => PacketSize})}.


