%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd websocket client.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_websocket).

-export([start_link/1, init/1, loop/3]).

-record(state, {request,
                peername,
                parse_state,
                proto_state,
                %packet_opts,
                keepalive}).

-record(client_state, {request, sender}).

-define(PACKET_OPTS, [{max_clientid_len, 1024},
                      {max_packet_size,  4096}]).

start_link(Req) ->
    {ReentryWs, ReplyChannel} = mochiweb_websocket:upgrade_connection(Req, fun ?MODULE:loop/3),
    {ok, Client} = gen_server:start_link(?MODULE, [Req, ReplyChannel], []),
    ReentryWs(#state{client = Client,
                     parse_state = emqtt_parser:init(?PACKET_OPTS)}).


init([Req, ReplyChannel]) ->
    {ok, 
                     peername = Req:get_header_value(peername),
    %ProtoState = emqttd_protocol:init({Transport, NewSock, Peername}, PacketOpts),

loop(Payload, State = #state{parse_state = ParserState}, ReplyChannel) ->
    io:format("Received data: ~p~n", [Payload]),
    ReplyChannel(Payload),
    State.



