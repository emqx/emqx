%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-export([handle_request/1, ws_loop/3]).

%% WebSocket Loop State
-record(wsocket_state, {peer, client_pid, packet_opts, parser_fun}).

-define(WSLOG(Level, Peer, Format, Args),
        lager:Level("WsClient(~s): " ++ Format, [Peer | Args])).

%%--------------------------------------------------------------------
%% Handle WebSocket Request
%%--------------------------------------------------------------------

%% @doc Handle WebSocket Request.
handle_request(Req) ->
    Peer = Req:get(peer),
    PktOpts = emqttd:env(mqtt, packet),
    ParserFun = emqttd_parser:new(PktOpts),
    {ReentryWs, ReplyChannel} = upgrade(Req),
    {ok, ClientPid} = emqttd_ws_client_sup:start_client(self(), Req, ReplyChannel),
    ReentryWs(#wsocket_state{peer = Peer, client_pid = ClientPid,
                             packet_opts = PktOpts, parser_fun = ParserFun}).

%% @doc Upgrade WebSocket.
%% @private
upgrade(Req) ->
    mochiweb_websocket:upgrade_connection(Req, fun ?MODULE:ws_loop/3).

%%--------------------------------------------------------------------
%% Receive Loop
%%--------------------------------------------------------------------

%% @doc WebSocket frame receive loop.
ws_loop(<<>>, State, _ReplyChannel) ->
    State;
ws_loop([<<>>], State, _ReplyChannel) ->
    State;
ws_loop(Data, State = #wsocket_state{peer = Peer, client_pid = ClientPid,
                                     parser_fun = ParserFun}, ReplyChannel) ->
    ?WSLOG(debug, Peer, "RECV ~p", [Data]),
    case catch ParserFun(iolist_to_binary(Data)) of
        {more, NewParser} ->
            State#wsocket_state{parser_fun = NewParser};
        {ok, Packet, Rest} ->
            gen_server:cast(ClientPid, {received, Packet}),
            ws_loop(Rest, reset_parser(State), ReplyChannel);
        {error, Error} ->
            ?WSLOG(error, Peer, "Frame error: ~p", [Error]),
            exit({shutdown, Error});
        {'EXIT', Reason} ->
            ?WSLOG(error, Peer, "Frame error: ~p", [Reason]),
            ?WSLOG(error, Peer, "Error data: ~p", [Data]),
            exit({shutdown, parser_error})
    end.

reset_parser(State = #wsocket_state{packet_opts = PktOpts}) ->
    State#wsocket_state{parser_fun = emqttd_parser:new(PktOpts)}.

