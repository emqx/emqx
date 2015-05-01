%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
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

-module(emqttd_ws_client).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("emqtt/include/emqtt.hrl").

-include_lib("emqtt/include/emqtt_packet.hrl").

-behaviour(gen_server).

%% API Exports
-export([start_link/1, ws_loop/3]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% WebSocket loop state
-record(wsocket_state, {request,
                        client_pid,
                        packet_opts,
                        parser_state}).

%% Client state
-record(state, {ws_pid, request, proto_state, keepalive}).

%%------------------------------------------------------------------------------
%% @doc Start WebSocket client.
%% @end
%%------------------------------------------------------------------------------
start_link(Req) ->
    PktOpts = emqttd:env(packet),
    {ReentryWs, ReplyChannel} = upgrade(Req),
    {ok, ClientPid} = gen_server:start_link(?MODULE, [self(), Req, ReplyChannel, PktOpts], []),
    ReentryWs(#wsocket_state{request      = Req,
                             client_pid   = ClientPid,
                             packet_opts  = PktOpts,
                             parser_state = emqtt_parser:init(PktOpts)}).

%%------------------------------------------------------------------------------
%% @private
%% @doc Start WebSocket client.
%% @end
%%------------------------------------------------------------------------------
upgrade(Req) ->
    mochiweb_websocket:upgrade_connection(Req, fun ?MODULE:ws_loop/3).

%%------------------------------------------------------------------------------
%% @doc WebSocket frame receive loop.
%% @end
%%------------------------------------------------------------------------------
ws_loop(<<>>, State, _ReplyChannel) ->
    State;
ws_loop([<<>>], State, _ReplyChannel) ->
    State;
ws_loop(Data, State = #wsocket_state{request = Req,
                                     client_pid = ClientPid,
                                     parser_state = ParserState}, ReplyChannel) ->
    Peer = Req:get(peer),
    lager:debug("RECV from ~s(WebSocket): ~p", [Peer, Data]),
    case emqtt_parser:parse(iolist_to_binary(Data), ParserState) of
    {more, ParserState1} ->
        State#wsocket_state{parser_state = ParserState1};
    {ok, Packet, Rest} ->
        gen_server:cast(ClientPid, {received, Packet}),
        ws_loop(Rest, reset_parser(State), ReplyChannel);
    {error, Error} ->
        lager:error("MQTT(WebSocket) detected framing error ~p for connection ~s", [Error, Peer]),
        exit({shutdown, Error})
    end.

reset_parser(State = #wsocket_state{packet_opts  = PktOpts}) ->
    State#wsocket_state{parser_state = emqtt_parser:init(PktOpts)}.

%%%=============================================================================
%%% gen_fsm callbacks
%%%=============================================================================

init([WsPid, Req, ReplyChannel, PktOpts]) ->
    process_flag(trap_exit, true),
    Socket = Req:get(socket),
    {ok, Peername} = emqttd_net:peername(Socket),
    SendFun = fun(Payload) -> ReplyChannel({binary, Payload}) end,
    ProtoState = emqttd_protocol:init(Peername, SendFun, PktOpts),
    {ok, #state{ws_pid = WsPid, request = Req, proto_state = ProtoState}}.

handle_call(_Req, _From, State) ->
    {reply, error, State}.

handle_cast({received, Packet}, State = #state{proto_state = ProtoState}) ->
    case emqttd_protocol:received(Packet, ProtoState) of
    {ok, ProtoState1} ->
        {noreply, State#state{proto_state = ProtoState1}};
    {error, Error} ->
        lager:error("MQTT protocol error ~p", [Error]),
        stop({shutdown, Error}, State);
    {error, Error, ProtoState1} ->
        stop({shutdown, Error}, State#state{proto_state = ProtoState1});
    {stop, Reason, ProtoState1} ->
        stop(Reason, State#state{proto_state = ProtoState1})
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({dispatch, {From, Messages}}, #state{proto_state = ProtoState} = State) when is_list(Messages) ->
    ProtoState1 =
    lists:foldl(fun(Message, PState) ->
            {ok, PState1} = emqttd_protocol:send({From, Message}, PState), PState1
        end, ProtoState, Messages),
    {noreply, State#state{proto_state = ProtoState1}};

handle_info({dispatch, {From, Message}}, #state{proto_state = ProtoState} = State) ->
    {ok, ProtoState1} = emqttd_protocol:send({From, Message}, ProtoState),
    {noreply, State#state{proto_state = ProtoState1}};

handle_info({redeliver, {?PUBREL, PacketId}}, #state{proto_state = ProtoState} = State) ->
    {ok, ProtoState1} = emqttd_protocol:redeliver({?PUBREL, PacketId}, ProtoState),
    {noreply, State#state{proto_state = ProtoState1}};

handle_info({stop, duplicate_id, _NewPid}, State=#state{proto_state = ProtoState}) ->
    lager:error("Shutdown for duplicate clientid: ~s", [emqttd_protocol:clientid(ProtoState)]), 
    stop({shutdown, duplicate_id}, State);

handle_info({keepalive, start, TimeoutSec}, State = #state{request = Req}) ->
    lager:debug("Client(WebSocket) ~s: Start KeepAlive with ~p seconds", [Req:get(peer), TimeoutSec]),
    %%TODO: fix esockd_transport...
    KeepAlive = emqttd_keepalive:new({esockd_transport, Req:get(socket)},
                                     TimeoutSec, {keepalive, timeout}),
    {noreply, State#state{keepalive = KeepAlive}};

handle_info({keepalive, timeout}, State = #state{request = Req, keepalive = KeepAlive}) ->
    case emqttd_keepalive:resume(KeepAlive) of
    timeout ->
        lager:debug("Client(WebSocket) ~s: Keepalive Timeout!", [Req:get(peer)]),
        stop({shutdown, keepalive_timeout}, State#state{keepalive = undefined});
    {resumed, KeepAlive1} ->
        lager:debug("Client(WebSocket) ~s: Keepalive Resumed", [Req:get(peer)]),
        {noreply, State#state{keepalive = KeepAlive1}}
    end;

handle_info({'EXIT', WsPid, Reason}, State = #state{ws_pid = WsPid}) ->
    stop(Reason, State);

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{proto_state = ProtoState, keepalive = KeepAlive}) ->
    emqttd_keepalive:cancel(KeepAlive),
    case Reason of
        {shutdown, Error} ->
            emqttd_protocol:shutdown(Error, ProtoState);
        _ -> ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

stop(Reason, State ) ->
    {stop, Reason, State}.

