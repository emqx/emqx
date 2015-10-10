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

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

%% API Exports
-export([start_link/1, ws_loop/3, session/1, info/1, kick/1]).

%% SUB/UNSUB Asynchronously
-export([subscribe/2, unsubscribe/2]).

-behaviour(gen_server).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% WebSocket Loop State
-record(wsocket_state, {request, client_pid, packet_opts, parser}).

%% Client State
-record(client_state, {ws_pid, request, proto_state, keepalive}).

%%------------------------------------------------------------------------------
%% @doc Start WebSocket client.
%% @end
%%------------------------------------------------------------------------------
start_link(Req) ->
    PktOpts = emqttd:env(mqtt, packet),
    {ReentryWs, ReplyChannel} = upgrade(Req),
    {ok, ClientPid} = gen_server:start_link(?MODULE, [self(), Req, ReplyChannel, PktOpts], []),
    ReentryWs(#wsocket_state{request      = Req,
                             client_pid   = ClientPid,
                             packet_opts  = PktOpts,
                             parser       = emqttd_parser:new(PktOpts)}).

session(CPid) ->
    gen_server:call(CPid, session, infinity).

info(CPid) ->
    gen_server:call(CPid, info, infinity).

kick(CPid) ->
    gen_server:call(CPid, kick).

subscribe(CPid, TopicTable) ->
    gen_server:cast(CPid, {subscribe, TopicTable}).

unsubscribe(CPid, Topics) ->
    gen_server:cast(CPid, {unsubscribe, Topics}).

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
ws_loop(Data, State = #wsocket_state{request    = Req,
                                     client_pid = ClientPid,
                                     parser     = Parser}, ReplyChannel) ->
    Peer = Req:get(peer),
    lager:debug("RECV from ~s(WebSocket): ~p", [Peer, Data]),
    case Parser(iolist_to_binary(Data)) of
    {more, NewParser} ->
        State#wsocket_state{parser = NewParser};
    {ok, Packet, Rest} ->
        gen_server:cast(ClientPid, {received, Packet}),
        ws_loop(Rest, reset_parser(State), ReplyChannel);
    {error, Error} ->
        lager:error("MQTT(WebSocket) detected framing error ~p for connection ~s", [Error, Peer]),
        exit({shutdown, Error})
    end.

reset_parser(State = #wsocket_state{packet_opts = PktOpts}) ->
    State#wsocket_state{parser = emqttd_parser:new (PktOpts)}.

%%%=============================================================================
%%% gen_fsm callbacks
%%%=============================================================================

init([WsPid, Req, ReplyChannel, PktOpts]) ->
    process_flag(trap_exit, true),
    {ok, Peername} = Req:get(peername),
    SendFun = fun(Payload) -> ReplyChannel({binary, Payload}) end,
    Headers = mochiweb_request:get(headers, Req),
    HeadersList = mochiweb_headers:to_list(Headers),
    ProtoState = emqttd_protocol:init(Peername, SendFun, [{ws_initial_headers, HeadersList}|PktOpts]),
    {ok, #client_state{ws_pid = WsPid, request = Req, proto_state = ProtoState}}.

handle_call(session, _From, State = #client_state{proto_state = ProtoState}) ->
    {reply, emqttd_protocol:session(ProtoState), State};

handle_call(info, _From, State = #client_state{request = Req,
                                               proto_state = ProtoState}) ->
    {reply, [{websocket, true}, {peer, Req:get(peer)}
             | emqttd_protocol:info(ProtoState)], State};

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call(_Req, _From, State) ->
    {reply, error, State}.

handle_cast({subscribe, TopicTable}, State) ->
    with_session(fun(SessPid) -> emqttd_session:subscribe(SessPid, TopicTable) end, State);

handle_cast({unsubscribe, Topics}, State) ->
    with_session(fun(SessPid) -> emqttd_session:unsubscribe(SessPid, Topics) end, State);

handle_cast({received, Packet}, State = #client_state{proto_state = ProtoState}) ->
    case emqttd_protocol:received(Packet, ProtoState) of
    {ok, ProtoState1} ->
        noreply(State#client_state{proto_state = ProtoState1});
    {error, Error} ->
        lager:error("MQTT protocol error ~p", [Error]),
        stop({shutdown, Error}, State);
    {error, Error, ProtoState1} ->
        stop({shutdown, Error}, State#client_state{proto_state = ProtoState1});
    {stop, Reason, ProtoState1} ->
        stop(Reason, State#client_state{proto_state = ProtoState1})
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({deliver, Message}, State = #client_state{proto_state = ProtoState}) ->
    {ok, ProtoState1} = emqttd_protocol:send(Message, ProtoState),
    noreply(State#client_state{proto_state = ProtoState1});

handle_info({redeliver, {?PUBREL, PacketId}}, State = #client_state{proto_state = ProtoState}) ->
    {ok, ProtoState1} = emqttd_protocol:redeliver({?PUBREL, PacketId}, ProtoState),
    noreply(State#client_state{proto_state = ProtoState1});

handle_info({stop, duplicate_id, _NewPid}, State = #client_state{proto_state = ProtoState}) ->
    lager:error("Shutdown for duplicate clientid: ~s", [emqttd_protocol:clientid(ProtoState)]), 
    stop({shutdown, duplicate_id}, State);

handle_info({keepalive, start, TimeoutSec}, State = #client_state{request = Req}) ->
    lager:debug("Client(WebSocket) ~s: Start KeepAlive with ~p seconds", [Req:get(peer), TimeoutSec]),
    Socket = Req:get(socket),
    StatFun = fun() ->
        case esockd_transport:getstat(Socket, [recv_oct]) of
            {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
            {error, Error}              -> {error, Error}
        end
    end,
    KeepAlive = emqttd_keepalive:start(StatFun, TimeoutSec, {keepalive, check}),
    noreply(State#client_state{keepalive = KeepAlive});

handle_info({keepalive, check}, State = #client_state{request = Req, keepalive = KeepAlive}) ->
    case emqttd_keepalive:check(KeepAlive) of
    {ok, KeepAlive1} ->
        lager:debug("Client(WebSocket) ~s: Keepalive Resumed", [Req:get(peer)]),
        noreply(State#client_state{keepalive = KeepAlive1});
    {error, timeout} ->
        lager:debug("Client(WebSocket) ~s: Keepalive Timeout!", [Req:get(peer)]),
        stop({shutdown, keepalive_timeout}, State#client_state{keepalive = undefined});
    {error, Error} ->
        lager:debug("Client(WebSocket) ~s: Keepalive Error: ~p", [Req:get(peer), Error]),
        stop({shutdown, keepalive_error}, State#client_state{keepalive = undefined})
    end;

handle_info({'EXIT', WsPid, Reason}, State = #client_state{ws_pid = WsPid, proto_state = ProtoState}) ->
    ClientId = emqttd_protocol:clientid(ProtoState),
    lager:warning("Websocket client ~s exit: reason=~p", [ClientId, Reason]),
    stop({shutdown, websocket_closed}, State);

handle_info(Info, State = #client_state{request = Req}) ->
    lager:error("Client(WebSocket) ~s: Unexpected Info - ~p", [Req:get(peer), Info]),
    noreply(State).

terminate(Reason, #client_state{proto_state = ProtoState, keepalive = KeepAlive}) ->
    lager:info("WebSocket client terminated: ~p", [Reason]),
    emqttd_keepalive:cancel(KeepAlive),
    case Reason of
        {shutdown, Error} ->
            emqttd_protocol:shutdown(Error, ProtoState);
        _ ->
            emqttd_protocol:shutdown(Reason, ProtoState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

noreply(State) ->
    {noreply, State, hibernate}.

stop(Reason, State ) ->
    {stop, Reason, State}.

with_session(Fun, State = #client_state{proto_state = ProtoState}) ->
    Fun(emqttd_protocol:session(ProtoState)), noreply(State).

