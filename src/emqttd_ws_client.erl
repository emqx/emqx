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
-record(wsocket_state, {request, client_pid, packet_opts, parser_fun}).

%% WebSocket Client State
-record(wsclient_state, {ws_pid, request, proto_state, keepalive}).

-define(WSLOG(Level, Format, Args, Req),
              lager:Level("WsClient(~s): " ++ Format, [Req:get(peer) | Args])).

%%------------------------------------------------------------------------------
%% @doc Start WebSocket client.
%% @end
%%------------------------------------------------------------------------------
start_link(Req) ->
    PktOpts = emqttd:env(mqtt, packet),
    ParserFun = emqttd_parser:new(PktOpts),
    {ReentryWs, ReplyChannel} = upgrade(Req),
    Params = [self(), Req, ReplyChannel, PktOpts],
    {ok, ClientPid} = gen_server:start_link(?MODULE, Params, []),
    ReentryWs(#wsocket_state{request     = Req,
                             client_pid  = ClientPid,
                             packet_opts = PktOpts,
                             parser_fun  = ParserFun}).

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
                                     parser_fun = ParserFun}, ReplyChannel) ->
    ?WSLOG(debug, "RECV ~p", [Data], Req),
    case catch ParserFun(iolist_to_binary(Data)) of
        {more, NewParser} ->
            State#wsocket_state{parser_fun = NewParser};
        {ok, Packet, Rest} ->
            gen_server:cast(ClientPid, {received, Packet}),
            ws_loop(Rest, reset_parser(State), ReplyChannel);
        {error, Error} ->
            ?WSLOG(error, "Frame error: ~p", [Error], Req),
            exit({shutdown, Error});
        {'EXIT', Reason} ->
            ?WSLOG(error, "Frame error: ~p", [Reason], Req),
            ?WSLOG(error, "Error data: ~p", [Data], Req),
            exit({shutdown, parser_error})
    end.

reset_parser(State = #wsocket_state{packet_opts = PktOpts}) ->
    State#wsocket_state{parser_fun = emqttd_parser:new(PktOpts)}.

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([WsPid, Req, ReplyChannel, PktOpts]) ->
    process_flag(trap_exit, true),
    {ok, Peername} = Req:get(peername),
    SendFun = fun(Payload) -> ReplyChannel({binary, Payload}) end,
    Headers = mochiweb_request:get(headers, Req),
    HeadersList = mochiweb_headers:to_list(Headers),
    ProtoState = emqttd_protocol:init(Peername, SendFun,
                                      [{ws_initial_headers, HeadersList} | PktOpts]),
    {ok, #wsclient_state{ws_pid = WsPid, request = Req, proto_state = ProtoState}}.

handle_call(session, _From, State = #wsclient_state{proto_state = ProtoState}) ->
    {reply, emqttd_protocol:session(ProtoState), State};

handle_call(info, _From, State = #wsclient_state{request     = Req,
                                                 proto_state = ProtoState}) ->
    ProtoInfo = emqttd_protocol:info(ProtoState),
    {reply, [{websocket, true}, {peer, Req:get(peer)}| ProtoInfo], State};

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call(Req, _From, State = #wsclient_state{request = HttpReq}) ->
    ?WSLOG(critical, "Unexpected request: ~p", [Req], HttpReq),
    {reply, {error, unsupported_request}, State}.

handle_cast({subscribe, TopicTable}, State) ->
    with_session(fun(SessPid) ->
                   emqttd_session:subscribe(SessPid, TopicTable)
                 end, State);

handle_cast({unsubscribe, Topics}, State) ->
    with_session(fun(SessPid) ->
                   emqttd_session:unsubscribe(SessPid, Topics)
                 end, State);

handle_cast({received, Packet}, State = #wsclient_state{request     = Req,
                                                        proto_state = ProtoState}) ->
    case emqttd_protocol:received(Packet, ProtoState) of
        {ok, ProtoState1} ->
            noreply(State#wsclient_state{proto_state = ProtoState1});
        {error, Error} ->
            ?WSLOG(error, "Protocol error - ~p", [Error], Req),
            shutdown(Error, State);
        {error, Error, ProtoState1} ->
            shutdown(Error, State#wsclient_state{proto_state = ProtoState1});
        {stop, Reason, ProtoState1} ->
            stop(Reason, State#wsclient_state{proto_state = ProtoState1})
    end;

handle_cast(Msg, State = #wsclient_state{request = Req}) ->
    ?WSLOG(critical, "Unexpected msg: ~p", [Msg], Req),
    {noreply, State}.

handle_info({suback, PacketId, GrantedQos}, State) ->
    with_proto_state(fun(ProtoState) ->
                       Packet = ?SUBACK_PACKET(PacketId, GrantedQos),
                       emqttd_protocol:send(Packet, ProtoState)
                     end, State);

handle_info({deliver, Message}, State) ->
    with_proto_state(fun(ProtoState) ->
                       emqttd_protocol:send(Message, ProtoState)
                     end, State);

handle_info({redeliver, {?PUBREL, PacketId}}, State) ->
    with_proto_state(fun(ProtoState) ->
                       emqttd_protocol:redeliver({?PUBREL, PacketId}, ProtoState)
                     end, State);

handle_info({shutdown, conflict, {ClientId, NewPid}}, State = #wsclient_state{request = Req}) ->
    ?WSLOG(warning, "clientid '~s' conflict with ~p", [ClientId, NewPid], Req),
    shutdown(conflict, State);

handle_info({keepalive, start, Interval}, State = #wsclient_state{request = Req}) ->
    ?WSLOG(debug, "Keepalive at the interval of ~p", [Interval], Req),
    Conn = Req:get(connection),
    StatFun = fun() ->
        case Conn:getstat([recv_oct]) of
            {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
            {error, Error}              -> {error, Error}
        end
    end,
    KeepAlive = emqttd_keepalive:start(StatFun, Interval, {keepalive, check}),
    noreply(State#wsclient_state{keepalive = KeepAlive});

handle_info({keepalive, check}, State = #wsclient_state{request   = Req,
                                                        keepalive = KeepAlive}) ->
    case emqttd_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            noreply(State#wsclient_state{keepalive = KeepAlive1});
        {error, timeout} ->
            ?WSLOG(debug, "Keepalive Timeout!", [], Req),
            shutdown(keepalive_timeout, State);
        {error, Error} ->
            ?WSLOG(warning, "Keepalive error - ~p", [Error], Req),
            shutdown(keepalive_error, State)
    end;

handle_info({'EXIT', WsPid, Reason}, State = #wsclient_state{ws_pid = WsPid}) ->
    stop(Reason, State);

handle_info(Info, State = #wsclient_state{request = Req}) ->
    ?WSLOG(error, "Unexpected Info: ~p", [Info], Req),
    noreply(State).

terminate(Reason, #wsclient_state{proto_state = ProtoState, keepalive = KeepAlive}) ->
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

with_proto_state(Fun, State = #wsclient_state{proto_state = ProtoState}) ->
    {ok, ProtoState1} = Fun(ProtoState),
    noreply(State#wsclient_state{proto_state = ProtoState1}).

with_session(Fun, State = #wsclient_state{proto_state = ProtoState}) ->
    Fun(emqttd_protocol:session(ProtoState)), noreply(State).

noreply(State) ->
    {noreply, State, hibernate}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

stop(Reason, State ) ->
    {stop, Reason, State}.

