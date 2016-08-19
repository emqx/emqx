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

-module(emqttd_ws_client).

-behaviour(gen_server).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

%% API Exports
-export([start_link/4, session/1, info/1, kick/1]).

%% SUB/UNSUB Asynchronously
-export([subscribe/2, unsubscribe/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% WebSocket Client State
-record(wsclient_state, {ws_pid, peer, connection, proto_state, keepalive}).

-define(WSLOG(Level, Peer, Format, Args),
              lager:Level("WsClient(~s): " ++ Format, [Peer | Args])).

%% @doc Start WebSocket Client.
start_link(MqttEnv, WsPid, Req, ReplyChannel) ->
    gen_server:start_link(?MODULE, [MqttEnv, WsPid, Req, ReplyChannel], []).

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

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([MqttEnv, WsPid, Req, ReplyChannel]) ->
    true = link(WsPid),
    {ok, Peername} = Req:get(peername),
    Headers = mochiweb_headers:to_list(
                mochiweb_request:get(headers, Req)),
    PktOpts = proplists:get_value(packet, MqttEnv),
    SendFun = fun(Payload) -> ReplyChannel({binary, Payload}) end,
    ProtoState = emqttd_protocol:init(Peername, SendFun,
                                      [{ws_initial_headers, Headers} | PktOpts]),
    {ok, #wsclient_state{ws_pid = WsPid, peer = Req:get(peer),
                         connection = Req:get(connection),
                         proto_state = ProtoState}, idle_timeout(MqttEnv)}.

idle_timeout(MqttEnv) ->
    ClientOpts = proplists:get_value(client, MqttEnv),
    timer:seconds(proplists:get_value(idle_timeout, ClientOpts, 10)).

handle_call(session, _From, State = #wsclient_state{proto_state = ProtoState}) ->
    {reply, emqttd_protocol:session(ProtoState), State};

handle_call(info, _From, State = #wsclient_state{peer = Peer,
                                                 proto_state = ProtoState}) ->
    ProtoInfo = emqttd_protocol:info(ProtoState),
    {reply, [{websocket, true}, {peer, Peer}| ProtoInfo], State};

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call(Req, _From, State = #wsclient_state{peer = Peer}) ->
    ?WSLOG(critical, Peer, "Unexpected request: ~p", [Req]),
    {reply, {error, unsupported_request}, State}.

handle_cast({subscribe, TopicTable}, State) ->
    with_session(fun(SessPid) ->
                   emqttd_session:subscribe(SessPid, TopicTable)
                 end, State);

handle_cast({unsubscribe, Topics}, State) ->
    with_session(fun(SessPid) ->
                   emqttd_session:unsubscribe(SessPid, Topics)
                 end, State);

handle_cast({received, Packet}, State = #wsclient_state{peer = Peer, proto_state = ProtoState}) ->
    case emqttd_protocol:received(Packet, ProtoState) of
        {ok, ProtoState1} ->
            noreply(State#wsclient_state{proto_state = ProtoState1});
        {error, Error} ->
            ?WSLOG(error, Peer, "Protocol error - ~p", [Error]),
            shutdown(Error, State);
        {error, Error, ProtoState1} ->
            shutdown(Error, State#wsclient_state{proto_state = ProtoState1});
        {stop, Reason, ProtoState1} ->
            stop(Reason, State#wsclient_state{proto_state = ProtoState1})
    end;

handle_cast(Msg, State = #wsclient_state{peer = Peer}) ->
    ?WSLOG(critical, Peer, "Unexpected msg: ~p", [Msg]),
    noreply(State).

handle_info(timeout, State) ->
    shutdown(idle_timeout, State);

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

handle_info({shutdown, conflict, {ClientId, NewPid}}, State = #wsclient_state{peer = Peer}) ->
    ?WSLOG(warning, Peer, "clientid '~s' conflict with ~p", [ClientId, NewPid]),
    shutdown(conflict, State);

handle_info({keepalive, start, Interval}, State = #wsclient_state{peer = Peer, connection = Conn}) ->
    ?WSLOG(debug, Peer, "Keepalive at the interval of ~p", [Interval]),
    StatFun = fun() ->
        case Conn:getstat([recv_oct]) of
            {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
            {error, Error}              -> {error, Error}
        end
    end,
    KeepAlive = emqttd_keepalive:start(StatFun, Interval, {keepalive, check}),
    noreply(State#wsclient_state{keepalive = KeepAlive});

handle_info({keepalive, check}, State = #wsclient_state{peer      = Peer,
                                                        keepalive = KeepAlive}) ->
    case emqttd_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            noreply(State#wsclient_state{keepalive = KeepAlive1});
        {error, timeout} ->
            ?WSLOG(debug, Peer, "Keepalive Timeout!", []),
            shutdown(keepalive_timeout, State);
        {error, Error} ->
            ?WSLOG(warning, Peer, "Keepalive error - ~p", [Error]),
            shutdown(keepalive_error, State)
    end;

handle_info(Info, State = #wsclient_state{peer = Peer}) ->
    ?WSLOG(critical, Peer, "Unexpected Info: ~p", [Info]),
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

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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

