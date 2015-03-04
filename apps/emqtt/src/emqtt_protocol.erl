%%-----------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------
-module(emqtt_protocol).

-include("emqtt.hrl").

-include("emqtt_packet.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([init/3, client_id/1]).

-export([handle_packet/2, send_message/2, send_packet/2, redeliver/2, shutdown/2]).

-export([info/1]).

%% ------------------------------------------------------------------
%% Protocol State
%% ------------------------------------------------------------------
-record(proto_state, {
        transport,
        socket,
        peer_name,
        connected = false, %received CONNECT action?
        proto_vsn,
        proto_name,
		%packet_id,
		client_id,
		clean_sess,
        session, %% session state or session pid
		will_msg
}).

-type(proto_state() :: #proto_state{}).

-spec(send_message({pid() | tuple(), mqtt_message()}, proto_state()) -> {ok, proto_state()}).

-spec(handle_packet(mqtt_packet(), proto_state()) -> {ok, proto_state()} | {error, any()}). 

-define(PACKET_TYPE(Packet, Type), 
    Packet = #mqtt_packet{header = #mqtt_packet_header { type = Type }}).

-define(PUBACK_PACKET(PacketId), #mqtt_packet_puback { packet_id = PacketId }).

init(Transport, Socket, Peername) ->
	#proto_state{
        transport       = Transport,
		socket			= Socket,
        peer_name       = Peername
	}. 

client_id(#proto_state { client_id = ClientId }) -> ClientId.

%%SHOULD be registered in emqtt_cm
info(#proto_state{ proto_vsn    = ProtoVsn,
                   proto_name   = ProtoName,
				   client_id	= ClientId,
				   clean_sess	= CleanSess,
				   will_msg		= WillMsg }) ->
	[ {proto_vsn,  ProtoVsn},
      {proto_name, ProtoName},
	  {client_id,  ClientId},
	  {clean_sess, CleanSess},
	  {will_msg,   WillMsg} ].


%%CONNECT – Client requests a connection to a Server

%%A Client can only send the CONNECT Packet once over a Network Connection. 
handle_packet(?PACKET_TYPE(Packet, ?CONNECT), State = #proto_state{connected = false}) ->
    handle_packet(?CONNECT, Packet, State#proto_state{connected = true});

handle_packet(?PACKET_TYPE(_Packet, ?CONNECT), State = #proto_state{connected = true}) ->
    {error, protocol_bad_connect, State};

%%Received other packets when CONNECT not arrived.
handle_packet(_Packet, State = #proto_state{connected = false}) ->
    {error, protocol_not_connected, State};

handle_packet(?PACKET_TYPE(Packet, Type),
				State = #proto_state { peer_name = PeerName, client_id = ClientId }) ->
	lager:info("RECV from ~s@~s: ~s", [ClientId, PeerName, emqtt_packet:dump(Packet)]),
	case validate_packet(Packet) of	
	ok ->
		handle_packet(Type, Packet, State);
	{error, Reason} ->
		{error, Reason, State}
	end.

handle_packet(?CONNECT, Packet = #mqtt_packet { 
                                    variable = #mqtt_packet_connect { 
                                         username   = Username, 
                                         password   = Password, 
                                         clean_sess = CleanSess, 
                                         keep_alive = KeepAlive, 
                                         client_id  = ClientId } = Var }, 
              State = #proto_state{ peer_name = PeerName } ) ->
    lager:info("RECV from ~s@~s: ~s", [ClientId, PeerName, emqtt_packet:dump(Packet)]),
    {ReturnCode1, State1} =
    case validate_connect(Var) of
        ?CONNACK_ACCEPT ->
            case emqtt_auth:check(Username, Password) of
                true ->
                    ClientId1 = clientid(ClientId, State), 
                    start_keepalive(KeepAlive),
                    emqtt_cm:register(ClientId1, self()),
                    {?CONNACK_ACCEPT, State#proto_state{ will_msg   = willmsg(Var), 
                                                         clean_sess = CleanSess,
                                                         client_id  = ClientId1 }};
                false ->
                    lager:error("~s@~s: username '~s' login failed - no credentials", [ClientId, PeerName, Username]),
                    {?CONNACK_CREDENTIALS, State#proto_state{client_id = ClientId}}
            end;
        ReturnCode ->
            {ReturnCode, State#proto_state{client_id = ClientId}}
    end,
    send_packet( #mqtt_packet {
                    header = #mqtt_packet_header { type = ?CONNACK }, 
                    variable = #mqtt_packet_connack{ return_code = ReturnCode1 }}, State1 ),
    %%Starting session
    {ok, Session} = emqtt_session:start({CleanSess, ClientId, self()}),
    {ok, State1#proto_state { session = Session }};

handle_packet(?PUBLISH, Packet = #mqtt_packet {
                                     header = #mqtt_packet_header {qos = ?QOS_0}}, 
                                 State = #proto_state{session = Session}) ->
    emqtt_session:publish(Session, {?QOS_0, emqtt_message:from_packet(Packet)}),
	{ok, State};

handle_packet(?PUBLISH, Packet = #mqtt_packet { 
                                     header = #mqtt_packet_header { qos = ?QOS_1 }, 
                                     variable = #mqtt_packet_publish{packet_id = PacketId }}, 
                                 State = #proto_state { session = Session }) ->
    emqtt_session:publish(Session, {?QOS_1, emqtt_message:from_packet(Packet)}),
    send_packet( make_packet(?PUBACK,  PacketId),  State);

handle_packet(?PUBLISH, Packet = #mqtt_packet { 
                                     header = #mqtt_packet_header { qos = ?QOS_2 }, 
                                     variable = #mqtt_packet_publish { packet_id = PacketId } }, 
                                 State = #proto_state { session = Session }) ->
    NewSession = emqtt_session:publish(Session, {?QOS_2, emqtt_message:from_packet(Packet)}),
	send_packet( make_packet(?PUBREC, PacketId), State#proto_state {session = NewSession} );

handle_packet(Puback, #mqtt_packet{variable = ?PUBACK_PACKET(PacketId) }, 
    State = #proto_state { session = Session }) 
    when Puback >= ?PUBACK andalso Puback =< ?PUBCOMP ->

    NewSession = emqtt_session:puback(Session, {Puback, PacketId}),
    NewState = State#proto_state {session = NewSession},
    if 
        Puback =:= ?PUBREC ->
            send_packet( make_packet(?PUBREL, PacketId), NewState);
        Puback =:= ?PUBREL ->
            send_packet( make_packet(?PUBCOMP, PacketId), NewState);
        true ->
            ok
    end,
	{ok, NewState};

handle_packet(?SUBSCRIBE, #mqtt_packet { 
                              variable = #mqtt_packet_subscribe{
                                            packet_id  = PacketId, 
                                            topic_table = TopicTable}, 
                              payload = undefined}, 
                      State = #proto_state { session = Session } ) ->

    {ok, NewSession, GrantedQos} = emqtt_session:subscribe(Session, TopicTable),
    send_packet(#mqtt_packet { header = #mqtt_packet_header { type = ?SUBACK }, 
                               variable = #mqtt_packet_suback{ packet_id = PacketId, 
                                                               qos_table  = GrantedQos }}, 
                   State#proto_state{ session = NewSession });

handle_packet(?UNSUBSCRIBE, #mqtt_packet{
                                variable = #mqtt_packet_unsubscribe{
                                              packet_id = PacketId, 
                                              topics    = Topics}, 
                                payload = undefined},
               State = #proto_state{session = Session}) ->
    {ok, NewSession} = emqtt_session:unsubscribe(Session, Topics),
    send_packet(#mqtt_packet { header = #mqtt_packet_header {type = ?UNSUBACK }, 
                               variable = #mqtt_packet_unsuback{packet_id = PacketId }}, 
                           State#proto_state { session = NewSession } );

handle_packet(?PINGREQ, #mqtt_packet{}, State) ->
    send_packet(make_packet(?PINGRESP), State);

handle_packet(?DISCONNECT, #mqtt_packet{}, State) ->
    %%TODO: how to handle session?
    % clean willmsg
    {stop, normal, State#proto_state{will_msg = undefined}}.

make_packet(Type) when Type >= ?CONNECT andalso Type =< ?DISCONNECT -> 
    #mqtt_packet{ header = #mqtt_packet_header { type = Type } }.

make_packet(PubAck, PacketId) when PubAck >= ?PUBACK andalso PubAck =< ?PUBCOMP ->
  #mqtt_packet { header = #mqtt_packet_header { type = PubAck, qos = puback_qos(PubAck) }, 
                 variable = #mqtt_packet_puback { packet_id = PacketId}}.

puback_qos(?PUBACK) ->  ?QOS_0;
puback_qos(?PUBREC) ->  ?QOS_0;
puback_qos(?PUBREL) ->  ?QOS_1;
puback_qos(?PUBCOMP) -> ?QOS_0.

%% qos0 message
send_message({_From, Message = #mqtt_message{ qos = ?QOS_0 }}, State) ->
	send_packet(emqtt_message:to_packet(Message), State);

%% message from session
send_message({_From = SessPid, Message}, State = #proto_state{session = SessPid}) when is_pid(SessPid) ->
	send_packet(emqtt_message:to_packet(Message), State);

%% message(qos1, qos2) not from session
send_message({_From, Message = #mqtt_message{ qos = Qos }}, State = #proto_state{ session = Session }) 
    when (Qos =:= ?QOS_1) orelse (Qos =:= ?QOS_2) ->
    {Message1, NewSession} = emqtt_session:store(Session, Message),
	send_packet(emqtt_message:to_packet(Message1), State#proto_state{session = NewSession}).

send_packet(Packet, State = #proto_state{transport = Transport, socket = Sock, peer_name = PeerName, client_id = ClientId}) ->
	lager:info("SENT to ~s@~s: ~s", [ClientId, PeerName, emqtt_packet:dump(Packet)]),
    Data = emqtt_serialiser:serialise(Packet),
    lager:debug("SENT to ~s: ~p", [PeerName, Data]),
    Transport:send(Sock, Data),
    {ok, State}.

%%
%% @doc redeliver PUBREL PacketId
%%
redeliver({?PUBREL, PacketId}, State) ->
    send_packet( make_packet(?PUBREL, PacketId), State).

shutdown(Error, #proto_state{peer_name = PeerName, client_id = ClientId, will_msg = WillMsg}) ->
    send_willmsg(WillMsg),
    try_unregister(ClientId, self()),
	lager:info("Protocol ~s@~s Shutdown: ~p", [ClientId, PeerName, Error]),
    ok.

willmsg(Packet) when is_record(Packet, mqtt_packet_connect) ->
    emqtt_message:from_packet(Packet).

clientid(<<>>, #proto_state{peer_name = PeerName}) ->
    <<"eMQTT/", (base64:encode(PeerName))/binary>>;

clientid(ClientId, _State) -> ClientId.

%%----------------------------------------------------------------------------

send_willmsg(undefined) -> ignore;
%%TODO:should call session...
send_willmsg(WillMsg) -> emqtt_router:route(WillMsg).

start_keepalive(0) -> ignore;
start_keepalive(Sec) when Sec > 0 ->
    self() ! {keepalive, start, round(Sec * 1.5)}.

%%----------------------------------------------------------------------------
%% Validate Packets
%%----------------------------------------------------------------------------
validate_connect( Connect = #mqtt_packet_connect{} ) ->
    case validate_protocol(Connect) of
        true -> 
            case validate_clientid(Connect) of
                true -> 
                    ?CONNACK_ACCEPT;
                false -> 
                    ?CONNACK_INVALID_ID
            end;
        false -> 
            ?CONNACK_PROTO_VER
    end.

validate_protocol(#mqtt_packet_connect { proto_ver = Ver, proto_name = Name }) ->
    lists:member({Ver, Name}, ?PROTOCOL_NAMES).

validate_clientid(#mqtt_packet_connect { client_id = ClientId }) 
    when ( size(ClientId) >= 1 ) andalso ( size(ClientId) =< ?MAX_CLIENTID_LEN ) ->
    true;

%% MQTT3.1.1 allow null clientId.
validate_clientid(#mqtt_packet_connect { proto_ver =?MQTT_PROTO_V311, client_id = ClientId }) 
    when size(ClientId) =:= 0 ->
    true;

validate_clientid(#mqtt_packet_connect { proto_ver = Ver, clean_sess = CleanSess, client_id = ClientId }) -> 
    lager:warning("Invalid ClientId: ~s, ProtoVer: ~p, CleanSess: ~s", [ClientId, Ver, CleanSess]),
    false.

validate_packet(#mqtt_packet { header  = #mqtt_packet_header { type = ?PUBLISH }, 
                               variable = #mqtt_packet_publish{ topic_name = Topic }}) ->
	case emqtt_topic:validate({name, Topic}) of
	true -> ok;
	false -> lager:warning("Error publish topic: ~p", [Topic]), {error, badtopic}
	end;

validate_packet(#mqtt_packet { header  = #mqtt_packet_header { type = ?SUBSCRIBE }, 
                               variable = #mqtt_packet_subscribe{topic_table = Topics }}) ->

    validate_topics(filter, Topics);

validate_packet(#mqtt_packet{ header  = #mqtt_packet_header { type = ?UNSUBSCRIBE }, 
                              variable = #mqtt_packet_subscribe{ topic_table = Topics }}) ->

    validate_topics(filter, Topics);

validate_packet(_Packet) -> 
    ok.

validate_topics(Type, []) when Type =:= name orelse Type =:= filter ->
	lager:error("Empty Topics!"),
    {error, empty_topics};

validate_topics(Type, Topics) when Type =:= name orelse Type =:= filter ->
	ErrTopics = [Topic || {Topic, Qos} <- Topics,
						not (emqtt_topic:validate({Type, Topic}) and validate_qos(Qos))],
	case ErrTopics of
	[] -> ok;
	_ -> lager:error("Error Topics: ~p", [ErrTopics]), {error, badtopic}
	end.

validate_qos(undefined) -> true;
validate_qos(Qos) when Qos =< ?QOS_2 -> true;
validate_qos(_) -> false.

try_unregister(undefined, _) -> ok;
try_unregister(ClientId, _) -> emqtt_cm:unregister(ClientId, self()).

