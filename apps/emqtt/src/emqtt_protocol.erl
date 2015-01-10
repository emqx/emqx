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

-record(proto_state, {
		socket,
		connected = false, %received CONNECT action?
		packet_id,
		client_id,
		clean_sess,
		will_msg,
		subscriptions,
		awaiting_ack,
		awaiting_rel
}).

-type proto_state() :: #proto_state{}.

-export([initial_state/1]).

-export([handle_packet/2, send_packet/2, client_terminated/1]).

-export([info/1]).

-define(PACKET_TYPE(Packet, Type), 
    Packet = #mqtt_packet { header = #mqtt_packet_header { type = Type }}).

initial_state(Socket) ->
	#proto_state{
		socket			= Socket,
		packet_id		= 1,
		subscriptions   = [],
		awaiting_ack	= gb_trees:empty(),
		awaiting_rel	= gb_trees:empty()
	}. 

info(#proto_state{ packet_id	= PacketId,
				   client_id	= ClientId,
				   clean_sess	= CleanSess,
				   will_msg		= WillMsg,
				   subscriptions= Subs }) ->
	[ {packet_id,  PacketId},
	  {client_id,   ClientId},
	  {clean_sess,  CleanSess},
	  {will_msg,    WillMsg},
	  {subscriptions, Subs} ].

-spec handle_packet(Packet, State) -> {ok, NewState} | {error, any()} when 
	Packet      :: mqtt_packet(), 
	State       :: proto_state(),
	NewState    :: proto_state().

%%CONNECT – Client requests a connection to a Server

%%A Client can only send the CONNECT Packet once over a Network Connection. 369
handle_packet(?PACKET_TYPE(Packet, ?CONNECT), State = #proto_state{connected = false}) ->
    handle_packet(?CONNECT, Packet, State#proto_state{connected = true});

handle_packet(?PACKET_TYPE(Packet, ?CONNECT), State = #proto_state{connected = true}) ->
    {error, protocol_bad_connect, State};

%%Received other packets when CONNECT not arrived.
handle_packet(_Packet, State = #proto_state{connected = false}) ->
    {error, protocol_not_connected, State};

handle_packet(?PACKET_TYPE(Packet, Type),
				State = #proto_state{client_id = ClientId}) ->
	lager:info("packet from ~s: ~p", [ClientId, Packet]),
	case validate_packet(Type, Packet) of	
	ok ->
		handle_packet(Type, Packet, State);
	{error, Reason} ->
		{error, Reason, State}
	end.

handle_packet(?CONNECT, #mqtt_packet { 
                           variable = #mqtt_packet_connect { 
                                         username   = Username, 
                                         password   = Password, 
                                         proto_ver  = ProtoVersion, 
                                         clean_sess = CleanSess, 
                                         keep_alive = KeepAlive, 
                                         client_id  = ClientId } = Var }, 
              State0 = #proto_state{socket = Sock}) ->

    State = State0#proto_state{client_id = ClientId},
    {ReturnCode, State1} =
        case {lists:member(ProtoVersion, proplists:get_keys(?PROTOCOL_NAMES)),
              valid_client_id(ClientId)} of
            {false, _} ->
                {?CONNACK_PROTO_VER, State};
            {_, false} ->
                {?CONNACK_INVALID_ID, State};
            _ ->
                case emqtt_auth:check(Username, Password) of
                    false ->
                        lager:error("MQTT login failed - no credentials"),
                        {?CONNACK_CREDENTIALS, State};
                    true ->
						lager:info("connect from clientid: ~p, keepalive: ", [ClientId, KeepAlive]),
                        start_keepalive(KeepAlive),
						emqtt_cm:register(ClientId, self()),
						{?CONNACK_ACCEPT,
						 State #proto_state{ will_msg   = make_will_msg(Var),
											 client_id  = ClientId }}
                end
        end,
		lager:info("[SENT] MQTT CONNACK: ~p", [ReturnCode]),
		send_packet(Sock, #mqtt_packet { 
                             header = #mqtt_packet_header { type = ?CONNACK }, 
                             variable = #mqtt_packet_connack{ return_code = ReturnCode }}),
    {ok, State1};

handle_packet(?PUBLISH, Packet = #mqtt_packet {
                                     header = #mqtt_packet_header {qos = ?QOS_0}}, State) ->
	emqtt_router:route(make_message(Packet)),
	{ok, State};

handle_packet(?PUBLISH, Packet = #mqtt_packet { 
                                     header = #mqtt_packet_header { qos    = ?QOS_1 }, 
                                     variable = #mqtt_packet_publish{packet_id = PacketId}}, 
				State=#proto_state{socket=Sock}) ->
	emqtt_router:route(make_message(Packet)),
	send_packet(Sock, #mqtt_packet { header = #mqtt_packet_header{ type = ?PUBACK }, 
                                     variable = #mqtt_packet_puback { packet_id = PacketId}}),
	{ok, State};

handle_packet(?PUBLISH, Packet = #mqtt_packet { 
                                     header = #mqtt_packet_header { qos    = ?QOS_2 }, 
                                     variable = #mqtt_packet_publish{packet_id = PacketId}}, 
				State=#proto_state{socket=Sock}) ->
    %%FIXME: this is not right...should store it first...
	emqtt_router:route(make_message(Packet)),
	put({msg, PacketId}, pubrec),
	send_packet(Sock, #mqtt_packet { header = #mqtt_packet_header{ type = ?PUBREC }, 
                                     variable = #mqtt_packet_puback { packet_id = PacketId}}),
	{ok, State};

handle_packet(?PUBACK, #mqtt_packet {}, State) ->
	%FIXME Later
	{ok, State};

handle_packet(?PUBREC, #mqtt_packet {
                           variable = #mqtt_packet_puback { packet_id = PktId }}, 
               State=#proto_state{socket=Sock}) ->
	%FIXME Later: should release the message here
	send_packet(Sock, #mqtt_packet {
                         header = #mqtt_packet_header { type = ?PUBREL}, 
                         variable = #mqtt_packet_puback { packet_id  = PktId}}),
	{ok, State};

handle_packet(?PUBREL, #mqtt_packet { 
                           variable = #mqtt_packet_puback { packet_id = PktId}}, 
               State=#proto_state{socket=Sock}) ->
    %%FIXME: not right...
	erase({msg, PktId}),
	send_packet(Sock, #mqtt_packet { header = #mqtt_packet_header { type = ?PUBCOMP}, 
                                     variable = #mqtt_packet_puback { packet_id = PktId}}),
	{ok, State};

handle_packet(?PUBCOMP, #mqtt_packet { 
                            variable = #mqtt_packet_puback{packet_id = _PktId}}, State) ->
	%TODO: fixme later
	{ok, State};

handle_packet(?SUBSCRIBE, #mqtt_packet { 
                              variable = #mqtt_packet_subscribe{
                                            packet_id  = PacketId, 
                                            topic_table = Topics}, 
                              payload = undefined}, 
               State = #proto_state{socket=Sock}) ->

    %%FIXME: this is not right...
	[emqtt_pubsub:subscribe({Name, Qos}, self()) || 
			#mqtt_topic{name=Name, qos=Qos} <- Topics],

    GrantedQos = [Qos || #mqtt_topic{qos=Qos} <- Topics],

    send_packet(Sock, #mqtt_packet { header = #mqtt_packet_header { type = ?SUBACK }, 
                                     variable = #mqtt_packet_suback{ 
                                                   packet_id = PacketId, 
                                                   qos_table  = GrantedQos }}),

    {ok, State};

handle_packet(?UNSUBSCRIBE, #mqtt_packet { 
                                variable = #mqtt_packet_subscribe{
                                              packet_id  = PacketId, 
                                              topic_table = Topics }, 
                                payload = undefined}, 
               State = #proto_state{socket = Sock, client_id = ClientId}) ->
	
	[emqtt_pubsub:unsubscribe(Name, self()) || #mqtt_topic{name=Name} <- Topics], 

    send_packet(Sock, #mqtt_packet { header = #mqtt_packet_header {type = ?UNSUBACK }, 
                                     variable = #mqtt_packet_suback{packet_id = PacketId }}),

    {ok, State};

handle_packet(?PINGREQ, #mqtt_packet{}, #proto_state{socket=Sock}=State) ->
    send_packet(Sock, make_packet(?PINGRESP)),
    {ok, State};

handle_packet(?DISCONNECT, #mqtt_packet{}, State=#proto_state{client_id=ClientId}) ->
	lager:info("~s disconnected", [ClientId]),
    {stop, State}.


make_packet(Type) when Type >= ?CONNECT andalso Type =< ?DISCONNECT -> 
    #mqtt_packet{ header = #mqtt_packet_header { type = Type } }.

-spec send_message(Message, State) -> {ok, NewState} when
	Message 	:: mqtt_message(),
	State		:: proto_state(),
	NewState	:: proto_state().

send_message(Message = #mqtt_message{ 
                          retain    = Retain, 
                          qos        = Qos, 
                          topic      = Topic, 
                          dup        = Dup, 
                          payload    = Payload}, 
             State = #proto_state{socket = Sock, packet_id = PacketId}) ->

    Packet = #mqtt_packet { 
                header = #mqtt_packet_header { 
                            type 	 = ?PUBLISH, 
                            qos    = Qos, 
                            retain = Retain, 
                            dup    = Dup }, 
                variable = #mqtt_packet_publish {
                             topic_name = Topic,
                             packet_id = if
                                             Qos == ?QOS_0 -> undefined; 
                                             true -> PacketId 
                                         end }, 
                payload = Payload},

	send_packet(Sock, Packet),
	if
	Qos == ?QOS_0 ->
		{ok, State};
	true ->
		{ok, next_packet_id(State)}
	end.

send_packet(Sock, Packet) ->
	lager:info("send packet:~p", [Packet]),
    %%FIXME Later...
    erlang:port_command(Sock, emqtt_packet:serialise(Packet)).

%%TODO: fix me later...
client_terminated(#proto_state{client_id = ClientId} = State) ->
    ok.
    %emqtt_cm:unregister(ClientId, self()).

make_message(#mqtt_packet { 
                header = #mqtt_packet_header{
                            qos    = Qos, 
                            retain = Retain, 
                            dup    = Dup }, 
                variable = #mqtt_packet_publish{
                              topic_name = Topic, 
                              packet_id = PacketId }, 
                payload = Payload }) ->

	#mqtt_message{ retain     = Retain, 
                   qos        = Qos, 
                   topic      = Topic, 
                   dup        = Dup, 
                   msgid      = PacketId, 
                   payload    = Payload}.

make_will_msg(#mqtt_packet_connect{ will_flag   = false }) ->
    undefined;

make_will_msg(#mqtt_packet_connect{ will_retain = Retain, 
                                    will_qos    = Qos, 
                                    will_topic  = Topic, 
                                    will_msg    = Msg }) ->
    #mqtt_message{ retain  = Retain, 
                   qos     = Qos, 
                   topic   = Topic, 
                   dup     = false, 
                   payload = Msg }.

next_packet_id(State = #proto_state{ packet_id = 16#ffff }) ->
    State #proto_state{ packet_id = 1 };
next_packet_id(State = #proto_state{ packet_id = PacketId }) ->
    State #proto_state{ packet_id = PacketId + 1 }.

valid_client_id(ClientId) ->
    ClientIdLen = size(ClientId),
    1 =< ClientIdLen andalso ClientIdLen =< ?MAX_CLIENTID_LEN.

validate_packet(?PUBLISH, #mqtt_packet {
                            variable = #mqtt_packet_publish{
                                          topic_name = Topic }}) ->
	case emqtt_topic:validate({publish, Topic}) of
	true -> ok;
	false -> {error, badtopic}
	end;

validate_packet(?UNSUBSCRIBE, #mqtt_packet { 
                                variable = #mqtt_packet_subscribe{
                                              topic_table = Topics }}) ->
	ErrTopics = [Topic || #mqtt_topic{name=Topic, qos=Qos} <- Topics,
						not emqtt_topic:validate({subscribe, Topic})],
	case ErrTopics of
	[] -> ok;
	_ -> lager:error("error topics: ~p", [ErrTopics]), {error, badtopic}
	end;

validate_packet(?SUBSCRIBE, #mqtt_packet{variable = #mqtt_packet_subscribe{topic_table = Topics}}) ->
	ErrTopics = [Topic || #mqtt_topic{name=Topic, qos=Qos} <- Topics,
						not (emqtt_topic:validate({subscribe, Topic}) and (Qos < 3))],
	case ErrTopics of
	[] -> ok;
	_ -> lager:error("error topics: ~p", [ErrTopics]), {error, badtopic}
	end;

validate_packet(_Type, _Frame) ->
	ok.

maybe_clean_sess(false, _Conn, _ClientId) ->
    % todo: establish subscription to deliver old unacknowledged messages
    ok.

%%----------------------------------------------------------------------------

send_will_msg(#proto_state{will_msg = undefined}) ->
	ignore;
send_will_msg(#proto_state{will_msg = WillMsg }) ->
	emqtt_router:route(WillMsg).

start_keepalive(0) -> ignore;
start_keepalive(Sec) when Sec > 0 ->
    self() ! {keepalive, start, Sec * 1.5}.

