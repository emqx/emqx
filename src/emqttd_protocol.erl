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
%%% emqttd protocol.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_protocol).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

%% API
-export([init/3, info/1, clientid/1, client/1, session/1]).

-export([received/2, send/2, redeliver/2, shutdown/2]).

-export([handle/2]).

%% Protocol State
-record(proto_state, {peername,
                      sendfun,
                      connected = false, %received CONNECT action?
                      proto_ver,
                      proto_name,
                      username,
                      client_id,
                      clean_sess,
                      session,
                      will_msg,
                      keepalive,
                      max_clientid_len = ?MAX_CLIENTID_LEN,
                      client_pid,
                      ws_initial_headers, %% Headers from first HTTP request for websocket client
                      connected_at}).

-type proto_state() :: #proto_state{}.

%%------------------------------------------------------------------------------
%% @doc Init protocol
%% @end
%%------------------------------------------------------------------------------

init(Peername, SendFun, Opts) ->
    MaxLen = proplists:get_value(max_clientid_len, Opts, ?MAX_CLIENTID_LEN),
    WsInitialHeaders = proplists:get_value(ws_initial_headers, Opts),
	#proto_state{peername           = Peername,
                 sendfun            = SendFun,
                 max_clientid_len   = MaxLen,
                 client_pid         = self(),
                 ws_initial_headers = WsInitialHeaders}.

info(#proto_state{client_id	         = ClientId,
                  username           = Username,
                  peername           = Peername,
                  proto_ver          = ProtoVer,
                  proto_name         = ProtoName,
                  keepalive          = KeepAlive,
				  clean_sess         = CleanSess,
                  ws_initial_headers = WsInitialHeaders,
				  will_msg	         = WillMsg,
                  connected_at       = ConnectedAt}) ->
    [{client_id, ClientId},
     {username, Username},
     {peername, Peername},
     {proto_ver, ProtoVer},
     {proto_name, ProtoName},
     {keepalive, KeepAlive},
     {clean_sess, CleanSess},
     {ws_initial_headers, WsInitialHeaders},
     {will_msg,	WillMsg},
     {connected_at, ConnectedAt}].

clientid(#proto_state{client_id = ClientId}) ->
    ClientId.

client(#proto_state{client_id          = ClientId,
                    peername           = Peername,
                    username           = Username,
                    clean_sess         = CleanSess,
                    proto_ver          = ProtoVer,
                    keepalive          = Keepalive,
                    will_msg           = WillMsg,
                    client_pid         = Pid,
                    ws_initial_headers = WsInitialHeaders,
                    connected_at       = Time}) ->
    WillTopic = if
                    WillMsg =:= undefined -> undefined;
                    true -> WillMsg#mqtt_message.topic
                end,
    #mqtt_client{client_id          = ClientId,
                 client_pid         = Pid,
                 username           = Username,
                 peername           = Peername,
                 clean_sess         = CleanSess,
                 proto_ver          = ProtoVer,
                 keepalive          = Keepalive,
                 will_topic         = WillTopic,
                 ws_initial_headers = WsInitialHeaders,
                 connected_at       = Time}.

session(#proto_state{session = Session}) ->
    Session.

%% CONNECT – Client requests a connection to a Server

%%A Client can only send the CONNECT Packet once over a Network Connection. 
-spec received(mqtt_packet(), proto_state()) -> {ok, proto_state()} | {error, any()}. 
received(Packet = ?PACKET(?CONNECT), State = #proto_state{connected = false}) ->
    handle(Packet, State#proto_state{connected = true});

received(?PACKET(?CONNECT), State = #proto_state{connected = true}) ->
    {error, protocol_bad_connect, State};

%%Received other packets when CONNECT not arrived.
received(_Packet, State = #proto_state{connected = false}) ->
    {error, protocol_not_connected, State};

received(Packet = ?PACKET(_Type), State) ->
    trace(recv, Packet, State),
	case validate_packet(Packet) of
	ok ->
        handle(Packet, State);
	{error, Reason} ->
		{error, Reason, State}
	end.

handle(Packet = ?CONNECT_PACKET(Var), State0 = #proto_state{peername = Peername}) ->

    #mqtt_packet_connect{proto_ver  = ProtoVer,
                         proto_name = ProtoName,
                         username   = Username,
                         password   = Password,
                         clean_sess = CleanSess,
                         keep_alive  = KeepAlive,
                         client_id  = ClientId} = Var,

    State1 = State0#proto_state{proto_ver  = ProtoVer,
                                proto_name = ProtoName,
                                username   = Username,
                                client_id  = ClientId,
                                clean_sess = CleanSess,
                                keepalive  = KeepAlive,
                                will_msg   = willmsg(Var),
                                connected_at = os:timestamp()},

    trace(recv, Packet, State1),

    {ReturnCode1, State3} =
    case validate_connect(Var, State1) of
        ?CONNACK_ACCEPT ->
            case emqttd_access_control:auth(client(State1), Password) of
                ok ->
                    %% Generate clientId if null
                    State2 = maybe_set_clientid(State1),

                    %% Start session
                    case emqttd_sm:start_session(CleanSess, clientid(State2)) of
                        {ok, Session} ->
                            %% Register the client
                            emqttd_cm:register(client(State2)),
                            %% Start keepalive
                            start_keepalive(KeepAlive),
                            %% ACCEPT
                            {?CONNACK_ACCEPT, State2#proto_state{session = Session}};
                        {error, Error} ->
                            exit({shutdown, Error})
                    end;
                {error, Reason}->
                    lager:error("~s@~s: username '~s', login failed - ~s",
                                    [ClientId, emqttd_net:format(Peername), Username, Reason]),
                    {?CONNACK_CREDENTIALS, State1}
                                                        
            end;
        ReturnCode ->
            {ReturnCode, State1}
    end,
    %% Run hooks
    emqttd_broker:foreach_hooks('client.connected', [ReturnCode1, client(State3)]),
    %% Send connack
    send(?CONNACK_PACKET(ReturnCode1), State3);

handle(Packet = ?PUBLISH_PACKET(_Qos, Topic, _PacketId, _Payload),
           State = #proto_state{client_id = ClientId}) ->

    case check_acl(publish, Topic, State) of
        allow ->
            publish(Packet, State);
        deny -> 
            lager:error("ACL Deny: ~s cannot publish to ~s", [ClientId, Topic])
    end,
	{ok, State};

handle(?PUBACK_PACKET(?PUBACK, PacketId), State = #proto_state{session = Session}) ->
    emqttd_session:puback(Session, PacketId),
    {ok, State};

handle(?PUBACK_PACKET(?PUBREC, PacketId), State = #proto_state{session = Session}) ->
    emqttd_session:pubrec(Session, PacketId),
    send(?PUBREL_PACKET(PacketId), State);

handle(?PUBACK_PACKET(?PUBREL, PacketId), State = #proto_state{session = Session}) ->
    emqttd_session:pubrel(Session, PacketId),
    send(?PUBACK_PACKET(?PUBCOMP, PacketId), State);

handle(?PUBACK_PACKET(?PUBCOMP, PacketId), State = #proto_state{session = Session}) ->
    emqttd_session:pubcomp(Session, PacketId),
    {ok, State};

%% protect from empty topic list
handle(?SUBSCRIBE_PACKET(PacketId, []), State) ->
    send(?SUBACK_PACKET(PacketId, []), State);

handle(?SUBSCRIBE_PACKET(PacketId, TopicTable), State = #proto_state{client_id = ClientId, session = Session}) ->
    AllowDenies = [check_acl(subscribe, Topic, State) || {Topic, _Qos} <- TopicTable],
    case lists:member(deny, AllowDenies) of
        true ->
            %%TODO: return 128 QoS when deny... no need to SUBACK?
            lager:error("SUBSCRIBE from '~s' Denied: ~p", [ClientId, TopicTable]),
            {ok, State};
        false ->
            %%TODO: GrantedQos should be renamed.
            {ok, GrantedQos} = emqttd_session:subscribe(Session, TopicTable),
            send(?SUBACK_PACKET(PacketId, GrantedQos), State)
    end;

handle({subscribe, TopicTable}, State = #proto_state{session = Session}) ->
    {ok, _GrantedQos} = emqttd_session:subscribe(Session, TopicTable),
    {ok, State};

%% protect from empty topic list
handle(?UNSUBSCRIBE_PACKET(PacketId, []), State) ->
    send(?UNSUBACK_PACKET(PacketId), State);

handle(?UNSUBSCRIBE_PACKET(PacketId, Topics), State = #proto_state{session = Session}) ->
    ok = emqttd_session:unsubscribe(Session, Topics),
    send(?UNSUBACK_PACKET(PacketId), State);

handle(?PACKET(?PINGREQ), State) ->
    send(?PACKET(?PINGRESP), State);

handle(?PACKET(?DISCONNECT), State) ->
    % clean willmsg
    {stop, normal, State#proto_state{will_msg = undefined}}.

publish(Packet = ?PUBLISH(?QOS_0, _PacketId), #proto_state{client_id = ClientId, session = Session}) ->
    emqttd_session:publish(Session, emqttd_message:from_packet(ClientId, Packet));

publish(Packet = ?PUBLISH(?QOS_1, PacketId), State = #proto_state{client_id = ClientId, session = Session}) ->
    case emqttd_session:publish(Session, emqttd_message:from_packet(ClientId, Packet)) of
        ok ->
            send(?PUBACK_PACKET(?PUBACK, PacketId), State);
        {error, Error} ->
            lager:error("Client ~s: publish qos1 error - ~p", [ClientId, Error])
    end;

publish(Packet = ?PUBLISH(?QOS_2, PacketId), State = #proto_state{client_id = ClientId, session = Session}) ->
    case emqttd_session:publish(Session, emqttd_message:from_packet(ClientId, Packet)) of
        ok ->
            send(?PUBACK_PACKET(?PUBREC, PacketId), State);
        {error, Error} ->
            lager:error("Client ~s: publish qos2 error - ~p", [ClientId, Error])
    end.

-spec send(mqtt_message() | mqtt_packet(), proto_state()) -> {ok, proto_state()}.
send(Msg, State) when is_record(Msg, mqtt_message) ->
	send(emqttd_message:to_packet(Msg), State);

send(Packet, State = #proto_state{sendfun = SendFun, peername = Peername})
        when is_record(Packet, mqtt_packet) ->
    trace(send, Packet, State),
    sent_stats(Packet),
    Data = emqttd_serialiser:serialise(Packet),
    lager:debug("SENT to ~s: ~p", [emqttd_net:format(Peername), Data]),
    emqttd_metrics:inc('bytes/sent', size(Data)),
    SendFun(Data),
    {ok, State}.

trace(recv, Packet, #proto_state{peername = Peername, client_id = ClientId}) ->
    lager:info([{client, ClientId}], "RECV from ~s@~s: ~s",
                   [ClientId, emqttd_net:format(Peername), emqttd_packet:format(Packet)]);

trace(send, Packet, #proto_state{peername  = Peername, client_id = ClientId}) ->
	lager:info([{client, ClientId}], "SEND to ~s@~s: ~s",
                   [ClientId, emqttd_net:format(Peername), emqttd_packet:format(Packet)]).

%% @doc redeliver PUBREL PacketId
redeliver({?PUBREL, PacketId}, State) ->
    send(?PUBREL_PACKET(PacketId), State).

shutdown(duplicate_id, _State) ->
    quiet; %%

shutdown(Error, #proto_state{client_id = undefined}) ->
    lager:info("Protocol shutdown ~p", [Error]),
    ignore;

%% TODO: ClientId??
shutdown(Error, #proto_state{peername = Peername, client_id = ClientId, will_msg = WillMsg}) ->
	lager:info([{client, ClientId}], "Client ~s@~s: shutdown ~p",
                   [ClientId, emqttd_net:format(Peername), Error]),
    send_willmsg(ClientId, WillMsg),
    emqttd_broker:foreach_hooks('client.disconnected', [Error, ClientId]),
    emqttd_cm:unregister(ClientId).

willmsg(Packet) when is_record(Packet, mqtt_packet_connect) ->
    emqttd_message:from_packet(Packet).

%% Generate a client if if nulll
maybe_set_clientid(State = #proto_state{client_id = NullId})
        when NullId =:= undefined orelse NullId =:= <<>> ->
    {_, NPid, _} = emqttd_guid:new(),
    ClientId = iolist_to_binary(["emqttd_", integer_to_list(NPid)]),
    State#proto_state{client_id = ClientId};

maybe_set_clientid(State) ->
    State.

send_willmsg(_ClientId, undefined) ->
    ignore;
send_willmsg(ClientId, WillMsg) -> 
    lager:info("Client ~s send willmsg: ~p", [ClientId, WillMsg]),
    emqttd_pubsub:publish(WillMsg#mqtt_message{from = ClientId}).

start_keepalive(0) -> ignore;

start_keepalive(Sec) when Sec > 0 ->
    self() ! {keepalive, start, round(Sec * 1.5)}.

%%----------------------------------------------------------------------------
%% Validate Packets
%%----------------------------------------------------------------------------
validate_connect(Connect = #mqtt_packet_connect{}, ProtoState) ->
    case validate_protocol(Connect) of
        true -> 
            case validate_clientid(Connect, ProtoState) of
                true -> 
                    ?CONNACK_ACCEPT;
                false -> 
                    ?CONNACK_INVALID_ID
            end;
        false -> 
            ?CONNACK_PROTO_VER
    end.

validate_protocol(#mqtt_packet_connect{proto_ver = Ver, proto_name = Name}) ->
    lists:member({Ver, Name}, ?PROTOCOL_NAMES).

validate_clientid(#mqtt_packet_connect{client_id = ClientId}, #proto_state{max_clientid_len = MaxLen})
    when ( size(ClientId) >= 1 ) andalso ( size(ClientId) =< MaxLen ) ->
    true;

%% MQTT3.1.1 allow null clientId.
validate_clientid(#mqtt_packet_connect{proto_ver =?MQTT_PROTO_V311, client_id = ClientId}, _ProtoState) 
    when size(ClientId) =:= 0 ->
    true;

validate_clientid(#mqtt_packet_connect {proto_ver = Ver, clean_sess = CleanSess, client_id = ClientId}, _ProtoState) -> 
    lager:warning("Invalid ClientId: ~s, ProtoVer: ~p, CleanSess: ~s", [ClientId, Ver, CleanSess]),
    false.

validate_packet(#mqtt_packet{header  = #mqtt_packet_header{type = ?PUBLISH}, 
                             variable = #mqtt_packet_publish{topic_name = Topic}}) ->
	case emqttd_topic:validate({name, Topic}) of
	true -> ok;
	false -> lager:warning("Error publish topic: ~p", [Topic]), {error, badtopic}
	end;

validate_packet(#mqtt_packet{header  = #mqtt_packet_header{type = ?SUBSCRIBE},
                             variable = #mqtt_packet_subscribe{topic_table = Topics}}) ->

    validate_topics(filter, Topics);

validate_packet(#mqtt_packet{ header  = #mqtt_packet_header{type = ?UNSUBSCRIBE}, 
                              variable = #mqtt_packet_subscribe{topic_table = Topics}}) ->

    validate_topics(filter, Topics);

validate_packet(_Packet) -> 
    ok.

validate_topics(Type, []) when Type =:= name orelse Type =:= filter ->
	lager:error("Empty Topics!"),
    {error, empty_topics};

validate_topics(Type, Topics) when Type =:= name orelse Type =:= filter ->
	ErrTopics = [Topic || {Topic, Qos} <- Topics,
						not (emqttd_topic:validate({Type, Topic}) and validate_qos(Qos))],
	case ErrTopics of
	[] -> ok;
	_ -> lager:error("Error Topics: ~p", [ErrTopics]), {error, badtopic}
	end.

validate_qos(undefined) -> true;
validate_qos(Qos) when Qos =< ?QOS_2 -> true;
validate_qos(_) -> false.

%% publish ACL is cached in process dictionary.
check_acl(publish, Topic, State) ->
    case get({acl, publish, Topic}) of
        undefined ->
            AllowDeny = emqttd_access_control:check_acl(client(State), publish, Topic),
            put({acl, publish, Topic}, AllowDeny),
            AllowDeny;
        AllowDeny ->
            AllowDeny
    end;

check_acl(subscribe, Topic, State) ->
    emqttd_access_control:check_acl(client(State), subscribe, Topic).

sent_stats(?PACKET(Type)) ->
    emqttd_metrics:inc('packets/sent'), 
    inc(Type).
inc(?CONNACK) ->
    emqttd_metrics:inc('packets/connack');
inc(?PUBLISH) ->
    emqttd_metrics:inc('messages/sent'),
    emqttd_metrics:inc('packets/publish/sent');
inc(?SUBACK) ->
    emqttd_metrics:inc('packets/suback');
inc(?UNSUBACK) ->
    emqttd_metrics:inc('packets/unsuback');
inc(?PINGRESP) ->
    emqttd_metrics:inc('packets/pingresp');
inc(_) ->
    ingore.

