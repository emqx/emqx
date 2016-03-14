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

%% @doc MQTT Protocol Processor.
-module(emqttd_protocol).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

-import(proplists, [get_value/2, get_value/3]).

%% API
-export([init/3, info/1, clientid/1, client/1, session/1]).

-export([received/2, send/2, redeliver/2, shutdown/2]).

-export([process/2]).

%% Protocol State
-record(proto_state, {peername, sendfun, connected = false,
                      client_id, client_pid, clean_sess,
                      proto_ver, proto_name, username,
                      will_msg, keepalive, max_clientid_len = ?MAX_CLIENTID_LEN,
                      session, ws_initial_headers, %% Headers from first HTTP request for websocket client
                      connected_at}).

-type proto_state() :: #proto_state{}.

-define(INFO_KEYS, [client_id, username, clean_sess, proto_ver, proto_name,
                    keepalive, will_msg, ws_initial_headers, connected_at]).

-define(LOG(Level, Format, Args, State),
            lager:Level([{client, State#proto_state.client_id}], "Client(~s@~s): " ++ Format,
                        [State#proto_state.client_id, esockd_net:format(State#proto_state.peername) | Args])).

%% @doc Init protocol
init(Peername, SendFun, Opts) ->
    MaxLen = get_value(max_clientid_len, Opts, ?MAX_CLIENTID_LEN),
    WsInitialHeaders = get_value(ws_initial_headers, Opts),
    #proto_state{peername           = Peername,
                 sendfun            = SendFun,
                 max_clientid_len   = MaxLen,
                 client_pid         = self(),
                 ws_initial_headers = WsInitialHeaders}.

info(ProtoState) ->
    ?record_to_proplist(proto_state, ProtoState, ?INFO_KEYS).

clientid(#proto_state{client_id = ClientId}) ->
    ClientId.

client(#proto_state{client_id          = ClientId,
                    client_pid         = ClientPid,
                    peername           = Peername,
                    username           = Username,
                    clean_sess         = CleanSess,
                    proto_ver          = ProtoVer,
                    keepalive          = Keepalive,
                    will_msg           = WillMsg,
                    ws_initial_headers = WsInitialHeaders,
                    connected_at       = Time}) ->
    WillTopic = if
                    WillMsg =:= undefined -> undefined;
                    true -> WillMsg#mqtt_message.topic
                end,
    #mqtt_client{client_id          = ClientId,
                 client_pid         = ClientPid,
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

%% A Client can only send the CONNECT Packet once over a Network Connection. 
-spec(received(mqtt_packet(), proto_state()) -> {ok, proto_state()} | {error, any()}).
received(Packet = ?PACKET(?CONNECT), State = #proto_state{connected = false}) ->
    process(Packet, State#proto_state{connected = true});

received(?PACKET(?CONNECT), State = #proto_state{connected = true}) ->
    {error, protocol_bad_connect, State};

%% Received other packets when CONNECT not arrived.
received(_Packet, State = #proto_state{connected = false}) ->
    {error, protocol_not_connected, State};

received(Packet = ?PACKET(_Type), State) ->
    trace(recv, Packet, State),
    case validate_packet(Packet) of
        ok ->
            process(Packet, State);
        {error, Reason} ->
            {error, Reason, State}
    end.

process(Packet = ?CONNECT_PACKET(Var), State0) ->

    #mqtt_packet_connect{proto_ver  = ProtoVer,
                         proto_name = ProtoName,
                         username   = Username,
                         password   = Password,
                         clean_sess = CleanSess,
                         keep_alive = KeepAlive,
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

    {ReturnCode1, SessPresent, State3} =
    case validate_connect(Var, State1) of
        ?CONNACK_ACCEPT ->
            case emqttd_access_control:auth(client(State1), Password) of
                ok ->
                    %% Generate clientId if null
                    State2 = maybe_set_clientid(State1),

                    %% Start session
                    case emqttd_sm:start_session(CleanSess, clientid(State2)) of
                        {ok, Session, SP} ->
                            %% Register the client
                            emqttd_cm:register(client(State2)),
                            %% Start keepalive
                            start_keepalive(KeepAlive),
                            %% ACCEPT
                            {?CONNACK_ACCEPT, SP, State2#proto_state{session = Session}};
                        {error, Error} ->
                            exit({shutdown, Error})
                    end;
                {error, Reason}->
                    ?LOG(error, "Username '~s' login failed for ~p", [Username, Reason], State1),
                    {?CONNACK_CREDENTIALS, false, State1}
            end;
        ReturnCode ->
            {ReturnCode, false, State1}
    end,
    %% Run hooks
    emqttd:run_hooks('client.connected', [ReturnCode1], client(State3)),
    %% Send connack
    send(?CONNACK_PACKET(ReturnCode1, sp(SessPresent)), State3);

process(Packet = ?PUBLISH_PACKET(_Qos, Topic, _PacketId, _Payload), State) ->
    case check_acl(publish, Topic, client(State)) of
        allow ->
            publish(Packet, State);
        deny ->
            ?LOG(error, "Cannot publish to ~s for ACL Deny", [Topic], State)
    end,
    {ok, State};

process(?PUBACK_PACKET(?PUBACK, PacketId), State = #proto_state{session = Session}) ->
    emqttd_session:puback(Session, PacketId),
    {ok, State};

process(?PUBACK_PACKET(?PUBREC, PacketId), State = #proto_state{session = Session}) ->
    emqttd_session:pubrec(Session, PacketId),
    send(?PUBREL_PACKET(PacketId), State);

process(?PUBACK_PACKET(?PUBREL, PacketId), State = #proto_state{session = Session}) ->
    emqttd_session:pubrel(Session, PacketId),
    send(?PUBACK_PACKET(?PUBCOMP, PacketId), State);

process(?PUBACK_PACKET(?PUBCOMP, PacketId), State = #proto_state{session = Session})->
    emqttd_session:pubcomp(Session, PacketId), {ok, State};

%% Protect from empty topic table
process(?SUBSCRIBE_PACKET(PacketId, []), State) ->
    send(?SUBACK_PACKET(PacketId, []), State);

process(?SUBSCRIBE_PACKET(PacketId, TopicTable), State = #proto_state{session = Session}) ->
    Client = client(State),
    AllowDenies = [check_acl(subscribe, Topic, Client) || {Topic, _Qos} <- TopicTable],
    case lists:member(deny, AllowDenies) of
        true ->
            ?LOG(error, "Cannot SUBSCRIBE ~p for ACL Deny", [TopicTable], State),
            send(?SUBACK_PACKET(PacketId, [16#80 || _ <- TopicTable]), State);
        false ->
            emqttd_session:subscribe(Session, PacketId, TopicTable), {ok, State}
    end;

%% Protect from empty topic list
process(?UNSUBSCRIBE_PACKET(PacketId, []), State) ->
    send(?UNSUBACK_PACKET(PacketId), State);

process(?UNSUBSCRIBE_PACKET(PacketId, Topics), State = #proto_state{session = Session}) ->
    emqttd_session:unsubscribe(Session, Topics),
    send(?UNSUBACK_PACKET(PacketId), State);

process(?PACKET(?PINGREQ), State) ->
    send(?PACKET(?PINGRESP), State);

process(?PACKET(?DISCONNECT), State) ->
    % Clean willmsg
    {stop, normal, State#proto_state{will_msg = undefined}}.

publish(Packet = ?PUBLISH_PACKET(?QOS_0, _PacketId),
        #proto_state{client_id = ClientId, username = Username, session = Session}) ->
    Msg = emqttd_message:from_packet(Username, ClientId, Packet),
    emqttd_session:publish(Session, Msg);

publish(Packet = ?PUBLISH_PACKET(?QOS_1, _PacketId), State) ->
    with_puback(?PUBACK, Packet, State);

publish(Packet = ?PUBLISH_PACKET(?QOS_2, _PacketId), State) ->
    with_puback(?PUBREC, Packet, State).

with_puback(Type, Packet = ?PUBLISH_PACKET(_Qos, PacketId),
            State = #proto_state{client_id = ClientId,
                                 username = Username,
                                 session = Session}) ->
    Msg = emqttd_message:from_packet(Username, ClientId, Packet),
    case emqttd_session:publish(Session, Msg) of
        ok ->
            send(?PUBACK_PACKET(Type, PacketId), State);
        {error, Error} ->
            ?LOG(error, "PUBLISH ~p error: ~p", [PacketId, Error], State)
    end.

-spec(send(mqtt_message() | mqtt_packet(), proto_state()) -> {ok, proto_state()}).
send(Msg, State = #proto_state{client_id = ClientId})
        when is_record(Msg, mqtt_message) ->
    emqttd:run_hooks('message.delivered', [ClientId], Msg),
    send(emqttd_message:to_packet(Msg), State);

send(Packet, State = #proto_state{sendfun = SendFun})
    when is_record(Packet, mqtt_packet) ->
    trace(send, Packet, State),
    emqttd_metrics:sent(Packet),
    Data = emqttd_serializer:serialize(Packet),
    ?LOG(debug, "SEND ~p", [Data], State),
    emqttd_metrics:inc('bytes/sent', size(Data)),
    SendFun(Data),
    {ok, State}.

trace(recv, Packet, ProtoState) ->
    ?LOG(info, "RECV ~s", [emqttd_packet:format(Packet)], ProtoState);

trace(send, Packet, ProtoState) ->
    ?LOG(info, "SEND ~s", [emqttd_packet:format(Packet)], ProtoState).

%% @doc redeliver PUBREL PacketId
redeliver({?PUBREL, PacketId}, State) ->
    send(?PUBREL_PACKET(PacketId), State).

shutdown(_Error, #proto_state{client_id = undefined}) ->
    ignore;

shutdown(conflict, #proto_state{client_id = _ClientId}) ->
    %% let it down
    %% emqttd_cm:unregister(ClientId);
    ignore;

shutdown(Error, State = #proto_state{client_id = ClientId, will_msg = WillMsg}) ->
    ?LOG(info, "Shutdown for ~p", [Error], State),
    send_willmsg(ClientId, WillMsg),
    emqttd:run_hooks('client.disconnected', [Error], ClientId),
    %% let it down
    %% emqttd_cm:unregister(ClientId).
    ok.

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
    emqttd:publish(WillMsg#mqtt_message{from = ClientId}).

start_keepalive(0) -> ignore;

start_keepalive(Sec) when Sec > 0 ->
    self() ! {keepalive, start, round(Sec * 1.25)}.

%%--------------------------------------------------------------------
%% Validate Packets
%%--------------------------------------------------------------------

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

validate_clientid(#mqtt_packet_connect{client_id = ClientId},
                  #proto_state{max_clientid_len = MaxLen})
    when (size(ClientId) >= 1) andalso (size(ClientId) =< MaxLen) ->
    true;

%% MQTT3.1.1 allow null clientId.
validate_clientid(#mqtt_packet_connect{proto_ver =?MQTT_PROTO_V311,
                                       client_id = ClientId}, _ProtoState)
    when size(ClientId) =:= 0 ->
    true;

validate_clientid(#mqtt_packet_connect{proto_ver  = ProtoVer,
                                       clean_sess = CleanSess}, ProtoState) ->
    ?LOG(warning, "Invalid clientId. ProtoVer: ~p, CleanSess: ~s",
         [ProtoVer, CleanSess], ProtoState),
    false.

validate_packet(?PUBLISH_PACKET(_Qos, Topic, _PacketId, _Payload)) ->
    case emqttd_topic:validate({name, Topic}) of
        true  -> ok;
        false -> {error, badtopic}
    end;

validate_packet(?SUBSCRIBE_PACKET(_PacketId, TopicTable)) ->
    validate_topics(filter, TopicTable);

validate_packet(?UNSUBSCRIBE_PACKET(_PacketId, Topics)) ->
    validate_topics(filter, Topics);

validate_packet(_Packet) -> 
    ok.

validate_topics(_Type, []) ->
    {error, empty_topics};

validate_topics(Type, TopicTable = [{_Topic, _Qos}|_])
    when Type =:= name orelse Type =:= filter ->
    Valid = fun(Topic, Qos) ->
              emqttd_topic:validate({Type, Topic}) and validate_qos(Qos)
            end,
    case [Topic || {Topic, Qos} <- TopicTable, not Valid(Topic, Qos)] of
        [] -> ok;
        _  -> {error, badtopic}
    end;

validate_topics(Type, Topics = [Topic0|_]) when is_binary(Topic0) ->
    case [Topic || Topic <- Topics, not emqttd_topic:validate({Type, Topic})] of
        [] -> ok;
        _  -> {error, badtopic}
    end.

validate_qos(undefined) ->
    true;
validate_qos(Qos) when ?IS_QOS(Qos) ->
    true;
validate_qos(_) ->
    false.

%% PUBLISH ACL is cached in process dictionary.
check_acl(publish, Topic, Client) ->
    case get({acl, publish, Topic}) of
        undefined ->
            AllowDeny = emqttd_access_control:check_acl(Client, publish, Topic),
            put({acl, publish, Topic}, AllowDeny),
            AllowDeny;
        AllowDeny ->
            AllowDeny
    end;

check_acl(subscribe, Topic, Client) ->
    emqttd_access_control:check_acl(Client, subscribe, Topic).

sp(true)  -> 1;
sp(false) -> 0.

