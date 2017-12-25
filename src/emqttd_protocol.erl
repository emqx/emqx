%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqttd_protocol).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

-import(proplists, [get_value/2, get_value/3]).

%% API
-export([init/3, init/4, info/1, stats/1, clientid/1, client/1, session/1]).

-export([subscribe/2, unsubscribe/2, pubrel/2, shutdown/2]).

-export([received/2, send/2]).

-export([process/2]).

-record(proto_stats, {enable_stats = false, recv_pkt = 0, recv_msg = 0,
                      send_pkt = 0, send_msg = 0}).

%% Protocol State
%% ws_initial_headers: Headers from first HTTP request for WebSocket Client.
-record(proto_state, {peername, sendfun, connected = false, client_id, client_pid,
                      clean_sess, proto_ver, proto_name, username, is_superuser,
                      will_msg, keepalive, keepalive_backoff, max_clientid_len,
                      session, stats_data, mountpoint, ws_initial_headers,
                      peercert_username, is_bridge, connected_at}).

-type(proto_state() :: #proto_state{}).

-define(INFO_KEYS, [client_id, username, clean_sess, proto_ver, proto_name,
                    keepalive, will_msg, ws_initial_headers, mountpoint,
                    peercert_username, connected_at]).

-define(STATS_KEYS, [recv_pkt, recv_msg, send_pkt, send_msg]).

-define(LOG(Level, Format, Args, State),
            lager:Level([{client, State#proto_state.client_id}], "Client(~s@~s): " ++ Format,
                        [State#proto_state.client_id, esockd_net:format(State#proto_state.peername) | Args])).

%% @doc Init protocol
init(Peername, SendFun, Opts) ->
    Backoff = get_value(keepalive_backoff, Opts, 1.25),
    EnableStats = get_value(client_enable_stats, Opts, false),
    MaxLen = get_value(max_clientid_len, Opts, ?MAX_CLIENTID_LEN),
    WsInitialHeaders = get_value(ws_initial_headers, Opts),
    #proto_state{peername           = Peername,
                 sendfun            = SendFun,
                 max_clientid_len   = MaxLen,
                 is_superuser       = false,
                 client_pid         = self(),
                 peercert_username  = undefined,
                 ws_initial_headers = WsInitialHeaders,
                 keepalive_backoff  = Backoff,
                 stats_data         = #proto_stats{enable_stats = EnableStats}}.

init(Conn, Peername, SendFun, Opts) ->
    enrich_opt(Conn:opts(), Conn, init(Peername, SendFun, Opts)).

enrich_opt([], _Conn, State) ->
    State;
enrich_opt([{mountpoint, MountPoint} | ConnOpts], Conn, State) ->
    enrich_opt(ConnOpts, Conn, State#proto_state{mountpoint = MountPoint});
enrich_opt([{peer_cert_as_username, N} | ConnOpts], Conn, State) ->
    enrich_opt(ConnOpts, Conn, State#proto_state{peercert_username = peercert_username(N, Conn)});
enrich_opt([_ | ConnOpts], Conn, State) ->
    enrich_opt(ConnOpts, Conn, State).

peercert_username(cn, Conn) ->
    Conn:peer_cert_common_name();
peercert_username(dn, Conn) ->
    Conn:peer_cert_subject().

repl_username_with_peercert(State = #proto_state{peercert_username = undefined}) ->
    State;
repl_username_with_peercert(State = #proto_state{peercert_username = PeerCert}) ->
    State#proto_state{username = PeerCert}.

info(ProtoState) ->
    ?record_to_proplist(proto_state, ProtoState, ?INFO_KEYS).

stats(#proto_state{stats_data = Stats}) ->
    tl(?record_to_proplist(proto_stats, Stats)).

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
                    mountpoint         = MountPoint,
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
                 mountpoint         = MountPoint,
                 connected_at       = Time}.

session(#proto_state{session = Session}) ->
    Session.

%% CONNECT – Client requests a connection to a Server

%% A Client can only send the CONNECT Packet once over a Network Connection. 
-spec(received(mqtt_packet(), proto_state()) -> {ok, proto_state()} | {error, term()}).
received(Packet = ?PACKET(?CONNECT),
         State = #proto_state{connected = false, stats_data = Stats}) ->
    trace(recv, Packet, State), Stats1 = inc_stats(recv, ?CONNECT, Stats),
    process(Packet, State#proto_state{connected = true, stats_data = Stats1});

received(?PACKET(?CONNECT), State = #proto_state{connected = true}) ->
    {error, protocol_bad_connect, State};

%% Received other packets when CONNECT not arrived.
received(_Packet, State = #proto_state{connected = false}) ->
    {error, protocol_not_connected, State};

received(Packet = ?PACKET(Type), State = #proto_state{stats_data = Stats}) ->
    trace(recv, Packet, State), Stats1 = inc_stats(recv, Type, Stats),
    case validate_packet(Packet) of
        ok ->
            process(Packet, State#proto_state{stats_data = Stats1});
        {error, Reason} ->
            {error, Reason, State}
    end.

subscribe(RawTopicTable, ProtoState = #proto_state{client_id = ClientId,
                                                   username  = Username,
                                                   session   = Session}) ->
    TopicTable = parse_topic_table(RawTopicTable),
    case emqttd_hooks:run('client.subscribe', [ClientId, Username], TopicTable) of
        {ok, TopicTable1} ->
            emqttd_session:subscribe(Session, TopicTable1);
        {stop, _} ->
            ok
    end,
    {ok, ProtoState}.

unsubscribe(RawTopics, ProtoState = #proto_state{client_id = ClientId,
                                                 username  = Username,
                                                 session   = Session}) ->
    case emqttd_hooks:run('client.unsubscribe', [ClientId, Username], parse_topics(RawTopics)) of
        {ok, TopicTable} ->
            emqttd_session:unsubscribe(Session, TopicTable);
        {stop, _} ->
            ok
    end,
    {ok, ProtoState}.

%% @doc Send PUBREL
pubrel(PacketId, State) -> send(?PUBREL_PACKET(PacketId), State).

process(?CONNECT_PACKET(Var), State0) ->

    #mqtt_packet_connect{proto_ver  = ProtoVer,
                         proto_name = ProtoName,
                         username   = Username,
                         password   = Password,
                         clean_sess = CleanSess,
                         keep_alive = KeepAlive,
                         client_id  = ClientId,
                         is_bridge  = IsBridge} = Var,

    State1 = repl_username_with_peercert(
               State0#proto_state{proto_ver    = ProtoVer,
                                  proto_name   = ProtoName,
                                  username     = Username,
                                  client_id    = ClientId,
                                  clean_sess   = CleanSess,
                                  keepalive    = KeepAlive,
                                  will_msg     = willmsg(Var, State0),
                                  is_bridge    = IsBridge,
                                  connected_at = os:timestamp()}),

    {ReturnCode1, SessPresent, State3} =
    case validate_connect(Var, State1) of
        ?CONNACK_ACCEPT ->
            case authenticate(client(State1), Password) of
                {ok, IsSuperuser} ->
                    %% Generate clientId if null
                    State2 = maybe_set_clientid(State1),

                    %% Start session
                    case emqttd_sm:start_session(CleanSess, {clientid(State2), Username}) of
                        {ok, Session, SP} ->
                            %% Register the client
                            emqttd_cm:reg(client(State2)),
                            %% Start keepalive
                            start_keepalive(KeepAlive, State2),
                            %% Emit Stats
                            self() ! emit_stats,
                            %% ACCEPT
                            {?CONNACK_ACCEPT, SP, State2#proto_state{session = Session, is_superuser = IsSuperuser}};
                        {error, Error} ->
                            {stop, {shutdown, Error}, State2}
                    end;
                {error, Reason}->
                    ?LOG(error, "Username '~s' login failed for ~p", [Username, Reason], State1),
                    {?CONNACK_CREDENTIALS, false, State1}
            end;
        ReturnCode ->
            {ReturnCode, false, State1}
    end,
    %% Run hooks
    emqttd_hooks:run('client.connected', [ReturnCode1], client(State3)),
    %% Send connack
    send(?CONNACK_PACKET(ReturnCode1, sp(SessPresent)), State3),
    %% stop if authentication failure
    stop_if_auth_failure(ReturnCode1, State3);

process(Packet = ?PUBLISH_PACKET(_Qos, Topic, _PacketId, _Payload), State = #proto_state{is_superuser = IsSuper}) ->
    case IsSuper orelse allow == check_acl(publish, Topic, client(State)) of
        true  -> publish(Packet, State);
        false -> ?LOG(error, "Cannot publish to ~s for ACL Deny", [Topic], State)
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

%% TODO: refactor later...
process(?SUBSCRIBE_PACKET(PacketId, RawTopicTable),
        State = #proto_state{client_id    = ClientId,
                             username     = Username,
                             is_superuser = IsSuperuser,
                             mountpoint   = MountPoint,
                             session      = Session}) ->
    Client = client(State), TopicTable = parse_topic_table(RawTopicTable),
    AllowDenies = if
                    IsSuperuser -> [];
                    true -> [check_acl(subscribe, Topic, Client) || {Topic, _Opts} <- TopicTable]
                  end,
    case lists:member(deny, AllowDenies) of
        true ->
            ?LOG(error, "Cannot SUBSCRIBE ~p for ACL Deny", [TopicTable], State),
            send(?SUBACK_PACKET(PacketId, [16#80 || _ <- TopicTable]), State);
        false ->
            case emqttd_hooks:run('client.subscribe', [ClientId, Username], TopicTable) of
                {ok, TopicTable1} ->
                    emqttd_session:subscribe(Session, PacketId, mount(MountPoint, TopicTable1)),
                    {ok, State};
                {stop, _} ->
                    {ok, State}
            end
    end;

%% Protect from empty topic list
process(?UNSUBSCRIBE_PACKET(PacketId, []), State) ->
    send(?UNSUBACK_PACKET(PacketId), State);

process(?UNSUBSCRIBE_PACKET(PacketId, RawTopics),
        State = #proto_state{client_id  = ClientId,
                             username   = Username,
                             mountpoint = MountPoint,
                             session    = Session}) ->
    case emqttd_hooks:run('client.unsubscribe', [ClientId, Username], parse_topics(RawTopics)) of
        {ok, TopicTable} ->
            emqttd_session:unsubscribe(Session, mount(MountPoint, TopicTable));
        {stop, _} ->
            ok
    end,
    send(?UNSUBACK_PACKET(PacketId), State);

process(?PACKET(?PINGREQ), State) ->
    send(?PACKET(?PINGRESP), State);

process(?PACKET(?DISCONNECT), State) ->
    % Clean willmsg
    {stop, normal, State#proto_state{will_msg = undefined}}.

publish(Packet = ?PUBLISH_PACKET(?QOS_0, _PacketId),
        #proto_state{client_id  = ClientId,
                     username   = Username,
                     mountpoint = MountPoint,
                     session    = Session}) ->
    Msg = emqttd_message:from_packet(Username, ClientId, Packet),
    emqttd_session:publish(Session, mount(MountPoint, Msg));

publish(Packet = ?PUBLISH_PACKET(?QOS_1, _PacketId), State) ->
    with_puback(?PUBACK, Packet, State);

publish(Packet = ?PUBLISH_PACKET(?QOS_2, _PacketId), State) ->
    with_puback(?PUBREC, Packet, State).

with_puback(Type, Packet = ?PUBLISH_PACKET(_Qos, PacketId),
            State = #proto_state{client_id  = ClientId,
                                 username   = Username,
                                 mountpoint = MountPoint,
                                 session    = Session}) ->
    Msg = emqttd_message:from_packet(Username, ClientId, Packet),
    case emqttd_session:publish(Session, mount(MountPoint, Msg)) of
        ok ->
            send(?PUBACK_PACKET(Type, PacketId), State);
        {error, Error} ->
            ?LOG(error, "PUBLISH ~p error: ~p", [PacketId, Error], State)
    end.

-spec(send(mqtt_message() | mqtt_packet(), proto_state()) -> {ok, proto_state()}).
send(Msg, State = #proto_state{client_id  = ClientId,
                               username   = Username,
                               mountpoint = MountPoint,
                               is_bridge  = IsBridge})
        when is_record(Msg, mqtt_message) ->
    emqttd_hooks:run('message.delivered', [ClientId, Username], Msg),
    send(emqttd_message:to_packet(unmount(MountPoint, clean_retain(IsBridge, Msg))), State);

send(Packet = ?PACKET(Type), State = #proto_state{sendfun = SendFun, stats_data = Stats}) ->
    trace(send, Packet, State),
    emqttd_metrics:sent(Packet),
    SendFun(Packet),
    {ok, State#proto_state{stats_data = inc_stats(send, Type, Stats)}}.

trace(recv, Packet, ProtoState) ->
    ?LOG(debug, "RECV ~s", [emqttd_packet:format(Packet)], ProtoState);

trace(send, Packet, ProtoState) ->
    ?LOG(debug, "SEND ~s", [emqttd_packet:format(Packet)], ProtoState).

inc_stats(_Direct, _Type, Stats = #proto_stats{enable_stats = false}) ->
    Stats;

inc_stats(recv, Type, Stats) ->
    #proto_stats{recv_pkt = PktCnt, recv_msg = MsgCnt} = Stats,
    inc_stats(Type, #proto_stats.recv_pkt, PktCnt, #proto_stats.recv_msg, MsgCnt, Stats);

inc_stats(send, Type, Stats) ->
    #proto_stats{send_pkt = PktCnt, send_msg = MsgCnt} = Stats,
    inc_stats(Type, #proto_stats.send_pkt, PktCnt, #proto_stats.send_msg, MsgCnt, Stats).

inc_stats(Type, PktPos, PktCnt, MsgPos, MsgCnt, Stats) ->
    Stats1 = setelement(PktPos, Stats, PktCnt + 1),
    case Type =:= ?PUBLISH of
        true  -> setelement(MsgPos, Stats1, MsgCnt + 1);
        false -> Stats1
    end.

stop_if_auth_failure(RC, State) when RC == ?CONNACK_CREDENTIALS; RC == ?CONNACK_AUTH ->
    {stop, {shutdown, auth_failure}, State};

stop_if_auth_failure(_RC, State) ->
    {ok, State}.

shutdown(_Error, #proto_state{client_id = undefined}) ->
    ignore;
shutdown(conflict, _State) ->
    %% let it down
    ignore;
shutdown(mnesia_conflict, _State) ->
    %% let it down
    ignore;
shutdown(Error, State = #proto_state{will_msg = WillMsg}) ->
    ?LOG(debug, "Shutdown for ~p", [Error], State),
    Client = client(State),
    %% Auth failure not publish the will message
    case Error =:= auth_failure of
        true -> ok;
        false -> send_willmsg(Client, WillMsg)
    end,
    emqttd_hooks:run('client.disconnected', [Error], Client),
    %% let it down
    %% emqttd_cm:unreg(ClientId).
    ok.

willmsg(Packet, #proto_state{mountpoint = MountPoint}) when is_record(Packet, mqtt_packet_connect) ->
    case emqttd_message:from_packet(Packet) of
        undefined -> undefined;
        Msg -> mount(MountPoint, Msg)
    end.

%% Generate a client if if nulll
maybe_set_clientid(State = #proto_state{client_id = NullId})
        when NullId =:= undefined orelse NullId =:= <<>> ->
    {_, NPid, _} = emqttd_guid:new(),
    ClientId = iolist_to_binary(["emqttd_", integer_to_list(NPid)]),
    State#proto_state{client_id = ClientId};

maybe_set_clientid(State) ->
    State.

send_willmsg(_Client, undefined) ->
    ignore;
send_willmsg(#mqtt_client{client_id = ClientId, username = Username}, WillMsg) ->
    emqttd:publish(WillMsg#mqtt_message{from = {ClientId, Username}}).

start_keepalive(0, _State) -> ignore;

start_keepalive(Sec, #proto_state{keepalive_backoff = Backoff}) when Sec > 0 ->
    self() ! {keepalive, start, round(Sec * Backoff)}.

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
    when (byte_size(ClientId) >= 1) andalso (byte_size(ClientId) =< MaxLen) ->
    true;

%% Issue#599: Null clientId and clean_sess = false
validate_clientid(#mqtt_packet_connect{client_id  = ClientId,
                                       clean_sess = CleanSess}, _ProtoState)
    when byte_size(ClientId) == 0 andalso (not CleanSess) ->
    false;

%% MQTT3.1.1 allow null clientId.
validate_clientid(#mqtt_packet_connect{proto_ver =?MQTT_PROTO_V4,
                                       client_id = ClientId}, _ProtoState)
    when byte_size(ClientId) =:= 0 ->
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

parse_topic_table(TopicTable) ->
    lists:map(fun({Topic0, Qos}) ->
                {Topic, Opts} = emqttd_topic:parse(Topic0),
                {Topic, [{qos, Qos}|Opts]}
        end, TopicTable).

parse_topics(Topics) ->
    [emqttd_topic:parse(Topic) || Topic <- Topics].

authenticate(Client, Password) ->
    case emqttd_access_control:auth(Client, Password) of
        ok             -> {ok, false};
        {ok, IsSuper}  -> {ok, IsSuper};
        {error, Error} -> {error, Error}
    end.

%% PUBLISH ACL is cached in process dictionary.
check_acl(publish, Topic, Client) ->
    IfCache = emqttd:env(cache_acl, true),
    case {IfCache, get({acl, publish, Topic})} of
        {true, undefined} ->
            AllowDeny = emqttd_access_control:check_acl(Client, publish, Topic),
            put({acl, publish, Topic}, AllowDeny),
            AllowDeny;
        {true, AllowDeny} ->
            AllowDeny;
        {false, _} ->
            emqttd_access_control:check_acl(Client, publish, Topic)
    end;

check_acl(subscribe, Topic, Client) ->
    emqttd_access_control:check_acl(Client, subscribe, Topic).

sp(true)  -> 1;
sp(false) -> 0.

%%--------------------------------------------------------------------
%% The retained flag should be propagated for bridge.
%%--------------------------------------------------------------------

clean_retain(false, Msg = #mqtt_message{retain = true}) ->
    Msg#mqtt_message{retain = false};
clean_retain(_IsBridge, Msg) ->
    Msg.

%%--------------------------------------------------------------------
%% Mount Point
%%--------------------------------------------------------------------

mount(undefined, Any) ->
    Any;
mount(MountPoint, Msg = #mqtt_message{topic = Topic}) ->
    Msg#mqtt_message{topic = <<MountPoint/binary, Topic/binary>>};
mount(MountPoint, TopicTable) when is_list(TopicTable) ->
    [{<<MountPoint/binary, Topic/binary>>, Opts} || {Topic, Opts} <- TopicTable].

unmount(undefined, Any) ->
    Any;
unmount(MountPoint, Msg = #mqtt_message{topic = Topic}) ->
    case catch split_binary(Topic, byte_size(MountPoint)) of
        {MountPoint, Topic0} -> Msg#mqtt_message{topic = Topic0};
        _ -> Msg
    end.

