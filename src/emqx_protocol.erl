%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_protocol).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("emqx_misc.hrl").

-export([init/2, info/1, stats/1, clientid/1, session/1]).
%%-export([capabilities/1]).
-export([parser/1]).
-export([received/2, process/2, deliver/2, send/2]).
-export([shutdown/2]).

-ifdef(TEST).
-compile(export_all).
-endif.

-define(CAPABILITIES, [{max_packet_size,  ?MAX_PACKET_SIZE},
                       {max_clientid_len, ?MAX_CLIENTID_LEN},
                       {max_topic_alias,  0},
                       {max_qos_allowed,  ?QOS2},
                       {retain_available, true},
                       {shared_subscription,   true},
                       {wildcard_subscription, true}]).

-record(proto_state, {zone, sockprops, capabilities, connected, client_id, client_pid,
                      clean_start, proto_ver, proto_name, username, connprops,
                      is_superuser, will_msg, keepalive, keepalive_backoff, session,
                      recv_pkt = 0, recv_msg = 0, send_pkt = 0, send_msg = 0,
                      mountpoint, is_bridge, connected_at}).

-define(INFO_KEYS, [capabilities, connected, client_id, clean_start, username, proto_ver, proto_name,
                    keepalive, will_msg, mountpoint, is_bridge, connected_at]).

-define(STATS_KEYS, [recv_pkt, recv_msg, send_pkt, send_msg]).

-define(LOG(Level, Format, Args, State),
        emqx_logger:Level([{client, State#proto_state.client_id}], "Client(~s@~s): " ++ Format,
                          [State#proto_state.client_id,
                           esockd_net:format(maps:get(peername, State#proto_state.sockprops)) | Args])).

-type(proto_state() :: #proto_state{}).

-export_type([proto_state/0]).

init(SockProps = #{peercert := Peercert}, Options) ->
    Zone = proplists:get_value(zone, Options),
    MountPoint = emqx_zone:env(Zone, mountpoint),
    Backoff = emqx_zone:env(Zone, keepalive_backoff, 0.75),
    Username = case proplists:get_value(peer_cert_as_username, Options) of
                   cn -> esockd_peercert:common_name(Peercert);
                   dn -> esockd_peercert:subject(Peercert);
                   _  -> undefined
               end,
    #proto_state{zone               = Zone,
                 sockprops          = SockProps,
                 capabilities       = capabilities(Zone),
                 connected          = false,
                 clean_start        = true,
                 client_pid         = self(),
                 proto_ver          = ?MQTT_PROTO_V4,
                 proto_name         = <<"MQTT">>,
                 username           = Username,
                 is_superuser       = false,
                 keepalive_backoff  = Backoff,
                 mountpoint         = MountPoint,
                 is_bridge          = false,
                 recv_pkt           = 0,
                 recv_msg           = 0,
                 send_pkt           = 0,
                 send_msg           = 0}.

capabilities(Zone) ->
    Capabilities = emqx_zone:env(Zone, mqtt_capabilities, []),
    maps:from_list(lists:ukeymerge(1, ?CAPABILITIES, Capabilities)).

parser(#proto_state{capabilities = #{max_packet_size := Size}, proto_ver = Ver}) ->
    emqx_frame:initial_state(#{max_packet_size => Size, version => Ver}).

info(ProtoState) ->
    ?record_to_proplist(proto_state, ProtoState, ?INFO_KEYS).

stats(ProtoState) ->
    ?record_to_proplist(proto_state, ProtoState, ?STATS_KEYS).

clientid(#proto_state{client_id = ClientId}) ->
    ClientId.

client(#proto_state{sockprops = #{peername := Peername},
                    client_id = ClientId, client_pid = ClientPid, username  = Username}) ->
    #client{id = ClientId, pid = ClientPid, username = Username, peername = Peername}.

session(#proto_state{session = Session}) ->
    Session.

%% CONNECT – Client requests a connection to a Server

%% A Client can only send the CONNECT Packet once over a Network Connection.
-spec(received(mqtt_packet(), proto_state()) -> {ok, proto_state()} | {error, term()}).
received(Packet = ?PACKET(?CONNECT), ProtoState = #proto_state{connected = false}) ->
    trace(recv, Packet, ProtoState),
    process(Packet, inc_stats(recv, ?CONNECT, ProtoState#proto_state{connected = true}));

received(?PACKET(?CONNECT), State = #proto_state{connected = true}) ->
    {error, protocol_bad_connect, State};

%% Received other packets when CONNECT not arrived.
received(_Packet, ProtoState = #proto_state{connected = false}) ->
    {error, protocol_not_connected, ProtoState};

received(Packet = ?PACKET(Type), ProtoState) ->
    trace(recv, Packet, ProtoState),
    case validate_packet(Packet) of
        ok ->
            process(Packet, inc_stats(recv, Type, ProtoState));
        {error, Reason} ->
            {error, Reason, ProtoState}
    end.

process(?CONNECT_PACKET(Var), ProtoState = #proto_state{zone       = Zone,
                                                        username   = Username0,
                                                        client_pid = ClientPid}) ->
    #mqtt_packet_connect{proto_name  = ProtoName,
                         proto_ver   = ProtoVer,
                         is_bridge   = IsBridge,
                         clean_start = CleanStart,
                         keepalive   = Keepalive,
                         properties  = ConnProps,
                         client_id   = ClientId,
                         username    = Username,
                         password    = Password} = Var,
    ProtoState1 = ProtoState#proto_state{proto_ver    = ProtoVer,
                                         proto_name   = ProtoName,
                                         username     = if Username0 == undefined ->
                                                               Username;
                                                           true -> Username0
                                                        end, %% TODO: fixme later.
                                         client_id    = ClientId,
                                         clean_start  = CleanStart,
                                         keepalive    = Keepalive,
                                         connprops    = ConnProps,
                                         will_msg     = willmsg(Var, ProtoState),
                                         is_bridge    = IsBridge,
                                         connected_at = os:timestamp()},

    {ReturnCode1, SessPresent, ProtoState3} =
    case validate_connect(Var, ProtoState1) of
        ?RC_SUCCESS ->
            case authenticate(client(ProtoState1), Password) of
                {ok, IsSuperuser} ->
                    %% Generate clientId if null
                    ProtoState2 = maybe_set_clientid(ProtoState1),
                    %% Open session
                    case emqx_sm:open_session(#{zone        => Zone,
                                                clean_start => CleanStart,
                                                client_id   => clientid(ProtoState2),
                                                username    => Username,
                                                client_pid  => ClientPid}) of
                        {ok, Session} -> %% TODO:...
                            SP = true, %% TODO:...
                            %% TODO: Register the client
                            emqx_cm:register_client(clientid(ProtoState2)),
                            %%emqx_cm:reg(client(State2)),
                            %% Start keepalive
                            start_keepalive(Keepalive, ProtoState2),
                            %% Emit Stats
                            %% self() ! emit_stats,
                            %% ACCEPT
                            {?RC_SUCCESS, SP, ProtoState2#proto_state{session = Session, is_superuser = IsSuperuser}};
                        {error, Error} ->
                            ?LOG(error, "Failed to open session: ~p", [Error], ProtoState2),
                            {?RC_UNSPECIFIED_ERROR, false, ProtoState2} %% TODO: the error reason???
                    end;
                {error, Reason}->
                    ?LOG(error, "Username '~s' login failed for ~p", [Username, Reason], ProtoState1),
                    {?RC_BAD_USER_NAME_OR_PASSWORD, false, ProtoState1}
            end;
        ReturnCode ->
            {ReturnCode, false, ProtoState1}
    end,
    %% Run hooks
    emqx_hooks:run('client.connected', [ReturnCode1], client(ProtoState3)),
    %%TODO: Send Connack
    send(?CONNACK_PACKET(ReturnCode1, sp(SessPresent)), ProtoState3),
    %% stop if authentication failure
    stop_if_auth_failure(ReturnCode1, ProtoState3);

process(Packet = ?PUBLISH_PACKET(_QoS, Topic, _PacketId, _Payload),
        State = #proto_state{is_superuser = IsSuper}) ->
    case IsSuper orelse allow == check_acl(publish, Topic, client(State)) of
        true  -> publish(Packet, State);
        false -> ?LOG(error, "Cannot publish to ~s for ACL Deny", [Topic], State)
    end,
    {ok, State};

process(?PUBACK_PACKET(PacketId), State = #proto_state{session = Session}) ->
    emqx_session:puback(Session, PacketId),
    {ok, State};

process(?PUBREC_PACKET(PacketId), State = #proto_state{session = Session}) ->
    emqx_session:pubrec(Session, PacketId),
    send(?PUBREL_PACKET(PacketId), State);

process(?PUBREL_PACKET(PacketId), State = #proto_state{session = Session}) ->
    emqx_session:pubrel(Session, PacketId),
    send(?PUBCOMP_PACKET(PacketId), State);

process(?PUBCOMP_PACKET(PacketId), State = #proto_state{session = Session})->
    emqx_session:pubcomp(Session, PacketId), {ok, State};

%% Protect from empty topic table
process(?SUBSCRIBE_PACKET(PacketId, []), State) ->
    send(?SUBACK_PACKET(PacketId, []), State);

%% TODO: refactor later...
process(?SUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters), State) ->
    #proto_state{client_id    = ClientId,
                 username     = Username,
                 is_superuser = IsSuperuser,
                 mountpoint   = MountPoint,
                 session      = Session} = State,
    Client = client(State),
    TopicFilters = parse_topic_filters(RawTopicFilters),
    AllowDenies = if
                    IsSuperuser -> [];
                    true -> [check_acl(subscribe, Topic, Client) || {Topic, _Opts} <- TopicFilters]
                  end,
    case lists:member(deny, AllowDenies) of
        true ->
            ?LOG(error, "Cannot SUBSCRIBE ~p for ACL Deny", [TopicFilters], State),
            send(?SUBACK_PACKET(PacketId, [?RC_NOT_AUTHORIZED || _ <- TopicFilters]), State);
        false ->
            case emqx_hooks:run('client.subscribe', [ClientId, Username], TopicFilters) of
                {ok, TopicFilters1} ->
                    ok = emqx_session:subscribe(Session, {PacketId, Properties, mount(replvar(MountPoint, State), TopicFilters1)}),
                    {ok, State};
                {stop, _} -> {ok, State}
            end
    end;

%% Protect from empty topic list
process(?UNSUBSCRIBE_PACKET(PacketId, []), State) ->
    send(?UNSUBACK_PACKET(PacketId), State);

process(?UNSUBSCRIBE_PACKET(PacketId, Properties, RawTopics),
        State = #proto_state{client_id  = ClientId,
                             username   = Username,
                             mountpoint = MountPoint,
                             session    = Session}) ->
    case emqx_hooks:run('client.unsubscribe', [ClientId, Username], parse_topics(RawTopics)) of
        {ok, TopicTable} ->
            emqx_session:unsubscribe(Session, {PacketId, Properties, mount(replvar(MountPoint, State), TopicTable)});
        {stop, _} ->
            ok
    end,
    send(?UNSUBACK_PACKET(PacketId), State);

process(?PACKET(?PINGREQ), ProtoState) ->
    send(?PACKET(?PINGRESP), ProtoState);

process(?PACKET(?DISCONNECT), ProtoState) ->
    % Clean willmsg
    {stop, normal, ProtoState#proto_state{will_msg = undefined}}.

deliver({publish, PacketId, Msg},
        State = #proto_state{client_id  = ClientId,
                             username   = Username,
                             mountpoint = MountPoint,
                             is_bridge  = IsBridge}) ->
    emqx_hooks:run('message.delivered', [ClientId],
                   emqx_message:set_header(username, Username, Msg)),
    Msg1 = unmount(MountPoint, clean_retain(IsBridge, Msg)),
    send(emqx_packet:from_message(PacketId, Msg1), State);

deliver({pubrel, PacketId}, State) ->
    send(?PUBREL_PACKET(PacketId), State);

deliver({suback, PacketId, ReasonCodes}, ProtoState) ->
    send(?SUBACK_PACKET(PacketId, ReasonCodes), ProtoState);

deliver({unsuback, PacketId, ReasonCodes}, ProtoState) ->
    send(?UNSUBACK_PACKET(PacketId, ReasonCodes), ProtoState).

publish(Packet = ?PUBLISH_PACKET(?QOS_0, PacketId),
        State = #proto_state{client_id  = ClientId,
                             username   = Username,
                             mountpoint = MountPoint,
                             session    = Session}) ->
    Msg = emqx_message:set_header(username, Username,
                                  emqx_packet:to_message(ClientId, Packet)),
    emqx_session:publish(Session, PacketId, mount(replvar(MountPoint, State), Msg));

publish(Packet = ?PUBLISH_PACKET(?QOS_1), State) ->
    with_puback(?PUBACK, Packet, State);

publish(Packet = ?PUBLISH_PACKET(?QOS_2), State) ->
    with_puback(?PUBREC, Packet, State).

with_puback(Type, Packet = ?PUBLISH_PACKET(_QoS, PacketId),
            State = #proto_state{client_id  = ClientId,
                                 username   = Username,
                                 mountpoint = MountPoint,
                                 session    = Session}) ->
    Msg = emqx_message:set_header(username, Username,
                                  emqx_packet:to_message(ClientId, Packet)),
    case emqx_session:publish(Session, PacketId, mount(replvar(MountPoint, State), Msg)) of
        {error, Error} ->
            ?LOG(error, "PUBLISH ~p error: ~p", [PacketId, Error], State);
        _Delivery -> send({Type, PacketId}, State) %% TODO:
    end.

-spec(send({mqtt_packet_type(), mqtt_packet_id()} |
           {mqtt_packet_id(), message()} |
           mqtt_packet(), proto_state()) -> {ok, proto_state()}).
send({?PUBACK, PacketId}, State) ->
    send(?PUBACK_PACKET(PacketId), State);

send({?PUBREC, PacketId}, State) ->
    send(?PUBREC_PACKET(PacketId), State);

send(Packet = ?PACKET(Type), ProtoState = #proto_state{proto_ver = Ver,
                                                       sockprops = #{sendfun := SendFun}}) ->
    Data = emqx_frame:serialize(Packet, #{version => Ver}),
    case SendFun(Data) of
        {error, Reason} ->
            {error, Reason};
        _ -> emqx_metrics:sent(Packet),
              trace(send, Packet, ProtoState),
              {ok, inc_stats(send, Type, ProtoState)}
    end.

trace(recv, Packet, ProtoState) ->
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)], ProtoState);

trace(send, Packet, ProtoState) ->
    ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)], ProtoState).

inc_stats(recv, Type, ProtoState = #proto_state{recv_pkt = PktCnt, recv_msg = MsgCnt}) ->
    ProtoState#proto_state{recv_pkt = PktCnt + 1,
                           recv_msg = if Type =:= ?PUBLISH -> MsgCnt + 1;
                                         true -> MsgCnt
                                      end};
inc_stats(send, Type, ProtoState = #proto_state{send_pkt = PktCnt, send_msg = MsgCnt}) ->
    ProtoState#proto_state{send_pkt = PktCnt + 1,
                           send_msg = if Type =:= ?PUBLISH -> MsgCnt + 1;
                                         true -> MsgCnt
                                      end}.

stop_if_auth_failure(?RC_SUCCESS, State) ->
    {ok, State};
stop_if_auth_failure(RC, State) when RC =/= ?RC_SUCCESS ->
    {stop, {shutdown, auth_failure}, State}.

shutdown(_Error, #proto_state{client_id = undefined}) ->
    ignore;
shutdown(conflict, _State = #proto_state{client_id = ClientId}) ->
    emqx_cm:unregister_client(ClientId),
    ignore;
shutdown(mnesia_conflict, _State = #proto_state{client_id = ClientId}) ->
    emqx_cm:unregister_client(ClientId),
    ignore;
shutdown(Error, State = #proto_state{client_id = ClientId,
                                     will_msg  = WillMsg}) ->
    ?LOG(info, "Shutdown for ~p", [Error], State),
    %% Auth failure not publish the will message
    case Error =:= auth_failure of
        true -> ok;
        false -> send_willmsg(ClientId, WillMsg)
    end,
    emqx_hooks:run('client.disconnected', [Error], client(State)),
    emqx_cm:unregister_client(ClientId),
    ok.

willmsg(Packet, State = #proto_state{client_id = ClientId, mountpoint = MountPoint})
    when is_record(Packet, mqtt_packet_connect) ->
    case emqx_packet:to_message(ClientId, Packet) of
        undefined -> undefined;
        Msg -> mount(replvar(MountPoint, State), Msg)
    end.

%% Generate a client if if nulll
maybe_set_clientid(State = #proto_state{client_id = NullId})
        when NullId =:= undefined orelse NullId =:= <<>> ->
    {_, NPid, _} = emqx_guid:new(),
    ClientId = iolist_to_binary(["emqx_", integer_to_list(NPid)]),
    State#proto_state{client_id = ClientId};

maybe_set_clientid(State) ->
    State.

send_willmsg(_ClientId, undefined) ->
    ignore;
send_willmsg(ClientId, WillMsg) ->
    emqx_broker:publish(WillMsg#message{from = ClientId}).

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
                true  -> ?RC_SUCCESS;
                false -> ?RC_CLIENT_IDENTIFIER_NOT_VALID
            end;
        false ->
            ?RC_UNSUPPORTED_PROTOCOL_VERSION
    end.

validate_protocol(#mqtt_packet_connect{proto_ver = Ver, proto_name = Name}) ->
    lists:member({Ver, Name}, ?PROTOCOL_NAMES).

validate_clientid(#mqtt_packet_connect{client_id = ClientId},
                  #proto_state{capabilities = #{max_clientid_len := MaxLen}})
    when (byte_size(ClientId) >= 1) andalso (byte_size(ClientId) =< MaxLen) ->
    true;

%% Issue#599: Null clientId and clean_start = false
validate_clientid(#mqtt_packet_connect{client_id   = ClientId,
                                       clean_start = CleanStart}, _ProtoState)
    when byte_size(ClientId) == 0 andalso (not CleanStart) ->
    false;

%% MQTT3.1.1 allow null clientId.
validate_clientid(#mqtt_packet_connect{proto_ver =?MQTT_PROTO_V4,
                                       client_id = ClientId}, _ProtoState)
    when byte_size(ClientId) =:= 0 ->
    true;

validate_clientid(#mqtt_packet_connect{proto_ver   = ProtoVer,
                                       clean_start = CleanStart}, ProtoState) ->
    ?LOG(warning, "Invalid clientId. ProtoVer: ~p, CleanStart: ~s",
         [ProtoVer, CleanStart], ProtoState),
    false.

validate_packet(?PUBLISH_PACKET(_QoS, Topic, _PacketId, _Payload)) ->
    case emqx_topic:validate({name, Topic}) of
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

validate_topics(Type, TopicTable = [{_Topic, _SubOpts}|_])
    when Type =:= name orelse Type =:= filter ->
    Valid = fun(Topic, QoS) ->
              emqx_topic:validate({Type, Topic}) and validate_qos(QoS)
            end,
    case [Topic || {Topic, SubOpts} <- TopicTable,
                   not Valid(Topic, SubOpts#mqtt_subopts.qos)] of
        [] -> ok;
        _  -> {error, badtopic}
    end;

validate_topics(Type, Topics = [Topic0|_]) when is_binary(Topic0) ->
    case [Topic || Topic <- Topics, not emqx_topic:validate({Type, Topic})] of
        [] -> ok;
        _  -> {error, badtopic}
    end.

validate_qos(undefined) ->
    true;
validate_qos(QoS) when ?IS_QOS(QoS) ->
    true;
validate_qos(_) ->
    false.

parse_topic_filters(TopicFilters) ->
    [begin
         {Topic, Opts} = emqx_topic:parse(RawTopic),
         {Topic, maps:merge(?record_to_map(mqtt_subopts, SubOpts), Opts)}
     end || {RawTopic, SubOpts} <- TopicFilters].

parse_topics(Topics) ->
    [emqx_topic:parse(Topic) || Topic <- Topics].

authenticate(Client, Password) ->
    case emqx_access_control:auth(Client, Password) of
        ok             -> {ok, false};
        {ok, IsSuper}  -> {ok, IsSuper};
        {error, Error} -> {error, Error}
    end.

%% PUBLISH ACL is cached in process dictionary.
check_acl(publish, Topic, Client) ->
    IfCache = emqx_config:get_env(cache_acl, true),
    case {IfCache, get({acl, publish, Topic})} of
        {true, undefined} ->
            AllowDeny = emqx_access_control:check_acl(Client, publish, Topic),
            put({acl, publish, Topic}, AllowDeny),
            AllowDeny;
        {true, AllowDeny} ->
            AllowDeny;
        {false, _} ->
            emqx_access_control:check_acl(Client, publish, Topic)
    end;

check_acl(subscribe, Topic, Client) ->
    emqx_access_control:check_acl(Client, subscribe, Topic).

sp(true)  -> 1;
sp(false) -> 0.

%%--------------------------------------------------------------------
%% The retained flag should be propagated for bridge.
%%--------------------------------------------------------------------

clean_retain(false, Msg = #message{flags = #{retain := true}, headers = Headers}) ->
    case maps:get(retained, Headers, false) of
        true  -> Msg;
        false -> emqx_message:set_flag(retain, false, Msg)
    end;
clean_retain(_IsBridge, Msg) ->
    Msg.

%%--------------------------------------------------------------------
%% Mount Point
%%--------------------------------------------------------------------

replvar(undefined, _State) ->
    undefined;
replvar(MountPoint, #proto_state{client_id = ClientId, username = Username}) ->
    lists:foldl(fun feed_var/2, MountPoint, [{<<"%c">>, ClientId}, {<<"%u">>, Username}]).

feed_var({<<"%c">>, ClientId}, MountPoint) ->
    emqx_topic:feed_var(<<"%c">>, ClientId, MountPoint);
feed_var({<<"%u">>, undefined}, MountPoint) ->
    MountPoint;
feed_var({<<"%u">>, Username}, MountPoint) ->
    emqx_topic:feed_var(<<"%u">>, Username, MountPoint).

mount(undefined, Any) ->
    Any;
mount(MountPoint, Msg = #message{topic = Topic}) ->
    Msg#message{topic = <<MountPoint/binary, Topic/binary>>};
mount(MountPoint, TopicTable) when is_list(TopicTable) ->
    [{<<MountPoint/binary, Topic/binary>>, Opts} || {Topic, Opts} <- TopicTable].

unmount(undefined, Any) ->
    Any;
unmount(MountPoint, Msg = #message{topic = Topic}) ->
    case catch split_binary(Topic, byte_size(MountPoint)) of
        {MountPoint, Topic0} -> Msg#message{topic = Topic0};
        _ -> Msg
    end.

