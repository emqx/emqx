%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% MQTT Channel
-module(emqx_channel).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Channel]").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([ info/1
        , info/2
        , attrs/1
        , stats/1
        , caps/1
        ]).

-export([ init/2
        , handle_in/2
        , handle_out/2
        , handle_call/2
        , handle_info/2
        , handle_timeout/3
        , terminate/2
        ]).

-export([ recvd/2
        , sent/2
        ]).

%% export for ct
-export([set_field/3]).

-import(emqx_misc,
        [ run_fold/3
        , pipeline/3
        , maybe_apply/2
        ]).

-export_type([channel/0]).

-record(channel, {
          %% MQTT ConnInfo
          conninfo :: emqx_types:conninfo(),
          %% MQTT ClientInfo
          clientinfo :: emqx_types:clientinfo(),
          %% MQTT Session
          session :: emqx_session:session(),
          %% Keepalive
          keepalive :: emqx_keepalive:keepalive(),
          %% MQTT Will Msg
          will_msg :: emqx_types:message(),
          %% MQTT Topic Aliases
          topic_aliases :: maybe(map()),
          %% MQTT Topic Alias Maximum
          alias_maximum :: maybe(map()),
          %% Publish Stats
          pub_stats :: emqx_types:stats(),
          %% Timers
          timers :: #{atom() => disabled | maybe(reference())},
          %% Conn State
          conn_state :: conn_state(),
          %% GC State
          gc_state :: maybe(emqx_gc:gc_state()),
          %% Takeover
          takeover :: boolean(),
          %% Resume
          resuming :: boolean(),
          %% Pending delivers when takeovering
          pendings :: list()
         }).

-opaque(channel() :: #channel{}).

-type(conn_state() :: idle | connecting | connected | disconnected).

-type(action() :: {enter, connected | disconnected}
                | {close, Reason :: atom()}
                | {outgoing, emqx_types:packet()}
                | {outgoing, [emqx_types:packet()]}).

-type(output() :: emqx_types:packet() | action() | [action()]).

-define(TIMER_TABLE, #{
          stats_timer  => emit_stats,
          alive_timer  => keepalive,
          retry_timer  => retry_delivery,
          await_timer  => expire_awaiting_rel,
          expire_timer => expire_session,
          will_timer   => will_message
         }).

-define(ATTR_KEYS, [conninfo, clientinfo, session, conn_state]).

-define(INFO_KEYS, ?ATTR_KEYS ++ [keepalive, will_msg, topic_aliases,
                                  alias_maximum, gc_state]).

%%--------------------------------------------------------------------
%% Info, Attrs and Caps
%%--------------------------------------------------------------------

%% @doc Get infos of the channel.
-spec(info(channel()) -> emqx_types:infos()).
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec(info(list(atom())|atom(), channel()) -> term()).
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, #channel{session = Session}) ->
    maybe_apply(fun emqx_session:info/1, Session);
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(keepalive, #channel{keepalive = Keepalive}) ->
    maybe_apply(fun emqx_keepalive:info/1, Keepalive);
info(topic_aliases, #channel{topic_aliases = Aliases}) ->
    Aliases;
info(alias_maximum, #channel{alias_maximum = Limits}) ->
    Limits;
info(will_msg, #channel{will_msg = undefined}) ->
    undefined;
info(will_msg, #channel{will_msg = WillMsg}) ->
    emqx_message:to_map(WillMsg);
info(pub_stats, #channel{pub_stats = PubStats}) ->
    PubStats;
info(gc_state, #channel{gc_state = GcState}) ->
    maybe_apply(fun emqx_gc:info/1, GcState).

%% @doc Get attrs of the channel.
-spec(attrs(channel()) -> emqx_types:attrs()).
attrs(Channel) ->
    Attrs = [{Key, attrs(Key, Channel)} || Key <- ?ATTR_KEYS],
    maps:from_list(Attrs).

attrs(session, #channel{session = Session}) ->
    maybe_apply(fun emqx_session:attrs/1, Session);
attrs(Key, Channel) -> info(Key, Channel).

-spec(stats(channel()) -> emqx_types:stats()).
stats(#channel{pub_stats = PubStats, session = undefined}) ->
    maps:to_list(PubStats);
stats(#channel{pub_stats = PubStats, session = Session}) ->
    maps:to_list(PubStats) ++ emqx_session:stats(Session).

-spec(caps(channel()) -> emqx_types:caps()).
caps(#channel{clientinfo = #{zone := Zone}}) ->
    emqx_mqtt_caps:get_caps(Zone).

%% For tests
set_field(Name, Val, Channel) ->
    Fields = record_info(fields, channel),
    Pos = emqx_misc:index_of(Name, Fields),
    setelement(Pos+1, Channel, Val).

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

-spec(init(emqx_types:conninfo(), proplists:proplist()) -> channel()).
init(ConnInfo = #{peername := {PeerHost, _Port}}, Options) ->
    Zone = proplists:get_value(zone, Options),
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Username = case peer_cert_as_username(Options) of
                   cn  -> esockd_peercert:common_name(Peercert);
                   dn  -> esockd_peercert:subject(Peercert);
                   crt -> Peercert;
                   _   -> undefined
               end,
    Protocol = maps:get(protocol, ConnInfo, mqtt),
    MountPoint = emqx_zone:get_env(Zone, mountpoint),
    ClientInfo = #{zone         => Zone,
                   protocol     => Protocol,
                   peerhost     => PeerHost,
                   peercert     => Peercert,
                   clientid     => undefined,
                   username     => Username,
                   mountpoint   => MountPoint,
                   is_bridge    => false,
                   is_superuser => false
                  },
    StatsTimer = case emqx_zone:enable_stats(Zone) of
                     true  -> undefined;
                     false -> disabled
                 end,
    #channel{conninfo   = ConnInfo,
             clientinfo = ClientInfo,
             pub_stats  = #{},
             timers     = #{stats_timer => StatsTimer},
             conn_state = idle,
             gc_state   = init_gc_state(Zone),
             takeover   = false,
             resuming   = false,
             pendings   = []
            }.

peer_cert_as_username(Options) ->
    proplists:get_value(peer_cert_as_username, Options).

init_gc_state(Zone) ->
    maybe_apply(fun emqx_gc:init/1, emqx_zone:force_gc_policy(Zone)).

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec(recvd(pos_integer(), channel()) -> channel()).
recvd(Bytes, Channel) ->
    ensure_timer(stats_timer, maybe_gc_and_check_oom(Bytes, Channel)).

-spec(handle_in(emqx_types:packet(), channel())
      -> {ok, channel()}
       | {ok, output(), channel()}
       | {shutdown, Reason :: term(), channel()}
       | {shutdown, Reason :: term(), output(), channel()}).
handle_in(?CONNECT_PACKET(_), Channel = #channel{conn_state = connected}) ->
     handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);

handle_in(?CONNECT_PACKET(ConnPkt), Channel) ->
    case pipeline([fun enrich_conninfo/2,
                   fun check_connect/2,
                   fun enrich_client/2,
                   fun set_logger_meta/2,
                   fun check_banned/2,
                   fun check_flapping/2,
                   fun auth_connect/2], ConnPkt, Channel) of
        {ok, NConnPkt, NChannel} ->
            process_connect(NConnPkt, NChannel);
        {error, ReasonCode, NChannel} ->
            ReasonName = emqx_reason_codes:formalized(connack, ReasonCode),
            handle_out(connack, {ReasonName, ConnPkt}, NChannel)
    end;

handle_in(Packet = ?PUBLISH_PACKET(_QoS), Channel) ->
    NChannel = inc_pub_stats(publish_in, Channel),
    case emqx_packet:check(Packet) of
        ok -> handle_publish(Packet, NChannel);
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, NChannel)
    end;

handle_in(?PUBACK_PACKET(PacketId, _ReasonCode),
          Channel = #channel{clientinfo = ClientInfo, session = Session}) ->
    NChannel = inc_pub_stats(puback_in, Channel),
    case emqx_session:puback(PacketId, Session) of
        {ok, Msg, Publishes, NSession} ->
            ok = emqx_hooks:run('message.acked', [ClientInfo, Msg]),
            handle_out({publish, Publishes}, NChannel#channel{session = NSession});
        {ok, Msg, NSession} ->
            ok = emqx_hooks:run('message.acked', [ClientInfo, Msg]),
            {ok, NChannel#channel{session = NSession}};
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?LOG(warning, "The PUBACK PacketId ~w is inuse.", [PacketId]),
            ok = emqx_metrics:inc('packets.puback.inuse'),
            {ok, NChannel};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?LOG(warning, "The PUBACK PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.puback.missed'),
            {ok, NChannel}
    end;

handle_in(?PUBREC_PACKET(PacketId, _ReasonCode),
          Channel = #channel{clientinfo = ClientInfo, session = Session}) ->
    Channel1 = inc_pub_stats(pubrec_in, Channel),
    case emqx_session:pubrec(PacketId, Session) of
        {ok, Msg, NSession} ->
            ok = emqx_hooks:run('message.acked', [ClientInfo, Msg]),
            NChannel = Channel1#channel{session = NSession},
            handle_out(pubrel, {PacketId, ?RC_SUCCESS}, NChannel);
        {error, RC = ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?LOG(warning, "The PUBREC PacketId ~w is inuse.", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrec.inuse'),
            handle_out(pubrel, {PacketId, RC}, Channel1);
        {error, RC = ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?LOG(warning, "The PUBREC ~w is not found.", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrec.missed'),
            handle_out(pubrel, {PacketId, RC}, Channel1)
    end;

handle_in(?PUBREL_PACKET(PacketId, _ReasonCode), Channel = #channel{session = Session}) ->
    Channel1 = inc_pub_stats(pubrel_in, Channel),
    case emqx_session:pubrel(PacketId, Session) of
        {ok, NSession} ->
            Channel2 = Channel1#channel{session = NSession},
            handle_out(pubcomp, {PacketId, ?RC_SUCCESS}, Channel2);
        {error, NotFound} ->
            ok = emqx_metrics:inc('packets.pubrel.missed'),
            ?LOG(warning, "The PUBREL PacketId ~w is not found", [PacketId]),
            handle_out(pubcomp, {PacketId, NotFound}, Channel1)
    end;

handle_in(?PUBCOMP_PACKET(PacketId, _ReasonCode), Channel = #channel{session = Session}) ->
    Channel1 = inc_pub_stats(pubcomp_in, Channel),
    case emqx_session:pubcomp(PacketId, Session) of
        {ok, Publishes, NSession} ->
            handle_out({publish, Publishes}, Channel1#channel{session = NSession});
        {ok, NSession} ->
            {ok, Channel1#channel{session = NSession}};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?LOG(warning, "The PUBCOMP PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.pubcomp.missed'),
            {ok, Channel1}
    end;

handle_in(Packet = ?SUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
          Channel = #channel{clientinfo = ClientInfo}) ->
    case emqx_packet:check(Packet) of
        ok -> TopicFilters1 = emqx_hooks:run_fold('client.subscribe',
                                                  [ClientInfo, Properties],
                                                  parse_topic_filters(TopicFilters)),
              TopicFilters2 = enrich_subid(Properties, TopicFilters1),
              {ReasonCodes, NChannel} = process_subscribe(TopicFilters2, Channel),
              handle_out(suback, {PacketId, ReasonCodes}, NChannel);
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, Channel)
    end;

handle_in(Packet = ?UNSUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
          Channel = #channel{clientinfo = ClientInfo}) ->
    case emqx_packet:check(Packet) of
        ok -> TopicFilters1 = emqx_hooks:run_fold('client.unsubscribe',
                                                  [ClientInfo, Properties],
                                                  parse_topic_filters(TopicFilters)),
              {ReasonCodes, NChannel} = process_unsubscribe(TopicFilters1, Channel),
              handle_out(unsuback, {PacketId, ReasonCodes}, NChannel);
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, Channel)
    end;

handle_in(?PACKET(?PINGREQ), Channel) ->
    {ok, ?PACKET(?PINGRESP), Channel};

handle_in(?DISCONNECT_PACKET(ReasonCode, Properties), Channel = #channel{conninfo = ConnInfo}) ->
    #{proto_ver := ProtoVer, expiry_interval := OldInterval} = ConnInfo,
    {ReasonName, Channel1} = case ReasonCode of
                                 ?RC_SUCCESS ->
                                     {normal, Channel#channel{will_msg = undefined}};
                                 _Other ->
                                     {emqx_reason_codes:name(ReasonCode, ProtoVer), Channel}
                             end,
    Interval = emqx_mqtt_props:get('Session-Expiry-Interval', Properties, OldInterval),
    if
        OldInterval == 0 andalso Interval > OldInterval ->
            handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel1);
        Interval == 0 ->
            shutdown(ReasonName, Channel1);
        true ->
            Channel2 = Channel1#channel{conninfo = ConnInfo#{expiry_interval => Interval}},
            {ok, {close, ReasonName}, Channel2}
    end;

handle_in(?AUTH_PACKET(), Channel) ->
    handle_out(disconnect, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, Channel);

handle_in({frame_error, Reason}, Channel = #channel{conn_state = idle}) ->
    shutdown(Reason, Channel);

handle_in({frame_error, Reason}, Channel = #channel{conn_state = connecting}) ->
    shutdown(Reason, ?CONNACK_PACKET(?RC_MALFORMED_PACKET), Channel);

handle_in({frame_error, _Reason}, Channel = #channel{conn_state = connected}) ->
    handle_out(disconnect, ?RC_MALFORMED_PACKET, Channel);

handle_in({frame_error, Reason}, Channel = #channel{conn_state = disconnected}) ->
    ?LOG(error, "Unexpected frame error: ~p", [Reason]),
    {ok, Channel};

handle_in(Packet, Channel) ->
    ?LOG(error, "Unexpected incoming: ~p", [Packet]),
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel).

%%--------------------------------------------------------------------
%% Process Connect
%%--------------------------------------------------------------------

process_connect(ConnPkt = #mqtt_packet_connect{clean_start = CleanStart},
                Channel = #channel{conninfo = ConnInfo, clientinfo = ClientInfo}) ->
    case emqx_cm:open_session(CleanStart, ClientInfo, ConnInfo) of
        {ok, #{session := Session, present := false}} ->
            NChannel = Channel#channel{session = Session},
            handle_out(connack, {?RC_SUCCESS, sp(false), ConnPkt}, NChannel);
        {ok, #{session := Session, present := true, pendings := Pendings}} ->
            %%TODO: improve later.
            NPendings = lists:usort(lists:append(Pendings, emqx_misc:drain_deliver())),
            NChannel = Channel#channel{session  = Session,
                                       resuming = true,
                                       pendings = NPendings},
            handle_out(connack, {?RC_SUCCESS, sp(true), ConnPkt}, NChannel);
        {error, Reason} ->
            %% TODO: Unknown error?
            ?LOG(error, "Failed to open session: ~p", [Reason]),
            handle_out(connack, {?RC_UNSPECIFIED_ERROR, ConnPkt}, Channel)
    end.

%%--------------------------------------------------------------------
%% Process Publish
%%--------------------------------------------------------------------

inc_pub_stats(Key, Channel) -> inc_pub_stats(Key, 1, Channel).
inc_pub_stats(Key, I, Channel = #channel{pub_stats = PubStats}) ->
    NPubStats = maps:update_with(Key, fun(V) -> V+I end, I, PubStats),
    Channel#channel{pub_stats = NPubStats}.

handle_publish(Packet = ?PUBLISH_PACKET(_QoS, Topic, _PacketId),
               Channel = #channel{conninfo = #{proto_ver := ProtoVer}}) ->
    case pipeline([fun process_alias/2,
                   fun check_pub_acl/2,
                   fun check_pub_alias/2,
                   fun check_pub_caps/2], Packet, Channel) of
        {ok, NPacket, NChannel} ->
            process_publish(NPacket, NChannel);
        {error, ReasonCode, NChannel} ->
            ?LOG(warning, "Cannot publish message to ~s due to ~s",
                 [Topic, emqx_reason_codes:text(ReasonCode, ProtoVer)]),
            handle_out(disconnect, ReasonCode, NChannel)
    end.

process_publish(Packet = ?PUBLISH_PACKET(_QoS, _Topic, PacketId), Channel) ->
    Msg = publish_to_msg(Packet, Channel),
    process_publish(PacketId, Msg, Channel).

process_publish(_PacketId, Msg = #message{qos = ?QOS_0}, Channel) ->
    _ = emqx_broker:publish(Msg),
    {ok, Channel};

process_publish(PacketId, Msg = #message{qos = ?QOS_1}, Channel) ->
    ReasonCode = case emqx_broker:publish(Msg) of
                     [] -> ?RC_NO_MATCHING_SUBSCRIBERS;
                     _  -> ?RC_SUCCESS
                 end,
    handle_out(puback, {PacketId, ReasonCode}, Channel);

process_publish(PacketId, Msg = #message{qos = ?QOS_2},
                Channel = #channel{session = Session}) ->
    case emqx_session:publish(PacketId, Msg, Session) of
        {ok, Results, NSession} ->
            RC = case Results of
                     [] -> ?RC_NO_MATCHING_SUBSCRIBERS;
                     _  -> ?RC_SUCCESS
                 end,
            NChannel = Channel#channel{session = NSession},
            handle_out(pubrec, {PacketId, RC}, ensure_timer(await_timer, NChannel));
        {error, RC = ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ok = emqx_metrics:inc('packets.publish.inuse'),
            handle_out(pubrec, {PacketId, RC}, Channel);
        {error, RC = ?RC_RECEIVE_MAXIMUM_EXCEEDED} ->
            ?LOG(warning, "Dropped qos2 packet ~w due to awaiting_rel is full", [PacketId]),
            ok = emqx_metrics:inc('messages.qos2.dropped'),
            handle_out(pubrec, {PacketId, RC}, Channel)
    end.

publish_to_msg(Packet, #channel{conninfo = #{proto_ver := ProtoVer},
                                clientinfo = ClientInfo = #{mountpoint := MountPoint}}) ->
    Msg = emqx_packet:to_message(ClientInfo, Packet),
    Msg1 = emqx_message:set_flag(dup, false, Msg),
    Msg2 = emqx_message:set_header(proto_ver, ProtoVer, Msg1),
    emqx_mountpoint:mount(MountPoint, Msg2).

%%--------------------------------------------------------------------
%% Process Subscribe
%%--------------------------------------------------------------------

process_subscribe(TopicFilters, Channel) ->
    process_subscribe(TopicFilters, [], Channel).

process_subscribe([], Acc, Channel) ->
    {lists:reverse(Acc), Channel};

process_subscribe([{TopicFilter, SubOpts}|More], Acc, Channel) ->
    {RC, NChannel} = do_subscribe(TopicFilter, SubOpts, Channel),
    process_subscribe(More, [RC|Acc], NChannel).

do_subscribe(TopicFilter, SubOpts = #{qos := QoS}, Channel =
             #channel{clientinfo = ClientInfo = #{mountpoint := MountPoint},
                      session = Session}) ->
    case check_subscribe(TopicFilter, SubOpts, Channel) of
        ok ->
            TopicFilter1 = emqx_mountpoint:mount(MountPoint, TopicFilter),
            SubOpts1 = enrich_subopts(maps:merge(?DEFAULT_SUBOPTS, SubOpts), Channel),
            case emqx_session:subscribe(ClientInfo, TopicFilter1, SubOpts1, Session) of
                {ok, NSession} ->
                    {QoS, Channel#channel{session = NSession}};
                {error, RC} -> {RC, Channel}
            end;
        {error, RC} -> {RC, Channel}
    end.

%%--------------------------------------------------------------------
%% Process Unsubscribe
%%--------------------------------------------------------------------

-compile({inline, [process_unsubscribe/2]}).
process_unsubscribe(TopicFilters, Channel) ->
    process_unsubscribe(TopicFilters, [], Channel).

process_unsubscribe([], Acc, Channel) ->
    {lists:reverse(Acc), Channel};

process_unsubscribe([{TopicFilter, SubOpts}|More], Acc, Channel) ->
    {RC, NChannel} = do_unsubscribe(TopicFilter, SubOpts, Channel),
    process_unsubscribe(More, [RC|Acc], NChannel).

do_unsubscribe(TopicFilter, _SubOpts, Channel =
               #channel{clientinfo = ClientInfo = #{mountpoint := MountPoint},
                        session = Session}) ->
    TopicFilter1 = emqx_mountpoint:mount(MountPoint, TopicFilter),
    case emqx_session:unsubscribe(ClientInfo, TopicFilter1, Session) of
        {ok, NSession} ->
            {?RC_SUCCESS, Channel#channel{session = NSession}};
        {error, RC} -> {RC, Channel}
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

-spec(sent(pos_integer(), channel()) -> channel()).
sent(Bytes, Channel) ->
    ensure_timer(stats_timer, maybe_gc_and_check_oom(Bytes, Channel)).

-spec(handle_out(term(), channel())
      -> {ok, channel()}
       | {ok, output(), channel()}
       | {shutdown, Reason :: term(), channel()}
       | {shutdown, Reason :: term(), output(), channel()}).
handle_out(Delivers, Channel = #channel{conn_state = disconnected,
                                        session = Session})
  when is_list(Delivers) ->
    NSession = emqx_session:enqueue(Delivers, Session),
    {ok, Channel#channel{session = NSession}};

handle_out(Delivers, Channel = #channel{takeover = true,
                                        pendings = Pendings})
  when is_list(Delivers) ->
    {ok, Channel#channel{pendings = lists:append(Pendings, Delivers)}};

handle_out(Delivers, Channel = #channel{session = Session}) when is_list(Delivers) ->
    case emqx_session:deliver(Delivers, Session) of
        {ok, Publishes, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out({publish, Publishes}, ensure_timer(retry_timer, NChannel));
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}}
    end;

handle_out({publish, Publishes}, Channel) when is_list(Publishes) ->
    Packets = lists:foldl(
                fun(Publish, Acc) ->
                    case handle_out(Publish, Channel) of
                        {ok, Packet, _Ch} ->
                            [Packet|Acc];
                        {ok, _Ch} -> Acc
                    end
                end, [], Publishes),
    NChannel = inc_pub_stats(publish_out, length(Packets), Channel),
    {ok, {outgoing, lists:reverse(Packets)}, NChannel};

%% Ignore loop deliver
handle_out({publish, _PacketId, #message{from  = ClientId,
                                         flags = #{nl := true}}},
           Channel = #channel{clientinfo = #{clientid := ClientId}}) ->
    {ok, Channel};

handle_out({publish, PacketId, Msg}, Channel =
           #channel{clientinfo = ClientInfo = #{mountpoint := MountPoint}}) ->
    Msg1 = emqx_message:update_expiry(Msg),
    Msg2 = emqx_hooks:run_fold('message.delivered', [ClientInfo], Msg1),
    Msg3 = emqx_mountpoint:unmount(MountPoint, Msg2),
    {ok, emqx_message:to_packet(PacketId, Msg3), Channel};

handle_out(Data, Channel) ->
    ?LOG(error, "Unexpected outgoing: ~p", [Data]),
    {ok, Channel}.

handle_out(connack, {?RC_SUCCESS, SP, ConnPkt},
           Channel = #channel{conninfo   = ConnInfo,
                              clientinfo = ClientInfo}) ->
    AckProps = run_fold([fun enrich_caps/2,
                         fun enrich_server_keepalive/2,
                         fun enrich_assigned_clientid/2], #{}, Channel),
    ConnInfo1 = ConnInfo#{connected_at => erlang:system_time(second)},
    Channel1 = Channel#channel{conninfo = ConnInfo1,
                               will_msg = emqx_packet:will_msg(ConnPkt),
                               conn_state = connected,
                               alias_maximum = init_alias_maximum(ConnPkt, ClientInfo)
                              },
    Channel2 = ensure_keepalive(AckProps, Channel1),
    ok = emqx_hooks:run('client.connected', [ClientInfo, ?RC_SUCCESS, ConnInfo]),
    AckPacket = ?CONNACK_PACKET(?RC_SUCCESS, SP, AckProps),
    case maybe_resume_session(Channel2) of
        ignore ->
            {ok, [{enter, connected}, {outgoing, AckPacket}], Channel2};
        {ok, Publishes, NSession} ->
            Channel3 = Channel2#channel{session  = NSession,
                                        resuming = false,
                                        pendings = []},
            {ok, {outgoing, Packets}, _} = handle_out({publish, Publishes}, Channel3),
            {ok, [{enter, connected}, {outgoing, [AckPacket|Packets]}], Channel3}
    end;

handle_out(connack, {ReasonCode, _ConnPkt}, Channel = #channel{conninfo = ConnInfo,
                                                               clientinfo = ClientInfo}) ->
    ok = emqx_hooks:run('client.connected', [ClientInfo, ReasonCode, ConnInfo]),
    ReasonCode1 = case ProtoVer = maps:get(proto_ver, ConnInfo) of
                      ?MQTT_PROTO_V5 -> ReasonCode;
                      _Ver -> emqx_reason_codes:compat(connack, ReasonCode)
                  end,
    Reason = emqx_reason_codes:name(ReasonCode1, ProtoVer),
    shutdown(Reason, ?CONNACK_PACKET(ReasonCode1), Channel);

handle_out(puback, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBACK_PACKET(PacketId, ReasonCode), inc_pub_stats(puback_out, Channel)};

handle_out(pubrel, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBREL_PACKET(PacketId, ReasonCode), inc_pub_stats(pubrel_out, Channel)};

handle_out(pubrec, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBREC_PACKET(PacketId, ReasonCode), inc_pub_stats(pubrec_out, Channel)};

handle_out(pubcomp, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBCOMP_PACKET(PacketId, ReasonCode), inc_pub_stats(pubcomp_out, Channel)};

handle_out(suback, {PacketId, ReasonCodes},
           Channel = #channel{conninfo = #{proto_ver := ?MQTT_PROTO_V5}}) ->
    {ok, ?SUBACK_PACKET(PacketId, ReasonCodes), Channel};

handle_out(suback, {PacketId, ReasonCodes}, Channel) ->
    ReasonCodes1 = [emqx_reason_codes:compat(suback, RC) || RC <- ReasonCodes],
    {ok, ?SUBACK_PACKET(PacketId, ReasonCodes1), Channel};

handle_out(unsuback, {PacketId, ReasonCodes},
           Channel = #channel{conninfo = #{proto_ver := ?MQTT_PROTO_V5}}) ->
    {ok, ?UNSUBACK_PACKET(PacketId, ReasonCodes), Channel};

handle_out(unsuback, {PacketId, _ReasonCodes}, Channel) ->
    {ok, ?UNSUBACK_PACKET(PacketId), Channel};

handle_out(disconnect, ReasonCode, Channel = #channel{conninfo = #{proto_ver := ProtoVer}})
  when is_integer(ReasonCode) ->
    ReasonName = emqx_reason_codes:name(ReasonCode, ProtoVer),
    handle_out(disconnect, {ReasonCode, ReasonName}, Channel);

handle_out(disconnect, {ReasonCode, ReasonName}, Channel = #channel{conninfo = ConnInfo}) ->
    #{proto_ver := ProtoVer, expiry_interval := ExpiryInterval} = ConnInfo,
    case {ExpiryInterval, ProtoVer} of
        {0, ?MQTT_PROTO_V5} ->
            shutdown(ReasonName, ?DISCONNECT_PACKET(ReasonCode), Channel);
        {0, _Ver} ->
            shutdown(ReasonName, Channel);
        {?UINT_MAX, ?MQTT_PROTO_V5} ->
            Output = [{outgoing, ?DISCONNECT_PACKET(ReasonCode)},
                      {close, ReasonName}],
            {ok, Output, Channel};
        {?UINT_MAX, _Ver} ->
            {ok, {close, ReasonName}, Channel};
        {Interval, ?MQTT_PROTO_V5} ->
            NChannel = ensure_timer(expire_timer, Interval, Channel),
            Output = [{outgoing, ?DISCONNECT_PACKET(ReasonCode)},
                      {close, ReasonName}],
            {ok, Output, NChannel};
        {Interval, _Ver} ->
            NChannel = ensure_timer(expire_timer, Interval, Channel),
            {ok, {close, ReasonName}, NChannel}
    end;

handle_out(Type, Data, Channel) ->
    ?LOG(error, "Unexpected outgoing: ~s, ~p", [Type, Data]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec(handle_call(Req :: term(), channel())
      -> {reply, Reply :: term(), channel()}
       | {shutdown, Reason :: term(), Reply :: term(), channel()}).
handle_call(kick, Channel) ->
    shutdown(kicked, ok, Channel);

handle_call(discard, Channel = #channel{conn_state = connected}) ->
    Packet = ?DISCONNECT_PACKET(?RC_SESSION_TAKEN_OVER),
    {shutdown, discarded, ok, Packet, Channel};

handle_call(discard, Channel = #channel{conn_state = disconnected}) ->
    shutdown(discarded, ok, Channel);

%% Session Takeover
handle_call({takeover, 'begin'}, Channel = #channel{session = Session}) ->
    reply(Session, Channel#channel{takeover = true});

handle_call({takeover, 'end'}, Channel = #channel{session  = Session,
                                                  pendings = Pendings}) ->
    ok = emqx_session:takeover(Session),
    %% TODO: Should not drain deliver here
    Delivers = emqx_misc:drain_deliver(),
    AllPendings = lists:append(Delivers, Pendings),
    shutdown(takeovered, AllPendings, Channel);

handle_call(Req, Channel) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    reply(ignored, Channel).

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec(handle_info(Info :: term(), channel())
      -> ok | {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_info({subscribe, TopicFilters}, Channel = #channel{clientinfo = ClientInfo}) ->
    TopicFilters1 = emqx_hooks:run_fold('client.subscribe',
                                        [ClientInfo, #{'Internal' => true}],
                                        parse_topic_filters(TopicFilters)),
    {_ReasonCodes, NChannel} = process_subscribe(TopicFilters1, Channel),
    {ok, NChannel};

handle_info({unsubscribe, TopicFilters}, Channel = #channel{clientinfo = ClientInfo}) ->
    TopicFilters1 = emqx_hooks:run_fold('client.unsubscribe',
                                        [ClientInfo, #{'Internal' => true}],
                                        parse_topic_filters(TopicFilters)),
    {_ReasonCodes, NChannel} = process_unsubscribe(TopicFilters1, Channel),
    {ok, NChannel};

handle_info({register, Attrs, Stats}, #channel{clientinfo = #{clientid := ClientId}}) ->
    ok = emqx_cm:register_channel(ClientId),
    emqx_cm:set_chan_attrs(ClientId, Attrs),
    emqx_cm:set_chan_stats(ClientId, Stats);

handle_info({sock_closed, _Reason}, Channel = #channel{conn_state = disconnected}) ->
    {ok, Channel};

handle_info({sock_closed, Reason}, Channel = #channel{conninfo = ConnInfo,
                                                      clientinfo = ClientInfo = #{zone := Zone},
                                                      will_msg = WillMsg}) ->
    emqx_zone:enable_flapping_detect(Zone) andalso emqx_flapping:detect(ClientInfo),
    ConnInfo1 = ConnInfo#{disconnected_at => erlang:system_time(second)},
    Channel1 = Channel#channel{conninfo = ConnInfo1, conn_state = disconnected},
    Channel2 = case timer:seconds(will_delay_interval(WillMsg)) of
                   0 -> publish_will_msg(WillMsg),
                        Channel1#channel{will_msg = undefined};
                   _ -> ensure_timer(will_timer, Channel1)
               end,
    case maps:get(expiry_interval, ConnInfo) of
        ?UINT_MAX ->
            {ok, {enter, disconnected}, Channel2};
        Int when Int > 0 ->
            {ok, {enter, disconnected}, ensure_timer(expire_timer, Channel2)};
        _Other ->
            shutdown(Reason, Channel2)
    end;

handle_info(Info, Channel) ->
    ?LOG(error, "Unexpected info: ~p~n", [Info]),
    error(unexpected_info),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

-spec(handle_timeout(reference(), Msg :: term(), channel())
      -> {ok, channel()}
       | {ok, Result :: term(), channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_timeout(TRef, {emit_stats, Stats},
               Channel = #channel{clientinfo = #{clientid := ClientId},
                                  timers = #{stats_timer := TRef}}) ->
    ok = emqx_cm:set_chan_stats(ClientId, Stats),
    {ok, clean_timer(stats_timer, Channel)};

handle_timeout(TRef, {keepalive, StatVal},
               Channel = #channel{keepalive = Keepalive,
                                  timers = #{alive_timer := TRef}}) ->
    case emqx_keepalive:check(StatVal, Keepalive) of
        {ok, NKeepalive} ->
            NChannel = Channel#channel{keepalive = NKeepalive},
            {ok, reset_timer(alive_timer, NChannel)};
        {error, timeout} ->
            handle_out(disconnect, ?RC_KEEP_ALIVE_TIMEOUT, Channel)
    end;

handle_timeout(TRef, retry_delivery,
               Channel = #channel{session = Session,
                                  timers = #{retry_timer := TRef}}) ->
    case emqx_session:retry(Session) of
        {ok, NSession} ->
            {ok, clean_timer(retry_timer, Channel#channel{session = NSession})};
        {ok, Publishes, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out({publish, Publishes}, reset_timer(retry_timer, NChannel));
        {ok, Publishes, Timeout, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out({publish, Publishes}, reset_timer(retry_timer, Timeout, NChannel))
    end;

handle_timeout(TRef, expire_awaiting_rel,
               Channel = #channel{session = Session,
                                  timers = #{await_timer := TRef}}) ->
    case emqx_session:expire(awaiting_rel, Session) of
        {ok, Session} ->
            {ok, clean_timer(await_timer, Channel#channel{session = Session})};
        {ok, Timeout, Session} ->
            {ok, reset_timer(await_timer, Timeout, Channel#channel{session = Session})}
    end;

handle_timeout(TRef, expire_session, Channel = #channel{timers = #{expire_timer := TRef}}) ->
    shutdown(expired, Channel);

handle_timeout(TRef, will_message, Channel = #channel{will_msg = WillMsg,
                                                      timers = #{will_timer := TRef}}) ->
    publish_will_msg(WillMsg),
    {ok, clean_timer(will_timer, Channel#channel{will_msg = undefined})};

handle_timeout(_TRef, Msg, Channel) ->
    ?LOG(error, "Unexpected timeout: ~p~n", [Msg]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Ensure timers
%%--------------------------------------------------------------------

ensure_timer([Name], Channel) ->
    ensure_timer(Name, Channel);
ensure_timer([Name | Rest], Channel) ->
    ensure_timer(Rest, ensure_timer(Name, Channel));

ensure_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, Channel),
    case TRef == undefined andalso Time > 0 of
        true ->
            ensure_timer(Name, Time, Channel);
        false -> Channel %% Timer disabled or exists
    end.

ensure_timer(Name, Time, Channel = #channel{timers = Timers}) ->
    Msg = maps:get(Name, ?TIMER_TABLE),
    TRef = emqx_misc:start_timer(Time, Msg),
    Channel#channel{timers = Timers#{Name => TRef}}.

reset_timer(Name, Channel) ->
    ensure_timer(Name, clean_timer(Name, Channel)).

reset_timer(Name, Time, Channel) ->
    ensure_timer(Name, Time, clean_timer(Name, Channel)).

clean_timer(Name, Channel = #channel{timers = Timers}) ->
    Channel#channel{timers = maps:remove(Name, Timers)}.

interval(stats_timer, #channel{clientinfo = #{zone := Zone}}) ->
    emqx_zone:get_env(Zone, idle_timeout, 30000);
interval(alive_timer, #channel{keepalive = KeepAlive}) ->
    emqx_keepalive:info(interval, KeepAlive);
interval(retry_timer, #channel{session = Session}) ->
    emqx_session:info(retry_interval, Session);
interval(await_timer, #channel{session = Session}) ->
    emqx_session:info(await_rel_timeout, Session);
interval(expire_timer, #channel{conninfo = ConnInfo}) ->
    timer:seconds(maps:get(expiry_interval, ConnInfo));
interval(will_timer, #channel{will_msg = WillMsg}) ->
    %% TODO: Ensure the header exists.
    timer:seconds(will_delay_interval(WillMsg)).

will_delay_interval(undefined) -> 0;
will_delay_interval(WillMsg) ->
    emqx_message:get_header('Will-Delay-Interval', WillMsg, 0).

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

terminate(_, #channel{conn_state = idle}) ->
    ok;
terminate(normal, #channel{conninfo = ConnInfo, clientinfo = ClientInfo}) ->
    ok = emqx_hooks:run('client.disconnected', [ClientInfo, normal, ConnInfo]);
terminate({shutdown, Reason}, #channel{conninfo = ConnInfo, clientinfo = ClientInfo})
    when Reason =:= kicked orelse Reason =:= discarded orelse Reason =:= takeovered ->
    ok = emqx_hooks:run('client.disconnected', [ClientInfo, Reason, ConnInfo]);
terminate(Reason, #channel{conninfo = ConnInfo, clientinfo = ClientInfo, will_msg = WillMsg}) ->
    publish_will_msg(WillMsg),
    ok = emqx_hooks:run('client.disconnected', [ClientInfo, Reason, ConnInfo]).

%% TODO: Improve will msg:)
publish_will_msg(undefined) ->
    ok;
publish_will_msg(Msg) ->
    emqx_broker:publish(Msg).

%% @doc Enrich MQTT Connect Info.
enrich_conninfo(#mqtt_packet_connect{
                   proto_name  = ProtoName,
                   proto_ver   = ProtoVer,
                   clean_start = CleanStart,
                   keepalive   = Keepalive,
                   properties  = ConnProps,
                   clientid    = ClientId,
                   username    = Username}, Channel) ->
    #channel{conninfo = ConnInfo, clientinfo = #{zone := Zone}} = Channel,
    MaxInflight = emqx_mqtt_props:get('Receive-Maximum',
                                      ConnProps, emqx_zone:max_inflight(Zone)),
    Interval = if ProtoVer == ?MQTT_PROTO_V5 ->
                      emqx_mqtt_props:get('Session-Expiry-Interval', ConnProps, 0);
                  true -> case CleanStart of
                              true -> 0;
                              false -> emqx_zone:session_expiry_interval(Zone)
                          end
               end,
    NConnInfo = ConnInfo#{proto_name  => ProtoName,
                          proto_ver   => ProtoVer,
                          clean_start => CleanStart,
                          keepalive   => Keepalive,
                          clientid    => ClientId,
                          username    => Username,
                          conn_props  => ConnProps,
                          receive_maximum => MaxInflight,
                          expiry_interval => Interval
                         },
    {ok, Channel#channel{conninfo = NConnInfo}}.

%% @doc Check connect packet.
check_connect(ConnPkt, #channel{clientinfo = #{zone := Zone}}) ->
    emqx_packet:check(ConnPkt, emqx_mqtt_caps:get_caps(Zone)).

%% @doc Enrich client
enrich_client(ConnPkt, Channel = #channel{clientinfo = ClientInfo}) ->
    {ok, NConnPkt, NClientInfo} =
        pipeline([fun set_username/2,
                  fun set_bridge_mode/2,
                  fun maybe_username_as_clientid/2,
                  fun maybe_assign_clientid/2,
                  fun fix_mountpoint/2], ConnPkt, ClientInfo),
    {ok, NConnPkt, Channel#channel{clientinfo = NClientInfo}}.

set_username(#mqtt_packet_connect{username = Username},
             ClientInfo = #{username := undefined}) ->
    {ok, ClientInfo#{username => Username}};
set_username(_ConnPkt, ClientInfo) ->
    {ok, ClientInfo}.

set_bridge_mode(#mqtt_packet_connect{is_bridge = true}, ClientInfo) ->
    {ok, ClientInfo#{is_bridge => true}};
set_bridge_mode(_ConnPkt, _ClientInfo) -> ok.

maybe_username_as_clientid(_ConnPkt, ClientInfo = #{username := undefined}) ->
    {ok, ClientInfo};
maybe_username_as_clientid(_ConnPkt, ClientInfo = #{zone := Zone, username := Username}) ->
    case emqx_zone:use_username_as_clientid(Zone) of
        true  -> {ok, ClientInfo#{clientid => Username}};
        false -> ok
    end.

maybe_assign_clientid(#mqtt_packet_connect{clientid = <<>>}, ClientInfo) ->
    %% Generate a rand clientId
    {ok, ClientInfo#{clientid => emqx_guid:to_base62(emqx_guid:gen())}};
maybe_assign_clientid(#mqtt_packet_connect{clientid = ClientId}, ClientInfo) ->
    {ok, ClientInfo#{clientid => ClientId}}.

fix_mountpoint(_ConnPkt, #{mountpoint := undefined}) -> ok;
fix_mountpoint(_ConnPkt, ClientInfo = #{mountpoint := Mountpoint}) ->
    {ok, ClientInfo#{mountpoint := emqx_mountpoint:replvar(Mountpoint, ClientInfo)}}.

%% @doc Set logger metadata.
set_logger_meta(_ConnPkt, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId).

%%--------------------------------------------------------------------
%% Check banned/flapping
%%--------------------------------------------------------------------

check_banned(_ConnPkt, #channel{clientinfo = ClientInfo = #{zone := Zone}}) ->
    case emqx_zone:enable_ban(Zone) andalso emqx_banned:check(ClientInfo) of
        true  -> {error, ?RC_BANNED};
        false -> ok
    end.

check_flapping(_ConnPkt, #channel{clientinfo = ClientInfo = #{zone := Zone}}) ->
    case emqx_zone:enable_flapping_detect(Zone)
         andalso emqx_flapping:check(ClientInfo) of
        true -> {error, ?RC_CONNECTION_RATE_EXCEEDED};
        false -> ok
    end.

%%--------------------------------------------------------------------
%% Auth Connect
%%--------------------------------------------------------------------

auth_connect(#mqtt_packet_connect{clientid = ClientId,
                                  username  = Username,
                                  password  = Password},
             Channel = #channel{clientinfo = ClientInfo}) ->
    case emqx_access_control:authenticate(ClientInfo#{password => Password}) of
        {ok, AuthResult} ->
            {ok, Channel#channel{clientinfo = maps:merge(ClientInfo, AuthResult)}};
        {error, Reason} ->
            ?LOG(warning, "Client ~s (Username: '~s') login failed for ~0p",
                 [ClientId, Username, Reason]),
            {error, emqx_reason_codes:connack_error(Reason)}
    end.

%%--------------------------------------------------------------------
%% Process publish message: Client -> Broker
%%--------------------------------------------------------------------

process_alias(Packet = #mqtt_packet{
                          variable = #mqtt_packet_publish{topic_name = <<>>,
                                                          properties = #{'Topic-Alias' := AliasId}
                                                         } = Publish
                         },
              Channel = #channel{topic_aliases = Aliases}) ->
    case find_alias(AliasId, Aliases) of
        {ok, Topic} ->
            {ok, Packet#mqtt_packet{
                   variable = Publish#mqtt_packet_publish{
                                topic_name = Topic}}, Channel};
        false -> {error, ?RC_PROTOCOL_ERROR}
    end;

process_alias(#mqtt_packet{
                 variable = #mqtt_packet_publish{topic_name = Topic,
                                                 properties = #{'Topic-Alias' := AliasId}
                                                }
                }, Channel = #channel{topic_aliases = Aliases}) ->
    {ok, Channel#channel{topic_aliases = save_alias(AliasId, Topic, Aliases)}};

process_alias(_Packet, Channel) ->
    {ok, Channel}.

find_alias(_AliasId, undefined) -> false;
find_alias(AliasId, Aliases) -> maps:find(AliasId, Aliases).

save_alias(AliasId, Topic, undefined) -> #{AliasId => Topic};
save_alias(AliasId, Topic, Aliases) -> maps:put(AliasId, Topic, Aliases).

%% Check Pub ACL
check_pub_acl(#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}},
              #channel{clientinfo = ClientInfo}) ->
    case is_acl_enabled(ClientInfo) andalso
         emqx_access_control:check_acl(ClientInfo, publish, Topic) of
        false -> ok;
        allow -> ok;
        deny  -> {error, ?RC_NOT_AUTHORIZED}
    end.

%% Check Pub Alias
check_pub_alias(#mqtt_packet{
                   variable = #mqtt_packet_publish{
                                 properties = #{'Topic-Alias' := AliasId}
                                }
                  },
                #channel{alias_maximum = Limits}) ->
    %% TODO: Move to Protocol
    case (Limits == undefined)
            orelse (Max = maps:get(inbound, Limits, 0)) == 0
                orelse (AliasId > Max) of
        false -> ok;
        true  -> {error, ?RC_TOPIC_ALIAS_INVALID}
    end;
check_pub_alias(_Packet, _Channel) -> ok.

%% Check Pub Caps
check_pub_caps(#mqtt_packet{header = #mqtt_packet_header{qos = QoS,
                                                         retain = Retain},
                            variable = #mqtt_packet_publish{topic_name = Topic}
                           },
               #channel{clientinfo = #{zone := Zone}}) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain, topic => Topic}).

%% Check Sub
check_subscribe(TopicFilter, SubOpts, Channel) ->
    case check_sub_acl(TopicFilter, Channel) of
        allow -> check_sub_caps(TopicFilter, SubOpts, Channel);
        deny  -> {error, ?RC_NOT_AUTHORIZED}
    end.

%% Check Sub ACL
check_sub_acl(TopicFilter, #channel{clientinfo = ClientInfo}) ->
    case is_acl_enabled(ClientInfo) andalso
         emqx_access_control:check_acl(ClientInfo, subscribe, TopicFilter) of
        false  -> allow;
        Result -> Result
    end.

%% Check Sub Caps
check_sub_caps(TopicFilter, SubOpts, #channel{clientinfo = #{zone := Zone}}) ->
    emqx_mqtt_caps:check_sub(Zone, TopicFilter, SubOpts).

enrich_subid(#{'Subscription-Identifier' := SubId}, TopicFilters) ->
    [{Topic, SubOpts#{subid => SubId}} || {Topic, SubOpts} <- TopicFilters];
enrich_subid(_Properties, TopicFilters) ->
    TopicFilters.

enrich_subopts(SubOpts, #channel{conninfo = #{proto_ver := ?MQTT_PROTO_V5}}) ->
    SubOpts;

enrich_subopts(SubOpts, #channel{clientinfo = #{zone := Zone, is_bridge := IsBridge}}) ->
    NL = flag(emqx_zone:ignore_loop_deliver(Zone)),
    SubOpts#{rap => flag(IsBridge), nl => NL}.

enrich_caps(AckProps, #channel{conninfo = #{proto_ver := ?MQTT_PROTO_V5},
                               clientinfo = #{zone := Zone}}) ->
    #{max_packet_size       := MaxPktSize,
      max_qos_allowed       := MaxQoS,
      retain_available      := Retain,
      max_topic_alias       := MaxAlias,
      shared_subscription   := Shared,
      wildcard_subscription := Wildcard
     } = emqx_mqtt_caps:get_caps(Zone),
    AckProps#{'Retain-Available'    => flag(Retain),
              'Maximum-Packet-Size' => MaxPktSize,
              'Topic-Alias-Maximum' => MaxAlias,
              'Wildcard-Subscription-Available'   => flag(Wildcard),
              'Subscription-Identifier-Available' => 1,
              'Shared-Subscription-Available'     => flag(Shared),
              'Maximum-QoS' => MaxQoS
             };
enrich_caps(AckProps, _Channel) ->
    AckProps.

enrich_server_keepalive(AckProps, #channel{clientinfo = #{zone := Zone}}) ->
    case emqx_zone:server_keepalive(Zone) of
        undefined -> AckProps;
        Keepalive -> AckProps#{'Server-Keep-Alive' => Keepalive}
    end.

enrich_assigned_clientid(AckProps, #channel{conninfo = ConnInfo,
                                            clientinfo = #{clientid := ClientId}
                                           }) ->
    case maps:get(clientid, ConnInfo) of
        <<>> -> %% Original ClientId is null.
            AckProps#{'Assigned-Client-Identifier' => ClientId};
        _Origin -> AckProps
    end.

init_alias_maximum(#mqtt_packet_connect{proto_ver  = ?MQTT_PROTO_V5,
                                        properties = Properties}, #{zone := Zone}) ->
    #{outbound => emqx_mqtt_props:get('Topic-Alias-Maximum', Properties, 0),
      inbound  => emqx_mqtt_caps:get_caps(Zone, max_topic_alias, 0)};
init_alias_maximum(_ConnPkt, _ClientInfo) -> undefined.

ensure_keepalive(#{'Server-Keep-Alive' := Interval}, Channel) ->
    ensure_keepalive_timer(Interval, Channel);
ensure_keepalive(_AckProps, Channel = #channel{conninfo = ConnInfo}) ->
    ensure_keepalive_timer(maps:get(keepalive, ConnInfo), Channel).

ensure_keepalive_timer(0, Channel) -> Channel;
ensure_keepalive_timer(Interval, Channel = #channel{clientinfo = #{zone := Zone}}) ->
    Backoff = emqx_zone:get_env(Zone, keepalive_backoff, 0.75),
    Keepalive = emqx_keepalive:init(round(timer:seconds(Interval) * Backoff)),
    ensure_timer(alive_timer, Channel#channel{keepalive = Keepalive}).

maybe_resume_session(#channel{resuming = false}) ->
    ignore;
maybe_resume_session(#channel{session  = Session,
                              resuming = true,
                              pendings = Pendings}) ->
    {ok, Publishes, Session1} = emqx_session:redeliver(Session),
    case emqx_session:deliver(Pendings, Session1) of
        {ok, Session2} ->
            {ok, Publishes, Session2};
        {ok, More, Session2} ->
            {ok, lists:append(Publishes, More), Session2}
    end.

%% @doc Is ACL enabled?
is_acl_enabled(#{zone := Zone, is_superuser := IsSuperuser}) ->
    (not IsSuperuser) andalso emqx_zone:enable_acl(Zone).

%% @doc Parse Topic Filters
-compile({inline, [parse_topic_filters/1]}).
parse_topic_filters(TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

%%--------------------------------------------------------------------
%% Maybe GC and Check OOM
%%--------------------------------------------------------------------

maybe_gc_and_check_oom(_Oct, Channel = #channel{gc_state = undefined}) ->
    Channel;
maybe_gc_and_check_oom(Oct, Channel = #channel{clientinfo = #{zone := Zone},
                                               gc_state   = GCSt}) ->
    {IsGC, GCSt1} = emqx_gc:run(1, Oct, GCSt),
    IsGC andalso emqx_metrics:inc('channel.gc.cnt'),
    IsGC andalso emqx_zone:check_oom(Zone, fun(Shutdown) -> self() ! Shutdown end),
    Channel#channel{gc_state = GCSt1}.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

-compile({inline, [reply/2]}).
reply(Reply, Channel) ->
    {reply, Reply, Channel}.

-compile({inline, [shutdown/2]}).
shutdown(Reason, Channel) ->
    {shutdown, Reason, Channel}.

-compile({inline, [shutdown/3]}).
shutdown(Reason, Reply, Channel) ->
    {shutdown, Reason, Reply, Channel}.

sp(true)  -> 1;
sp(false) -> 0.

flag(true)  -> 1;
flag(false) -> 0.

