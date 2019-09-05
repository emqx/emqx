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

-export([init/2]).

-export([ info/1
        , info/2
        , attrs/1
        , stats/1
        , caps/1
        ]).

%% Exports for unit tests:(
-export([set_field/3]).

-export([ handle_in/2
        , handle_out/2
        , handle_call/2
        , handle_cast/2
        , handle_info/2
        , timeout/3
        , terminate/2
        ]).

-export([ received/2
        , sent/2
        ]).

-import(emqx_misc,
        [ run_fold/2
        , run_fold/3
        , pipeline/3
        , maybe_apply/2
        ]).

-export_type([channel/0]).

-record(channel, {
          %% MQTT Client
          client :: emqx_types:client(),
          %% MQTT Session
          session :: emqx_session:session(),
          %% MQTT Protocol
          protocol :: emqx_protocol:protocol(),
          %% Keepalive
          keepalive :: emqx_keepalive:keepalive(),
          %% Timers
          timers :: #{atom() => disabled | maybe(reference())},
          %% GC State
          gc_state :: maybe(emqx_gc:gc_state()),
          %% OOM Policy
          oom_policy :: maybe(emqx_oom:oom_policy()),
          %% Connected
          connected :: undefined | boolean(),
          %% Connected at
          connected_at :: erlang:timestamp(),
          %% Disconnected at
          disconnected_at :: erlang:timestamp(),
          %% Takeover
          takeover :: boolean(),
          %% Resume
          resuming :: boolean(),
          %% Pending delivers when takeovering
          pendings :: list()
         }).

-opaque(channel() :: #channel{}).

-define(TIMER_TABLE, #{
          stats_timer  => emit_stats,
          alive_timer  => keepalive,
          retry_timer  => retry_delivery,
          await_timer  => expire_awaiting_rel,
          expire_timer => expire_session,
          will_timer   => will_message
         }).

-define(ATTR_KEYS, [client, session, protocol, connected, connected_at, disconnected_at]).

-define(INFO_KEYS, ?ATTR_KEYS ++ [keepalive, gc_state, disconnected_at]).

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

-spec(init(emqx_types:conn(), proplists:proplist()) -> channel()).
init(ConnInfo, Options) ->
    Zone = proplists:get_value(zone, Options),
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Username = case peer_cert_as_username(Options) of
                   cn  -> esockd_peercert:common_name(Peercert);
                   dn  -> esockd_peercert:subject(Peercert);
                   crt -> Peercert;
                   _   -> undefined
               end,
    MountPoint = emqx_zone:get_env(Zone, mountpoint),
    Client = maps:merge(#{zone         => Zone,
                          username     => Username,
                          client_id    => <<>>,
                          mountpoint   => MountPoint,
                          is_bridge    => false,
                          is_superuser => false
                         }, ConnInfo),
    EnableStats = emqx_zone:get_env(Zone, enable_stats, true),
    StatsTimer = if
                     EnableStats -> undefined;
                     ?Otherwise  -> disabled
                 end,
    GcState = maybe_apply(fun emqx_gc:init/1,
                          emqx_zone:get_env(Zone, force_gc_policy)),
    OomPolicy = maybe_apply(fun emqx_oom:init/1,
                            emqx_zone:get_env(Zone, force_shutdown_policy)),
    #channel{client       = Client,
             gc_state     = GcState,
             oom_policy   = OomPolicy,
             timers       = #{stats_timer => StatsTimer},
             connected    = undefined,
             takeover     = false,
             resuming     = false,
             pendings     = []
            }.

peer_cert_as_username(Options) ->
    proplists:get_value(peer_cert_as_username, Options).

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
info(client, #channel{client = Client}) ->
    Client;
info(session, #channel{session = Session}) ->
    maybe_apply(fun emqx_session:info/1, Session);
info(protocol, #channel{protocol = Protocol}) ->
    maybe_apply(fun emqx_protocol:info/1, Protocol);
info(keepalive, #channel{keepalive = Keepalive}) ->
    maybe_apply(fun emqx_keepalive:info/1, Keepalive);
info(gc_state, #channel{gc_state = GcState}) ->
    maybe_apply(fun emqx_gc:info/1, GcState);
info(oom_policy, #channel{oom_policy = OomPolicy}) ->
    maybe_apply(fun emqx_oom:info/1, OomPolicy);
info(connected, #channel{connected = Connected}) ->
    Connected;
info(connected_at, #channel{connected_at = ConnectedAt}) ->
    ConnectedAt;
info(disconnected_at, #channel{disconnected_at = DisconnectedAt}) ->
    DisconnectedAt.

%% @doc Get attrs of the channel.
-spec(attrs(channel()) -> emqx_types:attrs()).
attrs(Channel) ->
    maps:from_list([{Key, attr(Key, Channel)} || Key <- ?ATTR_KEYS]).

attr(protocol, #channel{protocol = Proto}) ->
    maybe_apply(fun emqx_protocol:attrs/1, Proto);
attr(session, #channel{session = Session}) ->
    maybe_apply(fun emqx_session:attrs/1, Session);
attr(Key, Channel) -> info(Key, Channel).

-spec(stats(channel()) -> emqx_types:stats()).
stats(#channel{session = Session}) ->
    emqx_session:stats(Session).

-spec(caps(channel()) -> emqx_types:caps()).
caps(#channel{client = #{zone := Zone}}) ->
    emqx_mqtt_caps:get_caps(Zone).

%% For tests
set_field(Name, Val, Channel) ->
    Fields = record_info(fields, channel),
    Pos = emqx_misc:index_of(Name, Fields),
    setelement(Pos+1, Channel, Val).

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec(handle_in(emqx_types:packet(), channel())
      -> {ok, channel()}
       | {ok, emqx_types:packet(), channel()}
       | {ok, list(emqx_types:packet()), channel()}
       | {stop, Error :: term(), channel()}
       | {stop, Error :: term(), emqx_types:packet(), channel()}).
handle_in(?CONNECT_PACKET(_), Channel = #channel{connected = true}) ->
     handle_out({disconnect, ?RC_PROTOCOL_ERROR}, Channel);

handle_in(?CONNECT_PACKET(ConnPkt), Channel) ->
    case pipeline([fun validate_packet/2,
                   fun check_connect/2,
                   fun init_protocol/2,
                   fun enrich_client/2,
                   fun set_logger_meta/2,
                   fun auth_connect/2], ConnPkt, Channel) of
        {ok, NConnPkt, NChannel} ->
            process_connect(NConnPkt, NChannel);
        {error, ReasonCode, NChannel} ->
            handle_out({connack, ReasonCode}, NChannel)
    end;

handle_in(Packet = ?PUBLISH_PACKET(_QoS, Topic, _PacketId),
          Channel = #channel{protocol = Protocol}) ->
    case pipeline([fun validate_packet/2,
                   fun process_alias/2,
                   fun check_publish/2], Packet, Channel) of
        {ok, NPacket, NChannel} ->
            process_publish(NPacket, NChannel);
        {error, ReasonCode, NChannel} ->
            ProtoVer = emqx_protocol:info(proto_ver, Protocol),
            ?LOG(warning, "Cannot publish message to ~s due to ~s",
                 [Topic, emqx_reason_codes:text(ReasonCode, ProtoVer)]),
            handle_out({disconnect, ReasonCode}, NChannel)
    end;

handle_in(?PUBACK_PACKET(PacketId, _ReasonCode),
          Channel = #channel{client = Client, session = Session}) ->
    case emqx_session:puback(PacketId, Session) of
        {ok, Msg, Publishes, NSession} ->
            ok = emqx_hooks:run('message.acked', [Client, Msg]),
            handle_out({publish, Publishes}, Channel#channel{session = NSession});
        {ok, Msg, NSession} ->
            ok = emqx_hooks:run('message.acked', [Client, Msg]),
            {ok, Channel#channel{session = NSession}};
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?LOG(warning, "The PUBACK PacketId ~w is inuse.", [PacketId]),
            ok = emqx_metrics:inc('packets.puback.inuse'),
            {ok, Channel};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?LOG(warning, "The PUBACK PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.puback.missed'),
            {ok, Channel}
    end;

handle_in(?PUBREC_PACKET(PacketId, _ReasonCode),
          Channel = #channel{client = Client, session = Session}) ->
    case emqx_session:pubrec(PacketId, Session) of
        {ok, Msg, NSession} ->
            ok = emqx_hooks:run('message.acked', [Client, Msg]),
            NChannel = Channel#channel{session = NSession},
            handle_out({pubrel, PacketId, ?RC_SUCCESS}, NChannel);
        {error, RC = ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?LOG(warning, "The PUBREC PacketId ~w is inuse.", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrec.inuse'),
            handle_out({pubrel, PacketId, RC}, Channel);
        {error, RC = ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?LOG(warning, "The PUBREC ~w is not found.", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrec.missed'),
            handle_out({pubrel, PacketId, RC}, Channel)
    end;

handle_in(?PUBREL_PACKET(PacketId, _ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:pubrel(PacketId, Session) of
        {ok, NSession} ->
            handle_out({pubcomp, PacketId, ?RC_SUCCESS}, Channel#channel{session = NSession});
        {error, NotFound} ->
            ?LOG(warning, "The PUBREL PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrel.missed'),
            handle_out({pubcomp, PacketId, NotFound}, Channel)
    end;

handle_in(?PUBCOMP_PACKET(PacketId, _ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:pubcomp(PacketId, Session) of
        {ok, Publishes, NSession} ->
            handle_out({publish, Publishes}, Channel#channel{session = NSession});
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?LOG(warning, "The PUBCOMP PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.pubcomp.missed'),
            {ok, Channel}
    end;

handle_in(Packet = ?SUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters), Channel) ->
    case validate_packet(Packet, Channel) of
        ok ->
            TopicFilters = preprocess_subscribe(Properties, RawTopicFilters, Channel),
            {ReasonCodes, NChannel} = process_subscribe(TopicFilters, Channel),
            handle_out({suback, PacketId, ReasonCodes}, NChannel);
        {error, ReasonCode} ->
            handle_out({disconnect, ReasonCode}, Channel)
    end;

handle_in(Packet = ?UNSUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters), Channel) ->
    case validate_packet(Packet, Channel) of
        ok ->
            TopicFilters = preprocess_unsubscribe(Properties, RawTopicFilters, Channel),
            {ReasonCodes, NChannel} = process_unsubscribe(TopicFilters, Channel),
            handle_out({unsuback, PacketId, ReasonCodes}, NChannel);
        {error, ReasonCode} ->
            handle_out({disconnect, ReasonCode}, Channel)
    end;

handle_in(?PACKET(?PINGREQ), Channel) ->
    {ok, ?PACKET(?PINGRESP), Channel};

handle_in(?DISCONNECT_PACKET(RC, Properties), Channel = #channel{session = Session, protocol = Protocol}) ->
    OldInterval = emqx_session:info(expiry_interval, Session),
    Interval = get_property('Session-Expiry-Interval', Properties, OldInterval),
    case OldInterval =:= 0 andalso Interval =/= OldInterval of
        true ->
            handle_out({disconnect, ?RC_PROTOCOL_ERROR}, Channel);
        false ->
            Channel1 = case RC of
                           ?RC_SUCCESS -> Channel#channel{protocol = emqx_protocol:clear_will_msg(Protocol)};
                           _ -> Channel
                       end,
            Channel2 = Channel1#channel{session = emqx_session:update_expiry_interval(Interval, Session)},
            case Interval of
                ?UINT_MAX ->
                    {ok, ensure_timer(will_timer, Channel2)};
                Int when Int > 0 ->
                    {ok, ensure_timer([will_timer, expire_timer], Channel2)};
                _Other ->
                    Reason = case RC of
                                 ?RC_SUCCESS -> normal;
                                 _ ->
                                     Ver = emqx_protocol:info(proto_ver, Protocol),
                                     emqx_reason_codes:name(RC, Ver)
                             end,
                    {stop, {shutdown, Reason}, Channel2}
            end
    end;

handle_in(?AUTH_PACKET(), Channel) ->
    %%TODO: implement later.
    {ok, Channel};

handle_in(Packet, Channel) ->
    ?LOG(error, "Unexpected incoming: ~p", [Packet]),
    {stop, {shutdown, unexpected_incoming_packet}, Channel}.

%%--------------------------------------------------------------------
%% Process Connect
%%--------------------------------------------------------------------

process_connect(ConnPkt, Channel) ->
    case open_session(ConnPkt, Channel) of
        {ok, #{session := Session, present := false}} ->
            NChannel = Channel#channel{session = Session},
            handle_out({connack, ?RC_SUCCESS, sp(false)}, NChannel);
        {ok, #{session := Session, present := true, pendings := Pendings}} ->
            %%TODO: improve later.
            NPendings = lists:usort(lists:append(Pendings, emqx_misc:drain_deliver())),
            NChannel = Channel#channel{session  = Session,
                                       resuming = true,
                                       pendings = NPendings},
            handle_out({connack, ?RC_SUCCESS, sp(true)}, NChannel);
        {error, Reason} ->
            %% TODO: Unknown error?
            ?LOG(error, "Failed to open session: ~p", [Reason]),
            handle_out({connack, ?RC_UNSPECIFIED_ERROR}, Channel)
    end.

%%--------------------------------------------------------------------
%% Process Publish
%%--------------------------------------------------------------------

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
    handle_out({puback, PacketId, ReasonCode}, Channel);

process_publish(PacketId, Msg = #message{qos = ?QOS_2},
                Channel = #channel{session = Session}) ->
    case emqx_session:publish(PacketId, Msg, Session) of
        {ok, Results, NSession} ->
            RC = case Results of
                     [] -> ?RC_NO_MATCHING_SUBSCRIBERS;
                     _  -> ?RC_SUCCESS
                 end,
            NChannel = Channel#channel{session = NSession},
            handle_out({pubrec, PacketId, RC}, ensure_timer(await_timer, NChannel));
        {error, RC = ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ok = emqx_metrics:inc('packets.publish.inuse'),
            handle_out({pubrec, PacketId, RC}, Channel);
        {error, RC = ?RC_RECEIVE_MAXIMUM_EXCEEDED} ->
            ?LOG(warning, "Dropped qos2 packet ~w due to awaiting_rel is full", [PacketId]),
            ok = emqx_metrics:inc('messages.qos2.dropped'),
            handle_out({pubrec, PacketId, RC}, Channel)
    end.

publish_to_msg(Packet, #channel{client   = Client = #{mountpoint := MountPoint},
                                protocol = Protocol}) ->
    Msg = emqx_packet:to_message(Client, Packet),
    Msg1 = emqx_message:set_flag(dup, false, Msg),
    ProtoVer = emqx_protocol:info(proto_ver, Protocol),
    Msg2 = emqx_message:set_header(proto_ver, ProtoVer, Msg1),
    emqx_mountpoint:mount(MountPoint, Msg2).

%%--------------------------------------------------------------------
%% Process Subscribe
%%--------------------------------------------------------------------

-compile({inline, [preprocess_subscribe/3]}).
preprocess_subscribe(Properties, RawTopicFilters, #channel{client = Client}) ->
    RunHook = fun(TopicFilters) ->
                      emqx_hooks:run_fold('client.subscribe',
                                          [Client, Properties], TopicFilters)
              end,
    Enrich = fun(TopicFilters) -> enrich_subid(Properties, TopicFilters) end,
    run_fold([fun parse_topic_filters/1, RunHook, Enrich], RawTopicFilters).

process_subscribe(TopicFilters, Channel) ->
    process_subscribe(TopicFilters, [], Channel).

process_subscribe([], Acc, Channel) ->
    {lists:reverse(Acc), Channel};

process_subscribe([{TopicFilter, SubOpts}|More], Acc, Channel) ->
    {RC, NChannel} = do_subscribe(TopicFilter, SubOpts, Channel),
    process_subscribe(More, [RC|Acc], NChannel).

do_subscribe(TopicFilter, SubOpts = #{qos := QoS}, Channel =
             #channel{client  = Client = #{mountpoint := MountPoint},
                      session = Session}) ->
    case check_subscribe(TopicFilter, SubOpts, Channel) of
        ok ->
            TopicFilter1 = emqx_mountpoint:mount(MountPoint, TopicFilter),
            SubOpts1 = enrich_subopts(maps:merge(?DEFAULT_SUBOPTS, SubOpts), Channel),
            case emqx_session:subscribe(Client, TopicFilter1, SubOpts1, Session) of
                {ok, NSession} ->
                    {QoS, Channel#channel{session = NSession}};
                {error, RC} -> {RC, Channel}
            end;
        {error, RC} -> {RC, Channel}
    end.

%%--------------------------------------------------------------------
%% Process Unsubscribe
%%--------------------------------------------------------------------

-compile({inline, [preprocess_unsubscribe/3]}).
preprocess_unsubscribe(Properties, RawTopicFilter, #channel{client = Client}) ->
    RunHook = fun(TopicFilters) ->
                      emqx_hooks:run_fold('client.unsubscribe',
                                          [Client, Properties], TopicFilters)
              end,
    run_fold([fun parse_topic_filters/1, RunHook], RawTopicFilter).

-compile({inline, [process_unsubscribe/2]}).
process_unsubscribe(TopicFilters, Channel) ->
    process_unsubscribe(TopicFilters, [], Channel).

process_unsubscribe([], Acc, Channel) ->
    {lists:reverse(Acc), Channel};

process_unsubscribe([{TopicFilter, SubOpts}|More], Acc, Channel) ->
    {RC, NChannel} = do_unsubscribe(TopicFilter, SubOpts, Channel),
    process_unsubscribe(More, [RC|Acc], NChannel).

do_unsubscribe(TopicFilter, _SubOpts, Channel =
               #channel{client  = Client = #{mountpoint := MountPoint},
                        session = Session}) ->
    TopicFilter1 = emqx_mountpoint:mount(MountPoint, TopicFilter),
    case emqx_session:unsubscribe(Client, TopicFilter1, Session) of
        {ok, NSession} ->
            {?RC_SUCCESS, Channel#channel{session = NSession}};
        {error, RC} -> {RC, Channel}
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

%%TODO: RunFold or Pipeline
handle_out({connack, ?RC_SUCCESS, SP}, Channel = #channel{client = Client}) ->
    AckProps = run_fold([fun enrich_caps/2,
                         fun enrich_server_keepalive/2,
                         fun enrich_assigned_clientid/2
                        ], #{}, Channel),
    Channel1 = ensure_keepalive(AckProps, ensure_connected(Channel)),
    ok = emqx_hooks:run('client.connected', [Client, ?RC_SUCCESS, attrs(Channel1)]),
    AckPacket = ?CONNACK_PACKET(?RC_SUCCESS, SP, AckProps),
    case maybe_resume_session(Channel1) of
        ignore -> {ok, AckPacket, Channel1};
        {ok, Publishes, NSession} ->
            Channel2 = Channel1#channel{session  = NSession,
                                        resuming = false,
                                        pendings = []},
            {ok, Packets, _} = handle_out({publish, Publishes}, Channel2),
            {ok, [AckPacket|Packets], Channel2}
    end;

handle_out({connack, ReasonCode}, Channel = #channel{client = Client,
                                                     protocol = Protocol
                                                    }) ->
    ok = emqx_hooks:run('client.connected', [Client, ReasonCode, attrs(Channel)]),
    ProtoVer = emqx_protocol:info(proto_ver, Protocol),
    ReasonCode1 = if
                      ProtoVer == ?MQTT_PROTO_V5 -> ReasonCode;
                      true -> emqx_reason_codes:compat(connack, ReasonCode)
                  end,
    Reason = emqx_reason_codes:name(ReasonCode1, ProtoVer),
    {stop, {shutdown, Reason}, ?CONNACK_PACKET(ReasonCode1), Channel};

handle_out({deliver, Delivers}, Channel = #channel{session   = Session,
                                                   connected = false}) ->
    NSession = emqx_session:enqueue(Delivers, Session),
    {ok, Channel#channel{session = NSession}};

handle_out({deliver, Delivers}, Channel = #channel{takeover = true,
                                                   pendings = Pendings}) ->
    {ok, Channel#channel{pendings = lists:append(Pendings, Delivers)}};

handle_out({deliver, Delivers}, Channel = #channel{session = Session}) ->
    case emqx_session:deliver(Delivers, Session) of
        {ok, Publishes, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out({publish, Publishes}, ensure_timer(retry_timer, NChannel));
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}}
    end;

handle_out({publish, [Publish]}, Channel) ->
    handle_out(Publish, Channel);

handle_out({publish, Publishes}, Channel) when is_list(Publishes) ->
    Packets = lists:foldl(
                fun(Publish, Acc) ->
                    case handle_out(Publish, Channel) of
                        {ok, Packet, _Ch} ->
                            [Packet|Acc];
                        {ok, _Ch} -> Acc
                    end
                end, [], Publishes),
    {ok, lists:reverse(Packets), Channel};

%% Ignore loop deliver
handle_out({publish, _PacketId, #message{from  = ClientId,
                                         flags = #{nl := true}}},
            Channel = #channel{client = #{client_id := ClientId}}) ->
    {ok, Channel};

handle_out({publish, PacketId, Msg}, Channel =
           #channel{client = Client = #{mountpoint := MountPoint}}) ->
    Msg1 = emqx_message:update_expiry(Msg),
    Msg2 = emqx_hooks:run_fold('message.delivered', [Client], Msg1),
    Msg3 = emqx_mountpoint:unmount(MountPoint, Msg2),
    {ok, emqx_packet:from_message(PacketId, Msg3), Channel};

handle_out({puback, PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBACK_PACKET(PacketId, ReasonCode), Channel};

handle_out({pubrel, PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBREL_PACKET(PacketId, ReasonCode), Channel};

handle_out({pubrec, PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBREC_PACKET(PacketId, ReasonCode), Channel};

handle_out({pubcomp, PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBCOMP_PACKET(PacketId, ReasonCode), Channel};

handle_out({suback, PacketId, ReasonCodes}, Channel = #channel{protocol = Protocol}) ->
    ReasonCodes1 = case emqx_protocol:info(proto_ver, Protocol) of
                       ?MQTT_PROTO_V5 -> ReasonCodes;
                       _Ver ->
                           [emqx_reason_codes:compat(suback, RC) || RC <- ReasonCodes]
                   end,
    {ok, ?SUBACK_PACKET(PacketId, ReasonCodes1), Channel};

handle_out({unsuback, PacketId, ReasonCodes}, Channel = #channel{protocol = Protocol}) ->
    Unsuback = case emqx_protocol:info(proto_ver, Protocol) of
                   ?MQTT_PROTO_V5 ->
                       ?UNSUBACK_PACKET(PacketId, ReasonCodes);
                   _Ver -> ?UNSUBACK_PACKET(PacketId)
               end,
    {ok, Unsuback, Channel};

handle_out({disconnect, ReasonCode}, Channel = #channel{protocol = Protocol}) ->
    case emqx_protocol:info(proto_ver, Protocol) of
        ?MQTT_PROTO_V5 ->
            Reason = emqx_reason_codes:name(ReasonCode),
            Packet = ?DISCONNECT_PACKET(ReasonCode),
            {stop, {shutdown, Reason}, Packet, Channel};
        ProtoVer ->
            Reason = emqx_reason_codes:name(ReasonCode, ProtoVer),
            {stop, {shutdown, Reason}, Channel}
    end;

handle_out({Type, Data}, Channel) ->
    ?LOG(error, "Unexpected outgoing: ~s, ~p", [Type, Data]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

handle_call(kick, Channel) ->
    {stop, {shutdown, kicked}, ok, Channel};

handle_call(discard, Channel) ->
    {stop, {shutdown, discarded}, ok, Channel};

%% Session Takeover
handle_call({takeover, 'begin'}, Channel = #channel{session = Session}) ->
    {ok, Session, Channel#channel{takeover = true}};

handle_call({takeover, 'end'}, Channel = #channel{session  = Session,
                                                  pendings = Pendings}) ->
    ok = emqx_session:takeover(Session),
    AllPendings = lists:append(emqx_misc:drain_deliver(), Pendings),
    {stop, {shutdown, takeovered}, AllPendings, Channel};

handle_call(Req, Channel) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {ok, ignored, Channel}.

%%--------------------------------------------------------------------
%% Handle cast
%%--------------------------------------------------------------------

-spec(handle_cast(Msg :: term(), channel())
      -> ok | {ok, channel()} | {stop, Reason :: term(), channel()}).
handle_cast({register, Attrs, Stats}, #channel{client = #{client_id := ClientId}}) ->
    ok = emqx_cm:register_channel(ClientId),
    emqx_cm:set_chan_attrs(ClientId, Attrs),
    emqx_cm:set_chan_stats(ClientId, Stats);

handle_cast(Msg, Channel) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec(handle_info(Info :: term(), channel())
      -> {ok, channel()} | {stop, Reason :: term(), channel()}).
handle_info({subscribe, RawTopicFilters}, Channel) ->
    TopicFilters = preprocess_subscribe(#{'Internal' => true},
                                        RawTopicFilters, Channel),
    {_ReasonCodes, NChannel} = process_subscribe(TopicFilters, Channel),
    {ok, NChannel};

handle_info({unsubscribe, RawTopicFilters}, Channel) ->
    TopicFilters = preprocess_unsubscribe(#{'Internal' => true},
                                          RawTopicFilters, Channel),
    {_ReasonCodes, NChannel} = process_unsubscribe(TopicFilters, Channel),
    {ok, NChannel};

handle_info(disconnected, Channel = #channel{connected = undefined}) ->
    shutdown(closed, Channel);

handle_info(disconnected, Channel = #channel{protocol = Protocol,
                                             session  = Session}) ->
    %% TODO: Why handle will_msg here?
    publish_will_msg(emqx_protocol:info(will_msg, Protocol)),
    NChannel = Channel#channel{protocol = emqx_protocol:clear_will_msg(Protocol)},
    Interval = emqx_session:info(expiry_interval, Session),
    case Interval of
        ?UINT_MAX ->
            {ok, ensure_disconnected(NChannel)};
        Int when Int > 0 ->
            {ok, ensure_timer(expire_timer, ensure_disconnected(NChannel))};
        _Other -> shutdown(closed, NChannel)
    end;

handle_info(Info, Channel) ->
    ?LOG(error, "Unexpected info: ~p~n", [Info]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

-spec(timeout(reference(), Msg :: term(), channel())
      -> {ok, channel()}
       | {ok, Result :: term(), channel()}
       | {stop, Reason :: term(), channel()}).
timeout(TRef, {emit_stats, Stats},
        Channel = #channel{client = #{client_id := ClientId},
                           timers = #{stats_timer := TRef}
                          }) ->
    ok = emqx_cm:set_chan_stats(ClientId, Stats),
    {ok, clean_timer(stats_timer, Channel)};

timeout(TRef, {keepalive, StatVal}, Channel = #channel{keepalive = Keepalive,
                                                       timers = #{alive_timer := TRef}
                                                      }) ->
    case emqx_keepalive:check(StatVal, Keepalive) of
        {ok, NKeepalive} ->
            NChannel = Channel#channel{keepalive = NKeepalive},
            {ok, reset_timer(alive_timer, NChannel)};
        {error, timeout} ->
            {stop, {shutdown, keepalive_timeout}, Channel}
    end;

timeout(TRef, retry_delivery, Channel = #channel{session = Session,
                                                 timers = #{retry_timer := TRef}
                                                }) ->
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

timeout(TRef, expire_awaiting_rel, Channel = #channel{session = Session,
                                                      timers = #{await_timer := TRef}}) ->
    case emqx_session:expire(awaiting_rel, Session) of
        {ok, Session} ->
            {ok, clean_timer(await_timer, Channel#channel{session = Session})};
        {ok, Timeout, Session} ->
            {ok, reset_timer(await_timer, Timeout, Channel#channel{session = Session})}
    end;

timeout(TRef, expire_session, Channel = #channel{timers = #{expire_timer := TRef}}) ->
    shutdown(expired, Channel);

timeout(TRef, will_message, Channel = #channel{protocol = Protocol,
                                               timers = #{will_timer := TRef}}) ->
    publish_will_msg(emqx_protocol:info(will_msg, Protocol)),
    {ok, clean_timer(will_timer, Channel#channel{protocol = emqx_protocol:clear_will_msg(Protocol)})};

timeout(_TRef, Msg, Channel) ->
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

interval(stats_timer, #channel{client = #{zone := Zone}}) ->
    emqx_zone:get_env(Zone, idle_timeout, 30000);
interval(alive_timer, #channel{keepalive = KeepAlive}) ->
    emqx_keepalive:info(interval, KeepAlive);
interval(retry_timer, #channel{session = Session}) ->
    emqx_session:info(retry_interval, Session);
interval(await_timer, #channel{session = Session}) ->
    emqx_session:info(await_rel_timeout, Session);
interval(expire_timer, #channel{session = Session}) ->
    timer:seconds(emqx_session:info(expiry_interval, Session));
interval(will_timer, #channel{protocol = Protocol}) ->
    timer:seconds(emqx_protocol:info(will_delay_interval, Protocol)).

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

terminate(normal, #channel{client = Client}) ->
    ok = emqx_hooks:run('client.disconnected', [Client, normal]);
terminate(Reason, #channel{client = Client,
                           protocol = Protocol
                          }) ->
    ok = emqx_hooks:run('client.disconnected', [Client, Reason]),
    if
        Protocol == undefined -> ok;
        true -> publish_will_msg(emqx_protocol:info(will_msg, Protocol))
    end.

-spec(received(pos_integer(), channel()) -> channel()).
received(Oct, Channel) ->
    ensure_timer(stats_timer, maybe_gc_and_check_oom(Oct, Channel)).

-spec(sent(pos_integer(), channel()) -> channel()).
sent(Oct, Channel) ->
    ensure_timer(stats_timer, maybe_gc_and_check_oom(Oct, Channel)).

%%TODO: Improve will msg:)
publish_will_msg(undefined) ->
    ok;
publish_will_msg(Msg) ->
    emqx_broker:publish(Msg).

%% @doc Validate incoming packet.
-spec(validate_packet(emqx_types:packet(), channel())
      -> ok | {error, emqx_types:reason_code()}).
validate_packet(Packet, _Channel) ->
    try emqx_packet:validate(Packet) of
        true -> ok
    catch
        error:protocol_error ->
            {error, ?RC_PROTOCOL_ERROR};
        error:subscription_identifier_invalid ->
            {error, ?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED};
        error:topic_alias_invalid ->
            {error, ?RC_TOPIC_ALIAS_INVALID};
        error:topic_filters_invalid ->
            {error, ?RC_TOPIC_FILTER_INVALID};
        error:topic_name_invalid ->
            {error, ?RC_TOPIC_FILTER_INVALID};
        error:_Reason ->
            {error, ?RC_MALFORMED_PACKET}
    end.

%%--------------------------------------------------------------------
%% Check connect packet
%%--------------------------------------------------------------------

check_connect(ConnPkt, Channel) ->
    pipeline([fun check_proto_ver/2,
              fun check_client_id/2,
              %%fun check_flapping/2,
              fun check_banned/2,
              fun check_will_topic/2,
              fun check_will_retain/2], ConnPkt, Channel).

check_proto_ver(#mqtt_packet_connect{proto_ver  = Ver,
                                     proto_name = Name}, _Channel) ->
    case lists:member({Ver, Name}, ?PROTOCOL_NAMES) of
        true  -> ok;
        false -> {error, ?RC_UNSUPPORTED_PROTOCOL_VERSION}
    end.

%% MQTT3.1 does not allow null clientId
check_client_id(#mqtt_packet_connect{proto_ver = ?MQTT_PROTO_V3,
                                     client_id = <<>>
                                    }, _Channel) ->
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};

%% Issue#599: Null clientId and clean_start = false
check_client_id(#mqtt_packet_connect{client_id   = <<>>,
                                     clean_start = false}, _Channel) ->
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};

check_client_id(#mqtt_packet_connect{client_id   = <<>>,
                                     clean_start = true}, _Channel) ->
    ok;

check_client_id(#mqtt_packet_connect{client_id = ClientId},
                #channel{client = #{zone := Zone}}) ->
    Len = byte_size(ClientId),
    MaxLen = emqx_zone:get_env(Zone, max_clientid_len),
    case (1 =< Len) andalso (Len =< MaxLen) of
        true  -> ok;
        false -> {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID}
    end.

%%TODO: check banned...
check_banned(#mqtt_packet_connect{client_id = ClientId,
                                  username = Username},
             #channel{client = Client = #{zone := Zone}}) ->
    case emqx_zone:get_env(Zone, enable_ban, false) of
        true ->
            case emqx_banned:check(Client#{client_id => ClientId,
                                           username  => Username}) of
                true  -> {error, ?RC_BANNED};
                false -> ok
            end;
        false -> ok
    end.

check_will_topic(#mqtt_packet_connect{will_flag = false}, _Channel) ->
    ok;
check_will_topic(#mqtt_packet_connect{will_topic = WillTopic}, _Channel) ->
    try emqx_topic:validate(WillTopic) of
        true -> ok
    catch error:_Error ->
        {error, ?RC_TOPIC_NAME_INVALID}
    end.

check_will_retain(#mqtt_packet_connect{will_retain = false}, _Channel) ->
    ok;
check_will_retain(#mqtt_packet_connect{will_retain = true},
                  #channel{client = #{zone := Zone}}) ->
    case emqx_zone:get_env(Zone, mqtt_retain_available, true) of
        true  -> ok;
        false -> {error, ?RC_RETAIN_NOT_SUPPORTED}
    end.

init_protocol(ConnPkt, Channel) ->
    {ok, Channel#channel{protocol = emqx_protocol:init(ConnPkt)}}.

%%--------------------------------------------------------------------
%% Enrich client
%%--------------------------------------------------------------------

enrich_client(ConnPkt = #mqtt_packet_connect{is_bridge = IsBridge},
              Channel = #channel{client = Client}) ->
    {ok, NConnPkt, NClient} = pipeline([fun set_username/2,
                                        fun maybe_username_as_clientid/2,
                                        fun maybe_assign_clientid/2,
                                        fun fix_mountpoint/2
                                       ], ConnPkt, Client),
    {ok, NConnPkt, Channel#channel{client = NClient#{is_bridge => IsBridge}}}.

%% Username may be not undefined if peer_cert_as_username
set_username(#mqtt_packet_connect{username = Username}, Client = #{username := undefined}) ->
    {ok, Client#{username => Username}};
set_username(_ConnPkt, Client) ->
    {ok, Client}.

maybe_username_as_clientid(_ConnPkt, Client = #{username := undefined}) ->
    {ok, Client};
maybe_username_as_clientid(_ConnPkt, Client = #{zone := Zone, username := Username}) ->
    case emqx_zone:get_env(Zone, use_username_as_clientid, false) of
        true  -> {ok, Client#{client_id => Username}};
        false -> ok
    end.

maybe_assign_clientid(#mqtt_packet_connect{client_id = <<>>}, Client) ->
    RandClientId = emqx_guid:to_base62(emqx_guid:gen()),
    {ok, Client#{client_id => RandClientId}};
maybe_assign_clientid(#mqtt_packet_connect{client_id = ClientId}, Client) ->
    {ok, Client#{client_id => ClientId}}.

fix_mountpoint(_ConnPkt, #{mountpoint := undefined}) -> ok;
fix_mountpoint(_ConnPkt, Client = #{mountpoint := Mountpoint}) ->
    {ok, Client#{mountpoint := emqx_mountpoint:replvar(Mountpoint, Client)}}.

%% @doc Set logger metadata.
set_logger_meta(_ConnPkt, #channel{client = #{client_id := ClientId}}) ->
    emqx_logger:set_metadata_client_id(ClientId).

%%--------------------------------------------------------------------
%% Auth Connect
%%--------------------------------------------------------------------

auth_connect(#mqtt_packet_connect{client_id = ClientId,
                                  username  = Username,
                                  password  = Password},
             Channel = #channel{client = Client}) ->
    case emqx_access_control:authenticate(Client#{password => Password}) of
        {ok, AuthResult} ->
            {ok, Channel#channel{client = maps:merge(Client, AuthResult)}};
        {error, Reason} ->
            ?LOG(warning, "Client ~s (Username: '~s') login failed for ~0p",
                 [ClientId, Username, Reason]),
            {error, emqx_reason_codes:connack_error(Reason)}
    end.

%%--------------------------------------------------------------------
%% Open session
%%--------------------------------------------------------------------

open_session(#mqtt_packet_connect{clean_start = CleanStart,
                                  properties  = ConnProps},
             #channel{client = Client = #{zone := Zone}, protocol = Protocol}) ->
    MaxInflight = get_property('Receive-Maximum', ConnProps,
                               emqx_zone:get_env(Zone, max_inflight, 65535)),
    Interval = 
        case emqx_protocol:info(proto_ver, Protocol) of
            ?MQTT_PROTO_V5 -> get_property('Session-Expiry-Interval', ConnProps, 0);
            _ ->
                case CleanStart of
                    true -> 0;
                    false -> emqx_zone:get_env(Zone, session_expiry_interval, 0)
                end
        end,
    emqx_cm:open_session(CleanStart, Client, #{max_inflight    => MaxInflight,
                                               expiry_interval => Interval
                                              }).

%%--------------------------------------------------------------------
%% Process publish message: Client -> Broker
%%--------------------------------------------------------------------

process_alias(Packet = #mqtt_packet{
                          variable = #mqtt_packet_publish{topic_name = <<>>,
                                                          properties = #{'Topic-Alias' := AliasId}
                                                         } = Publish
                         },
              Channel = #channel{protocol = Protocol}) ->
    case emqx_protocol:find_alias(AliasId, Protocol) of
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
                }, Channel = #channel{protocol = Protocol}) ->
    {ok, Channel#channel{protocol = emqx_protocol:save_alias(AliasId, Topic, Protocol)}};

process_alias(_Packet, Channel) ->
    {ok, Channel}.

%% Check Publish
check_publish(Packet, Channel) ->
    pipeline([fun check_pub_acl/2,
              fun check_pub_alias/2,
              fun check_pub_caps/2], Packet, Channel).

%% Check Pub ACL
check_pub_acl(#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}},
              #channel{client = Client}) ->
    case is_acl_enabled(Client) andalso
         emqx_access_control:check_acl(Client, publish, Topic) of
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
                #channel{protocol = Protocol}) ->
    %% TODO: Move to Protocol
    Limits = emqx_protocol:info(alias_maximum, Protocol),
    case (Limits == undefined)
            orelse (Max = maps:get(inbound, Limits, 0)) == 0
                orelse (AliasId > Max) of
        false -> ok;
        true  -> {error, ?RC_TOPIC_ALIAS_INVALID}
    end;
check_pub_alias(_Packet, _Channel) -> ok.

%% Check Pub Caps
check_pub_caps(#mqtt_packet{header = #mqtt_packet_header{qos = QoS,
                                                         retain = Retain
                                                        }
                           },
               #channel{client = #{zone := Zone}}) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain}).

%% Check Sub
check_subscribe(TopicFilter, SubOpts, Channel) ->
    case check_sub_acl(TopicFilter, Channel) of
        allow -> check_sub_caps(TopicFilter, SubOpts, Channel);
        deny  -> {error, ?RC_NOT_AUTHORIZED}
    end.

%% Check Sub ACL
check_sub_acl(TopicFilter, #channel{client = Client}) ->
    case is_acl_enabled(Client) andalso
         emqx_access_control:check_acl(Client, subscribe, TopicFilter) of
        false  -> allow;
        Result -> Result
    end.

%% Check Sub Caps
check_sub_caps(TopicFilter, SubOpts, #channel{client = #{zone := Zone}}) ->
    emqx_mqtt_caps:check_sub(Zone, TopicFilter, SubOpts).

enrich_subid(#{'Subscription-Identifier' := SubId}, TopicFilters) ->
    [{Topic, SubOpts#{subid => SubId}} || {Topic, SubOpts} <- TopicFilters];
enrich_subid(_Properties, TopicFilters) ->
    TopicFilters.

enrich_subopts(SubOpts, #channel{client = Client, protocol = Proto}) ->
    #{zone := Zone, is_bridge := IsBridge} = Client,
    case emqx_protocol:info(proto_ver, Proto) of
        ?MQTT_PROTO_V5 -> SubOpts;
        _Ver -> Rap = flag(IsBridge),
                Nl = flag(emqx_zone:get_env(Zone, ignore_loop_deliver, false)),
                SubOpts#{rap => Rap, nl => Nl}
    end.

enrich_caps(AckProps, #channel{client = #{zone := Zone}, protocol = Protocol}) ->
    case emqx_protocol:info(proto_ver, Protocol) of
        ?MQTT_PROTO_V5 ->
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
        _Ver -> AckProps
    end.

enrich_server_keepalive(AckProps, #channel{client = #{zone := Zone}}) ->
    case emqx_zone:get_env(Zone, server_keepalive) of
        undefined -> AckProps;
        Keepalive -> AckProps#{'Server-Keep-Alive' => Keepalive}
    end.

enrich_assigned_clientid(AckProps, #channel{client = #{client_id := ClientId},
                                            protocol = Protocol}) ->
    case emqx_protocol:info(client_id, Protocol) of
        <<>> -> %% Original ClientId.
            AckProps#{'Assigned-Client-Identifier' => ClientId};
        _Origin -> AckProps
    end.

ensure_connected(Channel) ->
    Channel#channel{connected = true, connected_at = os:timestamp(), disconnected_at = undefined}.

ensure_disconnected(Channel) ->
    Channel#channel{connected = false, disconnected_at = os:timestamp()}.

ensure_keepalive(#{'Server-Keep-Alive' := Interval}, Channel) ->
    ensure_keepalive_timer(Interval, Channel);
ensure_keepalive(_AckProp, Channel = #channel{protocol = Protocol}) ->
    case emqx_protocol:info(keepalive, Protocol) of
        0 -> Channel;
        Interval -> ensure_keepalive_timer(Interval, Channel)
    end.

ensure_keepalive_timer(Interval, Channel = #channel{client = #{zone := Zone}}) ->
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
    (not IsSuperuser) andalso emqx_zone:get_env(Zone, enable_acl, true).

%% @doc Parse Topic Filters
-compile({inline, [parse_topic_filters/1]}).
parse_topic_filters(TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

%%--------------------------------------------------------------------
%% Maybe GC and Check OOM
%%--------------------------------------------------------------------

maybe_gc_and_check_oom(_Oct, Channel = #channel{gc_state = undefined}) ->
    Channel;
maybe_gc_and_check_oom(Oct, Channel = #channel{gc_state   = GCSt,
                                               oom_policy = OomPolicy}) ->
    {IsGC, GCSt1} = emqx_gc:run(1, Oct, GCSt),
    IsGC andalso emqx_metrics:inc('channel.gc.cnt'),
    IsGC andalso maybe_apply(fun check_oom/1, OomPolicy),
    Channel#channel{gc_state = GCSt1}.

check_oom(OomPolicy) ->
    case emqx_oom:check(OomPolicy) of
        ok -> ok;
        Shutdown -> self() ! Shutdown
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

get_property(_Name, undefined, Default) ->
    Default;
get_property(Name, Props, Default) ->
    maps:get(Name, Props, Default).

sp(true)  -> 1;
sp(false) -> 0.

flag(true)  -> 1;
flag(false) -> 0.

shutdown(Reason, Channel) ->
    {stop, {shutdown, Reason}, Channel}.

