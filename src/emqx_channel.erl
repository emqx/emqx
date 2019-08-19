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

-export([ info/1
        , info/2
        , attrs/1
        , caps/1
        ]).

%% for tests
-export([set/3]).

-export([takeover/2]).

-export([ init/2
        , handle_in/2
        , handle_out/2
        , handle_out/3
        , handle_call/2
        , handle_cast/2
        , handle_info/2
        , timeout/3
        , terminate/2
        ]).

-export([ensure_timer/2]).

-export([gc/3]).

-import(emqx_access_control,
        [ authenticate/1
        , check_acl/3
        ]).

-import(emqx_misc, [start_timer/2]).

-export_type([channel/0]).

-record(channel, {
          client        :: emqx_types:client(),
          session       :: emqx_session:session(),
          proto_name    :: binary(),
          proto_ver     :: emqx_types:ver(),
          keepalive     :: non_neg_integer(),
          will_msg      :: emqx_types:message(),
          topic_aliases :: maybe(map()),
          alias_maximum :: maybe(map()),
          ack_props     :: maybe(emqx_types:properties()),
          idle_timeout  :: timeout(),
          retry_timer   :: maybe(reference()),
          alive_timer   :: maybe(reference()),
          stats_timer   :: disabled | maybe(reference()),
          expiry_timer  :: maybe(reference()),
          gc_state      :: emqx_gc:gc_state(), %% GC State
          oom_policy    :: emqx_oom:oom_policy(), %% OOM Policy
          connected     :: boolean(),
          connected_at  :: erlang:timestamp(),
          resuming      :: boolean(),
          pendings      :: list()
         }).

-opaque(channel() :: #channel{}).

-define(NO_PROPS, undefined).

%%--------------------------------------------------------------------
%% Info, Attrs and Caps
%%--------------------------------------------------------------------

-spec(info(channel()) -> emqx_types:infos()).
info(#channel{client        = Client,
              session       = Session,
              proto_name    = ProtoName,
              proto_ver     = ProtoVer,
              keepalive     = Keepalive,
              will_msg      = WillMsg,
              topic_aliases = Aliases,
              stats_timer   = StatsTimer,
              idle_timeout  = IdleTimeout,
              gc_state      = GCState,
              connected     = Connected,
              connected_at  = ConnectedAt}) ->
    #{client        => Client,
      session       => if Session == undefined ->
                              undefined;
                          true -> emqx_session:info(Session)
                       end,
      proto_name    => ProtoName,
      proto_ver     => ProtoVer,
      keepalive     => Keepalive,
      will_msg      => WillMsg,
      topic_aliases => Aliases,
      enable_stats  => case StatsTimer of
                           disabled   -> false;
                           _Otherwise -> true
                       end,
      idle_timeout  => IdleTimeout,
      gc_state      => emqx_gc:info(GCState),
      connected     => Connected,
      connected_at  => ConnectedAt,
      resuming      => false,
      pendings      => []
     }.

-spec(info(atom(), channel()) -> term()).
info(client, #channel{client = Client}) ->
    Client;
info(zone, #channel{client = #{zone := Zone}}) ->
    Zone;
info(client_id, #channel{client = #{client_id := ClientId}}) ->
    ClientId;
info(session, #channel{session = Session}) ->
    Session;
info(proto_name, #channel{proto_name = ProtoName}) ->
    ProtoName;
info(proto_ver, #channel{proto_ver = ProtoVer}) ->
    ProtoVer;
info(keepalive, #channel{keepalive = Keepalive}) ->
    Keepalive;
info(will_msg, #channel{will_msg = WillMsg}) ->
    WillMsg;
info(topic_aliases, #channel{topic_aliases = Aliases}) ->
    Aliases;
info(enable_stats, #channel{stats_timer = disabled}) ->
    false;
info(enable_stats, #channel{stats_timer = _TRef}) ->
    true;
info(idle_timeout, #channel{idle_timeout = IdleTimeout}) ->
    IdleTimeout;
info(gc_state, #channel{gc_state = GCState}) ->
    emqx_gc:info(GCState);
info(connected, #channel{connected = Connected}) ->
    Connected;
info(connected_at, #channel{connected_at = ConnectedAt}) ->
    ConnectedAt.

-spec(attrs(channel()) -> emqx_types:attrs()).
attrs(#channel{client       = Client,
               session      = Session,
               proto_name   = ProtoName,
               proto_ver    = ProtoVer,
               keepalive    = Keepalive,
               connected    = Connected,
               connected_at = ConnectedAt}) ->
    #{client       => Client,
      session      => if Session == undefined ->
                             undefined;
                         true -> emqx_session:attrs(Session)
                      end,
      proto_name   => ProtoName,
      proto_ver    => ProtoVer,
      keepalive    => Keepalive,
      connected    => Connected,
      connected_at => ConnectedAt
     }.

-spec(caps(channel()) -> emqx_types:caps()).
caps(#channel{client = #{zone := Zone}}) ->
    emqx_mqtt_caps:get_caps(Zone).

%%--------------------------------------------------------------------
%% For unit tests
%%--------------------------------------------------------------------

set(client, Client, Channel) ->
    Channel#channel{client = Client};
set(session, Session, Channel) ->
    Channel#channel{session = Session}.

%%--------------------------------------------------------------------
%% Takeover session
%%--------------------------------------------------------------------

takeover('begin', Channel = #channel{session = Session}) ->
    {ok, Session, Channel#channel{resuming = true}};

takeover('end', Channel = #channel{session = Session,
                                   pendings = Pendings}) ->
    ok = emqx_session:takeover(Session),
    {ok, Pendings, Channel}.

%%--------------------------------------------------------------------
%% Init a channel
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
                          is_superuser => false}, ConnInfo),
    IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
    EnableStats = emqx_zone:get_env(Zone, enable_stats, true),
    StatsTimer = if EnableStats -> undefined;
                    ?Otherwise  -> disabled
                 end,
    GcState = emqx_gc:init(emqx_zone:get_env(Zone, force_gc_policy, false)),
    OomPolicy = emqx_oom:init(emqx_zone:get_env(Zone, force_shutdown_policy)),
    #channel{client       = Client,
             proto_name   = <<"MQTT">>,
             proto_ver    = ?MQTT_PROTO_V4,
             keepalive    = 0,
             idle_timeout = IdleTimout,
             stats_timer  = StatsTimer,
             gc_state     = GcState,
             oom_policy   = OomPolicy,
             connected    = false
            }.

peer_cert_as_username(Options) ->
    proplists:get_value(peer_cert_as_username, Options).

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
     handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);

handle_in(?CONNECT_PACKET(
             #mqtt_packet_connect{proto_name = ProtoName,
                                  proto_ver  = ProtoVer,
                                  keepalive  = Keepalive,
                                  client_id  = ClientId
                                 } = ConnPkt), Channel) ->
    Channel1 = Channel#channel{proto_name = ProtoName,
                               proto_ver  = ProtoVer,
                               keepalive  = Keepalive
                              },
    ok = emqx_logger:set_metadata_client_id(ClientId),
    case pipeline([fun validate_in/2,
                   fun process_props/2,
                   fun check_connect/2,
                   fun enrich_client/2,
                   fun auth_connect/2], ConnPkt, Channel1) of
        {ok, NConnPkt, NChannel = #channel{client = #{client_id := ClientId1}}} ->
            ok = emqx_logger:set_metadata_client_id(ClientId1),
            process_connect(NConnPkt, NChannel);
        {error, ReasonCode, NChannel} ->
            handle_out(connack, ReasonCode, NChannel)
    end;

handle_in(Packet = ?PUBLISH_PACKET(_QoS, Topic, _PacketId), Channel = #channel{proto_ver = Ver}) ->
    case pipeline([fun validate_in/2,
                   fun process_alias/2,
                   fun check_publish/2], Packet, Channel) of
        {ok, NPacket, NChannel} ->
            process_publish(NPacket, NChannel);
        {error, ReasonCode, NChannel} ->
            ?LOG(warning, "Cannot publish message to ~s due to ~s",
                 [Topic, emqx_reason_codes:text(ReasonCode, Ver)]),
                 handle_out(disconnect, ReasonCode, NChannel)
    end;

%%TODO: How to handle the ReasonCode?
handle_in(?PUBACK_PACKET(PacketId, _ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:puback(PacketId, Session) of
        {ok, Publishes, NSession} ->
            handle_out(publish, Publishes, Channel#channel{session = NSession});
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}};
        {error, _NotFound} ->
            %%TODO: How to handle NotFound, inc metrics?
            {ok, Channel}
    end;

%%TODO: How to handle the ReasonCode?
handle_in(?PUBREC_PACKET(PacketId, _ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:pubrec(PacketId, Session) of
        {ok, NSession} ->
            handle_out(pubrel, {PacketId, ?RC_SUCCESS}, Channel#channel{session = NSession});
        {error, ReasonCode} ->
            handle_out(pubrel, {PacketId, ReasonCode}, Channel)
    end;

%%TODO: How to handle the ReasonCode?
handle_in(?PUBREL_PACKET(PacketId, _ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:pubrel(PacketId, Session) of
        {ok, NSession} ->
            handle_out(pubcomp, {PacketId, ?RC_SUCCESS}, Channel#channel{session = NSession});
        {error, ReasonCode} ->
            handle_out(pubcomp, {PacketId, ReasonCode}, Channel)
    end;

handle_in(?PUBCOMP_PACKET(PacketId, _ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:pubcomp(PacketId, Session) of
        {ok, Publishes, NSession} ->
            handle_out(publish, Publishes, Channel#channel{session = NSession});
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}};
        {error, _NotFound} ->
            %% TODO: how to handle NotFound?
            {ok, Channel}
    end;

handle_in(Packet = ?SUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
          Channel = #channel{client = Client}) ->
    case validate_in(Packet, Channel) of
        ok ->
            TopicFilters1 = [emqx_topic:parse(TopicFilter, SubOpts)
                             || {TopicFilter, SubOpts} <- TopicFilters],
            TopicFilters2 = emqx_hooks:run_fold('client.subscribe',
                                                [Client, Properties],
                                                TopicFilters1),
            TopicFilters3 = enrich_subid(Properties, TopicFilters2),
            {ReasonCodes, NChannel} = process_subscribe(TopicFilters3, Channel),
            handle_out(suback, {PacketId, ReasonCodes}, NChannel);
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, Channel)
    end;

handle_in(Packet = ?UNSUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
          Channel = #channel{client = Client}) ->
    case validate_in(Packet, Channel) of
        ok ->
            TopicFilters1 = lists:map(fun emqx_topic:parse/1, TopicFilters),
            TopicFilters2 = emqx_hooks:run_fold('client.unsubscribe',
                                                [Client, Properties],
                                                TopicFilters1),
            {ReasonCodes, NChannel} = process_unsubscribe(TopicFilters2, Channel),
            handle_out(unsuback, {PacketId, ReasonCodes}, NChannel);
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, Channel)
    end;

handle_in(?PACKET(?PINGREQ), Channel) ->
    {ok, ?PACKET(?PINGRESP), Channel};

handle_in(?DISCONNECT_PACKET(?RC_SUCCESS), Channel) ->
    %% Clear will msg
    {stop, normal, Channel#channel{will_msg = undefined}};

handle_in(?DISCONNECT_PACKET(RC), Channel = #channel{proto_ver = Ver}) ->
    {stop, {shutdown, emqx_reason_codes:name(RC, Ver)}, Channel};

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
        {ok, Session, SP} ->
            WillMsg = emqx_packet:will_msg(ConnPkt),
            NChannel = Channel#channel{session = Session,
                                       will_msg = WillMsg,
                                       connected = true,
                                       connected_at = os:timestamp()
                                      },
            handle_out(connack, {?RC_SUCCESS, sp(SP)}, NChannel);
        {error, Reason} ->
            %% TODO: Unknown error?
            ?LOG(error, "Failed to open session: ~p", [Reason]),
            handle_out(connack, ?RC_UNSPECIFIED_ERROR, Channel)
    end.

%%--------------------------------------------------------------------
%% Process Publish
%%--------------------------------------------------------------------

%% Process Publish
process_publish(Packet = ?PUBLISH_PACKET(_QoS, _Topic, PacketId),
                Channel = #channel{client = Client, proto_ver = ProtoVer}) ->
    Msg = emqx_packet:to_message(Client, Packet),
    %%TODO: Improve later.
    Msg1 = emqx_message:set_flag(dup, false, emqx_message:set_header(proto_ver, ProtoVer, Msg)),
    process_publish(PacketId, mount(Client, Msg1), Channel).

process_publish(_PacketId, Msg = #message{qos = ?QOS_0}, Channel) ->
    _ = emqx_broker:publish(Msg),
    {ok, Channel};

process_publish(PacketId, Msg = #message{qos = ?QOS_1}, Channel) ->
    Deliveries = emqx_broker:publish(Msg),
    ReasonCode = emqx_reason_codes:puback(Deliveries),
    handle_out(puback, {PacketId, ReasonCode}, Channel);

process_publish(PacketId, Msg = #message{qos = ?QOS_2},
                Channel = #channel{session = Session}) ->
    case emqx_session:publish(PacketId, Msg, Session) of
        {ok, Deliveries, NSession} ->
            ReasonCode = emqx_reason_codes:puback(Deliveries),
            handle_out(pubrec, {PacketId, ReasonCode},
                       Channel#channel{session = NSession});
        {error, ReasonCode} ->
            handle_out(pubrec, {PacketId, ReasonCode}, Channel)
    end.

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

do_subscribe(TopicFilter, SubOpts = #{qos := QoS},
             Channel = #channel{client = Client, session = Session}) ->
    case check_subscribe(TopicFilter, SubOpts, Channel) of
        ok -> TopicFilter1 = mount(Client, TopicFilter),
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

process_unsubscribe(TopicFilters, Channel) ->
    process_unsubscribe(TopicFilters, [], Channel).

process_unsubscribe([], Acc, Channel) ->
    {lists:reverse(Acc), Channel};

process_unsubscribe([{TopicFilter, SubOpts}|More], Acc, Channel) ->
    {RC, Channel1} = do_unsubscribe(TopicFilter, SubOpts, Channel),
    process_unsubscribe(More, [RC|Acc], Channel1).

do_unsubscribe(TopicFilter, _SubOpts, Channel = #channel{client = Client,
                                                         session = Session}) ->
    case emqx_session:unsubscribe(Client, mount(Client, TopicFilter), Session) of
        {ok, NSession} ->
            {?RC_SUCCESS, Channel#channel{session = NSession}};
        {error, RC} -> {RC, Channel}
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

handle_out(Deliver = {deliver, _Topic, _Msg},
           Channel = #channel{resuming = true, pendings = Pendings}) ->
    Delivers = emqx_misc:drain_deliver([Deliver]),
    {ok, Channel#channel{pendings = lists:append(Pendings, Delivers)}};

handle_out(Deliver = {deliver, _Topic, _Msg}, Channel = #channel{session = Session}) ->
    Delivers = emqx_misc:drain_deliver([Deliver]),
    case emqx_session:deliver(Delivers, Session) of
        {ok, Publishes, NSession} ->
            handle_out(publish, Publishes, Channel#channel{session = NSession});
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}}
    end;

handle_out({publish, PacketId, Msg}, Channel = #channel{client = Client}) ->
    Msg1 = emqx_hooks:run_fold('message.deliver', [Client],
                               emqx_message:update_expiry(Msg)),
    Packet = emqx_packet:from_message(PacketId, unmount(Client, Msg1)),
    {ok, Packet, Channel}.

handle_out(connack, {?RC_SUCCESS, SP},
           Channel = #channel{client = Client = #{zone := Zone},
                              ack_props = AckProps,
                              alias_maximum = AliasMaximum}) ->
    ok = emqx_hooks:run('client.connected', [Client, ?RC_SUCCESS, attrs(Channel)]),
    #{max_packet_size := MaxPktSize,
      max_qos_allowed := MaxQoS,
      retain_available := Retain,
      max_topic_alias := MaxAlias,
      shared_subscription := Shared,
      wildcard_subscription := Wildcard
     } = caps(Channel),
    %% Response-Information is so far not set by broker.
    %% i.e. It's a Client-to-Client contract for the request-response topic naming scheme.
    %% According to MQTT 5.0 spec:
    %%   A common use of this is to pass a globally unique portion of the topic tree which
    %%   is reserved for this Client for at least the lifetime of its Session.
    %%   This often cannot just be a random name as both the requesting Client and the
    %%   responding Client need to be authorized to use it.
    %% If we are to support it in the feature, the implementation should be flexible
    %% to allow prefixing the response topic based on different ACL config.
    %% e.g. prefix by username or client-id, so that unauthorized clients can not
    %% subscribe requests or responses that are not intended for them.
    AckProps1 = if AckProps == undefined -> #{}; true -> AckProps end,
    AckProps2 = AckProps1#{'Retain-Available' => flag(Retain),
                           'Maximum-Packet-Size' => MaxPktSize,
                           'Topic-Alias-Maximum' => MaxAlias,
                           'Wildcard-Subscription-Available' => flag(Wildcard),
                           'Subscription-Identifier-Available' => 1,
                           %'Response-Information' =>
                           'Shared-Subscription-Available' => flag(Shared),
                           'Maximum-QoS' => MaxQoS
                          },
    AckProps3 = case emqx_zone:get_env(Zone, server_keepalive) of
                    undefined -> AckProps2;
                    Keepalive -> AckProps2#{'Server-Keep-Alive' => Keepalive}
                end,
    AliasMaximum1 = set_property(inbound, MaxAlias, AliasMaximum),
    Channel1 = Channel#channel{alias_maximum = AliasMaximum1,
                               ack_props = undefined
                              },
    {ok, ?CONNACK_PACKET(?RC_SUCCESS, SP, AckProps3), Channel1};

handle_out(connack, ReasonCode, Channel = #channel{client = Client,
                                                   proto_ver = ProtoVer}) ->
    ok = emqx_hooks:run('client.connected', [Client, ReasonCode, attrs(Channel)]),
    ReasonCode1 = if
                      ProtoVer == ?MQTT_PROTO_V5 -> ReasonCode;
                      true -> emqx_reason_codes:compat(connack, ReasonCode)
                  end,
    Reason = emqx_reason_codes:name(ReasonCode1, ProtoVer),
    {stop, {shutdown, Reason}, ?CONNACK_PACKET(ReasonCode1), Channel};

handle_out(publish, Publishes, Channel) ->
    Packets = [element(2, handle_out(Publish, Channel)) || Publish <- Publishes],
    {ok, Packets, Channel};

%% TODO: How to handle the puberr?
handle_out(puberr, _ReasonCode, Channel) ->
    {ok, Channel};

handle_out(puback, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBACK_PACKET(PacketId, ReasonCode), Channel};

handle_out(pubrel, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBREL_PACKET(PacketId, ReasonCode), Channel};

handle_out(pubrec, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBREC_PACKET(PacketId, ReasonCode), Channel};

handle_out(pubcomp, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBCOMP_PACKET(PacketId, ReasonCode), Channel};

handle_out(suback, {PacketId, ReasonCodes},
           Channel = #channel{proto_ver = ?MQTT_PROTO_V5}) ->
    %% TODO: ACL Deny
    {ok, ?SUBACK_PACKET(PacketId, ReasonCodes), Channel};

handle_out(suback, {PacketId, ReasonCodes}, Channel) ->
    %% TODO: ACL Deny
    ReasonCodes1 = [emqx_reason_codes:compat(suback, RC) || RC <- ReasonCodes],
    {ok, ?SUBACK_PACKET(PacketId, ReasonCodes1), Channel};

handle_out(unsuback, {PacketId, ReasonCodes},
           Channel = #channel{proto_ver = ?MQTT_PROTO_V5}) ->
    {ok, ?UNSUBACK_PACKET(PacketId, ReasonCodes), Channel};

%% Ignore reason codes if not MQTT5
handle_out(unsuback, {PacketId, _ReasonCodes}, Channel) ->
    {ok, ?UNSUBACK_PACKET(PacketId), Channel};

handle_out(disconnect, ReasonCode, Channel = #channel{proto_ver = ?MQTT_PROTO_V5}) ->
    Reason = emqx_reason_codes:name(ReasonCode),
    {stop, {shutdown, Reason}, ?DISCONNECT_PACKET(ReasonCode), Channel};

handle_out(disconnect, ReasonCode, Channel = #channel{proto_ver = ProtoVer}) ->
    {stop, {shutdown, emqx_reason_codes:name(ReasonCode, ProtoVer)}, Channel};

handle_out(Type, Data, Channel) ->
    ?LOG(error, "Unexpected outgoing: ~s, ~p", [Type, Data]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

handle_call(Req, Channel) ->
    ?LOG(error, "Unexpected call: Req", [Req]),
    {ok, ignored, Channel}.

%%--------------------------------------------------------------------
%% Handle cast
%%--------------------------------------------------------------------

handle_cast(discard, Channel) ->
    {stop, {shutdown, discarded}, Channel};

handle_cast(Msg, Channel) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec(handle_info(Info :: term(), channel())
      -> {ok, channel()} | {stop, Reason :: term(), channel()}).
handle_info({subscribe, TopicFilters}, Channel = #channel{client = Client}) ->
    TopicFilters1 = emqx_hooks:run_fold('client.subscribe',
                                        [Client, #{'Internal' => true}],
                                        parse(subscribe, TopicFilters)),
    {_ReasonCodes, NChannel} = process_subscribe(TopicFilters1, Channel),
    {ok, NChannel};

handle_info({unsubscribe, TopicFilters}, Channel = #channel{client = Client}) ->
    TopicFilters1 = emqx_hooks:run_fold('client.unsubscribe',
                                        [Client, #{'Internal' => true}],
                                        parse(unsubscribe, TopicFilters)),
    {_ReasonCodes, NChannel} = process_unsubscribe(TopicFilters1, Channel),
    {ok, NChannel};

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
timeout(TRef, {emit_stats, Stats}, Channel = #channel{stats_timer = TRef}) ->
    ClientId = info(client_id, Channel),
    ok = emqx_cm:set_chan_stats(ClientId, Stats),
    {ok, Channel#channel{stats_timer = undefined}};

timeout(TRef, retry_deliver, Channel = #channel{%%session = Session,
                                                retry_timer = TRef}) ->
    %% case emqx_session:retry(Session) of
    %% TODO: ...
    {ok, Channel#channel{retry_timer = undefined}};

timeout(_TRef, Msg, Channel) ->
    ?LOG(error, "Unexpected timeout: ~p~n", [Msg]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Ensure timers
%%--------------------------------------------------------------------

ensure_timer(emit_stats, Channel = #channel{stats_timer = undefined,
                                            idle_timeout = IdleTimeout
                                           }) ->
    Channel#channel{stats_timer = start_timer(IdleTimeout, emit_stats)};

ensure_timer(retry, Channel = #channel{session = Session,
                                       retry_timer = undefined}) ->
    Interval = emqx_session:info(retry_interval, Session),
    TRef = emqx_misc:start_timer(Interval, retry_deliver),
    Channel#channel{retry_timer = TRef};

%% disabled or timer existed
ensure_timer(_Name, Channel) ->
    Channel.

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

terminate(normal, #channel{client = Client}) ->
    ok = emqx_hooks:run('client.disconnected', [Client, normal]);
terminate(Reason, #channel{client = Client, will_msg = WillMsg}) ->
    ok = emqx_hooks:run('client.disconnected', [Client, Reason]),
    publish_will_msg(WillMsg).

%%TODO: Improve will msg:)
publish_will_msg(undefined) ->
    ok;
publish_will_msg(Msg) ->
    emqx_broker:publish(Msg).

%%--------------------------------------------------------------------
%% GC the channel.
%%--------------------------------------------------------------------

gc(_Cnt, _Oct, Channel = #channel{gc_state = undefined}) ->
    Channel;
gc(Cnt, Oct, Channel = #channel{gc_state = GCSt}) ->
    {Ok, GCSt1} = emqx_gc:run(Cnt, Oct, GCSt),
    Ok andalso emqx_metrics:inc('channel.gc.cnt'),
    Channel#channel{gc_state = GCSt1}.

%%--------------------------------------------------------------------
%% Validate incoming packet
%%--------------------------------------------------------------------

-spec(validate_in(emqx_types:packet(), channel())
      -> ok | {error, emqx_types:reason_code()}).
validate_in(Packet, _Channel) ->
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
%% Preprocess properties
%%--------------------------------------------------------------------

process_props(#mqtt_packet_connect{
                 properties = #{'Topic-Alias-Maximum' := Max}
                },
              Channel = #channel{alias_maximum = AliasMaximum}) ->
    NAliasMaximum = if AliasMaximum == undefined ->
                           #{outbound => Max};
                       true -> AliasMaximum#{outbound => Max}
                    end,
    {ok, Channel#channel{alias_maximum = NAliasMaximum}};

process_props(Packet, Channel) ->
    {ok, Packet, Channel}.

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

%%--------------------------------------------------------------------
%% Enrich client
%%--------------------------------------------------------------------

enrich_client(ConnPkt, Channel) ->
    pipeline([fun set_username/2,
              fun maybe_use_username_as_clientid/2,
              fun maybe_assign_clientid/2,
              fun set_rest_client_fields/2], ConnPkt, Channel).

maybe_use_username_as_clientid(_ConnPkt, Channel = #channel{client = #{username := undefined}}) ->
    {ok, Channel};
maybe_use_username_as_clientid(_ConnPkt, Channel = #channel{client = Client = #{zone := Zone,
                                                                                username := Username}}) ->
    NClient =
        case emqx_zone:get_env(Zone, use_username_as_clientid, false) of
            true -> Client#{client_id => Username};
            false -> Client
        end,
    {ok, Channel#channel{client = NClient}}.

maybe_assign_clientid(#mqtt_packet_connect{client_id = <<>>},
                      Channel = #channel{client = Client,
                                         ack_props = AckProps}) ->
    ClientId = emqx_guid:to_base62(emqx_guid:gen()),
    AckProps1 = set_property('Assigned-Client-Identifier', ClientId, AckProps),
    {ok, Channel#channel{client = Client#{client_id => ClientId}, ack_props = AckProps1}};
maybe_assign_clientid(#mqtt_packet_connect{client_id = ClientId},
                      Channel = #channel{client = Client}) ->
    {ok, Channel#channel{client = Client#{client_id => ClientId}}}.

%% Username maybe not undefined if peer_cert_as_username
set_username(#mqtt_packet_connect{username = Username},
             Channel = #channel{client = Client = #{username := undefined}}) ->
    {ok, Channel#channel{client = Client#{username => Username}}};
set_username(_ConnPkt, Channel) ->
    {ok, Channel}.

set_rest_client_fields(#mqtt_packet_connect{is_bridge = IsBridge},
                       Channel = #channel{client = Client}) ->
    {ok, Channel#channel{client = Client#{is_bridge => IsBridge}}}.

%%--------------------------------------------------------------------
%% Auth Connect
%%--------------------------------------------------------------------

auth_connect(#mqtt_packet_connect{client_id = ClientId,
                                  username  = Username,
                                  password  = Password},
             Channel = #channel{client = Client}) ->
    case authenticate(Client#{password => Password}) of
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
             #channel{client = Client = #{zone := Zone}}) ->
    MaxInflight = get_property('Receive-Maximum', ConnProps,
                               emqx_zone:get_env(Zone, max_inflight, 65535)),
    Interval = get_property('Session-Expiry-Interval', ConnProps,
                            emqx_zone:get_env(Zone, session_expiry_interval, 0)),
    emqx_cm:open_session(CleanStart, Client, #{max_inflight => MaxInflight,
                                               expiry_interval => Interval
                                              }).

%%--------------------------------------------------------------------
%% Process publish message: Client -> Broker
%%--------------------------------------------------------------------

process_alias(Packet = #mqtt_packet{
                          variable = #mqtt_packet_publish{topic_name = <<>>,
                                                          properties = #{'Topic-Alias' := AliasId}
                                                         } = Publish
                         }, Channel = #channel{topic_aliases = Aliases}) ->
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

find_alias(_AliasId, undefined) ->
    false;
find_alias(AliasId, Aliases) ->
    maps:find(AliasId, Aliases).

save_alias(AliasId, Topic, undefined) ->
    #{AliasId => Topic};
save_alias(AliasId, Topic, Aliases) ->
    maps:put(AliasId, Topic, Aliases).

%% Check Publish
check_publish(Packet, Channel) ->
    pipeline([fun check_pub_acl/2,
              fun check_pub_alias/2,
              fun check_pub_caps/2], Packet, Channel).

%% Check Pub ACL
check_pub_acl(#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}},
              #channel{client = Client}) ->
    case is_acl_enabled(Client) andalso check_acl(Client, publish, Topic) of
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
         check_acl(Client, subscribe, TopicFilter) of
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

enrich_subopts(SubOpts, #channel{proto_ver = ?MQTT_PROTO_V5}) ->
    SubOpts;
enrich_subopts(SubOpts, #channel{client = #{zone := Zone, is_bridge := IsBridge}}) ->
    Rap = flag(IsBridge),
    Nl = flag(emqx_zone:get_env(Zone, ignore_loop_deliver, false)),
    SubOpts#{rap => Rap, nl => Nl}.

%%--------------------------------------------------------------------
%% Is ACL enabled?
%%--------------------------------------------------------------------

is_acl_enabled(#{zone := Zone, is_superuser := IsSuperuser}) ->
    (not IsSuperuser) andalso emqx_zone:get_env(Zone, enable_acl, true).

%%--------------------------------------------------------------------
%% Parse Topic Filters
%%--------------------------------------------------------------------

parse(subscribe, TopicFilters) ->
    [emqx_topic:parse(TopicFilter, SubOpts) || {TopicFilter, SubOpts} <- TopicFilters];

parse(unsubscribe, TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

%%--------------------------------------------------------------------
%% Mount/Unmount
%%--------------------------------------------------------------------

mount(Client = #{mountpoint := MountPoint}, TopicOrMsg) ->
    emqx_mountpoint:mount(
      emqx_mountpoint:replvar(MountPoint, Client), TopicOrMsg).

unmount(Client = #{mountpoint := MountPoint}, TopicOrMsg) ->
    emqx_mountpoint:unmount(
      emqx_mountpoint:replvar(MountPoint, Client), TopicOrMsg).

%%--------------------------------------------------------------------
%% Pipeline
%%--------------------------------------------------------------------

pipeline([], Packet, Channel) ->
    {ok, Packet, Channel};

pipeline([Fun|More], Packet, Channel) ->
    case Fun(Packet, Channel) of
        ok -> pipeline(More, Packet, Channel);
        {ok, NChannel} ->
            pipeline(More, Packet, NChannel);
        {ok, NPacket, NChannel} ->
            pipeline(More, NPacket, NChannel);
        {error, ReasonCode} ->
            {error, ReasonCode, Channel};
        {error, ReasonCode, NChannel} ->
            {error, ReasonCode, NChannel}
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

set_property(Name, Value, ?NO_PROPS) ->
    #{Name => Value};
set_property(Name, Value, Props) ->
    Props#{Name => Value}.

get_property(_Name, undefined, Default) ->
    Default;
get_property(Name, Props, Default) ->
    maps:get(Name, Props, Default).

sp(true)  -> 1;
sp(false) -> 0.

flag(true)  -> 1;
flag(false) -> 0.

