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

%%--------------------------------------------------------------------
%% @doc
%% A stateful interaction between a Client and a Server. Some Sessions
%% last only as long as the Network Connection, others can span multiple
%% consecutive Network Connections between a Client and a Server.
%%
%% The Session State in the Server consists of:
%%
%% The existence of a Session, even if the rest of the Session State is empty.
%%
%% The Clients subscriptions, including any Subscription Identifiers.
%%
%% QoS 1 and QoS 2 messages which have been sent to the Client, but have not
%% been completely acknowledged.
%%
%% QoS 1 and QoS 2 messages pending transmission to the Client and OPTIONALLY
%% QoS 0 messages pending transmission to the Client.
%%
%% QoS 2 messages which have been received from the Client, but have not been
%% completely acknowledged.The Will Message and the Will Delay Interval
%%
%% If the Session is currently not connected, the time at which the Session
%% will end and Session State will be discarded.
%% @end
%%--------------------------------------------------------------------

%% MQTT Session
-module(emqx_session).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Session]").

-export([new/1]).

-export([ info/1
        , attrs/1
        , stats/1
        ]).

-export([ subscribe/3
        , unsubscribe/3
        ]).

-export([publish/3]).

-export([ puback/3
        , pubrec/3
        , pubrel/3
        , pubcomp/3
        ]).

-export([ deliver/3
        , await/3
        , enqueue/2
        ]).

-export_type([ session/0
             , puback_ret/0
             ]).

-import(emqx_zone,
        [ get_env/2
        , get_env/3
        ]).

-record(session, {
          %% Clean Start Flag
          clean_start :: boolean(),

          %% Max subscriptions allowed
          max_subscriptions :: non_neg_integer(),

          %% Client’s Subscriptions.
          subscriptions :: map(),

          %% Upgrade QoS?
          upgrade_qos :: boolean(),

          %% Client <- Broker:
          %% Inflight QoS1, QoS2 messages sent to the client but unacked.
          inflight :: emqx_inflight:inflight(),

          %% All QoS1, QoS2 messages published to when client is disconnected.
          %% QoS 1 and QoS 2 messages pending transmission to the Client.
          %%
          %% Optionally, QoS 0 messages pending transmission to the Client.
          mqueue :: emqx_mqueue:mqueue(),

          %% Next packet id of the session
          next_pkt_id = 1 :: emqx_mqtt:packet_id(),

          %% Retry interval for redelivering QoS1/2 messages
          retry_interval :: timeout(),

          %% Retry delivery timer
          retry_timer :: maybe(reference()),

          %% Client -> Broker:
          %% Inflight QoS2 messages received from client and waiting for pubrel.
          awaiting_rel :: map(),

          %% Awaiting PUBREL Timer
          await_rel_timer :: maybe(reference()),

          %% Max Packets Awaiting PUBREL
          max_awaiting_rel :: non_neg_integer(),

          %% Awaiting PUBREL Timeout
          await_rel_timeout :: timeout(),

          %% Session Expiry Interval
          expiry_interval :: timeout(),

          %% Expired Timer
          expiry_timer :: maybe(reference()),

          %% Created at
          created_at :: erlang:timestamp()
         }).

-opaque(session() :: #session{}).

-type(puback_ret() :: {ok, session()}
                    | {ok, emqx_types:message(), session()}
                    | {error, emqx_mqtt:reason_code()}).

%% @doc Create a session.
-spec(new(Attrs :: map()) -> session()).
new(#{zone            := Zone,
      clean_start     := CleanStart,
      max_inflight    := MaxInflight,
      expiry_interval := ExpiryInterval}) ->
    %% emqx_logger:set_metadata_client_id(ClientId),
    #session{clean_start       = CleanStart,
             max_subscriptions = get_env(Zone, max_subscriptions, 0),
             subscriptions     = #{},
             upgrade_qos       = get_env(Zone, upgrade_qos, false),
             inflight          = emqx_inflight:new(MaxInflight),
             mqueue            = init_mqueue(Zone),
             next_pkt_id       = 1,
             retry_interval    = get_env(Zone, retry_interval, 0),
             awaiting_rel      = #{},
             max_awaiting_rel  = get_env(Zone, max_awaiting_rel, 100),
             await_rel_timeout = get_env(Zone, await_rel_timeout),
             expiry_interval   = ExpiryInterval,
             created_at        = os:timestamp()
            }.

init_mqueue(Zone) ->
    emqx_mqueue:init(#{max_len => get_env(Zone, max_mqueue_len, 1000),
                       store_qos0 => get_env(Zone, mqueue_store_qos0, true),
                       priorities => get_env(Zone, mqueue_priorities),
                       default_priority => get_env(Zone, mqueue_default_priority)
                      }).

%%------------------------------------------------------------------------------
%% Info, Attrs, Stats
%%------------------------------------------------------------------------------

%% @doc Get session info
-spec(info(session()) -> map()).
info(#session{clean_start = CleanStart,
              max_subscriptions = MaxSubscriptions,
              subscriptions = Subscriptions,
              upgrade_qos = UpgradeQoS,
              inflight = Inflight,
              retry_interval = RetryInterval,
              mqueue = MQueue,
              next_pkt_id = PktId,
              max_awaiting_rel = MaxAwaitingRel,
              awaiting_rel = AwaitingRel,
              await_rel_timeout = AwaitRelTimeout,
              expiry_interval = ExpiryInterval,
              created_at = CreatedAt}) ->
    #{clean_start => CleanStart,
      max_subscriptions => MaxSubscriptions,
      subscriptions => Subscriptions,
      upgrade_qos => UpgradeQoS,
      inflight => Inflight,
      retry_interval => RetryInterval,
      mqueue_len => emqx_mqueue:len(MQueue),
      next_pkt_id => PktId,
      awaiting_rel => AwaitingRel,
      max_awaiting_rel => MaxAwaitingRel,
      await_rel_timeout => AwaitRelTimeout,
      expiry_interval => ExpiryInterval div 1000,
      created_at => CreatedAt
     }.

%% @doc Get session attrs.
-spec(attrs(session()) -> map()).
attrs(#session{clean_start = CleanStart,
               expiry_interval = ExpiryInterval,
               created_at = CreatedAt}) ->
    #{clean_start => CleanStart,
      expiry_interval => ExpiryInterval div 1000,
      created_at => CreatedAt
     }.

%% @doc Get session stats.
-spec(stats(session()) -> #{atom() => non_neg_integer()}).
stats(#session{max_subscriptions = MaxSubscriptions,
               subscriptions = Subscriptions,
               inflight = Inflight,
               mqueue = MQueue,
               max_awaiting_rel = MaxAwaitingRel,
               awaiting_rel = AwaitingRel}) ->
    #{max_subscriptions => MaxSubscriptions,
      subscriptions_count => maps:size(Subscriptions),
      max_inflight => emqx_inflight:max_size(Inflight),
      inflight_len => emqx_inflight:size(Inflight),
      max_mqueue => emqx_mqueue:max_len(MQueue),
      mqueue_len => emqx_mqueue:len(MQueue),
      mqueue_dropped => emqx_mqueue:dropped(MQueue),
      max_awaiting_rel => MaxAwaitingRel,
      awaiting_rel_len => maps:size(AwaitingRel)
     }.

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

%% Client -> Broker: SUBSCRIBE
-spec(subscribe(emqx_types:credentials(), emqx_mqtt:topic_filters(), session())
      -> {ok, list(emqx_mqtt:reason_code()), session()}).
subscribe(Credentials, RawTopicFilters, Session = #session{subscriptions = Subscriptions})
  when is_list(RawTopicFilters) ->
    TopicFilters = [emqx_topic:parse(RawTopic, maps:merge(?DEFAULT_SUBOPTS, SubOpts))
                    || {RawTopic, SubOpts} <- RawTopicFilters],
    {ReasonCodes, Subscriptions1} =
        lists:foldr(
          fun({Topic, SubOpts = #{qos := QoS, rc := RC}}, {RcAcc, SubMap})
                when RC == ?QOS_0; RC == ?QOS_1; RC == ?QOS_2 ->
                  {[QoS|RcAcc], do_subscribe(Credentials, Topic, SubOpts, SubMap)};
             ({_Topic, #{rc := RC}}, {RcAcc, SubMap}) ->
                  {[RC|RcAcc], SubMap}
          end, {[], Subscriptions}, TopicFilters),
    {ok, ReasonCodes, Session#session{subscriptions = Subscriptions1}}.

do_subscribe(Credentials = #{client_id := ClientId}, Topic, SubOpts, SubMap) ->
    case maps:find(Topic, SubMap) of
        {ok, SubOpts} ->
            ok = emqx_hooks:run('session.subscribed', [Credentials, Topic, SubOpts#{first => false}]),
            SubMap;
        {ok, _SubOpts} ->
            emqx_broker:set_subopts(Topic, SubOpts),
            %% Why???
            ok = emqx_hooks:run('session.subscribed', [Credentials, Topic, SubOpts#{first => false}]),
            maps:put(Topic, SubOpts, SubMap);
        error ->
            ok = emqx_broker:subscribe(Topic, ClientId, SubOpts),
            ok = emqx_hooks:run('session.subscribed', [Credentials, Topic, SubOpts#{first => true}]),
            maps:put(Topic, SubOpts, SubMap)
    end.

%% Client -> Broker: UNSUBSCRIBE
-spec(unsubscribe(emqx_types:credentials(), emqx_mqtt:topic_filters(), session())
      -> {ok, list(emqx_mqtt:reason_code()), session()}).
unsubscribe(Credentials, RawTopicFilters, Session = #session{subscriptions = Subscriptions})
  when is_list(RawTopicFilters) ->
    TopicFilters = lists:map(fun({RawTopic, Opts}) ->
                                     emqx_topic:parse(RawTopic, Opts);
                                (RawTopic) when is_binary(RawTopic) ->
                                     emqx_topic:parse(RawTopic)
                             end, RawTopicFilters),
    {ReasonCodes, Subscriptions1} =
        lists:foldr(fun({Topic, _SubOpts}, {Acc, SubMap}) ->
                            case maps:find(Topic, SubMap) of
                                {ok, SubOpts} ->
                                    ok = emqx_broker:unsubscribe(Topic),
                                    ok = emqx_hooks:run('session.unsubscribed', [Credentials, Topic, SubOpts]),
                                    {[?RC_SUCCESS|Acc], maps:remove(Topic, SubMap)};
                                error ->
                                    {[?RC_NO_SUBSCRIPTION_EXISTED|Acc], SubMap}
                            end
                    end, {[], Subscriptions}, TopicFilters),
    {ok, ReasonCodes, Session#session{subscriptions = Subscriptions1}}.

%% Client -> Broker: QoS2 PUBLISH
-spec(publish(emqx_mqtt:packet_id(), emqx_types:message(), session())
      -> {ok, emqx_types:deliver_results(), session()} | {error, emqx_mqtt:reason_code()}).
publish(PacketId, Msg = #message{qos = ?QOS_2, timestamp = Ts},
        Session = #session{awaiting_rel = AwaitingRel,
                           max_awaiting_rel = MaxAwaitingRel}) ->
    case is_awaiting_full(MaxAwaitingRel, AwaitingRel) of
        false ->
            case maps:is_key(PacketId, AwaitingRel) of
                false ->
                    DeliverResults = emqx_broker:publish(Msg),
                    AwaitingRel1 = maps:put(PacketId, Ts, AwaitingRel),
                    NSession = Session#session{awaiting_rel = AwaitingRel1},
                    {ok, DeliverResults, ensure_await_rel_timer(NSession)};
                  true ->
                    {error, ?RC_PACKET_IDENTIFIER_IN_USE}
              end;
        true ->
            ?LOG(warning, "Dropped qos2 packet ~w for too many awaiting_rel", [PacketId]),
            ok = emqx_metrics:inc('messages.qos2.dropped'),
            {error, ?RC_RECEIVE_MAXIMUM_EXCEEDED}
    end;

%% QoS0/1
publish(_PacketId, Msg, Session) ->
    {ok, emqx_broker:publish(Msg)}.

%% Client -> Broker: PUBACK
-spec(puback(emqx_mqtt:packet_id(), emqx_mqtt:reason_code(), session())
      -> puback_ret()).
puback(PacketId, _ReasonCode, Session = #session{inflight = Inflight, mqueue = Q}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, {_, Msg}, _Ts}} ->
            %% #{client_id => ClientId, username => Username}
            %% ok = emqx_hooks:run('message.acked', [], Msg]),
            Inflight1 = emqx_inflight:delete(PacketId, Inflight),
            Session1 = Session#session{inflight = Inflight1},
            case (emqx_mqueue:is_empty(Q) orelse emqx_mqueue:out(Q)) of
                true -> {ok, Session1};
                {{value, Msg}, Q1} ->
                    {ok, Msg, Session1#session{mqueue = Q1}}
            end;
        false ->
            ?LOG(warning, "The PUBACK PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.puback.missed'),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%% Client -> Broker: PUBREC
-spec(pubrec(emqx_mqtt:packet_id(), emqx_mqtt:reason_code(), session())
      -> {ok, session()} | {error, emqx_mqtt:reason_code()}).
pubrec(PacketId, _ReasonCode, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, {_, Msg}, _Ts}} ->
            %% ok = emqx_hooks:run('message.acked', [#{client_id => ClientId, username => Username}, Msg]),
            Inflight1 = emqx_inflight:update(PacketId, {pubrel, PacketId, os:timestamp()}, Inflight),
            {ok, Session#session{inflight = Inflight1}};
        {value, {pubrel, PacketId, _Ts}} ->
            ?LOG(warning, "The PUBREC ~w is duplicated", [PacketId]),
            {error, ?RC_PACKET_IDENTIFIER_IN_USE};
        none ->
            ?LOG(warning, "The PUBREC ~w is not found.", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrec.missed'),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%% Client -> Broker: PUBREL
-spec(pubrel(emqx_mqtt:packet_id(), emqx_mqtt:reason_code(), session())
      -> {ok, session()} | {error, emqx_mqtt:reason_code()}).
pubrel(PacketId, ReasonCode, Session = #session{awaiting_rel = AwaitingRel}) ->
    case maps:take(PacketId, AwaitingRel) of
        {_Ts, AwaitingRel1} ->
            {ok, Session#session{awaiting_rel = AwaitingRel1}};
        error ->
            ?LOG(warning, "The PUBREL PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrel.missed'),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
      end.

%% Client -> Broker: PUBCOMP
-spec(pubcomp(emqx_mqtt:packet_id(), emqx_mqtt:reason_code(), session()) -> puback_ret()).
pubcomp(PacketId, ReasonCode, Session = #session{inflight = Inflight, mqueue = Q}) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            Inflight1 = emqx_inflight:delete(PacketId, Inflight),
            Session1 = Session#session{inflight = Inflight1},
            case (emqx_mqueue:is_empty(Q) orelse emqx_mqueue:out(Q)) of
                true -> {ok, Session1};
                {{value, Msg}, Q1} ->
                    {ok, Msg, Session1#session{mqueue = Q1}}
            end;
        false ->
            ?LOG(warning, "The PUBCOMP PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.pubcomp.missed'),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Handle delivery
%%--------------------------------------------------------------------

deliver(Topic, Msg, Session = #session{subscriptions = SubMap}) ->
    SubOpts = get_subopts(Topic, SubMap),
    case enrich(SubOpts, Msg, Session) of
        {ok, Msg1} ->
            deliver(Msg1, Session);
        ignore -> ignore
    end.

%% Enqueue message if the client has been disconnected
%% process_msg(Msg, Session = #session{conn_pid = undefined}) ->
%%    {ignore, enqueue_msg(Msg, Session)};

deliver(Msg = #message{qos = ?QOS_0}, Session) ->
    {ok, {publish, undefined, Msg}, Session};

deliver(Msg = #message{qos = QoS},
        Session = #session{next_pkt_id = PacketId, inflight = Inflight})
    when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->
    case emqx_inflight:is_full(Inflight) of
        true ->
            {ignore, enqueue(Msg, Session)};
        false ->
            Publish = {publish, PacketId, Msg},
            NSession = await(PacketId, Msg, Session),
            {ok, Publish, next_pkt_id(NSession)}
    end.

enqueue(Msg, Session = #session{mqueue = Q}) ->
    emqx_pd:update_counter(enqueue_stats, 1),
    {Dropped, NewQ} = emqx_mqueue:in(Msg, Q),
    if
        Dropped =/= undefined ->
            %% SessProps = #{client_id => ClientId, username => Username},
            ok; %% = emqx_hooks:run('message.dropped', [SessProps, Dropped]);
        true -> ok
    end,
    Session#session{mqueue = NewQ}.

%%------------------------------------------------------------------------------
%% Awaiting ACK for QoS1/QoS2 Messages
%%------------------------------------------------------------------------------

await(PacketId, Msg, Session = #session{inflight = Inflight}) ->
    Publish = {publish, {PacketId, Msg}, os:timestamp()},
    Inflight1 = emqx_inflight:insert(PacketId, Publish, Inflight),
    ensure_retry_timer(Session#session{inflight = Inflight1}).

get_subopts(Topic, SubMap) ->
    case maps:find(Topic, SubMap) of
        {ok, #{nl := Nl, qos := QoS, rap := Rap, subid := SubId}} ->
            [{nl, Nl}, {qos, QoS}, {rap, Rap}, {subid, SubId}];
        {ok, #{nl := Nl, qos := QoS, rap := Rap}} ->
            [{nl, Nl}, {qos, QoS}, {rap, Rap}];
        error -> []
    end.

enrich([], Msg, _Session) ->
    {ok, Msg};
%%enrich([{nl, 1}|_Opts], #message{from = ClientId}, #session{client_id = ClientId}) ->
%%    ignore;
enrich([{nl, _}|Opts], Msg, Session) ->
    enrich(Opts, Msg, Session);
enrich([{qos, SubQoS}|Opts], Msg = #message{qos = PubQoS}, Session = #session{upgrade_qos= true}) ->
    enrich(Opts, Msg#message{qos = max(SubQoS, PubQoS)}, Session);
enrich([{qos, SubQoS}|Opts], Msg = #message{qos = PubQoS}, Session = #session{upgrade_qos= false}) ->
    enrich(Opts, Msg#message{qos = min(SubQoS, PubQoS)}, Session);
enrich([{rap, _Rap}|Opts], Msg = #message{flags = Flags, headers = #{retained := true}}, Session = #session{}) ->
    enrich(Opts, Msg#message{flags = maps:put(retain, true, Flags)}, Session);
enrich([{rap, 0}|Opts], Msg = #message{flags = Flags}, Session) ->
    enrich(Opts, Msg#message{flags = maps:put(retain, false, Flags)}, Session);
enrich([{rap, _}|Opts], Msg, Session) ->
    enrich(Opts, Msg, Session);
enrich([{subid, SubId}|Opts], Msg, Session) ->
    enrich(Opts, emqx_message:set_header('Subscription-Identifier', SubId, Msg), Session).

%%--------------------------------------------------------------------
%% Ensure retry timer
%%--------------------------------------------------------------------

ensure_retry_timer(Session = #session{retry_interval = Interval, retry_timer = undefined}) ->
    ensure_retry_timer(Interval, Session);
ensure_retry_timer(Session) ->
    Session.

ensure_retry_timer(Interval, Session = #session{retry_timer = undefined}) ->
    TRef = emqx_misc:start_timer(Interval, retry_delivery),
    Session#session{retry_timer = TRef};
ensure_retry_timer(_Interval, Session) ->
    Session.

%%--------------------------------------------------------------------
%% Check awaiting rel
%%--------------------------------------------------------------------

is_awaiting_full(_MaxAwaitingRel = 0, _AwaitingRel) ->
    false;
is_awaiting_full(MaxAwaitingRel, AwaitingRel) ->
    maps:size(AwaitingRel) >= MaxAwaitingRel.

%%--------------------------------------------------------------------
%% Ensure await_rel timer
%%--------------------------------------------------------------------

ensure_await_rel_timer(Session = #session{await_rel_timeout = Timeout,
                                          await_rel_timer = undefined}) ->
    ensure_await_rel_timer(Timeout, Session);
ensure_await_rel_timer(Session) ->
    Session.

ensure_await_rel_timer(Timeout, Session = #session{await_rel_timer = undefined}) ->
    TRef = emqx_misc:start_timer(Timeout, check_awaiting_rel),
    Session#session{await_rel_timer = TRef};
ensure_await_rel_timer(_Timeout, Session) ->
    Session.

%%--------------------------------------------------------------------
%% Expire Awaiting Rel
%%--------------------------------------------------------------------

expire_awaiting_rel(Session = #session{awaiting_rel = AwaitingRel}) ->
    case maps:size(AwaitingRel) of
        0 -> Session;
        _ -> expire_awaiting_rel(lists:keysort(2, maps:to_list(AwaitingRel)), os:timestamp(), Session)
    end.

expire_awaiting_rel([], _Now, Session) ->
    Session#session{await_rel_timer = undefined};

expire_awaiting_rel([{PacketId, Ts} | More], Now,
                    Session = #session{awaiting_rel = AwaitingRel,
                                       await_rel_timeout = Timeout}) ->
    case (timer:now_diff(Now, Ts) div 1000) of
        Age when Age >= Timeout ->
            ok = emqx_metrics:inc('messages.qos2.expired'),
            ?LOG(warning, "Dropped qos2 packet ~s for await_rel_timeout", [PacketId]),
            NSession = Session#session{awaiting_rel = maps:remove(PacketId, AwaitingRel)},
            expire_awaiting_rel(More, Now, NSession);
        Age ->
            ensure_await_rel_timer(Timeout - max(0, Age), Session)
    end.

%%--------------------------------------------------------------------
%% Next Packet Id
%%--------------------------------------------------------------------

next_pkt_id(Session = #session{next_pkt_id = 16#FFFF}) ->
    Session#session{next_pkt_id = 1};

next_pkt_id(Session = #session{next_pkt_id = Id}) ->
    Session#session{next_pkt_id = Id + 1}.

