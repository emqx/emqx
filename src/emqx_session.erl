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

-module(emqx_session).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-export([ new/1
        , handle/2
        , close/1
        ]).

-export([ info/1
        , attrs/1
        , stats/1
        ]).

-export([ subscribe/2
        , unsubscribe/2
        , publish/3
        , puback/2
        , puback/3
        , pubrec/2
        , pubrec/3
        , pubrel/3
        , pubcomp/3
        ]).

-record(session, {
          %% ClientId: Identifier of Session
          client_id :: binary(),

          %% Clean Start Flag
          clean_start = false :: boolean(),

          %% Username
          username :: maybe(binary()),

          %% Conn Binding: local | remote
          %% binding = local :: local | remote,

          %% Deliver fun
          deliver_fun :: function(),

          %% Next packet id of the session
          next_pkt_id = 1 :: emqx_mqtt_types:packet_id(),

          %% Max subscriptions
          max_subscriptions :: non_neg_integer(),

          %% Client’s Subscriptions.
          subscriptions :: map(),

          %% Upgrade QoS?
          upgrade_qos = false :: boolean(),

          %% Client <- Broker: Inflight QoS1, QoS2 messages sent to the client but unacked.
          inflight :: emqx_inflight:inflight(),

          %% Max Inflight Size. DEPRECATED: Get from inflight
          %% max_inflight = 32 :: non_neg_integer(),

          %% Retry interval for redelivering QoS1/2 messages
          retry_interval = 20000 :: timeout(),

          %% Retry Timer
          retry_timer :: maybe(reference()),

          %% All QoS1, QoS2 messages published to when client is disconnected.
          %% QoS 1 and QoS 2 messages pending transmission to the Client.
          %%
          %% Optionally, QoS 0 messages pending transmission to the Client.
          mqueue :: emqx_mqueue:mqueue(),

          %% Max Packets Awaiting PUBREL
          max_awaiting_rel = 100 :: non_neg_integer(),

          %% Awaiting PUBREL Timeout
          await_rel_timeout = 20000 :: timeout(),

          %% Client -> Broker: Inflight QoS2 messages received from client and waiting for pubrel.
          awaiting_rel :: map(),

          %% Awaiting PUBREL Timer
          await_rel_timer :: maybe(reference()),

          will_msg :: emqx:message(),

          will_delay_timer :: maybe(reference()),

          %% Session Expiry Interval
          expiry_interval = 7200 :: timeout(),

          %% Expired Timer
          expiry_timer :: maybe(reference()),

          %% Created at
          created_at :: erlang:timestamp()
         }).

-opaque(session() :: #session{}).

-export_type([session/0]).

%% @doc Create a session.
-spec(new(Attrs :: map()) -> session()).
new(#{zone            := Zone,
      client_id       := ClientId,
      clean_start     := CleanStart,
      username        := Username,
      expiry_interval := ExpiryInterval,
      max_inflight    := MaxInflight,
      will_msg        := WillMsg}) ->
    emqx_logger:set_metadata_client_id(ClientId),
    #session{client_id       = ClientId,
             clean_start     = CleanStart,
             username        = Username,
             subscriptions   = #{},
             inflight        = emqx_inflight:new(MaxInflight),
             mqueue          = init_mqueue(Zone),
             awaiting_rel    = #{},
             expiry_interval = ExpiryInterval,
             created_at      = os:timestamp(),
             will_msg        = WillMsg}.

init_mqueue(Zone) ->
    emqx_mqueue:init(#{max_len => emqx_zone:get_env(Zone, max_mqueue_len, 1000),
                       store_qos0 => emqx_zone:get_env(Zone, mqueue_store_qos0, true),
                       priorities => emqx_zone:get_env(Zone, mqueue_priorities),
                       default_priority => emqx_zone:get_env(Zone, mqueue_default_priority)
                      }).

%% @doc Get session info
-spec(info(session()) -> list({atom(), term()})).
info(Session = #session{next_pkt_id = PktId,
                        max_subscriptions = MaxSubscriptions,
                        subscriptions = Subscriptions,
                        upgrade_qos = UpgradeQoS,
                        inflight = Inflight,
                        retry_interval = RetryInterval,
                        mqueue = MQueue,
                        max_awaiting_rel = MaxAwaitingRel,
                        awaiting_rel = AwaitingRel,
                        await_rel_timeout = AwaitRelTimeout}) ->
    attrs(Session) ++ [{next_pkt_id, PktId},
                       {max_subscriptions, MaxSubscriptions},
                       {subscriptions, Subscriptions},
                       {upgrade_qos, UpgradeQoS},
                       {inflight, Inflight},
                       {retry_interval, RetryInterval},
                       {mqueue_len, emqx_mqueue:len(MQueue)},
                       {awaiting_rel, AwaitingRel},
                       {max_awaiting_rel, MaxAwaitingRel},
                       {await_rel_timeout, AwaitRelTimeout}].

%% @doc Get session attrs
-spec(attrs(session()) -> list({atom(), term()})).
attrs(#session{client_id = ClientId,
               clean_start = CleanStart,
               username = Username,
               expiry_interval = ExpiryInterval,
               created_at = CreatedAt}) ->
    [{client_id, ClientId},
     {clean_start, CleanStart},
     {username, Username},
     {expiry_interval, ExpiryInterval div 1000},
     {created_at, CreatedAt}].

%% @doc Get session stats
-spec(stats(session()) -> list({atom(), non_neg_integer()})).
stats(#session{max_subscriptions = MaxSubscriptions,
               subscriptions = Subscriptions,
               inflight = Inflight,
               mqueue = MQueue,
               max_awaiting_rel = MaxAwaitingRel,
               awaiting_rel = AwaitingRel}) ->
    lists:append(emqx_misc:proc_stats(),
                 [{max_subscriptions, MaxSubscriptions},
                  {subscriptions_count, maps:size(Subscriptions)},
                  {max_inflight, emqx_inflight:max_size(Inflight)},
                  {inflight_len, emqx_inflight:size(Inflight)},
                  {max_mqueue, emqx_mqueue:max_len(MQueue)},
                  {mqueue_len, emqx_mqueue:len(MQueue)},
                  {mqueue_dropped, emqx_mqueue:dropped(MQueue)},
                  {max_awaiting_rel, MaxAwaitingRel},
                  {awaiting_rel_len, maps:size(AwaitingRel)},
                  {deliver_msg, emqx_pd:get_counter(deliver_stats)},
                  {enqueue_msg, emqx_pd:get_counter(enqueue_stats)}]).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

%% SUBSCRIBE:
-spec(subscribe(list({emqx_topic:topic(), emqx_types:subopts()}), session())
      -> {ok, list(emqx_mqtt_types:reason_code()), session()}).
subscribe(RawTopicFilters, Session = #session{client_id = ClientId,
                                              username = Username,
                                              subscriptions = Subscriptions})
  when is_list(RawTopicFilters) ->
    TopicFilters = [emqx_topic:parse(RawTopic, maps:merge(?DEFAULT_SUBOPTS, SubOpts))
                    || {RawTopic, SubOpts} <- RawTopicFilters],
    {ReasonCodes, Subscriptions1} =
        lists:foldr(
            fun ({Topic, SubOpts = #{qos := QoS, rc := RC}}, {RcAcc, SubMap}) when
                      RC == ?QOS_0; RC == ?QOS_1; RC == ?QOS_2 ->
                    {[QoS|RcAcc], do_subscribe(ClientId, Username, Topic, SubOpts, SubMap)};
                ({_Topic, #{rc := RC}}, {RcAcc, SubMap}) ->
                    {[RC|RcAcc], SubMap}
            end, {[], Subscriptions}, TopicFilters),
    {ok, ReasonCodes, Session#session{subscriptions = Subscriptions1}}.

%% TODO: improve later.
do_subscribe(ClientId, Username, Topic, SubOpts, SubMap) ->
    case maps:find(Topic, SubMap) of
        {ok, SubOpts} ->
            ok = emqx_hooks:run('session.subscribed', [#{client_id => ClientId, username => Username}, Topic, SubOpts#{first => false}]),
            SubMap;
        {ok, _SubOpts} ->
            emqx_broker:set_subopts(Topic, SubOpts),
            %% Why???
            ok = emqx_hooks:run('session.subscribed', [#{client_id => ClientId, username => Username}, Topic, SubOpts#{first => false}]),
            maps:put(Topic, SubOpts, SubMap);
        error ->
            emqx_broker:subscribe(Topic, ClientId, SubOpts),
            ok = emqx_hooks:run('session.subscribed', [#{client_id => ClientId, username => Username}, Topic, SubOpts#{first => true}]),
            maps:put(Topic, SubOpts, SubMap)
    end.

%% PUBLISH:
-spec(publish(emqx_mqtt_types:packet_id(), emqx_types:message(), session())
      -> {ok, emqx_types:deliver_results()} | {error, term()}).
publish(_PacketId, Msg = #message{qos = ?QOS_0}, _Session) ->
    %% Publish QoS0 message directly
    {ok, emqx_broker:publish(Msg)};

publish(_PacketId, Msg = #message{qos = ?QOS_1}, _Session) ->
    %% Publish QoS1 message directly
    {ok, emqx_broker:publish(Msg)};

%% PUBLISH: This is only to register packetId to session state.
%% The actual message dispatching should be done by the caller.
publish(PacketId, Msg = #message{qos = ?QOS_2, timestamp = Ts},
        Session = #session{awaiting_rel = AwaitingRel,
                           max_awaiting_rel = MaxAwaitingRel}) ->
    %% Register QoS2 message packet ID (and timestamp) to session, then publish
    case is_awaiting_full(MaxAwaitingRel, AwaitingRel) of
          false ->
              case maps:is_key(PacketId, AwaitingRel) of
                  false ->
                      NewAwaitingRel = maps:put(PacketId, Ts, AwaitingRel),
                      NSession = Session#session{awaiting_rel = NewAwaitingRel},
                      {ok, emqx_broker:publish(Msg), ensure_await_rel_timer(NSession)};
                  true ->
                      {error, ?RC_PACKET_IDENTIFIER_IN_USE}
              end;
          true ->
              ?LOG(warning, "[Session] Dropped qos2 packet ~w for too many awaiting_rel", [PacketId]),
              ok = emqx_metrics:inc('messages.qos2.dropped'),
              {error, ?RC_RECEIVE_MAXIMUM_EXCEEDED}
      end.

%% PUBACK:
-spec(puback(emqx_mqtt_types:packet_id(), session()) -> session()).
puback(PacketId, Session) ->
    puback(PacketId, ?RC_SUCCESS, Session).

-spec(puback(emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code(), session())
      -> session()).
puback(PacketId, ReasonCode, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            dequeue(acked(puback, PacketId, Session));
        false ->
            ?LOG(warning, "[Session] The PUBACK PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.puback.missed'),
            Session
    end.

%% PUBREC:
-spec(pubrec(emqx_mqtt_types:packet_id(), session())
      -> ok | {error, emqx_mqtt_types:reason_code()}).
pubrec(PacketId, Session) ->
    pubrec(PacketId, ?RC_SUCCESS, Session).

-spec(pubrec(emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code(), session())
      -> {ok, session()} | {error, emqx_mqtt_types:reason_code()}).
pubrec(PacketId, ReasonCode, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            {ok, acked(pubrec, PacketId, Session)};
        false ->
            ?LOG(warning, "[Session] The PUBREC PacketId ~w is not found.", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrec.missed'),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%% PUBREL:
-spec(pubrel(emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code(), session())
      -> {ok, session()} | {error, emqx_mqtt_types:reason_code()}).
pubrel(PacketId, ReasonCode, Session = #session{awaiting_rel = AwaitingRel}) ->
    case maps:take(PacketId, AwaitingRel) of
        {_Ts, AwaitingRel1} ->
            {ok, Session#session{awaiting_rel = AwaitingRel1}};
        error ->
            ?LOG(warning, "[Session] The PUBREL PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrel.missed'),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
      end.

%% PUBCOMP:
-spec(pubcomp(emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code(), session())
      -> {ok, session()} | {error, emqx_mqtt_types:reason_code()}).
pubcomp(PacketId, ReasonCode, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            {ok, dequeue(acked(pubcomp, PacketId, Session))};
        false ->
            ?LOG(warning, "[Session] The PUBCOMP PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.pubcomp.missed'),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%------------------------------------------------------------------------------
%% Awaiting ACK for QoS1/QoS2 Messages
%%------------------------------------------------------------------------------

await(PacketId, Msg, Session = #session{inflight = Inflight}) ->
    Inflight1 = emqx_inflight:insert(
                  PacketId, {publish, {PacketId, Msg}, os:timestamp()}, Inflight),
    ensure_retry_timer(Session#session{inflight = Inflight1}).

acked(puback, PacketId, Session = #session{client_id = ClientId, username = Username, inflight  = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, {_, Msg}, _Ts}} ->
            ok = emqx_hooks:run('message.acked', [#{client_id => ClientId, username => Username}, Msg]),
            Session#session{inflight = emqx_inflight:delete(PacketId, Inflight)};
        none ->
            ?LOG(warning, "[Session] Duplicated PUBACK PacketId ~w", [PacketId]),
            Session
    end;

acked(pubrec, PacketId, Session = #session{client_id = ClientId, username = Username, inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, {_, Msg}, _Ts}} ->
            ok = emqx_hooks:run('message.acked', [#{client_id => ClientId, username => Username}, Msg]),
            Inflight1 = emqx_inflight:update(PacketId, {pubrel, PacketId, os:timestamp()}, Inflight),
            Session#session{inflight = Inflight1};
        {value, {pubrel, PacketId, _Ts}} ->
            ?LOG(warning, "[Session] Duplicated PUBREC PacketId ~w", [PacketId]),
            Session;
        none ->
            ?LOG(warning, "[Session] Unexpected PUBREC PacketId ~w", [PacketId]),
            Session
    end;

acked(pubcomp, PacketId, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            Session#session{inflight = emqx_inflight:delete(PacketId, Inflight)};
        false ->
            ?LOG(warning, "[Session] PUBCOMP PacketId ~w is not found", [PacketId]),
            Session
    end.

%% UNSUBSCRIBE:
-spec(unsubscribe(emqx_mqtt_types:topic_filters(), session())
      -> {ok, list(emqx_mqtt_types:reason_code()), session()}).
unsubscribe(RawTopicFilters, Session = #session{client_id = ClientId,
                                                username = Username,
                                                subscriptions = Subscriptions})
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
                                    ok = emqx_hooks:run('session.unsubscribed', [#{client_id => ClientId, username => Username}, Topic, SubOpts]),
                                    {[?RC_SUCCESS|Acc], maps:remove(Topic, SubMap)};
                                error ->
                                    {[?RC_NO_SUBSCRIPTION_EXISTED|Acc], SubMap}
                            end
                    end, {[], Subscriptions}, TopicFilters),
    {ok, ReasonCodes, Session#session{subscriptions = Subscriptions1}}.

-spec(resume(map(), session()) -> session()).
resume(#{will_msg        := WillMsg,
         expiry_interval := ExpiryInterval,
         max_inflight    := MaxInflight},
       Session = #session{client_id        = ClientId,
                          clean_start      = CleanStart,
                          retry_timer      = RetryTimer,
                          await_rel_timer  = AwaitTimer,
                          expiry_timer     = ExpireTimer,
                          will_delay_timer = WillDelayTimer}) ->

    %% ?LOG(info, "[Session] Resumed by ~p ", [self()]),

    %% Cancel Timers
    lists:foreach(fun emqx_misc:cancel_timer/1,
                  [RetryTimer, AwaitTimer, ExpireTimer, WillDelayTimer]),

    %% case kick(ClientId, OldConnPid, ConnPid) of
    %%    ok -> ?LOG(warning, "[Session] Connection ~p kickout ~p", [ConnPid, OldConnPid]);
    %%    ignore -> ok
    %% end,

    Inflight = emqx_inflight:update_size(MaxInflight, Session#session.inflight),

    Session1 = Session#session{clean_start      = false,
                               retry_timer      = undefined,
                               awaiting_rel     = #{},
                               await_rel_timer  = undefined,
                               expiry_timer     = undefined,
                               expiry_interval  = ExpiryInterval,
                               inflight         = Inflight,
                               will_delay_timer = undefined,
                               will_msg         = WillMsg
                              },

    %% Clean Session: true -> false???
    CleanStart andalso emqx_cm:set_session_attrs(ClientId, attrs(Session1)),

    %%ok = emqx_hooks:run('session.resumed', [#{client_id => ClientId}, attrs(Session1)]),

    %% Replay delivery and Dequeue pending messages
    dequeue(retry_delivery(true, Session1)).

-spec(update_expiry_interval(timeout(), session()) -> session()).
update_expiry_interval(Interval, Session) ->
    Session#session{expiry_interval = Interval}.

-spec(close(session()) -> ok).
close(_Session) -> ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------


%%deliver_fun(ConnPid) when node(ConnPid) == node() ->
%%    fun(Packet) -> ConnPid ! {deliver, Packet}, ok end;
%%deliver_fun(ConnPid) ->
%%    Node = node(ConnPid),
%%    fun(Packet) ->
%%        true = emqx_rpc:cast(Node, erlang, send, [ConnPid, {deliver, Packet}]), ok
%%    end.

%%------------------------------------------------------------------------------
%% Replay or Retry Delivery

%% Redeliver at once if force is true
retry_delivery(Force, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:is_empty(Inflight) of
        true  -> Session;
        false ->
            SortFun = fun({_, _, Ts1}, {_, _, Ts2}) -> Ts1 < Ts2 end,
            Msgs = lists:sort(SortFun, emqx_inflight:values(Inflight)),
            retry_delivery(Force, Msgs, os:timestamp(), Session)
    end.

retry_delivery(_Force, [], _Now, Session) ->
    %% Retry again...
    ensure_retry_timer(Session);

retry_delivery(Force, [{Type, Msg0, Ts} | Msgs], Now,
               Session = #session{inflight = Inflight,
                                  retry_interval = Interval}) ->
    %% Microseconds -> MilliSeconds
    Age = timer:now_diff(Now, Ts) div 1000,
    if
        Force orelse (Age >= Interval) ->
            Inflight1 = case {Type, Msg0} of
                            {publish, {PacketId, Msg}} ->
                                case emqx_message:is_expired(Msg) of
                                    true ->
                                        ok = emqx_metrics:inc('messages.expired'),
                                        emqx_inflight:delete(PacketId, Inflight);
                                    false ->
                                        redeliver({PacketId, Msg}, Session),
                                        emqx_inflight:update(PacketId, {publish, {PacketId, Msg}, Now}, Inflight)
                                end;
                            {pubrel, PacketId} ->
                                redeliver({pubrel, PacketId}, Session),
                                emqx_inflight:update(PacketId, {pubrel, PacketId, Now}, Inflight)
                        end,
            retry_delivery(Force, Msgs, Now, Session#session{inflight = Inflight1});
        true ->
            ensure_retry_timer(Interval - max(0, Age), Session)
    end.

%%------------------------------------------------------------------------------
%% Send Will Message
%%------------------------------------------------------------------------------

send_willmsg(undefined) ->
    ignore;
send_willmsg(WillMsg) ->
    emqx_broker:publish(WillMsg).

%%------------------------------------------------------------------------------
%% Expire Awaiting Rel
%%------------------------------------------------------------------------------

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
            ?LOG(warning, "[Session] Dropped qos2 packet ~s for await_rel_timeout", [PacketId]),
            NSession = Session#session{awaiting_rel = maps:remove(PacketId, AwaitingRel)},
            expire_awaiting_rel(More, Now, NSession);
        Age ->
            ensure_await_rel_timer(Timeout - max(0, Age), Session)
    end.

%%------------------------------------------------------------------------------
%% Check awaiting rel
%%------------------------------------------------------------------------------

is_awaiting_full(_MaxAwaitingRel = 0, _AwaitingRel) ->
    false;
is_awaiting_full(MaxAwaitingRel, AwaitingRel) ->
    maps:size(AwaitingRel) >= MaxAwaitingRel.

%%------------------------------------------------------------------------------
%% Dispatch messages
%%------------------------------------------------------------------------------

handle(Msgs, Session = #session{inflight = Inflight,
                                         client_id = ClientId,
                                         username = Username,
                                         subscriptions = SubMap}) ->
    SessProps = #{client_id => ClientId, username => Username},
    %% Drain the mailbox and batch deliver
    Msgs1 = Msgs, %% drain_m(batch_n(Inflight), Msgs),
    %% Ack the messages for shared subscription
    Msgs2 = maybe_ack_shared(Msgs1, Session),
    %% Process suboptions
    Msgs3 = lists:foldr(
              fun({Topic, Msg}, Acc) ->
                      SubOpts = find_subopts(Topic, SubMap),
                      case process_subopts(SubOpts, Msg, Session) of
                          {ok, Msg1} -> [Msg1|Acc];
                          ignore ->
                              emqx_hooks:run('message.dropped', [SessProps, Msg]),
                              Acc
                      end
              end, [], Msgs2),
    batch_process(Msgs3, Session).

%% Ack or nack the messages of shared subscription?
maybe_ack_shared(Msgs, Session) when is_list(Msgs) ->
    lists:foldr(
      fun({Topic, Msg}, Acc) ->
            case maybe_ack_shared(Msg, Session) of
                ok -> Acc;
                Msg1 -> [{Topic, Msg1}|Acc]
            end
      end, [], Msgs);

maybe_ack_shared(Msg, Session) ->
    case emqx_shared_sub:is_ack_required(Msg) of
        true -> do_ack_shared(Msg, Session);
        false -> Msg
    end.

do_ack_shared(Msg, Session = #session{inflight = Inflight}) ->
    case {true, %% is_connection_alive(Session),
          emqx_inflight:is_full(Inflight)} of
        {false, _} ->
            %% Require ack, but we do not have connection
            %% negative ack the message so it can try the next subscriber in the group
            emqx_shared_sub:nack_no_connection(Msg);
        {_, true} ->
            emqx_shared_sub:maybe_nack_dropped(Msg);
         _ ->
            %% Ack QoS1/QoS2 messages when message is delivered to connection.
            %% NOTE: NOT to wait for PUBACK because:
            %% The sender is monitoring this session process,
            %% if the message is delivered to client but connection or session crashes,
            %% sender will try to dispatch the message to the next shared subscriber.
            %% This violates spec as QoS2 messages are not allowed to be sent to more
            %% than one member in the group.
            emqx_shared_sub:maybe_ack(Msg)
    end.

process_subopts([], Msg, _Session) ->
    {ok, Msg};
process_subopts([{nl, 1}|_Opts], #message{from = ClientId}, #session{client_id = ClientId}) ->
    ignore;
process_subopts([{nl, _}|Opts], Msg, Session) ->
    process_subopts(Opts, Msg, Session);
process_subopts([{qos, SubQoS}|Opts], Msg = #message{qos = PubQoS},
                Session = #session{upgrade_qos= true}) ->
    process_subopts(Opts, Msg#message{qos = max(SubQoS, PubQoS)}, Session);
process_subopts([{qos, SubQoS}|Opts], Msg = #message{qos = PubQoS},
                Session = #session{upgrade_qos= false}) ->
    process_subopts(Opts, Msg#message{qos = min(SubQoS, PubQoS)}, Session);
process_subopts([{rap, _Rap}|Opts], Msg = #message{flags = Flags, headers = #{retained := true}}, Session = #session{}) ->
    process_subopts(Opts, Msg#message{flags = maps:put(retain, true, Flags)}, Session);
process_subopts([{rap, 0}|Opts], Msg = #message{flags = Flags}, Session = #session{}) ->
    process_subopts(Opts, Msg#message{flags = maps:put(retain, false, Flags)}, Session);
process_subopts([{rap, _}|Opts], Msg, Session) ->
    process_subopts(Opts, Msg, Session);
process_subopts([{subid, SubId}|Opts], Msg, Session) ->
    process_subopts(Opts, emqx_message:set_header('Subscription-Identifier', SubId, Msg), Session).

find_subopts(Topic, SubMap) ->
    case maps:find(Topic, SubMap) of
        {ok, #{nl := Nl, qos := QoS, rap := Rap, subid := SubId}} ->
            [{nl, Nl}, {qos, QoS}, {rap, Rap}, {subid, SubId}];
        {ok, #{nl := Nl, qos := QoS, rap := Rap}} ->
            [{nl, Nl}, {qos, QoS}, {rap, Rap}];
        error -> []
    end.

batch_process(Msgs, Session) ->
    {ok, Publishes, NSession} = process_msgs(Msgs, [], Session),
    ok = batch_deliver(Publishes, NSession),
    NSession.

process_msgs([], Publishes, Session) ->
    {ok, lists:reverse(Publishes), Session};

process_msgs([Msg|Msgs], Publishes, Session) ->
    case process_msg(Msg, Session) of
        {ok, Publish, NSession} ->
            process_msgs(Msgs, [Publish|Publishes], NSession);
        {ignore, NSession} ->
            process_msgs(Msgs, Publishes, NSession)
    end.

%% Enqueue message if the client has been disconnected
%% process_msg(Msg, Session = #session{conn_pid = undefined}) ->
%%    {ignore, enqueue_msg(Msg, Session)};

%% Prepare the qos0 message delivery
process_msg(Msg = #message{qos = ?QOS_0}, Session) ->
    {ok, {publish, undefined, Msg}, Session};

process_msg(Msg = #message{qos = QoS},
            Session = #session{next_pkt_id = PacketId, inflight = Inflight})
    when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->
    case emqx_inflight:is_full(Inflight) of
        true ->
            {ignore, enqueue_msg(Msg, Session)};
        false ->
            Publish = {publish, PacketId, Msg},
            NSession = await(PacketId, Msg, Session),
            {ok, Publish, next_pkt_id(NSession)}
    end.

enqueue_msg(Msg, Session = #session{mqueue = Q, client_id = ClientId, username = Username}) ->
    emqx_pd:update_counter(enqueue_stats, 1),
    {Dropped, NewQ} = emqx_mqueue:in(Msg, Q),
    if
        Dropped =/= undefined ->
            SessProps = #{client_id => ClientId, username => Username},
            ok = emqx_hooks:run('message.dropped', [SessProps, Dropped]);
        true -> ok
    end,
    Session#session{mqueue = NewQ}.

%%------------------------------------------------------------------------------
%% Deliver
%%------------------------------------------------------------------------------

redeliver({PacketId, Msg = #message{qos = QoS}}, Session) when QoS =/= ?QOS_0 ->
    Msg1 = emqx_message:set_flag(dup, Msg),
    do_deliver(PacketId, Msg1, Session);

redeliver({pubrel, PacketId}, #session{deliver_fun = DeliverFun}) ->
    DeliverFun({pubrel, PacketId}).

do_deliver(PacketId, Msg, #session{deliver_fun = DeliverFun}) ->
    emqx_pd:update_counter(deliver_stats, 1),
    DeliverFun({publish, PacketId, Msg}).

batch_deliver(Publishes, #session{deliver_fun = DeliverFun}) ->
    emqx_pd:update_counter(deliver_stats, length(Publishes)),
    DeliverFun(Publishes).

%%------------------------------------------------------------------------------
%% Dequeue
%%------------------------------------------------------------------------------

dequeue(Session = #session{inflight = Inflight, mqueue = Q}) ->
    case emqx_mqueue:is_empty(Q)
         orelse emqx_inflight:is_full(Inflight) of
        true -> Session;
        false ->
            %% TODO:
            Msgs = [],
            Q1 = Q,
            %% {Msgs, Q1} = drain_q(batch_n(Inflight), [], Q),
            batch_process(lists:reverse(Msgs), Session#session{mqueue = Q1})
    end.

drain_q(Cnt, Msgs, Q) when Cnt =< 0 ->
    {Msgs, Q};

drain_q(Cnt, Msgs, Q) ->
    case emqx_mqueue:out(Q) of
        {empty, _Q} -> {Msgs, Q};
        {{value, Msg}, Q1} ->
            drain_q(Cnt-1, [Msg|Msgs], Q1)
    end.

%%------------------------------------------------------------------------------
%% Ensure timers

ensure_await_rel_timer(Session = #session{await_rel_timeout = Timeout,
                                          await_rel_timer = undefined}) ->
    ensure_await_rel_timer(Timeout, Session);
ensure_await_rel_timer(Session) ->
    Session.

ensure_await_rel_timer(Timeout, Session = #session{await_rel_timer = undefined}) ->
    Session#session{await_rel_timer = emqx_misc:start_timer(Timeout, check_awaiting_rel)};
ensure_await_rel_timer(_Timeout, Session) ->
    Session.

ensure_retry_timer(Session = #session{retry_interval = Interval, retry_timer = undefined}) ->
    ensure_retry_timer(Interval, Session);
ensure_retry_timer(Session) ->
    Session.

ensure_retry_timer(Interval, Session = #session{retry_timer = undefined}) ->
    Session#session{retry_timer = emqx_misc:start_timer(Interval, retry_delivery)};
ensure_retry_timer(_Timeout, Session) ->
    Session.

ensure_expire_timer(Session = #session{expiry_interval = Interval})
  when Interval > 0 andalso Interval =/= 16#ffffffff ->
    Session#session{expiry_timer = emqx_misc:start_timer(Interval * 1000, expired)};
ensure_expire_timer(Session) ->
    Session.

ensure_will_delay_timer(Session = #session{will_msg = #message{headers = #{'Will-Delay-Interval' := WillDelayInterval}}}) ->
    Session#session{will_delay_timer = emqx_misc:start_timer(WillDelayInterval * 1000, will_delay)};
ensure_will_delay_timer(Session = #session{will_msg = WillMsg}) ->
    send_willmsg(WillMsg),
    Session#session{will_msg = undefined}.

%%--------------------------------------------------------------------
%% Next Packet Id

next_pkt_id(Session = #session{next_pkt_id = 16#FFFF}) ->
    Session#session{next_pkt_id = 1};

next_pkt_id(Session = #session{next_pkt_id = Id}) ->
    Session#session{next_pkt_id = Id + 1}.

