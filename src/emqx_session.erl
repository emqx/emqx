%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([init/2]).

-export([ info/1
        , info/2
        , stats/1
        ]).

-export([ subscribe/4
        , unsubscribe/4
        ]).

-export([ publish/3
        , puback/3
        , pubrec/2
        , pubrel/2
        , pubcomp/3
        ]).

-export([ deliver/3
        , enqueue/3
        , dequeue/2
        , retry/2
        , terminate/3
        ]).

-export([ takeover/1
        , resume/2
        , replay/2
        , upgrade/2
        , downgrade/1
        ]).

-export([expire/2]).

%% Export for CT
-export([set_field/3]).

-export_type([session/0]).

-import(emqx_zone, [get_env/3]).

-record(session, {
          %% Client's id
          clientid :: emqx_types:clientid(),
          %% Client’s Subscriptions.
          subscriptions :: map(),
          %% Max subscriptions allowed
          max_subscriptions :: non_neg_integer(),
          %% Upgrade QoS?
          upgrade_qos :: boolean(),
          %% Client <- Broker: QoS1/2 messages sent to the client but
          %% have not been unacked.
          inflight :: emqx_inflight:inflight(),
          %% All QoS1/2 messages published to when client is disconnected,
          %% or QoS1/2 messages pending transmission to the Client.
          %%
          %% Optionally, QoS0 messages pending transmission to the Client.
          mqueue :: emqx_mqueue:mqueue(),
          %% Next packet id of the session
          next_pkt_id = 1 :: emqx_types:packet_id(),
          %% Retry interval for redelivering QoS1/2 messages (Unit: millsecond)
          retry_interval :: timeout(),
          %% Client -> Broker: QoS2 messages received from the client, but
          %% have not been completely acknowledged
          awaiting_rel :: map(),
          %% Maximum number of awaiting QoS2 messages allowed
          max_awaiting_rel :: non_neg_integer(),
          %% Awaiting PUBREL Timeout (Unit: millsecond)
          await_rel_timeout :: timeout(),
          %% Created at
          created_at :: pos_integer(),

          extras :: map()
          }).

%% in the previous code, we will replace the message record with the pubrel atom
%% in the pubrec function, this will lose the creation time of the message,
-record(pubrel_await, {message :: emqx_types:message()}).

-type(session() :: #session{}).

-type(publish() :: {maybe(emqx_types:packet_id()), emqx_types:message()}).

-type(pubrel() :: {pubrel, emqx_types:packet_id()}).

-type(replies() :: list(publish() | pubrel())).

-define(INFO_KEYS, [subscriptions,
                    upgrade_qos,
                    retry_interval,
                    await_rel_timeout,
                    created_at
                   ]).

-define(STATS_KEYS, [subscriptions_cnt,
                     subscriptions_max,
                     inflight_cnt,
                     inflight_max,
                     mqueue_len,
                     mqueue_max,
                     mqueue_dropped,
                     next_pkt_id,
                     awaiting_rel_cnt,
                     awaiting_rel_max
                    ]).

-define(DEFAULT_BATCH_N, 1000).

-ifdef(TEST).
-define(GET_CLIENT_ID(C), maps:get(clientid, C, <<>>)).
-export([dummy/0]).
-else.
-define(GET_CLIENT_ID(C), maps:get(clientid, C)).
-endif.

%%--------------------------------------------------------------------
%% Init a Session
%%--------------------------------------------------------------------

-spec(init(emqx_types:clientinfo(), emqx_types:conninfo()) -> session()).
init(#{zone := Zone} = CInfo, #{receive_maximum := MaxInflight}) ->
    #session{clientid          = ?GET_CLIENT_ID(CInfo),
             max_subscriptions = get_env(Zone, max_subscriptions, 0),
             subscriptions     = #{},
             upgrade_qos       = get_env(Zone, upgrade_qos, false),
             inflight          = emqx_inflight:new(MaxInflight),
             mqueue            = init_mqueue(Zone),
             next_pkt_id       = 1,
             retry_interval    = timer:seconds(get_env(Zone, retry_interval, 0)),
             awaiting_rel      = #{},
             max_awaiting_rel  = get_env(Zone, max_awaiting_rel, 100),
             await_rel_timeout = timer:seconds(get_env(Zone, await_rel_timeout, 300)),
             created_at        = erlang:system_time(millisecond),
             extras            = #{}
            }.

%% @private init mq
init_mqueue(Zone) ->
    emqx_mqueue:init(#{max_len => get_env(Zone, max_mqueue_len, 1000),
                       store_qos0 => get_env(Zone, mqueue_store_qos0, true),
                       priorities => get_env(Zone, mqueue_priorities, none),
                       default_priority => get_env(Zone, mqueue_default_priority, lowest)
                      }).

%% @doc uprade from 4.3
upgrade(CInfo, S) ->
    [session | Fields] = tuple_to_list(S),
    #session{} = list_to_tuple([session, ?GET_CLIENT_ID(CInfo) | Fields] ++ [#{}]).

%% @doc Downgrade to 4.3
downgrade(#session{} = S) ->
    [session, _ClientID | Fields] = tuple_to_list(S),
    list_to_tuple([session | lists:reverse(tl(lists:reverse(Fields)))]).

-ifdef(TEST).
dummy() ->
    #session{}.
-endif.
%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------

%% @doc Get infos of the session.
-spec(info(session()) -> emqx_types:infos()).
info(Session) ->
    maps:from_list(info(?INFO_KEYS, Session)).

info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(subscriptions, #session{subscriptions = Subs}) ->
    Subs;
info(subscriptions_cnt, #session{subscriptions = Subs}) ->
    maps:size(Subs);
info(subscriptions_max, #session{max_subscriptions = MaxSubs}) ->
    MaxSubs;
info(upgrade_qos, #session{upgrade_qos = UpgradeQoS}) ->
    UpgradeQoS;
info(inflight, #session{inflight = Inflight}) ->
    Inflight;
info(inflight_cnt, #session{inflight = Inflight}) ->
    emqx_inflight:size(Inflight);
info(inflight_max, #session{inflight = Inflight}) ->
    emqx_inflight:max_size(Inflight);
info(retry_interval, #session{retry_interval = Interval}) ->
    Interval div 1000;
info(mqueue, #session{mqueue = MQueue}) ->
    MQueue;
info(mqueue_len, #session{mqueue = MQueue}) ->
    emqx_mqueue:len(MQueue);
info(mqueue_max, #session{mqueue = MQueue}) ->
    emqx_mqueue:max_len(MQueue);
info(mqueue_dropped, #session{mqueue = MQueue}) ->
    emqx_mqueue:dropped(MQueue);
info(next_pkt_id, #session{next_pkt_id = PacketId}) ->
    PacketId;
info(awaiting_rel, #session{awaiting_rel = AwaitingRel}) ->
    AwaitingRel;
info(awaiting_rel_cnt, #session{awaiting_rel = AwaitingRel}) ->
    maps:size(AwaitingRel);
info(awaiting_rel_max, #session{max_awaiting_rel = Max}) ->
    Max;
info(await_rel_timeout, #session{await_rel_timeout = Timeout}) ->
    Timeout div 1000;
info(created_at, #session{created_at = CreatedAt}) ->
    CreatedAt.

%% @doc Get stats of the session.
-spec(stats(session()) -> emqx_types:stats()).
stats(Session) -> info(?STATS_KEYS, Session).

%%--------------------------------------------------------------------
%% Client -> Broker: SUBSCRIBE
%%--------------------------------------------------------------------

-spec(subscribe(emqx_types:clientinfo(), emqx_types:topic(),
                emqx_types:subopts(), session())
      -> {ok, session()} | {error, emqx_types:reason_code()}).
subscribe(ClientInfo = #{clientid := ClientId}, TopicFilter, SubOpts,
          Session = #session{subscriptions = Subs}) ->
    IsNew = not maps:is_key(TopicFilter, Subs),
    case IsNew andalso is_subscriptions_full(Session) of
        false ->
            ok = emqx_broker:subscribe(TopicFilter, ClientId, SubOpts),
            ok = emqx_hooks:run('session.subscribed',
                                [ClientInfo, TopicFilter, SubOpts#{is_new => IsNew}]),
            {ok, Session#session{subscriptions = maps:put(TopicFilter, SubOpts, Subs)}};
        true -> {error, ?RC_QUOTA_EXCEEDED}
    end.

-compile({inline, [is_subscriptions_full/1]}).
is_subscriptions_full(#session{max_subscriptions = 0}) ->
    false;
is_subscriptions_full(#session{subscriptions = Subs,
                               max_subscriptions = MaxLimit}) ->
    maps:size(Subs) >= MaxLimit.

%%--------------------------------------------------------------------
%% Client -> Broker: UNSUBSCRIBE
%%--------------------------------------------------------------------

-spec(unsubscribe(emqx_types:clientinfo(), emqx_types:topic(), emqx_types:subopts(), session())
      -> {ok, session()} | {error, emqx_types:reason_code()}).
unsubscribe(ClientInfo, TopicFilter, UnSubOpts, Session = #session{subscriptions = Subs}) ->
    case maps:find(TopicFilter, Subs) of
        {ok, SubOpts} ->
            ok = emqx_broker:unsubscribe(TopicFilter),
            ok = emqx_hooks:run('session.unsubscribed',
                                [ClientInfo, TopicFilter, maps:merge(SubOpts, UnSubOpts)]),
            {ok, Session#session{subscriptions = maps:remove(TopicFilter, Subs)}};
        error ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED}
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBLISH
%%--------------------------------------------------------------------

-spec(publish(emqx_types:packet_id(), emqx_types:message(), session())
      -> {ok, emqx_types:publish_result(), session()}
       | {error, emqx_types:reason_code()}).
publish(PacketId, Msg = #message{qos = ?QOS_2, timestamp = Ts},
        Session = #session{awaiting_rel = AwaitingRel}) ->
    case is_awaiting_full(Session) of
        false ->
            case maps:is_key(PacketId, AwaitingRel) of
                false ->
                    Results = emqx_broker:publish(Msg),
                    AwaitingRel1 = maps:put(PacketId, Ts, AwaitingRel),
                    {ok, Results, Session#session{awaiting_rel = AwaitingRel1}};
                true ->
                    ?LOG(warning, "Dropped the qos2 packet ~w "
                         "due to packet ID in use.", [PacketId]),
                    drop_qos2_msg(Msg, ?RC_PACKET_IDENTIFIER_IN_USE)
            end;
        true ->
            ?LOG(warning, "Dropped the qos2 packet ~w "
                 "due to awaiting_rel is full.", [PacketId]),
            drop_qos2_msg(Msg, ?RC_RECEIVE_MAXIMUM_EXCEEDED)
    end;

%% Publish QoS0/1 directly
publish(_PacketId, Msg, Session) ->
    {ok, emqx_broker:publish(Msg), Session}.

drop_qos2_msg(Msg, ReasonCode) ->
    ok = emqx_metrics:inc('messages.dropped'),
    ok = emqx_hooks:run('message.dropped', [Msg, #{node => node()}, emqx_reason_codes:name(ReasonCode)]),
    {error, ReasonCode}.

-compile({inline, [is_awaiting_full/1]}).
is_awaiting_full(#session{max_awaiting_rel = 0}) ->
    false;
is_awaiting_full(#session{awaiting_rel = AwaitingRel,
                          max_awaiting_rel = MaxLimit}) ->
    maps:size(AwaitingRel) >= MaxLimit.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBACK
%%--------------------------------------------------------------------

-spec(puback(emqx_types:clientinfo(), emqx_types:packet_id(), session())
      -> {ok, emqx_types:message(), session()}
       | {ok, emqx_types:message(), replies(), session()}
       | {error, emqx_types:reason_code()}).
puback(ClientInfo, PacketId, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {Msg, _Ts}} when is_record(Msg, message) ->
            on_delivery_completed(ClientInfo, Msg, Session),
            Inflight1 = emqx_inflight:delete(PacketId, Inflight),
            return_with(Msg, dequeue(ClientInfo, Session#session{inflight = Inflight1}));
        {value, _Other} ->
            {error, ?RC_PACKET_IDENTIFIER_IN_USE};
        none ->
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

-compile({inline, [return_with/2]}).
return_with(Msg, {ok, Session}) ->
    {ok, Msg, Session};
return_with(Msg, {ok, Publishes, Session}) ->
    {ok, Msg, Publishes, Session}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREC
%%--------------------------------------------------------------------

-spec(pubrec(emqx_types:packet_id(), session())
      -> {ok, emqx_types:message(), session()}
       | {error, emqx_types:reason_code()}).
pubrec(PacketId, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {Msg, _Ts}} when is_record(Msg, message) ->
            Update = with_ts(#pubrel_await{message = Msg}),
            Inflight1 = emqx_inflight:update(PacketId, Update, Inflight),
            {ok, Msg, Session#session{inflight = Inflight1}};
        {value, _Other} ->
            {error, ?RC_PACKET_IDENTIFIER_IN_USE};
        none ->
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREL
%%--------------------------------------------------------------------

-spec(pubrel(emqx_types:packet_id(), session())
      -> {ok, session()} | {error, emqx_types:reason_code()}).
pubrel(PacketId, Session = #session{awaiting_rel = AwaitingRel}) ->
    case maps:take(PacketId, AwaitingRel) of
        {_Ts, AwaitingRel1} ->
            {ok, Session#session{awaiting_rel = AwaitingRel1}};
        error ->
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBCOMP
%%--------------------------------------------------------------------

-spec(pubcomp(emqx_types:clientinfo(), emqx_types:packet_id(), session())
      -> {ok, session()} | {ok, replies(), session()}
       | {error, emqx_types:reason_code()}).
pubcomp(ClientInfo, PacketId, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {#pubrel_await{message = Msg}, _Ts}} ->
            on_delivery_completed(ClientInfo, Msg, Session),
            Inflight1 = emqx_inflight:delete(PacketId, Inflight),
            dequeue(ClientInfo, Session#session{inflight = Inflight1});
        {value, _Other} ->
            {error, ?RC_PACKET_IDENTIFIER_IN_USE};
        none ->
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Dequeue Msgs
%%--------------------------------------------------------------------

dequeue(ClientInfo, Session = #session{inflight = Inflight, mqueue = Q}) ->
    case emqx_mqueue:is_empty(Q) of
        true  -> {ok, Session};
        false ->
            {Msgs, Q1} = dequeue(ClientInfo, batch_n(Inflight), [], Q),
            do_deliver(ClientInfo, Msgs, [], Session#session{mqueue = Q1})
    end.

dequeue(_ClientInfo, 0, Msgs, Q) ->
    {lists:reverse(Msgs), Q};

dequeue(ClientInfo, Cnt, Msgs, Q) ->
    case emqx_mqueue:out(Q) of
        {empty, _Q} -> dequeue(ClientInfo, 0, Msgs, Q);
        {{value, Msg}, Q1} ->
            case emqx_message:is_expired(Msg) of
                true ->
                    ok = inc_delivery_expired_cnt(),
                    ok = emqx_hooks:run('delivery.dropped', [ClientInfo, Msg, expired]),
                    dequeue(ClientInfo, Cnt, Msgs, Q1);
                false -> dequeue(ClientInfo, acc_cnt(Msg, Cnt), [Msg | Msgs], Q1)
            end
    end.

-compile({inline, [acc_cnt/2]}).
acc_cnt(#message{qos = ?QOS_0}, Cnt) -> Cnt;
acc_cnt(_Msg, Cnt) -> Cnt - 1.

%%--------------------------------------------------------------------
%% Broker -> Client: Deliver
%%--------------------------------------------------------------------

-spec(deliver(emqx_types:clientinfo(), list(emqx_types:deliver()), session())
      -> {ok, session()} | {ok, replies(), session()}).
deliver(ClientInfo, [Deliver], Session) -> %% Optimize
    Msg = enrich_delivers(Deliver, Session),
    deliver_msg(ClientInfo, Msg, Session);

deliver(ClientInfo, Delivers, Session) ->
    Msgs = [enrich_delivers(D, Session) || D <- Delivers],
    do_deliver(ClientInfo, Msgs, [], Session).

do_deliver(_ClientInfo, [], Publishes, Session) ->
    {ok, lists:reverse(Publishes), Session};

do_deliver(ClientInfo, [Msg | More], Acc, Session) ->
    case deliver_msg(ClientInfo, Msg, Session) of
        {ok, Session1} ->
            do_deliver(ClientInfo, More, Acc, Session1);
        {ok, [Publish], Session1} ->
            do_deliver(ClientInfo, More, [Publish | Acc], Session1)
    end.

deliver_msg(ClientInfo, Msg = #message{qos = ?QOS_0}, Session) ->
    on_delivery_completed(ClientInfo, Msg, Session),
    {ok, [{undefined, maybe_ack(Msg)}], Session};

deliver_msg(ClientInfo, Msg = #message{qos = QoS}, Session =
                #session{next_pkt_id = PacketId, inflight = Inflight})
  when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->
    case emqx_inflight:is_full(Inflight) of
        true ->
            Session1 = case maybe_nack(Msg) of
                           drop -> Session;
                           store -> enqueue(ClientInfo, Msg, Session)
                       end,
            {ok, Session1};
        false ->
            %% Note that we publish message without shared ack header
            %% But add to inflight with ack headers
            %% This ack header is required for redispatch-on-terminate feature to work
            Publish = {PacketId, maybe_ack(Msg)},
            Msg2 = mark_begin_deliver(Msg),
            Inflight1 = emqx_inflight:insert(PacketId, with_ts(Msg2), Inflight),
            {ok, [Publish], next_pkt_id(Session#session{inflight = Inflight1})}
    end.

-spec(enqueue(emqx_types:clientinfo(), list(emqx_types:deliver()) | emqx_types:message(),
              session()) -> session()).
enqueue(ClientInfo, Delivers, Session) when is_list(Delivers) ->
    Msgs = [enrich_delivers(D, Session) || D <- Delivers],
    lists:foldl(fun(Msg, Session0) ->
            enqueue(ClientInfo, Msg, Session0)
        end, Session, Msgs);

enqueue(ClientInfo, #message{} = Msg, Session = #session{mqueue = Q}) ->
    {Dropped, NewQ} = emqx_mqueue:in(Msg, Q),
    (Dropped =/= undefined) andalso log_dropped(ClientInfo, Dropped, Session),
    Session#session{mqueue = NewQ}.

log_dropped(ClientInfo, Msg = #message{qos = QoS}, #session{mqueue = Q}) ->
    case (QoS == ?QOS_0) andalso (not emqx_mqueue:info(store_qos0, Q)) of
        true  ->
            ok = emqx_hooks:run('delivery.dropped', [ClientInfo, Msg, qos0_msg]),
            ok = emqx_metrics:inc('delivery.dropped'),
            ok = emqx_metrics:inc('delivery.dropped.qos0_msg'),
            ok = inc_pd('send_msg.dropped'),
            ?LOG(warning, "Dropped qos0 msg: ~s", [emqx_message:format(Msg)]);
        false ->
            ok = emqx_hooks:run('delivery.dropped', [ClientInfo, Msg, queue_full]),
            ok = emqx_metrics:inc('delivery.dropped'),
            ok = emqx_metrics:inc('delivery.dropped.queue_full'),
            ok = inc_pd('send_msg.dropped'),
            ok = inc_pd('send_msg.dropped.queue_full'),
            ?LOG(warning, "Dropped msg due to mqueue is full: ~s",
                 [emqx_message:format(Msg)])
    end.

enrich_delivers({deliver, Topic, Msg}, Session = #session{subscriptions = Subs}) ->
    enrich_subopts(get_subopts(Topic, Subs), Msg, Session).

maybe_ack(Msg) ->
    emqx_shared_sub:maybe_ack(Msg).

maybe_nack(Msg) ->
    emqx_shared_sub:maybe_nack_dropped(Msg).

get_subopts(Topic, SubMap) ->
    case maps:find(Topic, SubMap) of
        {ok, #{nl := Nl, qos := QoS, rap := Rap, subid := SubId}} ->
            [{nl, Nl}, {qos, QoS}, {rap, Rap}, {subid, SubId}];
        {ok, #{nl := Nl, qos := QoS, rap := Rap}} ->
            [{nl, Nl}, {qos, QoS}, {rap, Rap}];
        error -> []
    end.

enrich_subopts([], Msg, _Session) -> Msg;
enrich_subopts([{nl, 1} | Opts], Msg, Session) ->
    enrich_subopts(Opts, emqx_message:set_flag(nl, Msg), Session);
enrich_subopts([{nl, 0} | Opts], Msg, Session) ->
    enrich_subopts(Opts, Msg, Session);
enrich_subopts([{qos, SubQoS} | Opts], Msg = #message{qos = PubQoS},
               Session = #session{upgrade_qos = true}) ->
    enrich_subopts(Opts, Msg#message{qos = max(SubQoS, PubQoS)}, Session);
enrich_subopts([{qos, SubQoS} | Opts], Msg = #message{qos = PubQoS},
               Session = #session{upgrade_qos = false}) ->
    enrich_subopts(Opts, Msg#message{qos = min(SubQoS, PubQoS)}, Session);
enrich_subopts([{rap, 1} | Opts], Msg, Session) ->
    enrich_subopts(Opts, Msg, Session);
enrich_subopts([{rap, 0} | Opts], Msg = #message{headers = #{retained := true}}, Session) ->
    enrich_subopts(Opts, Msg, Session);
enrich_subopts([{rap, 0} | Opts], Msg, Session) ->
    enrich_subopts(Opts, emqx_message:set_flag(retain, false, Msg), Session);
enrich_subopts([{subid, SubId} | Opts], Msg, Session) ->
    Props = emqx_message:get_header(properties, Msg, #{}),
    Msg1 = emqx_message:set_header(properties, Props#{'Subscription-Identifier' => SubId}, Msg),
    enrich_subopts(Opts, Msg1, Session).

%%--------------------------------------------------------------------
%% Retry Delivery
%%--------------------------------------------------------------------

-spec(retry(emqx_types:clientinfo(), session())
    -> {ok, session()}
     | {ok, replies(), timeout(), session()}).
retry(ClientInfo, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:is_empty(Inflight) of
        true  -> {ok, Session};
        false ->
            Now = erlang:system_time(millisecond),
            retry_delivery(emqx_inflight:to_list(sort_fun(), Inflight),
                           [], Now, Session, ClientInfo)
    end.

retry_delivery([], Acc, _Now, Session = #session{retry_interval = Interval}, _ClientInfo) ->
    {ok, lists:reverse(Acc), Interval, Session};

retry_delivery([{PacketId, {Msg, Ts}} | More], Acc, Now, Session =
               #session{retry_interval = Interval, inflight = Inflight}, ClientInfo) ->
    case (Age = age(Now, Ts)) >= Interval of
        true ->
            {Acc1, Inflight1} = do_retry_delivery(PacketId, Msg, Now, Acc, Inflight, ClientInfo),
            retry_delivery(More, Acc1, Now, Session#session{inflight = Inflight1}, ClientInfo);
        false ->
            {ok, lists:reverse(Acc), Interval - max(0, Age), Session}
    end.

do_retry_delivery(PacketId, Msg, Now, Acc, Inflight, ClientInfo)
        when is_record(Msg, message) ->
    case emqx_message:is_expired(Msg) of
        true ->
            ok = emqx_hooks:run('delivery.dropped', [ClientInfo, Msg, expired]),
            ok = inc_delivery_expired_cnt(),
            {Acc, emqx_inflight:delete(PacketId, Inflight)};
        false ->
            Msg1 = emqx_message:set_flag(dup, true, Msg),
            Inflight1 = emqx_inflight:update(PacketId, {Msg1, Now}, Inflight),
            {[{PacketId, Msg1} | Acc], Inflight1}
    end;

do_retry_delivery(PacketId, Pubrel, Now, Acc, Inflight, _) ->
    Inflight1 = emqx_inflight:update(PacketId, {Pubrel, Now}, Inflight),
    {[{pubrel, PacketId} | Acc], Inflight1}.

%%--------------------------------------------------------------------
%% Expire Awaiting Rel
%%--------------------------------------------------------------------

-spec(expire(awaiting_rel, session()) -> {ok, session()} | {ok, timeout(), session()}).
expire(awaiting_rel, Session = #session{awaiting_rel = AwaitingRel}) ->
    case maps:size(AwaitingRel) of
        0 -> {ok, Session};
        _ -> expire_awaiting_rel(erlang:system_time(millisecond), Session)
    end.

expire_awaiting_rel(Now, Session = #session{awaiting_rel = AwaitingRel,
                                            await_rel_timeout = Timeout}) ->
    NotExpired = fun(_PacketId, Ts) -> age(Now, Ts) < Timeout end,
    AwaitingRel1 = maps:filter(NotExpired, AwaitingRel),
    ExpiredCnt = maps:size(AwaitingRel) - maps:size(AwaitingRel1),
    (ExpiredCnt > 0) andalso inc_await_pubrel_timeout(ExpiredCnt),
    NSession = Session#session{awaiting_rel = AwaitingRel1},
    case maps:size(AwaitingRel1) of
        0 -> {ok, NSession};
        _ -> {ok, Timeout, NSession}
    end.

%%--------------------------------------------------------------------
%% Takeover, Resume and Replay
%%--------------------------------------------------------------------

-spec(takeover(session()) -> ok).
takeover(#session{subscriptions = Subs}) ->
    lists:foreach(fun emqx_broker:unsubscribe/1, maps:keys(Subs)).

-spec(resume(emqx_types:clientinfo(), session()) -> ok).
resume(ClientInfo = #{clientid := ClientId}, Session = #session{subscriptions = Subs}) ->
    lists:foreach(fun({TopicFilter, SubOpts}) ->
                      ok = emqx_broker:subscribe(TopicFilter, ClientId, SubOpts)
                  end, maps:to_list(Subs)),
    ok = emqx_metrics:inc('session.resumed'),
    emqx_hooks:run('session.resumed', [ClientInfo, info(Session)]).

-spec(replay(emqx_types:clientinfo(), session()) -> {ok, replies(), session()}).
replay(ClientInfo, Session = #session{inflight = Inflight}) ->
    Pubs = lists:map(fun({PacketId, {Pubrel,  _Msg}}) when is_record(Pubrel, pubrel_await) ->
                         {pubrel, PacketId};
                        ({PacketId, {Msg, _Ts}}) ->
                         {PacketId, emqx_message:set_flag(dup, true, Msg)}
                     end, emqx_inflight:to_list(Inflight)),
    case dequeue(ClientInfo, Session) of
        {ok, NSession} -> {ok, Pubs, NSession};
        {ok, More, NSession} ->
            {ok, lists:append(Pubs, More), NSession}
    end.

-spec(terminate(emqx_types:clientinfo(), Reason :: term(), session()) -> ok).
terminate(ClientInfo, {shutdown, Reason}, Session) ->
    terminate(ClientInfo, Reason, Session);
terminate(ClientInfo, Reason, Session) ->
    run_terminate_hooks(ClientInfo, Reason, Session),
    maybe_redispatch_shared_messages(Reason, Session),
    ok.

run_terminate_hooks(ClientInfo, discarded, Session) ->
    run_hook('session.discarded', [ClientInfo, info(Session)]);
run_terminate_hooks(ClientInfo, takeovered, Session) ->
    run_hook('session.takeovered', [ClientInfo, info(Session)]);
run_terminate_hooks(ClientInfo, Reason, Session) ->
    run_hook('session.terminated', [ClientInfo, Reason, info(Session)]).

maybe_redispatch_shared_messages(takeovered, _Session) ->
    ok;
maybe_redispatch_shared_messages(kicked, _Session) ->
    ok;
maybe_redispatch_shared_messages(_Reason, Session) ->
    redispatch_shared_messages(Session).

redispatch_shared_messages(#session{inflight = Inflight, mqueue = Q}) ->
    AllInflights = emqx_inflight:to_list(sort_fun(), Inflight),
    F = fun({_, {Msg, _Ts}}) ->
            case Msg of
                #message{qos = ?QOS_1} ->
                    %% For QoS 2, here is what the spec says:
                    %% If the Client's Session terminates before the Client reconnects,
                    %% the Server MUST NOT send the Application Message to any other
                    %% subscribed Client [MQTT-4.8.2-5].
                    {true, Msg};
                _ ->
                    %% QoS 2, after pubrec is received
                    %% the inflight record is updated to an atom
                    false
            end
        end,
    InflightList = lists:filtermap(F, AllInflights),
    MqList = mqueue_to_list(Q, []),
    emqx_shared_sub:redispatch(InflightList ++ MqList).

%% convert mqueue to a list
%% the messages at the head of the list is to be dispatched before the tail
mqueue_to_list(Q, Acc) ->
    case emqx_mqueue:out(Q) of
        {empty, _Q} ->
            lists:reverse(Acc);
        {{value, Msg}, Q1} ->
            mqueue_to_list(Q1, [Msg | Acc])
    end.

-compile({inline, [run_hook/2]}).
run_hook(Name, Args) ->
    ok = emqx_metrics:inc(Name),
    emqx_hooks:run(Name, Args).

%%--------------------------------------------------------------------
%% Inc message/delivery expired counter
%%--------------------------------------------------------------------
inc_delivery_expired_cnt() ->
    inc_delivery_expired_cnt(1).

inc_delivery_expired_cnt(N) ->
    ok = inc_pd('send_msg.dropped', N),
    ok = inc_pd('send_msg.dropped.expired', N),
    ok = emqx_metrics:inc('delivery.dropped', N),
    emqx_metrics:inc('delivery.dropped.expired', N).

inc_await_pubrel_timeout(N) ->
    ok = inc_pd('recv_msg.dropped', N),
    ok = inc_pd('recv_msg.dropped.await_pubrel_timeout', N),
    ok = emqx_metrics:inc('messages.dropped', N),
    emqx_metrics:inc('messages.dropped.await_pubrel_timeout', N).

inc_pd(Key) ->
    inc_pd(Key, 1).
inc_pd(Key, Inc) ->
    _ = emqx_pd:inc_counter(Key, Inc),
    ok.

%%--------------------------------------------------------------------
%% Next Packet Id
%%--------------------------------------------------------------------

-compile({inline, [next_pkt_id/1]}).

next_pkt_id(Session = #session{next_pkt_id = ?MAX_PACKET_ID}) ->
    Session#session{next_pkt_id = 1};

next_pkt_id(Session = #session{next_pkt_id = Id}) ->
    Session#session{next_pkt_id = Id + 1}.

%%--------------------------------------------------------------------
%% Message Latency Stats
%%--------------------------------------------------------------------
on_delivery_completed(ClientInfo,
                      Msg,
                      #session{created_at = CreateAt}) when is_record(Msg, message) ->
    emqx:run_hook('delivery.completed',
                  [ClientInfo, Msg, #{session_birth_time => CreateAt}]);

%% Hot upgrade compatibility clause.
%% In the 4.4.0, timestamp are stored in pubrel_await, not a message record.
%% This clause should be kept in all 4.4.x versions.
on_delivery_completed(_ClientInfo, _Ts, _Session) ->
    ok.

mark_begin_deliver(Msg) ->
    emqx_message:set_header(deliver_begin_at, erlang:system_time(millisecond), Msg).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

-compile({inline, [sort_fun/0, batch_n/1, with_ts/1, age/2]}).

sort_fun() ->
    fun({_, {_, Ts1}}, {_, {_, Ts2}}) -> Ts1 =< Ts2 end.

batch_n(Inflight) ->
    case emqx_inflight:max_size(Inflight) of
        0 -> ?DEFAULT_BATCH_N;
        Sz -> Sz - emqx_inflight:size(Inflight)
    end.

with_ts(Msg) ->
    {Msg, erlang:system_time(millisecond)}.

age(Now, Ts) -> Now - Ts.

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

set_field(Name, Value, Session) ->
    Pos = emqx_misc:index_of(Name, record_info(fields, session)),
    setelement(Pos+1, Session, Value).
