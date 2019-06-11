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

-module(emqx_session).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-export([start_link/1]).

-export([ info/1
        , attrs/1
        , stats/1
        ]).

-export([ resume/2
        , discard/2
        , update_expiry_interval/2
        ]).

-export([ subscribe/2
        , subscribe/4
        , unsubscribe/2
        , unsubscribe/4
        , publish/3
        , puback/2
        , puback/3
        , pubrec/2
        , pubrec/3
        , pubrel/3
        , pubcomp/3
        ]).

-export([close/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-import(emqx_zone, [get_env/2, get_env/3]).

-record(state, {
          %% zone
          zone :: atom(),

          %% Idle timeout
          idle_timeout :: pos_integer(),

          %% Clean Start Flag
          clean_start = false :: boolean(),

          %% Conn Binding: local | remote
          %% binding = local :: local | remote,

          %% Deliver fun
          deliver_fun :: function(),

          %% ClientId: Identifier of Session
          client_id :: binary(),

          %% Username
          username :: maybe(binary()),

          %% Connection pid binding with session
          conn_pid :: pid(),

          %% Old Connection Pid that has been kickout
          old_conn_pid :: pid(),

          %% Next packet id of the session
          next_pkt_id = 1 :: emqx_mqtt_types:packet_id(),

          %% Client’s Subscriptions.
          subscriptions :: map(),

          %% Client <- Broker: Inflight QoS1, QoS2 messages sent to the client but unacked.
          inflight :: emqx_inflight:inflight(),

          %% Max Inflight Size. DEPRECATED: Get from inflight
          %% max_inflight = 32 :: non_neg_integer(),

          %% Retry Timer
          retry_timer :: maybe(reference()),

          %% All QoS1, QoS2 messages published to when client is disconnected.
          %% QoS 1 and QoS 2 messages pending transmission to the Client.
          %%
          %% Optionally, QoS 0 messages pending transmission to the Client.
          mqueue :: emqx_mqueue:mqueue(),

          %% Client -> Broker: Inflight QoS2 messages received from client and waiting for pubrel.
          awaiting_rel :: map(),

          %% Awaiting PUBREL Timer
          await_rel_timer :: maybe(reference()),

          %% Session Expiry Interval
          expiry_interval = 7200 :: timeout(),

          %% Expired Timer
          expiry_timer :: maybe(reference()),

          %% Stats timer
          stats_timer :: maybe(reference()),

          %% GC State
          gc_state,

          %% Created at
          created_at :: erlang:timestamp(),

          will_msg :: emqx:message(),

          will_delay_timer :: maybe(reference())

         }).

-type(spid() :: pid()).
-type(attr() :: {atom(), term()}).

-export_type([attr/0]).

-define(DEFAULT_BATCH_N, 1000).

%% @doc Start a session proc.
-spec(start_link(SessAttrs :: map()) -> {ok, pid()}).
start_link(SessAttrs) ->
    proc_lib:start_link(?MODULE, init, [[self(), SessAttrs]]).

%% @doc Get session info
-spec(info(spid() | #state{}) -> list({atom(), term()})).
info(SPid) when is_pid(SPid) ->
    gen_server:call(SPid, info, infinity);

info(State = #state{zone = Zone,
                    conn_pid = ConnPid,
                    next_pkt_id = PktId,
                    subscriptions = Subscriptions,
                    inflight = Inflight,
                    mqueue = MQueue,
                    awaiting_rel = AwaitingRel}) ->
    attrs(State) ++ [{conn_pid, ConnPid},
                     {next_pkt_id, PktId},
                     {max_subscriptions, get_env(Zone, max_subscriptions, 0)},
                     {subscriptions, Subscriptions},
                     {upgrade_qos, get_env(Zone, upgrade_qos, false)},
                     {inflight, Inflight},
                     {retry_interval, get_env(Zone, retry_interval, 0)},
                     {mqueue_len, emqx_mqueue:len(MQueue)},
                     {awaiting_rel, AwaitingRel},
                     {max_awaiting_rel, get_env(Zone, max_awaiting_rel)},
                     {await_rel_timeout, get_env(Zone, await_rel_timeout)}].

%% @doc Get session attrs
-spec(attrs(spid() | #state{}) -> list({atom(), term()})).
attrs(SPid) when is_pid(SPid) ->
    gen_server:call(SPid, attrs, infinity);

attrs(#state{clean_start = CleanStart,
             client_id = ClientId,
             conn_pid  = ConnPid,
             username = Username,
             expiry_interval = ExpiryInterval,
             created_at = CreatedAt}) ->
    [{clean_start, CleanStart},
     {binding, binding(ConnPid)},
     {client_id, ClientId},
     {username, Username},
     {expiry_interval, ExpiryInterval div 1000},
     {created_at, CreatedAt}].

-spec(stats(spid() | #state{}) -> list({atom(), non_neg_integer()})).
stats(SPid) when is_pid(SPid) ->
    gen_server:call(SPid, stats, infinity);

stats(#state{zone = Zone,
             subscriptions = Subscriptions,
             inflight = Inflight,
             mqueue = MQueue,
             awaiting_rel = AwaitingRel}) ->
    lists:append(emqx_misc:proc_stats(),
                 [{max_subscriptions, get_env(Zone, max_subscriptions, 0)},
                  {subscriptions_count, maps:size(Subscriptions)},
                  {max_inflight, emqx_inflight:max_size(Inflight)},
                  {inflight_len, emqx_inflight:size(Inflight)},
                  {max_mqueue, emqx_mqueue:max_len(MQueue)},
                  {mqueue_len, emqx_mqueue:len(MQueue)},
                  {mqueue_dropped, emqx_mqueue:dropped(MQueue)},
                  {max_awaiting_rel, get_env(Zone, max_awaiting_rel)},
                  {awaiting_rel_len, maps:size(AwaitingRel)},
                  {deliver_msg, emqx_pd:get_counter(deliver_stats)},
                  {enqueue_msg, emqx_pd:get_counter(enqueue_stats)}]).

%%------------------------------------------------------------------------------
%% PubSub API
%%------------------------------------------------------------------------------

-spec(subscribe(spid(), list({emqx_topic:topic(), emqx_types:subopts()})) -> ok).
subscribe(SPid, RawTopicFilters) when is_list(RawTopicFilters) ->
    TopicFilters = [emqx_topic:parse(RawTopic, maps:merge(?DEFAULT_SUBOPTS, SubOpts))
                    || {RawTopic, SubOpts} <- RawTopicFilters],
    subscribe(SPid, undefined, #{}, TopicFilters).

-spec(subscribe(spid(), emqx_mqtt_types:packet_id(),
                emqx_mqtt_types:properties(), emqx_mqtt_types:topic_filters()) -> ok).
subscribe(SPid, PacketId, Properties, TopicFilters) ->
    SubReq = {PacketId, Properties, TopicFilters},
    gen_server:cast(SPid, {subscribe, self(), SubReq}).

%% @doc Called by connection processes when publishing messages
-spec(publish(spid(), emqx_mqtt_types:packet_id(), emqx_types:message())
      -> {ok, emqx_types:deliver_results()} | {error, term()}).
publish(_SPid, _PacketId, Msg = #message{qos = ?QOS_0}) ->
    %% Publish QoS0 message directly
    {ok, emqx_broker:publish(Msg)};

publish(_SPid, _PacketId, Msg = #message{qos = ?QOS_1}) ->
    %% Publish QoS1 message directly
    {ok, emqx_broker:publish(Msg)};

publish(SPid, PacketId, Msg = #message{qos = ?QOS_2, timestamp = Ts}) ->
    %% Register QoS2 message packet ID (and timestamp) to session, then publish
    case gen_server:call(SPid, {register_publish_packet_id, PacketId, Ts}, infinity) of
        ok -> {ok, emqx_broker:publish(Msg)};
        {error, Reason} -> {error, Reason}
    end.

-spec(puback(spid(), emqx_mqtt_types:packet_id()) -> ok).
puback(SPid, PacketId) ->
    gen_server:cast(SPid, {puback, PacketId, ?RC_SUCCESS}).

-spec(puback(spid(), emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code()) -> ok).
puback(SPid, PacketId, ReasonCode) ->
    gen_server:cast(SPid, {puback, PacketId, ReasonCode}).

-spec(pubrec(spid(), emqx_mqtt_types:packet_id()) -> ok | {error, emqx_mqtt_types:reason_code()}).
pubrec(SPid, PacketId) ->
    pubrec(SPid, PacketId, ?RC_SUCCESS).

-spec(pubrec(spid(), emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code())
      -> ok | {error, emqx_mqtt_types:reason_code()}).
pubrec(SPid, PacketId, ReasonCode) ->
    gen_server:call(SPid, {pubrec, PacketId, ReasonCode}, infinity).

-spec(pubrel(spid(), emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code())
      -> ok | {error, emqx_mqtt_types:reason_code()}).
pubrel(SPid, PacketId, ReasonCode) ->
    gen_server:call(SPid, {pubrel, PacketId, ReasonCode}, infinity).

-spec(pubcomp(spid(), emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code()) -> ok).
pubcomp(SPid, PacketId, ReasonCode) ->
    gen_server:cast(SPid, {pubcomp, PacketId, ReasonCode}).

-spec(unsubscribe(spid(), emqx_types:topic_table()) -> ok).
unsubscribe(SPid, RawTopicFilters) when is_list(RawTopicFilters) ->
   TopicFilters = lists:map(fun({RawTopic, Opts}) ->
                                    emqx_topic:parse(RawTopic, Opts);
                               (RawTopic) when is_binary(RawTopic) ->
                                    emqx_topic:parse(RawTopic)
                            end, RawTopicFilters),
    unsubscribe(SPid, undefined, #{}, TopicFilters).

-spec(unsubscribe(spid(), emqx_mqtt_types:packet_id(),
                  emqx_mqtt_types:properties(), emqx_mqtt_types:topic_filters()) -> ok).
unsubscribe(SPid, PacketId, Properties, TopicFilters) ->
    UnsubReq = {PacketId, Properties, TopicFilters},
    gen_server:cast(SPid, {unsubscribe, self(), UnsubReq}).

-spec(resume(spid(), map()) -> ok).
resume(SPid, SessAttrs) ->
    gen_server:cast(SPid, {resume, SessAttrs}).

%% @doc Discard the session
-spec(discard(spid(), ByPid :: pid()) -> ok).
discard(SPid, ByPid) ->
    gen_server:call(SPid, {discard, ByPid}, infinity).

-spec(update_expiry_interval(spid(), timeout()) -> ok).
update_expiry_interval(SPid, Interval) ->
    gen_server:cast(SPid, {update_expiry_interval, Interval}).

-spec(close(spid()) -> ok).
close(SPid) ->
    gen_server:call(SPid, close, infinity).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Parent, #{zone            := Zone,
                client_id       := ClientId,
                username        := Username,
                conn_pid        := ConnPid,
                clean_start     := CleanStart,
                expiry_interval := ExpiryInterval,
                max_inflight    := MaxInflight,
                will_msg        := WillMsg}]) ->
    process_flag(trap_exit, true),
    true = link(ConnPid),
    emqx_logger:set_metadata_client_id(ClientId),
    GcPolicy = emqx_zone:get_env(Zone, force_gc_policy, false),
    IdleTimout = get_env(Zone, idle_timeout, 30000),
    State = #state{zone              = Zone,
                   idle_timeout      = IdleTimout,
                   clean_start       = CleanStart,
                   deliver_fun       = deliver_fun(ConnPid),
                   client_id         = ClientId,
                   username          = Username,
                   conn_pid          = ConnPid,
                   subscriptions     = #{},
                   inflight          = emqx_inflight:new(MaxInflight),
                   mqueue            = init_mqueue(Zone),
                   awaiting_rel      = #{},
                   expiry_interval   = ExpiryInterval,
                   gc_state          = emqx_gc:init(GcPolicy),
                   created_at        = os:timestamp(),
                   will_msg          = WillMsg
                  },
    ok = emqx_sm:register_session(ClientId, self()),
    true = emqx_sm:set_session_attrs(ClientId, attrs(State)),
    true = emqx_sm:set_session_stats(ClientId, stats(State)),
    ok = emqx_hooks:run('session.created', [#{client_id => ClientId}, info(State)]),
    ok = emqx_misc:init_proc_mng_policy(Zone),
    ok = proc_lib:init_ack(Parent, {ok, self()}),
    gen_server:enter_loop(?MODULE, [{hibernate_after, IdleTimout}], State).

init_mqueue(Zone) ->
    emqx_mqueue:init(#{max_len => get_env(Zone, max_mqueue_len, 1000),
                       store_qos0 => get_env(Zone, mqueue_store_qos0, true),
                       priorities => get_env(Zone, mqueue_priorities),
                       default_priority => get_env(Zone, mqueue_default_priority)
                      }).

binding(undefined) -> undefined;
binding(ConnPid) ->
    case node(ConnPid) =:= node() of true -> local; false -> remote end.

deliver_fun(ConnPid) when node(ConnPid) == node() ->
    fun(Packet) -> ConnPid ! {deliver, Packet}, ok end;
deliver_fun(ConnPid) ->
    Node = node(ConnPid),
    fun(Packet) ->
        true = emqx_rpc:cast(Node, erlang, send, [ConnPid, {deliver, Packet}]), ok
    end.

handle_call(info, _From, State) ->
    reply(info(State), State);

handle_call(attrs, _From, State) ->
    reply(attrs(State), State);

handle_call(stats, _From, State) ->
    reply(stats(State), State);

handle_call({discard, ByPid}, _From, State = #state{conn_pid = undefined}) ->
    ?LOG(warning, "[Session] Discarded by ~p", [ByPid]),
    {stop, {shutdown, discarded}, ok, State};

handle_call({discard, ByPid}, _From, State = #state{client_id = ClientId, conn_pid = ConnPid}) ->
    ?LOG(warning, "[Session] Conn ~p is discarded by ~p", [ConnPid, ByPid]),
    ConnPid ! {shutdown, discard, {ClientId, ByPid}},
    {stop, {shutdown, discarded}, ok, State};

%% PUBLISH: This is only to register packetId to session state.
%% The actual message dispatching should be done by the caller (e.g. connection) process.
handle_call({register_publish_packet_id, PacketId, Ts}, _From,
            State = #state{zone = Zone, awaiting_rel = AwaitingRel}) ->
    MaxAwaitingRel = get_env(Zone, max_awaiting_rel),
    reply(
      case is_awaiting_full(MaxAwaitingRel, AwaitingRel) of
          false ->
              case maps:is_key(PacketId, AwaitingRel) of
                  true ->
                      {{error, ?RC_PACKET_IDENTIFIER_IN_USE}, State};
                  false ->
                      State1 = State#state{awaiting_rel = maps:put(PacketId, Ts, AwaitingRel)},
                      {ok, ensure_stats_timer(ensure_await_rel_timer(State1))}
              end;
          true ->
              ?LOG(warning, "[Session] Dropped qos2 packet ~w for too many awaiting_rel", [PacketId]),
              ok = emqx_metrics:inc('messages.qos2.dropped'),
              {{error, ?RC_RECEIVE_MAXIMUM_EXCEEDED}, State}
      end);

%% PUBREC:
handle_call({pubrec, PacketId, _ReasonCode}, _From, State = #state{inflight = Inflight}) ->
    reply(
      case emqx_inflight:contain(PacketId, Inflight) of
          true ->
              {ok, ensure_stats_timer(acked(pubrec, PacketId, State))};
          false ->
              ?LOG(warning, "[Session] The PUBREC PacketId ~w is not found.", [PacketId]),
              ok = emqx_metrics:inc('packets.pubrec.missed'),
              {{error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}, State}
      end);

%% PUBREL:
handle_call({pubrel, PacketId, _ReasonCode}, _From, State = #state{awaiting_rel = AwaitingRel}) ->
    reply(
      case maps:take(PacketId, AwaitingRel) of
          {_Ts, AwaitingRel1} ->
              {ok, ensure_stats_timer(State#state{awaiting_rel = AwaitingRel1})};
          error ->
              ?LOG(warning, "[Session] The PUBREL PacketId ~w is not found", [PacketId]),
              ok = emqx_metrics:inc('packets.pubrel.missed'),
              {{error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}, State}
      end);

handle_call(close, _From, State) ->
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "[Session] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

%% SUBSCRIBE:
handle_cast({subscribe, FromPid, {PacketId, _Properties, TopicFilters}},
            State = #state{client_id = ClientId, username = Username, subscriptions = Subscriptions}) ->
    {ReasonCodes, Subscriptions1} =
        lists:foldr(
            fun ({Topic, SubOpts = #{qos := QoS, rc := RC}}, {RcAcc, SubMap}) when
                      RC == ?QOS_0; RC == ?QOS_1; RC == ?QOS_2 ->
                    {[QoS|RcAcc], do_subscribe(ClientId, Username, Topic, SubOpts, SubMap)};
                ({_Topic, #{rc := RC}}, {RcAcc, SubMap}) ->
                    {[RC|RcAcc], SubMap}
            end, {[], Subscriptions}, TopicFilters),
    suback(FromPid, PacketId, ReasonCodes),
    noreply(ensure_stats_timer(State#state{subscriptions = Subscriptions1}));

%% UNSUBSCRIBE:
handle_cast({unsubscribe, From, {PacketId, _Properties, TopicFilters}},
            State = #state{client_id = ClientId, username = Username, subscriptions = Subscriptions}) ->
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
    unsuback(From, PacketId, ReasonCodes),
    noreply(ensure_stats_timer(State#state{subscriptions = Subscriptions1}));

%% PUBACK:
handle_cast({puback, PacketId, _ReasonCode}, State = #state{inflight = Inflight}) ->
    noreply(
      case emqx_inflight:contain(PacketId, Inflight) of
          true ->
              ensure_stats_timer(dequeue(acked(puback, PacketId, State)));
          false ->
              ?LOG(warning, "[Session] The PUBACK PacketId ~w is not found", [PacketId]),
              ok = emqx_metrics:inc('packets.puback.missed'),
              State
      end);

%% PUBCOMP:
handle_cast({pubcomp, PacketId, _ReasonCode}, State = #state{inflight = Inflight}) ->
    noreply(
      case emqx_inflight:contain(PacketId, Inflight) of
          true ->
              ensure_stats_timer(dequeue(acked(pubcomp, PacketId, State)));
          false ->
              ?LOG(warning, "[Session] The PUBCOMP PacketId ~w is not found", [PacketId]),
              ok = emqx_metrics:inc('packets.pubcomp.missed'),
              State
      end);

%% RESUME:
handle_cast({resume, #{conn_pid        := ConnPid,
                       will_msg        := WillMsg,
                       expiry_interval := ExpiryInterval,
                       max_inflight    := MaxInflight}},
            State = #state{client_id        = ClientId,
                           conn_pid         = OldConnPid,
                           clean_start      = CleanStart,
                           retry_timer      = RetryTimer,
                           await_rel_timer  = AwaitTimer,
                           expiry_timer     = ExpireTimer,
                           will_delay_timer = WillDelayTimer}) ->

    ?LOG(info, "[Session] Resumed by connection ~p ", [ConnPid]),

    %% Cancel Timers
    lists:foreach(fun emqx_misc:cancel_timer/1,
                  [RetryTimer, AwaitTimer, ExpireTimer, WillDelayTimer]),

    case kick(ClientId, OldConnPid, ConnPid) of
        ok -> ?LOG(warning, "[Session] Connection ~p kickout ~p", [ConnPid, OldConnPid]);
        ignore -> ok
    end,

    true = link(ConnPid),

    State1 = State#state{conn_pid         = ConnPid,
                         deliver_fun      = deliver_fun(ConnPid),
                         old_conn_pid     = OldConnPid,
                         clean_start      = false,
                         retry_timer      = undefined,
                         awaiting_rel     = #{},
                         await_rel_timer  = undefined,
                         expiry_timer     = undefined,
                         expiry_interval  = ExpiryInterval,
                         inflight         = emqx_inflight:update_size(MaxInflight, State#state.inflight),
                         will_delay_timer = undefined,
                         will_msg         = WillMsg},

    %% Clean Session: true -> false???
    CleanStart andalso emqx_sm:set_session_attrs(ClientId, attrs(State1)),

    ok = emqx_hooks:run('session.resumed', [#{client_id => ClientId}, attrs(State)]),

    %% Replay delivery and Dequeue pending messages
    noreply(ensure_stats_timer(dequeue(retry_delivery(true, State1))));

handle_cast({update_expiry_interval, Interval}, State) ->
    {noreply, State#state{expiry_interval = Interval}};

handle_cast(Msg, State) ->
    ?LOG(error, "[Session] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({dispatch, Topic, Msg}, State) when is_record(Msg, message) ->
    handle_dispatch([{Topic, Msg}], State);

handle_info({dispatch, Topic, Msgs}, State) when is_list(Msgs) ->
    handle_dispatch([{Topic, Msg} || Msg <- Msgs], State);

%% Do nothing if the client has been disconnected.
handle_info({timeout, Timer, retry_delivery}, State = #state{conn_pid = undefined, retry_timer = Timer}) ->
    noreply(State#state{retry_timer = undefined});

handle_info({timeout, Timer, retry_delivery}, State = #state{retry_timer = Timer}) ->
    noreply(retry_delivery(false, State#state{retry_timer = undefined}));

handle_info({timeout, Timer, check_awaiting_rel}, State = #state{await_rel_timer = Timer}) ->
    State1 = State#state{await_rel_timer = undefined},
    noreply(ensure_stats_timer(expire_awaiting_rel(State1)));

handle_info({timeout, Timer, emit_stats},
            State = #state{client_id = ClientId,
                           stats_timer = Timer,
                           gc_state = GcState}) ->
    _ = emqx_sm:set_session_stats(ClientId, stats(State)),
    NewState = State#state{stats_timer = undefined},
    Limits = erlang:get(force_shutdown_policy),
    case emqx_misc:conn_proc_mng_policy(Limits) of
        continue ->
            {noreply, NewState};
        hibernate ->
            %% going to hibernate, reset gc stats
            GcState1 = emqx_gc:reset(GcState),
            {noreply, NewState#state{gc_state = GcState1}, hibernate};
        {shutdown, Reason} ->
            ?LOG(warning, "[Session] Shutdown exceptionally due to ~p", [Reason]),
            shutdown(Reason, NewState)
    end;

handle_info({timeout, Timer, expired}, State = #state{expiry_timer = Timer}) ->
    ?LOG(info, "[Session] Expired, shutdown now.", []),
    shutdown(expired, State);

handle_info({timeout, Timer, will_delay}, State = #state{will_msg = WillMsg, will_delay_timer = Timer}) ->
    send_willmsg(WillMsg),
    {noreply, State#state{will_msg = undefined}};

%% ConnPid is shutting down by the supervisor.
handle_info({'EXIT', ConnPid, Reason}, #state{conn_pid = ConnPid})
    when Reason =:= killed; Reason =:= shutdown ->
    exit(Reason);

handle_info({'EXIT', ConnPid, Reason}, State = #state{will_msg = WillMsg, expiry_interval = 0, conn_pid = ConnPid}) ->
    case Reason of
        normal ->
            ignore;
        _ ->
            send_willmsg(WillMsg)
    end,
    {stop, Reason, State#state{will_msg = undefined, conn_pid = undefined}};

handle_info({'EXIT', ConnPid, Reason}, State = #state{conn_pid = ConnPid}) ->
    State1 = case Reason of
                 normal ->
                     State#state{will_msg = undefined};
                 _ ->
                     ensure_will_delay_timer(State)
             end,
    {noreply, ensure_expire_timer(State1#state{conn_pid = undefined})};

handle_info({'EXIT', OldPid, _Reason}, State = #state{old_conn_pid = OldPid}) ->
    %% ignore
    {noreply, State#state{old_conn_pid = undefined}};

handle_info({'EXIT', Pid, Reason}, State = #state{conn_pid = ConnPid}) ->
    ?LOG(error, "[Session] Unexpected EXIT: conn_pid=~p, exit_pid=~p, reason=~p",
         [ConnPid, Pid, Reason]),
    {noreply, State};

handle_info(Info, State) ->
    ?LOG(error, "[Session] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(Reason, #state{will_msg = WillMsg,
                         client_id = ClientId,
                         username = Username,
                         conn_pid = ConnPid,
                         old_conn_pid = OldConnPid}) ->
    send_willmsg(WillMsg),
    [maybe_shutdown(Pid, Reason) || Pid <- [ConnPid, OldConnPid]],
    ok = emqx_hooks:run('session.terminated', [#{client_id => ClientId, username => Username}, Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_shutdown(undefined, _Reason) ->
    ok;
maybe_shutdown(Pid, normal) ->
    Pid ! {shutdown, normal};
maybe_shutdown(Pid, Reason) ->
    exit(Pid, Reason).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

is_connection_alive(#state{conn_pid = Pid}) ->
    is_pid(Pid) andalso is_process_alive(Pid).

%%------------------------------------------------------------------------------
%% Suback and unsuback

suback(_From, undefined, _ReasonCodes) ->
    ignore;
suback(From, PacketId, ReasonCodes) ->
    From ! {deliver, {suback, PacketId, ReasonCodes}}.

unsuback(_From, undefined, _ReasonCodes) ->
    ignore;
unsuback(From, PacketId, ReasonCodes) ->
    From ! {deliver, {unsuback, PacketId, ReasonCodes}}.

%%------------------------------------------------------------------------------
%% Kickout old connection

kick(_ClientId, undefined, _ConnPid) ->
    ignore;
kick(_ClientId, ConnPid, ConnPid) ->
    ignore;
kick(ClientId, OldConnPid, ConnPid) ->
    unlink(OldConnPid),
    OldConnPid ! {shutdown, conflict, {ClientId, ConnPid}},
    %% Clean noproc
    receive {'EXIT', OldConnPid, _} -> ok after 1 -> ok end.

%%------------------------------------------------------------------------------
%% Replay or Retry Delivery

%% Redeliver at once if force is true
retry_delivery(Force, State = #state{inflight = Inflight}) ->
    case emqx_inflight:is_empty(Inflight) of
        true  -> State;
        false ->
            SortFun = fun({_, _, Ts1}, {_, _, Ts2}) -> Ts1 < Ts2 end,
            Msgs = lists:sort(SortFun, emqx_inflight:values(Inflight)),
            retry_delivery(Force, Msgs, os:timestamp(), State)
    end.

retry_delivery(_Force, [], _Now, State) ->
    %% Retry again...
    ensure_retry_timer(State);

retry_delivery(Force, [{Type, Msg0, Ts} | Msgs], Now,
               State = #state{zone = Zone, inflight = Inflight}) ->
    Interval = get_env(Zone, retry_interval, 0),
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
                                        redeliver({PacketId, Msg}, State),
                                        emqx_inflight:update(PacketId, {publish, {PacketId, Msg}, Now}, Inflight)
                                end;
                            {pubrel, PacketId} ->
                                redeliver({pubrel, PacketId}, State),
                                emqx_inflight:update(PacketId, {pubrel, PacketId, Now}, Inflight)
                        end,
            retry_delivery(Force, Msgs, Now, State#state{inflight = Inflight1});
        true ->
            ensure_retry_timer(Interval - max(0, Age), State)
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

expire_awaiting_rel(State = #state{awaiting_rel = AwaitingRel}) ->
    case maps:size(AwaitingRel) of
        0 -> State;
        _ -> expire_awaiting_rel(lists:keysort(2, maps:to_list(AwaitingRel)), os:timestamp(), State)
    end.

expire_awaiting_rel([], _Now, State) ->
    State#state{await_rel_timer = undefined};

expire_awaiting_rel([{PacketId, Ts} | More], Now,
                    State = #state{zone = Zone, awaiting_rel = AwaitingRel}) ->
    Timeout = get_env(Zone, await_rel_timeout),
    case (timer:now_diff(Now, Ts) div 1000) of
        Age when Age >= Timeout ->
            ok = emqx_metrics:inc('messages.qos2.expired'),
            ?LOG(warning, "[Session] Dropped qos2 packet ~s for await_rel_timeout", [PacketId]),
            expire_awaiting_rel(More, Now, State#state{awaiting_rel = maps:remove(PacketId, AwaitingRel)});
        Age ->
            ensure_await_rel_timer(Timeout - max(0, Age), State)
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

handle_dispatch(Msgs, State = #state{inflight = Inflight,
                                     client_id = ClientId,
                                     username = Username,
                                     subscriptions = SubMap}) ->
    SessProps = #{client_id => ClientId, username => Username},
    %% Drain the mailbox and batch deliver
    Msgs1 = drain_m(batch_n(Inflight), Msgs),
    %% Ack the messages for shared subscription
    Msgs2 = maybe_ack_shared(Msgs1, State),
    %% Process suboptions
    Msgs3 = lists:foldr(
              fun({Topic, Msg}, Acc) ->
                      SubOpts = find_subopts(Topic, SubMap),
                      case process_subopts(SubOpts, Msg, State) of
                          {ok, Msg1} -> [Msg1|Acc];
                          ignore ->
                              emqx_hooks:run('message.dropped', [SessProps, Msg]),
                              Acc
                      end
              end, [], Msgs2),
    NState = batch_process(Msgs3, State),
    noreply(ensure_stats_timer(NState)).

batch_n(Inflight) ->
    case emqx_inflight:max_size(Inflight) of
        0 -> ?DEFAULT_BATCH_N;
        Sz -> Sz - emqx_inflight:size(Inflight)
    end.

drain_m(Cnt, Msgs) when Cnt =< 0 ->
    lists:reverse(Msgs);
drain_m(Cnt, Msgs) ->
    receive
        {dispatch, Topic, Msg} when is_record(Msg, message)->
            drain_m(Cnt-1, [{Topic, Msg} | Msgs]);
        {dispatch, Topic, InMsgs} when is_list(InMsgs) ->
            Msgs1 = lists:foldl(
                      fun(Msg, Acc) ->
                              [{Topic, Msg} | Acc]
                      end, Msgs, InMsgs),
            drain_m(Cnt-length(InMsgs), Msgs1)
    after 0 ->
        lists:reverse(Msgs)
    end.

%% Ack or nack the messages of shared subscription?
maybe_ack_shared(Msgs, State) when is_list(Msgs) ->
    lists:foldr(
      fun({Topic, Msg}, Acc) ->
            case maybe_ack_shared(Msg, State) of
                ok -> Acc;
                Msg1 -> [{Topic, Msg1}|Acc]
            end
      end, [], Msgs);

maybe_ack_shared(Msg, State) ->
    case emqx_shared_sub:is_ack_required(Msg) of
        true -> do_ack_shared(Msg, State);
        false -> Msg
    end.

do_ack_shared(Msg, State = #state{inflight = Inflight}) ->
    case {is_connection_alive(State),
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

process_subopts([], Msg, _State) ->
    {ok, Msg};
process_subopts([{nl, 1}|_Opts], #message{from = ClientId}, #state{client_id = ClientId}) ->
    ignore;
process_subopts([{nl, _}|Opts], Msg, State) ->
    process_subopts(Opts, Msg, State);
process_subopts([{qos, SubQoS}|Opts], Msg = #message{qos = PubQoS}, State = #state{zone = Zone}) ->
    case get_env(Zone, upgrade_qos, false) of
        true -> process_subopts(Opts, Msg#message{qos = max(SubQoS, PubQoS)}, State);
        false -> process_subopts(Opts, Msg#message{qos = min(SubQoS, PubQoS)}, State)
    end;
process_subopts([{rap, _Rap}|Opts], Msg = #message{flags = Flags, headers = #{retained := true}}, State = #state{}) ->
    process_subopts(Opts, Msg#message{flags = maps:put(retain, true, Flags)}, State);
process_subopts([{rap, 0}|Opts], Msg = #message{flags = Flags}, State = #state{}) ->
    process_subopts(Opts, Msg#message{flags = maps:put(retain, false, Flags)}, State);
process_subopts([{rap, _}|Opts], Msg, State) ->
    process_subopts(Opts, Msg, State);
process_subopts([{subid, SubId}|Opts], Msg, State) ->
    process_subopts(Opts, emqx_message:set_header('Subscription-Identifier', SubId, Msg), State).

find_subopts(Topic, SubMap) ->
    case maps:find(Topic, SubMap) of
        {ok, #{nl := Nl, qos := QoS, rap := Rap, subid := SubId}} ->
            [{nl, Nl}, {qos, QoS}, {rap, Rap}, {subid, SubId}];
        {ok, #{nl := Nl, qos := QoS, rap := Rap}} ->
            [{nl, Nl}, {qos, QoS}, {rap, Rap}];
        error -> []
    end.

batch_process(Msgs, State) ->
    {ok, Publishes, NState} = process_msgs(Msgs, [], State),
    ok = batch_deliver(Publishes, NState),
    maybe_gc(msg_cnt(Msgs), NState).

process_msgs([], Publishes, State) ->
    {ok, lists:reverse(Publishes), State};

process_msgs([Msg|Msgs], Publishes, State) ->
    case process_msg(Msg, State) of
        {ok, Publish, NState} ->
            process_msgs(Msgs, [Publish|Publishes], NState);
        {ignore, NState} ->
            process_msgs(Msgs, Publishes, NState)
    end.

%% Enqueue message if the client has been disconnected
process_msg(Msg, State = #state{conn_pid = undefined}) ->
    {ignore, enqueue_msg(Msg, State)};

%% Prepare the qos0 message delivery
process_msg(Msg = #message{qos = ?QOS_0}, State) ->
    {ok, {publish, undefined, Msg}, State};

process_msg(Msg = #message{qos = QoS},
            State = #state{next_pkt_id = PacketId, inflight = Inflight})
    when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->
    case emqx_inflight:is_full(Inflight) of
        true ->
            {ignore, enqueue_msg(Msg, State)};
        false ->
            Publish = {publish, PacketId, Msg},
            NState = await(PacketId, Msg, State),
            {ok, Publish, next_pkt_id(NState)}
    end.

enqueue_msg(Msg, State = #state{mqueue = Q, client_id = ClientId, username = Username}) ->
    emqx_pd:update_counter(enqueue_stats, 1),
    {Dropped, NewQ} = emqx_mqueue:in(Msg, Q),
    if
        Dropped =/= undefined ->
            SessProps = #{client_id => ClientId, username => Username},
            ok = emqx_hooks:run('message.dropped', [SessProps, Dropped]);
        true -> ok
    end,
    State#state{mqueue = NewQ}.

%%------------------------------------------------------------------------------
%% Deliver
%%------------------------------------------------------------------------------

redeliver({PacketId, Msg = #message{qos = QoS}}, State) when QoS =/= ?QOS_0 ->
    Msg1 = emqx_message:set_flag(dup, Msg),
    do_deliver(PacketId, Msg1, State);

redeliver({pubrel, PacketId}, #state{deliver_fun = DeliverFun}) ->
    DeliverFun({pubrel, PacketId}).

do_deliver(PacketId, Msg, #state{deliver_fun = DeliverFun}) ->
    emqx_pd:update_counter(deliver_stats, 1),
    DeliverFun({publish, PacketId, Msg}).

batch_deliver(Publishes, #state{deliver_fun = DeliverFun}) ->
    emqx_pd:update_counter(deliver_stats, length(Publishes)),
    DeliverFun(Publishes).

%%------------------------------------------------------------------------------
%% Awaiting ACK for QoS1/QoS2 Messages
%%------------------------------------------------------------------------------

await(PacketId, Msg, State = #state{inflight = Inflight}) ->
    Inflight1 = emqx_inflight:insert(
                  PacketId, {publish, {PacketId, Msg}, os:timestamp()}, Inflight),
    ensure_retry_timer(State#state{inflight = Inflight1}).

acked(puback, PacketId, State = #state{client_id = ClientId, username = Username, inflight  = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, {_, Msg}, _Ts}} ->
            ok = emqx_hooks:run('message.acked', [#{client_id => ClientId, username => Username}, Msg]),
            State#state{inflight = emqx_inflight:delete(PacketId, Inflight)};
        none ->
            ?LOG(warning, "[Session] Duplicated PUBACK PacketId ~w", [PacketId]),
            State
    end;

acked(pubrec, PacketId, State = #state{client_id = ClientId, username = Username, inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, {_, Msg}, _Ts}} ->
            ok = emqx_hooks:run('message.acked', [#{client_id => ClientId, username => Username}, Msg]),
            State#state{inflight = emqx_inflight:update(PacketId, {pubrel, PacketId, os:timestamp()}, Inflight)};
        {value, {pubrel, PacketId, _Ts}} ->
            ?LOG(warning, "[Session] Duplicated PUBREC PacketId ~w", [PacketId]),
            State;
        none ->
            ?LOG(warning, "[Session] Unexpected PUBREC PacketId ~w", [PacketId]),
            State
    end;

acked(pubcomp, PacketId, State = #state{inflight = Inflight}) ->
    State#state{inflight = emqx_inflight:delete(PacketId, Inflight)}.

%%------------------------------------------------------------------------------
%% Dequeue
%%------------------------------------------------------------------------------

%% Do nothing if client is disconnected
dequeue(State = #state{conn_pid = undefined}) ->
    State;

dequeue(State = #state{inflight = Inflight, mqueue = Q}) ->
    case emqx_mqueue:is_empty(Q)
         orelse emqx_inflight:is_full(Inflight) of
        true -> State;
        false ->
            {Msgs, Q1} = drain_q(batch_n(Inflight), [], Q),
            batch_process(lists:reverse(Msgs), State#state{mqueue = Q1})
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

ensure_await_rel_timer(State = #state{zone = Zone,
                                      await_rel_timer = undefined}) ->
    Timeout = get_env(Zone, await_rel_timeout),
    ensure_await_rel_timer(Timeout, State);
ensure_await_rel_timer(State) ->
    State.

ensure_await_rel_timer(Timeout, State = #state{await_rel_timer = undefined}) ->
    State#state{await_rel_timer = emqx_misc:start_timer(Timeout, check_awaiting_rel)};
ensure_await_rel_timer(_Timeout, State) ->
    State.

ensure_retry_timer(State = #state{zone = Zone, retry_timer = undefined}) ->
    Interval = get_env(Zone, retry_interval, 0),
    ensure_retry_timer(Interval, State);
ensure_retry_timer(State) ->
    State.

ensure_retry_timer(Interval, State = #state{retry_timer = undefined}) ->
    State#state{retry_timer = emqx_misc:start_timer(Interval, retry_delivery)};
ensure_retry_timer(_Timeout, State) ->
    State.

ensure_expire_timer(State = #state{expiry_interval = Interval})
  when Interval > 0 andalso Interval =/= 16#ffffffff ->
    State#state{expiry_timer = emqx_misc:start_timer(Interval * 1000, expired)};
ensure_expire_timer(State) ->
    State.

ensure_will_delay_timer(State = #state{will_msg = #message{headers = #{'Will-Delay-Interval' := WillDelayInterval}}}) ->
    State#state{will_delay_timer = emqx_misc:start_timer(WillDelayInterval * 1000, will_delay)};
ensure_will_delay_timer(State = #state{will_msg = WillMsg}) ->
    send_willmsg(WillMsg),
    State#state{will_msg = undefined}.

ensure_stats_timer(State = #state{zone = Zone,
                                  stats_timer = undefined,
                                  idle_timeout = IdleTimeout}) ->
    case get_env(Zone, enable_stats, true) of
        true -> State#state{stats_timer = emqx_misc:start_timer(IdleTimeout, emit_stats)};
        _Other -> State
    end;
ensure_stats_timer(State) ->
    State.

%%------------------------------------------------------------------------------
%% Next Packet Id

next_pkt_id(State = #state{next_pkt_id = 16#FFFF}) ->
    State#state{next_pkt_id = 1};

next_pkt_id(State = #state{next_pkt_id = Id}) ->
    State#state{next_pkt_id = Id + 1}.

%%------------------------------------------------------------------------------
%% Maybe GC

msg_cnt(Msgs) ->
    lists:foldl(fun(Msg, {Cnt, Oct}) ->
                        {Cnt+1, Oct+msg_size(Msg)}
                end, {0, 0}, Msgs).

%% Take only the payload size into account, add other fields if necessary
msg_size(#message{payload = Payload}) -> payload_size(Payload).

%% Payload should be binary(), but not 100% sure. Need dialyzer!
payload_size(Payload) -> erlang:iolist_size(Payload).

maybe_gc(_, State = #state{gc_state = undefined}) ->
    State;
maybe_gc({Cnt, Oct}, State = #state{gc_state = GCSt}) ->
    {_, GCSt1} = emqx_gc:run(Cnt, Oct, GCSt),
    State#state{gc_state = GCSt1}.

%%------------------------------------------------------------------------------
%% Helper functions

reply({Reply, State}) ->
    reply(Reply, State).

reply(Reply, State) ->
    {reply, Reply, State}.

noreply(State) ->
    {noreply, State}.

shutdown(Reason, State) ->
    {stop, {shutdown, Reason}, State}.

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
