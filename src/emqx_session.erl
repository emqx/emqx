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

-export([start_link/1]).
-export([info/1, stats/1]).
-export([resume/2, discard/2]).
-export([subscribe/2, subscribe/4]).
-export([publish/3]).
-export([puback/2, puback/3]).
-export([pubrec/2, pubrec/3]).
-export([pubrel/3, pubcomp/3]).
-export([unsubscribe/2, unsubscribe/4]).
-export([close/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-import(emqx_zone, [get_env/2, get_env/3]).

-record(state, {
          %% Clean Start Flag
          clean_start = false :: boolean(),

          %% Client Binding: local | remote
          binding = local :: local | remote,

          %% ClientId: Identifier of Session
          client_id :: binary(),

          %% Username
          username :: binary() | undefined,

          %% Client pid binding with session
          client_pid :: pid(),

          %% Old client Pid that has been kickout
          old_client_pid :: pid(),

          %% Next packet id of the session
          next_pkt_id = 1 :: mqtt_packet_id(),

          %% Max subscriptions
          max_subscriptions :: non_neg_integer(),

          %% Client’s Subscriptions.
          subscriptions :: map(),

          %% Upgrade QoS?
          upgrade_qos = false :: boolean(),

          %% Client <- Broker: Inflight QoS1, QoS2 messages sent to the client but unacked.
          inflight :: emqx_inflight:inflight(),

          %% Max Inflight Size
          max_inflight = 32 :: non_neg_integer(),

          %% Retry interval for redelivering QoS1/2 messages
          retry_interval = 20000 :: timeout(),

          %% Retry Timer
          retry_timer :: reference() | undefined,

          %% All QoS1, QoS2 messages published to when client is disconnected.
          %% QoS 1 and QoS 2 messages pending transmission to the Client.
          %%
          %% Optionally, QoS 0 messages pending transmission to the Client.
          mqueue :: emqx_mqueue:mqueue(),

          %% Client -> Broker: Inflight QoS2 messages received from client and waiting for pubrel.
          awaiting_rel :: map(),

          %% Max Packets Awaiting PUBREL
          max_awaiting_rel = 100 :: non_neg_integer(),

          %% Awaiting PUBREL Timeout
          await_rel_timeout = 20000 :: timeout(),

          %% Awaiting PUBREL Timer
          await_rel_timer :: reference() | undefined,

          %% Session Expiry Interval
          expiry_interval = 7200000 :: timeout(),

          %% Expired Timer
          expiry_timer :: reference() | undefined,

          %% Enable Stats
          enable_stats :: boolean(),

          %% Stats timer
          stats_timer  :: reference() | undefined,

          %% TODO:
          deliver_stats = 0,

          %% TODO:
          enqueue_stats = 0,

          %% Created at
          created_at :: erlang:timestamp()
        }).

-define(TIMEOUT, 60000).

-define(INFO_KEYS, [clean_start, client_id, username, binding, client_pid, old_client_pid,
                    next_pkt_id, max_subscriptions, subscriptions, upgrade_qos, inflight,
                    max_inflight, retry_interval, mqueue, awaiting_rel, max_awaiting_rel,
                    await_rel_timeout, expiry_interval, enable_stats, created_at]).

-define(LOG(Level, Format, Args, State),
        emqx_logger:Level([{client, State#state.client_id}],
                          "Session(~s): " ++ Format, [State#state.client_id | Args])).

%% @doc Start a session
-spec(start_link(SessAttrs :: map()) -> {ok, pid()} | {error, term()}).
start_link(SessAttrs) ->
    IdleTimeout = maps:get(idle_timeout, SessAttrs, 30000),
    gen_server:start_link(?MODULE, SessAttrs, [{hibernate_after, IdleTimeout}]).

%%------------------------------------------------------------------------------
%% PubSub API
%%------------------------------------------------------------------------------

-spec(subscribe(pid(), list({topic(), map()}) |
                {mqtt_packet_id(), mqtt_properties(), topic_table()}) -> ok).
subscribe(SPid, RawTopicFilters) when is_list(RawTopicFilters) ->
    TopicFilters = [emqx_topic:parse(RawTopic, maps:merge(?DEFAULT_SUBOPTS, SubOpts))
                    || {RawTopic, SubOpts} <- RawTopicFilters],
    subscribe(SPid, undefined, #{}, TopicFilters).

%% for mqtt 5.0
subscribe(SPid, PacketId, Properties, TopicFilters) ->
    SubReq = {PacketId, Properties, TopicFilters},
    gen_server:cast(SPid, {subscribe, self(), SubReq}).

-spec(publish(pid(), mqtt_packet_id(), message()) -> {ok, emqx_types:dispatches()}).
publish(_SPid, _PacketId, Msg = #message{qos = ?QOS_0}) ->
    %% Publish QoS0 message to broker directly
    emqx_broker:publish(Msg);

publish(_SPid, _PacketId, Msg = #message{qos = ?QOS_1}) ->
    %% Publish QoS1 message to broker directly
    emqx_broker:publish(Msg);

publish(SPid, PacketId, Msg = #message{qos = ?QOS_2}) ->
    %% Publish QoS2 message to session
    gen_server:call(SPid, {publish, PacketId, Msg}, infinity).

-spec(puback(pid(), mqtt_packet_id()) -> ok).
puback(SPid, PacketId) ->
    gen_server:cast(SPid, {puback, PacketId, ?RC_SUCCESS}).

puback(SPid, PacketId, ReasonCode) ->
    gen_server:cast(SPid, {puback, PacketId, ReasonCode}).

-spec(pubrec(pid(), mqtt_packet_id()) -> ok | {error, mqtt_reason_code()}).
pubrec(SPid, PacketId) ->
    pubrec(SPid, PacketId, ?RC_SUCCESS).

-spec(pubrec(pid(), mqtt_packet_id(), mqtt_reason_code())
      -> ok | {error, mqtt_reason_code()}).
pubrec(SPid, PacketId, ReasonCode) ->
    gen_server:call(SPid, {pubrec, PacketId, ReasonCode}, infinity).

-spec(pubrel(pid(), mqtt_packet_id(), mqtt_reason_code())
      -> ok | {error, mqtt_reason_code()}).
pubrel(SPid, PacketId, ReasonCode) ->
    gen_server:call(SPid, {pubrel, PacketId, ReasonCode}, infinity).

-spec(pubcomp(pid(), mqtt_packet_id(), mqtt_reason_code()) -> ok).
pubcomp(SPid, PacketId, ReasonCode) ->
    gen_server:cast(SPid, {pubcomp, PacketId, ReasonCode}).

-spec(unsubscribe(pid(), topic_table()) -> ok).
unsubscribe(SPid, RawTopicFilters) when is_list(RawTopicFilters) ->
    unsubscribe(SPid, undefined, #{}, lists:map(fun emqx_topic:parse/1, RawTopicFilters)).

-spec(unsubscribe(pid(), mqtt_packet_id(), mqtt_properties(), topic_table()) -> ok).
unsubscribe(SPid, PacketId, Properties, TopicFilters) ->
    UnsubReq = {PacketId, Properties, TopicFilters},
    gen_server:cast(SPid, {unsubscribe, self(), UnsubReq}).

-spec(resume(pid(), pid()) -> ok).
resume(SPid, ClientPid) ->
    gen_server:cast(SPid, {resume, ClientPid}).

%% @doc Get session info
-spec(info(pid() | #state{}) -> list(tuple())).
info(SPid) when is_pid(SPid) ->
    gen_server:call(SPid, info);

info(#state{clean_start = CleanStart,
            binding = Binding,
            client_id = ClientId,
            username = Username,
            max_subscriptions = MaxSubscriptions,
            subscriptions = Subscriptions,
            upgrade_qos = UpgradeQoS,
            inflight = Inflight,
            max_inflight = MaxInflight,
            retry_interval = RetryInterval,
            mqueue = MQueue,
            awaiting_rel = AwaitingRel,
            max_awaiting_rel = MaxAwaitingRel,
            await_rel_timeout = AwaitRelTimeout,
            expiry_interval = ExpiryInterval,
            created_at = CreatedAt}) ->
    [{clean_start, CleanStart},
     {binding, Binding},
     {client_id, ClientId},
     {username, Username},
     {max_subscriptions, MaxSubscriptions},
     {subscriptions, maps:size(Subscriptions)},
     {upgrade_qos, UpgradeQoS},
     {inflight, emqx_inflight:size(Inflight)},
     {max_inflight, MaxInflight},
     {retry_interval, RetryInterval},
     {mqueue_len, emqx_mqueue:len(MQueue)},
     {awaiting_rel, maps:size(AwaitingRel)},
     {max_awaiting_rel, MaxAwaitingRel},
     {await_rel_timeout, AwaitRelTimeout},
     {expiry_interval, ExpiryInterval},
     {created_at, CreatedAt}].

-spec(stats(pid() | #state{}) -> list({atom(), non_neg_integer()})).
stats(SPid) when is_pid(SPid) ->
    gen_server:call(SPid, stats, infinity);

stats(#state{max_subscriptions = MaxSubscriptions,
              subscriptions     = Subscriptions,
              inflight          = Inflight,
              max_inflight      = MaxInflight,
              mqueue            = MQueue,
              max_awaiting_rel  = MaxAwaitingRel,
              awaiting_rel      = AwaitingRel,
              deliver_stats     = DeliverMsg,
              enqueue_stats     = EnqueueMsg}) ->
    lists:append(emqx_misc:proc_stats(),
                 [{max_subscriptions, MaxSubscriptions},
                  {subscriptions,     maps:size(Subscriptions)},
                  {max_inflight,      MaxInflight},
                  {inflight_len,      emqx_inflight:size(Inflight)},
                  {max_mqueue,        emqx_mqueue:max_len(MQueue)},
                  {mqueue_len,        emqx_mqueue:len(MQueue)},
                  {mqueue_dropped,    emqx_mqueue:dropped(MQueue)},
                  {max_awaiting_rel,  MaxAwaitingRel},
                  {awaiting_rel_len,  maps:size(AwaitingRel)},
                  {deliver_msg,       DeliverMsg},
                  {enqueue_msg,       EnqueueMsg}]).

%% @doc Discard the session
-spec(discard(pid(), client_id()) -> ok).
discard(SPid, ClientId) ->
    gen_server:call(SPid, {discard, ClientId}, infinity).

-spec(close(pid()) -> ok).
close(SPid) ->
    gen_server:call(SPid, close, infinity).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init(#{zone        := Zone,
       client_id   := ClientId,
       client_pid  := ClientPid,
       clean_start := CleanStart,
       username    := Username,
       %% TODO:
       conn_props  := _ConnProps}) ->
    process_flag(trap_exit, true),
    true = link(ClientPid),
    MaxInflight = get_env(Zone, max_inflight),
    State = #state{clean_start       = CleanStart,
                   binding           = binding(ClientPid),
                   client_id         = ClientId,
                   client_pid        = ClientPid,
                   username          = Username,
                   subscriptions     = #{},
                   max_subscriptions = get_env(Zone, max_subscriptions, 0),
                   upgrade_qos       = get_env(Zone, upgrade_qos, false),
                   max_inflight      = MaxInflight,
                   inflight          = emqx_inflight:new(MaxInflight),
                   mqueue            = init_mqueue(Zone, ClientId),
                   retry_interval    = get_env(Zone, retry_interval, 0),
                   awaiting_rel      = #{},
                   await_rel_timeout = get_env(Zone, await_rel_timeout),
                   max_awaiting_rel  = get_env(Zone, max_awaiting_rel),
                   expiry_interval   = get_env(Zone, session_expiry_interval),
                   enable_stats      = get_env(Zone, enable_stats, true),
                   deliver_stats      = 0,
                   enqueue_stats      = 0,
                   created_at        = os:timestamp()},
    emqx_sm:register_session(ClientId, info(State)),
    emqx_hooks:run('session.created', [ClientId]),
    {ok, ensure_stats_timer(State), hibernate}.

init_mqueue(Zone, ClientId) ->
    emqx_mqueue:new(ClientId, #{type => simple,
                                max_len => get_env(Zone, max_mqueue_len),
                                store_qos0 => get_env(Zone, mqueue_store_qos0)}).

binding(ClientPid) ->
    case node(ClientPid) =:= node() of true -> local; false -> remote end.

handle_call({discard, ClientPid}, _From, State = #state{client_pid = undefined}) ->
    ?LOG(warning, "Discarded by ~p", [ClientPid], State),
    {stop, {shutdown, discard}, ok, State};

handle_call({discard, ClientPid}, _From, State = #state{client_pid = OldClientPid}) ->
    ?LOG(warning, " ~p kickout ~p", [ClientPid, OldClientPid], State),
    {stop, {shutdown, conflict}, ok, State};

%% PUBLISH:
handle_call({publish, PacketId, Msg = #message{qos = ?QOS_2}}, _From,
            State = #state{awaiting_rel = AwaitingRel}) ->
    case is_awaiting_full(State) of
        false ->
            case maps:is_key(PacketId, AwaitingRel) of
                true ->
                    reply({error, ?RC_PACKET_IDENTIFIER_IN_USE}, State);
                false ->
                    State1 = State#state{awaiting_rel = maps:put(PacketId, Msg, AwaitingRel)},
                    reply(emqx_broker:publish(Msg), ensure_await_rel_timer(State1))
            end;
        true ->
            ?LOG(warning, "Dropped QoS2 Message for too many awaiting_rel: ~p", [Msg], State),
            emqx_metrics:inc('messages/qos2/dropped'),
            reply({error, ?RC_RECEIVE_MAXIMUM_EXCEEDED}, State)
    end;

%% PUBREC:
handle_call({pubrec, PacketId, _ReasonCode}, _From, State = #state{inflight = Inflight}) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            reply(ok, acked(pubrec, PacketId, State));
        false ->
            ?LOG(warning, "The PUBREC PacketId is not found: ~w", [PacketId], State),
            emqx_metrics:inc('packets/pubrec/missed'),
            reply({error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}, State)
    end;

%% PUBREL:
handle_call({pubrel, PacketId, _ReasonCode}, _From,
            State = #state{awaiting_rel = AwaitingRel}) ->
    case maps:take(PacketId, AwaitingRel) of
        {_, AwaitingRel1} ->
            reply(ok, State#state{awaiting_rel = AwaitingRel1});
        error ->
            ?LOG(warning, "Cannot find PUBREL: ~p", [PacketId], State),
            emqx_metrics:inc('packets/pubrel/missed'),
            reply({error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}, State)
    end;

handle_call(info, _From, State) ->
    reply(info(State), State);

handle_call(stats, _From, State) ->
    reply(stats(State), State);

handle_call(close, _From, State) ->
    {stop, normal, State};

handle_call(Req, _From, State) ->
    emqx_logger:error("[Session] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

%% SUBSCRIBE:
handle_cast({subscribe, FromPid, {PacketId, _Properties, TopicFilters}},
            State = #state{client_id = ClientId, subscriptions = Subscriptions}) ->
    {ReasonCodes, Subscriptions1} =
    lists:foldr(fun({Topic, SubOpts = #{qos := QoS}}, {RcAcc, SubMap}) ->
                    {[QoS|RcAcc], case maps:find(Topic, SubMap) of
                                      {ok, SubOpts} ->
                                          SubMap;
                                      {ok, _SubOpts} ->
                                          emqx_broker:set_subopts(Topic, {self(), ClientId}, SubOpts),
                                          emqx_hooks:run('session.subscribed', [ClientId, Topic, SubOpts]),
                                          maps:put(Topic, SubOpts, SubMap);
                                      error ->
                                          emqx_broker:subscribe(Topic, ClientId, SubOpts),
                                          emqx_hooks:run('session.subscribed', [ClientId, Topic, SubOpts]),
                                          maps:put(Topic, SubOpts, SubMap)
                                  end}
                end, {[], Subscriptions}, TopicFilters),
    suback(FromPid, PacketId, ReasonCodes),
    {noreply, State#state{subscriptions = Subscriptions1}};

%% UNSUBSCRIBE:
handle_cast({unsubscribe, From, {PacketId, _Properties, TopicFilters}},
            State = #state{client_id = ClientId, subscriptions = Subscriptions}) ->
    {ReasonCodes, Subscriptions1} =
    lists:foldr(fun({Topic, _SubOpts}, {Acc, SubMap}) ->
                        case maps:find(Topic, SubMap) of
                            {ok, SubOpts} ->
                                ok = emqx_broker:unsubscribe(Topic, ClientId),
                                emqx_hooks:run('session.unsubscribed', [ClientId, Topic, SubOpts]),
                                {[?RC_SUCCESS|Acc], maps:remove(Topic, SubMap)};
                            error ->
                                {[?RC_NO_SUBSCRIPTION_EXISTED|Acc], SubMap}
                        end
                end, {[], Subscriptions}, TopicFilters),
    unsuback(From, PacketId, ReasonCodes),
    {noreply, State#state{subscriptions = Subscriptions1}};

%% PUBACK:
handle_cast({puback, PacketId, _ReasonCode}, State = #state{inflight = Inflight}) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            {noreply, dequeue(acked(puback, PacketId, State))};
        false ->
            ?LOG(warning, "The PUBACK PacketId is not found: ~w", [PacketId], State),
            emqx_metrics:inc('packets/puback/missed'),
            {noreply, State}
    end;

%% PUBCOMP:
handle_cast({pubcomp, PacketId, _ReasonCode}, State = #state{inflight = Inflight}) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            {noreply, dequeue(acked(pubcomp, PacketId, State))};
        false ->
            ?LOG(warning, "The PUBCOMP PacketId is not found: ~w", [PacketId], State),
            emqx_metrics:inc('packets/pubcomp/missed'),
            {noreply, State}
    end;

%% RESUME:
handle_cast({resume, ClientPid},
            State = #state{client_id       = ClientId,
                           client_pid      = OldClientPid,
                           clean_start     = CleanStart,
                           retry_timer     = RetryTimer,
                           await_rel_timer = AwaitTimer,
                           expiry_timer    = ExpireTimer}) ->

    ?LOG(info, "Resumed by ~p ", [ClientPid], State),

    %% Cancel Timers
    lists:foreach(fun emqx_misc:cancel_timer/1,
                  [RetryTimer, AwaitTimer, ExpireTimer]),

    case kick(ClientId, OldClientPid, ClientPid) of
        ok -> ?LOG(warning, "~p kickout ~p", [ClientPid, OldClientPid], State);
        ignore -> ok
    end,

    true = link(ClientPid),

    State1 = State#state{client_pid      = ClientPid,
                         binding         = binding(ClientPid),
                         old_client_pid  = OldClientPid,
                         clean_start     = false,
                         retry_timer     = undefined,
                         awaiting_rel    = #{},
                         await_rel_timer = undefined,
                         expiry_timer    = undefined},

    %% Clean Session: true -> false?
    if
        CleanStart =:= true ->
            ?LOG(error, "CleanSess changed to false.", [], State1);
            %%TODO::
            %%emqx_sm:register_session(ClientId, info(State1));
        CleanStart =:= false ->
            ok
    end,

    %% Replay delivery and Dequeue pending messages
    {noreply, ensure_stats_timer(dequeue(retry_delivery(true, State1)))};

handle_cast(Msg, State) ->
    emqx_logger:error("[Session] unexpected cast: ~p", [Msg]),
    {noreply, State}.

%% Batch dispatch
handle_info({dispatch, Topic, Msgs}, State) when is_list(Msgs) ->
    {noreply, lists:foldl(fun(Msg, NewState) ->
                              element(2, handle_info({dispatch, Topic, Msg}, NewState))
                          end, State, Msgs)};

%% Dispatch message
handle_info({dispatch, Topic, Msg}, State = #state{subscriptions = SubMap}) when is_record(Msg, message) ->
    {noreply, case maps:find(Topic, SubMap) of
                  {ok, #{nl := Nl, qos := QoS, subid := SubId}} ->
                      run_dispatch_steps([{nl, Nl},{qos, QoS}, {subid, SubId}], Msg, State);
                  {ok, #{nl := Nl, qos := QoS}} ->
                      run_dispatch_steps([{nl, Nl},{qos, QoS}], Msg, State);
                  error ->
                      dispatch(reset_dup(Msg), State)
              end};

%% Do nothing if the client has been disconnected.
handle_info({timeout, _Timer, retry_delivery}, State = #state{client_pid = undefined}) ->
    {noreply, ensure_stats_timer(State#state{retry_timer = undefined})};

handle_info({timeout, _Timer, retry_delivery}, State) ->
    {noreply, ensure_stats_timer(retry_delivery(false, State#state{retry_timer = undefined}))};

handle_info({timeout, _Timer, check_awaiting_rel}, State) ->
    {noreply, ensure_stats_timer(expire_awaiting_rel(State#state{await_rel_timer = undefined}))};

handle_info({timeout, _Timer, expired}, State) ->
    ?LOG(info, "Expired, shutdown now.", [], State),
    shutdown(expired, State);

handle_info({'EXIT', ClientPid, _Reason},
            State = #state{clean_start= true, client_pid = ClientPid}) ->
    {stop, normal, State};

handle_info({'EXIT', ClientPid, Reason},
            State = #state{clean_start     = false,
                           client_pid      = ClientPid,
                           expiry_interval = Interval}) ->
    ?LOG(info, "Client ~p EXIT for ~p", [ClientPid, Reason], State),
    ExpireTimer = emqx_misc:start_timer(Interval, expired),
    State1 = State#state{client_pid = undefined, expiry_timer = ExpireTimer},
    {noreply, State1, hibernate};

handle_info({'EXIT', Pid, _Reason}, State = #state{old_client_pid = Pid}) ->
    %% ignore
    {noreply, State, hibernate};

handle_info({'EXIT', Pid, Reason}, State = #state{client_pid = ClientPid}) ->
    ?LOG(error, "unexpected EXIT: client_pid=~p, exit_pid=~p, reason=~p",
         [ClientPid, Pid, Reason], State),
    {noreply, State, hibernate};

handle_info(emit_stats, State = #state{client_id = ClientId}) ->
    emqx_sm:set_session_stats(ClientId, stats(State)),
    {noreply, State#state{stats_timer = undefined}, hibernate};

handle_info(Info, State) ->
    emqx_logger:error("[Session] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(Reason, #state{client_id = ClientId, username = Username}) ->
    emqx_hooks:run('session.terminated', [ClientId, Username, Reason]),
    emqx_sm:unregister_session(ClientId).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

suback(_From, undefined, _ReasonCodes) ->
    ignore;
suback(From, PacketId, ReasonCodes) ->
    From ! {deliver, {suback, PacketId, ReasonCodes}}.

unsuback(_From, undefined, _ReasonCodes) ->
    ignore;
unsuback(From, PacketId, ReasonCodes) ->
    From ! {deliver, {unsuback, PacketId, ReasonCodes}}.

%%------------------------------------------------------------------------------
%% Kickout old client

kick(_ClientId, undefined, _Pid) ->
    ignore;
kick(_ClientId, Pid, Pid) ->
    ignore;
kick(ClientId, OldPid, Pid) ->
    unlink(OldPid),
    OldPid ! {shutdown, conflict, {ClientId, Pid}},
    %% Clean noproc
    receive {'EXIT', OldPid, _} -> ok after 0 -> ok end.

%%------------------------------------------------------------------------------
%% Replay or Retry Delivery
%%------------------------------------------------------------------------------

%% Redeliver at once if force is true
retry_delivery(Force, State = #state{inflight = Inflight}) ->
    case emqx_inflight:is_empty(Inflight) of
        true  ->
            State;
        false ->
            Msgs = lists:sort(sortfun(inflight), emqx_inflight:values(Inflight)),
            retry_delivery(Force, Msgs, os:timestamp(), State)
    end.

retry_delivery(_Force, [], _Now, State = #state{retry_interval = Interval}) ->
    State#state{retry_timer = emqx_misc:start_timer(Interval, retry_delivery)};

retry_delivery(Force, [{Type, Msg0, Ts} | Msgs], Now,
               State = #state{inflight = Inflight, retry_interval = Interval}) ->
    Diff = timer:now_diff(Now, Ts) div 1000, %% micro -> ms
    if
        Force orelse (Diff >= Interval) ->
            case {Type, Msg0} of
                {publish, {PacketId, Msg}} ->
                    redeliver({PacketId, Msg}, State),
                    Inflight1 = emqx_inflight:update(PacketId, {publish, {PacketId, Msg}, Now}, Inflight),
                    retry_delivery(Force, Msgs, Now, State#state{inflight = Inflight1});
                {pubrel, PacketId} ->
                    redeliver({pubrel, PacketId}, State),
                    Inflight1 = emqx_inflight:update(PacketId, {pubrel, PacketId, Now}, Inflight),
                    retry_delivery(Force, Msgs, Now, State#state{inflight = Inflight1})
            end;
        true ->
            State#state{retry_timer = emqx_misc:start_timer(Interval - Diff, retry_delivery)}
    end.

%%------------------------------------------------------------------------------
%% Expire Awaiting Rel
%%------------------------------------------------------------------------------

expire_awaiting_rel(State = #state{awaiting_rel = AwaitingRel}) ->
    case maps:size(AwaitingRel) of
        0 -> State;
        _ -> Msgs = lists:sort(sortfun(awaiting_rel), maps:to_list(AwaitingRel)),
             expire_awaiting_rel(Msgs, os:timestamp(), State)
    end.

expire_awaiting_rel([], _Now, State) ->
    State#state{await_rel_timer = undefined};

expire_awaiting_rel([{PacketId, Msg = #message{timestamp = TS}} | Msgs],
                    Now, State = #state{awaiting_rel      = AwaitingRel,
                                        await_rel_timeout = Timeout}) ->
    case (timer:now_diff(Now, TS) div 1000) of
        Diff when Diff >= Timeout ->
            ?LOG(warning, "Dropped Qos2 Message for await_rel_timeout: ~p", [Msg], State),
            emqx_metrics:inc('messages/qos2/dropped'),
            expire_awaiting_rel(Msgs, Now, State#state{awaiting_rel = maps:remove(PacketId, AwaitingRel)});
        Diff ->
            State#state{await_rel_timer = emqx_misc:start_timer(Timeout - Diff, check_awaiting_rel)}
    end.

%%------------------------------------------------------------------------------
%% Sort Inflight, AwaitingRel
%%------------------------------------------------------------------------------

sortfun(inflight) ->
    fun({_, _, Ts1}, {_, _, Ts2}) -> Ts1 < Ts2 end;

sortfun(awaiting_rel) ->
    fun({_, #message{timestamp = Ts1}},
        {_, #message{timestamp = Ts2}}) ->
        Ts1 < Ts2
    end.

%%------------------------------------------------------------------------------
%% Check awaiting rel
%%------------------------------------------------------------------------------

is_awaiting_full(#state{max_awaiting_rel = 0}) ->
    false;
is_awaiting_full(#state{awaiting_rel = AwaitingRel, max_awaiting_rel = MaxLen}) ->
    maps:size(AwaitingRel) >= MaxLen.

%%------------------------------------------------------------------------------
%% Dispatch Messages
%%------------------------------------------------------------------------------

run_dispatch_steps([], Msg, State) ->
    dispatch(Msg, State);
run_dispatch_steps([{nl, 1}|_Steps], #message{from = ClientId}, State = #state{client_id = ClientId}) ->
    State;
run_dispatch_steps([{nl, 0}|Steps], Msg, State) ->
    run_dispatch_steps(Steps, Msg, State);
run_dispatch_steps([{qos, SubQoS}|Steps], Msg = #message{qos = PubQoS}, State = #state{upgrade_qos = false}) ->
    run_dispatch_steps(Steps, Msg#message{qos = min(SubQoS, PubQoS)}, State);
run_dispatch_steps([{qos, SubQoS}|Steps], Msg = #message{qos = PubQoS}, State = #state{upgrade_qos = true}) ->
    run_dispatch_steps(Steps, Msg#message{qos = max(SubQoS, PubQoS)}, State);
run_dispatch_steps([{subid, SubId}|Steps], Msg, State) ->
    run_dispatch_steps(Steps, emqx_message:set_header('Subscription-Identifier', SubId, Msg), State).

%% Enqueue message if the client has been disconnected
dispatch(Msg, State = #state{client_id = ClientId, client_pid = undefined}) ->
    case emqx_hooks:run('message.dropped', [ClientId, Msg]) of
        ok   -> enqueue_msg(Msg, State);
        stop -> State
    end;

%% Deliver qos0 message directly to client
dispatch(Msg = #message{qos = ?QOS0}, State) ->
    deliver(undefined, Msg, State),
    inc_stats(deliver, State);

dispatch(Msg = #message{qos = QoS}, State = #state{next_pkt_id = PacketId, inflight = Inflight})
    when QoS =:= ?QOS1 orelse QoS =:= ?QOS2 ->
    case emqx_inflight:is_full(Inflight) of
        true ->
            enqueue_msg(Msg, State);
        false ->
            deliver(PacketId, Msg, State),
            %% TODO inc_stats??
            await(PacketId, Msg, next_pkt_id(inc_stats(deliver, State)))
    end.

enqueue_msg(Msg, State = #state{mqueue = Q}) ->
    inc_stats(enqueue, State#state{mqueue = emqx_mqueue:in(Msg, Q)}).

%%------------------------------------------------------------------------------
%% Deliver
%%------------------------------------------------------------------------------

redeliver({PacketId, Msg = #message{qos = QoS}}, State) ->
    deliver(PacketId, if QoS =:= ?QOS2 -> Msg;
                         true -> emqx_message:set_flag(dup, Msg)
                      end, State);

redeliver({pubrel, PacketId}, #state{client_pid = Pid}) ->
    Pid ! {deliver, {pubrel, PacketId}}.

deliver(PacketId, Msg, #state{client_pid = Pid, binding = local}) ->
    Pid ! {deliver, {publish, PacketId, Msg}};
deliver(PacketId, Msg, #state{client_pid = Pid, binding = remote}) ->
    emqx_rpc:cast(node(Pid), erlang, send, [Pid, {deliver, PacketId, Msg}]).

%%------------------------------------------------------------------------------
%% Awaiting ACK for QoS1/QoS2 Messages
%%------------------------------------------------------------------------------

await(PacketId, Msg, State = #state{inflight       = Inflight,
                                    retry_timer    = RetryTimer,
                                    retry_interval = Interval}) ->
    %% Start retry timer if the Inflight is still empty
    State1 = case RetryTimer == undefined of
                 true  -> State#state{retry_timer = emqx_misc:start_timer(Interval, retry_delivery)};
                 false -> State
             end,
    State1#state{inflight = emqx_inflight:insert(PacketId, {publish, {PacketId, Msg}, os:timestamp()}, Inflight)}.

acked(puback, PacketId, State = #state{client_id = ClientId,
                                       username  = Username,
                                       inflight  = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, Msg, _Ts}} ->
            emqx_hooks:run('message.acked', [ClientId, Username], Msg),
            State#state{inflight = emqx_inflight:delete(PacketId, Inflight)};
        none ->
            ?LOG(warning, "Duplicated PUBACK Packet: ~p", [PacketId], State),
            State
    end;

acked(pubrec, PacketId, State = #state{client_id = ClientId,
                                       username  = Username,
                                       inflight  = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, Msg, _Ts}} ->
            emqx_hooks:run('message.acked', [ClientId, Username], Msg),
            State#state{inflight = emqx_inflight:update(PacketId, {pubrel, PacketId, os:timestamp()}, Inflight)};
        {value, {pubrel, PacketId, _Ts}} ->
            ?LOG(warning, "Duplicated PUBREC Packet: ~p", [PacketId], State),
            State;
        none ->
            ?LOG(warning, "Unexpected PUBREC Packet: ~p", [PacketId], State),
            State
    end;

acked(pubcomp, PacketId, State = #state{inflight = Inflight}) ->
    State#state{inflight = emqx_inflight:delete(PacketId, Inflight)}.

%%------------------------------------------------------------------------------
%% Dequeue
%%------------------------------------------------------------------------------

%% Do nothing if client is disconnected
dequeue(State = #state{client_pid = undefined}) ->
    State;

dequeue(State = #state{inflight = Inflight}) ->
    case emqx_inflight:is_full(Inflight) of
        true  -> State;
        false -> dequeue2(State)
    end.

dequeue2(State = #state{mqueue = Q}) ->
    case emqx_mqueue:out(Q) of
        {empty, _Q} ->
            State;
        {{value, Msg}, Q1} ->
            %% Dequeue more
            dequeue(dispatch(Msg, State#state{mqueue = Q1}))
    end.


%%------------------------------------------------------------------------------
%% Ensure timers

ensure_await_rel_timer(State = #state{await_rel_timer = undefined, await_rel_timeout = Timeout}) ->
     State#state{await_rel_timer = emqx_misc:start_timer(Timeout, check_awaiting_rel)};
ensure_await_rel_timer(State) ->
    State.

%%------------------------------------------------------------------------------
%% Reset Dup

reset_dup(Msg) ->
    emqx_message:unset_flag(dup, Msg).

%%------------------------------------------------------------------------------
%% Next Msg Id

next_pkt_id(State = #state{next_pkt_id = 16#FFFF}) ->
    State#state{next_pkt_id = 1};

next_pkt_id(State = #state{next_pkt_id = Id}) ->
    State#state{next_pkt_id = Id + 1}.

%%------------------------------------------------------------------------------
%% Ensure stats timer

ensure_stats_timer(State = #state{enable_stats = true,
                                  stats_timer  = undefined}) ->
    State#state{stats_timer = erlang:send_after(30000, self(), emit_stats)};
ensure_stats_timer(State) ->
    State.

inc_stats(deliver, State = #state{deliver_stats = I}) ->
    State#state{deliver_stats = I + 1};
inc_stats(enqueue, State = #state{enqueue_stats = I}) ->
    State#state{enqueue_stats = I + 1}.

%%--------------------------------------------------------------------
%% Helper functions

reply(Reply, State) ->
    {reply, Reply, State}.

shutdown(Reason, State) ->
    {stop, {shutdown, Reason}, State}.

%%TODO: maybe_gc(State) -> State.

