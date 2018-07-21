%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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

%%
%% @doc MQTT Session
%%
%% A stateful interaction between a Client and a Server. Some Sessions
%% last only as long as the Network Connection, others can span multiple
%% consecutive Network Connections between a Client and a Server.
%%
%% The Session state in the Server consists of:
%%
%% The existence of a Session, even if the rest of the Session state is empty.
%%
%% The Client’s subscriptions.
%%
%% QoS 1 and QoS 2 messages which have been sent to the Client, but have not
%% been completely acknowledged.
%%
%% QoS 1 and QoS 2 messages pending transmission to the Client.
%%
%% QoS 2 messages which have been received from the Client, but have not
%% been completely acknowledged.
%%
%% Optionally, QoS 0 messages pending transmission to the Client.
%%
%% If the session is currently disconnected, the time at which the Session state
%% will be deleted.
%%
%% @end
%%

-module(emqttd_session).

-behaviour(gen_server2).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

-import(emqttd_misc, [start_timer/2]).

-import(proplists, [get_value/2, get_value/3]).

%% Session API
-export([start_link/3, resume/3, destroy/2]).

%% Management and Monitor API
-export([state/1, info/1, stats/1]).

%% PubSub API
-export([subscribe/2, subscribe/3, publish/2, puback/2, pubrec/2,
         pubrel/2, pubcomp/2, unsubscribe/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% gen_server2 Message Priorities
-export([prioritise_call/4, prioritise_cast/3, prioritise_info/3,
         handle_pre_hibernate/1]).

-define(MQueue, emqttd_mqueue).

-record(state,
        {
         %% Clean Session Flag
         clean_sess = false :: boolean(),

         %% Client Binding: local | remote
         binding = local :: local | remote,

         %% ClientId: Identifier of Session
         client_id :: binary(),

         %% Username
         username :: binary() | undefined,

         %% Client Pid binding with session
         client_pid :: pid(),

         %% Old Client Pid that has been kickout
         old_client_pid :: pid(),

         %% Next message id of the session
         next_msg_id = 1 :: mqtt_packet_id(),

         max_subscriptions :: non_neg_integer(),

         %% Client’s subscriptions.
         subscriptions :: map(),

         %% Upgrade Qos?
         upgrade_qos = false :: boolean(),

         %% Client <- Broker: Inflight QoS1, QoS2 messages sent to the client but unacked.
         inflight :: emqttd_inflight:inflight(),

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
         mqueue :: ?MQueue:mqueue(),

         %% Client -> Broker: Inflight QoS2 messages received from client and waiting for pubrel.
         awaiting_rel :: map(),

         %% Max Packets that Awaiting PUBREL
         max_awaiting_rel = 100 :: non_neg_integer(),

         %% Awaiting PUBREL timeout
         await_rel_timeout = 20000 :: timeout(),

         %% Awaiting PUBREL timer
         await_rel_timer :: reference() | undefined,

         %% Session Expiry Interval
         expiry_interval = 7200000 :: timeout(),

         %% Expired Timer
         expiry_timer :: reference() | undefined,

         %% Enable Stats
         enable_stats :: boolean(),

         %% Force GC Count
         force_gc_count :: undefined | integer(),

         %% Ignore loop deliver?
         ignore_loop_deliver = false :: boolean(),

         created_at :: erlang:timestamp()
        }).

-define(TIMEOUT, 60000).

-define(INFO_KEYS, [clean_sess, client_id, username, client_pid, binding, created_at]).

-define(STATE_KEYS, [clean_sess, client_id, username, binding, client_pid, old_client_pid,
                     next_msg_id, max_subscriptions, subscriptions, upgrade_qos, inflight,
                     max_inflight, retry_interval, mqueue, awaiting_rel, max_awaiting_rel,
                     await_rel_timeout, expiry_interval, enable_stats, force_gc_count,
                     created_at]).

-define(LOG(Level, Format, Args, State),
            lager:Level([{client, State#state.client_id}],
                        "Session(~s): " ++ Format, [State#state.client_id | Args])).

%% @doc Start a Session
-spec(start_link(boolean(), {mqtt_client_id(), mqtt_username()}, pid()) -> {ok, pid()} | {error, term()}).
start_link(CleanSess, {ClientId, Username}, ClientPid) ->
    gen_server2:start_link(?MODULE, [CleanSess, {ClientId, Username}, ClientPid], []).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

%% @doc Subscribe topics
-spec(subscribe(pid(), [{binary(), [emqttd_topic:option()]}]) -> ok).
subscribe(Session, TopicTable) -> %%TODO: the ack function??...
    gen_server2:cast(Session, {subscribe, self(), TopicTable, fun(_) -> ok end}).

-spec(subscribe(pid(), mqtt_packet_id(), [{binary(), [emqttd_topic:option()]}]) -> ok).
subscribe(Session, PacketId, TopicTable) -> %%TODO: the ack function??...
    From = self(),
    AckFun = fun(GrantedQos) -> From ! {suback, PacketId, GrantedQos} end,
    gen_server2:cast(Session, {subscribe, From, TopicTable, AckFun}).

%% @doc Publish Message
-spec(publish(pid(), mqtt_message()) -> ok | {error, term()}).
publish(_Session, Msg = #mqtt_message{qos = ?QOS_0}) ->
    %% Publish QoS0 Directly
    emqttd_server:publish(Msg), ok;

publish(_Session, Msg = #mqtt_message{qos = ?QOS_1}) ->
    %% Publish QoS1 message directly for client will PubAck automatically
    emqttd_server:publish(Msg), ok;

publish(Session, Msg = #mqtt_message{qos = ?QOS_2}) ->
    %% Publish QoS2 to Session
    gen_server2:call(Session, {publish, Msg}, ?TIMEOUT).

%% @doc PubAck Message
-spec(puback(pid(), mqtt_packet_id()) -> ok).
puback(Session, PacketId) ->
    gen_server2:cast(Session, {puback, PacketId}).

-spec(pubrec(pid(), mqtt_packet_id()) -> ok).
pubrec(Session, PacketId) ->
    gen_server2:cast(Session, {pubrec, PacketId}).

-spec(pubrel(pid(), mqtt_packet_id()) -> ok).
pubrel(Session, PacketId) ->
    gen_server2:cast(Session, {pubrel, PacketId}).

-spec(pubcomp(pid(), mqtt_packet_id()) -> ok).
pubcomp(Session, PacketId) ->
    gen_server2:cast(Session, {pubcomp, PacketId}).

%% @doc Unsubscribe the topics
-spec(unsubscribe(pid(), [{binary(), [emqttd_topic:option()]}]) -> ok).
unsubscribe(Session, TopicTable) ->
    gen_server2:cast(Session, {unsubscribe, self(), TopicTable}).

%% @doc Resume the session
-spec(resume(pid(), mqtt_client_id(), pid()) -> ok).
resume(Session, ClientId, ClientPid) ->
    gen_server2:cast(Session, {resume, ClientId, ClientPid}).

%% @doc Get session state
state(Session) when is_pid(Session) ->
    gen_server2:call(Session, state).

%% @doc Get session info
-spec(info(pid() | #state{}) -> list(tuple())).
info(Session) when is_pid(Session) ->
    gen_server2:call(Session, info);

info(State) when is_record(State, state) ->
    ?record_to_proplist(state, State, ?INFO_KEYS).

-spec(stats(pid() | #state{}) -> list({atom(), non_neg_integer()})).
stats(Session) when is_pid(Session) ->
    gen_server2:call(Session, stats);

stats(#state{max_subscriptions = MaxSubscriptions,
             subscriptions     = Subscriptions,
             inflight          = Inflight,
             max_inflight      = MaxInflight,
             mqueue            = MQueue,
             max_awaiting_rel  = MaxAwaitingRel,
             awaiting_rel      = AwaitingRel}) ->
    lists:append(emqttd_misc:proc_stats(),
                 [{max_subscriptions, MaxSubscriptions},
                  {subscriptions,     maps:size(Subscriptions)},
                  {max_inflight,      MaxInflight},
                  {inflight_len,      Inflight:size()},
                  {max_mqueue,        ?MQueue:max_len(MQueue)},
                  {mqueue_len,        ?MQueue:len(MQueue)},
                  {mqueue_dropped,    ?MQueue:dropped(MQueue)},
                  {max_awaiting_rel,  MaxAwaitingRel},
                  {awaiting_rel_len,  maps:size(AwaitingRel)},
                  {deliver_msg,       get(deliver_msg)},
                  {enqueue_msg,       get(enqueue_msg)}]).

%% @doc Destroy the session
-spec(destroy(pid(), mqtt_client_id()) -> ok).
destroy(Session, ClientId) ->
    gen_server2:cast(Session, {destroy, ClientId}).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([CleanSess, {ClientId, Username}, ClientPid]) ->
    process_flag(trap_exit, true),
    true = link(ClientPid),
    init_stats([deliver_msg, enqueue_msg]),
    {ok, Env} = emqttd:env(session),
    {ok, QEnv} = emqttd:env(mqueue),
    MaxInflight = get_value(max_inflight, Env, 0),
    EnableStats = get_value(enable_stats, Env, false),
    ForceGcCount = emqttd_gc:conn_max_gc_count(),
    IgnoreLoopDeliver = get_value(ignore_loop_deliver, Env, false),
    MQueue = ?MQueue:new(ClientId, QEnv, emqttd_alarm:alarm_fun()),
    State = #state{clean_sess        = CleanSess,
                   binding           = binding(ClientPid),
                   client_id         = ClientId,
                   client_pid        = ClientPid,
                   username          = Username,
                   subscriptions     = #{},
                   max_subscriptions = get_value(max_subscriptions, Env, 0),
                   upgrade_qos       = get_value(upgrade_qos, Env, false),
                   max_inflight      = MaxInflight,
                   inflight          = emqttd_inflight:new(MaxInflight),
                   mqueue            = MQueue,
                   retry_interval    = get_value(retry_interval, Env),
                   awaiting_rel      = #{},
                   await_rel_timeout = get_value(await_rel_timeout, Env),
                   max_awaiting_rel  = get_value(max_awaiting_rel, Env),
                   expiry_interval   = get_value(expiry_interval, Env),
                   enable_stats      = EnableStats,
                   force_gc_count    = ForceGcCount,
                   created_at        = os:timestamp(),
                   ignore_loop_deliver = IgnoreLoopDeliver},
    emqttd_sm:register_session(ClientId, CleanSess, info(State)),
    emqttd_hooks:run('session.created', [ClientId, Username]),
    {ok, emit_stats(State), hibernate, {backoff, 1000, 1000, 10000}}.

init_stats(Keys) ->
    lists:foreach(fun(K) -> put(K, 0) end, Keys).

binding(ClientPid) ->
    case node(ClientPid) =:= node() of true -> local; false -> remote end.

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of info -> 10; stats -> 10; state -> 10; _ -> 5 end.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {destroy, _}        -> 10;
        {resume, _, _}      -> 9;
        {pubrel,  _}        -> 8;
        {pubcomp, _}        -> 8;
        {pubrec,  _}        -> 8;
        {puback,  _}        -> 7;
        {unsubscribe, _, _} -> 6;
        {subscribe, _, _}   -> 5;
        _                   -> 0
    end.

prioritise_info(Msg, _Len, _State) ->
    case Msg of
        {'EXIT', _, _}   -> 10;
        {timeout, _, _}  -> 5;
        {dispatch, _, _} -> 1;
        _                -> 0
    end.

handle_pre_hibernate(State) ->
    {hibernate, emqttd_gc:reset_conn_gc_count(#state.force_gc_count, emit_stats(State))}.

handle_call({publish, Msg = #mqtt_message{qos = ?QOS_2, pktid = PacketId}}, _From,
            State = #state{awaiting_rel      = AwaitingRel,
                           await_rel_timer   = Timer,
                           await_rel_timeout = Timeout}) ->
    case is_awaiting_full(State) of
        false ->
            State1 = case Timer == undefined of
                         true  -> State#state{await_rel_timer = start_timer(Timeout, check_awaiting_rel)};
                         false -> State
                     end,
            reply(ok, State1#state{awaiting_rel = maps:put(PacketId, Msg, AwaitingRel)});
        true ->
            ?LOG(warning, "Dropped Qos2 Message for too many awaiting_rel: ~p", [Msg], State),
            emqttd_metrics:inc('messages/qos2/dropped'),
            reply({error, dropped}, State)
    end;

handle_call(info, _From, State) ->
    reply(info(State), State);

handle_call(stats, _From, State) ->
    reply(stats(State), State);

handle_call(state, _From, State) ->
    reply(?record_to_proplist(state, State, ?STATE_KEYS), State);

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast({subscribe, _From, TopicTable, AckFun},
            State = #state{client_id     = ClientId,
                           username      = Username,
                           subscriptions = Subscriptions}) ->
    ?LOG(debug, "Subscribe ~p", [TopicTable], State),
    {GrantedQos, Subscriptions1} =
    lists:foldl(fun({Topic, Opts}, {QosAcc, SubMap}) ->
                NewQos = proplists:get_value(qos, Opts),
                SubMap1 =
                case maps:find(Topic, SubMap) of
                    {ok, NewQos} ->
                        emqttd_hooks:run('session.subscribed', [ClientId, Username], {Topic, Opts}),
                        ?LOG(warning, "Duplicated subscribe: ~s, qos = ~w", [Topic, NewQos], State),
                        SubMap;
                    {ok, OldQos} ->
                        emqttd:setqos(Topic, ClientId, NewQos),
                        emqttd_hooks:run('session.subscribed', [ClientId, Username], {Topic, Opts}),
                        ?LOG(warning, "Duplicated subscribe ~s, old_qos=~w, new_qos=~w",
                            [Topic, OldQos, NewQos], State),
                        maps:put(Topic, NewQos, SubMap);
                    error ->
                        emqttd:subscribe(Topic, ClientId, Opts),
                        emqttd_hooks:run('session.subscribed', [ClientId, Username], {Topic, Opts}),
                        maps:put(Topic, NewQos, SubMap)
                end,
                {[NewQos|QosAcc], SubMap1}
        end, {[], Subscriptions}, TopicTable),
    AckFun(lists:reverse(GrantedQos)),
    hibernate(emit_stats(State#state{subscriptions = Subscriptions1}));

handle_cast({unsubscribe, _From, TopicTable},
            State = #state{client_id     = ClientId,
                           username      = Username,
                           subscriptions = Subscriptions}) ->
    ?LOG(debug, "Unsubscribe ~p", [TopicTable], State),
    Subscriptions1 =
    lists:foldl(fun({Topic, Opts}, SubMap) ->
                case maps:find(Topic, SubMap) of
                    {ok, _Qos} ->
                        emqttd:unsubscribe(Topic, ClientId),
                        emqttd_hooks:run('session.unsubscribed', [ClientId, Username], {Topic, Opts}),
                        maps:remove(Topic, SubMap);
                    error ->
                        SubMap
                end
        end, Subscriptions, TopicTable),
    hibernate(emit_stats(State#state{subscriptions = Subscriptions1}));

%% PUBACK:
handle_cast({puback, PacketId}, State = #state{inflight = Inflight}) ->
    {noreply,
     case Inflight:contain(PacketId) of
         true ->
             dequeue(acked(puback, PacketId, State));
         false ->
             ?LOG(warning, "PUBACK ~p missed inflight: ~p",
                  [PacketId, Inflight:window()], State),
             emqttd_metrics:inc('packets/puback/missed'),
             State
     end, hibernate};

%% PUBREC:
handle_cast({pubrec, PacketId}, State = #state{inflight = Inflight}) ->
    {noreply,
     case Inflight:contain(PacketId) of
         true ->
             acked(pubrec, PacketId, State);
         false ->
             ?LOG(warning, "PUBREC ~p missed inflight: ~p",
                  [PacketId, Inflight:window()], State),
             emqttd_metrics:inc('packets/pubrec/missed'),
             State
     end, hibernate};

%% PUBREL:
handle_cast({pubrel, PacketId}, State = #state{awaiting_rel = AwaitingRel}) ->
    {noreply,
     case maps:take(PacketId, AwaitingRel) of
         {Msg, AwaitingRel1} ->
             %% Implement Qos2 by method A [MQTT 4.33]
             %% Dispatch to subscriber when received PUBREL
             spawn(emqttd_server, publish, [Msg]), %%:)
             gc(State#state{awaiting_rel = AwaitingRel1});
         error ->
             ?LOG(warning, "Cannot find PUBREL: ~p", [PacketId], State),
             emqttd_metrics:inc('packets/pubrel/missed'),
             State
     end, hibernate};

%% PUBCOMP:
handle_cast({pubcomp, PacketId}, State = #state{inflight = Inflight}) ->
    {noreply,
     case Inflight:contain(PacketId) of
         true ->
             dequeue(acked(pubcomp, PacketId, State));
         false ->
             ?LOG(warning, "The PUBCOMP ~p is not inflight: ~p",
                  [PacketId, Inflight:window()], State),
             emqttd_metrics:inc('packets/pubcomp/missed'),
             State
     end, hibernate};

%% RESUME:
handle_cast({resume, ClientId, ClientPid},
            State = #state{client_id       = ClientId,
                           client_pid      = OldClientPid,
                           clean_sess      = CleanSess,
                           retry_timer     = RetryTimer,
                           await_rel_timer = AwaitTimer,
                           expiry_timer    = ExpireTimer}) ->

    ?LOG(debug, "Resumed by ~p", [ClientPid], State),

    %% Cancel Timers
    lists:foreach(fun emqttd_misc:cancel_timer/1,
                  [RetryTimer, AwaitTimer, ExpireTimer]),

    case kick(ClientId, OldClientPid, ClientPid) of
        ok -> ?LOG(warning, "~p kickout ~p", [ClientPid, OldClientPid], State);
        ignore -> ok
    end,

    true = link(ClientPid),

    State1 = State#state{client_pid      = ClientPid,
                         binding         = binding(ClientPid),
                         old_client_pid  = OldClientPid,
                         clean_sess      = false,
                         retry_timer     = undefined,
                         awaiting_rel    = #{},
                         await_rel_timer = undefined,
                         expiry_timer    = undefined},

    %% Clean Session: true -> false?
    if
        CleanSess =:= true ->
            ?LOG(info, "CleanSess changed to false.", [], State1),
            emqttd_sm:register_session(ClientId, false, info(State1));
        CleanSess =:= false ->
            ok
    end,

    %% Replay delivery and Dequeue pending messages
    hibernate(emit_stats(dequeue(retry_delivery(true, State1))));

handle_cast({destroy, ClientId},
            State = #state{client_id = ClientId, client_pid = undefined}) ->
    ?LOG(warning, "Destroyed", [], State),
    shutdown(destroy, State);

handle_cast({destroy, ClientId},
            State = #state{client_id = ClientId, client_pid = OldClientPid}) ->
    ?LOG(warning, "kickout ~p", [OldClientPid], State),
    shutdown(conflict, State);

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

%% Ignore Messages delivered by self
handle_info({dispatch, _Topic, #mqtt_message{from = {ClientId, _}}},
             State = #state{client_id = ClientId, ignore_loop_deliver = true}) ->
    hibernate(State);

%% Dispatch Message
handle_info({dispatch, Topic, Msg}, State) when is_record(Msg, mqtt_message) ->
    hibernate(gc(dispatch(tune_qos(Topic, reset_dup(Msg), State), State)));

%% Do nothing if the client has been disconnected.
handle_info({timeout, _Timer, retry_delivery}, State = #state{client_pid = undefined}) ->
    hibernate(emit_stats(State#state{retry_timer = undefined}));

handle_info({timeout, _Timer, retry_delivery}, State) ->
    hibernate(emit_stats(retry_delivery(false, State#state{retry_timer = undefined})));

handle_info({timeout, _Timer, check_awaiting_rel}, State) ->
    hibernate(expire_awaiting_rel(emit_stats(State#state{await_rel_timer = undefined})));

handle_info({timeout, _Timer, expired}, State) ->
    ?LOG(info, "Expired, shutdown now.", [], State),
    shutdown(expired, State);

handle_info({'EXIT', ClientPid, _Reason},
            State = #state{clean_sess = true, client_pid = ClientPid}) ->
    {stop, normal, State};

handle_info({'EXIT', ClientPid, Reason},
            State = #state{clean_sess      = false,
                           client_pid      = ClientPid,
                           expiry_interval = Interval}) ->
    ?LOG(info, "Client ~p EXIT for ~p", [ClientPid, Reason], State),
    ExpireTimer = start_timer(Interval, expired),
    State1 = State#state{client_pid = undefined, expiry_timer = ExpireTimer},
    hibernate(emit_stats(State1));

handle_info({'EXIT', Pid, _Reason}, State = #state{old_client_pid = Pid}) ->
    %%ignore
    hibernate(State);

handle_info({'EXIT', Pid, Reason}, State = #state{client_pid = ClientPid}) ->

    ?LOG(error, "Unexpected EXIT: client_pid=~p, exit_pid=~p, reason=~p",
         [ClientPid, Pid, Reason], State),
    hibernate(State);

handle_info(Info, Session) ->
    ?UNEXPECTED_INFO(Info, Session).

terminate(Reason, #state{client_id = ClientId, username = Username}) ->
    %% Move to emqttd_sm to avoid race condition
    %%emqttd_stats:del_session_stats(ClientId),
    emqttd_hooks:run('session.terminated', [ClientId, Username, Reason]),
    emqttd_sm:unregister_session(ClientId).

code_change(_OldVsn, Session, _Extra) ->
    {ok, Session}.

%%--------------------------------------------------------------------
%% Kickout old client
%%--------------------------------------------------------------------
kick(_ClientId, undefined, _Pid) ->
    ignore;
kick(_ClientId, Pid, Pid) ->
    ignore;
kick(ClientId, OldPid, Pid) ->
    unlink(OldPid),
    OldPid ! {shutdown, conflict, {ClientId, Pid}},
    %% Clean noproc
    receive {'EXIT', OldPid, _} -> ok after 0 -> ok end.

%%--------------------------------------------------------------------
%% Replay or Retry Delivery
%%--------------------------------------------------------------------

%% Redeliver at once if Force is true

retry_delivery(Force, State = #state{inflight = Inflight}) ->
    case Inflight:is_empty() of
        true  -> State;
        false -> Msgs = lists:sort(sortfun(inflight), Inflight:values()),
                 retry_delivery(Force, Msgs, os:timestamp(), State)
    end.

retry_delivery(_Force, [], _Now, State = #state{retry_interval = Interval}) ->
    State#state{retry_timer = start_timer(Interval, retry_delivery)};

retry_delivery(Force, [{Type, Msg, Ts} | Msgs], Now,
               State = #state{inflight       = Inflight,
                              retry_interval = Interval}) ->
    Diff = timer:now_diff(Now, Ts) div 1000, %% micro -> ms
    if
        Force orelse (Diff >= Interval) ->
            case {Type, Msg} of
                {publish, Msg = #mqtt_message{pktid = PacketId}} ->
                    redeliver(Msg, State),
                    Inflight1 = Inflight:update(PacketId, {publish, Msg, Now}),
                    retry_delivery(Force, Msgs, Now, State#state{inflight = Inflight1});
                {pubrel, PacketId} ->
                    redeliver({pubrel, PacketId}, State),
                    Inflight1 = Inflight:update(PacketId, {pubrel, PacketId, Now}),
                    retry_delivery(Force, Msgs, Now, State#state{inflight = Inflight1})
            end;
        true ->
            State#state{retry_timer = start_timer(Interval - Diff, retry_delivery)}
    end.

%%--------------------------------------------------------------------
%% Expire Awaiting Rel
%%--------------------------------------------------------------------

expire_awaiting_rel(State = #state{awaiting_rel = AwaitingRel}) ->
    case maps:size(AwaitingRel) of
        0 -> State;
        _ -> Msgs = lists:sort(sortfun(awaiting_rel), maps:to_list(AwaitingRel)),
             expire_awaiting_rel(Msgs, os:timestamp(), State)
    end.

expire_awaiting_rel([], _Now, State) ->
    State#state{await_rel_timer = undefined};

expire_awaiting_rel([{PacketId, Msg = #mqtt_message{timestamp = TS}} | Msgs],
                    Now, State = #state{awaiting_rel      = AwaitingRel,
                                        await_rel_timeout = Timeout}) ->
    case (timer:now_diff(Now, TS) div 1000) of
        Diff when Diff >= Timeout ->
            ?LOG(warning, "Dropped Qos2 Message for await_rel_timeout: ~p", [Msg], State),
            emqttd_metrics:inc('messages/qos2/dropped'),
            expire_awaiting_rel(Msgs, Now, State#state{awaiting_rel = maps:remove(PacketId, AwaitingRel)});
        Diff ->
            State#state{await_rel_timer = start_timer(Timeout - Diff, check_awaiting_rel)}
    end.

%%--------------------------------------------------------------------
%% Sort Inflight, AwaitingRel
%%--------------------------------------------------------------------

sortfun(inflight) ->
    fun({_, _, Ts1}, {_, _, Ts2}) -> Ts1 < Ts2 end;

sortfun(awaiting_rel) ->
    fun({_, #mqtt_message{timestamp = Ts1}},
        {_, #mqtt_message{timestamp = Ts2}}) ->
        Ts1 < Ts2
    end.

%%--------------------------------------------------------------------
%% Check awaiting rel
%%--------------------------------------------------------------------

is_awaiting_full(#state{max_awaiting_rel = 0}) ->
    false;
is_awaiting_full(#state{awaiting_rel = AwaitingRel, max_awaiting_rel = MaxLen}) ->
    maps:size(AwaitingRel) >= MaxLen.

%%--------------------------------------------------------------------
%% Dispatch Messages
%%--------------------------------------------------------------------

%% Enqueue message if the client has been disconnected
dispatch(Msg, State = #state{client_pid = undefined}) ->
    enqueue_msg(Msg, State);

%% Deliver qos0 message directly to client
dispatch(Msg = #mqtt_message{qos = ?QOS0}, State) ->
    deliver(Msg, State), State;

dispatch(Msg = #mqtt_message{qos = QoS},
         State = #state{next_msg_id = MsgId, inflight = Inflight})
    when QoS =:= ?QOS1 orelse QoS =:= ?QOS2 ->
    case Inflight:is_full() of
        true  ->
            enqueue_msg(Msg, State);
        false ->
            Msg1 = Msg#mqtt_message{pktid = MsgId},
            deliver(Msg1, State),
            await(Msg1, next_msg_id(State))
    end.

enqueue_msg(Msg, State = #state{mqueue = Q}) ->
    inc_stats(enqueue_msg),
    State#state{mqueue = ?MQueue:in(Msg, Q)}.

%%--------------------------------------------------------------------
%% Deliver
%%--------------------------------------------------------------------

redeliver(Msg = #mqtt_message{qos = QoS}, State) ->
    deliver(Msg#mqtt_message{dup = if QoS =:= ?QOS0 -> false; true -> true end}, State);

redeliver({pubrel, PacketId}, #state{client_pid = Pid}) ->
    Pid ! {redeliver, {?PUBREL, PacketId}}.

deliver(Msg, #state{client_pid = Pid}) ->
    inc_stats(deliver_msg),
    Pid ! {deliver, Msg}.

%%--------------------------------------------------------------------
%% Awaiting ACK for QoS1/QoS2 Messages
%%--------------------------------------------------------------------

await(Msg = #mqtt_message{pktid = PacketId},
      State = #state{inflight       = Inflight,
                     retry_timer    = RetryTimer,
                     retry_interval = Interval}) ->
    %% Start retry timer if the Inflight is still empty
    State1 = ?IF(RetryTimer == undefined, State#state{retry_timer = start_timer(Interval, retry_delivery)}, State),
    State1#state{inflight = Inflight:insert(PacketId, {publish, Msg, os:timestamp()})}.

acked(puback, PacketId, State = #state{client_id = ClientId,
                                       username  = Username,
                                       inflight  = Inflight}) ->
    case Inflight:lookup(PacketId) of
        {publish, Msg, _Ts} ->
            emqttd_hooks:run('message.acked', [ClientId, Username], Msg),
            State#state{inflight = Inflight:delete(PacketId)};
        _ ->
            ?LOG(warning, "Duplicated PUBACK Packet: ~p", [PacketId], State),
            State
    end;

acked(pubrec, PacketId, State = #state{client_id = ClientId,
                                       username  = Username,
                                       inflight  = Inflight}) ->
    case Inflight:lookup(PacketId) of
        {publish, Msg, _Ts} ->
            emqttd_hooks:run('message.acked', [ClientId, Username], Msg),
            State#state{inflight = Inflight:update(PacketId, {pubrel, PacketId, os:timestamp()})};
        {pubrel, PacketId, _Ts} ->
            ?LOG(warning, "Duplicated PUBREC Packet: ~p", [PacketId], State),
            State
    end;

acked(pubcomp, PacketId, State = #state{inflight = Inflight}) ->
    State#state{inflight = Inflight:delete(PacketId)}.

%%--------------------------------------------------------------------
%% Dequeue
%%--------------------------------------------------------------------

%% Do nothing if client is disconnected
dequeue(State = #state{client_pid = undefined}) ->
    State;

dequeue(State = #state{inflight = Inflight}) ->
    case Inflight:is_full() of
        true  -> State;
        false -> dequeue2(State)
    end.

dequeue2(State = #state{mqueue = Q}) ->
    case ?MQueue:out(Q) of
        {empty, _Q} ->
            State;
        {{value, Msg}, Q1} ->
            %% Dequeue more
            dequeue(dispatch(Msg, State#state{mqueue = Q1}))
    end.

%%--------------------------------------------------------------------
%% Tune QoS
%%--------------------------------------------------------------------

tune_qos(Topic, Msg = #mqtt_message{qos = PubQoS},
         #state{subscriptions = SubMap, upgrade_qos = UpgradeQoS}) ->
    case maps:find(Topic, SubMap) of
        {ok, SubQoS} when UpgradeQoS andalso (SubQoS > PubQoS) ->
            Msg#mqtt_message{qos = SubQoS};
        {ok, SubQoS} when (not UpgradeQoS) andalso (SubQoS < PubQoS) ->
            Msg#mqtt_message{qos = SubQoS};
        {ok, _} ->
            Msg;
        error ->
            Msg
    end.

%%--------------------------------------------------------------------
%% Reset Dup
%%--------------------------------------------------------------------

reset_dup(Msg = #mqtt_message{dup = true}) ->
    Msg#mqtt_message{dup = false};
reset_dup(Msg) -> Msg.

%%--------------------------------------------------------------------
%% Next Msg Id
%%--------------------------------------------------------------------

next_msg_id(State = #state{next_msg_id = 16#FFFF}) ->
    State#state{next_msg_id = 1};

next_msg_id(State = #state{next_msg_id = Id}) ->
    State#state{next_msg_id = Id + 1}.

%%--------------------------------------------------------------------
%% Emit session stats
%%--------------------------------------------------------------------

emit_stats(State = #state{enable_stats = false}) ->
    State;
emit_stats(State = #state{client_id = ClientId}) ->
    emqttd_stats:set_session_stats(ClientId, stats(State)),
    State.

inc_stats(Key) -> put(Key, get(Key) + 1).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

hibernate(State) ->
    {noreply, State, hibernate}.

shutdown(Reason, State) ->
    {stop, {shutdown, Reason}, State}.

gc(State) ->
    emqttd_gc:maybe_force_gc(#state.force_gc_count, State).
