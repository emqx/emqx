%%--------------------------------------------------------------------
%% Copyright © 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_session).

-behaviour(gen_server).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

-include("emqx_misc.hrl").

-import(emqx_misc, [start_timer/2]).

-import(proplists, [get_value/2, get_value/3]).

%% Session API
-export([start_link/1, resume/3, discard/2]).

%% Management and Monitor API
-export([state/1, info/1, stats/1]).

%% PubSub API
-export([subscribe/2, subscribe/3, publish/2, puback/2, pubrec/2,
         pubrel/2, pubcomp/2, unsubscribe/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(MQueue, emqx_mqueue).

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
-record(state,
        { %% Clean Start Flag
          clean_start = false :: boolean(),

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

          %% Force GC reductions
          reductions = 0 :: non_neg_integer(),

          %% Ignore loop deliver?
          ignore_loop_deliver = false :: boolean(),

          created_at :: erlang:timestamp()
        }).

-define(TIMEOUT, 60000).

-define(INFO_KEYS, [clean_start, client_id, username, client_pid, binding, created_at]).

-define(STATE_KEYS, [clean_start, client_id, username, binding, client_pid, old_client_pid,
                     next_msg_id, max_subscriptions, subscriptions, upgrade_qos, inflight,
                     max_inflight, retry_interval, mqueue, awaiting_rel, max_awaiting_rel,
                     await_rel_timeout, expiry_interval, enable_stats, force_gc_count,
                     created_at]).

-define(LOG(Level, Format, Args, State),
            lager:Level([{client, State#state.client_id}],
                        "Session(~s): " ++ Format, [State#state.client_id | Args])).

%% @doc Start a Session
-spec(start_link(map()) -> {ok, pid()} | {error, term()}).
start_link(Attrs) ->
    gen_server:start_link(?MODULE, Attrs, [{hibernate_after, 10000}]).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

%% @doc Subscribe topics
-spec(subscribe(pid(), [{binary(), [emqx_topic:option()]}]) -> ok).
subscribe(SessionPid, TopicTable) -> %%TODO: the ack function??...
    gen_server:cast(SessionPid, {subscribe, self(), TopicTable, fun(_) -> ok end}).

-spec(subscribe(pid(), mqtt_packet_id(), [{binary(), [emqx_topic:option()]}]) -> ok).
subscribe(SessionPid, PacketId, TopicTable) -> %%TODO: the ack function??...
    From = self(),
    AckFun = fun(GrantedQos) -> From ! {suback, PacketId, GrantedQos} end,
    gen_server:cast(SessionPid, {subscribe, From, TopicTable, AckFun}).

%% @doc Publish Message
-spec(publish(pid(), message()) -> ok | {error, term()}).
publish(_SessionPid, Msg = #message{qos = ?QOS_0}) ->
    %% Publish QoS0 Directly
    emqx_broker:publish(Msg), ok;

publish(_SessionPid, Msg = #message{qos = ?QOS_1}) ->
    %% Publish QoS1 message directly for client will PubAck automatically
    emqx_broker:publish(Msg), ok;

publish(SessionPid, Msg = #message{qos = ?QOS_2}) ->
    %% Publish QoS2 to Session
    gen_server:call(SessionPid, {publish, Msg}, ?TIMEOUT).

%% @doc PubAck Message
-spec(puback(pid(), mqtt_packet_id()) -> ok).
puback(SessionPid, PacketId) ->
    gen_server:cast(SessionPid, {puback, PacketId}).

-spec(pubrec(pid(), mqtt_packet_id()) -> ok).
pubrec(SessionPid, PacketId) ->
    gen_server:cast(SessionPid, {pubrec, PacketId}).

-spec(pubrel(pid(), mqtt_packet_id()) -> ok).
pubrel(SessionPid, PacketId) ->
    gen_server:cast(SessionPid, {pubrel, PacketId}).

-spec(pubcomp(pid(), mqtt_packet_id()) -> ok).
pubcomp(SessionPid, PacketId) ->
    gen_server:cast(SessionPid, {pubcomp, PacketId}).

%% @doc Unsubscribe the topics
-spec(unsubscribe(pid(), [{binary(), [suboption()]}]) -> ok).
unsubscribe(SessionPid, TopicTable) ->
    gen_server:cast(SessionPid, {unsubscribe, self(), TopicTable}).

%% @doc Resume the session
-spec(resume(pid(), client_id(), pid()) -> ok).
resume(SessionPid, ClientId, ClientPid) ->
    gen_server:cast(SessionPid, {resume, ClientId, ClientPid}).

%% @doc Get session state
state(SessionPid) when is_pid(SessionPid) ->
    gen_server:call(SessionPid, state).

%% @doc Get session info
-spec(info(pid() | #state{}) -> list(tuple())).
info(SessionPid) when is_pid(SessionPid) ->
    gen_server:call(SessionPid, info);

info(State) when is_record(State, state) ->
    ?record_to_proplist(state, State, ?INFO_KEYS).

-spec(stats(pid() | #state{}) -> list({atom(), non_neg_integer()})).
stats(SessionPid) when is_pid(SessionPid) ->
    gen_server:call(SessionPid, stats);

stats(#state{max_subscriptions = MaxSubscriptions,
             subscriptions     = Subscriptions,
             inflight          = Inflight,
             max_inflight      = MaxInflight,
             mqueue            = MQueue,
             max_awaiting_rel  = MaxAwaitingRel,
             awaiting_rel      = AwaitingRel}) ->
    lists:append(emqx_misc:proc_stats(),
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

%% @doc Discard the session
-spec(discard(pid(), client_id()) -> ok).
discard(SessionPid, ClientId) ->
    gen_server:call(SessionPid, {discard, ClientId}).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init(#{clean_start := CleanStart,
       client_id   := ClientId,
       username    := Username,
       client_pid  := ClientPid}) ->
    process_flag(trap_exit, true),
    true = link(ClientPid),
    init_stats([deliver_msg, enqueue_msg]),
    {ok, Env} = emqx:env(session),
    {ok, QEnv} = emqx:env(mqueue),
    MaxInflight = get_value(max_inflight, Env, 0),
    EnableStats = get_value(enable_stats, Env, false),
    IgnoreLoopDeliver = get_value(ignore_loop_deliver, Env, false),
    MQueue = ?MQueue:new(ClientId, QEnv, emqx_alarm:alarm_fun()),
    State = #state{clean_start       = CleanStart,
                   binding           = binding(ClientPid),
                   client_id         = ClientId,
                   client_pid        = ClientPid,
                   username          = Username,
                   subscriptions     = #{},
                   max_subscriptions = get_value(max_subscriptions, Env, 0),
                   upgrade_qos       = get_value(upgrade_qos, Env, false),
                   max_inflight      = MaxInflight,
                   inflight          = emqx_inflight:new(MaxInflight),
                   mqueue            = MQueue,
                   retry_interval    = get_value(retry_interval, Env),
                   awaiting_rel      = #{},
                   await_rel_timeout = get_value(await_rel_timeout, Env),
                   max_awaiting_rel  = get_value(max_awaiting_rel, Env),
                   expiry_interval   = get_value(expiry_interval, Env),
                   enable_stats      = EnableStats,
                   ignore_loop_deliver = IgnoreLoopDeliver,
                   created_at        = os:timestamp()},
    emqx_sm:register_session(#session{sid = ClientId, pid = self()}, info(State)),
    emqx_hooks:run('session.created', [ClientId, Username]),
    io:format("Session started: ~p~n", [self()]),
    {ok, emit_stats(State), hibernate}.

init_stats(Keys) ->
    lists:foreach(fun(K) -> put(K, 0) end, Keys).

binding(ClientPid) ->
    case node(ClientPid) =:= node() of true -> local; false -> remote end.

handle_call({discard, ClientPid}, _From, State = #state{client_pid = undefined}) ->
    ?LOG(warning, "Discarded by ~p", [ClientPid], State),
    {stop, {shutdown, discard}, ok, State};

handle_call({discard, ClientPid}, _From, State = #state{client_pid = OldClientPid}) ->
    ?LOG(warning, " ~p kickout ~p", [ClientPid, OldClientPid], State),
    {stop, {shutdown, conflict}, ok, State};

handle_call({publish, Msg = #message{qos = ?QOS_2, headers = #{packet_id := PacketId}}}, _From,
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
            emqx_metrics:inc('messages/qos2/dropped'),
            reply({error, dropped}, State)
    end;

handle_call(info, _From, State) ->
    reply(info(State), State);

handle_call(stats, _From, State) ->
    reply(stats(State), State);

handle_call(state, _From, State) ->
    reply(?record_to_proplist(state, State, ?STATE_KEYS), State);

handle_call(Req, _From, State) ->
    lager:error("[~s] Unexpected Call: ~p", [?MODULE, Req]),
    {reply, ignore, State}.

handle_cast({subscribe, From, TopicTable, AckFun},
            State = #state{client_id     = ClientId,
                           username      = Username,
                           subscriptions = Subscriptions}) ->
    ?LOG(info, "Subscribe ~p", [TopicTable], State),
    {GrantedQos, Subscriptions1} =
    lists:foldl(fun({Topic, Opts}, {QosAcc, SubMap}) ->
                io:format("SubOpts: ~p~n", [Opts]),
                Fastlane = lists:member(fastlane, Opts),
                NewQos = if Fastlane == true -> ?QOS_0; true -> get_value(qos, Opts) end,
                SubMap1 =
                case maps:find(Topic, SubMap) of
                    {ok, NewQos} ->
                        ?LOG(warning, "Duplicated subscribe: ~s, qos = ~w", [Topic, NewQos], State),
                        SubMap;
                    {ok, OldQos} ->
                        emqx_broker:setopts(Topic, ClientId, [{qos, NewQos}]),
                        emqx_hooks:run('session.subscribed', [ClientId, Username], {Topic, Opts}),
                        ?LOG(warning, "Duplicated subscribe ~s, old_qos=~w, new_qos=~w",
                            [Topic, OldQos, NewQos], State),
                        maps:put(Topic, NewQos, SubMap);
                    error ->
                        case Fastlane of
                            true  -> emqx:subscribe(Topic, From, Opts);
                            false -> emqx:subscribe(Topic, ClientId, Opts)
                        end,
                        emqx_hooks:run('session.subscribed', [ClientId, Username], {Topic, Opts}),
                        maps:put(Topic, NewQos, SubMap)
                end,
                {[NewQos|QosAcc], SubMap1}
        end, {[], Subscriptions}, TopicTable),
    io:format("GrantedQos: ~p~n", [GrantedQos]),
    AckFun(lists:reverse(GrantedQos)),
    {noreply, emit_stats(State#state{subscriptions = Subscriptions1}), hibernate};

handle_cast({unsubscribe, From, TopicTable},
            State = #state{client_id     = ClientId,
                           username      = Username,
                           subscriptions = Subscriptions}) ->
    ?LOG(info, "Unsubscribe ~p", [TopicTable], State),
    Subscriptions1 =
    lists:foldl(fun({Topic, Opts}, SubMap) ->
                Fastlane = lists:member(fastlane, Opts),
                case maps:find(Topic, SubMap) of
                    {ok, _Qos} ->
                        case Fastlane of
                            true  -> emqx:unsubscribe(Topic, From);
                            false -> emqx:unsubscribe(Topic, ClientId)
                        end,
                        emqx_hooks:run('session.unsubscribed', [ClientId, Username], {Topic, Opts}),
                        maps:remove(Topic, SubMap);
                    error ->
                        SubMap
                end
        end, Subscriptions, TopicTable),
    {noreply, emit_stats(State#state{subscriptions = Subscriptions1}), hibernate};

%% PUBACK:
handle_cast({puback, PacketId}, State = #state{inflight = Inflight}) ->
    {noreply,
     case Inflight:contain(PacketId) of
         true ->
             dequeue(acked(puback, PacketId, State));
         false ->
             ?LOG(warning, "PUBACK ~p missed inflight: ~p",
                  [PacketId, Inflight:window()], State),
             emqx_metrics:inc('packets/puback/missed'),
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
             emqx_metrics:inc('packets/pubrec/missed'),
             State
     end, hibernate};

%% PUBREL:
handle_cast({pubrel, PacketId}, State = #state{awaiting_rel = AwaitingRel}) ->
    {noreply,
     case maps:take(PacketId, AwaitingRel) of
         {Msg, AwaitingRel1} ->
             %% Implement Qos2 by method A [MQTT 4.33]
             %% Dispatch to subscriber when received PUBREL
             emqx_broker:publish(Msg), %% FIXME:
             gc(State#state{awaiting_rel = AwaitingRel1});
         error ->
             ?LOG(warning, "Cannot find PUBREL: ~p", [PacketId], State),
             emqx_metrics:inc('packets/pubrel/missed'),
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
             emqx_metrics:inc('packets/pubcomp/missed'),
             State
     end, hibernate};

%% RESUME:
handle_cast({resume, ClientId, ClientPid},
            State = #state{client_id       = ClientId,
                           client_pid      = OldClientPid,
                           clean_start     = CleanStart,
                           retry_timer     = RetryTimer,
                           await_rel_timer = AwaitTimer,
                           expiry_timer    = ExpireTimer}) ->

    ?LOG(info, "Resumed by ~p", [ClientPid], State),

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
    {noreply, emit_stats(dequeue(retry_delivery(true, State1)))};

handle_cast(Msg, State) ->
    lager:error("[~s] Unexpected Cast: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% Ignore Messages delivered by self
handle_info({dispatch, _Topic, #message{from = {ClientId, _}}},
             State = #state{client_id = ClientId, ignore_loop_deliver = true}) ->
    {noreply, State};

%% Dispatch Message
handle_info({dispatch, Topic, Msg}, State) when is_record(Msg, message) ->
    {noreply, gc(dispatch(tune_qos(Topic, reset_dup(Msg), State), State))};

%% Do nothing if the client has been disconnected.
handle_info({timeout, _Timer, retry_delivery}, State = #state{client_pid = undefined}) ->
    {noreply, emit_stats(State#state{retry_timer = undefined})};

handle_info({timeout, _Timer, retry_delivery}, State) ->
    {noreply, emit_stats(retry_delivery(false, State#state{retry_timer = undefined}))};

handle_info({timeout, _Timer, check_awaiting_rel}, State) ->
    {noreply, expire_awaiting_rel(emit_stats(State#state{await_rel_timer = undefined}))};

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
    ExpireTimer = start_timer(Interval, expired),
    State1 = State#state{client_pid = undefined, expiry_timer = ExpireTimer},
    {noreply, emit_stats(State1), hibernate};

handle_info({'EXIT', Pid, _Reason}, State = #state{old_client_pid = Pid}) ->
    %%ignore
    {noreply, State, hibernate};

handle_info({'EXIT', Pid, Reason}, State = #state{client_pid = ClientPid}) ->

    ?LOG(error, "Unexpected EXIT: client_pid=~p, exit_pid=~p, reason=~p",
         [ClientPid, Pid, Reason], State),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    lager:error("[~s] Unexpected Info: ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(Reason, #state{client_id = ClientId, username = Username}) ->

    emqx_hooks:run('session.terminated', [ClientId, Username, Reason]),
    emqx_sm:unregister_session(#session{sid = ClientId, pid = self()}).

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
                {publish, Msg = #message{headers = #{packet_id := PacketId}}} ->
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

expire_awaiting_rel([{PacketId, Msg = #message{timestamp = TS}} | Msgs],
                    Now, State = #state{awaiting_rel      = AwaitingRel,
                                        await_rel_timeout = Timeout}) ->
    case (timer:now_diff(Now, TS) div 1000) of
        Diff when Diff >= Timeout ->
            ?LOG(warning, "Dropped Qos2 Message for await_rel_timeout: ~p", [Msg], State),
            emqx_metrics:inc('messages/qos2/dropped'),
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
    fun({_, #message{timestamp = Ts1}},
        {_, #message{timestamp = Ts2}}) ->
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
dispatch(Msg, State = #state{client_id = ClientId, client_pid = undefined}) ->
    case emqx_hooks:run('message.dropped', [ClientId, Msg]) of
        ok   -> enqueue_msg(Msg, State);
        stop -> State
    end;

%% Deliver qos0 message directly to client
dispatch(Msg = #message{qos = ?QOS0}, State) ->
    deliver(Msg, State), State;

dispatch(Msg = #message{qos = QoS},
         State = #state{next_msg_id = MsgId, inflight = Inflight})
    when QoS =:= ?QOS1 orelse QoS =:= ?QOS2 ->
    case Inflight:is_full() of
        true  ->
            enqueue_msg(Msg, State);
        false ->
            Msg1 = emqx_message:set_header(packet_id, MsgId, Msg),
            deliver(Msg1, State),
            await(Msg1, next_msg_id(State))
    end.

enqueue_msg(Msg, State = #state{mqueue = Q}) ->
    inc_stats(enqueue_msg),
    State#state{mqueue = ?MQueue:in(Msg, Q)}.

%%--------------------------------------------------------------------
%% Deliver
%%--------------------------------------------------------------------

redeliver(Msg = #message{qos = QoS}, State) ->
    deliver(if QoS =:= ?QOS2 -> Msg; true -> emqx_message:set_flag(dup, Msg) end, State);

redeliver({pubrel, PacketId}, #state{client_pid = Pid}) ->
    Pid ! {redeliver, {?PUBREL, PacketId}}.

deliver(Msg, #state{client_pid = Pid, binding = local}) ->
    inc_stats(deliver_msg), Pid ! {deliver, Msg};
deliver(Msg, #state{client_pid = Pid, binding = remote}) ->
    inc_stats(deliver_msg), emqx_rpc:cast(node(Pid), erlang, send, [Pid, {deliver, Msg}]).

%%--------------------------------------------------------------------
%% Awaiting ACK for QoS1/QoS2 Messages
%%--------------------------------------------------------------------

await(Msg = #message{headers = #{packet_id := PacketId}},
      State = #state{inflight       = Inflight,
                     retry_timer    = RetryTimer,
                     retry_interval = Interval}) ->
    %% Start retry timer if the Inflight is still empty
    State1 = case RetryTimer == undefined of
                 true  -> State#state{retry_timer = start_timer(Interval, retry_delivery)};
                 false -> State
             end,
    State1#state{inflight = Inflight:insert(PacketId, {publish, Msg, os:timestamp()})}.

acked(puback, PacketId, State = #state{client_id = ClientId,
                                       username  = Username,
                                       inflight  = Inflight}) ->
    case Inflight:lookup(PacketId) of
        {publish, Msg, _Ts} ->
            emqx_hooks:run('message.acked', [ClientId, Username], Msg),
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
            emqx_hooks:run('message.acked', [ClientId, Username], Msg),
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

tune_qos(Topic, Msg = #message{qos = PubQoS},
         #state{subscriptions = SubMap, upgrade_qos = UpgradeQoS}) ->
    case maps:find(Topic, SubMap) of
        {ok, SubQoS} when UpgradeQoS andalso (SubQoS > PubQoS) ->
            Msg#message{qos = SubQoS};
        {ok, SubQoS} when (not UpgradeQoS) andalso (SubQoS < PubQoS) ->
            Msg#message{qos = SubQoS};
        {ok, _} ->
            Msg;
        error ->
            Msg
    end.

%%--------------------------------------------------------------------
%% Reset Dup
%%--------------------------------------------------------------------

reset_dup(Msg) ->
    emqx_message:unset_flag(dup, Msg).

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
    Session = #session{sid = ClientId, pid = self()},
    emqx_sm_stats:set_session_stats(Session, stats(State)),
    State.

inc_stats(Key) -> put(Key, get(Key) + 1).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

shutdown(Reason, State) ->
    {stop, {shutdown, Reason}, State}.

gc(State) ->
    State.
    %%emqx_gc:maybe_force_gc(#state.force_gc_count, State).

