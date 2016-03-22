%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc Session for persistent MQTT client.
%%
%% Session State in the broker consists of:
%%
%% 1. The Client’s subscriptions.
%%
%% 2. inflight qos1/2 messages sent to the client but unacked, QoS 1 and QoS 2
%%    messages which have been sent to the Client, but have not been completely
%%    acknowledged.
%%
%% 3. inflight qos2 messages received from client and waiting for pubrel. QoS 2
%%    messages which have been received from the Client, but have not been
%%    completely acknowledged.
%%
%% 4. all qos1, qos2 messages published to when client is disconnected.
%%    QoS 1 and QoS 2 messages pending transmission to the Client.
%%
%% 5. Optionally, QoS 0 messages pending transmission to the Client.
%%
%% State of Message:  newcome, inflight, pending
%%
%% @end

-module(emqttd_session).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

-behaviour(gen_server2).

-import(proplists, [get_value/2, get_value/3]).

%% Session API
-export([start_link/3, resume/3, info/1, destroy/2]).

%% PubSub APIs
-export([publish/2, puback/2, pubrec/2, pubrel/2, pubcomp/2,
         subscribe/2, subscribe/3, unsubscribe/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% gen_server2 Message Priorities
-export([prioritise_call/4, prioritise_cast/3, prioritise_info/3]).

-record(session, {

        %% Clean Session Flag
        clean_sess = true,

        %% ClientId: Identifier of Session
        client_id   :: binary(),

        %% Client Pid bind with session
        client_pid  :: pid(),

        %% Old Client Pid that has been kickout
        old_client_pid :: pid(),

        %% Last packet id of the session
        packet_id = 1,
        
        %% Client’s subscriptions.
        subscriptions :: dict:dict(),

        %% Inflight qos1, qos2 messages sent to the client but unacked,
        %% QoS 1 and QoS 2 messages which have been sent to the Client,
        %% but have not been completely acknowledged.
        %% Client <- Broker
        inflight_queue :: list(),

        max_inflight = 0,

        %% All qos1, qos2 messages published to when client is disconnected.
        %% QoS 1 and QoS 2 messages pending transmission to the Client.
        %%
        %% Optionally, QoS 0 messages pending transmission to the Client.
        message_queue  :: emqttd_mqueue:mqueue(),

        %% Inflight qos2 messages received from client and waiting for pubrel.
        %% QoS 2 messages which have been received from the Client,
        %% but have not been completely acknowledged.
        %% Client -> Broker
        awaiting_rel  :: map(),

        %% Awaiting PUBREL timeout
        await_rel_timeout = 8,

        %% Max Packets that Awaiting PUBREL
        max_awaiting_rel = 100,

        %% Awaiting timers for ack, rel.
        awaiting_ack  :: map(),

        %% Retry interval for redelivering QoS1/2 messages
        retry_interval = 20,

        %% Awaiting for PUBCOMP
        awaiting_comp :: map(),

        %% session expired after 48 hours
        expired_after = 172800,

        expired_timer,

        collect_interval,

        collect_timer,
        
        timestamp}).

-define(PUBSUB_TIMEOUT, 60000).

-define(LOG(Level, Format, Args, State),
            lager:Level([{client, State#session.client_id}],
                        "Session(~s): " ++ Format, [State#session.client_id | Args])).

%% @doc Start a session.
-spec(start_link(boolean(), mqtt_client_id(), pid()) -> {ok, pid()} | {error, any()}).
start_link(CleanSess, ClientId, ClientPid) ->
    gen_server2:start_link(?MODULE, [CleanSess, ClientId, ClientPid], []).

%% @doc Resume a session.
-spec(resume(pid(), mqtt_client_id(), pid()) -> ok).
resume(SessPid, ClientId, ClientPid) ->
    gen_server2:cast(SessPid, {resume, ClientId, ClientPid}).

%% @doc Session Info.
info(SessPid) ->
    gen_server2:call(SessPid, info).

%% @doc Destroy a session.
-spec(destroy(pid(), mqtt_client_id()) -> ok).
destroy(SessPid, ClientId) ->
    gen_server2:cast(SessPid, {destroy, ClientId}).

%%--------------------------------------------------------------------
%% PubSub
%%--------------------------------------------------------------------

%% @doc Subscribe Topics
-spec(subscribe(pid(), [{binary(), mqtt_qos()}]) -> ok).
subscribe(SessPid, TopicTable) ->
    gen_server2:cast(SessPid, {subscribe, TopicTable, fun(_) -> ok end}).

-spec(subscribe(pid(), mqtt_packet_id(), [{binary(), mqtt_qos()}]) -> ok).
subscribe(SessPid, PacketId, TopicTable) ->
    From   = self(),
    AckFun = fun(GrantedQos) ->
               From ! {suback, PacketId, GrantedQos}
             end,
    gen_server2:cast(SessPid, {subscribe, TopicTable, AckFun}).

%% @doc Publish message
-spec(publish(pid(), mqtt_message()) -> ok | {error, any()}).
publish(_SessPid, Msg = #mqtt_message{qos = ?QOS_0}) ->
    %% publish qos0 directly
    emqttd:publish(Msg);

publish(_SessPid, Msg = #mqtt_message{qos = ?QOS_1}) ->
    %% publish qos1 directly, and client will puback automatically
    emqttd:publish(Msg);

publish(SessPid, Msg = #mqtt_message{qos = ?QOS_2}) ->
    %% publish qos2 by session 
    gen_server2:call(SessPid, {publish, Msg}, ?PUBSUB_TIMEOUT).

%% @doc PubAck message
-spec(puback(pid(), mqtt_packet_id()) -> ok).
puback(SessPid, PktId) ->
    gen_server2:cast(SessPid, {puback, PktId}).

-spec(pubrec(pid(), mqtt_packet_id()) -> ok).
pubrec(SessPid, PktId) ->
    gen_server2:cast(SessPid, {pubrec, PktId}).

-spec(pubrel(pid(), mqtt_packet_id()) -> ok).
pubrel(SessPid, PktId) ->
    gen_server2:cast(SessPid, {pubrel, PktId}).

-spec(pubcomp(pid(), mqtt_packet_id()) -> ok).
pubcomp(SessPid, PktId) ->
    gen_server2:cast(SessPid, {pubcomp, PktId}).

%% @doc Unsubscribe Topics
-spec(unsubscribe(pid(), [binary()]) -> ok).
unsubscribe(SessPid, Topics) ->
    gen_server2:cast(SessPid, {unsubscribe, Topics}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([CleanSess, ClientId, ClientPid]) ->
    process_flag(trap_exit, true),
    true    = link(ClientPid),
    QEnv    = emqttd:env(mqtt, queue),
    SessEnv = emqttd:env(mqtt, session),
    Session = #session{
            clean_sess        = CleanSess,
            client_id         = ClientId,
            client_pid        = ClientPid,
            subscriptions     = dict:new(),
            inflight_queue    = [],
            max_inflight      = get_value(max_inflight, SessEnv, 0),
            message_queue     = emqttd_mqueue:new(ClientId, QEnv, emqttd_alarm:alarm_fun()),
            awaiting_rel      = #{},
            awaiting_ack      = #{},
            awaiting_comp     = #{},
            retry_interval    = get_value(unack_retry_interval, SessEnv),
            await_rel_timeout = get_value(await_rel_timeout, SessEnv),
            max_awaiting_rel  = get_value(max_awaiting_rel, SessEnv),
            expired_after     = get_value(expired_after, SessEnv) * 60,
            collect_interval  = get_value(collect_interval, SessEnv, 0),
            timestamp         = os:timestamp()},
    emqttd_sm:register_session(CleanSess, ClientId, sess_info(Session)),
    %% start statistics
    {ok, start_collector(Session), hibernate}.

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of 
        info -> 10;
        _    -> 0
    end.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {destroy, _}        -> 10;
        {resume, _, _}      -> 9;
        {pubrel,  _PktId}   -> 8;
        {pubcomp, _PktId}   -> 8;
        {pubrec,  _PktId}   -> 8;
        {puback,  _PktId}   -> 7;
        {unsubscribe, _, _} -> 6;
        {subscribe, _, _}   -> 5;
        _                   -> 0
    end.

prioritise_info(Msg, _Len, _State) ->
    case Msg of
        {'EXIT', _, _}   -> 10;
        expired          -> 10;
        {timeout, _, _}  -> 5;
        collect_info     -> 2;
        {dispatch, _, _} -> 1;
        _                -> 0
    end.

handle_call(info, _From, State) ->
    {reply, sess_info(State), State, hibernate};

handle_call({publish, Msg = #mqtt_message{qos = ?QOS_2, pktid = PktId}},
            _From, Session = #session{awaiting_rel      = AwaitingRel,
                                      await_rel_timeout = Timeout}) ->
    case check_awaiting_rel(Session) of
        true ->
            TRef = timer(Timeout, {timeout, awaiting_rel, PktId}),
            AwaitingRel1 = maps:put(PktId, {Msg, TRef}, AwaitingRel),
            {reply, ok, Session#session{awaiting_rel = AwaitingRel1}};
        false ->
            ?LOG(critical, "Dropped Qos2 message for too many awaiting_rel: ~p", [Msg], Session),
            {reply, {error, dropped}, Session, hibernate}
    end;

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast({subscribe, TopicTable0, AckFun}, Session = #session{client_id     = ClientId,
                                                                 subscriptions = Subscriptions}) ->

    case emqttd:run_hooks('client.subscribe', [ClientId], TopicTable0) of
        {ok, TopicTable} ->
            ?LOG(info, "Subscribe ~p", [TopicTable], Session),
            Subscriptions1 = lists:foldl(
                fun({Topic, Qos}, SubDict) ->
                    case dict:find(Topic, SubDict) of
                        {ok, Qos} ->
                            ?LOG(warning, "duplicated subscribe: ~s, qos = ~w", [Topic, Qos], Session),
                            SubDict;
                        {ok, OldQos} ->
                            emqttd_server:update_subscription(ClientId, Topic, OldQos, Qos),
                            ?LOG(warning, "duplicated subscribe ~s, old_qos=~w, new_qos=~w", [Topic, OldQos, Qos], Session),
                            dict:store(Topic, Qos, SubDict);
                        error ->
                            emqttd:subscribe(ClientId, Topic, Qos),
                            %%TODO: the design is ugly...
                            %% <MQTT V3.1.1>: 3.8.4
                            %% Where the Topic Filter is not identical to any existing Subscription’s filter,
                            %% a new Subscription is created and all matching retained messages are sent.
                            emqttd_retainer:dispatch(Topic, self()),

                            dict:store(Topic, Qos, SubDict)
                    end
                end, Subscriptions, TopicTable),
            AckFun([Qos || {_, Qos} <- TopicTable]),
            emqttd:run_hooks('client.subscribe.after', [ClientId], TopicTable),
            hibernate(Session#session{subscriptions = Subscriptions1});
        {stop, TopicTable} ->
            ?LOG(error, "Cannot subscribe: ~p", [TopicTable], Session),
            hibernate(Session)
    end;

handle_cast({unsubscribe, Topics0}, Session = #session{client_id     = ClientId,
                                                       subscriptions = Subscriptions}) ->

    case emqttd:run_hooks('client.unsubscribe', [ClientId], Topics0) of
        {ok, Topics} ->
            ?LOG(info, "unsubscribe ~p", [Topics], Session),
            Subscriptions1 = lists:foldl(
                fun(Topic, SubDict) ->
                    case dict:find(Topic, SubDict) of
                        {ok, Qos} ->
                            emqttd:unsubscribe(ClientId, Topic, Qos),
                            dict:erase(Topic, SubDict);
                        error ->
                            SubDict
                    end
                end, Subscriptions, Topics),
            hibernate(Session#session{subscriptions = Subscriptions1});
        {stop, Topics} ->
            ?LOG(info, "Cannot unsubscribe: ~p", [Topics], Session),
            hibernate(Session)
    end;

handle_cast({destroy, ClientId}, Session = #session{client_id = ClientId}) ->
    ?LOG(warning, "destroyed", [], Session),
    shutdown(destroy, Session);

handle_cast({resume, ClientId, ClientPid}, Session = #session{client_id      = ClientId,
                                                              client_pid     = OldClientPid,
                                                              clean_sess     = CleanSess,
                                                              inflight_queue = InflightQ,
                                                              awaiting_ack   = AwaitingAck,
                                                              awaiting_comp  = AwaitingComp,
                                                              expired_timer  = ETimer} = Session) ->

    ?LOG(info, "resumed by ~p", [ClientPid], Session),

    %% Cancel expired timer
    cancel_timer(ETimer),

    case kick(ClientId, OldClientPid, ClientPid) of
        ok -> ?LOG(warning, "~p kickout ~p", [ClientPid, OldClientPid], Session);
        ignore -> ok
    end,

    true = link(ClientPid),

    %% Redeliver PUBREL
    [ClientPid ! {redeliver, {?PUBREL, PktId}} || PktId <- maps:keys(AwaitingComp)],

    %% Clear awaiting_ack timers
    [cancel_timer(TRef) || TRef <- maps:values(AwaitingAck)],

    %% Clear awaiting_comp timers
    [cancel_timer(TRef) || TRef <- maps:values(AwaitingComp)],

    Session1 = Session#session{client_pid     = ClientPid,
                               old_client_pid = OldClientPid,
                               clean_sess     = false,
                               awaiting_ack   = #{},
                               awaiting_comp  = #{},
                               expired_timer  = undefined},

    %% CleanSess: true -> false?
    if
        CleanSess =:= true  ->
            ?LOG(warning, "CleanSess changed to false.", [], Session),
            emqttd_sm:unregister_session(CleanSess, ClientId),
            emqttd_sm:register_session(false, ClientId, sess_info(Session1));
        CleanSess =:= false ->
            ok
    end,

    %% Redeliver inflight messages
    Session2 =
    lists:foldl(fun({_Id, Msg}, Sess) ->
            redeliver(Msg, Sess)
        end, Session1, lists:reverse(InflightQ)),

    %% Dequeue pending messages
    hibernate(dequeue(Session2));

%% PUBACK
handle_cast({puback, PktId}, Session = #session{awaiting_ack = AwaitingAck}) ->
    case maps:find(PktId, AwaitingAck) of
        {ok, TRef} ->
            cancel_timer(TRef),
            hibernate(dequeue(acked(PktId, Session)));
        error ->
            ?LOG(warning, "Cannot find PUBACK: ~p", [PktId], Session),
            hibernate(Session)
    end;

%% PUBREC
handle_cast({pubrec, PktId}, Session = #session{awaiting_ack      = AwaitingAck,
                                                awaiting_comp     = AwaitingComp,
                                                await_rel_timeout = Timeout}) ->
    case maps:find(PktId, AwaitingAck) of
        {ok, TRef} ->
            cancel_timer(TRef),
            TRef1 = timer(Timeout, {timeout, awaiting_comp, PktId}),
            AwaitingComp1 = maps:put(PktId, TRef1, AwaitingComp),
            Session1 = acked(PktId, Session#session{awaiting_comp = AwaitingComp1}),
            hibernate(dequeue(Session1));
        error ->
            ?LOG(error, "Cannot find PUBREC: ~p", [PktId], Session),
            hibernate(Session)
    end;

%% PUBREL
handle_cast({pubrel, PktId}, Session = #session{awaiting_rel = AwaitingRel}) ->
    case maps:find(PktId, AwaitingRel) of
        {ok, {Msg, TRef}} ->
            cancel_timer(TRef),
            emqttd:publish(Msg),
            hibernate(Session#session{awaiting_rel = maps:remove(PktId, AwaitingRel)});
        error ->
            ?LOG(error, "Cannot find PUBREL: ~p", [PktId], Session),
            hibernate(Session)
    end;

%% PUBCOMP
handle_cast({pubcomp, PktId}, Session = #session{awaiting_comp = AwaitingComp}) ->
    case maps:find(PktId, AwaitingComp) of
        {ok, TRef} ->
            cancel_timer(TRef),
            hibernate(Session#session{awaiting_comp = maps:remove(PktId, AwaitingComp)});
        error ->
            ?LOG(error, "Cannot find PUBCOMP: ~p", [PktId], Session),
            hibernate(Session)
    end;

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

%% Dispatch Message
handle_info({dispatch, Topic, Msg}, Session = #session{subscriptions = Subscriptions})
    when is_record(Msg, mqtt_message) ->
    dispatch(tune_qos(Topic, Msg, Subscriptions), Session);

handle_info({timeout, awaiting_ack, PktId}, Session = #session{client_pid = undefined,
                                                               awaiting_ack = AwaitingAck}) ->
    %% just remove awaiting
    hibernate(Session#session{awaiting_ack = maps:remove(PktId, AwaitingAck)});

handle_info({timeout, awaiting_ack, PktId}, Session = #session{inflight_queue = InflightQ,
                                                               awaiting_ack   = AwaitingAck}) ->
    case maps:find(PktId, AwaitingAck) of
        {ok, _TRef} ->
            case lists:keyfind(PktId, 1, InflightQ) of
                {_, Msg} ->
                    hibernate(redeliver(Msg, Session));
                false ->
                    ?LOG(error, "AwaitingAck timeout but Cannot find PktId: ~p", [PktId], Session),
                    hibernate(dequeue(Session))
                end;
        error ->
            ?LOG(error, "Cannot find AwaitingAck: ~p", [PktId], Session),
            hibernate(Session)
    end;

handle_info({timeout, awaiting_rel, PktId}, Session = #session{awaiting_rel = AwaitingRel}) ->
    case maps:find(PktId, AwaitingRel) of
        {ok, {_Msg, _TRef}} ->
            ?LOG(warning, "AwaitingRel Timout: ~p, Drop Message!", [PktId], Session),
            hibernate(Session#session{awaiting_rel = maps:remove(PktId, AwaitingRel)});
        error ->
            ?LOG(error, "Cannot find AwaitingRel: ~p", [PktId], Session),
            hibernate(Session)
    end;

handle_info({timeout, awaiting_comp, PktId}, Session = #session{awaiting_comp = Awaiting}) ->
    case maps:find(PktId, Awaiting) of
        {ok, _TRef} ->
            ?LOG(warning, "Awaiting PUBCOMP Timout: ~p", [PktId], Session),
            hibernate(Session#session{awaiting_comp = maps:remove(PktId, Awaiting)});
        error ->
            ?LOG(error, "Cannot find Awaiting PUBCOMP: ~p", [PktId], Session),
            hibernate(Session)
    end;

handle_info(collect_info, Session = #session{clean_sess = CleanSess, client_id = ClientId}) ->
    emqttd_sm:register_session(CleanSess, ClientId, sess_info(Session)),
    hibernate(start_collector(Session));

handle_info({'EXIT', ClientPid, _Reason}, Session = #session{clean_sess = true,
                                                             client_pid = ClientPid}) ->
    {stop, normal, Session};

handle_info({'EXIT', ClientPid, Reason}, Session = #session{clean_sess    = false,
                                                            client_pid    = ClientPid,
                                                            expired_after = Expires}) ->
    ?LOG(info, "Client ~p EXIT for ~p", [ClientPid, Reason], Session),
    TRef = timer(Expires, expired),
    hibernate(Session#session{client_pid = undefined, expired_timer = TRef});

handle_info({'EXIT', Pid, _Reason}, Session = #session{old_client_pid = Pid}) ->
    %%ignore
    hibernate(Session);

handle_info({'EXIT', Pid, Reason}, Session = #session{client_pid = ClientPid}) ->

    ?LOG(error, "Unexpected EXIT: client_pid=~p, exit_pid=~p, reason=~p",
         [ClientPid, Pid, Reason], Session),
    hibernate(Session);

handle_info(expired, Session) ->
    ?LOG(info, "expired, shutdown now.", [], Session),
    shutdown(expired, Session);

handle_info(Info, Session) ->
    ?UNEXPECTED_INFO(Info, Session).

terminate(_Reason, #session{clean_sess = CleanSess, client_id = ClientId}) ->
    emqttd_sm:unregister_session(CleanSess, ClientId).

code_change(_OldVsn, Session, _Extra) ->
    {ok, Session}.

%%--------------------------------------------------------------------
%% Kick old client out
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
%% Dispatch Messages
%%--------------------------------------------------------------------

%% Queue message if client disconnected
dispatch(Msg, Session = #session{client_pid = undefined, message_queue = Q}) ->
    hibernate(Session#session{message_queue = emqttd_mqueue:in(Msg, Q)});

%% Deliver qos0 message directly to client
dispatch(Msg = #mqtt_message{qos = ?QOS0}, Session = #session{client_pid = ClientPid}) ->
    ClientPid ! {deliver, Msg},
    hibernate(Session);

dispatch(Msg = #mqtt_message{qos = QoS}, Session = #session{message_queue = MsgQ})
    when QoS =:= ?QOS1 orelse QoS =:= ?QOS2 ->
    case check_inflight(Session) of
        true  ->
            noreply(deliver(Msg, Session));
        false ->
            hibernate(Session#session{message_queue = emqttd_mqueue:in(Msg, MsgQ)})
    end.

tune_qos(Topic, Msg = #mqtt_message{qos = PubQos}, Subscriptions) ->
    case dict:find(Topic, Subscriptions) of
        {ok, SubQos} when PubQos > SubQos ->
            Msg#mqtt_message{qos = SubQos};
        {ok, _SubQos} ->
            Msg;
        error ->
            Msg
    end.

%%--------------------------------------------------------------------
%% Check inflight and awaiting_rel
%%--------------------------------------------------------------------

check_inflight(#session{max_inflight = 0}) ->
     true;
check_inflight(#session{max_inflight = Max, inflight_queue = Q}) ->
    Max > length(Q).

check_awaiting_rel(#session{max_awaiting_rel = 0}) ->
    true;
check_awaiting_rel(#session{awaiting_rel     = AwaitingRel,
                            max_awaiting_rel = MaxLen}) ->
    maps:size(AwaitingRel) < MaxLen.

%%--------------------------------------------------------------------
%% Dequeue and Deliver
%%--------------------------------------------------------------------

dequeue(Session = #session{client_pid = undefined}) ->
    %% do nothing if client is disconnected
    Session;

dequeue(Session) ->
    case check_inflight(Session) of
        true  -> dequeue2(Session);
        false -> Session
    end.

dequeue2(Session = #session{message_queue = Q}) ->
    case emqttd_mqueue:out(Q) of
        {empty, _Q} ->
            Session;
        {{value, Msg}, Q1} ->
            %% dequeue more
            dequeue(deliver(Msg, Session#session{message_queue = Q1}))
    end.

deliver(Msg = #mqtt_message{qos = ?QOS_0}, Session = #session{client_pid = ClientPid}) ->
    ClientPid ! {deliver, Msg}, Session; 

deliver(Msg = #mqtt_message{qos = QoS}, Session = #session{packet_id = PktId,
                                                           client_pid = ClientPid,
                                                           inflight_queue = InflightQ})
    when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->
    Msg1 = Msg#mqtt_message{pktid = PktId, dup = false},
    ClientPid ! {deliver, Msg1},
    await(Msg1, next_packet_id(Session#session{inflight_queue = [{PktId, Msg1}|InflightQ]})).

redeliver(Msg = #mqtt_message{qos = ?QOS_0}, Session) ->
    deliver(Msg, Session); 

redeliver(Msg = #mqtt_message{qos = QoS}, Session = #session{client_pid = ClientPid})
    when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->
    ClientPid ! {deliver, Msg#mqtt_message{dup = true}},
    await(Msg, Session).

%%--------------------------------------------------------------------
%% Awaiting ack for qos1, qos2 message
%%------------------------------------------------------------------------------
await(#mqtt_message{pktid = PktId}, Session = #session{awaiting_ack   = Awaiting,
                                                       retry_interval = Timeout}) ->
    TRef = timer(Timeout, {timeout, awaiting_ack, PktId}),
    Awaiting1 = maps:put(PktId, TRef, Awaiting),
    Session#session{awaiting_ack = Awaiting1}.

acked(PktId, Session = #session{client_id      = ClientId,
                                inflight_queue = InflightQ,
                                awaiting_ack   = Awaiting}) ->
    case lists:keyfind(PktId, 1, InflightQ) of
        {_, Msg} ->
            emqttd:run_hooks('message.acked', [ClientId], Msg);
        false ->
            ?LOG(error, "Cannot find acked pktid: ~p", [PktId], Session)
    end,
    Session#session{awaiting_ack   = maps:remove(PktId, Awaiting),
                    inflight_queue = lists:keydelete(PktId, 1, InflightQ)}.

next_packet_id(Session = #session{packet_id = 16#ffff}) ->
    Session#session{packet_id = 1};

next_packet_id(Session = #session{packet_id = Id}) ->
    Session#session{packet_id = Id + 1}.

timer(TimeoutSec, TimeoutMsg) ->
    erlang:send_after(timer:seconds(TimeoutSec), self(), TimeoutMsg).

cancel_timer(undefined) -> 
    undefined;
cancel_timer(Ref) -> 
    catch erlang:cancel_timer(Ref).

noreply(State) ->
    {noreply, State}.

hibernate(State) ->
    {noreply, State, hibernate}.

shutdown(Reason, State) ->
    {stop, {shutdown, Reason}, State}.

start_collector(Session = #session{collect_interval = 0}) ->
    Session;

start_collector(Session = #session{collect_interval = Interval}) ->
    TRef = erlang:send_after(timer:seconds(Interval), self(), collect_info),
    Session#session{collect_timer = TRef}.

sess_info(#session{clean_sess      = CleanSess,
                   inflight_queue  = InflightQueue,
                   max_inflight    = MaxInflight,
                   message_queue   = MessageQueue,
                   awaiting_rel    = AwaitingRel,
                   awaiting_ack    = AwaitingAck,
                   awaiting_comp   = AwaitingComp,
                   timestamp       = CreatedAt}) ->
    Stats = emqttd_mqueue:stats(MessageQueue),
    [{clean_sess,     CleanSess},
     {max_inflight,   MaxInflight},
     {inflight_queue, length(InflightQueue)},
     {message_queue,  get_value(len, Stats)},
     {message_dropped,get_value(dropped, Stats)},
     {awaiting_rel,   maps:size(AwaitingRel)},
     {awaiting_ack,   maps:size(AwaitingAck)},
     {awaiting_comp,  maps:size(AwaitingComp)},
     {created_at,     CreatedAt}].

