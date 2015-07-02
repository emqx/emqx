%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%%
%%% Session for persistent MQTT client.
%%%
%%% Session State in the broker consists of:
%%%
%%% 1. The Client’s subscriptions.
%%%
%%% 2. inflight qos1/2 messages sent to the client but unacked, QoS 1 and QoS 2
%%%    messages which have been sent to the Client, but have not been completely
%%%    acknowledged.
%%%
%%% 3. inflight qos2 messages received from client and waiting for pubrel. QoS 2
%%%    messages which have been received from the Client, but have not been
%%%    completely acknowledged.
%%%
%%% 4. all qos1, qos2 messages published to when client is disconnected.
%%%    QoS 1 and QoS 2 messages pending transmission to the Client.
%%%
%%% 5. Optionally, QoS 0 messages pending transmission to the Client.
%%%
%%% State of Message:  newcome, inflight, pending
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_session).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

%% Session API
-export([start_link/3, resume/3, destroy/2]).

%% PubSub APIs
-export([publish/2,
         puback/2, pubrec/2, pubrel/2, pubcomp/2,
         subscribe/2, unsubscribe/2]).

-behaviour(gen_server).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(session, {

        %% Clean Session Flag
        clean_sess = true,

        %% ClientId: Identifier of Session
        client_id   :: binary(),

        %% Client Pid linked with session
        client_pid  :: pid(),

        %% Last message id of the session
		message_id = 1,
        
        %% Client’s subscriptions.
        subscriptions :: list(),

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

        %% Awaiting timers for ack, rel and comp.
        awaiting_ack  :: map(),

        %% Retries to resend the unacked messages
        unack_retries = 3,

        %% 4, 8, 16 seconds if 3 retries:)
        unack_timeout = 4,

        %% Awaiting for PUBCOMP
        awaiting_comp :: map(),

        %% session expired after 48 hours
        expired_after = 172800,

        expired_timer,
        
        timestamp}).

%%------------------------------------------------------------------------------
%% @doc Start a session.
%% @end
%%------------------------------------------------------------------------------
-spec start_link(boolean(), mqtt_client_id(), pid()) -> {ok, pid()} | {error, any()}.
start_link(CleanSess, ClientId, ClientPid) ->
    gen_server:start_link(?MODULE, [CleanSess, ClientId, ClientPid], []).

%%------------------------------------------------------------------------------
%% @doc Resume a session.
%% @end
%%------------------------------------------------------------------------------
-spec resume(pid(), mqtt_client_id(), pid()) -> ok.
resume(Session, ClientId, ClientPid) ->
    gen_server:cast(Session, {resume, ClientId, ClientPid}).

%%------------------------------------------------------------------------------
%% @doc Destroy a session.
%% @end
%%------------------------------------------------------------------------------
-spec destroy(pid(), mqtt_client_id()) -> ok.
destroy(Session, ClientId) ->
    gen_server:call(Session, {destroy, ClientId}).

%%------------------------------------------------------------------------------
%% @doc Subscribe Topics
%% @end
%%------------------------------------------------------------------------------
-spec subscribe(pid(), [{binary(), mqtt_qos()}]) -> {ok, [mqtt_qos()]}.
subscribe(Session, TopicTable) ->
    gen_server:call(Session, {subscribe, TopicTable}).

%%------------------------------------------------------------------------------
%% @doc Publish message
%% @end
%%------------------------------------------------------------------------------
-spec publish(pid(), mqtt_message()) -> ok.
publish(_Session, Msg = #mqtt_message{qos = ?QOS_0}) ->
    %% publish qos0 directly
    emqttd_pubsub:publish(Msg);

publish(_Session, Msg = #mqtt_message{qos = ?QOS_1}) ->
    %% publish qos1 directly, and client will puback automatically
	emqttd_pubsub:publish(Msg);

publish(Session, Msg = #mqtt_message{qos = ?QOS_2}) ->
    %% publish qos2 by session 
    gen_server:call(Session, {publish, Msg}).

%%------------------------------------------------------------------------------
%% @doc PubAck message
%% @end
%%------------------------------------------------------------------------------
-spec puback(pid(), mqtt_msgid()) -> ok.
puback(Session, MsgId) ->
    gen_server:cast(Session, {puback, MsgId}).

-spec pubrec(pid(), mqtt_msgid()) -> ok.
pubrec(Session, MsgId) ->
    gen_server:cast(Session, {pubrec, MsgId}).

-spec pubrel(pid(), mqtt_msgid()) -> ok.
pubrel(Session, MsgId) ->
    gen_server:cast(Session, {pubrel, MsgId}).

-spec pubcomp(pid(), mqtt_msgid()) -> ok.
pubcomp(Session, MsgId) ->
    gen_server:cast(Session, {pubcomp, MsgId}).

%%------------------------------------------------------------------------------
%% @doc Unsubscribe Topics
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(pid(), [binary()]) -> ok.
unsubscribe(Session, Topics) ->
    gen_server:call(Session, {unsubscribe, Topics}).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([CleanSess, ClientId, ClientPid]) ->
    process_flag(trap_exit, true),
    true = link(ClientPid),
    QEnv = emqttd:env(mqtt, queue),
    SessEnv = emqttd:env(mqtt, session),
    Session = #session{
            clean_sess        = CleanSess,
            client_id         = ClientId,
            client_pid        = ClientPid,
            subscriptions     = [],
            inflight_queue    = [],
            max_inflight      = emqttd_opts:g(max_inflight, SessEnv, 0),
            message_queue     = emqttd_mqueue:new(ClientId, QEnv, emqttd_alarm:alarm_fun()),
            awaiting_rel      = #{},
            awaiting_ack      = #{},
            awaiting_comp     = #{},
            unack_retries     = emqttd_opts:g(unack_retries, SessEnv),
            unack_timeout     = emqttd_opts:g(unack_timeout, SessEnv),
            await_rel_timeout = emqttd_opts:g(await_rel_timeout, SessEnv),
            max_awaiting_rel  = emqttd_opts:g(max_awaiting_rel, SessEnv),
            expired_after     = emqttd_opts:g(expired_after, SessEnv) * 3600,
            timestamp         = os:timestamp()},
    {ok, Session, hibernate}.

handle_call({subscribe, Topics}, _From, Session = #session{client_id = ClientId,
                                                           subscriptions = Subscriptions}) ->

    %% subscribe first and don't care if the subscriptions have been existed
    {ok, GrantedQos} = emqttd_pubsub:subscribe(Topics),

    lager:info([{client, ClientId}], "Session ~s subscribe ~p, Granted QoS: ~p",
                [ClientId, Topics, GrantedQos]),

    Subscriptions1 =
    lists:foldl(fun({Topic, Qos}, Acc) ->
                    case lists:keyfind(Topic, 1, Acc) of
                        {Topic, Qos} ->
                            lager:warning([{client, ClientId}], "Session ~s "
                                            "resubscribe ~p: qos = ~p", [ClientId, Topic, Qos]), Acc;
                        {Topic, OldQos} ->
                            lager:warning([{client, ClientId}], "Session ~s "
                                            "resubscribe ~p: old qos=~p, new qos=~p", [ClientId, Topic, OldQos, Qos]),
                            lists:keyreplace(Topic, 1, Acc, {Topic, Qos});
                        false ->
                            %%TODO: the design is ugly, rewrite later...:(
                            %% <MQTT V3.1.1>: 3.8.4
                            %% Where the Topic Filter is not identical to any existing Subscription’s filter,
                            %% a new Subscription is created and all matching retained messages are sent.
                            emqttd_msg_store:redeliver(Topic, self()),
                            [{Topic, Qos} | Acc]
                    end
                end, Subscriptions, Topics),
    {reply, {ok, GrantedQos}, Session#session{subscriptions = Subscriptions1}};

handle_call({unsubscribe, Topics}, _From, Session = #session{client_id = ClientId,
                                                             subscriptions = Subscriptions}) ->

    %% unsubscribe from topic tree
    ok = emqttd_pubsub:unsubscribe(Topics),

    lager:info([{client, ClientId}], "Session ~s unsubscribe ~p", [ClientId, Topics]),

    Subscriptions1 =
    lists:foldl(fun(Topic, Acc) ->
                    case lists:keyfind(Topic, 1, Acc) of
                        {Topic, _Qos} ->
                            lists:keydelete(Topic, 1, Acc);
                        false ->
                            lager:warning([{client, ClientId}], "Session ~s not subscribe ~s", [ClientId, Topic]), Acc
                    end
                end, Subscriptions, Topics),

    {reply, ok, Session#session{subscriptions = Subscriptions1}};

handle_call({publish, Msg = #mqtt_message{qos = ?QOS_2, msgid = MsgId}}, _From, 
            Session = #session{client_id = ClientId,
                               awaiting_rel = AwaitingRel,
                               await_rel_timeout = Timeout}) ->
    case check_awaiting_rel(Session) of
        true ->
            TRef = timer(Timeout, {timeout, awaiting_rel, MsgId}),
            AwaitingRel1 = maps:put(MsgId, {Msg, TRef}, AwaitingRel),
            {reply, ok, Session#session{awaiting_rel = AwaitingRel1}};
        false ->
            lager:critical([{client, ClientId}], "Session ~s dropped Qos2 message "
                                "for too many awaiting_rel: ~p", [ClientId, Msg]),
            {reply, {error, dropped}, Session}
    end;

handle_call({destroy, ClientId}, _From, Session = #session{client_id = ClientId}) ->
    lager:warning("Session ~s destroyed", [ClientId]),
    {stop, {shutdown, destroy}, ok, Session};

handle_call(Req, _From, State) ->
    lager:critical("Unexpected Request: ~p", [Req]),
    {reply, {error, badreq}, State}.

handle_cast({resume, ClientId, ClientPid}, Session) ->

    #session{client_id       = ClientId,
             client_pid     = OldClientPid,
             inflight_queue = InflightQ,
             awaiting_ack   = AwaitingAck,
             awaiting_comp  = AwaitingComp,
             expired_timer  = ETimer} = Session,

    lager:info([{client, ClientId}], "Session ~s resumed by ~p",[ClientId, ClientPid]),

    %% cancel expired timer
    cancel_timer(ETimer),

    kick(ClientId, ClientPid, OldClientPid),

    true = link(ClientPid),

    %% Redeliver PUBREL
    [ClientPid ! {redeliver, {?PUBREL, MsgId}} || MsgId <- maps:keys(AwaitingComp)],

    %% Clear awaiting_ack timers
    [cancel_timer(TRef) || {_, TRef} <- maps:values(AwaitingAck)],

    %% Clear awaiting_comp timers
    [cancel_timer(TRef) || TRef <- maps:values(AwaitingComp)],

    Session1 = Session#session{client_pid    = ClientPid,
                               awaiting_ack  = #{},
                               awaiting_comp = #{},
                               expired_timer = undefined},

    %% Redeliver inflight messages
    Session2 =
    lists:foldl(fun({_Id, Msg}, Sess) ->
            redeliver(Msg#mqtt_message{dup = true}, Sess)
        end, Session1, lists:reverse(InflightQ)),

    %% Dequeue pending messages
    {noreply, dequeue(Session2), hibernate};

%% PUBRAC
handle_cast({puback, MsgId}, Session = #session{client_id = ClientId, awaiting_ack = Awaiting}) ->
    case maps:find(MsgId, Awaiting) of
        {ok, {_, TRef}} ->
            cancel_timer(TRef),
            Session1 = acked(MsgId, Session),
            {noreply, dequeue(Session1)};
        error ->
            lager:error("Session ~s cannot find PUBACK '~p'!", [ClientId, MsgId]),
            {noreply, Session}
    end;

%% PUBREC
handle_cast({pubrec, MsgId}, Session = #session{client_id = ClientId,
                                                awaiting_ack = AwaitingAck,
                                                awaiting_comp = AwaitingComp,
                                                await_rel_timeout = Timeout}) ->
    case maps:find(MsgId, AwaitingAck) of
        {ok, {_, TRef}} ->
            cancel_timer(TRef),
            TRef1 = timer(Timeout, {timeout, awaiting_comp, MsgId}),
            Session1 = acked(MsgId, Session#session{awaiting_comp = maps:put(MsgId, TRef1, AwaitingComp)}),
            {noreply, dequeue(Session1)};
        error ->
            lager:error("Session ~s cannot find PUBREC '~p'!", [ClientId, MsgId]),
            {noreply, Session}
    end;

%% PUBREL
handle_cast({pubrel, MsgId}, Session = #session{client_id = ClientId,
                                                awaiting_rel = AwaitingRel}) ->
    case maps:find(MsgId, AwaitingRel) of
        {ok, {Msg, TRef}} ->
            cancel_timer(TRef),
            emqttd_pubsub:publish(Msg),
            {noreply, Session#session{awaiting_rel = maps:remove(MsgId, AwaitingRel)}};
        error ->
            lager:error("Session ~s cannot find PUBREL: msgid=~p!", [ClientId, MsgId]),
            {noreply, Session}
    end;

%% PUBCOMP
handle_cast({pubcomp, MsgId}, Session = #session{client_id = ClientId, awaiting_comp = AwaitingComp}) ->
    case maps:find(MsgId, AwaitingComp) of
        {ok, TRef} ->
            cancel_timer(TRef),
            {noreply, Session#session{awaiting_comp = maps:remove(MsgId, AwaitingComp)}};
        error ->
            lager:error("Session ~s cannot find PUBCOMP: MsgId=~p", [ClientId, MsgId]),
            {noreply, Session}
    end;

handle_cast(Msg, State) ->
    lager:critical("Unexpected Msg: ~p, State: ~p", [Msg, State]),
    {noreply, State}.

%% Queue messages when client is offline
handle_info({dispatch, Msg}, Session = #session{client_pid = undefined,
                                                message_queue = Q})
    when is_record(Msg, mqtt_message) ->
    {noreply, Session#session{message_queue = emqttd_mqueue:in(Msg, Q)}};

%% Dispatch qos0 message directly to client
handle_info({dispatch, Msg = #mqtt_message{qos = ?QOS_0}},
            Session = #session{client_pid = ClientPid}) ->
    ClientPid ! {deliver, Msg}, 
    {noreply, Session};

handle_info({dispatch, Msg = #mqtt_message{qos = QoS}},
            Session = #session{client_id = ClientId, message_queue  = MsgQ})
    when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->

    case check_inflight(Session) of
        true ->
            {noreply, deliver(Msg, Session)};
        false ->
            lager:warning([{client, ClientId}], "Session ~s inflight queue is full!", [ClientId]),
            {noreply, Session#session{message_queue = emqttd_mqueue:in(Msg, MsgQ)}}
    end;

handle_info({timeout, awaiting_ack, MsgId}, Session = #session{client_pid = undefined,
                                                               awaiting_ack = AwaitingAck}) ->
    %% just remove awaiting
    {noreply, Session#session{awaiting_ack = maps:remove(MsgId, AwaitingAck)}};

handle_info({timeout, awaiting_ack, MsgId}, Session = #session{client_id = ClientId,
                                                               inflight_queue = InflightQ,
                                                               awaiting_ack = AwaitingAck}) ->
    case maps:find(MsgId, AwaitingAck) of
        {ok, {{0, _Timeout}, _TRef}} ->
            Session1 = Session#session{inflight_queue = lists:keydelete(MsgId, 1, InflightQ),
                                       awaiting_ack = maps:remove(MsgId, AwaitingAck)},
            {noreply, dequeue(Session1)};
        {ok, {{Retries, Timeout}, _TRef}} ->
            TRef = timer(Timeout, {timeout, awaiting_ack, MsgId}),
            AwaitingAck1 = maps:put(MsgId, {{Retries-1, Timeout*2}, TRef}, AwaitingAck),
            {noreply, Session#session{awaiting_ack = AwaitingAck1}};
        error ->
            lager:error([{client, ClientId}], "Session ~s "
                            "cannot find Awaiting Ack:~p", [ClientId, MsgId]),
            {noreply, Session}
    end;

handle_info({timeout, awaiting_rel, MsgId}, Session = #session{client_id = ClientId,
                                                               awaiting_rel = AwaitingRel}) ->
    case maps:find(MsgId, AwaitingRel) of
        {ok, {Msg, _TRef}} ->
            lager:error([{client, ClientId}], "Session ~s AwaitingRel Timout!~n"
                            "Drop Message:~p", [ClientId, Msg]),
            {noreply, Session#session{awaiting_rel = maps:remove(MsgId, AwaitingRel)}};
        error ->
            lager:error([{client, ClientId}], "Session ~s Cannot find AwaitingRel: MsgId=~p", [ClientId, MsgId]),
            {noreply, Session}
    end;

handle_info({timeout, awaiting_comp, MsgId}, Session = #session{client_id = ClientId,
                                                                awaiting_comp = Awaiting}) ->
    case maps:find(MsgId, Awaiting) of
        {ok, _TRef} ->
            lager:error([{client, ClientId}], "Session ~s "
                            "Awaiting PUBCOMP Timout: MsgId=~p!", [ClientId, MsgId]),
            {noreply, Session#session{awaiting_comp = maps:remove(MsgId, Awaiting)}};
        error ->
            lager:error([{client, ClientId}], "Session ~s "
                            "Cannot find Awaiting PUBCOMP: MsgId=~p", [ClientId, MsgId]),
            {noreply, Session}
    end;

handle_info({'EXIT', ClientPid, _Reason}, Session = #session{clean_sess = true,
                                                             client_pid = ClientPid}) ->
    {stop, normal, Session};

handle_info({'EXIT', ClientPid, Reason}, Session = #session{clean_sess = false,
                                                            client_id   = ClientId,
                                                            client_pid = ClientPid,
                                                            expired_after = Expires}) ->
    lager:info("Session ~s unlink with client ~p: reason=~p", [ClientId, ClientPid, Reason]),
    TRef = timer(Expires, session_expired),
    {noreply, Session#session{client_pid = undefined, expired_timer = TRef}, hibernate};

handle_info({'EXIT', Pid, _Reason}, Session = #session{client_id = ClientId,
                                                     client_pid = ClientPid}) ->
                                                            
    lager:error("Session ~s received unexpected EXIT:"
                    " client_pid=~p, exit_pid=~p", [ClientId, ClientPid, Pid]),
    {noreply, Session};

handle_info(session_expired, Session = #session{client_id = ClientId}) ->
    lager:error("Session ~s expired, shutdown now!", [ClientId]),
    {stop, {shutdown, expired}, Session};

handle_info(Info, Session = #session{client_id = ClientId}) ->
    lager:critical("Session ~s received unexpected info: ~p", [ClientId, Info]),
    {noreply, Session}.

terminate(_Reason, _Session) ->
    ok.

code_change(_OldVsn, Session, _Extra) ->
    {ok, Session}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% Kick duplicated client
%%------------------------------------------------------------------------------

kick(_ClientId, _ClientPid, undefined) ->
            ok;
kick(_ClientId, ClientPid, ClientPid) ->
            ok;
kick(ClientId, ClientPid, OldClientPid) ->
    lager:error("Session '~s' is duplicated: pid=~p, oldpid=~p", [ClientId, ClientPid, OldClientPid]),
    unlink(OldClientPid),
    OldClientPid ! {stop, duplicate_id, ClientPid}.

%%------------------------------------------------------------------------------
%% Check inflight and awaiting_rel
%%------------------------------------------------------------------------------

check_inflight(#session{max_inflight = 0}) ->
     true;
check_inflight(#session{max_inflight = Max, inflight_queue = Q}) ->
    Max > length(Q).

check_awaiting_rel(#session{max_awaiting_rel = 0}) ->
    true;
check_awaiting_rel(#session{awaiting_rel = AwaitingRel,
                            max_awaiting_rel = MaxLen}) ->
    maps:size(AwaitingRel) < MaxLen.

%%------------------------------------------------------------------------------
%% Dequeue and Deliver
%%------------------------------------------------------------------------------

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
        {empty, _Q} -> Session;
        {{value, Msg}, Q1} ->
            Session1 = deliver(Msg, Session#session{message_queue = Q1}),
            dequeue(Session1) %% dequeue more
    end.

deliver(Msg = #mqtt_message{qos = ?QOS_0}, Session = #session{client_pid = ClientPid}) ->
    ClientPid ! {deliver, Msg}, Session; 

deliver(Msg = #mqtt_message{qos = QoS}, Session = #session{message_id = MsgId,
                                                           client_pid = ClientPid,
                                                           inflight_queue = InflightQ})
    when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->
    Msg1 = Msg#mqtt_message{msgid = MsgId, dup = false},
    ClientPid ! {deliver, Msg1},
    await(Msg1, next_msgid(Session#session{inflight_queue = [{MsgId, Msg1}|InflightQ]})).

redeliver(Msg = #mqtt_message{qos = ?QOS_0}, Session) ->
    deliver(Msg, Session); 

redeliver(Msg = #mqtt_message{qos = QoS}, Session = #session{client_pid = ClientPid})
    when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->
    ClientPid ! {deliver, Msg},
    await(Msg, Session).

%%------------------------------------------------------------------------------
%% Awaiting ack for qos1, qos2 message
%%------------------------------------------------------------------------------
await(#mqtt_message{msgid = MsgId}, Session = #session{awaiting_ack = Awaiting,
                                                       unack_retries = Retries,
                                                       unack_timeout = Timeout}) ->
    TRef = timer(Timeout, {timeout, awaiting_ack, MsgId}),
    Awaiting1 = maps:put(MsgId, {{Retries, Timeout}, TRef}, Awaiting),
    Session#session{awaiting_ack = Awaiting1}.

acked(MsgId, Session = #session{inflight_queue = InflightQ,
                                awaiting_ack   = Awaiting}) ->
    Session#session{inflight_queue = lists:keydelete(MsgId, 1, InflightQ),
                    awaiting_ack   = maps:remove(MsgId, Awaiting)}.

next_msgid(Session = #session{message_id = 16#ffff}) ->
    Session#session{message_id = 1};

next_msgid(Session = #session{message_id = MsgId}) ->
    Session#session{message_id = MsgId + 1}.

timer(Timeout, TimeoutMsg) ->
    erlang:send_after(Timeout * 1000, self(), TimeoutMsg).

cancel_timer(undefined) -> 
	undefined;
cancel_timer(Ref) -> 
	catch erlang:cancel_timer(Ref).

