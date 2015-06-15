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
        clientid    :: binary(),

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
        inflight_queue :: emqttd_inflight:inflight(),

        %% All qos1, qos2 messages published to when client is disconnected.
        %% QoS 1 and QoS 2 messages pending transmission to the Client.
        %%
        %% Optionally, QoS 0 messages pending transmission to the Client.
        pending_queue  :: emqttd_mqueue:mqueue(),

        %% Inflight qos2 messages received from client and waiting for pubrel.
        %% QoS 2 messages which have been received from the Client,
        %% but have not been completely acknowledged.
        %% Client -> Broker
        awaiting_rel  :: map(),

        %% Awaiting timers for ack, rel and comp.
        awaiting_ack  :: map(),

        awaiting_comp :: map(),

        %% Retries to resend the unacked messages
        unack_retries = 3,

        %% 4, 8, 16 seconds if 3 retries:)
        unack_timeout = 4,

        %% Awaiting PUBREL timeout
        await_rel_timeout = 8,

        %% Max Packets that Awaiting PUBREL
        max_awaiting_rel = 100,

        %% session expired after 48 hours
        expired_after = 48,

        expired_timer,
        
        timestamp}).

%%------------------------------------------------------------------------------
%% @doc Start a session.
%% @end
%%------------------------------------------------------------------------------
-spec start_link(boolean(), binary(), pid()) -> {ok, pid()} | {error, any()}.
start_link(CleanSess, ClientId, ClientPid) ->
    gen_server:start_link(?MODULE, [CleanSess, ClientId, ClientPid], []).

%%------------------------------------------------------------------------------
%% @doc Resume a session.
%% @end
%%------------------------------------------------------------------------------
-spec resume(pid(), binary(), pid()) -> ok.
resume(Session, ClientId, ClientPid) ->
    gen_server:cast(Session, {resume, ClientId, ClientPid}).

%%------------------------------------------------------------------------------
%% @doc Destroy a session.
%% @end
%%------------------------------------------------------------------------------
-spec destroy(Session:: pid(), ClientId :: binary()) -> ok.
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
-spec publish(Session :: pid(), {mqtt_qos(), mqtt_message()}) -> ok.
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
    PendingQ = emqttd_mqueue:new(ClientId, QEnv),
    InflightQ = emqttd_inflight:new(ClientId, emqttd_opts:g(max_inflight, SessEnv)),
    Session = #session{
        clean_sess       = CleanSess,
        clientid         = ClientId,
        client_pid       = ClientPid,
        subscriptions    = [],
        inflight_queue   = InflightQ,
        pending_queue    = PendingQ,
        awaiting_rel     = #{},
        awaiting_ack     = #{},
        awaiting_comp    = #{},
        unack_retries    = emqttd_opts:g(unack_retries, SessEnv),
        unack_timeout    = emqttd_opts:g(unack_timeout, SessEnv),
        await_rel_timeout = emqttd_opts:g(await_rel_timeout, SessEnv),
        max_awaiting_rel  = emqttd_opts:g(max_awaiting_rel, SessEnv),
        expired_after     = emqttd_opts:g(expired_after, SessEnv) * 3600,
        timestamp         = os:timestamp()},
    {ok, Session, hibernate}.

handle_call({subscribe, Topics}, _From, Session = #session{clientid = ClientId,
                                                           subscriptions = Subscriptions}) ->

    %% subscribe first and don't care if the subscriptions have been existed
    {ok, GrantedQos} = emqttd_pubsub:subscribe(Topics),

    lager:info([{client, ClientId}], "Session ~s subscribe ~p, Granted QoS: ~p",
                   [ClientId, Topics, GrantedQos]),

    Subscriptions1 =
    lists:foldl(fun({Topic, Qos}, Acc) ->
                case lists:keyfind(Topic, 1, Acc) of
                    {Topic, Qos} ->
                        lager:warning([{client, ClientId}], "Session ~s resubscribe ~p: qos = ~p", [ClientId, Topic, Qos]), Acc;
                    {Topic, Old} ->
                        lager:warning([{client, ClientId}], "Session ~s resubscribe ~p: old qos=~p, new qos=~p",
                                          [ClientId, Topic, Old, Qos]),
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

handle_call({unsubscribe, Topics}, _From, Session = #session{clientid = ClientId, subscriptions = Subscriptions}) ->

    %%unsubscribe from topic tree
    ok = emqttd_pubsub:unsubscribe(Topics),
    lager:info([{client, ClientId}], "Session ~s unsubscribe ~p.", [ClientId, Topics]),

    Subscriptions1 =
    lists:foldl(fun(Topic, Acc) ->
                    case lists:keyfind(Topic, 1, Acc) of
                        {Topic, _Qos} ->
                            lists:keydelete(Topic, 1, Acc);
                        false ->
                            lager:warning([{client, ClientId}], "~s not subscribe ~s", [ClientId, Topic]), Acc
                    end
                end, Subscriptions, Topics),

    {reply, ok, Session#session{subscriptions = Subscriptions1}};

handle_call({publish, Message = #mqtt_message{qos = ?QOS_2, msgid = MsgId}}, _From, 
            Session = #session{clientid = ClientId, awaiting_rel = AwaitingRel, await_rel_timeout = Timeout}) ->
    case check_awaiting_rel(Session) of
        true -> 
            TRef = timer(Timeout, {timeout, awaiting_rel, MsgId}),
            {reply, ok, Session#session{awaiting_rel = maps:put(MsgId, {Message, TRef}, AwaitingRel)}};
        false ->
            lager:error([{clientid, ClientId}], "Session ~s "
                                " dropped Qos2 message for too many awaiting_rel: ~p", [ClientId, Message]),
            {reply, {error, dropped}, Session}
    end;

handle_call({destroy, ClientId}, _From, Session = #session{clientid = ClientId}) ->
    lager:warning("Session ~s destroyed", [ClientId]),
    {stop, {shutdown, destroy}, ok, Session};

handle_call(Req, _From, State) ->
    lager:critical("Unexpected Request: ~p", [Req]),
    {reply, {error, badreq}, State}.

handle_cast({resume, ClientId, ClientPid}, State = #session{
                                                      clientid      = ClientId,
                                                      client_pid    = OldClientPid,
                                                      pending_queue     = Queue,
                                                      awaiting_ack  = AwaitingAck,
                                                      awaiting_comp = AwaitingComp,
                                                      expired_timer  = ETimer}) ->

    lager:info([{client, ClientId}], "Session ~s resumed by ~p",[ClientId, ClientPid]),

    %% kick old client...
    if
        OldClientPid =:= undefined ->
            ok;
        OldClientPid =:= ClientPid ->
            ok;
        true ->
			lager:error("Session '~s' is duplicated: pid=~p, oldpid=~p", [ClientId, ClientPid, OldClientPid]),
            unlink(OldClientPid),
			OldClientPid ! {stop, duplicate_id, ClientPid}
    end,

    %% cancel timeout timer
    emqttd_util:cancel_timer(ETimer),

    %% redelivery PUBREL
    lists:foreach(fun(MsgId) ->
                ClientPid ! {redeliver, {?PUBREL, MsgId}}
        end, maps:keys(AwaitingComp)),

    %% redelivery messages that awaiting PUBACK or PUBREC
    Dup = fun(Msg) -> Msg#mqtt_message{dup = true} end,
    lists:foreach(fun(Msg) ->
                ClientPid ! {dispatch, {self(), Dup(Msg)}}
        end, maps:values(AwaitingAck)),

    %% send offline messages
    lists:foreach(fun(Msg) ->
                ClientPid ! {dispatch, {self(), Msg}}
        end, emqttd_queue:all(Queue)),

    {noreply, State#session{client_pid   = ClientPid,
                            %%TODO:
                            pending_queue = emqttd_queue:clear(Queue),
                            expired_timer = undefined}, hibernate};


handle_cast({puback, MsgId}, Session = #session{clientid = ClientId, inflight_queue = Q, awaiting_ack = Awaiting}) ->
    case maps:find(MsgId, Awaiting) of
        {ok, {_, TRef}} ->
            catch erlang:cancel_timer(TRef),
            {noreply, dispatch(Session#session{inflight_queue = emqttd_inflight:ack(MsgId, Q),
                                               awaiting_ack   = maps:remove(MsgId, Awaiting)})};
        error ->
            lager:error("Session ~s cannot find PUBACK '~p'!", [ClientId, MsgId]),
            {noreply, Session}
    end;

%% PUBREC
handle_cast({pubrec, MsgId}, Session = #session{clientid = ClientId,
                                                awaiting_ack = AwaitingAck,
                                                awaiting_comp = AwaitingComp,
                                                await_rel_timeout = Timeout}) ->
    case maps:find(MsgId, AwaitingAck) of
        {ok, {_, TRef}} ->
            catch erlang:cancel_timer(TRef),
            TRef1 = timer(Timeout, {timeout, awaiting_comp, MsgId}),
            {noreply, dispatch(Session#session{awaiting_ack  = maps:remove(MsgId, AwaitingAck),
                                               awaiting_comp = maps:put(MsgId, TRef1, AwaitingComp)})};
        error ->
            lager:error("Session ~s cannot find PUBREC '~p'!", [ClientId, MsgId]),
            {noreply, Session}
    end;

handle_cast({pubrel, MsgId}, Session = #session{clientid = ClientId, awaiting_rel = AwaitingRel}) ->
    case maps:find(MsgId, AwaitingRel) of
        {ok, {Msg, TRef}} ->
            catch erlang:cancel_timer(TRef),
            emqttd_pubsub:publish(Msg),
            {noreply, Session#session{awaiting_rel = maps:remove(MsgId, AwaitingRel)}};
        error ->
            lager:error("Session ~s cannot find PUBREL '~p'!", [ClientId, MsgId]),
            {noreply, Session}
    end;

%% PUBCOMP
handle_cast({pubcomp, MsgId}, Session = #session{clientid = ClientId, awaiting_comp = AwaitingComp}) ->
    case maps:is_key(MsgId, AwaitingComp) of
        true -> 
            {noreply, Session#session{awaiting_comp = maps:remove(MsgId, AwaitingComp)}};
        false ->
            lager:error("Session ~s cannot find PUBREC MsgId '~p'", [ClientId, MsgId]),
            {noreply, Session}
    end;

handle_cast(Msg, State) ->
    lager:critical("Unexpected Msg: ~p, State: ~p", [Msg, State]),
    {noreply, State}.

handle_info({dispatch, MsgList}, Session) when is_list(MsgList) ->
    NewSession = lists:foldl(fun(Msg, S) -> 
                                     dispatch({new, Msg}, S)
                             end, Session, MsgList),
    {noreply, NewSession};

handle_info({dispatch, {old, Msg}}, Session) when is_record(Msg, mqtt_message) ->
    {noreply, dispatch({old, Msg}, Session)};

handle_info({dispatch, Msg}, Session) when is_record(Msg, mqtt_message) ->
    {noreply, dispatch({new, Msg}, Session)};

handle_info({'EXIT', ClientPid, Reason}, Session = #session{clean_sess = false,
                                                            clientid = ClientId,
                                                            client_pid = ClientPid,
                                                            expired_after = Expires}) ->
    %%TODO: Clean puback, pubrel, pubcomp timers
    lager:info("Session ~s: client ~p exited for ~p", [ClientId, ClientPid, Reason]),
    TRef = timer(Expires * 1000, session_expired),
    {noreply, Session#session{expired_timer = TRef}};

handle_info({'EXIT', ClientPid, _Reason}, Session = #session{clean_sess = true, client_pid = ClientPid}) ->
    %%TODO: reason...
    {stop, normal, Session};

handle_info({'EXIT', ClientPid0, _Reason}, State = #session{client_pid = ClientPid}) ->
    lager:critical("Unexpected Client EXIT: pid=~p, pid(state): ~p", [ClientPid0, ClientPid]),
    {noreply, State};

handle_info(session_expired, State = #session{clientid = ClientId}) ->
    lager:error("Session ~s expired, shutdown now!", [ClientId]),
    {stop, {shutdown, expired}, State};

handle_info({timeout, awaiting_rel, MsgId}, Session = #session{clientid = ClientId, awaiting_rel = Awaiting}) ->
    case maps:find(MsgId, Awaiting) of
        {ok, {Msg, _TRef}} ->
            lager:error([{client, ClientId}], "Session ~s Awaiting Rel Timout!~nDrop Message:~p", [ClientId, Msg]),
            {noreply, Session#session{awaiting_rel = maps:remove(MsgId, Awaiting)}};
        error ->
            lager:error([{client, ClientId}], "Session ~s Cannot find Awaiting Rel: MsgId=~p", [ClientId, MsgId]),
            {noreply, Session}
    end;

handle_info({timeout, awaiting_comp, MsgId}, Session = #session{clientid = ClientId, awaiting_comp = Awaiting}) ->
    case maps:find(MsgId, Awaiting) of
        {ok, _TRef} ->
            lager:error([{client, ClientId}], "Session ~s Awaiting PUBCOMP Timout: MsgId=~p!", [ClientId, MsgId]),
            {noreply, Session#session{awaiting_comp = maps:remove(MsgId, Awaiting)}};
        error ->
            lager:error([{client, ClientId}], "Session ~s Cannot find Awaiting PUBCOMP: MsgId=~p", [ClientId, MsgId]),
            {noreply, Session}
    end;

handle_info(Info, Session) ->
    lager:critical("Unexpected Info: ~p, Session: ~p", [Info, Session]),
    {noreply, Session}.

terminate(_Reason, _Session) ->
    ok.

code_change(_OldVsn, Session, _Extra) ->
    {ok, Session}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% @doc Plubish Qos2 message from client -> broker, and then wait for pubrel.
%% @end
%%------------------------------------------------------------------------------


check_awaiting_rel(#session{max_awaiting_rel = 0}) ->
    true;
check_awaiting_rel(#session{awaiting_rel = AwaitingRel,
                            max_awaiting_rel = MaxLen}) ->
    maps:size(AwaitingRel) < MaxLen.

%%%=============================================================================
%%% Dispatch message from broker -> client.
%%%=============================================================================

dispatch(Session = #session{client_pid = undefined}) ->
    %% do nothing
    Session;

dispatch(Session = #session{pending_queue = PendingQ}) ->
    case emqttd_mqueue:out(PendingQ) of
        {empty, _Q} ->
            Session;
        {{value, Msg}, Q1} ->
            self() ! {dispatch, {old, Msg}},
            Session#session{pending_queue = Q1}
        end.

%% queued the message if client is offline
dispatch({Type, Msg}, Session = #session{client_pid = undefined,
                                         pending_queue= PendingQ}) ->
    Session#session{pending_queue = emqttd_mqueue:in({Type, Msg}, PendingQ)};

%% dispatch qos0 directly to client process
dispatch({_Type, Msg} = #mqtt_message{qos = ?QOS_0}, Session = #session{client_pid = ClientPid}) ->
    ClientPid ! {deliver,  Msg}, Session;

%% dispatch qos1/2 message and wait for puback
dispatch({Type, Msg = #mqtt_message{qos = Qos}}, Session = #session{clientid = ClientId,
                                                                    client_pid = ClientPid,
                                                                    message_id = MsgId,
                                                                    pending_queue = PendingQ,
                                                                    inflight_queue= InflightQ})
    when Qos =:= ?QOS_1 orelse Qos =:= ?QOS_2 ->
    %% assign id first
    Msg1 = Msg#mqtt_message{msgid = MsgId},
    Msg2 =
    if
        Qos =:= ?QOS_1 -> Msg1;
        Qos =:= ?QOS_2 -> Msg1#mqtt_message{dup = false}
    end,
    case emqttd_inflight:in(Msg1, InflightQ) of
        {error, full} ->
            lager:error("Session ~s inflight queue is full!", [ClientId]),
            Session#session{pending_queue = emqttd_mqueue:in({Type, Msg}, PendingQ)};
        {ok, InflightQ1} ->
            ClientPid ! {deliver, Msg1},
            await_ack(Msg1, next_msgid(Session#session{inflight_queue = InflightQ1}))
    end.

deliver(Msg, Session) ->
    ok.

await(Msg, Session) ->
    ok.

% message(qos1/2) is awaiting ack
await_ack(Msg = #mqtt_message{msgid = MsgId}, Session = #session{awaiting_ack = Awaiting,
                                                                unack_retries = Retries,
                                                                unack_timeout = Timeout}) ->
    
    TRef = timer(Timeout * 1000, {retry, MsgId}),
    Awaiting1 = maps:put(MsgId, {{Retries, Timeout}, TRef}, Awaiting),
    Session#session{awaiting_ack = Awaiting1}.

timer(Timeout, TimeoutMsg) ->
    erlang:send_after(Timeout * 1000, self(), TimeoutMsg).

next_msgid(Session = #session{message_id = 16#ffff}) ->
    Session#session{message_id = 1};

next_msgid(Session = #session{message_id = MsgId}) ->
    Session#session{message_id = MsgId + 1}.

