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
%%% emqttd session for persistent client.
%%%
%%% Session State in the broker consists of:
%%%
%%% 1. The Client’s subscriptions.
%%%
%%% 2. inflight qos1, qos2 messages sent to the client but unacked, QoS 1 and QoS 2
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

-include_lib("emqtt/include/emqtt.hrl").

-include_lib("emqtt/include/emqtt_packet.hrl").

%% Start gen_server
-export([start_link/2, resume/3, destroy/2]).

%% Init Session State
-export([new/1]).

%% PubSub APIs
-export([publish/3,
         puback/2,
         subscribe/2,
         unsubscribe/2,
         await/2,
         dispatch/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(session, {
        %% ClientId: Identifier of Session
        clientid    :: binary(),

        %% Clean Session Flag
        clean_sess = true,

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
        inflight_window :: emqttd_mqwin:mqwin(),

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
        expired_after = 172800,

        expired_timer,
        
        timestamp}).

-type session() :: #session{}.

%%%=============================================================================
%%% Session API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start a session process.
%% @end
%%------------------------------------------------------------------------------
start_link(ClientId, ClientPid) ->
    gen_server:start_link(?MODULE, [ClientId, ClientPid], []).

%%------------------------------------------------------------------------------
%% @doc Resume a session.
%% @end
%%------------------------------------------------------------------------------
resume(Session, _ClientId, _ClientPid) when is_record(Session, session) ->
    Session;
resume(SessPid, ClientId, ClientPid) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {resume, ClientId, ClientPid}), SessPid.

%%------------------------------------------------------------------------------
%% @doc Destroy a session.
%% @end
%%------------------------------------------------------------------------------
-spec destroy(SessPid :: pid(), ClientId :: binary()) -> ok.
destroy(SessPid, ClientId)  when is_pid(SessPid) ->
    gen_server:cast(SessPid, {destroy, ClientId}), SessPid.

%%------------------------------------------------------------------------------
%% @doc Init Session State.
%% @end
%%------------------------------------------------------------------------------
-spec new(binary()) -> session().
new(ClientId) ->
    QEnv = emqttd:env(mqtt, queue),
    SessEnv = emqttd:env(mqtt, session),
    #session{
        clientid         = ClientId,
        clean_sess       = true,
        subscriptions    = [],
        inflight_window  = emqttd_mqwin:new(ClientId, QEnv),
        pending_queue    = emqttd_mqueue:new(ClientId, QEnv),
        awaiting_rel     = #{},  
        awaiting_ack     = #{},
        awaiting_comp    = #{},
        unack_retries    = emqttd_opts:g(unack_retries, SessEnv),
        unack_timeout    = emqttd_opts:g(unack_timeout, SessEnv),
        await_rel_timeout = emqttd_opts:g(await_rel_timeout, SessEnv),
        max_awaiting_rel  = emqttd_opts:g(max_awaiting_rel, SessEnv),
        expired_after    = emqttd_opts:g(expired_after, SessEnv) * 3600
    }.

%%------------------------------------------------------------------------------
%% @doc Publish message
%% @end
%%------------------------------------------------------------------------------
-spec publish(session() | pid(), mqtt_clientid(), {mqtt_qos(), mqtt_message()}) -> session() | pid().
publish(Session, ClientId, {?QOS_0, Message}) ->
    %% publish qos0 directly
    emqttd_pubsub:publish(ClientId, Message), Session;

publish(Session, ClientId, {?QOS_1, Message}) ->
    %% publish qos1 directly, and client will puback
	emqttd_pubsub:publish(ClientId, Message), Session;

publish(Session = #session{awaiting_rel      = AwaitingRel,
                           await_rel_timeout = Timeout,
                           max_awaiting_rel  = MaxLen}, ClientId,
        {?QOS_2, Message = #mqtt_message{msgid = MsgId}}) ->
    case maps:size(AwaitingRel) >= MaxLen of
        true -> lager:error([{clientid, ClientId}], "Session ~s "
                                " dropped Qos2 message for too many awaiting_rel: ~p", [ClientId, Message]);
        false ->
            %% store in awaiting_rel
            TRef = erlang:send_after(Timeout * 1000, self(), {timeout, awaiting_rel, MsgId}),
            Session#session{awaiting_rel = maps:put(MsgId, {Message, TRef}, AwaitingRel)};
    end;
publish(SessPid, ClientId, {?QOS_2, Message}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {publish, ClientId, {?QOS_2, Message}}), SessPid.

%%------------------------------------------------------------------------------
%% @doc PubAck message
%% @end
%%------------------------------------------------------------------------------

-spec puback(session(), {mqtt_packet_type(), mqtt_packet_id()}) -> session().
puback(Session = #session{clientid = ClientId, awaiting_ack = Awaiting}, {?PUBACK, PacketId}) ->
    case maps:is_key(PacketId, Awaiting) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBACK PacketId '~p' not found!", [ClientId, PacketId])
    end,
    Session#session{awaiting_ack = maps:remove(PacketId, Awaiting)};

puback(SessPid, {?PUBACK, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {puback, {?PUBACK, PacketId});

%% PUBREC
puback(Session = #session{clientid = ClientId, 
                                  awaiting_ack = AwaitingAck,
                                  awaiting_comp = AwaitingComp}, {?PUBREC, PacketId}) ->
    case maps:is_key(PacketId, AwaitingAck) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBREC PacketId '~p' not found!", [ClientId, PacketId])
    end,
    Session#session{awaiting_ack   = maps:remove(PacketId, AwaitingAck),
                      awaiting_comp  = maps:put(PacketId, true, AwaitingComp)};

puback(SessPid, {?PUBREC, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {puback, {?PUBREC, PacketId});

%% PUBREL
puback(Session = #session{clientid = ClientId, awaiting_rel = Awaiting}, {?PUBREL, PacketId}) ->
    case maps:find(PacketId, Awaiting) of
        {ok, {Msg, TRef}} ->
            catch erlang:cancel_timer(TRef),
            emqttd_pubsub:publish(ClientId, Msg);
        error ->
            lager:error("Session ~s cannot find PUBREL PacketId '~p'!", [ClientId, PacketId])
    end,
    Session#session{awaiting_rel = maps:remove(PacketId, Awaiting)};

puback(SessPid, {?PUBREL, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {puback, {?PUBREL, PacketId});

%% PUBCOMP
puback(Session = #session{clientid = ClientId, 
                                  awaiting_comp = AwaitingComp}, {?PUBCOMP, PacketId}) ->
    case maps:is_key(PacketId, AwaitingComp) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBREC PacketId '~p' not exist", [ClientId, PacketId])
    end,
    Session#session{awaiting_comp = maps:remove(PacketId, AwaitingComp)};

puback(SessPid, {?PUBCOMP, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {puback, {?PUBCOMP, PacketId});

wait_ack

timeout(awaiting_rel, MsgId, Session = #session{clientid = ClientId, awaiting_rel = Awaiting}) ->
    case maps:find(MsgId, Awaiting) of
        {ok, {Msg, _TRef}} ->
            lager:error([{client, ClientId}], "Session ~s Awaiting Rel Timout!~nDrop Message:~p", [ClientId, Msg]),
            Session#session{awaiting_rel = maps:remove(MsgId, Awaiting)};
        error ->
            lager:error([{client, ClientId}], "Session ~s Cannot find Awaiting Rel: MsgId=~p", [ClientId, MsgId]),
            Session
    end.

%%------------------------------------------------------------------------------
%% @doc Subscribe Topics
%% @end
%%------------------------------------------------------------------------------
-spec subscribe(session() | pid(), [{binary(), mqtt_qos()}]) -> {ok, session() | pid(), [mqtt_qos()]}.
subscribe(Session = #session{clientid = ClientId, subscriptions = Subscriptions}, Topics) ->

    %% subscribe first and don't care if the subscriptions have been existed
    {ok, GrantedQos} = emqttd_pubsub:subscribe(Topics),

    lager:info([{client, ClientId}], "Client ~s subscribe ~p. Granted QoS: ~p",
               [ClientId, Topics, GrantedQos]),

    Subscriptions1 =
    lists:foldl(fun({Topic, Qos}, Acc) ->
                case lists:keyfind(Topic, 1, Acc) of
                    {Topic, Qos} ->
                        lager:warning([{client, ClientId}], "~s resubscribe ~p: qos = ~p", [ClientId, Topic, Qos]), Acc;
                    {Topic, Old} ->
                        lager:warning([{client, ClientId}], "~s resubscribe ~p: old qos=~p, new qos=~p",
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

    {ok, Session#session{subscriptions = Subscriptions1}, GrantedQos};

subscribe(SessPid, Topics) when is_pid(SessPid) ->
    {ok, GrantedQos} = gen_server:call(SessPid, {subscribe, Topics}),
    {ok, SessPid, GrantedQos}.

%%------------------------------------------------------------------------------
%% @doc Unsubscribe Topics
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(session() | pid(), [binary()]) -> {ok, session() | pid()}.
unsubscribe(Session = #session{clientid = ClientId, subscriptions = Subscriptions}, Topics) ->

    %%unsubscribe from topic tree
    ok = emqttd_pubsub:unsubscribe(Topics),
    lager:info([{client, ClientId}], "Client ~s unsubscribe ~p.", [ClientId, Topics]),

    Subscriptions1 =
    lists:foldl(fun(Topic, Acc) ->
                    case lists:keyfind(Topic, 1, Acc) of
                        {Topic, _Qos} ->
                            lists:keydelete(Topic, 1, Acc);
                        false ->
                            lager:warning([{client, ClientId}], "~s not subscribe ~s", [ClientId, Topic]), Acc
                    end
                end, Subscriptions, Topics),

    {ok, Session#session{subscriptions = Subscriptions1}};

unsubscribe(SessPid, Topics) when is_pid(SessPid) ->
    gen_server:call(SessPid, {unsubscribe, Topics}),
    {ok, SessPid}.

%%------------------------------------------------------------------------------
%% @doc Destroy Session
%% @end
%%------------------------------------------------------------------------------

% message(qos1) is awaiting ack
await_ack(Msg = #mqtt_message{qos = ?QOS_1}, Session = #session{message_id = MsgId,
                                                                  inflight_queue = InflightQ,
                                                                  awaiting_ack = Awaiting,
                                                                  unack_retry_after = Time,
                                                                  max_unack_retries = Retries}) ->
    %% assign msgid before send
    Msg1 = Msg#mqtt_message{msgid = MsgId},
    TRef = erlang:send_after(Time * 1000, self(), {retry, MsgId}),
    Awaiting1 = maps:put(MsgId, {TRef, Retries, Time}, Awaiting),
    {Msg1, next_msgid(Session#session{inflight_queue = [{MsgId, Msg1} | InflightQ],
                                        awaiting_ack = Awaiting1})}.

% message(qos2) is awaiting ack
await_ack(Message = #mqtt_message{qos = Qos}, Session = #session{message_id = MsgId, awaiting_ack = Awaiting},)
    when (Qos =:= ?QOS_1) orelse (Qos =:= ?QOS_2) ->
    %%assign msgid before send
    Message1 = Message#mqtt_message{msgid = MsgId, dup = false},
    Message2 =
    if
        Qos =:= ?QOS_2 -> Message1#mqtt_message{dup = false};
        true -> Message1
    end,
    Awaiting1 = maps:put(MsgId, Message2, Awaiting),
    {Message1, next_msgid(Session#session{awaiting_ack = Awaiting1})}.


%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================
init([ClientId, ClientPid]) ->
    process_flag(trap_exit, true),
    true = link(ClientPid),
    Session = emqttd_session:new(ClientId),
    {ok, Session#session{clean_sess = false,
                         client_pid = ClientPid,
                         timestamp = os:timestamp()}, hibernate}.


handle_call({subscribe, Topics}, _From, Session) ->
    {ok, NewSession, GrantedQos} = subscribe(Session, Topics),
    {reply, {ok, GrantedQos}, NewSession};

handle_call({unsubscribe, Topics}, _From, Session) ->
    {ok, NewSession} = unsubscribe(Session, Topics),
    {reply, ok, NewSession};

handle_call(Req, _From, State) ->
    lager:error("Unexpected Request: ~p", [Req]),
    {reply, {error, badreq}, State}.

handle_cast({resume, ClientId, ClientPid}, State = #session{
                                                      clientid      = ClientId,
                                                      client_pid    = OldClientPid,
                                                      msg_queue     = Queue,
                                                      awaiting_ack  = AwaitingAck,
                                                      awaiting_comp = AwaitingComp,
                                                      expire_timer  = ETimer}) ->

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
    lists:foreach(fun(PacketId) ->
                ClientPid ! {redeliver, {?PUBREL, PacketId}}
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
                            msg_queue    = emqttd_queue:clear(Queue),
                            expire_timer = undefined}, hibernate};

handle_cast({publish, ClientId, {?QOS_2, Message}}, Session) ->
    {noreply, publish(Session, ClientId, {?QOS_2, Message})};

handle_cast({puback, {PubAck, PacketId}, Session) ->
    {noreply, puback(Session, {PubAck, PacketId})};

handle_cast({destroy, ClientId}, Session = #session{clientid = ClientId}) ->
    lager:warning("Session ~s destroyed", [ClientId]),
    {stop, normal, Session};

handle_cast(Msg, State) ->
    lager:critical("Unexpected Msg: ~p, State: ~p", [Msg, State]), 
    {noreply, State}.

handle_info({dispatch, {_From, Messages}}, Session) when is_list(Messages) ->
    F = fun(Message, S) -> dispatch(Message, S) end,
    {noreply, lists:foldl(F, Session, Messages)};

handle_info({dispatch, {_From, Message}}, State) ->
    {noreply, dispatch(Message, State)};

handle_info({'EXIT', ClientPid, Reason}, Session = #session{clientid = ClientId,
                                                            client_pid = ClientPid}) ->
    lager:info("Session: client ~s@~p exited for ~p", [ClientId, ClientPid, Reason]),
    {noreply, start_expire_timer(Session#session{client_pid = undefined})};

handle_info({'EXIT', ClientPid0, _Reason}, State = #session{client_pid = ClientPid}) ->
    lager:error("Unexpected Client EXIT: pid=~p, pid(state): ~p", [ClientPid0, ClientPid]),
    {noreply, State};

handle_info(session_expired, State = #session{clientid = ClientId}) ->
    lager:warning("Session ~s expired!", [ClientId]),
    {stop, {shutdown, expired}, State};

handle_info({timeout, awaiting_rel, MsgId}, Session) ->
    {noreply, timeout(awaiting_rel, MsgId, Session)};

handle_info(Info, Session) ->
    lager:critical("Unexpected Info: ~p, Session: ~p", [Info, Session]),
    {noreply, Session}.

terminate(_Reason, _Session) ->
    ok.

code_change(_OldVsn, Session, _Extra) ->
    {ok, Session}.

%%%=============================================================================
%%% Dispatch message from broker -> client.
%%%=============================================================================

%% queued the message if client is offline
dispatch(Msg, Session = #session{client_pid = undefined}) ->
    queue(Msg, Session);

%% dispatch qos0 directly to client process
dispatch(Msg = #mqtt_message{qos = ?QOS_0}, Session = #session{client_pid = ClientPid}) ->
    ClientPid ! {dispatch, {self(), Msg}}, Session;

%% dispatch qos1/2 messages and wait for puback
dispatch(Msg = #mqtt_message{qos = Qos}, Session = #session{clientid = ClientId,
                                                            message_id = MsgId,
                                                            pending_queue = Q,
                                                            inflight_window = Win})
    when (Qos =:= ?QOS_1) orelse (Qos =:= ?QOS_2) ->

    case emqttd_mqwin:is_full(InflightWin) of
        true  ->
            lager:error("Session ~s inflight window is full!", [ClientId]),
            Session#session{pending_queue = emqttd_mqueue:in(Msg, Q)};
        false ->
            Msg1 = Msg#mqtt_message{msgid = MsgId},
            Msg2 =
            if
                Qos =:= ?QOS_2 -> Msg1#mqtt_message{dup = false};
                true -> Msg1
            end,
            ClientPid ! {dispatch, {self(), Msg2}},
            NewWin = emqttd_mqwin:in(Msg2, Win),
            await_ack(Msg2, next_msgid(Session#session{inflight_window = NewWin}))
    end.

queue(Msg, Session = #session{pending_queue= Queue}) ->
    Session#session{pending_queue = emqttd_mqueue:in(Msg, Queue)}.

next_msgid(State = #session{message_id = 16#ffff}) ->
    State#session{message_id = 1};

next_msgid(State = #session{message_id = MsgId}) ->
    State#session{message_id = MsgId + 1}.

start_expire_timer(Session = #session{expired_after = Expires,
                                      expired_timer = OldTimer}) ->
    emqttd_util:cancel_timer(OldTimer),
    Timer = erlang:send_after(Expires * 1000, self(), session_expired),
    Session#session{expired_timer = Timer}.

