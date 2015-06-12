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
%%% emqttd session.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_session).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include_lib("emqtt/include/emqtt.hrl").

-include_lib("emqtt/include/emqtt_packet.hrl").

-define(SessProc, emqttd_session_proc). 

%% Session Managenent APIs
-export([start/1,
         resume/3,
         destroy/2]).

%% PubSub APIs
-export([publish/3,
         puback/2,
         subscribe/2,
         unsubscribe/2,
         await/2,
         dispatch/2]).

-record(session, {
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

        %% session expired after 48 hours
        sess_expired_after = 172800,

        sess_expired_timer,
        
        timestamp}).

-type session() :: #session{}.

%%%=============================================================================
%%% Session API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start Session
%% @end
%%------------------------------------------------------------------------------
-spec start({boolean(), binary(), pid()}) -> {ok, session()}.
start({true = _CleanSess, ClientId, _ClientPid}) ->
    %%Destroy old session if CleanSess is true before.
    ok = emqttd_sm:destroy_session(ClientId),
    {ok, initial_state(ClientId)};

start({false = _CleanSess, ClientId, ClientPid}) ->
    {ok, SessPid} = emqttd_sm:start_session(ClientId, ClientPid),
    {ok, SessPid}.

%%------------------------------------------------------------------------------
%% @doc Resume Session
%% @end
%%------------------------------------------------------------------------------
-spec resume(session(), binary(), pid()) -> session().
resume(SessState = #session{}, _ClientId, _ClientPid) ->
    SessState;
resume(SessPid, ClientId, ClientPid) when is_pid(SessPid) ->
    ?SessProc:
    gen_server:cast(SessPid, {resume, ClientId, ClientPid}),
    SessPid.

%%------------------------------------------------------------------------------
%% @doc Publish message
%% @end
%%------------------------------------------------------------------------------
-spec publish(session(), mqtt_clientid(), {mqtt_qos(), mqtt_message()}) -> session().
publish(Session, ClientId, {?QOS_0, Message}) ->
    emqttd_pubsub:publish(ClientId, Message), Session;

publish(Session, ClientId, {?QOS_1, Message}) ->
	emqttd_pubsub:publish(ClientId, Message), Session;

publish(SessState = #session{awaiting_rel = AwaitingRel,
                             await_rel_timeout = Timeout}, _ClientId,
        {?QOS_2, Message = #mqtt_message{msgid = MsgId}}) ->
    %% store in awaiting_rel
    TRef = erlang:send_after(Timeout * 1000, self(), {timeout, awaiting_rel, MsgId}),
    SessState#session{awaiting_rel = maps:put(MsgId, {Message, TRef}, AwaitingRel)};

publish(SessPid, ClientId, {?QOS_2, Message}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {publish, ClientId, {?QOS_2, Message}}),
    SessPid.

%%------------------------------------------------------------------------------
%% @doc PubAck message
%% @end
%%------------------------------------------------------------------------------
-spec puback(session(), {mqtt_packet_type(), mqtt_packet_id()}) -> session().
puback(SessState = #session{clientid = ClientId, awaiting_ack = Awaiting}, {?PUBACK, PacketId}) ->
    case maps:is_key(PacketId, Awaiting) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBACK PacketId '~p' not found!", [ClientId, PacketId])
    end,
    SessState#session{awaiting_ack = maps:remove(PacketId, Awaiting)};
puback(SessPid, {?PUBACK, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {puback, PacketId}), SessPid;

%% PUBREC
puback(SessState = #session{clientid = ClientId, 
                                  awaiting_ack = AwaitingAck,
                                  awaiting_comp = AwaitingComp}, {?PUBREC, PacketId}) ->
    case maps:is_key(PacketId, AwaitingAck) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBREC PacketId '~p' not found!", [ClientId, PacketId])
    end,
    SessState#session{awaiting_ack   = maps:remove(PacketId, AwaitingAck),
                      awaiting_comp  = maps:put(PacketId, true, AwaitingComp)};

puback(SessPid, {?PUBREC, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubrec, PacketId}), SessPid;

%% PUBREL
puback(SessState = #session{clientid = ClientId,
                            awaiting_rel = Awaiting}, {?PUBREL, PacketId}) ->
    case maps:find(PacketId, Awaiting) of
        {ok, {Msg, TRef}} ->
            catch erlang:cancel_timer(TRef),
            emqttd_pubsub:publish(ClientId, Msg);
        error ->
            lager:error("Session ~s PUBREL PacketId '~p' not found!", [ClientId, PacketId])
    end,
    SessState#session{awaiting_rel = maps:remove(PacketId, Awaiting)};

puback(SessPid, {?PUBREL, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubrel, PacketId}), SessPid;

%% PUBCOMP
puback(SessState = #session{clientid = ClientId, 
                                  awaiting_comp = AwaitingComp}, {?PUBCOMP, PacketId}) ->
    case maps:is_key(PacketId, AwaitingComp) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBREC PacketId '~p' not exist", [ClientId, PacketId])
    end,
    SessState#session{awaiting_comp = maps:remove(PacketId, AwaitingComp)};

puback(SessPid, {?PUBCOMP, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubcomp, PacketId}), SessPid.

timeout(awaiting_rel, MsgId, SessState = #session{clientid = ClientId, awaiting_rel = Awaiting}) ->
    case maps:find(MsgId, Awaiting) of
        {ok, {Msg, _TRef}} ->
            lager:error([{client, ClientId}], "Session ~s Awaiting Rel Timout!~nDrop Message:~p", [ClientId, Msg]),
            SessState#session{awaiting_rel = maps:remove(MsgId, Awaiting)};
        error ->
            lager:error([{client, ClientId}], "Session ~s Cannot find Awaiting Rel: MsgId=~p", [ClientId, MsgId]),
            SessState
    end.

%%------------------------------------------------------------------------------
%% @doc Subscribe Topics
%% @end
%%------------------------------------------------------------------------------
-spec subscribe(session(), [{binary(), mqtt_qos()}]) -> {ok, session(), [mqtt_qos()]}.
subscribe(SessState = #session{clientid = ClientId, subscriptions = Subscriptions}, Topics) ->

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

    {ok, SessState#session{subscriptions = Subscriptions1}, GrantedQos};

subscribe(SessPid, Topics) when is_pid(SessPid) ->
    {ok, GrantedQos} = gen_server:call(SessPid, {subscribe, Topics}),
    {ok, SessPid, GrantedQos}.

%%------------------------------------------------------------------------------
%% @doc Unsubscribe Topics
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(session(), [binary()]) -> {ok, session()}.
unsubscribe(SessState = #session{clientid = ClientId, subscriptions = Subscriptions}, Topics) ->

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

    {ok, SessState#session{subscriptions = Subscriptions1}};

unsubscribe(SessPid, Topics) when is_pid(SessPid) ->
    gen_server:call(SessPid, {unsubscribe, Topics}),
    {ok, SessPid}.

%%------------------------------------------------------------------------------
%% @doc Destroy Session
%% @end
%%------------------------------------------------------------------------------
-spec destroy(SessPid :: pid(), ClientId :: binary()) -> ok.
destroy(SessPid, ClientId)  when is_pid(SessPid) ->
    gen_server:cast(SessPid, {destroy, ClientId}).

% message(qos1) is awaiting ack
await_ack(Msg = #mqtt_message{qos = ?QOS_1}, SessState = #session{message_id = MsgId,
                                                                  inflight_queue = InflightQ,
                                                                  awaiting_ack = Awaiting,
                                                                  unack_retry_after = Time,
                                                                  max_unack_retries = Retries}) ->
    %% assign msgid before send
    Msg1 = Msg#mqtt_message{msgid = MsgId},
    TRef = erlang:send_after(Time * 1000, self(), {retry, MsgId}),
    Awaiting1 = maps:put(MsgId, {TRef, Retries, Time}, Awaiting),
    {Msg1, next_msgid(SessState#session{inflight_queue = [{MsgId, Msg1} | InflightQ],
                                        awaiting_ack = Awaiting1})}.

% message(qos2) is awaiting ack
await_ack(Message = #mqtt_message{qos = Qos}, SessState = #session{message_id = MsgId, awaiting_ack = Awaiting},)
    when (Qos =:= ?QOS_1) orelse (Qos =:= ?QOS_2) ->
    %%assign msgid before send
    Message1 = Message#mqtt_message{msgid = MsgId, dup = false},
    Message2 =
    if
        Qos =:= ?QOS_2 -> Message1#mqtt_message{dup = false};
        true -> Message1
    end,
    Awaiting1 = maps:put(MsgId, Message2, Awaiting),
    {Message1, next_msgid(SessState#session{awaiting_ack = Awaiting1})}.

initial_state(ClientId) ->
    %%TODO: init session options.
    #session{clientid       = ClientId,
             subscriptions  = [],
             inflight_queue = [],
             awaiting_queue = [],
             awaiting_ack   = #{},
             awaiting_rel   = #{},
             awaiting_comp  = #{}}.

initial_state(ClientId, ClientPid) ->
    State = initial_state(ClientId),
    State#session{client_pid = ClientPid}.


%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%% client is offline
dispatch(Msg, SessState = #session{client_pid = undefined}) ->
    queue(Msg, SessState);

%% dispatch qos0 directly
dispatch(Msg = #mqtt_message{qos = ?QOS_0}, SessState = #session{client_pid = ClientPid}) ->
    ClientPid ! {dispatch, {self(), Msg}}, SessState;

%% queue if inflight_queue is full
dispatch(Msg = #mqtt_message{qos = Qos}, SessState = #session{inflight_window = InflightWin,
                                                              inflight_queue  = InflightQ})
        when (Qos > ?QOS_0) andalso (length(InflightQ) >= InflightWin) ->
    %%TODO: set alarms
    lager:error([{clientid, ClientId}], "Session ~s inflight_queue is full!", [ClientId]),
    queue(Msg, SessState);

%% dispatch and await ack
dispatch(Msg = #mqtt_message{qos = Qos}, SessState = #session{client_pid = ClientPid})
    when (Qos =:= ?QOS_1) orelse (Qos =:= ?QOS_2) ->
    %% assign msgid and await
    {NewMsg, NewState} = await_ack(Msg, SessState),
    ClientPid ! {dispatch, {self(), NewMsg}},

queue(Msg, SessState = #session{pending_queue = Queue}) ->
    SessState#session{pending_queue = emqttd_mqueue:in(Msg, Queue)}.

next_msgid(State = #session{message_id = 16#ffff}) ->
    State#session{message_id = 1};

next_msgid(State = #session{message_id = MsgId}) ->
    State#session{message_id = MsgId + 1}.

start_expire_timer(State = #session{expires = Expires, expire_timer = OldTimer}) ->
    emqttd_util:cancel_timer(OldTimer),
    Timer = erlang:send_after(Expires * 1000, self(), session_expired),
    State#session{expire_timer = Timer}.

