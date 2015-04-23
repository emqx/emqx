%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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

-include_lib("emqtt/include/emqtt.hrl").
-include_lib("emqtt/include/emqtt_packet.hrl").
-include("emqttd.hrl").

%% API Function Exports
-export([start/1,
         resume/3,
         publish/3,
         puback/2,
         subscribe/2,
         unsubscribe/2,
         destroy/2]).

-export([store/2]).

%% Start gen_server
-export([start_link/3]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(session_state, { 
        clientid   :: binary(),
        client_pid  :: pid(),
		message_id  = 1,
        submap      :: map(),
        msg_queue, %% do not receive rel
        awaiting_ack :: map(),
        awaiting_rel :: map(),
        awaiting_comp :: map(),
        expires,
        expire_timer}).

-type session() :: #session_state{} | pid().

%%%=============================================================================
%%% Session API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start Session.
%%
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
%% @doc
%% Resume Session.
%%
%% @end
%%------------------------------------------------------------------------------
-spec resume(session(), binary(), pid()) -> session().
resume(SessState = #session_state{}, _ClientId, _ClientPid) ->
    SessState;
resume(SessPid, ClientId, ClientPid) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {resume, ClientId, ClientPid}),
    SessPid.

%%------------------------------------------------------------------------------
%% @doc
%% Publish message.
%%
%% @end
%%------------------------------------------------------------------------------
-spec publish(session(), mqtt_clientid(), {mqtt_qos(), mqtt_message()}) -> session().
publish(Session, ClientId, {?QOS_0, Message}) ->
    emqttd_pubsub:publish(ClientId, Message), Session;

publish(Session, ClientId, {?QOS_1, Message}) ->
	emqttd_pubsub:publish(ClientId, Message), Session;

publish(SessState = #session_state{awaiting_rel = AwaitingRel}, _ClientId,
        {?QOS_2, Message = #mqtt_message{msgid = MsgId}}) ->
    %% store in awaiting_rel
    SessState#session_state{awaiting_rel = maps:put(MsgId, Message, AwaitingRel)};

publish(SessPid, ClientId, {?QOS_2, Message}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {publish, ClientId, {?QOS_2, Message}}),
    SessPid.

%%------------------------------------------------------------------------------
%% @doc
%% PubAck message.
%%
%% @end
%%------------------------------------------------------------------------------
-spec puback(session(), {mqtt_packet_type(), mqtt_packet_id()}) -> session().
puback(SessState = #session_state{clientid = ClientId, awaiting_ack = Awaiting}, {?PUBACK, PacketId}) ->
    case maps:is_key(PacketId, Awaiting) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBACK PacketId '~p' not found!", [ClientId, PacketId])
    end,
    SessState#session_state{awaiting_ack = maps:remove(PacketId, Awaiting)};
puback(SessPid, {?PUBACK, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {puback, PacketId}), SessPid;

%% PUBREC
puback(SessState = #session_state{clientid = ClientId, 
                                  awaiting_ack = AwaitingAck,
                                  awaiting_comp = AwaitingComp}, {?PUBREC, PacketId}) ->
    case maps:is_key(PacketId, AwaitingAck) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBREC PacketId '~p' not found!", [ClientId, PacketId])
    end,
    SessState#session_state{awaiting_ack   = maps:remove(PacketId, AwaitingAck), 
                            awaiting_comp  = maps:put(PacketId, true, AwaitingComp)};

puback(SessPid, {?PUBREC, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubrec, PacketId}), SessPid;

%% PUBREL
puback(SessState = #session_state{clientid = ClientId,
                                  awaiting_rel = Awaiting}, {?PUBREL, PacketId}) ->
    case maps:find(PacketId, Awaiting) of
        {ok, Msg} -> emqttd_pubsub:publish(ClientId, Msg);
        error -> lager:warning("Session ~s: PUBREL PacketId '~p' not found!", [ClientId, PacketId])
    end,
    SessState#session_state{awaiting_rel = maps:remove(PacketId, Awaiting)};

puback(SessPid, {?PUBREL, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubrel, PacketId}), SessPid;

%% PUBCOMP
puback(SessState = #session_state{clientid = ClientId, 
                                  awaiting_comp = AwaitingComp}, {?PUBCOMP, PacketId}) ->
    case maps:is_key(PacketId, AwaitingComp) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBREC PacketId '~p' not exist", [ClientId, PacketId])
    end,
    SessState#session_state{awaiting_comp = maps:remove(PacketId, AwaitingComp)};

puback(SessPid, {?PUBCOMP, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubcomp, PacketId}), SessPid.

%%------------------------------------------------------------------------------
%% @doc
%% Subscribe Topics.
%%
%% @end
%%------------------------------------------------------------------------------
-spec subscribe(session(), [{binary(), mqtt_qos()}]) -> {ok, session(), [mqtt_qos()]}.
subscribe(SessState = #session_state{clientid = ClientId, submap = SubMap}, Topics) ->
    Resubs = [Topic || {Name, _Qos} = Topic <- Topics, maps:is_key(Name, SubMap)], 
    case Resubs of
        [] -> ok;
        _  -> lager:warning("~s resubscribe ~p", [ClientId, Resubs])
    end,
    SubMap1 = lists:foldl(fun({Name, Qos}, Acc) -> maps:put(Name, Qos, Acc) end, SubMap, Topics),
    {ok, GrantedQos} = emqttd_pubsub:subscribe(Topics),
    lager:info("Client ~s subscribe ~p. Granted QoS: ~p", [ClientId, Topics, GrantedQos]),
    %%TODO: should be gen_event and notification...
    [emqttd_msg_store:redeliver(Name, self()) || {Name, _} <- Topics],
    {ok, SessState#session_state{submap = SubMap1}, GrantedQos};

subscribe(SessPid, Topics) when is_pid(SessPid) ->
    {ok, GrantedQos} = gen_server:call(SessPid, {subscribe, Topics}),
    {ok, SessPid, GrantedQos}.

%%------------------------------------------------------------------------------
%% @doc
%% Unsubscribe Topics.
%%
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(session(), [binary()]) -> {ok, session()}.
unsubscribe(SessState = #session_state{clientid = ClientId, submap = SubMap}, Topics) ->
    %%TODO: refactor later.
    case Topics -- maps:keys(SubMap) of
        [] -> ok;
        BadUnsubs -> lager:warning("~s should not unsubscribe ~p", [ClientId, BadUnsubs])
    end,
    %%unsubscribe from topic tree
    ok = emqttd_pubsub:unsubscribe(Topics),
    lager:info("Client ~s unsubscribe ~p.", [ClientId, Topics]),
    SubMap1 = lists:foldl(fun(Topic, Acc) -> maps:remove(Topic, Acc) end, SubMap, Topics),
    {ok, SessState#session_state{submap = SubMap1}};

unsubscribe(SessPid, Topics) when is_pid(SessPid) ->
    gen_server:call(SessPid, {unsubscribe, Topics}),
    {ok, SessPid}.

%%------------------------------------------------------------------------------
%% @doc
%% Destroy Session.
%%
%% @end
%%------------------------------------------------------------------------------
-spec destroy(SessPid :: pid(), ClientId :: binary()) -> ok.
destroy(SessPid, ClientId)  when is_pid(SessPid) ->
    gen_server:cast(SessPid, {destroy, ClientId}).

%store message(qos1) that sent to client
store(SessState = #session_state{message_id = MsgId, awaiting_ack = Awaiting}, 
    Message = #mqtt_message{qos = Qos}) when (Qos =:= ?QOS_1) orelse (Qos =:= ?QOS_2) ->
    %%assign msgid before send
    Message1 = Message#mqtt_message{msgid = MsgId},
    Message2 =
    if
        Qos =:= ?QOS_2 -> Message1#mqtt_message{dup = false};
        true -> Message1
    end,
    Awaiting1 = maps:put(MsgId, Message2, Awaiting),
    {Message1, next_msg_id(SessState#session_state{awaiting_ack = Awaiting1})}.

initial_state(ClientId) ->
    #session_state{clientid     = ClientId,
                   submap        = #{}, 
                   awaiting_ack  = #{},
                   awaiting_rel  = #{},
                   awaiting_comp = #{}}.

initial_state(ClientId, ClientPid) ->
    State = initial_state(ClientId),
    State#session_state{client_pid = ClientPid}.

%%------------------------------------------------------------------------------
%% @doc Start a session process.
%% @end
%%------------------------------------------------------------------------------
start_link(SessOpts, ClientId, ClientPid) ->
    gen_server:start_link(?MODULE, [SessOpts, ClientId, ClientPid], []).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([SessOpts, ClientId, ClientPid]) ->
    process_flag(trap_exit, true),
    %%TODO: Is this OK? should monitor...
    true = link(ClientPid),
    State = initial_state(ClientId, ClientPid),
    Expires = proplists:get_value(expires, SessOpts, 1) * 3600,
    MsgQueue = emqttd_queue:new(proplists:get_value(max_queue, SessOpts, 1000), 
                               proplists:get_value(store_qos0, SessOpts, false)),
    {ok, State#session_state{expires = Expires,
                             msg_queue = MsgQueue}, hibernate}.

handle_call({subscribe, Topics}, _From, State) ->
    {ok, NewState, GrantedQos} = subscribe(State, Topics),
    {reply, {ok, GrantedQos}, NewState};

handle_call({unsubscribe, Topics}, _From, State) ->
    {ok, NewState} = unsubscribe(State, Topics),
    {reply, ok, NewState};

handle_call(Req, _From, State) ->
    lager:error("Unexpected request: ~p", [Req]),
    {reply, error, State}.

handle_cast({resume, ClientId, ClientPid}, State = #session_state{
                                                      clientid      = ClientId,
                                                      client_pid    = OldClientPid,
                                                      msg_queue     = Queue,
                                                      awaiting_ack  = AwaitingAck,
                                                      awaiting_comp = AwaitingComp,
                                                      expire_timer  = ETimer}) ->
    lager:info("Session ~s resumed by ~p", [ClientId, ClientPid]),

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
    emqttd_utils:cancel_timer(ETimer),

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

    {noreply, State#session_state{client_pid   = ClientPid,
                                  msg_queue    = emqttd_queue:clear(Queue),
                                  expire_timer = undefined}, hibernate};

handle_cast({publish, ClientId, {?QOS_2, Message}}, State) ->
    NewState = publish(State, ClientId, {?QOS_2, Message}),
    {noreply, NewState};

handle_cast({puback, PacketId}, State) ->
    NewState = puback(State, {?PUBACK, PacketId}),
    {noreply, NewState};

handle_cast({pubrec, PacketId}, State) ->
    NewState = puback(State, {?PUBREC, PacketId}),
    {noreply, NewState};

handle_cast({pubrel, PacketId}, State) ->
    NewState = puback(State, {?PUBREL, PacketId}),
    {noreply, NewState};

handle_cast({pubcomp, PacketId}, State) ->
    NewState = puback(State, {?PUBCOMP, PacketId}),
    {noreply, NewState};

handle_cast({destroy, ClientId}, State = #session_state{clientid = ClientId}) ->
    lager:warning("Session ~s destroyed", [ClientId]),
    {stop, normal, State};

handle_cast(Msg, State) ->
    lager:critical("Unexpected Msg: ~p, State: ~p", [Msg, State]), 
    {noreply, State}.

handle_info({dispatch, {_From, Messages}}, State) when is_list(Messages) ->
    F = fun(Message, S) -> dispatch(Message, S) end,
    {noreply, lists:foldl(F, State, Messages)};

handle_info({dispatch, {_From, Message}}, State) ->
    {noreply, dispatch(Message, State)};

handle_info({'EXIT', ClientPid, Reason}, State = #session_state{clientid = ClientId,
                                                                client_pid = ClientPid}) ->
    lager:error("Session: client ~s@~p exited, caused by ~p", [ClientId, ClientPid, Reason]),
    {noreply, start_expire_timer(State#session_state{client_pid = undefined})};

handle_info({'EXIT', ClientPid0, _Reason}, State = #session_state{client_pid = ClientPid}) ->
    lager:error("Unexpected Client EXIT: pid=~p, pid(state): ~p", [ClientPid0, ClientPid]),
    {noreply, State};

handle_info(session_expired, State = #session_state{clientid = ClientId}) ->
    lager:warning("Session ~s expired!", [ClientId]),
    {stop, {shutdown, expired}, State};

handle_info(Info, State) ->
    lager:critical("Unexpected Info: ~p, State: ~p", [Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

dispatch(Message, State = #session_state{clientid = ClientId, 
                                         client_pid = undefined}) ->
    queue(ClientId, Message, State);

dispatch(Message = #mqtt_message{qos = ?QOS_0}, State = #session_state{client_pid = ClientPid}) ->
    ClientPid ! {dispatch, {self(), Message}},
    State;

dispatch(Message = #mqtt_message{qos = Qos}, State = #session_state{client_pid = ClientPid})
        when (Qos =:= ?QOS_1) orelse (Qos =:= ?QOS_2) ->
    {Message1, NewState} = store(State, Message),
    ClientPid ! {dispatch, {self(), Message1}},
    NewState.

queue(ClientId, Message, State = #session_state{msg_queue = Queue}) ->
    State#session_state{msg_queue = emqttd_queue:in(ClientId, Message, Queue)}.

next_msg_id(State = #session_state{message_id = 16#ffff}) ->
    State#session_state{message_id = 1};

next_msg_id(State = #session_state{message_id = MsgId}) ->
    State#session_state{message_id = MsgId + 1}.

start_expire_timer(State = #session_state{expires = Expires,
                                          expire_timer = OldTimer}) ->
    emqttd_utils:cancel_timer(OldTimer),
    Timer = erlang:send_after(Expires * 1000, self(), session_expired),
    State#session_state{expire_timer = Timer}.

