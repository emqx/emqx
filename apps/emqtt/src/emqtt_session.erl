%%-----------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------

-module(emqtt_session).

-include("emqtt.hrl").

-include("emqtt_packet.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([start/1, resume/3, publish/2, puback/2, subscribe/2, unsubscribe/2, destroy/2]).

-export([store/2]).

%%start gen_server
-export([start_link/3]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(session_state, { 
        client_id   :: binary(),
        client_pid  :: pid(),
		message_id  = 1,
        submap      :: map(),
        msg_queue, %% do not receive rel
        awaiting_ack :: map(),
        awaiting_rel :: map(),
        awaiting_comp :: map(),
        expires,
        expire_timer }).

%% ------------------------------------------------------------------
%% Start Session
%% ------------------------------------------------------------------
start({true = _CleanSess, ClientId, _ClientPid}) ->
    %%Destroy old session if CleanSess is true before.
    ok = emqtt_sm:destroy_session(ClientId),
    {ok, initial_state(ClientId)};

start({false = _CleanSess, ClientId, ClientPid}) ->
    {ok, SessPid} = emqtt_sm:start_session(ClientId, ClientPid),
    {ok, SessPid}.

%% ------------------------------------------------------------------
%% Session API
%% ------------------------------------------------------------------
resume(SessState = #session_state{}, _ClientId, _ClientPid) ->
    SessState;
resume(SessPid, ClientId, ClientPid) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {resume, ClientId, ClientPid}),
    SessPid.

publish(Session, {?QOS_0, Message}) ->
    emqtt_router:route(Message), Session;

publish(Session, {?QOS_1, Message}) ->
	emqtt_router:route(Message), Session;

publish(SessState = #session_state{awaiting_rel = AwaitingRel}, 
    {?QOS_2, Message = #mqtt_message{ msgid = MsgId }}) ->
    %% store in awaiting_rel
    SessState#session_state{awaiting_rel = maps:put(MsgId, Message, AwaitingRel)};

publish(SessPid, {?QOS_2, Message}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {publish, ?QOS_2, Message}),
    SessPid.

%% PUBACK
puback(SessState = #session_state{client_id = ClientId, awaiting_ack = Awaiting}, {?PUBACK, PacketId}) ->
    case maps:is_key(PacketId, Awaiting) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBACK PacketId '~p' not found!", [ClientId, PacketId])
    end,
    SessState#session_state{awaiting_ack = maps:remove(PacketId, Awaiting)};
puback(SessPid, {?PUBACK, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {puback, PacketId}), SessPid;

%% PUBREC
puback(SessState = #session_state{ client_id = ClientId, 
                                   awaiting_ack = AwaitingAck,
                                   awaiting_comp = AwaitingComp }, {?PUBREC, PacketId}) ->
    case maps:is_key(PacketId, AwaitingAck) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBREC PacketId '~p' not found!", [ClientId, PacketId])
    end,
    SessState#session_state{ awaiting_ack   = maps:remove(PacketId, AwaitingAck), 
                             awaiting_comp  = maps:put(PacketId, true, AwaitingComp) };

puback(SessPid, {?PUBREC, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubrec, PacketId}), SessPid;

%% PUBREL
puback(SessState = #session_state{client_id = ClientId, awaiting_rel = Awaiting}, {?PUBREL, PacketId}) ->
    case maps:find(PacketId, Awaiting) of
        {ok, Msg} -> emqtt_router:route(Msg);
        error -> lager:warning("Session ~s: PUBREL PacketId '~p' not found!", [ClientId, PacketId])
    end,
    SessState#session_state{awaiting_rel = maps:remove(PacketId, Awaiting)};

puback(SessPid, {?PUBREL, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubrel, PacketId}), SessPid;

%% PUBCOMP
puback(SessState = #session_state{ client_id = ClientId, 
                                   awaiting_comp = AwaitingComp}, {?PUBCOMP, PacketId}) ->
    case maps:is_key(PacketId, AwaitingComp) of
        true -> ok;
        false -> lager:warning("Session ~s: PUBREC PacketId '~p' not exist", [ClientId, PacketId])
    end,
    SessState#session_state{ awaiting_comp  = maps:remove(PacketId, AwaitingComp) };

puback(SessPid, {?PUBCOMP, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubcomp, PacketId}), SessPid.

%% SUBSCRIBE
subscribe(SessState = #session_state{client_id = ClientId, submap = SubMap}, Topics) ->
    Resubs = [Topic || {Name, _Qos} = Topic <- Topics, maps:is_key(Name, SubMap)], 
    case Resubs of
        [] -> ok;
        _  -> lager:warning("~s resubscribe ~p", [ClientId, Resubs])
    end,
    SubMap1 = lists:foldl(fun({Name, Qos}, Acc) -> maps:put(Name, Qos, Acc) end, SubMap, Topics),
    {ok, GrantedQos} = emqtt_pubsub:subscribe(Topics, self()),
    %%TODO: should be gen_event and notification...
    emqtt_server:subscribe([ Name || {Name, _} <- Topics ], self()),
    {ok, SessState#session_state{submap = SubMap1}, GrantedQos};

subscribe(SessPid, Topics) when is_pid(SessPid) ->
    {ok, GrantedQos} = gen_server:call(SessPid, {subscribe, Topics}),
    {ok, SessPid, GrantedQos}.

%%
%% @doc UNSUBSCRIBE
%%
unsubscribe(SessState = #session_state{client_id = ClientId, submap = SubMap}, Topics) ->
    %%TODO: refactor later.
    case Topics -- maps:keys(SubMap) of
        [] -> ok;
        BadUnsubs -> lager:warning("~s should not unsubscribe ~p", [ClientId, BadUnsubs])
    end,
    %%unsubscribe from topic tree
    ok = emqtt_pubsub:unsubscribe(Topics, self()),
    SubMap1 = lists:foldl(fun(Topic, Acc) -> maps:remove(Topic, Acc) end, SubMap, Topics),
    {ok, SessState#session_state{submap = SubMap1}};

unsubscribe(SessPid, Topics) when is_pid(SessPid) ->
    gen_server:call(SessPid, {unsubscribe, Topics}),
    {ok, SessPid}.

destroy(SessPid, ClientId)  when is_pid(SessPid) ->
    gen_server:cast(SessPid, {destroy, ClientId}).

%store message(qos1) that sent to client
store(SessState = #session_state{ message_id = MsgId, awaiting_ack = Awaiting}, 
    Message = #mqtt_message{ qos = Qos }) when (Qos =:= ?QOS_1) orelse (Qos =:= ?QOS_2) ->
    %%assign msgid before send
    Message1 = Message#mqtt_message{ msgid = MsgId },
    Message2 =
    if
        Qos =:= ?QOS_2 -> Message1#mqtt_message{dup = false};
        true -> Message1
    end,
    Awaiting1 = maps:put(MsgId, Message2, Awaiting),
    {Message1, next_msg_id(SessState#session_state{ awaiting_ack = Awaiting1 })}.

initial_state(ClientId) ->
    #session_state { client_id  = ClientId,
                     submap     = #{}, 
                     awaiting_ack = #{},
                     awaiting_rel = #{},
                     awaiting_comp = #{} }.

initial_state(ClientId, ClientPid) ->
    State = initial_state(ClientId),
    State#session_state{client_pid = ClientPid}.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

start_link(SessOpts, ClientId, ClientPid) ->
    gen_server:start_link(?MODULE, [SessOpts, ClientId, ClientPid], []).

init([SessOpts, ClientId, ClientPid]) ->
    process_flag(trap_exit, true),
    %%TODO: Is this OK?
    true = link(ClientPid),
    State = initial_state(ClientId, ClientPid),
    Expires = proplists:get_value(expires, SessOpts, 1) * 3600,
    MsgQueue = emqtt_queue:new( proplists:get_value(max_queue, SessOpts, 1000), 
                                proplists:get_value(store_qos0, SessOpts, false) ),
    {ok, State#session_state{ expires = Expires,
                              msg_queue = MsgQueue }, hibernate}.

handle_call({subscribe, Topics}, _From, State) ->
    {ok, NewState, GrantedQos} = subscribe(State, Topics),
    {reply, {ok, GrantedQos}, NewState};

handle_call({unsubscribe, Topics}, _From, State) ->
    {ok, NewState} = unsubscribe(State, Topics),
    {reply, ok, NewState};

handle_call(Req, _From, State) ->
    {stop, {badreq, Req}, State}.

handle_cast({resume, ClientId, ClientPid}, State = #session_state { 
                                                client_id = ClientId, 
                                                client_pid = undefined, 
                                                msg_queue = Queue,
                                                awaiting_ack = AwaitingAck,
                                                awaiting_comp = AwaitingComp,
                                                expire_timer = ETimer}) ->
    lager:info("Session ~s resumed by ~p", [ClientId, ClientPid]),
    %cancel timeout timer
    erlang:cancel_timer(ETimer),

    %% redelivery PUBREL
    lists:foreach(fun(PacketId) ->
                ClientPid ! {redeliver, {?PUBREL, PacketId}}
        end, maps:keys(AwaitingComp)),

    %% redelivery messages that awaiting PUBACK or PUBREC
    Dup = fun(Msg) -> Msg#mqtt_message{ dup = true } end,
    lists:foreach(fun(Msg) ->
                ClientPid ! {dispatch, {self(), Dup(Msg)}}
        end, maps:values(AwaitingAck)),

    %% send offline messages
    lists:foreach(fun(Msg) ->
                ClientPid ! {dispatch, {self(), Msg}}
        end, emqtt_queue:all(Queue)),

    NewState = State#session_state{ client_pid = ClientPid, 
                                    msg_queue = emqtt_queue:clear(Queue), 
                                    expire_timer = undefined},
    {noreply, NewState, hibernate};

handle_cast({publish, ?QOS_2, Message}, State) ->
    NewState = publish(State, {?QOS_2, Message}),
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

handle_cast({destroy, ClientId}, State = #session_state{client_id = ClientId}) ->
    lager:warning("Session ~s destroyed", [ClientId]),
    {stop, normal, State};

handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info({dispatch, {_From, Message}}, State) ->
    {noreply, dispatch(Message, State)};

handle_info({'EXIT', ClientPid, Reason}, State = #session_state{ 
        client_id = ClientId, client_pid = ClientPid, expires = Expires}) ->
    lager:warning("Session: client ~s@~p exited, caused by ~p", [ClientId, ClientPid, Reason]),
    Timer = erlang:send_after(Expires * 1000, self(), session_expired),
    {noreply, State#session_state{ client_pid = undefined, expire_timer = Timer}};

handle_info(session_expired, State = #session_state{client_id = ClientId}) ->
    lager:warning("Session ~s expired!", [ClientId]),
    {stop, {shutdown, expired}, State};

handle_info(Info, State) ->
    {stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

dispatch(Message, State = #session_state{ client_id = ClientId, 
                                          client_pid = undefined }) ->
    queue(ClientId, Message, State);

dispatch(Message = #mqtt_message{ qos = ?QOS_0 }, State = #session_state{ 
        client_pid = ClientPid }) ->
    ClientPid ! {dispatch, {self(), Message}},
    State;

dispatch(Message = #mqtt_message{ qos = Qos }, State = #session_state{ client_pid = ClientPid }) 
    when (Qos =:= ?QOS_1) orelse (Qos =:= ?QOS_2) ->
    {Message1, NewState} = store(State, Message),
    ClientPid ! {dispatch, {self(), Message1}},
    NewState.

queue(ClientId, Message, State = #session_state{msg_queue = Queue}) ->
    State#session_state{msg_queue = emqtt_queue:in(ClientId, Message, Queue)}.

next_msg_id(State = #session_state{ message_id = 16#ffff }) ->
    State#session_state{ message_id = 1 };

next_msg_id(State = #session_state{ message_id = MsgId }) ->
    State#session_state{ message_id = MsgId + 1 }.

    
