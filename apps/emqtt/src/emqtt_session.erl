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
		packet_id   = 1,
        submap      :: map(),
        messages    = [], %% do not receive rel
        awaiting_ack :: map(),
        awaiting_rel :: map(),
        expires,
        expire_timer,
        max_queue }).

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

publish(_, {?QOS_0, Message}) ->
    emqtt_router:route(Message);
%%TODO:
publish(_, {?QOS_1, Message}) ->
	emqtt_router:route(Message);
%%TODO:
publish(SessState = #session_state{awaiting_rel = Awaiting}, 
    {?QOS_2, Message = #mqtt_message{ msgid = MsgId }}) ->
    %% store in awaiting map 
    %%TODO: TIMEOUT
    Awaiting1 = maps:put(MsgId,  Message, Awaiting),
    SessState#session_state{awaiting_rel = Awaiting1};

publish(SessPid, {?QOS_2, Message}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {publish, ?QOS_2, Message}),
    SessPid.

puback(SessState = #session_state{client_id = ClientId, awaiting_ack = Awaiting}, {?PUBACK, PacketId}) ->
    Awaiting1 = 
    case maps:is_key(PacketId, Awaiting) of
        true -> maps:remove(PacketId, Awaiting);
        false -> lager:warning("~s puback packetid '~p' not exist", [ClientId, PacketId])
    end,
    SessState#session_state{awaiting_ack= Awaiting1};
puback(SessPid, {?PUBACK, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {puback, PacketId}), SessPid;

puback(SessState = #session_state{}, {?PUBREC, PacketId}) ->
    %%TODO'
   SessState;
puback(SessPid, {?PUBREC, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubrec, PacketId}), SessPid;

puback(SessState = #session_state{}, {?PUBREL, PacketId}) ->
	%FIXME Later: should release the message here
	%%emqtt_router:route(Message).
    'TODO', erase({msg, PacketId}), SessState;
puback(SessPid, {?PUBREL, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubrel, PacketId}), SessPid;

puback(SessState = #session_state{}, {?PUBCOMP, PacketId}) ->
    'TODO', SessState;
puback(SessPid, {?PUBCOMP, PacketId}) when is_pid(SessPid) ->
    gen_server:cast(SessPid, {pubcomp, PacketId}), SessPid.

subscribe(SessState = #session_state{client_id = ClientId, submap = SubMap}, Topics) ->
    Resubs = [Topic || {Name, _Qos} = Topic <- Topics, maps:is_key(Name, SubMap)], 
    case Resubs of
        [] -> ok;
        _  -> lager:warning("~s resubscribe ~p", [ClientId, Resubs])
    end,
    SubMap1 = lists:foldl(fun({Name, Qos}, Acc) -> maps:put(Name, Qos, Acc) end, SubMap, Topics),
    [ok = emqtt_pubsub:subscribe({Topic, Qos}, self()) || {Topic, Qos} <- Topics],
    %%TODO: granted all?
    GrantedQos = [Qos || {_Name, Qos} <- Topics],
    {ok, SessState#session_state{submap = SubMap1}, GrantedQos};

subscribe(SessPid, Topics) when is_pid(SessPid) ->
    {ok, GrantedQos} = gen_server:call(SessPid, {subscribe, Topics}),
    {ok, SessPid, GrantedQos}.

unsubscribe(SessState = #session_state{client_id = ClientId, submap = SubMap}, Topics) ->
    %%TODO: refactor later.
    case Topics -- maps:keys(SubMap) of
        [] -> ok;
        BadUnsubs -> lager:warning("~s should not unsubscribe ~p", [ClientId, BadUnsubs])
    end,
    %%unsubscribe from topic tree
    [ok = emqtt_pubsub:unsubscribe(Topic, self()) || Topic <- Topics],
    SubMap1 = lists:foldl(fun(Topic, Acc) -> maps:remove(Topic, Acc) end, SubMap, Topics),
    {ok, SessState#session_state{submap = SubMap1}};

unsubscribe(SessPid, Topics) when is_pid(SessPid) ->
    gen_server:call(SessPid, {unsubscribe, Topics}),
    {ok, SessPid}.

destroy(SessPid, ClientId)  when is_pid(SessPid) ->
    gen_server:cast(SessPid, {destroy, ClientId}).

initial_state(ClientId) ->
    #session_state { client_id  = ClientId,
                     packet_id  = 1, 
                     submap     = #{}, 
                     awaiting_ack = #{},
                     awaiting_rel = #{} }.

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
    %%TODO: OK?
    true = link(ClientPid),
    State = initial_state(ClientId, ClientPid),
    {ok, State#session_state{ 
            expires = proplists:get_value(expires, SessOpts, 24) * 3600, 
            max_queue = proplists:get_value(max_queue, SessOpts, 1000) } }.

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
        messages = Messages,
        expire_timer = ETimer}) ->
    lager:info("Session: client ~s resumed by ~p", [ClientId, ClientPid]),
    erlang:cancel_timer(ETimer),
    [ClientPid ! {dispatch, {self(), Message}} || Message <- lists:reverse(Messages)],
    NewState = State#session_state{ client_pid = ClientPid, messages = [], expire_timer = undefined},
    {noreply, NewState};

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
    lager:warning("Session: ~s destroyed", [ClientId]),
    {stop, normal, State};

handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info({dispatch, {_From, Message}}, State = #session_state{
        client_pid = undefined, messages = Messages}) ->
    %%TODO: queue len
    NewState = State#session_state{messages = [Message | Messages]},
    {noreply, NewState};

handle_info({dispatch, {_From, Message}}, State = #session_state{client_pid = ClientPid}) ->
    %%TODO: replace From with self(), ok?
    ClientPid ! {dispatch, {self(), Message}},
    {noreply, State};

handle_info({'EXIT', ClientPid, Reason}, State = #session_state{ 
        client_id = ClientId, client_pid = ClientPid, expires = Expires}) ->
    lager:warning("Session: client ~s@~p exited, caused by ~p", [ClientId, ClientPid, Reason]),
    Timer = erlang:send_after(Expires * 1000, self(), session_expired),
    {noreply, State#session_state{ client_pid = undefined, expire_timer = Timer}};

handle_info(session_expired, State = #session_state{client_id = ClientId}) ->
    lager:warning("Session: ~s session expired!", [ClientId]),
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


