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
%%% emqttd session process of persistent client.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_session_proc).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include_lib("emqtt/include/emqtt.hrl").

-include_lib("emqtt/include/emqtt_packet.hrl").

%% Start gen_server
-export([start_link/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Refactor this API.
start({false = _CleanSess, ClientId, ClientPid}) ->
    {ok, SessPid} = emqttd_sm:start_session(ClientId, ClientPid),
    {ok, SessPid}.

%%------------------------------------------------------------------------------
%% @doc Start a session process.
%% @end
%%------------------------------------------------------------------------------
start_link(ClientId, ClientPid) ->
    gen_server:start_link(?MODULE, [ClientId, ClientPid], []).

resume(SessProc, ClientId, ClientPid) when is_pid(SessProc) ->
    cast(SessProc, {resume, ClientId, ClientPid}).

-spec publish(pid(), mqtt_clientid(), {mqtt_qos(), mqtt_message()}) -> pid().
publish(SessProc, ClientId, {?QOS_0, Message}) when is_pid(SessProc) ->
    emqttd_pubsub:publish(ClientId, Message), Session;

publish(SessProc, ClientId, {?QOS_1, Message}) when is_pid(SessProc) ->
	emqttd_pubsub:publish(ClientId, Message), Session;

publish(SessProc, ClientId, {?QOS_2, Message}) when is_pid(SessProc) ->
    cast(SessProc, {publish, ClientId, {?QOS_2, Message}}).

puback(SessProc, {?PUBACK, PacketId}) when is_pid(SessProc) ->
    cast(SessProc, {puback, PacketId}).

puback(SessProc, {?PUBREL, PacketId}) when is_pid(SessProc) ->
    cast(SessPid, {pubrel, PacketId}).

puback(SessPid, {?PUBCOMP, PacketId}) when is_pid(SessPid) ->
    cast(SessPid, {pubcomp, PacketId}).

subscribe(SessPid, Topics) when is_pid(SessPid) ->
    {ok, GrantedQos} = gen_server:call(SessPid, {subscribe, Topics}),
    {ok, SessPid, GrantedQos}.

unsubscribe(SessPid, Topics) when is_pid(SessPid) ->
    gen_server:call(SessPid, {unsubscribe, Topics}),
    {ok, SessPid}.

-spec destroy(SessPid :: pid(), ClientId :: binary()) -> ok.
destroy(SessPid, ClientId)  when is_pid(SessPid) ->
    gen_server:cast(SessPid, {destroy, ClientId}).

cast(SessProc, Msg) ->
    gen_server:cast(SessProc, Msg), SessProc.


%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([ClientId, ClientPid]) ->
    process_flag(trap_exit, true),
    true = link(ClientPid),
    State = initial_state(ClientId, ClientPid),
    MQueue = emqttd_mqueue:new(ClientId, emqttd:env(mqtt, queue)),
    State1 = State#session{pending_queue = MQueue,
                                 timestamp = os:timestamp()},
    {ok, init(emqttd:env(mqtt, session), State1), hibernate}.

init([], State) ->
    State;

%% Session expired after hours
init([{expired_after, Hours} | Opts], State) ->
    init(Opts, State#session{sess_expired_after = Hours * 3600});
    
%% Max number of QoS 1 and 2 messages that can be “inflight” at one time.
init([{max_inflight_messages, MaxInflight} | Opts], State) ->
    init(Opts, State#session{inflight_window = MaxInflight});

%% Max retries for unacknolege Qos1/2 messages
init([{max_unack_retries, Retries} | Opts], State) ->
    init(Opts, State#session{max_unack_retries = Retries});

%% Retry after 4, 8, 16 seconds
init([{unack_retry_after, Secs} | Opts], State) ->
    init(Opts, State#session{unack_retry_after = Secs});

%% Awaiting PUBREL timeout
init([{await_rel_timeout, Secs} | Opts], State) ->
    init(Opts, State#session{await_rel_timeout = Secs});

init([Opt | Opts], State) ->
    lager:error("Bad Session Option: ~p", [Opt]),
    init(Opts, State).

handle_call({subscribe, Topics}, _From, State) ->
    {ok, NewState, GrantedQos} = subscribe(State, Topics),
    {reply, {ok, GrantedQos}, NewState};

handle_call({unsubscribe, Topics}, _From, State) ->
    {ok, NewState} = unsubscribe(State, Topics),
    {reply, ok, NewState};

handle_call(Req, _From, State) ->
    lager:error("Unexpected request: ~p", [Req]),
    {reply, error, State}.

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

handle_cast({destroy, ClientId}, State = #session{clientid = ClientId}) ->
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

handle_info({'EXIT', ClientPid, Reason}, State = #session{clientid = ClientId,
                                                                client_pid = ClientPid}) ->
    lager:info("Session: client ~s@~p exited for ~p", [ClientId, ClientPid, Reason]),
    {noreply, start_expire_timer(State#session{client_pid = undefined})};

handle_info({'EXIT', ClientPid0, _Reason}, State = #session{client_pid = ClientPid}) ->
    lager:error("Unexpected Client EXIT: pid=~p, pid(state): ~p", [ClientPid0, ClientPid]),
    {noreply, State};

handle_info(session_expired, State = #session{clientid = ClientId}) ->
    lager:warning("Session ~s expired!", [ClientId]),
    {stop, {shutdown, expired}, State};

handle_info({timeout, awaiting_rel, MsgId}, SessState) ->
    NewState = timeout(awaiting_rel, MsgId, SessState),
    {noreply, NewState};

handle_info(Info, State) ->
    lager:critical("Unexpected Info: ~p, State: ~p", [Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


