-module(emqtt_client).

-behaviour(gen_server2).

-export([start_link/0, go/2]).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
        code_change/3,
		terminate/2]).

-include("emqtt.hrl").

-define(CLIENT_ID_MAXLEN, 23).

-record(state, 	{ socket,
				  conn_name,
				  await_recv,
				  connection_state,
				  conserve,
				  parse_state,
				  proc_state }).

-record(proc_state, { socket,
                      subscriptions,
                      consumer_tags,
                      unacked_pubs,
                      awaiting_ack,
                      awaiting_seqno,
                      message_id,
                      client_id,
                      clean_sess,
                      will_msg,
                      channels,
                      connection,
                      exchange }).


start_link() ->
    gen_server2:start_link(?MODULE, [], []).

go(Pid, Sock) ->
	gen_server2:call(Pid, {go, Sock}).

init([]) ->
    {ok, undefined, hibernate, {backoff, 1000, 1000, 10000}}.

handle_call({go, Sock}, _From, _State) ->
    process_flag(trap_exit, true),
    ok = throw_on_error(
           inet_error, fun () -> emqtt_net:tune_buffer_size(Sock) end),
    {ok, ConnStr} = emqtt_net:connection_string(Sock, inbound),
    error_logger:info_msg("accepting MQTT connection (~s)~n", [ConnStr]),
    control_throttle(
       #state{ socket           = Sock,
               conn_name        = ConnStr,
               await_recv       = false,
               connection_state = running,
               conserve         = false,
               parse_state      = emqtt_frame:initial_state(),
               proc_state       = emqtt_processor:initial_state(Sock) }).

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info({route, Msg}, State) ->
	emqtt_processor:send_client(Msg),
    {noreply, State};

handle_info({inet_reply, _Ref, ok}, State) ->
    {noreply, State, hibernate};

handle_info({inet_async, Sock, _Ref, {ok, Data}}, #state{ socket = Sock }=State) ->
    process_received_bytes(
      Data, control_throttle(State #state{ await_recv = false }));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    network_error(Reason, State);

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
	error_logger:info_msg("sock error: ~p~n", [Reason]), 
	{noreply, State};

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
	
throw_on_error(E, Thunk) ->
    case Thunk() of
	{error, Reason} -> throw({E, Reason});
	{ok, Res}       -> Res;
	Res             -> Res
    end.

async_recv(Sock, Length, infinity) when is_port(Sock) ->
    prim_inet:async_recv(Sock, Length, -1);

async_recv(Sock, Length, Timeout) when is_port(Sock) ->
    prim_inet:async_recv(Sock, Length, Timeout).

process_received_bytes(<<>>, State) ->
    {noreply, State};
process_received_bytes(Bytes,
                       State = #state{ parse_state = ParseState,
                                       proc_state  = ProcState,
                                       conn_name   = ConnStr }) ->
    case
        emqtt_frame:parse(Bytes, ParseState) of
            {more, ParseState1} ->
                {noreply,
                 control_throttle( State #state{ parse_state = ParseState1 }),
                 hibernate};
            {ok, Frame, Rest} ->
                case emqtt_processor:process_frame(Frame, ProcState) of
                    {ok, ProcState1} ->
                        PS = emqtt_frame:initial_state(),
                        process_received_bytes(
                          Rest,
                          State #state{ parse_state = PS,
                                        proc_state = ProcState1 });
                    {err, Reason, ProcState1} ->
                        error_logger:info_msg("MQTT protocol error ~p for connection ~p~n",
                                  [Reason, ConnStr]),
                        stop({shutdown, Reason}, pstate(State, ProcState1));
                    {stop, ProcState1} ->
                        stop(normal, pstate(State, ProcState1))
                end;
            {error, Error} ->
                error_logger:erro_msg("MQTT detected framing error ~p for connection ~p~n",
                           [ConnStr, Error]),
                stop({shutdown, Error}, State)
    end.

pstate(State = #state {}, PState = #proc_state{}) ->
    State #state{ proc_state = PState }.

%%----------------------------------------------------------------------------
network_error(_Reason,
              State = #state{ conn_name  = ConnStr,
                              proc_state = PState }) ->
    error_logger:info_msg("MQTT detected network error for ~p~n", [ConnStr]),
    emqtt_processor:send_will(PState),
    % todo: flush channel after publish
    stop({shutdown, conn_closed}, State).

run_socket(State = #state{ connection_state = blocked }) ->
    State;
run_socket(State = #state{ await_recv = true }) ->
    State;
run_socket(State = #state{ socket = Sock }) ->
    async_recv(Sock, 0, infinity),
    State#state{ await_recv = true }.

control_throttle(State = #state{ connection_state = Flow,
                                 conserve         = Conserve }) ->
    case {Flow, Conserve orelse credit_flow:blocked()} of
        {running,   true} -> State #state{ connection_state = blocked };
        {blocked,  false} -> run_socket(State #state{
                                                connection_state = running });
        {_,            _} -> run_socket(State)
    end.

stop(Reason, State ) ->
    {stop, Reason, State}.

