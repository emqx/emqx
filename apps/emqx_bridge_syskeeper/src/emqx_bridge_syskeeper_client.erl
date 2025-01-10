%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_syskeeper_client).

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    forward/3,
    heartbeat/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-include("emqx_bridge_syskeeper.hrl").

-type duration() :: non_neg_integer().

-type state() :: #{
    ack_mode := need_ack | no_ack,
    ack_timeout := duration(),
    socket := undefined | inet:socket(),
    frame_state := emqx_bridge_syskeeper_frame:state(),
    last_error := undefined | tuple()
}.

-type send_result() :: {ok, state()} | {error, term()}.

%% -------------------------------------------------------------------------------------------------
%% API
forward(Pid, Msg, Timeout) ->
    call(Pid, {?FUNCTION_NAME, Msg}, Timeout).

heartbeat(Pid, Timeout) ->
    ok =:= call(Pid, ?FUNCTION_NAME, Timeout).

%% -------------------------------------------------------------------------------------------------
%% Starts Bridge which transfer data to Syskeeper

start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

%% -------------------------------------------------------------------------------------------------
%%% gen_server callbacks

%% Initialize syskeeper client
init(#{ack_timeout := AckTimeout, ack_mode := AckMode} = Options) ->
    erlang:process_flag(trap_exit, true),
    connect(Options, #{
        ack_timeout => AckTimeout,
        ack_mode => AckMode,
        socket => undefined,
        last_error => undefined,
        frame_state => emqx_bridge_syskeeper_frame:make_state_with_conf(Options)
    }).

handle_call({forward, Msgs}, _From, State) ->
    Result = send_packet(forward, Msgs, State),
    handle_reply_result(Result, State);
handle_call(heartbeat, _From, State) ->
    Result = send_ack_packet(heartbeat, none, State),
    handle_reply_result(Result, State);
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({tcp_closed, _} = Reason, State) ->
    {noreply, State#{socket := undefined, last_error := Reason}};
handle_info({last_error, _, _} = Reason, State) ->
    {noreply, State#{socket := undefined, last_error := Reason}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{socket := Socket} = _State) ->
    close_socket(Socket),
    ok.

%% ------------------------------------------------------------------------------------------------
connect(
    #{
        hostname := Host,
        port := Port
    },
    State
) ->
    case
        gen_tcp:connect(Host, Port, [
            {active, true},
            {mode, binary},
            {nodelay, true}
        ])
    of
        {ok, Socket} ->
            send_ack_packet(handshake, none, State#{socket := Socket});
        {error, Reason} ->
            {stop, Reason}
    end.

-spec send_ack_packet(packet_type(), packet_data(), state()) -> send_result().
send_ack_packet(Type, Data, State) ->
    send_packet(Type, Data, State, true).

-spec send_packet(packet_type(), packet_data(), state()) -> send_result().
send_packet(Type, Data, State) ->
    send_packet(Type, Data, State, false).

-spec send_packet(packet_type(), packet_data(), state(), boolean()) -> send_result().
send_packet(_Type, _Data, #{socket := undefined, last_error := Reason}, _Force) ->
    {error, Reason};
send_packet(Type, Data, #{frame_state := FrameState} = State, Force) ->
    Packet = emqx_bridge_syskeeper_frame:encode(Type, Data, FrameState),
    case socket_send(Packet, State) of
        ok ->
            wait_ack(State, Force);
        {error, _} = Error ->
            Error
    end.

-spec socket_send(binary() | [binary()], state()) -> ok | {error, _Reason}.
socket_send(Bin, State) when is_binary(Bin) ->
    socket_send([Bin], State);
socket_send(Bins, #{socket := Socket}) ->
    Map = fun(Data) ->
        Len = erlang:byte_size(Data),
        VarLen = emqx_bridge_syskeeper_frame:serialize_variable_byte_integer(Len),
        <<VarLen/binary, Data/binary>>
    end,
    gen_tcp:send(Socket, lists:map(Map, Bins)).

-spec wait_ack(state(), boolean()) -> send_result().
wait_ack(#{ack_timeout := AckTimeout, ack_mode := AckMode} = State, Force) when
    AckMode =:= need_ack; Force
->
    receive
        {tcp, _Socket, <<16#FF>>} ->
            {ok, State};
        {tcp_closed, _} = Reason ->
            {error, Reason};
        {tcp_error, _, _} = Reason ->
            {error, Reason}
    after AckTimeout ->
        {error, wait_ack_timeout}
    end;
wait_ack(State, _Force) ->
    {ok, State}.

close_socket(undefined) ->
    ok;
close_socket(Socket) ->
    catch gen_tcp:close(Socket),
    ok.

call(Pid, Msg, Timeout) ->
    gen_server:call(Pid, Msg, Timeout).

handle_reply_result({ok, _}, State) ->
    {reply, ok, State};
handle_reply_result({error, Reason}, State) ->
    {reply, {error, {recoverable_error, Reason}}, State#{last_error := Reason}}.
