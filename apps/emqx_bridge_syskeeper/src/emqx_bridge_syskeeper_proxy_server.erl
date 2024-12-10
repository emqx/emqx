%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_syskeeper_proxy_server).

-behaviour(gen_statem).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%% `emqx_resource' API
-export([
    resource_type/0,
    query_mode/1,
    on_start/2,
    on_stop/2,
    on_get_status/2
]).

%% API
-export([start_link/3]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([handle_event/4]).

-type state() :: wait_ready | handshake | running.
-type data() :: #{
    transport := atom(),
    socket := inet:socket(),
    frame_state :=
        undefined
        | emqx_bridge_syskeeper_frame:state(),
    buffer := binary(),
    conf := map()
}.

-define(DEFAULT_PORT, 9092).

%% -------------------------------------------------------------------------------------------------
%% emqx_resource
resource_type() ->
    syskeeper_proxy_server.

query_mode(_) ->
    no_queries.

on_start(
    InstanceId,
    #{
        listen := Server,
        acceptors := Acceptors
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_syskeeper_proxy_server",
        connector => InstanceId,
        config => Config
    }),

    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, #{
        default_port => ?DEFAULT_PORT
    }),
    ListenOn = {Host, Port},

    Options = [
        {acceptors, Acceptors},
        {tcp_options, [{mode, binary}, {reuseaddr, true}, {nodelay, true}]}
    ],
    MFArgs = {?MODULE, start_link, [maps:with([handshake_timeout], Config)]},

    %% Since the esockd only supports atomic name and we don't want to introduce a new atom per each instance
    %% when the port is same for two instance/connector, them will reference to a same esockd listener
    %% to prevent the failed one dealloctes the listener which created by a earlier instance
    %% we need record only when the listen is successed
    case esockd:open(?MODULE, ListenOn, Options, MFArgs) of
        {ok, _} ->
            ok = emqx_resource:allocate_resource(InstanceId, listen_on, ListenOn),
            {ok, #{listen_on => ListenOn}};
        {error, {already_started, _}} ->
            {error, eaddrinuse};
        Error ->
            Error
    end.

on_stop(InstanceId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_syskeeper_proxy_server",
        connector => InstanceId
    }),
    case emqx_resource:get_allocated_resources(InstanceId) of
        #{listen_on := ListenOn} ->
            case esockd:close(?MODULE, ListenOn) of
                {error, not_found} ->
                    ok;
                Result ->
                    Result
            end;
        _ ->
            ok
    end.

on_get_status(_InstanceId, #{listen_on := ListenOn}) ->
    try
        _ = esockd:listener({?MODULE, ListenOn}),
        ?status_connected
    catch
        _:_ ->
            ?status_disconnected
    end.

%% -------------------------------------------------------------------------------------------------
-spec start_link(atom(), inet:socket(), map()) ->
    {ok, Pid :: pid()}
    | ignore
    | {error, Error :: term()}.
start_link(Transport, Socket, Conf) ->
    gen_statem:start_link(?MODULE, [Transport, Socket, Conf], []).

%% -------------------------------------------------------------------------------------------------
%% gen_statem callbacks

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> handle_event_function.

%% -------------------------------------------------------------------------------------------------
-spec init(Args :: term()) ->
    gen_statem:init_result(term()).
init([Transport, Socket, Conf]) ->
    {ok, wait_ready,
        #{
            transport => Transport,
            socket => Socket,
            conf => Conf,
            buffer => <<>>,
            frame_state => undefined
        },
        {next_event, internal, wait_ready}}.

handle_event(internal, wait_ready, wait_ready, Data) ->
    wait_ready(Data);
handle_event(state_timeout, handshake_timeout, handshake, Data) ->
    ?SLOG(info, #{
        msg => "syskeeper_proxy_server_handshake_timeout",
        data => Data
    }),
    {stop, normal};
handle_event(internal, try_parse, running, Data) ->
    try_parse(running, Data);
handle_event(info, {tcp, _Socket, Bin}, State, Data) ->
    try_parse(State, combine_buffer(Bin, Data));
handle_event(info, {tcp_closed, _}, _State, _Data) ->
    {stop, normal};
handle_event(info, {tcp_error, Error, Reason}, _State, _Data) ->
    ?SLOG(warning, #{
        msg => "syskeeper_proxy_server_tcp_error",
        error => Error,
        reason => Reason
    }),
    {stop, normal};
handle_event(Event, Content, State, Data) ->
    ?SLOG(warning, #{
        msg => "syskeeper_proxy_server_unexpected_event",
        event => Event,
        content => Content,
        state => State,
        data => Data
    }),
    keep_state_and_data.

-spec terminate(Reason :: term(), State :: state(), Data :: data()) ->
    any().
terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%% -------------------------------------------------------------------------------------------------
%%% Internal functions
send(#{transport := Transport, socket := Socket}, Bin) ->
    Transport:send(Socket, Bin).

ack(Data) ->
    ack(Data, true).

ack(Data, false) ->
    send(Data, <<0>>);
ack(Data, true) ->
    send(Data, <<16#FF>>).

wait_ready(
    #{
        transport := Transport,
        socket := RawSocket,
        conf := #{handshake_timeout := Timeout}
    } =
        Data
) ->
    case Transport:wait(RawSocket) of
        {ok, Socket} ->
            Transport:setopts(Socket, [{active, true}]),
            {next_state, handshake,
                Data#{
                    socket => Socket,
                    frame_state => undefined
                },
                {state_timeout, Timeout, handshake_timeout}};
        {error, Reason} ->
            ok = Transport:fast_close(RawSocket),
            ?SLOG(error, #{
                msg => "syskeeper_proxy_server_listen_error",
                transport => Transport,
                reason => Reason
            }),
            {stop, Reason}
    end.

combine_buffer(Bin, #{buffer := Buffer} = Data) ->
    Data#{buffer := <<Buffer/binary, Bin/binary>>}.

try_parse(State, #{buffer := Bin} = Data) ->
    case emqx_bridge_syskeeper_frame:parse_variable_byte_integer(Bin) of
        {ok, Len, Rest} ->
            case Rest of
                <<Payload:Len/binary, Rest2/binary>> ->
                    Data2 = Data#{buffer := Rest2},
                    Result = parse(Payload, Data2),
                    handle_parse_result(Result, State, Data2);
                _ ->
                    {keep_state, Data}
            end;
        {error, incomplete} ->
            {keep_state, Data};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "syskeeper_proxy_server_try_parse_error",
                state => State,
                data => Data,
                reason => Reason
            }),
            {stop, parse_error}
    end.

%% maybe handshake
parse(Bin, #{frame_state := undefined}) ->
    emqx_bridge_syskeeper_frame:parse_handshake(Bin);
parse(Bin, #{frame_state := State}) ->
    emqx_bridge_syskeeper_frame:parse(Bin, State).

do_forward(Ack, Messages, Data) ->
    lists:foreach(
        fun(Message) ->
            Msg = emqx_message:from_map(Message#{headers => #{}, extra => #{}}),
            _ = emqx_broker:safe_publish(Msg)
        end,
        Messages
    ),
    case Ack of
        true ->
            ack(Data);
        _ ->
            ok
    end.

handle_parse_result({ok, Msg}, State, Data) ->
    handle_packet(Msg, State, Data);
handle_parse_result({error, Reason} = Error, State, Data) ->
    handle_parse_error(Error, State, #{buffer := _Bin} = Data),
    ?SLOG(error, #{
        msg => "syskeeper_proxy_server_parse_result_error",
        state => State,
        data => Data,
        reason => Reason
    }),
    {stop, parse_error}.

handle_parse_error(_, handshake, Data) ->
    ack(Data, false);
handle_parse_error(_, _, _) ->
    ok.

handle_packet({FrameState, _Shake}, handshake, Data) ->
    ack(Data),
    {next_state, running, Data#{frame_state := FrameState}, {next_event, internal, try_parse}};
handle_packet(#{type := forward, ack := Ack, messages := Messages}, running, Data) ->
    do_forward(Ack, Messages, Data),
    try_parse(running, Data);
handle_packet(#{type := heartbeat}, running, Data) ->
    ack(Data),
    try_parse(running, Data).
