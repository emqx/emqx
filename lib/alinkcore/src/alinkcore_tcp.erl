-module(alinkcore_tcp).
-include("alinkcore.hrl").

-export([start_listen/3, start_listen/4, start_link/4, send/2, sync_send/3, sync_send/4]).

-export([init/4, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    mod,
    conn_state,
    active_n,
    incoming_bytes = 0,
    rate_limit,
    limit_timer,
    child = #tcp_state{}
}).

-define(KEEPALIVE_IDLE, 30).
-define(KEEPALIVE_INTERVAL, 10).
-define(KEEPALIVE_PROBES, 3).

-define(TCP_KEEPALIVE_OPTS, [
    {raw, 6, 4, <<?KEEPALIVE_IDLE:32/native>>},
    {raw, 6, 5, <<?KEEPALIVE_INTERVAL:32/native>>},
    {raw, 6, 6, <<?KEEPALIVE_PROBES:32/native>>}
]).

-define(TCP_OPTIONS, [
    {backlog, 512},
    {keepalive, true},
    {send_timeout, 15000},
    {send_timeout_close, true},
    {nodelay, true},
    {reuseaddr, true},
    binary,
    {packet, raw},
    {exit_on_close, true}
]).

start_listen(Name, Mod, Port) ->
    start_listen(Name, Mod, Port, []).

start_listen(Name, Mod, Port, Opts) ->
    Spec = child_spec(Name, Mod, Port, Opts),
    esockd_sup:start_child(Spec).

start_link(Transport, Sock, Mod, Opts) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [Mod, Transport, Opts, Sock])}.

send(#tcp_state{transport = Transport, socket = Socket}, Payload) ->
    case Socket == undefined of
        true ->
            {error, disconnected};
        false ->
            Transport:send(Socket, Payload)
    end;
send(Pid, Bin) ->
    gen_server:call(Pid, {send, Bin}).

sync_send(State, Payload, Timeout) ->
    sync_send(State, Payload, Timeout, undefined).

sync_send(#tcp_state{ socket = Socket, transport = Transport }, Payload, Timeout, RecvDataFun) ->
    ok = inet:setopts(Socket, [{active, false}]),
    case Transport:send(Socket, Payload) of
        ok ->
            active_recv(Transport, Socket, Timeout, RecvDataFun);
        {error, Reason} ->
            {error, Reason}
    end.

active_recv(Transport, Socket, Timeout, RecvDataFun) ->
    case Transport:recv(Socket, 0, Timeout) of
        {ok, Data} when RecvDataFun =:= undefined ->
            ok = inet:setopts(Socket, [{active, once}]),
            {ok, Data};
        {ok, Data} ->
            case RecvDataFun(Data) of
                break ->
                    ok = inet:setopts(Socket, [{active, once}]),
                    {ok, Data};
                {break, RecvData} ->
                    ok = inet:setopts(Socket, [{active, once}]),
                    {ok, RecvData};
                continue ->
                    active_recv(Transport, Socket, Timeout, RecvDataFun);
                Err ->
                    {error, Err}
            end;
        {error, Reason} ->
            {error, Reason};
        Other ->
            {error, {unexpected_data, Other}}
    end.


init(Mod, Transport, Opts, Sock0) ->
    case Transport:wait(Sock0) of
        {ok, Sock} ->
            case Transport:setopts(Sock, ?TCP_KEEPALIVE_OPTS) of
                ok -> logger:info("custom_socket_options_successfully ~p", [#{opts => Opts}]);
                Err -> logger:error("failed_to_set_custom_socket_optionn ~p", [#{reason => Err}])
            end,
            {ok, {RemoteIp, _RemotePort}} = Transport:peername(Sock),
            RemoteIpB = alinkutil_type:to_binary(inet:ntoa(RemoteIp)),
            ChildState = #tcp_state{socket = Sock, transport = Transport, ip = RemoteIpB},
            case Mod:init(ChildState) of
                {ok, NewChildState} ->
                    GState = #state{
                        mod = Mod,
                        incoming_bytes = 0,
                        conn_state = running,
                        active_n = proplists:get_value(active_n, Opts, 8),
                        rate_limit = rate_limit(proplists:get_value(rate_limit, Opts)),
                        child = NewChildState
                    },
                    ok = activate_socket(GState),
                    gen_server:enter_loop(?MODULE, [], GState);
                {error, Reason} ->
                    {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({send, Bin}, _From, #state{child = ChildState} = State) ->
    Reply = send(ChildState, Bin),
    {reply, Reply, State};

handle_call(Request, From, #state{mod = Mod, child = ChildState} = State) ->
    case Mod:handle_call(Request, From, ChildState) of
        {reply, Reply, NewChildState} ->
            {reply, Reply, State#state{child = NewChildState}, hibernate};
        {noreply, NewChildState} ->
            {noreply, State#state{child = NewChildState}, hibernate};
        {stop, Reason, Reply, NewChildState} ->
            {stop, Reason, Reply, State#state{child = NewChildState}}
    end.

handle_cast(Msg, #state{mod = Mod, child = ChildState} = State) ->
    case Mod:handle_cast(Msg, ChildState) of
        {noreply, NewChildState} ->
            {noreply, State#state{child = NewChildState}, hibernate};
        {stop, Reason, NewChildState} ->
            {stop, Reason, State#state{child = NewChildState}}
    end.

handle_info(activate_socket, State) ->
    NewState = State#state{limit_timer = undefined, conn_state = running},
    ok = activate_socket(NewState),
    {noreply, NewState, hibernate};

handle_info({tcp_passive, _Sock}, State) ->
    NState = ensure_rate_limit(State),
    ok = activate_socket(NState),
    {noreply, NState};

handle_info({tcp, Sock, Data}, #state{mod = Mod, incoming_bytes = Incoming, child = ChildState} = State) ->
    #tcp_state{socket = Sock} = ChildState,
    Bin = iolist_to_binary(Data),
    Cnt = byte_size(Bin),
    Buff =
        case binary:referenced_byte_size(Bin) of
            Large when Large > 2 * Cnt ->
                binary:copy(Bin);
            _ ->
                Bin
        end,
    case Mod:handle_info({tcp, Buff}, ChildState) of
        {noreply, NewChild} ->
            {noreply, State#state{child = NewChild, incoming_bytes = Incoming + Cnt}, hibernate};
        {stop, Reason, NewChild} ->
            {stop, Reason, State#state{child = NewChild}}
    end;

handle_info({tcp_error, _Sock, Reason}, State) ->
    handle_error({tcp_closed, Reason}, State);

handle_info({tcp_closed, Sock}, #state{child = #tcp_state{socket = Sock}} = State) ->
    handle_error({tcp_closed, normal}, State);

handle_info(Info, #state{mod = Mod, child = ChildState} = State) ->
    case Mod:handle_info(Info, ChildState) of
        {noreply, NewChildState} ->
            {noreply, State#state{child = NewChildState}, hibernate};
        {stop, Reason, NewChildState} ->
            {stop, Reason, State#state{child = NewChildState}}
    end.

handle_error(Reason, #state{mod = Mod, child = ChildState} = State) ->
    case Mod:handle_info(Reason, ChildState) of
        {noreply, NewChild} ->
            {stop, normal, State#state{child = NewChild#tcp_state{socket = undefined}}};
        {stop, Reason1, NewChild} ->
            {stop, Reason1, State#state{child = NewChild#tcp_state{socket = undefined}}}
    end.

terminate(Reason, #state{mod = Mod, child = ChildState}) ->
    Mod:terminate(Reason, ChildState).

code_change(OldVsn, #state{mod = Mod, child = ChildState}, Extra) ->
    Mod:code_change(OldVsn, ChildState, Extra).


child_spec(Name, Mod, Port, Opts) ->
    TCPOpts = [
        {tcp_options, ?TCP_OPTIONS},
        {acceptors, 16},
        {max_connections, 1000000},
        {max_conn_rate, {1000, 1}}
    ],
    RateLimit = {1024, 4096},
    NewOpts = [{active_n, 10}, {rate_limit, RateLimit}] ++ Opts,
    MFArgs = {?MODULE, start_link, [Mod, NewOpts]},
    esockd:child_spec(Name, Port, TCPOpts, MFArgs).

rate_limit({Rate, Burst}) ->
    esockd_rate_limit:new(Rate, Burst).

activate_socket(#state{conn_state = blocked}) ->
    ok;
activate_socket(#state{child = #tcp_state{transport = Transport, socket = Socket}, active_n = N}) ->
    TrueOrN =
        case Transport:is_ssl(Socket) of
            true -> true; %% Cannot set '{active, N}' for SSL:(
            false -> N
        end,
    case Transport:setopts(Socket, [{active, TrueOrN}]) of
        ok -> ok;
        {error, Reason} ->
            self() ! {shutdown, Reason},
            ok
    end.

ensure_rate_limit(State) ->
    case esockd_rate_limit:check(State#state.incoming_bytes, State#state.rate_limit) of
        {0, RateLimit} ->
            State#state{conn_state = running, incoming_bytes = 0, rate_limit = RateLimit};
        {Pause, RateLimit} ->
            TRef = erlang:send_after(Pause, self(), activate_socket),
            State#state{conn_state = blocked, incoming_bytes = 0, rate_limit = RateLimit, limit_timer = TRef}
    end.


