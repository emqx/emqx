%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_selftrace).

-moduledoc "Collect semantic runtime events and custom slices from selected processes.".

-behaviour(gen_server).

-include("emqx_mqtt.hrl").

%% API
-export([
    start/1,
    stop/0,
    trace/2,
    untrace/1,
    trace_function/1
]).

%% Self-tracing API
-export([
    selected/2,
    enable/1,
    disable/0,
    enabled/0,
    instant/2,
    slice_open/2,
    slice_close/0,
    slice_close_all/0
]).

-export([packet_type/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([start_link/1]).

-type aspect() :: garbage_collection | send | 'receive' | call.
-type event_kind() :: slice_begin | slice_end | instant | counter.
-type event() :: #{
    pid := pid(),
    category := gc | call | message | custom,
    kind := event_kind(),
    name := term(),
    args := map(),
    timestamp := integer()
}.

-export_type([event/0, event_kind/0, aspect/0]).

-define(KEY, '$emqx_selftrace').
-define(SLICES, '$emqx_selftrace_slices').

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Start a capture under `emqx_sys_sup` supervisor.
Call `stop/0` to drain events and close JSON trace file.
""".
-spec start(file:name_all() | iodata()) ->
    {ok, pid()} | {error, term()}.
start(Path) ->
    supervisor:start_child(emqx_sys_sup, #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Path]},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }).

-spec start_link(file:name_all() | iodata()) -> {ok, pid()} | {error, term()}.
start_link(Path) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Path, []).

-spec stop() -> ok | {error, file:posix() | badarg | terminated}.
stop() ->
    Result = gen_server:call(?MODULE, stop, infinity),
    _ = supervisor:terminate_child(emqx_sys_sup, ?MODULE),
    Result.

-doc "Trace a local PID with garbage_collection, send, receive or call flags.".
-spec trace(pid(), [aspect()]) ->
    ok | {error, dead}.
trace(Pid, Flags) when is_pid(Pid), node(Pid) =:= node() ->
    Allowed = [garbage_collection, send, 'receive', call],
    maybe
        true ?= lists:all(fun(F) -> lists:member(F, Allowed) end, Flags),
        true ?= Pid =/= whereis(?MODULE),
        gen_server:call(?MODULE, {trace, Pid, Flags})
    else
        _ ->
            error(badarg)
    end.

-doc "Remove all runtime tracing of a local process from this capture.".
-spec untrace(pid()) ->
    ok | {error, dead}.
untrace(Pid) when is_pid(Pid), node(Pid) =:= node() ->
    gen_server:call(?MODULE, {untrace, Pid}).

-doc "Trace local calls and normal or exceptional returns for an MFA pattern.".
-spec trace_function({module(), atom(), arity() | '_'}) ->
    non_neg_integer().
trace_function(MFA) ->
    case gen_server:call(?MODULE, {function, MFA}) of
        {error, badarg} ->
            error(badarg);
        Result ->
            Result
    end.

-doc "Evaluate the module's configured MFA selector with arguments followed by extra arguments.".
-spec selected(module(), [term()]) -> term().
selected(Module, Args) ->
    Selectors = application:get_env(emqx, selftrace_selector, #{}),
    case Selectors of
        #{Module := {M, F, ExtraArgs}} ->
            apply(M, F, Args ++ ExtraArgs);
        #{} ->
            false
    end.

-doc "Enable custom events and the given runtime trace aspects in the calling process.".
-spec enable([aspect()]) ->
    ok | {error, not_started}.
enable(Aspects) ->
    case whereis(?MODULE) of
        undefined ->
            {error, not_started};
        Collector ->
            case trace(self(), Aspects) of
                ok ->
                    put(?KEY, Collector),
                    ok;
                Error ->
                    Error
            end
    end.

-spec disable() -> ok.
disable() ->
    slice_close_all(),
    erase(?KEY),
    ok.

-spec enabled() -> boolean().
enabled() ->
    is_pid(get(?KEY)).

-spec instant(term(), map()) -> ok.
instant(Name, Args) ->
    emit(instant, Name, Args).

-spec packet_type(term()) -> atom() | string().
packet_type(Packet = ?PACKET(_)) ->
    emqx_packet:type_name(Packet);
packet_type(_) ->
    "FRAME ERROR".

-doc "Open a slice in the calling process. Slices close in reverse opening order.".
-spec slice_open(term(), map()) -> ok.
slice_open(Name, Args) when is_map(Args) ->
    case get(?KEY) of
        Collector when is_pid(Collector) ->
            emit(Collector, slice_begin, Name, Args),
            put(?SLICES, [{Collector, Name} | slices()]),
            ok;
        _ ->
            ok
    end.

-doc "Close the most recently opened slice, or do nothing if none is open.".
-spec slice_close() -> ok.
slice_close() ->
    case slices() of
        [{Collector, Name} | Rest] ->
            put(?SLICES, Rest),
            emit(Collector, slice_end, Name, #{});
        [] ->
            ok
    end.

-doc "Close all open slices in the calling process.".
-spec slice_close_all() -> ok.
slice_close_all() ->
    case slices() of
        [] ->
            ok;
        [_ | _] ->
            slice_close(),
            slice_close_all()
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(Path) ->
    process_flag(trap_exit, true),
    case emqx_perfetto_writer:open(Path) of
        {ok, Writer} ->
            Session = trace:session_create(?MODULE, self(), []),
            {ok, #{session => Session, writer => Writer, stopping => false}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(stop, From, State = #{session := Session, stopping := false}) ->
    trace:process(Session, all, false, [all]),
    Ref = trace:delivered(Session, all),
    {noreply, State#{stopping := {Ref, [From]}}};
handle_call(stop, From, State = #{stopping := {Ref, Callers}}) ->
    {noreply, State#{stopping := {Ref, [From | Callers]}}};
handle_call({trace, Pid, Flags}, _, State = #{session := Session}) ->
    Result =
        try trace:process(Session, Pid, true, [monotonic_timestamp, arity | Flags]) of
            1 -> ok
        catch
            error:badarg -> {error, dead}
        end,
    {reply, Result, State};
handle_call({untrace, Pid}, _, State = #{session := Session}) ->
    Result =
        try trace:process(Session, Pid, false, [all]) of
            1 -> ok
        catch
            error:badarg -> {error, dead}
        end,
    {reply, Result, State};
handle_call({function, MFA}, _, State = #{session := Session}) ->
    Result =
        try
            trace:function(Session, MFA, [{'_', [], [{exception_trace}]}], [local])
        catch
            error:badarg -> {error, badarg}
        end,
    {reply, Result, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({trace_delivered, all, Ref}, State = #{stopping := {Ref, Callers}}) ->
    Result = emqx_perfetto_writer:close(maps:get(writer, State)),
    lists:foreach(fun(From) -> gen_server:reply(From, Result) end, Callers),
    {stop, normal, State#{writer := closed}};
handle_info({custom, Pid, Kind, Name, Args, Ts}, State) ->
    {noreply, event(Pid, custom, Kind, Name, Args, Ts, State)};
handle_info({trace_ts, Pid, Event, Info, Ts}, State) ->
    {noreply, runtime(Pid, Event, Info, Ts, State)};
handle_info({trace_ts, Pid, Event, Info, Extra, Ts}, State) ->
    {noreply, runtime(Pid, Event, {Info, Extra}, Ts, State)};
handle_info(_, State) ->
    {noreply, State}.

terminate(_, #{session := Session, writer := Writer}) ->
    trace:session_destroy(Session),
    case Writer of
        closed ->
            ok;
        _ ->
            emqx_perfetto_writer:close(Writer)
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

slices() ->
    case get(?SLICES) of
        undefined -> [];
        Slices -> Slices
    end.

emit(Kind, Name, Args) when is_map(Args) ->
    case get(?KEY) of
        Collector when is_pid(Collector) ->
            emit(Collector, Kind, Name, Args);
        _ ->
            ok
    end.

emit(Collector, Kind, Name, Args) ->
    Collector ! {custom, self(), Kind, Name, Args, erlang:monotonic_time(nanosecond)},
    ok.

runtime(Pid, gc_minor_start, Info, Ts, State) ->
    gc(Pid, slice_begin, gc_minor, Info, Ts, State);
runtime(Pid, gc_major_start, Info, Ts, State) ->
    gc(Pid, slice_begin, gc_major, Info, Ts, State);
runtime(Pid, gc_minor_end, Info, Ts, State) ->
    gc(Pid, slice_end, gc_minor, Info, Ts, State);
runtime(Pid, gc_major_end, Info, Ts, State) ->
    gc(Pid, slice_end, gc_major, Info, Ts, State);
runtime(Pid, call, MFA, Ts, State) ->
    event(Pid, call, slice_begin, MFA, #{}, Ts, State);
runtime(Pid, Return, {MFA, _Value}, Ts, State) when
    Return =:= return_from; Return =:= exception_from
->
    event(Pid, call, slice_end, MFA, #{exit => Return}, Ts, State);
runtime(Pid, Send, {_Message, To}, Ts, State) when
    Send =:= send; Send =:= send_to_non_existing_process
->
    event(Pid, message, instant, Send, #{to => To}, Ts, State);
runtime(Pid, 'receive', _Message, Ts, State) ->
    event(Pid, message, instant, 'receive', #{}, Ts, State);
runtime(_, _, _, _, State) ->
    State.

gc(Pid, Kind, Name, Info, Ts, State0) ->
    Args = maps:from_list(Info),
    State = event(Pid, gc, Kind, Name, Args, Ts, State0),
    Gauges = maps:with(
        [
            heap_size,
            heap_block_size,
            old_heap_size,
            old_heap_block_size,
            stack_size,
            recent_size,
            mbuf_size,
            bin_vheap_size,
            bin_vheap_block_size,
            bin_old_vheap_size,
            bin_old_vheap_block_size
        ],
        Args
    ),
    case map_size(Gauges) of
        0 -> State;
        _ -> event(Pid, gc, counter, <<"GC">>, Gauges, Ts, State)
    end.

event(Pid, Category, Kind, Name, Args, Ts, State = #{writer := Writer}) ->
    Event = #{
        pid => Pid,
        category => Category,
        kind => Kind,
        name => Name,
        args => Args,
        timestamp => Ts
    },
    State#{writer := emqx_perfetto_writer:event(Event, Writer)}.
