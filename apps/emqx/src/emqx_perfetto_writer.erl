%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
-module(emqx_perfetto_writer).
-moduledoc "Streaming Chrome Trace JSON writer for Perfetto.".

-export([
    open/1,
    event/2,
    close/1
]).

-opaque state() :: #{
    file := file:io_device(),
    origin := integer(),
    processes := #{pid() => pos_integer()},
    tracks := #{{pos_integer(), atom()} => pos_integer()},
    separator := [] | binary()
}.

-export_type([state/0]).

%% Chrome Trace JSON object in traceEvents.
%% * `ph` selects slice begin/end (B/E), instant (i), counter (C), or metadata (M).
%% * `pid`/`tid` identify the process/track.
%% * `ts` is relative time in microseconds.
%% * `args` holds JSON-compatible event data.
%% * `s` selects instant-event scope: thread (t), process (p), or global (g).
-type trace_event() :: #{
    pid := pos_integer(),
    tid => pos_integer(),
    ph := binary(),
    name := binary(),
    cat => binary(),
    ts => number(),
    args := map(),
    s => binary()
}.

%%

-spec open(file:name_all() | iodata()) ->
    {ok, state()} | {error, file:posix() | badarg | system_limit}.
open(Path) ->
    case file:open(Path, [write, raw, binary, exclusive]) of
        {ok, File} ->
            ok = file:write(File, <<"""
            {"traceEvents":[
            """>>),
            {ok, #{
                file => File,
                origin => erlang:monotonic_time(nanosecond),
                processes => #{},
                tracks => #{},
                separator => []
            }};
        Error ->
            Error
    end.

-spec event(emqx_selftrace:event(), state()) -> state().
event(
    #{
        pid := Pid,
        category := Category,
        kind := Kind,
        name := Name,
        args := Args,
        timestamp := Timestamp
    },
    State0
) ->
    {ProcessId, State1} = process(Pid, State0),
    {Tid, State} = track(ProcessId, Category, State1),
    Event = #{
        pid => ProcessId,
        tid => Tid,
        cat => text(Category),
        ph => phase(Kind),
        name => event_name(Category, Name),
        ts => (Timestamp - maps:get(origin, State)) / 1000,
        args => normalize(Args)
    },
    write(
        case Kind of
            instant ->
                Event#{s => <<"t">>};
            _ ->
                Event
        end,
        State
    ).

process(Pid, State = #{processes := Processes}) ->
    case Processes of
        #{Pid := ProcessId} ->
            {ProcessId, State};
        #{} ->
            ProcessId = map_size(Processes) + 1,
            Next = write(
                #{
                    pid => ProcessId,
                    ph => <<"M">>,
                    %% Chrome Trace metadata labels the numeric pid with args.name.
                    name => <<"process_name">>,
                    args => #{name => text(Pid)}
                },
                State
            ),
            {ProcessId, Next#{processes := Processes#{Pid => ProcessId}}}
    end.

track(ProcessId, Category, State = #{tracks := Tracks}) ->
    Key = {ProcessId, Category},
    case Tracks of
        #{Key := Tid} ->
            {Tid, State};
        #{} ->
            Tid = map_size(Tracks) + 1,
            Name = text(Category),
            Next = write(
                %% NOTE
                %% Chrome Trace metadata uses `thread_name` to label the tid with `args.name`.
                #{
                    pid => ProcessId,
                    tid => Tid,
                    ph => <<"M">>,
                    name => <<"thread_name">>,
                    args => #{name => Name}
                },
                State
            ),
            {Tid, Next#{tracks := Tracks#{Key => Tid}}}
    end.

-spec write(trace_event(), state()) -> state().
write(Event, State = #{file := File, separator := Separator}) ->
    ok = file:write(File, [Separator, json:encode(Event)]),
    State#{separator := <<",">>}.

-spec close(state()) -> ok | {error, file:posix() | badarg | terminated}.
close(#{file := File}) ->
    Result = file:write(File, <<"]}">>),
    Closed = file:close(File),
    case Result of
        ok ->
            Closed;
        _ ->
            Result
    end.

phase(slice_begin) ->
    <<"B">>;
phase(slice_end) ->
    <<"E">>;
phase(instant) ->
    <<"i">>;
phase(counter) ->
    <<"C">>.

event_name(call, {M, F, A}) ->
    unicode:characters_to_binary(io_lib:format("~tp:~tp/~p", [M, F, A]));
event_name(_Category, Name) ->
    text(Name).

normalize(Map) when is_map(Map) ->
    maps:from_list([{text(K), normalize(V)} || {K, V} <- maps:to_list(Map)]);
normalize(Value) when is_number(Value); is_boolean(Value); Value =:= null ->
    Value;
normalize(Value) ->
    text(Value).

text(Value) when is_binary(Value) ->
    case unicode:characters_to_binary(Value) of
        Value ->
            Value;
        _ ->
            iolist_to_binary(io_lib:format("~w", [Value]))
    end;
text(Value) when is_atom(Value) ->
    atom_to_binary(Value);
text(Value) ->
    unicode:characters_to_binary(io_lib:format("~0tp", [Value])).
