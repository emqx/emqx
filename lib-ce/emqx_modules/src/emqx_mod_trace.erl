%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_mod_trace).

-behaviour(gen_server).
-behaviour(emqx_gen_mod).


-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("kernel/include/file.hrl").

-logger_header("[Trace]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([ load/1
        , unload/1
        , description/0
        ]).

-export([ start_link/0
        , list/0
        , list/1
        , get_trace_filename/1
        , create/1
        , delete/1
        , clear/0
        , update/2
        ]).

-export([ format/1
        , zip_dir/0
        , trace_dir/0
        , trace_file/1
        , delete_files_after_send/2
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TRACE, ?MODULE).
-define(PACKETS, tuple_to_list(?TYPE_NAMES)).
-define(MAX_SIZE, 30).

-ifdef(TEST).
-export([log_file/2]).
-endif.

-record(?TRACE,
        { name :: binary() | undefined | '_'
        , type :: clientid | topic | undefined | '_'
        , topic :: emqx_types:topic() | undefined | '_'
        , clientid :: emqx_types:clientid() | undefined | '_'
        , packets = [] :: list() | '_'
        , enable = true :: boolean() | '_'
        , start_at :: integer() | undefined | binary() | '_'
        , end_at :: integer() | undefined | binary()  | '_'
        , log_size = #{} :: map() | '_'
        }).

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TRACE, [
        {type, set},
        {disc_copies, [node()]},
        {record_name, ?TRACE},
        {attributes, record_info(fields, ?TRACE)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TRACE, disc_copies).

description() ->
    "EMQ X Trace Module".

-spec load(any()) -> ok.
load(_Env) ->
    emqx_mod_sup:start_child(?MODULE, worker).

-spec unload(any()) -> ok.
unload(_Env) ->
    _ = emqx_mod_sup:stop_child(?MODULE),
    stop_all_trace_handler().

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec list() -> [tuple()].
list() ->
    ets:match_object(?TRACE, #?TRACE{_ = '_'}).

-spec list(boolean()) -> [tuple()].
list(Enable) ->
    ets:match_object(?TRACE, #?TRACE{enable = Enable, _ = '_'}).

-spec create([{Key :: binary(), Value :: binary()}]) ->
    ok | {error, {duplicate_condition, iodata()} | {already_existed, iodata()} | iodata()}.
create(Trace) ->
    case mnesia:table_info(?TRACE, size) < ?MAX_SIZE of
        true ->
            case to_trace(Trace) of
                {ok, TraceRec} -> create_new_trace(TraceRec);
                {error, Reason} -> {error, Reason}
            end;
        false ->
            {error, """The number of traces created has reached the maximum,
                       please delete the useless ones first"""}
    end.

-spec delete(Name :: binary()) -> ok | {error, not_found}.
delete(Name) ->
    Tran = fun() ->
        case mnesia:read(?TRACE, Name) of
            [_] -> mnesia:delete(?TRACE, Name, write);
            [] -> mnesia:abort(not_found)
        end
           end,
    transaction(Tran).

-spec clear() -> ok | {error, Reason :: term()}.
clear() ->
    case mnesia:clear_table(?TRACE) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

-spec update(Name :: binary(), Enable :: boolean()) ->
    ok | {error, not_found | finished}.
update(Name, Enable) ->
    Tran = fun() ->
        case mnesia:read(?TRACE, Name) of
            [] -> mnesia:abort(not_found);
            [#?TRACE{enable = Enable}] -> ok;
            [Rec] ->
                case erlang:system_time(second) >= Rec#?TRACE.end_at of
                    false -> mnesia:write(?TRACE, Rec#?TRACE{enable = Enable}, write);
                    true -> mnesia:abort(finished)
                end
        end
           end,
    transaction(Tran).

-spec get_trace_filename(Name :: binary()) ->
    {ok, FileName :: string()} |{error, not_found}.
get_trace_filename(Name) ->
    Tran = fun() ->
        case mnesia:read(?TRACE, Name, read) of
            [] -> mnesia:abort(not_found);
            [#?TRACE{start_at = Start}] -> {ok, filename(Name, Start)}
        end end,
    transaction(Tran).

-spec trace_file(File :: list()) ->
    {ok, Node :: list(), Binary :: binary()} |
    {error, Node :: list(), Reason :: term()}.
trace_file(File) ->
    FileName = filename:join(trace_dir(), File),
    Node = atom_to_list(node()),
    case file:read_file(FileName) of
        {ok, Bin} -> {ok, Node, Bin};
        {error, Reason} -> {error, Node, Reason}
    end.

delete_files_after_send(TraceLog, Zips) ->
    gen_server:cast(?MODULE, {delete_tag, self(), [TraceLog | Zips]}).

-spec format(list(#?TRACE{})) -> list(map()).
format(Traces) ->
    Fields = record_info(fields, ?TRACE),
    lists:map(fun(Trace0 = #?TRACE{start_at = StartAt, end_at = EndAt}) ->
        Trace = Trace0#?TRACE{
            start_at = list_to_binary(calendar:system_time_to_rfc3339(StartAt)),
            end_at   = list_to_binary(calendar:system_time_to_rfc3339(EndAt))
        },
        [_ | Values] = tuple_to_list(Trace),
        maps:from_list(lists:zip(Fields, Values))
              end, Traces).

init([]) ->
    erlang:process_flag(trap_exit, true),
    OriginLogLevel = emqx_logger:get_primary_log_level(),
    ok = filelib:ensure_dir(trace_dir()),
    ok = filelib:ensure_dir(zip_dir()),
    {ok, _} = mnesia:subscribe({table, ?TRACE, simple}),
    Traces = get_enable_trace(),
    ok = update_log_primary_level(Traces, OriginLogLevel),
    TRef = update_trace(Traces),
    {ok, #{timer => TRef, monitors => #{}, primary_log_level => OriginLogLevel}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ok, State}.

handle_cast({delete_tag, Pid, Files}, State = #{monitors := Monitors}) ->
    erlang:monitor(process, Pid),
    {noreply, State#{monitors => Monitors#{Pid => Files}}};
handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _ref, process, Pid, _Reason}, State = #{monitors := Monitors}) ->
    case maps:take(Pid, Monitors) of
        error -> {noreply, State};
        {Files, NewMonitors} ->
            lists:foreach(fun(F) -> file:delete(F) end, Files),
            {noreply, State#{monitors => NewMonitors}}
    end;
handle_info({timeout, TRef, update_trace},
    #{timer := TRef, primary_log_level := OriginLogLevel} = State) ->
    Traces = get_enable_trace(),
    ok = update_log_primary_level(Traces, OriginLogLevel),
    NextTRef = update_trace(Traces),
    {noreply, State#{timer => NextTRef}};

handle_info({mnesia_table_event, _Events}, State = #{timer := TRef}) ->
    emqx_misc:cancel_timer(TRef),
    handle_info({timeout, TRef, update_trace}, State);

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{timer := TRef, primary_log_level := OriginLogLevel}) ->
    ok = recover_log_primary_level(OriginLogLevel),
    _ = mnesia:unsubscribe({table, ?TRACE, simple}),
    emqx_misc:cancel_timer(TRef),
    stop_all_trace_handler(),
    _ = file:del_dir_r(zip_dir()),
    ok.

code_change(_, State, _Extra) ->
    {ok, State}.

create_new_trace(Trace) ->
    Tran = fun() ->
        case mnesia:read(?TRACE, Trace#?TRACE.name) of
            [] ->
                #?TRACE{start_at = StartAt, topic = Topic, clientid = ClientId, packets = Packets} = Trace,
                Match = #?TRACE{_ = '_', start_at = StartAt, topic = Topic, clientid = ClientId, packets = Packets},
                case mnesia:match_object(?TRACE, Match, read) of
                    [] -> mnesia:write(?TRACE, Trace, write);
                    [#?TRACE{name = Name}] -> mnesia:abort({duplicate_condition, Name})
                end;
            [#?TRACE{name = Name}] -> mnesia:abort({already_existed, Name})
        end
           end,
    transaction(Tran).

update_trace(Traces) ->
    Now = erlang:system_time(second),
    {_Waiting, Running, Finished} = classify_by_time(Traces, Now),
    disable_finished(Finished),
    Started = already_running(),
    {NeedRunning, AllStarted} = start_trace(Running, Started),
    NeedStop = AllStarted -- NeedRunning,
    ok = stop_trace(NeedStop, Started),
    clean_stale_trace_files(),
    NextTime = find_closest_time(Traces, Now),
    emqx_misc:start_timer(NextTime, update_trace).

stop_all_trace_handler() ->
    lists:foreach(fun(#{type := Type, name := Name} = Trace) ->
        _ = emqx_tracer:stop_trace(Type, maps:get(Type, Trace), Name)
                  end
        , already_running()).

already_running() ->
    emqx_tracer:lookup_traces().

get_enable_trace() ->
    {atomic, Traces} =
        mnesia:transaction(fun() ->
            mnesia:match_object(?TRACE, #?TRACE{enable = true, _ = '_'}, read)
                           end),
    Traces.

find_closest_time(Traces, Now) ->
    Sec =
        lists:foldl(fun(#?TRACE{start_at = Start, end_at = End}, Closest) ->
            if
                Start >= Now andalso Now < End -> %% running
                    min(End - Now, Closest);
                Start < Now -> %% waiting
                    min(Now - Start, Closest);
                true -> Closest %% finished
            end
                    end, 60 * 15, Traces),
    timer:seconds(Sec).

disable_finished([]) -> ok;
disable_finished(Traces) ->
    NameWithLogSize =
        lists:map(fun(#?TRACE{name = Name, start_at = StartAt}) ->
            FileSize = filelib:file_size(log_file(Name, StartAt)),
            {Name, FileSize} end, Traces),
    transaction(fun() ->
        lists:map(fun({Name, LogSize}) ->
            case mnesia:read(?TRACE, Name, write) of
                [] -> ok;
                [Trace = #?TRACE{log_size = Logs}] ->
                    mnesia:write(?TRACE, Trace#?TRACE{enable = false, log_size = Logs#{node() => LogSize}}, write)
            end end, NameWithLogSize)
                end).

start_trace(Traces, Started0) ->
    Started = lists:map(fun(#{name := Name}) -> Name end, Started0),
    lists:foldl(fun(#?TRACE{name = Name} = Trace, {Running, StartedAcc}) ->
        case lists:member(Name, StartedAcc) of
            true -> {[Name | Running], StartedAcc};
            false ->
                case start_trace(Trace) of
                    ok -> {[Name | Running], [Name | StartedAcc]};
                    Error ->
                        ?LOG(error, "(~p)start trace failed by:~p", [Name, Error]),
                        {[Name | Running], StartedAcc}
                end
        end
                end, {[], Started}, Traces).

start_trace(Trace) ->
    #?TRACE{name   = Name
        , type     = Type
        , clientid = ClientId
        , topic    = Topic
        , packets  = Packets
        , start_at = Start
    } = Trace,
    Who0 = #{name => Name, labels => Packets},
    Who =
        case Type of
            topic -> Who0#{type => topic, topic => Topic};
            clientid -> Who0#{type => clientid, clientid => ClientId}
        end,
    case emqx_tracer:start_trace(Who, debug, log_file(Name, Start)) of
        ok -> ok;
        {error, {already_exist, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

stop_trace(Finished, Started) ->
    lists:foreach(fun(#{name := Name, type := Type} = Trace) ->
        case lists:member(Name, Finished) of
            true -> emqx_tracer:stop_trace(Type, maps:get(Type, Trace), Name);
            false -> ok
        end
                  end, Started).

clean_stale_trace_files() ->
    TraceDir = trace_dir(),
    case file:list_dir(TraceDir) of
        {ok, AllFiles} when AllFiles =/= ["zip"] ->
            FileFun = fun(#?TRACE{name = Name, start_at = StartAt}) -> filename(Name, StartAt) end,
            KeepFiles = lists:map(FileFun, list()),
            case AllFiles -- ["zip" | KeepFiles] of
                [] -> ok;
                DeleteFiles ->
                    DelFun = fun(F) -> file:delete(filename:join(TraceDir, F)) end,
                    lists:foreach(DelFun, DeleteFiles)
            end;
        _ -> ok
    end.

classify_by_time(Traces, Now) ->
    classify_by_time(Traces, Now, [], [], []).

classify_by_time([], _Now, Wait, Run, Finish) -> {Wait, Run, Finish};
classify_by_time([Trace = #?TRACE{start_at = Start} | Traces], Now, Wait, Run, Finish) when Start > Now ->
    classify_by_time(Traces, Now, [Trace | Wait], Run, Finish);
classify_by_time([Trace = #?TRACE{end_at = End} | Traces], Now, Wait, Run, Finish) when End =< Now ->
    classify_by_time(Traces, Now, Wait, Run, [Trace | Finish]);
classify_by_time([Trace | Traces], Now, Wait, Run, Finish) ->
    classify_by_time(Traces, Now, Wait, [Trace | Run], Finish).

to_trace(TraceList) ->
    case to_trace(TraceList, #?TRACE{}) of
        {error, Reason} -> {error, Reason};
        {ok, #?TRACE{name = undefined}} ->
            {error, "name required"};
        {ok, #?TRACE{type = undefined}} ->
            {error, "type required"};
        {ok, #?TRACE{topic = undefined, clientid = undefined}} ->
            {error, "topic/clientid cannot be both empty"};
        {ok, Trace} ->
            case fill_default(Trace) of
                #?TRACE{start_at = Start, end_at = End} when End =< Start ->
                    {error, "failed by start_at >= end_at"};
                Trace1 -> {ok, Trace1}
            end
    end.

fill_default(Trace = #?TRACE{start_at = undefined}) ->
    fill_default(Trace#?TRACE{start_at = erlang:system_time(second)});
fill_default(Trace = #?TRACE{end_at = undefined, start_at = StartAt}) ->
    fill_default(Trace#?TRACE{end_at = StartAt + 10 * 60});
fill_default(Trace) -> Trace.

to_trace([], Rec) -> {ok, Rec};
to_trace([{<<"name">>, Name} | Trace], Rec) ->
    case binary:match(Name, [<<"/">>], []) of
        nomatch -> to_trace(Trace, Rec#?TRACE{name = Name});
        _ -> {error, "name cannot contain /"}
    end;
to_trace([{<<"type">>, Type} | Trace], Rec) ->
    case lists:member(Type, [<<"clientid">>, <<"topic">>]) of
        true -> to_trace(Trace, Rec#?TRACE{type = binary_to_existing_atom(Type)});
        false -> {error, "incorrect type: only support clientid/topic"}
    end;
to_trace([{<<"topic">>, Topic} | Trace], Rec) ->
    case validate_topic(Topic) of
        ok -> to_trace(Trace, Rec#?TRACE{topic = Topic});
        {error, Reason} -> {error, Reason}
    end;
to_trace([{<<"start_at">>, StartAt} | Trace], Rec) ->
    case to_system_second(StartAt) of
        {ok, Sec} -> to_trace(Trace, Rec#?TRACE{start_at = Sec});
        {error, Reason} -> {error, Reason}
    end;
to_trace([{<<"end_at">>, EndAt} | Trace], Rec) ->
    Now = erlang:system_time(second),
    case to_system_second(EndAt) of
        {ok, Sec} when Sec > Now ->
            to_trace(Trace, Rec#?TRACE{end_at = Sec});
        {ok, _Sec} ->
            {error, "end_at time has already passed"};
        {error, Reason} ->
            {error, Reason}
    end;
to_trace([{<<"clientid">>, ClientId} | Trace], Rec) ->
    to_trace(Trace, Rec#?TRACE{clientid = ClientId});
to_trace([{<<"packets">>, PacketList} | Trace], Rec) ->
    case to_packets(PacketList) of
        {ok, Packets} -> to_trace(Trace, Rec#?TRACE{packets = Packets});
        {error, Reason} -> {error, io_lib:format("unsupport packets: ~p", [Reason])}
    end;
to_trace([Unknown | _Trace], _Rec) -> {error, io_lib:format("unknown field: ~p", [Unknown])}.

validate_topic(TopicName) ->
    try emqx_topic:validate(name, TopicName) of
        true -> ok
    catch
        error:Error ->
            {error, io_lib:format("~s invalid by ~p", [TopicName, Error])}
    end.

to_system_second(At) ->
    try
        Sec = calendar:rfc3339_to_system_time(binary_to_list(At), [{unit, second}]),
        {ok, Sec}
    catch error: {badmatch, _} ->
        {error, ["The rfc3339 specification not satisfied: ", At]}
    end.

to_packets(Packets) when is_list(Packets) ->
    AtomTypes = lists:map(fun(Type) -> binary_to_existing_atom(Type) end, Packets),
    case lists:filter(fun(T) -> not lists:member(T, ?PACKETS) end, AtomTypes) of
        [] -> {ok, AtomTypes};
        InvalidE -> {error, InvalidE}
    end;
to_packets(Packets) -> {error, Packets}.

zip_dir() ->
    trace_dir() ++ "zip/".

trace_dir() ->
    filename:join(emqx:get_env(data_dir), "trace") ++ "/".

log_file(Name, Start) ->
    filename:join(trace_dir(), filename(Name, Start)).

filename(Name, Start) ->
    [Time, _] = string:split(calendar:system_time_to_rfc3339(Start), "T", leading),
    lists:flatten(["trace_", binary_to_list(Name), "_", Time, ".log"]).

transaction(Tran) ->
    case mnesia:transaction(Tran) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.

update_log_primary_level([], OriginLevel) -> recover_log_primary_level(OriginLevel);
update_log_primary_level(_, _) -> set_log_primary_level(debug).

set_log_primary_level(NewLevel) ->
    case NewLevel =/= emqx_logger:get_primary_log_level() of
        true -> emqx_logger:set_primary_log_level(NewLevel);
        false -> ok
    end.

recover_log_primary_level(OriginLevel) ->
    case OriginLevel =/= emqx_logger:get_primary_log_level() of
        true -> emqx_logger:set_primary_log_level(OriginLevel);
        false -> ok
    end.
