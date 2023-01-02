%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_trace).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("kernel/include/file.hrl").

-logger_header("[Tracer]").

-export([ publish/1
        , subscribe/3
        , unsubscribe/2
        ]).

-export([ start_link/0
        , list/0
        , list/1
        , get_trace_filename/1
        , create/1
        , delete/1
        , clear/0
        , update/2
        , os_now/0
        ]).

-export([ format/1
        , zip_dir/0
        , filename/2
        , trace_dir/0
        , trace_file/1
        , trace_file_detail/1
        , delete_files_after_send/2
        , is_enable/0
        ]).


-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TRACE, ?MODULE).
-define(MAX_SIZE, 30).

-ifdef(TEST).
-export([ log_file/2
        , create_table/0
        , find_closest_time/2
        ]).
-endif.

-export_type([ip_address/0]).
-type ip_address() :: string().

-record(?TRACE,
        { name :: binary() | undefined | '_'
        , type :: clientid | topic | ip_address | undefined | '_'
        , filter :: emqx_types:topic() | emqx_types:clientid() | ip_address() | undefined | '_'
        , enable = true :: boolean() | '_'
        , start_at :: integer() | undefined | '_'
        , end_at :: integer() | undefined | '_'
        }).

publish(#message{topic = <<"$SYS/", _/binary>>}) -> ignore;
publish(#message{from = From, topic = Topic, payload = Payload}) when
    is_binary(From); is_atom(From) ->
    emqx_logger:info(
        #{topic => Topic, mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY}},
        "PUBLISH to ~s: ~0p",
        [Topic, Payload]
    ).

subscribe(<<"$SYS/", _/binary>>, _SubId, _SubOpts) -> ignore;
subscribe(Topic, SubId, SubOpts) ->
    emqx_logger:info(
        #{topic => Topic, mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY}},
        "~ts SUBSCRIBE ~ts: Options: ~0p",
        [SubId, Topic, SubOpts]
    ).

unsubscribe(<<"$SYS/", _/binary>>, _SubOpts) -> ignore;
unsubscribe(Topic, SubOpts) ->
    emqx_logger:info(
        #{topic => Topic, mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY}},
        "~ts UNSUBSCRIBE ~ts: Options: ~0p",
        [maps:get(subid, SubOpts, ""), Topic, SubOpts]
    ).

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec list() -> [tuple()].
list() ->
    case ets:info(?TRACE) of
        undefined -> [];
        _ -> ets:match_object(?TRACE, #?TRACE{_ = '_'})
    end.

-spec is_enable() -> boolean().
is_enable() ->
    undefined =/= erlang:whereis(?MODULE).

-spec list(boolean()) -> [tuple()].
list(Enable) ->
    ets:match_object(?TRACE, #?TRACE{enable = Enable, _ = '_'}).

-spec create([{Key :: binary(), Value :: binary()}] | #{atom() => binary()}) ->
    ok | {error, {duplicate_condition, iodata()} | {already_existed, iodata()} | iodata()}.
create(Trace) ->
    case mnesia:table_info(?TRACE, size) < ?MAX_SIZE of
        true ->
            case to_trace(Trace) of
                {ok, TraceRec} -> insert_new_trace(TraceRec);
                {error, Reason} -> {error, Reason}
            end;
        false ->
            {error, "The number of traces created has reache the maximum"
            " please delete the useless ones first"}
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
                case os_now() >= Rec#?TRACE.end_at of
                    false -> mnesia:write(?TRACE, Rec#?TRACE{enable = Enable}, write);
                    true -> mnesia:abort(finished)
                end
        end
           end,
    transaction(Tran).

-spec get_trace_filename(Name :: binary()) ->
    {ok, FileName :: string()} | {error, not_found}.
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
        {error, enoent} ->
            case emqx_trace:is_enable() of
                false -> {error, Node, trace_disabled};
                true -> {error, Node, enoent}
            end;
        {error, Reason} ->
            {error, Node, Reason}
    end.

trace_file_detail(File) ->
    FileName = filename:join(trace_dir(), File),
    Node = atom_to_binary(node()),
    case file:read_file_info(FileName, [{'time', 'posix'}]) of
        {ok, #file_info{size = Size, mtime = Mtime}} -> {ok, Node, #{size => Size, mtime => Mtime}};
        {error, Reason} -> {error, Node, Reason}
    end.

delete_files_after_send(TraceLog, Zips) ->
    gen_server:cast(?MODULE, {delete_tag, self(), [TraceLog | Zips]}).

-spec format(list(#?TRACE{})) -> list(map()).
format(Traces) ->
    Fields = record_info(fields, ?TRACE),
    lists:map(fun(Trace0 = #?TRACE{}) ->
        [_ | Values] = tuple_to_list(Trace0),
        maps:from_list(lists:zip(Fields, Values))
              end, Traces).

init([]) ->
    ok = create_table(),
    erlang:process_flag(trap_exit, true),
    OriginLogLevel = emqx_logger:get_primary_log_level(),
    ok = filelib:ensure_dir(filename:join([trace_dir(), dummy])),
    ok = filelib:ensure_dir(filename:join([zip_dir(), dummy])),
    {ok, _} = mnesia:subscribe({table, ?TRACE, simple}),
    Traces = get_enable_trace(),
    ok = update_log_primary_level(Traces, OriginLogLevel),
    TRef = update_trace(Traces),
    {ok, #{timer => TRef, monitors => #{}, primary_log_level => OriginLogLevel}}.

create_table() ->
    ok = ekka_mnesia:create_table(?TRACE, [
        {type, set},
        {disc_copies, [node()]},
        {record_name, ?TRACE},
        {attributes, record_info(fields, ?TRACE)}]),
    ok = ekka_mnesia:copy_table(?TRACE, disc_copies).

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ok, State}.

handle_cast({delete_tag, Pid, Files}, State = #{monitors := Monitors}) ->
    erlang:monitor(process, Pid),
    {noreply, State#{monitors => Monitors#{Pid => Files}}};
handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State = #{monitors := Monitors}) ->
    case maps:take(Pid, Monitors) of
        error -> {noreply, State};
        {Files, NewMonitors} ->
            ZipDir = emqx_trace:zip_dir(),
            lists:foreach(fun(F) -> file:delete(filename:join([ZipDir, F])) end,  Files),
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
    ok = set_log_primary_level(OriginLogLevel),
    _ = mnesia:unsubscribe({table, ?TRACE, simple}),
    emqx_misc:cancel_timer(TRef),
    stop_all_trace_handler(),
    _ = file:del_dir_r(zip_dir()),
    ok.

code_change(_, State, _Extra) ->
    {ok, State}.

insert_new_trace(Trace) ->
    Tran = fun() ->
        case mnesia:read(?TRACE, Trace#?TRACE.name) of
            [] ->
                #?TRACE{start_at = StartAt, type = Type, filter = Filter} = Trace,
                Match = #?TRACE{_ = '_', start_at = StartAt, type = Type, filter = Filter},
                case mnesia:match_object(?TRACE, Match, read) of
                    [] -> mnesia:write(?TRACE, Trace, write);
                    [#?TRACE{name = Name}] -> mnesia:abort({duplicate_condition, Name})
                end;
            [#?TRACE{name = Name}] -> mnesia:abort({already_existed, Name})
        end
           end,
    transaction(Tran).

update_trace(Traces) ->
    Now = os_now(),
    {_Waiting, Running, Finished} = classify_by_time(Traces, Now),
    disable_finished(Finished),
    Started = emqx_trace_handler:running(),
    {NeedRunning, AllStarted} = start_trace(Running, Started),
    NeedStop = AllStarted -- NeedRunning,
    ok = stop_trace(NeedStop, Started),
    clean_stale_trace_files(),
    NextTime = find_closest_time(Traces, Now),
    emqx_misc:start_timer(NextTime, update_trace).

stop_all_trace_handler() ->
    lists:foreach(fun(#{id := Id}) -> emqx_trace_handler:uninstall(Id) end,
        emqx_trace_handler:running()).
get_enable_trace() ->
    {atomic, Traces} =
        mnesia:transaction(fun() ->
            mnesia:match_object(?TRACE, #?TRACE{enable = true, _ = '_'}, read)
                           end),
    Traces.

find_closest_time(Traces, Now) ->
    Sec =
        lists:foldl(
            fun(#?TRACE{start_at = Start, end_at = End, enable = true}, Closest) ->
                min(closest(End, Now, Closest), closest(Start, Now, Closest));
                (_, Closest) -> Closest
            end, 60 * 15, Traces),
    timer:seconds(Sec).

closest(Time, Now, Closest) when Now >= Time -> Closest;
closest(Time, Now, Closest) -> min(Time - Now, Closest).

disable_finished([]) -> ok;
disable_finished(Traces) ->
    transaction(fun() ->
        lists:map(fun(#?TRACE{name = Name}) ->
            case mnesia:read(?TRACE, Name, write) of
                [] -> ok;
                [Trace] -> mnesia:write(?TRACE, Trace#?TRACE{enable = false}, write)
            end end, Traces)
                end).

start_trace(Traces, Started0) ->
    Started = lists:map(fun(#{name := Name}) -> Name end, Started0),
    lists:foldl(fun(#?TRACE{name = Name} = Trace, {Running, StartedAcc}) ->
        case lists:member(Name, StartedAcc) of
            true ->
                {[Name | Running], StartedAcc};
            false ->
                case start_trace(Trace) of
                    ok -> {[Name | Running], [Name | StartedAcc]};
                    {error, _Reason} -> {[Name | Running], StartedAcc}
                end
        end
                end, {[], Started}, Traces).

start_trace(Trace) ->
    #?TRACE{name   = Name
        , type     = Type
        , filter   = Filter
        , start_at = Start
    } = Trace,
    Who = #{name => Name, type => Type, filter => Filter},
    emqx_trace_handler:install(Who, debug, log_file(Name, Start)).

stop_trace(Finished, Started) ->
    lists:foreach(fun(#{name := Name, type := Type}) ->
        case lists:member(Name, Finished) of
            true -> emqx_trace_handler:uninstall(Type, Name);
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
classify_by_time([Trace = #?TRACE{start_at = Start} | Traces],
    Now, Wait, Run, Finish) when Start > Now ->
    classify_by_time(Traces, Now, [Trace | Wait], Run, Finish);
classify_by_time([Trace = #?TRACE{end_at = End} | Traces],
    Now, Wait, Run, Finish) when End =< Now ->
    classify_by_time(Traces, Now, Wait, Run, [Trace | Finish]);
classify_by_time([Trace | Traces], Now, Wait, Run, Finish) ->
    classify_by_time(Traces, Now, Wait, [Trace | Run], Finish).

to_trace(TraceParam) ->
    case to_trace(ensure_map(TraceParam), #?TRACE{}) of
        {error, Reason} -> {error, Reason};
        {ok, #?TRACE{name = undefined}} ->
            {error, "name required"};
        {ok, #?TRACE{type = undefined}} ->
            {error, "type=[topic,clientid,ip_address] required"};
        {ok, TraceRec0 = #?TRACE{}} ->
            case fill_default(TraceRec0) of
                #?TRACE{start_at = Start, end_at = End} when End =< Start ->
                    {error, "failed by start_at >= end_at"};
                TraceRec ->
                    {ok, TraceRec}
            end
    end.

ensure_map(#{} = Trace) -> Trace;
ensure_map(Trace) when is_list(Trace) ->
    lists:foldl(
        fun({K, V}, Acc) when is_binary(K) -> Acc#{binary_to_existing_atom(K) => V};
            ({K, V}, Acc) when is_atom(K) -> Acc#{K => V};
            (_, Acc) -> Acc
        end, #{}, Trace).

fill_default(Trace = #?TRACE{start_at = undefined}) ->
    fill_default(Trace#?TRACE{start_at = os_now()});
fill_default(Trace = #?TRACE{end_at = undefined, start_at = StartAt}) ->
    fill_default(Trace#?TRACE{end_at = StartAt + 10 * 60});
fill_default(Trace) -> Trace.

-define(NAME_RE, "^[0-9A-Za-z]+[A-Za-z0-9-_]*$").

to_trace(#{name := Name} = Trace, Rec) ->
    case re:run(Name, ?NAME_RE) of
        nomatch -> {error, "Name should be " ?NAME_RE};
        _ -> to_trace(maps:remove(name, Trace), Rec#?TRACE{name = Name})
    end;
to_trace(#{type := <<"clientid">>, clientid := Filter} = Trace, Rec) ->
    Trace0 = maps:without([type, clientid], Trace),
    to_trace(Trace0, Rec#?TRACE{type = clientid, filter = Filter});
to_trace(#{type := <<"topic">>, topic := Filter} = Trace, Rec) ->
    case validate_topic(Filter) of
        ok ->
            Trace0 = maps:without([type, topic], Trace),
            to_trace(Trace0, Rec#?TRACE{type = topic, filter = Filter});
        Error -> Error
    end;
to_trace(#{type := <<"ip_address">>, ip_address := Filter} = Trace, Rec) ->
    case validate_ip_address(Filter) of
        ok ->
            Trace0 = maps:without([type, ip_address], Trace),
            to_trace(Trace0, Rec#?TRACE{type = ip_address, filter = Filter});
        Error -> Error
    end;
to_trace(#{type := Type}, _Rec) -> {error, io_lib:format("required ~s field", [Type])};
to_trace(#{start_at := StartAt} = Trace, Rec) ->
    case to_system_second(StartAt) of
        {ok, Sec} -> to_trace(maps:remove(start_at, Trace), Rec#?TRACE{start_at = Sec});
        {error, Reason} -> {error, Reason}
    end;
to_trace(#{end_at := EndAt} = Trace, Rec) ->
    Now = os_now(),
    case to_system_second(EndAt) of
        {ok, Sec} when Sec > Now ->
            to_trace(maps:remove(end_at, Trace), Rec#?TRACE{end_at = Sec});
        {ok, _Sec} ->
            {error, "end_at time has already passed"};
        {error, Reason} ->
            {error, Reason}
    end;
to_trace(_, Rec) -> {ok, Rec}.

validate_topic(TopicName) ->
    try emqx_topic:validate(filter, TopicName) of
        true -> ok
    catch
        error:Error ->
            {error, io_lib:format("topic: ~s invalid by ~p", [TopicName, Error])}
    end.
validate_ip_address(IP) ->
    case inet:parse_address(binary_to_list(IP)) of
        {ok, _} -> ok;
        {error, Reason} -> {error, lists:flatten(io_lib:format("ip address: ~p", [Reason]))}
    end.

to_system_second(At) ->
    try
        Sec = calendar:rfc3339_to_system_time(binary_to_list(At), [{unit, second}]),
        Now = os_now(),
        {ok, erlang:max(Now, Sec)}
    catch error: {badmatch, _} ->
        {error, ["The rfc3339 specification not satisfied: ", At]}
    end.

zip_dir() ->
    filename:join(trace_dir(), "zip").

trace_dir() ->
    filename:join(emqx:get_env(data_dir), "trace").

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

update_log_primary_level([], OriginLevel) -> set_log_primary_level(OriginLevel);
update_log_primary_level(_, _) -> set_log_primary_level(debug).

set_log_primary_level(NewLevel) ->
    case NewLevel =/= emqx_logger:get_primary_log_level() of
        true -> emqx_logger:set_primary_log_level(NewLevel);
        false -> ok
    end.

%% the dashboard use os time to create trace, do not use erlang:system_time/1
os_now() ->
    os:system_time(second).
