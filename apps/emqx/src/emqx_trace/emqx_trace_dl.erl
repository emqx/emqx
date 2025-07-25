%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Data layer for emqx_trace
-module(emqx_trace_dl).

%% API:
-export([
    update/2,
    insert_new_trace/1,
    delete/1,
    get_trace_filename/1,
    delete_finished/1,
    get_enabled_trace/0
]).

-include("emqx_trace.hrl").

%%================================================================================
%% API functions
%%================================================================================

%% Introduced in 5.0
-spec update(Name :: binary(), Enable :: boolean()) ->
    ok.
update(Name, Enable) ->
    case mnesia:read(?TRACE, Name) of
        [] ->
            mnesia:abort(not_found);
        [#?TRACE{enable = Enable}] ->
            ok;
        [Rec] ->
            case emqx_trace:now_second() >= Rec#?TRACE.end_at of
                false -> mnesia:write(?TRACE, Rec#?TRACE{enable = Enable}, write);
                true -> mnesia:abort(finished)
            end
    end.

%% Introduced in 5.0
insert_new_trace(Trace) ->
    %% Name should be unique:
    case mnesia:read(?TRACE, Trace#?TRACE.name) of
        [] ->
            ok;
        [#?TRACE{name = Name}] ->
            mnesia:abort({already_existed, Name})
    end,
    %% Disallow more than `max_traces` records:
    MaxTraces = max_traces(),
    mnesia:lock({table, ?TRACE}, write),
    case mnesia:table_info(?TRACE, size) of
        S when S < MaxTraces ->
            ok;
        _ ->
            mnesia:abort({max_limit_reached, MaxTraces})
    end,
    %% Allow only one trace for each filter in the same second:
    #?TRACE{start_at = StartAt, type = Type, filter = Filter} = Trace,
    Match = #?TRACE{_ = '_', start_at = StartAt, type = Type, filter = Filter},
    case mnesia:match_object(?TRACE, Match, read) of
        [] ->
            ok = mnesia:write(?TRACE, Trace, write);
        [#?TRACE{name = Another}] ->
            mnesia:abort({duplicate_condition, Another})
    end.

%% Introduced in 5.0
-spec delete(Name :: binary()) -> ok.
delete(Name) ->
    case mnesia:read(?TRACE, Name) of
        [_] -> mnesia:delete(?TRACE, Name, write);
        [] -> mnesia:abort(not_found)
    end.

%% Introduced in 5.0
-spec get_trace_filename(Name :: binary()) -> {ok, string()}.
get_trace_filename(Name) ->
    case mnesia:read(?TRACE, Name, read) of
        [] -> mnesia:abort(not_found);
        [#?TRACE{start_at = Start}] -> {ok, emqx_trace:filename(Name, Start)}
    end.

%% Introduced in 5.0
delete_finished(Traces) ->
    lists:map(
        fun(#?TRACE{name = Name}) ->
            case mnesia:read(?TRACE, Name, write) of
                [] -> ok;
                [Trace] -> mnesia:write(?TRACE, Trace#?TRACE{enable = false}, write)
            end
        end,
        Traces
    ).

%% Introduced in 5.0
get_enabled_trace() ->
    mnesia:match_object(?TRACE, #?TRACE{enable = true, _ = '_'}, read).

%%================================================================================
%% Internal functions
%%================================================================================

-spec max_traces() -> non_neg_integer().
max_traces() ->
    emqx_config:get([trace, max_traces]).
