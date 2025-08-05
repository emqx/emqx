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
    get/1,
    disable_finished/1,
    get_enabled_trace/0
]).

-include("emqx_trace.hrl").

-export_type([record/0]).
-type record() :: #?TRACE{
    name :: binary(),
    type :: clientid | topic | ip_address | ruleid,
    filter :: emqx_trace:filter(),
    enable :: boolean(),
    payload_encode :: hex | text | hidden,
    extra :: trace_extra(),
    start_at :: integer(),
    end_at :: integer()
}.

-type trace_extra() :: #{
    formatter => text | json,
    payload_limit => integer(),
    namespace => atom() | binary(),
    slot => non_neg_integer()
}.

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
-spec insert_new_trace(record()) -> ok.
insert_new_trace(
    Trace0 = #?TRACE{
        name = Name,
        start_at = StartAt,
        type = Type,
        filter = Filter,
        extra = Extra
    }
) ->
    %% Name should be unique:
    case mnesia:read(?TRACE, Name) of
        [] ->
            ok;
        [#?TRACE{name = Name}] ->
            mnesia:abort({already_existed, Name})
    end,
    %% Disallow more than `max_traces` records:
    MaxTraces = max_traces(),
    _ = mnesia:lock({table, ?TRACE}, write),
    case mnesia:table_info(?TRACE, size) of
        S when S < MaxTraces ->
            ok;
        _ ->
            mnesia:abort({max_limit_reached, MaxTraces})
    end,
    %% Find free ID slot / verify uniqueness condition:
    SlotList = mnesia:foldl(
        fun(Trace, FL) ->
            case Trace of
                #?TRACE{start_at = StartAt, type = Type, filter = Filter} ->
                    %% Allow only one trace for each filter in the same second:
                    mnesia:abort({duplicate_condition, Trace#?TRACE.name});
                #?TRACE{extra = #{slot := Slot}} ->
                    emqx_trace_freelist:occupy(Slot, FL)
            end
        end,
        emqx_trace_freelist:range(1, MaxTraces),
        ?TRACE
    ),
    Slot = emqx_trace_freelist:first(SlotList),
    true = is_integer(Slot),
    Trace = Trace0#?TRACE{
        extra = Extra#{slot => Slot}
    },
    ok = mnesia:write(?TRACE, Trace, write).

%% Introduced in 5.0
-spec delete(Name :: binary()) -> ok.
delete(Name) ->
    case mnesia:read(?TRACE, Name) of
        [_] -> mnesia:delete(?TRACE, Name, write);
        [] -> mnesia:abort(not_found)
    end.

%% Introduced in 5.0
%% Deprecated since 5.10.0
-spec get_trace_filename(Name :: binary()) -> {ok, string()}.
get_trace_filename(Name) ->
    case mnesia:read(?TRACE, Name, read) of
        [] -> mnesia:abort(not_found);
        [#?TRACE{start_at = Start}] -> {ok, emqx_trace:log_filename(Name, Start)}
    end.

%% Introduced in 6.0.0
-spec get(Name :: binary()) -> {ok, record()} | {error, not_found}.
get(Name) ->
    case mnesia:dirty_read(?TRACE, Name) of
        [Trace] -> {ok, Trace};
        [] -> {error, not_found}
    end.

%% Introduced in 5.0
-spec disable_finished([record()]) -> ok.
disable_finished(Traces) ->
    lists:foreach(
        fun(#?TRACE{name = Name}) ->
            case mnesia:read(?TRACE, Name, write) of
                [] -> ok;
                [Trace] -> mnesia:write(?TRACE, Trace#?TRACE{enable = false}, write)
            end
        end,
        Traces
    ).

%% Introduced in 5.0
-spec get_enabled_trace() -> [record()].
get_enabled_trace() ->
    mnesia:match_object(?TRACE, #?TRACE{enable = true, _ = '_'}, read).

%%================================================================================
%% Internal functions
%%================================================================================

-spec max_traces() -> non_neg_integer().
max_traces() ->
    emqx_config:get([trace, max_traces]).
