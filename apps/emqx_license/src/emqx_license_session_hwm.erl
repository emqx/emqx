%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_session_hwm).

-moduledoc """
Maintains a durable daily history of cluster session high-watermarks.

Each calendar day (in the configured timezone) gets at most one row.
Monthly billing peaks are derived at query time by folding daily rows.
The table uses `ordered_set` so that GC of old periods is efficient.
""".

-behaviour(gen_server).

-include("emqx_license.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    create_tables/0,
    start_link/0,
    observe/2,
    sync/0,
    list_history/2,
    gc/0
]).

-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(TAB, emqx_license_session_hwm).

-record(emqx_license_session_hwm, {
    %% Calendar tuple `{Year, Month, Day}` — sorts naturally in ordered_set.
    period :: {non_neg_integer(), non_neg_integer(), non_neg_integer()},
    high_watermark :: non_neg_integer(),
    observed_at :: non_neg_integer()
}).

-doc "Create the MRIA table. Safe to call on repeated application starts.".
create_tables() ->
    ok = mria:create_table(?TAB, [
        {type, ordered_set},
        {storage, disc_copies},
        {rlog_shard, ?LICENSE_SHARD},
        {record_name, emqx_license_session_hwm},
        {attributes, record_info(fields, emqx_license_session_hwm)}
    ]),
    [?TAB].

-doc "Start the recorder worker. Returns `ignore` on replicant nodes.".
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Feed an observed session total to the recorder. Always returns `ok`.".
observe(Timestamp, Count) when is_integer(Timestamp), is_integer(Count), Count >= 0 ->
    case should_observe(Timestamp, Count) of
        true ->
            gen_server:cast(?MODULE, {observe, Timestamp, Count});
        false ->
            ok
    end.

-doc "Synchronize with the recorder — blocks until all prior casts are processed.".
sync() ->
    gen_server:call(?MODULE, sync, timer:seconds(5)).

-doc "Query history rows. `Period` is `daily` or `monthly`; `Limit` caps the result count.".
list_history(Period, Limit) when (Period =:= daily orelse Period =:= monthly), is_integer(Limit) ->
    Limit1 = max(1, Limit),
    Rows = read_rows(),
    case Period of
        daily ->
            lists:sublist([format_row(Row) || Row <- Rows], Limit1);
        monthly ->
            lists:sublist(fold_monthly(Rows), Limit1)
    end.

-doc "Remove rows older than the retention window (24 months).".
gc() ->
    gc(erlang:system_time(millisecond)).

init([]) ->
    case mria_rlog:role() of
        replicant ->
            ignore;
        _ ->
            {ok, #{last_gc_period => undefined}, {continue, gc}}
    end.

handle_continue(gc, State) ->
    ok = gc(),
    {noreply, State};
handle_continue(_, State) ->
    {noreply, State}.

handle_call(sync, _From, State) ->
    {reply, ok, State};
handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast({observe, Timestamp, Count}, State) ->
    State1 =
        case maybe_store(Timestamp, Count) of
            ok ->
                maybe_gc_on_day_change(Timestamp, State);
            {error, Reason} ->
                ?SLOG(
                    error,
                    #{
                        msg => "failed_to_store_license_session_hwm",
                        reason => Reason,
                        timestamp => Timestamp,
                        count => Count
                    },
                    #{tag => "LICENSE"}
                ),
                State
        end,
    {noreply, State1};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

maybe_store(Timestamp, Count) ->
    Period = period(Timestamp),
    case
        mria:transaction(
            ?LICENSE_SHARD,
            fun() -> maybe_update_in_transaction(Period, Timestamp, Count) end
        )
    of
        {atomic, _Result} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.

%% Run gc only when the day period changes, to avoid scanning the table on every write.
maybe_gc_on_day_change(Timestamp, #{last_gc_period := LastPeriod} = State) ->
    CurrentPeriod = period(Timestamp),
    case CurrentPeriod of
        LastPeriod ->
            State;
        _ ->
            _ = gc(Timestamp),
            State#{last_gc_period => CurrentPeriod}
    end.

should_observe(Timestamp, Count) ->
    case whereis(?MODULE) of
        undefined ->
            false;
        _Pid ->
            Period = period(Timestamp),
            try
                case mnesia:dirty_read(?TAB, Period) of
                    [] ->
                        true;
                    [#emqx_license_session_hwm{high_watermark = High}] ->
                        Count > High
                end
            catch
                _:_ ->
                    true
            end
    end.

maybe_update_in_transaction(Period, Timestamp, Count) ->
    case mnesia:read(?TAB, Period, write) of
        [] ->
            mnesia:write(?TAB, new_record(Period, Timestamp, Count), write),
            inserted;
        [#emqx_license_session_hwm{high_watermark = High} = Record] when Count > High ->
            mnesia:write(
                ?TAB,
                Record#emqx_license_session_hwm{
                    high_watermark = Count,
                    observed_at = Timestamp
                },
                write
            ),
            updated;
        [#emqx_license_session_hwm{}] ->
            unchanged
    end.

new_record(Period, Timestamp, Count) ->
    #emqx_license_session_hwm{
        period = Period,
        high_watermark = Count,
        observed_at = Timestamp
    }.

read_rows() ->
    lists:reverse(ets:tab2list(?TAB)).

format_row(#emqx_license_session_hwm{
    period = Period,
    high_watermark = HighWatermark,
    observed_at = ObservedAt
}) ->
    #{
        period => format_period(Period),
        high_watermark => HighWatermark,
        observed_at => format_timestamp(ObservedAt),
        observed_at_ms => ObservedAt
    }.

format_period({Year, Month, Day}) ->
    iolist_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B", [Year, Month, Day])).

fold_monthly(Rows) ->
    Monthly = lists:foldl(
        fun(#emqx_license_session_hwm{period = {Y, M, _}} = Row, Acc) ->
            MonthKey = {Y, M},
            Formatted = format_row(Row),
            Candidate = Formatted#{period => format_month(MonthKey)},
            maps:update_with(
                MonthKey,
                fun(Existing) -> pick_better_monthly(Candidate, Existing) end,
                Candidate,
                Acc
            )
        end,
        #{},
        Rows
    ),
    lists:sort(
        fun(#{period := P1}, #{period := P2}) -> P1 >= P2 end,
        maps:values(Monthly)
    ).

format_month({Year, Month}) ->
    iolist_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

pick_better_monthly(
    #{high_watermark := High1, observed_at_ms := ObservedAt1} = Candidate,
    #{high_watermark := High2, observed_at_ms := ObservedAt2} = Existing
) ->
    case (High1 > High2) orelse ((High1 =:= High2) andalso (ObservedAt1 > ObservedAt2)) of
        true -> Candidate;
        false -> Existing
    end.

gc(Now) ->
    Earliest = earliest_period_to_keep(Now),
    gc_before(mnesia:dirty_first(?TAB), Earliest).

gc_before({_, _, _} = Key, Earliest) when Key < Earliest ->
    Next = mnesia:dirty_next(?TAB, Key),
    mria:dirty_delete(?TAB, Key),
    gc_before(Next, Earliest);
gc_before(_Key, _Earliest) ->
    ok.

%% Keep 24 months — earliest kept period is the 1st of the same month two years ago.
earliest_period_to_keep(Now) ->
    {Y, M, _} = period(Now),
    {Y - 2, M, 1}.

period(Timestamp) ->
    OffsetSec = timezone_offset_seconds(),
    {Date, _Time} = calendar:system_time_to_universal_time(
        Timestamp div 1000 + OffsetSec, second
    ),
    Date.

timezone_offset_seconds() ->
    case timezone() of
        system -> emqx_utils_calendar:offset_second(local);
        Offset -> emqx_utils_calendar:offset_second(Offset)
    end.

format_timestamp(ObservedAt) ->
    case timezone() of
        system ->
            emqx_utils_calendar:epoch_to_rfc3339(ObservedAt, millisecond);
        Offset ->
            iolist_to_binary(
                emqx_utils_calendar:format(ObservedAt, millisecond, Offset, <<"%Y-%m-%dT%H:%M:%S">>)
            )
    end.

timezone() ->
    emqx_conf:get([license, high_watermark_timezone], system).
