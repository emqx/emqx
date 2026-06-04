%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_buffer_mon).

-behaviour(gen_server).

-include("emqx_cm.hrl").
-include("logger.hrl").

-export([
    start_link/0,
    update/1,
    maybe_log/3,
    run_top/1,
    local_top/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-ifdef(TEST).
-export([
    scan_local/2,
    csv_rows/1,
    result_rows/1,
    write_csv/2
]).
-endif.

-define(CONF_KEY, {?MODULE, conf}).
-define(DEFAULT_CONF, #{buffered_payload_high_watermark => 0}).
-define(LOG_MSG, session_buffer_high_watermark).
-define(TOP_TIMEOUT, 300000).
-define(SCAN_BATCH_SIZE, 1000).
-define(SCAN_SLEEP_MS, 1).

-type stats() :: emqx_types:stats() | map().
-type sort_by() :: mqueue_length | total_payload_bytes.
-type row() :: #{
    clientid := emqx_types:clientid(),
    pid := pid(),
    node := node(),
    mqueue_length := non_neg_integer(),
    total_payload_bytes := non_neg_integer(),
    inflight_count := non_neg_integer()
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec update(map() | undefined) -> ok.
update(Conf0) ->
    Conf = put_conf(Conf0),
    gen_server:cast(?MODULE, {update, Conf}).

-spec maybe_log(emqx_types:clientid(), pid(), stats()) -> ok.
maybe_log(ClientId, ChanPid, Stats) ->
    Conf = persistent_term:get(?CONF_KEY, ?DEFAULT_CONF),
    HighWatermark = maps:get(buffered_payload_high_watermark, Conf, 0),
    TotalPayloadBytes = stat_value(total_payload_bytes, Stats, 0),
    case HighWatermark > 0 andalso TotalPayloadBytes > HighWatermark of
        true ->
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => ?LOG_MSG,
                    clientid => ClientId,
                    pid => ChanPid,
                    mqueue_length => stat_value(mqueue_len, Stats, 0),
                    inflight_count => stat_value(inflight_cnt, Stats, 0),
                    total_payload_bytes => TotalPayloadBytes,
                    buffered_payload_high_watermark => HighWatermark
                },
                #{clientid => ClientId}
            );
        false ->
            ok
    end.

-spec run_top(#{count := pos_integer(), sort := sort_by(), out := file:name_all()}) ->
    {ok, pid()} | {error, busy}.
run_top(Opts) ->
    gen_server:call(?MODULE, {run_top, Opts}, infinity).

-spec local_top(pos_integer(), sort_by()) -> [row()].
local_top(Count, SortBy) ->
    gen_server:call(?MODULE, {local_top, Count, SortBy}, infinity).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    Conf = put_conf(emqx_config:get([sysmon, session], ?DEFAULT_CONF)),
    {ok, #{conf => Conf, scan => undefined}}.

handle_call({run_top, Opts}, _From, State = #{scan := undefined}) ->
    {Pid, MRef} = erlang:spawn_monitor(fun() -> do_run_top(Opts) end),
    {reply, {ok, Pid}, State#{scan := {Pid, MRef}}};
handle_call({run_top, _Opts}, _From, State) ->
    {reply, {error, busy}, State};
handle_call({local_top, Count, SortBy}, _From, State) ->
    {reply, scan_local(Count, SortBy), State};
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast({update, Conf0}, State) ->
    Conf = put_conf(Conf0),
    {noreply, State#{conf := Conf}};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, Reason}, State = #{scan := {Pid, MRef}}) ->
    maybe_log_scan_down(Reason),
    {noreply, State#{scan := undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Top-K scan
%%--------------------------------------------------------------------

do_run_top(#{count := Count, sort := SortBy, out := OutFile}) ->
    Nodes = emqx:running_nodes(),
    {Results, BadNodes} = rpc:multicall(Nodes, ?MODULE, local_top, [Count, SortBy], ?TOP_TIMEOUT),
    {Rows0, BadReplies} = result_rows(Results),
    Rows = top_rows(Rows0, Count, SortBy),
    case write_csv(OutFile, Rows) of
        ok ->
            ?SLOG(info, #{
                msg => session_top_written,
                file => OutFile,
                rows => length(Rows),
                partial => BadNodes =/= [] orelse BadReplies =/= [],
                bad_nodes => BadNodes,
                bad_replies => BadReplies
            });
        {error, Reason} ->
            ?SLOG(error, #{
                msg => session_top_write_failed,
                file => OutFile,
                reason => Reason,
                partial => BadNodes =/= [] orelse BadReplies =/= [],
                bad_nodes => BadNodes,
                bad_replies => BadReplies
            })
    end.

result_rows(Results) ->
    lists:foldr(fun result_rows/2, {[], []}, Results).

result_rows(Rows, {AccRows, AccBad}) when is_list(Rows) ->
    {Rows ++ AccRows, AccBad};
result_rows({badrpc, Reason}, {AccRows, AccBad}) ->
    {AccRows, [Reason | AccBad]};
result_rows(Other, {AccRows, AccBad}) ->
    {AccRows, [Other | AccBad]}.

-spec scan_local(pos_integer(), sort_by()) -> [row()].
scan_local(Count, SortBy) ->
    try
        scan_local(ets:first(?CHAN_INFO_TAB), Count, SortBy, gb_trees:empty(), 0)
    catch
        error:badarg ->
            []
    end.

scan_local('$end_of_table', _Count, _SortBy, TopRows, _BatchCount) ->
    top_heap_rows(TopRows);
scan_local(Key, Count, SortBy, TopRows, BatchCount) ->
    NextKey = ets:next(?CHAN_INFO_TAB, Key),
    TopRows1 =
        case ets:lookup(?CHAN_INFO_TAB, Key) of
            [{Key = {ClientId, ChanPid}, _Info, Stats}] ->
                keep_top(row(ClientId, ChanPid, Stats), Count, SortBy, TopRows);
            _ ->
                TopRows
        end,
    BatchCount1 = BatchCount + 1,
    case BatchCount1 rem ?SCAN_BATCH_SIZE of
        0 ->
            timer:sleep(?SCAN_SLEEP_MS);
        _ ->
            ok
    end,
    scan_local(NextKey, Count, SortBy, TopRows1, BatchCount1).

row(ClientId, ChanPid, Stats) ->
    #{
        clientid => ClientId,
        pid => ChanPid,
        node => node(),
        mqueue_length => stat_value(mqueue_len, Stats, 0),
        total_payload_bytes => stat_value(total_payload_bytes, Stats, 0),
        inflight_count => stat_value(inflight_cnt, Stats, 0)
    }.

keep_top(_Row, 0, _SortBy, TopRows) ->
    TopRows;
keep_top(Row, Count, SortBy, TopRows) ->
    insert_top(Row, Count, top_key(Row, SortBy), TopRows).

top_rows(Rows, Count, SortBy) ->
    top_heap_rows(
        lists:foldl(
            fun(Row, Heap) -> keep_top(Row, Count, SortBy, Heap) end, gb_trees:empty(), Rows
        )
    ).

insert_top(Row, Count, Key, TopRows) ->
    case gb_trees:size(TopRows) < Count of
        true ->
            gb_trees:insert(Key, Row, TopRows);
        false ->
            {SmallestKey, SmallestRow, TopRows1} = gb_trees:take_smallest(TopRows),
            case Key > SmallestKey of
                true ->
                    gb_trees:insert(Key, Row, TopRows1);
                false ->
                    gb_trees:insert(SmallestKey, SmallestRow, TopRows1)
            end
    end.

top_key(Row, SortBy) ->
    {
        sort_value(Row, SortBy),
        stable_value(Row, clientid),
        stable_value(Row, node),
        stable_value(Row, pid)
    }.

top_heap_rows(TopRows) ->
    [Row || {_Key, Row} <- lists:reverse(gb_trees:to_list(TopRows))].

sort_value(Row, mqueue_length) ->
    maps:get(mqueue_length, Row, 0);
sort_value(Row, total_payload_bytes) ->
    maps:get(total_payload_bytes, Row, 0).

stable_value(Row, Key) ->
    to_binary(maps:get(Key, Row)).

write_csv(OutFile, Rows) ->
    case file:open(OutFile, [write, raw, binary]) of
        {ok, IoDev} ->
            try
                write_csv_chunks(IoDev, [
                    <<"clientid,pid,node,mqueue_length,total_payload_bytes,inflight_count\n">>,
                    csv_rows(Rows)
                ])
            after
                _ = file:close(IoDev)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

write_csv_chunks(_IoDev, []) ->
    ok;
write_csv_chunks(IoDev, [Chunk | More]) ->
    case file:write(IoDev, Chunk) of
        ok -> write_csv_chunks(IoDev, More);
        {error, Reason} -> {error, Reason}
    end.

csv_rows(Rows) ->
    [csv_row(Row) || Row <- Rows].

csv_row(Row) ->
    [
        csv_cell(maps:get(clientid, Row)),
        <<",">>,
        csv_cell(maps:get(pid, Row)),
        <<",">>,
        csv_cell(maps:get(node, Row)),
        <<",">>,
        integer_to_binary(maps:get(mqueue_length, Row)),
        <<",">>,
        integer_to_binary(maps:get(total_payload_bytes, Row)),
        <<",">>,
        integer_to_binary(maps:get(inflight_count, Row)),
        $\n
    ].

csv_cell(Value) ->
    Bin = to_binary(Value),
    case needs_quote(Bin) of
        true -> [$", binary:replace(Bin, <<"\"">>, <<"\"\"">>, [global]), $"];
        false -> Bin
    end.

needs_quote(Bin) ->
    lists:any(
        fun(Pattern) -> binary:match(Bin, Pattern) =/= nomatch end,
        [<<",">>, <<"\"">>, <<"\n">>, <<"\r">>]
    ).

to_binary(Value) when is_binary(Value) ->
    Value;
to_binary(Value) when is_atom(Value) ->
    atom_to_binary(Value, utf8);
to_binary(Value) when is_pid(Value) ->
    list_to_binary(pid_to_list(Value));
to_binary(Value) when is_integer(Value) ->
    integer_to_binary(Value);
to_binary(Value) ->
    unicode:characters_to_binary(io_lib:format("~p", [Value])).

maybe_log_scan_down(normal) ->
    ok;
maybe_log_scan_down(Reason) ->
    ?SLOG(error, #{msg => session_top_scan_failed, reason => Reason}).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

put_conf(Conf0) ->
    Conf = normalize_conf(Conf0),
    persistent_term:put(?CONF_KEY, Conf),
    Conf.

normalize_conf(undefined) ->
    ?DEFAULT_CONF;
normalize_conf(Conf) when is_map(Conf) ->
    maps:merge(?DEFAULT_CONF, maps:filter(fun(_Key, Value) -> Value =/= undefined end, Conf)).

stat_value(Key, Stats, Default) when is_map(Stats) ->
    maps:get(Key, Stats, Default);
stat_value(Key, Stats, Default) when is_list(Stats) ->
    proplists:get_value(Key, Stats, Default).
