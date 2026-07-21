%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_top_collector).

-behaviour(gen_server).

-include("logger.hrl").

-export([
    start_link/0,
    run/2,
    cancel/0,
    status/0,
    top_scan_result/3
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-define(SCANNER_TIMEOUT, 5000).
-define(CALL_TIMEOUT, 15000).
-define(TOP_BPAPI_VSN, 1).
-define(DEFAULT_SCAN_BATCH_SIZE, 1000).
-define(DEFAULT_SCAN_SLEEP_MS, 1).
-define(SESSION_TOP_EXTRA_STATS, [mqueue_len, total_payload_bytes, inflight_cnt]).

-type sort_by() :: mqueue_length | total_payload_bytes.
-type row() :: #{
    clientid := emqx_types:clientid(),
    node := node(),
    mqueue_length := non_neg_integer(),
    total_payload_bytes := non_neg_integer(),
    inflight_count := non_neg_integer()
}.
-type completion_fun() :: fun(([row()]) -> ok | {error, term()}).
-type top_status() ::
    #{status := idle}
    | #{
        status := running,
        count := pos_integer(),
        sort := sort_by(),
        batch_size := pos_integer(),
        sleep_ms := non_neg_integer(),
        cluster_nodes := pos_integer(),
        started_at := integer(),
        bad_replies => [{node(), term()}]
    }
    | #{
        status := completed,
        rows := non_neg_integer(),
        started_at := integer(),
        bad_replies => [{node(), term()}]
    }
    | #{
        status := failed,
        reason := term(),
        started_at := integer(),
        bad_replies => [{node(), term()}]
    }
    | #{
        status := cancelled,
        reason := term(),
        started_at := integer()
    }.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec run(map(), completion_fun()) -> {ok, term()} | {error, busy | {busy, node()}}.
run(Opts, Completion) when is_function(Completion, 1) ->
    gen_server:call(?MODULE, {run, Opts, Completion}, ?CALL_TIMEOUT).

-spec cancel() -> {ok, cancelled} | {error, not_running}.
cancel() ->
    gen_server:call(?MODULE, cancel, ?CALL_TIMEOUT).

-spec status() -> top_status().
status() ->
    gen_server:call(?MODULE, status, ?CALL_TIMEOUT).

-spec top_scan_result(term(), node(), {ok, [emqx_session_tool:row()]} | {error, term()}) -> ok.
top_scan_result(ScanId, Node, Result) ->
    gen_server:cast(?MODULE, {top_scan_result, ScanId, Node, Result}).

normalize_rows(Rows) ->
    [normalize_row(Row) || Row <- Rows].

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{top => undefined, top_status => #{status => idle}}}.

handle_call({run, Opts0, Completion}, _From, State = #{top := undefined}) ->
    Opts = normalize_opts(Opts0),
    start_collector(Opts, Completion, State);
handle_call({run, _Opts, _Completion}, _From, State) ->
    {reply, {error, busy}, State};
handle_call(cancel, _From, State = #{top := Top = #{scan_id := ScanId}}) ->
    cancel_scans(maps:get(nodes, Top), ScanId),
    {reply, {ok, cancelled}, State#{
        top := undefined,
        top_status := cancelled_status(Top, cancelled)
    }};
handle_call(cancel, _From, State) ->
    {reply, {error, not_running}, State};
handle_call(status, _From, State = #{top_status := Status}) ->
    {reply, Status, State};
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(
    {top_scan_result, ScanId, Node, Result},
    State = #{top := #{scan_id := ScanId}}
) ->
    {noreply, complete_node(Node, Result, State)};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Collector
%%--------------------------------------------------------------------

start_collector(Opts, Completion, State) ->
    Nodes = lists:usort([node() | scan_nodes()]),
    Req = maps:merge(maps:remove(started_at, Opts), #{
        collector => node(),
        extra_stats => ?SESSION_TOP_EXTRA_STATS
    }),
    case emqx_session_top_scanner:start_scan(Req) of
        {ok, accepted} ->
            RemoteNodes = Nodes -- [node()],
            {StartedRemoteNodes, BadReplies} = start_remote_scans(RemoteNodes, Req),
            Pending = lists:usort([node() | StartedRemoteNodes]),
            Top = #{
                scan_id => maps:get(scan_id, Opts),
                opts => Opts,
                nodes => Nodes,
                pending => Pending,
                rows_by_node => #{},
                bad_replies => BadReplies,
                completion => Completion
            },
            {reply, {ok, maps:get(scan_id, Opts)}, State#{
                top := Top,
                top_status := running_status(Top)
            }};
        {error, {busy, Collector}} ->
            {reply, {error, {busy, Collector}}, State}
    end.

scan_nodes() ->
    try emqx_bpapi:nodes_supporting_bpapi_version(emqx_session_top, ?TOP_BPAPI_VSN) of
        Nodes -> Nodes
    catch
        _:_ -> emqx:running_nodes()
    end.

start_remote_scans([], _Req) ->
    {[], []};
start_remote_scans(Nodes, Req) ->
    Replies = emqx_session_top_proto_v1:start_top_scan(Nodes, Req, ?SCANNER_TIMEOUT),
    lists:foldl(
        fun({Node, Reply}, {Started, Bad}) ->
            case start_reply(Reply) of
                ok -> {[Node | Started], Bad};
                {error, Reason} -> {Started, [{Node, Reason} | Bad]}
            end
        end,
        {[], []},
        lists:zip(Nodes, Replies)
    ).

start_reply({ok, {ok, accepted}}) -> ok;
start_reply({ok, {error, Reason}}) -> {error, Reason};
start_reply({error, Reason}) -> {error, Reason};
start_reply(Reply) -> {error, Reply}.

complete_node(
    Node,
    Result,
    State = #{top := Top = #{pending := Pending0, rows_by_node := RowsByNode0, bad_replies := Bad0}}
) ->
    case lists:member(Node, Pending0) of
        false ->
            State;
        true ->
            Pending = lists:delete(Node, Pending0),
            {RowsByNode, Bad} =
                case Result of
                    {ok, Rows} -> {RowsByNode0#{Node => normalize_rows(Rows)}, Bad0};
                    {error, Reason} -> {RowsByNode0, [{Node, Reason} | Bad0]}
                end,
            Top1 = Top#{
                pending := Pending,
                rows_by_node := RowsByNode,
                bad_replies := Bad
            },
            case Pending of
                [] -> finish(State#{top := Top1});
                _ -> State#{top := Top1, top_status := running_status(Top1)}
            end
    end.

finish(State = #{top := Top = #{opts := Opts, rows_by_node := RowsByNode}}) ->
    Rows0 = lists:append(maps:values(RowsByNode)),
    Rows = top_rows(Rows0, maps:get(sort, Opts), maps:get(count, Opts)),
    case complete(maps:get(completion, Top), Rows) of
        ok ->
            State#{
                top := undefined,
                top_status := with_bad_replies(completed_status(Top, length(Rows)), Top)
            };
        {error, Reason} ->
            State#{
                top := undefined,
                top_status := with_bad_replies(failed_status(Top, Reason), Top)
            }
    end.

complete(Completion, Rows) ->
    try Completion(Rows) of
        ok -> ok;
        {error, Reason} -> {error, Reason};
        Result -> {error, {bad_completion_result, Result}}
    catch
        Class:Reason:Stack ->
            ?SLOG(error, #{
                msg => session_top_completion_failed,
                class => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, {Class, Reason}}
    end.

top_rows(Rows, SortBy, Count) ->
    Sorted = lists:sort(
        fun(RowA, RowB) -> row_sort_key(RowA, SortBy) =< row_sort_key(RowB, SortBy) end,
        Rows
    ),
    lists:sublist(Sorted, Count).

row_sort_key(Row, SortBy) ->
    {-maps:get(SortBy, Row), maps:get(clientid, Row), maps:get(node, Row)}.

normalize_row(Row) ->
    #{
        clientid => maps:get(clientid, Row),
        node => maps:get(node, Row),
        mqueue_length => stat_value(mqueue_len, Row),
        total_payload_bytes => stat_value(total_payload_bytes, Row),
        inflight_count => stat_value(inflight_cnt, Row)
    }.

stat_value(Key, #{extras := Extras} = Row) when is_map(Extras) ->
    case maps:get(Key, Extras, undefined) of
        undefined -> metric_value(Key, Row);
        Value -> Value
    end;
stat_value(Key, Row) ->
    metric_value(Key, Row).

metric_value(mqueue_len, #{metric := mqueue_len, value := Value}) -> Value;
metric_value(total_payload_bytes, #{metric := total_payload_bytes, value := Value}) -> Value;
metric_value(inflight_cnt, #{metric := inflight_cnt, value := Value}) -> Value;
metric_value(_Key, _Row) -> 0.

cancel_scans(Nodes, ScanId) ->
    %% Cancellation is best-effort and targets every node selected at startup;
    %% scanners ignore unknown or already-finished scan IDs.
    case lists:member(node(), Nodes) of
        true ->
            cancel_local_scan(ScanId);
        false ->
            ok
    end,
    cancel_remote_scans(Nodes -- [node()], ScanId).

cancel_local_scan(ScanId) ->
    try emqx_session_top_scanner:cancel(ScanId) of
        _ ->
            ok
    catch
        Class:Reason ->
            ?SLOG(warning, #{
                msg => session_top_cancel_failed,
                node => node(),
                class => Class,
                reason => Reason
            })
    end.

cancel_remote_scans([], _ScanId) ->
    ok;
cancel_remote_scans(Nodes, ScanId) ->
    try emqx_session_top_proto_v1:cancel_top_scan(Nodes, ScanId) of
        ok ->
            ok
    catch
        Class:Reason ->
            ?SLOG(warning, #{
                msg => session_top_cancel_failed,
                nodes => Nodes,
                class => Class,
                reason => Reason
            })
    end.

normalize_opts(Opts) ->
    StartedAt = maps:get(started_at, Opts, erlang:system_time(millisecond)),
    ScanId = maps:get(
        scan_id,
        Opts,
        {node(), StartedAt, erlang:unique_integer([positive])}
    ),
    Opts#{
        batch_size => maps:get(batch_size, Opts, ?DEFAULT_SCAN_BATCH_SIZE),
        sleep_ms => maps:get(sleep_ms, Opts, ?DEFAULT_SCAN_SLEEP_MS),
        scan_id => ScanId,
        started_at => StartedAt
    }.

running_status(#{opts := Opts, nodes := Nodes} = Top) ->
    with_bad_replies(
        #{
            status => running,
            count => maps:get(count, Opts),
            sort => maps:get(sort, Opts),
            batch_size => maps:get(batch_size, Opts),
            sleep_ms => maps:get(sleep_ms, Opts),
            cluster_nodes => length(Nodes),
            started_at => maps:get(started_at, Opts)
        },
        Top
    ).

completed_status(#{opts := Opts}, Rows) ->
    #{status => completed, rows => Rows, started_at => maps:get(started_at, Opts)}.

cancelled_status(#{opts := Opts}, Reason) ->
    #{status => cancelled, reason => Reason, started_at => maps:get(started_at, Opts)}.

failed_status(#{opts := Opts}, Reason) ->
    #{status => failed, reason => Reason, started_at => maps:get(started_at, Opts)}.

with_bad_replies(Status, #{bad_replies := []}) ->
    Status;
with_bad_replies(Status, #{bad_replies := BadReplies}) ->
    Status#{bad_replies => lists:reverse(BadReplies)}.
