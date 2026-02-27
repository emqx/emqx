%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_cluster_watch).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-export([
    start_link/0,
    immediate_node_clear/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("emqx_username_quota.hrl").

-define(SERVER, ?MODULE).
-define(BOOTSTRAP_BATCH_SIZE, 100).
-define(BOOTSTRAP_REPL_POLL_INTERVAL_MS, 10).
-define(BOOTSTRAP_REPL_TIMEOUT_MS, 10000).

-ifdef(TEST).
-define(CLEAR_NODE_DELAY_SECONDS, 0).
-define(CLEAR_NODE_JITTER_MAX_SECONDS, 0).
-else.
-define(CLEAR_NODE_DELAY_SECONDS, 600).
-define(CLEAR_NODE_JITTER_MAX_SECONDS, 30).
-endif.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

immediate_node_clear(Node) ->
    _ = erlang:send(?SERVER, {clear_for_node, Node}),
    async.

init([]) ->
    process_flag(trap_exit, true),
    ok = ekka:monitor(membership),
    ok = emqx_username_quota_state:clear_self_node(),
    self() ! bootstrap,
    {ok, #{}}.

handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(bootstrap, State) ->
    case bootstrap_local_sessions() of
        {ok, Count} ->
            ?SLOG(info, #{msg => "username_quota_bootstrap_done", count => Count});
        {error, replication_timeout, Count} ->
            ?SLOG(error, #{
                msg => "username_quota_bootstrap_aborted",
                cause => "Replication timeout, maybe due to Core node overloaded",
                registered_before_abort => Count
            })
    end,
    {noreply, State};
handle_info({clear_for_node, Node}, State) ->
    case lists:member(Node, emqx:running_nodes()) of
        true ->
            ok;
        false ->
            _ = emqx_username_quota_state:clear_for_node(Node),
            ok
    end,
    {noreply, State};
handle_info({nodedown, Node}, State) ->
    case mria_rlog:role() of
        core ->
            _ = erlang:send_after(
                clear_node_delay_ms(Node), self(), {clear_for_node, Node}
            ),
            ok;
        replicant ->
            ok
    end,
    {noreply, State};
handle_info({membership, {mnesia, down, Node}}, State) ->
    handle_info({nodedown, Node}, State);
handle_info({membership, {node, down, Node}}, State) ->
    handle_info({nodedown, Node}, State);
handle_info({membership, _}, State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ekka:unmonitor(membership).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

bootstrap_local_sessions() ->
    Stream = emqx_cm:all_channels_stream([emqx_connection, emqx_ws_connection]),
    try
        bootstrap_loop(Stream, 0, 0, 0)
    after
        cleanup_repl_watermark()
    end.

-doc """
Iterate the channel stream in batches of ?BOOTSTRAP_BATCH_SIZE.
After each batch, write a replication watermark to COUNTER_TAB and wait
for it to appear locally. This confirms the replication pipeline has
caught up without depending on any specific session record (which could
be deleted by a concurrent client disconnect).
""".
bootstrap_loop(Stream, Count, BatchCount, BatchSeq) ->
    case emqx_utils_stream:next(Stream) of
        [] ->
            %% Stream exhausted — wait for final partial batch if needed
            case BatchCount > 0 of
                true ->
                    case wait_for_replication(BatchSeq + 1) of
                        ok -> {ok, Count};
                        timeout -> {error, replication_timeout, Count}
                    end;
                false ->
                    {ok, Count}
            end;
        [{ClientId, ChanPid, _ConnState, _ConnInfo, ClientInfo} | Rest] ->
            Username = emqx_utils_conv:bin(maps:get(username, ClientInfo, <<>>)),
            ClientIdBin = emqx_utils_conv:bin(ClientId),
            case Username =:= <<>> orelse ClientIdBin =:= <<>> of
                true ->
                    bootstrap_loop(Rest, Count, BatchCount, BatchSeq);
                false ->
                    emqx_username_quota_pool:add(Username, ClientIdBin, ChanPid),
                    NewCount = Count + 1,
                    NewBatchCount = BatchCount + 1,
                    case NewBatchCount >= ?BOOTSTRAP_BATCH_SIZE of
                        true ->
                            NewBatchSeq = BatchSeq + 1,
                            case wait_for_replication(NewBatchSeq) of
                                ok ->
                                    bootstrap_loop(Rest, NewCount, 0, NewBatchSeq);
                                timeout ->
                                    {error, replication_timeout, NewCount}
                            end;
                        false ->
                            bootstrap_loop(Rest, NewCount, NewBatchCount, BatchSeq)
                    end
            end
    end.

-define(REPL_WATERMARK_KEY, {<<"$repl_watermark$">>, node()}).

-doc """
Write a monotonically increasing watermark to COUNTER_TAB and wait for it
to appear in the local ETS replica. Unlike polling for a specific session
record, this watermark key is never touched by client disconnect hooks,
so it cannot be deleted out from under the poll loop.
""".
wait_for_replication(BatchSeq) ->
    mria:dirty_write(?COUNTER_TAB, #?COUNTER_TAB{key = ?REPL_WATERMARK_KEY, count = BatchSeq}),
    Deadline = erlang:monotonic_time(millisecond) + ?BOOTSTRAP_REPL_TIMEOUT_MS,
    poll_watermark(BatchSeq, Deadline).

poll_watermark(BatchSeq, Deadline) ->
    case ets:lookup(?COUNTER_TAB, ?REPL_WATERMARK_KEY) of
        [#?COUNTER_TAB{count = N}] when N >= BatchSeq ->
            ok;
        _ ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    timer:sleep(?BOOTSTRAP_REPL_POLL_INTERVAL_MS),
                    poll_watermark(BatchSeq, Deadline);
                false ->
                    timeout
            end
    end.

cleanup_repl_watermark() ->
    try
        mria:dirty_delete(?COUNTER_TAB, ?REPL_WATERMARK_KEY)
    catch
        _:_ -> ok
    end.

clear_node_delay_ms(Node) ->
    BaseS = ?CLEAR_NODE_DELAY_SECONDS,
    MaxJitterS = ?CLEAR_NODE_JITTER_MAX_SECONDS,
    JitterS = erlang:phash2({node(), Node}, MaxJitterS + 1),
    timer:seconds(BaseS + JitterS).
