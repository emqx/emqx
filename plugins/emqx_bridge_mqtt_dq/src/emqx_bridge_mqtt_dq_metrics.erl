%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_metrics).

-behaviour(gen_server).

-include("emqx_bridge_mqtt_dq.hrl").

-define(TAB, emqx_bridge_mqtt_dq_metrics).
-define(SNAPSHOT_DEADLINE_MS, 5000).

-export([
    start_link/0,
    reset/0,
    incr_bridge_enqueue/1,
    incr_bridge_enqueue/2,
    incr_bridge_dequeue/2,
    incr_bridge_publish/2,
    incr_bridge_drop/2,
    incr_bridge_retried/3,
    set_buffered/3,
    set_buffered_bytes/3,
    set_connector_backlog/3,
    set_connector_connected/3,
    set_connector_inflight/3,
    delete_bridge/1,
    snapshot/0
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

reset() ->
    gen_server:call(?MODULE, reset).

incr_bridge_enqueue(BridgeName) ->
    safe_ets(fun() -> incr_counter({bridge, BridgeName, enqueue}, 1) end).

incr_bridge_enqueue(BridgeName, Count) when is_integer(Count), Count > 0 ->
    safe_ets(fun() -> incr_counter({bridge, BridgeName, enqueue}, Count) end);
incr_bridge_enqueue(_BridgeName, _Count) ->
    ok.

incr_bridge_dequeue(BridgeName, Count) when is_integer(Count), Count > 0 ->
    safe_ets(fun() -> incr_counter({bridge, BridgeName, dequeue}, Count) end);
incr_bridge_dequeue(_BridgeName, _Count) ->
    ok.

incr_bridge_publish(BridgeName, Count) when is_integer(Count), Count > 0 ->
    safe_ets(fun() -> incr_counter({bridge, BridgeName, publish}, Count) end);
incr_bridge_publish(_BridgeName, _Count) ->
    ok.

incr_bridge_drop(BridgeName, Count) when is_integer(Count), Count > 0 ->
    safe_ets(fun() -> incr_counter({bridge, BridgeName, drop}, Count) end);
incr_bridge_drop(_BridgeName, _Count) ->
    ok.

incr_bridge_retried(BridgeName, Reason, Count) when is_integer(Count), Count > 0 ->
    RetryKey = retry_metric_key(Reason),
    safe_ets(fun() -> incr_counter({bridge, BridgeName, RetryKey}, Count) end);
incr_bridge_retried(_BridgeName, _Reason, _Count) ->
    ok.

set_buffered(BridgeName, Index, Value) ->
    safe_ets(fun() -> ets:insert(?TAB, {{buffer, BridgeName, Index, buffered}, Value}) end).

set_buffered_bytes(BridgeName, Index, Value) ->
    safe_ets(fun() -> ets:insert(?TAB, {{buffer, BridgeName, Index, buffered_bytes}, Value}) end).

set_connector_backlog(BridgeName, Index, Value) ->
    safe_ets(fun() -> ets:insert(?TAB, {{connector, BridgeName, Index, backlog}, Value}) end).

set_connector_connected(BridgeName, Index, Value) ->
    safe_ets(fun() -> ets:insert(?TAB, {{connector, BridgeName, Index, connected}, Value}) end).

set_connector_inflight(BridgeName, Index, Value) ->
    safe_ets(fun() -> ets:insert(?TAB, {{connector, BridgeName, Index, inflight}, Value}) end).

delete_bridge(BridgeName) when is_binary(BridgeName) ->
    safe_ets(fun() ->
        ets:match_delete(?TAB, {{bridge, BridgeName, '_'}, '_'}),
        ets:match_delete(?TAB, {{buffer, BridgeName, '_', '_'}, '_'}),
        ets:match_delete(?TAB, {{connector, BridgeName, '_', '_'}, '_'})
    end);
delete_bridge(BridgeName) ->
    ?LOG(warning, #{
        msg => "mqtt_dq_metrics_delete_bridge_invalid_name",
        bridge => BridgeName
    }),
    ok.

snapshot() ->
    gen_server:call(?MODULE, snapshot).

init([]) ->
    _ = ets:new(?TAB, [
        named_table,
        ordered_set,
        public,
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    reset_ets(),
    {ok, #{}}.

handle_call(reset, _From, State) ->
    reset_ets(),
    ok = refresh_live_metrics(),
    {reply, ok, State};
handle_call(snapshot, _From, State) ->
    {reply, aggregate_cluster_stats(), State};
handle_call({snapshot_local, DeadlineMs}, _From, State) ->
    case now_millisecond() > DeadlineMs of
        true ->
            {noreply, State};
        false ->
            {reply, local_snapshot(), State}
    end;
handle_call(_Call, _From, State) ->
    {reply, {error, bad_request}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

reset_ets() ->
    true = ets:delete_all_objects(?TAB),
    ets:insert(?TAB, {{global, started_at_ms}, now_millisecond()}),
    ok.

safe_ets(Fun) ->
    %% The ETS table may disappear during plugin shutdown or test teardown.
    try Fun() of
        _ -> ok
    catch
        error:badarg -> ok
    end.

incr_counter(Key, Count) ->
    ets:update_counter(?TAB, Key, {2, Count}, {Key, 0}),
    ok.

now_millisecond() ->
    erlang:system_time(millisecond).

local_snapshot() ->
    StartedAtMs = get_value({global, started_at_ms}, now_millisecond()),
    Rows = ets:tab2list(?TAB),
    Snapshot0 = zero_snapshot(),
    Snapshot1 = lists:foldl(fun fold_row/2, Snapshot0, Rows),
    Snapshot1#{
        uptime_seconds => erlang:max(0, (now_millisecond() - StartedAtMs) div 1000)
    }.

fold_row({{bridge, BridgeName, enqueue}, Value}, #{bridges := Bridges} = Acc) ->
    BridgeMetrics0 = maps:get(BridgeName, Bridges, zero_bridge_metrics()),
    BridgeMetrics1 = BridgeMetrics0#{enqueue => Value},
    Acc#{bridges := Bridges#{BridgeName => BridgeMetrics1}};
fold_row({{bridge, BridgeName, dequeue}, Value}, #{bridges := Bridges} = Acc) ->
    BridgeMetrics0 = maps:get(BridgeName, Bridges, zero_bridge_metrics()),
    BridgeMetrics1 = BridgeMetrics0#{dequeue => Value},
    Acc#{bridges := Bridges#{BridgeName => BridgeMetrics1}};
fold_row({{bridge, BridgeName, publish}, Value}, #{bridges := Bridges} = Acc) ->
    BridgeMetrics0 = maps:get(BridgeName, Bridges, zero_bridge_metrics()),
    BridgeMetrics1 = BridgeMetrics0#{publish => Value},
    Acc#{bridges := Bridges#{BridgeName => BridgeMetrics1}};
fold_row({{bridge, BridgeName, drop}, Value}, #{bridges := Bridges} = Acc) ->
    BridgeMetrics0 = maps:get(BridgeName, Bridges, zero_bridge_metrics()),
    BridgeMetrics1 = BridgeMetrics0#{drop => Value},
    Acc#{bridges := Bridges#{BridgeName => BridgeMetrics1}};
fold_row({{bridge, BridgeName, RetryKey}, Value}, #{bridges := Bridges} = Acc) ->
    case is_retry_detail_key(RetryKey) of
        true ->
            BridgeMetrics0 = maps:get(BridgeName, Bridges, zero_bridge_metrics()),
            RetryReason = retry_reason_name(RetryKey),
            RetriedByReason0 = maps:get(retried_by_reason, BridgeMetrics0, #{}),
            BridgeMetrics1 = BridgeMetrics0#{
                retried_by_reason => RetriedByReason0#{RetryReason => Value}
            },
            Acc#{bridges := Bridges#{BridgeName => BridgeMetrics1}};
        false ->
            Acc
    end;
fold_row({{buffer, BridgeName, Index, buffered}, Value}, #{buffers := Buffers} = Acc) ->
    BufMetrics0 = maps:get({BridgeName, Index}, Buffers, #{buffered => 0, buffered_bytes => 0}),
    Acc#{buffers := Buffers#{{BridgeName, Index} => BufMetrics0#{buffered => Value}}};
fold_row({{buffer, BridgeName, Index, buffered_bytes}, Value}, #{buffers := Buffers} = Acc) ->
    BufMetrics0 = maps:get({BridgeName, Index}, Buffers, #{buffered => 0, buffered_bytes => 0}),
    Acc#{buffers := Buffers#{{BridgeName, Index} => BufMetrics0#{buffered_bytes => Value}}};
fold_row({{connector, BridgeName, Index, backlog}, Value}, #{connectors := Connectors} = Acc) ->
    ConnectorMetrics0 = maps:get(
        {BridgeName, Index}, Connectors, #{backlog => 0, inflight => 0, connected => 0}
    ),
    ConnectorMetrics1 = ConnectorMetrics0#{backlog => Value},
    Acc#{connectors := Connectors#{{BridgeName, Index} => ConnectorMetrics1}};
fold_row({{connector, BridgeName, Index, connected}, Value}, #{connectors := Connectors} = Acc) ->
    ConnectorMetrics0 = maps:get(
        {BridgeName, Index}, Connectors, #{backlog => 0, inflight => 0, connected => 0}
    ),
    ConnectorMetrics1 = ConnectorMetrics0#{connected => Value},
    Acc#{connectors := Connectors#{{BridgeName, Index} => ConnectorMetrics1}};
fold_row({{connector, BridgeName, Index, inflight}, Value}, #{connectors := Connectors} = Acc) ->
    ConnectorMetrics0 = maps:get(
        {BridgeName, Index}, Connectors, #{backlog => 0, inflight => 0, connected => 0}
    ),
    ConnectorMetrics1 = ConnectorMetrics0#{inflight => Value},
    Acc#{connectors := Connectors#{{BridgeName, Index} => ConnectorMetrics1}};
fold_row(_Row, Acc) ->
    Acc.

get_value(Key, Default) ->
    case ets:lookup(?TAB, Key) of
        [{_, Value}] -> Value;
        [] -> Default
    end.

aggregate_cluster_stats() ->
    DeadlineMs = now_millisecond() + ?SNAPSHOT_DEADLINE_MS,
    Nodes = emqx:running_nodes(),
    NodeResults = emqx_utils:pmap(
        fun(Node) -> get_node_snapshot(Node, DeadlineMs) end, Nodes, infinity
    ),
    build_cluster_snapshot(NodeResults).

get_node_snapshot(Node, _DeadlineMs) when Node =:= node() ->
    {ok, Node, local_snapshot()};
get_node_snapshot(Node, DeadlineMs) ->
    TimeoutMs = erlang:max(1, DeadlineMs - now_millisecond()),
    try gen_server:call({?MODULE, Node}, {snapshot_local, DeadlineMs}, TimeoutMs) of
        Snapshot when is_map(Snapshot) ->
            {ok, Node, normalize_snapshot(Snapshot)};
        Other ->
            ?LOG(error, #{
                msg => "mqtt_dq_metrics_snapshot_invalid_response",
                node => Node,
                response => Other
            }),
            {error, Node, invalid_response}
    catch
        exit:{timeout, _} ->
            ?LOG(error, #{
                msg => "mqtt_dq_metrics_snapshot_timeout",
                node => Node,
                timeout_ms => TimeoutMs
            }),
            {error, Node, timeout};
        exit:{noproc, _} ->
            ?LOG(error, #{
                msg => "mqtt_dq_metrics_snapshot_no_process",
                node => Node
            }),
            {error, Node, no_process};
        exit:{nodedown, _} ->
            ?LOG(error, #{
                msg => "mqtt_dq_metrics_snapshot_node_down",
                node => Node
            }),
            {error, Node, node_down};
        Class:Reason:Stacktrace ->
            ?LOG(error, #{
                msg => "mqtt_dq_metrics_snapshot_failed",
                node => Node,
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, Node, {Class, Reason}}
    end.

normalize_snapshot(
    #{bridges := Bridges, buffers := Buffers, connectors := Connectors} = Snapshot
) when
    is_map(Bridges), is_map(Buffers), is_map(Connectors)
->
    #{
        uptime_seconds => maps:get(uptime_seconds, Snapshot, 0),
        bridges => Bridges,
        buffers => Buffers,
        connectors => Connectors
    };
normalize_snapshot(BadSnapshot) ->
    ?LOG(error, #{
        msg => "mqtt_dq_metrics_snapshot_malformed",
        snapshot => BadSnapshot
    }),
    zero_snapshot().

build_cluster_snapshot(NodeResults) ->
    {RespondedNodes, FailedNodes, Aggregated} = lists:foldl(
        fun
            ({ok, Node, Snapshot}, {OkAcc, ErrAcc, StatsAcc}) ->
                {[Node | OkAcc], ErrAcc, merge_snapshot(Snapshot, StatsAcc)};
            ({error, Node, Reason}, {OkAcc, ErrAcc, StatsAcc}) ->
                {OkAcc, [#{node => node_to_bin(Node), reason => reason_to_bin(Reason)} | ErrAcc],
                    StatsAcc}
        end,
        {[], [], zero_snapshot()},
        NodeResults
    ),
    Aggregated#{
        cluster => #{
            complete => FailedNodes =:= [],
            responded_nodes => lists:sort([node_to_bin(Node) || Node <- RespondedNodes]),
            failed_nodes => lists:sort(FailedNodes),
            timeout_ms => ?SNAPSHOT_DEADLINE_MS
        }
    }.

merge_snapshot(Stats, Acc) ->
    #{
        uptime_seconds := AccUptime,
        bridges := AccBridges,
        buffers := AccBuffers,
        connectors := AccConnectors
    } = Acc,
    Acc#{
        uptime_seconds => erlang:max(
            AccUptime,
            maps:get(uptime_seconds, Stats, 0)
        ),
        bridges => merge_bridges(AccBridges, maps:get(bridges, Stats, #{})),
        buffers => merge_buffers(AccBuffers, maps:get(buffers, Stats, #{})),
        connectors => merge_connectors(AccConnectors, maps:get(connectors, Stats, #{}))
    }.

merge_bridges(Left, Right) ->
    maps:fold(
        fun(BridgeName, StatsR, Acc0) ->
            StatsL = maps:get(BridgeName, Acc0, zero_bridge_metrics()),
            Acc0#{
                BridgeName => #{
                    enqueue => maps:get(enqueue, StatsL, 0) + maps:get(enqueue, StatsR, 0),
                    dequeue => maps:get(dequeue, StatsL, 0) + maps:get(dequeue, StatsR, 0),
                    publish => maps:get(publish, StatsL, 0) + maps:get(publish, StatsR, 0),
                    drop => maps:get(drop, StatsL, 0) + maps:get(drop, StatsR, 0),
                    retried_by_reason => merge_detail_counters(
                        maps:get(retried_by_reason, StatsL, #{}),
                        maps:get(retried_by_reason, StatsR, #{})
                    )
                }
            }
        end,
        Left,
        Right
    ).

merge_detail_counters(Left, Right) ->
    maps:fold(
        fun(Reason, CountR, Acc0) ->
            Acc0#{Reason => maps:get(Reason, Acc0, 0) + CountR}
        end,
        Left,
        Right
    ).

merge_buffers(Left, Right) ->
    Zero = #{buffered => 0, buffered_bytes => 0},
    maps:fold(
        fun(Id, StatsR, Acc0) ->
            StatsL = maps:get(Id, Acc0, Zero),
            Acc0#{
                Id => #{
                    buffered =>
                        maps:get(buffered, StatsL, 0) + maps:get(buffered, StatsR, 0),
                    buffered_bytes =>
                        maps:get(buffered_bytes, StatsL, 0) + maps:get(buffered_bytes, StatsR, 0)
                }
            }
        end,
        Left,
        Right
    ).

merge_connectors(Left, Right) ->
    maps:fold(
        fun(Id, StatsR, Acc0) ->
            StatsL = maps:get(Id, Acc0, #{backlog => 0, inflight => 0, connected => 0}),
            Acc0#{
                Id => #{
                    backlog => maps:get(backlog, StatsL, 0) + maps:get(backlog, StatsR, 0),
                    inflight => maps:get(inflight, StatsL, 0) + maps:get(inflight, StatsR, 0),
                    connected => maps:get(connected, StatsL, 0) + maps:get(connected, StatsR, 0)
                }
            }
        end,
        Left,
        Right
    ).

zero_snapshot() ->
    #{
        uptime_seconds => 0,
        bridges => #{},
        buffers => #{},
        connectors => #{},
        cluster => #{
            complete => true,
            responded_nodes => [],
            failed_nodes => [],
            timeout_ms => ?SNAPSHOT_DEADLINE_MS
        }
    }.

refresh_live_metrics() ->
    refresh_buffers(),
    refresh_connectors().

refresh_buffers() ->
    lists:foreach(fun refresh_bridge_buffers/1, emqx_bridge_mqtt_dq_config:get_bridges()),
    ok.

refresh_connectors() ->
    lists:foreach(fun refresh_bridge_connectors/1, emqx_bridge_mqtt_dq_config:get_bridges()),
    ok.

refresh_bridge_buffers(#{name := BridgeName, buffer_pool_size := PoolSize}) ->
    lists:foreach(
        fun(Index) -> sync_buffer_metrics(BridgeName, Index) end, lists:seq(0, PoolSize - 1)
    );
refresh_bridge_buffers(_) ->
    ok.

sync_buffer_metrics(BridgeName, Index) ->
    try
        Pid = emqx_bridge_mqtt_dq_buffer:get_pid(BridgeName, Index),
        ok = emqx_bridge_mqtt_dq_buffer:sync_metrics(Pid)
    catch
        Class:Reason ->
            ?LOG(debug, #{
                msg => "mqtt_dq_metrics_refresh_buffer_failed",
                bridge => BridgeName,
                index => Index,
                class => Class,
                reason => Reason
            }),
            ok
    end.

refresh_bridge_connectors(#{name := BridgeName}) ->
    try
        SupName = emqx_bridge_mqtt_dq_conn_sup:sup_pid(BridgeName),
        lists:foreach(fun sync_connector_metrics/1, supervisor:which_children(SupName))
    catch
        Class:Reason ->
            ?LOG(debug, #{
                msg => "mqtt_dq_metrics_refresh_connector_sup_failed",
                bridge => BridgeName,
                class => Class,
                reason => Reason
            }),
            ok
    end;
refresh_bridge_connectors(_) ->
    ok.

sync_connector_metrics({_, Pid, _, _}) when is_pid(Pid) ->
    try
        ok = emqx_bridge_mqtt_dq_connector:sync_metrics(Pid)
    catch
        Class:Reason ->
            ?LOG(debug, #{
                msg => "mqtt_dq_metrics_refresh_connector_failed",
                pid => Pid,
                class => Class,
                reason => Reason
            }),
            ok
    end;
sync_connector_metrics(_) ->
    ok.

retry_metric_key(reason_code) -> 'retried.reason_code';
retry_metric_key(timeout) -> 'retried.timeout';
retry_metric_key(connect_failed) -> 'retried.connect_failed';
retry_metric_key(connection_lost) -> 'retried.connection_lost';
retry_metric_key(_Other) -> 'retried.other'.

is_retry_detail_key(RetryKey) when is_atom(RetryKey) ->
    lists:prefix("retried.", atom_to_list(RetryKey));
is_retry_detail_key(_RetryKey) ->
    false.

retry_reason_name(RetryKey) ->
    Prefix = "retried.",
    RetryKeyList = atom_to_list(RetryKey),
    Reason = lists:nthtail(length(Prefix), RetryKeyList),
    list_to_binary(Reason).

zero_bridge_metrics() ->
    #{enqueue => 0, dequeue => 0, publish => 0, drop => 0, retried_by_reason => #{}}.

node_to_bin(Node) when is_atom(Node) ->
    atom_to_binary(Node, utf8);
node_to_bin(Node) when is_binary(Node) ->
    Node.

reason_to_bin(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
reason_to_bin({Class, Detail}) ->
    iolist_to_binary(io_lib:format("~p:~p", [Class, Detail]));
reason_to_bin(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).
