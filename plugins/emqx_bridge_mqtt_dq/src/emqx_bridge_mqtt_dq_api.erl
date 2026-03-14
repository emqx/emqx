%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_api).

-export([handle/3]).

handle(get, [<<"metrics">>], _Request) ->
    with_stats(fun(Stats) ->
        {ok, 200, #{<<"content-type">> => <<"text/plain; version=0.0.4; charset=utf-8">>},
            iolist_to_binary(prometheus_metrics(Stats))}
    end);
handle(get, [<<"stats">>], _Request) ->
    with_stats(fun(Stats) -> {ok, 200, json_headers(), Stats} end);
handle(get, [<<"stats">>, BridgeName], _Request) ->
    with_stats(fun(Stats) ->
        case find_bridge(Stats, BridgeName) of
            {ok, Bridge} ->
                {ok, 200, json_headers(), #{cluster => maps:get(cluster, Stats), bridge => Bridge}};
            error ->
                {error, 404, json_headers(), #{
                    code => <<"NOT_FOUND">>,
                    message => <<"Bridge not found">>,
                    bridge => BridgeName
                }}
        end
    end);
handle(get, [<<"status">>], _Request) ->
    with_stats(fun(Stats) -> {ok, 200, json_headers(), status_payload(Stats)} end);
handle(get, [<<"ui">>], _Request) ->
    {ok, 200,
        #{
            <<"content-type">> => <<"text/html; charset=utf-8">>,
            <<"cache-control">> => <<"no-store, no-cache, must-revalidate">>,
            <<"pragma">> => <<"no-cache">>,
            <<"expires">> => <<"0">>
        },
        ui_html()};
handle(_Method, _Path, _Request) ->
    {error, not_found}.

with_stats(Fun) ->
    try build_stats(emqx_bridge_mqtt_dq_metrics:snapshot()) of
        Stats ->
            Fun(Stats)
    catch
        exit:{noproc, _} ->
            {error, 503, json_headers(), #{
                code => <<"SERVICE_UNAVAILABLE">>, message => <<"Metrics not ready">>
            }};
        Class:Reason ->
            {error, 503, json_headers(), #{
                code => <<"SERVICE_UNAVAILABLE">>,
                message => <<"Metrics unavailable">>,
                reason => iolist_to_binary(io_lib:format("~p:~p", [Class, Reason]))
            }}
    end.

build_stats(Snapshot) ->
    Bridges = [
        format_bridge(Bridge, Snapshot)
     || Bridge <- bridge_sort(emqx_bridge_mqtt_dq_config:get_bridges())
    ],
    #{
        cluster => maps:get(cluster, Snapshot, #{
            complete => true,
            responded_nodes => [],
            failed_nodes => [],
            timeout_ms => 5000
        }),
        uptime_seconds => maps:get(uptime_seconds, Snapshot, 0),
        summary => summary(Bridges),
        bridges => Bridges
    }.

bridge_sort(Bridges) ->
    lists:sort(fun(#{name := NameA}, #{name := NameB}) -> NameA =< NameB end, Bridges).

format_bridge(Bridge, Snapshot) ->
    #{
        name := Name,
        enable := Enabled,
        buffer_pool_size := BufferPoolSize,
        pool_size := PoolSize
    } = Bridge,
    BridgeMetrics = maps:get(Name, maps:get(bridges, Snapshot, #{}), #{}),
    Buffers = [
        format_buffer(
            Name, Index, maps:get({Name, Index}, maps:get(buffers, Snapshot, #{}), undefined)
        )
     || Index <- lists:seq(0, BufferPoolSize - 1)
    ],
    Connectors = [
        format_connector(
            Name,
            Index,
            maps:get({Name, Index}, maps:get(connectors, Snapshot, #{}), undefined),
            length(maps:get(responded_nodes, maps:get(cluster, Snapshot, #{}), []))
        )
     || Index <- lists:seq(0, PoolSize - 1)
    ],
    Buffered = lists:sum([maps:get(buffered, Row, 0) || Row <- Buffers]),
    Backlog = lists:sum([maps:get(backlog, Row, 0) || Row <- Connectors]),
    Inflight = lists:sum([maps:get(inflight, Row, 0) || Row <- Connectors]),
    RuntimeState = bridge_runtime_state(Enabled, Buffers, Connectors),
    Status = bridge_status(Enabled, Connectors, maps:get(cluster, Snapshot, #{})),
    #{
        name => Name,
        config_state => bool_state(Enabled, enabled, disabled),
        runtime_state => RuntimeState,
        status => Status,
        status_reason => status_reason(Status, maps:get(cluster, Snapshot, #{})),
        enqueue => maps:get(enqueue, BridgeMetrics, 0),
        dequeue => maps:get(dequeue, BridgeMetrics, 0),
        publish => maps:get(publish, BridgeMetrics, 0),
        drop => maps:get(drop, BridgeMetrics, 0),
        retried_by_reason => maps:get(retried_by_reason, BridgeMetrics, #{}),
        buffered => Buffered,
        backlog => Backlog,
        inflight => Inflight,
        buffers => buffer_view(Enabled, Buffers),
        connectors => connector_view(Enabled, Connectors)
    }.

format_buffer(Name, Index, undefined) ->
    #{bridge => Name, index => Index, status => missing, buffered => 0};
format_buffer(Name, Index, Metrics) ->
    #{
        bridge => Name,
        index => Index,
        status => running,
        buffered => maps:get(buffered, Metrics, 0)
    }.

format_connector(Name, Index, undefined, _RespondedNodes) ->
    #{
        bridge => Name,
        index => Index,
        status => missing,
        backlog => 0,
        inflight => 0
    };
format_connector(Name, Index, Metrics, RespondedNodes) ->
    ConnectedCount = maps:get(connected, Metrics, 0),
    #{
        bridge => Name,
        index => Index,
        status => connector_status(ConnectedCount, RespondedNodes),
        backlog => maps:get(backlog, Metrics, 0),
        inflight => maps:get(inflight, Metrics, 0)
    }.

buffer_view(false, _Buffers) ->
    [];
buffer_view(true, Buffers) ->
    Buffers.

connector_view(false, _Connectors) ->
    [];
connector_view(true, Connectors) ->
    Connectors.

connector_status(_ConnectedCount, 0) ->
    unknown;
connector_status(ConnectedCount, RespondedNodes) when ConnectedCount =:= RespondedNodes ->
    connected;
connector_status(0, _RespondedNodes) ->
    disconnected;
connector_status(_ConnectedCount, _RespondedNodes) ->
    partial.

bridge_runtime_state(false, _Buffers, _Connectors) ->
    purged;
bridge_runtime_state(true, Buffers, Connectors) ->
    case has_missing_worker(Buffers, Connectors) of
        true -> degraded;
        false -> running
    end.

has_missing_worker(Buffers, Connectors) ->
    lists:any(fun(#{status := Status}) -> Status =:= missing end, Buffers ++ Connectors).

bridge_status(false, _Connectors, _Cluster) ->
    disabled;
bridge_status(true, Connectors, Cluster) ->
    Complete = maps:get(complete, Cluster, true),
    ConnectorStatuses = [maps:get(status, Row) || Row <- Connectors],
    case lists:any(fun(Status) -> Status =:= missing end, ConnectorStatuses) of
        true ->
            error;
        false ->
            case
                {lists:all(fun(Status) -> Status =:= connected end, ConnectorStatuses), Complete}
            of
                {true, true} ->
                    ok;
                {true, false} ->
                    partial;
                _ ->
                    case lists:any(fun(Status) -> Status =:= partial end, ConnectorStatuses) of
                        true ->
                            partial;
                        false ->
                            case
                                lists:any(
                                    fun(Status) -> Status =:= connected end, ConnectorStatuses
                                )
                            of
                                true -> partial;
                                false -> disconnected
                            end
                    end
            end
    end.

status_reason(ok, _Cluster) ->
    null;
status_reason(disabled, _Cluster) ->
    <<"Bridge disabled in config; workers stopped and queue purged">>;
status_reason(disconnected, _Cluster) ->
    <<"All connector slots are disconnected">>;
status_reason(error, _Cluster) ->
    <<"Expected bridge workers are missing">>;
status_reason(partial, Cluster) ->
    case maps:get(complete, Cluster, true) of
        false -> <<"Cluster aggregation is partial">>;
        true -> <<"Only part of the connector capacity is connected">>
    end;
status_reason(unknown, _Cluster) ->
    <<"Bridge status is unknown">>.

summary(Bridges) ->
    #{
        bridge_count => length(Bridges),
        running_bridge_count => length([B || #{runtime_state := running} = B <- Bridges]),
        buffered => lists:sum([maps:get(buffered, B, 0) || B <- Bridges]),
        backlog => lists:sum([maps:get(backlog, B, 0) || B <- Bridges]),
        inflight => lists:sum([maps:get(inflight, B, 0) || B <- Bridges]),
        enqueue => lists:sum([maps:get(enqueue, B, 0) || B <- Bridges]),
        dequeue => lists:sum([maps:get(dequeue, B, 0) || B <- Bridges]),
        publish => lists:sum([maps:get(publish, B, 0) || B <- Bridges]),
        drop => lists:sum([maps:get(drop, B, 0) || B <- Bridges])
    }.

status_payload(Stats) ->
    Cluster = maps:get(cluster, Stats, #{}),
    Bridges = maps:get(bridges, Stats, []),
    #{
        plugin => <<"emqx_bridge_mqtt_dq">>,
        cluster => Cluster,
        status =>
            case
                maps:get(complete, Cluster, true) andalso
                    lists:all(fun bridge_ok_for_status/1, Bridges)
            of
                true -> ok;
                false -> degraded
            end,
        bridge_count => length(Bridges)
    }.

bridge_ok_for_status(#{status := ok}) ->
    true;
bridge_ok_for_status(#{status := disabled}) ->
    true;
bridge_ok_for_status(_) ->
    false.

find_bridge(#{bridges := Bridges}, BridgeName) ->
    case [Bridge || #{name := Name} = Bridge <- Bridges, Name =:= BridgeName] of
        [Bridge] -> {ok, Bridge};
        [] -> error
    end.

json_headers() ->
    #{<<"content-type">> => <<"application/json; charset=utf-8">>}.

bool_state(true, TrueValue, _FalseValue) ->
    TrueValue;
bool_state(false, _TrueValue, FalseValue) ->
    FalseValue.

prometheus_metrics(Stats) ->
    Bridges = maps:get(bridges, Stats, []),
    Summary = maps:get(summary, Stats, #{}),
    [
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_uptime_seconds">>,
            <<"MQTT DQ plugin uptime in seconds (cluster-aggregated).">>,
            <<"gauge">>
        ),
        format_metric(
            <<"emqx_bridge_mqtt_dq_uptime_seconds">>,
            maps:get(uptime_seconds, Stats, 0)
        ),
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_bridge_enqueue_total">>,
            <<"Messages accepted into the bridge enqueue path (cluster-aggregated).">>,
            <<"counter">>
        ),
        [
            format_bridge_counter(<<"emqx_bridge_mqtt_dq_bridge_enqueue_total">>, enqueue, Bridge)
         || Bridge <- Bridges
        ],
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_bridge_dequeue_total">>,
            <<"Messages durably removed from the local queue (cluster-aggregated).">>,
            <<"counter">>
        ),
        [
            format_bridge_counter(<<"emqx_bridge_mqtt_dq_bridge_dequeue_total">>, dequeue, Bridge)
         || Bridge <- Bridges
        ],
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_bridge_publish_total">>,
            <<"Messages published by bridge to the remote broker (cluster-aggregated).">>,
            <<"counter">>
        ),
        [
            format_bridge_counter(<<"emqx_bridge_mqtt_dq_bridge_publish_total">>, publish, Bridge)
         || Bridge <- Bridges
        ],
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_bridge_drop_total">>,
            <<"Messages dropped after entering the local queue (cluster-aggregated).">>,
            <<"counter">>
        ),
        [
            format_bridge_counter(<<"emqx_bridge_mqtt_dq_bridge_drop_total">>, drop, Bridge)
         || Bridge <- Bridges
        ],
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_bridge_status">>,
            <<"Bridge runtime status as a labeled gauge (cluster-aggregated).">>,
            <<"gauge">>
        ),
        [format_bridge_status(Bridge) || Bridge <- Bridges],
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_bridge_retry_reason_total">>,
            <<"Retry attempts by bridge broken down by reason (cluster-aggregated).">>,
            <<"counter">>
        ),
        [format_bridge_retry_reasons(Bridge) || Bridge <- Bridges],
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_buffer_buffered">>,
            <<"Buffered messages per queue partition (cluster-aggregated).">>,
            <<"gauge">>
        ),
        [format_buffer_metrics(Bridge) || Bridge <- Bridges],
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_connector_backlog">>,
            <<"Connector backlog size (cluster-aggregated).">>,
            <<"gauge">>
        ),
        [
            format_connector_metrics(Bridge, backlog, <<"emqx_bridge_mqtt_dq_connector_backlog">>)
         || Bridge <- Bridges
        ],
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_connector_inflight">>,
            <<"Connector inflight messages (cluster-aggregated).">>,
            <<"gauge">>
        ),
        [
            format_connector_metrics(Bridge, inflight, <<"emqx_bridge_mqtt_dq_connector_inflight">>)
         || Bridge <- Bridges
        ],
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_summary_buffered">>,
            <<"Total buffered messages across all bridges (cluster-aggregated).">>,
            <<"gauge">>
        ),
        format_metric(<<"emqx_bridge_mqtt_dq_summary_buffered">>, maps:get(buffered, Summary, 0))
    ].

format_bridge_counter(MetricName, Key, Bridge) ->
    format_metric_with_labels(
        MetricName,
        [{<<"bridge">>, maps:get(name, Bridge)}],
        maps:get(Key, Bridge, 0)
    ).

format_bridge_status(Bridge) ->
    format_metric_with_labels(
        <<"emqx_bridge_mqtt_dq_bridge_status">>,
        [
            {<<"bridge">>, maps:get(name, Bridge)},
            {<<"status">>, atom_to_binary(maps:get(status, Bridge), utf8)}
        ],
        1
    ).

format_bridge_retry_reasons(Bridge) ->
    maps:fold(
        fun(Reason, Count, Acc) ->
            [
                Acc,
                format_metric_with_labels(
                    <<"emqx_bridge_mqtt_dq_bridge_retry_reason_total">>,
                    [{<<"bridge">>, maps:get(name, Bridge)}, {<<"reason">>, Reason}],
                    Count
                )
            ]
        end,
        [],
        maps:get(retried_by_reason, Bridge, #{})
    ).

format_buffer_metrics(Bridge) ->
    [
        format_metric_with_labels(
            <<"emqx_bridge_mqtt_dq_buffer_buffered">>,
            [
                {<<"bridge">>, maps:get(name, Bridge)},
                {<<"index">>, integer_to_binary(maps:get(index, Buffer))}
            ],
            maps:get(buffered, Buffer, 0)
        )
     || Buffer <- maps:get(buffers, Bridge, [])
    ].

format_connector_metrics(Bridge, Key, MetricName) ->
    [
        format_metric_with_labels(
            MetricName,
            [
                {<<"bridge">>, maps:get(name, Bridge)},
                {<<"index">>, integer_to_binary(maps:get(index, Connector))}
            ],
            maps:get(Key, Connector, 0)
        )
     || Connector <- maps:get(connectors, Bridge, [])
    ].

format_help_and_type(Name, Help, Type) ->
    [
        <<"# HELP ">>,
        Name,
        <<" ">>,
        Help,
        <<"\n">>,
        <<"# TYPE ">>,
        Name,
        <<" ">>,
        Type,
        <<"\n">>
    ].

format_metric(Name, Value) ->
    [Name, <<" ">>, integer_to_binary(Value), <<"\n">>].

format_metric_with_labels(Name, Labels, Value) ->
    [Name, <<"{">>, format_labels(Labels), <<"} ">>, integer_to_binary(Value), <<"\n">>].

format_labels(Labels) ->
    iolist_join(
        [
            [Key, <<"=\"">>, prom_escape_label(Value), <<"\"">>]
         || {Key, Value} <- Labels
        ],
        <<",">>
    ).

prom_escape_label(Value) when is_binary(Value) ->
    binary:replace(
        binary:replace(
            binary:replace(Value, <<"\\">>, <<"\\\\">>, [global]), <<"\"">>, <<"\\\"">>, [global]
        ),
        <<"\n">>,
        <<"\\n">>,
        [global]
    );
prom_escape_label(Value) ->
    prom_escape_label(iolist_to_binary(io_lib:format("~p", [Value]))).

iolist_join([], _Sep) ->
    [];
iolist_join([One], _Sep) ->
    One;
iolist_join([H | T], Sep) ->
    [H, [[Sep, Elem] || Elem <- T]].

ui_html() ->
    maybe
        Dir = code:priv_dir(emqx_bridge_mqtt_dq),
        true ?= is_list(Dir),
        {ok, Bin} ?= file:read_file(filename:join(Dir, "ui.html")),
        Bin
    else
        _ ->
            <<
                "<!doctype html><html><body><h1>MQTT DQ UI unavailable</h1>",
                "<p>Missing priv/ui.html</p></body></html>"
            >>
    end.
