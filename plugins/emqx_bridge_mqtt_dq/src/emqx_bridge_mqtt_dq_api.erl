%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_api).

-export([handle/3]).

handle(get, [<<"metrics">>], _Request) ->
    with_snapshot(fun(Snapshot) ->
        {ok, 200, #{<<"content-type">> => <<"text/plain; version=0.0.4; charset=utf-8">>},
            iolist_to_binary(prometheus_metrics(Snapshot))}
    end);
handle(get, [<<"stats">>], _Request) ->
    with_snapshot(fun(Snapshot) -> {ok, 200, #{}, format_snapshot(Snapshot)} end);
handle(_Method, _Path, _Request) ->
    {error, not_found}.

with_snapshot(Fun) ->
    try emqx_bridge_mqtt_dq_metrics:snapshot() of
        Snapshot ->
            Fun(Snapshot)
    catch
        exit:{noproc, _} ->
            {error, 503, #{}, #{
                code => <<"SERVICE_UNAVAILABLE">>, message => <<"Metrics not ready">>
            }};
        Class:Reason ->
            {error, 503, #{}, #{
                code => <<"SERVICE_UNAVAILABLE">>,
                message => <<"Metrics unavailable">>,
                reason => iolist_to_binary(io_lib:format("~p:~p", [Class, Reason]))
            }}
    end.

format_snapshot(Snapshot) ->
    #{
        uptime_seconds => maps:get(uptime_seconds, Snapshot, 0),
        bridges => format_bridges(maps:get(bridges, Snapshot, #{})),
        buffers => format_buffers(maps:get(buffers, Snapshot, #{})),
        connectors => format_connectors(maps:get(connectors, Snapshot, #{}))
    }.

format_bridges(Bridges) ->
    lists:map(
        fun({BridgeName, Metrics}) ->
            #{
                name => BridgeName,
                matched => maps:get(matched, Metrics, 0),
                acked => maps:get(acked, Metrics, 0),
                dropped => maps:get(dropped, Metrics, 0),
                dropped_by_reason => format_dropped_by_reason(
                    maps:get(dropped_by_reason, Metrics, #{})
                )
            }
        end,
        lists:sort(maps:to_list(Bridges))
    ).

format_buffers(Buffers) ->
    lists:map(
        fun({{BridgeName, Index}, Metrics}) ->
            #{
                bridge => BridgeName,
                index => Index,
                buffered => maps:get(buffered, Metrics, 0)
            }
        end,
        lists:sort(maps:to_list(Buffers))
    ).

format_connectors(Connectors) ->
    lists:map(
        fun({{BridgeName, Index}, Metrics}) ->
            #{
                bridge => BridgeName,
                index => Index,
                backlog => maps:get(backlog, Metrics, 0),
                inflight => maps:get(inflight, Metrics, 0)
            }
        end,
        lists:sort(maps:to_list(Connectors))
    ).

format_dropped_by_reason(DroppedByReason) ->
    lists:map(
        fun({Reason, Count}) ->
            #{
                reason => Reason,
                dropped => Count
            }
        end,
        lists:sort(maps:to_list(DroppedByReason))
    ).

prometheus_metrics(Snapshot) ->
    Bridges = maps:get(bridges, Snapshot, #{}),
    Buffers = maps:get(buffers, Snapshot, #{}),
    Connectors = maps:get(connectors, Snapshot, #{}),
    [
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_uptime_seconds">>,
            <<"MQTT DQ plugin uptime in seconds (cluster-aggregated).">>,
            <<"gauge">>
        ),
        format_metric(
            <<"emqx_bridge_mqtt_dq_uptime_seconds">>, maps:get(uptime_seconds, Snapshot, 0)
        ),
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_bridge_matched_total">>,
            <<"Messages matched by bridge filter (cluster-aggregated).">>,
            <<"counter">>
        ),
        format_bridge_metric_lines(
            <<"emqx_bridge_mqtt_dq_bridge_matched_total">>, Bridges, matched
        ),
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_bridge_acked_total">>,
            <<"Messages durably acknowledged by bridge (cluster-aggregated).">>,
            <<"counter">>
        ),
        format_bridge_metric_lines(<<"emqx_bridge_mqtt_dq_bridge_acked_total">>, Bridges, acked),
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_bridge_dropped_total">>,
            <<"Messages dropped by bridge (cluster-aggregated).">>,
            <<"counter">>
        ),
        format_bridge_metric_lines(
            <<"emqx_bridge_mqtt_dq_bridge_dropped_total">>, Bridges, dropped
        ),
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_bridge_dropped_reason_total">>,
            <<"Messages dropped by bridge broken down by reason (cluster-aggregated).">>,
            <<"counter">>
        ),
        format_bridge_dropped_reason_lines(Bridges),
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_buffer_buffered">>,
            <<"Buffered messages per queue partition (cluster-aggregated).">>,
            <<"gauge">>
        ),
        format_buffer_lines(Buffers),
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_connector_backlog">>,
            <<"Connector backlog size (cluster-aggregated).">>,
            <<"gauge">>
        ),
        format_connector_lines(Connectors, backlog, <<"emqx_bridge_mqtt_dq_connector_backlog">>),
        format_help_and_type(
            <<"emqx_bridge_mqtt_dq_connector_inflight">>,
            <<"Connector inflight messages (cluster-aggregated).">>,
            <<"gauge">>
        ),
        format_connector_lines(Connectors, inflight, <<"emqx_bridge_mqtt_dq_connector_inflight">>)
    ].

format_bridge_metric_lines(Name, Bridges, Key) ->
    maps:fold(
        fun(BridgeName, Metrics, Acc) ->
            [
                Acc,
                format_metric_with_labels(
                    Name,
                    [{<<"bridge">>, BridgeName}],
                    maps:get(Key, Metrics, 0)
                )
            ]
        end,
        [],
        Bridges
    ).

format_bridge_dropped_reason_lines(Bridges) ->
    maps:fold(
        fun(BridgeName, Metrics, Acc0) ->
            maps:fold(
                fun(Reason, Count, Acc1) ->
                    [
                        Acc1,
                        format_metric_with_labels(
                            <<"emqx_bridge_mqtt_dq_bridge_dropped_reason_total">>,
                            [{<<"bridge">>, BridgeName}, {<<"reason">>, Reason}],
                            Count
                        )
                    ]
                end,
                Acc0,
                maps:get(dropped_by_reason, Metrics, #{})
            )
        end,
        [],
        Bridges
    ).

format_buffer_lines(Buffers) ->
    maps:fold(
        fun({BridgeName, Index}, Metrics, Acc) ->
            [
                Acc,
                format_metric_with_labels(
                    <<"emqx_bridge_mqtt_dq_buffer_buffered">>,
                    [{<<"bridge">>, BridgeName}, {<<"index">>, integer_to_binary(Index)}],
                    maps:get(buffered, Metrics, 0)
                )
            ]
        end,
        [],
        Buffers
    ).

format_connector_lines(Connectors, Key, Name) ->
    maps:fold(
        fun({BridgeName, Index}, Metrics, Acc) ->
            [
                Acc,
                format_metric_with_labels(
                    Name,
                    [{<<"bridge">>, BridgeName}, {<<"index">>, integer_to_binary(Index)}],
                    maps:get(Key, Metrics, 0)
                )
            ]
        end,
        [],
        Connectors
    ).

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
