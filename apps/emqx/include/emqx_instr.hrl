-ifndef(EMQX_INSTR_HRL).
-define(EMQX_INSTR_HRL, true).

-define(BROKER_INSTR_METRICS_WORKER, broker_instr_metrics).

-ifdef(EMQX_BROKER_INSTR).

-define(BROKER_INSTR_METRICS, [broker, connection]).
-define(BROKER_INSTR_METRICS_DECL, [
    {broker, [
        {hist, dispatch_total_lat_us, ?BROKER_INSTR_BUCKETS_US_MEDIUM},
        {hist, dispatch_shard_delay_us, ?BROKER_INSTR_BUCKETS_US_SHORT},
        {hist, dispatch_shard_lat_us, ?BROKER_INSTR_BUCKETS_US_MEDIUM}
    ]},
    {connection, [
        {hist, deliver_delay_us, ?BROKER_INSTR_BUCKETS_US_SHORT},
        {hist, deliver_total_lat_us, ?BROKER_INSTR_BUCKETS_US_MEDIUM}
    ]}
]).

-else.

-define(BROKER_INSTR_METRICS, []).
-define(BROKER_INSTR_METRICS_DECL, []).

-endif.

-define(BROKER_INSTR_BUCKETS_US_SHORT, [
    100,
    200,
    500,
    1000,
    2000,
    5000,
    10000,
    15000,
    20000,
    25000,
    30000,
    40000,
    50000,
    60000,
    70000,
    80000,
    90000,
    100000
]).

-define(BROKER_INSTR_BUCKETS_US_MEDIUM, [
    1000,
    2000,
    5000,
    10000,
    15000,
    20000,
    25000,
    30000,
    40000,
    50000,
    60000,
    70000,
    80000,
    90000,
    100000,
    200000,
    500000
]).

-ifdef(EMQX_BROKER_INSTR).

-define(BROKER_INSTR_OBSERVE_HIST(METRIC, NAME, V),
    (emqx_metrics_worker:observe_hist(
        ?BROKER_INSTR_METRICS_WORKER,
        METRIC,
        NAME,
        V
    ))
).

-define(BROKER_INSTR_SETMARK(X, Y), (erlang:put(X, Y))).
-define(BROKER_INSTR_WMARK(X, MATCH, EXPR),
    (case erlang:erase(X) of
        MATCH -> EXPR;
        _ -> ok
    end)
).

-define(BROKER_INSTR_TS(), (os:perf_counter())).
-define(BROKER_INSTR_TS(BIND), (BIND = os:perf_counter())).
-define(BROKER_INSTR_BIND(BIND, _, X), (BIND = X)).

-else.

-define(BROKER_INSTR_SETMARK(X, Y), stub).
-define(BROKER_INSTR_WMARK(X, MATCH, EXPR), stub).
-define(BROKER_INSTR_OBSERVE_HIST(METRIC, NAME, V), stub).

-define(BROKER_INSTR_TS(), stub).
-define(BROKER_INSTR_TS(BIND), stub).
-define(BROKER_INSTR_BIND(BIND, X, _), (BIND = X)).

-endif.

-define(US_SINCE(PC), ?US(os:perf_counter() - PC)).
-define(US(PC),
    (erlang:convert_time_unit(PC, perf_counter, microsecond))
).

-endif.
