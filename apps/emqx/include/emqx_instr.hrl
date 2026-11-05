-ifndef(EMQX_INSTR_HRL).
-define(EMQX_INSTR_HRL, true).

-define(BROKER_INSTR_METRICS_WORKER, broker_instr_metrics).

%% MQTT packet-to-message conversion and local dispatch start, in perf_counter units.
-define(BROKER_INSTR_HEADER, '$broker_instr').
-define(BROKER_INSTR_MARK(T0, TDISPATCH), {T0, TDISPATCH}).

%% Feature selection is supplied by mix.exs via EMQX_BROKER_INSTR.
-ifdef(EMQX_BROKER_INSTR_MESSAGE_LATENCY).

-define(BROKER_INSTR_MSGLATENCY_METRICS_DECL, [
    {broker, [
        {hist, dispatch_total_lat_us, ?BROKER_INSTR_BUCKETS_US_MEDIUM},
        {hist, dispatch_shard_delay_us, ?BROKER_INSTR_BUCKETS_US_SHORT},
        {hist, dispatch_shard_lat_us, ?BROKER_INSTR_BUCKETS_US_MEDIUM}
    ]},
    {connection, [
        {hist, deliver_delay_us, ?BROKER_INSTR_BUCKETS_US_SHORT},
        {hist, deliver_total_lat_us, ?BROKER_INSTR_BUCKETS_US_MEDIUM},
        {hist, brokerage_delay_us, ?BROKER_INSTR_BUCKETS_US_SHORT},
        {hist, brokerage_latency_us, ?BROKER_INSTR_BUCKETS_US_MEDIUM}
    ]}
]).

-else.

-define(BROKER_INSTR_MSGLATENCY_METRICS_DECL, []).

-endif.

-ifdef(EMQX_BROKER_INSTR_SOCKET_PACKETS).

-define(BROKER_INSTR_SOCKPACKET_METRICS_DECL, [
    {socket_packets, [
        {hist, recv_bytes, ?BROKER_INSTR_BUCKETS_BYTES},
        {hist, recv_requested_bytes, ?BROKER_INSTR_BUCKETS_BYTES},
        {hist, recv_shortfall_bytes, ?BROKER_INSTR_BUCKETS_BYTES},
        {hist, packet_remaining_bytes, ?BROKER_INSTR_BUCKETS_BYTES},
        {counter, body_binary_append_total},
        {counter, body_list_append_total}
    ]}
]).

-else.

-define(BROKER_INSTR_SOCKPACKET_METRICS_DECL, []).

-endif.

-define(BROKER_INSTR_METRICS_DECL,
    (?BROKER_INSTR_MSGLATENCY_METRICS_DECL ++ ?BROKER_INSTR_SOCKPACKET_METRICS_DECL)
).

-ifdef(EMQX_BROKER_INSTR).
-define(BROKER_INSTR_METRICS, [Id || {Id, _} <- ?BROKER_INSTR_METRICS_DECL]).
-else.
-define(BROKER_INSTR_METRICS, []).
-endif.

-define(BROKER_INSTR_BUCKETS_BYTES, [
    0,
    64,
    256,
    1024,
    4096,
    8192,
    16384,
    32768,
    65536,
    131072,
    262144,
    524288,
    1048576,
    4194304,
    16777216,
    67108864,
    268435455
]).

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

-define(BROKER_INSTR_BIND(BIND, X), ?BROKER_INSTR_BIND(BIND, stub, X)).

-ifdef(EMQX_BROKER_INSTR).

-define(BROKER_INSTR_TS(), (os:perf_counter())).
-define(BROKER_INSTR_TS(BIND), (BIND = os:perf_counter())).
-define(BROKER_INSTR_BIND(BIND, _, X), (BIND = X)).

-define(BROKER_INSTR_SETMARK(X, Y), (erlang:put(X, Y))).
-define(BROKER_INSTR_WMARK(X, MATCH, EXPR),
    (case erlang:erase(X) of
        MATCH -> EXPR;
        _ -> ok
    end)
).

-define(BROKER_INSTR_OBSERVE_HIST(METRIC, NAME, V),
    (emqx_metrics_worker:observe_hist(
        ?BROKER_INSTR_METRICS_WORKER,
        METRIC,
        NAME,
        V
    ))
).

-define(BROKER_INSTR_OBSERVE_INTVL(METRIC, NAME, START, END),
    (case is_integer(START) of
        true -> ?BROKER_INSTR_OBSERVE_HIST(METRIC, NAME, ?US(END - START));
        false -> ok
    end)
).

-define(BROKER_INSTR_OBSERVE_BS(METRIC, NAME, VALUE),
    ?BROKER_INSTR_OBSERVE_BS(METRIC, NAME, VALUE, true)
).

-define(BROKER_INSTR_OBSERVE_BS(METRIC, NAME, VALUE, VALUEGUARD),
    (case is_integer(VALUE) of
        true when (VALUEGUARD) -> ?BROKER_INSTR_OBSERVE_HIST(METRIC, NAME, VALUE);
        _ -> ok
    end)
).

-define(BROKER_INSTR_INC(METRIC, NAME),
    emqx_metrics_worker:inc(?BROKER_INSTR_METRICS_WORKER, METRIC, NAME)
).

-else.

-define(BROKER_INSTR_TS(), stub).
-define(BROKER_INSTR_TS(BIND), stub).
-define(BROKER_INSTR_SETMARK(X, Y), stub).
-define(BROKER_INSTR_WMARK(X, MATCH, EXPR), stub).
-define(BROKER_INSTR_BIND(BIND, X, _), (BIND = X)).
-define(BROKER_INSTR_OBSERVE_HIST(METRIC, NAME, V), stub).
-define(BROKER_INSTR_OBSERVE_INTVL(METRIC, NAME, START, END), stub).
-define(BROKER_INSTR_OBSERVE_BS(METRIC, NAME, VALUE), stub).
-define(BROKER_INSTR_OBSERVE_BS(METRIC, NAME, VALUE, VALUEGUARD), stub).
-define(BROKER_INSTR_INC(METRIC, NAME), stub).

-endif.

-ifdef(EMQX_BROKER_INSTR_MESSAGE_LATENCY).

-define(BROKER_INSTR_MSGLATENCY(EXPR), EXPR).

-define(BROKER_INSTR_HEADERS(MARK), #{?BROKER_INSTR_HEADER => MARK}).

-define(BROKER_INSTR_EXTRACT_MARK(BIND, MSG),
    (case MSG of
        #message{headers = #{?BROKER_INSTR_HEADER := BIND}} -> ok;
        _ -> BIND = ?BROKER_INSTR_MARK(undefined, undefined)
    end)
).

-define(BROKER_INSTR_UPDATE_MARK(MARK, MSG), (MSG)#message{
    headers = ((MSG)#message.headers)#{?BROKER_INSTR_HEADER => MARK}
}).

-else.

-define(BROKER_INSTR_MSGLATENCY(EXPR), stub).

-define(BROKER_INSTR_HEADERS(MARK), #{}).
-define(BROKER_INSTR_EXTRACT_MARK(BIND, MSG), stub).
-define(BROKER_INSTR_UPDATE_MARK(MARK, MSG), MSG).

-endif.

-define(US_SINCE(PC), ?US(os:perf_counter() - PC)).
-define(US(PC), (erlang:convert_time_unit(PC, perf_counter, microsecond))).

-ifdef(EMQX_BROKER_INSTR_SOCKET_PACKETS).

-define(BROKER_INSTR_SOCKPACKET(EXPR), EXPR).

-else.

-define(BROKER_INSTR_SOCKPACKET(EXPR), stub).

-endif.

-endif.
