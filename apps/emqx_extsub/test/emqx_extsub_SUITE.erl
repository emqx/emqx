%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_extsub_internal.hrl").

-define(BUFFER_SIZE, 100).

all() ->
    [
        {group, durable_sessions},
        {group, memory_sessions}
    ].

groups() ->
    All = emqx_common_test_helpers:all(?MODULE),
    [
        {durable_sessions, [], All},
        {memory_sessions, [], All}
    ].

init_per_group(durable_sessions, Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                emqx_durable_timer,
                {emqx, #{config => #{<<"durable_sessions">> => #{<<"enable">> => true}}}},
                emqx_extsub
            ],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    emqx_persistent_message:wait_readiness(15_000),
    [{suite_apps, Apps} | Config];
init_per_group(memory_sessions, Config) ->
    Apps =
        emqx_cth_suite:start(
            [emqx, emqx_extsub],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    [{suite_apps, Apps} | Config].

end_per_group(_, Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_CaseName, Config) ->
    ok = snabbkaffe:start_trace(),
    ok = emqx_extsub_handler:register(emqx_extsub_test_handler, #{buffer_size => ?BUFFER_SIZE}),
    Config.

end_per_testcase(_CaseName, _Config) ->
    ok = emqx_extsub_handler:unregister(emqx_extsub_test_handler),
    ok = snabbkaffe:stop().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% We emulate different kinds of message flow into the external subscription handler.
%% We deliver message by 1, in batches smaller and larger than the extsub buffer size.
%% We expect the handler to deliver the messages into the channel correctly.
t_smoke(Config) ->
    Samples = [
        {10, 1, 10},
        {10, 1, 0},
        {10, ?BUFFER_SIZE div 2, 10},
        {10, ?BUFFER_SIZE div 2, 0},
        {10, ?BUFFER_SIZE * 2, 10},
        {10, ?BUFFER_SIZE * 2, 0}
    ],
    lists:foreach(
        fun({NBatches, BatchSize, IntervalMs}) ->
            test_smoke(Config, NBatches, BatchSize, IntervalMs)
        end,
        Samples
    ).

test_smoke(_Config, NBatches, BatchSize, IntervalMs) ->
    NBatchesBin = integer_to_binary(NBatches),
    BatchSizeBin = integer_to_binary(BatchSize),
    IntervalMsBin = integer_to_binary(IntervalMs),
    TopicFilter =
        <<"extsub_test/", NBatchesBin/binary, "/", BatchSizeBin/binary, "/", IntervalMsBin/binary>>,
    ?tp(warning, test_smoke, #{
        n_batches => NBatches,
        batch_size => BatchSize,
        interval_ms => IntervalMs,
        topic_filter => TopicFilter
    }),
    CSub = emqx_extsub_test_utils:emqtt_connect([
        {clientid, <<"csub-", TopicFilter/binary>>},
        {properties, #{'Session-Expiry-Interval' => 10000}},
        {clean_start, false}
    ]),
    ok = emqx_extsub_test_utils:emqtt_subscribe(CSub, TopicFilter),
    {ok, Msgs} = emqx_extsub_test_utils:emqtt_drain(NBatches * BatchSize, 5000),
    ok = emqtt:disconnect(CSub),
    ?assertEqual(
        NBatches * BatchSize,
        length(Msgs),
        #{n_batches => NBatches, batch_size => BatchSize, interval_ms => IntervalMs}
    ).
