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

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps =
        emqx_cth_suite:start(
            [emqx, emqx_extsub],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    ok = emqx_extsub_handler:register(emqx_extsub_test_handler),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_extsub_handler:unregister(emqx_extsub_test_handler),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_CaseName, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_CaseName, _Config) ->
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
        {10, ?MIN_SUB_DELIVERING div 2, 100},
        {10, ?MIN_SUB_DELIVERING div 2, 0},
        {10, ?MIN_SUB_DELIVERING * 2, 10},
        {10, ?MIN_SUB_DELIVERING * 2, 0}
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
    CSub = emqx_extsub_test_utils:emqtt_connect([]),
    ok = emqx_extsub_test_utils:emqtt_subscribe(CSub, TopicFilter),
    {ok, Msgs} = emqx_extsub_test_utils:emqtt_drain(NBatches * BatchSize, max(IntervalMs * 2, 100)),
    ok = emqtt:disconnect(CSub),
    ?assertEqual(
        NBatches * BatchSize,
        length(Msgs),
        #{n_batches => NBatches, batch_size => BatchSize, interval_ms => IntervalMs}
    ).
