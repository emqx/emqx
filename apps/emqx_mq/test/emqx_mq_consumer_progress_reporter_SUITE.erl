%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_progress_reporter_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_mq_internal.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_, _) ->
    snabbkaffe:stop(),
    meck:unload().

%% Check that the count threshold of half a bucket batches notifications and a flush sends the rest.
t_count_threshold(_Config) ->
    Reporter0 = reporter(#{count => 20, threshold_percentage => 50}, self()),
    %% The bucket threshold is 10 records, so notifications start at 5 consumed records
    Reporter1 = consume(Reporter0, 10, totals(2, 100)),
    ?assertEqual([], notifications()),
    Reporter2 = consume(Reporter1, 20, totals(3, 100)),
    ?assertEqual([20], notifications()),
    %% The totals restart after a notification
    Reporter3 = consume(Reporter2, 30, totals(4, 100)),
    ?assertEqual([], notifications()),
    %% A flush sends progress below the threshold once
    Reporter4 = emqx_mq_consumer_progress_reporter:flush(Reporter3),
    ?assertEqual([30], notifications()),
    _ = emqx_mq_consumer_progress_reporter:flush(Reporter4),
    ?assertEqual([], notifications()).

%% Check that the byte threshold of half a bucket triggers a notification without a count limit.
t_bytes_threshold(_Config) ->
    Reporter0 = reporter(#{bytes => 1000, threshold_percentage => 10}, self()),
    %% The bucket threshold is 100 bytes, so notifications start at 50 consumed bytes
    Reporter1 = consume(Reporter0, 10, totals(100, 40)),
    ?assertEqual([], notifications()),
    _ = consume(Reporter1, 20, totals(1, 10)),
    ?assertEqual([20], notifications()).

%% Check that progress without totals reports its boundary below the thresholds.
t_restored_buffer(_Config) ->
    Reporter0 = reporter(#{count => 100, bytes => 100000, threshold_percentage => 10}, self()),
    Reporter1 = consume(Reporter0, 10, totals(1, 10)),
    ?assertEqual([], notifications()),
    _ = consume(Reporter1, 20, undefined),
    ?assertEqual([20], notifications()).

%% Check that an attached or replaced cleaner receives the latest consumed boundary.
t_cleaner_replacement(_Config) ->
    Reporter0 = reporter(#{count => 2, threshold_percentage => 50}, undefined),
    %% Progress without a cleaner is kept
    Reporter1 = consume(Reporter0, 20, totals(1, 10)),
    ?assertEqual([], notifications()),
    Reporter2 = emqx_mq_consumer_progress_reporter:attach(Reporter1, self()),
    ?assertEqual([20], notifications()),
    %% A replacement cleaner receives the same progress again
    Reporter3 = emqx_mq_consumer_progress_reporter:attach(Reporter2, self()),
    ?assertEqual([20], notifications()),
    _ = emqx_mq_consumer_progress_reporter:flush(Reporter3),
    ?assertEqual([], notifications()).

reporter(Opts, Cleaner) ->
    meck:new(emqx_mq_prop, [passthrough, no_link]),
    meck:expect(emqx_mq_prop, quota_index_opts, fun(_) -> Opts end),
    emqx_mq_consumer_progress_reporter:new(#{limits => #{on_full => reject}}, <<"shard">>, Cleaner).

consume(Reporter, Boundary, Totals) ->
    emqx_mq_consumer_progress_reporter:handle_progress(
        Reporter, #progress{boundary = Boundary, totals = Totals}
    ).

totals(Count, Bytes) ->
    #{count => Count, bytes => Bytes}.

%% Boundaries sent to the cleaner since the previous call. Collecting the trace clears it.
notifications() ->
    [
        Boundary
     || #{shard := <<"shard">>, boundary := Boundary} <- ?of_kind(
            mq_cleanup_consumed, snabbkaffe:collect_trace(0)
        )
    ].
