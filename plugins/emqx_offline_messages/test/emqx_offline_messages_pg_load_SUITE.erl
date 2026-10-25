%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_pg_load_SUITE).
-moduledoc "Small PostgreSQL delivery load probes. Run on an otherwise idle test host.".
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> [t_delivery_load].
init_per_suite(Config) -> emqx_offline_messages_pg_SUITE:init_per_suite(Config).
end_per_suite(Config) -> emqx_offline_messages_pg_SUITE:end_per_suite(Config).
init_per_testcase(Case, Config) -> emqx_offline_messages_pg_SUITE:init_per_testcase(Case, Config).
end_per_testcase(Case, Config) -> emqx_offline_messages_pg_SUITE:end_per_testcase(Case, Config).

%% Measure idle polling, caught-up reads, commit-to-delivery latency and a stalled channel's mailbox.
-doc "Check accepted work stays bounded while live copies can accumulate in the mailbox.".
t_delivery_load(Config) ->
    Raw = ?config(raw, Config),
    Read0 = #{<<"window">> => 4, <<"page_max_messages">> => 2, <<"idle_poll_interval">> => 500},
    ok = emqx_offline_messages_pg:on_config_changed(Raw, Raw#{<<"read">> => Read0}),
    Idle = [
        begin
            Id = <<"idle-", (integer_to_binary(N))/binary>>,
            subscribe(Config, Id, <<"offline/", Id/binary>>, [])
        end
     || N <- lists:seq(1, 50)
    ],
    ok = meck:new(emqx_omp_pg_store, [passthrough, no_link]),
    Start = erlang:monotonic_time(millisecond),
    timer:sleep(1500),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    IdleReads = page_reads(),
    %% One sequence per subscription polls about once per interval.
    ?assert(IdleReads >= 100),
    ?assert(IdleReads =< 200),
    lists:foreach(fun emqtt:disconnect/1, Idle),
    meck:unload(emqx_omp_pg_store),

    Read1 = Read0#{<<"idle_poll_interval">> := 60000},
    ok = emqx_offline_messages_pg:on_config_changed(Raw, Raw#{<<"read">> => Read1}),
    Topic = <<"offline/latency">>,
    Fast = subscribe(Config, <<"fast-probe">>, Topic, []),
    ok = meck:new(emqx_omp_pg_store, [passthrough, no_link]),
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Latencies = [
        begin
            Msg = emqx_message:make(<<"probe">>, 1, Topic, integer_to_binary(N)),
            {ok, {Id, Offset}} = emqx_omp_pg_store:run(
                Conn, {append, Namespace, Topic, [emqx_omp_pg_store:encode(Msg)]}
            ),
            CommittedAt = erlang:monotonic_time(microsecond),
            ok = emqx_omp_pg_store:dispatch(Msg, {Namespace, Id}, Offset),
            [_] = emqx_offline_messages_pg_SUITE:receive_messages(1),
            erlang:monotonic_time(microsecond) - CommittedAt
        end
     || N <- lists:seq(1, 100)
    ],
    LiveReads = page_reads(),
    ?assertEqual(0, LiveReads),
    ok = emqtt:disconnect(Fast),
    meck:unload(emqx_omp_pg_store),

    SlowTopic = <<"offline/slow-probe">>,
    Slow = subscribe(
        Config,
        <<"slow-probe">>,
        SlowTopic,
        [{auto_ack, false}, {properties, #{'Receive-Maximum' => 1}}]
    ),
    [Channel] = emqx_cm:lookup_channels(<<"slow-probe">>),
    ok = sys:suspend(Channel),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Channel) end),
    lists:foreach(
        fun(N) ->
            {ok, [persisted], _} = emqx_broker:publish2(
                emqx_message:make(<<"probe">>, 1, SlowTopic, integer_to_binary(N))
            )
        end,
        lists:seq(1, 100)
    ),
    {message_queue_len, StalledQueue} = process_info(Channel, message_queue_len),
    ?assert(StalledQueue >= 100),
    ok = sys:resume(Channel),
    [_] = emqx_offline_messages_pg_SUITE:receive_messages(1),
    ?retry(20, 100, ?assertEqual({message_queue_len, 0}, process_info(Channel, message_queue_len))),
    ?assertMatch({ok, #{unacked_count := 1}}, emqx_extsub:inspect(Channel)),
    ?assertEqual(100, length(emqx_offline_messages_pg_SUITE:messages(Config, SlowTopic))),
    ok = emqtt:disconnect(Slow),
    Metrics = #{
        idle_subscriptions => 50,
        idle_interval_ms => 500,
        sample_ms => Elapsed,
        idle_page_reads => IdleReads,
        live_messages => 100,
        live_page_reads => LiveReads,
        commit_to_delivery_us => percentiles(Latencies),
        stalled_mailbox_messages => StalledQueue,
        configured_window => 4
    },
    ct:pal("PostgreSQL delivery probe: ~p", [Metrics]),
    ok = file:write_file(
        filename:join(?config(priv_dir, Config), "delivery-probe.eterm"),
        io_lib:format("~p.~n", [Metrics])
    ).

subscribe(Config, Id, Topic, Options) ->
    Client = emqx_offline_messages_pg_SUITE:client(Config, Id, Options),
    ?assertWaitEvent(
        emqtt:subscribe(Client, Topic, 1),
        #{?snk_kind := omp_pg_page, clientid := Id, dirty := false},
        5000
    ),
    Client.

page_reads() ->
    meck:num_calls(emqx_omp_pg_store, exec_async, ['_', {page, '_', '_', '_'}]).

percentiles(Values) ->
    Sorted = lists:sort(Values),
    #{p50 => lists:nth(50, Sorted), p95 => lists:nth(95, Sorted), p99 => lists:nth(99, Sorted)}.
