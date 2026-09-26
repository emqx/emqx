%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_consumer_gate_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(SUP, emqx_bridge_kafka_consumer_sup).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_bridge_kafka],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    emqx_cth_suite:stop(?config(apps, TCConfig)).

init_per_testcase(_TestCase, TCConfig) ->
    ok = meck:new(brod_group_subscriber_v2, [passthrough]),
    snabbkaffe:start_trace(),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok = meck:unload(brod_group_subscriber_v2),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

fake_subscriber() ->
    receive
        stop -> ok
    end.

num_subscriber_starts() ->
    meck:num_calls(brod_group_subscriber_v2, start_link, '_').

mark_ready() ->
    ?wait_async_action(
        emqx_node_readiness:mark_ready(),
        #{?snk_kind := kafka_consumer_gate_scan_done},
        5_000
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
A group subscriber that fails to start once the node is ready is retried.
""".
t_retry_failed_start(_TCConfig) ->
    Id = <<"kafka_subscriber:t_retry_failed_start">>,
    on_exit(fun() -> ?SUP:ensure_child_deleted(Id) end),
    on_exit(fun emqx_node_readiness:mark_ready/0),
    ok = meck:expect(
        brod_group_subscriber_v2,
        start_link,
        1,
        meck:seq([
            meck:val({error, boom}),
            meck:exec(fun(_Config) -> {ok, spawn_link(fun fake_subscriber/0)} end)
        ])
    ),
    ok = emqx_node_readiness:mark_not_ready(),
    ?assertEqual({ok, undefined}, ?SUP:start_child(Id, #{})),
    ?assertMatch({ok, {ok, #{subscriber_ids := [Id]}}}, mark_ready()),
    ?assertMatch([{Id, undefined, worker, _}], supervisor:which_children(?SUP)),
    ?assertMatch(
        {ok, _},
        ?block_until(
            #{
                ?snk_kind := "kafka_consumer_subscriber_started_after_node_ready",
                subscriber_id := Id
            },
            5_000
        )
    ),
    ?assertMatch([{Id, Pid, worker, _}] when is_pid(Pid), supervisor:which_children(?SUP)),
    ?assertEqual(2, num_subscriber_starts()),
    ok.
