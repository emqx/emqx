%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_aggregator_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_connector_aggregator],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_delivery' API
%%------------------------------------------------------------------------------

init_transfer_state_and_container_opts(_Buffer, Opts) ->
    #{container := ContainerOpts} = Opts,
    {ok, #{opts => Opts}, ContainerOpts}.

process_append(_IOData, State) ->
    State.

process_write(State) ->
    {ok, State}.

process_complete(_State) ->
    {ok, done}.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

now_ms() ->
    erlang:system_time(millisecond).

now_s() ->
    erlang:system_time(second).

wait_downs(Refs, _Timeout) when map_size(Refs) =:= 0 ->
    ok;
wait_downs(Refs0, Timeout) ->
    receive
        {'DOWN', Ref, process, _Pid, _Reason} when is_map_key(Ref, Refs0) ->
            Refs = maps:remove(Ref, Refs0),
            wait_downs(Refs, Timeout)
    after Timeout ->
        ct:fail("processes didn't die; remaining: ~b", [map_size(Refs0)])
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Verifies that a new delivery is triggered when we reach the configured maximum number
%% of records.
t_trigger_max_records(Config) ->
    ?check_trace(
        #{timetrap => 3_000},
        begin
            AggregId = ?FUNCTION_NAME,
            MaxRecords = 3,
            AggregOpts = #{
                max_records => MaxRecords,
                time_interval => 120_000,
                work_dir => emqx_cth_suite:work_dir(Config)
            },
            ContainerOpts = #{
                type => csv,
                column_order => []
            },
            DeliveryOpts = #{
                callback_module => ?MODULE,
                container => ContainerOpts,
                upload_options => #{}
            },
            {ok, Sup} = emqx_connector_aggreg_upload_sup:start_link(
                AggregId, AggregOpts, DeliveryOpts
            ),
            on_exit(fun() -> gen_server:stop(Sup) end),
            Timestamp = now_ms(),
            Records = lists:duplicate(MaxRecords, #{}),
            %% Should immediately trigger a delivery process to be kicked off.
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqx_connector_aggregator:push_records(AggregId, Timestamp, Records),
                    #{?snk_kind := connector_aggreg_delivery_completed}
                )
            ),
            ok
        end,
        []
    ),
    ok.

%% Checks that we handle concurrent `close_buffer` and `next_buffer` events.  See
%% https://emqx.atlassian.net/browse/EMQX-14204 and
%% https://github.com/emqx/emqx/pull/14983#issuecomment-2795145277 for more details.
t_concurrent_close_buffer_and_next_buffer(Config) ->
    ?check_trace(
        #{timetrap => 10_000},
        begin
            ?force_ordering(
                #{?snk_kind := "connector_aggregator_cast_rotate_buffer_received"},
                #{?snk_kind := "connector_aggregator_tick_close_buffer_async"}
            ),
            AggregId = ?FUNCTION_NAME,
            TimeInterval = 1,
            MaxRecords = 2,
            AggregOpts = #{
                max_records => MaxRecords,
                time_interval => TimeInterval,
                work_dir => emqx_cth_suite:work_dir(Config)
            },
            ContainerOpts = #{
                type => csv,
                column_order => []
            },
            DeliveryOpts = #{
                callback_module => ?MODULE,
                container => ContainerOpts,
                upload_options => #{}
            },
            {ok, Sup} = emqx_connector_aggreg_upload_sup:start_link(
                AggregId, AggregOpts, DeliveryOpts
            ),
            on_exit(fun() -> gen_server:stop(Sup) end),
            AggregatorPid = emqx_connector_aggregator:where(AggregId),
            ?assert(is_pid(AggregatorPid)),
            MRef = monitor(process, AggregatorPid),
            Now = now_s(),
            LargeBatch = lists:duplicate(3, #{}),

            %% Time at which the buffer has just become outdated.
            TimeJustExpired = Now + TimeInterval,

            {_, MRef1} = spawn_opt(
                fun() ->
                    %% Added time ensures the buffer is considered outdated, but still low enough
                    %% so that it's considered before the new `#buffer.until` after it's rotated.
                    emqx_connector_aggregator:tick(AggregId, TimeJustExpired)
                end,
                [link, monitor]
            ),

            {_, MRef2} = spawn_opt(
                fun() ->
                    %% `Now + TimeInterval` to trigger sync `next_buffer`.
                    emqx_connector_aggregator:push_records(AggregId, TimeJustExpired, LargeBatch)
                end,
                [link, monitor]
            ),

            wait_downs(maps:from_keys([MRef1, MRef2], true), 1_000),

            MRef
        end,
        fun(MRef, Trace) ->
            %% Original bug resulted in the aggregator process crashing due to function
            %% clause.
            ?assertNotReceive({'DOWN', MRef, process, _, _}),
            ?assertMatch(
                [],
                [
                    E
                 || E = #{transfer := T} <- ?of_kind(connector_aggreg_delivery_completed, Trace),
                    T == empty
                ]
            ),
            ok
        end
    ),
    ok.
