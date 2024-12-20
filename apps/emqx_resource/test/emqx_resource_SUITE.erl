%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_resource_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource_buffer_worker_internal.hrl").

-define(TEST_RESOURCE, emqx_connector_demo).
-define(ID, <<"id">>).
-define(ID1, <<"id1">>).
-define(DEFAULT_RESOURCE_GROUP, <<"default">>).
-define(RESOURCE_ERROR(REASON), {error, {resource_error, #{reason := REASON}}}).
-define(TRACE_OPTS, #{timetrap => 10000, timeout => 1000}).
-define(TELEMETRY_PREFIX, emqx, resource).
-define(tpal(MSG), begin
    ct:pal(MSG),
    ?tp(notice, MSG, #{})
end).

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(_, Config) ->
    ct:timetrap({seconds, 30}),
    emqx_connector_demo:set_callback_mode(always_sync),
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_, _Config) ->
    snabbkaffe:stop(),
    _ = emqx_resource:remove_local(?ID),
    emqx_common_test_helpers:call_janitor(),
    ok.

init_per_suite(Config) ->
    code:ensure_loaded(?TEST_RESOURCE),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_resource
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = proplists:get_value(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_list_types(_) ->
    ?assert(lists:member(?TEST_RESOURCE, emqx_resource:list_types())).

t_check_config(_) ->
    {ok, #{}} = emqx_resource:check_config(?TEST_RESOURCE, bin_config()),
    {ok, #{}} = emqx_resource:check_config(?TEST_RESOURCE, config()),

    {error, _} = emqx_resource:check_config(?TEST_RESOURCE, <<"not a config">>),
    {error, _} = emqx_resource:check_config(?TEST_RESOURCE, #{invalid => config}).

t_create_remove(_) ->
    ?check_trace(
        begin
            ?assertMatch(
                {error, _},
                emqx_resource:check_and_create_local(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{unknown => test_resource}
                )
            ),

            ?assertMatch(
                {ok, _},
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => test_resource}
                )
            ),

            ?assertMatch(
                {ok, _},
                emqx_resource:recreate_local(
                    ?ID,
                    ?TEST_RESOURCE,
                    #{name => test_resource},
                    #{}
                )
            ),

            {ok, #{pid := Pid}} = emqx_resource:query(?ID, get_state),

            ?assert(is_process_alive(Pid)),

            ?assertEqual(ok, emqx_resource:remove_local(?ID)),
            ?assertMatch(ok, emqx_resource:remove_local(?ID)),

            ?assertNot(is_process_alive(Pid))
        end,
        [log_consistency_prop()]
    ).

t_create_remove_local(_) ->
    ?check_trace(
        begin
            ?assertMatch(
                {error, _},
                emqx_resource:check_and_create_local(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{unknown => test_resource}
                )
            ),

            ?assertMatch(
                {ok, _},
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => test_resource}
                )
            ),

            emqx_resource:recreate_local(
                ?ID,
                ?TEST_RESOURCE,
                #{name => test_resource},
                #{}
            ),

            {ok, #{pid := Pid}} = emqx_resource:query(?ID, get_state),

            ?assert(is_process_alive(Pid)),

            emqx_resource:set_resource_status_connecting(?ID),

            emqx_resource:recreate_local(
                ?ID,
                ?TEST_RESOURCE,
                #{name => test_resource},
                #{}
            ),

            ?assertEqual(ok, emqx_resource:remove_local(?ID)),
            ?assertMatch(ok, emqx_resource:remove_local(?ID)),

            ?assertMatch(
                {error, not_found},
                emqx_resource:query(?ID, get_state)
            ),

            ?assertNot(is_process_alive(Pid))
        end,
        [log_consistency_prop()]
    ).

t_do_not_start_after_created(_) ->
    ?check_trace(
        begin
            ?assertMatch(
                {ok, _},
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => test_resource},
                    #{start_after_created => false}
                )
            ),
            %% the resource should remain `disconnected` after created
            timer:sleep(200),
            ?assertMatch(
                ?RESOURCE_ERROR(stopped),
                emqx_resource:query(?ID, get_state)
            ),
            ?assertMatch(
                {ok, _, #{status := stopped}},
                emqx_resource:get_instance(?ID)
            ),

            %% start the resource manually..
            ?assertEqual(ok, emqx_resource:start(?ID)),
            {ok, #{pid := Pid}} = emqx_resource:query(?ID, get_state),
            ?assert(is_process_alive(Pid)),

            %% restart the resource
            ?assertEqual(ok, emqx_resource:restart(?ID)),
            ?assertNot(is_process_alive(Pid)),
            {ok, #{pid := Pid2}} = emqx_resource:query(?ID, get_state),
            ?assert(is_process_alive(Pid2)),

            ?assertEqual(ok, emqx_resource:remove_local(?ID)),

            ?assertNot(is_process_alive(Pid2))
        end,
        [log_consistency_prop()]
    ).

t_query(_) ->
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
    ),

    {ok, #{pid := _}} = emqx_resource:query(?ID, get_state),

    ?assertMatch(
        {error, not_found},
        emqx_resource:query(<<"unknown">>, get_state)
    ),

    ok = emqx_resource:remove_local(?ID).

t_query_counter(_) ->
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true}
    ),

    {ok, 0} = emqx_resource:query(?ID, get_counter),
    ok = emqx_resource:query(?ID, {inc_counter, 1}),
    {ok, 1} = emqx_resource:query(?ID, get_counter),
    ok = emqx_resource:query(?ID, {inc_counter, 5}),
    {ok, 6} = emqx_resource:query(?ID, get_counter),

    ok = emqx_resource:remove_local(?ID).

t_batch_query_counter(_) ->
    BatchSize = 100,
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true},
        #{
            batch_size => BatchSize,
            batch_time => 100,
            query_mode => sync
        }
    ),

    ?check_trace(
        ?TRACE_OPTS,
        emqx_resource:query(?ID, get_counter),
        fun(Result, Trace) ->
            ?assertMatch({ok, 0}, Result),
            QueryTrace = ?of_kind(call_batch_query, Trace),
            ?assertMatch([#{batch := [?QUERY(_, get_counter, _, _, _, _)]}], QueryTrace)
        end
    ),
    NMsgs = 1_000,
    ?check_trace(
        ?TRACE_OPTS,
        begin
            NEvents = round(math:ceil(NMsgs / BatchSize)),
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := connector_demo_inc_counter}),
                NEvents,
                _Timeout = 10_000
            ),
            inc_counter_in_parallel(NMsgs),
            {ok, _} = snabbkaffe:receive_events(SRef),
            ok
        end,
        fun(Trace) ->
            QueryTrace = [
                Event
             || Event = #{
                    ?snk_kind := call_batch_query,
                    batch := BatchReq
                } <- Trace,
                length(BatchReq) > 1
            ],
            ?assertMatch([_ | _], QueryTrace)
        end
    ),
    {ok, NMsgs} = emqx_resource:query(?ID, get_counter),

    ok = emqx_resource:remove_local(?ID).

t_query_counter_async_query(_) ->
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true},
        #{
            query_mode => async,
            batch_size => 1,
            metrics_flush_interval => 50
        }
    ),
    ?assertMatch({ok, 0}, emqx_resource:simple_sync_query(?ID, get_counter)),
    NMsgs = 1_000,
    ?check_trace(
        ?TRACE_OPTS,
        begin
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := connector_demo_inc_counter}),
                NMsgs,
                _Timeout = 60_000
            ),
            inc_counter_in_parallel(NMsgs),
            {ok, _} = snabbkaffe:receive_events(SRef),
            ok
        end,
        fun(Trace) ->
            %% the callback_mode of 'emqx_connector_demo' is 'always_sync'.
            QueryTrace = ?of_kind(call_query, Trace),
            ?assertMatch([#{query := ?QUERY(_, {inc_counter, 1}, _, _, _, _)} | _], QueryTrace)
        end
    ),
    %% simple query ignores the query_mode and batching settings in the resource_worker
    ?check_trace(
        ?TRACE_OPTS,
        emqx_resource:simple_sync_query(?ID, get_counter),
        fun(Result, Trace) ->
            ?assertMatch({ok, 1000}, Result),
            %% the callback_mode if 'emqx_connector_demo' is 'always_sync'.
            QueryTrace = ?of_kind(call_query, Trace),
            ?assertMatch([#{query := ?QUERY(_, get_counter, _, _, _, _)}], QueryTrace)
        end
    ),
    #{counters := C} = emqx_resource:get_metrics(?ID),
    ?retry(
        _Sleep = 300,
        _Attempts0 = 20,
        ?assertMatch(#{matched := 1002, 'success' := 1002, 'failed' := 0}, C)
    ),
    ok = emqx_resource:remove_local(?ID).

t_query_counter_async_callback(_) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),

    Tab0 = ets:new(?FUNCTION_NAME, [bag, public]),
    Insert = fun(Tab, Result) ->
        ets:insert(Tab, {make_ref(), Result})
    end,
    ReqOpts = #{async_reply_fun => {Insert, [Tab0]}},
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true},
        #{
            query_mode => async,
            batch_size => 1,
            inflight_window => 1000000
        }
    ),
    ?assertMatch({ok, 0}, emqx_resource:simple_sync_query(?ID, get_counter)),
    NMsgs = 1_000,
    ?check_trace(
        ?TRACE_OPTS,
        begin
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
                NMsgs,
                _Timeout = 60_000
            ),
            inc_counter_in_parallel(NMsgs, ReqOpts),
            {ok, _} = snabbkaffe:receive_events(SRef),
            ok
        end,
        fun(Trace) ->
            QueryTrace = ?of_kind(call_query_async, Trace),
            ?assertMatch([#{query := ?QUERY(_, {inc_counter, 1}, _, _, _, _)} | _], QueryTrace)
        end
    ),

    %% simple query ignores the query_mode and batching settings in the resource_worker
    ?check_trace(
        ?TRACE_OPTS,
        emqx_resource:simple_sync_query(?ID, get_counter),
        fun(Result, Trace) ->
            ?assertMatch({ok, 1000}, Result),
            QueryTrace = ?of_kind(call_query, Trace),
            ?assertMatch([#{query := ?QUERY(_, get_counter, _, _, _, _)}], QueryTrace)
        end
    ),
    #{counters := C} = emqx_resource:get_metrics(?ID),
    ?assertMatch(#{matched := 1002, 'success' := 1002, 'failed' := 0}, C),
    ?assertMatch(1000, ets:info(Tab0, size)),
    ?assert(
        lists:all(
            fun
                ({_, ok}) -> true;
                (_) -> false
            end,
            ets:tab2list(Tab0)
        )
    ),
    ok = emqx_resource:remove_local(?ID).

t_query_counter_async_inflight(_) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),
    MetricsTab = ets:new(metrics_tab, [ordered_set, public]),
    ok = telemetry:attach_many(
        ?FUNCTION_NAME,
        emqx_resource_metrics:events(),
        fun(Event, Measurements, Meta, _Config) ->
            ets:insert(
                MetricsTab,
                {erlang:monotonic_time(), #{
                    event => Event, measurements => Measurements, metadata => Meta
                }}
            ),
            ok
        end,
        unused_config
    ),
    on_exit(fun() -> telemetry:detach(?FUNCTION_NAME) end),

    Tab0 = ets:new(?FUNCTION_NAME, [bag, public]),
    Insert0 = fun(Tab, Ref, Result) ->
        ct:pal("inserting ~p", [{Ref, Result}]),
        ets:insert(Tab, {Ref, Result})
    end,
    ReqOpts = fun() -> #{async_reply_fun => {Insert0, [Tab0, make_ref()]}} end,
    WindowSize = 15,
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true},
        #{
            query_mode => async,
            batch_size => 1,
            inflight_window => WindowSize,
            worker_pool_size => 1,
            resume_interval => 300
        }
    ),
    ?assertMatch({ok, 0}, emqx_resource:simple_sync_query(?ID, get_counter)),

    %% block the resource
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, block)),

    %% send async query to make the inflight window full
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                %% one more so that inflight would be already full upon last query
                inc_counter_in_parallel(WindowSize + 1, ReqOpts),
                #{?snk_kind := buffer_worker_flush_but_inflight_full},
                1_000
            ),
        fun(Trace) ->
            QueryTrace = ?of_kind(call_query_async, Trace),
            ?assertMatch([#{query := ?QUERY(_, {inc_counter, 1}, _, _, _, _)} | _], QueryTrace)
        end
    ),
    tap_metrics(?LINE),
    ?assertMatch(0, ets:info(Tab0, size)),

    tap_metrics(?LINE),
    %% send query now will fail because the resource is blocked.
    Insert = fun(Tab, Ref, Result) ->
        ct:pal("inserting ~p", [{Ref, Result}]),
        ets:insert(Tab, {Ref, Result}),
        ?tp(tmp_query_inserted, #{})
    end,
    %% since this counts as a failure, it'll be enqueued and retried
    %% later, when the resource is unblocked.
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_resource:query(?ID, {inc_counter, 99}, #{
                async_reply_fun => {Insert, [Tab0, tmp_query]}
            }),
            #{?snk_kind := buffer_worker_appended_to_queue},
            1_000
        ),
    tap_metrics(?LINE),

    %% all responses should be received after the resource is resumed.
    {ok, SRef0} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
        %% +2 because the tmp_query above will be retried and succeed
        %% this time.
        WindowSize + 2,
        _Timeout0 = 10_000
    ),
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, resume)),
    tap_metrics(?LINE),
    {ok, _} = snabbkaffe:receive_events(SRef0),
    tap_metrics(?LINE),
    %% since the previous tmp_query was enqueued to be retried, we
    %% take it again from the table; this time, it should have
    %% succeeded.
    ?assertMatch([{tmp_query, ok}], ets:take(Tab0, tmp_query)),

    %% send async query, this time everything should be ok.
    Num = 10,
    ?check_trace(
        begin
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
                Num,
                _Timeout0 = 10_000
            ),
            inc_counter_in_parallel_increasing(Num, 1, ReqOpts),
            {ok, _} = snabbkaffe:receive_events(SRef),
            ok
        end,
        fun(Trace) ->
            QueryTrace = ?of_kind(call_query_async, Trace),
            ?assertMatch([#{query := ?QUERY(_, {inc_counter, _}, _, _, _, _)} | _], QueryTrace),
            ?assertEqual(WindowSize + Num + 1, ets:info(Tab0, size), #{tab => ets:tab2list(Tab0)}),
            tap_metrics(?LINE),
            ok
        end
    ),

    %% block the resource
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, block)),
    %% again, send async query to make the inflight window full
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                %% one more so that inflight would be already full upon last query
                inc_counter_in_parallel(WindowSize + 1, ReqOpts),
                #{?snk_kind := buffer_worker_flush_but_inflight_full},
                1_000
            ),
        fun(Trace) ->
            QueryTrace = ?of_kind(call_query_async, Trace),
            ?assertMatch([#{query := ?QUERY(_, {inc_counter, 1}, _, _, _, _)} | _], QueryTrace)
        end
    ),

    %% this will block the resource_worker
    ok = emqx_resource:query(?ID, {inc_counter, 4}),

    Sent = WindowSize + 1 + Num + WindowSize + 1,
    {ok, SRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
        WindowSize + 1,
        _Timeout0 = 10_000
    ),
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, resume)),
    {ok, _} = snabbkaffe:receive_events(SRef1),
    ?assertEqual(Sent, ets:info(Tab0, size), #{tab => ets:tab2list(Tab0)}),
    tap_metrics(?LINE),

    {ok, Counter} = emqx_resource:simple_sync_query(?ID, get_counter),
    ct:pal("get_counter: ~p, sent: ~p", [Counter, Sent]),
    ?assert(Sent =< Counter),

    %% give the metrics some time to stabilize.
    ct:sleep(1000),
    #{counters := C, gauges := G} = tap_metrics(?LINE),
    ?assertMatch(
        #{
            counters :=
                #{matched := M, success := Ss, dropped := Dp},
            gauges := #{queuing := Qing, inflight := Infl}
        } when
            M == Ss + Dp + Qing + Infl,
        #{counters => C, gauges => G},
        #{
            metrics => #{counters => C, gauges => G},
            results => ets:tab2list(Tab0),
            metrics_trace => ets:tab2list(MetricsTab)
        }
    ),
    ?assert(
        lists:all(
            fun
                ({_, ok}) -> true;
                (_) -> false
            end,
            ets:tab2list(Tab0)
        )
    ),
    ok = emqx_resource:remove_local(?ID).

t_query_counter_async_inflight_batch(_) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),
    MetricsTab = ets:new(metrics_tab, [ordered_set, public]),
    ok = telemetry:attach_many(
        ?FUNCTION_NAME,
        emqx_resource_metrics:events(),
        fun(Event, Measurements, Meta, _Config) ->
            ets:insert(
                MetricsTab,
                {erlang:monotonic_time(), #{
                    event => Event, measurements => Measurements, metadata => Meta
                }}
            ),
            ok
        end,
        unused_config
    ),
    on_exit(fun() -> telemetry:detach(?FUNCTION_NAME) end),

    Tab0 = ets:new(?FUNCTION_NAME, [bag, public]),
    Insert0 = fun(Tab, Ref, Result) ->
        ct:pal("inserting ~p", [{Ref, Result}]),
        ets:insert(Tab, {Ref, Result})
    end,
    ReqOpts = fun() -> #{async_reply_fun => {Insert0, [Tab0, make_ref()]}} end,
    BatchSize = 2,
    WindowSize = 15,
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true},
        #{
            query_mode => async,
            batch_size => BatchSize,
            batch_time => 100,
            inflight_window => WindowSize,
            worker_pool_size => 1,
            resume_interval => 300
        }
    ),
    ?assertMatch({ok, 0}, emqx_resource:simple_sync_query(?ID, get_counter)),

    %% block the resource
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, block)),

    %% send async query to make the inflight window full
    NumMsgs = BatchSize * WindowSize,
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                %% a batch more so that inflight would be already full upon last query
                inc_counter_in_parallel(NumMsgs + BatchSize, ReqOpts),
                #{?snk_kind := buffer_worker_flush_but_inflight_full},
                5_000
            ),
        fun(Trace) ->
            QueryTrace = [
                Event
             || Event = #{
                    ?snk_kind := call_batch_query_async,
                    batch := [
                        ?QUERY(_, {inc_counter, 1}, _, _, _, _),
                        ?QUERY(_, {inc_counter, 1}, _, _, _, _)
                    ]
                } <-
                    Trace
            ],
            ?assertMatch([_ | _], QueryTrace)
        end
    ),
    tap_metrics(?LINE),

    Sent1 = NumMsgs + BatchSize,

    ?check_trace(
        begin
            %% this will block the resource_worker as the inflight window is full now
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_resource:query(?ID, {inc_counter, 2}, ReqOpts()),
                    #{?snk_kind := buffer_worker_flush_but_inflight_full},
                    5_000
                ),
            ?assertMatch(0, ets:info(Tab0, size)),
            ok
        end,
        []
    ),

    Sent2 = Sent1 + 1,

    tap_metrics(?LINE),
    %% send query now will fail because the resource is blocked.
    Insert = fun(Tab, Ref, Result) ->
        ct:pal("inserting ~p", [{Ref, Result}]),
        ets:insert(Tab, {Ref, Result}),
        ?tp(tmp_query_inserted, #{})
    end,
    %% since this counts as a failure, it'll be enqueued and retried
    %% later, when the resource is unblocked.
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_resource:query(?ID, {inc_counter, 3}, #{
                async_reply_fun => {Insert, [Tab0, tmp_query]}
            }),
            #{?snk_kind := buffer_worker_appended_to_queue},
            1_000
        ),
    tap_metrics(?LINE),

    %% all responses should be received after the resource is resumed.
    {ok, SRef0} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
        %% +2 because the tmp_query above will be retried and succeed
        %% this time.
        WindowSize + 2,
        5_000
    ),
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, resume)),
    tap_metrics(?LINE),
    {ok, _} = snabbkaffe:receive_events(SRef0),
    %% since the previous tmp_query was enqueued to be retried, we
    %% take it again from the table; this time, it should have
    %% succeeded.
    ?assertEqual([{tmp_query, ok}], ets:take(Tab0, tmp_query)),
    ?assertEqual(Sent2, ets:info(Tab0, size), #{tab => ets:tab2list(Tab0)}),
    tap_metrics(?LINE),

    %% send async query, this time everything should be ok.
    NumBatches1 = 3,
    NumMsgs1 = BatchSize * NumBatches1,
    ?check_trace(
        ?TRACE_OPTS,
        begin
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
                NumBatches1,
                5_000
            ),
            inc_counter_in_parallel(NumMsgs1, ReqOpts),
            {ok, _} = snabbkaffe:receive_events(SRef),
            ok
        end,
        fun(Trace) ->
            QueryTrace = ?of_kind(call_batch_query_async, Trace),
            ?assertMatch(
                [#{batch := [?QUERY(_, {inc_counter, _}, _, _, _, _) | _]} | _],
                QueryTrace
            )
        end
    ),

    Sent3 = Sent2 + NumMsgs1,

    ?assertEqual(Sent3, ets:info(Tab0, size), #{tab => ets:tab2list(Tab0)}),
    tap_metrics(?LINE),

    %% block the resource
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, block)),
    %% again, send async query to make the inflight window full
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                %% a batch more so that inflight would be already full upon last query
                inc_counter_in_parallel(NumMsgs + BatchSize, ReqOpts),
                #{?snk_kind := buffer_worker_flush_but_inflight_full},
                5_000
            ),
        fun(Trace) ->
            QueryTrace = ?of_kind(call_batch_query_async, Trace),
            ?assertMatch(
                [#{batch := [?QUERY(_, {inc_counter, _}, _, _, _, _) | _]} | _],
                QueryTrace
            )
        end
    ),

    Sent4 = Sent3 + NumMsgs + BatchSize,

    %% this will block the resource_worker
    ok = emqx_resource:query(?ID, {inc_counter, 1}),

    {ok, SRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
        WindowSize + 1,
        5_000
    ),
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, resume)),
    {ok, _} = snabbkaffe:receive_events(SRef1),
    ?assertEqual(Sent4, ets:info(Tab0, size), #{tab => ets:tab2list(Tab0)}),

    {ok, Counter} = emqx_resource:simple_sync_query(?ID, get_counter),
    ct:pal("get_counter: ~p, sent: ~p", [Counter, Sent4]),
    ?assert(Sent4 =< Counter),

    %% give the metrics some time to stabilize.
    ct:sleep(1000),
    #{counters := C, gauges := G} = tap_metrics(?LINE),
    ?assertMatch(
        #{
            counters :=
                #{matched := M, success := Ss, dropped := Dp},
            gauges := #{queuing := Qing, inflight := Infl}
        } when
            M == Ss + Dp + Qing + Infl,
        #{counters => C, gauges => G},
        #{
            metrics => #{counters => C, gauges => G},
            results => ets:tab2list(Tab0),
            metrics_trace => ets:tab2list(MetricsTab)
        }
    ),
    ?assert(
        lists:all(
            fun
                ({_, ok}) -> true;
                (_) -> false
            end,
            ets:tab2list(Tab0)
        )
    ),
    ok = emqx_resource:remove_local(?ID).

t_healthy_timeout(_) ->
    ?check_trace(
        begin
            ?assertMatch(
                {ok, _},
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => <<"bad_not_atom_name">>, register => true},
                    %% the ?TEST_RESOURCE always returns the `Mod:on_get_status/2` 300ms later.
                    #{health_check_interval => 200}
                )
            ),
            ?assertMatch(
                {error, {resource_error, #{reason := timeout}}},
                emqx_resource:query(?ID, get_state, #{timeout => 1_000})
            ),
            ?assertMatch(
                {ok, _Group, #{status := disconnected}}, emqx_resource_manager:lookup(?ID)
            ),
            ?assertEqual(ok, emqx_resource:remove_local(?ID))
        end,
        [log_consistency_prop()]
    ).

t_healthy(_) ->
    ?check_trace(
        #{timetrap => 10_000},
        begin
            ?assertMatch(
                {ok, _},
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => test_resource}
                )
            ),
            ct:pal("getting state"),
            {ok, #{pid := Pid}} = emqx_resource:query(?ID, get_state),
            timer:sleep(300),
            ct:pal("setting state as `connecting`"),
            emqx_resource:set_resource_status_connecting(?ID),

            ct:pal("health check"),
            ?assertEqual({ok, connected}, emqx_resource:health_check(?ID)),
            ?assertMatch(
                [#{status := connected}],
                emqx_resource:list_instances_verbose()
            ),

            erlang:exit(Pid, shutdown),

            ?assertEqual({ok, disconnected}, emqx_resource:health_check(?ID)),

            ?assertMatch(
                [#{status := disconnected}],
                emqx_resource:list_instances_verbose()
            ),

            ?assertEqual(ok, emqx_resource:remove_local(?ID))
        end,
        [log_consistency_prop()]
    ).

t_unhealthy_target(_) ->
    HealthCheckError = {unhealthy_target, "some message"},
    ?assertMatch(
        {ok, _},
        create(
            ?ID,
            ?DEFAULT_RESOURCE_GROUP,
            ?TEST_RESOURCE,
            #{name => test_resource, health_check_error => {msg, HealthCheckError}}
        )
    ),
    ?assertEqual(
        {ok, disconnected},
        emqx_resource:health_check(?ID)
    ),
    ?assertMatch(
        {ok, _Group, #{error := HealthCheckError}},
        emqx_resource_manager:lookup(?ID)
    ),
    %% messages are dropped when bridge is unhealthy
    lists:foreach(
        fun(_) ->
            ?assertMatch(
                {error, {resource_error, #{reason := unhealthy_target}}},
                emqx_resource:query(?ID, message)
            )
        end,
        lists:seq(1, 3)
    ),
    ?assertEqual(3, emqx_resource_metrics:matched_get(?ID)),
    ?assertEqual(3, emqx_resource_metrics:dropped_resource_stopped_get(?ID)).

t_stop_start(_) ->
    ?check_trace(
        begin
            ?assertMatch(
                {error, _},
                emqx_resource:check_and_create_local(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{unknown => test_resource}
                )
            ),

            ?assertMatch(
                {ok, _},
                emqx_resource:check_and_create_local(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{<<"name">> => <<"test_resource">>}
                )
            ),

            %% add some metrics to test their persistence
            WorkerID0 = <<"worker:0">>,
            WorkerID1 = <<"worker:1">>,
            emqx_resource_metrics:inflight_set(?ID, WorkerID0, 2),
            emqx_resource_metrics:inflight_set(?ID, WorkerID1, 3),
            ?assertEqual(5, emqx_resource_metrics:inflight_get(?ID)),

            ?assertMatch(
                {ok, _},
                emqx_resource:check_and_recreate_local(
                    ?ID,
                    ?TEST_RESOURCE,
                    #{<<"name">> => <<"test_resource">>},
                    #{}
                )
            ),

            {ok, #{pid := Pid0}} = emqx_resource:query(?ID, get_state),

            ?assert(is_process_alive(Pid0)),

            %% metrics are reset when recreating
            %% depending on timing, might show the request we just did.
            ct:sleep(500),
            ?assertEqual(0, emqx_resource_metrics:inflight_get(?ID)),

            ok = emqx_resource:stop(?ID),

            ?assertNot(is_process_alive(Pid0)),

            ?assertMatch(
                ?RESOURCE_ERROR(stopped),
                emqx_resource:query(?ID, get_state)
            ),

            ?assertEqual(ok, emqx_resource:restart(?ID)),
            timer:sleep(300),

            {ok, #{pid := Pid1}} = emqx_resource:query(?ID, get_state),

            ?assert(is_process_alive(Pid1)),

            %% now stop while resetting the metrics
            ct:sleep(500),
            emqx_resource_metrics:inflight_set(?ID, WorkerID0, 1),
            emqx_resource_metrics:inflight_set(?ID, WorkerID1, 4),
            ?assertEqual(5, emqx_resource_metrics:inflight_get(?ID)),
            ?assertEqual(ok, emqx_resource:stop(?ID)),
            ?assertEqual(0, emqx_resource_metrics:inflight_get(?ID))
        end,
        [log_consistency_prop()]
    ).

t_stop_start_local(_) ->
    ?check_trace(
        begin
            ?assertMatch(
                {error, _},
                emqx_resource:check_and_create_local(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{unknown => test_resource}
                )
            ),

            ?assertMatch(
                {ok, _},
                emqx_resource:check_and_create_local(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{<<"name">> => <<"test_resource">>}
                )
            ),

            ?assertMatch(
                {ok, _},
                emqx_resource:check_and_recreate_local(
                    ?ID,
                    ?TEST_RESOURCE,
                    #{<<"name">> => <<"test_resource">>},
                    #{}
                )
            ),

            {ok, #{pid := Pid0}} = emqx_resource:query(?ID, get_state),

            ?assert(is_process_alive(Pid0)),

            ?assertEqual(ok, emqx_resource:stop(?ID)),

            ?assertNot(is_process_alive(Pid0)),

            ?assertMatch(
                ?RESOURCE_ERROR(stopped),
                emqx_resource:query(?ID, get_state)
            ),

            ?assertEqual(ok, emqx_resource:restart(?ID)),

            {ok, #{pid := Pid1}} = emqx_resource:query(?ID, get_state),

            ?assert(is_process_alive(Pid1))
        end,
        [log_consistency_prop()]
    ).

t_list_filter(_) ->
    {ok, _} = create(
        emqx_resource:generate_id(<<"a">>),
        <<"group1">>,
        ?TEST_RESOURCE,
        #{name => a}
    ),
    {ok, _} = create(
        emqx_resource:generate_id(<<"a">>),
        <<"group2">>,
        ?TEST_RESOURCE,
        #{name => grouped_a}
    ),

    [Id1] = emqx_resource:list_group_instances(<<"group1">>),
    ?assertMatch(
        {ok, <<"group1">>, #{config := #{name := a}}},
        emqx_resource:get_instance(Id1)
    ),

    [Id2] = emqx_resource:list_group_instances(<<"group2">>),
    ?assertMatch(
        {ok, <<"group2">>, #{config := #{name := grouped_a}}},
        emqx_resource:get_instance(Id2)
    ).

t_create_dry_run_local(_) ->
    lists:foreach(
        fun(_) ->
            create_dry_run_local_succ()
        end,
        lists:seq(1, 10)
    ),
    ?retry(
        100,
        5,
        ?assertEqual(
            [],
            emqx_resource:list_instances_verbose()
        )
    ).

create_dry_run_local_succ() ->
    ?assertEqual(
        ok,
        emqx_resource:create_dry_run_local(
            ?TEST_RESOURCE,
            #{name => test_resource, register => true}
        )
    ),
    ?assertEqual(undefined, whereis(test_resource)).

t_create_dry_run_local_failed(_) ->
    ct:timetrap({seconds, 120}),
    emqx_utils:nolink_apply(fun() ->
        ct:pal("creating with creation error"),
        Res1 = emqx_resource:create_dry_run_local(
            ?TEST_RESOURCE,
            #{create_error => true}
        ),
        ?assertMatch({error, _}, Res1),

        ct:pal("creating with health check error"),
        Res2 = emqx_resource:create_dry_run_local(
            ?TEST_RESOURCE,
            #{name => test_resource, health_check_error => true}
        ),
        ?assertMatch({error, _}, Res2),

        ct:pal("creating with stop error"),
        Res3 = emqx_resource:create_dry_run_local(
            ?TEST_RESOURCE,
            #{name => test_resource, stop_error => true}
        ),
        ?assertEqual(ok, Res3),
        ok
    end),
    ?retry(
        100,
        50,
        ?assertEqual(
            [],
            emqx_resource:list_instances_verbose()
        )
    ).

t_test_func(_) ->
    IsErrorMsgPlainString = fun({error, Msg}) -> io_lib:printable_list(Msg) end,
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:not_empty("not_empty"), [<<"someval">>])),
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:min(int, 3), [4])),
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:max(array, 10), [[a, b, c, d]])),
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:max(string, 10), ["less10"])),
    ?assertEqual(
        true, IsErrorMsgPlainString(erlang:apply(emqx_resource_validator:min(int, 66), [42]))
    ),
    ?assertEqual(
        true, IsErrorMsgPlainString(erlang:apply(emqx_resource_validator:max(int, 42), [66]))
    ),
    ?assertEqual(
        true, IsErrorMsgPlainString(erlang:apply(emqx_resource_validator:min(array, 3), [[1, 2]]))
    ),
    ?assertEqual(
        true,
        IsErrorMsgPlainString(erlang:apply(emqx_resource_validator:max(array, 3), [[1, 2, 3, 4]]))
    ),
    ?assertEqual(
        true, IsErrorMsgPlainString(erlang:apply(emqx_resource_validator:min(string, 3), ["1"]))
    ),
    ?assertEqual(
        true, IsErrorMsgPlainString(erlang:apply(emqx_resource_validator:max(string, 3), ["1234"]))
    ),
    NestedMsg = io_lib:format("The answer: ~p", [42]),
    ExpectedMsg = "The answer: 42",
    BinMsg = <<"The answer: 42">>,
    MapMsg = #{question => "The question", answer => 42},
    ?assertEqual(
        {error, ExpectedMsg},
        erlang:apply(emqx_resource_validator:not_empty(NestedMsg), [""])
    ),
    ?assertEqual(
        {error, ExpectedMsg},
        erlang:apply(emqx_resource_validator:not_empty(NestedMsg), [<<>>])
    ),
    ?assertEqual(
        {error, ExpectedMsg},
        erlang:apply(emqx_resource_validator:not_empty(NestedMsg), [undefined])
    ),
    ?assertEqual(
        {error, ExpectedMsg},
        erlang:apply(emqx_resource_validator:not_empty(NestedMsg), [undefined])
    ),
    ?assertEqual(
        {error, BinMsg},
        erlang:apply(emqx_resource_validator:not_empty(BinMsg), [undefined])
    ),
    ?assertEqual(
        {error, MapMsg},
        erlang:apply(emqx_resource_validator:not_empty(MapMsg), [""])
    ).

t_reset_metrics(_) ->
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
    ),

    {ok, #{pid := Pid}} = emqx_resource:query(?ID, get_state),
    emqx_resource:reset_metrics(?ID),
    ?assert(is_process_alive(Pid)),
    ok = emqx_resource:remove_local(?ID),
    ?assertNot(is_process_alive(Pid)).

t_auto_retry(_) ->
    {Res, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, create_error => true},
        #{health_check_interval => 100}
    ),
    ?assertEqual(ok, Res).

%% tests resources that have an asynchronous start: they are created
%% without problems, but later some issue is found when calling the
%% health check.
t_start_throw_error(_Config) ->
    Message = "something went wrong",
    ?assertMatch(
        {{ok, _}, {ok, _}},
        ?wait_async_action(
            create(
                ?ID,
                ?DEFAULT_RESOURCE_GROUP,
                ?TEST_RESOURCE,
                #{name => test_resource, health_check_error => {msg, Message}},
                #{health_check_interval => 100}
            ),
            #{?snk_kind := connector_demo_health_check_error},
            1_000
        )
    ),
    %% Now, if we try to "reconnect" (restart) it, we should get the error
    ?assertMatch({error, Message}, emqx_resource:start(?ID, _Opts = #{})),
    ok.

t_health_check_disconnected(_) ->
    ?check_trace(
        begin
            _ = create(
                ?ID,
                ?DEFAULT_RESOURCE_GROUP,
                ?TEST_RESOURCE,
                #{name => test_resource, create_error => true},
                #{health_check_interval => 100}
            ),
            ?assertEqual(
                {ok, disconnected},
                emqx_resource:health_check(?ID)
            )
        end,
        [log_consistency_prop()]
    ).

t_unblock_only_required_buffer_workers(_) ->
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 5,
            metrics_flush_interval => 50,
            batch_time => 100
        }
    ),
    lists:foreach(
        fun emqx_resource_buffer_worker:block/1,
        emqx_resource_buffer_worker_sup:worker_pids(?ID)
    ),
    create(
        ?ID1,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 5,
            batch_time => 100
        }
    ),
    %% creation of `?ID1` should not have unblocked `?ID`'s buffer workers
    %% so we should see resumes now (`buffer_worker_enter_running`).
    ?check_trace(
        ?wait_async_action(
            lists:foreach(
                fun emqx_resource_buffer_worker:resume/1,
                emqx_resource_buffer_worker_sup:worker_pids(?ID)
            ),
            #{?snk_kind := buffer_worker_enter_running},
            5000
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{id := ?ID} | _],
                ?of_kind(buffer_worker_enter_running, Trace)
            )
        end
    ).

t_retry_batch(_Config) ->
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 5,
            batch_time => 100,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => 1_000
        }
    ),

    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, block)),
    Matched0 = emqx_resource_metrics:matched_get(?ID),
    ?assertEqual(1, Matched0),

    %% these requests will batch together and fail; the buffer worker
    %% will enter the `blocked' state and they'll be retried later,
    %% after it unblocks.
    Payloads = lists:seq(1, 5),
    NumPayloads = length(Payloads),
    ExpectedCount = 15,

    ?check_trace(
        begin
            {ok, {ok, _}} =
                ?wait_async_action(
                    lists:foreach(
                        fun(N) ->
                            ok = emqx_resource:query(?ID, {inc_counter, N})
                        end,
                        Payloads
                    ),
                    #{?snk_kind := buffer_worker_enter_blocked},
                    5_000
                ),
            %% now the individual messages should have been counted
            Matched1 = emqx_resource_metrics:matched_get(?ID),
            ?assertEqual(Matched0 + NumPayloads, Matched1),

            %% wait for two more retries while the failure is enabled; the
            %% batch shall remain enqueued.
            {ok, _} =
                snabbkaffe:block_until(
                    ?match_n_events(2, #{?snk_kind := buffer_worker_retry_inflight_failed}),
                    5_000
                ),
            %% should not have increased the matched count with the retries
            Matched2 = emqx_resource_metrics:matched_get(?ID),
            ?assertEqual(Matched1, Matched2),

            %% now unblock the buffer worker so it may retry the batch,
            %% but it'll still fail
            {ok, {ok, _}} =
                ?wait_async_action(
                    ok = emqx_resource:simple_sync_query(?ID, resume),
                    #{?snk_kind := buffer_worker_retry_inflight_succeeded},
                    5_000
                ),
            %% 1 more because of the `resume' call
            Matched3 = emqx_resource_metrics:matched_get(?ID),
            ?assertEqual(Matched2 + 1, Matched3),

            {ok, Counter} = emqx_resource:simple_sync_query(?ID, get_counter),
            {Counter, Matched3}
        end,
        fun({Counter, Matched3}, Trace) ->
            %% 1 original attempt + 2 failed retries + final
            %% successful attempt.
            %% each time should be the original batch (no duplicate
            %% elements or reordering).
            ExpectedSeenPayloads = lists:flatten(lists:duplicate(4, Payloads)),
            Trace1 = lists:sublist(
                ?projection(n, ?of_kind(connector_demo_batch_inc_individual, Trace)),
                length(ExpectedSeenPayloads)
            ),
            ?assertEqual(ExpectedSeenPayloads, Trace1),
            ?assertMatch(
                [#{n := ExpectedCount}],
                ?of_kind(connector_demo_inc_counter, Trace)
            ),
            ?assertEqual(ExpectedCount, Counter),
            %% matched should count only the original requests, and not retries
            %% + 1 for `resume' call
            %% + 1 for `block' call
            %% + 1 for `get_counter' call
            %% and the message count (1 time)
            Matched4 = emqx_resource_metrics:matched_get(?ID),
            ?assertEqual(Matched3 + 1, Matched4),
            ok
        end
    ),
    ok.

t_delete_and_re_create_with_same_name(_Config) ->
    NumBufferWorkers = 2,
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 1,
            worker_pool_size => NumBufferWorkers,
            buffer_mode => volatile_offload,
            buffer_seg_bytes => 100,
            metrics_flush_interval => 50,
            resume_interval => 1_000
        }
    ),
    %% pre-condition: we should have just created a new queue
    Queuing0 = emqx_resource_metrics:queuing_get(?ID),
    QueuingBytes0 = emqx_resource_metrics:queuing_bytes_get(?ID),
    Inflight0 = emqx_resource_metrics:inflight_get(?ID),
    ?assertEqual(0, Queuing0),
    ?assertEqual(0, QueuingBytes0),
    ?assertEqual(0, Inflight0),
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, block)),
            NumRequests = 10,
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := buffer_worker_enter_blocked}),
                NumBufferWorkers,
                _Timeout = 5_000
            ),
            %% ensure replayq offloads to disk
            NumBytes = 119,
            Payload = binary:copy(<<"a">>, NumBytes),
            lists:foreach(
                fun(N) ->
                    spawn_link(fun() ->
                        {error, _} =
                            emqx_resource:query(
                                ?ID,
                                {big_payload, <<(integer_to_binary(N))/binary, Payload/binary>>}
                            )
                    end)
                end,
                lists:seq(1, NumRequests)
            ),

            {ok, _} = snabbkaffe:receive_events(SRef),

            %% ensure that stuff got enqueued into disk
            tap_metrics(?LINE),
            ?retry(
                _Sleep = 300,
                _Attempts0 = 20,
                ?assert(emqx_resource_metrics:queuing_get(?ID) > 0)
            ),
            ?retry(
                _Sleep = 300,
                _Attempts0 = 20,
                %% `> NumBytes' because replayq reports total usage, not just payload, so
                %% headers and metadata are included.
                ?assert(emqx_resource_metrics:queuing_bytes_get(?ID) > NumBytes)
            ),
            ?retry(
                _Sleep = 300,
                _Attempts0 = 20,
                ?assertEqual(2, emqx_resource_metrics:inflight_get(?ID))
            ),

            %% now, we delete the resource
            process_flag(trap_exit, true),
            ok = emqx_resource:remove_local(?ID),
            ?assertEqual({error, not_found}, emqx_resource_manager:lookup(?ID)),

            %% re-create the resource with the *same name*
            {{ok, _}, {ok, _Events}} =
                ?wait_async_action(
                    create(
                        ?ID,
                        ?DEFAULT_RESOURCE_GROUP,
                        ?TEST_RESOURCE,
                        #{name => test_resource},
                        #{
                            query_mode => async,
                            batch_size => 1,
                            worker_pool_size => 2,
                            buffer_seg_bytes => 100,
                            resume_interval => 1_000
                        }
                    ),
                    #{?snk_kind := buffer_worker_enter_running},
                    5_000
                ),

            %% it shouldn't have anything enqueued, as it's a fresh resource
            Queuing2 = emqx_resource_metrics:queuing_get(?ID),
            QueuingBytes2 = emqx_resource_metrics:queuing_bytes_get(?ID),
            Inflight2 = emqx_resource_metrics:inflight_get(?ID),
            ?assertEqual(0, Queuing2),
            ?assertEqual(0, QueuingBytes2),
            ?assertEqual(0, Inflight2),

            ok
        end,
        []
    ),
    ok.

%% check that, if we configure a max queue size too small, then we
%% never send requests and always overflow.
t_always_overflow(_Config) ->
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 1,
            worker_pool_size => 1,
            max_buffer_bytes => 1,
            metrics_flush_interval => 50,
            resume_interval => 1_000
        }
    ),
    ?check_trace(
        begin
            Payload = binary:copy(<<"a">>, 100),
            %% since it's sync and it should never send a request, this
            %% errors with `timeout'.
            ?assertEqual(
                {error, buffer_overflow},
                emqx_resource:query(
                    ?ID,
                    {big_payload, Payload},
                    #{timeout => 500}
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(call_query_enter, Trace)),
            ok
        end
    ),
    ok.

t_retry_sync_inflight(_Config) ->
    ResumeInterval = 1_000,
    emqx_connector_demo:set_callback_mode(always_sync),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 1,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => ResumeInterval
        }
    ),
    QueryOpts = #{},
    ?check_trace(
        begin
            %% now really make the resource go into `blocked' state.
            %% this results in a retriable error when sync.
            ok = emqx_resource:simple_sync_query(?ID, block),
            TestPid = self(),
            {_, {ok, _}} =
                ?wait_async_action(
                    spawn_link(fun() ->
                        Res = emqx_resource:query(?ID, {big_payload, <<"a">>}, QueryOpts),
                        TestPid ! {res, Res}
                    end),
                    #{?snk_kind := buffer_worker_retry_inflight_failed},
                    ResumeInterval * 2
                ),
            {ok, {ok, _}} =
                ?wait_async_action(
                    ok = emqx_resource:simple_sync_query(?ID, resume),
                    #{?snk_kind := buffer_worker_retry_inflight_succeeded},
                    ResumeInterval * 3
                ),
            receive
                {res, Res} ->
                    ?assertEqual(ok, Res)
            after 5_000 ->
                ct:fail("no response")
            end,
            ok
        end,
        [fun ?MODULE:assert_sync_retry_fail_then_succeed_inflight/1]
    ),
    ok.

t_retry_sync_inflight_batch(_Config) ->
    ResumeInterval = 1_000,
    emqx_connector_demo:set_callback_mode(always_sync),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 2,
            batch_time => 200,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => ResumeInterval
        }
    ),
    QueryOpts = #{},
    ?check_trace(
        begin
            %% make the resource go into `blocked' state.  this
            %% results in a retriable error when sync.
            ok = emqx_resource:simple_sync_query(?ID, block),
            process_flag(trap_exit, true),
            TestPid = self(),
            {_, {ok, _}} =
                ?wait_async_action(
                    spawn_link(fun() ->
                        Res = emqx_resource:query(?ID, {big_payload, <<"a">>}, QueryOpts),
                        TestPid ! {res, Res}
                    end),
                    #{?snk_kind := buffer_worker_retry_inflight_failed},
                    ResumeInterval * 2
                ),
            {ok, {ok, _}} =
                ?wait_async_action(
                    ok = emqx_resource:simple_sync_query(?ID, resume),
                    #{?snk_kind := buffer_worker_retry_inflight_succeeded},
                    ResumeInterval * 3
                ),
            receive
                {res, Res} ->
                    ?assertEqual(ok, Res)
            after 5_000 ->
                ct:fail("no response")
            end,
            ok
        end,
        [fun ?MODULE:assert_sync_retry_fail_then_succeed_inflight/1]
    ),
    ok.

t_retry_async_inflight(_Config) ->
    ResumeInterval = 1_000,
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 1,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => ResumeInterval
        }
    ),
    QueryOpts = #{},
    ?check_trace(
        begin
            %% block
            ok = emqx_resource:simple_sync_query(?ID, block),

            %% then send an async request; that should be retriable.
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_resource:query(?ID, {big_payload, <<"b">>}, QueryOpts),
                    #{?snk_kind := buffer_worker_retry_inflight_failed},
                    ResumeInterval * 2
                ),

            %% will reply with success after the resource is healed
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_resource:simple_sync_query(?ID, resume),
                    #{?snk_kind := buffer_worker_enter_running},
                    ResumeInterval * 2
                ),
            ok
        end,
        [fun ?MODULE:assert_async_retry_fail_then_succeed_inflight/1]
    ),
    ok.

t_retry_async_inflight_full(_Config) ->
    ResumeInterval = 1_000,
    AsyncInflightWindow = 5,
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => ?FUNCTION_NAME},
        #{
            query_mode => async,
            inflight_window => AsyncInflightWindow,
            batch_size => 1,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => ResumeInterval
        }
    ),
    ?check_trace(
        #{timetrap => 15_000},
        begin
            %% block
            ok = emqx_resource:simple_sync_query(?ID, block),

            {ok, {ok, _}} =
                ?wait_async_action(
                    inc_counter_in_parallel(
                        AsyncInflightWindow * 2,
                        fun() ->
                            For = (ResumeInterval div 4) + rand:uniform(ResumeInterval div 4),
                            {sleep_before_reply, For}
                        end,
                        #{async_reply_fun => {fun(Res) -> ct:pal("Res = ~p", [Res]) end, []}}
                    ),
                    #{?snk_kind := buffer_worker_flush_but_inflight_full},
                    ResumeInterval * 2
                ),

            %% will reply with success after the resource is healed
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_resource:simple_sync_query(?ID, resume),
                    #{?snk_kind := buffer_worker_enter_running}
                ),
            ok
        end,
        [
            fun(Trace) ->
                ?assertMatch([#{} | _], ?of_kind(buffer_worker_flush_but_inflight_full, Trace))
            end
        ]
    ),
    ?retry(
        _Sleep = 300,
        _Attempts0 = 20,
        ?assertEqual(0, emqx_resource_metrics:inflight_get(?ID))
    ),
    ok.

%% this test case is to ensure the buffer worker will not go crazy even
%% if the underlying connector is misbehaving: evaluate async callbacks multiple times
t_async_reply_multi_eval(_Config) ->
    ResumeInterval = 5,
    TotalTime = 5_000,
    AsyncInflightWindow = 3,
    TotalQueries = AsyncInflightWindow * 5,
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => ?FUNCTION_NAME},
        #{
            query_mode => async,
            inflight_window => AsyncInflightWindow,
            batch_size => 3,
            batch_time => 10,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => ResumeInterval
        }
    ),
    %% block
    ok = emqx_resource:simple_sync_query(?ID, block),
    inc_counter_in_parallel(
        TotalQueries,
        fun() ->
            Rand = rand:uniform(1000),
            {random_reply, Rand}
        end,
        #{}
    ),
    ?retry(
        2 * ResumeInterval,
        TotalTime div ResumeInterval,
        begin
            Metrics = tap_metrics(?LINE),
            #{
                counters := Counters,
                gauges := #{queuing := 0, inflight := 0}
            } = Metrics,
            #{
                matched := Matched,
                success := Success,
                dropped := Dropped,
                late_reply := LateReply,
                failed := Failed
            } = Counters,
            ?assertEqual(TotalQueries, Matched - 1),
            ?assertEqual(Matched, Success + Dropped + LateReply + Failed, #{counters => Counters})
        end
    ).

t_retry_async_inflight_batch(_Config) ->
    ResumeInterval = 1_000,
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 2,
            batch_time => 200,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => ResumeInterval
        }
    ),
    QueryOpts = #{},
    ?check_trace(
        begin
            %% block
            ok = emqx_resource:simple_sync_query(?ID, block),

            %% then send an async request; that should be retriable.
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_resource:query(?ID, {big_payload, <<"b">>}, QueryOpts),
                    #{?snk_kind := buffer_worker_retry_inflight_failed},
                    ResumeInterval * 2
                ),

            %% will reply with success after the resource is healed
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_resource:simple_sync_query(?ID, resume),
                    #{?snk_kind := buffer_worker_enter_running},
                    ResumeInterval * 2
                ),
            ok
        end,
        [fun ?MODULE:assert_async_retry_fail_then_succeed_inflight/1]
    ),
    ok.

%% check that we monitor async worker pids and abort their inflight
%% requests if they die.
t_async_pool_worker_death(_Config) ->
    ResumeInterval = 1_000,
    NumBufferWorkers = 2,
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 1,
            worker_pool_size => NumBufferWorkers,
            metrics_refresh_interval => 50,
            resume_interval => ResumeInterval
        }
    ),
    Tab0 = ets:new(?FUNCTION_NAME, [bag, public]),
    Insert0 = fun(Tab, Ref, Result) ->
        ct:pal("inserting ~p", [{Ref, Result}]),
        ets:insert(Tab, {Ref, Result})
    end,
    ReqOpts = fun() -> #{async_reply_fun => {Insert0, [Tab0, make_ref()]}} end,
    ?check_trace(
        begin
            ok = emqx_resource:simple_sync_query(?ID, block),

            NumReqs = 10,
            {ok, SRef0} =
                snabbkaffe:subscribe(
                    ?match_event(#{?snk_kind := buffer_worker_appended_to_inflight}),
                    NumReqs,
                    1_000
                ),
            inc_counter_in_parallel_increasing(NumReqs, 1, ReqOpts),
            {ok, _} = snabbkaffe:receive_events(SRef0),

            ?retry(
                _Sleep = 300,
                _Attempts0 = 20,
                ?assertEqual(NumReqs, emqx_resource_metrics:inflight_get(?ID))
            ),

            %% grab one of the worker pids and kill it
            {ok, #{pid := Pid0}} = emqx_resource:simple_sync_query(?ID, get_state),
            MRef = monitor(process, Pid0),
            ct:pal("will kill ~p", [Pid0]),
            exit(Pid0, kill),
            receive
                {'DOWN', MRef, process, Pid0, killed} ->
                    ct:pal("~p killed", [Pid0]),
                    ok
            after 200 ->
                ct:fail("worker should have died")
            end,

            %% inflight requests should have been marked as retriable
            wait_until_all_marked_as_retriable(NumReqs),
            Inflight1 = emqx_resource_metrics:inflight_get(?ID),
            ?assertEqual(NumReqs, Inflight1),

            NumReqs
        end,
        fun(NumReqs, Trace) ->
            Events = ?of_kind(buffer_worker_async_agent_down, Trace),
            %% At least one buffer worker should have marked its
            %% requests as retriable.  If a single one has
            %% received all requests, that's all we got.
            ?assertMatch([_ | _], Events),
            %% All requests distributed over all buffer workers
            %% should have been marked as retriable, by the time
            %% the inflight has been drained.
            ?assertEqual(
                NumReqs,
                lists:sum([N || #{num_affected := N} <- Events])
            ),

            %% The `DOWN' signal must trigger the transition to the `blocked' state,
            %% otherwise the request won't be retried until the buffer worker is `blocked'
            %% for other reasons.
            ?assert(
                ?strict_causality(
                    #{?snk_kind := buffer_worker_async_agent_down, buffer_worker := _Pid0},
                    #{?snk_kind := buffer_worker_enter_blocked, buffer_worker := _Pid1},
                    _Pid0 =:= _Pid1,
                    Trace
                )
            ),
            ok
        end
    ),
    ok.

t_expiration_sync_before_sending(_Config) ->
    emqx_connector_demo:set_callback_mode(always_sync),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 1,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => 1_000
        }
    ),
    do_t_expiration_before_sending(sync).

t_expiration_sync_batch_before_sending(_Config) ->
    emqx_connector_demo:set_callback_mode(always_sync),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 2,
            batch_time => 100,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => 1_000
        }
    ),
    do_t_expiration_before_sending(sync).

t_expiration_async_before_sending(_Config) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 1,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => 1_000
        }
    ),
    do_t_expiration_before_sending(async).

t_expiration_async_batch_before_sending(_Config) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 2,
            batch_time => 100,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => 1_000
        }
    ),
    do_t_expiration_before_sending(async).

do_t_expiration_before_sending(QueryMode) ->
    ?check_trace(
        begin
            ok = emqx_resource:simple_sync_query(?ID, block),

            ?force_ordering(
                #{?snk_kind := buffer_worker_flush_before_pop},
                #{?snk_kind := delay_enter}
            ),
            ?force_ordering(
                #{?snk_kind := delay},
                #{?snk_kind := buffer_worker_flush_before_sieve_expired}
            ),

            TimeoutMS = 100,
            spawn_link(fun() ->
                case QueryMode of
                    sync ->
                        ?assertMatch(
                            {error, {resource_error, #{reason := timeout}}},
                            emqx_resource:query(?ID, {inc_counter, 99}, #{timeout => TimeoutMS})
                        );
                    async ->
                        ?assertEqual(
                            ok, emqx_resource:query(?ID, {inc_counter, 99}, #{timeout => TimeoutMS})
                        )
                end
            end),
            spawn_link(fun() ->
                ?tp(delay_enter, #{}),
                ct:sleep(2 * TimeoutMS),
                ?tp(delay, #{}),
                ok
            end),

            {ok, _} = ?block_until(#{?snk_kind := buffer_worker_flush_all_expired}, 4 * TimeoutMS),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{batch := [?QUERY(_, {inc_counter, 99}, _, _, _, _)]}],
                ?of_kind(buffer_worker_flush_all_expired, Trace)
            ),
            Metrics = tap_metrics(?LINE),
            ?assertMatch(
                #{
                    counters := #{
                        matched := 2,
                        %% the block call
                        success := 1,
                        dropped := 1,
                        'dropped.expired' := 1,
                        retried := 0,
                        failed := 0
                    }
                },
                Metrics
            ),
            ok
        end
    ),
    ok.

t_expiration_sync_before_sending_partial_batch(_Config) ->
    emqx_connector_demo:set_callback_mode(always_sync),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 2,
            batch_time => 100,
            worker_pool_size => 1,
            metrics_flush_interval => 250,
            resume_interval => 1_000
        }
    ),
    install_telemetry_handler(?FUNCTION_NAME),
    do_t_expiration_before_sending_partial_batch(sync).

t_expiration_async_before_sending_partial_batch(_Config) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 2,
            batch_time => 100,
            worker_pool_size => 1,
            metrics_flush_interval => 250,
            resume_interval => 1_000
        }
    ),
    install_telemetry_handler(?FUNCTION_NAME),
    do_t_expiration_before_sending_partial_batch(async).

do_t_expiration_before_sending_partial_batch(QueryMode) ->
    ?check_trace(
        begin
            ok = emqx_resource:simple_sync_query(?ID, block),

            ?force_ordering(
                #{?snk_kind := buffer_worker_flush_before_pop},
                #{?snk_kind := delay_enter}
            ),
            ?force_ordering(
                #{?snk_kind := delay},
                #{?snk_kind := buffer_worker_flush_before_sieve_expired}
            ),

            Pid0 =
                spawn_link(fun() ->
                    ?assertEqual(
                        ok, emqx_resource:query(?ID, {inc_counter, 99}, #{timeout => infinity})
                    ),
                    ?tp(infinity_query_returned, #{})
                end),
            TimeoutMS = 100,
            Pid1 =
                spawn_link(fun() ->
                    case QueryMode of
                        sync ->
                            ?assertMatch(
                                {error, {resource_error, #{reason := timeout}}},
                                emqx_resource:query(?ID, {inc_counter, 199}, #{timeout => TimeoutMS})
                            );
                        async ->
                            ?assertEqual(
                                ok,
                                emqx_resource:query(?ID, {inc_counter, 199}, #{timeout => TimeoutMS})
                            )
                    end
                end),
            Pid2 =
                spawn_link(fun() ->
                    ?tp(delay_enter, #{}),
                    ct:sleep(2 * TimeoutMS),
                    ?tp(delay, #{}),
                    ok
                end),

            {ok, _} = ?block_until(
                #{?snk_kind := buffer_worker_flush_potentially_partial}, 4 * TimeoutMS
            ),
            ok = emqx_resource:simple_sync_query(?ID, resume),
            case QueryMode of
                async ->
                    {ok, _} = ?block_until(
                        #{
                            ?snk_kind := handle_async_reply,
                            action := ack,
                            batch_or_query := [?QUERY(_, {inc_counter, 99}, _, _, _, _)]
                        },
                        10 * TimeoutMS
                    );
                sync ->
                    %% more time because it needs to retry if sync
                    {ok, _} = ?block_until(#{?snk_kind := infinity_query_returned}, 20 * TimeoutMS)
            end,

            lists:foreach(
                fun(Pid) ->
                    unlink(Pid),
                    exit(Pid, kill)
                end,
                [Pid0, Pid1, Pid2]
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        expired := [?QUERY(_, {inc_counter, 199}, _, _, _, _)],
                        not_expired := [?QUERY(_, {inc_counter, 99}, _, _, _, _)]
                    }
                ],
                ?of_kind(buffer_worker_flush_potentially_partial, Trace)
            ),
            wait_until_gauge_is(
                inflight,
                #{
                    expected_value => 0,
                    timeout => 500,
                    max_events => 10
                }
            ),
            Metrics = tap_metrics(?LINE),
            case QueryMode of
                async ->
                    ?assertMatch(
                        #{
                            counters := #{
                                matched := 4,
                                %% the block call, the request with
                                %% infinity timeout, and the resume
                                %% call.
                                success := 3,
                                dropped := 1,
                                'dropped.expired' := 1,
                                %% was sent successfully and held by
                                %% the test connector.
                                retried := 0,
                                failed := 0
                            }
                        },
                        Metrics
                    );
                sync ->
                    ?assertMatch(
                        #{
                            counters := #{
                                matched := 4,
                                %% the block call, the request with
                                %% infinity timeout, and the resume
                                %% call.
                                success := 3,
                                dropped := 1,
                                'dropped.expired' := 1,
                                %% currently, the test connector
                                %% replies with an error that may make
                                %% the buffer worker retry.
                                retried := Retried,
                                failed := 0
                            }
                        } when Retried =< 1,
                        Metrics
                    )
            end,
            ok
        end
    ),
    ok.

t_expiration_async_after_reply(_Config) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 1,
            worker_pool_size => 1,
            resume_interval => 1_000
        }
    ),
    install_telemetry_handler(?FUNCTION_NAME),
    do_t_expiration_async_after_reply(single).

t_expiration_async_batch_after_reply(_Config) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 3,
            batch_time => 100,
            worker_pool_size => 1,
            resume_interval => 2_000
        }
    ),
    install_telemetry_handler(?FUNCTION_NAME),
    do_t_expiration_async_after_reply(batch).

do_t_expiration_async_after_reply(IsBatch) ->
    ?check_trace(
        begin
            NAcks =
                case IsBatch of
                    batch -> 1;
                    single -> 3
                end,
            ?force_ordering(
                #{?snk_kind := buffer_worker_flush_ack},
                NAcks,
                #{?snk_kind := delay_enter},
                _Guard = true
            ),
            ?force_ordering(
                #{?snk_kind := delay},
                #{
                    ?snk_kind := handle_async_reply_enter,
                    batch_or_query := [?QUERY(_, {inc_counter, 199}, _, _, _, _) | _]
                }
            ),

            TimeoutMS = 100,
            ?assertEqual(
                ok,
                emqx_resource:query(?ID, {inc_counter, 199}, #{timeout => TimeoutMS})
            ),
            ?assertEqual(
                ok,
                emqx_resource:query(?ID, {inc_counter, 299}, #{timeout => TimeoutMS})
            ),
            ?assertEqual(
                ok, emqx_resource:query(?ID, {inc_counter, 99}, #{timeout => infinity})
            ),
            Pid0 =
                spawn_link(fun() ->
                    ?tp(delay_enter, #{}),
                    ct:sleep(2 * TimeoutMS),
                    ?tp(delay, #{}),
                    ok
                end),

            {ok, _} = ?block_until(
                #{?snk_kind := buffer_worker_flush_potentially_partial}, 4 * TimeoutMS
            ),
            {ok, _} = ?block_until(
                #{?snk_kind := handle_async_reply_expired}, 10 * TimeoutMS
            ),
            wait_telemetry_event(success, #{n_events => 1, timeout => 4_000}),

            unlink(Pid0),
            exit(Pid0, kill),
            ok
        end,
        fun(Trace) ->
            case IsBatch of
                batch ->
                    ?assertMatch(
                        [
                            #{
                                expired := [
                                    ?QUERY(_, {inc_counter, 199}, _, _, _, _),
                                    ?QUERY(_, {inc_counter, 299}, _, _, _, _)
                                ]
                            }
                        ],
                        ?of_kind(handle_async_reply_expired, Trace)
                    ),
                    ?assertMatch(
                        [
                            #{
                                inflight_count := 1,
                                num_inflight_messages := 1
                            }
                        ],
                        ?of_kind(handle_async_reply_partially_expired, Trace)
                    );
                single ->
                    ?assertMatch(
                        [
                            #{expired := [?QUERY(_, {inc_counter, 199}, _, _, _, _)]},
                            #{expired := [?QUERY(_, {inc_counter, 299}, _, _, _, _)]}
                        ],
                        ?of_kind(handle_async_reply_expired, Trace)
                    )
            end,
            Metrics = tap_metrics(?LINE),
            ?assertMatch(
                #{
                    counters := #{
                        matched := 3,
                        %% the request with infinity timeout.
                        success := 1,
                        dropped := 0,
                        late_reply := 2,
                        retried := 0,
                        failed := 0
                    }
                },
                Metrics
            ),
            ok
        end
    ),
    ok.

t_expiration_batch_all_expired_after_reply(_Config) ->
    ResumeInterval = 300,
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 3,
            batch_time => 100,
            worker_pool_size => 1,
            resume_interval => ResumeInterval
        }
    ),
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := buffer_worker_flush_ack},
                #{?snk_kind := delay_enter}
            ),
            ?force_ordering(
                #{?snk_kind := delay},
                #{
                    ?snk_kind := handle_async_reply_enter,
                    batch_or_query := [?QUERY(_, {inc_counter, 199}, _, _, _, _) | _]
                }
            ),

            TimeoutMS = 200,
            ?assertEqual(
                ok,
                emqx_resource:query(?ID, {inc_counter, 199}, #{timeout => TimeoutMS})
            ),
            ?assertEqual(
                ok,
                emqx_resource:query(?ID, {inc_counter, 299}, #{timeout => TimeoutMS})
            ),
            Pid0 =
                spawn_link(fun() ->
                    ?tp(delay_enter, #{}),
                    ct:sleep(2 * TimeoutMS),
                    ?tp(delay, #{}),
                    ok
                end),

            {ok, _} = ?block_until(
                #{?snk_kind := handle_async_reply_expired}, 10 * TimeoutMS
            ),

            unlink(Pid0),
            exit(Pid0, kill),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        expired := [
                            ?QUERY(_, {inc_counter, 199}, _, _, _, _),
                            ?QUERY(_, {inc_counter, 299}, _, _, _, _)
                        ]
                    }
                ],
                ?of_kind(handle_async_reply_expired, Trace)
            ),
            Metrics = tap_metrics(?LINE),
            ?assertMatch(
                #{
                    counters := #{
                        matched := 2,
                        success := 0,
                        dropped := 0,
                        late_reply := 2,
                        retried := 0,
                        failed := 0
                    },
                    gauges := #{
                        inflight := 0,
                        queuing := 0
                    }
                },
                Metrics
            ),
            ok
        end
    ),
    ok.

t_expiration_retry(_Config) ->
    emqx_connector_demo:set_callback_mode(always_sync),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 1,
            worker_pool_size => 1,
            resume_interval => 300
        }
    ),
    do_t_expiration_retry(#{is_batch => false}).

t_expiration_retry_batch(_Config) ->
    emqx_connector_demo:set_callback_mode(always_sync),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 2,
            batch_time => 100,
            worker_pool_size => 1,
            resume_interval => 300
        }
    ),
    do_t_expiration_retry(#{is_batch => true}).

do_t_expiration_retry(Context) ->
    IsBatch = maps:get(is_batch, Context),
    ResumeInterval = 300,
    ?check_trace(
        #{timetrap => 10_000},
        begin
            ok = emqx_resource:simple_sync_query(?ID, block),

            TimeoutMS = 200,
            %% the request that expires must be first, so it's the
            %% head of the inflight table (and retriable).
            {ok, SRef1} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := buffer_worker_appended_to_queue}),
                1,
                ResumeInterval * 2
            ),
            spawn_link(fun() ->
                ?assertMatch(
                    {error, {resource_error, #{reason := timeout}}},
                    emqx_resource:query(
                        ?ID,
                        {inc_counter, 1},
                        #{timeout => TimeoutMS}
                    )
                )
            end),
            %% This second message must be enqueued while the resource is blocked by the
            %% previous message.
            Pid1 =
                spawn_link(fun() ->
                    receive
                        go -> ok
                    end,
                    ?assertEqual(
                        ok,
                        emqx_resource:query(
                            ?ID,
                            {inc_counter, 2},
                            #{timeout => infinity}
                        )
                    )
                end),
            ?tp("waiting for first message to be appended to the queue", #{}),
            {ok, _} = snabbkaffe:receive_events(SRef1),

            ?tp("waiting for first message to expire during blocked retries", #{}),
            {ok, _} = ?block_until(#{?snk_kind := buffer_worker_retry_expired}),

            %% Now we wait until the worker tries the second message at least once before
            %% unblocking it.
            Pid1 ! go,
            ?tp("waiting for second message to be retried and be nacked while blocked", #{}),
            case IsBatch of
                false ->
                    {ok, _} = ?block_until(#{
                        ?snk_kind := buffer_worker_flush_nack,
                        batch_or_query := ?QUERY(_, {inc_counter, 2}, _, _, _, _)
                    });
                true ->
                    {ok, _} = ?block_until(#{
                        ?snk_kind := buffer_worker_flush_nack,
                        batch_or_query := [?QUERY(_, {inc_counter, 2}, _, _, _, _) | _]
                    })
            end,

            %% Bypass the buffer worker and unblock the resource.
            ok = emqx_resource:simple_sync_query(?ID, resume),
            ?tp("waiting for second message to be retried and be acked, unblocking", #{}),
            {ok, _} = ?block_until(#{?snk_kind := buffer_worker_retry_inflight_succeeded}),

            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{expired := [?QUERY(_, {inc_counter, 1}, _, _, _, _)]}],
                ?of_kind(buffer_worker_retry_expired, Trace)
            ),
            Metrics = tap_metrics(?LINE),
            ?assertMatch(
                #{
                    gauges := #{
                        inflight := 0,
                        queuing := 0
                    }
                },
                Metrics
            ),
            ok
        end
    ),
    ok.

t_expiration_retry_batch_multiple_times(_Config) ->
    ResumeInterval = 300,
    emqx_connector_demo:set_callback_mode(always_sync),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 2,
            batch_time => 100,
            worker_pool_size => 1,
            resume_interval => ResumeInterval
        }
    ),
    ?check_trace(
        begin
            ok = emqx_resource:simple_sync_query(?ID, block),

            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := buffer_worker_flush_nack}),
                1,
                200
            ),
            TimeoutMS = 100,
            spawn_link(fun() ->
                ?assertMatch(
                    {error, {resource_error, #{reason := timeout}}},
                    emqx_resource:query(
                        ?ID,
                        {inc_counter, 1},
                        #{timeout => TimeoutMS}
                    )
                )
            end),
            spawn_link(fun() ->
                ?assertMatch(
                    {error, {resource_error, #{reason := timeout}}},
                    emqx_resource:query(
                        ?ID,
                        {inc_counter, 2},
                        #{timeout => ResumeInterval + TimeoutMS}
                    )
                )
            end),
            {ok, _} = snabbkaffe:receive_events(SRef),

            {ok, _} =
                snabbkaffe:block_until(
                    ?match_n_events(2, #{?snk_kind := buffer_worker_retry_expired}),
                    ResumeInterval * 10
                ),

            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{expired := [?QUERY(_, {inc_counter, 1}, _, _, _, _)]},
                    #{expired := [?QUERY(_, {inc_counter, 2}, _, _, _, _)]}
                ],
                ?of_kind(buffer_worker_retry_expired, Trace)
            ),
            ok
        end
    ),
    ok.

t_batch_individual_reply_sync(_Config) ->
    ResumeInterval = 300,
    emqx_connector_demo:set_callback_mode(always_sync),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 5,
            batch_time => 100,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => ResumeInterval
        }
    ),
    do_t_batch_individual_reply().

t_batch_individual_reply_async(_Config) ->
    ResumeInterval = 300,
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => sync,
            batch_size => 5,
            batch_time => 100,
            worker_pool_size => 1,
            metrics_flush_interval => 50,
            resume_interval => ResumeInterval
        }
    ),
    on_exit(fun() -> emqx_resource:remove_local(?ID) end),
    do_t_batch_individual_reply().

do_t_batch_individual_reply() ->
    ?check_trace(
        begin
            {Results, {ok, _}} =
                ?wait_async_action(
                    emqx_utils:pmap(
                        fun(N) ->
                            emqx_resource:query(?ID, {individual_reply, N rem 2 =:= 0})
                        end,
                        lists:seq(1, 5)
                    ),
                    #{?snk_kind := buffer_worker_flush_ack, batch_or_query := [_, _ | _]},
                    5_000
                ),

            Ok = ok,
            Error = {error, {unrecoverable_error, bad_request}},
            ?assertEqual([Error, Ok, Error, Ok, Error], Results),

            ?retry(
                200,
                10,
                ?assertMatch(
                    #{
                        counters := #{
                            matched := 5,
                            failed := 3,
                            success := 2
                        }
                    },
                    tap_metrics(?LINE)
                )
            ),

            ok
        end,
        []
    ),
    ok.

t_recursive_flush(_Config) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 1,
            worker_pool_size => 1
        }
    ),
    do_t_recursive_flush().

t_recursive_flush_batch(_Config) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 2,
            batch_time => 10_000,
            worker_pool_size => 1
        }
    ),
    do_t_recursive_flush().

do_t_recursive_flush() ->
    ?check_trace(
        begin
            Timeout = 1_000,
            Pid = spawn_link(fun S() ->
                emqx_resource:query(?ID, {inc_counter, 1}),
                S()
            end),
            %% we want two reflushes to happen before we analyze the
            %% trace, so that we get a single full interaction
            {ok, _} = snabbkaffe:block_until(
                ?match_n_events(2, #{?snk_kind := buffer_worker_flush_ack_reflush}), Timeout
            ),
            unlink(Pid),
            exit(Pid, kill),
            ok
        end,
        fun(Trace) ->
            %% check that a recursive flush leads to a new call to flush/1
            Pairs = ?find_pairs(
                #{?snk_kind := buffer_worker_flush_ack_reflush},
                #{?snk_kind := buffer_worker_flush},
                Trace
            ),
            ?assert(lists:any(fun(E) -> E end, [true || {pair, _, _} <- Pairs]))
        end
    ),
    ok.

t_call_mode_uncoupled_from_query_mode(_Config) ->
    DefaultOpts = #{
        batch_size => 1,
        batch_time => 5,
        worker_pool_size => 1
    },
    ?check_trace(
        begin
            %% We check that we can call the buffer workers with async
            %% calls, even if the underlying connector itself only
            %% supports sync calls.
            emqx_connector_demo:set_callback_mode(always_sync),
            {ok, _} = create(
                ?ID,
                ?DEFAULT_RESOURCE_GROUP,
                ?TEST_RESOURCE,
                #{name => test_resource},
                DefaultOpts#{query_mode => async}
            ),
            ?tp_span(
                async_query_sync_driver,
                #{},
                ?assertMatch(
                    {ok, {ok, _}},
                    ?wait_async_action(
                        emqx_resource:query(?ID, {inc_counter, 1}),
                        #{?snk_kind := buffer_worker_flush_ack},
                        500
                    )
                )
            ),
            ?assertEqual(ok, emqx_resource:remove_local(?ID)),

            %% And we check the converse: a connector that allows async
            %% calls can be called synchronously, but the underlying
            %% call should be async.
            emqx_connector_demo:set_callback_mode(async_if_possible),
            {ok, _} = create(
                ?ID,
                ?DEFAULT_RESOURCE_GROUP,
                ?TEST_RESOURCE,
                #{name => test_resource},
                DefaultOpts#{query_mode => sync}
            ),
            ?tp_span(
                sync_query_async_driver,
                #{},
                ?assertEqual(ok, emqx_resource:query(?ID, {inc_counter, 2}))
            ),
            ?assertEqual(ok, emqx_resource:remove_local(?ID)),
            ?tp(sync_query_async_driver, #{}),
            ok
        end,
        fun(Trace0) ->
            Trace1 = trace_between_span(Trace0, async_query_sync_driver),
            ct:pal("async query calling sync driver\n  ~p", [Trace1]),
            ?assert(
                ?strict_causality(
                    #{?snk_kind := async_query, request := {inc_counter, 1}},
                    #{?snk_kind := call_query, call_mode := sync},
                    Trace1
                )
            ),

            Trace2 = trace_between_span(Trace0, sync_query_async_driver),
            ct:pal("sync query calling async driver\n  ~p", [Trace2]),
            ?assert(
                ?strict_causality(
                    #{?snk_kind := sync_query, request := {inc_counter, 2}},
                    #{?snk_kind := call_query_async},
                    Trace2
                )
            ),
            ok
        end
    ).

%% The default mode is currently `memory_only'.
t_volatile_offload_mode(_Config) ->
    MaxBufferBytes = 1_000,
    DefaultOpts = #{
        max_buffer_bytes => MaxBufferBytes,
        worker_pool_size => 1
    },
    ?check_trace(
        begin
            emqx_connector_demo:set_callback_mode(async_if_possible),
            %% Create without any specified segment bytes; should
            %% default to equal max bytes.
            ?assertMatch(
                {ok, _},
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => test_resource},
                    DefaultOpts#{buffer_mode => volatile_offload}
                )
            ),
            ?assertEqual(ok, emqx_resource:remove_local(?ID)),

            %% Create with segment bytes < max bytes
            ?assertMatch(
                {ok, _},
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => test_resource},
                    DefaultOpts#{
                        buffer_mode => volatile_offload,
                        buffer_seg_bytes => MaxBufferBytes div 2
                    }
                )
            ),
            ?assertEqual(ok, emqx_resource:remove_local(?ID)),
            %% Create with segment bytes = max bytes
            ?assertMatch(
                {ok, _},
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => test_resource},
                    DefaultOpts#{
                        buffer_mode => volatile_offload,
                        buffer_seg_bytes => MaxBufferBytes
                    }
                )
            ),
            ?assertEqual(ok, emqx_resource:remove_local(?ID)),

            %% Create with segment bytes > max bytes; should normalize
            %% to max bytes.
            ?assertMatch(
                {ok, _},
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => test_resource},
                    DefaultOpts#{
                        buffer_mode => volatile_offload,
                        buffer_seg_bytes => 2 * MaxBufferBytes
                    }
                )
            ),
            ?assertEqual(ok, emqx_resource:remove_local(?ID)),

            ok
        end,
        fun(Trace) ->
            HalfMaxBufferBytes = MaxBufferBytes div 2,
            ?assertMatch(
                [
                    #{
                        dir := _,
                        max_total_bytes := MaxTotalBytes,
                        seg_bytes := MaxTotalBytes,
                        offload := {true, volatile}
                    },
                    #{
                        dir := _,
                        max_total_bytes := MaxTotalBytes,
                        %% uses the specified value since it's smaller
                        %% than max bytes.
                        seg_bytes := HalfMaxBufferBytes,
                        offload := {true, volatile}
                    },
                    #{
                        dir := _,
                        max_total_bytes := MaxTotalBytes,
                        seg_bytes := MaxTotalBytes,
                        offload := {true, volatile}
                    },
                    #{
                        dir := _,
                        max_total_bytes := MaxTotalBytes,
                        seg_bytes := MaxTotalBytes,
                        offload := {true, volatile}
                    }
                ],
                ?projection(queue_opts, ?of_kind(buffer_worker_init, Trace))
            ),
            ok
        end
    ).

t_late_call_reply(_Config) ->
    emqx_connector_demo:set_callback_mode(always_sync),
    RequestTTL = 500,
    ?assertMatch(
        {ok, _},
        create(
            ?ID,
            ?DEFAULT_RESOURCE_GROUP,
            ?TEST_RESOURCE,
            #{name => test_resource},
            #{
                buffer_mode => memory_only,
                request_ttl => RequestTTL,
                query_mode => sync
            }
        )
    ),
    ?check_trace(
        begin
            %% Sleep for longer than the request timeout; the call reply will
            %% have been already returned (a timeout), but the resource will
            %% still send a message with the reply.
            %% The demo connector will reply with `{error, timeout}' after 1 s.
            SleepFor = RequestTTL + 500,
            ?assertMatch(
                {error, {resource_error, #{reason := timeout}}},
                emqx_resource:query(
                    ?ID,
                    {sync_sleep_before_reply, SleepFor},
                    #{timeout => RequestTTL}
                )
            ),
            %% Our process shouldn't receive any late messages.
            receive
                LateReply ->
                    ct:fail("received late reply: ~p", [LateReply])
            after SleepFor ->
                ok
            end,
            ok
        end,
        []
    ),
    ok.

t_resource_create_error_activate_alarm_once(_) ->
    do_t_resource_activate_alarm_once(
        #{name => test_resource, create_error => true},
        connector_demo_start_error
    ).

t_resource_health_check_error_activate_alarm_once(_) ->
    do_t_resource_activate_alarm_once(
        #{name => test_resource, health_check_error => true},
        connector_demo_health_check_error
    ).

do_t_resource_activate_alarm_once(ResourceConfig, SubscribeEvent) ->
    ?check_trace(
        begin
            ?wait_async_action(
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    ResourceConfig,
                    #{health_check_interval => 100}
                ),
                #{?snk_kind := resource_activate_alarm, resource_id := ?ID}
            ),
            ?assertMatch([#{activated := true, name := ?ID}], emqx_alarm:get_alarms(activated)),
            {ok, SubRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := SubscribeEvent}), 4, 7000
            ),
            ?assertMatch({ok, [_, _, _, _]}, snabbkaffe:receive_events(SubRef))
        end,
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(resource_activate_alarm, Trace))
        end
    ).

t_telemetry_handler_crash(_Config) ->
    %% Check that a crash while handling a telemetry event, such as when a busy resource
    %% is restarted and its metrics are not recreated while handling an increment, does
    %% not lead to the handler being uninstalled.
    ?check_trace(
        begin
            NonExistentId = <<"I-dont-exist">>,
            WorkerId = 1,
            HandlersBefore = telemetry:list_handlers([?TELEMETRY_PREFIX]),
            ?assertMatch([_ | _], HandlersBefore),
            lists:foreach(fun(Fn) -> Fn(NonExistentId) end, counter_metric_inc_fns()),
            emqx_common_test_helpers:with_mock(
                emqx_metrics_worker,
                set_gauge,
                fun(_Name, _Id, _WorkerId, _Metric, _Val) ->
                    error(random_crash)
                end,
                fun() ->
                    lists:foreach(
                        fun(Fn) -> Fn(NonExistentId, WorkerId, 1) end, gauge_metric_set_fns()
                    )
                end
            ),
            ?assertEqual(HandlersBefore, telemetry:list_handlers([?TELEMETRY_PREFIX])),
            ok
        end,
        []
    ),
    ok.

t_non_blocking_resource_health_check(_Config) ->
    ?check_trace(
        begin
            {ok, _} =
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => test_resource, health_check_error => {delay, 1_000}},
                    #{health_check_interval => 100}
                ),
            %% concurrently attempt to health check the resource; should do it only once
            %% for all callers
            NumCallers = 20,
            Expected = lists:duplicate(NumCallers, {ok, connected}),
            ?assertEqual(
                Expected,
                emqx_utils:pmap(
                    fun(_) -> emqx_resource:health_check(?ID) end,
                    lists:seq(1, NumCallers)
                )
            ),

            NumCallers
        end,
        [
            log_consistency_prop(),
            fun(NumCallers, Trace) ->
                %% shouldn't have one health check per caller
                SubTrace = ?of_kind(connector_demo_health_check_delay, Trace),
                ?assertMatch([_ | _], SubTrace),
                ?assert(length(SubTrace) < (NumCallers div 2), #{trace => Trace}),
                ok
            end
        ]
    ),
    ok.

t_non_blocking_channel_health_check(_Config) ->
    ?check_trace(
        begin
            {ok, _} =
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{name => test_resource, health_check_error => {delay, 500}},
                    #{health_check_interval => 100}
                ),
            ChanId = <<"chan">>,
            ok =
                emqx_resource_manager:add_channel(
                    ?ID,
                    ChanId,
                    #{health_check_delay => 500}
                ),

            %% concurrently attempt to health check the resource; should do it only once
            %% for all callers
            NumCallers = 20,
            Expected = lists:duplicate(
                NumCallers,
                #{error => undefined, status => connected}
            ),
            ?assertEqual(
                Expected,
                emqx_utils:pmap(
                    fun(_) -> emqx_resource_manager:channel_health_check(?ID, ChanId) end,
                    lists:seq(1, NumCallers)
                )
            ),

            NumCallers
        end,
        [
            log_consistency_prop(),
            fun(NumCallers, Trace) ->
                %% shouldn't have one health check per caller
                SubTrace = ?of_kind(connector_demo_channel_health_check_delay, Trace),
                ?assertMatch([_ | _], SubTrace),
                ?assert(length(SubTrace) < (NumCallers div 2), #{trace => Trace}),
                ok
            end
        ]
    ),
    ok.

%% Test that `stop' forcefully stops the resource manager even if it's stuck on a sync
%% call such as `on_start', and that the claimed resources, if any, are freed.
t_force_stop(_Config) ->
    ?check_trace(
        #{timetrap => 5_000},
        begin
            {ok, Agent} = emqx_utils_agent:start_link(not_called),
            {ok, _} =
                create(
                    ?ID,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{
                        name => test_resource,
                        create_error => {delay, 30_000, Agent}
                    },
                    #{
                        health_check_interval => 100,
                        start_timeout => 100
                    }
                ),
            ?assertEqual(ok, emqx_resource_manager:stop(?ID, _Timeout = 100)),
            ?block_until(#{?snk_kind := connector_demo_free_resources_without_state}),
            ok
        end,
        [
            log_consistency_prop(),
            fun(Trace) ->
                ?assertMatch([_ | _], ?of_kind(connector_demo_start_delay, Trace)),
                ?assertMatch(
                    [_ | _], ?of_kind("forcefully_stopping_resource_due_to_timeout", Trace)
                ),
                ?assertMatch([_ | _], ?of_kind(connector_demo_free_resources_without_state, Trace)),
                ok
            end
        ]
    ),
    ok.

%% https://emqx.atlassian.net/browse/EEC-1101
t_resource_and_channel_health_check_race(_Config) ->
    ?check_trace(
        #{timetrap => 5_000},
        begin
            %% 0) Connector and channel are initially healthy and in resource manager
            %%    state.
            AgentState0 = #{
                resource_health_check => connected,
                channel_health_check => connected
            },
            {ok, Agent} = emqx_utils_agent:start_link(AgentState0),
            ConnName = <<"cname">>,
            %% Needs to have this form to satifisfy internal, implicit requirements of
            %% `emqx_resource_cache'.
            ConnResId = <<"connector:ctype:", ConnName/binary>>,
            {ok, _} =
                create(
                    ConnResId,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{
                        name => test_resource,
                        health_check_agent => Agent
                    },
                    #{
                        health_check_interval => 100,
                        start_timeout => 100
                    }
                ),
            %% Needs to have this form to satifisfy internal, implicit requirements of
            %% `emqx_resource_cache'.
            ChanId = <<"action:atype:aname:", ConnResId/binary>>,
            ok =
                emqx_resource_manager:add_channel(
                    ConnResId,
                    ChanId,
                    #{resource_opts => #{health_check_interval => 100}}
                ),
            ?assertMatch({ok, connected}, emqx_resource:health_check(ConnResId)),
            ?assertMatch(
                #{status := connected}, emqx_resource:channel_health_check(ConnResId, ChanId)
            ),

            %% 1) Connector and channel HCs fire concurrently
            ?tpal("1) Connector and channel HCs fire concurrently"),
            Me = self(),
            _ = emqx_utils_agent:get_and_update(Agent, fun(Old) ->
                {Old, Old#{
                    resource_health_check := {ask, Me},
                    channel_health_check := [{ask, Me}, connected]
                }}
            end),
            receive
                {waiting_health_check_result, ConnHCAlias1, resource, _ConnResId1} ->
                    ?tpal("received connector hc request"),
                    ok
            end,
            receive
                {waiting_health_check_result, ChanHCAlias1, channel, _ConnResId2, _ChanId} ->
                    ?tpal("received channel hc request"),
                    ok
            end,
            %% 2) Connector HC returns `disconnected'.  This makes manager call
            %%    `on_remove_channel' for each channel, removing them from the connector
            %%    state.
            ?tpal("2) Connector HC returns `disconnected'"),
            _ = emqx_utils_agent:get_and_update(Agent, fun(Old) ->
                {Old, Old#{resource_health_check := connected}}
            end),
            {_, {ok, _}} =
                ?wait_async_action(
                    ConnHCAlias1 ! {ConnHCAlias1, disconnected},
                    #{?snk_kind := resource_disconnected_enter}
                ),
            %% 3) Channel HC returns `connected'.
            ?tpal("3) Channel HC returns `connected'"),
            ChanHCAlias1 ! {ChanHCAlias1, connected},
            %% 4) A new connector HC returns `connected'.
            ?tpal("4) A new connector HC returns `connected'"),
            ?assertMatch({ok, connected}, emqx_resource:health_check(ConnResId)),
            ?assertMatch(
                #{status := connected}, emqx_resource:channel_health_check(ConnResId, ChanId)
            ),
            %% 5) Should contain action both in connector state and in resource manager's
            %% `added_channels'.  Original bug: connector state didn't contain the channel
            %% state, but `added_channels' did.
            ?assertMatch(
                [
                    {?DEFAULT_RESOURCE_GROUP, #{
                        status := connected,
                        state := #{channels := #{ChanId := _}},
                        added_channels := #{ChanId := _}
                    }}
                ],
                emqx_resource_cache:read(ConnResId)
            ),
            ok
        end,
        []
    ),
    ok.

%% Simulates the race condition where a dry run request takes too long, and the HTTP API
%% request process is then forcefully killed before it has the chance to properly cleanup
%% and remove the dry run/probe resource.
t_dryrun_timeout_then_force_kill_during_stop(_Config) ->
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?force_ordering(
                #{?snk_kind := connector_demo_on_stop_will_delay},
                #{?snk_kind := will_kill_request}
            ),

            %% Simulates a cowboy request process.
            {ok, StartAgent} = emqx_utils_agent:start_link(not_called),
            {ok, StopAgent} = emqx_utils_agent:start_link({delay, 1_000}),
            HowToStop = fun() ->
                %% Delay only the first time, so test cleanup is faster.
                Action = emqx_utils_agent:get_and_update(StopAgent, fun
                    (continue) ->
                        {continue, continue};
                    ({delay, _} = Delay) ->
                        {Delay, continue}
                end),
                case Action of
                    {delay, Delay} ->
                        ?tp(connector_demo_on_stop_will_delay, #{}),
                        timer:sleep(Delay),
                        continue;
                    continue ->
                        continue
                end
            end,
            {Pid, MRef} = spawn_monitor(fun() ->
                Res = dryrun(
                    ?ID,
                    ?TEST_RESOURCE,
                    #{
                        name => test_resource,
                        create_error => {delay, 1_000, StartAgent},
                        stop_error => {ask, HowToStop},
                        resource_opts => #{
                            health_check_interval => 100,
                            start_timeout => 100
                        }
                    }
                ),
                exit(Res)
            end),
            on_exit(fun() -> exit(Pid, kill) end),

            %% Simulates cowboy forcefully killing the request after it takes too long and the caller
            %% has already closed the connection.
            spawn_link(fun() ->
                ?tp(will_kill_request, #{}),
                exit(Pid, kill)
            end),

            receive
                {'DOWN', MRef, process, Pid, Reason} ->
                    ct:pal("request ~p died: ~p", [Pid, Reason]),
                    ?assertEqual(killed, Reason),
                    ok
            end,

            ?block_until(#{?snk_kind := "resource_cache_cleaner_deleted_child"}),

            %% No children should be lingering
            ?assertEqual([], supervisor:which_children(emqx_resource_manager_sup)),
            %% Cache should be clean too
            ?assertEqual([], emqx_resource:list_instances()),

            ok
        end,
        []
    ),
    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

inc_counter_in_parallel(N) ->
    inc_counter_in_parallel(N, {inc_counter, 1}, #{}).

inc_counter_in_parallel(N, Opts0) ->
    inc_counter_in_parallel(N, {inc_counter, 1}, Opts0).

inc_counter_in_parallel(N, Query, Opts) ->
    Parent = self(),
    Pids = [
        erlang:spawn(fun() ->
            emqx_resource:query(?ID, maybe_apply(Query), maybe_apply(Opts)),
            Parent ! {complete, self()}
        end)
     || _ <- lists:seq(1, N)
    ],
    [
        receive
            {complete, Pid} -> ok
        after 1000 ->
            ct:fail({wait_for_query_timeout, Pid})
        end
     || Pid <- Pids
    ],
    ok.

inc_counter_in_parallel_increasing(N, StartN, Opts) ->
    Parent = self(),
    Pids = [
        erlang:spawn(fun() ->
            emqx_resource:query(?ID, {inc_counter, M}, maybe_apply(Opts)),
            Parent ! {complete, self()}
        end)
     || M <- lists:seq(StartN, StartN + N - 1)
    ],
    [
        receive
            {complete, Pid} -> ok
        after 1000 ->
            ct:fail({wait_for_query_timeout, Pid})
        end
     || Pid <- Pids
    ].

maybe_apply(FunOrTerm) ->
    maybe_apply(FunOrTerm, []).

maybe_apply(Fun, Args) when is_function(Fun) ->
    erlang:apply(Fun, Args);
maybe_apply(Term, _Args) ->
    Term.

bin_config() ->
    <<"\"name\": \"test_resource\"">>.

config() ->
    {ok, Config} = hocon:binary(bin_config()),
    Config.

tap_metrics(Line) ->
    #{counters := C, gauges := G} = emqx_resource:get_metrics(?ID),
    ct:pal("metrics (l. ~b): ~p", [Line, #{counters => C, gauges => G}]),
    #{counters => C, gauges => G}.

install_telemetry_handler(TestCase) ->
    Tid = ets:new(TestCase, [ordered_set, public]),
    HandlerId = TestCase,
    TestPid = self(),
    _ = telemetry:attach_many(
        HandlerId,
        emqx_resource_metrics:events(),
        fun(EventName, Measurements, Metadata, _Config) ->
            Data = #{
                name => EventName,
                measurements => Measurements,
                metadata => Metadata
            },
            ets:insert(Tid, {erlang:monotonic_time(), Data}),
            TestPid ! {telemetry, Data},
            ok
        end,
        unused_config
    ),
    on_exit(fun() ->
        telemetry:detach(HandlerId),
        ets:delete(Tid)
    end),
    put({?MODULE, telemetry_table}, Tid),
    Tid.

wait_until_gauge_is(
    GaugeName,
    #{
        expected_value := ExpectedValue,
        timeout := Timeout,
        max_events := MaxEvents
    }
) ->
    Events = receive_all_events(GaugeName, Timeout, MaxEvents),
    case length(Events) > 0 andalso lists:last(Events) of
        #{measurements := #{gauge_set := ExpectedValue}} ->
            ok;
        #{measurements := #{gauge_set := Value}} ->
            ct:fail(
                "gauge ~p didn't reach expected value ~p; last value: ~p",
                [GaugeName, ExpectedValue, Value]
            );
        false ->
            ct:pal("no ~p gauge events received!", [GaugeName])
    end.

receive_all_events(EventName, Timeout) ->
    receive_all_events(EventName, Timeout, _MaxEvents = 50, _Count = 0, _Acc = []).

receive_all_events(EventName, Timeout, MaxEvents) ->
    receive_all_events(EventName, Timeout, MaxEvents, _Count = 0, _Acc = []).

receive_all_events(_EventName, _Timeout, MaxEvents, Count, Acc) when Count >= MaxEvents ->
    lists:reverse(Acc);
receive_all_events(EventName, Timeout, MaxEvents, Count, Acc) ->
    receive
        {telemetry, #{name := [_, _, EventName]} = Event} ->
            ct:pal("telemetry event: ~p", [Event]),
            receive_all_events(EventName, Timeout, MaxEvents, Count + 1, [Event | Acc])
    after Timeout ->
        lists:reverse(Acc)
    end.

wait_telemetry_event(EventName) ->
    wait_telemetry_event(EventName, #{timeout => 5_000, n_events => 1}).

wait_telemetry_event(
    EventName,
    Opts0
) ->
    DefaultOpts = #{timeout => 5_000, n_events => 1},
    #{timeout := Timeout, n_events := NEvents} = maps:merge(DefaultOpts, Opts0),
    wait_n_events(NEvents, Timeout, EventName).

wait_n_events(NEvents, _Timeout, _EventName) when NEvents =< 0 ->
    ok;
wait_n_events(NEvents, Timeout, EventName) ->
    TelemetryTable = get({?MODULE, telemetry_table}),
    receive
        {telemetry, #{name := [_, _, EventName]}} ->
            wait_n_events(NEvents - 1, Timeout, EventName)
    after Timeout ->
        RecordedEvents = ets:tab2list(TelemetryTable),
        ct:pal("recorded events: ~p", [RecordedEvents]),
        error({timeout_waiting_for_telemetry, EventName})
    end.

assert_sync_retry_fail_then_succeed_inflight(Trace) ->
    ?assert(
        ?strict_causality(
            #{?snk_kind := buffer_worker_flush_nack, ref := _Ref},
            #{?snk_kind := buffer_worker_retry_inflight_failed, ref := _Ref},
            Trace
        )
    ),
    %% not strict causality because it might retry more than once
    %% before restoring the resource health.
    ?assert(
        ?causality(
            #{?snk_kind := buffer_worker_retry_inflight_failed, ref := _Ref},
            #{?snk_kind := buffer_worker_retry_inflight_succeeded, ref := _Ref},
            Trace
        )
    ),
    ok.

assert_async_retry_fail_then_succeed_inflight(Trace) ->
    ?assert(
        ?strict_causality(
            #{?snk_kind := handle_async_reply, action := nack},
            #{?snk_kind := buffer_worker_retry_inflight_failed, ref := _Ref},
            Trace
        )
    ),
    %% not strict causality because it might retry more than once
    %% before restoring the resource health.
    ?assert(
        ?causality(
            #{?snk_kind := buffer_worker_retry_inflight_failed, ref := _Ref},
            #{?snk_kind := buffer_worker_retry_inflight_succeeded, ref := _Ref},
            Trace
        )
    ),
    ok.

trace_between_span(Trace0, Marker) ->
    {Trace1, [_ | _]} = ?split_trace_at(#{?snk_kind := Marker, ?snk_span := {complete, _}}, Trace0),
    {[_ | _], [_ | Trace2]} = ?split_trace_at(#{?snk_kind := Marker, ?snk_span := start}, Trace1),
    Trace2.

wait_until_all_marked_as_retriable(NumExpected) when NumExpected =< 0 ->
    ok;
wait_until_all_marked_as_retriable(NumExpected) ->
    Seen = #{},
    do_wait_until_all_marked_as_retriable(NumExpected, Seen).

do_wait_until_all_marked_as_retriable(NumExpected, _Seen) when NumExpected =< 0 ->
    ok;
do_wait_until_all_marked_as_retriable(NumExpected, Seen) ->
    Res = ?block_until(
        #{?snk_kind := buffer_worker_async_agent_down, ?snk_meta := #{pid := P}} when
            not is_map_key(P, Seen),
        10_000
    ),
    case Res of
        {timeout, Evts} ->
            ct:pal("events so far:\n  ~p", [Evts]),
            ct:fail("timeout waiting for events");
        {ok, #{num_affected := NumAffected, ?snk_meta := #{pid := Pid}}} ->
            ct:pal("affected: ~p; pid: ~p", [NumAffected, Pid]),
            case NumAffected >= NumExpected of
                true ->
                    ok;
                false ->
                    do_wait_until_all_marked_as_retriable(NumExpected - NumAffected, Seen#{
                        Pid => true
                    })
            end
    end.

counter_metric_inc_fns() ->
    Mod = emqx_resource_metrics,
    [
        fun Mod:Fn/1
     || {Fn, 1} <- Mod:module_info(functions),
        case string:find(atom_to_list(Fn), "_inc", trailing) of
            "_inc" -> true;
            _ -> false
        end
    ].

gauge_metric_set_fns() ->
    Mod = emqx_resource_metrics,
    [
        fun Mod:Fn/3
     || {Fn, 3} <- Mod:module_info(functions),
        case string:find(atom_to_list(Fn), "_set", trailing) of
            "_set" -> true;
            _ -> false
        end
    ].

create(Id, Group, Type, Config) ->
    emqx_resource:create_local(Id, Group, Type, Config, #{}).

create(Id, Group, Type, Config, Opts) ->
    Res = emqx_resource:create_local(Id, Group, Type, Config, Opts),
    on_exit(fun() -> emqx_resource:remove_local(Id) end),
    Res.

dryrun(Id, Type, Config) ->
    TestPid = self(),
    OnReady = fun(ResId) -> TestPid ! {resource_ready, ResId} end,
    emqx_resource:create_dry_run_local(Id, Type, Config, OnReady).

log_consistency_prop() ->
    {"check state and cache consistency", fun ?MODULE:log_consistency_prop/1}.
log_consistency_prop(Trace) ->
    ?assertEqual([], ?of_kind("inconsistent_status", Trace)),
    ?assertEqual([], ?of_kind("inconsistent_cache", Trace)),
    ok.
