%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("common_test/include/ct.hrl").
-include("emqx_resource.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(TEST_RESOURCE, emqx_connector_demo).
-define(ID, <<"id">>).
-define(DEFAULT_RESOURCE_GROUP, <<"default">>).
-define(RESOURCE_ERROR(REASON), {error, {resource_error, #{reason := REASON}}}).
-define(TRACE_OPTS, #{timetrap => 10000, timeout => 1000}).

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(_, Config) ->
    ct:timetrap({seconds, 30}),
    emqx_connector_demo:set_callback_mode(always_sync),
    Config.

end_per_testcase(_, _Config) ->
    snabbkaffe:stop(),
    _ = emqx_resource:remove(?ID),
    emqx_common_test_helpers:call_janitor(),
    ok.

init_per_suite(Config) ->
    code:ensure_loaded(?TEST_RESOURCE),
    ok = emqx_common_test_helpers:start_apps([emqx_conf]),
    {ok, _} = application:ensure_all_started(emqx_resource),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_resource, emqx_conf]).

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
    {error, _} = emqx_resource:check_and_create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{unknown => test_resource}
    ),

    {ok, _} = emqx_resource:create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
    ),

    {ok, _} = emqx_resource:recreate(
        ?ID,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{}
    ),
    {ok, #{pid := Pid}} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid)),

    ok = emqx_resource:remove(?ID),
    {error, _} = emqx_resource:remove(?ID),

    ?assertNot(is_process_alive(Pid)).

t_create_remove_local(_) ->
    {error, _} = emqx_resource:check_and_create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{unknown => test_resource}
    ),

    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
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

    ok = emqx_resource:remove_local(?ID),
    {error, _} = emqx_resource:remove_local(?ID),

    ?assertMatch(
        ?RESOURCE_ERROR(not_found),
        emqx_resource:query(?ID, get_state)
    ),
    ?assertNot(is_process_alive(Pid)).

t_do_not_start_after_created(_) ->
    ct:pal("creating resource"),
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{start_after_created => false}
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
    ct:pal("starting resource manually"),
    ok = emqx_resource:start(?ID),
    {ok, #{pid := Pid}} = emqx_resource:query(?ID, get_state),
    ?assert(is_process_alive(Pid)),

    %% restart the resource
    ct:pal("restarting resource"),
    ok = emqx_resource:restart(?ID),
    ?assertNot(is_process_alive(Pid)),
    {ok, #{pid := Pid2}} = emqx_resource:query(?ID, get_state),
    ?assert(is_process_alive(Pid2)),

    ct:pal("removing resource"),
    ok = emqx_resource:remove_local(?ID),

    ?assertNot(is_process_alive(Pid2)).

t_query(_) ->
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
    ),

    {ok, #{pid := _}} = emqx_resource:query(?ID, get_state),

    ?assertMatch(
        ?RESOURCE_ERROR(not_found),
        emqx_resource:query(<<"unknown">>, get_state)
    ),

    ok = emqx_resource:remove_local(?ID).

t_query_counter(_) ->
    {ok, _} = emqx_resource:create_local(
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
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true},
        #{batch_size => BatchSize, query_mode => sync}
    ),

    ?check_trace(
        ?TRACE_OPTS,
        emqx_resource:query(?ID, get_counter),
        fun(Result, Trace) ->
            ?assertMatch({ok, 0}, Result),
            QueryTrace = ?of_kind(call_batch_query, Trace),
            ?assertMatch([#{batch := [{query, _, get_counter, _}]}], QueryTrace)
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
            QueryTrace = ?of_kind(call_batch_query, Trace),
            ?assertMatch([#{batch := BatchReq} | _] when length(BatchReq) > 1, QueryTrace)
        end
    ),
    {ok, NMsgs} = emqx_resource:query(?ID, get_counter),

    ok = emqx_resource:remove_local(?ID).

t_query_counter_async_query(_) ->
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true},
        #{query_mode => async, batch_size => 1}
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
            ?assertMatch([#{query := {query, _, {inc_counter, 1}, _}} | _], QueryTrace)
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
            ?assertMatch([#{query := {query, _, get_counter, _}}], QueryTrace)
        end
    ),
    {ok, _, #{metrics := #{counters := C}}} = emqx_resource:get_instance(?ID),
    ?assertMatch(#{matched := 1002, 'success' := 1002, 'failed' := 0}, C),
    ok = emqx_resource:remove_local(?ID).

t_query_counter_async_callback(_) ->
    emqx_connector_demo:set_callback_mode(async_if_possible),

    Tab0 = ets:new(?FUNCTION_NAME, [bag, public]),
    Insert = fun(Tab, Result) ->
        ets:insert(Tab, {make_ref(), Result})
    end,
    ReqOpts = #{async_reply_fun => {Insert, [Tab0]}},
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true},
        #{
            query_mode => async,
            batch_size => 1,
            async_inflight_window => 1000000
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
            ?assertMatch([#{query := {query, _, {inc_counter, 1}, _}} | _], QueryTrace)
        end
    ),

    %% simple query ignores the query_mode and batching settings in the resource_worker
    ?check_trace(
        ?TRACE_OPTS,
        emqx_resource:simple_sync_query(?ID, get_counter),
        fun(Result, Trace) ->
            ?assertMatch({ok, 1000}, Result),
            QueryTrace = ?of_kind(call_query, Trace),
            ?assertMatch([#{query := {query, _, get_counter, _}}], QueryTrace)
        end
    ),
    {ok, _, #{metrics := #{counters := C}}} = emqx_resource:get_instance(?ID),
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
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true},
        #{
            query_mode => async,
            batch_size => 1,
            async_inflight_window => WindowSize,
            worker_pool_size => 1,
            resume_interval => 300
        }
    ),
    ?assertMatch({ok, 0}, emqx_resource:simple_sync_query(?ID, get_counter)),

    %% block the resource
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, block)),

    %% send async query to make the inflight window full
    ?check_trace(
        ?TRACE_OPTS,
        inc_counter_in_parallel(WindowSize, ReqOpts),
        fun(Trace) ->
            QueryTrace = ?of_kind(call_query_async, Trace),
            ?assertMatch([#{query := {query, _, {inc_counter, 1}, _}} | _], QueryTrace)
        end
    ),
    tap_metrics(?LINE),

    %% this will block the resource_worker as the inflight window is full now
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_resource:query(?ID, {inc_counter, 2}),
            #{?snk_kind := resource_worker_enter_blocked},
            1_000
        ),
    ?assertMatch(0, ets:info(Tab0, size)),

    tap_metrics(?LINE),
    %% send query now will fail because the resource is blocked.
    Insert = fun(Tab, Ref, Result) ->
        ct:pal("inserting ~p", [{Ref, Result}]),
        ets:insert(Tab, {Ref, Result}),
        ?tp(tmp_query_inserted, #{})
    end,
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_resource:query(?ID, {inc_counter, 3}, #{
                async_reply_fun => {Insert, [Tab0, tmp_query]}
            }),
            #{?snk_kind := tmp_query_inserted},
            1_000
        ),
    %% since this counts as a failure, it'll be enqueued and retried
    %% later, when the resource is unblocked.
    ?assertMatch([{_, {error, {resource_error, #{reason := blocked}}}}], ets:take(Tab0, tmp_query)),
    tap_metrics(?LINE),

    %% all responses should be received after the resource is resumed.
    {ok, SRef0} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
        %% +1 because the tmp_query above will be retried and succeed
        %% this time.
        WindowSize + 1,
        _Timeout = 60_000
    ),
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, resume)),
    tap_metrics(?LINE),
    {ok, _} = snabbkaffe:receive_events(SRef0),
    %% since the previous tmp_query was enqueued to be retried, we
    %% take it again from the table; this time, it should have
    %% succeeded.
    ?assertMatch([{tmp_query, ok}], ets:take(Tab0, tmp_query)),
    ?assertEqual(WindowSize, ets:info(Tab0, size)),
    tap_metrics(?LINE),

    %% send async query, this time everything should be ok.
    Num = 10,
    ?check_trace(
        ?TRACE_OPTS,
        begin
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
                Num,
                _Timeout = 60_000
            ),
            inc_counter_in_parallel(Num, ReqOpts),
            {ok, _} = snabbkaffe:receive_events(SRef),
            ok
        end,
        fun(Trace) ->
            QueryTrace = ?of_kind(call_query_async, Trace),
            ?assertMatch([#{query := {query, _, {inc_counter, _}, _}} | _], QueryTrace)
        end
    ),
    ?assertEqual(WindowSize + Num, ets:info(Tab0, size), #{tab => ets:tab2list(Tab0)}),
    tap_metrics(?LINE),

    %% block the resource
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, block)),
    %% again, send async query to make the inflight window full
    ?check_trace(
        ?TRACE_OPTS,
        inc_counter_in_parallel(WindowSize, ReqOpts),
        fun(Trace) ->
            QueryTrace = ?of_kind(call_query_async, Trace),
            ?assertMatch([#{query := {query, _, {inc_counter, 1}, _}} | _], QueryTrace)
        end
    ),

    %% this will block the resource_worker
    ok = emqx_resource:query(?ID, {inc_counter, 1}),

    Sent = WindowSize + Num + WindowSize,
    {ok, SRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
        WindowSize,
        _Timeout = 60_000
    ),
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, resume)),
    {ok, _} = snabbkaffe:receive_events(SRef1),
    ?assertEqual(Sent, ets:info(Tab0, size)),

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
    WindowSize = 3,
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, register => true},
        #{
            query_mode => async,
            batch_size => BatchSize,
            async_inflight_window => WindowSize,
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
        begin
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := call_batch_query_async}),
                WindowSize,
                _Timeout = 60_000
            ),
            inc_counter_in_parallel(NumMsgs, ReqOpts),
            {ok, _} = snabbkaffe:receive_events(SRef),
            ok
        end,
        fun(Trace) ->
            QueryTrace = ?of_kind(call_batch_query_async, Trace),
            ?assertMatch(
                [
                    #{
                        batch := [
                            {query, _, {inc_counter, 1}, _},
                            {query, _, {inc_counter, 1}, _}
                        ]
                    }
                    | _
                ],
                QueryTrace
            )
        end
    ),
    tap_metrics(?LINE),

    ?check_trace(
        begin
            %% this will block the resource_worker as the inflight window is full now
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_resource:query(?ID, {inc_counter, 2}),
                    #{?snk_kind := resource_worker_enter_blocked},
                    5_000
                ),
            ?assertMatch(0, ets:info(Tab0, size)),
            ok
        end,
        []
    ),

    tap_metrics(?LINE),
    %% send query now will fail because the resource is blocked.
    Insert = fun(Tab, Ref, Result) ->
        ct:pal("inserting ~p", [{Ref, Result}]),
        ets:insert(Tab, {Ref, Result}),
        ?tp(tmp_query_inserted, #{})
    end,
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_resource:query(?ID, {inc_counter, 3}, #{
                async_reply_fun => {Insert, [Tab0, tmp_query]}
            }),
            #{?snk_kind := tmp_query_inserted},
            1_000
        ),
    %% since this counts as a failure, it'll be enqueued and retried
    %% later, when the resource is unblocked.
    ?assertMatch([{_, {error, {resource_error, #{reason := blocked}}}}], ets:take(Tab0, tmp_query)),
    tap_metrics(?LINE),

    %% all responses should be received after the resource is resumed.
    {ok, SRef0} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
        %% +1 because the tmp_query above will be retried and succeed
        %% this time.
        WindowSize + 1,
        _Timeout = 60_000
    ),
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, resume)),
    tap_metrics(?LINE),
    {ok, _} = snabbkaffe:receive_events(SRef0),
    %% since the previous tmp_query was enqueued to be retried, we
    %% take it again from the table; this time, it should have
    %% succeeded.
    ?assertMatch([{tmp_query, ok}], ets:take(Tab0, tmp_query)),
    ?assertEqual(NumMsgs, ets:info(Tab0, size), #{tab => ets:tab2list(Tab0)}),
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
                _Timeout = 60_000
            ),
            inc_counter_in_parallel(NumMsgs1, ReqOpts),
            {ok, _} = snabbkaffe:receive_events(SRef),
            ok
        end,
        fun(Trace) ->
            QueryTrace = ?of_kind(call_batch_query_async, Trace),
            ?assertMatch(
                [#{batch := [{query, _, {inc_counter, _}, _} | _]} | _],
                QueryTrace
            )
        end
    ),
    ?assertEqual(
        NumMsgs + NumMsgs1,
        ets:info(Tab0, size),
        #{tab => ets:tab2list(Tab0)}
    ),
    tap_metrics(?LINE),

    %% block the resource
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, block)),
    %% again, send async query to make the inflight window full
    ?check_trace(
        ?TRACE_OPTS,
        inc_counter_in_parallel(WindowSize, ReqOpts),
        fun(Trace) ->
            QueryTrace = ?of_kind(call_batch_query_async, Trace),
            ?assertMatch(
                [#{batch := [{query, _, {inc_counter, _}, _} | _]} | _],
                QueryTrace
            )
        end
    ),

    %% this will block the resource_worker
    ok = emqx_resource:query(?ID, {inc_counter, 1}),

    Sent = NumMsgs + NumMsgs1 + WindowSize,
    {ok, SRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := connector_demo_inc_counter_async}),
        WindowSize,
        _Timeout = 60_000
    ),
    ?assertMatch(ok, emqx_resource:simple_sync_query(?ID, resume)),
    {ok, _} = snabbkaffe:receive_events(SRef1),
    ?assertEqual(Sent, ets:info(Tab0, size)),

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

t_healthy_timeout(_) ->
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => <<"bad_not_atom_name">>, register => true},
        %% the ?TEST_RESOURCE always returns the `Mod:on_get_status/2` 300ms later.
        #{health_check_interval => 200}
    ),
    ?assertMatch(
        ?RESOURCE_ERROR(not_connected),
        emqx_resource:query(?ID, get_state)
    ),
    ok = emqx_resource:remove_local(?ID).

t_healthy(_) ->
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
    ),
    {ok, #{pid := Pid}} = emqx_resource:query(?ID, get_state),
    timer:sleep(300),
    emqx_resource:set_resource_status_connecting(?ID),

    {ok, connected} = emqx_resource:health_check(?ID),
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

    ok = emqx_resource:remove_local(?ID).

t_stop_start(_) ->
    {error, _} = emqx_resource:check_and_create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{unknown => test_resource}
    ),

    {ok, _} = emqx_resource:check_and_create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{<<"name">> => <<"test_resource">>}
    ),

    %% add some metrics to test their persistence
    WorkerID0 = <<"worker:0">>,
    WorkerID1 = <<"worker:1">>,
    emqx_resource_metrics:inflight_set(?ID, WorkerID0, 2),
    emqx_resource_metrics:inflight_set(?ID, WorkerID1, 3),
    ?assertEqual(5, emqx_resource_metrics:inflight_get(?ID)),

    {ok, _} = emqx_resource:check_and_recreate(
        ?ID,
        ?TEST_RESOURCE,
        #{<<"name">> => <<"test_resource">>},
        #{}
    ),

    {ok, #{pid := Pid0}} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid0)),

    %% metrics are reset when recreating
    ?assertEqual(0, emqx_resource_metrics:inflight_get(?ID)),

    ok = emqx_resource:stop(?ID),

    ?assertNot(is_process_alive(Pid0)),

    ?assertMatch(
        ?RESOURCE_ERROR(stopped),
        emqx_resource:query(?ID, get_state)
    ),

    ok = emqx_resource:restart(?ID),
    timer:sleep(300),

    {ok, #{pid := Pid1}} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid1)),

    %% now stop while resetting the metrics
    emqx_resource_metrics:inflight_set(?ID, WorkerID0, 1),
    emqx_resource_metrics:inflight_set(?ID, WorkerID1, 4),
    ?assertEqual(5, emqx_resource_metrics:inflight_get(?ID)),
    ok = emqx_resource:stop(?ID),
    ?assertEqual(0, emqx_resource_metrics:inflight_get(?ID)),

    ok.

t_stop_start_local(_) ->
    {error, _} = emqx_resource:check_and_create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{unknown => test_resource}
    ),

    {ok, _} = emqx_resource:check_and_create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{<<"name">> => <<"test_resource">>}
    ),

    {ok, _} = emqx_resource:check_and_recreate_local(
        ?ID,
        ?TEST_RESOURCE,
        #{<<"name">> => <<"test_resource">>},
        #{}
    ),

    {ok, #{pid := Pid0}} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid0)),

    ok = emqx_resource:stop(?ID),

    ?assertNot(is_process_alive(Pid0)),

    ?assertMatch(
        ?RESOURCE_ERROR(stopped),
        emqx_resource:query(?ID, get_state)
    ),

    ok = emqx_resource:restart(?ID),

    {ok, #{pid := Pid1}} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid1)).

t_list_filter(_) ->
    {ok, _} = emqx_resource:create_local(
        emqx_resource:generate_id(<<"a">>),
        <<"group1">>,
        ?TEST_RESOURCE,
        #{name => a}
    ),
    {ok, _} = emqx_resource:create_local(
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
    ets:match_delete(emqx_resource_manager, {{owner, '$1'}, '_'}),
    lists:foreach(
        fun(_) ->
            create_dry_run_local_succ()
        end,
        lists:seq(1, 10)
    ),
    [] = ets:match(emqx_resource_manager, {{owner, '$1'}, '_'}).

create_dry_run_local_succ() ->
    case whereis(test_resource) of
        undefined -> ok;
        Pid -> exit(Pid, kill)
    end,
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
    ?assertEqual(ok, Res3).

t_test_func(_) ->
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:not_empty("not_empty"), [<<"someval">>])),
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:min(int, 3), [4])),
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:max(array, 10), [[a, b, c, d]])),
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:max(string, 10), ["less10"])).

t_reset_metrics(_) ->
    {ok, _} = emqx_resource:create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
    ),

    {ok, #{pid := Pid}} = emqx_resource:query(?ID, get_state),
    emqx_resource:reset_metrics(?ID),
    ?assert(is_process_alive(Pid)),
    ok = emqx_resource:remove(?ID),
    ?assertNot(is_process_alive(Pid)).

t_auto_retry(_) ->
    {Res, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, create_error => true},
        #{auto_retry_interval => 100}
    ),
    ?assertEqual(ok, Res).

t_retry_batch(_Config) ->
    {ok, _} = emqx_resource:create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{
            query_mode => async,
            batch_size => 5,
            worker_pool_size => 1,
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
                    #{?snk_kind := resource_worker_enter_blocked},
                    5_000
                ),
            %% now the individual messages should have been counted
            Matched1 = emqx_resource_metrics:matched_get(?ID),
            ?assertEqual(Matched0 + NumPayloads, Matched1),

            %% wait for two more retries while the failure is enabled; the
            %% batch shall remain enqueued.
            {ok, _} =
                snabbkaffe:block_until(
                    ?match_n_events(2, #{?snk_kind := resource_worker_retry_queue_batch_failed}),
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
                    #{?snk_kind := resource_worker_retry_queue_batch_succeeded},
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
            ?assertEqual(
                ExpectedSeenPayloads,
                ?projection(n, ?of_kind(connector_demo_batch_inc_individual, Trace))
            ),
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

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------
inc_counter_in_parallel(N) ->
    inc_counter_in_parallel(N, #{}).

inc_counter_in_parallel(N, Opts0) ->
    Parent = self(),
    Pids = [
        erlang:spawn(fun() ->
            Opts =
                case is_function(Opts0) of
                    true -> Opts0();
                    false -> Opts0
                end,
            emqx_resource:query(?ID, {inc_counter, 1}, Opts),
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
    ].

bin_config() ->
    <<"\"name\": \"test_resource\"">>.

config() ->
    {ok, Config} = hocon:binary(bin_config()),
    Config.

tap_metrics(Line) ->
    {ok, _, #{metrics := #{counters := C, gauges := G}}} = emqx_resource:get_instance(?ID),
    ct:pal("metrics (l. ~b): ~p", [Line, #{counters => C, gauges => G}]),
    #{counters => C, gauges => G}.
