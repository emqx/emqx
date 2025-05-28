%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource_buffer_worker_internal.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_resource/include/emqx_resource_runtime.hrl").

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

-define(fallback_actions, fallback_actions).

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_cases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.

matrix_cases() ->
    lists:filter(
        fun(TestCase) ->
            maybe
                true ?= erlang:function_exported(?MODULE, TestCase, 0),
                {matrix, true} ?= proplists:lookup(matrix, ?MODULE:TestCase()),
                true
            else
                _ -> false
            end
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

groups() ->
    emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()).

init_per_testcase(TestCase, Config) ->
    ct:timetrap({seconds, 30}),
    emqx_connector_demo:set_callback_mode(always_sync),
    snabbkaffe:start_trace(),
    ct:print(asciiart:visible($%, "~s", [TestCase])),
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
%% Helper fns
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
    create(Id, Group, Type, Config, #{}).

create(Id, Group, Type, Config, Opts) ->
    Res = emqx_resource:create_local(Id, Group, Type, Config, Opts),
    on_exit(fun() -> emqx_resource:remove_local(Id) end),
    case Type of
        ?TEST_RESOURCE ->
            on_exit(fun() -> emqx_connector_demo:clear_emulated_config(Id) end);
        _ ->
            ok
    end,
    Res.

add_channel_emulate_config(ConnResId, ChanId, ChanConfig, Mode) ->
    emqx_connector_demo:add_channel_emulate_config(ConnResId, ChanId, ChanConfig, Mode).

remove_channel_emulate_config(ConnResId, ChanId, Mode) ->
    emqx_connector_demo:remove_channel_emulate_config(ConnResId, ChanId, Mode).

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

connector_res_id(Name) ->
    %% Needs to have this form to satifisfy internal, implicit requirements of
    %% `emqx_resource_cache'.
    <<"connector:ctype:", Name/binary>>.

action_res_id(ConnResId) ->
    %% Needs to have this form to satifisfy internal, implicit requirements of
    %% `emqx_resource_cache'.
    <<"action:atype:aname:", ConnResId/binary>>.

add_channel(ConnResId, ChanId, Config) ->
    do_add_channel(ConnResId, ChanId, Config, sync).

add_channel_async(ConnResId, ChanId, Config) ->
    do_add_channel(ConnResId, ChanId, Config, async).

do_add_channel(ConnResId, ChanId, Config, Mode) ->
    ok = emqx_resource:create_metrics(ChanId),
    on_exit(fun() -> emqx_resource:clear_metrics(ChanId) end),
    ResourceOpts = emqx_resource:fetch_creation_opts(Config),
    on_exit(fun() -> emqx_resource_buffer_worker_sup:stop_workers(ChanId, ResourceOpts) end),
    ok = emqx_resource_buffer_worker_sup:start_workers(ChanId, ResourceOpts),
    emqx_connector_demo:add_channel_emulate_config(ConnResId, ChanId, Config, Mode).

remove_channel(ConnResId, ChanId) ->
    do_remove_channel(ConnResId, ChanId, sync).

remove_channel_async(ConnResId, ChanId) ->
    do_remove_channel(ConnResId, ChanId, async).

do_remove_channel(ConnResId, ChanId, Mode) ->
    emqx_connector_demo:remove_channel_emulate_config(ConnResId, ChanId, Mode).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] -> Default;
        Path -> Path
    end.

fallback_actions_batch_opts(no_batch) ->
    #{
        resource_opts => #{batch_time => 0, batch_size => 1},
        n_reqs => 1
    };
fallback_actions_batch_opts(batch) ->
    #{
        resource_opts => #{batch_time => 100, batch_size => 100},
        n_reqs => 10
    }.

fallback_action_query(ConnResId, ChanId, FallbackFn, Ctx, #{query_mode := async} = QueryOpts0) ->
    AdjustQueryOptsFn = maps:get(adjust_query_opts, Ctx, fun(_Ctx, QO) -> QO end),
    Alias = alias([reply]),
    ReplyTo = {
        fun(ReplyCallerContext) ->
            Alias ! {Alias, ReplyCallerContext}
        end,
        [],
        #{reply_dropped => true}
    },
    QueryOpts1 = maps:merge(
        #{connector_resource_id => ConnResId},
        QueryOpts0#{reply_to => ReplyTo}
    ),
    QueryOpts = AdjustQueryOptsFn(Ctx, QueryOpts1),
    Req = #{q => ask, fn => FallbackFn, ctx => Ctx},
    ct:pal("~p sending query\n  ~p", [self(), #{query_opts => QueryOpts, ctx => Ctx}]),
    Res = emqx_resource:query(
        ChanId,
        {ChanId, Req},
        QueryOpts
    ),
    case Res of
        ok ->
            ok;
        {async_return, {ok, _Pid}} ->
            %% Simple async queries do not massage the returned value to stip the
            %% `{async_return, _}' tag.
            ok
    end,
    ct:pal("~p waiting for reply", [self()]),
    receive
        {Alias, ReplyCallerContext} ->
            ct:pal("~p received async reply:\n  ~p", [self(), ReplyCallerContext]),
            case ReplyCallerContext of
                #{result := Result} ->
                    Result;
                Result ->
                    %% If expired, it's a simple tuple
                    Result
            end
    after 1_000 -> ct:fail("~p timed out waiting for reply", [self()])
    end;
fallback_action_query(ConnResId, ChanId, FallbackFn, Ctx, #{query_mode := sync} = QueryOpts0) ->
    AdjustQueryOptsFn = maps:get(adjust_query_opts, Ctx, fun(QO) -> QO end),
    QueryOpts1 = maps:merge(
        #{connector_resource_id => ConnResId},
        QueryOpts0
    ),
    QueryOpts = AdjustQueryOptsFn(QueryOpts1),
    Req = #{q => ask, fn => FallbackFn, ctx => Ctx},
    ct:pal("~p sending query\n  ~p", [self(), #{query_opts => QueryOpts, ctx => Ctx}]),
    emqx_resource:query(
        ChanId,
        {ChanId, Req},
        QueryOpts
    ).

fallback_query_wait_async(N, RequestQueryKind, QueryOpts, Ctx) ->
    #{
        conn_res_id := ConnResId,
        chan_id := ChanId,
        mk_event_matcher := MkEventMatcher,
        fallback_fn := FallbackFn
    } = Ctx,
    AssertExpectedCallMode = maps:get(assert_expected_callmode, Ctx, true),
    ReqCtx = Ctx#{req_n => N},
    case AssertExpectedCallMode of
        true ->
            ct:pal("~p sending request ~b", [self(), N]),
            EventMatcher = MkEventMatcher(RequestQueryKind),
            {Result, {ok, _}} =
                snabbkaffe:wait_async_action(
                    fun() ->
                        fallback_action_query(
                            ConnResId, ChanId, FallbackFn, ReqCtx, QueryOpts#{
                                query_mode => RequestQueryKind
                            }
                        )
                    end,
                    EventMatcher,
                    infinity
                );
        false ->
            Result = fallback_action_query(
                ConnResId, ChanId, FallbackFn, ReqCtx, QueryOpts#{
                    query_mode => RequestQueryKind
                }
            )
    end,
    ct:pal("~p received reply ~b:\n  ~p", [self(), N, Result]),
    Result.

fallback_send_many(RequestQueryKind, Ctx) ->
    #{n_reqs := NReqs} = Ctx,
    BaseQueryOpts = maps:get(base_query_opts, Ctx),
    QueryOpts0 = maps:get(query_opts, Ctx, #{}),
    QueryOpts = emqx_utils_maps:deep_merge(BaseQueryOpts, QueryOpts0),
    Results = emqx_utils:pmap(
        fun(N) ->
            fallback_query_wait_async(N, RequestQueryKind, QueryOpts, Ctx)
        end,
        lists:seq(1, NReqs)
    ),
    Results.

assert_all_queries_triggered_fallbacks(Ctx) ->
    #{n_reqs := NReqs} = Ctx,
    lists:foreach(
        fun(N) ->
            ?assertReceive(
                {fallback_called, #{n := 1, request := #{ctx := #{req_n := N}}}}
            ),
            ?assertReceive({fallback_called, #{n := 2, request := #{ctx := #{req_n := N}}}})
        end,
        lists:seq(1, NReqs)
    ),
    ?assertNotReceive({fallback_called, _}),
    ok.

merge_maps(Maps) ->
    lists:foldl(fun emqx_utils_maps:deep_merge/2, #{}, Maps).

%% Poor man's `timer:apply_after', prior to OTP 27.
apply_after(Time, Fn) ->
    spawn(fun() ->
        timer:sleep(Time),
        Fn()
    end).

fallback_actions_basic_setup(ConnName, ChanQueryMode, CallbackMode, ShouldBatch) ->
    fallback_actions_basic_setup(ConnName, ChanQueryMode, CallbackMode, ShouldBatch, _Opts = #{}).

-doc """"
Options:
  - `fallback_fn`: the function that will be called by the resource itself to trigger
    the fallback actions.
  - `assert_expected_callmode`: if true, asserts that the expected query mode in buffer
    worker was called.
  - `adjust_resource_config_fn`: a function that receives the default resource config for
    this scenario and returns a new resource config.
"""".
fallback_actions_basic_setup(ConnName, ChanQueryMode, CallbackMode, ShouldBatch, Opts) ->
    ChanResourceOptsOverride = maps:get(chan_resource_opts_override, Opts, #{}),
    emqx_connector_demo:set_callback_mode(CallbackMode),
    #{
        resource_opts := ResourceOpts0,
        n_reqs := NReqs
    } = fallback_actions_batch_opts(ShouldBatch),
    MkEventMatcher = fun(RequestQueryKind) ->
        QMSummary = emqx_resource_manager:summarize_query_mode(ChanQueryMode, RequestQueryKind),
        case {ShouldBatch, CallbackMode, QMSummary} of
            {no_batch, always_sync, _} ->
                ?match_event(#{?snk_kind := call_query});
            {batch, always_sync, _} ->
                ?match_event(#{?snk_kind := call_batch_query});
            {no_batch, async_if_possible, #{is_simple := false, has_internal_buffer := false}} ->
                ?match_event(#{?snk_kind := call_query_async});
            {batch, async_if_possible, #{is_simple := false, has_internal_buffer := false}} ->
                ?match_event(#{?snk_kind := call_batch_query_async});
            {no_batch, async_if_possible, #{has_internal_buffer := true}} ->
                ?match_event(#{?snk_kind := call_query_async});
            {no_batch, async_if_possible, #{is_simple := true, requested_query_kind := sync}} ->
                ?match_event(#{?snk_kind := call_query});
            {no_batch, async_if_possible, #{is_simple := true, requested_query_kind := async}} ->
                ?match_event(#{?snk_kind := call_query_async})
        end
    end,
    ConnResId = connector_res_id(ConnName),
    AdjustConnConfigFn = maps:get(adjust_conn_config_fn, Opts, fun(Cfg) -> Cfg end),
    ConnConfig = AdjustConnConfigFn(#{name => test_resource}),
    {ok, _} =
        create(
            ConnResId,
            ?DEFAULT_RESOURCE_GROUP,
            ?TEST_RESOURCE,
            ConnConfig,
            #{
                health_check_interval => 100,
                start_timeout => 100
            }
        ),
    ChanId = action_res_id(ConnResId),
    ChanResourceOpts = merge_maps([
        #{
            health_check_interval => 100,
            worker_pool_size => 1
        },
        ResourceOpts0,
        ChanResourceOptsOverride
    ]),
    ChanConfig = #{
        force_query_mode => ChanQueryMode,
        resource_opts => ChanResourceOpts
    },
    ok =
        add_channel(
            ConnResId,
            ChanId,
            ChanConfig
        ),
    FallbackFn = maps:get(
        fallback_fn,
        Opts,
        fun(_Ctx) ->
            {error, {unrecoverable_error, fallback_time}}
        end
    ),
    TestPid = self(),
    FallbackAction = fun(Ctx) ->
        ct:pal("fallback triggered:\n  ~p", [Ctx]),
        TestPid ! {fallback_called, Ctx},
        ok
    end,
    MkFallback = fun(N) -> fun(Ctx) -> FallbackAction(Ctx#{n => N}) end end,
    QueryOpts = #{
        ?fallback_actions => [
            MkFallback(1),
            MkFallback(2)
        ]
    },
    #{
        base_query_opts => QueryOpts,
        mk_fallback => MkFallback,
        fallback_action => FallbackAction,
        conn_res_id => ConnResId,
        chan_id => ChanId,
        mk_event_matcher => MkEventMatcher,
        chan_resource_opts => ChanResourceOpts,
        n_reqs => NReqs,
        fallback_fn => FallbackFn
    }.

%%------------------------------------------------------------------------------
%% Properties
%%------------------------------------------------------------------------------

%% Verifies that we compress operations for a given channel id that may happen to stack up
%% while the resource manager process is stuck on a call.
t_prop_compress_channel_operations(_Config) ->
    ct:timetrap({seconds, 60}),
    ?assert(
        proper:quickcheck(prop_compress_channel_operations(), [{numtests, 100}, {to_file, user}])
    ),
    ok.

prop_compress_channel_operations() ->
    ?FORALL(
        {ConnStatus, Commands},
        {resource_status_gen(), list({range(1, 3), channel_op_gen()})},
        begin
            ConnResId = <<"connector:ctype:c">>,
            AgentState0 = #{
                resource_health_check => ConnStatus,
                channel_health_check => connected
            },
            {ok, Agent} = emqx_utils_agent:start_link(AgentState0),
            %% We mock the module so we may count the calls to add/remove channel
            %% callbacks.
            ok = meck:new(?TEST_RESOURCE, [passthrough]),
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
            ?assertMatch({ok, ConnStatus}, emqx_resource:health_check(ConnResId)),
            %% We suspend the process so operations are stacked on its mailbox.
            Pid = emqx_resource_manager:where(ConnResId),
            ok = sys:suspend(Pid),
            lists:foreach(
                fun(Command) -> exec_channel_operation(Command, ConnResId) end,
                Commands
            ),
            %% Resume and let it process everything.
            ok = sys:resume(Pid),
            %% To force processing of the mailbox, and trigger channel ops.  Must go to
            %% `?status_connected` to actually process state changing operations.
            _ = emqx_utils_agent:get_and_update(Agent, fun(Old) ->
                {unused, Old#{resource_health_check := connected}}
            end),
            ?retry(100, 10, ?assertMatch({ok, connected}, emqx_resource:health_check(ConnResId))),
            {ok, InConfigChannelsList} = emqx_resource_manager:get_channels(ConnResId),
            InConfigChannels = maps:from_list(InConfigChannelsList),
            InStateChannels = emqx_resource_manager:get_channel_configs(ConnResId),
            {ok, _Group, ResData} = emqx_resource_manager:lookup_cached(ConnResId),
            History = meck:history(?TEST_RESOURCE),
            ok = meck:unload(?TEST_RESOURCE),
            ok = emqx_resource:remove_local(ConnResId),
            emqx_connector_demo:clear_emulated_config(ConnResId),
            ExpectedOps = process_commands_expectation(Commands),
            ExpectedChannels = maps:filter(fun(_, V) -> V /= del end, ExpectedOps),
            %% Even if not added yet to the resource state, the known channel configs must
            %% be the latest received additions, or the channel should be absent from
            %% installed channels.
            HasExpectedConfigs = ExpectedChannels =:= InConfigChannels,
            %% If the resource status is connected, then the channel must also be in the
            %% connector resource state.
            IsInExpectedResState = ExpectedChannels =:= InStateChannels,
            %% We compress the operations so that operations that cancel out are not
            %% executed needlessly.  We can only know which calls were made from the
            %% callback module in the connected status.
            ExpectedOpCount = map_size(ExpectedOps),
            ActualOpCount = lists:sum([
                1
             || {_Pid, {?TEST_RESOURCE, Fn, _Args}, _Res} <- History,
                lists:member(Fn, [on_add_channel, on_remove_channel])
            ]),
            IsCompressedAsExpected =
                case ConnStatus of
                    ?status_connected ->
                        %% One op per distinct channel
                        ExpectedOpCount =:= ActualOpCount;
                    _ ->
                        true
                end,
            ?WHENFAIL(
                io:format(
                    user,
                    "Connector status:\n  ~s\n\n"
                    "Commands:\n  ~p\n\n"
                    "Expected Channels:\n  ~p\n\n"
                    "Channels in config:\n  ~p\n\n"
                    "Channels in state:\n  ~p\n\n"
                    "Expected op count:\n  ~p\n\n"
                    "Actual op count:\n  ~p\n\n"
                    "Module call history:\n  ~p\n\n"
                    "Resource:\n  ~p\n\n",
                    [
                        ConnStatus,
                        Commands,
                        ExpectedChannels,
                        InConfigChannels,
                        InStateChannels,
                        ExpectedOpCount,
                        ActualOpCount,
                        History,
                        ResData
                    ]
                ),
                aggregate(
                    [ConnStatus],
                    aggregate(
                        Commands,
                        HasExpectedConfigs andalso
                            IsInExpectedResState andalso
                            IsCompressedAsExpected
                    )
                )
            )
        end
    ).

process_commands_expectation(Commands) ->
    lists:foldl(
        fun
            ({N, {add, ChannelConf}}, ModelState) ->
                ChannelId = channel_num_to_id(N),
                ModelState#{ChannelId => ChannelConf};
            ({N, del}, ModelState) ->
                ChannelId = channel_num_to_id(N),
                maps:remove(ChannelId, ModelState)
        end,
        #{},
        Commands
    ).

channel_num_to_id(N) ->
    <<"action:atype:", (integer_to_binary(N))/binary, ":connector:ctype:c">>.

exec_channel_operation({N, {add, ChannelConf}}, ConnResId) ->
    ChannelId = channel_num_to_id(N),
    ok = add_channel_async(ConnResId, ChannelId, ChannelConf);
exec_channel_operation({N, del}, ConnResId) ->
    ChannelId = channel_num_to_id(N),
    ok = remove_channel_async(ConnResId, ChannelId).

channel_op_gen() ->
    frequency([{2, add_channel_gen()}, {1, del}]).

add_channel_gen() ->
    ?LET(
        HealthCheckInterval,
        range(1, 10),
        {add, #{resource_opts => #{health_check_interval => HealthCheckInterval}}}
    ).

resource_status_gen() ->
    %% Stopped is a more special state, which is not entered automatically
    oneof([
        ?status_connected,
        ?status_connecting,
        ?status_disconnected
    ]).

%%------------------------------------------------------------------------------
%% Test cases
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
        #{
            name => test_resource,
            register => true,
            force_query_mode => sync
        },
        #{
            batch_size => BatchSize,
            batch_time => 100
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
    Insert = fun(Tab, #{result := Result}) ->
        ets:insert(Tab, {make_ref(), Result})
    end,
    ReqOpts = #{async_reply_fun => {Insert, [Tab0]}},
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{
            name => test_resource,
            register => true,
            force_query_mode => async
        },
        #{
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
    Insert0 = fun(Tab, Ref, #{result := Result}) ->
        ct:pal("inserting ~p", [{Ref, Result}]),
        ets:insert(Tab, {Ref, Result})
    end,
    ReqOpts = fun() -> #{async_reply_fun => {Insert0, [Tab0, make_ref()]}} end,
    WindowSize = 15,
    {ok, _} = create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{
            name => test_resource,
            register => true,
            force_query_mode => async
        },
        #{
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
    Insert = fun(Tab, Ref, #{result := Result}) ->
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
    Insert0 = fun(Tab, Ref, #{result := Result}) ->
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
        #{
            name => test_resource,
            register => true,
            force_query_mode => async
        },
        #{
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
    Insert = fun(Tab, Ref, #{result := Result}) ->
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
        #{
            name => ?FUNCTION_NAME,
            force_query_mode => async
        },
        #{
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
                        #{
                            async_reply_fun => {
                                fun(#{result := Res}) -> ct:pal("Res = ~p", [Res]) end, []
                            }
                        }
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
        #{
            name => ?FUNCTION_NAME,
            force_query_mode => async
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
            batch_size => 1,
            worker_pool_size => NumBufferWorkers,
            metrics_refresh_interval => 50,
            resume_interval => ResumeInterval
        }
    ),
    Tab0 = ets:new(?FUNCTION_NAME, [bag, public]),
    Insert0 = fun(Tab, Ref, #{result := Result}) ->
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => sync
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
        #{
            name => test_resource,
            force_query_mode => async
        },
        #{
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
                #{
                    name => test_resource,
                    force_query_mode => async
                },
                DefaultOpts
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
                #{
                    name => test_resource,
                    force_query_mode => sync
                },
                DefaultOpts
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
                add_channel(
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
            ConnResId = connector_res_id(ConnName),
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
            ChanId = action_res_id(ConnResId),
            ok =
                add_channel(
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
                {Old, Old#{resource_health_check := {notify, Me, ?status_connected}}}
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
            ?assertReceive({returning_resource_health_check_result, ConnResId, ?status_connected}),
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
            %% Important: we must use a resource id with this format so that the resouce
            %% manager child spawned has `temporary` restart type instead of `transient`.
            Id = ?PROBE_ID_NEW(),

            %% Kill the caller process while it's blocked during an `on_stop` call.
            ?force_ordering(
                #{?snk_kind := connector_demo_on_stop_will_delay},
                #{?snk_kind := will_kill_request}
            ),

            %% Simulates a cowboy request process.
            {ok, StartAgent} = emqx_utils_agent:start_link(not_called),
            %% Note: this should be larger than `emqx_resource_manager:?T_OPERATION`.
            StopDelay = 6_000,
            {ok, StopAgent} = emqx_utils_agent:start_link({delay, StopDelay}),
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
                    Id,
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
            %% Deallocations should evenutally happen, when cache cleaner runs.

            ?assertEqual(#{}, emqx_resource:get_allocated_resources(Id)),

            ok
        end,
        []
    ),
    ok.

%% Happy path smoke test for fallback actions.
t_fallback_actions() ->
    [{matrix, true}].
t_fallback_actions(matrix) ->
    [
        [sync, always_sync, no_batch],
        [sync, always_sync, batch],
        [async, async_if_possible, no_batch],
        [async, async_if_possible, batch],
        [simple_sync, async_if_possible, no_batch],
        [simple_async, async_if_possible, no_batch],
        [simple_sync_internal_buffer, async_if_possible, no_batch],
        [simple_async_internal_buffer, async_if_possible, no_batch]
    ];
t_fallback_actions(Config) when is_list(Config) ->
    [ChanQueryMode, CallbackMode, ShouldBatch] =
        group_path(Config, [sync, always_sync, no_batch]),
    ?check_trace(
        #{timetrap => 6_000},
        begin
            ConnName = <<"fallback">>,
            Ctx = fallback_actions_basic_setup(
                ConnName, ChanQueryMode, CallbackMode, ShouldBatch
            ),

            %% Sync
            ct:pal("sync query mode"),
            Results1 = fallback_send_many(sync, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),
            lists:foreach(
                fun(Res) ->
                    ?assertEqual({error, {unrecoverable_error, fallback_time}}, Res)
                end,
                Results1
            ),

            %% Async
            ct:pal("async query mode"),
            Results2 = fallback_send_many(async, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),
            lists:foreach(
                fun(Res) ->
                    ?assertEqual({error, {unrecoverable_error, fallback_time}}, Res)
                end,
                Results2
            ),

            ok
        end,
        []
    ),
    ok.

t_fallback_actions_overflow() ->
    [{matrix, true}].
t_fallback_actions_overflow(matrix) ->
    [
        [sync, always_sync, no_batch],
        [sync, always_sync, batch],
        [async, async_if_possible, no_batch],
        [async, async_if_possible, batch]
    ];
t_fallback_actions_overflow(Config) when is_list(Config) ->
    [ChanQueryMode, CallbackMode, ShouldBatch] =
        group_path(Config, [sync, always_sync, no_batch]),
    ?check_trace(
        begin
            ConnName = <<"fallback_overflow">>,
            %% Always overflows
            Opts = #{
                chan_resource_opts_override => #{
                    max_buffer_bytes => 0,
                    buffer_seg_bytes => 0
                }
            },
            Ctx0 = fallback_actions_basic_setup(
                ConnName, ChanQueryMode, CallbackMode, ShouldBatch, Opts
            ),
            Ctx = Ctx0#{assert_expected_callmode => false},
            Results = fallback_send_many(sync, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),
            lists:foreach(
                fun(Res) ->
                    ?assertEqual({error, buffer_overflow}, Res)
                end,
                Results
            ),
            ok
        end,
        []
    ),
    ok.

t_fallback_actions_overflow_internal_buffer() ->
    [{matrix, true}].
t_fallback_actions_overflow_internal_buffer(matrix) ->
    [[simple_sync_internal_buffer], [simple_async_internal_buffer]];
t_fallback_actions_overflow_internal_buffer(Config) when is_list(Config) ->
    [ChanQueryMode] = group_path(Config, [simple_sync_internal_buffer]),
    ShouldBatch = no_batch,
    CallbackMode = async_if_possible,
    ?check_trace(
        begin
            ConnName = <<"fallback_overflow_internal_buffer">>,
            Opts = #{
                fallback_fn => fun(#{reply_fn := ReplyFn}) ->
                    Pid = apply_after(
                        100,
                        fun() ->
                            emqx_resource:apply_reply_fun(ReplyFn, {error, buffer_overflow})
                        end
                    ),
                    {ok, Pid}
                end
            },
            Ctx0 = fallback_actions_basic_setup(
                ConnName, ChanQueryMode, CallbackMode, ShouldBatch, Opts
            ),
            Ctx = Ctx0#{assert_expected_callmode => true},

            ct:pal("sync query kind"),
            fallback_send_many(sync, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),

            ct:pal("async query kind"),
            fallback_send_many(async, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),

            ok
        end,
        fun(Trace) ->
            %% Sync queries for internal buffer resources use async version under the
            %% hood.
            case ChanQueryMode of
                simple_sync_internal_buffer ->
                    ?assertMatch([_], ?of_kind(simple_sync_internal_buffer_query, Trace));
                simple_async_internal_buffer ->
                    ok
            end,
            ?assertMatch([_, _], ?of_kind(simple_async_internal_buffer_query, Trace)),
            ?assertMatch([_, _], ?of_kind("buffer_worker_internal_buffer_async_overflow", Trace)),
            ok
        end
    ),
    ok.

%% Tests that fallback actions are triggered when (all) requests expire.
t_fallback_actions_expired() ->
    [{matrix, true}].
t_fallback_actions_expired(matrix) ->
    [
        [sync, always_sync, no_batch],
        [sync, always_sync, batch],
        [async, async_if_possible, no_batch],
        [async, async_if_possible, batch]
    ];
t_fallback_actions_expired(Config) when is_list(Config) ->
    [ChanQueryMode, CallbackMode, ShouldBatch] =
        group_path(Config, [sync, always_sync, no_batch]),
    ?check_trace(
        begin
            ConnName = <<"fallback_expired">>,
            Ctx0 = fallback_actions_basic_setup(
                ConnName, ChanQueryMode, CallbackMode, ShouldBatch
            ),
            %% Always expires
            QueryOpts = #{timeout => 0},
            Ctx = Ctx0#{
                assert_expected_callmode => false,
                query_opts => QueryOpts
            },
            %% Async so we may set a `reply_to' that receives late replies and expired
            %% results.
            Results = fallback_send_many(async, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),
            lists:foreach(
                fun(Res) ->
                    ?assertEqual({error, request_expired}, Res)
                end,
                Results
            ),
            ok
        end,
        []
    ),
    ok.

%% Tests that fallback actions are triggered when a batch request partially expires
%% before actually calling the resource.
t_fallback_actions_partially_expired_batch_before() ->
    [{matrix, true}].
t_fallback_actions_partially_expired_batch_before(matrix) ->
    [
        [sync, always_sync],
        [async, async_if_possible]
    ];
t_fallback_actions_partially_expired_batch_before(Config) when is_list(Config) ->
    [ChanQueryMode, CallbackMode] =
        group_path(Config, [sync, always_sync]),
    ShouldBatch = batch,
    ?check_trace(
        begin
            ConnName = <<"fallback_expired_batch_before">>,
            Ctx0 =
                #{n_reqs := NReqs} = fallback_actions_basic_setup(
                    ConnName, ChanQueryMode, CallbackMode, ShouldBatch
                ),
            NReqsToTimeout = 7,
            AdjustQueryOptsFn = fun(#{req_n := N}, QueryOpts) ->
                case N > NReqsToTimeout of
                    true -> QueryOpts;
                    false -> QueryOpts#{timeout => 0}
                end
            end,
            Ctx = Ctx0#{
                assert_expected_callmode => false,
                adjust_query_opts => AdjustQueryOptsFn,
                fallback_fn => fun(_Ctx) -> ok end
            },
            %% Async so we may set a `reply_to' that receives late replies and expired
            %% results.
            Results = fallback_send_many(async, Ctx),
            %% Requests 8, 9, ... will succeed
            assert_all_queries_triggered_fallbacks(Ctx#{n_reqs := NReqsToTimeout}),
            lists:foreach(
                fun
                    ({N, Res}) when N =< NReqsToTimeout ->
                        ?assertEqual({error, request_expired}, Res);
                    ({_N, Res}) ->
                        ?assertEqual(ok, Res)
                end,
                lists:enumerate(Results)
            ),
            {NReqs, NReqsToTimeout}
        end,
        fun({NReqs, NReqsToTimeout}, Trace) ->
            ?assertMatch(
                [
                    #{
                        expired := Expired,
                        not_expired := NotExpired
                    }
                ] when
                    length(Expired) =:= NReqsToTimeout andalso
                        length(NotExpired) =:= NReqs - NReqsToTimeout,
                ?of_kind(buffer_worker_flush_potentially_partial, Trace)
            ),
            ok
        end
    ),
    ok.

%% Tests that fallback actions are triggered when a (batch) request partially expires
%% after calling the resource and receiving a too late async reply.
t_fallback_actions_late_reply() ->
    [{matrix, true}].
t_fallback_actions_late_reply(matrix) ->
    [
        [batch],
        [no_batch]
    ];
t_fallback_actions_late_reply(Config) when is_list(Config) ->
    [ShouldBatch] =
        group_path(Config, [batch]),
    ChanQueryMode = async,
    CallbackMode = async_if_possible,
    ?check_trace(
        begin
            ConnName = <<"fallback_expired_batch_late">>,
            Ctx0 =
                #{
                    n_reqs := NReqs,
                    chan_resource_opts := #{batch_time := BatchTime}
                } = fallback_actions_basic_setup(
                    ConnName, ChanQueryMode, CallbackMode, ShouldBatch
                ),
            NReqsToTimeout = min(7, NReqs),
            Timeout = max(round(BatchTime * 1.2), 100),
            AdjustQueryOptsFn = fun(#{req_n := N}, QueryOpts) ->
                case N > NReqsToTimeout of
                    true ->
                        QueryOpts#{timeout => infinity};
                    false ->
                        QueryOpts#{timeout => Timeout}
                end
            end,
            Ctx = Ctx0#{
                assert_expected_callmode => false,
                adjust_query_opts => AdjustQueryOptsFn,
                fallback_fn => fun(#{req_n := N}) ->
                    case N =:= 1 of
                        true ->
                            %% Make one request hold the batch so others may expire
                            ct:sleep(Timeout * 2);
                        false ->
                            ok
                    end,
                    ok
                end
            },
            %% Async so we may set a `reply_to' that receives late replies and expired
            %% results.
            Results = fallback_send_many(async, Ctx),
            %% Requests 8, 9, ... will succeed
            assert_all_queries_triggered_fallbacks(Ctx#{n_reqs := NReqsToTimeout}),
            lists:foreach(
                fun
                    ({N, Res}) when N =< NReqsToTimeout ->
                        ?assertEqual({error, late_reply}, Res);
                    ({_N, Res}) ->
                        ?assertEqual(ok, Res)
                end,
                lists:enumerate(Results)
            ),
            {NReqs, NReqsToTimeout}
        end,
        fun({NReqs, NReqsToTimeout}, Trace) ->
            NSuccess = NReqs - NReqsToTimeout,
            case ShouldBatch of
                batch ->
                    ?assertMatch(
                        [
                            #{
                                inflight_count := 1,
                                num_inflight_messages := NSuccess
                            }
                        ],
                        ?of_kind(handle_async_reply_partially_expired, Trace)
                    );
                no_batch ->
                    ?assertMatch([_], ?of_kind(handle_async_reply_expired, Trace))
            end,
            ok
        end
    ),
    ok.

t_fallback_actions_expired_internal_buffer() ->
    [{matrix, true}].
t_fallback_actions_expired_internal_buffer(matrix) ->
    [[simple_sync_internal_buffer], [simple_async_internal_buffer]];
t_fallback_actions_expired_internal_buffer(Config) when is_list(Config) ->
    [ChanQueryMode] = group_path(Config, [simple_sync_internal_buffer]),
    ShouldBatch = no_batch,
    CallbackMode = async_if_possible,
    ?check_trace(
        begin
            ConnName = <<"fallback_expired_internal_buffer">>,
            Opts = #{
                fallback_fn => fun(_Ctx) -> {error, request_expired} end
            },
            Ctx0 = fallback_actions_basic_setup(
                ConnName, ChanQueryMode, CallbackMode, ShouldBatch, Opts
            ),
            Ctx = Ctx0#{assert_expected_callmode => true},

            ct:pal("sync query kind"),
            fallback_send_many(sync, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),

            ct:pal("async query kind"),
            fallback_send_many(async, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),

            ok
        end,
        fun(Trace) ->
            %% Sync queries for internal buffer resources use async version under the
            %% hood.
            case ChanQueryMode of
                simple_sync_internal_buffer ->
                    ?assertMatch([_], ?of_kind(simple_sync_internal_buffer_query, Trace));
                simple_async_internal_buffer ->
                    ok
            end,
            ?assertMatch([_, _], ?of_kind(simple_async_internal_buffer_query, Trace)),
            ?assertMatch([_, _], ?of_kind("buffer_worker_internal_buffer_async_expired", Trace)),
            ok
        end
    ),
    ok.

%% When a resource/channel is an "unhealthy target", messages do not even reach the
%% buffering layer, where most of the fallback actions logic live.  So we have to deal
%% with fallback actions outside it.
t_fallback_actions_unhealthy_target(_Config) ->
    ConnName = <<"fallback_actions_unhealthy_target">>,
    ChanQueryMode = sync,
    CallbackMode = always_sync,
    ShouldBatch = no_batch,
    ?check_trace(
        begin
            AgentState0 = #{
                resource_health_check => ?status_connected,
                channel_health_check =>
                    {?status_disconnected, {unhealthy_target, <<"something is wrong">>}}
            },
            {ok, HCAgent} = emqx_utils_agent:start_link(AgentState0),
            AdjustConnConfigFn = fun(Cfg) ->
                Cfg#{health_check_agent => HCAgent}
            end,
            Opts = #{adjust_conn_config_fn => AdjustConnConfigFn},
            Ctx0 = fallback_actions_basic_setup(
                ConnName, ChanQueryMode, CallbackMode, ShouldBatch, Opts
            ),
            Ctx = Ctx0#{assert_expected_callmode => false},
            ConnResId = maps:get(conn_res_id, Ctx),
            ChanId = maps:get(chan_id, Ctx),
            ?assertMatch(
                #{
                    status := ?status_disconnected,
                    error := {unhealthy_target, <<"something is wrong">>}
                },
                emqx_resource:channel_health_check(ConnResId, ChanId)
            ),
            Results1 = fallback_send_many(sync, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),
            ?assertMatch([?RESOURCE_ERROR(unhealthy_target)], Results1),

            _ = emqx_utils_agent:get_and_update(HCAgent, fun(Old) ->
                {unused, Old#{
                    channel_health_check :=
                        {?status_disconnected, unhealthy_target}
                }}
            end),
            ?retry(
                200,
                10,
                ?assertMatch(
                    #{
                        status := ?status_disconnected,
                        error := unhealthy_target
                    },
                    emqx_resource:channel_health_check(ConnResId, ChanId)
                )
            ),
            Results2 = fallback_send_many(sync, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),
            ?assertMatch([?RESOURCE_ERROR(unhealthy_target)], Results2),

            _ = emqx_utils_agent:get_and_update(HCAgent, fun(Old) ->
                {unused, Old#{
                    %% Not entirely sure if this path is reached in reality, but adding this case
                    %% to cover the corresponding code path in
                    %% `emqx_resource_buffer_worker:call_query`.
                    resource_health_check := {?status_connecting, unhealthy_target},

                    channel_health_check := ?status_connected
                }}
            end),
            ?retry(
                200,
                10,
                ?assertMatch(
                    %% Connecting, because connector is not connected
                    #{status := ?status_connecting},
                    emqx_resource:channel_health_check(ConnResId, ChanId)
                )
            ),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {ok, ?status_connecting},
                    emqx_resource:health_check(ConnResId)
                )
            ),
            Results3 = fallback_send_many(sync, Ctx),
            assert_all_queries_triggered_fallbacks(Ctx),
            ?assertMatch([{error, {unrecoverable_error, unhealthy_target}}], Results3),

            ok
        end,
        []
    ),
    ok.

%% Checks that, if one removal from the state fails, it's periodically retried until it
%% succeeds.  Adding the same `ChannelId' again should cancel such retries.
t_retry_remove_channel(_Config) ->
    ConnResId = <<"connector:ctype:c">>,
    TestPid = self(),
    {ok, Agent} = emqx_utils_agent:start_link(#{
        remove_channel => [
            {notify, TestPid, {error, oops}},
            {ask, TestPid}
        ]
    }),
    ?check_trace(
        #{timetrap => 15_000},
        begin
            {ok, _} =
                create(
                    ConnResId,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{
                        name => test_resource,
                        remove_channel_agent => Agent
                    },
                    #{
                        health_check_interval => 100,
                        start_timeout => 100
                    }
                ),
            ChanId = <<"action:atype:aname:", ConnResId/binary>>,
            ct:pal("testing async retries"),
            ok =
                add_channel(
                    ConnResId,
                    ChanId,
                    #{health_check_delay => 500}
                ),
            ok = remove_channel_async(ConnResId, ChanId),
            RetryTimeout = 2_000,
            Timeout = RetryTimeout + 1_000,
            %% Should then retry asynchronously
            receive
                {waiting_remove_channel_result, Alias1, ConnResId, ChanId} ->
                    ct:pal("l. ~b : attempted once", [?LINE]),
                    Alias1 ! {Alias1, {error, oops_again}}
            after Timeout -> ct:fail("didn't retry removing channel async")
            end,
            ?assertMatch(#{ChanId := _}, emqx_resource_manager:get_channel_configs(ConnResId)),
            %% Now should try again, and we'll let it succeed.
            receive
                {waiting_remove_channel_result, Alias2, ConnResId, ChanId} ->
                    ct:pal("l. ~b : attempted twice", [?LINE]),
                    Alias2 ! {Alias2, continue}
            after Timeout -> ct:fail("didn't retry removing channel async 2")
            end,
            ?assertEqual(#{}, emqx_resource_manager:get_channel_configs(ConnResId)),
            %% Now, let's see that adding the channel again while it's retrying to remove should
            %% cancel such timer.
            ct:pal("mailbox:\n  ~p", [?drainMailbox()]),
            ct:pal(asciiart:visible($%, "testing timer cancellation", [])),
            emqx_utils_agent:get_and_update(Agent, fun(_Old) ->
                {unused, #{remove_channel => {notify, TestPid, {error, oops_yet_again}}}}
            end),
            ct:pal("adding channel"),
            {ok, {ok, _}} =
                ?wait_async_action(
                    add_channel(
                        ConnResId,
                        ChanId,
                        #{health_check_delay => 500}
                    ),
                    #{?snk_kind := added_channel}
                ),
            ?assertMatch(#{ChanId := _}, emqx_resource_manager:get_channel_configs(ConnResId)),
            ct:pal("removing channel async"),
            ok = remove_channel_async(ConnResId, ChanId),
            ct:pal("waiting for attempt"),
            ?assertReceive({attempted_to_remove_channel, ConnResId, ChanId}, Timeout),
            ?assertMatch(#{ChanId := _}, emqx_resource_manager:get_channel_configs(ConnResId)),
            %% Now add again while the timer is going on.
            ct:pal("adding channel again"),
            ok =
                add_channel(
                    ConnResId,
                    ChanId,
                    #{health_check_delay => 500}
                ),
            %% Should stop trying to remove
            ?assertNotReceive({attempted_to_remove_channel, ConnResId, ChanId}, Timeout),
            ?assertMatch(#{ChanId := _}, emqx_resource_manager:get_channel_configs(ConnResId)),
            ok
        end,
        []
    ),
    ok.

%% Checks that, if a channel is already installed, and we receive a remove and an add
%% request (as happens in an config update operation), we do update the channel by
%% removing and re-adding it with the new config to the resource state.
t_compress_channel_operations_existing_channel() ->
    [{matrix, true}].
t_compress_channel_operations_existing_channel(matrix) ->
    [[connected], [connecting], [disconnected]];
t_compress_channel_operations_existing_channel(Config) when is_list(Config) ->
    [ConnectorStatus] = group_path(Config, [connected]),
    TestPid = self(),
    ?check_trace(
        begin
            ConnResId = <<"connector:ctype:c">>,
            {ok, ChanAgent} = emqx_utils_agent:start_link(#{
                add_channel => [
                    continue,
                    {notify, TestPid, continue}
                ],
                remove_channel => [
                    {notify, TestPid, {error, oops}},
                    {notify, TestPid, continue}
                ]
            }),
            AgentState0 = #{
                resource_health_check => connected,
                channel_health_check => connected
            },
            {ok, HCAgent} = emqx_utils_agent:start_link(AgentState0),
            ?tpal("creating connector resource"),
            {ok, _} =
                create(
                    ConnResId,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{
                        name => test_resource,
                        remove_channel_agent => ChanAgent,
                        add_channel_agent => ChanAgent,
                        health_check_agent => HCAgent
                    },
                    #{
                        health_check_interval => 100,
                        start_timeout => 100
                    }
                ),
            ?assertMatch({ok, connected}, emqx_resource:health_check(ConnResId)),

            ChanId = action_res_id(ConnResId),

            ?tpal("creating channel"),
            HCInterval1 = 500,
            ChanConfig1 = #{
                enable => true,
                resource_opts => #{health_check_interval => HCInterval1}
            },
            ok = add_channel(ConnResId, ChanId, ChanConfig1),
            ?assertMatch(
                #{status := connected, error := undefined},
                emqx_resource:channel_health_check(ConnResId, ChanId)
            ),
            ?assertEqual(
                #{ChanId => ChanConfig1},
                emqx_resource_manager:get_channel_configs(ConnResId)
            ),
            ?assertEqual(
                {ok, [{ChanId, ChanConfig1}]},
                emqx_resource_manager:get_channels(ConnResId)
            ),

            %% Set desired connector resource status
            ?tpal("changing connector status"),
            _ = emqx_utils_agent:get_and_update(HCAgent, fun(Old) ->
                {unused, Old#{resource_health_check := ConnectorStatus}}
            end),
            ?retry(
                100, 10, ?assertMatch({ok, ConnectorStatus}, emqx_resource:health_check(ConnResId))
            ),

            %% We suspend the process so operations are stacked on its mailbox.
            ?tpal("suspending resource manager process"),
            Pid = emqx_resource_manager:where(ConnResId),
            ok = sys:suspend(Pid),

            %% We mock the module so we may count the calls to add/remove channel
            %% callbacks.
            on_exit(fun() -> meck:unload() end),
            ok = meck:new(?TEST_RESOURCE, [passthrough]),

            %% Stack config update ops in its mailbox
            ?tpal("sending async channel operations"),
            ok = remove_channel_async(ConnResId, ChanId),
            HCInterval2 = HCInterval1 + 200,
            ChanConfig2 = emqx_utils_maps:deep_put(
                [resource_opts, health_check_interval],
                ChanConfig1,
                HCInterval2
            ),
            ok = add_channel_async(ConnResId, ChanId, ChanConfig2),

            %% Restore connected status to connector so that it may actually modify
            %% internal state, if not updated yet.
            _ = emqx_utils_agent:get_and_update(HCAgent, fun(Old) ->
                {unused, Old#{resource_health_check := connected}}
            end),
            %% Resume and make a call so that messages are processed
            ?tpal("changing status and resuming resource manager process"),
            ok = sys:resume(Pid),
            ?retry(
                100,
                10,
                ?assertMatch(
                    {ok, connected},
                    emqx_resource:health_check(ConnResId)
                )
            ),

            %% Related to retry operation interval
            ?tpal("waiting for channel removal attempts"),
            Timeout = 3_000,
            %% Should have tried at least once to remove...
            ?assertReceive({attempted_to_remove_channel, ConnResId, ChanId}, Timeout),
            %% ... and later succeeded (only retries if resource doesn't to disconnected,
            %% in which case it'll attempt to remove only once)
            case ConnectorStatus of
                disconnected ->
                    ok;
                _ ->
                    ?assertReceive({attempted_to_remove_channel, ConnResId, ChanId}, Timeout)
            end,
            ?assertReceive({attempted_to_add_channel, ConnResId, ChanId, ChanConfig2}, Timeout),

            %% We must eventually observe the new configuration in the channel.
            ?tpal("checking channels configs"),
            ?retry(
                200,
                10,
                ?assertEqual(
                    #{ChanId => ChanConfig2},
                    emqx_resource_manager:get_channel_configs(ConnResId)
                )
            ),
            ?retry(
                200,
                10,
                ?assertEqual(
                    {ok, [{ChanId, ChanConfig2}]},
                    emqx_resource_manager:get_channels(ConnResId)
                )
            ),

            ChanConfig2
        end,
        [
            log_consistency_prop(),
            fun(FinalConfig, _Trace) ->
                History = meck:history(?TEST_RESOURCE),
                Calls = [
                    {Fn, Args, Res}
                 || {_Pid, {?TEST_RESOURCE, Fn, Args}, Res} <- History,
                    lists:member(Fn, [on_add_channel, on_remove_channel])
                ],
                case ConnectorStatus of
                    disconnected ->
                        %% Depending on whether the channel operations are performed while
                        %% still disconnected or when the resource reconnects, we may not
                        %% see the first removal (for the latter case).
                        case Calls of
                            [
                                {on_remove_channel, _, {error, _}},
                                {on_add_channel, [_ConnResId, _ConnState, _ChanId, FinalConfig], _}
                            ] ->
                                ok;
                            [
                                {on_remove_channel, _, {error, _}},
                                %% Retry
                                {on_remove_channel, _, {ok, _}},
                                {on_add_channel, [_ConnResId, _ConnState, _ChanId, FinalConfig], _}
                            ] ->
                                ok;
                            _ ->
                                error({"unexpected (disconnected) history", Calls})
                        end;
                    _ ->
                        ?assertMatch(
                            [
                                {on_remove_channel, _, {error, _}},
                                %% Retry
                                {on_remove_channel, _, {ok, _}},
                                {on_add_channel, [_ConnResId, _ConnState, _ChanId, ChanConfig], _}
                            ] when ChanConfig == FinalConfig,
                            Calls,
                            #{final_config => FinalConfig}
                        )
                end,
                ok
            end
        ]
    ),
    ok.

%% Verifies that we do not perform connector resource health checks while in the
%% `disconnected' state, so that a manual health check doesn't trigger an undue state
%% change.
t_health_check_while_resource_is_disconnected(_Config) ->
    ?check_trace(
        #{timetrap => 10_000},
        begin
            %% 1) Resource will start successfully and enter connected state.
            ConnResId = <<"connector:ctype:c">>,
            TestPid = self(),
            StartAgentState0 = #{
                start_resource => [
                    continue,
                    {notify, TestPid, {error, <<"fail once">>}},
                    {ask, TestPid},
                    continue
                ]
            },
            HCAgentState = #{
                resource_health_check => [
                    {notify, TestPid, ?status_connected},
                    {notify, TestPid, ?status_disconnected},
                    {ask, TestPid},
                    {notify, TestPid, ?status_connected}
                ]
            },
            {ok, HCAgent} = emqx_utils_agent:start_link(HCAgentState),
            {ok, StartAgent} = emqx_utils_agent:start_link(StartAgentState0),
            ?tpal("creating connector resource"),
            {ok, _} =
                create(
                    ConnResId,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{
                        name => test_resource,
                        start_resource_agent => StartAgent,
                        health_check_agent => HCAgent
                    },
                    #{
                        health_check_interval => 100,
                        start_timeout => 100,
                        spawn_buffer_workers => false
                    }
                ),
            ?tpal("waiting for first (successful) resource health check"),
            ?assertReceive({returning_resource_health_check_result, ConnResId, ?status_connected}),

            %% 2) The resource then enters the disconnected state.  It'll try to restart at
            %%    least once and fail.  Note that this may destroy some resources related
            %%    to the connector state.  For example, calling `emqx_resource_pool:start'
            %%    using an already started pool will make it stop such pool before
            %%    restarting.  If the restart then fails, we are left with no pool.
            ?tpal("waiting connector to be disconnected"),
            ?assertReceive(
                {returning_resource_health_check_result, ConnResId, ?status_disconnected}
            ),
            ?tpal("waiting connector to attempt and fail to restart"),
            receive
                {attempted_to_start_resource, ConnResId, {error, <<"fail once">>}} ->
                    ok
            after 500 ->
                ct:fail("l. ~b: did not attempt a failed restart!", [?LINE])
            end,

            %% 3) While the resource manager process does not trigger health checks itself
            %%    while `disconnected', an external caller might manyally trigger them.  In
            %%    this case, this check will later return `connected', but before the
            %%    resource has time to successfully restart its inner resource.
            %%
            %%    If we let this happen, the health check might return `connected', and the
            %%    resource will incorrectly enter the `connected' state, without actually
            %%    calling `on_start' and hence becoming corrupted (e.g. no pool).
            %%
            %%    After fixing the original issue (performing the health check while
            %%    disconnected), manually calling the health check should return
            %%    `disconnected' (the current state) before it's able to initialize its
            %%    state.  Once initialized, it'll do its own normal health checks.
            ?tpal("attempting manual resource health checks"),
            ?assertMatch({ok, ?status_disconnected}, emqx_resource:health_check(ConnResId)),
            ?assertMatch({ok, ?status_disconnected}, emqx_resource:health_check(ConnResId)),
            %% Should not attempt health check before starting
            ?assertNotReceive({waiting_health_check_result, _Alias, resource, ConnResId}),
            ?tpal("waiting for resource to attempt another restart, which will succeed"),
            {waiting_start_resource_result, Alias1, ConnResId} =
                ?assertReceive({waiting_start_resource_result, _, ConnResId}),
            Alias1 ! {Alias1, continue},
            {waiting_health_check_result, Alias2, resource, ConnResId} =
                ?assertReceive({waiting_health_check_result, _Alias, resource, ConnResId}),
            Alias2 ! {Alias2, ?status_connected},
            ?tpal("now it should return to normal"),
            ?assertReceive({returning_resource_health_check_result, ConnResId, ?status_connected}),
            ?assertMatch({ok, ?status_connected}, emqx_resource:health_check(ConnResId)),

            ok
        end,
        [log_consistency_prop()]
    ),
    ok.

%% Checks that we impose a timeout on resource (connector) health checks.
t_resource_health_check_timeout(_Config) ->
    ?check_trace(
        begin
            TestPid = self(),
            %% Succeed at first, and then hang indefinitely
            HCAgentState = #{
                resource_health_check => [
                    ?status_connected,
                    {ask, TestPid}
                ]
            },
            {ok, HCAgent} = emqx_utils_agent:start_link(HCAgentState),
            %% Same thing for start: we want the restarts to fail.  Otherwise, it'll enter
            %% `?status_connecting` when it attempts to restart and succeed after being
            %% `?status_disconnected`.
            StartAgentState0 = #{
                start_resource => [
                    continue,
                    {ask, TestPid},
                    {error, <<"won't restart!">>}
                ]
            },
            {ok, StartAgent} = emqx_utils_agent:start_link(StartAgentState0),
            ConnName = atom_to_binary(?FUNCTION_NAME),
            ConnResId = connector_res_id(ConnName),
            {ok, _} =
                create(
                    ConnResId,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{
                        name => test_resource,
                        start_resource_agent => StartAgent,
                        health_check_agent => HCAgent
                    },
                    #{
                        health_check_interval => 100,
                        health_check_timeout => 750,
                        start_timeout => 100
                    }
                ),
            %% Immediately after creatin is `?status_connected`
            ?assertMatch(
                {ok, _, #{status := ?status_connected}},
                emqx_resource_manager:lookup_cached(ConnResId)
            ),
            %% After that, HC will hang, but abort after timeout.
            ?assertReceive({waiting_health_check_result, _Alias, resource, ConnResId}, 1_000),
            %% It'll then attempt to restart.  We want it to fail so it doesn't become
            %% `?status_connecting`.
            {waiting_start_resource_result, Alias, ConnResId} =
                ?assertReceive({waiting_start_resource_result, _, ConnResId}),
            Alias ! {Alias, {error, <<"won't restart!">>}},
            %% After those failures, status should be `?status_disconnected`
            ?assertMatch(
                {ok, _, #{
                    status := ?status_disconnected,
                    error := <<"resource_health_check_timed_out">>
                }},
                emqx_resource_manager:lookup_cached(ConnResId)
            ),
            ok
        end,
        [log_consistency_prop()]
    ),
    ok.

%% Checks that we impose a timeout on channel (action/source) health checks.
t_channel_health_check_timeout(_Config) ->
    ?check_trace(
        begin
            TestPid = self(),
            ConnName = atom_to_binary(?FUNCTION_NAME),
            ConnResId = connector_res_id(ConnName),
            HCAgentState = #{
                resource_health_check => ?status_connected,
                %% Succeed at first, and then hang indefinitely
                channel_health_check => [
                    ?status_connected,
                    {ask, TestPid}
                ]
            },
            {ok, HCAgent} = emqx_utils_agent:start_link(HCAgentState),
            ConnName = atom_to_binary(?FUNCTION_NAME),
            ConnResId = connector_res_id(ConnName),
            {ok, _} =
                create(
                    ConnResId,
                    ?DEFAULT_RESOURCE_GROUP,
                    ?TEST_RESOURCE,
                    #{
                        name => test_resource,
                        health_check_agent => HCAgent
                    },
                    #{
                        health_check_interval => 100,
                        start_timeout => 100
                    }
                ),
            ChanId = action_res_id(ConnResId),
            ok =
                add_channel(
                    ConnResId,
                    ChanId,
                    #{
                        resource_opts => #{
                            health_check_interval => 100,
                            health_check_timeout => 750
                        }
                    }
                ),
            %% Immediately after creatin is `?status_connected`
            ?assertMatch(
                {ok, #rt{channel_status = ?status_connected}},
                emqx_resource_cache:get_runtime(ChanId)
            ),
            %% After that, HC will hang, but abort after timeout.  Since the
            %% resource/connector is healthy, this should retry.
            ?assertReceive(
                {waiting_health_check_result, _Alias, channel, ConnResId, ChanId}, 1_000
            ),
            ?assertReceive(
                {waiting_health_check_result, _Alias, channel, ConnResId, ChanId}, 1_000
            ),
            %% After those failures, status should be `?status_disconnected`
            ?assertMatch(
                {ok, #rt{channel_status = ?status_disconnected}},
                emqx_resource_cache:get_runtime(ChanId)
            ),
            ok
        end,
        [log_consistency_prop()]
    ),
    ok.
