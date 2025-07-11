%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../src/internals.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% This testcase verifies lazy initialization sequence of the
%% application. It is expected that the application is started without
%% creating any durable databases until the first timer type is
%% registered.
%%
%% Additionally, this testcase doesn't create any timer data, so it's
%% used to verify that the logic handles lack of any timers.
t_lazy_initialization(Config) ->
    Env = [{heartbeat_interval, 1_000}],
    Cluster = cluster(?FUNCTION_NAME, Config, 1, Env),
    ?check_trace(
        #{timetrap => 15_000},
        try
            [Node] = emqx_cth_cluster:start(Cluster),
            %% Application is started but dormant. Databases don't exist yet:
            ?ON(
                Node,
                begin
                    {DBs, _} = lists:unzip(emqx_ds:which_dbs()),
                    ?defer_assert(?assertNot(lists:member(?DB_GLOB, DBs)))
                end
            ),
            %%
            %% Now let's register a timer handler. This should
            %% activate the application. Expect: node adds its
            %% metadata to the tables.
            %%
            ?assertMatch(
                ok,
                ?ON(Node, emqx_durable_test_timer:init())
            ),
            %% Verify that the node has opened the DB:
            ?ON(
                Node,
                begin
                    {DBs, _} = lists:unzip(emqx_ds:which_dbs()),
                    ?defer_assert(?assert(lists:member(?DB_GLOB, DBs)))
                end
            ),
            %%
            %% The node should create a new epoch:
            %%
            wait_heartbeat(Node),
            Epoch1 = ?ON(
                Node, emqx_durable_timer:epoch()
            ),
            [{Epoch1, true, T1_1}] = ?ON(Node, emqx_durable_timer:ls_epochs(Node)),
            %% Also verify ls_epochs/0 API:
            ?assertMatch(
                [{Node, Epoch1, true, _}],
                ?ON(Node, emqx_durable_timer:ls_epochs())
            ),
            %% Verify that heartbeat is periodically incrementing:
            wait_heartbeat(Node),
            [{Epoch1, true, T1_2}] = ?ON(Node, emqx_durable_timer:ls_epochs(Node)),
            ?assert(is_integer(T1_1), T1_1),
            ?assert(is_integer(T1_2) andalso T1_2 > T1_1, T1_2),
            %%
            %% Restart the node. Expect that node will create a new
            %% epoch and close the old one.
            %%
            ?tp(test_restart_cluster, #{}),
            emqx_cth_cluster:restart(Cluster),
            ?ON(
                Node,
                ?assertMatch(ok, emqx_durable_test_timer:init())
            ),
            Epoch2 = ?ON(Node, emqx_durable_timer:epoch()),
            wait_heartbeat(Node),
            %% Restart complete.
            %% Verify that the new epoch has been created:
            [{Epoch1, false, T1_3}, {Epoch2, true, T2_1}] =
                ?ON(Node, emqx_durable_timer:ls_epochs(Node)),
            wait_heartbeat(Node),
            %% Heartbeat for Epoch1 stays the same and Epoch2 is ticking:
            [{Epoch1, false, T1_3}, {Epoch2, true, T2_2}] = ?ON(
                Node, emqx_durable_timer:ls_epochs(Node)
            ),
            ?assert(is_integer(T2_2) andalso T2_2 > T2_1, T2_2),
            %% Register a new type, verify that workers have been started:
            ?assertMatch(
                ok,
                ?ON(Node, emqx_durable_test_timer2:init())
            ),
            wait_heartbeat(Node),
            {Epoch1, Epoch2}
        after
            emqx_cth_cluster:stop(Cluster)
        end,
        [
            fun no_unexpected/1,
            fun no_read_conflicts/1,
            fun no_replay_failures/1,
            fun verify_timers/1,
            {"Workers lifetime", fun({Epoch1, Epoch2}, Trace) ->
                F = fun(#{kind := A}, #{kind := B}) -> A =< B end,
                %% Trace before and after restart:
                {T1, T23} = ?split_trace_at(#{?snk_kind := test_restart_cluster}, Trace),
                %% Trace before and after registration of the second type:
                {T2, T3} = ?split_trace_at(#{?snk_kind := ?tp_register, type := 16#ffffffff}, T23),
                ?assertMatch(
                    [#{type := 16#fffffffe, kind := active, epoch := Epoch1}],
                    ?projection(?snk_meta, ?of_kind(?tp_worker_started, T1))
                ),
                ?assertMatch(
                    [
                        #{type := 16#fffffffe, kind := active, epoch := Epoch2},
                        #{type := 16#fffffffe, kind := {closed, dead_hand}, epoch := Epoch1},
                        #{type := 16#fffffffe, kind := {closed, started}, epoch := Epoch1}
                    ],
                    lists:sort(F, ?projection(?snk_meta, ?of_kind(?tp_worker_started, T2)))
                ),
                %% Verify that workers have been spawned after type 2 was registered:
                ?assertMatch(
                    [
                        #{type := 16#ffffffff, kind := active, epoch := Epoch2},
                        #{type := 16#ffffffff, kind := {closed, dead_hand}, epoch := Epoch1},
                        #{type := 16#ffffffff, kind := {closed, started}, epoch := Epoch1}
                    ],
                    lists:sort(F, ?projection(?snk_meta, ?of_kind(?tp_worker_started, T3)))
                )
            end}
        ]
    ).

%% This testcase verifies normal operation of the timers when the
%% owner node is not restarted.
t_normal_execution(Config) ->
    Cluster = cluster(?FUNCTION_NAME, Config, 1, []),
    Type = emqx_durable_test_timer:durable_timer_type(),
    ?check_trace(
        #{timetrap => 15_000},
        try
            [Node] = emqx_cth_cluster:start(Cluster),
            ?assertMatch(ok, ?ON(Node, emqx_durable_test_timer:init())),
            %%
            %% Test a timer with 0 delay:
            ?wait_async_action(
                ?assertMatch(
                    ok,
                    ?ON(
                        Node,
                        emqx_durable_test_timer:apply_after(<<>>, <<>>, 0)
                    )
                ),
                #{?snk_kind := ?tp_fire, key := <<>>, val := <<>>},
                infinity
            ),
            %%
            %% Test a timer with 1s delay:
            ?wait_async_action(
                ?assertMatch(
                    ok,
                    ?ON(
                        Node,
                        emqx_durable_test_timer:apply_after(<<1>>, <<1>>, 1_000)
                    )
                ),
                #{?snk_kind := ?tp_fire, key := <<1>>, val := <<1>>},
                infinity
            ),
            %%
            %% Test overriding of timers:
            ?wait_async_action(
                ?ON(
                    Node,
                    begin
                        %% 1. Start a timer with key <<2>>
                        ok = emqx_durable_test_timer:apply_after(<<2>>, <<1>>, 500),
                        %% 2. Then override delay and value:
                        ok = emqx_durable_test_timer:apply_after(<<2>>, <<2>>, 1_000)
                    end
                ),
                #{?snk_kind := ?tp_fire, key := <<2>>, val := <<2>>},
                infinity
            ),
            %%
            %% Test overriding of dead hand:
            ?wait_async_action(
                ?ON(
                    Node,
                    begin
                        %% Derive topic name:
                        DHT = emqx_durable_timer_worker:dead_hand_topic(
                            Type, emqx_durable_timer:epoch(), '+'
                        ),
                        %% 1. Set up a dead hand timer with key <<3>>
                        ok = emqx_durable_test_timer:dead_hand(<<3>>, <<1>>, 0),
                        ?assertMatch([_], emqx_ds:dirty_read(?DB_GLOB, DHT)),
                        %% 2. Override it with a regular timer:
                        ok = emqx_durable_test_timer:apply_after(<<3>>, <<2>>, 100),
                        %% First version of the timer is gone:
                        ?assertMatch([], emqx_ds:dirty_read(?DB_GLOB, DHT))
                    end
                ),
                #{?snk_kind := ?tp_fire, key := <<3>>, val := <<2>>},
                infinity
            ),
            ct:sleep(1000),
            %% Verify that all timers have been GC'd:
            ?assertMatch(
                [],
                ?ON(
                    Node,
                    emqx_ds:dirty_read(
                        ?DB_GLOB, emqx_durable_timer_worker:dead_hand_topic(Type, '+', '+')
                    )
                )
            ),
            ?assertMatch(
                [],
                ?ON(
                    Node,
                    emqx_ds:dirty_read(
                        ?DB_GLOB, emqx_durable_timer_worker:started_topic(Type, '+', '+')
                    )
                )
            ),
            ok
        after
            emqx_cth_cluster:stop(Cluster)
        end,
        [
            fun no_unexpected/1,
            fun no_read_conflicts/1,
            fun no_replay_failures/1,
            fun verify_timers/1
        ]
    ).

%% This testcase verifies the functionality related to timer
%% cancellation.
t_cancellation(Config) ->
    Cluster = cluster(?FUNCTION_NAME, Config, 1, []),
    ?check_trace(
        #{timetrap => 30_000},
        try
            [Node] = emqx_cth_cluster:start(Cluster),
            ?assertMatch(ok, ?ON(Node, emqx_durable_test_timer:init())),
            %%
            %% 1. Test dead hand functionality. It's fairly trivial as
            %% long as the node doesn't restart: we just need to
            %% verify that the record is deleted.
            ?ON(
                Node,
                begin
                    DHT = emqx_durable_timer_worker:dead_hand_topic(
                        emqx_durable_test_timer:durable_timer_type(),
                        emqx_durable_timer:epoch(),
                        '+'
                    ),
                    emqx_durable_test_timer:dead_hand(<<0>>, <<0>>, 5_000),
                    ?assertMatch([_], emqx_ds:dirty_read(?DB_GLOB, DHT)),
                    %% Cancel it and verify that the record is deleted:
                    emqx_durable_test_timer:cancel(<<0>>),
                    ?assertMatch([], emqx_ds:dirty_read(?DB_GLOB, DHT))
                end
            ),
            %% 2. Test apply_after cancellation.
            Delay = 2_000,
            AAKey = <<1>>,
            ?ON(
                Node,
                begin
                    AAT = emqx_durable_timer_worker:started_topic(
                        emqx_durable_test_timer:durable_timer_type(),
                        emqx_durable_timer:epoch(),
                        '+'
                    ),
                    emqx_durable_test_timer:apply_after(AAKey, <<1>>, Delay),
                    ?assertMatch([_], emqx_ds:dirty_read(?DB_GLOB, AAT)),
                    %% Cancel it and verify that the record is deleted:
                    emqx_durable_test_timer:cancel(AAKey),
                    ?assertMatch([], emqx_ds:dirty_read(?DB_GLOB, AAT))
                end
            ),
            ct:sleep(2 * Delay)
        after
            emqx_cth_cluster:stop(Cluster)
        end,
        [
            fun no_unexpected/1,
            fun no_read_conflicts/1,
            fun no_replay_failures/1,
            fun verify_timers/1,
            fun(Trace) ->
                ?assertMatch([], ?of_kind(?tp_fire, Trace))
            end
        ]
    ).

%%------------------------------------------------------------------------------
%% Trace specs
%%------------------------------------------------------------------------------

%% Verify that the workes didn't have to retry fetching batches. This
%% property should hold as long as DS state is not degraded.
no_replay_failures(Trace) ->
    ?assertMatch([], ?of_kind(?tp_replay_failed, Trace)).

%% Verify that none of the workers encountered an unknwon message.
%% This should always hold.
no_unexpected(Trace) ->
    ?assertMatch([], ?of_kind(?tp_unknown_event, Trace)).

%% Active Timer State:
-record(ats, {
    val :: binary(),
    min_ts :: integer()
}).

%% Verifier State:
-record(vs, {
    active = #{},
    dead_hand = #{},
    stats = [],
    errors = []
}).

%% Verify arbitrary sequence of operations with the timers, node
%% restarts and side effects.
verify_timers(Trace) ->
    do_verify_timers(Trace, #vs{}).

do_verify_timers([], #vs{errors = Errors, stats = Stats}) ->
    case Stats of
        [] ->
            ok;
        _ ->
            Min = lists:min(Stats),
            Max = lists:max(Stats),
            Median = median(Stats),
            ct:pal(
                """
                Deviation statistics (ms):
                   Min:    ~p
                   Max:    ~p
                   Median: ~p
                """,
                [Min, Max, Median]
            )
    end,
    ?assertMatch([], Errors);
do_verify_timers(
    [TP = #{?snk_kind := Kind, ?snk_meta := #{time := TimeUs}} | Trace], S0
) ->
    TS = erlang:convert_time_unit(TimeUs, microsecond, millisecond),
    #vs{active = A0, dead_hand = DH0, errors = Err0, stats = Stats0} = S0,
    S =
        case Kind of
            ?tp_new_apply_after ->
                #{type := T, key := K, val := Val, delay := Delay} = TP,
                S0#vs{
                    active = A0#{{T, K} => #ats{val = Val, min_ts = Delay + TS}},
                    dead_hand = maps:remove(K, DH0)
                };
            ?tp_new_dead_hand ->
                #{type := T, key := K, val := ValGot, delay := Delay} = TP,
                S0#vs{
                    active = maps:remove({T, K}, A0),
                    dead_hand = DH0#{{T, K} => {ValGot, Delay}}
                };
            ?tp_delete ->
                #{type := T, key := K} = TP,
                S0#vs{
                    active = maps:remove({T, K}, A0),
                    dead_hand = maps:remove({T, K}, DH0)
                };
            ?tp_fire ->
                #{type := T, key := K, val := ValGot} = TP,
                case maps:take({T, K}, A0) of
                    {#ats{val = ValExpected, min_ts = MinTS}, A} ->
                        %% Check value:
                        Err1 =
                            case ValGot of
                                ValExpected ->
                                    Err0;
                                _ ->
                                    [
                                        #{
                                            '_' => value_mismath,
                                            k => K,
                                            expected => ValExpected,
                                            got => ValGot
                                        }
                                        | Err0
                                    ]
                            end,
                        %% Check delay:
                        Diff = TS - MinTS,
                        Err =
                            case Diff >= 0 of
                                true ->
                                    Err1;
                                false ->
                                    [
                                        #{
                                            '_' => fired_too_early,
                                            k => K,
                                            expected => MinTS,
                                            t => TS,
                                            v => ValGot
                                        }
                                        | Err1
                                    ]
                            end,
                        S0#vs{
                            active = A,
                            stats = [Diff | Stats0],
                            errors = Err
                        };
                    error ->
                        S0#vs{
                            errors = [{unexpected, TP} | Err0]
                        }
                end;
            _ ->
                S0
        end,
    do_verify_timers(
        Trace,
        S
    ).

%% This should hold as long as testcase doesn't issue operations that
%% update the same key in parallel.
no_read_conflicts(Trace) ->
    ?assertMatch(
        [],
        [E || E = #{?snk_kind := emqx_ds_tx_retry, reason := {read_conflict, _}} <- Trace]
    ).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

suite() -> [{timetrap, {minutes, 1}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_standalone_test() of
        false ->
            Config;
        true ->
            {skip, standalone_not_supported}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(TC, Config) ->
    %% emqx_cth_suite:clean_work_dir(emqx_cth_suite:work_dir(TC, Config)),
    ok.

cluster(TC, Config, Nnodes, Env) ->
    AppSpecs = [
        {emqx_durable_timer, #{
            override_env => Env,
            after_start => fun fix_logging/0
        }}
    ],
    NodeSpecs =
        [
            {list_to_atom("emqx_durable_timer_SUITE" ++ integer_to_list(I)), #{
                role => core,
                apps => AppSpecs
            }}
         || I <- lists:seq(1, Nnodes)
        ],
    emqx_cth_cluster:mk_nodespecs(
        NodeSpecs,
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ).

wait_heartbeat(Node) ->
    ?block_until(
        #{?snk_kind := ?tp_heartbeat, ?snk_meta := #{node := Node}}, infinity, 0
    ).

fix_logging() ->
    logger:set_primary_config(level, debug),
    logger:remove_handler(default),
    logger:add_handler(
        default,
        logger_std_h,
        #{
            config => #{file => "erlang.log", max_no_files => 1}
        }
    ).

median(L0) ->
    L = lists:sort(L0),
    N = length(L),
    case N rem 2 of
        1 ->
            lists:nth(N div 2 + 1, L);
        0 ->
            [A, B | _] = lists:nthtail(N div 2 - 1, L),
            (A + B) / 2
    end.
