%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_dashboard_monitor_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_dashboard_SUITE, [auth_header_/0]).
-import(emqx_common_test_helpers, [on_exit/1]).

-include("emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(SERVER, "http://127.0.0.1:18083").
-define(BASE_PATH, "/api/v5").

-define(BASE_RETAINER_CONF, <<
    "retainer {\n"
    "    enable = true\n"
    "    msg_clear_interval = 0s\n"
    "    msg_expiry_interval = 0s\n"
    "    max_payload_size = 1MB\n"
    "    flow_control {\n"
    "        batch_read_number = 0\n"
    "        batch_deliver_number = 0\n"
    "     }\n"
    "   backend {\n"
    "        type = built_in_database\n"
    "        storage_type = ram\n"
    "        max_retained_messages = 0\n"
    "     }\n"
    "}"
>>).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%--------------------------------------------------------------------
%% CT boilerplate
%%--------------------------------------------------------------------

all() ->
    [
        {group, common},
        {group, persistent_sessions}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    PSTCs = persistent_session_testcases(),
    [
        {common, [], AllTCs -- PSTCs},
        {persistent_sessions, [], PSTCs}
    ].

persistent_session_testcases() ->
    [
        t_persistent_session_stats
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(persistent_sessions = Group, Config) ->
    case emqx_ds_test_helpers:skip_if_norepl() of
        false ->
            Port = 18083,
            NodeSpecs = [
                {dashboard_monitor1, #{apps => cluster_node_appspec(true, Port)}},
                {dashboard_monitor2, #{apps => cluster_node_appspec(false, Port)}}
            ],
            Nodes = emqx_cth_cluster:start(
                NodeSpecs,
                #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
            ),
            [{cluster, Nodes} | Config];
        Yes ->
            Yes
    end;
init_per_group(common = Group, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_retainer, ?BASE_RETAINER_CONF},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(
                "dashboard.listeners.http { enable = true, bind = 18083 }\n"
                "dashboard.sample_interval = 1s"
            )
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    [{apps, Apps} | Config].

end_per_group(persistent_sessions, Config) ->
    Cluster = ?config(cluster, Config),
    emqx_cth_cluster:stop(Cluster),
    ok;
end_per_group(common, Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(t_smoke_test_monitor_multiple_windows = TestCase, Config) ->
    Port = 28083,
    NodeSpecs = [
        {smoke_multiple_windows1, #{apps => cluster_node_appspec(true, Port)}},
        {smoke_multiple_windows2, #{apps => cluster_node_appspec(false, Port)}}
    ],
    Nodes =
        [N1 | _] = emqx_cth_cluster:start(
            NodeSpecs,
            #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
        ),
    ok = snabbkaffe:start_trace(),
    [{nodes, Nodes}, {api_node, N1} | Config];
init_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    ct:timetrap({seconds, 30}),
    Config.

end_per_testcase(t_smoke_test_monitor_multiple_windows, Config) ->
    Nodes = ?config(nodes, Config),
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_empty_table(_Config) ->
    pause_monitor_process(),
    clean_data(),
    ?assertEqual({ok, []}, request(["monitor"], "latest=20000")).

t_pmap_nodes(_Config) ->
    MaxAge = timer:hours(1),
    Now = erlang:system_time(millisecond) - 1,
    Interval = emqx_dashboard_monitor:sample_interval(MaxAge),
    StartTs = round_down(Now - MaxAge, Interval),
    DataPoints = 5,
    clean_data(),
    LastVal = insert_data_points(DataPoints, fun sent_n/1, StartTs, Now),
    Nodes = [node(), node(), node()],
    %% this function calls emqx_utils:pmap to do the job
    Data0 = emqx_dashboard_monitor:sample_nodes(Nodes, StartTs),
    Data1 = emqx_dashboard_monitor:fill_gaps(Data0, StartTs),
    Data = emqx_dashboard_monitor:format(Data1),
    ok = check_sample_intervals(Interval, hd(Data), tl(Data)),
    ?assertEqual(LastVal * length(Nodes), maps:get(sent, lists:last(Data))).

t_inplace_downsample(_Config) ->
    clean_data(),
    %% -20s to ensure the oldest data point will not expire during the test
    SinceT = 7 * timer:hours(24) - timer:seconds(20),
    Total = 10_000,
    ConnectionGauge = 3,
    emqx_dashboard_monitor:randomize(Total, #{sent => 1, connections => ConnectionGauge}, SinceT),
    %% assert original data (before downsample)
    All0 = emqx_dashboard_monitor:all_data(),
    AllSent0 = lists:map(fun({_, #{sent := S}}) -> S end, All0),
    ?assertEqual(Total, lists:sum(AllSent0)),
    emqx_dashboard_monitor ! clean_expired,
    %% ensure downsample happened
    ok = gen_server:call(emqx_dashboard_monitor, dummy, infinity),
    All1 = emqx_dashboard_monitor:all_data(),
    All = drop_dummy_data_points(All1),
    %% check timestamps are not random after downsample
    ExpectedIntervals = [timer:minutes(10), timer:minutes(5), timer:minutes(1), timer:seconds(10)],
    ok = check_intervals(ExpectedIntervals, All),
    %% Gauges, such as `connections', are not summed.
    AllConnections = lists:map(fun({_Ts, #{connections := C}}) -> C end, All),
    DistinctConnections = lists:usort(AllConnections),
    ?assertEqual([ConnectionGauge], DistinctConnections),
    ok.

%% there might be some data points added while downsample is running
%% because the sampling interval during test is 1s, so they do not perfectly
%% match the expected intervals
%% this function is to dorp those dummy data points
drop_dummy_data_points(All) ->
    IsZeroValues = fun(Map) -> lists:all(fun(Value) -> Value =:= 0 end, maps:values(Map)) end,
    lists:filter(fun({_, Map}) -> not IsZeroValues(Map) end, All).

check_intervals(_, []) ->
    ok;
check_intervals([], All) ->
    throw({bad_intervals, All});
check_intervals([Interval | Rest], [{Ts, _} | RestData] = All) ->
    case (Ts rem Interval) =:= 0 of
        true ->
            check_intervals([Interval | Rest], RestData);
        false ->
            check_intervals(Rest, All)
    end.

t_randomize(_Config) ->
    clean_data(),
    emqx_dashboard_monitor:randomize(1, #{sent => 100}),
    Since = integer_to_list(7 * timer:hours(24)),
    {ok, Samplers} = request(["monitor"], "latest=" ++ Since),
    Count = lists:sum(lists:map(fun(#{<<"sent">> := S}) -> S end, Samplers)),
    ?assertEqual(100, Count).

t_downsample_7d(_Config) ->
    MaxAge = 7 * timer:hours(24),
    test_downsample(MaxAge, 10).

t_downsample_3d(_Config) ->
    MaxAge = 3 * timer:hours(24),
    test_downsample(MaxAge, 10).

t_downsample_1d(_Config) ->
    MaxAge = timer:hours(24),
    test_downsample(MaxAge, 10).

t_downsample_1h(_Config) ->
    MaxAge = timer:hours(1),
    test_downsample(MaxAge, 10).

%% Since the monitor process is running, and tests like `t_downsample_*' expect some
%% degree of determinism, we need to pause that process to avoid having it insert a rogue
%% point amidst the "carefully crafted" dataset.
pause_monitor_process() ->
    ok = sys:suspend(emqx_dashboard_monitor),
    on_exit(fun() -> ok = sys:resume(emqx_dashboard_monitor) end),
    ok.

sent_n(N) -> #{sent => N}.
sent_1() -> sent_n(1).

%% a gauge
connections_n(N) -> #{connections => N}.
connections_1() -> connections_n(1).

round_down(Ts, Interval) ->
    Ts - (Ts rem Interval).

test_downsample(MaxAge, DataPoints) ->
    ok = pause_monitor_process(),
    Now = erlang:system_time(millisecond) - 1,
    Interval = emqx_dashboard_monitor:sample_interval(MaxAge),
    StartTs = round_down(Now - MaxAge, Interval),
    clean_data(),
    %% insert the start mark for deterministic test boundary
    ok = write(StartTs, connections_1()),
    TsMax = round_down(Now, Interval),
    LastVal = insert_data_points(DataPoints - 1, fun connections_n/1, StartTs, TsMax),
    AllData = emqx_dashboard_monitor:all_data(),
    Data = emqx_dashboard_monitor:format(emqx_dashboard_monitor:sample_fill_gap(all, StartTs)),
    ?assertEqual(StartTs, maps:get(time_stamp, hd(Data)), #{
        expected_one_of => StartTs,
        start_ts => StartTs,
        interval => Interval,
        data => Data,
        all_data => AllData,
        now => Now
    }),
    ok = check_sample_intervals(Interval, hd(Data), tl(Data)),
    ?assertEqual(LastVal, maps:get(connections, lists:last(Data)), #{
        data => Data,
        interval => Interval,
        start_ts => StartTs,
        all_data => AllData,
        now => Now
    }),
    ok.

sum_value(Data, Key) ->
    sum_value(Data, Key, 0).

sum_value([], _, V) ->
    V;
sum_value([D | Rest], Key, V) ->
    sum_value(Rest, Key, maps:get(Key, D, 0) + V).

check_sample_intervals(_Interval, _, []) ->
    ok;
check_sample_intervals(Interval, #{time_stamp := T}, [First | Rest]) ->
    #{time_stamp := T2} = First,
    ?assertEqual(T + Interval, T2, #{
        t => T,
        interval => Interval,
        diff => T + Interval - T2,
        rest => Rest
    }),
    check_sample_intervals(Interval, First, Rest).

insert_data_points(N, MkPointFn, TsMin, TsMax) ->
    insert_data_points(N, MkPointFn, {_LastTs = 0, _LastVal = 0}, N, TsMin, TsMax).

insert_data_points(0, _MkPointFn, {_LastTs, LastVal}, _InitialN, _TsMin, _TsMax) ->
    LastVal;
insert_data_points(N, MkPointFn, {LastTs, LastVal}, InitialN, TsMin, TsMax) when N > 0 ->
    %% assert
    true = TsMax - TsMin > 1,
    %% + 2 because we don't want to insert 1.  It's used as a special "beginning-of-test"
    %% marker.
    Val = InitialN - N + 2,
    Data = MkPointFn(Val),
    FakeTs =
        case N of
            1 ->
                %% Last point should be `TsMax', otherwise the resulting
                %% `sample_interval' for this dataset might be different from the one
                %% expected in the test case.
                TsMax;
            _ ->
                TsMin + rand:uniform(TsMax - TsMin - 1)
        end,
    case read(FakeTs) of
        [] ->
            ok = write(FakeTs, Data),
            {NewLastTs, NewLastVal} =
                case FakeTs >= LastTs of
                    true -> {FakeTs, Val};
                    false -> {LastTs, LastVal}
                end,
            insert_data_points(N - 1, MkPointFn, {NewLastTs, NewLastVal}, InitialN, TsMin, TsMax);
        _ when N =:= 1 ->
            %% clashed, but trying again won't help because `FakeTs = TsMax'.  shouldn't happen.
            ct:fail("failed to generate data set: last timestamp is taken!");
        _ ->
            %% clashed, try again
            insert_data_points(N, MkPointFn, {LastTs, LastVal}, InitialN, TsMin, TsMax)
    end.

read(Ts) ->
    emqx_dashboard_monitor:lookup(Ts).

write(Time, Data) ->
    {atomic, ok} = emqx_dashboard_monitor:store({emqx_monit, Time, Data}),
    ok.

t_monitor_sampler_format(_Config) ->
    {ok, _} =
        snabbkaffe:block_until(
            ?match_event(#{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    Latest = hd(emqx_dashboard_monitor:samplers(node(), 1)),
    SamplerKeys = maps:keys(Latest),
    [?assert(lists:member(SamplerName, SamplerKeys)) || SamplerName <- ?SAMPLER_LIST],
    ok.

t_sample_specific_node_but_badrpc(_Config) ->
    meck:new(emqx_dashboard_monitor, [non_strict, passthrough, no_history, no_link]),
    meck:expect(
        emqx_dashboard_monitor,
        do_sample,
        fun(_Node, _Time) -> {badrpc, test} end
    ),
    ?assertMatch(
        {error, {404, #{<<"code">> := <<"NOT_FOUND">>}}},
        request(["monitor", "nodes", "a@b.net"], "latest=1000")
    ),
    %% arguably, it should be a 503
    ?assertMatch(
        {error, {400, #{<<"code">> := <<"BAD_REQUEST">>}}},
        request(["monitor", "nodes", atom_to_list(node())], "latest=1000")
    ),
    meck:unload(emqx_dashboard_monitor),
    ok.

t_handle_old_monitor_data(_Config) ->
    Now = erlang:system_time(second),
    FakeOldData = maps:from_list(
        lists:map(
            fun(N) ->
                Time = (Now - N) * 1000,
                {Time, #{foo => 123}}
            end,
            lists:seq(0, 9)
        )
    ),

    Self = self(),

    ok = meck:new(emqx, [passthrough, no_history]),
    ok = meck:expect(emqx, running_nodes, fun() -> [node(), 'other@node'] end),
    ok = meck:new(emqx_dashboard_proto_v2, [passthrough, no_history]),
    ok = meck:expect(emqx_dashboard_proto_v2, do_sample, fun('other@node', _Time) ->
        Self ! sample_called,
        FakeOldData
    end),

    {ok, _} =
        snabbkaffe:block_until(
            ?match_event(#{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    ?assertMatch(
        #{},
        hd(emqx_dashboard_monitor:samplers())
    ),
    ?assertReceive(sample_called, 1_000),
    ok = meck:unload([emqx, emqx_dashboard_proto_v2]),
    ok.

t_monitor_api(_) ->
    clean_data(),
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(2, #{?snk_kind := dashboard_monitor_flushed}),
            infinity,
            0
        ),
    {ok, Samplers} = request(["monitor"], "latest=20"),
    ?assert(erlang:length(Samplers) >= 2, #{samplers => Samplers}),
    Fun =
        fun(Sampler) ->
            Keys = [binary_to_atom(Key, utf8) || Key <- maps:keys(Sampler)],
            case Keys =:= [time_stamp] of
                true ->
                    %% this is a dummy data point filling the gap
                    ok;
                false ->
                    lists:all(
                        fun(K) ->
                            lists:member(K, Keys)
                        end,
                        ?SAMPLER_LIST
                    ) orelse
                        ct:fail(Keys)
            end
        end,
    [Fun(Sampler) || Sampler <- Samplers],
    {ok, NodeSamplers} = request(["monitor", "nodes", node()]),
    [Fun(NodeSampler) || NodeSampler <- NodeSamplers],
    ok.

t_monitor_current_api(_) ->
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(2, #{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    {ok, Rate} = request(["monitor_current"]),
    [
        ?assert(maps:is_key(atom_to_binary(Key, utf8), Rate))
     || Key <- maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST,
        %% We rename `durable_subscriptions' key.
        Key =/= durable_subscriptions
    ],
    ?assert(maps:is_key(<<"subscriptions_durable">>, Rate)),
    ?assert(maps:is_key(<<"disconnected_durable_sessions">>, Rate)),
    {ok, NodeRate} = request(["monitor_current", "nodes", node()]),
    ExpectedKeys = lists:map(
        fun atom_to_binary/1,
        (?GAUGE_SAMPLER_LIST ++ maps:values(?DELTA_SAMPLER_RATE_MAP)) -- ?CLUSTERONLY_SAMPLER_LIST
    ),
    ?assertEqual(
        [],
        ExpectedKeys -- maps:keys(NodeRate),
        NodeRate
    ),
    ?assertNot(maps:is_key(<<"subscriptions_durable">>, NodeRate)),
    ?assertNot(maps:is_key(<<"subscriptions_ram">>, NodeRate)),
    ?assertNot(maps:is_key(<<"disconnected_durable_sessions">>, NodeRate)),
    ok.

t_monitor_current_api_live_connections(_) ->
    process_flag(trap_exit, true),
    ClientId = <<"live_conn_tests">>,
    ClientId1 = <<"live_conn_tests1">>,
    {ok, C} = emqtt:start_link([{clean_start, false}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    ok = emqtt:disconnect(C),
    {ok, C1} = emqtt:start_link([{clean_start, true}, {clientid, ClientId1}]),
    {ok, _} = emqtt:connect(C1),
    ok = waiting_emqx_stats_and_monitor_update('live_connections.max'),
    ?retry(1_100, 5, begin
        {ok, Rate} = request(["monitor_current"]),
        ?assertEqual(1, maps:get(<<"live_connections">>, Rate)),
        ?assertEqual(2, maps:get(<<"connections">>, Rate))
    end),
    %% clears
    ok = emqtt:disconnect(C1),
    {ok, C2} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C2),
    ok = emqtt:disconnect(C2).

t_monitor_current_retained_count(_) ->
    process_flag(trap_exit, true),
    ClientId = <<"live_conn_tests">>,
    {ok, C} = emqtt:start_link([{clean_start, false}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    _ = emqtt:publish(C, <<"t1">>, <<"qos1-retain">>, [{qos, 1}, {retain, true}]),

    ok = waiting_emqx_stats_and_monitor_update('retained.count'),
    {ok, Res} = request(["monitor_current"]),
    {ok, ResNode} = request(["monitor_current", "nodes", node()]),

    ?assertEqual(1, maps:get(<<"retained_msg_count">>, Res)),
    ?assertEqual(1, maps:get(<<"retained_msg_count">>, ResNode)),
    ok = emqtt:disconnect(C),
    ok.

t_monitor_current_shared_subscription(_) ->
    process_flag(trap_exit, true),
    ShareT = <<"$share/group1/t/1">>,
    AssertFun = fun(Num) ->
        {ok, Res} = request(["monitor_current"]),
        {ok, ResNode} = request(["monitor_current", "nodes", node()]),
        ?assertEqual(Num, maps:get(<<"shared_subscriptions">>, Res)),
        ?assertEqual(Num, maps:get(<<"shared_subscriptions">>, ResNode)),
        ok
    end,

    ok = AssertFun(0),

    ClientId1 = <<"live_conn_tests1">>,
    ClientId2 = <<"live_conn_tests2">>,
    {ok, C1} = emqtt:start_link([{clean_start, false}, {clientid, ClientId1}]),
    {ok, _} = emqtt:connect(C1),
    _ = emqtt:subscribe(C1, {ShareT, 1}),

    ok = ?retry(100, 10, AssertFun(1)),

    {ok, C2} = emqtt:start_link([{clean_start, true}, {clientid, ClientId2}]),
    {ok, _} = emqtt:connect(C2),
    _ = emqtt:subscribe(C2, {ShareT, 1}),
    ok = ?retry(100, 10, AssertFun(2)),

    _ = emqtt:unsubscribe(C2, ShareT),
    ok = ?retry(100, 10, AssertFun(1)),
    _ = emqtt:subscribe(C2, {ShareT, 1}),
    ok = ?retry(100, 10, AssertFun(2)),

    ok = emqtt:disconnect(C1),
    %% C1: clean_start = false, proto_ver = 3.1.1
    %% means disconnected but the session pid with a share-subscription is still alive
    ok = ?retry(100, 10, AssertFun(2)),

    _ = emqx_cm:kick_session(ClientId1),
    ok = ?retry(100, 10, AssertFun(1)),

    ok = emqtt:disconnect(C2),
    ok = ?retry(100, 10, AssertFun(0)),
    ok.

t_monitor_reset(_) ->
    restart_monitor(),
    {ok, Rate} = request(["monitor_current"]),
    [
        ?assert(maps:is_key(atom_to_binary(Key, utf8), Rate))
     || Key <- maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST,
        %% We rename `durable_subscriptions' key.
        Key =/= durable_subscriptions
    ],
    ?assert(maps:is_key(<<"subscriptions_durable">>, Rate)),
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(1, #{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    {ok, Samplers} = request(["monitor"], "latest=1"),
    ?assertEqual(1, erlang:length(Samplers)),
    ok = delete(["monitor"]),
    ?assertMatch({ok, []}, request(["monitor"], "latest=1")),
    ok.

t_monitor_api_error(_) ->
    {error, {404, #{<<"code">> := <<"NOT_FOUND">>}}} =
        request(["monitor", "nodes", 'emqx@127.0.0.2']),
    {error, {404, #{<<"code">> := <<"NOT_FOUND">>}}} =
        request(["monitor_current", "nodes", 'emqx@127.0.0.2']),
    {error, {400, #{<<"code">> := <<"BAD_REQUEST">>}}} =
        request(["monitor"], "latest=0"),
    {error, {400, #{<<"code">> := <<"BAD_REQUEST">>}}} =
        request(["monitor"], "latest=-1"),
    ok.

%% Verifies that subscriptions from persistent sessions are correctly accounted for.
t_persistent_session_stats(Config) ->
    [N1, N2 | _] = ?config(cluster, Config),
    %% pre-condition
    true = ?ON(N1, emqx_persistent_message:is_persistence_enabled()),
    Port1 = get_mqtt_port(N1, tcp),
    Port2 = get_mqtt_port(N2, tcp),

    NonPSClient = start_and_connect(#{
        port => Port1,
        clientid => <<"non-ps">>,
        expiry_interval => 0
    }),
    PSClient1 = start_and_connect(#{
        port => Port1,
        clientid => <<"ps1">>,
        expiry_interval => 30
    }),
    PSClient2 = start_and_connect(#{
        port => Port2,
        clientid => <<"ps2">>,
        expiry_interval => 30
    }),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(NonPSClient, <<"non/ps/topic/+">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(NonPSClient, <<"non/ps/topic">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(NonPSClient, <<"common/topic/+">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(NonPSClient, <<"common/topic">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(PSClient1, <<"ps/topic/+">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(PSClient1, <<"ps/topic">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(PSClient1, <<"common/topic/+">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(PSClient1, <<"common/topic">>, 2),
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(2, #{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    ?retry(1_000, 10, begin
        ?assertMatch(
            {ok, #{
                <<"connections">> := 3,
                <<"disconnected_durable_sessions">> := 0,
                %% N.B.: we currently don't perform any deduplication between persistent
                %% and non-persistent routes, so we count `commont/topic' twice and get 8
                %% instead of 6 here.
                <<"topics">> := 8,
                <<"subscriptions">> := 8,
                <<"subscriptions_ram">> := 4,
                <<"subscriptions_durable">> := 4
            }},
            ?ON(N1, request(["monitor_current"]))
        )
    end),
    %% Sanity checks
    PSRouteCount = ?ON(N1, emqx_persistent_session_ds_router:stats(n_routes)),
    ?assert(PSRouteCount > 0, #{ps_route_count => PSRouteCount}),
    PSSubCount = ?ON(N1, emqx_persistent_session_bookkeeper:get_subscription_count()),
    ?assert(PSSubCount > 0, #{ps_sub_count => PSSubCount}),

    %% Now with disconnected but alive persistent sessions
    {ok, {ok, _}} =
        ?wait_async_action(
            emqtt:disconnect(PSClient1),
            #{?snk_kind := dashboard_monitor_flushed}
        ),
    ?retry(1_000, 10, begin
        ?assertMatch(
            {ok, #{
                <<"connections">> := 3,
                <<"disconnected_durable_sessions">> := 1,
                %% N.B.: we currently don't perform any deduplication between persistent
                %% and non-persistent routes, so we count `common/topic' twice and get 8
                %% instead of 6 here.
                <<"topics">> := 8,
                <<"subscriptions">> := 8,
                <<"subscriptions_ram">> := 4,
                <<"subscriptions_durable">> := 4
            }},
            ?ON(N1, request(["monitor_current"]))
        )
    end),
    %% Verify that historical metrics are in line with the current ones.
    ?assertMatch(
        {ok, [
            #{
                <<"time_stamp">> := _,
                <<"connections">> := 3,
                <<"disconnected_durable_sessions">> := 1,
                <<"topics">> := 8,
                <<"subscriptions">> := 8,
                <<"subscriptions_ram">> := 4,
                <<"subscriptions_durable">> := 4
            }
        ]},
        ?ON(N1, request(["monitor"], "latest=1"))
    ),
    {ok, {ok, _}} =
        ?wait_async_action(
            emqtt:disconnect(PSClient2),
            #{?snk_kind := dashboard_monitor_flushed}
        ),
    ?retry(1_000, 10, begin
        ?assertMatch(
            {ok, #{
                <<"connections">> := 3,
                <<"disconnected_durable_sessions">> := 2,
                %% N.B.: we currently don't perform any deduplication between persistent
                %% and non-persistent routes, so we count `common/topic' twice and get 8
                %% instead of 6 here.
                <<"topics">> := 8,
                <<"subscriptions">> := 8,
                <<"subscriptions_ram">> := 4,
                <<"subscriptions_durable">> := 4
            }},
            ?ON(N1, request(["monitor_current"]))
        )
    end),
    ?assertNotMatch({ok, []}, ?ON(N1, request(["monitor"]))),
    ?assertMatch(ok, ?ON(N1, delete(["monitor"]))),
    ?assertMatch({ok, []}, ?ON(N1, request(["monitor"]))),
    ok.

%% Checks that we get consistent data when changing the requested time window for
%% `/monitor'.
t_smoke_test_monitor_multiple_windows(Config) ->
    [N1, N2 | _] = ?config(nodes, Config),
    %% pre-condition
    true = ?ON(N1, emqx_persistent_message:is_persistence_enabled()),

    Port1 = get_mqtt_port(N1, tcp),
    Port2 = get_mqtt_port(N2, tcp),
    NonPSClient = start_and_connect(#{
        port => Port1,
        clientid => <<"non-ps">>,
        expiry_interval => 0
    }),
    PSClient1 = start_and_connect(#{
        port => Port1,
        clientid => <<"ps1">>,
        expiry_interval => 30
    }),
    PSClient2 = start_and_connect(#{
        port => Port2,
        clientid => <<"ps2">>,
        expiry_interval => 30
    }),
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(2, #{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    ?retry(1_000, 10, begin
        ?assertMatch(
            {ok, #{
                <<"connections">> := 3,
                <<"live_connections">> := 3
            }},
            get_latest_from_window(Config, {hours, 1})
        )
    end),
    ?retry(1_000, 10, begin
        ?assertMatch(
            {ok, #{
                <<"connections">> := 3,
                <<"live_connections">> := 3
            }},
            get_latest_from_window(Config, {hours, 6})
        )
    end),
    ?retry(1_000, 10, begin
        ?assertMatch(
            {ok, #{
                <<"connections">> := 3,
                <<"live_connections">> := 3
            }},
            get_latest_from_window(Config, {days, 7})
        )
    end),
    %% Stop one memory and one persistent client
    {ok, {ok, _}} =
        ?wait_async_action(
            begin
                ok = emqtt:stop(NonPSClient),
                ok = emqtt:stop(PSClient1)
            end,
            #{?snk_kind := dashboard_monitor_flushed}
        ),
    ?retry(1_000, 10, begin
        ?assertMatch(
            {ok, #{
                <<"connections">> := 2,
                <<"live_connections">> := 1
            }},
            get_latest_from_window(Config, {hours, 1})
        )
    end),
    ?retry(1_000, 10, begin
        ?assertMatch(
            {ok, #{
                <<"connections">> := 2,
                <<"live_connections">> := 1
            }},
            get_latest_from_window(Config, {hours, 6})
        )
    end),
    ?retry(1_000, 10, begin
        ?assertMatch(
            {ok, #{
                <<"connections">> := 2,
                <<"live_connections">> := 1
            }},
            get_latest_from_window(Config, {days, 7})
        )
    end),
    ok = emqtt:stop(PSClient2),
    ok.

request(Path) ->
    request(Path, "").

request(Path, QS) ->
    Url = url(Path, QS),
    do_request_api(get, {Url, [auth_header_()]}).

get_latest_from_window(Config, Window) ->
    WindowS = integer_to_list(window_in_seconds(Window)),
    case get_req_cluster(Config, ["monitor"], "latest=" ++ WindowS) of
        {ok, Points} when is_list(Points) ->
            {ok, lists:last(Points)};
        Error ->
            Error
    end.

window_in_seconds({hours, N}) ->
    N * 3_600;
window_in_seconds({days, N}) ->
    N * 86_400.

get_req_cluster(Config, Path, QS) ->
    APINode = ?config(api_node, Config),
    Port = get_http_dashboard_port(APINode),
    Host = host(Port),
    Url = url(Host, Path, QS),
    Auth = ?ON(APINode, auth_header_()),
    do_request_api(get, {Url, [Auth]}).

host(Port) ->
    "http://127.0.0.1:" ++ integer_to_list(Port).

delete(Path) ->
    Url = url(Path, ""),
    do_request_api(delete, {Url, [auth_header_()]}).

url(Parts, QS) ->
    url(?SERVER, Parts, QS).

url(Host, Parts, QS) ->
    case QS of
        "" ->
            Host ++ filename:join([?BASE_PATH | Parts]);
        _ ->
            Host ++ filename:join([?BASE_PATH | Parts]) ++ "?" ++ QS
    end.

do_request_api(Method, Request) ->
    ct:pal("Req ~p ~p~n", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", 204, _}, _, _}} ->
            ok;
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            ct:pal("Resp ~p ~p~n", [Code, Return]),
            {ok, emqx_utils_json:decode(Return, [return_maps])};
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} ->
            ct:pal("Resp ~p ~p~n", [Code, Return]),
            {error, {Code, emqx_utils_json:decode(Return, [return_maps])}};
        {error, Reason} ->
            {error, Reason}
    end.

restart_monitor() ->
    OldMonitor = erlang:whereis(emqx_dashboard_monitor),
    erlang:exit(OldMonitor, kill),
    ?assertEqual(ok, wait_new_monitor(OldMonitor, 10)).

wait_new_monitor(_OldMonitor, Count) when Count =< 0 -> timeout;
wait_new_monitor(OldMonitor, Count) ->
    NewMonitor = erlang:whereis(emqx_dashboard_monitor),
    case is_pid(NewMonitor) andalso NewMonitor =/= OldMonitor of
        true ->
            ok;
        false ->
            timer:sleep(100),
            wait_new_monitor(OldMonitor, Count - 1)
    end.

waiting_emqx_stats_and_monitor_update(WaitKey) ->
    Self = self(),
    meck:new(emqx_stats, [passthrough]),
    meck:expect(
        emqx_stats,
        setstat,
        fun(Stat, MaxStat, Val) ->
            (Stat =:= WaitKey orelse MaxStat =:= WaitKey) andalso (Self ! updated),
            meck:passthrough([Stat, MaxStat, Val])
        end
    ),
    receive
        updated -> ok
    after 5000 ->
        error(waiting_emqx_stats_update_timeout)
    end,
    meck:unload([emqx_stats]),
    %% manually call monitor update
    _ = emqx_dashboard_monitor:current_rate_cluster(),
    ok.

start_and_connect(Opts) ->
    Defaults = #{
        clean_start => false,
        expiry_interval => 30,
        port => 1883
    },
    #{
        clientid := ClientId,
        clean_start := CleanStart,
        expiry_interval := EI,
        port := Port
    } = maps:merge(Defaults, Opts),
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {clean_start, CleanStart},
        {port, Port},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => EI}}
    ]),
    on_exit(fun() ->
        catch emqtt:disconnect(Client, ?RC_NORMAL_DISCONNECTION, #{'Session-Expiry-Interval' => 0})
    end),
    {ok, _} = emqtt:connect(Client),
    Client.

get_mqtt_port(Node, Type) ->
    {_IP, Port} = ?ON(Node, emqx_config:get([listeners, Type, default, bind])),
    Port.

get_http_dashboard_port(Node) ->
    ?ON(Node, emqx_config:get([dashboard, listeners, http, bind])).

cluster_node_appspec(Enable, Port0) ->
    Port =
        case Enable of
            true -> integer_to_list(Port0);
            false -> "0"
        end,
    [
        emqx_conf,
        {emqx, "durable_sessions {enable = true}"},
        {emqx_retainer, ?BASE_RETAINER_CONF},
        emqx_management,
        emqx_mgmt_api_test_util:emqx_dashboard(
            lists:concat([
                "dashboard.listeners.http { bind = " ++ Port ++ " }\n",
                "dashboard.sample_interval = 1s\n",
                "dashboard.listeners.http.enable = " ++ atom_to_list(Enable)
            ])
        )
    ].

clean_data() ->
    ok = emqx_dashboard_monitor:clean(-100000).
