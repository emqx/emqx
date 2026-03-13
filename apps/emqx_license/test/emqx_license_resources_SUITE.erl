%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_resources_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("emqx_license.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_license, #{
                config => #{license => #{key => ?DEFAULT_EVALUATION_LICENSE_KEY}}
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_connection_count({init, Config}) ->
    Config;
t_connection_count({'end', _Config}) ->
    ok;
t_connection_count(Config) when is_list(Config) ->
    ?check_trace(
        begin
            ?wait_async_action(
                emqx_license_resources:update_now(),
                #{?snk_kind := emqx_license_resources_updated},
                1000
            ),
            emqx_license_resources:cached_connection_count()
        end,
        fun(ConnCount, Trace) ->
            ?assertEqual(0, ConnCount),
            ?assertMatch([_ | _], ?of_kind(emqx_license_resources_updated, Trace))
        end
    ),

    ok = emqx_alarm:ensure_deactivated(license_quota),
    {ok, #{max_sessions := MaxSessions}} = emqx_license_checker:limits(),
    High = emqx_conf:get([license, connection_high_watermark], 0.80),
    Low = emqx_conf:get([license, connection_low_watermark], 0.75),
    Baseline = emqx_cm:get_sessions_count(),
    TargetHigh = min(MaxSessions - 1, trunc(MaxSessions * High) + 1),
    Needed = max(0, TargetHigh - Baseline),
    Pids = start_clients(Needed),
    try
        ?retry(
            20,
            50,
            ?assert(emqx_cm:get_sessions_count() >= TargetHigh)
        ),
        ?check_trace(
            begin
                ?wait_async_action(
                    whereis(emqx_license_resources) ! update_resources,
                    #{?snk_kind := emqx_license_resources_updated},
                    1000
                ),
                emqx_license_resources:cached_connection_count()
            end,
            fun(ConnCount, _Trace) ->
                ?assert(ConnCount >= TargetHigh)
            end
        ),
        ?retry(
            20,
            50,
            ?assertMatch({ok, _}, emqx_alarm:read_details(license_quota))
        ),
        stop_clients(Pids),
        ?retry(
            40,
            50,
            ?assert(emqx_cm:get_sessions_count() =< trunc(MaxSessions * Low))
        ),
        ?check_trace(
            begin
                ?wait_async_action(
                    whereis(emqx_license_resources) ! update_resources,
                    #{?snk_kind := emqx_license_resources_updated},
                    1000
                ),
                emqx_license_resources:cached_connection_count()
            end,
            fun(_ConnCount, _Trace) ->
                ok
            end
        ),
        ?retry(
            20,
            50,
            ?assertEqual({error, not_found}, emqx_alarm:read_details(license_quota))
        )
    after
        stop_clients(Pids),
        ok = emqx_alarm:ensure_deactivated(license_quota)
    end.

t_stats_ignore_errors({init, Config}) ->
    Config;
t_stats_ignore_errors({'end', _Config}) ->
    ok;
t_stats_ignore_errors(_Config) ->
    meck:new(emqx_license_proto_v3, [passthrough, no_history]),
    meck:expect(emqx_license_proto_v3, stats, fun(_Nodes, _Now) -> [{error, bad_rpc}] end),
    try
        ok = update_now(),
        ?assertEqual(0, emqx_license_resources:cached_connection_count())
    after
        meck:unload(emqx_license_proto_v3)
    end.

t_local_connection_count({init, Config}) ->
    Config;
t_local_connection_count({'end', _Config}) ->
    ok;
t_local_connection_count(_Config) ->
    Count = emqx_license_resources:local_connection_count(),
    ?assert(is_integer(Count)),
    ?assert(Count >= 0).

t_handle_cast({init, Config}) ->
    Config;
t_handle_cast({'end', _Config}) ->
    ok;
t_handle_cast(_Config) ->
    ok = gen_server:cast(emqx_license_resources, some_cast),
    ?assertEqual(ignored, gen_server:call(emqx_license_resources, ignored)).

t_code_change({init, Config}) ->
    Config;
t_code_change({'end', _Config}) ->
    ok;
t_code_change(_Config) ->
    Pid = whereis(emqx_license_resources),
    State = sys:get_state(Pid),
    ?assertEqual({ok, State}, emqx_license_resources:code_change(dummy, State, [])).

t_terminate_restart({init, Config}) ->
    Config;
t_terminate_restart({'end', _Config}) ->
    ok;
t_terminate_restart(_Config) ->
    Pid = whereis(emqx_license_resources),
    State = sys:get_state(Pid),
    ?assertEqual(ok, emqx_license_resources:terminate(normal, State)).

t_stats_v2_path({init, Config}) ->
    Config;
t_stats_v2_path({'end', _Config}) ->
    ok;
t_stats_v2_path(_Config) ->
    meck:new(emqx_bpapi, [passthrough, no_history]),
    meck:new(emqx_license_proto_v2, [passthrough, no_history]),
    meck:new(emqx_gateway_cm_registry, [passthrough, no_history]),
    try
        meck:expect(emqx_bpapi, supported_version, fun(emqx_license) -> 2 end),
        meck:expect(
            emqx_license_proto_v2,
            remote_connection_counts,
            fun(_Nodes) -> [{ok, 3}, {ok, 7}, {error, bad}] end
        ),
        meck:expect(emqx_gateway_cm_registry, get_connected_client_count, fun() -> 0 end),
        ok = update_now(),
        ?assertEqual(10, emqx_license_resources:cached_connection_count())
    after
        meck:unload(emqx_gateway_cm_registry),
        meck:unload(emqx_license_proto_v2),
        meck:unload(emqx_bpapi)
    end.

t_proto_versions({init, Config}) ->
    Config;
t_proto_versions({'end', _Config}) ->
    ok;
t_proto_versions(_Config) ->
    ?assertEqual("5.0.0", emqx_license_proto_v2:introduced_in()),
    ?assertEqual("6.0.0", emqx_license_proto_v3:introduced_in()),
    ?assertEqual([], emqx_license_proto_v2:remote_connection_counts([])),
    ?assertEqual([], emqx_license_proto_v3:remote_connection_counts([])).

%% This test verifies below behaviors:
%% - The alarm is activated when the latest TPS exceeds the limit.
%% - The alarm details is updated when the latest TPS becomes higher than the history TPS.
%% - The alarm details is NOT updated when the latest TPS is lower than the history TPS.
%% - The alarm is NOT deactivated when the latest TPS is below the limit
%% - The alarm is deactivated when the history TPS is below the limit (with new license).
t_alarm_if_tps_over_limit({init, Config}) ->
    emqx_license_test_lib:mock_parser(),
    Key = emqx_license_test_lib:make_license(#{max_tps => 10}),
    emqx_license:update_key(Key),
    meck:new(emqx_alarm, [passthrough]),
    meck:new(emqx_license_proto_v3, [passthrough]),
    Config;
t_alarm_if_tps_over_limit({'end', _Config}) ->
    emqx_license_test_lib:unmock_parser(),
    emqx_license:update_key(?DEFAULT_EVALUATION_LICENSE_KEY),
    meck:unload(emqx_alarm),
    meck:unload(emqx_license_proto_v3),
    ok;
t_alarm_if_tps_over_limit(Config) when is_list(Config) ->
    Tester = self(),
    meck:expect(
        emqx_alarm,
        activate,
        fun(license_tps, Details, Msg) ->
            Res = meck:passthrough([license_tps, Details, Msg]),
            Tester ! {alarm_activated, Msg, Details},
            Res
        end
    ),
    meck:expect(
        emqx_alarm,
        ensure_deactivated,
        fun(license_tps) ->
            Tester ! alarm_deactivated,
            meck:passthrough([license_tps])
        end
    ),
    MockTps = fun(Tps) ->
        meck:expect(
            emqx_license_proto_v3,
            stats,
            fun(_Nodes, _Now) -> [{ok, #{sessions => 21, tps => Tps}}] end
        ),
        %% the timestamp is at millisecond precision, so we need to sleep for 10ms to ensure the timestamp is different.
        timer:sleep(10),
        ok = update_now()
    end,
    %% The alarm is activated if TPS exceeds the limit.
    MockTps(11),
    ?assertReceive(
        {alarm_activated, <<"License: TPS limit (10) exceeded.">>, #{max_tps := 11}}, 100
    ),
    Details1 = read_alarm_details(license_tps),
    ?assertMatch(#{max_tps := 11}, Details1),
    ObservedAt1 = maps:get(observed_at, Details1),
    %% The alarm details is updated when the latest TPS becomes higher than the history TPS.
    MockTps(12),
    ?assertNotReceive({alarm_activated, _, _}, 100),
    Details2 = read_alarm_details(license_tps),
    ?assertMatch(#{max_tps := 12}, Details2),
    ObservedAt2 = maps:get(observed_at, Details2),
    ?assert(ObservedAt2 > ObservedAt1),
    %% The alarm details is NOT updated when the latest TPS is lower than the history TPS.
    MockTps(11),
    ?assertNotReceive({alarm_activated, _, _}, 100),
    Details3 = read_alarm_details(license_tps),
    ?assertMatch(#{max_tps := 12}, Details3),
    ?assertEqual(ObservedAt2, maps:get(observed_at, Details3)),
    %% The alarm is NOT deactivated when the latest TPS is below the limit.
    MockTps(9),
    ?assertNotReceive(alarm_deactivated, 100),
    Details4 = read_alarm_details(license_tps),
    ?assertMatch(#{max_tps := 12}, Details4),
    ?assertEqual(ObservedAt2, maps:get(observed_at, Details4)),
    %% The alarm is deactivated when the history TPS is below the limit (with new license).
    InfinityTpsKey = emqx_license_test_lib:make_license(#{max_tps => "inf"}),
    emqx_license:update_key(InfinityTpsKey),
    ok = update_now(),
    ?assertReceive(alarm_deactivated, 100),
    ok.

update_now() ->
    emqx_license_resources:update_now(),
    ignored = gen_server:call(emqx_license_resources, ignored),
    ok.

read_alarm_details(Name) ->
    %% make a gen_call to ensure the alarm update call is completed
    _ = emqx_alarm:get_alarms(activated),
    {ok, Details} = emqx_alarm:read_details(Name),
    Details.

start_clients(Count) when Count =< 0 ->
    [];
start_clients(Count) ->
    lists:map(
        fun(I) ->
            ClientId =
                iolist_to_binary([
                    <<"license-res-">>,
                    integer_to_list(erlang:unique_integer([positive, monotonic])),
                    <<"-">>,
                    integer_to_list(I)
                ]),
            {ok, C} = emqtt:start_link([{clientid, ClientId}, {proto_ver, v5}]),
            {ok, _} = emqtt:connect(C),
            C
        end,
        lists:seq(1, Count)
    ).

stop_clients(Pids) ->
    lists:foreach(
        fun(Pid) ->
            case is_process_alive(Pid) of
                true -> catch emqtt:stop(Pid);
                false -> ok
            end
        end,
        Pids
    ).
