%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

    Tester = self(),
    meck:new(emqx_alarm, [passthrough]),
    meck:expect(
        emqx_alarm,
        activate,
        fun(license_quota, _, Msg) ->
            Tester ! {alarm_activated, Msg},
            ok
        end
    ),
    meck:expect(
        emqx_alarm,
        ensure_deactivated,
        fun(license_quota) ->
            Tester ! alarm_deactivated,
            ok
        end
    ),
    meck:new(emqx_cm, [passthrough]),
    meck:expect(emqx_cm, get_sessions_count, fun() -> 10 end),

    meck:new(emqx_license_proto_v3, [passthrough]),

    meck:expect(
        emqx_license_proto_v3,
        stats,
        fun(_Nodes, _Now) -> [{ok, #{sessions => 21, tps => 2}}] end
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
            ?assertEqual(21, ConnCount)
        end
    ),
    ?assertReceive({alarm_activated, <<"License: sessions quota exceeds 80%">>}, 100),

    meck:expect(
        emqx_license_proto_v3,
        stats,
        fun(Nodes, Time) ->
            RpcRes = meck:passthrough([Nodes, Time]),
            [{ok, #{sessions => 5, tps => 3}}, {error, some_error}] ++ RpcRes
        end
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
            ?assertEqual(15, ConnCount)
        end
    ),
    ?assertReceive(alarm_deactivated, 100),

    meck:unload(emqx_license_proto_v3),
    meck:unload(emqx_cm),
    meck:unload(emqx_alarm).

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
