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

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_connection_count(_Config) ->
    ?check_trace(
        begin
            ?wait_async_action(
                whereis(emqx_license_resources) ! update_resources,
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
    meck:new(emqx_alarm, []),
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

    meck:new(emqx_license_proto_v2, [passthrough]),

    meck:expect(
        emqx_license_proto_v2,
        remote_connection_counts,
        fun(_Nodes) -> [{ok, 21}] end
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
        emqx_license_proto_v2,
        remote_connection_counts,
        fun(Nodes) ->
            [{ok, 5}, {error, some_error}] ++ meck:passthrough([Nodes])
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

    meck:unload(emqx_license_proto_v2),
    meck:unload(emqx_cm),
    meck:unload(emqx_alarm).

t_emqx_license_proto(_Config) ->
    ?assert("5.0.0" =< emqx_license_proto_v2:introduced_in()).
