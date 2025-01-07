%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_resources_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_license, #{
                config => #{license => #{key => emqx_license_test_lib:default_license()}}
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

    meck:new(emqx_cm, [passthrough]),
    meck:expect(emqx_cm, get_connected_client_count, fun() -> 10 end),

    meck:new(emqx_license_proto_v2, [passthrough]),
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

    meck:unload(emqx_license_proto_v2),
    meck:unload(emqx_cm).

t_emqx_license_proto(_Config) ->
    ?assert("5.0.0" =< emqx_license_proto_v2:introduced_in()).
