%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_relup_oracle_upgrade_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_Case, Config) ->
    ok = meck:new(emqx_resource, [non_strict, passthrough, no_link]),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = meck:unload(emqx_resource),
    ok.

t_restart_oracle_connectors_skips_stopped_resources(_Config) ->
    Running = <<"oracle-running">>,
    Stopped = <<"oracle-stopped">>,
    Missing = <<"oracle-missing">>,
    ok = meck:expect(emqx_resource, list_instances_by_type, fun
        (emqx_oracle) ->
            [Running, Stopped, Missing];
        (_Type) ->
            []
    end),
    ok = meck:expect(emqx_resource, get_instance, fun
        (ResId) when ResId =:= Running ->
            {ok, <<"default">>, #{status => connected}};
        (ResId) when ResId =:= Stopped ->
            {ok, <<"default">>, #{status => stopped}};
        (ResId) when ResId =:= Missing ->
            {error, not_found}
    end),
    ok = meck:expect(emqx_resource, restart, fun(_ResId) -> ok end),

    ok = emqx_relup_oracle_upgrade:restart_oracle_connectors(),

    ?assertEqual(1, meck:num_calls(emqx_resource, restart, [Running])),
    ?assertEqual(0, meck:num_calls(emqx_resource, restart, [Stopped])),
    ?assertEqual(0, meck:num_calls(emqx_resource, restart, [Missing])).
