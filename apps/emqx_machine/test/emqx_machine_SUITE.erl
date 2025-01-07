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

-module(emqx_machine_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(APPS, [
    emqx_prometheus,
    emqx_modules,
    emqx_dashboard,
    emqx_gateway,
    emqx_resource,
    emqx_rule_engine,
    emqx_bridge,
    emqx_management,
    emqx_retainer,
    emqx_exhook,
    emqx_auth,
    emqx_plugin
]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        app_specs(),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

app_specs() ->
    [
        emqx_conf,
        emqx_prometheus,
        emqx_modules,
        emqx_dashboard,
        emqx_gateway,
        emqx_resource,
        emqx_rule_engine,
        emqx_bridge,
        emqx_management,
        emqx_retainer,
        emqx_exhook,
        emqx_auth,
        emqx_plugins
    ].

init_per_testcase(t_custom_shard_transports, Config) ->
    OldConfig = application:get_env(emqx_machine, custom_shard_transports),
    [{old_config, OldConfig} | Config];
init_per_testcase(t_open_ports_check = TestCase, Config) ->
    AppSpecs = [emqx],
    Cluster = [
        {emqx_machine_SUITE1, #{role => core, apps => AppSpecs}},
        {emqx_machine_SUITE2, #{role => core, apps => AppSpecs}},
        {emqx_machine_SUITE3, #{role => replicant, apps => AppSpecs}}
    ],
    Nodes = emqx_cth_cluster:start(Cluster, #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}),
    [{nodes, Nodes} | Config];
init_per_testcase(t_sorted_reboot_apps, Config) ->
    application:set_env(emqx_machine, applications, ?APPS ++ [emqx_license]),
    Config;
init_per_testcase(_TestCase, Config) ->
    application:set_env(emqx_machine, applications, ?APPS),
    Config.

end_per_testcase(t_custom_shard_transports, Config) ->
    OldConfig0 = ?config(old_config, Config),
    application:stop(ekka),
    case OldConfig0 of
        {ok, OldConfig} ->
            application:set_env(emqx_machine, custom_shard_transports, OldConfig);
        undefined ->
            application:unset_env(emqx_machine, custom_shard_transports)
    end,
    ok;
end_per_testcase(t_open_ports_check, Config) ->
    Nodes = ?config(nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

t_shutdown_reboot(Config) ->
    [Node] = emqx_cth_cluster:start(
        [{machine_reboot_SUITE1, #{role => core, apps => app_specs()}}],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    try
        erpc:call(Node, fun() ->
            true = emqx:is_running(node()),
            emqx_machine_boot:stop_apps(),
            false = emqx:is_running(node()),
            %% This is to emulate the presence of `emqx.conf' or `cluster.hocon' files,
            %% which are not present in the peer.
            %% This is done by `emqx_cth_suite' initially.
            ok = emqx_app:set_config_loader(emqx_cth_suite),
            emqx_machine_boot:ensure_apps_started(),
            true = emqx:is_running(node()),
            ok = emqx_machine_boot:stop_apps(),
            false = emqx:is_running(node()),
            ok
        end)
    after
        catch emqx_cth_cluster:stop([Node])
    end.

t_sorted_reboot_apps(_Config) ->
    Apps = emqx_machine_boot:sorted_reboot_apps(),
    SortApps = [App || App <- Apps, (App =:= emqx_dashboard orelse App =:= emqx_license)],
    %% make sure emqx_license start early than emqx_dashboard
    ?assertEqual([emqx_license, emqx_dashboard], SortApps).

t_custom_shard_transports(_Config) ->
    %% used to ensure the atom exists
    Shard = test_shard,
    %% the config keys are binaries
    ShardBin = atom_to_binary(Shard),
    DefaultTransport = distr,
    ?assertEqual(DefaultTransport, mria_config:shard_transport(Shard)),
    application:set_env(emqx_machine, custom_shard_transports, #{ShardBin => distr}),
    emqx_machine:start(),
    ?assertEqual(distr, mria_config:shard_transport(Shard)),
    ok.

t_node_status(_Config) ->
    JSON = emqx_machine:node_status(),
    ?assertMatch(
        #{
            <<"backend">> := _,
            <<"role">> := <<"core">>
        },
        jsx:decode(JSON)
    ).

t_open_ports_check(Config) ->
    [Core1, Core2, Replicant] = ?config(nodes, Config),

    Plan = erpc:call(Core1, emqx_machine, create_plan, []),
    ?assertMatch(
        [{Core2, #{ports_to_check := [_GenRPC0, _Ekka0], resolved_ips := [_]}}],
        Plan
    ),
    [{Core2, #{ports_to_check := [GenRPCPort, EkkaPort], resolved_ips := [_]}}] = Plan,
    ?assertMatch(
        [{Core1, #{ports_to_check := [_GenRPC1, _Ekka1], resolved_ips := [_]}}],
        erpc:call(Core2, emqx_machine, create_plan, [])
    ),
    ?assertMatch(
        [],
        erpc:call(Replicant, emqx_machine, create_plan, [])
    ),

    ?assertEqual(ok, erpc:call(Core1, emqx_machine, open_ports_check, [])),
    ?assertEqual(ok, erpc:call(Core2, emqx_machine, open_ports_check, [])),
    ?assertEqual(ok, erpc:call(Replicant, emqx_machine, open_ports_check, [])),

    true = erlang:monitor_node(Core2, true),
    ok = emqx_cth_cluster:stop_node(Core2),
    receive
        {nodedown, Core2} -> ok
    after 10000 ->
        ct:fail("nodedown message not received after 10 seconds.")
    end,

    ?assertEqual(ok, erpc:call(Replicant, emqx_machine, open_ports_check, [])),
    ?retry(200, 20, begin
        Results = erpc:call(Core1, emqx_machine, open_ports_check, []),
        ?assertMatch(
            #{
                msg := "some ports are unreachable",
                results :=
                    #{
                        Core2 :=
                            #{
                                open_ports := #{},
                                ports_to_check := [_, _],
                                resolved_ips := [_],
                                status := bad_ports
                            }
                    }
            },
            Results,
            #{core2 => Core2, gen_rpc_port => GenRPCPort, ekka_port => EkkaPort}
        ),
        %% 2 ports to check; we don't assert the exact ekka port because, when running
        %% multiple nodes on the same machine as we do in tests, the order of returned ports
        %% might change between invocations.
        NumPorts = 2,
        ?assertEqual(
            NumPorts, map_size(emqx_utils_maps:deep_get([results, Core2, open_ports], Results))
        ),
        ok
    end),
    ok.
