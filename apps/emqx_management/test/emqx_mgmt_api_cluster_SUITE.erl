%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(APPS, [emqx_conf, emqx_management]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_) ->
    ok.

init_per_testcase(TC = t_cluster_topology_api_replicants, Config0) ->
    Config = [{tc_name, TC} | Config0],
    [{cluster, cluster(Config)} | setup(Config)];
init_per_testcase(_TC, Config) ->
    emqx_mgmt_api_test_util:init_suite(?APPS),
    Config.

end_per_testcase(t_cluster_topology_api_replicants, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    cleanup(Config);
end_per_testcase(_TC, _Config) ->
    emqx_mgmt_api_test_util:end_suite(?APPS).

t_cluster_topology_api_empty_resp(_) ->
    ClusterTopologyPath = emqx_mgmt_api_test_util:api_path(["cluster", "topology"]),
    {ok, Resp} = emqx_mgmt_api_test_util:request_api(get, ClusterTopologyPath),
    ?assertEqual(
        [#{<<"core_node">> => atom_to_binary(node()), <<"replicant_nodes">> => []}],
        emqx_utils_json:decode(Resp, [return_maps])
    ).

t_cluster_topology_api_replicants(Config) ->
    %% some time to stabilize
    timer:sleep(3000),
    [Core1, Core2, Replicant] = _NodesList = ?config(cluster, Config),
    {200, Core1Resp} = rpc:call(Core1, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    {200, Core2Resp} = rpc:call(Core2, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    {200, ReplResp} = rpc:call(Replicant, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    [
        ?assertMatch(
            [
                #{
                    core_node := Core1,
                    replicant_nodes :=
                        [#{node := Replicant, streams := _}]
                },
                #{
                    core_node := Core2,
                    replicant_nodes :=
                        [#{node := Replicant, streams := _}]
                }
            ],
            Resp
        )
     || Resp <- [lists:sort(R) || R <- [Core1Resp, Core2Resp, ReplResp]]
    ].

cluster(Config) ->
    Nodes = emqx_cth_cluster:start(
        [
            {data_backup_core1, #{role => core, apps => ?APPS}},
            {data_backup_core2, #{role => core, apps => ?APPS}},
            {data_backup_replicant, #{role => replicant, apps => ?APPS}}
        ],
        #{work_dir => work_dir(Config)}
    ),
    Nodes.

setup(Config) ->
    WorkDir = filename:join(work_dir(Config), local),
    Started = emqx_cth_suite:start(?APPS, #{work_dir => WorkDir}),
    [{suite_apps, Started} | Config].

cleanup(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

work_dir(Config) ->
    filename:join(?config(priv_dir, Config), ?config(tc_name, Config)).
