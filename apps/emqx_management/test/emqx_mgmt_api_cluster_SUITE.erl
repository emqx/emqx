%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    case emqx_cth_suite:skip_if_oss() of
        false ->
            emqx_common_test_helpers:all(?MODULE);
        True ->
            True
    end.

init_per_suite(Config) ->
    Config.

end_per_suite(_) ->
    ok.

init_per_testcase(TC = t_cluster_topology_api_replicants, Config0) ->
    Config = [{tc_name, TC} | Config0],
    [{cluster, cluster(Config)} | setup(Config)];
init_per_testcase(TC = t_cluster_invite_api_timeout, Config0) ->
    Config = [{tc_name, TC} | Config0],
    [{cluster, cluster(Config)} | setup(Config)];
init_per_testcase(TC = t_cluster_invite_async, Config0) ->
    Config = [{tc_name, TC} | Config0],
    [{cluster, cluster(Config)} | setup(Config)];
init_per_testcase(TC, Config) ->
    Apps = emqx_cth_suite:start(
        ?APPS ++ [emqx_mgmt_api_test_util:emqx_dashboard()],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    [{tc_apps, Apps} | Config].

end_per_testcase(t_cluster_topology_api_replicants, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    cleanup(Config);
end_per_testcase(t_cluster_invite_api_timeout, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    cleanup(Config);
end_per_testcase(t_cluster_invite_async, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    cleanup(Config);
end_per_testcase(_TC, Config) ->
    ok = emqx_cth_suite:stop(?config(tc_apps, Config)).

t_cluster_topology_api_empty_resp(_) ->
    ClusterTopologyPath = emqx_mgmt_api_test_util:api_path(["cluster", "topology"]),
    {ok, Resp} = emqx_mgmt_api_test_util:request_api(get, ClusterTopologyPath),
    ?assertEqual(
        [#{<<"core_node">> => atom_to_binary(node()), <<"replicant_nodes">> => []}],
        emqx_utils_json:decode(Resp, [return_maps])
    ).

t_cluster_topology_api_replicants(Config) ->
    [Core1, Core2, Replicant] = _NodesList = ?config(cluster, Config),
    {200, Core1Resp} = rpc:call(Core1, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    {200, Core2Resp} = rpc:call(Core2, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    {200, ReplResp} = rpc:call(Replicant, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    [
        ?assertMatch(
            [
                #{
                    core_node := Core1,
                    replicant_nodes := _
                },
                #{
                    core_node := Core2,
                    replicant_nodes := _
                }
            ],
            Resp
        )
     || Resp <- [lists:sort(R) || R <- [Core1Resp, Core2Resp, ReplResp]]
    ],
    %% Occasionally, the replicant may decide to not connect to one core (seen at tests)...
    Core1RespReplicants = lists:usort([
        Rep
     || R <- [Core1Resp, Core2Resp, ReplResp],
        #{replicant_nodes := Reps} <- R,
        #{node := Rep} <- Reps
    ]),
    ?assertMatch([Replicant], Core1RespReplicants),
    ok.

t_cluster_invite_api_timeout(Config) ->
    %% assert the cluster is created
    [Core1, Core2, Replicant] = _NodesList = ?config(cluster, Config),
    {200, Core1Resp} = rpc:call(Core1, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    ?assertMatch(
        [
            #{
                core_node := Core1,
                replicant_nodes := _
            },
            #{
                core_node := Core2,
                replicant_nodes := _
            }
        ],
        lists:sort(Core1Resp)
    ),
    %% Occasionally, the replicant may decide to connect to one core (seen at tests)...
    Core1RespReplicants = lists:usort([
        Rep
     || #{replicant_nodes := Reps} <- Core1Resp,
        #{node := Rep} <- Reps
    ]),
    ?assertMatch([Replicant], Core1RespReplicants),

    %% force leave the core2
    {204} = rpc:call(
        Core1,
        emqx_mgmt_api_cluster,
        force_leave,
        [delete, #{bindings => #{node => atom_to_binary(Core2)}}]
    ),

    %% assert the cluster is updated
    {200, Core1Resp2} = rpc:call(Core1, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    ?assertMatch(
        [
            #{
                core_node := Core1,
                replicant_nodes :=
                    [#{node := Replicant, streams := _}]
            }
        ],
        lists:sort(Core1Resp2)
    ),

    %% assert timeout parameter checking
    Invite = fun(Node, Timeout) ->
        Node1 = atom_to_binary(Node),
        rpc:call(
            Core1,
            emqx_mgmt_api_cluster,
            invite_node,
            [put, #{bindings => #{node => Node1}, body => #{<<"timeout">> => Timeout}}]
        )
    end,
    ?assertMatch(
        {400, #{code := 'BAD_REQUEST', message := <<"timeout must be an integer">>}},
        Invite(Core2, not_a_integer_timeout)
    ),
    ?assertMatch(
        {400, #{code := 'BAD_REQUEST', message := <<"timeout cannot be less than 5000ms">>}},
        Invite(Core2, 3000)
    ),

    %% assert cluster is updated after invite
    ?assertMatch(
        {200},
        Invite(Core2, 15000)
    ),
    {200, Core1Resp3} = rpc:call(Core1, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    ?assertMatch(
        [
            #{
                core_node := Core1,
                replicant_nodes :=
                    [#{node := Replicant, streams := _}]
            },
            #{
                core_node := Core2,
                replicant_nodes := _
            }
        ],
        lists:sort(Core1Resp3)
    ).

t_cluster_invite_async(Config) ->
    %% assert the cluster is created
    [Core1, Core2, Replicant] = _NodesList = ?config(cluster, Config),
    {200, Core1Resp} = rpc:call(Core1, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    ?assertMatch(
        [
            #{
                core_node := Core1,
                replicant_nodes := _
            },
            #{
                core_node := Core2,
                replicant_nodes := _
            }
        ],
        lists:sort(Core1Resp)
    ),
    %% Occasionally, the replicant may decide to connect to one core (seen at tests)...
    Core1RespReplicants = lists:usort([
        Rep
     || #{replicant_nodes := Reps} <- Core1Resp,
        #{node := Rep} <- Reps
    ]),
    ?assertMatch([Replicant], Core1RespReplicants),

    %% force leave the core2
    {204} = rpc:call(
        Core1,
        emqx_mgmt_api_cluster,
        force_leave,
        [delete, #{bindings => #{node => atom_to_binary(Core2)}}]
    ),
    %% assert the cluster is updated
    {200, Core1Resp2} = rpc:call(Core1, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    ?assertMatch(
        [
            #{
                core_node := Core1,
                replicant_nodes := _
            }
        ],
        lists:sort(Core1Resp2)
    ),

    Invite = fun(Node) ->
        Node1 = atom_to_binary(Node),
        rpc:call(
            Core1,
            emqx_mgmt_api_cluster,
            invite_node_async,
            [put, #{bindings => #{node => Node1}}]
        )
    end,

    %% parameter checking
    ?assertMatch(
        {400, #{code := 'BAD_REQUEST', message := <<"Can't invite self">>}},
        Invite(Core1)
    ),
    ?assertMatch(
        {200},
        Invite(Core2)
    ),
    %% already invited
    ?assertMatch(
        {400, #{
            code := 'BAD_REQUEST',
            message := <<"The invitation task already created for this node">>
        }},
        Invite(Core2)
    ),

    %% assert: core2 is in_progress status
    ?assertMatch(
        {200, #{in_progress := [#{node := Core2}]}},
        rpc:call(Core1, emqx_mgmt_api_cluster, get_invitation_status, [get, #{}])
    ),

    %% waiting the async invitation_succeed
    ?assertMatch({succeed, _}, waiting_the_async_invitation_succeed(Core1, Core2)),

    {200, Core1Resp3} = rpc:call(Core1, emqx_mgmt_api_cluster, cluster_topology, [get, #{}]),
    ?assertMatch(
        [
            #{
                core_node := Core1,
                replicant_nodes :=
                    [#{node := Replicant, streams := _}]
            },
            #{
                core_node := Core2,
                replicant_nodes := _
            }
        ],
        lists:sort(Core1Resp3)
    ),

    %% force leave the core2
    {204} = rpc:call(
        Core1,
        emqx_mgmt_api_cluster,
        force_leave,
        [delete, #{bindings => #{node => atom_to_binary(Core2)}}]
    ),
    %% invite core2 again
    ?assertMatch(
        {200},
        Invite(Core2)
    ),

    %% assert: core2 is in_progress status
    {200, InvitationStatus1} = rpc:call(Core1, emqx_mgmt_api_cluster, get_invitation_status, [
        get, #{}
    ]),
    ?assertMatch(
        #{succeed := [], in_progress := [#{node := Core2}], failed := []},
        InvitationStatus1
    ),

    %% waiting the async invitation_succeed
    ?assertMatch({succeed, _}, waiting_the_async_invitation_succeed(Core1, Core2)),

    {200, InvitationStatus2} = rpc:call(Core1, emqx_mgmt_api_cluster, get_invitation_status, [
        get, #{}
    ]),
    ?assertMatch(
        #{succeed := [#{node := Core2}], in_progress := [], failed := []},
        InvitationStatus2
    ),
    ok.

cluster(Config) ->
    NodeSpec = #{apps => ?APPS},
    Nodes = emqx_cth_cluster:start(
        [
            {data_backup_core1, NodeSpec#{role => core}},
            {data_backup_core2, NodeSpec#{role => core}},
            {data_backup_replicant, NodeSpec#{role => replicant}}
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

waiting_the_async_invitation_succeed(Node, TargetNode) ->
    waiting_the_async_invitation_succeed(Node, TargetNode, 100).

waiting_the_async_invitation_succeed(_Node, _TargetNode, 0) ->
    error(timeout);
waiting_the_async_invitation_succeed(Node, TargetNode, N) ->
    {200, #{
        in_progress := InProgress,
        succeed := Succeed,
        failed := Failed
    }} = rpc:call(Node, emqx_mgmt_api_cluster, get_invitation_status, [get, #{}]),
    case find_node_info_list(TargetNode, InProgress) of
        error ->
            case find_node_info_list(TargetNode, Succeed) of
                error ->
                    case find_node_info_list(TargetNode, Failed) of
                        error -> error;
                        Info1 -> {failed, Info1}
                    end;
                Info2 ->
                    {succeed, Info2}
            end;
        _Info ->
            timer:sleep(1000),
            waiting_the_async_invitation_succeed(Node, TargetNode, N - 1)
    end.

find_node_info_list(Node, List) ->
    L = lists:filter(fun(#{node := N}) -> N =:= Node end, List),
    case L of
        [] -> error;
        [Info] -> Info
    end.
