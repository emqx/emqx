%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_conf_cluster_sync_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_conf.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = ?config(priv_dir, Config),
    Cluster = mk_cluster_spec(#{}),
    Nodes = emqx_cth_cluster:start(Cluster, #{work_dir => WorkDir}),
    [{cluster_nodes, Nodes} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config)).

t_fix(Config) ->
    [Node1, Node2] = ?config(cluster_nodes, Config),
    ?ON(Node1, ?assertMatch({atomic, []}, emqx_cluster_rpc:status())),
    ?ON(Node2, ?assertMatch({atomic, []}, emqx_cluster_rpc:status())),
    ?ON(Node1, emqx_conf_proto_v4:update([<<"mqtt">>], #{<<"max_topic_levels">> => 100}, #{})),
    ?assertEqual(100, emqx_conf_proto_v4:get_config(Node1, [mqtt, max_topic_levels])),
    ?assertEqual(100, emqx_conf_proto_v4:get_config(Node2, [mqtt, max_topic_levels])),
    ?ON(
        Node1,
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 1},
                #{node := Node2, tnx_id := 1}
            ]},
            emqx_cluster_rpc:status()
        )
    ),
    %% fix normal, nothing changed
    ?ON(Node1, begin
        ok = emqx_conf_cli:admins(["fix"]),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 1},
                #{node := Node2, tnx_id := 1}
            ]},
            emqx_cluster_rpc:status()
        )
    end),
    %% fix inconsistent_key. tnx_id is the same, so nothing changed.
    emqx_conf_proto_v4:update(Node1, [<<"mqtt">>], #{<<"max_topic_levels">> => 99}, #{}),
    ?ON(Node1, begin
        ok = emqx_conf_cli:admins(["fix"]),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 1},
                #{node := Node2, tnx_id := 1}
            ]},
            emqx_cluster_rpc:status()
        )
    end),
    ?assertMatch(99, emqx_conf_proto_v4:get_config(Node1, [mqtt, max_topic_levels])),
    ?assertMatch(100, emqx_conf_proto_v4:get_config(Node2, [mqtt, max_topic_levels])),

    %% fix inconsistent_tnx_id_key. tnx_id and key are updated.
    ?ON(Node1, fake_mfa(2, Node1, {?MODULE, undef, []})),
    %% 2 -> fake_mfa, 3-> mark_begin_log, 4-> mqtt 5 -> zones
    ?ON(Node2, begin
        ok = emqx_conf_cli:admins(["fix"]),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 5},
                #{node := Node2, tnx_id := 5}
            ]},
            emqx_cluster_rpc:status()
        )
    end),
    ?assertMatch(99, emqx_conf_proto_v4:get_config(Node1, [mqtt, max_topic_levels])),
    ?assertMatch(99, emqx_conf_proto_v4:get_config(Node2, [mqtt, max_topic_levels])),

    %% fix inconsistent_tnx_id. tnx_id is updated.
    {ok, _} = ?ON(
        Node1, emqx_conf_proto_v4:update([<<"mqtt">>], #{<<"max_topic_levels">> => 98}, #{})
    ),
    ?ON(Node2, fake_mfa(7, Node2, {?MODULE, undef1, []})),
    ?ON(Node1, begin
        ok = emqx_conf_cli:admins(["fix"]),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 8},
                #{node := Node2, tnx_id := 8}
            ]},
            emqx_cluster_rpc:status()
        )
    end),
    ?assertMatch(98, emqx_conf_proto_v4:get_config(Node1, [mqtt, max_topic_levels])),
    ?assertMatch(98, emqx_conf_proto_v4:get_config(Node2, [mqtt, max_topic_levels])),
    %% unchanged
    ?ON(Node1, begin
        ok = emqx_conf_cli:admins(["fix"]),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 8},
                #{node := Node2, tnx_id := 8}
            ]},
            emqx_cluster_rpc:status()
        )
    end),
    ok.

fake_mfa(TnxId, Node, MFA) ->
    Func = fun() ->
        MFARec = #cluster_rpc_mfa{
            tnx_id = TnxId,
            mfa = MFA,
            initiator = Node,
            created_at = erlang:localtime()
        },
        ok = mnesia:write(?CLUSTER_MFA, MFARec, write),
        ok = emqx_cluster_rpc:commit(Node, TnxId)
    end,
    {atomic, ok} = mria:transaction(?CLUSTER_RPC_SHARD, Func, []),
    ok.

mk_cluster_spec(Opts) ->
    Conf = #{
        listeners => #{
            tcp => #{default => <<"marked_for_deletion">>},
            ssl => #{default => <<"marked_for_deletion">>},
            ws => #{default => <<"marked_for_deletion">>},
            wss => #{default => <<"marked_for_deletion">>}
        }
    },
    Apps = [
        {emqx, #{config => Conf}},
        {emqx_conf, #{config => Conf}}
    ],
    [
        {emqx_authz_api_cluster_SUITE1, Opts#{role => core, apps => Apps}},
        {emqx_authz_api_cluster_SUITE2, Opts#{role => core, apps => Apps}}
    ].
