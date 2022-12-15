%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_listeners_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% setup
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_) ->
    ok.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_cluster_add_listener_failed_then_rollback(_Config) ->
    net_kernel:start(['node_api@127.0.0.1', longnames]),
    ct:timetrap({seconds, 120}),
    snabbkaffe:fix_ct_logging(),
    Seq1 = list_to_atom(atom_to_list(?MODULE) ++ "1"),
    Seq2 = list_to_atom(atom_to_list(?MODULE) ++ "2"),
    Cluster =
        [{Name, Opts}, {Name1, Opts1}] =
        cluster([
            {core, Seq1, #{listener_ports => [{tcp, 2883}]}},
            {core, Seq2, #{listener_ports => [{tcp, 3883}]}}
        ]),
    ct:pal("Starting ~p", [Cluster]),
    Node1 = emqx_common_test_helpers:start_slave(Name, Opts),
    ct:sleep(1000),
    Node2 = emqx_common_test_helpers:start_slave(Name1, Opts1),
    try
        ct:sleep(200),
        ct:pal("application:~p~n", [rpc:call(Node1, application, which_applications, [])]),
        ct:pal("mira: ~p~n", [rpc:call(Node1, application, get_all_env, [mria])]),
        Test = rpc:call(Node2, emqx_cluster_rpc, multicall, [inet, parse_address, ["127.0.0.1"]]),
        Node1Status = rpc:call(Node1, emqx_cluster_rpc, status, []),
        Node2Status = rpc:call(Node2, emqx_cluster_rpc, status, []),
        ct:pal("Test:~p~p~n", [Test, #{Node1 => Node1Status, Node2 => Node2Status}]),

        %% cluster_rpc is ok now.
        ?assertMatch(
            {ok, _},
            rpc:call(Node2, emqx_cluster_rpc, multicall, [inet, parse_address, ["127.0.0.1"]])
        ),

        %% Create the same port listener on both nodes to test the cluster failed
        Id = 'tcp:test2',
        Body = #{
            <<"acceptors">> => 16,
            <<"bind">> => 1235,
            <<"id">> => atom_to_binary(Id),
            <<"max_connections">> => 102400,
            <<"proxy_protocol">> => false,
            <<"proxy_protocol_timeout">> => <<"15s">>,
            <<"tcp_options">> => #{
                <<"active_n">> => 100,
                <<"buffer">> => <<"4KB">>,
                <<"nodelay">> => false,
                <<"reuseaddr">> => true,
                <<"send_timeout">> => <<"15s">>,
                <<"send_timeout_close">> => true
            },
            <<"type">> => <<"tcp">>,
            <<"zone">> => <<"default">>
        },
        Failure = rpc:call(Node1, emqx_mgmt_api_listeners, crud_listeners_by_id, [
            post,
            #{bindings => #{id => Id}, body => Body}
        ]),
        ?assertMatch({400, #{code := 'PARTIAL_FAILURE'}}, Failure),

        %% Check the listener is not created on both nodes
        ?assertEqual(
            undefined, rpc:call(Node1, emqx_conf, get, [[listeners, 'tcp:test2'], undefined])
        ),
        ?assertEqual(
            undefined, rpc:call(Node2, emqx_conf, get, [[listeners, 'tcp:test2'], undefined])
        ),

        %% Check cluster_rpc status is ok.
        Node3Status = rpc:call(Node1, emqx_cluster_rpc, status, []),
        Node4Status = rpc:call(Node2, emqx_cluster_rpc, status, []),
        ct:pal("~p~n", [#{Node1 => Node3Status, Node2 => Node4Status}]),
        ct:pal("ddd:~p~n", [rpc:call(Node1, ets, tab2list, [cluster_rpc_commit])]),
        ct:pal("ddd:~p~n", [rpc:call(Node1, ets, tab2list, [cluster_rpc_mfa])]),
        ct:sleep(1000),
        ct:pal("fff:~p~n", [rpc:call(Node1, ets, tab2list, [cluster_rpc_commit])]),
        ct:pal("fff:~p~n", [rpc:call(Node1, ets, tab2list, [cluster_rpc_mfa])]),
        ?assertMatch(
            {ok, _},
            rpc:call(Node1, emqx_cluster_rpc, multicall, [inet, parse_address, ["127.0.0.1"]])
        ),
        ?assertMatch(
            {ok, _},
            rpc:call(Node2, emqx_cluster_rpc, multicall, [inet, parse_address, ["127.0.0.1"]])
        )
    after
        Delete = rpc:call(Node1, emqx_mgmt_api_listeners, crud_listeners_by_id, [
            delete,
            #{bindings => #{id => 'tcp:test2'}}
        ]),
        ct:pal("cleanup listeners: ~p~n", [Delete]),
        emqx_common_test_helpers:stop_slave(Node1),
        emqx_common_test_helpers:stop_slave(Node2)
    end,
    ok.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

cluster(Specs) ->
    Env = [{emqx, boot_modules, all}],
    emqx_common_test_helpers:emqx_cluster(Specs, [
        {env, Env},
        {conf, [
            {[listeners, ssl, default, enabled], false},
            {[listeners, ws, default, enabled], false},
            {[listeners, wss, default, enabled], false}
        ]},
        {start_apps, true},
        {configure_gen_rpc, true},
        {apps, [emqx_conf]},
        {load_schema, true},
        {join_to, true},
        {env_handler, fun
            (emqx) ->
                application:set_env(emqx, boot_modules, all),
                emqx_config:put(
                    [node, cluster_call],
                    #{
                        cleanup_interval => 300000,
                        max_history => 100,
                        retry_interval => 1000
                    }
                ),
                ok;
            (_) ->
                ok
        end}
    ]).
