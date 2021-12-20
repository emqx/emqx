%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_monitor_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include("emqx_dashboard.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(t_badrpc_collect, Config) ->
    Cluster = cluster_specs(2),
    Apps = [emqx_modules, emqx_dashboard],
    Nodes = [N1, N2] =  lists:map(fun(Spec) -> start_slave(Spec, Apps) end, Cluster),
    %% form the cluster
    ok = rpc:call(N2, mria, join, [N1]),
    %% Wait until all nodes are healthy:
    [rpc:call(Node, mria_rlog, wait_for_shards, [[?DASHBOARD_SHARD], 5000])
     || Node <- Nodes],
    [ {nodes, Nodes}
    , {apps, Apps}
    | Config];
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(t_badrpc_collect, Config) ->
    Apps = ?config(apps, Config),
    Nodes = ?config(nodes, Config),
    lists:foreach(fun(Node) -> stop_slave(Node, Apps) end, Nodes),
    ok;
end_per_testcase(_, _Config) ->
    ok.

t_badrpc_collect(Config) ->
    [N1, N2] = ?config(nodes, Config),
    %% simulate badrpc on one node
    ok = rpc:call(N2, meck, new, [emqx_dashboard_collection, [no_history, no_link]]),
    %% we don't mock the `emqx_dashboard_collection:get_collect/0' to
    %% provoke the `badrpc' error.
    ?assertMatch(
       {200, #{nodes := 2}},
       rpc:call(N1, emqx_dashboard_monitor_api, current_counters, [get, #{}])),
    ok = rpc:call(N2, meck, unload, [emqx_dashboard_collection]),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

cluster_specs(NumNodes) ->
    BaseGenRpcPort = 9000,
    Specs0 = [#{ name => node_name(N)
               , num => N
               }
              || N <- lists:seq(1, NumNodes)],
    GenRpcPorts = maps:from_list([{node_id(Name), {tcp, BaseGenRpcPort + N}}
                                  || #{name := Name, num := N} <- Specs0]),
    [ Spec#{env => [ {gen_rpc, tcp_server_port, BaseGenRpcPort + N}
                   , {gen_rpc, client_config_per_node, {internal, GenRpcPorts}}
                   ]}
      || Spec = #{num := N} <- Specs0].

node_name(N) ->
    list_to_atom("n" ++ integer_to_list(N)).

node_id(Name) ->
    list_to_atom(lists:concat([Name, "@", host()])).

start_slave(Spec = #{ name := Name}, Apps) ->
    CommonBeamOpts = "+S 1:1 ", % We want VMs to only occupy a single core
    {ok, Node} = slave:start_link(host(), Name, CommonBeamOpts ++ ebin_path()),
    setup_node(Node, Spec, Apps),
    Node.

stop_slave(Node, Apps) ->
    ok = rpc:call(Node, emqx_common_test_helpers, start_apps, [Apps]),
    slave:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path() ->
    string:join(["-pa" | lists:filter(fun is_lib/1, code:get_path())], " ").

is_lib(Path) ->
    string:prefix(Path, code:lib_dir()) =:= nomatch.

setenv(Node, Env) ->
    [rpc:call(Node, application, set_env, [App, Key, Val]) || {App, Key, Val} <- Env].

setup_node(Node, _Spec = #{env := Env}, Apps) ->
    %% load these before starting ekka and such
    [rpc:call(Node, application, load, [App]) || App <- [gen_rpc, emqx_conf, emqx]],
    setenv(Node, Env),
    EnvHandler =
        fun(emqx) ->
                application:set_env(emqx, boot_modules, [router, broker]);
           (_) ->
                ok
        end,
    ok = rpc:call(Node, emqx_common_test_helpers, start_apps, [Apps, EnvHandler]),
    ok.
