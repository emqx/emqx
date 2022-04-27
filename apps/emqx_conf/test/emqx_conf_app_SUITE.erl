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

-module(emqx_conf_app_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_conf.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_copy_conf_override_on_restarts(_Config) ->
    ct:timetrap({seconds, 120}),
    snabbkaffe:fix_ct_logging(),
    Cluster = cluster([core, core, core]),
    try
        %% 1. Start all nodes
        Nodes = start_cluster(Cluster),
        [join_cluster(Spec) || Spec <- Cluster],
        assert_config_load_done(Nodes),

        %% 2. Stop each in order.
        lists:foreach(fun stop_slave/1, Nodes),

        %% 3. Restart nodes in the same order.  This should not
        %% crash and eventually all nodes should be ready.
        start_cluster_async(Cluster),

        timer:sleep(15_000),

        assert_config_load_done(Nodes),

        ok
    after
        teardown_cluster(Cluster)
    end.

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

assert_config_load_done(Nodes) ->
    lists:foreach(
        fun(Node) ->
            Done = rpc:call(Node, emqx_app, get_init_config_load_done, []),
            ?assert(Done, #{node => Node})
        end,
        Nodes
    ).

start_cluster(Specs) ->
    [start_slave(I) || I <- Specs].

start_cluster_async(Specs) ->
    [
        begin
            spawn_link(fun() -> start_slave(I) end),
            timer:sleep(7_000)
        end
     || I <- Specs
    ].

cluster(Specs) ->
    cluster(Specs, []).

cluster(Specs0, CommonEnv) ->
    Specs1 = lists:zip(Specs0, lists:seq(1, length(Specs0))),
    Specs = expand_node_specs(Specs1, CommonEnv),
    CoreNodes = [node_id(Name) || {{core, Name, _}, _} <- Specs],
    %% Assign grpc ports:
    BaseGenRpcPort = 9000,
    GenRpcPorts = maps:from_list([
        {node_id(Name), {tcp, BaseGenRpcPort + Num}}
     || {{_, Name, _}, Num} <- Specs
    ]),
    %% Set the default node of the cluster:
    JoinTo =
        case CoreNodes of
            [First | _] -> #{join_to => First};
            _ -> #{}
        end,
    [
        JoinTo#{
            name => Name,
            node => node_id(Name),
            env => [
                {mria, core_nodes, CoreNodes},
                {mria, node_role, Role},
                {gen_rpc, tcp_server_port, BaseGenRpcPort + Number},
                {gen_rpc, client_config_per_node, {internal, GenRpcPorts}}
                | Env
            ],
            number => Number,
            role => Role
        }
     || {{Role, Name, Env}, Number} <- Specs
    ].

start_apps(Node) ->
    Handler = fun
        (emqx) ->
            application:set_env(emqx, boot_modules, []),
            ok;
        (_) ->
            ok
    end,
    {Node, ok} =
        {Node, rpc:call(Node, emqx_common_test_helpers, start_apps, [[emqx_conf], Handler])},
    ok.

stop_apps(Node) ->
    ok = rpc:call(Node, emqx_common_test_helpers, stop_apps, [[emqx_conf]]).

join_cluster(#{node := Node, join_to := JoinTo}) ->
    case rpc:call(Node, ekka, join, [JoinTo]) of
        ok -> ok;
        ignore -> ok;
        Err -> error({failed_to_join_cluster, #{node => Node, error => Err}})
    end.

start_slave(#{node := Node, env := Env}) ->
    %% We want VMs to only occupy a single core
    CommonBeamOpts =
        "+S 1:1 " ++
            %% redirect logs to the master test node
            " -master " ++ atom_to_list(node()) ++ " ",
    %% We use `ct_slave' instead of `slave' because, in
    %% `t_copy_conf_override_on_restarts', the nodes might be stuck
    %% some time during boot up, and `slave' has a hard-coded boot
    %% timeout.
    {ok, Node} = ct_slave:start(
        Node,
        [
            {erl_flags, CommonBeamOpts ++ ebin_path()},
            {kill_if_fail, true},
            {monitor_master, true},
            {init_timeout, 30_000},
            {startup_timeout, 30_000}
        ]
    ),

    %% Load apps before setting the enviroment variables to avoid
    %% overriding the environment during app start:
    [rpc:call(Node, application, load, [App]) || App <- [gen_rpc]],
    %% Disable gen_rpc listener by default:
    Env1 = [{gen_rpc, tcp_server_port, false} | Env],
    setenv(Node, Env1),
    ok = start_apps(Node),
    Node.

expand_node_specs(Specs, CommonEnv) ->
    lists:map(
        fun({Spec, Num}) ->
            {
                case Spec of
                    core ->
                        {core, gen_node_name(Num), CommonEnv};
                    replicant ->
                        {replicant, gen_node_name(Num), CommonEnv};
                    {Role, Name} when is_atom(Name) ->
                        {Role, Name, CommonEnv};
                    {Role, Env} when is_list(Env) ->
                        {Role, gen_node_name(Num), CommonEnv ++ Env};
                    {Role, Name, Env} ->
                        {Role, Name, CommonEnv ++ Env}
                end,
                Num
            }
        end,
        Specs
    ).

setenv(Node, Env) ->
    [rpc:call(Node, application, set_env, [App, Key, Val]) || {App, Key, Val} <- Env].

teardown_cluster(Specs) ->
    Nodes = [I || #{node := I} <- Specs],
    [rpc:call(I, emqx_common_test_helpers, stop_apps, [emqx_conf]) || I <- Nodes],
    [stop_slave(I) || I <- Nodes],
    ok.

stop_slave(Node) ->
    ct_slave:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    Host.

node_id(Name) ->
    list_to_atom(lists:concat([Name, "@", host()])).

gen_node_name(N) ->
    list_to_atom("n" ++ integer_to_list(N)).

ebin_path() ->
    string:join(["-pa" | paths()], " ").

paths() ->
    [
        Path
     || Path <- code:get_path(),
        string:prefix(Path, code:lib_dir()) =:= nomatch,
        string:str(Path, "_build/default/plugins") =:= 0
    ].
