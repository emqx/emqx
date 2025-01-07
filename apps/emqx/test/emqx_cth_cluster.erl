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

%% @doc Common Test Helper / Running tests in a cluster
%%
%% This module allows setting up and tearing down clusters of EMQX nodes with
%% the purpose of running integration tests in a distributed environment, but
%% with the same isolation measures that `emqx_cth_suite` provides.
%%
%% Additionally to what `emqx_cth_suite` does with respect to isolation, each
%% node in the cluster is started with a separate, unique working directory.
%%
%% What should be started on each node is defined by the same appspecs that are
%% used by `emqx_cth_suite` to start applications on the CT node. However, there
%% are additional set of defaults applied to appspecs to make sure that the
%% cluster is started in a consistent, interconnected state, with no conflicts
%% between applications.
%%
%% Most of the time, you just need to:
%% 1. Describe the cluster with one or more _nodespecs_.
%% 2. Call `emqx_cth_cluster:start/2` before the testrun (e.g. in `init_per_suite/1`
%%    or `init_per_group/2`), providing unique work dir (e.g.
%%    `emqx_cth_suite:work_dir/1`). Save the result in a context.
%% 3. Call `emqx_cth_cluster:stop/1` after the testrun concludes (e.g.
%%    in `end_per_suite/1` or `end_per_group/2`) with the result from step 2.
-module(emqx_cth_cluster).

-export([start/1, start/2, restart/1]).
-export([stop/1, stop_node/1]).

-export([start_bare_nodes/1, start_bare_nodes/2]).

-export([share_load_module/2]).
-export([node_name/1, mk_nodespecs/2]).
-export([start_apps/2]).

-define(APPS_CLUSTERING, [gen_rpc, mria, ekka]).

-define(TIMEOUT_NODE_START_MS, 15000).
-define(TIMEOUT_APPS_START_MS, 30000).
-define(TIMEOUT_NODE_STOP_S, 15).
-define(TIMEOUT_CLUSTER_WAIT_MS, timer:seconds(10)).

%%

-type nodespec() :: {_ShortName :: atom(), #{
    % DB Role
    % Default: `core`
    role => core | replicant,

    % DB Backend
    % Default: `mnesia` if there are no replicants in cluster, otherwise `rlog`
    %
    % NOTE
    % Default are chosen with the intention of lowering the chance of observing
    % inconsistencies due to data races (i.e. missing mria shards on nodes where some
    % application hasn't been started yet).
    db_backend => mnesia | rlog,

    % Applications to start on the node
    % Default: only applications needed for clustering are started
    %
    % NOTES
    % 1. Apps needed for clustering started unconditionally.
    %  * It's not possible to redefine their startup order.
    %  * It's possible to add `{ekka, #{start => false}}` appspec though.
    % 2. There are defaults applied to some appspecs if they present.
    %  * We try to keep `emqx_conf` config consistent with default configuration of
    %    clustering applications.
    apps => [emqx_cth_suite:appspec()],

    base_port => inet:port_number(),

    % Node to join to in clustering phase
    % If set to `undefined` this node won't try to join the cluster
    % Default: no (first core node is used to join to by default)
    join_to => node() | undefined,

    %% Working directory
    %% If this directory is not empty, starting up the node applications will fail
    %% Default: "${ClusterOpts.work_dir}/${nodename}"
    work_dir => file:name()
}}.

-spec start([nodespec()], ClusterOpts) ->
    [node()]
when
    ClusterOpts :: #{
        %% Working directory
        %% Everything a test produces should go here. Each node's stuff should go in its
        %% own directory.
        work_dir := file:name()
    }.
start(Nodes, ClusterOpts) ->
    NodeSpecs = mk_nodespecs(Nodes, ClusterOpts),
    start(NodeSpecs).

start(NodeSpecs) ->
    emqx_common_test_helpers:clear_screen(),
    perform(start, NodeSpecs).

perform(Act, NodeSpecs) ->
    ct:pal("~ping nodes: ~p", [Act, NodeSpecs]),
    % 1. Start bare nodes with only basic applications running
    ok = start_nodes_init(NodeSpecs, ?TIMEOUT_NODE_START_MS),
    % 2. Start applications needed to enable clustering
    % Generally, this causes some applications to restart, but we deliberately don't
    % start them yet.
    case Act of
        start ->
            ShouldAppearInRunningNodes = [run_node_phase_cluster(Act, NS) || NS <- NodeSpecs],
            WaitClustered = lists:member(true, ShouldAppearInRunningNodes);
        restart ->
            Timeout = ?TIMEOUT_APPS_START_MS,
            _ = emqx_utils:pmap(fun(NS) -> run_node_phase_cluster(Act, NS) end, NodeSpecs, Timeout),
            WaitClustered = false
    end,
    % 3. Start applications after cluster is formed
    % Cluster-joins are complete, so they shouldn't restart in the background anymore.
    _ = emqx_utils:pmap(fun run_node_phase_apps/1, NodeSpecs, ?TIMEOUT_APPS_START_MS),
    Nodes = [Node || #{name := Node} <- NodeSpecs],
    %% 4. Wait for the nodes to cluster
    _Ok = WaitClustered andalso wait_clustered(Nodes, ?TIMEOUT_CLUSTER_WAIT_MS),
    Nodes.

%% Wait until all nodes see all nodes as mria running nodes
wait_clustered(Nodes, Timeout) ->
    Check = fun(Node) ->
        Running = erpc:call(Node, mria, running_nodes, []),
        case Nodes -- Running of
            [] ->
                true;
            NotRunning ->
                {false, NotRunning}
        end
    end,
    wait_clustered(Nodes, Check, deadline(Timeout)).

wait_clustered([], _Check, _Deadline) ->
    ok;
wait_clustered([Node | Nodes] = All, Check, Deadline) ->
    IsOverdue = is_overdue(Deadline),
    case Check(Node) of
        true ->
            wait_clustered(Nodes, Check, Deadline);
        {false, NodesNotRunnging} when IsOverdue ->
            error(
                {timeout, #{
                    checking_from_node => Node,
                    nodes_not_running => NodesNotRunnging
                }}
            );
        {false, _Nodes} ->
            timer:sleep(100),
            wait_clustered(All, Check, Deadline)
    end.

restart(NodeSpecs = [_ | _]) ->
    Nodes = [maps:get(name, Spec) || Spec <- NodeSpecs],
    ct:pal("Stopping peer nodes: ~p", [Nodes]),
    ok = stop(Nodes),
    perform(restart, NodeSpecs);
restart(NodeSpec = #{}) ->
    restart([NodeSpec]).

mk_nodespecs(Nodes, ClusterOpts) ->
    NodeSpecs = lists:zipwith(
        fun(N, {Name, Opts}) -> mk_init_nodespec(N, Name, Opts, ClusterOpts) end,
        lists:seq(1, length(Nodes)),
        Nodes
    ),
    CoreNodes = [Node || #{name := Node, role := core} <- NodeSpecs],
    Backend =
        case length(CoreNodes) of
            L when L == length(NodeSpecs) ->
                mnesia;
            _ ->
                rlog
        end,
    lists:map(
        fun(Spec0) ->
            Spec1 = maps:merge(#{core_nodes => CoreNodes, db_backend => Backend}, Spec0),
            Spec2 = merge_default_appspecs(Spec1, NodeSpecs),
            Spec3 = merge_clustering_appspecs(Spec2, NodeSpecs),
            Spec3
        end,
        NodeSpecs
    ).

mk_init_nodespec(N, Name, NodeOpts, ClusterOpts) ->
    Node = node_name(Name),
    BasePort = base_port(N),
    WorkDir = maps:get(work_dir, ClusterOpts),
    Defaults = #{
        name => Node,
        role => core,
        apps => [],
        base_port => BasePort,
        work_dir => filename:join([WorkDir, Node])
    },
    maps:merge(Defaults, NodeOpts).

merge_default_appspecs(#{apps := Apps} = Spec, NodeSpecs) ->
    Spec#{apps => [mk_node_appspec(App, Spec, NodeSpecs) || App <- Apps]}.

merge_clustering_appspecs(#{apps := Apps} = Spec, NodeSpecs) ->
    AppsClustering = lists:map(
        fun(App) ->
            case lists:keyfind(App, 1, Apps) of
                AppSpec = {App, _} ->
                    AppSpec;
                false ->
                    {App, default_appspec(App, Spec, NodeSpecs)}
            end
        end,
        ?APPS_CLUSTERING
    ),
    AppsRest = [AppSpec || AppSpec = {App, _} <- Apps, not lists:member(App, ?APPS_CLUSTERING)],
    Spec#{apps => AppsClustering ++ AppsRest}.

mk_node_appspec({App, Opts}, Spec, NodeSpecs) ->
    {App, emqx_cth_suite:merge_appspec(default_appspec(App, Spec, NodeSpecs), Opts)};
mk_node_appspec(App, Spec, NodeSpecs) ->
    {App, default_appspec(App, Spec, NodeSpecs)}.

default_appspec(gen_rpc, #{name := Node}, NodeSpecs) ->
    NodePorts = lists:foldl(
        fun(#{name := CNode, base_port := Port}, Acc) ->
            Acc#{CNode => {tcp, gen_rpc_port(Port)}}
        end,
        #{},
        NodeSpecs
    ),
    {tcp, Port} = maps:get(Node, NodePorts),
    #{
        override_env => [
            % NOTE
            % This is needed to make sure `gen_rpc` peers will find each other.
            {port_discovery, manual},
            {tcp_server_port, Port},
            {client_config_per_node, {internal, NodePorts}}
        ]
    };
default_appspec(mria, #{role := Role, db_backend := Backend}, _NodeSpecs) ->
    #{
        override_env => [
            {node_role, Role},
            {db_backend, Backend}
        ]
    };
default_appspec(ekka, Spec, _NodeSpecs) ->
    Overrides =
        case get_cluster_seeds(Spec) of
            [_ | _] = Seeds ->
                % NOTE
                % Presumably, this is needed for replicants to find core nodes.
                [{cluster_discovery, {static, [{seeds, Seeds}]}}];
            [] ->
                []
        end,
    #{
        override_env => Overrides
    };
default_appspec(emqx_conf, Spec, _NodeSpecs) ->
    % NOTE
    % This usually sets up a lot of `gen_rpc` / `mria` / `ekka` application envs in
    % `emqx_config:init_load/2` during configuration mapping, so we need to keep them
    % in sync with the values we set up here.
    #{
        name := Node,
        role := Role,
        db_backend := Backend,
        base_port := BasePort,
        work_dir := WorkDir
    } = Spec,
    Cluster =
        case get_cluster_seeds(Spec) of
            [_ | _] = Seeds ->
                % NOTE
                % Presumably, this is needed for replicants to find core nodes.
                #{discovery_strategy => static, static => #{seeds => Seeds}};
            [] ->
                #{}
        end,
    #{
        config => #{
            node => #{
                name => Node,
                role => Role,
                cookie => erlang:get_cookie(),
                % TODO: will it be synced to the same value eventually?
                data_dir => unicode:characters_to_binary(WorkDir),
                db_backend => Backend
            },
            cluster => Cluster,
            rpc => #{
                % NOTE
                % This (along with `gen_rpc` env overrides) is needed to make sure `gen_rpc`
                % peers will find each other.
                protocol => tcp,
                tcp_server_port => gen_rpc_port(BasePort),
                port_discovery => manual
            },
            listeners => allocate_listener_ports([tcp, ssl, ws, wss], Spec)
        }
    };
default_appspec(emqx, Spec, _NodeSpecs) ->
    #{config => #{listeners => allocate_listener_ports([tcp, ssl, ws, wss], Spec)}};
default_appspec(_App, _, _) ->
    #{}.

get_cluster_seeds(#{join_to := undefined}) ->
    [];
get_cluster_seeds(#{join_to := Node}) ->
    [Node];
get_cluster_seeds(#{core_nodes := CoreNodes}) ->
    CoreNodes.

allocate_listener_port(Type, #{base_port := BasePort}) ->
    Port = listener_port(BasePort, Type),
    #{Type => #{default => #{bind => format("127.0.0.1:~p", [Port])}}}.

allocate_listener_ports(Types, Spec) ->
    lists:foldl(fun maps:merge/2, #{}, [allocate_listener_port(Type, Spec) || Type <- Types]).

start_nodes_init(Specs, Timeout) ->
    Names = lists:map(fun(#{name := Name}) -> Name end, Specs),
    _Nodes = start_bare_nodes(Names, Timeout),
    lists:foreach(fun node_init/1, Specs).

start_bare_nodes(Names) ->
    start_bare_nodes(Names, ?TIMEOUT_NODE_START_MS).

start_bare_nodes(Names, Timeout) ->
    Args = erl_flags(),
    Envs = [],
    Waits = lists:map(
        fun(Name) ->
            WaitTag = {boot_complete, Name},
            WaitBoot = {self(), WaitTag},
            {ok, _} = emqx_cth_peer:start(Name, Args, Envs, WaitBoot),
            WaitTag
        end,
        Names
    ),
    Deadline = deadline(Timeout),
    Nodes = wait_boot_complete(Waits, Deadline),
    lists:foreach(fun(Node) -> pong = net_adm:ping(Node) end, Nodes),
    Nodes.

deadline(Timeout) ->
    erlang:monotonic_time() + erlang:convert_time_unit(Timeout, millisecond, native).

is_overdue(Deadline) ->
    erlang:monotonic_time() > Deadline.

wait_boot_complete([], _) ->
    [];
wait_boot_complete(Waits, Deadline) ->
    case is_overdue(Deadline) of
        true ->
            error({timeout, Waits});
        false ->
            ok
    end,
    receive
        {{boot_complete, _Name} = Wait, {started, Node, _Pid}} ->
            ct:pal("~p", [Wait]),
            [Node | wait_boot_complete(Waits -- [Wait], Deadline)];
        {{boot_complete, _Name}, Otherwise} ->
            error({unexpected, Otherwise})
    after 100 ->
        wait_boot_complete(Waits, Deadline)
    end.

node_init(#{name := Node, work_dir := WorkDir}) ->
    %% Create exclusive current directory for the node.  Some configurations, like plugin
    %% installation directory, are the same for the whole cluster, and nodes on the same
    %% machine will step on each other's toes...
    ok = filelib:ensure_path(WorkDir),
    ok = erpc:call(Node, file, set_cwd, [WorkDir]),
    %% Make it possible to call `ct:pal` and friends (if running under rebar3)
    _ = share_load_module(Node, cthr),
    %% Enable snabbkaffe trace forwarding
    ok = snabbkaffe:forward_trace(Node),
    when_cover_enabled(fun() ->
        case cover:start([Node]) of
            {ok, _} ->
                ok;
            {error, {already_started, _}} ->
                ok
        end
    end),
    ok.

%% Returns 'true' if this node should appear in running nodes list.
run_node_phase_cluster(Act, Spec = #{name := Node}) ->
    ok = load_apps(Node, Spec),
    ok = start_apps_clustering(Act, Node, Spec),
    maybe_join_cluster(Act, Node, Spec).

run_node_phase_apps(Spec = #{name := Node}) ->
    ok = start_apps(Node, Spec),
    ok.

load_apps(Node, #{apps := Apps}) ->
    erpc:call(Node, emqx_cth_suite, load_apps, [Apps]).

start_apps_clustering(Act, Node, #{apps := Apps} = Spec) ->
    SuiteOpts = (suite_opts(Spec))#{boot_type => Act},
    AppsClustering = [lists:keyfind(App, 1, Apps) || App <- ?APPS_CLUSTERING],
    _Started = erpc:call(Node, emqx_cth_suite, start, [AppsClustering, SuiteOpts]),
    ok.

start_apps(Node, #{apps := Apps} = Spec) ->
    SuiteOpts = suite_opts(Spec),
    AppsRest = [AppSpec || AppSpec = {App, _} <- Apps, not lists:member(App, ?APPS_CLUSTERING)],
    _Started = erpc:call(Node, emqx_cth_suite, start_apps, [AppsRest, SuiteOpts]),
    ok.

suite_opts(#{work_dir := WorkDir}) ->
    #{work_dir => WorkDir}.

%% Returns 'true' if this node should appear in the cluster.
maybe_join_cluster(restart, _Node, #{}) ->
    %% when restart, the node should already be in the cluster
    %% hence no need to (re)join
    true;
maybe_join_cluster(_Act, _Node, #{role := replicant}) ->
    true;
maybe_join_cluster(start, Node, Spec) ->
    case get_cluster_seeds(Spec) of
        [JoinTo | _] ->
            ok = join_cluster(Node, JoinTo),
            true;
        [] ->
            false
    end.

join_cluster(Node, JoinTo) ->
    case erpc:call(Node, ekka, join, [JoinTo]) of
        ok ->
            ok;
        ignore ->
            ok;
        Error ->
            error({failed_to_join_cluster, #{node => Node, error => Error}})
    end.

%%

stop(Nodes) ->
    _ = emqx_utils:pmap(fun stop_node/1, Nodes, ?TIMEOUT_NODE_STOP_S * 1000),
    ok.

stop_node(Name) ->
    Node = node_name(Name),
    when_cover_enabled(fun() -> ok = cover:flush([Node]) end),
    ok = emqx_cth_peer:stop(Node).

%% Ports

base_port(Number) ->
    10000 + Number * 100.

gen_rpc_port(BasePort) ->
    BasePort - 1.

listener_port(BasePort, tcp) ->
    BasePort;
listener_port(BasePort, ssl) ->
    BasePort + 1;
listener_port(BasePort, quic) ->
    BasePort + 2;
listener_port(BasePort, ws) ->
    BasePort + 3;
listener_port(BasePort, wss) ->
    BasePort + 4.

%%

erl_flags() ->
    %% One core
    ["+S", "1:1"] ++ ebin_path().

ebin_path() ->
    ["-pa" | lists:filter(fun is_lib/1, code:get_path())].

is_lib(Path) ->
    string:prefix(Path, code:lib_dir()) =:= nomatch andalso
        string:str(Path, "_build/default/plugins") =:= 0.

share_load_module(Node, Module) ->
    case code:get_object_code(Module) of
        {Module, Code, Filename} ->
            {module, Module} = erpc:call(Node, code, load_binary, [Module, Filename, Code]),
            ok;
        error ->
            error
    end.

-spec node_name(atom()) -> node().
node_name(Name) ->
    case string:tokens(atom_to_list(Name), "@") of
        [_Name, _Host] ->
            %% the name already has a @
            Name;
        _ ->
            list_to_atom(atom_to_list(Name) ++ "@" ++ host())
    end.

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    Host.

%%

format(Format, Args) ->
    unicode:characters_to_binary(io_lib:format(Format, Args)).

is_cover_enabled() ->
    case os:getenv("ENABLE_COVER_COMPILE") of
        "1" -> true;
        "true" -> true;
        _ -> false
    end.

when_cover_enabled(Fun) ->
    %% We need to check if cover is enabled to avoid crashes when attempting to start it
    %% on the peer.
    case is_cover_enabled() of
        true ->
            Fun();
        false ->
            ok
    end.
