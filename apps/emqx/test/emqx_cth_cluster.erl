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

-export([start/2, start_nodespecs/1]).
-export([stop/1, stop_node/1]).

-export([start_bare_node/2]).
-export([maybe_join_cluster/2]).

-export([share_load_module/2]).
-export([node_name/1, mk_nodespecs/2]).
-export([start_apps/2, set_node_opts/2]).

-define(APPS_CLUSTERING, [gen_rpc, mria, ekka]).

-define(TIMEOUT_NODE_START_MS, 15000).
-define(TIMEOUT_APPS_START_MS, 30000).
-define(TIMEOUT_NODE_STOP_S, 15).

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
    work_dir => file:name(),

    % Tooling to manage nodes
    % Default: `ct_slave`.
    driver => ct_slave | slave
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
    start_nodespecs(NodeSpecs).

start_nodespecs(NodeSpecs) ->
    ct:pal("Starting cluster:\n  ~p", [NodeSpecs]),
    % 1. Start bare nodes with only basic applications running
    _ = emqx_utils:pmap(fun start_node_init/1, NodeSpecs, ?TIMEOUT_NODE_START_MS),
    % 2. Start applications needed to enable clustering
    % Generally, this causes some applications to restart, but we deliberately don't
    % start them yet.
    _ = lists:foreach(fun run_node_phase_cluster/1, NodeSpecs),
    % 3. Start applications after cluster is formed
    % Cluster-joins are complete, so they shouldn't restart in the background anymore.
    _ = emqx_utils:pmap(fun run_node_phase_apps/1, NodeSpecs, ?TIMEOUT_APPS_START_MS),
    [Node || #{name := Node} <- NodeSpecs].

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
        work_dir => filename:join([WorkDir, Node]),
        driver => ct_slave
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

start_node_init(Spec = #{name := Node}) ->
    Node = start_bare_node(Node, Spec),
    % Make it possible to call `ct:pal` and friends (if running under rebar3)
    _ = share_load_module(Node, cthr),
    % Enable snabbkaffe trace forwarding
    ok = snabbkaffe:forward_trace(Node),
    ok.

run_node_phase_cluster(Spec = #{name := Node}) ->
    ok = load_apps(Node, Spec),
    ok = start_apps_clustering(Node, Spec),
    ok = maybe_join_cluster(Node, Spec),
    ok.

run_node_phase_apps(Spec = #{name := Node}) ->
    ok = start_apps(Node, Spec),
    ok.

set_node_opts(Node, Spec) ->
    erpc:call(Node, persistent_term, put, [{?MODULE, opts}, Spec]).

get_node_opts(Node) ->
    erpc:call(Node, persistent_term, get, [{?MODULE, opts}]).

load_apps(Node, #{apps := Apps}) ->
    erpc:call(Node, emqx_cth_suite, load_apps, [Apps]).

start_apps_clustering(Node, #{apps := Apps} = Spec) ->
    SuiteOpts = suite_opts(Spec),
    AppsClustering = [lists:keyfind(App, 1, Apps) || App <- ?APPS_CLUSTERING],
    _Started = erpc:call(Node, emqx_cth_suite, start, [AppsClustering, SuiteOpts]),
    ok.

start_apps(Node, #{apps := Apps} = Spec) ->
    SuiteOpts = suite_opts(Spec),
    AppsRest = [AppSpec || AppSpec = {App, _} <- Apps, not lists:member(App, ?APPS_CLUSTERING)],
    _Started = erpc:call(Node, emqx_cth_suite, start_apps, [AppsRest, SuiteOpts]),
    ok.

suite_opts(Spec) ->
    maps:with([work_dir], Spec).

maybe_join_cluster(_Node, #{role := replicant}) ->
    ok;
maybe_join_cluster(Node, Spec) ->
    case get_cluster_seeds(Spec) of
        [JoinTo | _] ->
            ok = join_cluster(Node, JoinTo);
        [] ->
            ok
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
    try get_node_opts(Node) of
        Opts ->
            stop_node(Name, Opts)
    catch
        error:{erpc, _} ->
            ok
    end.

stop_node(Node, #{driver := ct_slave}) ->
    case ct_slave:stop(Node, [{stop_timeout, ?TIMEOUT_NODE_STOP_S}]) of
        {ok, _} ->
            ok;
        {error, Reason, _} when Reason == not_connected; Reason == not_started ->
            ok
    end;
stop_node(Node, #{driver := slave}) ->
    slave:stop(Node).

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

-spec start_bare_node(atom(), map()) -> node().
start_bare_node(Name, Spec = #{driver := ct_slave}) ->
    {ok, Node} = ct_slave:start(
        node_name(Name),
        [
            {kill_if_fail, true},
            {monitor_master, true},
            {init_timeout, 20_000},
            {startup_timeout, 20_000},
            {erl_flags, erl_flags(Spec)},
            {env, []}
        ]
    ),
    init_bare_node(Node, Spec);
start_bare_node(Name, Spec = #{driver := slave}) ->
    ExtraErlFlags = maps:get(extra_erl_flags, Spec, ""),
    {ok, Node} = slave:start_link(host(), Name, ExtraErlFlags ++ " " ++ ebin_path()),
    init_bare_node(Node, Spec).

init_bare_node(Node, Spec) ->
    pong = net_adm:ping(Node),
    % Preserve node spec right on the remote node
    ok = set_node_opts(Node, Spec),
    Node.

erl_flags(Spec) ->
    ExtraErlFlags = maps:get(extra_erl_flags, Spec, ""),
    %% One core and redirecting logs to master
    ExtraErlFlags ++ " +S 1:1 -master " ++ atom_to_list(node()) ++ " " ++ ebin_path().

ebin_path() ->
    string:join(["-pa" | lists:filter(fun is_lib/1, code:get_path())], " ").

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
