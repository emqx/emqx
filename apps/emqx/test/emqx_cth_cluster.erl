%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([wait_for_conditions/3, verify_peers/2, verify_run_level/1, verify_business_apps/0]).
-export([stop/1, stop_node/1]).

-export([join_cluster/2]).

-export([share_load_module/2]).
-export([node_name/1, mk_nodespecs/2]).
-export([sync_routes/1, sync_routes/2, get_tcp_mqtt_port/1]).
-export([when_cover_enabled/1]).
-export([setup_logging/1, do_setup_logging/1]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(TIMEOUT_NODE_START_MS, 15000).
-define(TIMEOUT_NODE_STOP_S, 15).

-define(log_handler, emqx_cth_cluster_log_handler).

%%

-type nodespec() :: {_ShortName :: atom(), #{
    % DB Role
    % Default: `core`
    role => core | replicant,

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

    base_port => inet:port_Node(),

    % number to join to in clustering phase
    % If set to `undefined` this node won't try to join the cluster
    % Default: no (first core node is used to join to by default)
    join_to => node() | undefined,

    %% Options that may affect `emqx_cth_peer:start{,_link}', such as `shutdown'.
    start_opts => #{},

    %% Working directory
    %% If this directory is not empty, starting up the node applications will fail
    %% Default: "${ClusterOpts.work_dir}/${nodename}"
    work_dir => file:name()
}}.

-type opts() :: #{
    %% Working directory
    %% Everything a test produces should go here. Each node's stuff should go in its
    %% own directory.
    work_dir := file:name(),
    %% Environment variables
    %% Defaults to `[]`.
    env_vars => [{string(), string()}]
}.

-type bakedspec() :: #{atom() => _}.

-spec start([nodespec()], opts()) -> [node()].
start(Nodes, ClusterOpts) ->
    NodeSpecs = mk_nodespecs(Nodes, ClusterOpts),
    StartOpts = get_start_opts(ClusterOpts),
    emqx_common_test_helpers:clear_screen(),
    perform(start, NodeSpecs, StartOpts).

-spec start(Complete :: [nodespec()]) -> [node()].
start(NodeSpecs) ->
    emqx_common_test_helpers:clear_screen(),
    perform(start, NodeSpecs, _StartOpts = #{}).

-doc """
Periodically run each function from the second argument on the set of nodes
until all functions retun ok or until timeout.

On success, this function returns an empty list,
or list of errors otherwise.
""".
-spec wait_for_conditions([node()], [fun((node()) -> ok | {error, _})], non_neg_integer()) ->
    [_Error].
wait_for_conditions(Nodes, Conditions, Timeout) ->
    do_wait_for_conditions(Nodes, Conditions, deadline(Timeout)).

-doc """
Check if the classy node set `NodeSet' contains all nodes from the list.
""".
-spec verify_peers(atom(), [node()]) -> fun((node()) -> ok | {error, _}).
verify_peers(NodeSet, Expected) ->
    fun(Node) ->
        Got = erpc:call(Node, classy, nodes, [NodeSet]),
        case Expected -- Got of
            [] ->
                ok;
            Diff ->
                {error, {peers_down, #{missing => Diff, got => Got}}}
        end
    end.

-doc """
Check if the node run level is greater or equal to the given one.
""".
-spec verify_run_level(classy:run_level()) -> fun((node()) -> ok | {error, _}).
verify_run_level(Expected) ->
    NExpected = classy_rl_changer:to_int(Expected),
    fun(Node) ->
        Got = erpc:call(Node, classy, run_level, []),
        NGot = classy_rl_changer:to_int(Got),
        case NGot >= NExpected of
            true ->
                ok;
            false ->
                {error, {run_level, #{got => Got, expected => Expected}}}
        end
    end.

-doc """
Check if the node has started business applications.
""".
-spec verify_business_apps() -> fun((node()) -> ok | {error, _}).
verify_business_apps() ->
    fun(Node) ->
        case erpc:call(Node, emqx_cth_suite, check_business_apps, []) of
            true ->
                ok;
            false ->
                {error, business_apps_not_running}
        end
    end.

-spec restart(Complete :: [bakedspec()] | bakedspec()) -> [node()].
restart(NodeSpecs = [_ | _]) ->
    Nodes = [maps:get(name, Spec) || Spec <- NodeSpecs],
    Cores = [maps:get(name, Spec) || Spec = #{role := core} <- NodeSpecs],
    %% The default `shutdown` option that we currently pass to `peer` does not allow
    %% `mnesia` to correctly sync its log and shutdown properly, even when using `shutdown
    %% => 5_000` in some situations.  Since we are restarting the cluster in our test
    %% here, it's expected that we don't lose the data in mnesia.  So we explicitly flush
    %% it here.
    ct:pal("Flushing mnesia in cores: ~p", [Cores]),
    emqx_utils:pforeach(
        fun(N) ->
            maybe
                Pid = whereis(N),
                true ?= is_pid(Pid),
                erpc:call(N, fun mnesia:sync_log/0)
            end
        end,
        Cores
    ),
    ct:pal("Stopping peer nodes: ~p", [Nodes]),
    ok = stop(Nodes),
    perform(restart, NodeSpecs, _Opts = #{});
restart(NodeSpec = #{}) ->
    restart([NodeSpec]).

get_start_opts(ClusterOpts) ->
    maps:with([start_apps_timeout, env_vars], ClusterOpts).

-spec mk_nodespecs([nodespec()], opts()) -> [bakedspec()].
mk_nodespecs(Nodes, ClusterOpts) ->
    NodeSpecs = lists:zipwith(
        fun(N, {Name, Opts}) -> mk_init_nodespec(N, Name, Opts, ClusterOpts) end,
        lists:seq(1, length(Nodes)),
        Nodes
    ),
    CoreNodes = [Node || #{name := Node, role := core} <- NodeSpecs],
    lists:map(
        fun(Spec0) ->
            Spec1 = maps:merge(#{core_nodes => CoreNodes}, Spec0),
            merge_default_appspecs(Spec1, NodeSpecs)
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
        start_opts => maps:get(start_opts, ClusterOpts, #{}),
        work_dir => filename:join([WorkDir, Node])
    },
    maps:merge(Defaults, NodeOpts).

merge_default_appspecs(#{apps := Apps} = Spec, NodeSpecs) ->
    Spec#{
        apps => [
            mk_node_appspec(App, Spec, NodeSpecs)
         || App <- [emqx_conf, gen_rpc, mria, classy | Apps]
        ]
    }.

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
default_appspec(classy, #{role := Role} = Spec, _NodeSpecs) ->
    Spec#{
        before_start =>
            fun() ->
                %% TODO: hack. Prevent replicants from advancing the
                %% run level before mria application is configured by
                %% the test harness.
                application:set_env(mria, node_role, Role)
            end
    };
default_appspec(mria, Spec, _NodeSpecs) ->
    Spec#{
        start => false
    };
default_appspec(emqx_conf, Spec, _NodeSpecs) ->
    % NOTE
    % This usually sets up a lot of `gen_rpc` / `mria` / `ekka` application envs in
    % `emqx_config:init_load/2` during configuration mapping, so we need to keep them
    % in sync with the values we set up here.
    #{
        name := Node,
        role := Role,
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
                data_dir => unicode:characters_to_binary(WorkDir)
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
        },
        start => true
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

start_nodes_init(Specs, Timeout, StartOpts) ->
    _Nodes = start_bare_nodes(Specs, Timeout, StartOpts),
    lists:foreach(fun node_init/1, Specs).

start_bare_nodes(Specs, Timeout, StartOpts) ->
    Args = erl_flags(),
    Envs = maps:get(env_vars, StartOpts, []),
    Waits = lists:map(
        fun(#{name := Name} = Spec) ->
            WaitTag = {boot_complete, Name},
            WaitBoot = {self(), WaitTag},
            Opts = peer_start_opts(Spec),
            {ok, _} = emqx_cth_peer:start(Name, Args, Envs, WaitBoot, Opts),
            WaitTag
        end,
        Specs
    ),
    Deadline = deadline(Timeout),
    Nodes = wait_boot_complete(Waits, Deadline),
    lists:foreach(fun(Node) -> pong = net_adm:ping(Node) end, Nodes),
    setup_logging(Specs),
    Nodes.

peer_start_opts(Spec) ->
    maps:get(start_opts, Spec, #{}).

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

do_setup_logging(#{work_dir := WD}) ->
    LogFile = filename:join(WD, "erlang.log"),
    Level = debug,
    HandlerConf = #{
        level => Level,
        filter_default => log,
        config => #{
            type => file,
            file => LogFile
        },
        formatter =>
            {logger_formatter, #{
                single_line => false,
                legacy_header => true
            }}
    },
    ok = logger:update_primary_config(#{level => Level}),
    ok = logger:add_handler(?log_handler, logger_std_h, HandlerConf),
    ok.

%% Helper function that sets up logging on remote node to a temporary
%% files. Useful for debugging. Note: this function is NOT used by
%% default for nodes started using functions from this module.
setup_logging(Specs) ->
    _ = [erpc:call(Node, ?MODULE, do_setup_logging, [Spec]) || Spec = #{name := Node} <- Specs],
    ok.

-spec get_tcp_mqtt_port(node()) -> pos_integer().
get_tcp_mqtt_port(Node) ->
    {_Host, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

%% Returns 'true' if this node should appear in running nodes list.
run_node_phase_cluster(Act, Spec = #{name := Node}) ->
    ok = load_apps(Node, Spec),
    ok = start_apps_clustering(Act, Node, Spec),
    maybe_join_cluster(Act, Node, Spec).

load_apps(Node, #{apps := Apps}) ->
    erpc:call(Node, emqx_cth_suite, load_apps, [Apps]).

start_apps_clustering(Act, Node, #{apps := Apps} = Spec) ->
    SuiteOpts = suite_opts(Act, Spec),
    _Started = erpc:call(Node, emqx_cth_suite, start, [0, Apps, SuiteOpts]),
    ok.

-spec sync_routes([node()]) -> ok.
sync_routes(Nodes) ->
    sync_routes(Nodes, 15_000).

%% @doc Wait until routing tables on the given set of nodes converge
%% to the same value.
%%
%% Since routing tables use mria merge tables, propagation of routes
%% to peers is async, even between the cores. Therefore, tests are
%% advised to call this function after creating a subscription if they
%% expect effects of that subscription to be seen on other nodes.
-spec sync_routes([node()], pos_integer()) -> ok.
sync_routes([], _) ->
    ok;
sync_routes(Nodes, Timeout) ->
    NRetries = 10,
    ?retry(
        Timeout div NRetries,
        NRetries,
        begin
            %% Compare full route entries (topic + destination), not just
            %% topic names. With the v3 schema's merge-table propagation,
            %% multiple nodes can each have their own local subscription
            %% to the same topic and `emqx_router:topics/0` returns the
            %% same single-element list on all of them before
            %% cross-propagation has actually delivered the remote
            %% routes. Folding the route table waits until each node has
            %% merged in every peer's destinations.
            Routes = erpc:multicall(
                Nodes,
                emqx_router,
                foldl_routes,
                [fun(R, Acc) -> [R | Acc] end, []],
                Timeout
            ),
            Diff = lists:uniq(
                fun({Node, Resp}) ->
                    case Resp of
                        {ok, L} -> lists:sort(L);
                        _Error -> {error, Node}
                    end
                end,
                lists:zip(Nodes, Routes)
            ),
            Diagnostic = erpc:multicall(
                Nodes,
                fun() ->
                    #{
                        node => node(),
                        mria_route_m_peers => mria_rlog_replica:ls(route_shard_m),
                        cluster => mria:cluster_nodes(all),
                        peers => nodes()
                    }
                end
            ),
            ?assertMatch(
                [{_, {ok, _}}],
                Diff,
                #{
                    msg => routes_did_not_converge,
                    nodes => nodes(),
                    diagnostic => Diagnostic
                }
            )
        end
    ).

suite_opts(restart, Spec) ->
    maps:merge(#{work_dir_dirty => true}, suite_opts(Spec));
suite_opts(_, Spec) ->
    suite_opts(Spec).

suite_opts(Spec) ->
    maps:with([work_dir, work_dir_dirty], Spec).

%% Returns 'true' if this node should appear in the cluster.
maybe_join_cluster(restart, _Node, #{}) ->
    %% when restart, the node should already be in the cluster
    %% hence no need to (re)join
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
    Result = ?tp_span(
        notice,
        test_cluster_join,
        #{node => Node, join_to => JoinTo},
        erpc:call(Node, emqx_cluster, join, [JoinTo, join])
    ),
    case Result of
        ok ->
            ok;
        ignore ->
            ok;
        Error ->
            ct:pal("Failed to join cluster: ~p", [Error]),
            error({failed_to_join_cluster, #{node => Node, error => Error}})
    end.

%%

stop(Nodes) ->
    _ = emqx_utils:pmap(fun stop_node/1, Nodes, ?TIMEOUT_NODE_STOP_S * 1000),
    ok.

stop_node(Name) when is_atom(Name) ->
    Node = node_name(Name),
    when_cover_enabled(fun() -> ok = cover:flush([Node]) end),
    _ = rpc:call(Node, logger_std_h, filesync, [?log_handler]),
    ok = emqx_cth_peer:stop(Node);
stop_node(#{name := Name}) ->
    stop_node(Name).

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

perform(Act, NodeSpecs, Opts) ->
    ct:pal("~ping nodes: ~p", [Act, NodeSpecs]),
    Nodes = [Node || #{name := Node} <- NodeSpecs],
    case do_perform(Act, NodeSpecs, Opts) of
        [] ->
            Nodes;
        Errors = [_ | _] ->
            %% A failure partway through startup leaves the already-started peer
            %% control processes registered locally (by their node name). If we
            %% leak them, a retry of the same test case (e.g. via the flaky-test
            %% hook, `emqx_cth_ct_hook_flaky`) reuses the same node names and
            %% `emqx_cth_peer:do_start/6`'s `erlang:register/2` would crash with
            %% `badarg`. Best-effort cleanup before re-raising so a retry starts
            %% from scratch.

            ct:pal("cleaning up partially started nodes after ~p", [Errors]),
            catch stop(Nodes),
            error({Act, Errors})
    end.

do_perform(Act, NodeSpecs, Opts) ->
    % 1. Start bare nodes with only basic applications running
    ok = start_nodes_init(NodeSpecs, ?TIMEOUT_NODE_START_MS, Opts),
    Nodes = [Node || #{name := Node} <- NodeSpecs],
    CommonChecks = [verify_run_level(cluster), verify_business_apps()],
    %% 2. Start applications:
    ShouldAppearInRunningNodes = [run_node_phase_cluster(Act, NS) || NS <- NodeSpecs],
    %% 3. Wait for the readiness:
    Checks =
        case Act of
            start ->
                WaitClustered = lists:member(true, ShouldAppearInRunningNodes),
                [verify_peers(connected, Nodes) || WaitClustered] ++
                    CommonChecks;
            restart ->
                CommonChecks
        end,
    wait_for_conditions(Nodes, Checks, ?TIMEOUT_NODE_START_MS).

%%

-spec do_wait_for_conditions([node()], [fun((node()) -> ok | {error, _})], non_neg_integer()) ->
    [_Error].
do_wait_for_conditions(Nodes, Conditions, Deadline) ->
    case check_conditions(Nodes, Conditions) of
        [] ->
            [];
        [_ | _] = Errors ->
            case is_overdue(Deadline) of
                true ->
                    Errors;
                false ->
                    timer:sleep(100),
                    do_wait_for_conditions(Nodes, Conditions, Deadline)
            end
    end.

check_conditions(_Nodes, []) ->
    [];
check_conditions(Nodes, [Condition | Rest]) ->
    Results = emqx_utils:pmap(
        fun(Node) ->
            try Condition(Node) of
                ok -> ok;
                {error, Err} -> {error, Node, Err}
            catch
                EC:Err:Stack ->
                    {error, Node, {EC, Err, Stack}}
            end
        end,
        Nodes
    ),
    case lists:filter(fun(A) -> A =/= ok end, Results) of
        [] ->
            check_conditions(Nodes, Rest);
        Errors ->
            Errors
    end.

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
