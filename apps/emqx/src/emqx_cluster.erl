%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster).

-export([
    name/0,
    join/2,
    leave/1,
    force_leave/2,
    ensure_normal_mode/0,
    ensure_singleton_mode/0,
    is_single_node_mode/0,

    running_core_nodelist/0,
    pre_join/4
]).

%% RPC callback functions
-export([
    can_i_join/1
]).

-define(CLUSTER_MODE_NORMAL, normal).
-define(CLUSTER_MODE_SINGLE, singleton).

-ifdef(TEST).
%% Some tests run cluster without emqx_license app
-define(DEFAULT_MODE, ?CLUSTER_MODE_NORMAL).
-else.
-define(DEFAULT_MODE, ?CLUSTER_MODE_SINGLE).
-endif.

-doc """
Return cluster name set by the user in the configuration.

Not to be confused with the cluster ID which is set automatically by classy.
""".
-spec name() -> atom().
name() ->
    application:get_env(emqx, emqx_cluster_name, undefined).

-spec join(node(), join) -> ok | ignore | {error, _}.
join(Node, _) when Node =:= node() ->
    ignore;
join(Node, Intent) ->
    classy:join_node(Node, Intent).

-doc """
Special intent `force_kick` bypasses some checks.
""".
-spec leave(leave | force_kick) -> ok | {error, _}.
leave(Intent) ->
    classy:kick_node(node(), Intent).

pre_join(_Cluster, _Remote, PeerNode, _Intent) ->
    check_permission(PeerNode).

-doc """
Special intent `force_kick` bypasses some checks.
""".
-spec force_leave(node(), by_remote | force_kick) -> ok | ignore | {error, _}.
force_leave(Node, _) when Node =:= node() ->
    ignore;
force_leave(Node, Intent) ->
    classy:kick_node(Node, Intent).

check_permission(PeerNode) ->
    %% This call happens before clustered, so it's not possible to
    %% check peer node's bpapi versions.
    try
        emqx_cluster_proto_v1:can_i_join(node(), PeerNode)
    catch
        error:{erpc, noconnection} ->
            {error, {node_down, PeerNode}};
        error:{exception, undef, [{emqx_cluster, can_i_join, _, _}]} ->
            %% The peer node is older than 5.9.0
            %% This can happen during rolling upgrade.
            ok
    end.

%% @doc Check if the requesting node is allowed to join the cluster.
%% Called on the join target node via `emqx_cluster_proto_v1:can_i_join/2'.
-spec can_i_join(node()) -> ok | {error, string()}.
can_i_join(_RequestingNode) ->
    maybe
        ok ?= check_boot_complete(),
        ok ?= check_single_node_mode()
    end.

-doc "Refuse join requests while this node's boot is not complete.".
check_boot_complete() ->
    case is_boot_complete() of
        false ->
            Msg = io_lib:format(
                "Node ~s has not fully booted yet. Please retry after it is started.",
                [node()]
            ),
            {error, lists:flatten(Msg)};
        true ->
            ok
    end.

check_single_node_mode() ->
    case is_single_node_mode() of
        true ->
            Msg = lists:flatten(io_lib:format("Node ~s has a single node license", [node()])),
            {error, Msg};
        false ->
            ok
    end.

%% @doc Returns `true' when this node runs under a community (single-node) license.
-spec is_single_node_mode() -> boolean().
is_single_node_mode() ->
    case application:get_env(emqx, cluster_mode, ?DEFAULT_MODE) of
        ?CLUSTER_MODE_SINGLE -> true;
        _ -> false
    end.

%% @doc Set the cluster mode to single node mode.
%% Called by license checker for community license.
ensure_singleton_mode() ->
    ensure_mode(?DEFAULT_MODE).

running_core_nodelist() ->
    ordsets:intersection([classy:nodes(core), classy:nodes(connected)]).

%% @doc Allow clustering.
ensure_normal_mode() ->
    ensure_mode(?CLUSTER_MODE_NORMAL).

ensure_mode(Mode) ->
    _ = application:set_env(emqx, cluster_mode, Mode),
    ok.
