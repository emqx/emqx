%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster).

-export([
    join/1,
    leave/0,
    force_leave/1,
    ensure_normal_mode/0,
    ensure_singleton_mode/0,
    is_single_node_mode/0,
    set_booting/1
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

join(PeerNode) ->
    case not is_boot_complete() andalso mria_rlog:role() =:= core of
        true ->
            {error,
                "This node has not fully booted yet. "
                "Please retry after it is started."};
        false ->
            do_join(PeerNode)
    end.

do_join(PeerNode) ->
    %% Local node starts with default license (singleton mode),
    %% But the peer node(s) may have a license which allows local node to join.
    %% So we do not check the license locally.
    case check_permission(PeerNode) of
        ok ->
            ekka:join(PeerNode);
        {error, Message} ->
            {error, Message}
    end.

-doc """
Mark boot as in progress (`true`) or complete (`false`).

Called by `emqx_machine`: set at boot entry and cleared once the boot app list
has been started and the ekka join callbacks are registered. While the flag is
set, `join/1` on a core node refuses to run: joining makes mria restart, and
doing that under a boot in progress crashes whichever mria-backed application
is starting at that moment, taking the node down.

Environments that boot without `emqx_machine` (e.g. common tests) never set
the flag, so `join/1` is not restricted there.
""".
-spec set_booting(boolean()) -> ok.
set_booting(Bool) when is_boolean(Bool) ->
    %% persistent: the value must survive a later `application:load(emqx)'
    %% because it can be set before the `emqx' application is loaded
    application:set_env(emqx, boot_in_progress, Bool, [{persistent, true}]).

is_booting() ->
    application:get_env(emqx, boot_in_progress, false).

-doc """
Return `true` once this node's boot is complete.

`is_booting/0` covers the first managed boot up to the point where the
ekka join callbacks are registered; `emqx_node_readiness` also covers
the `ensure_apps_started/0` re-run a cluster rejoin triggers.
""".
is_boot_complete() ->
    not is_booting() andalso emqx_node_readiness:is_ready().

leave() ->
    ekka:leave().

force_leave(Node) ->
    ekka:force_leave(Node).

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

%% @doc Allow clustering.
ensure_normal_mode() ->
    ensure_mode(?CLUSTER_MODE_NORMAL).

ensure_mode(Mode) ->
    _ = application:set_env(emqx, cluster_mode, Mode),
    ok.
