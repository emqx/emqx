%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster).

-export([
    join/1,
    leave/0,
    force_leave/1,
    ensure_normal_mode/0,
    ensure_singleton_mode/0
]).

%% RPC callback functions
-export([
    can_i_join/1
]).

-define(CLUSTER_MODE_NORMAL, normal).
-define(CLUSTER_MODE_SINGLE, singleton).

%% Allow cluster when running tests
-ifdef(TEST).
-define(DEFAULT_MODE, ?CLUSTER_MODE_NORMAL).
-else.
-define(DEFAULT_MODE, ?CLUSTER_MODE_SINGLE).
-endif.

join(PeerNode) ->
    %% Local node starts with default license (singleton mode),
    %% But the peer node(s) may have a license which allows local node to join.
    %% So we do not check the license locally.
    case check_permission(PeerNode) of
        ok ->
            ekka:join(PeerNode);
        {error, Message} ->
            {error, Message}
    end.

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
        error:{exception, undef, [{emqx_cluster, can_i_join, _, _}]} ->
            %% The peer node is older than 5.9.0
            %% This can happen during rolling upgrade.
            ok
    end.

%% @doc Check if the requesting node is allowed to join the cluster.
%% Called by license checker for community license.
-spec can_i_join(node()) -> ok | {error, string()}.
can_i_join(_RequestingNode) ->
    case is_single_node_mode() of
        true ->
            Msg = lists:flatten(io_lib:format("Node ~s has a single node license", [node()])),
            {error, Msg};
        false ->
            ok
    end.

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
