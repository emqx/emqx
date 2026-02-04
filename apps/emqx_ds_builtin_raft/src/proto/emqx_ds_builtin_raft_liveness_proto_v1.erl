%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_raft_liveness_proto_v1).
-moduledoc """
Protocol used to exchange shard liveness information.
""".

-behavior(emqx_bpapi).

%% API:
-export([multicast_shard_up/3]).

%% behavior callbacks:
-export([introduced_in/0]).

%%================================================================================
%% API functions
%%================================================================================

-doc """
OTX process uses this to notify all peer nodes that it has started.
""".
-spec multicast_shard_up([node()], emqx_ds:db(), emqx_ds:shard()) -> ok.
multicast_shard_up(Nodes, DB, Shard) ->
    erpc:multicast(Nodes, emqx_ds_builtin_raft_liveness, do_notify_shard_up_v1, [DB, Shard]).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() -> "6.1.1".
