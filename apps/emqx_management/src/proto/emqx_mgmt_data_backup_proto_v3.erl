%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_data_backup_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    list_files/3,
    read_file/4,
    delete_file/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.0.4".

%% `Namespace' scopes the operation to the caller's backup directory. Global
%% (and legacy) backups use `?global_ns' and live in the root backup directory;
%% namespaced backups are isolated under a per-namespace subdirectory.
-spec list_files([node()], emqx_config:maybe_namespace(), timeout()) ->
    emqx_rpc:erpc_multicall({non_neg_integer(), map()}).
list_files(Nodes, Namespace, Timeout) ->
    erpc:multicall(Nodes, emqx_mgmt_data_backup, list_files, [Namespace], Timeout).

-spec read_file(node(), emqx_config:maybe_namespace(), binary(), timeout()) ->
    {ok, binary()} | {error, _} | {badrpc, _}.
read_file(Node, Namespace, FileName, Timeout) ->
    rpc:call(Node, emqx_mgmt_data_backup, read_file, [Namespace, FileName], Timeout).

-spec delete_file(node(), emqx_config:maybe_namespace(), binary(), timeout()) ->
    ok | {error, _} | {badrpc, _}.
delete_file(Node, Namespace, FileName, Timeout) ->
    rpc:call(Node, emqx_mgmt_data_backup, delete_file, [Namespace, FileName], Timeout).
