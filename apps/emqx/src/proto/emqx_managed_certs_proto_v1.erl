%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%-------------------------------------------------------------------
-module(emqx_managed_certs_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    delete_bundle/3,
    add_managed_files/4,
    delete_managed_file/4
]).

-include_lib("emqx/include/bpapi.hrl").

-type maybe_namespace() :: emqx_config:maybe_namespace().
-type bundle_name() :: emqx_managed_certs:bundle_name().
-type file_kind() :: emqx_managed_certs:file_kind().

introduced_in() ->
    "6.1.0".

-spec delete_bundle([node()], maybe_namespace(), bundle_name()) ->
    emqx_rpc:erpc_multicall(ok | {error, term()}).
delete_bundle(Nodes, Namespace, BundleName) ->
    erpc:multicall(Nodes, emqx_managed_certs, delete_bundle_v1, [Namespace, BundleName]).

-spec add_managed_files([node()], maybe_namespace(), bundle_name(), #{file_kind() := iodata()}) ->
    emqx_rpc:erpc_multicall(ok | {error, #{file_kind() := file:posix()}}).
add_managed_files(Nodes, Namespace, BundleName, Files) ->
    erpc:multicall(
        Nodes,
        emqx_managed_certs,
        add_managed_files_v1,
        [Namespace, BundleName, Files]
    ).

-spec delete_managed_file([node()], maybe_namespace(), bundle_name(), file_kind()) ->
    emqx_rpc:erpc_multicall(ok | {error, #{file_kind() := file:posix()}}).
delete_managed_file(Nodes, Namespace, BundleName, Kind) ->
    erpc:multicall(
        Nodes,
        emqx_managed_certs,
        delete_managed_file_v1,
        [Namespace, BundleName, Kind]
    ).
