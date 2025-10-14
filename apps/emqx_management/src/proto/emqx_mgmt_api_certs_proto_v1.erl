%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%-------------------------------------------------------------------
-module(emqx_mgmt_api_certs_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    delete_bundle/3,
    add_managed_file/5
]).

-include_lib("emqx/include/bpapi.hrl").

-type maybe_namespace() :: emqx_config:maybe_namespace().
-type bundle_name() :: emqx_conf_certs:bundle_name().
-type file_kind() :: emqx_conf_certs:file_kind().

introduced_in() ->
    "6.1.0".

-spec delete_bundle([node()], maybe_namespace(), bundle_name()) ->
    emqx_rpc:erpc_multicall(ok | {error, term()}).
delete_bundle(Nodes, Namespace, BundleName) ->
    erpc:multicall(Nodes, emqx_conf_certs, delete_bundle_v1, [Namespace, BundleName]).

-spec add_managed_file([node()], maybe_namespace(), bundle_name(), file_kind(), iodata()) ->
    emqx_rpc:erpc_multicall(ok | {error, term()}).
add_managed_file(Nodes, Namespace, BundleName, Kind, Contents) ->
    erpc:multicall(
        Nodes,
        emqx_conf_certs,
        add_managed_file_v1,
        [Namespace, BundleName, Kind, Contents]
    ).
