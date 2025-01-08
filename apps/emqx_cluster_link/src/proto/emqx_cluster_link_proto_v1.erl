%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    get_resource/2,
    get_all_resources/1
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 15000).

introduced_in() ->
    "5.7.2".

-spec get_resource([node()], binary()) ->
    emqx_rpc:erpc_multicall({ok, emqx_resource:resource_data()} | {error, not_found}).
get_resource(Nodes, ClusterName) ->
    erpc:multicall(Nodes, emqx_cluster_link_mqtt, get_resource_local_v1, [ClusterName], ?TIMEOUT).

-spec get_all_resources([node()]) ->
    emqx_rpc:erpc_multicall(#{binary() => emqx_resource:resource_data()}).
get_all_resources(Nodes) ->
    erpc:multicall(Nodes, emqx_cluster_link_mqtt, get_all_resources_local_v1, [], ?TIMEOUT).
