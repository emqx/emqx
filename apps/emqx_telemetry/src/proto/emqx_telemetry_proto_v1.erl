%%--------------------------------------------------------------------
%%Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_telemetry_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    get_node_uuid/1,
    get_cluster_uuid/1
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec get_node_uuid(node()) -> {ok, binary()} | emqx_rpc:badrpc().
get_node_uuid(Node) ->
    rpc:call(Node, emqx_telemetry, get_node_uuid, []).

-spec get_cluster_uuid(node()) -> {ok, binary()} | emqx_rpc:badrpc().
get_cluster_uuid(Node) ->
    rpc:call(Node, emqx_telemetry, get_cluster_uuid, []).
