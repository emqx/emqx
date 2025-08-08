%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_conf_proto_v5).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    sync_data_from_node/1,
    get_config/3,
    get_config/4,

    update/3,
    update/4,
    remove_config/2,
    remove_config/3,

    reset/2,
    reset/3,

    get_override_config_file/1,

    get_hocon_config/2,
    get_hocon_config/3,

    get_raw_config/3
]).

-include_lib("emqx/include/bpapi.hrl").

-type update_config_key_path() :: [emqx_utils_maps:config_key(), ...].
-type maybe_namespace() :: emqx_config:maybe_namespace().

introduced_in() ->
    "6.0.0".

-spec sync_data_from_node(node()) -> {ok, binary()} | emqx_rpc:badrpc().
sync_data_from_node(Node) ->
    rpc:call(Node, emqx_conf_app, sync_data_from_node, [], 20_000).

-spec get_config(node(), maybe_namespace(), emqx_utils_maps:config_key_path()) ->
    term() | emqx_rpc:badrpc().
get_config(Node, Namespace, KeyPath) ->
    rpc:call(Node, emqx, get_namespaced_config, [Namespace, KeyPath]).

-spec get_config(node(), maybe_namespace(), emqx_utils_maps:config_key_path(), _Default) ->
    term() | emqx_rpc:badrpc().
get_config(Node, Namespace, KeyPath, Default) ->
    rpc:call(Node, emqx, get_namespaced_config, [Namespace, KeyPath, Default]).

-spec update(
    update_config_key_path(),
    emqx_config:update_request(),
    emqx_config:update_opts()
) -> {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update(KeyPath, UpdateReq, Opts) ->
    emqx_cluster_rpc:multicall(emqx, update_config, [KeyPath, UpdateReq, Opts]).

-spec update(
    node(),
    update_config_key_path(),
    emqx_config:update_request(),
    emqx_config:update_opts()
) ->
    {ok, emqx_config:update_result()}
    | {error, emqx_config:update_error()}
    | emqx_rpc:badrpc().
update(Node, KeyPath, UpdateReq, Opts) ->
    rpc:call(Node, emqx, update_config, [KeyPath, UpdateReq, Opts], 5_000).

-spec remove_config(update_config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove_config(KeyPath, Opts) ->
    emqx_cluster_rpc:multicall(emqx, remove_config, [KeyPath, Opts]).

-spec remove_config(node(), update_config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()}
    | {error, emqx_config:update_error()}
    | emqx_rpc:badrpc().
remove_config(Node, KeyPath, Opts) ->
    rpc:call(Node, emqx, remove_config, [KeyPath, Opts], 5_000).

-spec reset(update_config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
reset(KeyPath, Opts) ->
    emqx_cluster_rpc:multicall(emqx, reset_config, [KeyPath, Opts]).

-spec reset(node(), update_config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()}
    | {error, emqx_config:update_error()}
    | emqx_rpc:badrpc().
reset(Node, KeyPath, Opts) ->
    rpc:call(Node, emqx, reset_config, [KeyPath, Opts]).

-spec get_override_config_file([node()]) -> emqx_rpc:multicall_result().
get_override_config_file(Nodes) ->
    rpc:multicall(Nodes, emqx_conf_app, get_override_config_file, [], 20_000).

-spec get_hocon_config(node(), maybe_namespace()) -> map() | {badrpc, _}.
get_hocon_config(Node, Namespace) ->
    rpc:call(Node, emqx_conf_cli, get_config_namespaced, [Namespace]).

-spec get_raw_config(node(), maybe_namespace(), update_config_key_path()) -> map() | {badrpc, _}.
get_raw_config(Node, Namespace, KeyPath) ->
    rpc:call(Node, emqx, get_raw_namespaced_config, [Namespace, KeyPath]).

-spec get_hocon_config(node(), maybe_namespace(), binary()) -> map() | {badrpc, _}.
get_hocon_config(Node, Namespace, Key) ->
    rpc:call(Node, emqx_conf_cli, get_config_namespaced, [Namespace, Key]).
