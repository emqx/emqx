%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_proto_v4).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    deprecated_since/0,

    sync_data_from_node/1,
    get_config/2,
    get_config/3,
    get_all/1,

    update/3,
    update/4,
    remove_config/2,
    remove_config/3,

    reset/2,
    reset/3,

    get_override_config_file/1
]).

-export([get_hocon_config/1, get_hocon_config/2]).
-export([get_raw_config/2]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.7.1".

deprecated_since() ->
    "6.0.0".

-spec sync_data_from_node(node()) -> {ok, binary()} | emqx_rpc:badrpc().
sync_data_from_node(Node) ->
    rpc:call(Node, emqx_conf_app, sync_data_from_node, [], 20000).
-type update_config_key_path() :: [emqx_utils_maps:config_key(), ...].

-spec get_config(node(), emqx_utils_maps:config_key_path()) ->
    term() | emqx_rpc:badrpc().
get_config(Node, KeyPath) ->
    rpc:call(Node, emqx, get_config, [KeyPath]).

-spec get_config(node(), emqx_utils_maps:config_key_path(), _Default) ->
    term() | emqx_rpc:badrpc().
get_config(Node, KeyPath, Default) ->
    rpc:call(Node, emqx, get_config, [KeyPath, Default]).

-spec get_all(emqx_utils_maps:config_key_path()) -> emqx_rpc:multicall_result().
get_all(KeyPath) ->
    rpc:multicall(emqx_conf, get_node_and_config, [KeyPath], 5000).

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
    rpc:call(Node, emqx, update_config, [KeyPath, UpdateReq, Opts], 5000).

-spec remove_config(update_config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove_config(KeyPath, Opts) ->
    emqx_cluster_rpc:multicall(emqx, remove_config, [KeyPath, Opts]).

-spec remove_config(node(), update_config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()}
    | {error, emqx_config:update_error()}
    | emqx_rpc:badrpc().
remove_config(Node, KeyPath, Opts) ->
    rpc:call(Node, emqx, remove_config, [KeyPath, Opts], 5000).

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
    rpc:multicall(Nodes, emqx_conf_app, get_override_config_file, [], 20000).

-spec get_hocon_config(node()) -> map() | {badrpc, _}.
get_hocon_config(Node) ->
    rpc:call(Node, emqx_conf_cli, get_config, []).

-spec get_raw_config(node(), update_config_key_path()) -> map() | {badrpc, _}.
get_raw_config(Node, KeyPath) ->
    rpc:call(Node, emqx, get_raw_config, [KeyPath]).

-spec get_hocon_config(node(), binary()) -> map() | {badrpc, _}.
get_hocon_config(Node, Key) ->
    rpc:call(Node, emqx_conf_cli, get_config, [Key]).
