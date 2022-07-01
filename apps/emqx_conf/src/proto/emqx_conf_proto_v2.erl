%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_conf_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
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

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.1".

-spec sync_data_from_node(node()) -> {ok, binary()} | emqx_rpc:badrpc().
sync_data_from_node(Node) ->
    rpc:call(Node, emqx_conf_app, sync_data_from_node, [], 20000).
-type update_config_key_path() :: [emqx_map_lib:config_key(), ...].

-spec get_config(node(), emqx_map_lib:config_key_path()) ->
    term() | emqx_rpc:badrpc().
get_config(Node, KeyPath) ->
    rpc:call(Node, emqx, get_config, [KeyPath]).

-spec get_config(node(), emqx_map_lib:config_key_path(), _Default) ->
    term() | emqx_rpc:badrpc().
get_config(Node, KeyPath, Default) ->
    rpc:call(Node, emqx, get_config, [KeyPath, Default]).

-spec get_all(emqx_map_lib:config_key_path()) -> emqx_rpc:multicall_result().
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
