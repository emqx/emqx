%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_conf).

-compile({no_auto_import, [get/1]}).

-export([add_handler/2, remove_handler/1]).
-export([get/1, get/2, get/3, get_node_and_config/1]).
-export([update/3, update/4]).
-export([remove/2, remove/3]).
-export([reset/2, reset/3]).

%% API
%% @doc Adds a new config handler to emqx_config_handler.
-spec add_handler(emqx_config:config_key_path(), module()) -> ok.
add_handler(ConfKeyPath, HandlerName) ->
    emqx_config_handler:add_handler(ConfKeyPath, HandlerName).

%% @doc remove config handler from emqx_config_handler.
-spec remove_handler(emqx_config:config_key_path()) -> ok.
remove_handler(ConfKeyPath) ->
    emqx_config_handler:remove_handler(ConfKeyPath).

%% @doc Returns all values in the cluster.
-spec get(emqx_map_lib:config_key_path()) -> #{node() => term()}.
get(KeyPath) ->
    {ResL, []} = rpc:multicall(?MODULE, get_node_and_config, [KeyPath], 5000),
    maps:from_list(ResL).

%% @doc Returns the specified node's KeyPath, or exception if not found
-spec get(node(), emqx_map_lib:config_key_path()) -> term().
get(Node, KeyPath)when Node =:= node() ->
    emqx:get_config(KeyPath);
get(Node, KeyPath) ->
    rpc:call(Node, ?MODULE, get, [Node, KeyPath]).

%% @doc Returns the specified node's KeyPath, or the default value if not found
-spec get(node(), emqx_map_lib:config_key_path(), term()) -> term().
get(Node, KeyPath, Default)when Node =:= node() ->
    emqx:get_config(KeyPath, Default);
get(Node, KeyPath, Default) ->
    rpc:call(Node, ?MODULE, get, [Node, KeyPath, Default]).

%% @doc Returns the specified node's KeyPath, or config_not_found if key path not found
-spec get_node_and_config(emqx_map_lib:config_key_path()) -> term().
get_node_and_config(KeyPath) ->
    {node(), emqx:get_config(KeyPath, config_not_found)}.

%% @doc Update all value of key path in cluster-override.conf or local-override.conf.
-spec update(emqx_map_lib:config_key_path(), emqx_config:update_args(),
    emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update(KeyPath, UpdateReq, Opts0) ->
    Args = [KeyPath, UpdateReq, Opts0],
    {ok, _TnxId, Res} = emqx_cluster_rpc:multicall(emqx, update_config, Args),
    Res.

%% @doc Update the specified node's key path in local-override.conf.
-spec update(node(), emqx_map_lib:config_key_path(), emqx_config:update_args(),
    emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update(Node, KeyPath, UpdateReq, Opts0)when Node =:= node() ->
    emqx:update_config(KeyPath, UpdateReq, Opts0#{override => local});
update(Node, KeyPath, UpdateReq, Opts0) ->
    rpc:call(Node, ?MODULE, update, [Node, KeyPath, UpdateReq, Opts0], 5000).

%% @doc remove all value of key path in cluster-override.conf or local-override.conf.
-spec remove(emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove(KeyPath, Opts0) ->
    Args = [KeyPath, Opts0],
    {ok, _TnxId, Res} = emqx_cluster_rpc:multicall(emqx, remove_config, Args),
    Res.

%% @doc remove the specified node's key path in local-override.conf.
-spec remove(node(), emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove(Node, KeyPath, Opts) when Node =:= node() ->
    emqx:remove_config(KeyPath, Opts#{override => local});
remove(Node, KeyPath, Opts) ->
    rpc:call(Node, ?MODULE, remove, [KeyPath, Opts]).

%% @doc reset all value of key path in cluster-override.conf or local-override.conf.
-spec reset(emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
reset(KeyPath, Opts0) ->
    Args = [KeyPath, Opts0],
    {ok, _TnxId, Res} = emqx_cluster_rpc:multicall(emqx, reset_config, Args),
    Res.

%% @doc reset the specified node's key path in local-override.conf.
-spec reset(node(), emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
reset(Node, KeyPath, Opts) when Node =:= node() ->
    emqx:reset_config(KeyPath, Opts#{override => local});
reset(Node, KeyPath, Opts) ->
    rpc:call(Node, ?MODULE, reset, [KeyPath, Opts]).
