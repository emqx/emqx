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

-compile({no_auto_import, [get/1, get/2]}).
-include_lib("emqx/include/logger.hrl").

-export([add_handler/2, remove_handler/1]).
-export([get/1, get/2, get_raw/2, get_all/1]).
-export([get_by_node/2, get_by_node/3]).
-export([update/3, update/4]).
-export([remove/2, remove/3]).
-export([reset/2, reset/3]).
-export([gen_doc/1]).

%% for rpc
-export([get_node_and_config/1]).

%% API
%% @doc Adds a new config handler to emqx_config_handler.
-spec add_handler(emqx_config:config_key_path(), module()) -> ok.
add_handler(ConfKeyPath, HandlerName) ->
    emqx_config_handler:add_handler(ConfKeyPath, HandlerName).

%% @doc remove config handler from emqx_config_handler.
-spec remove_handler(emqx_config:config_key_path()) -> ok.
remove_handler(ConfKeyPath) ->
    emqx_config_handler:remove_handler(ConfKeyPath).

-spec get(emqx_map_lib:config_key_path()) -> term().
get(KeyPath) ->
    emqx:get_config(KeyPath).

-spec get(emqx_map_lib:config_key_path(), term()) -> term().
get(KeyPath, Default) ->
    emqx:get_config(KeyPath, Default).

-spec get_raw(emqx_map_lib:config_key_path(), term()) -> term().
get_raw(KeyPath, Default) ->
    emqx_config:get_raw(KeyPath, Default).

%% @doc Returns all values in the cluster.
-spec get_all(emqx_map_lib:config_key_path()) -> #{node() => term()}.
get_all(KeyPath) ->
    {ResL, []} = rpc:multicall(?MODULE, get_node_and_config, [KeyPath], 5000),
    maps:from_list(ResL).

%% @doc Returns the specified node's KeyPath, or exception if not found
-spec get_by_node(node(), emqx_map_lib:config_key_path()) -> term().
get_by_node(Node, KeyPath)when Node =:= node() ->
    emqx:get_config(KeyPath);
get_by_node(Node, KeyPath) ->
    rpc:call(Node, ?MODULE, get_by_node, [Node, KeyPath]).

%% @doc Returns the specified node's KeyPath, or the default value if not found
-spec get_by_node(node(), emqx_map_lib:config_key_path(), term()) -> term().
get_by_node(Node, KeyPath, Default)when Node =:= node() ->
    emqx:get_config(KeyPath, Default);
get_by_node(Node, KeyPath, Default) ->
    rpc:call(Node, ?MODULE, get_by_node, [Node, KeyPath, Default]).

%% @doc Returns the specified node's KeyPath, or config_not_found if key path not found
-spec get_node_and_config(emqx_map_lib:config_key_path()) -> term().
get_node_and_config(KeyPath) ->
    {node(), emqx:get_config(KeyPath, config_not_found)}.

%% @doc Update all value of key path in cluster-override.conf or local-override.conf.
-spec update(emqx_map_lib:config_key_path(), emqx_config:update_request(),
    emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update(KeyPath, UpdateReq, Opts0) ->
    Args = [KeyPath, UpdateReq, Opts0],
    multicall(emqx, update_config, Args).

%% @doc Update the specified node's key path in local-override.conf.
-spec update(node(), emqx_map_lib:config_key_path(), emqx_config:update_request(),
    emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update(Node, KeyPath, UpdateReq, Opts0)when Node =:= node() ->
    emqx:update_config(KeyPath, UpdateReq, Opts0#{override_to => local});
update(Node, KeyPath, UpdateReq, Opts0) ->
    rpc:call(Node, ?MODULE, update, [Node, KeyPath, UpdateReq, Opts0], 5000).

%% @doc remove all value of key path in cluster-override.conf or local-override.conf.
-spec remove(emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove(KeyPath, Opts0) ->
    Args = [KeyPath, Opts0],
    multicall(emqx, remove_config, Args).

%% @doc remove the specified node's key path in local-override.conf.
-spec remove(node(), emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove(Node, KeyPath, Opts) when Node =:= node() ->
    emqx:remove_config(KeyPath, Opts#{override_to => local});
remove(Node, KeyPath, Opts) ->
    rpc:call(Node, ?MODULE, remove, [KeyPath, Opts]).

%% @doc reset all value of key path in cluster-override.conf or local-override.conf.
-spec reset(emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
reset(KeyPath, Opts0) ->
    Args = [KeyPath, Opts0],
    multicall(emqx, reset_config, Args).

%% @doc reset the specified node's key path in local-override.conf.
-spec reset(node(), emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
reset(Node, KeyPath, Opts) when Node =:= node() ->
    emqx:reset_config(KeyPath, Opts#{override_to => local});
reset(Node, KeyPath, Opts) ->
    rpc:call(Node, ?MODULE, reset, [KeyPath, Opts]).

-spec gen_doc(file:name_all()) -> ok.
gen_doc(File) ->
    Version = emqx_release:version(),
    Title = "# EMQ X " ++ Version ++ " Configuration",
    BodyFile = filename:join([code:lib_dir(emqx_conf), "etc", "emqx_conf.md"]),
    {ok, Body} = file:read_file(BodyFile),
    Doc = hocon_schema_doc:gen(emqx_conf_schema, #{title => Title,
                                                   body => Body}),
    file:write_file(File, Doc).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

multicall(M, F, Args) ->
    case emqx_cluster_rpc:multicall(M, F, Args) of
        {ok, _TnxId, Res} -> Res;
        {retry, TnxId, Res, Nodes} ->
            %% The init MFA return ok, but other nodes failed.
            %% We return ok and alert an alarm.
            ?SLOG(error, #{msg => "failed to update config in cluster", nodes => Nodes,
                tnx_id => TnxId, mfa => {M, F, Args}}),
            Res;
        {error, Error} -> %% all MFA return not ok or {ok, term()}.
            Error
    end.
