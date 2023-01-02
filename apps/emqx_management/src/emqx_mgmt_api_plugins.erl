%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_plugins).

-include("emqx_mgmt.hrl").

-include_lib("emqx/include/emqx.hrl").

-rest_api(#{name   => list_all_plugins,
            method => 'GET',
            path   => "/plugins/",
            func   => list,
            descr  => "List all plugins in the cluster"}).

-rest_api(#{name   => list_node_plugins,
            method => 'GET',
            path   => "/nodes/:atom:node/plugins/",
            func   => list,
            descr  => "List all plugins on a node"}).

-rest_api(#{name   => load_node_plugin,
            method => 'PUT',
            path   => "/nodes/:atom:node/plugins/:atom:plugin/load",
            func   => load,
            descr  => "Load a plugin"}).

-rest_api(#{name   => unload_node_plugin,
            method => 'PUT',
            path   => "/nodes/:atom:node/plugins/:atom:plugin/unload",
            func   => unload,
            descr  => "Unload a plugin"}).

-rest_api(#{name   => reload_node_plugin,
            method => 'PUT',
            path   => "/nodes/:atom:node/plugins/:atom:plugin/reload",
            func   => reload,
            descr  => "Reload a plugin"}).

-rest_api(#{name   => unload_plugin,
            method => 'PUT',
            path   => "/plugins/:atom:plugin/unload",
            func   => unload,
            descr  => "Unload a plugin in the cluster"}).

-rest_api(#{name   => reload_plugin,
            method => 'PUT',
            path   => "/plugins/:atom:plugin/reload",
            func   => reload,
            descr  => "Reload a plugin in the cluster"}).

-export([ list/2
        , load/2
        , unload/2
        , reload/2
        ]).

list(#{node := Node}, _Params) ->
    minirest:return({ok, [format(Plugin) || Plugin <- emqx_mgmt:list_plugins(Node)]});

list(_Bindings, _Params) ->
    minirest:return({ok, [format({Node, Plugins}) || {Node, Plugins} <- emqx_mgmt:list_plugins()]}).

load(#{node := Node, plugin := Plugin}, _Params) ->
    minirest:return(emqx_mgmt:load_plugin(Node, Plugin)).

unload(#{node := Node, plugin := Plugin}, _Params) ->
    minirest:return(emqx_mgmt:unload_plugin(Node, Plugin));

unload(#{plugin := Plugin}, _Params) ->
    Results = [emqx_mgmt:unload_plugin(Node, Plugin) || {Node, _Info} <- emqx_mgmt:list_nodes()],
    case lists:filter(fun(Item) -> Item =/= ok end, Results) of
        [] ->
            minirest:return(ok);
        Errors ->
            minirest:return(lists:last(Errors))
    end.

reload(#{node := Node, plugin := Plugin}, _Params) ->
    minirest:return(emqx_mgmt:reload_plugin(Node, Plugin));

reload(#{plugin := Plugin}, _Params) ->
    Results = [emqx_mgmt:reload_plugin(Node, Plugin) || {Node, _Info} <- emqx_mgmt:list_nodes()],
    case lists:filter(fun(Item) -> Item =/= ok end, Results) of
        [] ->
            minirest:return(ok);
        Errors ->
            minirest:return(lists:last(Errors))
    end.

format({Node, Plugins}) ->
    #{node => Node, plugins => [format(Plugin) || Plugin <- Plugins]};

format(#plugin{name = Name,
               descr = Descr,
               active = Active,
               type = Type}) ->
    #{name => Name,
      description => iolist_to_binary(Descr),
      active => Active,
      type => Type}.

