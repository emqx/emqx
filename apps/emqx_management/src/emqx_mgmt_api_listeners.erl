%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_listeners).

-import(minirest, [return/1]).

-rest_api(#{name   => list_listeners,
            method => 'GET',
            path   => "/listeners/",
            func   => list,
            descr  => "A list of listeners in the cluster"}).

-rest_api(#{name   => list_node_listeners,
            method => 'GET',
            path   => "/nodes/:atom:node/listeners",
            func   => list,
            descr  => "A list of listeners on the node"}).

-export([list/2]).

%% List listeners on a node.
list(#{node := Node}, _Params) ->
    return({ok, format(emqx_mgmt:list_listeners(Node))});

%% List listeners in the cluster.
list(_Binding, _Params) ->
    return({ok, [#{node => Node, listeners => format(Listeners)}
                              || {Node, Listeners} <- emqx_mgmt:list_listeners()]}).

format(Listeners) when is_list(Listeners) ->
    [ Info#{listen_on => list_to_binary(esockd:to_string(ListenOn))}
     || Info = #{listen_on := ListenOn} <- Listeners ];

format({error, Reason}) -> [{error, Reason}].

