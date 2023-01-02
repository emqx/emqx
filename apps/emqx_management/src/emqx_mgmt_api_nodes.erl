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

-module(emqx_mgmt_api_nodes).

-rest_api(#{name   => list_nodes,
            method => 'GET',
            path   => "/nodes/",
            func   => list,
            descr  => "A list of nodes in the cluster"}).

-rest_api(#{name   => get_node,
            method => 'GET',
            path   => "/nodes/:atom:node",
            func   => get,
            descr  => "Lookup a node in the cluster"}).

-export([ list/2
        , get/2
        ]).

list(_Bindings, _Params) ->
    minirest:return({ok, [format(Node, Info) || {Node, Info} <- emqx_mgmt:list_nodes()]}).

get(#{node := Node}, _Params) ->
    minirest:return({ok, format(Node, emqx_mgmt:lookup_node(Node))}).

format(Node, {error, Reason}) -> #{node => Node, error => Reason};

format(_Node, Info = #{memory_total := Total, memory_used := Used}) ->
    Info#{memory_total := emqx_mgmt_util:kmg(Total),
          memory_used  := emqx_mgmt_util:kmg(Used)}.
