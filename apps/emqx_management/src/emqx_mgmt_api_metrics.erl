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

-module(emqx_mgmt_api_metrics).

-rest_api(#{name   => list_all_metrics,
            method => 'GET',
            path   => "/metrics",
            func   => list,
            descr  => "A list of metrics of all nodes in the cluster"}).

-rest_api(#{name   => list_node_metrics,
            method => 'GET',
            path   => "/nodes/:atom:node/metrics",
            func   => list,
            descr  => "A list of metrics of a node"}).

-export([list/2]).

list(Bindings, _Params) when map_size(Bindings) == 0 ->
    minirest:return({ok, [#{node => Node, metrics => maps:from_list(Metrics)}
                              || {Node, Metrics} <- emqx_mgmt:get_metrics()]});

list(#{node := Node}, _Params) ->
    case emqx_mgmt:get_metrics(Node) of
        {error, Reason} -> minirest:return({error, Reason});
        Metrics         -> minirest:return({ok, maps:from_list(Metrics)})
    end.

