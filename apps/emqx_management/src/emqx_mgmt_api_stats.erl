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

-module(emqx_mgmt_api_stats).

-rest_api(#{name   => list_stats,
            method => 'GET',
            path   => "/stats/",
            func   => list,
            descr  => "A list of stats of all nodes in the cluster"}).

-rest_api(#{name   => lookup_node_stats,
            method => 'GET',
            path   => "/nodes/:atom:node/stats/",
            func   => lookup,
            descr  => "A list of stats of a node"}).

-export([ list/2
        , lookup/2
        ]).

%% List stats of all nodes
list(Bindings, _Params) when map_size(Bindings) == 0 ->
    minirest:return({ok, [#{node => Node, stats => maps:from_list(Stats)}
                              || {Node, Stats} <- emqx_mgmt:get_stats()]}).

%% List stats of a node
lookup(#{node := Node}, _Params) ->
    case emqx_mgmt:get_stats(Node) of
        {error, Reason} -> minirest:return({error, Reason});
        Stats -> minirest:return({ok, maps:from_list(Stats)})
    end.
