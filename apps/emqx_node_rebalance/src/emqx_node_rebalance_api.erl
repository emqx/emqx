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

-module(emqx_node_rebalance_api).

-rest_api(#{name   => node_rebalance,
            method => 'GET',
            path   => "/node_rebalance/status",
            func   => node_rebalance,
            descr  => "Get node rebalance status"}).

-export([node_rebalance/2]).

node_rebalance(_Bindings, _Params) ->
    case emqx_node_rebalance_evacuation:status() of
        disabled ->
            {ok, #{status => disabled}};
        {enabled, Stats} ->
            {ok, format_stats(Stats)}
    end.

format_stats(Stats) ->
    #{
      status => enabled,
      connection_eviction_rate => maps:get(conn_evict_rate, Stats),
      %% for evacuation
      goal => 0,
      stats => #{
        initial_connected => maps:get(initial_conns, Stats),
        current_connected => maps:get(current_conns, Stats)
      }
     }.
