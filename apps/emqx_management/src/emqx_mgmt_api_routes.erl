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

-module(emqx_mgmt_api_routes).

-include_lib("emqx/include/emqx.hrl").

-rest_api(#{name   => list_routes,
            method => 'GET',
            path   => "/routes/",
            func   => list,
            descr  => "List routes"}).

-rest_api(#{name   => lookup_routes,
            method => 'GET',
            path   => "/routes/:bin:topic",
            func   => lookup,
            descr  => "Lookup routes to a topic"}).

-export([ list/2
        , lookup/2
        ]).

list(Bindings, Params) when map_size(Bindings) == 0 ->
    minirest:return({ok, emqx_mgmt_api:paginate(emqx_route, Params, fun format/1)}).

lookup(#{topic := Topic}, _Params) ->
    Topic1 = emqx_mgmt_util:urldecode(Topic),
    minirest:return({ok, [format(R) || R <- emqx_mgmt:lookup_routes(Topic1)]}).
format(#route{topic = Topic, dest = {_, Node}}) ->
    #{topic => Topic, node => Node};
format(#route{topic = Topic, dest = Node}) ->
    #{topic => Topic, node => Node}.

