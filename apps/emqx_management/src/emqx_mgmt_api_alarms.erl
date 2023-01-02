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

-module(emqx_mgmt_api_alarms).

-include("emqx_mgmt.hrl").

-include_lib("emqx/include/emqx.hrl").

-rest_api(#{name   => list_all_alarms,
            method => 'GET',
            path   => "/alarms",
            func   => list,
            descr  => "List all alarms in the cluster"}).

-rest_api(#{name   => list_node_alarms,
            method => 'GET',
            path   => "nodes/:atom:node/alarms",
            func   => list,
            descr  => "List all alarms on a node"}).

-rest_api(#{name   => list_all_activated_alarms,
            method => 'GET',
            path   => "/alarms/activated",
            func   => list_activated,
            descr  => "List all activated alarm in the cluster"}).

-rest_api(#{name   => list_node_activated_alarms,
            method => 'GET',
            path   => "nodes/:atom:node/alarms/activated",
            func   => list_activated,
            descr  => "List all activated alarm on a node"}).

-rest_api(#{name   => list_all_deactivated_alarms,
            method => 'GET',
            path   => "/alarms/deactivated",
            func   => list_deactivated,
            descr  => "List all deactivated alarm in the cluster"}).

-rest_api(#{name   => list_node_deactivated_alarms,
            method => 'GET',
            path   => "nodes/:atom:node/alarms/deactivated",
            func   => list_deactivated,
            descr  => "List all deactivated alarm on a node"}).

-rest_api(#{name   => deactivate_alarm,
            method => 'POST',
            path   => "/alarms/deactivated",
            func   => deactivate,
            descr  => "Delete the special alarm on a node"}).

-rest_api(#{name   => delete_all_deactivated_alarms,
            method => 'DELETE',
            path   => "/alarms/deactivated",
            func   => delete_deactivated,
            descr  => "Delete all deactivated alarm in the cluster"}).

-rest_api(#{name   => delete_node_deactivated_alarms,
            method => 'DELETE',
            path   => "nodes/:atom:node/alarms/deactivated",
            func   => delete_deactivated,
            descr  => "Delete all deactivated alarm on a node"}).

-export([ list/2
        , deactivate/2
        , list_activated/2
        , list_deactivated/2
        , delete_deactivated/2
        ]).

list(Bindings, _Params) when map_size(Bindings) == 0 ->
    {ok, #{code => ?SUCCESS,
           data => [#{node => Node, alarms => Alarms} || {Node, Alarms} <- emqx_mgmt:get_alarms(all)]}};

list(#{node := Node}, _Params) ->
    {ok, #{code => ?SUCCESS,
           data => emqx_mgmt:get_alarms(Node, all)}}.

list_activated(Bindings, _Params) when map_size(Bindings) == 0 ->
    {ok, #{code => ?SUCCESS,
           data => [#{node => Node, alarms => Alarms} || {Node, Alarms} <- emqx_mgmt:get_alarms(activated)]}};

list_activated(#{node := Node}, _Params) ->
    {ok, #{code => ?SUCCESS,
           data => emqx_mgmt:get_alarms(Node, activated)}}.

list_deactivated(Bindings, _Params) when map_size(Bindings) == 0 ->
    {ok, #{code => ?SUCCESS,
           data => [#{node => Node, alarms => Alarms} || {Node, Alarms} <- emqx_mgmt:get_alarms(deactivated)]}};

list_deactivated(#{node := Node}, _Params) ->
    {ok, #{code => ?SUCCESS,
           data => emqx_mgmt:get_alarms(Node, deactivated)}}.

deactivate(_Bindings, Params) ->
    Node = get_node(Params),
    Name = get_name(Params),
    do_deactivate(Node, Name).

delete_deactivated(Bindings, _Params) when map_size(Bindings) == 0 ->
    _ = emqx_mgmt:delete_all_deactivated_alarms(),
    {ok, #{code => ?SUCCESS}};

delete_deactivated(#{node := Node}, _Params) ->
    emqx_mgmt:delete_all_deactivated_alarms(Node),
    {ok, #{code => ?SUCCESS}}.

get_node(Params) ->
    binary_to_atom(proplists:get_value(<<"node">>, Params, undefined), utf8).

get_name(Params) ->
    binary_to_atom(proplists:get_value(<<"name">>, Params, undefined), utf8).

do_deactivate(undefined, _) ->
    minirest:return({error, missing_param});
do_deactivate(_, undefined) ->
    minirest:return({error, missing_param});
do_deactivate(Node, Name) ->
    case emqx_mgmt:deactivate(Node, Name) of
        ok ->
            minirest:return();
        {error, Reason} ->
            minirest:return({error, Reason})
    end.
