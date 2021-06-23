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
-module(emqx_data_bridge_api).

-rest_api(#{ name => list_data_bridges
           , method => 'GET'
           , path => "/data_bridges"
           , func => list_bridges
           , descr => "List all data bridges"
           }).

-rest_api(#{ name => get_data_bridge
           , method => 'GET'
           , path => "/data_bridges/:bin:name"
           , func => get_bridge
           , descr => "Get a data bridge by name"
           }).

-rest_api(#{ name => create_data_bridge
           , method => 'POST'
           , path => "/data_bridges/:bin:name"
           , func => create_bridge
           , descr => "Create a new data bridge"
           }).

-rest_api(#{ name => update_data_bridge
           , method => 'PUT'
           , path => "/data_bridges/:bin:name"
           , func => update_bridge
           , descr => "Update an existing data bridge"
           }).

-rest_api(#{ name => delete_data_bridge
           , method => 'DELETE'
           , path => "/data_bridges/:bin:name"
           , func => delete_bridge
           , descr => "Delete an existing data bridge"
           }).

-export([ list_bridges/2
        , get_bridge/2
        , create_bridge/2
        , update_bridge/2
        , delete_bridge/2
        ]).

-define(BRIDGE(N, T, C), #{<<"name">> => N, <<"type">> => T, <<"config">> => C}).

list_bridges(_Binding, _Params) ->
    {200, #{code => 0, data => [format_api_reply(Data) ||
        Data <- emqx_data_bridge:list_bridges()]}}.

get_bridge(#{name := Name}, _Params) ->
    case emqx_resource:get_instance(emqx_data_bridge:name_to_resource_id(Name)) of
        {ok, Data} ->
            {200, #{code => 0, data => format_api_reply(emqx_resource_api:format_data(Data))}};
        {error, not_found} ->
            {404, #{code => 102, message => <<"not_found: ", Name/binary>>}}
    end.

create_bridge(#{name := Name}, Params) ->
    Config = proplists:get_value(<<"config">>, Params),
    BridgeType = proplists:get_value(<<"type">>, Params),
    case emqx_resource:check_and_create(
            emqx_data_bridge:name_to_resource_id(Name),
            emqx_data_bridge:resource_type(BridgeType), Config) of
        {ok, Data} ->
            update_config_and_reply(Name, BridgeType, Config, Data);
        {error, already_created} ->
            {400, #{code => 102, message => <<"bridge already created: ", Name/binary>>}};
        {error, Reason0} ->
            Reason = emqx_resource_api:stringnify(Reason0),
            {500, #{code => 102, message => <<"create bridge ", Name/binary,
                        " failed:", Reason/binary>>}}
    end.

update_bridge(#{name := Name}, Params) ->
    Config = proplists:get_value(<<"config">>, Params),
    BridgeType = proplists:get_value(<<"type">>, Params),
    case emqx_resource:check_and_update(
            emqx_data_bridge:name_to_resource_id(Name),
            emqx_data_bridge:resource_type(BridgeType), Config, []) of
        {ok, Data} ->
            update_config_and_reply(Name, BridgeType, Config, Data);
        {error, not_found} ->
            {400, #{code => 102, message => <<"bridge not_found: ", Name/binary>>}};
        {error, Reason0} ->
            Reason = emqx_resource_api:stringnify(Reason0),
            {500, #{code => 102, message => <<"update bridge ", Name/binary,
                        " failed:", Reason/binary>>}}
    end.

delete_bridge(#{name := Name}, _Params) ->
    case emqx_resource:remove(emqx_data_bridge:name_to_resource_id(Name)) of
        ok -> delete_config_and_reply(Name);
        {error, Reason} ->
            {500, #{code => 102, message => emqx_resource_api:stringnify(Reason)}}
    end.

format_api_reply(#{resource_type := Type, id := Id, config := Conf, status := Status}) ->
    #{type => emqx_data_bridge:bridge_type(Type),
      name => emqx_data_bridge:resource_id_to_name(Id),
      config => Conf, status => Status}.

% format_conf(#{resource_type := Type, id := Id, config := Conf}) ->
%     #{type => Type, name => emqx_data_bridge:resource_id_to_name(Id),
%       config => Conf}.

% get_all_configs() ->
%     [format_conf(Data) || Data <- emqx_data_bridge:list_bridges()].

update_config_and_reply(Name, BridgeType, Config, Data) ->
    case emqx_data_bridge:update_config({update, ?BRIDGE(Name, BridgeType, Config)}) of
        ok ->
            {200, #{code => 0, data => format_api_reply(
                        emqx_resource_api:format_data(Data))}};
        {error, Reason} ->
            {500, #{code => 102, message => Reason}}
    end.

delete_config_and_reply(Name) ->
    case emqx_data_bridge:update_config({delete, Name}) of
        ok -> {200, #{code => 0, data => #{}}};
        {error, Reason} -> {500, #{code => 102, message => Reason}}
    end.
