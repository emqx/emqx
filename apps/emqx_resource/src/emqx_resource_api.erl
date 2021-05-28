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
-module(emqx_resource_api).

-export([ get_all/3
        , get/3
        , put/3
        , delete/3
        ]).
get_all(Mod, _Binding, _Params) ->
    {200, #{code => 0, data =>
        [format_data(Mod, Data) || Data <- emqx_resource:list_instances_verbose()]}}.

get(Mod, #{id := Id}, _Params) ->
    case emqx_resource:get_instance(stringnify(Id)) of
        {ok, Data} ->
            {200, #{code => 0, data => format_data(Mod, Data)}};
        {error, not_found} ->
            {404, #{code => 102, message => {resource_instance_not_found, stringnify(Id)}}}
    end.

put(Mod, #{id := Id}, Params) ->
    ConfigParams = proplists:get_value(<<"config">>, Params),
    ResourceTypeStr = proplists:get_value(<<"resource_type">>, Params),
    case emqx_resource:resource_type_from_str(ResourceTypeStr) of
        {ok, ResourceType} ->
            do_put(Mod, stringnify(Id), ConfigParams, ResourceType, Params);
        {error, Reason} ->
            {404, #{code => 102, message => stringnify(Reason)}}
    end.

do_put(Mod, Id, ConfigParams, ResourceType, Params) ->
    case emqx_resource:parse_config(ResourceType, ConfigParams) of
        {ok, Config} ->
            case emqx_resource:update(Id, ResourceType, Config, Params) of
                {ok, Data} ->
                    {200, #{code => 0, data => format_data(Mod, Data)}};
                {error, Reason} ->
                    {500, #{code => 102, message => stringnify(Reason)}}
            end;
        {error, Reason} ->
            {400, #{code => 108, message => stringnify(Reason)}}
    end.

delete(_Mod, #{id := Id}, _Params) ->
    case emqx_resource:remove(stringnify(Id)) of
        ok -> {200, #{code => 0, data => #{}}};
        {error, Reason} ->
            {500, #{code => 102, message => stringnify(Reason)}}
    end.

format_data(Mod, Data) ->
    case erlang:function_exported(Mod, on_api_reply_format, 1) of
        false ->
            default_api_reply_format(Data);
        true ->
            Mod:on_api_reply_format(Data)
    end.

default_api_reply_format(#{id := Id, status := Status, config := Config}) ->
    #{node => node(), id => Id, status => Status, config => Config}.

stringnify(Bin) when is_binary(Bin) -> Bin;
stringnify(Str) when is_list(Str) -> list_to_binary(Str);
stringnify(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).
