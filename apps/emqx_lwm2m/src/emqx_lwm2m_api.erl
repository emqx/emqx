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

-module(emqx_lwm2m_api).

-import(minirest,  [return/1]).

-rest_api(#{name   => list,
            method => 'GET',
            path   => "/lwm2m_channels/",
            func   => list,
            descr  => "A list of all lwm2m channel"
           }).

-rest_api(#{name   => list,
            method => 'GET',
            path   => "/nodes/:atom:node/lwm2m_channels/",
            func   => list,
            descr  => "A list of lwm2m channel of a node"
           }).

-rest_api(#{name   => lookup_cmd,
            method => 'GET',
            path   => "/lookup_cmd/:bin:ep/",
            func   => lookup_cmd,
            descr  => "Send a lwm2m downlink command"
           }).

-rest_api(#{name   => lookup_cmd,
            method => 'GET',
            path   => "/nodes/:atom:node/lookup_cmd/:bin:ep/",
            func   => lookup_cmd,
            descr  => "Send a lwm2m downlink command of a node"
           }).

-export([ list/2
        , lookup_cmd/2
        ]).

list(#{node := Node }, Params) ->
    case Node =:= node() of
        true -> list(#{}, Params);
        _ -> rpc_call(Node, list, [#{}, Params])
    end;

list(#{}, _Params) ->
    Channels = emqx_lwm2m_cm:all_channels(),
    return({ok, format(Channels)}).

lookup_cmd(#{ep := Ep, node := Node}, Params) ->
    case Node =:= node() of
        true -> lookup_cmd(#{ep => Ep}, Params);
        _ -> rpc_call(Node, lookup_cmd, [#{ep => Ep}, Params])
    end;

lookup_cmd(#{ep := Ep}, Params) ->
    MsgType = proplists:get_value(<<"msgType">>, Params),
    Path0 = proplists:get_value(<<"path">>, Params),
    case emqx_lwm2m_cm:lookup_cmd(Ep, Path0, MsgType) of
        [] -> return({ok, []});
        [{_, undefined} | _] -> return({ok, []});
        [{{IMEI, Path, MsgType}, undefined}] ->
            return({ok, [{imei, IMEI},
                         {'msgType', IMEI},
                         {'code', <<"6.01">>},
                         {'codeMsg', <<"reply_not_received">>},
                         {'path', Path}]});
        [{{IMEI, Path, MsgType}, {Code, CodeMsg, Content}}] ->
            Payload1 = format_cmd_content(Content, MsgType),
            return({ok, [{imei, IMEI},
                         {'msgType', IMEI},
                         {'code', Code},
                         {'codeMsg', CodeMsg},
                         {'path', Path}] ++ Payload1})
    end.

rpc_call(Node, Fun, Args) ->
    case rpc:call(Node, ?MODULE, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Res -> Res
    end.

format(Channels) ->
    lists:map(fun({IMEI, #{lifetime := LifeTime,
                           peername := Peername,
                           version := Version,
                           reg_info := RegInfo}}) ->
        ObjectList = lists:map(fun(Path) ->
            [ObjId | _] = path_list(Path),
            case emqx_lwm2m_xml_object:get_obj_def(binary_to_integer(ObjId), true) of
                {error, _} ->
                    {Path, Path};
                ObjDefinition ->
                    ObjectName = emqx_lwm2m_xml_object:get_object_name(ObjDefinition),
                    {Path, list_to_binary(ObjectName)}
            end
        end, maps:get(<<"objectList">>, RegInfo)),
        {IpAddr, Port} = Peername,
        [{imei, IMEI},
         {lifetime, LifeTime},
         {ip_address, iolist_to_binary(ntoa(IpAddr))},
         {port, Port},
         {version, Version},
         {'objectList', ObjectList}]
    end, Channels).

format_cmd_content(undefined, _MsgType) -> [];
format_cmd_content(Content, <<"discover">>) ->
    [H | Content1] = Content,
    {_, [HObjId]} = emqx_lwm2m_coap_resource:parse_object_list(H),
    [ObjId | _]= path_list(HObjId),
    ObjectList = case Content1 of
        [Content2 | _] ->
            {_, ObjL} = emqx_lwm2m_coap_resource:parse_object_list(Content2),
            ObjL;
        [] -> []
    end,
    R = case emqx_lwm2m_xml_object:get_obj_def(binary_to_integer(ObjId), true) of
        {error, _} ->
            lists:map(fun(Object) -> {Object, Object} end, ObjectList);
        ObjDefinition ->
            lists:map(fun(Object) ->
                [_, _,  ResId| _] = path_list(Object),
                Operations = case emqx_lwm2m_xml_object:get_resource_operations(binary_to_integer(ResId), ObjDefinition) of
                    "E" -> [{operations, list_to_binary("E")}];
                    Oper -> [{'dataType', list_to_binary(emqx_lwm2m_xml_object:get_resource_type(binary_to_integer(ResId), ObjDefinition))},
                             {operations, list_to_binary(Oper)}]
                end,
                [{path, Object},
                 {name, list_to_binary(emqx_lwm2m_xml_object:get_resource_name(binary_to_integer(ResId), ObjDefinition))}
                ] ++ Operations
            end, ObjectList)
    end,
    [{content, R}];
format_cmd_content(Content, _) ->
    [{content, Content}].

ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
    inet_parse:ntoa(IP).

path_list(Path) ->
    case binary:split(binary_util:trim(Path, $/), [<<$/>>], [global]) of
        [ObjId, ObjInsId, ResId, ResInstId] -> [ObjId, ObjInsId, ResId, ResInstId];
        [ObjId, ObjInsId, ResId] -> [ObjId, ObjInsId, ResId];
        [ObjId, ObjInsId] -> [ObjId, ObjInsId];
        [ObjId] -> [ObjId]
    end.
