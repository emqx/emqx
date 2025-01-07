%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-export([api_spec/0, paths/0, schema/1, fields/1, namespace/0]).

-export([lookup/2, observe/2, read/2, write/2]).

-define(PATH(Suffix), "/gateways/lwm2m/clients/:clientid" Suffix).
-define(DATA_TYPE, ['Integer', 'Float', 'Time', 'String', 'Boolean', 'Opaque', 'Objlnk']).
-define(TAGS, [<<"LwM2M Gateways">>]).

-import(hoconsc, [mk/2, ref/1, ref/2]).
-import(emqx_dashboard_swagger, [error_codes/2]).

-elvis([{elvis_style, atom_naming_convention, disable}]).

namespace() -> "lwm2m".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE).

paths() ->
    [
        ?PATH("/lookup"), ?PATH("/observe"), ?PATH("/read"), ?PATH("/write")
    ].

schema(?PATH("/lookup")) ->
    #{
        'operationId' => lookup,
        get => #{
            tags => ?TAGS,
            desc => ?DESC(lookup_resource),
            summary => <<"List Client's Resources">>,
            parameters => [
                {clientid, mk(binary(), #{in => path, example => "urn:oma:lwm2m:oma:2"})},
                {path, mk(binary(), #{in => query, required => true, example => "/3/0/7"})},
                {action, mk(binary(), #{in => query, required => true, example => "discover"})}
            ],
            'requestBody' => [],
            responses => #{
                200 => [
                    {clientid, mk(binary(), #{example => "urn:oma:lwm2m:oma:2"})},
                    {path, mk(binary(), #{example => "/3/0/7"})},
                    {action, mk(binary(), #{example => "discover"})},
                    {'codeMsg', mk(binary(), #{example => "reply_not_received"})},
                    {content, mk(hoconsc:array(ref(resource)), #{})}
                ],
                404 => error_codes(['CLIENT_NOT_FOUND'], <<"Client not found">>)
            }
        }
    };
schema(?PATH("/observe")) ->
    #{
        'operationId' => observe,
        post => #{
            tags => ?TAGS,
            desc => ?DESC(observe_resource),
            summary => <<"Observe a Resource">>,
            parameters => [
                {clientid, mk(binary(), #{in => path, example => "urn:oma:lwm2m:oma:2"})},
                {path, mk(binary(), #{in => query, required => true, example => "/3/0/7"})},
                {enable, mk(boolean(), #{in => query, required => true, example => true})}
            ],
            'requestBody' => [],
            responses => #{
                204 => <<"No Content">>,
                404 => error_codes(['CLIENT_NOT_FOUND'], <<"Clientid not found">>)
            }
        }
    };
schema(?PATH("/read")) ->
    #{
        'operationId' => read,
        post => #{
            tags => ?TAGS,
            desc => ?DESC(read_resource),
            summary => <<"Read Value from a Resource Path">>,
            parameters => [
                {clientid, mk(binary(), #{in => path, example => "urn:oma:lwm2m:oma:2"})},
                {path, mk(binary(), #{in => query, required => true, example => "/3/0/7"})}
            ],
            responses => #{
                204 => <<"No Content">>,
                404 => error_codes(['CLIENT_NOT_FOUND'], <<"clientid not found">>)
            }
        }
    };
schema(?PATH("/write")) ->
    #{
        'operationId' => write,
        post => #{
            tags => ?TAGS,
            desc => ?DESC(write_resource),
            summary => <<"Write a Value to Resource Path">>,
            parameters => [
                {clientid, mk(binary(), #{in => path, example => "urn:oma:lwm2m:oma:2"})},
                {path, mk(binary(), #{in => query, required => true, example => "/3/0/7"})},
                {type,
                    mk(
                        hoconsc:enum(?DATA_TYPE),
                        #{in => query, required => true, example => 'Integer'}
                    )},
                {value, mk(binary(), #{in => query, required => true, example => 123})}
            ],
            responses => #{
                204 => <<"No Content">>,
                404 => error_codes(['CLIENT_NOT_FOUND'], <<"Clientid not found">>)
            }
        }
    }.

fields(resource) ->
    [
        {operations, mk(binary(), #{desc => ?DESC(operations), example => "E"})},
        {'dataType',
            mk(hoconsc:enum(?DATA_TYPE), #{
                desc => ?DESC(dataType),
                example => 'Integer'
            })},
        {path, mk(binary(), #{desc => ?DESC(path), example => "urn:oma:lwm2m:oma:2"})},
        {name, mk(binary(), #{desc => ?DESC(name), example => "lwm2m-test"})}
    ].

lookup(get, #{bindings := Bindings, query_string := QS}) ->
    ClientId = maps:get(clientid, Bindings),
    case emqx_gateway_cm_registry:lookup_channels(lwm2m, ClientId) of
        [Channel | _] ->
            #{
                <<"path">> := Path,
                <<"action">> := Action
            } = QS,
            {ok, Result} = emqx_lwm2m_channel:lookup_cmd(Channel, Path, Action),
            lookup_return(Result, ClientId, Action, Path);
        _ ->
            {404, #{code => 'CLIENT_NOT_FOUND'}}
    end.

lookup_return(undefined, ClientId, Action, Path) ->
    {200, #{
        clientid => ClientId,
        action => Action,
        code => <<"6.01">>,
        codeMsg => <<"reply_not_received">>,
        path => Path
    }};
lookup_return({Code, CodeMsg, Content}, ClientId, Action, Path) ->
    {200,
        format_cmd_content(
            Content,
            Action,
            #{
                clientid => ClientId,
                action => Action,
                code => Code,
                codeMsg => CodeMsg,
                path => Path
            }
        )}.

format_cmd_content(undefined, _MsgType, Result) ->
    Result;
format_cmd_content(Content, <<"discover">>, Result) ->
    [H | Content1] = Content,
    {_, [HObjId]} = emqx_lwm2m_session:parse_object_list(H),
    [ObjId | _] = path_list(HObjId),
    ObjectList =
        case Content1 of
            [Content2 | _] ->
                {_, ObjL} = emqx_lwm2m_session:parse_object_list(Content2),
                ObjL;
            [] ->
                []
        end,

    R =
        case emqx_lwm2m_xml_object:get_obj_def(binary_to_integer(ObjId), true) of
            {error, _} ->
                lists:map(fun(Object) -> #{Object => Object} end, ObjectList);
            ObjDefinition ->
                lists:map(fun(Obj) -> to_operations(Obj, ObjDefinition) end, ObjectList)
        end,
    Result#{content => R};
format_cmd_content(Content, _, Result) ->
    Result#{content => Content}.

to_operations(Obj, ObjDefinition) ->
    [_, _, RawResId | _] = path_list(Obj),
    ResId = binary_to_integer(RawResId),
    Operations =
        case emqx_lwm2m_xml_object:get_resource_operations(ResId, ObjDefinition) of
            "E" ->
                #{operations => <<"E">>};
            Oper ->
                #{
                    'dataType' =>
                        list_to_binary(
                            emqx_lwm2m_xml_object:get_resource_type(ResId, ObjDefinition)
                        ),
                    operations => list_to_binary(Oper)
                }
        end,
    Operations#{
        path => Obj,
        name => list_to_binary(emqx_lwm2m_xml_object:get_resource_name(ResId, ObjDefinition))
    }.

path_list(Path) ->
    case binary:split(emqx_utils_binary:trim(Path, $/), [<<$/>>], [global]) of
        [ObjId, ObjInsId, ResId, ResInstId] -> [ObjId, ObjInsId, ResId, ResInstId];
        [ObjId, ObjInsId, ResId] -> [ObjId, ObjInsId, ResId];
        [ObjId, ObjInsId] -> [ObjId, ObjInsId];
        [ObjId] -> [ObjId]
    end.

observe(post, #{
    bindings := #{clientid := ClientId},
    query_string := #{<<"path">> := Path, <<"enable">> := Enable}
}) ->
    MsgType =
        case Enable of
            true -> <<"observe">>;
            _ -> <<"cancel-observe">>
        end,

    Cmd = #{
        <<"msgType">> => MsgType,
        <<"data">> => #{<<"path">> => Path}
    },

    send_cmd(ClientId, Cmd).

read(post, #{
    bindings := #{clientid := ClientId},
    query_string := Qs
}) ->
    Cmd = #{
        <<"msgType">> => <<"read">>,
        <<"data">> => Qs
    },

    send_cmd(ClientId, Cmd).

write(post, #{
    bindings := #{clientid := ClientId},
    query_string := Qs
}) ->
    Cmd = #{
        <<"msgType">> => <<"write">>,
        <<"data">> => Qs
    },

    send_cmd(ClientId, Cmd).

send_cmd(ClientId, Cmd) ->
    case emqx_gateway_cm_registry:lookup_channels(lwm2m, ClientId) of
        [Channel | _] ->
            ok = emqx_lwm2m_channel:send_cmd(Channel, Cmd),
            {204};
        _ ->
            {404, #{code => 'CLIENT_NOT_FOUND'}}
    end.
