%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([api_spec/0]).

-export([lookup_cmd/2]).

-define(PREFIX, "/gateway/lwm2m/:clientid").

-import(emqx_mgmt_util, [ object_schema/1
                        , error_schema/2
                        , properties/1]).

api_spec() ->
    {[lookup_cmd_api()], []}.

lookup_cmd_paramters() ->
    [ make_paramter(clientid, path, true, "string")
    , make_paramter(path, query, true, "string")
    , make_paramter(action, query, true, "string")].

lookup_cmd_properties() ->
    properties([ {clientid, string}
               , {path, string}
               , {action, string}
               , {code, string}
               , {codeMsg, string}
               , {content, {array, object}, lookup_cmd_content_props()}]).

lookup_cmd_content_props() ->
    [ {operations, string, <<"Resource Operations">>}
    , {dataType, string, <<"Resource Type">>}
    , {path, string, <<"Resource Path">>}
    , {name, string, <<"Resource Name">>}].

lookup_cmd_api() ->
    Metadata = #{get =>
                     #{description => <<"look up resource">>,
                       parameters => lookup_cmd_paramters(),
                       responses =>
                           #{<<"200">> => object_schema(lookup_cmd_properties()),
                             <<"404">> => error_schema("client not found error", ['CLIENT_NOT_FOUND'])
                            }
                      }},
    {?PREFIX ++ "/lookup_cmd", Metadata, lookup_cmd}.


lookup_cmd(get, #{bindings := Bindings, query_string := QS}) ->
    ClientId = maps:get(clientid, Bindings),
    case emqx_gateway_cm_registry:lookup_channels(lwm2m, ClientId) of
        [Channel | _] ->
            #{<<"path">> := Path,
              <<"action">> := Action} = QS,
            {ok, Result} = emqx_lwm2m_channel:lookup_cmd(Channel, Path, Action),
            lookup_cmd_return(Result, ClientId, Action, Path);
        _ ->
            {404, #{code => 'CLIENT_NOT_FOUND'}}
    end.

lookup_cmd_return(undefined, ClientId, Action, Path) ->
    {200,
     #{clientid => ClientId,
       action => Action,
       code => <<"6.01">>,
       codeMsg => <<"reply_not_received">>,
       path => Path}};

lookup_cmd_return({Code, CodeMsg, Content}, ClientId, Action, Path) ->
    {200,
     format_cmd_content(Content,
                        Action,
                        #{clientid => ClientId,
                          action => Action,
                          code => Code,
                          codeMsg => CodeMsg,
                          path => Path})}.

format_cmd_content(undefined, _MsgType, Result) ->
    Result;

format_cmd_content(Content, <<"discover">>, Result) ->
    [H | Content1] = Content,
    {_, [HObjId]} = emqx_lwm2m_session:parse_object_list(H),
    [ObjId | _]= path_list(HObjId),
    ObjectList = case Content1 of
                     [Content2 | _] ->
                         {_, ObjL} = emqx_lwm2m_session:parse_object_list(Content2),
                         ObjL;
                     [] -> []
                 end,

    R = case emqx_lwm2m_xml_object:get_obj_def(binary_to_integer(ObjId), true) of
            {error, _} ->
                lists:map(fun(Object) -> #{Object => Object} end, ObjectList);
            ObjDefinition ->
                lists:map(
                  fun(Object) ->
                          [_, _, RawResId| _] = path_list(Object),
                          ResId = binary_to_integer(RawResId),
                          Operations = case emqx_lwm2m_xml_object:get_resource_operations(ResId, ObjDefinition) of
                                           "E" ->
                                               #{operations => list_to_binary("E")};
                                           Oper ->
                                               #{'dataType' => list_to_binary(emqx_lwm2m_xml_object:get_resource_type(ResId, ObjDefinition)),
                                                 operations => list_to_binary(Oper)}
                                               end,
                          Operations#{path => Object,
                                      name => list_to_binary(emqx_lwm2m_xml_object:get_resource_name(ResId, ObjDefinition))}
                  end, ObjectList)
        end,
    Result#{content => R};

format_cmd_content(Content, _, Result) ->
    Result#{content => Content}.

path_list(Path) ->
    case binary:split(binary_util:trim(Path, $/), [<<$/>>], [global]) of
        [ObjId, ObjInsId, ResId, ResInstId] -> [ObjId, ObjInsId, ResId, ResInstId];
        [ObjId, ObjInsId, ResId] -> [ObjId, ObjInsId, ResId];
        [ObjId, ObjInsId] -> [ObjId, ObjInsId];
        [ObjId] -> [ObjId]
    end.

make_paramter(Name, In, IsRequired, Type) ->
    #{name => Name,
      in => In,
      required => IsRequired,
      schema => #{type => Type}}.
