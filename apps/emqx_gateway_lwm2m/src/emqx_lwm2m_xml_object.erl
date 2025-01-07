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

-module(emqx_lwm2m_xml_object).

-include("emqx_lwm2m.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-export([
    get_obj_def/2,
    get_obj_def_assertive/2,
    get_object_id/1,
    get_object_name/1,
    get_object_and_resource_id/2,
    get_resource_type/2,
    get_resource_name/2,
    get_resource_operations/2
]).

get_obj_def_assertive(ObjectId, IsInt) ->
    case get_obj_def(ObjectId, IsInt) of
        {error, no_xml_definition} ->
            erlang:throw({bad_request, {unknown_object_id, ObjectId}});
        Xml ->
            Xml
    end.

get_obj_def(ObjectIdInt, true) ->
    emqx_lwm2m_xml_object_db:find_objectid(ObjectIdInt);
get_obj_def(ObjectNameStr, false) ->
    emqx_lwm2m_xml_object_db:find_name(ObjectNameStr).

get_object_id(ObjDefinition) ->
    [#xmlText{value = ObjectId}] = xmerl_xpath:string("ObjectID/text()", ObjDefinition),
    ObjectId.

get_object_name(ObjDefinition) ->
    [#xmlText{value = ObjectName}] = xmerl_xpath:string("Name/text()", ObjDefinition),
    ObjectName.

get_object_and_resource_id(ResourceNameBinary, ObjDefinition) ->
    ResourceNameString = binary_to_list(ResourceNameBinary),
    [#xmlText{value = ObjectId}] = xmerl_xpath:string("ObjectID/text()", ObjDefinition),
    [#xmlAttribute{value = ResourceId}] = xmerl_xpath:string(
        "Resources/Item/Name[.=\"" ++ ResourceNameString ++ "\"]/../@ID", ObjDefinition
    ),
    {ObjectId, ResourceId}.

get_resource_type(ResourceIdInt, ObjDefinition) ->
    ResourceIdString = integer_to_list(ResourceIdInt),
    [#xmlText{value = DataType}] = xmerl_xpath:string(
        "Resources/Item[@ID=\"" ++ ResourceIdString ++ "\"]/Type/text()", ObjDefinition
    ),
    DataType.

get_resource_name(ResourceIdInt, ObjDefinition) ->
    ResourceIdString = integer_to_list(ResourceIdInt),
    [#xmlText{value = Name}] = xmerl_xpath:string(
        "Resources/Item[@ID=\"" ++ ResourceIdString ++ "\"]/Name/text()", ObjDefinition
    ),
    Name.

get_resource_operations(ResourceIdInt, ObjDefinition) ->
    ResourceIdString = integer_to_list(ResourceIdInt),
    [#xmlText{value = Operations}] = xmerl_xpath:string(
        "Resources/Item[@ID=\"" ++ ResourceIdString ++ "\"]/Operations/text()", ObjDefinition
    ),
    Operations.
