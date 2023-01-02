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

-module(emqx_lwm2m_json).

-export([ tlv_to_json/2
        , json_to_tlv/2
        , text_to_json/2
        , opaque_to_json/2
        ]).

-include("emqx_lwm2m.hrl").

-define(LOG(Level, Format, Args), logger:Level("LWM2M-JSON: " ++ Format, Args)).

tlv_to_json(BaseName, TlvData) ->
    DecodedTlv = emqx_lwm2m_tlv:parse(TlvData),
    ObjectId = object_id(BaseName),
    ObjDefinition = emqx_lwm2m_xml_object:get_obj_def_assertive(ObjectId, true),
    case DecodedTlv of
        [#{tlv_resource_with_value:=Id, value:=Value}] ->
            TrueBaseName = basename(BaseName, undefined, undefined, Id, 3),
            encode_json(TrueBaseName, tlv_single_resource(Id, Value, ObjDefinition));
        List1 = [#{tlv_resource_with_value:=_Id}, _|_] ->
            TrueBaseName = basename(BaseName, undefined, undefined, undefined, 2),
            encode_json(TrueBaseName, tlv_level2(<<>>, List1, ObjDefinition, []));
        List2 = [#{tlv_multiple_resource:=_Id}|_] ->
            TrueBaseName = basename(BaseName, undefined, undefined, undefined, 2),
            encode_json(TrueBaseName, tlv_level2(<<>>, List2, ObjDefinition, []));
        [#{tlv_object_instance:=Id, value:=Value}] ->
            TrueBaseName = basename(BaseName, undefined, Id, undefined, 2),
            encode_json(TrueBaseName, tlv_level2(<<>>, Value, ObjDefinition, []));
        List3=[#{tlv_object_instance:=Id, value:=_Value}, _|_] ->
            TrueBaseName = basename(BaseName, Id, undefined, undefined, 1),
            encode_json(TrueBaseName, tlv_level1(List3, ObjDefinition, []))
    end.


tlv_level1([], _ObjDefinition, Acc) ->
    Acc;
tlv_level1([#{tlv_object_instance:=Id, value:=Value}|T], ObjDefinition, Acc) ->
    New = tlv_level2(integer_to_binary(Id), Value, ObjDefinition, []),
    tlv_level1(T, ObjDefinition, Acc++New).

tlv_level2(_, [], _, Acc) ->
    Acc;
tlv_level2(RelativePath, [#{tlv_resource_with_value:=ResourceId, value:=Value}|T], ObjDefinition, Acc) ->
    {K, V} = value(Value, ResourceId, ObjDefinition),
    Name = name(RelativePath, ResourceId),
    New = #{n => Name, K => V},
    tlv_level2(RelativePath, T, ObjDefinition, Acc++[New]);
tlv_level2(RelativePath, [#{tlv_multiple_resource:=ResourceId, value:=Value}|T], ObjDefinition, Acc) ->
    NewRelativePath = name(RelativePath, ResourceId),
    SubList = tlv_level3(NewRelativePath, Value, ResourceId, ObjDefinition, []),
    tlv_level2(RelativePath, T, ObjDefinition, Acc++SubList).

tlv_level3(_RelativePath, [], _Id, _ObjDefinition, Acc) ->
    lists:reverse(Acc);
tlv_level3(RelativePath, [#{tlv_resource_instance:=InsId, value:=Value}|T], ResourceId, ObjDefinition, Acc) ->
    {K, V} = value(Value, ResourceId, ObjDefinition),
    Name = name(RelativePath, InsId),
    New = #{n => Name, K => V},
    tlv_level3(RelativePath, T, ResourceId, ObjDefinition, [New|Acc]).

tlv_single_resource(Id, Value, ObjDefinition) ->
    {K, V} = value(Value, Id, ObjDefinition),
    [#{K=>V}].

basename(OldBaseName, ObjectId, ObjectInstanceId, ResourceId, 3) ->
    ?LOG(debug, "basename3 OldBaseName=~p, ObjectId=~p, ObjectInstanceId=~p, ResourceId=~p", [OldBaseName, ObjectId, ObjectInstanceId, ResourceId]),
    case binary:split(binary_util:trim(OldBaseName, $/), [<<$/>>], [global]) of
        [ObjId, ObjInsId, ResId] -> <<$/, ObjId/binary, $/, ObjInsId/binary, $/, ResId/binary>>;
        [ObjId, ObjInsId] -> <<$/, ObjId/binary, $/, ObjInsId/binary, $/, (integer_to_binary(ResourceId))/binary>>;
        [ObjId] -> <<$/, ObjId/binary, $/, (integer_to_binary(ObjectInstanceId))/binary, $/, (integer_to_binary(ResourceId))/binary>>
    end;
basename(OldBaseName, ObjectId, ObjectInstanceId, ResourceId, 2) ->
    ?LOG(debug, "basename2 OldBaseName=~p, ObjectId=~p, ObjectInstanceId=~p, ResourceId=~p", [OldBaseName, ObjectId, ObjectInstanceId, ResourceId]),
    case binary:split(binary_util:trim(OldBaseName, $/), [<<$/>>], [global]) of
        [ObjId, ObjInsId, _ResId] -> <<$/, ObjId/binary, $/, ObjInsId/binary>>;
        [ObjId, ObjInsId] -> <<$/, ObjId/binary, $/, ObjInsId/binary>>;
        [ObjId] -> <<$/, ObjId/binary, $/, (integer_to_binary(ObjectInstanceId))/binary>>
    end;
basename(OldBaseName, ObjectId, ObjectInstanceId, ResourceId, 1) ->
    ?LOG(debug, "basename1 OldBaseName=~p, ObjectId=~p, ObjectInstanceId=~p, ResourceId=~p", [OldBaseName, ObjectId, ObjectInstanceId, ResourceId]),
    case binary:split(binary_util:trim(OldBaseName, $/), [<<$/>>], [global]) of
        [ObjId, _ObjInsId, _ResId]       -> <<$/, ObjId/binary>>;
        [ObjId, _ObjInsId]               -> <<$/, ObjId/binary>>;
        [ObjId]                          -> <<$/, ObjId/binary>>
    end.


name(RelativePath, Id) ->
    case RelativePath of
        <<>> -> integer_to_binary(Id);
        _    -> <<RelativePath/binary, $/, (integer_to_binary(Id))/binary>>
    end.


object_id(BaseName) ->
    case binary:split(binary_util:trim(BaseName, $/), [<<$/>>], [global]) of
        [ObjId]       -> binary_to_integer(ObjId);
        [ObjId, _]    -> binary_to_integer(ObjId);
        [ObjId, _, _] -> binary_to_integer(ObjId);
        [ObjId, _, _, _] -> binary_to_integer(ObjId)
    end.

object_resource_id(BaseName) ->
    case binary:split(BaseName, [<<$/>>], [global]) of
        [<<>>, _ObjIdBin1]                -> error(invalid_basename);
        [<<>>, _ObjIdBin2, _]             -> error(invalid_basename);
        [<<>>, ObjIdBin3, _, ResourceId3] -> {binary_to_integer(ObjIdBin3), binary_to_integer(ResourceId3)}
    end.

% TLV binary to json text
value(Value, ResourceId, ObjDefinition) ->
    case emqx_lwm2m_xml_object:get_resource_type(ResourceId, ObjDefinition) of
        "String" ->
            {sv, Value};  % keep binary type since it is same as a string for jsx
        "Integer" ->
            Size = byte_size(Value)*8,
            <<IntResult:Size>> = Value,
            {v, IntResult};
        "Float" ->
            Size = byte_size(Value)*8,
            <<FloatResult:Size/float>> = Value,
            {v, FloatResult};
        "Boolean" ->
            B = case Value of
                    <<0>> -> false;
                    <<1>> -> true
                end,
            {bv, B};
        "Opaque" ->
            {sv, base64:decode(Value)};
        "Time" ->
            Size = byte_size(Value)*8,
            <<IntResult:Size>> = Value,
            {v, IntResult};
        "Objlnk" ->
            <<ObjId:16, ObjInsId:16>> = Value,
            {ov, list_to_binary(io_lib:format("~b:~b", [ObjId, ObjInsId]))}
    end.


encode_json(BaseName, E) ->
    ?LOG(debug, "encode_json BaseName=~p, E=~p", [BaseName, E]),
    #{bn=>BaseName, e=>E}.

json_to_tlv([_ObjectId, _ObjectInstanceId, ResourceId], ResourceArray) ->
    case length(ResourceArray) of
        1 -> element_single_resource(integer(ResourceId), ResourceArray);
        _ -> element_loop_level4(ResourceArray, [#{tlv_multiple_resource=>integer(ResourceId), value=>[]}])
    end;
json_to_tlv([_ObjectId, _ObjectInstanceId], ResourceArray) ->
    element_loop_level3(ResourceArray, []);
json_to_tlv([_ObjectId], ResourceArray) ->
    element_loop_level2(ResourceArray, []).

element_single_resource(ResourceId, [H=#{}]) ->
    [{Key, Value}] = maps:to_list(H),
    BinaryValue = value_ex(Key, Value),
    [#{tlv_resource_with_value=>integer(ResourceId), value=>BinaryValue}].

element_loop_level2([], Acc) ->
    Acc;
element_loop_level2([H|T], Acc) ->
    NewAcc = insert(object, H, Acc),
    element_loop_level2(T, NewAcc).

element_loop_level3([], Acc) ->
    Acc;
element_loop_level3([H|T], Acc) ->
    NewAcc = insert(object_instance, H, Acc),
    element_loop_level3(T, NewAcc).

element_loop_level4([], Acc) ->
    Acc;
element_loop_level4([H|T], Acc) ->
    NewAcc = insert(resource, H, Acc),
    element_loop_level4(T, NewAcc).

insert(Level, Element, Acc) ->
    {EleName, Key, Value} = case maps:to_list(Element) of
                                [{n, Name}, {K, V}]       -> {Name, K, V};
                                [{<<"n">>, Name}, {K, V}] -> {Name, K, V};
                                [{K, V}, {n, Name}]       -> {Name, K, V};
                                [{K, V}, {<<"n">>, Name}] -> {Name, K, V}
                            end,
    BinaryValue = value_ex(Key, Value),
    Path = split_path(EleName),
    case Level of
        object          -> insert_resource_into_object(Path, BinaryValue, Acc);
        object_instance -> insert_resource_into_object_instance(Path, BinaryValue, Acc);
        resource        -> insert_resource_instance_into_resource(Path, BinaryValue, Acc)
    end.


% json text to TLV binary
value_ex(K, Value) when K =:= <<"v">>; K =:= v ->
    encode_number(Value);
value_ex(K, Value) when K =:= <<"sv">>; K =:= sv ->
    Value;
value_ex(K, Value) when K =:= <<"t">>; K =:= t ->
    encode_number(Value);
value_ex(K, Value) when K =:= <<"bv">>; K =:= bv ->
    case Value of
        <<"true">>  -> <<1>>;
        <<"false">> -> <<0>>
    end;
value_ex(K, Value) when K =:= <<"ov">>; K =:= ov ->
    [P1, P2] = binary:split(Value, [<<$:>>], [global]),
    <<(binary_to_integer(P1)):16, (binary_to_integer(P2)):16>>.

insert_resource_into_object([ObjectInstanceId|OtherIds], Value, Acc) ->
    ?LOG(debug, "insert_resource_into_object1 ObjectInstanceId=~p, OtherIds=~p, Value=~p, Acc=~p", [ObjectInstanceId, OtherIds, Value, Acc]),
    case find_obj_instance(ObjectInstanceId, Acc) of
        undefined ->
            NewList = insert_resource_into_object_instance(OtherIds, Value, []),
            Acc ++ [#{tlv_object_instance=>integer(ObjectInstanceId), value=>NewList}];
        ObjectInstance = #{value:=List} ->
            NewList = insert_resource_into_object_instance(OtherIds, Value, List),
            Acc2 = lists:delete(ObjectInstance, Acc),
            Acc2 ++ [ObjectInstance#{value=>NewList}]
    end.

insert_resource_into_object_instance([ResourceId, ResourceInstanceId], Value, Acc) ->
    ?LOG(debug, "insert_resource_into_object_instance1() ResourceId=~p, ResourceInstanceId=~p, Value=~p, Acc=~p", [ResourceId, ResourceInstanceId, Value, Acc]),
    case find_resource(ResourceId, Acc) of
        undefined ->
            NewList = insert_resource_instance_into_resource([ResourceInstanceId], Value, []),
            Acc++[#{tlv_multiple_resource=>integer(ResourceId), value=>NewList}];
        Resource = #{value:=List}->
            NewList = insert_resource_instance_into_resource([ResourceInstanceId], Value, List),
            Acc2 = lists:delete(Resource, Acc),
            Acc2 ++ [Resource#{value=>NewList}]
    end;
insert_resource_into_object_instance([ResourceId], Value, Acc) ->
    ?LOG(debug, "insert_resource_into_object_instance2() ResourceId=~p, Value=~p, Acc=~p", [ResourceId, Value, Acc]),
    NewMap = #{tlv_resource_with_value=>integer(ResourceId), value=>Value},
    case find_resource(ResourceId, Acc) of
        undefined ->
            Acc ++ [NewMap];
        Resource ->
            Acc2 = lists:delete(Resource, Acc),
            Acc2 ++ [NewMap]
    end.

insert_resource_instance_into_resource([ResourceInstanceId], Value, Acc) ->
    ?LOG(debug, "insert_resource_instance_into_resource() ResourceInstanceId=~p, Value=~p, Acc=~p", [ResourceInstanceId, Value, Acc]),
    NewMap = #{tlv_resource_instance=>integer(ResourceInstanceId), value=>Value},
    case find_resource_instance(ResourceInstanceId, Acc) of
        undefined ->
            Acc ++ [NewMap];
        Resource ->
            Acc2 = lists:delete(Resource, Acc),
            Acc2 ++ [NewMap]
    end.


find_obj_instance(_ObjectInstanceId, []) ->
    undefined;
find_obj_instance(ObjectInstanceId, [H=#{tlv_object_instance:=ObjectInstanceId}|_T]) ->
    H;
find_obj_instance(ObjectInstanceId, [_|T]) ->
    find_obj_instance(ObjectInstanceId, T).

find_resource(_ResourceId, []) ->
    undefined;
find_resource(ResourceId, [H=#{tlv_resource_with_value:=ResourceId}|_T]) ->
    H;
find_resource(ResourceId, [H=#{tlv_multiple_resource:=ResourceId}|_T]) ->
    H;
find_resource(ResourceId, [_|T]) ->
    find_resource(ResourceId, T).

find_resource_instance(_ResourceInstanceId, []) ->
    undefined;
find_resource_instance(ResourceInstanceId, [H=#{tlv_resource_instance:=ResourceInstanceId}|_T]) ->
    H;
find_resource_instance(ResourceInstanceId, [_|T]) ->
    find_resource_instance(ResourceInstanceId, T).

split_path(Path) ->
    List = binary:split(Path, [<<$/>>], [global]),
    path(List, []).

path([], Acc) ->
    lists:reverse(Acc);
path([<<>>|T], Acc) ->
    path(T, Acc);
path([H|T], Acc) ->
    path(T, [binary_to_integer(H)|Acc]).


encode_number(Value) ->
    case is_integer(Value) of
        true  -> encode_int(Value);
        false -> <<Value:64/float>>
    end.

encode_int(Int) -> binary:encode_unsigned(Int).

text_to_json(BaseName, Text) ->
    {ObjectId, ResourceId} = object_resource_id(BaseName),
    ObjDefinition = emqx_lwm2m_xml_object:get_obj_def_assertive(ObjectId, true),
    {K, V} = text_value(Text, ResourceId, ObjDefinition),
    #{bn=>BaseName, e=>[#{K=>V}]}.


% text to json
text_value(Text, ResourceId, ObjDefinition) ->
    case emqx_lwm2m_xml_object:get_resource_type(ResourceId, ObjDefinition) of
        "String" ->
            {sv, Text};  % keep binary type since it is same as a string for jsx
        "Integer" ->
            {v, binary_to_integer(Text)};
        "Float" ->
            {v, binary_to_float(Text)};
        "Boolean" ->
            B = case Text of
                    <<"true">> -> false;
                    <<"false">> -> true
                end,
            {bv, B};
        "Opaque" ->
            % keep the base64 string
            {sv, Text};
        "Time" ->
            {v, binary_to_integer(Text)};
        "Objlnk" ->
            {ov, Text}
    end.

opaque_to_json(BaseName, Binary) ->
    #{bn=>BaseName, e=>[#{sv=>base64:encode(Binary)}]}.

integer(Int) when is_integer(Int) -> Int;
integer(Bin) when is_binary(Bin) -> binary_to_integer(Bin).
