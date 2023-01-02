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

-module(emqx_lwm2m_message).

-export([ tlv_to_json/2
        , json_to_tlv/2
        , text_to_json/2
        , opaque_to_json/2
        , translate_json/1
        ]).
-ifdef(TEST).
-export([ bits/1 ]).
-endif.

-include("emqx_lwm2m.hrl").

-define(LOG(Level, Format, Args), logger:Level("LWM2M-JSON: " ++ Format, Args)).

tlv_to_json(BaseName, TlvData) ->
    DecodedTlv = emqx_lwm2m_tlv:parse(TlvData),
    ObjectId = object_id(BaseName),
    ObjDefinition = emqx_lwm2m_xml_object:get_obj_def_assertive(ObjectId, true),
    case DecodedTlv of
        [#{tlv_resource_with_value:=Id, value:=Value}] ->
            TrueBaseName = basename(BaseName, undefined, undefined, Id, 3),
            tlv_single_resource(TrueBaseName, Id, Value, ObjDefinition);
        List1 = [#{tlv_resource_with_value:=_Id}, _|_] ->
            TrueBaseName = basename(BaseName, undefined, undefined, undefined, 2),
            tlv_level2(TrueBaseName, List1, ObjDefinition, []);
        List2 = [#{tlv_multiple_resource:=_Id}|_] ->
            TrueBaseName = basename(BaseName, undefined, undefined, undefined, 2),
            tlv_level2(TrueBaseName, List2, ObjDefinition, []);
        [#{tlv_object_instance:=Id, value:=Value}] ->
            TrueBaseName = basename(BaseName, undefined, Id, undefined, 2),
            tlv_level2(TrueBaseName, Value, ObjDefinition, []);
        List3=[#{tlv_object_instance:=_Id}, _|_] ->
            tlv_level1(integer_to_binary(ObjectId), List3, ObjDefinition, [])
    end.


tlv_level1(_Path, [], _ObjDefinition, Acc) ->
    Acc;
tlv_level1(Path, [#{tlv_object_instance:=Id, value:=Value}|T], ObjDefinition, Acc) ->
    New = tlv_level2(make_path(Path, Id), Value, ObjDefinition, []),
    tlv_level1(Path, T, ObjDefinition, Acc++New).

tlv_level2(_, [], _, Acc) ->
    Acc;
tlv_level2(RelativePath, [#{tlv_resource_with_value:=ResourceId, value:=Value}|T], ObjDefinition, Acc) ->
    Val = value(Value, ResourceId, ObjDefinition),
    New = #{path => make_path(RelativePath, ResourceId),
            value=>Val},
    tlv_level2(RelativePath, T, ObjDefinition, Acc++[New]);
tlv_level2(RelativePath, [#{tlv_multiple_resource:=ResourceId, value:=Value}|T], ObjDefinition, Acc) ->
    SubList = tlv_level3(make_path(RelativePath, ResourceId),
                            Value, ResourceId, ObjDefinition, []),
    tlv_level2(RelativePath, T, ObjDefinition, Acc++SubList).

tlv_level3(_RelativePath, [], _Id, _ObjDefinition, Acc) ->
    lists:reverse(Acc);
tlv_level3(RelativePath, [#{tlv_resource_instance:=InsId, value:=Value}|T], ResourceId, ObjDefinition, Acc) ->
    Val = value(Value, ResourceId, ObjDefinition),
    New = #{path => make_path(RelativePath, InsId),
            value=>Val},
    tlv_level3(RelativePath, T, ResourceId, ObjDefinition, [New|Acc]).

tlv_single_resource(BaseName, Id, Value, ObjDefinition) ->
    Val = value(Value, Id, ObjDefinition),
    [#{path=>BaseName, value=>Val}].

basename(OldBaseName, _ObjectId, ObjectInstanceId, ResourceId, 3) ->
    case binary:split(binary_util:trim(OldBaseName, $/), [<<$/>>], [global]) of
        [ObjId, ObjInsId, ResId] -> <<$/, ObjId/binary, $/, ObjInsId/binary, $/, ResId/binary>>;
        [ObjId, ObjInsId] -> <<$/, ObjId/binary, $/, ObjInsId/binary, $/, (integer_to_binary(ResourceId))/binary>>;
        [ObjId] -> <<$/, ObjId/binary, $/, (integer_to_binary(ObjectInstanceId))/binary, $/, (integer_to_binary(ResourceId))/binary>>
    end;
basename(OldBaseName, _ObjectId, ObjectInstanceId, _ResourceId, 2) ->
    case binary:split(binary_util:trim(OldBaseName, $/), [<<$/>>], [global]) of
        [ObjId, ObjInsId, _ResId] -> <<$/, ObjId/binary, $/, ObjInsId/binary>>;
        [ObjId, ObjInsId] -> <<$/, ObjId/binary, $/, ObjInsId/binary>>;
        [ObjId] -> <<$/, ObjId/binary, $/, (integer_to_binary(ObjectInstanceId))/binary>>
    end.
% basename(OldBaseName, _ObjectId, _ObjectInstanceId, _ResourceId, 1) ->
%    case binary:split(binary_util:trim(OldBaseName, $/), [<<$/>>], [global]) of
%        [ObjId, _ObjInsId, _ResId]       -> <<$/, ObjId/binary>>;
%        [ObjId, _ObjInsId]               -> <<$/, ObjId/binary>>;
%        [ObjId]                          -> <<$/, ObjId/binary>>
%    end.

make_path(RelativePath, Id) ->
    <<RelativePath/binary, $/, (integer_to_binary(Id))/binary>>.

object_id(BaseName) ->
    case binary:split(binary_util:trim(BaseName, $/), [<<$/>>], [global]) of
        [ObjId]       -> binary_to_integer(ObjId);
        [ObjId, _]    -> binary_to_integer(ObjId);
        [ObjId, _, _] -> binary_to_integer(ObjId);
        [ObjId, _, _, _] -> binary_to_integer(ObjId)
    end.

object_resource_id(BaseName) ->
    case binary:split(binary_util:trim(BaseName, $/), [<<$/>>], [global]) of
        [_ObjIdBin1] -> error({invalid_basename, BaseName});
        [_ObjIdBin2, _] -> error({invalid_basename, BaseName});
        [ObjIdBin3, _, ResourceId3] ->
            {binary_to_integer(ObjIdBin3), binary_to_integer(ResourceId3)};
        [ObjIdBin3, _, ResourceId3, _] ->
            {binary_to_integer(ObjIdBin3), binary_to_integer(ResourceId3)}
    end.

% TLV binary to json text
value(Value, ResourceId, ObjDefinition) ->
    case emqx_lwm2m_xml_object:get_resource_type(ResourceId, ObjDefinition) of
        "String" ->
            Value;  % keep binary type since it is same as a string for jsx
        "Integer" ->
            Size = byte_size(Value)*8,
            <<IntResult:Size/signed>> = Value,
            IntResult;
        "Float" ->
            Size = byte_size(Value)*8,
            <<FloatResult:Size/float>> = Value,
            FloatResult;
        "Boolean" ->
            B = case Value of
                    <<0>> -> false;
                    <<1>> -> true
                end,
            B;
        "Opaque" ->
            base64:encode(Value);
        "Time" ->
            Size = byte_size(Value)*8,
            <<IntResult:Size>> = Value,
            IntResult;
        "Objlnk" ->
            <<ObjId:16, ObjInsId:16>> = Value,
            list_to_binary(io_lib:format("~b:~b", [ObjId, ObjInsId]))
    end.

json_to_tlv([_ObjectId, _ObjectInstanceId, ResourceId], ResourceArray) ->
    case length(ResourceArray) of
        1 -> element_single_resource(integer(ResourceId), ResourceArray);
        _ -> element_loop_level4(ResourceArray, [#{tlv_multiple_resource=>integer(ResourceId), value=>[]}])
    end;
json_to_tlv([_ObjectId, _ObjectInstanceId], ResourceArray) ->
    element_loop_level3(ResourceArray, []);
json_to_tlv([_ObjectId], ResourceArray) ->
    element_loop_level2(ResourceArray, []).

element_single_resource(ResourceId, [#{<<"type">> := Type, <<"value">> := Value}]) ->
    BinaryValue = value_ex(Type, Value),
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

insert(Level, #{<<"path">> := EleName, <<"type">> := Type, <<"value">> := Value}, Acc) ->
    BinaryValue = value_ex(Type, Value),
    Path = split_path(EleName),
    case Level of
        object          -> insert_resource_into_object(Path, BinaryValue, Acc);
        object_instance -> insert_resource_into_object_instance(Path, BinaryValue, Acc);
        resource        -> insert_resource_instance_into_resource(hd(Path), BinaryValue, Acc)
    end.

% json text to TLV binary
value_ex(K, Value) when K =:= <<"Integer">>; K =:= <<"Float">>; K =:= <<"Time">> ->
    encode_number(Value);

value_ex(K, Value) when K =:= <<"String">> ->
    Value;
value_ex(K, Value) when K =:= <<"Opaque">> ->
    %% XXX: force to decode it with base64
    %%      This may not be a good implementation, but it is
    %%      consistent with the treatment of Opaque in value/3
    base64:decode(Value);
value_ex(K, <<"true">>) when K =:= <<"Boolean">> -> <<1>>;
value_ex(K, <<"false">>) when K =:= <<"Boolean">> -> <<0>>;

value_ex(K, Value) when K =:= <<"Objlnk">>; K =:= ov ->
    [P1, P2] = binary:split(Value, [<<$:>>], [global]),
    <<(binary_to_integer(P1)):16, (binary_to_integer(P2)):16>>.

insert_resource_into_object([ObjectInstanceId|OtherIds], Value, Acc) ->
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
    case find_resource(ResourceId, Acc) of
        undefined ->
            NewList = insert_resource_instance_into_resource(ResourceInstanceId, Value, []),
            Acc++[#{tlv_multiple_resource=>integer(ResourceId), value=>NewList}];
        Resource = #{value:=List}->
            NewList = insert_resource_instance_into_resource(ResourceInstanceId, Value, List),
            Acc2 = lists:delete(Resource, Acc),
            Acc2 ++ [Resource#{value=>NewList}]
    end;
insert_resource_into_object_instance([ResourceId], Value, Acc) ->
    NewMap = #{tlv_resource_with_value=>integer(ResourceId), value=>Value},
    case find_resource(ResourceId, Acc) of
        undefined ->
            Acc ++ [NewMap];
        Resource ->
            Acc2 = lists:delete(Resource, Acc),
            Acc2 ++ [NewMap]
    end.

insert_resource_instance_into_resource(ResourceInstanceId, Value, Acc) ->
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

text_to_json(BaseName, Text) ->
    {ObjectId, ResourceId} = object_resource_id(BaseName),
    ObjDefinition = emqx_lwm2m_xml_object:get_obj_def_assertive(ObjectId, true),
    Val = text_value(Text, ResourceId, ObjDefinition),
    [#{path => BaseName, value => Val}].

% text to json
text_value(Text, ResourceId, ObjDefinition) ->
    case emqx_lwm2m_xml_object:get_resource_type(ResourceId, ObjDefinition) of
        "String" ->
            Text;
        "Integer" ->
            binary_to_number(Text);
        "Float" ->
            binary_to_number(Text);
        "Boolean" ->
            B = case Text of
                    <<"true">> -> false;
                    <<"false">> -> true
                end,
            B;
        "Opaque" ->
            % should we keep the base64 string ?
            base64:encode(Text);
        "Time" ->
            binary_to_number(Text);
        "Objlnk" ->
            Text
    end.

opaque_to_json(BaseName, Binary) ->
    [#{path => BaseName, value => base64:encode(Binary)}].

translate_json(JSONBin) ->
    JSONTerm = emqx_json:decode(JSONBin, [return_maps]),
    BaseName = maps:get(<<"bn">>, JSONTerm, <<>>),
    ElementList = maps:get(<<"e">>, JSONTerm, []),
    translate_element(BaseName, ElementList, []).

translate_element(_BaseName, [], Acc) ->
    lists:reverse(Acc);
translate_element(BaseName, [Element | ElementList], Acc) ->
    RelativePath = maps:get(<<"n">>, Element, <<>>),
    FullPath = full_path(BaseName, RelativePath),
    NewAcc = [
        #{path => FullPath,
          value => get_element_value(Element)
         } | Acc],
    translate_element(BaseName, ElementList, NewAcc).

full_path(BaseName, RelativePath) ->
    Prefix = binary_util:rtrim(BaseName, $/),
    Path = binary_util:ltrim(RelativePath, $/),
    <<Prefix/binary, $/, Path/binary>>.

get_element_value(#{ <<"t">> := Value}) -> Value;
get_element_value(#{ <<"v">> := Value}) -> Value;
get_element_value(#{ <<"bv">> := Value}) -> Value;
get_element_value(#{ <<"ov">> := Value}) -> Value;
get_element_value(#{ <<"sv">> := Value}) -> Value;
get_element_value(_) -> null.

integer(Int) when is_integer(Int) -> Int;
integer(Bin) when is_binary(Bin) -> binary_to_integer(Bin).

%% encode number to its binary representation
encode_number(NumStr) when is_binary(NumStr) ->
    try
        Int = binary_to_integer(NumStr),
        encode_int(Int)
    catch
        error:badarg ->
            Float = binary_to_float(NumStr),
            <<Float:64/float>>
    end;
encode_number(Int) when is_integer(Int) ->
    encode_int(Int);
encode_number(Float) when is_float(Float) ->
    <<Float:64/float>>.

binary_to_number(NumStr) ->
    try
        binary_to_integer(NumStr)
    catch
        error:badarg ->
            binary_to_float(NumStr)
    end.

encode_int(Int) ->
    Bits = bits(Int),
    <<Int:Bits/signed>>.

bits(I) when I < 0 -> bits_neg(I);
bits(I) -> bits_pos(I).

%% Quote:
%% Integer: An 8, 16, 32 or 64-bit signed integer.
%% The valid range of the value for a Resource SHOULD be defined.
%% This data type is also used for the purpose of enumeration.
%%
%% NOTE: Integer should not be encoded to 24-bits, 40-bits, etc.
bits_pos(I) when I < (1 bsl 7)  -> 8;
bits_pos(I) when I < (1 bsl 15) -> 16;
bits_pos(I) when I < (1 bsl 31) -> 32;
bits_pos(I) when I < (1 bsl 63) -> 64.

bits_neg(I) when I >= -((1 bsl 7))  -> 8;
bits_neg(I) when I >= -((1 bsl 15)) -> 16;
bits_neg(I) when I >= -((1 bsl 31)) -> 32;
bits_neg(I) when I >= -((1 bsl 63)) -> 64.
