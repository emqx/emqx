%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_lwm2m_tlv).

-export([
    parse/1,
    encode/1
]).

-ifdef(TEST).
-export([binary_to_hex_string/1]).
-endif.

-include("src/lwm2m/include/emqx_lwm2m.hrl").

-define(TLV_TYPE_OBJECT_INSTANCE, 0).
-define(TLV_TYPE_RESOURCE_INSTANCE, 1).
-define(TLV_TYPE_MULTIPLE_RESOURCE, 2).
-define(TLV_TYPE_RESOURCE_WITH_VALUE, 3).

-define(TLV_NO_LENGTH_FIELD, 0).
-define(TLV_LEGNTH_8_BIT, 1).
-define(TLV_LEGNTH_16_BIT, 2).
-define(TLV_LEGNTH_24_BIT, 3).

%----------------------------------------------------------------------------------------------------------------------------------------
% [#{tlv_object_instance := Id11, value := Value11}, #{tlv_object_instance := Id12, value := Value12}, ...]
% where Value11 and Value12 is a list:
%     [#{tlv_resource_with_value => Id21, value => Value21}, #{tlv_multiple_resource => Id22, value = Value22}, ...]
%     where Value21 is a binary
%           Value22 is a list:
%                 [#{tlv_resource_instance => Id31,  value => Value31}, #{tlv_resource_instance => Id32,  value => Value32}, ...]
%                 where Value31 and Value32 is a binary
%
% correspond to three levels:
% 1)    Object Instance Level
% 2)        Resource Level
% 3)            Resource Instance Level
%
% NOTE: TLV does not has object level, only has object instance level. It implies TLV can not represent multiple objects
%----------------------------------------------------------------------------------------------------------------------------------------

parse(Data) ->
    parse_loop(Data, []).

parse_loop(<<>>, Acc) ->
    lists:reverse(Acc);
parse_loop(Data, Acc) ->
    {New, Rest} = parse_step1(Data),
    parse_loop(Rest, [New | Acc]).

parse_step1(<<?TLV_TYPE_OBJECT_INSTANCE:2, IdLength:1, LengthType:2, InlineLength:3, Rest/binary>>) ->
    {Id, Value, Rest2} = parse_step2(id_length_bit_width(IdLength), LengthType, InlineLength, Rest),
    {#{tlv_object_instance => Id, value => parse_loop(Value, [])}, Rest2};
parse_step1(
    <<?TLV_TYPE_RESOURCE_INSTANCE:2, IdLength:1, LengthType:2, InlineLength:3, Rest/binary>>
) ->
    {Id, Value, Rest2} = parse_step2(id_length_bit_width(IdLength), LengthType, InlineLength, Rest),
    {#{tlv_resource_instance => Id, value => Value}, Rest2};
parse_step1(
    <<?TLV_TYPE_MULTIPLE_RESOURCE:2, IdLength:1, LengthType:2, InlineLength:3, Rest/binary>>
) ->
    {Id, Value, Rest2} = parse_step2(id_length_bit_width(IdLength), LengthType, InlineLength, Rest),
    {#{tlv_multiple_resource => Id, value => parse_loop(Value, [])}, Rest2};
parse_step1(
    <<?TLV_TYPE_RESOURCE_WITH_VALUE:2, IdLength:1, LengthType:2, InlineLength:3, Rest/binary>>
) ->
    {Id, Value, Rest2} = parse_step2(id_length_bit_width(IdLength), LengthType, InlineLength, Rest),
    {#{tlv_resource_with_value => Id, value => Value}, Rest2}.

parse_step2(IdLength, ?TLV_NO_LENGTH_FIELD, Length, Data) ->
    <<Id:IdLength, Value:Length/binary, Rest/binary>> = Data,
    {Id, Value, Rest};
parse_step2(IdLength, ?TLV_LEGNTH_8_BIT, _, Data) ->
    <<Id:IdLength, Length:8, Rest/binary>> = Data,
    parse_step3(Id, Length, Rest);
parse_step2(IdLength, ?TLV_LEGNTH_16_BIT, _, Data) ->
    <<Id:IdLength, Length:16, Rest/binary>> = Data,
    parse_step3(Id, Length, Rest);
parse_step2(IdLength, ?TLV_LEGNTH_24_BIT, _, Data) ->
    <<Id:IdLength, Length:24, Rest/binary>> = Data,
    parse_step3(Id, Length, Rest).

parse_step3(Id, Length, Data) ->
    <<Value:Length/binary, Rest/binary>> = Data,
    {Id, Value, Rest}.

id_length_bit_width(0) -> 8;
id_length_bit_width(1) -> 16.

encode(TlvList) ->
    encode(TlvList, <<>>).

encode([], Acc) ->
    Acc;
encode([#{tlv_object_instance := Id, value := Value} | T], Acc) ->
    SubItems = encode(Value, <<>>),
    NewBinary = encode_body(?TLV_TYPE_OBJECT_INSTANCE, Id, SubItems),
    encode(T, <<Acc/binary, NewBinary/binary>>);
encode([#{tlv_resource_instance := Id, value := Value} | T], Acc) ->
    ValBinary = encode_value(Value),
    NewBinary = encode_body(?TLV_TYPE_RESOURCE_INSTANCE, Id, ValBinary),
    encode(T, <<Acc/binary, NewBinary/binary>>);
encode([#{tlv_multiple_resource := Id, value := Value} | T], Acc) ->
    SubItems = encode(Value, <<>>),
    NewBinary = encode_body(?TLV_TYPE_MULTIPLE_RESOURCE, Id, SubItems),
    encode(T, <<Acc/binary, NewBinary/binary>>);
encode([#{tlv_resource_with_value := Id, value := Value} | T], Acc) ->
    ValBinary = encode_value(Value),
    NewBinary = encode_body(?TLV_TYPE_RESOURCE_WITH_VALUE, Id, ValBinary),
    encode(T, <<Acc/binary, NewBinary/binary>>).

encode_body(Type, Id, Value) ->
    Size = byte_size(Value),
    {IdLength, IdBinarySize, IdBinary} =
        if
            Id < 256 -> {0, 1, <<Id:8>>};
            true -> {1, 2, <<Id:16>>}
        end,
    if
        Size < 8 ->
            <<Type:2, IdLength:1, ?TLV_NO_LENGTH_FIELD:2, Size:3, IdBinary:IdBinarySize/binary,
                Value:Size/binary>>;
        Size < 256 ->
            <<Type:2, IdLength:1, ?TLV_LEGNTH_8_BIT:2, 0:3, IdBinary:IdBinarySize/binary, Size:8,
                Value:Size/binary>>;
        Size < 65536 ->
            <<Type:2, IdLength:1, ?TLV_LEGNTH_16_BIT:2, 0:3, IdBinary:IdBinarySize/binary, Size:16,
                Value:Size/binary>>;
        true ->
            <<Type:2, IdLength:1, ?TLV_LEGNTH_24_BIT:2, 0:3, IdBinary:IdBinarySize/binary, Size:24,
                Value:Size/binary>>
    end.

encode_value(Value) when is_binary(Value) ->
    Value;
encode_value(Value) when is_list(Value) ->
    list_to_binary(Value);
encode_value(true) ->
    <<1>>;
encode_value(false) ->
    <<0>>;
encode_value(Value) when is_integer(Value) ->
    if
        Value > -128, Value < 128 -> <<Value>>;
        Value > -32768, Value < 32768 -> <<Value:16>>;
        true -> <<Value:24>>
    end;
encode_value(Value) when is_float(Value) ->
    <<Value:32/float>>;
encode_value(Value) ->
    error(io_lib:format("unsupported format ~p", [Value])).

-ifdef(TEST).
binary_to_hex_string(Data) ->
    lists:flatten([io_lib:format("~2.16.0B ", [X]) || <<X:8>> <= Data]).
-endif.
