%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_pb_wire).
-moduledoc """
Read the fields of an encoded protobuf message at the wire level, without a schema.

Use this module to pick a few fields from a large message without a full decode. The
caller knows the field numbers and decodes the values it needs. Nested messages are
returned as length-delimited binaries, so the caller decides which ones to read.
""".

-export([fold/3]).

-export_type([field_value/0]).

-type field_value() ::
    {varint, 0..18446744073709551615}
    | {len, binary()}
    | {fixed64, <<_:64>>}
    | {fixed32, <<_:32>>}.

-define(WT_VARINT, 0).
-define(WT_I64, 1).
-define(WT_LEN, 2).
-define(WT_I32, 5).
-define(UINT64_MAX, ((1 bsl 64) - 1)).

-doc """
Call `Fun(FieldNumber, Value, Acc)` for each field of the encoded message `Bin`, in
wire order.

A varint is returned as an unsigned 64-bit integer: the caller decodes signed and
zigzag types. A length-delimited value (string, bytes, nested message or packed
repeated field) is returned as a sub-binary of `Bin`: copy it before storing it, so
that it does not keep `Bin` alive.

Raise `{bad_protobuf, Reason}` when `Bin` is not valid protobuf, or when it contains a
group (wire types 3 and 4).
""".
-spec fold(fun((non_neg_integer(), field_value(), Acc) -> Acc), Acc, binary()) -> Acc.
fold(_Fun, Acc, <<>>) ->
    Acc;
fold(Fun, Acc, Bin0) ->
    {Key, Bin1} = read_varint(Bin0),
    {Value, Bin} = read_value(Key band 7, Bin1),
    fold(Fun, Fun(Key bsr 3, Value, Acc), Bin).

read_value(?WT_VARINT, Bin0) ->
    {Value, Bin} = read_varint(Bin0),
    %% The 10th byte of a varint can carry more than 64 bits. Protobuf decoders drop
    %% the extra bits of a field value. Keys and lengths keep them, so that an
    %% oversized length is rejected instead of wrapping to a small one.
    {{varint, Value band ?UINT64_MAX}, Bin};
read_value(?WT_LEN, Bin0) ->
    {Len, Bin1} = read_varint(Bin0),
    case Bin1 of
        <<Value:Len/binary, Bin/binary>> ->
            {{len, Value}, Bin};
        _ ->
            error({bad_protobuf, truncated})
    end;
read_value(?WT_I64, <<Value:8/binary, Bin/binary>>) ->
    {{fixed64, Value}, Bin};
read_value(?WT_I32, <<Value:4/binary, Bin/binary>>) ->
    {{fixed32, Value}, Bin};
read_value(WireType, _Bin) when WireType =:= ?WT_I64; WireType =:= ?WT_I32 ->
    error({bad_protobuf, truncated});
read_value(WireType, _Bin) ->
    error({bad_protobuf, {wire_type, WireType}}).

%% A varint has at most 10 bytes (64 bits in groups of 7 bits).
read_varint(Bin) ->
    read_varint(Bin, 0, 0).

read_varint(<<1:1, X:7, Bin/binary>>, Shift, Acc) when Shift < 63 ->
    read_varint(Bin, Shift + 7, Acc bor (X bsl Shift));
read_varint(<<0:1, X:7, Bin/binary>>, Shift, Acc) ->
    {Acc bor (X bsl Shift), Bin};
read_varint(_Bin, _Shift, _Acc) ->
    error({bad_protobuf, bad_varint}).
