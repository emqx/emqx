%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_nkey).

-export([decode_public/1, decode_public_any/1, encode_public/1, normalize/1, verify_signature/3]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(NKEY_ACCOUNT_PREFIX, 16#00).
-define(NKEY_OPERATOR_PREFIX, 16#70).
-define(NKEY_USER_PREFIX, 16#A0).
-define(NKEY_PUBLIC_RAW_SIZE, 35).
-define(NKEY_PUBLIC_ENCODED_SIZE, 56).
-define(BASE32_ALPHABET, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567").

decode_public(NKey0) ->
    decode_public_with_allowed_prefixes(NKey0, [?NKEY_USER_PREFIX]).

decode_public_any(NKey0) ->
    decode_public_with_allowed_prefixes(
        NKey0,
        [?NKEY_ACCOUNT_PREFIX, ?NKEY_OPERATOR_PREFIX, ?NKEY_USER_PREFIX]
    ).

verify_signature(NKey, Sig0, Nonce) ->
    case decode_public(NKey) of
        {ok, PubKey} ->
            case decode_sig(Sig0) of
                {ok, Sig} ->
                    case crypto:verify(eddsa, none, Nonce, Sig, [PubKey, ed25519]) of
                        true -> {ok, PubKey};
                        false -> {error, invalid_nkey_sig}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

encode_public(PubKey) when is_binary(PubKey), byte_size(PubKey) =:= 32 ->
    Prefix = <<?NKEY_USER_PREFIX>>,
    Payload = <<Prefix/binary, PubKey/binary>>,
    Crc = crc16_xmodem(Payload),
    Raw = <<Payload/binary, Crc:16/little-unsigned>>,
    base32_encode(Raw).

normalize(Value) when is_binary(Value) ->
    emqx_utils_conv:bin(string:uppercase(binary_to_list(Value)));
normalize(Value) ->
    normalize(emqx_utils_conv:bin(Value)).

decode_public_with_allowed_prefixes(NKey0, AllowedPrefixes) ->
    NKey = normalize(NKey0),
    case nkey_prefix_char(NKey) of
        {ok, PrefixChar} ->
            case prefix_from_char(PrefixChar) of
                {ok, Prefix} ->
                    case lists:member(Prefix, AllowedPrefixes) of
                        true ->
                            decode_public_with_prefix(NKey, Prefix);
                        false ->
                            {error, invalid_nkey_prefix}
                    end;
                _ ->
                    {error, invalid_nkey_prefix}
            end;
        error ->
            {error, invalid_nkey_prefix}
    end.

nkey_prefix_char(<<Prefix, _/binary>>) ->
    {ok, Prefix};
nkey_prefix_char(_) ->
    error.

prefix_from_char($A) ->
    {ok, ?NKEY_ACCOUNT_PREFIX};
prefix_from_char($O) ->
    {ok, ?NKEY_OPERATOR_PREFIX};
prefix_from_char($U) ->
    {ok, ?NKEY_USER_PREFIX};
prefix_from_char(_) ->
    error.

decode_public_with_prefix(NKey, PrefixByte) ->
    case base32_decode(NKey) of
        {ok, Raw} when
            byte_size(Raw) =:= ?NKEY_PUBLIC_RAW_SIZE,
            byte_size(NKey) =:= ?NKEY_PUBLIC_ENCODED_SIZE
        ->
            <<Prefix:8, PubKey:32/binary, Crc:16/little-unsigned>> = Raw,
            case Prefix =:= PrefixByte of
                false ->
                    {error, invalid_nkey_prefix};
                true ->
                    case crc16_xmodem(<<Prefix, PubKey/binary>>) of
                        Crc -> {ok, PubKey};
                        _ -> {error, invalid_nkey_crc}
                    end
            end;
        {ok, _Raw} ->
            {error, invalid_nkey_size};
        {error, Reason} ->
            {error, Reason}
    end.

decode_sig(Sig0) ->
    Sig = emqx_utils_conv:bin(Sig0),
    case base64_decode(Sig, #{mode => urlsafe, padding => false}) of
        {ok, Bin} ->
            {ok, Bin};
        error ->
            case base64_decode(Sig, #{}) of
                {ok, Bin} -> {ok, Bin};
                error -> {error, invalid_nkey_sig_format}
            end
    end.

base64_decode(Sig, Opts) ->
    try base64:decode(Sig, Opts) of
        Bin -> {ok, Bin}
    catch
        _:_ -> error
    end.

base32_decode(Bin) ->
    Chars = string:uppercase(binary_to_list(Bin)),
    case base32_bits(Chars, <<>>) of
        {ok, Bits} ->
            BytesSize = (bit_size(Bits) div 8) * 8,
            <<Out:BytesSize/bits, _/bits>> = Bits,
            {ok, <<Out:BytesSize/bits>>};
        {error, Reason} ->
            {error, Reason}
    end.

base32_bits([], Acc) ->
    {ok, Acc};
base32_bits([Char | Rest], Acc) ->
    case base32_val(Char) of
        {ok, Val} ->
            base32_bits(Rest, <<Acc/bits, Val:5>>);
        {error, Reason} ->
            {error, Reason}
    end.

base32_val(Char) when Char >= $A, Char =< $Z ->
    {ok, Char - $A};
base32_val(Char) when Char >= $2, Char =< $7 ->
    {ok, 26 + (Char - $2)};
base32_val(_) ->
    {error, invalid_base32_char}.

base32_encode(Bin) ->
    base32_encode_bits(Bin, []).

base32_encode_bits(<<>>, Acc) ->
    list_to_binary(lists:reverse(Acc));
base32_encode_bits(Bits, Acc) when bit_size(Bits) >= 5 ->
    <<Val:5, Rest/bits>> = Bits,
    base32_encode_bits(Rest, [base32_char(Val) | Acc]);
base32_encode_bits(Bits, Acc) ->
    Size = bit_size(Bits),
    PadSize = 5 - Size,
    <<Val:Size>> = Bits,
    Padded = Val bsl PadSize,
    base32_encode_bits(<<>>, [base32_char(Padded) | Acc]).

base32_char(Val) when Val >= 0, Val < 32 ->
    lists:nth(Val + 1, ?BASE32_ALPHABET).

crc16_xmodem(Bin) ->
    crc16_xmodem_bin(Bin, 0).

crc16_xmodem_bin(<<>>, Crc) ->
    Crc band 16#FFFF;
crc16_xmodem_bin(<<Byte:8, Rest/binary>>, Crc) ->
    crc16_xmodem_bin(Rest, crc16_xmodem_byte(Crc, Byte)).

crc16_xmodem_byte(Crc0, Byte) ->
    Crc1 = Crc0 bxor (Byte bsl 8),
    crc16_xmodem_bits(Crc1, 8).

crc16_xmodem_bits(Crc, 0) ->
    Crc band 16#FFFF;
crc16_xmodem_bits(Crc, N) ->
    Crc1 =
        case (Crc band 16#8000) =:= 0 of
            true -> Crc bsl 1;
            false -> (Crc bsl 1) bxor 16#1021
        end,
    crc16_xmodem_bits(Crc1 band 16#FFFF, N - 1).
