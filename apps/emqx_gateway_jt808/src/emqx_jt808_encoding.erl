%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc String Encoding Helpers for JT/T 808 Gateway (GBK <-> UTF-8)
%%
%% JT/T 808 protocol specifies GBK encoding for STRING type fields.
%% This module provides conversion functions when `string_encoding = gbk`
%% is configured in the gateway.
%%
%% MQTT payloads are always UTF-8 regardless of this setting.

-module(emqx_jt808_encoding).

-include_lib("emqx/include/logger.hrl").

-export([
    maybe_decode_string/3,
    maybe_encode_string/3
]).

%% @doc Decode string from wire format (GBK) to internal format (UTF-8)
%% when string_encoding=gbk is configured
-spec maybe_decode_string(binary(), atom() | binary(), map()) -> binary().
maybe_decode_string(Binary, FieldName, #{string_encoding := gbk}) ->
    {ok, Utf8} = egbk:to_utf8(Binary),
    case has_replacement_char(Utf8) of
        true ->
            ?SLOG(warning, #{
                msg => gbk_decode_replacement,
                field => FieldName,
                original_bytes => binary_to_hex(Binary),
                direction => parsing
            });
        false ->
            ok
    end,
    Utf8;
maybe_decode_string(Binary, _FieldName, #{string_encoding := utf8}) ->
    Binary;
maybe_decode_string(Binary, _FieldName, #{}) ->
    %% Default to utf8 (no conversion)
    Binary.

%% @doc Encode string from internal format (UTF-8) to wire format (GBK)
%% when string_encoding=gbk is configured
-spec maybe_encode_string(binary(), atom() | binary(), map()) -> binary().
maybe_encode_string(Binary, FieldName, #{string_encoding := gbk}) ->
    {ok, Gbk} = egbk:to_gbk(Binary),
    case has_gbk_replacement_char(Binary, Gbk) of
        true ->
            ?SLOG(warning, #{
                msg => gbk_encode_replacement,
                field => FieldName,
                original_string => Binary,
                direction => serializing
            });
        false ->
            ok
    end,
    Gbk;
maybe_encode_string(Binary, _FieldName, #{string_encoding := utf8}) ->
    Binary;
maybe_encode_string(Binary, _FieldName, #{}) ->
    %% Default to utf8 (no conversion)
    Binary.

%% Check for UTF-8 replacement character U+FFFD (0xEF 0xBF 0xBD)
has_replacement_char(Binary) ->
    case binary:match(Binary, <<16#EF, 16#BF, 16#BD>>) of
        nomatch -> false;
        _ -> true
    end.

%% Check if GBK output contains replacement chars that weren't in input
%% Replacement char is 0x3F (?)
has_gbk_replacement_char(OrigUtf8, GbkOutput) ->
    %% Count ? in original vs output to detect replacements
    OrigCount = count_byte(OrigUtf8, 16#3F),
    OutputCount = count_byte(GbkOutput, 16#3F),
    OutputCount > OrigCount.

count_byte(Binary, Byte) ->
    count_byte(Binary, Byte, 0).

count_byte(<<>>, _Byte, Count) ->
    Count;
count_byte(<<B, Rest/binary>>, Byte, Count) when B =:= Byte ->
    count_byte(Rest, Byte, Count + 1);
count_byte(<<_, Rest/binary>>, Byte, Count) ->
    count_byte(Rest, Byte, Count).

binary_to_hex(Binary) ->
    lists:flatten([io_lib:format("~2.16.0B", [B]) || <<B>> <= Binary]).
