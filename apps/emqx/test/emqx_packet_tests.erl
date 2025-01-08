%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_packet_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(HEX(P), {bin(P), hex}).
-define(HIDDEN(P), {bin(P), hidden}).
-define(TEXT(P), {bin(P), text}).

format_payload_test_() ->
    Hidden = format_payload(hidden),
    Hex = format_payload(hex),
    [
        {"hidden", fun() -> ?assertEqual(?HIDDEN("******"), Hidden(<<>>)) end},
        {"hex empty", fun() -> ?assertEqual(?HEX(<<"">>), Hex(<<"">>)) end},
        {"hex short", fun() -> ?assertEqual(?HEX(<<"303030">>), Hex(<<"000">>)) end},
        {"hex at limit", fun() ->
            Payload = bin(lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE, 0)),
            Expected = binary:encode_hex(bin(lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE, 0))),
            ?assertEqual(?HEX(Expected), Hex(Payload))
        end},
        {"hex long", fun() ->
            Payload = bin(lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE + 2, 0)),
            Prefix = binary:encode_hex(bin(lists:duplicate(?TRUNCATED_PAYLOAD_SIZE, 0))),
            Lost = size(Payload) - ?TRUNCATED_PAYLOAD_SIZE,
            Expected = [Prefix, "...(", integer_to_list(Lost), " bytes)"],
            ?assertEqual(?HEX(Expected), Hex(Payload))
        end}
    ].

format_payload_utf8_test_() ->
    Fmt = format_payload(text),
    [
        {"empty", fun() -> ?assertEqual(?TEXT(<<"">>), Fmt(<<>>)) end},
        {"short ascii", fun() -> ?assertEqual(?TEXT(<<"abc">>), Fmt(<<"abc">>)) end},
        {"short unicode", fun() -> ?assertEqual(?TEXT(<<"日志"/utf8>>), Fmt(<<"日志"/utf8>>)) end},
        {"unicode at limit", fun() ->
            Payload = bin(lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE div 2, <<"¢"/utf8>>)),
            Expected = ["", Payload],
            ?assertEqual(?TEXT(Expected), Fmt(Payload))
        end}
    ].

format_payload_utf8_cutoff_test_() ->
    Fmt = format_payload(text),
    Check = fun(MultiBytesChar) ->
        Prefix = [lists:duplicate(?TRUNCATED_PAYLOAD_SIZE - 1, $a), MultiBytesChar],
        Payload = bin([Prefix, MultiBytesChar, lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE, $b)]),
        Lost = size(Payload) - iolist_size(Prefix),
        Expected = [Prefix, "...(", integer_to_list(Lost), " bytes)"],
        ?assertEqual(?TEXT(Expected), Fmt(Payload))
    end,
    [
        {"utf8 1B", fun() -> Check(<<"x"/utf8>>) end},
        {"utf8 2B", fun() -> Check(<<"¢"/utf8>>) end},
        {"utf8 3B", fun() -> Check(<<"€"/utf8>>) end},
        {"utf8 4B", fun() -> Check(<<"𐍈"/utf8>>) end}
    ].

invalid_utf8_fallback_test() ->
    Fmt = format_payload(text),
    %% trucate after the first byte of a utf8 encoded unicode character
    <<FirstByte:8, _Last3Bytes/binary>> = <<"𐍈"/utf8>>,
    Prefix = iolist_to_binary([lists:duplicate(?TRUNCATED_PAYLOAD_SIZE - 1, $a), FirstByte]),
    %% invalidate utf8 byte sequence, so it should fallback to hex
    InvalidUtf8 = 255,
    Payload = iolist_to_binary([
        Prefix, InvalidUtf8, lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE, $b)
    ]),
    Lost = size(Payload) - iolist_size(Prefix),
    Expected = iolist_to_binary([
        binary:encode_hex(Prefix), "...(", integer_to_list(Lost), " bytes)"
    ]),
    ?assertEqual(?HEX(Expected), Fmt(Payload)),
    ok.

format_payload(Encode) ->
    fun(Payload) ->
        {Payload1, Encode1} = emqx_packet:format_payload(Payload, Encode),
        {bin(Payload1), Encode1}
    end.

bin(X) ->
    unicode:characters_to_binary(X).
