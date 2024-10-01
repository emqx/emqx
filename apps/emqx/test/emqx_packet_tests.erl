%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

format_payload_test_() ->
    Hidden = fun(Payload) -> emqx_packet:format_payload(Payload, hidden) end,
    Hex = fun(Payload) -> bin(emqx_packet:format_payload(Payload, hex)) end,
    [
        {"hidden", fun() -> ?assertEqual("******", Hidden(<<>>)) end},
        {"hex empty", fun() -> ?assertEqual(<<"">>, Hex(<<"">>)) end},
        {"hex short", fun() -> ?assertEqual(<<"hex:303030">>, Hex(<<"000">>)) end},
        {"hex at limit", fun() ->
            Payload = bin(lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE, 0)),
            Expected = bin(
                [
                    "hex:",
                    binary:encode_hex(bin(lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE, 0)))
                ]
            ),
            ?assertEqual(Expected, Hex(Payload))
        end},
        {"hex long", fun() ->
            Payload = bin(lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE + 2, 0)),
            Prefix = binary:encode_hex(bin(lists:duplicate(?TRUNCATED_PAYLOAD_SIZE, 0))),
            Lost = size(Payload) - ?TRUNCATED_PAYLOAD_SIZE,
            Expected = bin(["hex:", Prefix, "...(", integer_to_list(Lost), " bytes)"]),
            ?assertEqual(Expected, Hex(Payload))
        end}
    ].

format_payload_utf8_test_() ->
    Fmt = fun(P) -> bin(emqx_packet:format_payload(P, text)) end,
    [
        {"empty", fun() -> ?assertEqual(<<"">>, Fmt(<<>>)) end},
        {"short ascii", fun() -> ?assertEqual(<<"abc">>, Fmt(<<"abc">>)) end},
        {"short unicode", fun() -> ?assertEqual(<<"日志"/utf8>>, Fmt(<<"日志"/utf8>>)) end},
        {"unicode at limit", fun() ->
            Payload = bin(lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE div 2, <<"¢"/utf8>>)),
            Expected = bin(["", Payload]),
            ?assertEqual(Expected, Fmt(Payload))
        end}
    ].

format_payload_utf8_cutoff_test_() ->
    Fmt = fun(P) -> bin(emqx_packet:format_payload(P, text)) end,
    Check = fun(MultiBytesChar) ->
        Prefix = [lists:duplicate(?TRUNCATED_PAYLOAD_SIZE - 1, $a), MultiBytesChar],
        Payload = bin([Prefix, MultiBytesChar, lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE, $b)]),
        Lost = size(Payload) - iolist_size(Prefix),
        Expected = bin([Prefix, "...(", integer_to_list(Lost), " bytes)"]),
        ?assertEqual(Expected, Fmt(Payload))
    end,
    [
        {"utf8 1B", fun() -> Check(<<"x"/utf8>>) end},
        {"utf8 2B", fun() -> Check(<<"¢"/utf8>>) end},
        {"utf8 3B", fun() -> Check(<<"€"/utf8>>) end},
        {"utf8 4B", fun() -> Check(<<"𐍈"/utf8>>) end}
    ].

invalid_utf8_fallback_test() ->
    %% trucate after the first byte of a utf8 encoded unicode character
    <<FirstByte:8, Last3Bytes/binary>> = <<"𐍈"/utf8>>,
    Prefix = iolist_to_binary([lists:duplicate(?TRUNCATED_PAYLOAD_SIZE - 1, $a), FirstByte]),
    %% invalidate utf8 byte sequence, so it should fallback to hex
    InvalidUtf8 = 255,
    Payload = iolist_to_binary([
        Prefix, Last3Bytes, InvalidUtf8, lists:duplicate(?MAX_PAYLOAD_FORMAT_SIZE, $b)
    ]),
    Lost = size(Payload) - iolist_size(Prefix),
    Expected = iolist_to_binary([
        "hex:", binary:encode_hex(Prefix), "...(", integer_to_list(Lost), " bytes)"
    ]),
    ?assertEqual(Expected, bin(emqx_packet:format_payload(Payload, text))),
    ok.

bin(X) ->
    unicode:characters_to_binary(X).
