%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_frame_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_frame_encode_decode_options(_) ->
    Options = #{
        if_match => [<<"a">>],
        uri_host => <<"example.com">>,
        etag => [<<"tag">>],
        if_none_match => true,
        uri_port => 5683,
        location_path => [<<"loc">>, undefined],
        uri_path => [<<"path">>, <<"sub">>],
        content_format => <<"application/json">>,
        max_age => 61,
        uri_query => [<<"a=1">>, <<"b=2">>],
        'accept' => 0,
        location_query => [<<"x=1">>],
        proxy_uri => <<"coap://example.com">>,
        proxy_scheme => <<"coap">>,
        size1 => 10,
        observe => 1,
        block1 => {0, true, 16},
        block2 => {1, false, 32}
    },
    Msg = #coap_message{
        type = con,
        method = get,
        id = 1,
        token = <<>>,
        options = Options,
        payload = <<"p">>
    },
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Bin, #{}),
    Opts = Decoded#coap_message.options,
    ?assertEqual([<<"a">>], maps:get(if_match, Opts)),
    ?assertEqual(<<"example.com">>, maps:get(uri_host, Opts)),
    ?assertEqual([<<"path">>, <<"sub">>], maps:get(uri_path, Opts)),
    ?assertEqual(#{<<"a">> => <<"1">>, <<"b">> => <<"2">>}, maps:get(uri_query, Opts)),
    ok.

t_frame_encode_extended_values(_) ->
    LongVal = binary:copy(<<"a">>, 300),
    Msg1 = #coap_message{
        type = con,
        method = get,
        id = 10,
        options = #{uri_query => #{<<"k">> => LongVal}},
        payload = <<>>
    },
    _ = emqx_coap_frame:serialize_pkt(Msg1, emqx_coap_frame:serialize_opts()),
    Msg2 = #coap_message{
        type = con,
        method = get,
        id = 11,
        options = #{if_match => [<<"a">>], size1 => 10},
        payload = <<>>
    },
    _ = emqx_coap_frame:serialize_pkt(Msg2, emqx_coap_frame:serialize_opts()),
    Msg3 = #coap_message{
        type = con,
        method = get,
        id = 12,
        options = #{if_match => [<<"a">>], 300 => <<1>>},
        payload = <<>>
    },
    _ = emqx_coap_frame:serialize_pkt(Msg3, emqx_coap_frame:serialize_opts()),
    Types = [
        <<"application/link-format">>,
        <<"application/xml">>,
        <<"application/exi">>,
        <<"application/json">>,
        <<"application/cbor">>,
        <<"application/vnd.oma.lwm2m+tlv">>,
        <<"application/vnd.oma.lwm2m+json">>,
        <<"application/unknown">>
    ],
    lists:foreach(
        fun(Type) ->
            Msg = #coap_message{
                type = con,
                method = get,
                id = 20,
                options = #{content_format => Type},
                payload = <<>>
            },
            _ = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts())
        end,
        Types
    ),
    _ = emqx_coap_frame:serialize_pkt(
        #coap_message{type = con, method = {ok, valid}, id = 21},
        emqx_coap_frame:serialize_opts()
    ),
    _ = emqx_coap_frame:serialize_pkt(
        #coap_message{type = con, method = {ok, continue}, id = 22},
        emqx_coap_frame:serialize_opts()
    ),
    Methods = [
        put,
        delete,
        {error, bad_option},
        {error, forbidden},
        {error, not_found},
        {error, method_not_allowed},
        {error, not_acceptable},
        {error, request_entity_incomplete},
        {error, precondition_failed},
        {error, request_entity_too_large},
        {error, unsupported_content_format},
        {error, internal_server_error},
        {error, not_implemented},
        {error, bad_gateway},
        {error, service_unavailable}
    ],
    lists:foreach(
        fun(Method) ->
            _ = emqx_coap_frame:serialize_pkt(
                #coap_message{type = con, method = Method, id = 23},
                emqx_coap_frame:serialize_opts()
            )
        end,
        Methods
    ),
    ?assertThrow(
        {bad_method, bad_method},
        emqx_coap_frame:serialize_pkt(
            #coap_message{type = con, method = bad_method, id = 23},
            emqx_coap_frame:serialize_opts()
        )
    ),
    ?assertThrow(
        {bad_option, bad_opt, <<"x">>},
        emqx_coap_frame:serialize_pkt(
            #coap_message{type = con, method = get, id = 24, options = #{bad_opt => <<"x">>}},
            emqx_coap_frame:serialize_opts()
        )
    ),
    ?assertThrow(
        {bad_option, 300, 1},
        emqx_coap_frame:serialize_pkt(
            #coap_message{type = con, method = get, id = 124, options = #{300 => 1}},
            emqx_coap_frame:serialize_opts()
        )
    ),
    MsgInt = #coap_message{
        type = con,
        method = get,
        id = 25,
        options = #{content_format => 11542},
        payload = <<>>
    },
    BinInt = emqx_coap_frame:serialize_pkt(MsgInt, emqx_coap_frame:serialize_opts()),
    {ok, DecodedInt, <<>>, _} = emqx_coap_frame:parse(BinInt, #{}),
    ?assertEqual(
        <<"application/vnd.oma.lwm2m+tlv">>,
        maps:get(content_format, DecodedInt#coap_message.options)
    ),
    MsgInt2 = #coap_message{
        type = con,
        method = get,
        id = 26,
        options = #{content_format => 11543},
        payload = <<>>
    },
    BinInt2 = emqx_coap_frame:serialize_pkt(MsgInt2, emqx_coap_frame:serialize_opts()),
    {ok, DecodedInt2, <<>>, _} = emqx_coap_frame:parse(BinInt2, #{}),
    ?assertEqual(
        <<"application/vnd.oma.lwm2m+json">>,
        maps:get(content_format, DecodedInt2#coap_message.options)
    ),
    _ = emqx_coap_frame:serialize_pkt(
        #coap_message{type = con, method = {error, gateway_timeout}, id = 27},
        emqx_coap_frame:serialize_opts()
    ),
    _ = emqx_coap_frame:serialize_pkt(
        #coap_message{type = con, method = {error, proxying_not_supported}, id = 28},
        emqx_coap_frame:serialize_opts()
    ),
    DecodeCodes = [
        {40, <<"application/link-format">>},
        {41, <<"application/xml">>},
        {47, <<"application/exi">>},
        {60, <<"application/cbor">>},
        {9999, <<"application/octet-stream">>}
    ],
    lists:foreach(
        fun({Code, Expected}) ->
            Msg = #coap_message{
                type = con,
                method = get,
                id = 30,
                options = #{content_format => Code},
                payload = <<>>
            },
            Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
            {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Bin, #{}),
            ?assertEqual(Expected, maps:get(content_format, Decoded#coap_message.options))
        end,
        DecodeCodes
    ),
    ok.

t_frame_empty_reset_and_undefined_option(_) ->
    Msg0 = #coap_message{type = reset, method = undefined, id = 42},
    Bin0 = emqx_coap_frame:serialize_pkt(Msg0, emqx_coap_frame:serialize_opts()),
    {ok, Decoded0, <<>>, _} = emqx_coap_frame:parse(Bin0, #{}),
    ?assertEqual(reset, Decoded0#coap_message.type),
    ?assertEqual(42, Decoded0#coap_message.id),
    ?assertEqual(undefined, Decoded0#coap_message.method),
    Msg1 = #coap_message{
        type = con,
        method = get,
        id = 43,
        options = #{uri_host => undefined},
        payload = <<>>
    },
    _ = emqx_coap_frame:serialize_pkt(Msg1, emqx_coap_frame:serialize_opts()),
    ok.

t_frame_decode_extended_values(_) ->
    OptVal13 = binary:copy(<<"x">>, 13),
    Header13 = <<1:2, 0:2, 0:4, 0:3, 1:5, 2:16>>,
    Opt13 = <<13:4, 13:4, 13:8, 0:8, OptVal13/binary>>,
    Packet13 = <<Header13/binary, Opt13/binary>>,
    {ok, Decoded13, <<>>, _} = emqx_coap_frame:parse(Packet13, #{}),
    Opts13 = Decoded13#coap_message.options,
    ?assertEqual(false, maps:is_key(26, Opts13)),
    OptVal = binary:copy(<<"z">>, 269),
    Header = <<1:2, 0:2, 0:4, 0:3, 1:5, 1:16>>,
    Opt = <<14:4, 14:4, 1:16, 0:16, OptVal/binary>>,
    Packet = <<Header/binary, Opt/binary>>,
    {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    Opts = Decoded#coap_message.options,
    ?assertEqual(false, maps:is_key(270, Opts)),
    ok.

t_frame_parse_codes(_) ->
    ResetPacket = <<1:2, 3:2, 0:4, 0:3, 0:5, 10:16>>,
    {ok, ResetMsg, <<>>, _} = emqx_coap_frame:parse(ResetPacket, #{}),
    ?assertEqual(reset, ResetMsg#coap_message.type),
    Codes = [
        {2, 2, {ok, deleted}},
        {2, 3, {ok, valid}},
        {2, 4, {ok, changed}},
        {2, 7, {ok, nocontent}},
        {2, 31, {ok, continue}},
        {4, 0, {error, bad_request}},
        {4, 1, {error, unauthorized}},
        {4, 2, {error, bad_option}},
        {4, 3, {error, forbidden}},
        {4, 4, {error, not_found}},
        {4, 5, {error, method_not_allowed}},
        {4, 6, {error, not_acceptable}},
        {4, 8, {error, request_entity_incomplete}},
        {4, 12, {error, precondition_failed}},
        {4, 13, {error, request_entity_too_large}},
        {4, 15, {error, unsupported_content_format}},
        {5, 0, {error, internal_server_error}},
        {5, 1, {error, not_implemented}},
        {5, 2, {error, bad_gateway}},
        {5, 3, {error, service_unavailable}},
        {5, 4, {error, gateway_timeout}},
        {5, 5, {error, proxying_not_supported}}
    ],
    lists:foreach(
        fun({Class, Code, Expected}) ->
            Packet = <<1:2, 0:2, 0:4, Class:3, Code:5, 11:16>>,
            {ok, Msg, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
            ?assertEqual(Expected, Msg#coap_message.method)
        end,
        Codes
    ),
    ok.

t_frame_parse_incomplete_more(_) ->
    ParseState = #{},
    ?assertEqual({more, ParseState}, emqx_coap_frame:parse(<<1>>, ParseState)),
    ok.

t_frame_empty_message_with_data(_) ->
    Packet = <<1:2, 0:2, 0:4, 0:3, 0:5, 120:16, 16#FF>>,
    {ok, {coap_format_error, con, 120, _}, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_truncated_token(_) ->
    Packet = <<1:2, 0:2, 1:4, 0:3, 1:5, 121:16>>,
    {ok, {coap_format_error, con, 121, _}, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_reserved_class_format_error(_) ->
    Packet = <<1:2, 0:2, 0:4, 1:3, 0:5, 12:16>>,
    {ok, {coap_format_error, con, 12, _}, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_query_and_truncated_option(_) ->
    QueryMsg = #coap_message{
        type = con,
        method = get,
        id = 30,
        options = #{uri_query => [<<"plain">>]}
    },
    QueryBin = emqx_coap_frame:serialize_pkt(QueryMsg, emqx_coap_frame:serialize_opts()),
    {ok, QueryDecoded, <<>>, _} = emqx_coap_frame:parse(QueryBin, #{}),
    ?assertEqual(#{}, maps:get(uri_query, QueryDecoded#coap_message.options)),
    Header = <<1:2, 0:2, 0:4, 0:3, 1:5, 31:16>>,
    Opt = <<0:4, 1:4>>,
    Packet = <<Header/binary, Opt/binary>>,
    {ok, {coap_format_error, con, 31, _}, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_unknown_version_ignore(_) ->
    Packet = <<2:2, 0:2, 0:4, 0:3, 1:5, 99:16>>,
    {ok, {coap_ignore, _}, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_invalid_tkl(_) ->
    Packet = <<1:2, 0:2, 9:4, 0:3, 1:5, 100:16>>,
    {ok, {coap_format_error, con, 100, _}, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_payload_marker_empty_error(_) ->
    Packet = <<1:2, 0:2, 0:4, 0:3, 1:5, 101:16, 16#FF>>,
    {ok, {coap_format_error, con, 101, _}, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_option_delta_len_reserved(_) ->
    Header = <<1:2, 0:2, 0:4, 0:3, 1:5, 102:16>>,
    Delta15 = <<15:4, 0:4>>,
    Len15 = <<0:4, 15:4>>,
    {ok, {coap_format_error, con, 102, _}, <<>>, _} =
        emqx_coap_frame:parse(<<Header/binary, Delta15/binary>>, #{}),
    {ok, {coap_format_error, con, 102, _}, <<>>, _} =
        emqx_coap_frame:parse(<<Header/binary, Len15/binary>>, #{}),
    ok.

t_frame_option_ext_delta_truncated(_) ->
    Header = <<1:2, 0:2, 0:4, 0:3, 1:5, 113:16>>,
    Delta13 = <<13:4, 0:4>>,
    Delta14 = <<14:4, 0:4>>,
    {ok, {coap_format_error, con, 113, _}, <<>>, _} =
        emqx_coap_frame:parse(<<Header/binary, Delta13/binary>>, #{}),
    {ok, {coap_format_error, con, 113, _}, <<>>, _} =
        emqx_coap_frame:parse(<<Header/binary, Delta14/binary>>, #{}),
    ok.

t_frame_option_ext_len_truncated(_) ->
    Header = <<1:2, 0:2, 0:4, 0:3, 1:5, 114:16>>,
    Len13 = <<0:4, 13:4>>,
    {ok, {coap_format_error, con, 114, _}, <<>>, _} =
        emqx_coap_frame:parse(<<Header/binary, Len13/binary>>, #{}),
    ok.

t_frame_unknown_critical_option(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 103,
        token = <<"t">>,
        options = #{uri_path => [<<"ps">>], 9 => <<1>>}
    },
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {ok, {coap_request_error, #coap_message{id = 103}, {error, bad_option}}, <<>>, _} =
        emqx_coap_frame:parse(Bin, #{}),
    ok.

t_frame_unknown_elective_option(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 111,
        options = #{10 => <<1>>}
    },
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Bin, #{}),
    ?assertEqual(false, maps:is_key(10, Decoded#coap_message.options)),
    ok.

t_frame_duplicate_critical_option(_) ->
    Header = <<1:2, 0:2, 0:4, 0:3, 1:5, 104:16>>,
    Opt1 = <<3:4, 1:4, "a">>,
    Opt2 = <<0:4, 1:4, "b">>,
    Packet = <<Header/binary, Opt1/binary, Opt2/binary>>,
    {ok, {coap_request_error, #coap_message{id = 104}, {error, bad_option}}, <<>>, _} =
        emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_duplicate_elective_option(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 105,
        options = #{content_format => 0, 12 => <<0>>}
    },
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Bin, #{}),
    ?assertEqual(<<"text/plain">>, maps:get(content_format, Decoded#coap_message.options)),
    ok.

t_frame_invalid_if_none_match(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 106,
        options = #{5 => <<1>>}
    },
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {ok, {coap_request_error, #coap_message{id = 106}, {error, bad_option}}, <<>>, _} =
        emqx_coap_frame:parse(Bin, #{}),
    ok.

t_frame_invalid_block_szx(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 107,
        options = #{23 => <<7>>}
    },
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {ok, {coap_request_error, #coap_message{id = 107}, {error, bad_request}}, <<>>, _} =
        emqx_coap_frame:parse(Bin, #{}),
    ok.

t_frame_block_option_empty(_) ->
    Header = <<1:2, 0:2, 0:4, 0:3, 1:5, 115:16>>,
    Opt = <<13:4, 0:4, 14>>,
    Packet = <<Header/binary, Opt/binary>>,
    {ok, Msg, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ?assertEqual({0, false, 16}, maps:get(block1, Msg#coap_message.options)),
    ok.

t_frame_block_option_invalid_len(_) ->
    Header = <<1:2, 0:2, 0:4, 0:3, 1:5, 116:16>>,
    Opt = <<13:4, 5:4, 14, 0:40>>,
    Packet = <<Header/binary, Opt/binary>>,
    {ok, {coap_request_error, #coap_message{id = 116}, {error, bad_request}}, <<>>, _} =
        emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_unknown_method_code(_) ->
    Packet = <<1:2, 0:2, 1:4, 0:3, 31:5, 108:16, 1>>,
    {ok, {coap_request_error, #coap_message{id = 108}, {error, method_not_allowed}}, <<>>, _} =
        emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_format_error_on_response_option(_) ->
    Header = <<1:2, 0:2, 0:4, 2:3, 5:5, 117:16>>,
    Delta15 = <<15:4, 0:4>>,
    Packet = <<Header/binary, Delta15/binary>>,
    {ok, {coap_format_error, con, 117, _}, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ok.

t_frame_unknown_response_code_ignore(_) ->
    Packet = <<1:2, 0:2, 0:4, 2:3, 30:5, 112:16>>,
    {ok, {coap_ignore, {unknown_response, 2, 30}}, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ok.

t_serialize_invalid_token_length(_) ->
    Msg = #coap_message{type = con, method = get, id = 109, token = <<0:72>>},
    ?assertThrow(
        {bad_token, token_too_long},
        emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts())
    ),
    ok.

t_serialize_invalid_block_size(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 110,
        options = #{block2 => {0, false, 2048}}
    },
    ?assertThrow(
        {bad_block, invalid_size},
        emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts())
    ),
    ok.

t_serialize_invalid_block_size_type(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 111,
        options = #{block2 => {0, false, <<"bad">>}}
    },
    ?assertThrow(
        {bad_block, invalid_size},
        emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts())
    ),
    ok.

t_frame_block_roundtrip(_) ->
    Cases = [
        {block1, {0, true, 16}, 40},
        {block1, {16, false, 16}, 41},
        {block2, {4096, true, 16}, 42}
    ],
    lists:foreach(
        fun({Opt, Val, Id}) ->
            Msg = #coap_message{type = con, method = get, id = Id, options = #{Opt => Val}},
            Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
            {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Bin, #{}),
            ?assertEqual(Val, maps:get(Opt, Decoded#coap_message.options))
        end,
        Cases
    ),
    ok.

t_frame_misc_helpers(_) ->
    Msg = #coap_message{type = con, method = get, id = 50},
    _ = emqx_coap_frame:format(Msg),
    ?assert(emqx_coap_frame:is_message(Msg)),
    ?assertEqual(false, emqx_coap_frame:is_message(#{bad => msg})),
    ok.

t_frame_type_error_variants(_) ->
    ?assertEqual(undefined, emqx_coap_frame:type({coap_ignore, ignore})),
    ?assertEqual(undefined, emqx_coap_frame:type({coap_format_error, con, 1, bad})),
    ?assertEqual(
        undefined,
        emqx_coap_frame:type({coap_request_error, #coap_message{id = 1}, {error, bad_option}})
    ),
    ?assertEqual(coap, emqx_coap_frame:type(#{bad => msg})),
    ok.
