%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_utils_cursor_tests).

-include_lib("eunit/include/eunit.hrl").

-define(B64(BIN), base64:encode(BIN, #{mode => urlsafe, padding => false})).

json_roundtrip_test() ->
    Map = #{<<"a">> => 1, <<"b">> => <<"x">>, <<"c">> => #{<<"d">> => [1, 2]}},
    Cursor = emqx_utils_cursor:encode_json(Map),
    ?assertEqual({ok, Map}, emqx_utils_cursor:decode_json(Cursor)).

json_bad_input_test_() ->
    [
        ?_assertEqual({error, bad_cursor}, emqx_utils_cursor:decode_json(Input))
     || Input <- [
            <<"not base64 !@#">>,
            ?B64(<<"not json">>),
            ?B64(<<"[1,2]">>),
            ?B64(<<"1">>),
            ?B64(term_to_binary(#{<<"a">> => 1})),
            undefined,
            123
        ]
    ].

term_roundtrip_test() ->
    Term = [1, 2, #{1 => {cont, make_ref(), self()}, 2 => <<"bin">>}],
    Cursor = emqx_utils_cursor:encode_term(test_domain, Term),
    ?assertEqual({ok, Term}, emqx_utils_cursor:decode_term(test_domain, Cursor)).

term_wrong_domain_test() ->
    Cursor = emqx_utils_cursor:encode_term(domain_a, [1, 2, 3]),
    ?assertEqual({error, bad_cursor}, emqx_utils_cursor:decode_term(domain_b, Cursor)).

term_tampered_test() ->
    Cursor = emqx_utils_cursor:encode_term(test_domain, [1, 2, 3]),
    <<Mac:32/binary, Payload/binary>> = base64:decode(Cursor, #{mode => urlsafe, padding => false}),
    <<_:8, MacRest/binary>> = Mac,
    BadMac = ?B64(<<0, MacRest/binary, Payload/binary>>),
    BadPayload = ?B64(<<Mac/binary, (term_to_binary([1, 2, 4]))/binary>>),
    ?assertEqual({error, bad_cursor}, emqx_utils_cursor:decode_term(test_domain, BadMac)),
    ?assertEqual({error, bad_cursor}, emqx_utils_cursor:decode_term(test_domain, BadPayload)).

term_unsigned_input_test_() ->
    Term = term_to_binary([1, 1, #{1 => 1, 2 => undefined}]),
    [
        ?_assertEqual({error, bad_cursor}, emqx_utils_cursor:decode_term(test_domain, Input))
     || Input <- [
            ?B64(Term),
            ?B64(<<0:256, Term/binary>>),
            ?B64(<<"short">>),
            <<"not base64 !@#">>,
            <<>>,
            undefined
        ]
    ].
