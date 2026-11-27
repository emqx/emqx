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
