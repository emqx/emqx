-module(emqx_lwm2m_message_tests).

-include_lib("eunit/include/eunit.hrl").

-import(emqx_lwm2m_message, [bits/1]).

bits_pos_test() ->
    ?assertEqual(8, bits(0)),
    ?assertEqual(8, bits(1)),
    ?assertEqual(8, bits(127)),
    ?assertEqual(16, bits(128)),
    ?assertEqual(16, bits(129)),
    ok.

bits_neg_test() ->
    ?assertEqual(8, bits(-1)),
    ?assertEqual(8, bits(-2)),
    ?assertEqual(8, bits(-127)),
    ?assertEqual(8, bits(-128)),
    ?assertEqual(16, bits(-129)),
    ok.
