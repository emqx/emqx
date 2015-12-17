-module(emqttd_mod_subscription_tests).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(M, emqttd_mod_subscription).

rep_test() ->
    ?assertEqual(<<"topic/clientId">>, ?M:rep(<<"$c">>, <<"clientId">>, <<"topic/$c">>)),
    ?assertEqual(<<"topic/username">>, ?M:rep(<<"$u">>, <<"username">>, <<"topic/$u">>)),
    ?assertEqual(<<"topic/username/clientId">>,
                 ?M:rep(<<"$c">>, <<"clientId">>,
                       ?M:rep(<<"$u">>, <<"username">>, <<"topic/$u/$c">>))).

-endif.
