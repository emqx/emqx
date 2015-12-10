-module(emqttd_retainer_tests).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

retain_test() ->
    mnesia:start(),
    emqttd_retainer:mnesia(boot),
    mnesia:stop().

-endif.
