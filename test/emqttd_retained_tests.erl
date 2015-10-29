-module(emqttd_retained_tests).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

retain_test() ->
    mnesia:start(),
    emqttd_retained:mnesia(boot),
    mnesia:stop().

-endif.
