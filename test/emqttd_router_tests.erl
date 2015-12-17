
-module(emqttd_router_tests).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(ROUTER, emqttd_router).

route_test_() ->
    {setup,
     fun()  -> ?ROUTER:init([]) end,
     fun(_) -> ?ROUTER:destory() end,
     [?_test(t_add_routes()),
      ?_test(t_delete_routes()),
      ?_test(t_has_route()),
      ?_test(t_route())
     ]}. 

t_add_routes() ->
    Pid = self(),
    ok.
    %?ROUTER:add_routes([<<"a">>, <<"b">>], Pid),
    %?assertEqual([{<<"a">>, Pid}, {<<"b">>, Pid}], lists:sort(ets:tab2list(route))),
    %?assertEqual([{Pid, <<"a">>}, {Pid, <<"b">>}], lists:sort(ets:tab2list(reverse_route))).

t_delete_routes() ->
    ok.

t_has_route() ->
    ok.

t_route() ->
    ok.

-endif.

