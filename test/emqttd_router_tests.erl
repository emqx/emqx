%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2016, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------

-module(emqttd_router_tests).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(R, emqttd_router).

route_test_() ->
    {foreach,
     fun setup/0, fun teardown/1,
     [?_test(t_add_route()),
      ?_test(t_add_routes()),
      ?_test(t_delete_route()),
      ?_test(t_delete_routes()),
      ?_test(t_route())
     ]}. 

setup() ->
    application:start(gproc),
    ensure_tab(route, [public, named_table, duplicate_bag]),
    gproc_pool:new(router, hash, [{size, 2}]),
    lists:foreach(fun(I) ->
                gproc_pool:add_worker(router, {router, I}, I),
                {ok, R} = ?R:start_link(router, I, fun(_) -> ok end, [])
        end, [1, 2]).

ensure_tab(Tab, Opts) ->
    case ets:info(Tab, name) of
        undefined -> ets:new(Tab, Opts);
        _ -> ok
    end.

teardown(_) ->
    lists:foreach(fun(I) ->
            ?R:stop(I), gproc_pool:remove_worker(router, {router, I})
        end, [1, 2]),
    gproc_pool:delete(router),
    ets:delete(route).

t_add_route() ->
    Self = self(),
    ?R:add_route(<<"topic1">>, Self),
    ?assert(?R:has_route(<<"topic1">>)),
    ?R:add_route(<<"topic2">>, Self),
    ?assert(?R:has_route(<<"topic2">>)),
    ?assertEqual([Self], ?R:lookup_routes(<<"topic1">>)),
    ?assertEqual([Self], ?R:lookup_routes(<<"topic2">>)).

t_add_routes() ->
    Self = self(),
    ?R:add_routes([], Self),
    ?R:add_routes([<<"t0">>], Self),
    ?R:add_routes([<<"t1">>,<<"t2">>,<<"t3">>], Self),
    ?assert(?R:has_route(<<"t1">>)),
    ?assertEqual([Self], ?R:lookup_routes(<<"t1">>)),
    ?assertEqual([Self], ?R:lookup_routes(<<"t2">>)),
    ?assertEqual([Self], ?R:lookup_routes(<<"t3">>)).

t_delete_route() ->
    Self = self(),
    ?R:add_routes([<<"t1">>,<<"t2">>,<<"t3">>], Self),
    ?assert(?R:has_route(<<"t1">>)),
    ?R:delete_route(<<"t2">>, Self),
    erlang:yield(),
    ?debugFmt("Routes: ~p~n", [ets:tab2list(route)]),
    ?assertNot(?R:has_route(<<"t2">>)),
    ?assert(?R:has_route(<<"t1">>)),
    ?R:delete_route(<<"t3">>, Self),
    erlang:yield(),
    ?assertNot(?R:has_route(<<"t3">>)).

t_delete_routes() ->
    Self = self(),
    ?R:add_routes([<<"t1">>,<<"t2">>,<<"t3">>], Self),
    ?R:delete_routes([<<"t3">>], Self),
    erlang:yield(), %% for delete_routes is cast
    ?assertNot(?R:has_route(<<"t3">>)),
    ?R:delete_routes([<<"t1">>, <<"t2">>], Self),
    erlang:yield(),
    ?assertNot(?R:has_route(<<"t2">>)).

t_route() ->
    Self = self(),
    Pid = spawn_link(fun() -> timer:sleep(1000) end),
    ?R:add_routes([<<"$Q/1">>,<<"t/2">>,<<"t/3">>], Self),
    ?R:add_routes([<<"t/2">>], Pid),
    Msg1 = #mqtt_message{topic = <<"$Q/1">>, payload = <<"q">>},
    Msg2 = #mqtt_message{topic = <<"t/2">>, payload = <<"t2">>},
    Msg3 = #mqtt_message{topic = <<"t/3">>, payload = <<"t3">>},
    ?R:route(<<"$Q/1">>, Msg1),
    ?R:route(<<"t/2">>, Msg2),
    ?R:route(<<"t/3">>, Msg3),
    ?assertEqual([Msg1, Msg2, Msg3], recv_loop([])),
    ?R:add_route(<<"$Q/1">>, Pid),
    ?R:route(<<"$Q/1">>, Msg1).

recv_loop(Msgs) ->
    receive
        {dispatch, _Topic, Msg} ->
            recv_loop([Msg|Msgs])
        after
            500 -> lists:reverse(Msgs)
    end.

-endif.

