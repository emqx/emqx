%%% Copyright (c) 2015 eMQTT.IO, All Rights Reserved.
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
%%% 
%%% @author Feng Lee <feng@emqtt.io>
%%%

-module(ecpool_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(POOL, test_pool).

-define(POOL_OPTS, [
        %% schedulers number
        {pool_size, 10},
        %% round-robbin | random | hash
        {pool_type, random},
        %% false | pos_integer()
        {auto_reconnect, false},
       
        %% DB Parameters 
        {host, "localhost"},
        {port, 5432},
        {username, "feng"},
        {password, ""},
        {database, "mqtt"},
        {encoding,  utf8}]).

pool_test_() ->
    {foreach,
     fun() ->
        application:start(gproc),
        application:start(ecpool)
     end,
     fun(_) ->
        application:stop(ecpool),
        application:stop(gproc)
     end,
     [?_test(t_start_pool()),
      ?_test(t_start_sup_pool()),
      ?_test(t_restart_client()),
      ?_test(t_reconnect_client())]}.

t_start_pool() ->
    ecpool:start_pool(?POOL, test_client, ?POOL_OPTS),
    ?assertEqual(10, length(ecpool:workers(test_pool))),
    ?debugFmt("~p~n",  [ecpool:workers(test_pool)]),
    lists:foreach(fun(I) ->
        ecpool:with_client(?POOL, fun(Client) ->
                                ?debugFmt("Call ~p: ~p~n", [I, Client])
                    end)
        end, lists:seq(1, 10)).

t_start_sup_pool() ->
    {ok, Pid1} = ecpool:start_sup_pool(xpool, test_client, ?POOL_OPTS),
    {ok, Pid2} = ecpool:start_sup_pool(ypool, test_client, ?POOL_OPTS),
    ?assertEqual([{xpool, Pid1}, {ypool, Pid2}], lists:sort(ecpool_sup:pools())),
    ecpool:stop_sup_pool(ypool),
    ecpool:stop_sup_pool(xpool),
    ?assertEqual([], ecpool_sup:pools()).

t_restart_client() ->
    ecpool:start_pool(?POOL, test_client, [{pool_size, 4}]),
    ?assertEqual(4, length(ecpool:workers(?POOL))),
    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, normal) end),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(3, length(ecpool:workers(?POOL))),
    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, {shutdown, x}) end),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(2, length(ecpool:workers(?POOL))),
    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, badarg) end),
    timer:sleep(100),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(2, length(ecpool:workers(?POOL))).

t_reconnect_client() ->
    ecpool:start_pool(?POOL, test_client, [{pool_size, 4}, {auto_reconnect, 1}]),
    ?assertEqual(4, length(ecpool:workers(?POOL))),
    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, normal) end),
    ?assert(lists:member(false, [ecpool_worker:is_connected(Pid) || {_, Pid} <- ecpool:workers(?POOL)])),
    timer:sleep(1100),
    ?assertNot(lists:member(false, [ecpool_worker:is_connected(Pid) || {_, Pid} <- ecpool:workers(?POOL)])),
    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, badarg) end),
    ?assert(lists:member(false, [ecpool_worker:is_connected(Pid) || {_, Pid} <- ecpool:workers(?POOL)])),
    timer:sleep(1100),
    ?assertEqual(4, length(ecpool:workers(?POOL))).

-endif.

