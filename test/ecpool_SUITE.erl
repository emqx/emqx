%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(ecpool_SUITE).

-compile(export_all).

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

all() ->
    [{group, all}].

groups() ->
    [{all, [sequence],
      [t_start_pool,
       t_start_sup_pool,
       t_restart_client,
       t_reconnect_client
      ]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ecpool),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(ecpool),
    ok = application:stop(gproc).

t_start_pool(_Config) ->
    ecpool:start_pool(?POOL, test_client, ?POOL_OPTS),
    ?assertEqual(10, length(ecpool:workers(test_pool))),
    ?debugFmt("~p~n",  [ecpool:workers(test_pool)]),
    lists:foreach(fun(I) ->
                      ecpool:with_client(?POOL, fun(Client) ->
                                                        ?debugFmt("Call ~p: ~p~n", [I, Client])
                                                end)
                  end, lists:seq(1, 10)).

t_start_sup_pool(_Config) ->
    {ok, Pid1} = ecpool:start_sup_pool(xpool, test_client, ?POOL_OPTS),
    {ok, Pid2} = ecpool:start_sup_pool(ypool, test_client, ?POOL_OPTS),
    ?assertEqual([{xpool, Pid1}, {ypool, Pid2}], lists:sort(ecpool_sup:pools())),
    ecpool:stop_sup_pool(ypool),
    ecpool:stop_sup_pool(xpool),
    ?assertEqual([], ecpool_sup:pools()).

t_restart_client(_Config) ->
    ecpool:start_pool(?POOL, test_client, [{pool_size, 4}]),
    ?assertEqual(4, length(ecpool:workers(?POOL))),
    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, normal) end),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(3, length(ecpool:workers(?POOL))),
    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, {shutdown, x}) end),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(2, length(ecpool:workers(?POOL))),
    ecpool:with_client(?POOL, fun(Client) ->
                                      test_client:stop(Client, badarg)
                              end),
    timer:sleep(100),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(2, length(ecpool:workers(?POOL))).

t_reconnect_client(_Config) ->
    ecpool:start_pool(?POOL, test_client, [{pool_size, 4}, {auto_reconnect, 1}]),
    ?assertEqual(4, length(ecpool:workers(?POOL))),
    ecpool:with_client(?POOL, fun(Client) ->
                                      test_client:stop(Client, normal)
                              end),
    ?assert(lists:member(false, [ecpool_worker:is_connected(Pid) || {_, Pid} <- ecpool:workers(?POOL)])),
    timer:sleep(1100),
    ?assertNot(lists:member(false, [ecpool_worker:is_connected(Pid) || {_, Pid} <- ecpool:workers(?POOL)])),
    ecpool:with_client(?POOL, fun(Client) ->
                                      test_client:stop(Client, {shutdown, badarg})
                              end),
    ?assert(lists:member(false, [ecpool_worker:is_connected(Pid) || {_, Pid} <- ecpool:workers(?POOL)])),
    timer:sleep(1100),
    ?assertEqual(4, length(ecpool:workers(?POOL))).

