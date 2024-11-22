%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_cache_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(_TestCase, Config) ->
    _ = ets:new(?MODULE, [named_table, set, public]),
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_config:erase(?MODULE),
    ets:delete(?MODULE).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_cache(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true,
        cleanup_interval => 500,
        cache_ttl => 100
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath),
    set_val(k1, v1),
    ?assertEqual(
        v1,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ),
    set_val(k1, v2),
    %% we should get the cached value, v1
    ?assertEqual(
        v1,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ),
    ct:sleep(101),
    %% cache expired, we should get the new value, v2
    ?assertEqual(
        v2,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ).

t_nocache(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true,
        cleanup_interval => 500,
        cache_ttl => 100
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath),
    set_val(k1, v1),
    ?assertEqual(
        v1,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() ->
            {nocache, get_val(k1)}
        end)
    ),
    set_val(k1, v2),
    %% cache disabled, we should get the new value, v2
    ?assertEqual(
        v2,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() ->
            {nocache, get_val(k1)}
        end)
    ).

t_cache_disabled(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => false,
        cleanup_interval => 500,
        cache_ttl => 100
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath),
    set_val(k1, v1),
    ?assertEqual(
        v1,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ),
    set_val(k1, v2),
    %% cache disabled, we should get the new value, v2
    ?assertEqual(
        v2,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ).

t_cleanup_expired(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true,
        cleanup_interval => 5,
        cache_ttl => 50
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath),
    set_val(k1, v1),
    _ = emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end),
    set_val(k1, v2),
    %% we should get the cached value, v1
    ?assertEqual(
        v1,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ),
    ct:sleep(100),
    ?assertMatch(
        #{size := 0, memory := _},
        emqx_auth_cache:stats(somecache)
    ).

t_reset(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true,
        cleanup_interval => 100,
        cache_ttl => 100
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath),
    set_val(k1, v1),
    _ = emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end),
    set_val(k1, v2),
    %% we should get the cached value, v1
    ?assertEqual(
        v1,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ),
    ok = emqx_auth_cache:reset(somecache),
    ?assertEqual(
        v2,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ).

t_reset_by_id(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true,
        cleanup_interval => 100,
        cache_ttl => 100
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath),
    set_val(k1, v1),
    set_val(k2, v1),
    _ = emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end),
    _ = emqx_auth_cache:with_cache(somecache, {<<"k2cache">>, v}, fun() -> {cache, get_val(k2)} end),
    set_val(k1, v2),
    set_val(k2, v2),
    ok = emqx_auth_cache:reset(somecache, <<"k1cache">>),
    %% we should get the new value for k1 but still cached value for k2 but
    ?assertEqual(
        v2,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ),
    ?assertEqual(
        v1,
        emqx_auth_cache:with_cache(somecache, {<<"k2cache">>, v}, fun() -> {cache, get_val(k2)} end)
    ).

t_size_limit(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true,
        cleanup_interval => 100,
        cache_ttl => 100,
        max_size => 2,
        max_memory => unlimited,
        stat_update_interval => 10
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath),
    set_val(k1, v1),
    set_val(k2, v1),
    set_val(k3, v1),
    _ = emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end),
    _ = emqx_auth_cache:with_cache(somecache, {<<"k2cache">>, v}, fun() -> {cache, get_val(k2)} end),
    ct:sleep(50),
    _ = emqx_auth_cache:with_cache(somecache, {<<"k3cache">>, v}, fun() -> {cache, get_val(k3)} end),
    set_val(k1, v2),
    set_val(k2, v2),
    set_val(k3, v2),
    ?assertEqual(
        v1,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ),
    ?assertEqual(
        v1,
        emqx_auth_cache:with_cache(somecache, {<<"k2cache">>, v}, fun() -> {cache, get_val(k2)} end)
    ),
    %% k3 should not have fit into the cache limits
    ?assertEqual(
        v2,
        emqx_auth_cache:with_cache(somecache, {<<"k3cache">>, v}, fun() -> {cache, get_val(k3)} end)
    ).

t_memory_limit(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true,
        cleanup_interval => 100,
        cache_ttl => 100,
        max_size => unlimited,
        max_memory => 10000,
        stat_update_interval => 10
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath),
    Value = lists:seq(1, 15000),
    set_val(k1, Value),
    set_val(k2, Value),
    _ = emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end),
    ct:sleep(50),
    _ = emqx_auth_cache:with_cache(somecache, {<<"k2cache">>, v}, fun() -> {cache, get_val(k2)} end),
    set_val(k1, v2),
    set_val(k2, v2),
    ?assertEqual(
        Value,
        emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end)
    ),
    %% k2 should not have fit into the cache limits
    ?assertEqual(
        v2,
        emqx_auth_cache:with_cache(somecache, {<<"k2cache">>, v}, fun() -> {cache, get_val(k2)} end)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

set_val(K, V) ->
    ets:insert(?MODULE, {K, V}).

get_val(K) ->
    case ets:lookup(?MODULE, K) of
        [] -> undefined;
        [{_, V}] -> V
    end.
