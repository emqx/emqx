%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(metrics_worker, emqx_auth_cache_metrics_worker).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(_TestCase, Config) ->
    _ = ets:new(?MODULE, [named_table, set, public]),
    {ok, _Pid} = emqx_metrics_worker:start_link(?metrics_worker),
    Config.

end_per_testcase(_TestCase, _Config) ->
    _ = emqx_metrics_worker:stop(?metrics_worker),
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
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath, ?metrics_worker),
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
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath, ?metrics_worker),
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
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath, ?metrics_worker),
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
        stat_update_interval => 10,
        cache_ttl => 50
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath, ?metrics_worker),
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
        #{count := 0},
        emqx_auth_cache:metrics(somecache)
    ).

t_reset(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true,
        cleanup_interval => 100,
        cache_ttl => 100
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath, ?metrics_worker),
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

t_size_limit(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true,
        cleanup_interval => 100,
        cache_ttl => 100,
        max_count => 2,
        max_memory => unlimited,
        stat_update_interval => 10
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath, ?metrics_worker),
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
        max_count => unlimited,
        max_memory => 10000,
        stat_update_interval => 10
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath, ?metrics_worker),
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

t_metrics(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true,
        cleanup_interval => 10,
        cache_ttl => 100,
        stat_update_interval => 10
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath, ?metrics_worker),
    set_val(k1, v1),
    %% Cache miss
    _ = emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end),
    set_val(k1, v2),
    %% Cache hit
    _ = emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end),
    ct:sleep(101),
    %% cache expired, miss
    _ = emqx_auth_cache:with_cache(somecache, {<<"k1cache">>, v}, fun() -> {cache, get_val(k1)} end),

    #{
        hits := #{value := Hit},
        misses := #{value := Miss},
        inserts := #{value := Insert},
        count := Count,
        memory := Memory
    } =
        emqx_auth_cache:metrics(somecache),

    ?assertEqual(Hit, 1),
    ?assertEqual(Miss, 2),
    ?assertEqual(Insert, 2),

    ?assertEqual(Count, 1),
    ?assert(Memory > 0),

    ok.

t_cluster(_Config) ->
    ConfigPath = [?MODULE, ?FUNCTION_NAME],
    emqx_config:put(ConfigPath, #{
        enable => true
    }),
    {ok, _Pid} = emqx_auth_cache:start_link(somecache, ConfigPath, ?metrics_worker),

    [{ok, {_Node, #{}}}] = emqx_auth_cache_proto_v1:metrics([node()], somecache),
    [{ok, ok}] = emqx_auth_cache_proto_v1:reset([node()], somecache).

t_cache_key_template(_Config) ->
    Vars = ["username", "password", "clientid"],
    Template = emqx_auth_template:cache_key_template(Vars),
    Values = #{username => <<"user1">>, password => <<"pass1">>, clientid => <<"client1">>},
    Key0 = emqx_auth_template:cache_key(Values, Template),
    Key1 = emqx_auth_template:cache_key(Values#{username => <<"user2">>}, Template),
    Key2 = emqx_auth_template:cache_key(Values#{password => <<"pass2">>}, Template),
    Key3 = emqx_auth_template:cache_key(Values#{clientid => <<"client2">>}, Template),
    ?assertNotEqual(Key0(), Key1()),
    ?assertNotEqual(Key0(), Key2()),
    ?assertNotEqual(Key0(), Key3()),

    ?assertEqual(
        nomatch,
        re:run(Key0(), <<"pass1">>)
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
