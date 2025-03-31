%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_limiter_client_container_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Groups = emqx_limiter_registry:list_groups(),
    lists:foreach(
        fun(Group) ->
            emqx_limiter:delete_group(Group)
        end,
        Groups
    ),
    Config.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_try_consume(_) ->
    ok = emqx_limiter:create_group(exclusive, group1, [
        {limiter1, #{capacity => 2, interval => 1000, burst_capacity => 0}},
        {limiter2, #{capacity => 1, interval => 1000, burst_capacity => 0}}
    ]),
    Container0 = emqx_limiter_client_container:new([
        {limiter1, emqx_limiter:connect({group1, limiter1})},
        {limiter2, emqx_limiter:connect({group1, limiter2})}
    ]),

    %% Try to consume 2 tokens from each limiter, but the second limiter has only 1 available
    {false, Container1, {failed_to_consume_from_limiter, {group1, limiter2}}} =
        emqx_limiter_client_container:try_consume(
            Container0,
            [{limiter1, 2}, {limiter2, 2}]
        ),

    %% Check that the tokens were put back into the limiters are available
    {true, _Container2} = emqx_limiter_client_container:try_consume(
        Container1,
        [{limiter1, 2}, {limiter2, 1}]
    ).

t_try_consume_from_nonexistent_limiter(_) ->
    Container = emqx_limiter_client_container:new([]),
    ?assertError(
        {limiter_not_found_in_container, limiter1},
        emqx_limiter_client_container:try_consume(Container, [{limiter1, 1}])
    ).
