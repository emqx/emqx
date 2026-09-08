%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    %% No listener is needed; not binding ports lets the suite run
    %% beside a running broker.
    ListenerConf =
        "listeners.tcp.default.enable = false\n"
        "listeners.ssl.default.enable = false\n"
        "listeners.ws.default.enable = false\n"
        "listeners.wss.default.enable = false",
    Apps = emqx_cth_suite:start(
        [{emqx, ListenerConf}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
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

-doc "A lazy entry of an unlimited limiter grants consume and stays lazy.".
t_lazy_entry_unlimited(_) ->
    ok = emqx_limiter:create_group(exclusive, group1, [
        {limiter1, #{capacity => infinity}}
    ]),
    Container0 = emqx_limiter_client_container:new([
        {limiter1, {lazy, [{group1, limiter1}]}}
    ]),
    {true, Container1} =
        emqx_limiter_client_container:try_consume(Container0, [{limiter1, 1000}]),
    ?assertEqual(Container0, Container1).

-doc "A lazy entry of a limited limiter connects on first consume and enforces the limit.".
t_lazy_entry_materializes(_) ->
    ok = emqx_limiter:create_group(exclusive, group1, [
        {limiter1, #{capacity => 2, interval => 60000, burst_capacity => 0}}
    ]),
    Container0 = emqx_limiter_client_container:new([
        {limiter1, {lazy, [{group1, limiter1}]}}
    ]),
    {true, Container1} =
        emqx_limiter_client_container:try_consume(Container0, [{limiter1, 2}]),
    ?assertMatch(#{limiter1 := #{module := _}}, Container1),
    {false, _Container2, {failed_to_consume_from_limiter, {group1, limiter1}}} =
        emqx_limiter_client_container:try_consume(Container1, [{limiter1, 1}]).

-doc "A limit configured after container creation applies on the next consume.".
t_lazy_entry_hot_update(_) ->
    ok = emqx_limiter:create_group(exclusive, group1, [
        {limiter1, #{capacity => infinity}}
    ]),
    Container0 = emqx_limiter_client_container:new([
        {limiter1, {lazy, [{group1, limiter1}]}}
    ]),
    {true, Container1} =
        emqx_limiter_client_container:try_consume(Container0, [{limiter1, 1000}]),
    ?assertEqual(Container0, Container1),
    ok = emqx_limiter:update_group(group1, [
        {limiter1, #{capacity => 2, interval => 60000, burst_capacity => 0}}
    ]),
    {true, Container2} =
        emqx_limiter_client_container:try_consume(Container1, [{limiter1, 2}]),
    ?assertMatch(#{limiter1 := #{module := _}}, Container2),
    {false, _Container3, {failed_to_consume_from_limiter, {group1, limiter1}}} =
        emqx_limiter_client_container:try_consume(Container2, [{limiter1, 1}]).

-doc "A lazy entry whose group is gone is treated as unlimited.".
t_lazy_entry_missing_group(_) ->
    Container0 = emqx_limiter_client_container:new([
        {limiter1, {lazy, [{nonexistent_group, limiter1}]}}
    ]),
    {true, Container1} =
        emqx_limiter_client_container:try_consume(Container0, [{limiter1, 1}]),
    ?assertEqual(Container0, Container1).

-doc "A lazy entry with several limiter ids connects them into a composite client.".
t_lazy_entry_composite(_) ->
    ok = emqx_limiter:create_group(exclusive, group1, [
        {limiter1, #{capacity => 2, interval => 60000, burst_capacity => 0}}
    ]),
    ok = emqx_limiter:create_group(exclusive, group2, [
        {limiter1, #{capacity => infinity}}
    ]),
    Container0 = emqx_limiter_client_container:new([
        {limiter1, {lazy, [{group1, limiter1}, {group2, limiter1}]}}
    ]),
    {true, Container1} =
        emqx_limiter_client_container:try_consume(Container0, [{limiter1, 2}]),
    {false, _Container2, {failed_to_consume_from_limiter, {group1, limiter1}}} =
        emqx_limiter_client_container:try_consume(Container1, [{limiter1, 1}]).

-doc """
When a later limiter denies, tokens consumed from real clients are put back;
lazy-granted entries need no put-back.
""".
t_lazy_entry_mixed_put_back(_) ->
    ok = emqx_limiter:create_group(exclusive, group1, [
        {limiter1, #{capacity => infinity}},
        {limiter2, #{capacity => 2, interval => 60000, burst_capacity => 0}},
        {limiter3, #{capacity => 1, interval => 60000, burst_capacity => 0}}
    ]),
    Container0 = emqx_limiter_client_container:new([
        {limiter1, {lazy, [{group1, limiter1}]}},
        {limiter2, emqx_limiter:connect({group1, limiter2})},
        {limiter3, emqx_limiter:connect({group1, limiter3})}
    ]),
    %% limiter3 denies; limiter2's tokens go back, limiter1 stays lazy.
    {false, Container1, {failed_to_consume_from_limiter, {group1, limiter3}}} =
        emqx_limiter_client_container:try_consume(
            Container0,
            [{limiter1, 1}, {limiter2, 2}, {limiter3, 2}]
        ),
    ?assertMatch(#{limiter1 := {lazy, _}}, Container1),
    {true, _Container2} = emqx_limiter_client_container:try_consume(
        Container1,
        [{limiter1, 1}, {limiter2, 2}, {limiter3, 1}]
    ).
