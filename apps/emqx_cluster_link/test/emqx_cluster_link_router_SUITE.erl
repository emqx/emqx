%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_router_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

%%

t_no_redundant_filters(_Config) ->
    %% Verify that redundant filters are effectively ignored.
    %% I.e. when supplied through a config file.
    Link = prepare_link(
        <<"L1">>,
        [<<"t/1">>, <<"t/2">>, <<"t/3">>, <<"t/+">>, <<"t/#">>, <<"t/+/+">>, <<"t/+/+/+">>]
    ),
    ok = add_route(<<"t/6">>, [Link]),
    ok = add_route(<<"t/1">>, [Link]),
    ok = delete_route(<<"t/6">>, [Link]),
    ok = delete_route(<<"t/1">>, [Link]),
    ?assertEqual(
        [
            {<<"L1">>, add, <<"t/6">>, <<"t/6">>},
            {<<"L1">>, add, <<"t/1">>, <<"t/1">>},
            {<<"L1">>, delete, <<"t/6">>, <<"t/6">>},
            {<<"L1">>, delete, <<"t/1">>, <<"t/1">>}
        ],
        ?drainMailbox()
    ).

t_overlapping_filters(_Config) ->
    %% Verify that partially overlapping filters are respected.
    %% If those filters produce partially overlapping intersections, this will result
    %% in _multiple_ route ops.
    Link = prepare_link(
        <<"L2">>,
        [<<"t/+/1/#">>, <<"t/z">>, <<"t/1/+">>]
    ),
    ok = add_route(<<"t/+">>, [Link]),
    ok = add_route(<<"t/+/+">>, [Link]),
    ?assertEqual(
        [
            {<<"L2">>, add, <<"t/z">>, <<"t/+">>},
            {<<"L2">>, add, <<"t/1/+">>, <<"t/+/+">>},
            {<<"L2">>, add, <<"t/+/1">>, <<"t/+/+">>}
        ],
        ?drainMailbox()
    ).

t_overlapping_filters_many(_Config) ->
    %% Verify that _all_ partially overlapping filters are respected.
    Link1 = prepare_link(
        <<"LO1">>,
        [<<"t/1">>, <<"t/2">>, <<"t/3">>]
    ),
    ok = add_route(<<"t/+/#">>, [Link1]),
    ?assertEqual(
        [
            {<<"LO1">>, add, <<"t/3">>, <<"t/+/#">>},
            {<<"LO1">>, add, <<"t/2">>, <<"t/+/#">>},
            {<<"LO1">>, add, <<"t/1">>, <<"t/+/#">>}
        ],
        ?drainMailbox()
    ),
    Link2 = prepare_link(
        <<"LO2">>,
        [<<"t/1">>, <<"t/2">>, <<"t/3">>, <<"t/+">>]
    ),
    ok = add_route(<<"t/+/#">>, [Link2]),
    ?assertEqual(
        [
            {<<"LO2">>, add, <<"t/+">>, <<"t/+/#">>}
        ],
        ?drainMailbox()
    ).

t_overlapping_filters_subset(_Config) ->
    %% Verify that partially overlapping filters are respected.
    %% But if those filters produce such intersections that one is a subset of another,
    %% this will result in a _single_ route op.
    Link = prepare_link(
        <<"L3">>,
        [<<"t/1/+/3/#">>, <<"t/z">>, <<"t/1/2/+">>]
    ),
    ok = add_route(<<"t/+">>, [Link]),
    ok = add_route(<<"t/+/2/3/#">>, [Link]),
    ?assertEqual(
        [
            {<<"L3">>, add, <<"t/z">>, <<"t/+">>},
            {<<"L3">>, add, <<"t/1/2/3/#">>, <<"t/+/2/3/#">>}
        ],
        ?drainMailbox()
    ).

add_route(Topic, Links) ->
    emqx_cluster_link_router:push_update(add, Topic, Topic, fun push_route/4, Links).

delete_route(Topic, Links) ->
    emqx_cluster_link_router:push_update(delete, Topic, Topic, fun push_route/4, Links).

push_route(Cluster, OpName, Topic, RouteID) ->
    self() ! {Cluster, OpName, Topic, RouteID},
    ok.

prepare_link(Name, Topics) ->
    emqx_cluster_link_config:prepare_link(#{name => Name, enable => true, topics => Topics}).
