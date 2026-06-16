%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_router_tool_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_router.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    AppSpecs = [
        {emqx, #{
            override_env => [{boot_modules, [broker]}]
        }}
    ],
    Apps = emqx_cth_suite:start(AppSpecs, #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    clear_tables(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_tables().

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

%% Insert a local subscription option row, the way emqx_broker would.
%% A live subscriber process is not needed: the scan only reads the
%% suboption and route tables.
add_sub(Topic) ->
    true = ets:insert(?SUBOPTION, {{Topic, self()}, #{}}).

clear_tables() ->
    catch ets:delete_all_objects(?SUBOPTION),
    lists:foreach(fun(Topic) -> emqx_router:delete_route(Topic) end, emqx_router:topics()).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "cluster_schema_view/0 includes the local node with a known schema.".
t_cluster_schema_view_returns_local_node(_) ->
    View = emqx_router_tool:cluster_schema_view(),
    ?assert(maps:is_key(node(), View)),
    ?assert(lists:member(maps:get(node(), View), [v1, v2, v3])).

-doc "A subscription with no route entry is reported as missing.".
t_scan_finds_missing_route(_) ->
    Topic = <<"a/b/c">>,
    add_sub(Topic),
    ok = emqx_router:add_route(Topic, node()),
    %% Drop the route while keeping the subscription.
    ok = emqx_router:delete_route(Topic, node()),
    Result = emqx_router_tool:scan_missing_routes(),
    ?assertMatch(#{node := _, schema := _, scanned := 1}, Result),
    ?assert(lists:member(Topic, maps:get(missing, Result))).

-doc "Two-pass scan filters out a route re-added between pass 1 and pass 2.".
t_scan_two_pass_filters_race(_) ->
    Topic = <<"race/topic">>,
    add_sub(Topic),
    %% Simulate an in-flight subscribe: has_route is false on the pass-1
    %% sample and true on the pass-2 recheck. The two-pass scan must not
    %% report the topic as missing.
    Counter = counters:new(1, []),
    meck:new(emqx_router, [passthrough]),
    try
        meck:expect(
            emqx_router,
            has_route,
            fun
                (T, _N) when T =:= Topic ->
                    case counters:get(Counter, 1) of
                        0 ->
                            counters:add(Counter, 1, 1),
                            false;
                        _ ->
                            true
                    end;
                (T, N) ->
                    meck:passthrough([T, N])
            end
        ),
        Result = emqx_router_tool:scan_missing_routes(),
        ?assertNot(lists:member(Topic, maps:get(missing, Result)))
    after
        meck:unload(emqx_router)
    end.

-doc "reconcile_missing_routes/0 re-adds the missing route.".
t_reconcile_repairs_missing(_) ->
    Topic = <<"repair/me">>,
    add_sub(Topic),
    ok = emqx_router:add_route(Topic, node()),
    ok = emqx_router:delete_route(Topic, node()),
    Reconciled = emqx_router_tool:reconcile_missing_routes(),
    ?assertEqual([Topic], maps:get(missing, Reconciled)),
    ?assertEqual([{Topic, ok}], maps:get(repaired, Reconciled)),
    %% A follow-up scan finds nothing missing.
    Result = emqx_router_tool:scan_missing_routes(),
    ?assertEqual([], maps:get(missing, Result)).

-doc "Both plain and wildcard subscriptions with missing routes are found.".
t_scan_handles_wildcard_and_plain(_) ->
    Plain = <<"a/b/c">>,
    Wildcard = <<"a/+/c">>,
    add_sub(Plain),
    add_sub(Wildcard),
    ok = emqx_router:add_route(Plain, node()),
    ok = emqx_router:add_route(Wildcard, node()),
    ok = emqx_router:delete_route(Plain, node()),
    ok = emqx_router:delete_route(Wildcard, node()),
    Result = emqx_router_tool:scan_missing_routes(),
    Missing = maps:get(missing, Result),
    ?assert(lists:member(Plain, Missing)),
    ?assert(lists:member(Wildcard, Missing)).

-doc "The scan yields (sleeps) after each chunk of topics.".
t_scan_yields_after_chunk(_) ->
    Topics = [<<"y/", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 5)],
    lists:foreach(
        fun(Topic) ->
            add_sub(Topic),
            ok = emqx_router:add_route(Topic, node())
        end,
        Topics
    ),
    Start = erlang:monotonic_time(millisecond),
    _ = emqx_router_tool:scan_missing_routes(#{chunk => 1, sleep_ms => 10}),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    %% 5 topics * 10ms, minus slack for timer granularity.
    ?assert(Elapsed >= 40, {elapsed, Elapsed}).
