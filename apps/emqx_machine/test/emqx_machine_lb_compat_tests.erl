%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% The predicate a replicant applies to the info a core announces,
%% evaluated over crafted terms: the current shape, the shape of the
%% releases before the db_schema version, and garbage.
-module(emqx_machine_lb_compat_tests).

-include_lib("eunit/include/eunit.hrl").

custom_info_test() ->
    Info = info(),
    ?assertMatch(#{db_schema := _, mria := _, emqx := _}, Info),
    ?assertEqual(emqx_release:db_schema_vsn(), maps:get(db_schema, Info)),
    ?assertEqual(app_vsn(mria), maps:get(mria, Info)),
    ?assertEqual(app_vsn(emqx), maps:get(emqx, Info)).

check_test_() ->
    Info = info(),
    [
        {"own info", ?_assert(check(Info))},
        {"same db_schema and mria, other emqx vsn", ?_assert(check(Info#{emqx => "0.0.0"}))},
        {"other db_schema", ?_assertNot(check(Info#{db_schema => "0"}))},
        {"other mria", ?_assertNot(check(Info#{mria => "0.0.0"}))},
        {"no db_schema key", ?_assertNot(check(maps:remove(db_schema, Info)))},
        {"no mria key", ?_assertNot(check(maps:remove(mria, Info)))},
        {"old release, same emqx vsn", ?_assert(check(app_vsn(emqx)))},
        {"old release, other emqx vsn", ?_assertNot(check("0.0.0"))},
        {"too old to answer", ?_assertNot(check(undefined))},
        {"garbage", ?_assertNot(check(42))}
    ].

info() ->
    _ = application:load(emqx),
    _ = application:load(mria),
    emqx_machine:mria_lb_custom_info().

check(Info) ->
    emqx_machine:mria_lb_custom_info_check(Info).

app_vsn(App) ->
    {ok, Vsn} = application:get_key(App, vsn),
    Vsn.
