%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_router_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_router.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(R, emqx_router).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

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

% t_lookup_routes(_) ->
%     error('TODO').

t_add_delete(_) ->
    ?R:add_route(<<"a/b/c">>),
    ?R:add_route(<<"a/b/c">>, node()),
    ?R:add_route(<<"a/+/b">>, node()),
    ?assertEqual([<<"a/+/b">>, <<"a/b/c">>], lists:sort(?R:topics())),
    ?R:delete_route(<<"a/b/c">>),
    ?R:delete_route(<<"a/+/b">>, node()),
    ?assertEqual([], ?R:topics()).

t_add_delete_incremental(_) ->
    ?R:add_route(<<"a/b/c">>),
    ?R:add_route(<<"a/+/c">>, node()),
    ?R:add_route(<<"a/+/+">>, node()),
    ?R:add_route(<<"a/b/#">>, node()),
    ?R:add_route(<<"#">>, node()),
    ?assertEqual(
        [
            #route{topic = <<"#">>, dest = node()},
            #route{topic = <<"a/+/+">>, dest = node()},
            #route{topic = <<"a/+/c">>, dest = node()},
            #route{topic = <<"a/b/#">>, dest = node()},
            #route{topic = <<"a/b/c">>, dest = node()}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    ?R:delete_route(<<"a/+/c">>, node()),
    ?assertEqual(
        [
            #route{topic = <<"#">>, dest = node()},
            #route{topic = <<"a/+/+">>, dest = node()},
            #route{topic = <<"a/b/#">>, dest = node()},
            #route{topic = <<"a/b/c">>, dest = node()}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    ?R:delete_route(<<"a/+/+">>, node()),
    ?assertEqual(
        [
            #route{topic = <<"#">>, dest = node()},
            #route{topic = <<"a/b/#">>, dest = node()},
            #route{topic = <<"a/b/c">>, dest = node()}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    ?R:delete_route(<<"a/b/#">>, node()),
    ?assertEqual(
        [
            #route{topic = <<"#">>, dest = node()},
            #route{topic = <<"a/b/c">>, dest = node()}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    ?R:delete_route(<<"a/b/c">>, node()),
    ?assertEqual(
        [#route{topic = <<"#">>, dest = node()}],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ).

t_do_add_delete(_) ->
    ?R:do_add_route(<<"a/b/c">>),
    ?R:do_add_route(<<"a/b/c">>, node()),
    ?R:do_add_route(<<"a/+/b">>, node()),
    ?assertEqual([<<"a/+/b">>, <<"a/b/c">>], lists:sort(?R:topics())),

    ?R:do_delete_route(<<"a/b/c">>, node()),
    ?R:do_delete_route(<<"a/+/b">>),
    ?assertEqual([], ?R:topics()).

t_match_routes(_) ->
    ?R:add_route(<<"a/b/c">>),
    ?R:add_route(<<"a/+/c">>, node()),
    ?R:add_route(<<"a/b/#">>, node()),
    ?R:add_route(<<"#">>, node()),
    ?assertEqual(
        [
            #route{topic = <<"#">>, dest = node()},
            #route{topic = <<"a/+/c">>, dest = node()},
            #route{topic = <<"a/b/#">>, dest = node()},
            #route{topic = <<"a/b/c">>, dest = node()}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    ?R:delete_route(<<"a/b/c">>, node()),
    ?R:delete_route(<<"a/+/c">>, node()),
    ?R:delete_route(<<"a/b/#">>, node()),
    ?R:delete_route(<<"#">>, node()),
    ?assertEqual([], lists:sort(?R:match_routes(<<"a/b/c">>))).

t_print_routes(_) ->
    ?R:add_route(<<"+/#">>),
    ?R:add_route(<<"+/+">>),
    ?R:print_routes(<<"a/b">>).

t_has_route(_) ->
    ?R:add_route(<<"devices/+/messages">>, node()),
    ?assert(?R:has_route(<<"devices/+/messages">>, node())),
    ?R:delete_route(<<"devices/+/messages">>).

t_unexpected(_) ->
    Router = emqx_utils:proc_name(?R, 1),
    ?assertEqual(ignored, gen_server:call(Router, bad_request)),
    ?assertEqual(ok, gen_server:cast(Router, bad_message)),
    Router ! bad_info.

clear_tables() ->
    lists:foreach(
        fun mnesia:clear_table/1,
        [?ROUTE_TAB, ?ROUTE_TAB_FILTERS]
    ).
