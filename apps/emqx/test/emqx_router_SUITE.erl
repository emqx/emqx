%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_router_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_router.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(R, emqx_router).

-define(WAIT_INDEX_SYNC(UPDATE),
    emqx_router_index:enabled() andalso
        snabbkaffe:retry(100, 10, fun() ->
            case emqx_router_index:peek_last_update() of
                UPDATE = __U -> __U;
                _ -> throw(missing_update)
            end
        end)
).

all() ->
    [
        {group, trie_global},
        {group, trie_local_async}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {trie_global, [], TCs},
        {trie_local_async, [], TCs}
    ].

init_per_group(GroupName, Config) ->
    WorkDir = filename:join([?config(priv_dir, Config), GroupName]),
    AppSpecs = [
        {emqx, #{
            config => mk_config(GroupName),
            override_env => [{boot_modules, [router]}]
        }}
    ],
    Apps = emqx_cth_suite:start(AppSpecs, #{work_dir => WorkDir}),
    [{group_apps, Apps} | Config].

end_per_group(_GroupName, Config) ->
    ok = emqx_cth_suite:stop(?config(group_apps, Config)).

mk_config(trie_global) ->
    "broker.perf.trie_local_async = false";
mk_config(trie_local_async) ->
    "broker.perf.trie_local_async = true".

init_per_testcase(_TestCase, Config) ->
    clear_tables(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_tables().

% t_add_route(_) ->
%     error('TODO').

% t_do_add_route(_) ->
%     error('TODO').

% t_lookup_routes(_) ->
%     error('TODO').

% t_delete_route(_) ->
%     error('TODO').

% t_do_delete_route(_) ->
%     error('TODO').

% t_topics(_) ->
%     error('TODO').

t_add_delete(_) ->
    add_route(<<"a/b/c">>),
    add_route(<<"a/b/c">>, node()),
    add_route(<<"a/+/b">>, node()),
    ?assertEqual([<<"a/+/b">>, <<"a/b/c">>], lists:sort(?R:topics())),
    delete_route(<<"a/b/c">>),
    delete_route(<<"a/+/b">>, node()),
    ?assertEqual([], ?R:topics()).

t_add_delete_incremental(_) ->
    add_route(<<"a/b/c">>),
    add_route(<<"a/+/c">>, node()),
    add_route(<<"a/+/+">>, node()),
    add_route(<<"a/b/#">>, node()),
    add_route(<<"#">>, node()),
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
    delete_route(<<"a/+/c">>, node()),
    ?assertEqual(
        [
            #route{topic = <<"#">>, dest = node()},
            #route{topic = <<"a/+/+">>, dest = node()},
            #route{topic = <<"a/b/#">>, dest = node()},
            #route{topic = <<"a/b/c">>, dest = node()}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/+/+">>, node()),
    ?assertEqual(
        [
            #route{topic = <<"#">>, dest = node()},
            #route{topic = <<"a/b/#">>, dest = node()},
            #route{topic = <<"a/b/c">>, dest = node()}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/b/#">>, node()),
    ?assertEqual(
        [
            #route{topic = <<"#">>, dest = node()},
            #route{topic = <<"a/b/c">>, dest = node()}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/b/c">>, node()),
    ?assertEqual(
        [#route{topic = <<"#">>, dest = node()}],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ).

t_do_add_delete(_) ->
    ?R:do_add_route(<<"a/b/c">>),
    ?R:do_add_route(<<"a/b/c">>, node()),
    ?R:do_add_route(<<"a/+/b">>, node()),
    ?WAIT_INDEX_SYNC({write, <<"a/+/b">>, _}),
    ?assertEqual([<<"a/+/b">>, <<"a/b/c">>], lists:sort(?R:topics())),

    ?R:do_delete_route(<<"a/b/c">>, node()),
    ?R:do_delete_route(<<"a/+/b">>),
    ?WAIT_INDEX_SYNC({delete, <<"a/+/b">>, _}),
    ?assertEqual([], ?R:topics()).

t_match_routes(_) ->
    add_route(<<"a/b/c">>),
    add_route(<<"a/+/c">>, node()),
    add_route(<<"a/b/#">>, node()),
    add_route(<<"#">>, node()),
    ?assertEqual(
        [
            #route{topic = <<"#">>, dest = node()},
            #route{topic = <<"a/+/c">>, dest = node()},
            #route{topic = <<"a/b/#">>, dest = node()},
            #route{topic = <<"a/b/c">>, dest = node()}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/b/c">>, node()),
    delete_route(<<"a/+/c">>, node()),
    delete_route(<<"a/b/#">>, node()),
    delete_route(<<"#">>, node()),
    ?assertEqual([], lists:sort(?R:match_routes(<<"a/b/c">>))).

t_print_routes(_) ->
    add_route(<<"+/#">>),
    add_route(<<"+/+">>),
    ?R:print_routes(<<"a/b">>).

t_has_routes(_) ->
    add_route(<<"devices/+/messages">>, node()),
    ?assert(?R:has_routes(<<"devices/+/messages">>)),
    delete_route(<<"devices/+/messages">>).

t_unexpected(_) ->
    Router = emqx_utils:proc_name(?R, 1),
    ?assertEqual(ignored, gen_server:call(Router, bad_request)),
    ?assertEqual(ok, gen_server:cast(Router, bad_message)),
    Router ! bad_info.

add_route(Topic) ->
    ok = emqx_router:add_route(Topic),
    ?WAIT_INDEX_SYNC({write, Topic, _}).

add_route(Topic, Dest) ->
    ok = emqx_router:add_route(Topic, Dest),
    ?WAIT_INDEX_SYNC({write, Topic, Dest}).

delete_route(Topic) ->
    ok = emqx_router:delete_route(Topic),
    ?WAIT_INDEX_SYNC({delete, Topic, _}).

delete_route(Topic, Dest) ->
    ok = emqx_router:delete_route(Topic, Dest),
    ?WAIT_INDEX_SYNC({delete, Topic, Dest}).

clear_tables() ->
    lists:foreach(
        fun mnesia:clear_table/1,
        [?ROUTE_TAB, ?TRIE, emqx_trie_node]
    ).
