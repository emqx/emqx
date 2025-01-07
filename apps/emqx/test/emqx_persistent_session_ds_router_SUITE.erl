%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_ds_router_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../src/emqx_persistent_session_ds/emqx_ps_ds_int.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(R, emqx_persistent_session_ds_router).
-define(DEF_DS_SESSION_ID, <<"some-client-id">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = filename:join([?config(priv_dir, Config), ?MODULE]),
    AppSpecs = [
        emqx_durable_storage,
        {emqx, #{
            config => #{durable_sessions => #{enable => true}},
            override_env => [{boot_modules, [broker]}]
        }}
    ],
    Apps = emqx_cth_suite:start(AppSpecs, #{work_dir => WorkDir}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_TestCase, Config) ->
    clear_tables(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_tables().

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

clear_tables() ->
    lists:foreach(
        fun mnesia:clear_table/1,
        [?PS_ROUTER_TAB, ?PS_FILTERS_TAB]
    ).

add_route(TopicFilter) ->
    ?R:do_add_route(TopicFilter, ?DEF_DS_SESSION_ID).

delete_route(TopicFilter) ->
    ?R:do_delete_route(TopicFilter, ?DEF_DS_SESSION_ID).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

% t_lookup_routes(_) ->
%     error('TODO').

t_add_delete(_) ->
    ?assertNot(?R:has_any_route(<<"a/b/c">>)),
    add_route(<<"a/b/c">>),
    ?assert(?R:has_any_route(<<"a/b/c">>)),
    add_route(<<"a/b/c">>),
    ?assert(?R:has_any_route(<<"a/b/c">>)),
    add_route(<<"a/+/b">>),
    ?assert(?R:has_any_route(<<"a/b/c">>)),
    ?assert(?R:has_any_route(<<"a/c/b">>)),
    ?assertEqual([<<"a/+/b">>, <<"a/b/c">>], lists:sort(?R:topics())),
    delete_route(<<"a/b/c">>),
    ?assertNot(?R:has_any_route(<<"a/b/c">>)),
    ?assert(?R:has_any_route(<<"a/c/b">>)),
    delete_route(<<"a/+/b">>),
    ?assertNot(?R:has_any_route(<<"a/b/c">>)),
    ?assertNot(?R:has_any_route(<<"a/c/b">>)),
    ?assertEqual([], ?R:topics()).

t_add_delete_incremental(_) ->
    add_route(<<"a/b/c">>),
    add_route(<<"a/+/c">>),
    add_route(<<"a/+/+">>),
    add_route(<<"a/b/#">>),
    add_route(<<"#">>),
    ?assert(?R:has_any_route(<<"any/topic">>)),
    ?assertEqual(
        [
            #ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/+/+">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/+/c">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/c">>, dest = ?DEF_DS_SESSION_ID}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/+/c">>),
    ?assertEqual(
        [
            #ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/+/+">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/c">>, dest = ?DEF_DS_SESSION_ID}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/+/+">>),
    ?assertEqual(
        [
            #ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/c">>, dest = ?DEF_DS_SESSION_ID}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/b/#">>),
    ?assertEqual(
        [
            #ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/c">>, dest = ?DEF_DS_SESSION_ID}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/b/c">>),
    ?assertEqual(
        [#ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID}],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ).

t_do_add_delete(_) ->
    add_route(<<"a/b/c">>),
    add_route(<<"a/b/c">>),
    add_route(<<"a/+/b">>),
    ?assertEqual([<<"a/+/b">>, <<"a/b/c">>], lists:sort(?R:topics())),

    delete_route(<<"a/b/c">>),
    delete_route(<<"a/+/b">>),
    ?assertEqual([], ?R:topics()).

t_match_routes(_) ->
    add_route(<<"a/b/c">>),
    add_route(<<"a/+/c">>),
    add_route(<<"a/b/#">>),
    add_route(<<"#">>),
    ?assertEqual(
        [
            #ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/+/c">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/c">>, dest = ?DEF_DS_SESSION_ID}
        ],
        lists:sort(?R:match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/b/c">>),
    delete_route(<<"a/+/c">>),
    delete_route(<<"a/b/#">>),
    delete_route(<<"#">>),
    ?assertEqual([], lists:sort(?R:match_routes(<<"a/b/c">>))).

t_print_routes(_) ->
    add_route(<<"+/#">>),
    add_route(<<"+/+">>),
    ?R:print_routes(<<"a/b">>).

t_has_route(_) ->
    add_route(<<"devices/+/messages">>),
    ?assert(?R:has_route(<<"devices/+/messages">>, ?DEF_DS_SESSION_ID)),
    delete_route(<<"devices/+/messages">>).
