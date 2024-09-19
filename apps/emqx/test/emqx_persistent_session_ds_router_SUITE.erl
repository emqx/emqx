%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("../src/emqx_persistent_session_ds/emqx_ps_ds_int.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

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
    lists:foreach(fun mnesia:clear_table/1, ?PS_ROUTER_TABS).

add_route(TopicFilter) ->
    emqx_persistent_session_ds_router:add_route(TopicFilter, ?DEF_DS_SESSION_ID).

add_route(TopicFilter, Scope) ->
    emqx_persistent_session_ds_router:add_route(TopicFilter, ?DEF_DS_SESSION_ID, Scope).

delete_route(TopicFilter) ->
    emqx_persistent_session_ds_router:delete_route(TopicFilter, ?DEF_DS_SESSION_ID).

delete_route(TopicFilter, Scope) ->
    emqx_persistent_session_ds_router:delete_route(TopicFilter, ?DEF_DS_SESSION_ID, Scope).

has_any_route(Topic) ->
    has_any_route(Topic, ?QOS_0).

has_any_route(Topic, QoS) ->
    Msg = emqx_message:make(_From = <<?MODULE_STRING>>, QoS, Topic, <<>>),
    emqx_persistent_session_ds_router:has_any_route(Msg).

match_routes(Topic) ->
    emqx_persistent_session_ds_router:match_routes(Topic).

match_routes(Topic, Scope) ->
    emqx_persistent_session_ds_router:match_routes(Topic, Scope).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_add_delete(_) ->
    add_route(<<"a/b/c">>),
    add_route(<<"a/b/c">>),
    add_route(<<"a/+/b">>),
    ?assertEqual(
        [<<"a/+/b">>, <<"a/b/c">>],
        lists:sort(emqx_persistent_session_ds_router:topics())
    ),
    delete_route(<<"a/b/c">>),
    delete_route(<<"a/+/b">>),
    ?assertEqual([], emqx_persistent_session_ds_router:topics()).

t_add_delete_match(_) ->
    ?assertNot(has_any_route(<<"a/b/c">>)),
    add_route(<<"a/b/c">>),
    ?assert(has_any_route(<<"a/b/c">>)),
    add_route(<<"a/b/c">>),
    ?assert(has_any_route(<<"a/b/c">>)),
    add_route(<<"a/+/b">>),
    ?assert(has_any_route(<<"a/b/c">>)),
    ?assert(has_any_route(<<"a/c/b">>)),
    ?assertEqual(
        [<<"a/+/b">>, <<"a/b/c">>],
        lists:sort(emqx_persistent_session_ds_router:topics())
    ),
    delete_route(<<"a/b/c">>),
    ?assertNot(has_any_route(<<"a/b/c">>)),
    ?assert(has_any_route(<<"a/c/b">>)),
    delete_route(<<"a/+/b">>),
    ?assertNot(has_any_route(<<"a/b/c">>)),
    ?assertNot(has_any_route(<<"a/c/b">>)),
    ?assertEqual([], emqx_persistent_session_ds_router:topics()).

t_add_delete_scope(_) ->
    add_route(<<"a/b/1">>),
    add_route(<<"a/+/2">>),
    add_route(<<"a/b/2">>, noqos0),
    add_route(<<"a/+/2">>, noqos0),
    add_route(<<"a/+/3">>, noqos0),
    ?assert(has_any_route(<<"a/x/2">>, ?QOS_0)),
    ?assert(has_any_route(<<"a/b/2">>, ?QOS_1)),
    ?assertNot(has_any_route(<<"a/x/3">>, ?QOS_0)),
    ?assert(has_any_route(<<"a/x/3">>, ?QOS_1)),
    ?assertEqual(
        [<<"a/+/2">>, <<"a/+/3">>, <<"a/b/1">>, <<"a/b/2">>],
        lists:sort(emqx_persistent_session_ds_router:topics())
    ),
    ?assertMatch(
        [#ps_route{topic = <<"a/+/2">>}, #ps_route{topic = <<"a/+/2">>}],
        lists:sort(emqx_persistent_session_ds_router:lookup_routes(<<"a/+/2">>))
    ),
    ?assertMatch(
        [#ps_route{topic = <<"a/+/3">>}],
        lists:sort(emqx_persistent_session_ds_router:lookup_routes(<<"a/+/3">>, noqos0))
    ),
    ?assertEqual(
        [],
        lists:sort(emqx_persistent_session_ds_router:lookup_routes(<<"a/b/1">>, noqos0))
    ),
    delete_route(<<"a/b/2">>, noqos0),
    delete_route(<<"a/+/2">>, noqos0),
    delete_route(<<"a/+/3">>, noqos0),
    ?assertEqual(
        [<<"a/+/2">>, <<"a/b/1">>],
        lists:sort(emqx_persistent_session_ds_router:topics())
    ),
    ?assertNot(has_any_route(<<"a/x/3">>, ?QOS_1)).

t_add_delete_incremental(_) ->
    add_route(<<"a/b/c">>),
    add_route(<<"a/+/c">>),
    add_route(<<"a/+/+">>),
    add_route(<<"a/b/#">>),
    add_route(<<"#">>),
    ?assert(has_any_route(<<"any/topic">>)),
    ?assertEqual(
        [
            #ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/+/+">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/+/c">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/c">>, dest = ?DEF_DS_SESSION_ID}
        ],
        lists:sort(match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/+/c">>),
    ?assertEqual(
        [
            #ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/+/+">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/c">>, dest = ?DEF_DS_SESSION_ID}
        ],
        lists:sort(match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/+/+">>),
    ?assertEqual(
        [
            #ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/c">>, dest = ?DEF_DS_SESSION_ID}
        ],
        lists:sort(match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/b/#">>),
    ?assertEqual(
        [
            #ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID},
            #ps_route{topic = <<"a/b/c">>, dest = ?DEF_DS_SESSION_ID}
        ],
        lists:sort(match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/b/c">>),
    ?assertEqual(
        [#ps_route{topic = <<"#">>, dest = ?DEF_DS_SESSION_ID}],
        lists:sort(match_routes(<<"a/b/c">>))
    ).

t_match_routes(_) ->
    add_route(<<"a/b/c">>),
    add_route(<<"a/+/c">>),
    add_route(<<"a/b/#">>),
    add_route(<<"#">>),
    ?assertMatch(
        [
            #ps_route{topic = <<"#">>},
            #ps_route{topic = <<"a/+/c">>},
            #ps_route{topic = <<"a/b/#">>},
            #ps_route{topic = <<"a/b/c">>}
        ],
        lists:sort(match_routes(<<"a/b/c">>))
    ),
    delete_route(<<"a/b/c">>),
    delete_route(<<"a/+/c">>),
    delete_route(<<"a/b/#">>),
    delete_route(<<"#">>),
    ?assertEqual([], lists:sort(match_routes(<<"a/b/c">>))).

t_match_routes_scope(_) ->
    add_route(<<"a/b/c">>),
    add_route(<<"a/+/c">>, noqos0),
    add_route(<<"a/b/#">>),
    add_route(<<"a/+">>),
    add_route(<<"a/#">>, noqos0),
    ?assertMatch(
        [#ps_route{topic = <<"a/b/#">>}, #ps_route{topic = <<"a/b/c">>}],
        lists:sort(match_routes(<<"a/b/c">>))
    ),
    ?assertMatch(
        [#ps_route{topic = <<"a/#">>}, #ps_route{topic = <<"a/+/c">>}],
        lists:sort(match_routes(<<"a/b/c">>, noqos0))
    ).

t_has_route(_) ->
    add_route(<<"devices/+/messages">>),
    ?assert(
        emqx_persistent_session_ds_router:has_route(<<"devices/+/messages">>, ?DEF_DS_SESSION_ID)
    ),
    delete_route(<<"devices/+/messages">>).

t_stream(_) ->
    add_route(<<"a/b/1">>),
    add_route(<<"a/+/2">>),
    add_route(<<"t/#">>),
    add_route(<<"a/b/2">>, noqos0),
    add_route(<<"a/+/2">>, noqos0),
    add_route(<<"a/+/3">>, noqos0),
    add_route(<<"t/+/+">>, noqos0),
    TopicsF = fun(Stream) ->
        lists:sort(
            emqx_utils_stream:consume(
                emqx_utils_stream:map(fun(R) -> R#route.topic end, Stream)
            )
        )
    end,
    ?assertEqual(
        [<<"a/+/2">>, <<"a/+/2">>, <<"a/+/3">>, <<"a/b/1">>, <<"a/b/2">>, <<"t/#">>, <<"t/+/+">>],
        TopicsF(emqx_persistent_session_ds_router:stream('_'))
    ),
    ?assertEqual(
        [<<"a/+/2">>, <<"a/+/3">>, <<"a/b/2">>, <<"t/+/+">>],
        TopicsF(emqx_persistent_session_ds_router:stream('_', noqos0))
    ),
    ?assertEqual(
        [<<"a/+/2">>, <<"a/+/2">>],
        TopicsF(emqx_persistent_session_ds_router:stream(<<"a/+/2">>))
    ),
    ?assertEqual(
        [<<"a/+/2">>],
        TopicsF(emqx_persistent_session_ds_router:stream(<<"a/+/2">>, noqos0))
    ).
