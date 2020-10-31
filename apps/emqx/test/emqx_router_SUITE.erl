%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(R, emqx_router).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules([router]),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

init_per_testcase(_TestCase, Config) ->
    clear_tables(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_tables().

t_mnesia(_) ->
    %% for coverage
    ok = emqx_router:mnesia(copy).

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
    ?R:add_route(<<"a/b/c">>),
    ?R:add_route(<<"a/b/c">>, node()),
    ?R:add_route(<<"a/+/b">>, node()),
    ?assertEqual([<<"a/+/b">>, <<"a/b/c">>], lists:sort(?R:topics())),
    ?R:delete_route(<<"a/b/c">>),
    ?R:delete_route(<<"a/+/b">>, node()),
    ?assertEqual([], ?R:topics()).

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
    ?assertEqual([#route{topic = <<"#">>, dest = node()},
                  #route{topic = <<"a/+/c">>, dest = node()},
                  #route{topic = <<"a/b/#">>, dest = node()},
                  #route{topic = <<"a/b/c">>, dest = node()}],
                 lists:sort(?R:match_routes(<<"a/b/c">>))),
    ?R:delete_route(<<"a/b/c">>, node()),
    ?R:delete_route(<<"a/+/c">>, node()),
    ?R:delete_route(<<"a/b/#">>, node()),
    ?R:delete_route(<<"#">>, node()),
    ?assertEqual([], lists:sort(?R:match_routes(<<"a/b/c">>))).

t_print_routes(_) ->
    ?R:add_route(<<"+/#">>),
    ?R:add_route(<<"+/+">>),
    ?R:print_routes(<<"a/b">>).

t_has_routes(_) ->
    ?R:add_route(<<"devices/+/messages">>, node()),
    ?assert(?R:has_routes(<<"devices/+/messages">>)),
    ?R:delete_route(<<"devices/+/messages">>).

t_unexpected(_) ->
    Router = emqx_misc:proc_name(?R, 1),
    ?assertEqual(ignored, gen_server:call(Router, bad_request)),
    ?assertEqual(ok, gen_server:cast(Router, bad_message)),
    Router ! bad_info.

clear_tables() ->
    lists:foreach(fun mnesia:clear_table/1,
                  [emqx_route, emqx_trie, emqx_trie_node]).

