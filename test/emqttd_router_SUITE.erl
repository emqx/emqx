%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqttd_router_SUITE).

-compile(export_all).

-include("emqttd.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(R, emqttd_router).

all() ->
    [{group, route},
     {group, local_route}].

groups() ->
    [{route, [sequence],
      [t_get_topics,
       t_add_del_route,
       t_match_route,
       t_print,
       t_has_route]},
     {local_route, [sequence],
      [t_get_local_topics,
       t_add_del_local_route,
       t_match_local_route]}].

init_per_suite(Config) ->
    ekka:start(),
    ekka_mnesia:ensure_started(),
    {ok, _R} = emqttd_router:start(),
    Config.

end_per_suite(_Config) ->
    emqttd_router:stop(),
    ekka:stop(),
    ekka_mnesia:ensure_stopped(),
    ekka_mnesia:delete_schema().

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_tables().

t_get_topics(_) ->
    ?R:add_route(<<"a/b/c">>),
    ?R:add_route(<<"a/b/c">>),
    ?R:add_route(<<"a/+/b">>),
    ?assertEqual([<<"a/+/b">>, <<"a/b/c">>], lists:sort(?R:topics())),
    ?R:del_route(<<"a/b/c">>),
    ?R:del_route(<<"a/+/b">>),
    ?assertEqual([], lists:sort(?R:topics())).

t_add_del_route(_) ->
    %%Node = node(),
    ?R:add_route(<<"a/b/c">>),
    ?R:add_route(<<"a/+/b">>),
    ?R:del_route(<<"a/b/c">>),
    ?R:del_route(<<"a/+/b">>).

t_match_route(_) ->
    Node = node(),
    ?R:add_route(<<"a/b/c">>),
    ?R:add_route(<<"a/+/c">>),
    ?R:add_route(<<"a/b/#">>),
    ?R:add_route(<<"#">>),
    ?assertEqual([#mqtt_route{topic = <<"#">>, node = Node},
                  #mqtt_route{topic = <<"a/+/c">>, node = Node},
                  #mqtt_route{topic = <<"a/b/#">>, node = Node},
                  #mqtt_route{topic = <<"a/b/c">>, node = Node}],
                 lists:sort(?R:match(<<"a/b/c">>))).

t_print(_) ->
    ?R:add_route(<<"topic">>),
    ?R:add_route(<<"topic/#">>),
    ?R:print(<<"topic">>).

t_has_route(_) ->
    ?R:add_route(<<"devices/+/messages">>),
    ?assert(?R:has_route(<<"devices/+/messages">>)).

t_get_local_topics(_) ->
    ?R:add_local_route(<<"a/b/c">>),
    ?R:add_local_route(<<"x/+/y">>),
    ?R:add_local_route(<<"z/#">>),
    ?assertEqual([<<"z/#">>, <<"x/+/y">>, <<"a/b/c">>], ?R:local_topics()),
    ?R:del_local_route(<<"x/+/y">>),
    ?R:del_local_route(<<"z/#">>),
    ?assertEqual([<<"a/b/c">>], ?R:local_topics()).

t_add_del_local_route(_) ->
    Node = node(),
    ?R:add_local_route(<<"a/b/c">>),
    ?R:add_local_route(<<"x/+/y">>),
    ?R:add_local_route(<<"z/#">>),
    ?assertEqual([{<<"a/b/c">>, Node},
                  {<<"x/+/y">>, Node},
                  {<<"z/#">>, Node}],
                 lists:sort(?R:get_local_routes())),
    ?R:del_local_route(<<"x/+/y">>),
    ?R:del_local_route(<<"z/#">>),
    ?assertEqual([{<<"a/b/c">>, Node}], lists:sort(?R:get_local_routes())).

t_match_local_route(_) ->
    ?R:add_local_route(<<"$SYS/#">>),
    ?R:add_local_route(<<"a/b/c">>),
    ?R:add_local_route(<<"a/+/c">>),
    ?R:add_local_route(<<"a/b/#">>),
    ?R:add_local_route(<<"#">>),
    Matched = [Topic || #mqtt_route{topic = {local, Topic}} <- ?R:match_local(<<"a/b/c">>)],
    ?assertEqual([<<"#">>, <<"a/+/c">>, <<"a/b/#">>, <<"a/b/c">>], lists:sort(Matched)).

clear_tables() ->
    ?R:clean_local_routes(),
    lists:foreach(fun mnesia:clear_table/1, [mqtt_route, mqtt_trie, mqtt_trie_node]).

