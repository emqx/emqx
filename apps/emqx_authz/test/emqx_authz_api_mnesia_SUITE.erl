%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_api_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_dashboard_api_test_helpers, [request/3, uri/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authz, emqx_dashboard],
        fun set_special_configs/1
    ),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    emqx_common_test_helpers:stop_apps([emqx_dashboard, emqx_authz, emqx_conf]),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config(
        [authorization, sources],
        [#{<<"type">> => <<"built_in_database">>}]
    ),
    ok;
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_api(_) ->
    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "username"]),
            [?USERNAME_RULES_EXAMPLE]
        ),

    {ok, 409, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "username"]),
            [?USERNAME_RULES_EXAMPLE]
        ),

    {ok, 200, Request1} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "username"]),
            []
        ),
    #{
        <<"data">> := [#{<<"username">> := <<"user1">>, <<"rules">> := Rules1}],
        <<"meta">> := #{
            <<"count">> := 1,
            <<"limit">> := 100,
            <<"page">> := 1
        }
    } = jsx:decode(Request1),
    ?assertEqual(3, length(Rules1)),

    {ok, 200, Request1_1} =
        request(
            get,
            uri([
                "authorization",
                "sources",
                "built_in_database",
                "username?page=1&limit=20&like_username=noexist"
            ]),
            []
        ),
    #{
        <<"data">> := [],
        <<"meta">> := #{
            <<"count">> := 0,
            <<"limit">> := 20,
            <<"page">> := 1
        }
    } = jsx:decode(Request1_1),

    {ok, 200, Request2} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "username", "user1"]),
            []
        ),
    #{<<"username">> := <<"user1">>, <<"rules">> := Rules1} = jsx:decode(Request2),

    {ok, 204, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database", "username", "user1"]),
            ?USERNAME_RULES_EXAMPLE#{rules => []}
        ),
    {ok, 200, Request3} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "username", "user1"]),
            []
        ),
    #{<<"username">> := <<"user1">>, <<"rules">> := Rules2} = jsx:decode(Request3),
    ?assertEqual(0, length(Rules2)),

    {ok, 204, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "username", "user1"]),
            []
        ),
    {ok, 404, _} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "username", "user1"]),
            []
        ),
    {ok, 404, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "username", "user1"]),
            []
        ),

    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "clientid"]),
            [?CLIENTID_RULES_EXAMPLE]
        ),

    {ok, 409, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "clientid"]),
            [?CLIENTID_RULES_EXAMPLE]
        ),

    {ok, 200, Request4} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "clientid"]),
            []
        ),
    {ok, 200, Request5} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "clientid", "client1"]),
            []
        ),
    #{
        <<"data">> := [#{<<"clientid">> := <<"client1">>, <<"rules">> := Rules3}],
        <<"meta">> := #{<<"count">> := 1, <<"limit">> := 100, <<"page">> := 1}
    } =
        jsx:decode(Request4),
    #{<<"clientid">> := <<"client1">>, <<"rules">> := Rules3} = jsx:decode(Request5),
    ?assertEqual(3, length(Rules3)),

    {ok, 204, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database", "clientid", "client1"]),
            ?CLIENTID_RULES_EXAMPLE#{rules => []}
        ),
    {ok, 200, Request6} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "clientid", "client1"]),
            []
        ),
    #{<<"clientid">> := <<"client1">>, <<"rules">> := Rules4} = jsx:decode(Request6),
    ?assertEqual(0, length(Rules4)),

    {ok, 204, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "clientid", "client1"]),
            []
        ),
    {ok, 404, _} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "clientid", "client1"]),
            []
        ),
    {ok, 404, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "clientid", "client1"]),
            []
        ),

    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "all"]),
            ?ALL_RULES_EXAMPLE
        ),
    {ok, 200, Request7} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "all"]),
            []
        ),
    #{<<"rules">> := Rules5} = jsx:decode(Request7),
    ?assertEqual(3, length(Rules5)),

    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "all"]),

            ?ALL_RULES_EXAMPLE#{rules => []}
        ),
    {ok, 200, Request8} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "all"]),
            []
        ),
    #{<<"rules">> := Rules6} = jsx:decode(Request8),
    ?assertEqual(0, length(Rules6)),

    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "username"]),
            [
                #{username => erlang:integer_to_binary(N), rules => []}
             || N <- lists:seq(1, 20)
            ]
        ),
    {ok, 200, Request9} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "username?page=2&limit=5"]),
            []
        ),
    #{<<"data">> := Data1} = jsx:decode(Request9),
    ?assertEqual(5, length(Data1)),

    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "clientid"]),
            [
                #{clientid => erlang:integer_to_binary(N), rules => []}
             || N <- lists:seq(1, 20)
            ]
        ),
    {ok, 200, Request10} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "clientid?limit=5"]),
            []
        ),
    #{<<"data">> := Data2} = jsx:decode(Request10),
    ?assertEqual(5, length(Data2)),

    {ok, 400, Msg1} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "purge-all"]),
            []
        ),
    ?assertMatch({match, _}, re:run(Msg1, "must\sbe\sdisabled\sbefore")),
    {ok, 204, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database"]),
            #{<<"enable">> => true, <<"type">> => <<"built_in_database">>}
        ),
    %% test idempotence
    {ok, 204, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database"]),
            #{<<"enable">> => true, <<"type">> => <<"built_in_database">>}
        ),
    {ok, 204, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database"]),
            #{<<"enable">> => false, <<"type">> => <<"built_in_database">>}
        ),
    {ok, 204, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "purge-all"]),
            []
        ),
    ?assertEqual(0, emqx_authz_mnesia:record_count()),
    ok.
