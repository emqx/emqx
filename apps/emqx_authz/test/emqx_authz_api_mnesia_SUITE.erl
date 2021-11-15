%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
           [emqx_conf, emqx_authz, emqx_dashboard],
           fun set_special_configs/1),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx:update_config(
                [authorization],
                #{<<"no_match">> => <<"allow">>,
                  <<"cache">> => #{<<"enable">> => <<"true">>},
                  <<"sources">> => []}),
    emqx_common_test_helpers:stop_apps([emqx_dashboard, emqx_authz, emqx_conf]),
    ok.

set_special_configs(emqx_dashboard) ->
    Config = #{
        default_username => <<"admin">>,
        default_password => <<"public">>,
        listeners => [#{
            protocol => http,
            port => 18083
        }]
    },
    emqx_config:put([emqx_dashboard], Config),
    ok;
set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config([authorization, sources],
                                 [#{<<"type">> => <<"built-in-database">>}]),
    ok;
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_api(_) ->
    {ok, 204, _} =
        request( post
               , uri(["authorization", "sources", "built-in-database", "username"])
               , [?USERNAME_RULES_EXAMPLE]),
    {ok, 200, Request1} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "username"])
               , []),
    {ok, 200, Request2} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "username", "user1"])
               , []),
    #{<<"data">> := [#{<<"username">> := <<"user1">>, <<"rules">> := Rules1}],
      <<"meta">> := #{<<"count">> := 1,
                      <<"limit">> := 100,
                      <<"page">> := 1}} = jsx:decode(Request1),
    #{<<"username">> := <<"user1">>, <<"rules">> := Rules1} = jsx:decode(Request2),
    ?assertEqual(3, length(Rules1)),

    {ok, 204, _} =
        request( put
               , uri(["authorization", "sources", "built-in-database", "username", "user1"])
               , ?USERNAME_RULES_EXAMPLE#{rules => []}),
    {ok, 200, Request3} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "username", "user1"])
               , []),
    #{<<"username">> := <<"user1">>, <<"rules">> := Rules2} = jsx:decode(Request3),
    ?assertEqual(0, length(Rules2)),

    {ok, 204, _} =
        request( delete
               , uri(["authorization", "sources", "built-in-database", "username", "user1"])
               , []),
    {ok, 404, _} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "username", "user1"])
               , []),

    {ok, 204, _} =
        request( post
               , uri(["authorization", "sources", "built-in-database", "clientid"])
               , [?CLIENTID_RULES_EXAMPLE]),
    {ok, 200, Request4} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "clientid"])
               , []),
    {ok, 200, Request5} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "clientid", "client1"])
               , []),
    #{<<"data">> := [#{<<"clientid">> := <<"client1">>, <<"rules">> := Rules3}],
      <<"meta">> := #{<<"count">> := 1, <<"limit">> := 100, <<"page">> := 1}}
        = jsx:decode(Request4),
    #{<<"clientid">> := <<"client1">>, <<"rules">> := Rules3} = jsx:decode(Request5),
    ?assertEqual(3, length(Rules3)),

    {ok, 204, _} =
        request( put
               , uri(["authorization", "sources", "built-in-database", "clientid", "client1"])
               , ?CLIENTID_RULES_EXAMPLE#{rules => []}),
    {ok, 200, Request6} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "clientid", "client1"])
               , []),
    #{<<"clientid">> := <<"client1">>, <<"rules">> := Rules4} = jsx:decode(Request6),
    ?assertEqual(0, length(Rules4)),

    {ok, 204, _} =
        request( delete
               , uri(["authorization", "sources", "built-in-database", "clientid", "client1"])
               , []),
    {ok, 404, _} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "clientid", "client1"])
               , []),

    {ok, 204, _} =
        request( put
               , uri(["authorization", "sources", "built-in-database", "all"])
               , ?ALL_RULES_EXAMPLE),
    {ok, 200, Request7} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "all"])
               , []),
    #{<<"rules">> := Rules5} = jsx:decode(Request7),
    ?assertEqual(3, length(Rules5)),

    {ok, 204, _} =
        request( put
               , uri(["authorization", "sources", "built-in-database", "all"])
               , ?ALL_RULES_EXAMPLE#{rules => []}),
    {ok, 200, Request8} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "all"])
               , []),
    #{<<"rules">> := Rules6} = jsx:decode(Request8),
    ?assertEqual(0, length(Rules6)),

    {ok, 204, _} =
        request( post
               , uri(["authorization", "sources", "built-in-database", "username"])
               , [ #{username => erlang:integer_to_binary(N), rules => []}
                   || N <- lists:seq(1, 20) ]),
    {ok, 200, Request9} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "username?page=2&limit=5"])
               , []),
    #{<<"data">> := Data1} = jsx:decode(Request9),
    ?assertEqual(5, length(Data1)),

    {ok, 204, _} =
        request( post
               , uri(["authorization", "sources", "built-in-database", "clientid"])
               , [ #{clientid => erlang:integer_to_binary(N), rules => []}
                   || N <- lists:seq(1, 20) ]),
    {ok, 200, Request10} =
        request( get
               , uri(["authorization", "sources", "built-in-database", "clientid?limit=5"])
               , []),
    #{<<"data">> := Data2} = jsx:decode(Request10),
    ?assertEqual(5, length(Data2)),

    {ok, 400, Msg1} =
        request( delete
               , uri(["authorization", "sources", "built-in-database", "purge-all"])
               , []),
    ?assertMatch({match, _}, re:run(Msg1, "must\sbe\sdisabled\sbefore")),
    {ok, 204, _} =
        request( put
               , uri(["authorization", "sources", "built-in-database"])
               ,  #{<<"enable">> => true}),
    %% test idempotence
    {ok, 204, _} =
        request( put
               , uri(["authorization", "sources", "built-in-database"])
               ,  #{<<"enable">> => true}),
    {ok, 204, _} =
        request( put
               , uri(["authorization", "sources", "built-in-database"])
               ,  #{<<"enable">> => false}),
    {ok, 204, _} =
        request( delete
               , uri(["authorization", "sources", "built-in-database", "purge-all"])
               , []),
    ?assertEqual([], mnesia:dirty_all_keys(?ACL_TABLE)),
    ok.

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------

request(Method, Url, Body) ->
    Request = case Body of
        [] -> {Url, [auth_header_()]};
        _ -> {Url, [auth_header_()], "application/json", jsx:encode(Body)}
    end,
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return} } ->
            {ok, Code, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [E || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION | NParts]).

get_sources(Result) -> jsx:decode(Result).

auth_header_() ->
    Username = <<"admin">>,
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.
