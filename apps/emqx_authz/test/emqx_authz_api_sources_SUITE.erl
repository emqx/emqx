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

-module(emqx_authz_api_sources_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, <<"authorization: {sources: []}">>).

-import(emqx_ct_http, [ request_api/3
                      , request_api/5
                      , get_http_data/1
                      , create_default_app/0
                      , delete_default_app/0
                      , default_auth_header/0
                      , auth_header/2
                      ]).

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

-define(SOURCE1, #{<<"type">> => <<"http">>,
                   <<"enable">> => true,
                   <<"config">> => #{
                      <<"url">> => <<"https://fake.com:443/">>,
                      <<"headers">> => #{},
                      <<"method">> => <<"get">>,
                      <<"request_timeout">> => 5000}
                  }).
-define(SOURCE2, #{<<"type">> => <<"mongo">>,
                   <<"enable">> => true,
                   <<"config">> => #{
                          <<"mongo_type">> => <<"single">>,
                          <<"server">> => <<"127.0.0.1:27017">>,
                          <<"pool_size">> => 1,
                          <<"database">> => <<"mqtt">>,
                          <<"ssl">> => #{<<"enable">> => false}},
                   <<"collection">> => <<"fake">>,
                   <<"find">> => #{<<"a">> => <<"b">>}
                  }).
-define(SOURCE3, #{<<"type">> => <<"mysql">>,
                   <<"enable">> => true,
                   <<"config">> => #{
                       <<"server">> => <<"127.0.0.1:27017">>,
                       <<"pool_size">> => 1,
                       <<"database">> => <<"mqtt">>,
                       <<"username">> => <<"xx">>,
                       <<"password">> => <<"ee">>,
                       <<"auto_reconnect">> => true,
                       <<"ssl">> => #{<<"enable">> => false}},
                   <<"sql">> => <<"abcb">>
                  }).
-define(SOURCE4, #{<<"type">> => <<"pgsql">>,
                   <<"enable">> => true,
                   <<"config">> => #{
                       <<"server">> => <<"127.0.0.1:27017">>,
                       <<"pool_size">> => 1,
                       <<"database">> => <<"mqtt">>,
                       <<"username">> => <<"xx">>,
                       <<"password">> => <<"ee">>,
                       <<"auto_reconnect">> => true,
                       <<"ssl">> => #{<<"enable">> => false}},
                   <<"sql">> => <<"abcb">>
                  }).
-define(SOURCE5, #{<<"type">> => <<"redis">>,
                   <<"enable">> => true,
                   <<"config">> => #{
                       <<"server">> => <<"127.0.0.1:27017">>,
                       <<"pool_size">> => 1,
                       <<"database">> => 0,
                       <<"password">> => <<"ee">>,
                       <<"auto_reconnect">> => true,
                       <<"ssl">> => #{<<"enable">> => false}},
                   <<"cmd">> => <<"HGETALL mqtt_authz:%u">>
                  }).

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    meck:new(emqx_schema, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_schema, fields, fun("authorization") ->
                                             meck:passthrough(["authorization"]) ++
                                             emqx_authz_schema:fields("authorization");
                                        (F) -> meck:passthrough([F])
                                     end),

    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create, fun(_, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, update, fun(_, _, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, health_check, fun(_) -> ok end),
    meck:expect(emqx_resource, remove, fun(_) -> ok end ),

    ok = emqx_config:init_load(emqx_authz_schema, ?CONF_DEFAULT),

    ok = emqx_ct_helpers:start_apps([emqx_authz, emqx_dashboard], fun set_special_configs/1),
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),

    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    emqx_ct_helpers:stop_apps([emqx_resource, emqx_authz, emqx_dashboard]),
    meck:unload(emqx_resource),
    meck:unload(emqx_schema),
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
    emqx_config:put([authorization], #{sources => []}),
    ok;
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_api(_) ->
    {ok, 200, Result1} = request(get, uri(["authorization", "sources"]), []),
    ?assertEqual([], get_sources(Result1)),

    lists:foreach(fun(_) ->
                        {ok, 204, _} = request(post, uri(["authorization", "sources"]), ?SOURCE1)
                  end, lists:seq(1, 20)),
    {ok, 200, Result2} = request(get, uri(["authorization", "sources"]), []),
    ?assertEqual(20, length(get_sources(Result2))),

    lists:foreach(fun(Page) ->
                          Query = "?page=" ++ integer_to_list(Page) ++ "&&limit=10",
                          Url = uri(["authorization/sources" ++ Query]),
                          {ok, 200, Result} = request(get, Url, []),
                          ?assertEqual(10, length(get_sources(Result)))
                  end, lists:seq(1, 2)),

    {ok, 204, _} = request(put, uri(["authorization", "sources"]), [?SOURCE1, ?SOURCE2, ?SOURCE3, ?SOURCE4]),

    {ok, 200, Result3} = request(get, uri(["authorization", "sources"]), []),
    Sources = get_sources(Result3),
    ?assertMatch([ #{<<"type">> := <<"http">>}
                 , #{<<"type">> := <<"mongo">>}
                 , #{<<"type">> := <<"mysql">>}
                 , #{<<"type">> := <<"pgsql">>}
                 ], Sources),

    {ok, 204, _} = request(put, uri(["authorization", "sources", "http"]),  ?SOURCE1#{<<"enable">> := false}),

    {ok, 200, Result4} = request(get, uri(["authorization", "sources", "http"]), []),
    ?assertMatch(#{<<"type">> := <<"http">>, <<"enable">> := false}, jsx:decode(Result4)),

    lists:foreach(fun(#{<<"type">> := Type}) ->
                    {ok, 204, _} = request(delete, uri(["authorization", "sources", binary_to_list(Type)]), [])
                  end, Sources),
    {ok, 200, Result5} = request(get, uri(["authorization", "sources"]), []),
    ?assertEqual([], get_sources(Result5)),
    ok.

t_move_source(_) ->
    {ok, _} = emqx_authz:update(replace, [?SOURCE1, ?SOURCE2, ?SOURCE3, ?SOURCE4, ?SOURCE5]),
    ?assertMatch([ #{type := http}
                 , #{type := mongo}
                 , #{type := mysql}
                 , #{type := pgsql}
                 , #{type := redis}
                 ], emqx_authz:lookup()),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "pgsql", "move"]),
                           #{<<"position">> => <<"top">>}),
    ?assertMatch([ #{type := pgsql}
                 , #{type := http}
                 , #{type := mongo}
                 , #{type := mysql}
                 , #{type := redis}
                 ], emqx_authz:lookup()),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "http", "move"]),
                           #{<<"position">> => <<"bottom">>}),
    ?assertMatch([ #{type := pgsql}
                 , #{type := mongo}
                 , #{type := mysql}
                 , #{type := redis}
                 , #{type := http}
                 ], emqx_authz:lookup()),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "mysql", "move"]),
                           #{<<"position">> => #{<<"before">> => <<"pgsql">>}}),
    ?assertMatch([ #{type := mysql}
                 , #{type := pgsql}
                 , #{type := mongo}
                 , #{type := redis}
                 , #{type := http}
                 ], emqx_authz:lookup()),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "mongo", "move"]),
                           #{<<"position">> => #{<<"after">> => <<"http">>}}),
    ?assertMatch([ #{type := mysql}
                 , #{type := pgsql}
                 , #{type := redis}
                 , #{type := http}
                 , #{type := mongo}
                 ], emqx_authz:lookup()),

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

get_sources(Result) ->
    maps:get(<<"sources">>, jsx:decode(Result), []).

auth_header_() ->
    Username = <<"admin">>,
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.
