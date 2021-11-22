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
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

-define(SOURCE1, #{<<"type">> => <<"http">>,
                   <<"enable">> => true,
                   <<"url">> => <<"https://fake.com:443/">>,
                   <<"headers">> => #{},
                   <<"method">> => <<"get">>,
                   <<"request_timeout">> => <<"5s">>
                  }).
-define(SOURCE2, #{<<"type">> => <<"mongodb">>,
                   <<"enable">> => true,
                   <<"mongo_type">> => <<"sharded">>,
                   <<"servers">> => [<<"127.0.0.1:27017">>,
                                     <<"192.168.0.1:27017">>
                                    ],
                   <<"pool_size">> => 1,
                   <<"database">> => <<"mqtt">>,
                   <<"ssl">> => #{<<"enable">> => false},
                   <<"collection">> => <<"fake">>,
                   <<"selector">> => #{<<"a">> => <<"b">>}
                  }).
-define(SOURCE3, #{<<"type">> => <<"mysql">>,
                   <<"enable">> => true,
                   <<"server">> => <<"127.0.0.1:3306">>,
                   <<"pool_size">> => 1,
                   <<"database">> => <<"mqtt">>,
                   <<"username">> => <<"xx">>,
                   <<"password">> => <<"ee">>,
                   <<"auto_reconnect">> => true,
                   <<"ssl">> => #{<<"enable">> => false},
                   <<"query">> => <<"abcb">>
                  }).
-define(SOURCE4, #{<<"type">> => <<"postgresql">>,
                   <<"enable">> => true,
                   <<"server">> => <<"127.0.0.1:5432">>,
                   <<"pool_size">> => 1,
                   <<"database">> => <<"mqtt">>,
                   <<"username">> => <<"xx">>,
                   <<"password">> => <<"ee">>,
                   <<"auto_reconnect">> => true,
                   <<"ssl">> => #{<<"enable">> => false},
                   <<"query">> => <<"abcb">>
                  }).
-define(SOURCE5, #{<<"type">> => <<"redis">>,
                   <<"enable">> => true,
                   <<"servers">> => [<<"127.0.0.1:6379">>,
                                     <<"127.0.0.1:6380">>
                                    ],
                   <<"pool_size">> => 1,
                   <<"database">> => 0,
                   <<"password">> => <<"ee">>,
                   <<"auto_reconnect">> => true,
                   <<"ssl">> => #{<<"enable">> => false},
                   <<"cmd">> => <<"HGETALL mqtt_authz:", ?PH_USERNAME/binary>>
                  }).
-define(SOURCE6, #{<<"type">> => <<"file">>,
                   <<"enable">> => true,
                   <<"rules">> =>
<<"{allow,{username,\"^dashboard?\"},subscribe,[\"$SYS/#\"]}."
  "\n{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}.">>
                  }).

all() ->
    []. %% Todo: Waiting for @terry-xiaoyu to fix the config_not_found error
    % emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create, fun(_, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, create_dry_run,
                fun(emqx_connector_mysql, _) -> {ok, meck_data};
                   (T, C) -> meck:passthrough([T, C])
                end),
    meck:expect(emqx_resource, update, fun(_, _, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, health_check, fun(_) -> ok end),
    meck:expect(emqx_resource, remove, fun(_) -> ok end ),

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
    meck:unload(emqx_resource),
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
    {ok, _} = emqx:update_config([authorization, sources], []),
    ok;
set_special_configs(_App) ->
    ok.

init_per_testcase(t_api, Config) ->
    meck:new(emqx_misc, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_misc, gen_id, fun() -> "fake" end),

    meck:new(emqx, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx, data_dir,
                fun() ->
                    {data_dir, Data} = lists:keyfind(data_dir, 1, Config),
                    Data
                end),
    Config;
init_per_testcase(_, Config) -> Config.

end_per_testcase(t_api, _Config) ->
    meck:unload(emqx_misc),
    meck:unload(emqx),
    ok;
end_per_testcase(_, _Config) -> ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_api(_) ->
    {ok, 200, Result1} = request(get, uri(["authorization", "sources"]), []),
    ?assertEqual([], get_sources(Result1)),

    {ok, 204, _} = request(put, uri(["authorization", "sources"]),
                           [?SOURCE2, ?SOURCE3, ?SOURCE4, ?SOURCE5, ?SOURCE6]),
    {ok, 204, _} = request(post, uri(["authorization", "sources"]), ?SOURCE1),

    {ok, 200, Result2} = request(get, uri(["authorization", "sources"]), []),
    Sources = get_sources(Result2),
    ?assertMatch([ #{<<"type">> := <<"http">>}
                 , #{<<"type">> := <<"mongodb">>}
                 , #{<<"type">> := <<"mysql">>}
                 , #{<<"type">> := <<"postgresql">>}
                 , #{<<"type">> := <<"redis">>}
                 , #{<<"type">> := <<"file">>}
                 ], Sources),
    ?assert(filelib:is_file(emqx_authz:acl_conf_file())),

    {ok, 204, _} = request(put, uri(["authorization", "sources", "http"]),
                           ?SOURCE1#{<<"enable">> := false}),
    {ok, 200, Result3} = request(get, uri(["authorization", "sources", "http"]), []),
    ?assertMatch(#{<<"type">> := <<"http">>, <<"enable">> := false}, jsx:decode(Result3)),

    {ok, 204, _} = request(put, uri(["authorization", "sources", "mongodb"]),
                           ?SOURCE2#{<<"ssl">> := #{
                                         <<"enable">> => true,
                                         <<"cacertfile">> => <<"fake cacert file">>,
                                         <<"certfile">> => <<"fake cert file">>,
                                         <<"keyfile">> => <<"fake key file">>,
                                         <<"verify">> => false
                                        }}),
    {ok, 200, Result4} = request(get, uri(["authorization", "sources", "mongodb"]), []),
    ?assertMatch(#{<<"type">> := <<"mongodb">>,
                   <<"ssl">> := #{<<"enable">> := true,
                                  <<"cacertfile">> := <<"fake cacert file">>,
                                  <<"certfile">> := <<"fake cert file">>,
                                  <<"keyfile">> := <<"fake key file">>,
                                  <<"verify">> := false
                                 }
                  }, jsx:decode(Result4)),
    ?assert(filelib:is_file(filename:join([data_dir(), "certs", "cacert-fake.pem"]))),
    ?assert(filelib:is_file(filename:join([data_dir(), "certs", "cert-fake.pem"]))),
    ?assert(filelib:is_file(filename:join([data_dir(), "certs", "key-fake.pem"]))),

    {ok, 204, _} = request(
                     put,
                     uri(["authorization", "sources", "mysql"]),
                     ?SOURCE3#{<<"server">> := <<"192.168.1.100:3306">>}),

    {ok, 400, _} = request(
                     put,
                     uri(["authorization", "sources", "postgresql"]),
                     ?SOURCE4#{<<"server">> := <<"fake">>}),
    {ok, 400, _} = request(
                     put,
                     uri(["authorization", "sources", "redis"]),
                     ?SOURCE5#{<<"servers">> := [<<"192.168.1.100:6379">>,
                                                 <<"192.168.1.100:6380">>]}),

    lists:foreach(
      fun(#{<<"type">> := Type}) ->
        {ok, 204, _} = request(
                         delete,
                         uri(["authorization", "sources", binary_to_list(Type)]),
                         [])
      end, Sources),
    {ok, 200, Result5} = request(get, uri(["authorization", "sources"]), []),
    ?assertEqual([], get_sources(Result5)),
    ?assertEqual([], emqx:get_config([authorization, sources])),
    ok.

t_move_source(_) ->
    {ok, _} = emqx_authz:update(replace, [?SOURCE1, ?SOURCE2, ?SOURCE3, ?SOURCE4, ?SOURCE5]),
    ?assertMatch([ #{type := http}
                 , #{type := mongodb}
                 , #{type := mysql}
                 , #{type := postgresql}
                 , #{type := redis}
                 ], emqx_authz:lookup()),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "postgresql", "move"]),
                           #{<<"position">> => <<"top">>}),
    ?assertMatch([ #{type := postgresql}
                 , #{type := http}
                 , #{type := mongodb}
                 , #{type := mysql}
                 , #{type := redis}
                 ], emqx_authz:lookup()),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "http", "move"]),
                           #{<<"position">> => <<"bottom">>}),
    ?assertMatch([ #{type := postgresql}
                 , #{type := mongodb}
                 , #{type := mysql}
                 , #{type := redis}
                 , #{type := http}
                 ], emqx_authz:lookup()),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "mysql", "move"]),
                           #{<<"position">> => #{<<"before">> => <<"postgresql">>}}),
    ?assertMatch([ #{type := mysql}
                 , #{type := postgresql}
                 , #{type := mongodb}
                 , #{type := redis}
                 , #{type := http}
                 ], emqx_authz:lookup()),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "mongodb", "move"]),
                           #{<<"position">> => #{<<"after">> => <<"http">>}}),
    ?assertMatch([ #{type := mysql}
                 , #{type := postgresql}
                 , #{type := redis}
                 , #{type := http}
                 , #{type := mongodb}
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

data_dir() -> emqx:data_dir().
