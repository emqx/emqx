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

-define(EXAMPLE_USERNAME, #{username => user1,
                            rules => [ #{topic => <<"test/toopic/1">>,
                                         permission => <<"allow">>,
                                         action => <<"publish">>
                                        }
                                     , #{topic => <<"test/toopic/2">>,
                                         permission => <<"allow">>,
                                         action => <<"subscribe">>
                                        }
                                     , #{topic => <<"eq test/#">>,
                                         permission => <<"deny">>,
                                         action => <<"all">>
                                        }
                                     ]
                           }).
-define(EXAMPLE_CLIENTID, #{clientid => client1,
                            rules => [ #{topic => <<"test/toopic/1">>,
                                         permission => <<"allow">>,
                                         action => <<"publish">>
                                        }
                                     , #{topic => <<"test/toopic/2">>,
                                         permission => <<"allow">>,
                                         action => <<"subscribe">>
                                        }
                                     , #{topic => <<"eq test/#">>,
                                         permission => <<"deny">>,
                                         action => <<"all">>
                                        }
                                     ]
                           }).
-define(EXAMPLE_ALL ,     #{rules => [ #{topic => <<"test/toopic/1">>,
                                         permission => <<"allow">>,
                                         action => <<"publish">>
                                        }
                                     , #{topic => <<"test/toopic/2">>,
                                         permission => <<"allow">>,
                                         action => <<"subscribe">>
                                        }
                                     , #{topic => <<"eq test/#">>,
                                         permission => <<"deny">>,
                                         action => <<"all">>
                                        }
                                     ]
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

    ok = emqx_config:init_load(emqx_authz_schema, ?CONF_DEFAULT),

    ok = emqx_ct_helpers:start_apps([emqx_authz, emqx_dashboard], fun set_special_configs/1),
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),

    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    emqx_ct_helpers:stop_apps([emqx_authz, emqx_dashboard]),
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
    emqx_config:put([authorization], #{sources => [#{type => 'built-in-database', 
                                                     enable => true}
                                                  ]}),
    ok;
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_api(_) ->
    {ok, 204, _} = request(post, uri(["authorization", "sources", "built-in-database", "username"]), [?EXAMPLE_USERNAME]),
    {ok, 200, Request1} = request(get, uri(["authorization", "sources", "built-in-database", "username"]), []),
    {ok, 200, Request2} = request(get, uri(["authorization", "sources", "built-in-database", "username", "user1"]), []),
    [#{<<"username">> := <<"user1">>, <<"rules">> := Rules1}] = jsx:decode(Request1),
    #{<<"username">> := <<"user1">>, <<"rules">> := Rules1} = jsx:decode(Request2),
    ?assertEqual(3, length(Rules1)),

    {ok, 204, _} = request(put, uri(["authorization", "sources", "built-in-database", "username", "user1"]), ?EXAMPLE_USERNAME#{rules => []}),
    {ok, 200, Request3} = request(get, uri(["authorization", "sources", "built-in-database", "username", "user1"]), []),
    #{<<"username">> := <<"user1">>, <<"rules">> := Rules2} = jsx:decode(Request3),
    ?assertEqual(0, length(Rules2)),

    {ok, 204, _} = request(delete, uri(["authorization", "sources", "built-in-database", "username", "user1"]), []),
    {ok, 404, _} = request(get, uri(["authorization", "sources", "built-in-database", "username", "user1"]), []),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "built-in-database", "clientid"]), [?EXAMPLE_CLIENTID]),
    {ok, 200, Request4} = request(get, uri(["authorization", "sources", "built-in-database", "clientid"]), []),
    {ok, 200, Request5} = request(get, uri(["authorization", "sources", "built-in-database", "clientid", "client1"]), []),
    [#{<<"clientid">> := <<"client1">>, <<"rules">> := Rules3}] = jsx:decode(Request4),
    #{<<"clientid">> := <<"client1">>, <<"rules">> := Rules3} = jsx:decode(Request5),
    ?assertEqual(3, length(Rules3)),

    {ok, 204, _} = request(put, uri(["authorization", "sources", "built-in-database", "clientid", "client1"]), ?EXAMPLE_CLIENTID#{rules => []}),
    {ok, 200, Request6} = request(get, uri(["authorization", "sources", "built-in-database", "clientid", "client1"]), []),
    #{<<"clientid">> := <<"client1">>, <<"rules">> := Rules4} = jsx:decode(Request6),
    ?assertEqual(0, length(Rules4)),

    {ok, 204, _} = request(delete, uri(["authorization", "sources", "built-in-database", "clientid", "client1"]), []),
    {ok, 404, _} = request(get, uri(["authorization", "sources", "built-in-database", "clientid", "client1"]), []),

    {ok, 204, _} = request(put, uri(["authorization", "sources", "built-in-database", "all"]), ?EXAMPLE_ALL),
    {ok, 200, Request7} = request(get, uri(["authorization", "sources", "built-in-database", "all"]), []),
    [#{<<"rules">> := Rules5}] = jsx:decode(Request7),
    ?assertEqual(3, length(Rules5)),

    {ok, 204, _} = request(put, uri(["authorization", "sources", "built-in-database", "all"]), ?EXAMPLE_ALL#{rules => []}),
    {ok, 200, Request8} = request(get, uri(["authorization", "sources", "built-in-database", "all"]), []),
    [#{<<"rules">> := Rules6}] = jsx:decode(Request8),
    ?assertEqual(0, length(Rules6)),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "built-in-database", "username"]), [ #{username => N, rules => []} || N <- lists:seq(1, 20) ]),
    {ok, 200, Request9} = request(get, uri(["authorization", "sources", "built-in-database", "username?page=2&limit=5"]), []),
    #{<<"data">> := Data1} = jsx:decode(Request9),
    ?assertEqual(5, length(Data1)),

    {ok, 204, _} = request(post, uri(["authorization", "sources", "built-in-database", "clientid"]), [ #{clientid => N, rules => []} || N <- lists:seq(1, 20) ]),
    {ok, 200, Request10} = request(get, uri(["authorization", "sources", "built-in-database", "clientid?limit=5"]), []),
    ?assertEqual(5, length(jsx:decode(Request10))),

    {ok, 204, _} = request(delete, uri(["authorization", "sources", "built-in-database", "purge-all"]), []),
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
