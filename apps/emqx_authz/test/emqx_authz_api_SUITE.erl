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

-module(emqx_authz_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_ct_http, [ request_api/3
                      , request_api/5
                      , get_http_data/1
                      , create_default_app/0
                      , delete_default_app/0
                      , default_auth_header/0
                      ]).

-define(HOST, "http://127.0.0.1:8081/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    application:load(emqx_modules),
    ok = emqx_ct_helpers:start_apps([emqx_management, emqx_authz], fun set_special_configs/1),
    create_default_app(),
    Config.

end_per_suite(_Config) ->
    delete_default_app(),
    file:delete(filename:join(emqx:get_env(plugins_etc_dir), 'authz.conf')),
    emqx_ct_helpers:stop_apps([emqx_authz, emqx_management]).

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, true),
    application:set_env(emqx, enable_acl_cache, false),
    ok;
set_special_configs(emqx_authz) ->
    application:set_env(emqx, plugins_etc_dir,
                        emqx_ct_helpers:deps_path(emqx_authz, "test")),
    Conf = #{<<"authz">> => #{<<"rules">> => []}},
    ok = file:write_file(filename:join(emqx:get_env(plugins_etc_dir), 'authz.conf'), jsx:encode(Conf)),

    ok;

set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_api(_Config) ->
    Rule1 = #{<<"principal">> =>
                    #{<<"and">> => [#{<<"username">> => <<"^test?">>},
                                    #{<<"clientid">> => <<"^test?">>}
                                   ]},
              <<"action">> => <<"sub">>,
              <<"topics">> => [<<"%u">>],
              <<"permission">> => <<"allow">>
            },
    {ok, _} = request_http_rest_add(["authz/push"], #{rules => [Rule1]}),
    {ok, Result1} = request_http_rest_lookup(["authz"]),
    ?assertMatch([Rule1 | _ ], get_http_data(Result1)),

    Rule2 = #{<<"principal">> => #{<<"ipaddress">> => <<"127.0.0.1">>},
              <<"action">> => <<"pub">>,
              <<"topics">> => [#{<<"eq">> => <<"#">>},
                               #{<<"eq">> => <<"+">>}
                              ],
              <<"permission">> => <<"deny">>
            },
    {ok, _} = request_http_rest_add(["authz/append"], #{rules => [Rule2]}),
    {ok, Result2} = request_http_rest_lookup(["authz"]),
    ?assertEqual(Rule2#{<<"principal">> => #{<<"ipaddress">> => "127.0.0.1"}}, 
                 lists:last(get_http_data(Result2))),

    {ok, _} = request_http_rest_update(["authz"], #{rules => []}),
    {ok, Result3} = request_http_rest_lookup(["authz"]),
    ?assertEqual([], get_http_data(Result3)),
    ok.

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------

request_http_rest_list(Path) ->
    request_api(get, uri(Path), default_auth_header()).

request_http_rest_lookup(Path) ->
    request_api(get, uri([Path]), default_auth_header()).

request_http_rest_add(Path, Params) ->
    request_api(post, uri(Path), [], default_auth_header(), Params).

request_http_rest_update(Path, Params) ->
    request_api(put, uri([Path]), [], default_auth_header(), Params).

request_http_rest_delete(Login) ->
    request_api(delete, uri([Login]), default_auth_header()).

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [b2l(E) || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION | NParts]).

%% @private
b2l(B) when is_binary(B) ->
    binary_to_list(B);
b2l(L) when is_list(L) ->
    L.
