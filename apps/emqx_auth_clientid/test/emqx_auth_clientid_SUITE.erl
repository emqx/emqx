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

-module(emqx_auth_clientid_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_libs/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_ct_http, [ request_api/3
                      , request_api/5
                      , get_http_data/1
                      , create_default_app/0
                      , default_auth_header/0
                      ]).

-define(HOST, "http://127.0.0.1:8081/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").

-define(CLIENTID,  <<"client_id_for_ct">>).
-define(PASSWORD,  <<"password">>).
-define(NPASSWORD, <<"password1">>).
-define(USER,      #{clientid => ?CLIENTID, zone => external}).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_auth_clientid, emqx_management], fun set_special_configs/1),
    create_default_app(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_auth_clientid, emqx_management]).

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, true),
    application:set_env(emqx, enable_acl_cache, false),
    LoadedPluginPath = filename:join(["test", "emqx_SUITE_data", "loaded_plugins"]),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, LoadedPluginPath));

set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_managing(_) ->
    clean_all_clientids(),

    ok = emqx_auth_clientid:add_clientid(?CLIENTID, ?PASSWORD),
    ?assertNotEqual(emqx_auth_clientid:lookup_clientid(?CLIENTID), []),

    {ok, #{auth_result := success,
           anonymous := false}} = emqx_access_control:authenticate(?USER#{password => ?PASSWORD}),

    {error, _} = emqx_access_control:authenticate(?USER#{password => ?NPASSWORD}),

    emqx_auth_clientid:update_password(?CLIENTID, ?NPASSWORD),
    {ok, #{auth_result := success,
           anonymous := false}} = emqx_access_control:authenticate(?USER#{password => ?NPASSWORD}),

    ok = emqx_auth_clientid:remove_clientid(?CLIENTID),
    {ok, #{auth_result := success,
           anonymous := true}} = emqx_access_control:authenticate(?USER#{password => ?PASSWORD}).

t_cli(_) ->
    clean_all_clientids(),

    HashType = application:get_env(emqx_auth_clientid, password_hash, sha256),

    emqx_auth_clientid:cli(["add", ?CLIENTID, ?PASSWORD]),
    [{_, ?CLIENTID, <<Salt:4/binary, Hash/binary>>}] = emqx_auth_clientid:lookup_clientid(?CLIENTID),
    ?assertEqual(Hash, emqx_passwd:hash(HashType, <<Salt/binary, ?PASSWORD/binary>>)),

    emqx_auth_clientid:cli(["update", ?CLIENTID, ?NPASSWORD]),
    [{_, ?CLIENTID, <<Salt1:4/binary, Hash1/binary>>}] = emqx_auth_clientid:lookup_clientid(?CLIENTID),
    ?assertEqual(Hash1, emqx_passwd:hash(HashType, <<Salt1/binary, ?NPASSWORD/binary>>)),

    emqx_auth_clientid:cli(["del", ?CLIENTID]),
    ?assertEqual([], emqx_auth_clientid:lookup_clientid(?CLIENTID)),

    emqx_auth_clientid:cli(["add", "user1", "pass1"]),
    emqx_auth_clientid:cli(["add", "user2", "pass2"]),
    ?assertEqual(2, length(emqx_auth_clientid:cli(["list"]))),

    emqx_auth_clientid:cli(usage).

t_rest_api(_Config) ->
    clean_all_clientids(),

    HashType = application:get_env(emqx_auth_clientid, password_hash, sha256),

    {ok, Result} = request_http_rest_list(),
    [] = get_http_data(Result),

    {ok, _} = request_http_rest_add(?CLIENTID, ?PASSWORD),
    {ok, Result1} = request_http_rest_lookup(?CLIENTID),
    #{<<"password">> := Hash} = get_http_data(Result1),
    [{_, ?CLIENTID, <<Salt:4/binary, Hash/binary>>}] = emqx_auth_clientid:lookup_clientid(?CLIENTID),
    ?assertEqual(Hash, emqx_passwd:hash(HashType, <<Salt/binary, ?PASSWORD/binary>>)),

    {ok, _} = request_http_rest_update(?CLIENTID, ?NPASSWORD),
    [{_, ?CLIENTID, <<Salt1:4/binary, Hash1/binary>>}] = emqx_auth_clientid:lookup_clientid(?CLIENTID),
    ?assertEqual(Hash1, emqx_passwd:hash(HashType, <<Salt1/binary, ?NPASSWORD/binary>>)),

    {ok, _} = request_http_rest_delete(?CLIENTID),
    ?assertEqual([], emqx_auth_clientid:lookup_clientid(?CLIENTID)).

t_conf_not_override_existed(_) ->
    clean_all_clientids(),

    application:stop(emqx_auth_clientid),
    application:set_env(emqx_auth_clientid, client_list, [{?CLIENTID, ?PASSWORD}]),
    application:ensure_all_started(emqx_auth_clientid),

    {ok, _} = emqx_access_control:authenticate(?USER#{password => ?PASSWORD}),
    emqx_auth_clientid:cli(["update", ?CLIENTID, ?NPASSWORD]),

    {error, _} = emqx_access_control:authenticate(?USER#{password => ?PASSWORD}),
    {ok, _} = emqx_access_control:authenticate(?USER#{password => ?NPASSWORD}),

    application:stop(emqx_auth_clientid),
    application:ensure_all_started(emqx_auth_clientid),
    {ok, _} = emqx_access_control:authenticate(?USER#{password => ?NPASSWORD}),

    ct:pal("~p", [ets:tab2list(emqx_auth_clientid)]),
    {ok, _} = request_http_rest_update(?CLIENTID, ?PASSWORD),
    application:stop(emqx_auth_clientid),
    application:ensure_all_started(emqx_auth_clientid),
    ct:pal("~p", [ets:tab2list(emqx_auth_clientid)]),
    {ok, _} = emqx_access_control:authenticate(?USER#{password => ?PASSWORD}).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

clean_all_clientids() ->
    [mnesia:dirty_delete({emqx_auth_clientid, Id})
     || Id <- mnesia:dirty_all_keys(emqx_auth_clientid)].

%%--------------------------------------------------------------------
%% REST API Requests

request_http_rest_list() ->
    request_api(get, uri(), default_auth_header()).

request_http_rest_lookup(ClientId) ->
    request_api(get, uri([ClientId]), default_auth_header()).

request_http_rest_add(ClientId, Password) ->
    Params = #{<<"clientid">> => ClientId, <<"password">> => Password},
    request_api(post, uri(), [], default_auth_header(), Params).

request_http_rest_update(ClientId, Password) ->
    Params = #{<<"password">> => Password},
    request_api(put, uri([ClientId]), [], default_auth_header(), Params).

request_http_rest_delete(ClientId) ->
    request_api(delete, uri([ClientId]), default_auth_header()).

%% @private
uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [b2l(E) || E <- Parts],
    %% http://127.0.0.1:8080/api/v4/auth_clientid/<P1>/<P2>/<Pn>
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION, "auth_clientid"| NParts]).

%% @private
b2l(B) when is_binary(B) ->
    binary_to_list(B);
b2l(L) when is_list(L) ->
    L.

