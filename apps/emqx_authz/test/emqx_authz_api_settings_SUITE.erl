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

-module(emqx_authz_api_settings_SUITE).

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
    {ok, _} = emqx:update_config([authorization, sources], []),
    ok;
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_api(_) ->
    Settings1 = #{<<"no_match">> => <<"deny">>,
                  <<"deny_action">> => <<"disconnect">>,
                  <<"cache">> => #{
                      <<"enable">> => false,
                      <<"max_size">> => 32,
                      <<"ttl">> => 60000
                     }
                 },

    {ok, 200, Result1} = request(put, uri(["authorization", "settings"]), Settings1),
    {ok, 200, Result1} = request(get, uri(["authorization", "settings"]), []),
    ?assertEqual(Settings1, jsx:decode(Result1)),

    Settings2 = #{<<"no_match">> => <<"allow">>,
                  <<"deny_action">> => <<"ignore">>,
                  <<"cache">> => #{
                      <<"enable">> => true,
                      <<"max_size">> => 32,
                      <<"ttl">> => 60000
                     }
                 },

    {ok, 200, Result2} = request(put, uri(["authorization", "settings"]), Settings2),
    {ok, 200, Result2} = request(get, uri(["authorization", "settings"]), []),
    ?assertEqual(Settings2, jsx:decode(Result2)),

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
