%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_api_SUITE).

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
    ok = emqx_common_test_helpers:start_apps([emqx_authn, emqx_dashboard], fun set_special_configs/1),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_authn, emqx_dashboard]),
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
set_special_configs(_App) ->
    ok.

t_create_http_authn(_) ->
    {ok, 200, _} = request(post, uri(["authentication"]),
                           emqx_authn_test_lib:http_example()),
    {ok, 200, _} = request(get, uri(["authentication"])).

request(Method, Url) ->
    request(Method, Url, []).

request(Method, Url, Body) ->
    Request =
        case Body of
            [] ->
                {Url, [auth_header()]};
            _ ->
                {Url, [auth_header()], "application/json", to_json(Body)}
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

auth_header() ->
    Username = <<"admin">>,
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

to_json(Hocon) ->
    {ok, Map} =hocon:binary(Hocon),
    jiffy:encode(Map).
