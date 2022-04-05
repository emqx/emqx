%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_api_test_helpers).

-export([set_default_config/0,
         set_default_config/1,
         request/2,
         request/3,
         request/4,
         uri/0,
         uri/1]).

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

set_default_config() ->
    set_default_config(<<"admin">>).

set_default_config(DefaultUsername) ->
    Config = #{listeners => [#{protocol => http,
                               port => 18083}],
               default_username => DefaultUsername,
               default_password => <<"public">>
              },
    emqx_config:put([dashboard], Config),
    ok.

request(Method, Url) ->
    request(Method, Url, []).

request(Method, Url, Body) ->
    request(<<"admin">>, Method, Url, Body).

request(Username, Method, Url, Body) ->
    Request = case Body of
        [] when Method =:= get orelse Method =:= put orelse
                Method =:= head orelse Method =:= delete orelse
                Method =:= trace -> {Url, [auth_header(Username)]};
        _ -> {Url, [auth_header(Username)], "application/json", jsx:encode(Body)}
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

auth_header(Username) ->
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.
