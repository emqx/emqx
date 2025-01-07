%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_common_test_http).

-include_lib("common_test/include/ct.hrl").

-export([
    request_api/3,
    request_api/4,
    request_api/5,
    get_http_data/1,
    create_default_app/0,
    delete_default_app/0,
    default_auth_header/0,
    auth_header/1,
    auth_header/2
]).

-define(DEFAULT_APP_ID, <<"default_appid">>).
-define(DEFAULT_APP_KEY, <<"default_app_key">>).
-define(DEFAULT_APP_SECRET, <<"default_app_secret">>).

%% from emqx_dashboard/include/emqx_dashboard_rbac.hrl
-define(ROLE_API_SUPERUSER, <<"administrator">>).

request_api(Method, Url, Auth) ->
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth) ->
    request_api(Method, Url, QueryParams, Auth, []).

request_api(Method, Url, QueryParams, Auth, Body) ->
    request_api(Method, Url, QueryParams, Auth, Body, []).

request_api(Method, Url, QueryParams, Auth, Body, HttpOpts) ->
    NewUrl =
        case QueryParams of
            [] ->
                Url;
            _ ->
                Url ++ "?" ++ QueryParams
        end,
    Request =
        case Body of
            [] ->
                {NewUrl, [Auth]};
            _ ->
                {NewUrl, [Auth], "application/json", emqx_utils_json:encode(Body)}
        end,
    do_request_api(Method, Request, HttpOpts).

do_request_api(Method, Request, HttpOpts) ->
    % ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, HttpOpts, [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return}} ->
            {ok, Code, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

get_http_data(ResponseBody) ->
    emqx_utils_json:decode(ResponseBody, [return_maps]).

auth_header(#{api_key := ApiKey, api_secret := Secret}) ->
    auth_header(binary_to_list(ApiKey), binary_to_list(Secret)).

auth_header(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User, ":", Pass])),
    {"Authorization", "Basic " ++ Encoded}.

default_auth_header() ->
    {ok, #{api_key := APIKey}} = emqx_mgmt_auth:read(?DEFAULT_APP_ID),
    auth_header(
        erlang:binary_to_list(APIKey), erlang:binary_to_list(?DEFAULT_APP_SECRET)
    ).

create_default_app() ->
    Now = erlang:system_time(second),
    ExpiredAt = Now + timer:minutes(10),
    case
        emqx_mgmt_auth:create(
            ?DEFAULT_APP_ID,
            ?DEFAULT_APP_KEY,
            ?DEFAULT_APP_SECRET,
            true,
            ExpiredAt,
            <<"default app key for test">>,
            ?ROLE_API_SUPERUSER
        )
    of
        {ok, App} ->
            {ok, App};
        {error, name_already_existed} ->
            {ok, _} = emqx_mgmt_auth:read(?DEFAULT_APP_ID)
    end.

delete_default_app() ->
    emqx_mgmt_auth:delete(?DEFAULT_APP_ID).
