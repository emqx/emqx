%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_test_util).
-compile(export_all).
-compile(nowarn_export_all).

-define(SERVER, "http://127.0.0.1:18083").
-define(BASE_PATH, "/api/v5").

init_suite() ->
    init_suite([]).

init_suite(Apps) ->
    mria:start(),
    application:load(emqx_management),
    emqx_common_test_helpers:start_apps(Apps ++ [emqx_dashboard], fun set_special_configs/1).


end_suite() ->
    end_suite([]).

end_suite(Apps) ->
    application:unload(emqx_management),
    emqx_common_test_helpers:stop_apps(Apps ++ [emqx_dashboard]).

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

request_api(Method, Url) ->
    request_api(Method, Url, [], auth_header_(), []).

request_api(Method, Url, Auth) ->
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth) ->
    request_api(Method, Url, QueryParams, Auth, []).

request_api(Method, Url, QueryParams, Auth, [])
  when (Method =:= options) orelse
         (Method =:= get) orelse
         (Method =:= put) orelse
         (Method =:= head) orelse
         (Method =:= delete) orelse
         (Method =:= trace) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth]});
request_api(Method, Url, QueryParams, Auth, Body)
    when (Method =:= post) orelse
         (Method =:= patch) orelse
         (Method =:= put) orelse
         (Method =:= delete) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_json:encode(Body)}).

do_request_api(Method, Request)->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return} }
            when Code >= 200 andalso Code =< 299 ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    Username = <<"admin">>,
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

api_path(Parts)->
    ?SERVER ++ filename:join([?BASE_PATH | Parts]).
