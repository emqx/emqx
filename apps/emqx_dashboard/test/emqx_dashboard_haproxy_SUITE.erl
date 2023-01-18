%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_haproxy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(
    emqx_common_test_http,
    [
        request_api/3
    ]
).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_dashboard.hrl").

-define(HOST, "http://127.0.0.1:18083").

-define(BASE_PATH, "/").

all() ->
    emqx_common_test_helpers:all(?MODULE).

end_suite() ->
    end_suite([]).

end_suite(Apps) ->
    application:unload(emqx_management),
    mnesia:clear_table(?ADMIN),
    emqx_common_test_helpers:stop_apps(Apps ++ [emqx_dashboard]).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps(
        [emqx_management, emqx_dashboard],
        fun set_special_configs/1
    ),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_dashboard, emqx_management]),
    mria:stop().

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(<<"admin">>, true),
    ok;
set_special_configs(_) ->
    ok.

disabled_t_status(_) ->
    %% no easy way since httpc doesn't support emulating the haproxy protocol
    {ok, 200, _Res} = http_get(["status"]),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------
http_get(Parts) ->
    request_api(get, api_path(Parts), auth_header_()).

auth_header_() ->
    auth_header_(<<"admin">>, <<"public">>).

auth_header_(Username, Password) ->
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).
