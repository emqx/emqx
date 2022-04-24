%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_bad_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/http_api.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(SERVER, "http://127.0.0.1:18083/api/v5").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    mria:start(),
    application:load(emqx_dashboard),
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_dashboard], fun set_special_configs/1),
    Config.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(),
    ok;
set_special_configs(_) ->
    ok.

end_per_suite(Config) ->
    end_suite(),
    Config.

end_suite() ->
    application:unload(emqx_management),
    emqx_common_test_helpers:stop_apps([emqx_dashboard]).

t_bad_api_path(_) ->
    Url = ?SERVER ++ "/for/test/some/path/not/exist",
    {error, {"HTTP/1.1", 404, "Not Found"}} = request(Url),
    ok.

request(Url) ->
    Request = {Url, []},
    case httpc:request(get, Request, [], []) of
        {error, Reason} ->
            {error, Reason};
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            {ok, emqx_json:decode(Return, [return_maps])};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.
