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

-module(emqx_dashboard_error_code_SUITE).

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

t_all_code(_) ->
    HrlDef = ?ERROR_CODES,
    All = emqx_dashboard_error_code:all(),
    ?assertEqual(length(HrlDef), length(All)),
    ok.

t_list_code(_) ->
    List = emqx_dashboard_error_code:list(),
    [?assert(exist(CodeName, List)) || CodeName <- emqx_dashboard_error_code:all()],
    ok.

t_look_up_code(_) ->
    Fun =
        fun(CodeName) ->
            {ok, _} = emqx_dashboard_error_code:look_up(CodeName)
        end,
    lists:foreach(Fun, emqx_dashboard_error_code:all()),
    {error, not_found} = emqx_dashboard_error_code:look_up('_____NOT_EXIST_NAME'),
    ok.

t_description_code(_) ->
    {error, not_found} = emqx_dashboard_error_code:description('_____NOT_EXIST_NAME'),
    {ok, <<"Request parameters are not legal">>} =
        emqx_dashboard_error_code:description('BAD_REQUEST'),
    ok.

t_format_code(_) ->
    #{code := 'TEST_SUITE_CODE', description := <<"for test suite">>} =
        emqx_dashboard_error_code:format({'TEST_SUITE_CODE', <<"for test suite">>}),
    ok.

t_api_codes(_) ->
    Url = ?SERVER ++ "/error_codes",
    {ok, List} = request(Url),
    [
        ?assert(exist(atom_to_binary(CodeName, utf8), List))
     || CodeName <- emqx_dashboard_error_code:all()
    ],
    ok.

t_api_code(_) ->
    Url = ?SERVER ++ "/error_codes/BAD_REQUEST",
    {ok, #{
        <<"code">> := <<"BAD_REQUEST">>,
        <<"description">> := <<"Request parameters are not legal">>
    }} = request(Url),
    ok.

exist(_CodeName, []) ->
    false;
exist(CodeName, [#{code := CodeName, description := _} | _List]) ->
    true;
exist(CodeName, [#{<<"code">> := CodeName, <<"description">> := _} | _List]) ->
    true;
exist(CodeName, [_ | List]) ->
    exist(CodeName, List).

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
