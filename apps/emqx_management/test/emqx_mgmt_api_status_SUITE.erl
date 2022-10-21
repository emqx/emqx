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
-module(emqx_mgmt_api_status_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(HOST, "http://127.0.0.1:18083/").

%%---------------------------------------------------------------------------------------
%% CT boilerplate
%%---------------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

init_per_testcase(t_status_not_ok, Config) ->
    ok = application:stop(emqx),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_status_not_ok, _Config) ->
    {ok, _} = application:ensure_all_started(emqx),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

%%---------------------------------------------------------------------------------------
%% Helper fns
%%---------------------------------------------------------------------------------------

do_request(Opts) ->
    #{
        path := Path,
        method := Method,
        headers := Headers,
        body := Body0
    } = Opts,
    URL = ?HOST ++ filename:join(Path),
    Request =
        case Body0 of
            no_body -> {URL, Headers};
            {Encoding, Body} -> {URL, Headers, Encoding, Body}
        end,
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{_, StatusCode, _}, Headers1, Body1}} ->
            Body2 =
                case emqx_json:safe_decode(Body1, [return_maps]) of
                    {ok, Json} -> Json;
                    {error, _} -> Body1
                end,
            {ok, #{status_code => StatusCode, headers => Headers1, body => Body2}}
    end.

%%---------------------------------------------------------------------------------------
%% Test cases
%%---------------------------------------------------------------------------------------

t_status_ok(_Config) ->
    {ok, #{
        body := Resp,
        status_code := StatusCode
    }} = do_request(#{
        method => get,
        path => ["status"],
        headers => [],
        body => no_body
    }),
    ?assertEqual(200, StatusCode),
    ?assertMatch(
        {match, _},
        re:run(Resp, <<"emqx is running$">>)
    ),
    ok.

t_status_not_ok(_Config) ->
    {ok, #{
        body := Resp,
        status_code := StatusCode
    }} = do_request(#{
        method => get,
        path => ["status"],
        headers => [],
        body => no_body
    }),
    ?assertEqual(503, StatusCode),
    ?assertMatch(
        {match, _},
        re:run(Resp, <<"emqx is not_running$">>)
    ),
    ok.
