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
        path := Path0,
        method := Method,
        headers := Headers,
        body := Body0
    } = Opts,
    URL = ?HOST ++ filename:join(Path0),
    {ok, #{host := Host, port := Port, path := Path}} = emqx_http_lib:uri_parse(URL),
    %% we must not use `httpc' here, because it keeps retrying when it
    %% receives a 503 with `retry-after' header, and there's no option
    %% to stop that behavior...
    {ok, Gun} = gun:open(Host, Port, #{retry => 0}),
    {ok, http} = gun:await_up(Gun),
    Request =
        fun() ->
            case Body0 of
                no_body -> gun:Method(Gun, Path, Headers);
                {_Encoding, Body} -> gun:Method(Gun, Path, Headers, Body)
            end
        end,
    Ref = Request(),
    receive
        {gun_response, Gun, Ref, nofin, StatusCode, Headers1} ->
            Data = data_loop(Gun, Ref, _Acc = <<>>),
            #{status_code => StatusCode, headers => maps:from_list(Headers1), body => Data}
    after 5_000 ->
        error({timeout, Opts, process_info(self(), messages)})
    end.

data_loop(Gun, Ref, Acc) ->
    receive
        {gun_data, Gun, Ref, nofin, Data} ->
            data_loop(Gun, Ref, <<Acc/binary, Data/binary>>);
        {gun_data, Gun, Ref, fin, Data} ->
            gun:shutdown(Gun),
            <<Acc/binary, Data/binary>>
    after 5000 ->
        error(timeout)
    end.

%%---------------------------------------------------------------------------------------
%% Test cases
%%---------------------------------------------------------------------------------------

t_status_ok(_Config) ->
    #{
        body := Resp,
        status_code := StatusCode
    } = do_request(#{
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
    #{
        body := Resp,
        headers := Headers,
        status_code := StatusCode
    } = do_request(#{
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
    ?assertMatch(
        #{<<"retry-after">> := <<"15">>},
        Headers
    ),
    ok.
