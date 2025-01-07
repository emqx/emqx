%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("common_test/include/ct.hrl").

-define(HOST, "http://127.0.0.1:18083/").

%%---------------------------------------------------------------------------------------
%% CT boilerplate
%%---------------------------------------------------------------------------------------

all() ->
    OtherTCs = emqx_common_test_helpers:all(?MODULE) -- get_status_tests(),
    [
        {group, api_status_endpoint},
        {group, non_api_status_endpoint}
        | OtherTCs
    ].

get_status_tests() ->
    [
        t_status_ok,
        t_status_not_ok,
        t_status_text_format,
        t_status_json_format,
        t_status_bad_format_qs
    ].

groups() ->
    [
        {api_status_endpoint, [], get_status_tests()},
        {non_api_status_endpoint, [], get_status_tests()}
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            %% Dashboard implicitly loads full schema so we must make preparations for it
            %% to be valid (see `emqx_cth_suite:default_appspec(emqx_conf, SuiteOpts)`).
            %% Otherwise, restarting `emqx` will fail in `t_status_not_ok`
            emqx_conf,
            emqx,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_group(api_status_endpoint, Config) ->
    [{get_status_path, ["api", "v5", "status"]} | Config];
init_per_group(non_api_status_endpoint, Config) ->
    [{get_status_path, ["status"]} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

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
    QS = maps:get(qs, Opts, ""),
    URL = ?HOST ++ filename:join(Path0),
    {ok, #{host := Host, port := Port, path := Path1}} = emqx_http_lib:uri_parse(URL),
    Path = Path1 ++ QS,
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

t_status_ok(Config) ->
    Path = ?config(get_status_path, Config),
    #{
        body := Resp,
        status_code := StatusCode
    } = do_request(#{
        method => get,
        path => Path,
        headers => [],
        body => no_body
    }),
    ?assertEqual(200, StatusCode),
    ?assertMatch(
        {match, _},
        re:run(Resp, <<"emqx is running$">>)
    ),
    ok.

t_status_not_ok(Config) ->
    Path = ?config(get_status_path, Config),
    #{
        body := Resp,
        headers := Headers,
        status_code := StatusCode
    } = do_request(#{
        method => get,
        path => Path,
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

t_status_text_format(Config) ->
    Path = ?config(get_status_path, Config),
    #{
        body := Resp,
        status_code := StatusCode
    } = do_request(#{
        method => get,
        path => Path,
        qs => "?format=text",
        headers => [],
        body => no_body
    }),
    ?assertEqual(200, StatusCode),
    ?assertMatch(
        {match, _},
        re:run(Resp, <<"emqx is running$">>)
    ),
    ok.

t_status_json_format(Config) ->
    Path = ?config(get_status_path, Config),
    #{
        body := Resp,
        status_code := StatusCode
    } = do_request(#{
        method => get,
        path => Path,
        qs => "?format=json",
        headers => [],
        body => no_body
    }),
    ?assertEqual(200, StatusCode),
    ?assertMatch(
        #{<<"app_status">> := <<"running">>},
        emqx_utils_json:decode(Resp)
    ),
    ok.

t_status_bad_format_qs(Config) ->
    lists:foreach(
        fun(QS) ->
            test_status_bad_format_qs(QS, Config)
        end,
        [
            "?a=b",
            "?format=",
            "?format=x"
        ]
    ).

%% when query-sting is invalid, fallback to text format
test_status_bad_format_qs(QS, Config) ->
    Path = ?config(get_status_path, Config),
    #{
        body := Resp,
        status_code := StatusCode
    } = do_request(#{
        method => get,
        path => Path,
        qs => QS,
        headers => [],
        body => no_body
    }),
    ?assertEqual(200, StatusCode),
    ?assertMatch(
        {match, _},
        re:run(Resp, <<"emqx is running$">>)
    ),
    ok.
