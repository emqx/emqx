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

-module(emqx_prometheus_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(nowarn_export_all).
-compile(export_all).

%% erlfmt-ignore
-define(LEGACY_CONF_DEFAULT, <<"
prometheus {
    push_gateway_server = \"http://127.0.0.1:9091\"
    interval = \"1s\"
    headers = { Authorization = \"some-authz-tokens\"}
    job_name = \"${name}~${host}\"
    enable = true
    vm_dist_collector = disabled
    mnesia_collector = disabled
    vm_statistics_collector = disabled
    vm_system_info_collector = disabled
    vm_memory_collector = disabled
    vm_msacc_collector = disabled
}
">>).

-define(CONF_DEFAULT, #{
    <<"prometheus">> =>
        #{
            <<"enable_basic_auth">> => false,
            <<"collectors">> =>
                #{
                    <<"mnesia">> => <<"disabled">>,
                    <<"vm_dist">> => <<"disabled">>,
                    <<"vm_memory">> => <<"disabled">>,
                    <<"vm_msacc">> => <<"disabled">>,
                    <<"vm_statistics">> => <<"disabled">>,
                    <<"vm_system_info">> => <<"disabled">>
                },
            <<"push_gateway">> =>
                #{
                    <<"enable">> => true,
                    <<"headers">> => #{<<"Authorization">> => <<"some-authz-tokens">>},
                    <<"interval">> => <<"1s">>,
                    <<"job_name">> => <<"${name}~${host}">>,
                    <<"url">> => <<"http://127.0.0.1:9091">>
                }
        }
}).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    [
        {group, new_config},
        {group, legacy_config}
    ].

groups() ->
    [
        {new_config, [sequence], common_tests()},
        {legacy_config, [sequence], common_tests()}
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

common_tests() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_group(new_config, Config) ->
    Apps = emqx_cth_suite:start(
        lists:flatten([
            %% coverage olp metrics
            {emqx_conf, "overload_protection.enable = true"},
            emqx,
            [
                {emqx_license, "license.key = default"}
             || emqx_release:edition() == ee
            ],
            {emqx_prometheus, #{config => config(default)}},
            emqx_management
        ]),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config];
init_per_group(legacy_config, Config) ->
    Apps = emqx_cth_suite:start(
        lists:flatten([
            {emqx_conf, "overload_protection.enable = true"},
            emqx,
            [
                {emqx_license, "license.key = default"}
             || emqx_release:edition() == ee
            ],
            {emqx_prometheus, #{config => config(legacy)}},
            emqx_management
        ]),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_group(_Group, Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(t_assert_push, Config) ->
    meck:new(httpc, [passthrough]),
    Config;
init_per_testcase(t_push_gateway, Config) ->
    start_mock_pushgateway(9091),
    Config;
init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(t_push_gateway, Config) ->
    stop_mock_pushgateway(),
    Config;
end_per_testcase(t_assert_push, _Config) ->
    meck:unload(httpc),
    ok;
end_per_testcase(_Testcase, _Config) ->
    ok.

config(default) ->
    ?CONF_DEFAULT;
config(legacy) ->
    ?LEGACY_CONF_DEFAULT.

conf_default() ->
    ?CONF_DEFAULT.

legacy_conf_default() ->
    ?LEGACY_CONF_DEFAULT.

maybe_meck_license() ->
    case emqx_release:edition() of
        ce ->
            ok;
        ee ->
            meck:new(emqx_license_checker, [non_strict, passthrough, no_link]),
            meck:expect(emqx_license_checker, expiry_epoch, fun() -> 1859673600 end)
    end.

maybe_unmeck_license() ->
    case emqx_release:edition() of
        ce -> ok;
        ee -> meck:unload(emqx_license_checker)
    end.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_start_stop(_) ->
    App = emqx_prometheus,
    Conf = emqx_prometheus_config:conf(),
    ?assertMatch(ok, emqx_prometheus_sup:start_child(App, Conf)),
    %% start twice return ok.
    ?assertMatch(ok, emqx_prometheus_sup:start_child(App, Conf)),
    ok = gen_server:call(emqx_prometheus, dump, 1000),
    ok = gen_server:cast(emqx_prometheus, dump),
    dump = erlang:send(emqx_prometheus, dump),
    ?assertMatch(ok, emqx_prometheus_sup:stop_child(App)),
    %% stop twice return ok.
    ?assertMatch(ok, emqx_prometheus_sup:stop_child(App)),
    %% wait the interval timer trigger
    timer:sleep(2000).

t_collector_no_crash_test(_) ->
    prometheus_text_format:format(),
    ok.

t_assert_push(_) ->
    Self = self(),
    AssertPush = fun(Method, Req = {Url, Headers, ContentType, _Data}, HttpOpts, Opts) ->
        ?assertEqual(post, Method),
        ?assertMatch("http://127.0.0.1:9091/metrics/job/test~127.0.0.1", Url),
        ?assertEqual([{"Authorization", "some-authz-tokens"}], Headers),
        ?assertEqual("text/plain", ContentType),
        Self ! pass,
        meck:passthrough([Method, Req, HttpOpts, Opts])
    end,
    meck:expect(httpc, request, AssertPush),
    Conf = emqx_prometheus_config:conf(),
    ?assertMatch(ok, emqx_prometheus_sup:start_child(emqx_prometheus, Conf)),
    receive
        pass -> ok
    after 2000 ->
        ct:fail(assert_push_request_failed)
    end.

t_push_gateway(_) ->
    Conf = emqx_prometheus_config:conf(),
    ?assertMatch(ok, emqx_prometheus_sup:stop_child(emqx_prometheus)),
    ?assertMatch(ok, emqx_prometheus_sup:start_child(emqx_prometheus, Conf)),
    ?assertMatch(#{ok := 0, failed := 0}, emqx_prometheus:info()),
    timer:sleep(1100),
    ?assertMatch(#{ok := 1, failed := 0}, emqx_prometheus:info()),
    ok = emqx_prometheus_sup:update_child(emqx_prometheus, Conf),
    ?assertMatch(#{ok := 0, failed := 0}, emqx_prometheus:info()),

    ok.

t_cert_expiry_epoch(_) ->
    Path = some_pem_path(),
    ?assertEqual(
        2666082573,
        emqx_prometheus:cert_expiry_at_from_path(Path)
    ).

%%--------------------------------------------------------------------
%% Helper functions

start_mock_pushgateway(Port) ->
    ensure_loaded(cowboy),
    ensure_loaded(ranch),
    {ok, _} = application:ensure_all_started(cowboy),
    Dispatch = cowboy_router:compile([{'_', [{'_', ?MODULE, []}]}]),
    {ok, _} = cowboy:start_clear(
        mock_pushgateway_listener,
        [{port, Port}],
        #{env => #{dispatch => Dispatch}}
    ).

ensure_loaded(App) ->
    case application:load(App) of
        ok -> ok;
        {error, {already_loaded, _}} -> ok
    end.

stop_mock_pushgateway() ->
    cowboy:stop_listener(mock_pushgateway_listener),
    ok = application:stop(cowboy),
    ok = application:stop(ranch).

init(Req0, Opts) ->
    Method = cowboy_req:method(Req0),
    Headers = cowboy_req:headers(Req0),
    ?assertEqual(<<"POST">>, Method),
    ?assertMatch(
        #{
            <<"authorization">> := <<"some-authz-tokens">>,
            <<"content-length">> := _,
            <<"content-type">> := <<"text/plain">>,
            <<"host">> := <<"127.0.0.1:9091">>
        },
        Headers
    ),
    RespHeader = #{<<"content-type">> => <<"text/plain; charset=utf-8">>},
    Req = cowboy_req:reply(200, RespHeader, <<"OK">>, Req0),
    {ok, Req, Opts}.

some_pem_path() ->
    Dir = code:lib_dir(emqx_prometheus),
    _Path = filename:join([Dir, "test", "data", "cert.crt"]).
