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

-module(emqx_prometheus_SUITE).

-include_lib("stdlib/include/assert.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).
-define(LEGACY_CONF_DEFAULT, <<
    "\n"
    "prometheus {\n"
    "  push_gateway_server = \"http://127.0.0.1:9091\"\n"
    "  interval = \"1s\"\n"
    "  headers = { Authorization = \"some-authz-tokens\"}\n"
    "  job_name = \"${name}~${host}\"\n"
    "  enable = true\n"
    "  vm_dist_collector = disabled\n"
    "  mnesia_collector = disabled\n"
    "  vm_statistics_collector = disabled\n"
    "  vm_system_info_collector = disabled\n"
    "  vm_memory_collector = disabled\n"
    "  vm_msacc_collector = disabled\n"
    "}\n"
>>).
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
    init_group(),
    load_config(),
    emqx_common_test_helpers:start_apps([emqx_prometheus]),
    %% coverage olp metrics
    {ok, _} = emqx:update_config([overload_protection, enable], true),
    Config;
init_per_group(legacy_config, Config) ->
    init_group(),
    load_legacy_config(),
    emqx_common_test_helpers:start_apps([emqx_prometheus]),
    {ok, _} = emqx:update_config([overload_protection, enable], false),
    Config.

init_group() ->
    application:load(emqx_conf),
    ok = ekka:start(),
    ok = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity),
    meck:new(emqx_alarm, [non_strict, passthrough, no_link]),
    meck:expect(emqx_alarm, activate, 3, ok),
    meck:expect(emqx_alarm, deactivate, 3, ok),
    meck:new(emqx_license_checker, [non_strict, passthrough, no_link]),
    meck:expect(emqx_license_checker, expiry_epoch, fun() -> 1859673600 end).

end_group() ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    meck:unload(emqx_alarm),
    meck:unload(emqx_license_checker),
    emqx_common_test_helpers:stop_apps([emqx_prometheus]).

end_per_group(_Group, Config) ->
    end_group(),
    Config.

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

load_config() ->
    ok = emqx_common_test_helpers:load_config(emqx_prometheus_schema, ?CONF_DEFAULT).

load_legacy_config() ->
    ok = emqx_common_test_helpers:load_config(emqx_prometheus_schema, ?LEGACY_CONF_DEFAULT).

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

start_mock_pushgateway(Port) ->
    application:ensure_all_started(cowboy),
    Dispatch = cowboy_router:compile([{'_', [{'_', ?MODULE, []}]}]),
    {ok, _} = cowboy:start_clear(
        mock_pushgateway_listener,
        [{port, Port}],
        #{env => #{dispatch => Dispatch}}
    ).

stop_mock_pushgateway() ->
    cowboy:stop_listener(mock_pushgateway_listener).

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
