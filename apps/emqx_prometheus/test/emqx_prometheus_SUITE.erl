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
-define(CONF_DEFAULT, <<
    "\n"
    "prometheus {\n"
    "  push_gateway_server = \"http://127.0.0.1:9091\"\n"
    "  interval = \"1s\"\n"
    "  headers = { Authorization = \"some-authz-tokens\"}\n"
    "  enable = true\n"
    "  vm_dist_collector = enabled\n"
    "  mnesia_collector = enabled\n"
    "  vm_statistics_collector = disabled\n"
    "  vm_system_info_collector = disabled\n"
    "  vm_memory_collector = enabled\n"
    "  vm_msacc_collector = enabled\n"
    "}\n"
>>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Cfg) ->
    application:load(emqx_conf),
    ok = ekka:start(),
    ok = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity),
    meck:new(emqx_alarm, [non_strict, passthrough, no_link]),
    meck:expect(emqx_alarm, activate, 3, ok),
    meck:expect(emqx_alarm, deactivate, 3, ok),

    load_config(),
    emqx_common_test_helpers:start_apps([emqx_prometheus]),
    Cfg.

end_per_suite(_Cfg) ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    meck:unload(emqx_alarm),

    emqx_common_test_helpers:stop_apps([emqx_prometheus]).

load_config() ->
    ok = emqx_common_test_helpers:load_config(emqx_prometheus_schema, ?CONF_DEFAULT).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_start_stop(_) ->
    App = emqx_prometheus,
    ?assertMatch(ok, emqx_prometheus_sup:start_child(App)),
    %% start twice return ok.
    ?assertMatch(ok, emqx_prometheus_sup:start_child(App)),
    ?assertMatch(ok, emqx_prometheus_sup:stop_child(App)),
    %% stop twice return ok.
    ?assertMatch(ok, emqx_prometheus_sup:stop_child(App)),
    %% wait the interval timer trigger
    timer:sleep(2000).

t_collector_no_crash_test(_) ->
    prometheus_text_format:format(),
    ok.

t_assert_push(_) ->
    meck:new(httpc, [passthrough]),
    Self = self(),
    AssertPush = fun(Method, Req = {Url, Headers, ContentType, _Data}, HttpOpts, Opts) ->
        ?assertEqual(post, Method),
        ?assertMatch("http://127.0.0.1:9091/metrics/job/" ++ _, Url),
        ?assertEqual([{"Authorization", "some-authz-tokens"}], Headers),
        ?assertEqual("text/plain", ContentType),
        Self ! pass,
        meck:passthrough([Method, Req, HttpOpts, Opts])
    end,
    meck:expect(httpc, request, AssertPush),
    ?assertMatch(ok, emqx_prometheus_sup:start_child(emqx_prometheus)),
    receive
        pass -> ok
    after 2000 ->
        ct:fail(assert_push_request_failed)
    end.

t_only_for_coverage(_) ->
    ?assertEqual("5.0.0", emqx_prometheus_proto_v1:introduced_in()),
    ok.
