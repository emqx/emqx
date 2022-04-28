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
    "  enable = true\n"
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
    ?assertMatch(ok, emqx_prometheus:start()),
    ?assertMatch(ok, emqx_prometheus:stop()),
    ?assertMatch(ok, emqx_prometheus:restart()),
    %% wait the interval timer tigger
    timer:sleep(2000).

t_test(_) ->
    ok.

t_only_for_coverage(_) ->
    ?assertEqual("5.0.0", emqx_prometheus_proto_v1:introduced_in()),
    ok.
