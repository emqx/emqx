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

-module(emqx_machine_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% CASE-SIDE-EFFICT:
    %%
    %% Running-Seq:
    %%   emqx_authz_api_mnesia_SUITE.erl
    %%   emqx_gateway_api_SUITE.erl
    %%   emqx_machine_SUITE.erl
    %%
    %% Reason:
    %%   the `emqx_machine_boot:ensure_apps_started()` will crash
    %%   on starting `emqx_authz` with dirty confs, which caused the file
    %%   `.._build/test/lib/emqx_conf/etc/acl.conf` could not be found
    %%
    %% Workaround:
    %%   Unload emqx_authz to avoid reboot this application
    %%
    application:unload(emqx_authz),
    emqx_common_test_helpers:start_apps([emqx_conf]),
    application:set_env(emqx_machine, applications, [
        emqx_prometheus,
        emqx_modules,
        emqx_dashboard,
        emqx_gateway,
        emqx_statsd,
        emqx_resource,
        emqx_rule_engine,
        emqx_bridge,
        emqx_plugin_libs,
        emqx_management,
        emqx_retainer,
        emqx_exhook,
        emqx_authn,
        emqx_authz,
        emqx_plugin
    ]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

init_per_testcase(t_custom_shard_transports, Config) ->
    OldConfig = application:get_env(emqx_machine, custom_shard_transports),
    [{old_config, OldConfig} | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_custom_shard_transports, Config) ->
    OldConfig0 = ?config(old_config, Config),
    application:stop(ekka),
    case OldConfig0 of
        {ok, OldConfig} ->
            application:set_env(emqx_machine, custom_shard_transports, OldConfig);
        undefined ->
            application:unset_env(emqx_machine, custom_shard_transports)
    end,
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

t_shutdown_reboot(_Config) ->
    emqx_machine_boot:stop_apps(),
    false = emqx:is_running(node()),
    emqx_machine_boot:ensure_apps_started(),
    true = emqx:is_running(node()),
    ok = emqx_machine_boot:stop_apps(),
    false = emqx:is_running(node()).

t_custom_shard_transports(_Config) ->
    %% used to ensure the atom exists
    Shard = test_shard,
    %% the config keys are binaries
    ShardBin = atom_to_binary(Shard),
    DefaultTransport = gen_rpc,
    ?assertEqual(DefaultTransport, mria_config:shard_transport(Shard)),
    application:set_env(emqx_machine, custom_shard_transports, #{ShardBin => distr}),
    emqx_machine:start(),
    ?assertEqual(distr, mria_config:shard_transport(Shard)),
    ok.
