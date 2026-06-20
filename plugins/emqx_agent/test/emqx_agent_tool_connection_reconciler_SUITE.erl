%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_connection_reconciler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_resource, emqx_agent],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_plugin_config_fixture:setup(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    _ = catch meck:unload(emqx_agent_tool_connections),
    ok = emqx_agent_plugin_config_fixture:teardown(),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Call retry and verify that reconcile is called within 200ms
t_retry_triggers_reconcile(_Config) ->
    ok = meck:new(emqx_agent_tool_connections, [passthrough]),
    ok = meck:expect(emqx_agent_tool_connections, reconcile, fun() -> ok end),
    ok = emqx_agent_tool_connection_reconciler:retry(),
    ct:sleep(200),
    ?assertEqual(1, meck:num_calls(emqx_agent_tool_connections, reconcile, 0)).
