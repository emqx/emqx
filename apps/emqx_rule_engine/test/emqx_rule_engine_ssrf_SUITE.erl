%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_ssrf_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(CACHE_KEY, {emqx_utils_ssrf, cache}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_rule_engine
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Snapshot raw config and cache so each test starts from boot defaults.
    OldRaw = emqx_conf:get_raw([<<"rule_engine">>], #{}),
    OldCache = persistent_term:get(?CACHE_KEY, undefined),
    [{old_raw_rule_engine, OldRaw}, {old_ssrf_cache, OldCache} | Config].

end_per_testcase(_TestCase, Config) ->
    OldRaw = ?config(old_raw_rule_engine, Config),
    {ok, _} = emqx_conf:update(
        [rule_engine], OldRaw, #{override_to => cluster}
    ),
    case ?config(old_ssrf_cache, Config) of
        undefined -> _ = persistent_term:erase(?CACHE_KEY);
        Cache -> persistent_term:put(?CACHE_KEY, Cache)
    end,
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
Regression for the runtime SSRF cache-refresh bug, driving the same
code path the reporter's `PUT /api/v5/configs?mode=merge` curl hits.
`emqx_conf_cli:load_config(?global_ns, Hocon, #{mode => merge})` is the
exact function the REST handler invokes (see `emqx_mgmt_api_configs:316`),
so this test covers the parent-path update without needing to spin up
the dashboard HTTP listener.

Internally that path updates the config tree at the parent
`[rule_engine]` path and relies on the framework to propagate into the
`[rule_engine, ssrf]` sub-handler via `propagated_post_config_update/5`.
If the rule-engine schema doesn't export that callback, the
`persistent_term` policy cache stays at the boot-time default
(`enable => false`) and `check_host/1` short-circuits on every URL.
""".
t_runtime_parent_path_update_via_rest_api(_Config) ->
    %% Boot default: SSRF disabled.
    ?assertMatch(#{enable := false}, ssrf_cache()),

    %% Same shape as the reporter's curl:
    %%   curl -X PUT '.../api/v5/configs?mode=merge' \
    %%        --data-binary 'rule_engine.ssrf.enable = true ...'
    EnableHocon = <<
        "rule_engine.ssrf.enable = true\n"
        "rule_engine.ssrf.deny_cidrs = [\"127.0.0.0/8\"]\n"
    >>,
    ok = load_configs_merge(EnableHocon),

    %% The config tree itself reflects the change either way.
    ?assertMatch(
        #{enable := true},
        emqx_conf:get([rule_engine, ssrf])
    ),

    %% The persistent_term cache must also be refreshed — this is the
    %% assertion that fails before the fix.
    ?assertMatch(#{enable := true}, ssrf_cache()),

    %% And the helper used by the connector layer must now actively deny
    %% loopback (default `deny_cidrs` covers `127.0.0.0/8`).
    ?assertMatch(
        {error, {denied, _, _, _}},
        emqx_utils_ssrf:check_host(<<"127.0.0.1">>)
    ),

    %% Symmetric path: disabling SSRF via the same endpoint must also
    %% propagate, otherwise the cache stays in deny-loopback mode and
    %% the operator can no longer create a legitimate loopback
    %% connector.
    ok = load_configs_merge(<<"rule_engine.ssrf.enable = false\n">>),
    ?assertMatch(#{enable := false}, ssrf_cache()),
    ?assertEqual(ok, emqx_utils_ssrf:check_host(<<"127.0.0.1">>)),
    ok.

-doc """
Control case: updating the SSRF sub-path directly already works on the
buggy code (the non-propagated `post_config_update/5` callback fires).
This case keeps the two update paths covered side by side so a future
regression cannot silently break either one.

There is no public REST endpoint for the `[rule_engine, ssrf]` sub-path
so this case talks to `emqx_conf:update/3` directly.
""".
t_runtime_direct_sub_path_update_refreshes_cache(_Config) ->
    ?assertMatch(#{enable := false}, ssrf_cache()),

    OldSsrfRaw = emqx_conf:get_raw([<<"rule_engine">>, <<"ssrf">>], #{}),
    NewSsrfRaw = OldSsrfRaw#{<<"enable">> => true},
    {ok, _} = emqx_conf:update(
        [rule_engine, ssrf], NewSsrfRaw, #{override_to => cluster}
    ),

    ?assertMatch(#{enable := true}, ssrf_cache()),
    ?assertMatch(
        {error, {denied, _, _, _}},
        emqx_utils_ssrf:check_host(<<"127.0.0.1">>)
    ),
    ok.

-doc """
Malformed CIDR strings — most commonly an IPv4 address with fewer than
four octets such as `10.0.0/24` — must be rejected by the schema
validator. `inet:parse_address/1` silently expands them BSD-style
(`10.0.0` → `10.0.0.0`, `10` → `0.0.0.10`), which would otherwise let
an operator quietly cover a different range than intended.
""".
t_invalid_cidr_is_rejected(_Config) ->
    %% Direct compile path: bad input throws with the offending string;
    %% good input still compiles cleanly (IPv6 `::` shorthand included).
    ?assertThrow(
        {invalid_cidr, <<"10.0.0/24">>},
        emqx_utils_ssrf:compile_cidrs([<<"10.0.0/24">>])
    ),
    ?assertThrow(
        {invalid_cidr, <<"10/8">>},
        emqx_utils_ssrf:compile_cidrs([<<"10/8">>])
    ),
    _ = emqx_utils_ssrf:compile_cidrs(
        [<<"10.0.0.0/24">>, <<"::1/128">>, <<"fe80::/10">>]
    ),

    %% End-to-end via the same code path operators hit through REST: a
    %% malformed CIDR in the body is rejected and the cache is left
    %% untouched.
    Hocon = <<
        "rule_engine.ssrf.enable = true\n"
        "rule_engine.ssrf.deny_cidrs = [\"10.0.0/24\"]\n"
    >>,
    ?assertMatch({error, _}, load_configs_merge(Hocon)),
    ?assertMatch(#{enable := false}, ssrf_cache()),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

ssrf_cache() ->
    persistent_term:get(?CACHE_KEY, #{enable => false}).

%% Mirror exactly what `PUT /api/v5/configs?mode=merge` does internally
%% (see `emqx_mgmt_api_configs:configs/3` → `emqx_conf_cli:load_config/3`).
%% Bypasses the HTTP listener so this suite doesn't need to start
%% `emqx_management` / `emqx_dashboard` — those drag in a swagger spec
%% rebuild that's vulnerable to lingering connector_info cache state
%% from earlier suites in the same CT run.
load_configs_merge(Hocon) ->
    emqx_conf_cli:load_config(?global_ns, Hocon, #{
        mode => merge, log => none, ignore_readonly => false
    }).
