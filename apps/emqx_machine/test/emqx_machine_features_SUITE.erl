%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_machine_features_SUITE).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ENV_VAR, "EMQX_FEATURES").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:start_trace(),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

setup_env(EnvVar) ->
    on_exit(fun clear_env/0),
    os:putenv(?ENV_VAR, EnvVar),
    emqx_machine_features:clear_features(),
    ok.

clear_env() ->
    os:unsetenv(?ENV_VAR),
    emqx_machine_features:clear_features(),
    ok.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_boot_bad_features(TCConfig) ->
    ?check_trace(
        begin
            Nodes =
                [N] = emqx_cth_cluster:start(
                    [{machine_bad_feat1, #{apps => []}}],
                    #{
                        work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, TCConfig),
                        env_vars => [{"EMQX_FEATURES", "unknown"}]
                    }
                ),
            %% hack to collect cover data, since the node will terminate abruptly.
            snabbkaffe_nemesis:inject_crash(
                ?match_event(#{?snk_kind := "invalid_feature_specification"}),
                fun(_) ->
                    emqx_cth_cluster:when_cover_enabled(fun() -> ok = cover:flush([N]) end),
                    false
                end
            ),
            try
                ?ON(N, application:ensure_all_started(emqx_machine)),
                ct:fail("should not have successfully booted")
            catch
                exit:{test_case_failed, _} = Exit ->
                    exit(Exit);
                K:E ->
                    ct:pal("crashed with: {~p, ~p}", [K, E]),
                    ok
            after
                emqx_cth_cluster:stop(Nodes)
            end
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        known := [_ | _],
                        reason := unknown_feature,
                        feature := "unknown",
                        hint := _
                    }
                ],
                ?of_kind(["invalid_feature_specification"], Trace)
            ),
            ok
        end
    ),
    ok.
