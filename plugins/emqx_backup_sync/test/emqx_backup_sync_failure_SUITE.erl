%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_backup_sync_failure_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [
        t_real_sync_cancelled_during_download_cleans_up_without_import,
        t_real_sync_cancelled_during_import_finishes_and_reports_success,
        t_real_sync_reports_partial_database_failure,
        t_real_sync_reports_failure_stages,
        t_real_sync_reports_observable_failure
    ].

suite() ->
    emqx_backup_sync_SUITE:suite().

init_per_testcase(TestCase, Config) ->
    emqx_backup_sync_SUITE:init_per_testcase(TestCase, Config).

end_per_testcase(TestCase, Config) ->
    emqx_backup_sync_SUITE:end_per_testcase(TestCase, Config).

t_real_sync_cancelled_during_download_cleans_up_without_import(Config) ->
    emqx_backup_sync_SUITE:t_real_sync_cancelled_during_download_cleans_up_without_import(Config).

t_real_sync_cancelled_during_import_finishes_and_reports_success(Config) ->
    emqx_backup_sync_SUITE:t_real_sync_cancelled_during_import_finishes_and_reports_success(Config).

t_real_sync_reports_partial_database_failure(Config) ->
    emqx_backup_sync_SUITE:t_real_sync_reports_partial_database_failure(Config).

t_real_sync_reports_failure_stages(Config) ->
    emqx_backup_sync_SUITE:t_real_sync_reports_failure_stages(Config).

t_real_sync_reports_observable_failure(Config) ->
    emqx_backup_sync_SUITE:t_real_sync_reports_observable_failure(Config).
