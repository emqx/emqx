%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_backup_sync_files_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [
        t_real_primary_sync_file_backed_schema_registry,
        t_real_primary_sync_listener_cert_files,
        t_real_primary_sync_connector_action_cert_files
    ].

suite() ->
    emqx_backup_sync_SUITE:suite().

init_per_testcase(TestCase, Config) ->
    emqx_backup_sync_SUITE:init_per_testcase(TestCase, Config).

end_per_testcase(TestCase, Config) ->
    emqx_backup_sync_SUITE:end_per_testcase(TestCase, Config).

t_real_primary_sync_file_backed_schema_registry(Config) ->
    emqx_backup_sync_SUITE:t_real_primary_sync_file_backed_schema_registry(Config).

t_real_primary_sync_listener_cert_files(Config) ->
    emqx_backup_sync_SUITE:t_real_primary_sync_listener_cert_files(Config).

t_real_primary_sync_connector_action_cert_files(Config) ->
    emqx_backup_sync_SUITE:t_real_primary_sync_connector_action_cert_files(Config).
