%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_backup_sync_primary_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [
        t_real_primary_sync_builtin_auth_runtime,
        t_real_primary_sync_repeated_updates_and_config_only_tables,
        t_real_primary_sync_deletes_default_table_data,
        t_real_primary_sync_deletes_retained_messages
    ].

suite() ->
    emqx_backup_sync_SUITE:suite().

init_per_testcase(TestCase, Config) ->
    emqx_backup_sync_SUITE:init_per_testcase(TestCase, Config).

end_per_testcase(TestCase, Config) ->
    emqx_backup_sync_SUITE:end_per_testcase(TestCase, Config).

t_real_primary_sync_builtin_auth_runtime(Config) ->
    emqx_backup_sync_SUITE:t_real_primary_sync_builtin_auth_runtime(Config).

t_real_primary_sync_repeated_updates_and_config_only_tables(Config) ->
    emqx_backup_sync_SUITE:t_real_primary_sync_repeated_updates_and_config_only_tables(Config).

t_real_primary_sync_deletes_default_table_data(Config) ->
    emqx_backup_sync_SUITE:t_real_primary_sync_deletes_default_table_data(Config).

t_real_primary_sync_deletes_retained_messages(Config) ->
    emqx_backup_sync_SUITE:t_real_primary_sync_deletes_retained_messages(Config).
