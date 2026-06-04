%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_config_sync_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(CORE_NODES_TAB, emqx_cluster_config_sync_SUITE_core_nodes).
-define(INTEGRATION_RULE_ID, <<"cluster_config_sync_integration_rule">>).
-define(INTEGRATION_BANNED_CLIENTID, <<"cluster_config_sync_banned_client">>).
-define(INTEGRATION_AUTHN_USER, <<"cluster_config_sync_authn_user">>).
-define(INTEGRATION_AUTHZ_USERNAME, <<"cluster_config_sync_authz_user">>).
-define(INTEGRATION_RETAINER_TOPIC, <<"cluster_config_sync/retained">>).
-define(INTEGRATION_RETAINER_PAYLOAD, <<"cluster config sync retained payload">>).
-define(INTEGRATION_SCHEMA_NAME, <<"cluster_config_sync_schema">>).
-define(INTEGRATION_TLS_LISTENER_NAME, cluster_config_sync_tls).
-define(INTEGRATION_TLS_LISTENER_NAME_BIN, <<"cluster_config_sync_tls">>).
-define(INTEGRATION_TLS_LISTENER_PORT, 29249).
-define(PRIMARY_API_PORT, 28249).
-define(PRIMARY_BASE_PORT, 20100).
-define(CLUSTER_HOCON_FILENAME, "cluster.hocon").

all() ->
    [
        t_enabled_secondary,
        t_interval_ms,
        t_sync_runs_async_and_health_check_does_not_block,
        t_sync_does_not_reenter_while_worker_running,
        t_sync_runs_only_on_selected_core,
        t_sync_rechecks_selected_core_in_worker,
        t_sync_skips_when_no_running_core_nodes,
        t_sync_skips_when_core_membership_unavailable,
        t_non_selected_core_takes_over_on_next_interval,
        t_sync_skips_when_cluster_lock_is_held,
        t_config_change_cancels_running_worker_gracefully,
        t_config_change_reports_cancel_timeout,
        t_terminate_waits_for_worker_cleanup,
        t_sync_once_uses_default_table_sets,
        t_sync_once_exports_downloads_imports_and_cleans_up,
        t_sync_once_cleans_up_when_cancelled_after_export,
        t_sync_once_reports_import_errors,
        t_sync_once_reports_cleanup_errors,
        t_sync_once_reports_remote_export_errors,
        t_real_cluster_sync_runs_on_one_core_only,
        t_real_primary_sync_respects_root_keys_and_table_sets,
        t_real_primary_sync_file_backed_schema_registry,
        t_real_primary_sync_listener_cert_files,
        t_real_sync_reports_observable_failure
    ].

suite() ->
    [{timetrap, {minutes, 3}}].

init_per_testcase(_TestCase, Config) ->
    cleanup_test_artifacts(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    cleanup_test_artifacts().

t_enabled_secondary(_Config) ->
    ?assertEqual(false, emqx_cluster_config_sync:enabled_secondary(#{})),
    ?assertEqual(
        false,
        emqx_cluster_config_sync:enabled_secondary(#{
            <<"enable">> => true,
            <<"role">> => <<"primary">>
        })
    ),
    ?assertEqual(
        true,
        emqx_cluster_config_sync:enabled_secondary(#{
            <<"enable">> => true,
            <<"role">> => <<"secondary">>
        })
    ).

t_interval_ms(_Config) ->
    ?assertEqual(60000, emqx_cluster_config_sync:interval_ms(sync_config(<<"1m">>))),
    ?assertEqual(300000, emqx_cluster_config_sync:interval_ms(sync_config(<<"invalid">>))).

t_sync_runs_async_and_health_check_does_not_block(_Config) ->
    setup_core_node(),
    setup_blocking_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    Pid ! sync,
    SyncPid = assert_sync_started(),
    Parent = self(),
    HealthCheck = spawn(fun() ->
        Parent ! {health_check, emqx_cluster_config_sync:on_health_check()}
    end),
    receive
        {health_check, Reply} ->
            ?assertEqual(ok, Reply)
    after 200 ->
        exit(HealthCheck, kill),
        SyncPid ! finish_sync,
        error(health_check_blocked)
    end,
    SyncPid ! finish_sync,
    assert_sync_finished().

t_sync_does_not_reenter_while_worker_running(_Config) ->
    setup_core_node(),
    setup_blocking_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    Pid ! sync,
    SyncPid = assert_sync_started(),
    Pid ! sync,
    assert_no_sync_started(100),
    SyncPid ! finish_sync,
    assert_sync_finished(),
    assert_no_sync_started(200).

t_sync_runs_only_on_selected_core(_Config) ->
    OtherCore = list_to_atom("a" ++ atom_to_list(node())),
    setup_core_nodes([node(), OtherCore]),
    setup_counting_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    Pid ! sync,
    assert_no_sync_started(200),
    ?assertEqual(ok, emqx_cluster_config_sync:on_health_check()).

t_sync_rechecks_selected_core_in_worker(_Config) ->
    OtherCore = list_to_atom("a" ++ atom_to_list(node())),
    setup_core_nodes_sequence([[node()], [OtherCore, node()]]),
    setup_counting_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    Pid ! sync,
    assert_no_sync_started(200),
    ?assertEqual(ok, emqx_cluster_config_sync:on_health_check()).

t_sync_skips_when_no_running_core_nodes(_Config) ->
    setup_core_nodes([]),
    setup_counting_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    Pid ! sync,
    assert_no_sync_started(200),
    ?assertEqual(ok, emqx_cluster_config_sync:on_health_check()).

t_sync_skips_when_core_membership_unavailable(_Config) ->
    setup_core_membership_unavailable(),
    setup_counting_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    Pid ! sync,
    assert_no_sync_started(200),
    ?assertEqual(ok, emqx_cluster_config_sync:on_health_check()).

t_non_selected_core_takes_over_on_next_interval(_Config) ->
    OtherCore = list_to_atom("a" ++ atom_to_list(node())),
    setup_dynamic_core_nodes([node(), OtherCore]),
    setup_counting_sync(),
    _Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(
        #{}, conf(<<"http://primary:18083/api/v5/">>, <<"100ms">>)
    ),

    assert_no_sync_started(200),
    set_core_nodes([node()]),
    _SyncPid = assert_sync_started().

t_sync_skips_when_cluster_lock_is_held(_Config) ->
    setup_core_node(),
    setup_counting_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),
    LockPid = hold_sync_lock(),

    Pid ! sync,
    assert_no_sync_started(200),
    ?assertEqual(ok, emqx_cluster_config_sync:on_health_check()),

    LockPid ! release_lock.

t_config_change_cancels_running_worker_gracefully(_Config) ->
    setup_core_node(),
    setup_cancellable_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    Pid ! sync,
    SyncPid = assert_sync_started(),
    Ref = erlang:monitor(process, SyncPid),
    ok = emqx_cluster_config_sync:on_config_changed(conf(), (conf())#{<<"enable">> => false}),
    SyncPid ! check_cancelled,
    assert_sync_cancel_requested(SyncPid),
    assert_sync_cleanup(SyncPid),
    assert_sync_down(Ref, SyncPid).

t_config_change_reports_cancel_timeout(_Config) ->
    setup_core_node(),
    setup_cancel_ignoring_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    Pid ! sync,
    SyncPid = assert_sync_started(),
    Ref = erlang:monitor(process, SyncPid),
    try
        ok = emqx_cluster_config_sync:on_config_changed(
            conf(), conf(<<"http://other:18083/api/v5/">>)
        ),
        wait_health_error(<<"worker_cancel_timeout">>),
        assert_worker_alive(Ref, SyncPid)
    after
        SyncPid ! stop_ignoring_sync,
        assert_sync_down(Ref, SyncPid)
    end.

t_terminate_waits_for_worker_cleanup(_Config) ->
    ?assertMatch(#{shutdown := infinity}, emqx_cluster_config_sync:child_spec()),
    setup_core_node(),
    setup_shutdown_cancellable_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    Pid ! sync,
    SyncPid = assert_sync_started(),
    Ref = erlang:monitor(process, SyncPid),
    Parent = self(),
    Stopper = spawn(fun() ->
        Parent ! {server_stopped, gen_server:stop(Pid, shutdown, 5000)}
    end),
    assert_sync_cancel_requested(SyncPid),
    assert_no_server_stopped(Stopper, 100),
    SyncPid ! finish_cleanup,
    assert_sync_cleanup(SyncPid),
    assert_sync_down(Ref, SyncPid),
    assert_server_stopped(Stopper).

t_sync_once_uses_default_table_sets(_Config) ->
    Self = self(),
    BackupName = <<"emqx-export-default-table-sets.tar.gz">>,
    Deps = #{
        request_fun => fun(Method, _Url, _Headers, Body, _Timeout) ->
            case Method of
                post ->
                    Self ! {export_request, emqx_utils_json:decode(Body)},
                    {ok, 200, [], emqx_utils_json:encode(#{<<"filename">> => BackupName})};
                get ->
                    {ok, 200, [], <<"backup-bytes">>};
                delete ->
                    {ok, 204, [], <<>>}
            end
        end,
        upload_fun => fun(_Filename, _Bin) -> ok end,
        import_fun => fun(_Filename) -> {ok, #{db_errors => #{}, config_errors => #{}}} end,
        delete_local_fun => fun(_Filename) -> ok end
    },
    Conf = remove_table_sets(conf()),

    ?assertMatch(
        {ok, #{filename := BackupName}}, emqx_cluster_config_sync_client:sync_once(Conf, Deps)
    ),

    receive
        {export_request, ExportReq} ->
            ?assertEqual(default_table_sets(), maps:get(<<"table_sets">>, ExportReq))
    after 0 ->
        error(missing_export_request)
    end.

t_sync_once_cleans_up_when_cancelled_after_export(_Config) ->
    Self = self(),
    BackupName = <<"emqx-export-2026-06-02.tar.gz">>,
    Deps = #{
        cancelled_fun => fun() ->
            Count = get(cancelled_check_count),
            put(cancelled_check_count, count(Count) + 1),
            Count =/= undefined
        end,
        request_fun => fun
            (post, _Url, _Headers, _Body, _Timeout) ->
                {ok, 200, [], emqx_utils_json:encode(#{<<"filename">> => BackupName})};
            (delete, Url, _Headers, undefined, _Timeout) ->
                Self ! {delete_remote, Url},
                {ok, 204, [], <<>>};
            (Method, Url, _Headers, _Body, _Timeout) ->
                error({unexpected_request, Method, Url})
        end,
        upload_fun => fun(_Filename, _Bin) -> error(unexpected_upload) end,
        import_fun => fun(_Filename) -> error(unexpected_import) end,
        delete_local_fun => fun(Filename) ->
            Self ! {delete_local, Filename},
            ok
        end
    },

    ?assertEqual({error, cancelled}, emqx_cluster_config_sync_client:sync_once(conf(), Deps)),
    assert_seen({delete_local, BackupName}),
    receive
        {delete_remote, Url} ->
            ?assertEqual(true, lists:suffix("/data/files/" ++ binary_to_list(BackupName), Url)),
            ok
    end.

t_sync_once_exports_downloads_imports_and_cleans_up(_Config) ->
    Self = self(),
    BackupName = <<"emqx-export-2026-06-02.tar.gz">>,
    BackupBin = <<"backup-bytes">>,
    Deps = #{
        request_fun => fun(Method, Url, Headers, Body, Timeout) ->
            Self ! {request, Method, Url, Headers, Body, Timeout},
            case Method of
                post -> {ok, 200, [], emqx_utils_json:encode(#{<<"filename">> => BackupName})};
                get -> {ok, 200, [], BackupBin};
                delete -> {ok, 204, [], <<>>}
            end
        end,
        upload_fun => fun(Filename, Bin) ->
            Self ! {upload, Filename, Bin},
            ok
        end,
        import_fun => fun(Filename) ->
            Self ! {import, Filename},
            {ok, #{db_errors => #{}, config_errors => #{}}}
        end,
        delete_local_fun => fun(Filename) ->
            Self ! {delete_local, Filename},
            ok
        end
    },

    ?assertMatch(
        {ok, #{filename := BackupName}}, emqx_cluster_config_sync_client:sync_once(conf(), Deps)
    ),

    receive
        {request, post, ExportUrl, Headers, ExportBody, 30000} ->
            ?assertEqual(true, lists:suffix("/data/export", ExportUrl)),
            ?assertEqual(
                {"Authorization", "Basic " ++ base64:encode_to_string(<<"key:secret">>)},
                lists:keyfind("Authorization", 1, Headers)
            ),
            ExportReq = emqx_utils_json:decode(ExportBody),
            ?assertEqual(default_root_keys(), maps:get(<<"root_keys">>, ExportReq)),
            ?assertEqual([], maps:get(<<"table_sets">>, ExportReq))
    after 0 ->
        error(missing_export_request)
    end,
    assert_request(get, "/data/files/" ++ binary_to_list(BackupName), BackupBin),
    assert_seen({upload, BackupName, BackupBin}),
    assert_seen({import, BackupName}),
    assert_request(delete, "/data/files/" ++ binary_to_list(BackupName), <<>>),
    assert_seen({delete_local, BackupName}).

t_sync_once_reports_import_errors(_Config) ->
    BackupName = <<"emqx-export-2026-06-02.tar.gz">>,
    Deps = #{
        request_fun => fun
            (post, _Url, _Headers, _Body, _Timeout) ->
                {ok, 200, [], emqx_utils_json:encode(#{<<"filename">> => BackupName})};
            (get, _Url, _Headers, undefined, _Timeout) ->
                {ok, 200, [], <<"backup">>};
            (delete, _Url, _Headers, undefined, _Timeout) ->
                {ok, 204, [], <<>>}
        end,
        upload_fun => fun(_Filename, _Bin) -> ok end,
        import_fun => fun(_Filename) ->
            {ok, #{db_errors => #{}, config_errors => #{[<<"rules">>] => {error, bad_rule}}}}
        end,
        delete_local_fun => fun(_Filename) -> ok end
    },
    ?assertMatch(
        {error, {import_failed, {ok, #{config_errors := #{}}}}},
        emqx_cluster_config_sync_client:sync_once(conf(), Deps)
    ).

t_sync_once_reports_cleanup_errors(_Config) ->
    BackupName = <<"emqx-export-2026-06-02.tar.gz">>,
    Deps = #{
        request_fun => fun
            (post, _Url, _Headers, _Body, _Timeout) ->
                {ok, 200, [], emqx_utils_json:encode(#{<<"filename">> => BackupName})};
            (get, _Url, _Headers, undefined, _Timeout) ->
                {ok, 200, [], <<"backup">>};
            (delete, _Url, _Headers, undefined, _Timeout) ->
                {ok, 500, [], <<"delete failed">>}
        end,
        upload_fun => fun(_Filename, _Bin) -> ok end,
        import_fun => fun(_Filename) ->
            {ok, #{db_errors => #{}, config_errors => #{}}}
        end,
        delete_local_fun => fun(_Filename) -> {error, eacces} end
    },
    ?assertMatch(
        {error,
            {cleanup_failed, #{
                remote := {error, {http_error, delete, _, 500, <<"delete failed">>}},
                local := {error, eacces}
            }}},
        emqx_cluster_config_sync_client:sync_once(conf(), Deps)
    ).

t_sync_once_reports_remote_export_errors(_Config) ->
    Deps = #{
        request_fun => fun(post, _Url, _Headers, _Body, _Timeout) ->
            {ok, 500, [], <<"boom">>}
        end,
        upload_fun => fun(_Filename, _Bin) -> error(unexpected_upload) end,
        import_fun => fun(_Filename) -> error(unexpected_import) end,
        delete_local_fun => fun(_Filename) -> error(unexpected_delete_local) end
    },
    ?assertMatch(
        {error, {http_error, post, _, 500, <<"boom">>}},
        emqx_cluster_config_sync_client:sync_once(conf(), Deps)
    ).

t_real_cluster_sync_runs_on_one_core_only(Config) ->
    BackupName = <<"real-cluster-config-sync.tar.gz">>,
    BackupBin = make_rule_engine_backup(Config, BackupName),
    {PrimaryPid, PrimaryUrl} = start_fake_primary(BackupName, BackupBin),
    Nodes = start_real_secondary_cluster(Config),
    try
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(Nodes),
        SyncConf = conf(PrimaryUrl),
        ok = ?ON(SelectedNode, emqx_cluster_config_sync:on_config_changed(#{}, SyncConf)),
        ok = ?ON(OtherNode, emqx_cluster_config_sync:on_config_changed(#{}, SyncConf)),

        trigger_sync(OtherNode),
        assert_no_fake_primary_request(500),

        trigger_sync(SelectedNode),
        Requests = wait_fake_primary_requests(3),
        ?assertEqual([post, get, delete], [Method || {Method, _Path, _Body} <- Requests]),
        wait_imported_rule(Nodes),
        ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check())),

        trigger_sync(OtherNode),
        assert_no_fake_primary_request(500),

        PrimaryRoleConf = SyncConf#{<<"role">> => <<"primary">>},
        ok = configure_sync_nodes([SelectedNode, OtherNode], PrimaryRoleConf),
        trigger_sync(SelectedNode),
        assert_no_fake_primary_request(500),
        ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check()))
    after
        stop_fake_primary(PrimaryPid),
        ok = emqx_cth_cluster:stop(Nodes)
    end.

t_real_primary_sync_respects_root_keys_and_table_sets(Config) ->
    ExtraApps = table_set_apps(),
    PrimaryNodes = start_real_primary_cluster(Config, "primary_table_sets", ExtraApps),
    SecondaryNodes = start_real_secondary_cluster(Config, "secondary_table_sets", ExtraApps),
    try
        [PrimaryNode] = PrimaryNodes,
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        {PrimaryUrl, ApiKey, ApiSecret} = setup_real_primary(PrimaryNode),
        ok = create_primary_rule_and_table_data(PrimaryNode),

        RootKeys = [<<"rule_engine">>],
        assert_primary_rule_exported(Config, PrimaryNode, RootKeys),
        ConfigOnly = conf(PrimaryUrl, ApiKey, ApiSecret, RootKeys, []),
        ok = configure_sync_nodes([SelectedNode, OtherNode], ConfigOnly),
        ?check_trace(
            begin
                trigger_sync(SelectedNode),
                wait_imported_rule(SecondaryNodes),
                ?block_until(#{?snk_kind := cluster_config_sync_result, stage := finished}, 10_000),
                assert_table_data_absent(SecondaryNodes),
                ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check()))
            end,
            fun(Trace) ->
                assert_success_trace(Trace, SelectedNode, RootKeys, [])
            end
        ),

        WithDefaultTableSets = conf_with_default_table_sets(
            PrimaryUrl, ApiKey, ApiSecret, RootKeys
        ),
        ok = configure_sync_nodes([SelectedNode, OtherNode], WithDefaultTableSets),
        ?check_trace(
            begin
                trigger_sync(SelectedNode),
                wait_default_table_data(SecondaryNodes),
                ?block_until(#{?snk_kind := cluster_config_sync_result, stage := finished}, 10_000),
                assert_retainer_absent(SecondaryNodes),
                ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check()))
            end,
            fun(Trace) ->
                assert_success_trace(Trace, SelectedNode, RootKeys, default_table_sets())
            end
        ),

        TableSets = [
            <<"banned">>,
            <<"builtin_authn">>,
            <<"builtin_authz">>,
            <<"builtin_retainer">>
        ],
        WithTableSets = conf(PrimaryUrl, ApiKey, ApiSecret, RootKeys, TableSets),
        ok = configure_sync_nodes([SelectedNode, OtherNode], WithTableSets),
        ?check_trace(
            begin
                trigger_sync(SelectedNode),
                wait_table_data(SecondaryNodes),
                ?block_until(#{?snk_kind := cluster_config_sync_result, stage := finished}, 10_000),
                ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check()))
            end,
            fun(Trace) ->
                assert_success_trace(Trace, SelectedNode, RootKeys, TableSets)
            end
        )
    after
        ok = emqx_cth_cluster:stop(SecondaryNodes),
        ok = emqx_cth_cluster:stop(PrimaryNodes)
    end.

t_real_primary_sync_file_backed_schema_registry(Config) ->
    ExtraApps = [emqx_schema_registry],
    PrimaryNodes = start_real_primary_cluster(Config, "primary_schema_registry", ExtraApps),
    SecondaryNodes = start_real_secondary_cluster(Config, "secondary_schema_registry", ExtraApps),
    try
        [PrimaryNode] = PrimaryNodes,
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        {PrimaryUrl, ApiKey, ApiSecret} = setup_real_primary(PrimaryNode),
        {ok, PrimaryRootPath} = create_primary_protobuf_bundle_schema(PrimaryNode),
        PrimaryDataDir = ?ON(PrimaryNode, emqx:data_dir()),
        assert_schema_absent(SecondaryNodes),

        RootKeys = [<<"schema_registry">>],
        SyncConf = conf(PrimaryUrl, ApiKey, ApiSecret, RootKeys, []),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        ?check_trace(
            begin
                trigger_sync(SelectedNode),
                wait_schema_usable(SecondaryNodes, PrimaryDataDir, PrimaryRootPath),
                ?block_until(#{?snk_kind := cluster_config_sync_result, stage := finished}, 10_000),
                ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check()))
            end,
            fun(Trace) ->
                assert_success_trace(Trace, SelectedNode, RootKeys, [])
            end
        )
    after
        ok = emqx_cth_cluster:stop(SecondaryNodes),
        ok = emqx_cth_cluster:stop(PrimaryNodes)
    end.

t_real_primary_sync_listener_cert_files(Config) ->
    PrimaryNodes = start_real_primary_cluster(Config, "primary_listener_files", []),
    SecondaryNodes = start_real_secondary_cluster(Config, "secondary_listener_files"),
    try
        [PrimaryNode] = PrimaryNodes,
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        {PrimaryUrl, ApiKey, ApiSecret} = setup_real_primary(PrimaryNode),
        {ok, #{data_dir := PrimaryDataDir, certs := Certs}} =
            create_primary_tls_listener(PrimaryNode),
        assert_tls_listener_absent(SecondaryNodes),

        RootKeys = [<<"listeners">>],
        SyncConf = conf(PrimaryUrl, ApiKey, ApiSecret, RootKeys, []),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        ?check_trace(
            begin
                trigger_sync(SelectedNode),
                wait_tls_listener_cert_files(SecondaryNodes, PrimaryDataDir, Certs),
                ?block_until(#{?snk_kind := cluster_config_sync_result, stage := finished}, 10_000),
                ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check()))
            end,
            fun(Trace) ->
                assert_success_trace(Trace, SelectedNode, RootKeys, [])
            end
        )
    after
        ok = emqx_cth_cluster:stop(SecondaryNodes),
        ok = emqx_cth_cluster:stop(PrimaryNodes)
    end.

t_real_sync_reports_observable_failure(Config) ->
    PrimaryNodes = start_real_primary_cluster(Config, "primary_observable_failure", []),
    SecondaryNodes = start_real_secondary_cluster(Config, "secondary_observable_failure"),
    try
        [PrimaryNode] = PrimaryNodes,
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        {PrimaryUrl, ApiKey, _ApiSecret} = setup_real_primary(PrimaryNode),
        WrongSecret = <<"wrong-secret-should-not-leak">>,
        RootKeys = [<<"rule_engine">>],
        SyncConf = conf(PrimaryUrl, ApiKey, WrongSecret, RootKeys, []),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        ?check_trace(
            begin
                trigger_sync(SelectedNode),
                ?block_until(#{?snk_kind := cluster_config_sync_result, result := failed}, 10_000),
                wait_remote_health_error(SelectedNode, <<"http_error">>)
            end,
            fun(Trace) ->
                assert_failure_trace(Trace, SelectedNode, RootKeys, [], WrongSecret)
            end
        )
    after
        ok = emqx_cth_cluster:stop(SecondaryNodes),
        ok = emqx_cth_cluster:stop(PrimaryNodes)
    end.

sync_config(Interval) ->
    #{<<"sync">> => #{<<"interval">> => Interval}}.

conf() ->
    conf(<<"http://primary:18083/api/v5/">>).

conf(BaseUrl) ->
    conf(BaseUrl, <<"5m">>).

conf(BaseUrl, Interval) ->
    #{
        <<"enable">> => true,
        <<"role">> => <<"secondary">>,
        <<"primary">> => #{
            <<"base_url">> => BaseUrl,
            <<"api_key">> => <<"key">>,
            <<"api_secret">> => <<"secret">>
        },
        <<"sync">> => #{
            <<"interval">> => Interval,
            <<"timeout">> => <<"30s">>,
            <<"root_keys">> => default_root_keys(),
            <<"table_sets">> => [],
            <<"delete_remote_backup">> => true,
            <<"delete_local_backup">> => true
        }
    }.

conf(BaseUrl, ApiKey, ApiSecret, RootKeys, TableSets) ->
    Conf = conf(BaseUrl),
    Primary = maps:get(<<"primary">>, Conf),
    Sync = maps:get(<<"sync">>, Conf),
    Conf#{
        <<"primary">> => Primary#{
            <<"api_key">> => ApiKey,
            <<"api_secret">> => ApiSecret
        },
        <<"sync">> => Sync#{
            <<"root_keys">> => RootKeys,
            <<"table_sets">> => TableSets
        }
    }.

conf_with_default_table_sets(BaseUrl, ApiKey, ApiSecret, RootKeys) ->
    remove_table_sets(conf(BaseUrl, ApiKey, ApiSecret, RootKeys, [])).

default_root_keys() ->
    emqx_cluster_config_sync_client:default_root_keys().

default_table_sets() ->
    emqx_cluster_config_sync_client:default_table_sets().

remove_table_sets(Conf) ->
    Sync = maps:get(<<"sync">>, Conf),
    Conf#{<<"sync">> => maps:remove(<<"table_sets">>, Sync)}.

assert_request(Method, UrlSuffix, Body) ->
    receive
        {request, Method, Url, _Headers, _ReqBody, _Timeout} ->
            ?assertEqual(true, lists:suffix(UrlSuffix, Url)),
            ok;
        {request, Method, Url, _Headers, undefined, _Timeout} when Method =:= get ->
            ?assertEqual(true, lists:suffix(UrlSuffix, Url)),
            ok
    after 0 ->
        error({missing_request, Method, UrlSuffix, Body})
    end.

assert_seen(Msg) ->
    receive
        Msg -> ok
    after 0 ->
        error({missing_message, Msg})
    end.

setup_core_node() ->
    setup_core_nodes([node()]).

setup_core_nodes(Nodes) ->
    meck:new(mria_rlog, [passthrough]),
    meck:expect(mria_rlog, role, 0, core),
    meck:new(mria_membership, [passthrough]),
    meck:expect(mria_membership, running_core_nodelist, 0, Nodes).

setup_core_membership_unavailable() ->
    meck:new(mria_rlog, [passthrough]),
    meck:expect(mria_rlog, role, 0, core),
    meck:new(mria_membership, [passthrough]),
    meck:expect(mria_membership, running_core_nodelist, fun() ->
        error(membership_unavailable)
    end).

setup_core_nodes_sequence(NodeSequences) ->
    new_core_nodes_tab(),
    true = ets:insert(?CORE_NODES_TAB, {node_sequences, NodeSequences}),
    meck:new(mria_rlog, [passthrough]),
    meck:expect(mria_rlog, role, 0, core),
    meck:new(mria_membership, [passthrough]),
    meck:expect(mria_membership, running_core_nodelist, fun() ->
        [{node_sequences, [Current | Rest]}] = ets:lookup(?CORE_NODES_TAB, node_sequences),
        case Rest of
            [] -> ok;
            _ -> true = ets:insert(?CORE_NODES_TAB, {node_sequences, Rest})
        end,
        Current
    end).

setup_dynamic_core_nodes(Nodes) ->
    new_core_nodes_tab(),
    set_core_nodes(Nodes),
    meck:new(mria_rlog, [passthrough]),
    meck:expect(mria_rlog, role, 0, core),
    meck:new(mria_membership, [passthrough]),
    meck:expect(mria_membership, running_core_nodelist, fun() ->
        [{nodes, CurrentNodes}] = ets:lookup(?CORE_NODES_TAB, nodes),
        CurrentNodes
    end).

new_core_nodes_tab() ->
    catch ets:delete(?CORE_NODES_TAB),
    ets:new(?CORE_NODES_TAB, [named_table, public, set]).

set_core_nodes(Nodes) ->
    true = ets:insert(?CORE_NODES_TAB, {nodes, Nodes}),
    ok.

setup_blocking_sync() ->
    Parent = self(),
    meck:new(emqx_cluster_config_sync_client, [passthrough]),
    meck:expect(emqx_cluster_config_sync_client, sync_once, fun(_Conf, _Deps) ->
        Parent ! {sync_started, self()},
        receive
            finish_sync ->
                Parent ! {sync_finished, self()},
                {ok, #{filename => <<"backup.tar.gz">>}}
        end
    end).

setup_cancellable_sync() ->
    Parent = self(),
    meck:new(emqx_cluster_config_sync_client, [passthrough]),
    meck:expect(emqx_cluster_config_sync_client, sync_once, fun(
        _Conf, #{cancelled_fun := Cancelled}
    ) ->
        Parent ! {sync_started, self()},
        receive
            check_cancelled -> ok
        end,
        Result =
            case Cancelled() of
                true ->
                    Parent ! {sync_cancel_requested, self()},
                    {error, cancelled};
                false ->
                    {ok, #{filename => <<"backup.tar.gz">>}}
            end,
        Parent ! {sync_cleanup, self()},
        Result
    end).

setup_cancel_ignoring_sync() ->
    Parent = self(),
    meck:new(emqx_cluster_config_sync_client, [passthrough]),
    meck:expect(emqx_cluster_config_sync_client, sync_once, fun(_Conf, _Deps) ->
        Parent ! {sync_started, self()},
        receive
            stop_ignoring_sync ->
                {error, stopped}
        end
    end).

setup_shutdown_cancellable_sync() ->
    Parent = self(),
    meck:new(emqx_cluster_config_sync_client, [passthrough]),
    meck:expect(emqx_cluster_config_sync_client, sync_once, fun(
        _Conf, #{cancelled_fun := Cancelled}
    ) ->
        Parent ! {sync_started, self()},
        wait_cancelled(Cancelled),
        Parent ! {sync_cancel_requested, self()},
        receive
            finish_cleanup ->
                Parent ! {sync_cleanup, self()},
                {error, cancelled}
        end
    end).

wait_cancelled(Cancelled) ->
    case Cancelled() of
        true ->
            ok;
        false ->
            receive
            after 10 ->
                wait_cancelled(Cancelled)
            end
    end.

count(undefined) -> 0;
count(Count) -> Count.

setup_counting_sync() ->
    Parent = self(),
    meck:new(emqx_cluster_config_sync_client, [passthrough]),
    meck:expect(emqx_cluster_config_sync_client, sync_once, fun(_Conf, _Deps) ->
        Parent ! {sync_started, self()},
        {ok, #{filename => <<"backup.tar.gz">>}}
    end).

start_test_server() ->
    cleanup_test_server(),
    ensure_plugin_app_loaded(),
    {ok, Pid} = emqx_cluster_config_sync:start_link(),
    unlink(Pid),
    Pid.

ensure_plugin_app_loaded() ->
    case application:load(emqx_cluster_config_sync) of
        ok -> ok;
        {error, {already_loaded, emqx_cluster_config_sync}} -> ok
    end.

cleanup_test_artifacts() ->
    cleanup_test_server(),
    catch meck:unload(mria_rlog),
    catch meck:unload(mria_membership),
    catch meck:unload(emqx_cluster_config_sync_client),
    catch ets:delete(?CORE_NODES_TAB),
    ok.

cleanup_test_server() ->
    case whereis(emqx_cluster_config_sync) of
        undefined ->
            ok;
        Pid ->
            unlink(Pid),
            exit(Pid, kill),
            wait_process_down(Pid)
    end.

wait_process_down(Pid) ->
    Ref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 1000 ->
        error({server_still_alive, Pid})
    end.

assert_sync_started() ->
    receive
        {sync_started, Pid} -> Pid
    after 1000 ->
        error(sync_not_started)
    end.

assert_sync_finished() ->
    receive
        {sync_finished, _Pid} -> ok
    after 1000 ->
        error(sync_not_finished)
    end.

assert_sync_cancel_requested(Pid) ->
    receive
        {sync_cancel_requested, Pid} -> ok
    after 1000 ->
        error({sync_cancel_not_requested, Pid})
    end.

assert_sync_cleanup(Pid) ->
    receive
        {sync_cleanup, Pid} -> ok
    after 1000 ->
        error({sync_cleanup_skipped, Pid})
    end.

assert_sync_down(Ref, Pid) ->
    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 1000 ->
        error({worker_not_down, Pid})
    end.

assert_worker_alive(Ref, Pid) ->
    receive
        {'DOWN', Ref, process, Pid, Reason} ->
            error({worker_stopped_before_import_finished, Pid, Reason})
    after 100 ->
        ok
    end.

assert_no_server_stopped(Stopper, Timeout) ->
    receive
        {server_stopped, Result} ->
            error({server_stopped_before_worker_cleanup, Stopper, Result})
    after Timeout ->
        ok
    end.

assert_server_stopped(Stopper) ->
    receive
        {server_stopped, ok} ->
            ok;
        {server_stopped, Result} ->
            error({server_stop_failed, Stopper, Result})
    after 1000 ->
        error({server_not_stopped, Stopper})
    end.

assert_no_sync_started(Timeout) ->
    receive
        {sync_started, Pid} ->
            error({unexpected_sync_started, Pid})
    after Timeout ->
        ok
    end.

hold_sync_lock() ->
    Parent = self(),
    LockPid = spawn_link(fun() ->
        global:trans(
            {{emqx_cluster_config_sync, sync}, self()},
            fun() ->
                Parent ! sync_lock_held,
                receive
                    release_lock -> ok
                end
            end,
            [node()],
            0
        )
    end),
    receive
        sync_lock_held -> LockPid
    after 1000 ->
        error(sync_lock_not_held)
    end.

wait_health_error(ExpectedFragment) ->
    Deadline = erlang:monotonic_time(millisecond) + 1000,
    wait_health_error(ExpectedFragment, Deadline).

wait_health_error(ExpectedFragment, Deadline) ->
    case emqx_cluster_config_sync:on_health_check() of
        {error, Reason} ->
            case binary:match(Reason, ExpectedFragment) of
                nomatch -> error({unexpected_health_error, Reason});
                _ -> ok
            end;
        ok ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    error(cancel_timeout_not_reported);
                false ->
                    receive
                    after 20 ->
                        wait_health_error(ExpectedFragment, Deadline)
                    end
            end
    end.

start_real_secondary_cluster(Config) ->
    start_real_secondary_cluster(Config, "secondary").

start_real_secondary_cluster(Config, WorkDirSuffix) ->
    start_real_secondary_cluster(Config, WorkDirSuffix, []).

start_real_secondary_cluster(Config, WorkDirSuffix, ExtraApps) ->
    WorkDir = cluster_work_dir(Config, WorkDirSuffix),
    Apps = real_cluster_apps(base_cluster_conf(), ExtraApps),
    ClusterSpec = [
        {cluster_config_sync_real1, #{role => core, apps => Apps}},
        {cluster_config_sync_real2, #{role => core, apps => Apps}}
    ],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        ClusterSpec,
        #{
            work_dir => WorkDir,
            start_apps_timeout => 60_000
        }
    ),
    emqx_cth_cluster:start(NodeSpecs).

start_real_primary_cluster(Config) ->
    start_real_primary_cluster(Config, "primary", []).

start_real_primary_cluster(Config, WorkDirSuffix, ExtraApps) ->
    WorkDir = cluster_work_dir(Config, WorkDirSuffix),
    Apps = real_primary_apps(?PRIMARY_API_PORT, ExtraApps),
    ClusterSpec = [
        {cluster_config_sync_primary1, #{
            role => core,
            apps => Apps,
            base_port => ?PRIMARY_BASE_PORT
        }}
    ],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        ClusterSpec,
        #{
            work_dir => WorkDir,
            start_apps_timeout => 60_000
        }
    ),
    emqx_cth_cluster:start(NodeSpecs).

cluster_work_dir(Config, Suffix) ->
    filename:join(emqx_cth_suite:work_dir(?FUNCTION_NAME, Config), Suffix).

real_cluster_apps() ->
    real_cluster_apps(base_cluster_conf(), []).

real_cluster_apps(Conf) ->
    real_cluster_apps(Conf, []).

real_cluster_apps(Conf, ExtraApps) ->
    [
        {emqx, #{config => Conf}},
        {emqx_conf, #{config => Conf}},
        emqx_management,
        emqx_rule_engine
    ] ++
        ExtraApps ++
        [
            emqx_cluster_config_sync
        ].

real_primary_apps(APIPort) ->
    real_primary_apps(APIPort, []).

real_primary_apps(APIPort, ExtraApps) ->
    Conf = base_cluster_conf(),
    [
        {emqx, #{config => Conf}},
        {emqx_conf, #{config => Conf}},
        emqx_management,
        {emqx_dashboard, dashboard_conf(APIPort)},
        emqx_rule_engine
    ] ++ ExtraApps.

table_set_apps() ->
    [
        emqx_auth,
        emqx_auth_mnesia,
        emqx_retainer
    ].

base_cluster_conf() ->
    Conf = #{
        listeners => #{
            tcp => #{default => <<"marked_for_deletion">>},
            ssl => #{default => <<"marked_for_deletion">>},
            ws => #{default => <<"marked_for_deletion">>},
            wss => #{default => <<"marked_for_deletion">>}
        }
    },
    Conf.

dashboard_conf(APIPort) ->
    #{
        config => #{
            dashboard => #{
                listeners => #{
                    http => #{bind => APIPort}
                },
                default_username => "",
                default_password => ""
            }
        }
    }.

setup_real_primary(PrimaryNode) ->
    {ok, #{api_key := ApiKey, api_secret := ApiSecret}} =
        ?ON(PrimaryNode, emqx_common_test_http:create_default_app()),
    PrimaryUrl = iolist_to_binary(
        io_lib:format("http://127.0.0.1:~B/api/v5/", [?PRIMARY_API_PORT])
    ),
    {PrimaryUrl, ApiKey, ApiSecret}.

create_primary_rule_and_table_data(PrimaryNode) ->
    ?ON(PrimaryNode, begin
        {201, _Rule} = emqx_rule_engine_api:'/rules'(post, #{
            body => #{
                <<"id">> => ?INTEGRATION_RULE_ID,
                <<"name">> => ?INTEGRATION_RULE_ID,
                <<"enable">> => true,
                <<"sql">> => <<"SELECT * FROM \"t/#\"">>,
                <<"actions">> => [#{<<"function">> => <<"console">>}],
                <<"description">> => <<"cluster config sync integration test">>
            }
        }),
        ok = create_banned(?INTEGRATION_BANNED_CLIENTID),
        ok = create_authn_user(?INTEGRATION_AUTHN_USER),
        ok = create_authz_rules(?INTEGRATION_AUTHZ_USERNAME),
        ok = store_retained_message(?INTEGRATION_RETAINER_TOPIC, ?INTEGRATION_RETAINER_PAYLOAD),
        ok
    end).

create_banned(ClientId) ->
    Now = erlang:system_time(second),
    Who = emqx_banned:who(clientid, ClientId),
    ok = emqx_banned:delete(Who),
    {ok, _} = emqx_banned:create(#{
        who => Who,
        by => <<"cluster_config_sync_SUITE">>,
        reason => <<"table_set_sync_test">>,
        at => Now,
        until => Now + 3600
    }),
    ok.

create_authn_user(UserId) ->
    {ok, State} = emqx_authn_mnesia:create(
        <<"cluster_config_sync:built_in_database">>, authn_config()
    ),
    {ok, _} = emqx_authn_mnesia:add_user(
        #{user_id => UserId, password => <<"secret">>}, State
    ),
    ok.

authn_config() ->
    #{
        user_id_type => username,
        password_hash_algorithm => #{
            name => bcrypt,
            salt_rounds => 8
        },
        user_group => <<"global:mqtt">>
    }.

create_authz_rules(Username) ->
    emqx_authz_mnesia:store_rules(
        {username, Username},
        [
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"cluster/config/sync">>
            }
        ]
    ).

store_retained_message(Topic, Payload) ->
    Msg = emqx_message:make(
        <<"cluster_config_sync">>,
        ?QOS_0,
        Topic,
        Payload,
        #{retain => true},
        #{}
    ),
    emqx_retainer_publisher:store_retained(Msg).

create_primary_protobuf_bundle_schema(PrimaryNode) ->
    ?ON(PrimaryNode, begin
        ok = emqx_schema_registry:add_schema(?INTEGRATION_SCHEMA_NAME, protobuf_bundle_schema()),
        {ok, #{source := #{root_proto_path := RootPath}}} =
            emqx_schema_registry:get_schema(?INTEGRATION_SCHEMA_NAME),
        {ok, RootPath}
    end).

protobuf_bundle_schema() ->
    #{
        <<"type">> => <<"protobuf">>,
        <<"source">> => #{
            <<"type">> => <<"bundle">>,
            <<"files">> => [
                #{
                    <<"path">> => <<"person.proto">>,
                    <<"root">> => true,
                    <<"contents">> => protobuf_person_schema()
                }
            ]
        }
    }.

protobuf_person_schema() ->
    <<
        "syntax = \"proto3\";\n"
        "message Person {\n"
        "  string name = 1;\n"
        "  int32 id = 2;\n"
        "}\n"
    >>.

create_primary_tls_listener(PrimaryNode) ->
    ?ON(PrimaryNode, begin
        Certs = tls_cert_contents(),
        {ok, _} = emqx_mgmt_listeners_conf:create(
            ssl,
            ?INTEGRATION_TLS_LISTENER_NAME,
            #{
                <<"enable">> => false,
                <<"bind">> => tls_listener_bind(),
                <<"ssl_options">> => #{
                    <<"cacertfile">> => maps:get(cacertfile, Certs),
                    <<"certfile">> => maps:get(certfile, Certs),
                    <<"keyfile">> => maps:get(keyfile, Certs)
                }
            }
        ),
        {ok, #{data_dir => emqx:data_dir(), certs => Certs}}
    end).

tls_listener_bind() ->
    iolist_to_binary(io_lib:format("127.0.0.1:~B", [?INTEGRATION_TLS_LISTENER_PORT])).

tls_cert_contents() ->
    #{
        cacertfile => cert_file("cacert.pem"),
        certfile => cert_file("cert.pem"),
        keyfile => cert_file("key.pem")
    }.

cert_file(Name) ->
    CertPath = filename:join([code:lib_dir(emqx), "etc", "certs", Name]),
    {ok, Bin} = file:read_file(CertPath),
    Bin.

assert_primary_rule_exported(Config, PrimaryNode, RootKeys) ->
    OutDir = filename:join(?config(priv_dir, Config), "primary_export_probe"),
    ?assertEqual(true, has_primary_rule(PrimaryNode)),
    ?assertEqual(true, primary_export_contains_rule(PrimaryNode, OutDir, RootKeys)).

has_primary_rule(PrimaryNode) ->
    ?ON(PrimaryNode, begin
        case emqx_rule_engine:get_rule(?INTEGRATION_RULE_ID) of
            {ok, _Rule} -> true;
            _ -> false
        end
    end).

primary_export_contains_rule(PrimaryNode, OutDir, RootKeys) ->
    ?ON(PrimaryNode, begin
        ok = filelib:ensure_dir(filename:join(OutDir, "placeholder")),
        Params = #{
            <<"root_keys">> => RootKeys,
            <<"table_sets">> => []
        },
        {ok, Opts0} = emqx_mgmt_data_backup:parse_export_request(Params),
        {ok, #{filename := Filename}} =
            emqx_mgmt_data_backup:export(Opts0#{out_dir => OutDir}),
        {ok, Entries} = erl_tar:extract(binary_to_list(Filename), [compressed, memory]),
        {_, ClusterHocon} =
            lists:keyfind(?CLUSTER_HOCON_FILENAME, 1, [
                {filename:basename(Path), Bin}
             || {Path, Bin} <- Entries
            ]),
        binary:match(ClusterHocon, ?INTEGRATION_RULE_ID) =/= nomatch
    end).

configure_sync_nodes(Nodes, SyncConf) ->
    lists:foreach(
        fun(Node) ->
            ok = ?ON(Node, emqx_cluster_config_sync:on_config_changed(#{}, SyncConf))
        end,
        Nodes
    ),
    ok.

select_cluster_sync_nodes(Nodes) ->
    SelectedNode =
        hd(lists:sort(?ON(hd(Nodes), mria_membership:running_core_nodelist()))),
    [OtherNode] = Nodes -- [SelectedNode],
    [SelectedNode, OtherNode].

trigger_sync(Node) ->
    ?ON(Node, whereis(emqx_cluster_config_sync) ! sync),
    ok.

make_rule_engine_backup(Config, BackupName) ->
    PrivDir = ?config(priv_dir, Config),
    BackupFile = filename:join(PrivDir, binary_to_list(BackupName)),
    BackupRoot = filename:basename(BackupFile, ".tar.gz"),
    Meta = unicode:characters_to_binary(
        hocon_pp:do(
            #{
                edition => emqx_release:edition(),
                version => emqx_release:version()
            },
            #{}
        )
    ),
    ClusterHocon = unicode:characters_to_binary(
        hocon_pp:do(
            #{
                <<"rule_engine">> => #{
                    <<"rules">> => #{
                        ?INTEGRATION_RULE_ID => #{
                            <<"description">> => <<"">>,
                            <<"sql">> => <<"SELECT * FROM \"t/#\"">>,
                            <<"actions">> => [
                                #{
                                    <<"function">> => <<"republish">>,
                                    <<"args">> => #{
                                        <<"topic">> => <<"cluster/config/sync/${topic}">>
                                    }
                                }
                            ]
                        }
                    }
                }
            },
            #{}
        )
    ),
    ok = erl_tar:create(
        BackupFile,
        [
            {filename:join(BackupRoot, "META.hocon"), Meta},
            {filename:join(BackupRoot, "cluster.hocon"), ClusterHocon}
        ],
        [compressed]
    ),
    {ok, BackupBin} = file:read_file(BackupFile),
    BackupBin.

wait_imported_rule(Nodes) ->
    try
        wait_until(
            fun() ->
                lists:all(fun has_imported_rule/1, Nodes)
            end,
            #{action => wait_imported_rule, nodes => Nodes}
        )
    catch
        error:{timeout, Context} ->
            error({timeout, Context#{diagnostics => sync_diagnostics(Nodes)}})
    end.

has_imported_rule(Node) ->
    ?ON(Node, begin
        Rules = emqx_conf:get([rule_engine, rules]),
        RuleIdAtom = binary_to_existing_atom_or_undefined(?INTEGRATION_RULE_ID),
        maps:is_key(?INTEGRATION_RULE_ID, Rules) orelse
            maps:is_key(RuleIdAtom, Rules)
    end).

binary_to_existing_atom_or_undefined(Bin) ->
    try binary_to_existing_atom(Bin, utf8) of
        Atom -> Atom
    catch
        error:badarg -> undefined
    end.

sync_diagnostics(Nodes) ->
    [
        {Node, #{
            health => ?ON(Node, emqx_cluster_config_sync:on_health_check()),
            rule_keys => ?ON(Node, maps:keys(emqx_conf:get([rule_engine, rules])))
        }}
     || Node <- Nodes
    ].

assert_table_data_absent(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assertEqual(false, has_banned(Node, ?INTEGRATION_BANNED_CLIENTID)),
            ?assertEqual(false, has_authn_user(Node, ?INTEGRATION_AUTHN_USER)),
            ?assertEqual(false, has_authz_rules(Node, ?INTEGRATION_AUTHZ_USERNAME)),
            ?assertEqual(false, has_retained_message(Node, ?INTEGRATION_RETAINER_TOPIC))
        end,
        Nodes
    ).

assert_retainer_absent(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assertEqual(false, has_retained_message(Node, ?INTEGRATION_RETAINER_TOPIC))
        end,
        Nodes
    ).

wait_default_table_data(Nodes) ->
    wait_until(
        fun() ->
            lists:all(fun has_default_table_data/1, Nodes)
        end,
        #{action => wait_default_table_data, nodes => Nodes}
    ).

wait_table_data(Nodes) ->
    wait_until(
        fun() ->
            lists:all(fun has_all_table_data/1, Nodes)
        end,
        #{action => wait_table_data, nodes => Nodes}
    ).

has_all_table_data(Node) ->
    has_banned(Node, ?INTEGRATION_BANNED_CLIENTID) andalso
        has_authn_user(Node, ?INTEGRATION_AUTHN_USER) andalso
        has_authz_rules(Node, ?INTEGRATION_AUTHZ_USERNAME) andalso
        has_retained_message(Node, ?INTEGRATION_RETAINER_TOPIC).

has_default_table_data(Node) ->
    has_banned(Node, ?INTEGRATION_BANNED_CLIENTID) andalso
        has_authn_user(Node, ?INTEGRATION_AUTHN_USER) andalso
        has_authz_rules(Node, ?INTEGRATION_AUTHZ_USERNAME).

has_banned(Node, ClientId) ->
    ?ON(Node, emqx_banned:check_clientid(ClientId)).

has_authn_user(Node, UserId) ->
    ?ON(Node, begin
        case emqx_authn_mnesia:lookup_user(UserId, authn_config()) of
            {ok, _} -> true;
            _ -> false
        end
    end).

has_authz_rules(Node, Username) ->
    ?ON(Node, begin
        case emqx_authz_mnesia:get_rules({username, Username}) of
            {ok, [_ | _]} -> true;
            _ -> false
        end
    end).

has_retained_message(Node, Topic) ->
    ?ON(Node, begin
        case emqx_retainer:read_message(Topic) of
            {ok, [#message{payload = ?INTEGRATION_RETAINER_PAYLOAD} | _]} -> true;
            _ -> false
        end
    end).

assert_schema_absent(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assertEqual(false, has_schema(Node))
        end,
        Nodes
    ).

wait_schema_usable(Nodes, PrimaryDataDir, PrimaryRootPath) ->
    wait_until(
        fun() ->
            lists:all(
                fun(Node) -> schema_usable(Node, PrimaryDataDir, PrimaryRootPath) end,
                Nodes
            )
        end,
        #{action => wait_schema_usable, nodes => Nodes}
    ).

has_schema(Node) ->
    ?ON(Node, begin
        case emqx_schema_registry:get_schema(?INTEGRATION_SCHEMA_NAME) of
            {ok, _} -> true;
            _ -> false
        end
    end).

schema_usable(Node, PrimaryDataDir, PrimaryRootPath) ->
    ?ON(Node, begin
        case emqx_schema_registry:get_schema(?INTEGRATION_SCHEMA_NAME) of
            {ok, #{source := #{root_proto_path := RootPath}}} ->
                RootPath1 = unicode:characters_to_list(RootPath),
                PrimaryDataDir1 = unicode:characters_to_list(PrimaryDataDir),
                PrimaryRootPath1 = unicode:characters_to_list(PrimaryRootPath),
                Data = #{<<"name">> => <<"Alice">>, <<"id">> => 7},
                Encoded = emqx_schema_registry_serde:encode(
                    ?INTEGRATION_SCHEMA_NAME, Data, [<<"Person">>]
                ),
                Decoded = emqx_schema_registry_serde:decode(
                    ?INTEGRATION_SCHEMA_NAME, Encoded, [<<"Person">>]
                ),
                filelib:is_regular(RootPath1) andalso
                    RootPath1 =/= PrimaryRootPath1 andalso
                    not lists:prefix(PrimaryDataDir1, RootPath1) andalso
                    Decoded =:= Data;
            _ ->
                false
        end
    end).

assert_tls_listener_absent(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assertEqual(false, has_tls_listener(Node))
        end,
        Nodes
    ).

has_tls_listener(Node) ->
    ?ON(Node, tls_listener_conf() =/= undefined).

wait_tls_listener_cert_files(Nodes, PrimaryDataDir, Certs) ->
    wait_until(
        fun() ->
            lists:all(
                fun(Node) -> tls_listener_cert_files_synced(Node, PrimaryDataDir, Certs) end,
                Nodes
            )
        end,
        #{action => wait_tls_listener_cert_files, nodes => Nodes}
    ).

tls_listener_cert_files_synced(Node, PrimaryDataDir, Certs) ->
    ?ON(Node, begin
        case tls_listener_conf() of
            #{<<"enable">> := false, <<"ssl_options">> := SSL} ->
                listener_bind_matches() andalso
                    tls_cert_file_synced(cacertfile, SSL, PrimaryDataDir, Certs) andalso
                    tls_cert_file_synced(certfile, SSL, PrimaryDataDir, Certs) andalso
                    tls_cert_file_synced(keyfile, SSL, PrimaryDataDir, Certs);
            _ ->
                false
        end
    end).

tls_listener_conf() ->
    emqx:get_raw_config([listeners, ssl, ?INTEGRATION_TLS_LISTENER_NAME], undefined).

listener_bind_matches() ->
    case tls_listener_conf() of
        #{<<"bind">> := Bind} -> Bind =:= tls_listener_bind();
        _ -> false
    end.

tls_cert_file_synced(Key, SSL, PrimaryDataDir, Certs) ->
    BinKey = atom_to_binary(Key, utf8),
    case maps:get(BinKey, SSL, undefined) of
        undefined ->
            false;
        Path0 ->
            Path = unicode:characters_to_list(Path0),
            PrimaryDataDir1 = unicode:characters_to_list(PrimaryDataDir),
            case file:read_file(Path) of
                {ok, Content} ->
                    filelib:is_regular(Path) andalso
                        not lists:prefix(PrimaryDataDir1, Path) andalso
                        Content =:= maps:get(Key, Certs);
                _ ->
                    false
            end
    end.

assert_success_trace(Trace, SelectedNode, RootKeys, TableSets) ->
    Events = [
        Event
     || Event <- ?of_kind(cluster_config_sync_result, Trace),
        maps:get(stage, Event, undefined) =:= finished
    ],
    ?assertMatch([_ | _], Events),
    Event = hd(Events),
    ?assertEqual(SelectedNode, maps:get(node, Event)),
    ?assertEqual(SelectedNode, maps:get(selected_core_node, Event)),
    ?assertEqual(RootKeys, maps:get(root_keys, Event)),
    ?assertEqual(TableSets, maps:get(table_sets, Event)),
    ?assert(is_binary(maps:get(filename, Event))),
    ?assertMatch(#{remote := _, local := _}, maps:get(cleanup, Event)),
    ok.

assert_failure_trace(Trace, SelectedNode, RootKeys, TableSets, Secret) ->
    Events = [
        Event
     || Event <- ?of_kind(cluster_config_sync_result, Trace),
        maps:get(result, Event, undefined) =:= failed
    ],
    ?assertMatch([_ | _], Events),
    Event = hd(Events),
    ?assertEqual(SelectedNode, maps:get(node, Event)),
    ?assertEqual(SelectedNode, maps:get(selected_core_node, Event)),
    ?assertEqual(RootKeys, maps:get(root_keys, Event)),
    ?assertEqual(TableSets, maps:get(table_sets, Event)),
    ?assertEqual(export, maps:get(stage, Event)),
    Reason = maps:get(reason, Event),
    ?assertMatch({http_error, post, _, _, _}, Reason),
    ?assertEqual(nomatch, binary:match(format_term(Reason), Secret)),
    ok.

format_term(Term) ->
    iolist_to_binary(io_lib:format("~0p", [Term])).

wait_remote_health_error(Node, ExpectedFragment) ->
    wait_remote_health_error(
        Node, ExpectedFragment, erlang:monotonic_time(millisecond) + 30_000
    ).

wait_remote_health_error(Node, ExpectedFragment, Deadline) ->
    case ?ON(Node, emqx_cluster_config_sync:on_health_check()) of
        {error, Reason} ->
            case binary:match(Reason, ExpectedFragment) of
                nomatch -> error({unexpected_health_error, Node, Reason});
                _ -> ok
            end;
        ok ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    error({health_error_timeout, Node, ExpectedFragment});
                false ->
                    receive
                    after 100 ->
                        wait_remote_health_error(Node, ExpectedFragment, Deadline)
                    end
            end
    end.

wait_until(Fun, Context) ->
    case Fun() of
        true ->
            ok;
        false ->
            receive
            after 100 ->
                wait_until(Fun, Context, erlang:monotonic_time(millisecond) + 30_000)
            end
    end.

wait_until(Fun, Context, Deadline) ->
    case Fun() of
        true ->
            ok;
        false ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    error({timeout, Context});
                false ->
                    receive
                    after 100 ->
                        wait_until(Fun, Context, Deadline)
                    end
            end
    end.

start_fake_primary(BackupName, BackupBin) ->
    Parent = self(),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, {_Addr, Port}} = inet:sockname(LSock),
    Pid = spawn(fun() -> fake_primary_loop(LSock, Parent, BackupName, BackupBin) end),
    BaseUrl = iolist_to_binary(io_lib:format("http://127.0.0.1:~B/api/v5/", [Port])),
    {Pid, BaseUrl}.

stop_fake_primary(Pid) ->
    exit(Pid, shutdown).

fake_primary_loop(LSock, Parent, BackupName, BackupBin) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            _ = spawn(fun() -> handle_fake_primary_request(Sock, Parent, BackupName, BackupBin) end),
            fake_primary_loop(LSock, Parent, BackupName, BackupBin);
        {error, closed} ->
            ok
    end.

handle_fake_primary_request(Sock, Parent, BackupName, BackupBin) ->
    try
        {Method, Path, Body} = read_http_request(Sock),
        Parent ! {fake_primary_request, Method, Path, Body},
        respond_fake_primary(Sock, Method, Path, BackupName, BackupBin)
    after
        gen_tcp:close(Sock)
    end.

read_http_request(Sock) ->
    {HeaderBin, Body0} = read_http_headers(Sock, <<>>),
    [RequestLine | HeaderLines] = binary:split(HeaderBin, <<"\r\n">>, [global]),
    [MethodBin, Path, _Version] = binary:split(RequestLine, <<" ">>, [global]),
    ContentLength = content_length(HeaderLines),
    Body = read_http_body(Sock, Body0, ContentLength),
    {method(MethodBin), Path, Body}.

read_http_headers(Sock, Acc) ->
    {ok, Bin} = gen_tcp:recv(Sock, 0, 5000),
    Acc1 = <<Acc/binary, Bin/binary>>,
    case binary:split(Acc1, <<"\r\n\r\n">>) of
        [HeaderBin, Body] ->
            {HeaderBin, Body};
        [_] ->
            read_http_headers(Sock, Acc1)
    end.

content_length(HeaderLines) ->
    lists:foldl(
        fun(Line, Acc) ->
            case binary:split(Line, <<":">>) of
                [Name, Value] ->
                    case string:lowercase(binary_to_list(Name)) of
                        "content-length" ->
                            list_to_integer(string:trim(binary_to_list(Value)));
                        _ ->
                            Acc
                    end;
                _ ->
                    Acc
            end
        end,
        0,
        HeaderLines
    ).

read_http_body(_Sock, Body, ContentLength) when byte_size(Body) >= ContentLength ->
    Body;
read_http_body(Sock, Body, ContentLength) ->
    {ok, Rest} = gen_tcp:recv(Sock, ContentLength - byte_size(Body), 5000),
    <<Body/binary, Rest/binary>>.

method(<<"POST">>) -> post;
method(<<"GET">>) -> get;
method(<<"DELETE">>) -> delete.

respond_fake_primary(Sock, post, _Path, BackupName, _BackupBin) ->
    Body = emqx_utils_json:encode(#{<<"filename">> => BackupName}),
    send_http_response(Sock, 200, <<"OK">>, Body);
respond_fake_primary(Sock, get, _Path, _BackupName, BackupBin) ->
    send_http_response(Sock, 200, <<"OK">>, BackupBin);
respond_fake_primary(Sock, delete, _Path, _BackupName, _BackupBin) ->
    send_http_response(Sock, 204, <<"No Content">>, <<>>).

send_http_response(Sock, Status, Reason, Body) ->
    Response = [
        <<"HTTP/1.1 ">>,
        integer_to_binary(Status),
        <<" ">>,
        Reason,
        <<"\r\nContent-Length: ">>,
        integer_to_binary(byte_size(Body)),
        <<"\r\nConnection: close\r\n\r\n">>,
        Body
    ],
    ok = gen_tcp:send(Sock, Response).

wait_fake_primary_requests(Count) ->
    wait_fake_primary_requests(Count, []).

wait_fake_primary_requests(Count, Acc) when length(Acc) >= Count ->
    lists:reverse(Acc);
wait_fake_primary_requests(Count, Acc) ->
    receive
        {fake_primary_request, Method, Path, Body} ->
            wait_fake_primary_requests(Count, [{Method, Path, Body} | Acc])
    after 10_000 ->
        error({missing_fake_primary_requests, Count, lists:reverse(Acc)})
    end.

assert_no_fake_primary_request(Timeout) ->
    receive
        {fake_primary_request, Method, Path, Body} ->
            error({unexpected_fake_primary_request, Method, Path, Body})
    after Timeout ->
        ok
    end.
