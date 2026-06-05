%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_config_sync_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
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
-define(INTEGRATION_HTTP_CONNECTOR_NAME, <<"cluster_config_sync_http_connector">>).
-define(INTEGRATION_HTTP_ACTION_NAME, <<"cluster_config_sync_http_action">>).
-define(INTEGRATION_UPDATED_RULE_ID, <<"cluster_config_sync_updated_rule">>).
-define(INTEGRATION_BANNED_CLIENTID_2, <<"cluster_config_sync_banned_client_2">>).
-define(INTEGRATION_AUTHN_USER_2, <<"cluster_config_sync_authn_user_2">>).
-define(INTEGRATION_AUTHZ_USERNAME_2, <<"cluster_config_sync_authz_user_2">>).
-define(INTEGRATION_CONFIG_ONLY_RULE_ID, <<"cluster_config_sync_config_only_rule">>).
-define(INTEGRATION_BANNED_CLIENTID_3, <<"cluster_config_sync_banned_client_3">>).
-define(INTEGRATION_AUTHN_USER_3, <<"cluster_config_sync_authn_user_3">>).
-define(INTEGRATION_AUTHZ_USERNAME_3, <<"cluster_config_sync_authz_user_3">>).
-define(PRIMARY_API_PORT, 28249).
-define(PRIMARY_BASE_PORT, 20100).
-define(CLUSTER_HOCON_FILENAME, "cluster.hocon").

all() ->
    [
        t_enabled,
        t_health_reports_missing_primary_config,
        t_interval_ms,
        t_config_change_triggers_immediate_sync,
        t_sync_runs_without_enable_or_role,
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
        t_terminate_kills_worker_after_cancel_timeout,
        t_config_schema_has_unique_record_name,
        t_config_i18n_is_populated,
        t_sync_once_uses_default_table_sets,
        t_sync_once_requires_primary_api_config,
        t_sync_once_exports_downloads_imports_and_cleans_up,
        t_sync_once_keeps_local_backup_by_default,
        t_sync_once_cleans_up_when_cancelled_after_export,
        t_sync_once_reports_import_errors,
        t_sync_once_reports_cleanup_errors,
        t_sync_once_reports_remote_export_errors,
        t_sync_once_passes_primary_ssl_options_to_httpc,
        t_sync_once_rejects_bad_primary_ssl_verify,
        t_sync_once_rejects_redirects_without_forwarding_auth,
        t_real_cluster_sync_runs_on_one_core_only,
        t_real_primary_sync_respects_root_keys_and_table_sets,
        t_real_primary_sync_file_backed_schema_registry,
        t_real_primary_sync_listener_cert_files,
        t_real_primary_sync_connector_action_cert_files,
        t_real_primary_sync_builtin_auth_runtime,
        t_real_primary_sync_repeated_updates_and_config_only_tables,
        t_real_primary_sync_deletes_default_table_data,
        t_real_sync_cancelled_during_download_cleans_up_without_import,
        t_real_sync_cancelled_during_import_finishes_and_reports_success,
        t_real_sync_reports_partial_database_failure,
        t_real_primary_sync_deletes_retained_messages,
        t_real_selected_core_takes_over_after_node_down,
        t_real_sync_reports_failure_stages,
        t_real_sync_reports_observable_failure
    ].

suite() ->
    [{timetrap, {minutes, 3}}].

init_per_testcase(_TestCase, Config) ->
    cleanup_test_artifacts(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    cleanup_test_artifacts().

t_enabled(_Config) ->
    ?assertEqual(
        false,
        emqx_cluster_config_sync:enabled(remove_primary_field(conf(), <<"base_url">>))
    ),
    ?assertEqual(
        true,
        emqx_cluster_config_sync:enabled(remove_enable_role(conf()))
    ).

t_health_reports_missing_primary_config(_Config) ->
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(
        #{}, remove_primary_field(conf(), <<"base_url">>)
    ),

    ?assertMatch({error, _}, emqx_cluster_config_sync:on_health_check()),
    Pid ! sync,
    assert_no_sync_started(100).

t_interval_ms(_Config) ->
    ?assertEqual(60000, emqx_cluster_config_sync:interval_ms(sync_config(<<"1m">>))),
    ?assertEqual(300000, emqx_cluster_config_sync:interval_ms(sync_config(<<"invalid">>))).

t_config_change_triggers_immediate_sync(_Config) ->
    setup_core_node(),
    setup_counting_sync(),
    _Pid = start_test_server(),

    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    _SyncPid = assert_sync_started().

t_sync_runs_without_enable_or_role(_Config) ->
    setup_core_node(),
    setup_counting_sync(),
    _Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, remove_enable_role(conf())),

    _SyncPid = assert_sync_started().

t_sync_runs_async_and_health_check_does_not_block(_Config) ->
    setup_core_node(),
    setup_blocking_sync(),
    _Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

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
    _Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    SyncPid = assert_sync_started(),
    Ref = erlang:monitor(process, SyncPid),
    ChangedConf = conf(<<"http://changed-primary:18083/api/v5">>),
    ok = emqx_cluster_config_sync:on_config_changed(conf(), ChangedConf),
    SyncPid ! check_cancelled,
    assert_sync_cancel_requested(SyncPid),
    assert_sync_cleanup(SyncPid),
    assert_sync_down(Ref, SyncPid).

t_config_change_reports_cancel_timeout(_Config) ->
    setup_core_node(),
    setup_cancel_ignoring_sync(),
    _Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

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

t_terminate_kills_worker_after_cancel_timeout(_Config) ->
    ?assertMatch(#{shutdown := 200}, emqx_cluster_config_sync:child_spec()),
    setup_core_node(),
    setup_cancel_ignoring_sync(),
    Pid = start_test_server(),
    ok = emqx_cluster_config_sync:on_config_changed(#{}, conf()),

    SyncPid = assert_sync_started(),
    Ref = erlang:monitor(process, SyncPid),

    ?assertEqual(ok, gen_server:stop(Pid, shutdown, 1000)),
    assert_sync_down(Ref, SyncPid).

t_config_schema_has_unique_record_name(_Config) ->
    Schema = read_plugin_json("config_schema.avsc"),
    ?assertEqual(<<"ClusterConfigSyncConfig">>, maps:get(<<"name">>, Schema)).

t_config_i18n_is_populated(_Config) ->
    I18n = read_plugin_json("config_i18n.json"),
    ?assert(maps:size(I18n) > 0),
    ?assertMatch(#{<<"en">> := _, <<"zh">> := _}, maps:get(<<"$primary_base_url_label">>, I18n)).

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

t_sync_once_requires_primary_api_config(_Config) ->
    ?assertEqual(
        {error, missing_primary_base_url},
        emqx_cluster_config_sync_client:sync_once(remove_primary_field(conf(), <<"base_url">>))
    ),
    ?assertEqual(
        {error, missing_primary_api_key},
        emqx_cluster_config_sync_client:sync_once(remove_primary_field(conf(), <<"api_key">>))
    ),
    ?assertEqual(
        {error, missing_primary_api_secret},
        emqx_cluster_config_sync_client:sync_once(remove_primary_field(conf(), <<"api_secret">>))
    ).

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

t_sync_once_keeps_local_backup_by_default(_Config) ->
    Self = self(),
    BackupName = <<"emqx-export-keep-local.tar.gz">>,
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
        import_fun => fun(_Filename) -> {ok, #{db_errors => #{}, config_errors => #{}}} end,
        delete_local_fun => fun(Filename) ->
            Self ! {delete_local, Filename},
            ok
        end
    },
    Conf = remove_delete_local_backup(conf()),

    ?assertMatch(
        {ok, #{filename := BackupName, cleanup := #{local := skipped}}},
        emqx_cluster_config_sync_client:sync_once(Conf, Deps)
    ),
    assert_not_seen({delete_local, BackupName}).

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

t_sync_once_passes_primary_ssl_options_to_httpc(Config) ->
    Self = self(),
    Cacertfile = filename:join(?config(priv_dir, Config), "cluster_config_sync_ca.pem"),
    ok = file:write_file(Cacertfile, <<"test ca">>),
    BackupName = <<"emqx-export-2026-06-02.tar.gz">>,
    ok = meck:new(httpc, [non_strict, passthrough, no_history, no_link]),
    meck:expect(httpc, request, fun
        (post, {_Url, _Headers, _ContentType, _Body}, HTTPOpts, _Opts) ->
            Self ! {http_options, HTTPOpts},
            {ok, {
                {"HTTP/1.1", 200, "OK"}, [], emqx_utils_json:encode(#{<<"filename">> => BackupName})
            }};
        (get, {_Url, _Headers}, HTTPOpts, _Opts) ->
            Self ! {http_options, HTTPOpts},
            {ok, {{"HTTP/1.1", 200, "OK"}, [], <<"backup">>}};
        (delete, {_Url, _Headers}, HTTPOpts, _Opts) ->
            Self ! {http_options, HTTPOpts},
            {ok, {{"HTTP/1.1", 204, "No Content"}, [], <<>>}}
    end),
    try
        Conf = with_primary_ssl(conf(<<"https://primary.example.com:18083/api/v5">>), #{
            <<"enable">> => true,
            <<"verify">> => <<"verify_peer">>,
            <<"server_name_indication">> => <<"primary.example.com">>,
            <<"cacertfile">> => unicode:characters_to_binary(Cacertfile)
        }),
        Deps = #{
            upload_fun => fun(_Filename, _Bin) -> ok end,
            import_fun => fun(_Filename) -> {ok, #{db_errors => #{}, config_errors => #{}}} end,
            delete_local_fun => fun(_Filename) -> ok end
        },

        ?assertMatch(
            {ok, #{filename := BackupName}},
            emqx_cluster_config_sync_client:sync_once(Conf, Deps)
        ),

        HTTPOpts = receive_http_options(),
        ?assertEqual(false, proplists:get_value(autoredirect, HTTPOpts)),
        SSLOpts = proplists:get_value(ssl, HTTPOpts),
        ?assert(is_list(SSLOpts)),
        ?assertEqual(verify_peer, proplists:get_value(verify, SSLOpts)),
        ?assertEqual(Cacertfile, proplists:get_value(cacertfile, SSLOpts)),
        ?assertEqual("primary.example.com", proplists:get_value(server_name_indication, SSLOpts))
    after
        catch meck:unload(httpc)
    end.

t_sync_once_rejects_bad_primary_ssl_verify(_Config) ->
    Conf = with_primary_ssl(conf(), #{
        <<"enable">> => true,
        <<"verify">> => <<"verify_bad">>
    }),
    ?assertEqual(
        {error, {bad_primary_ssl_verify, <<"verify_bad">>}},
        emqx_cluster_config_sync_client:sync_once(Conf)
    ).

t_sync_once_rejects_redirects_without_forwarding_auth(_Config) ->
    {TargetPid, TargetUrl} = start_redirect_target(),
    {PrimaryPid, PrimaryUrl} = start_redirect_primary(TargetUrl),
    try
        Result = emqx_cluster_config_sync_client:sync_once(conf(PrimaryUrl)),
        ?assertEqual([], collect_redirect_target_requests(100)),
        ?assertMatch({error, {http_error, post, _, 302, <<>>}}, Result)
    after
        stop_fake_primary(PrimaryPid),
        stop_fake_primary(TargetPid)
    end.

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

        Requests0 = wait_fake_primary_requests(3),
        ?assertEqual([post, get, delete], [Method || {Method, _Path, _Body} <- Requests0]),
        wait_imported_rule(Nodes),

        trigger_sync(OtherNode),
        assert_no_fake_primary_request(500),

        trigger_sync(SelectedNode),
        Requests = wait_fake_primary_requests(3),
        ?assertEqual([post, get, delete], [Method || {Method, _Path, _Body} <- Requests]),
        wait_imported_rule(Nodes),
        ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check())),

        trigger_sync(OtherNode),
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

t_real_primary_sync_connector_action_cert_files(Config) ->
    ExtraApps = bridge_apps(),
    PrimaryNodes = start_real_primary_cluster(Config, "primary_connector_files", ExtraApps),
    SecondaryNodes = start_real_secondary_cluster(Config, "secondary_connector_files", ExtraApps),
    try
        [PrimaryNode] = PrimaryNodes,
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        {PrimaryUrl, ApiKey, ApiSecret} = setup_real_primary(PrimaryNode),
        {ok, #{data_dir := PrimaryDataDir, certs := Certs}} =
            create_primary_http_connector_action(PrimaryNode),
        assert_http_connector_action_absent(SecondaryNodes),

        RootKeys = [<<"connectors">>, <<"actions">>],
        SyncConf = conf(PrimaryUrl, ApiKey, ApiSecret, RootKeys, []),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        ?check_trace(
            begin
                trigger_sync(SelectedNode),
                wait_http_connector_action_cert_files(SecondaryNodes, PrimaryDataDir, Certs),
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

t_real_primary_sync_builtin_auth_runtime(Config) ->
    ExtraApps = table_set_apps(),
    PrimaryNodes = start_real_primary_cluster(Config, "primary_auth_runtime", ExtraApps),
    SecondaryNodes = start_real_secondary_cluster(Config, "secondary_auth_runtime", ExtraApps),
    try
        [PrimaryNode] = PrimaryNodes,
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        {PrimaryUrl, ApiKey, ApiSecret} = setup_real_primary(PrimaryNode),
        ok = setup_primary_builtin_auth_runtime(PrimaryNode),
        assert_auth_runtime_absent(SecondaryNodes),

        RootKeys = [<<"authentication">>, <<"authorization">>],
        SyncConf = conf_with_default_table_sets(PrimaryUrl, ApiKey, ApiSecret, RootKeys),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        ?check_trace(
            begin
                trigger_sync(SelectedNode),
                wait_auth_runtime_usable(SecondaryNodes),
                ?block_until(#{?snk_kind := cluster_config_sync_result, stage := finished}, 10_000),
                ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check()))
            end,
            fun(Trace) ->
                assert_success_trace(Trace, SelectedNode, RootKeys, default_table_sets())
            end
        )
    after
        ok = emqx_cth_cluster:stop(SecondaryNodes),
        ok = emqx_cth_cluster:stop(PrimaryNodes)
    end.

t_real_primary_sync_repeated_updates_and_config_only_tables(Config) ->
    ExtraApps = table_set_apps(),
    PrimaryNodes = start_real_primary_cluster(Config, "primary_repeated_updates", ExtraApps),
    SecondaryNodes = start_real_secondary_cluster(Config, "secondary_repeated_updates", ExtraApps),
    try
        [PrimaryNode] = PrimaryNodes,
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        {PrimaryUrl, ApiKey, ApiSecret} = setup_real_primary(PrimaryNode),
        ok = create_primary_rule_and_table_data(PrimaryNode),

        RootKeys = [<<"rule_engine">>],
        WithDefaultTableSets = conf_with_default_table_sets(
            PrimaryUrl, ApiKey, ApiSecret, RootKeys
        ),
        ok = configure_sync_nodes([SelectedNode, OtherNode], WithDefaultTableSets),
        assert_real_sync_finished(SelectedNode, RootKeys, default_table_sets()),
        wait_imported_rule(SecondaryNodes),
        wait_default_table_data(SecondaryNodes),
        ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check())),

        ok = update_primary_rule_and_add_table_data(PrimaryNode),
        ok = configure_sync_nodes([SelectedNode, OtherNode], WithDefaultTableSets),
        assert_real_sync_finished(SelectedNode, RootKeys, default_table_sets()),
        wait_updated_rule(SecondaryNodes),
        wait_additional_default_table_data(SecondaryNodes),
        ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check())),

        ok = create_primary_config_only_update_and_more_table_data(PrimaryNode),
        ConfigOnly = conf(PrimaryUrl, ApiKey, ApiSecret, RootKeys, []),
        ok = configure_sync_nodes([SelectedNode, OtherNode], ConfigOnly),
        assert_real_sync_finished(SelectedNode, RootKeys, []),
        wait_config_only_rule(SecondaryNodes),
        assert_config_only_table_data_not_synced(SecondaryNodes),
        assert_additional_default_table_data(SecondaryNodes),
        ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check()))
    after
        ok = emqx_cth_cluster:stop(SecondaryNodes),
        ok = emqx_cth_cluster:stop(PrimaryNodes)
    end.

t_real_primary_sync_deletes_default_table_data(Config) ->
    ExtraApps = table_set_apps(),
    PrimaryNodes = start_real_primary_cluster(Config, "primary_delete_table_data", ExtraApps),
    SecondaryNodes = start_real_secondary_cluster(Config, "secondary_delete_table_data", ExtraApps),
    try
        [PrimaryNode] = PrimaryNodes,
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        {PrimaryUrl, ApiKey, ApiSecret} = setup_real_primary(PrimaryNode),
        ok = create_primary_rule_and_table_data(PrimaryNode),

        RootKeys = [<<"rule_engine">>],
        SyncConf = conf_with_default_table_sets(PrimaryUrl, ApiKey, ApiSecret, RootKeys),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        assert_real_sync_finished(SelectedNode, RootKeys, default_table_sets()),
        wait_imported_rule(SecondaryNodes),
        wait_default_table_data(SecondaryNodes),

        ok = delete_primary_default_table_data(PrimaryNode),
        assert_default_table_data_absent([PrimaryNode]),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        assert_real_sync_finished(SelectedNode, RootKeys, default_table_sets()),
        wait_default_table_data_absent(SecondaryNodes),
        wait_imported_rule(SecondaryNodes),
        ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check()))
    after
        ok = emqx_cth_cluster:stop(SecondaryNodes),
        ok = emqx_cth_cluster:stop(PrimaryNodes)
    end.

t_real_sync_cancelled_during_download_cleans_up_without_import(Config) ->
    BackupName = <<"real-cluster-config-sync-cancelled.tar.gz">>,
    BackupBin = make_rule_engine_backup(Config, BackupName),
    {PrimaryPid, PrimaryUrl} = start_fake_primary(
        fake_primary_opts(#{
            backup_name => BackupName,
            backup_bin => BackupBin,
            download_wait => true
        })
    ),
    Nodes = start_real_secondary_cluster(Config, "secondary_cancel_download"),
    try
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(Nodes),
        SyncConf = conf(PrimaryUrl),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        trigger_sync(SelectedNode),
        Requests = wait_fake_primary_requests(2),
        ?assertEqual([post, get], [Method || {Method, _Path, _Body} <- Requests]),

        DownloadHandler = wait_fake_primary_download_blocked(),
        ChangedConf = conf(<<"http://changed-primary:18083/api/v5">>),
        ok = configure_sync_nodes([SelectedNode, OtherNode], ChangedConf),
        continue_fake_primary_download(DownloadHandler),
        [{delete, DeletePath, <<>>}] = wait_fake_primary_requests(1),
        ?assertMatch(<<"/api/v5/data/files/", _/binary>>, DeletePath),
        wait_rule_absent(Nodes),
        ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check())),
        ?assertEqual(ok, ?ON(OtherNode, emqx_cluster_config_sync:on_health_check()))
    after
        stop_fake_primary(PrimaryPid),
        ok = emqx_cth_cluster:stop(Nodes)
    end.

t_real_sync_cancelled_during_import_finishes_and_reports_success(Config) ->
    BackupName = <<"real-cluster-config-sync-cancelled-during-import.tar.gz">>,
    BackupBin = make_rule_engine_backup(Config, BackupName),
    {PrimaryPid, PrimaryUrl} = start_fake_primary(BackupName, BackupBin),
    Nodes = start_real_secondary_cluster(Config, "secondary_cancel_import"),
    try
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(Nodes),
        SyncConf = conf(PrimaryUrl),
        ok = install_import_gate(SelectedNode),
        ?check_trace(
            begin
                ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
                Requests = wait_fake_primary_requests(2),
                ?assertEqual([post, get], [Method || {Method, _Path, _Body} <- Requests]),
                ImportPid = wait_import_blocked(),
                ChangedConf = conf(<<"http://changed-primary:18083/api/v5">>),
                ok = configure_sync_nodes([SelectedNode, OtherNode], ChangedConf),
                continue_import(ImportPid),
                [{delete, DeletePath, <<>>}] = wait_fake_primary_requests(1),
                ?assertMatch(<<"/api/v5/data/files/", _/binary>>, DeletePath),
                wait_imported_rule(Nodes),
                ?block_until(#{?snk_kind := cluster_config_sync_result, stage := finished}, 60_000),
                ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check())),
                ?assertEqual(ok, ?ON(OtherNode, emqx_cluster_config_sync:on_health_check()))
            end,
            fun(Trace) ->
                assert_success_trace(Trace, SelectedNode, default_root_keys(), [])
            end
        )
    after
        unload_import_gate(Nodes),
        stop_fake_primary(PrimaryPid),
        ok = emqx_cth_cluster:stop(Nodes)
    end.

t_real_sync_reports_partial_database_failure(Config) ->
    ExtraApps = table_set_apps(),
    PrimaryNodes = start_real_primary_cluster(Config, "primary_partial_db_failure", ExtraApps),
    SecondaryNodes = start_real_secondary_cluster(
        Config, "secondary_partial_db_failure", ExtraApps
    ),
    try
        [PrimaryNode] = PrimaryNodes,
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        ok = create_primary_rule_and_table_data(PrimaryNode),
        RootKeys = [<<"rule_engine">>],
        TableSets = default_table_sets(),
        BackupName = <<"cluster-config-sync-partial-db-failure.tar.gz">>,
        BackupBin0 = export_primary_backup_bin(Config, PrimaryNode, RootKeys, TableSets),
        BackupBin = corrupt_backup_mnesia_records(
            Config,
            BackupName,
            BackupBin0,
            emqx_authn_mnesia,
            fun
                ({schema, _Tab, _CreateList} = Schema) ->
                    Schema;
                (_Record) ->
                    {user_info, <<"bad_record">>}
            end
        ),
        {FakePid, PrimaryUrl} = start_fake_primary(BackupName, BackupBin),
        try
            SyncConf = conf(PrimaryUrl, <<"key">>, <<"secret">>, RootKeys, TableSets),
            ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
            ?check_trace(
                begin
                    trigger_sync(SelectedNode),
                    wait_partial_default_table_data(SecondaryNodes),
                    wait_remote_health_error(SelectedNode, <<"emqx_authn_mnesia">>),
                    ?block_until(
                        #{?snk_kind := cluster_config_sync_result, result := failed}, 60_000
                    )
                end,
                fun(Trace) ->
                    Event = assert_failure_trace_stage(
                        Trace, SelectedNode, RootKeys, TableSets, import
                    ),
                    ?assertNotEqual(
                        nomatch,
                        binary:match(format_term(maps:get(reason, Event)), <<"emqx_authn_mnesia">>)
                    )
                end
            )
        after
            stop_fake_primary(FakePid)
        end
    after
        ok = emqx_cth_cluster:stop(SecondaryNodes),
        ok = emqx_cth_cluster:stop(PrimaryNodes)
    end.

t_real_primary_sync_deletes_retained_messages(Config) ->
    ExtraApps = table_set_apps(),
    PrimaryNodes = start_real_primary_cluster(Config, "primary_delete_retainer", ExtraApps),
    SecondaryNodes = start_real_secondary_cluster(Config, "secondary_delete_retainer", ExtraApps),
    try
        [PrimaryNode] = PrimaryNodes,
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        {PrimaryUrl, ApiKey, ApiSecret} = setup_real_primary(PrimaryNode),
        ok = create_primary_rule_and_table_data(PrimaryNode),

        RootKeys = [<<"rule_engine">>],
        TableSets = [<<"builtin_retainer">>],
        SyncConf = conf(PrimaryUrl, ApiKey, ApiSecret, RootKeys, TableSets),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        assert_real_sync_finished(SelectedNode, RootKeys, TableSets),
        wait_retainer_data(SecondaryNodes),

        ok = delete_primary_retained_message(PrimaryNode),
        assert_retainer_absent([PrimaryNode]),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        assert_real_sync_finished(SelectedNode, RootKeys, TableSets),
        wait_retainer_absent(SecondaryNodes),
        wait_imported_rule(SecondaryNodes),
        ?assertEqual(ok, ?ON(SelectedNode, emqx_cluster_config_sync:on_health_check()))
    after
        ok = emqx_cth_cluster:stop(SecondaryNodes),
        ok = emqx_cth_cluster:stop(PrimaryNodes)
    end.

t_real_selected_core_takes_over_after_node_down(Config) ->
    BackupName = <<"real-cluster-config-sync-failover.tar.gz">>,
    BackupBin = make_rule_engine_backup(Config, BackupName),
    {PrimaryPid, PrimaryUrl} = start_fake_primary(BackupName, BackupBin),
    Nodes = start_real_secondary_cluster(Config, "secondary_failover"),
    try
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(Nodes),
        SyncConf = conf(PrimaryUrl),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        ok = emqx_cth_cluster:stop([SelectedNode]),
        wait_until(
            fun() -> ?ON(OtherNode, mria_membership:running_core_nodelist()) =:= [OtherNode] end,
            #{action => wait_failover_membership, node => OtherNode}
        ),

        ?check_trace(
            begin
                trigger_sync(OtherNode),
                Requests = wait_fake_primary_requests(3),
                ?assertEqual([post, get, delete], [Method || {Method, _Path, _Body} <- Requests]),
                wait_imported_rule([OtherNode]),
                ?block_until(#{?snk_kind := cluster_config_sync_result, stage := finished}, 10_000),
                ?assertEqual(ok, ?ON(OtherNode, emqx_cluster_config_sync:on_health_check()))
            end,
            fun(Trace) ->
                assert_success_trace(Trace, OtherNode, default_root_keys(), [])
            end
        )
    after
        stop_fake_primary(PrimaryPid),
        catch emqx_cth_cluster:stop(Nodes)
    end.

t_real_sync_reports_failure_stages(Config) ->
    SecondaryNodes = start_real_secondary_cluster(Config, "secondary_failure_stages"),
    try
        [SelectedNode, OtherNode] = select_cluster_sync_nodes(SecondaryNodes),
        RootKeys = [<<"rule_engine">>],
        ok = assert_real_sync_failure_stage(
            Config,
            [SelectedNode, OtherNode],
            RootKeys,
            download,
            fake_primary_opts(#{
                backup_name => <<"cluster-config-sync-download-failure.tar.gz">>,
                download_status => 500,
                download_body => <<"download failed">>
            }),
            <<"http_error">>
        ),
        ok = assert_real_sync_failure_stage(
            Config,
            [SelectedNode, OtherNode],
            RootKeys,
            upload,
            fake_primary_opts(#{
                backup_name => <<"cluster-config-sync-upload-failure.tar.gz">>,
                backup_bin => make_upload_error_backup(
                    Config, <<"cluster-config-sync-upload-failure.tar.gz">>
                )
            }),
            <<"upload_failed">>
        ),
        ok = assert_real_sync_failure_stage(
            Config,
            [SelectedNode, OtherNode],
            RootKeys,
            import,
            fake_primary_opts(#{
                backup_name => <<"cluster-config-sync-import-failure.tar.gz">>,
                backup_bin => make_import_error_backup(
                    Config, <<"cluster-config-sync-import-failure.tar.gz">>
                )
            }),
            <<"import_failed">>
        ),
        ok = assert_real_sync_failure_stage(
            Config,
            [SelectedNode, OtherNode],
            RootKeys,
            cleanup,
            fake_primary_opts(#{
                backup_name => <<"cluster-config-sync-cleanup-failure.tar.gz">>,
                backup_bin => make_rule_engine_backup(
                    Config, <<"cluster-config-sync-cleanup-failure.tar.gz">>
                ),
                delete_status => 500,
                delete_body => <<"delete failed">>
            }),
            <<"cleanup_failed">>
        )
    after
        ok = emqx_cth_cluster:stop(SecondaryNodes)
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

remove_delete_local_backup(Conf) ->
    Sync = maps:get(<<"sync">>, Conf),
    Conf#{<<"sync">> => maps:remove(<<"delete_local_backup">>, Sync)}.

remove_enable_role(Conf) ->
    maps:without([<<"enable">>, <<"role">>], Conf).

remove_primary_field(Conf, Field) ->
    Primary = maps:get(<<"primary">>, Conf),
    Conf#{<<"primary">> => maps:remove(Field, Primary)}.

with_primary_ssl(Conf, SSL) ->
    Primary = maps:get(<<"primary">>, Conf),
    Conf#{<<"primary">> => Primary#{<<"ssl">> => SSL}}.

receive_http_options() ->
    receive
        {http_options, HTTPOpts} -> HTTPOpts
    after 0 ->
        error(missing_http_options)
    end.

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

assert_not_seen(Msg) ->
    receive
        Msg -> error({unexpected_message, Msg})
    after 0 ->
        ok
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

read_plugin_json(Filename) ->
    Path = filename:join([code:priv_dir(emqx_cluster_config_sync), Filename]),
    {ok, Bin} = file:read_file(Path),
    emqx_utils_json:decode(Bin).

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

bridge_apps() ->
    [
        emqx_connector,
        emqx_bridge,
        emqx_bridge_http
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
        ok = create_rule(
            ?INTEGRATION_RULE_ID,
            <<"SELECT * FROM \"t/#\"">>,
            <<"cluster config sync integration test">>
        ),
        ok = create_banned(?INTEGRATION_BANNED_CLIENTID),
        ok = create_authn_user(?INTEGRATION_AUTHN_USER),
        ok = create_authz_rules(?INTEGRATION_AUTHZ_USERNAME),
        ok = store_retained_message(?INTEGRATION_RETAINER_TOPIC, ?INTEGRATION_RETAINER_PAYLOAD),
        ok
    end).

update_primary_rule_and_add_table_data(PrimaryNode) ->
    ?ON(PrimaryNode, begin
        ok = update_rule(
            ?INTEGRATION_RULE_ID,
            <<"SELECT * FROM \"t/updated/#\"">>,
            <<"cluster config sync integration test updated">>
        ),
        ok = create_rule(
            ?INTEGRATION_UPDATED_RULE_ID,
            <<"SELECT * FROM \"t/added/#\"">>,
            <<"cluster config sync integration test added">>
        ),
        ok = create_banned(?INTEGRATION_BANNED_CLIENTID_2),
        ok = create_authn_user(?INTEGRATION_AUTHN_USER_2),
        ok = create_authz_rules(?INTEGRATION_AUTHZ_USERNAME_2),
        ok
    end).

create_primary_config_only_update_and_more_table_data(PrimaryNode) ->
    ?ON(PrimaryNode, begin
        ok = create_rule(
            ?INTEGRATION_CONFIG_ONLY_RULE_ID,
            <<"SELECT * FROM \"t/config-only/#\"">>,
            <<"cluster config sync integration test config only">>
        ),
        ok = create_banned(?INTEGRATION_BANNED_CLIENTID_3),
        ok = create_authn_user(?INTEGRATION_AUTHN_USER_3),
        ok = create_authz_rules(?INTEGRATION_AUTHZ_USERNAME_3),
        ok
    end).

delete_primary_default_table_data(PrimaryNode) ->
    ?ON(PrimaryNode, begin
        ok = delete_banned(?INTEGRATION_BANNED_CLIENTID),
        ok = delete_authn_user(?INTEGRATION_AUTHN_USER),
        ok = delete_authz_rules(?INTEGRATION_AUTHZ_USERNAME),
        ok
    end).

delete_primary_retained_message(PrimaryNode) ->
    ?ON(PrimaryNode, emqx_retainer:delete(?INTEGRATION_RETAINER_TOPIC)).

create_rule(RuleId, SQL, Description) ->
    {201, _Rule} = emqx_rule_engine_api:'/rules'(post, #{
        body => rule_body(RuleId, SQL, Description)
    }),
    ok.

update_rule(RuleId, SQL, Description) ->
    {200, _Rule} = emqx_rule_engine_api:'/rules/:id'(put, #{
        bindings => #{id => RuleId},
        body => rule_body(RuleId, SQL, Description)
    }),
    ok.

rule_body(RuleId, SQL, Description) ->
    #{
        <<"id">> => RuleId,
        <<"name">> => RuleId,
        <<"enable">> => true,
        <<"sql">> => SQL,
        <<"actions">> => [#{<<"function">> => <<"console">>}],
        <<"description">> => Description
    }.

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

delete_banned(ClientId) ->
    ok = emqx_banned:delete(emqx_banned:who(clientid, ClientId)).

create_authn_user(UserId) ->
    {ok, State} = emqx_authn_mnesia:create(
        <<"cluster_config_sync:built_in_database">>, authn_config()
    ),
    {ok, _} = emqx_authn_mnesia:add_user(
        #{user_id => UserId, password => <<"secret">>}, State
    ),
    ok.

delete_authn_user(UserId) ->
    ok = emqx_authn_mnesia:delete_user(UserId, authn_config()).

authn_config() ->
    #{
        user_id_type => username,
        password_hash_algorithm => #{
            name => bcrypt,
            salt_rounds => 8
        },
        user_group => 'mqtt:global'
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

delete_authz_rules(Username) ->
    ok = emqx_authz_mnesia:delete_rules({username, Username}).

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

export_primary_backup_bin(Config, PrimaryNode, RootKeys, TableSets) ->
    OutDir = filename:join(?config(priv_dir, Config), "primary_export"),
    ?ON(PrimaryNode, begin
        ok = filelib:ensure_dir(filename:join(OutDir, "placeholder")),
        {ok, Opts0} = emqx_mgmt_data_backup:parse_export_request(#{
            <<"root_keys">> => RootKeys,
            <<"table_sets">> => TableSets
        }),
        {ok, #{filename := Filename}} = emqx_mgmt_data_backup:export(Opts0#{out_dir => OutDir}),
        {ok, Bin} = file:read_file(Filename),
        Bin
    end).

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

create_primary_http_connector_action(PrimaryNode) ->
    ?ON(PrimaryNode, begin
        Certs = tls_cert_contents(),
        ConnectorConfig = http_connector_config(Certs),
        ActionConfig = http_action_config(),
        {ok, _Connector} = emqx_connector:create(
            <<"http">>, ?INTEGRATION_HTTP_CONNECTOR_NAME, ConnectorConfig
        ),
        {ok, _Action} = emqx_bridge_v2:create(
            <<"http">>, ?INTEGRATION_HTTP_ACTION_NAME, ActionConfig
        ),
        {ok, #{data_dir => emqx:data_dir(), certs => Certs}}
    end).

http_connector_config(Certs) ->
    #{
        <<"enable">> => false,
        <<"url">> => <<"https://127.0.0.1:65535">>,
        <<"headers">> => #{},
        <<"connect_timeout">> => <<"1s">>,
        <<"max_inactive">> => <<"10s">>,
        <<"pool_type">> => <<"hash">>,
        <<"pool_size">> => 1,
        <<"ssl">> => #{
            <<"enable">> => true,
            <<"verify">> => <<"verify_peer">>,
            <<"cacertfile">> => maps:get(cacertfile, Certs),
            <<"certfile">> => maps:get(certfile, Certs),
            <<"keyfile">> => maps:get(keyfile, Certs)
        },
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"100ms">>
        }
    }.

http_action_config() ->
    #{
        <<"enable">> => false,
        <<"connector">> => ?INTEGRATION_HTTP_CONNECTOR_NAME,
        <<"parameters">> => #{
            <<"path">> => <<"/cluster-config-sync">>,
            <<"method">> => <<"post">>,
            <<"headers">> => #{},
            <<"body">> => <<"${.}">>
        },
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"100ms">>
        }
    }.

setup_primary_builtin_auth_runtime(PrimaryNode) ->
    ?ON(PrimaryNode, begin
        {ok, _} = emqx:update_config(
            [authentication],
            {create_authenticator, 'mqtt:global', builtin_authn_raw_config()}
        ),
        ok = create_authn_user(?INTEGRATION_AUTHN_USER),
        {ok, _} = emqx:update_config([authorization], builtin_authz_raw_config()),
        ok = create_authz_rules(?INTEGRATION_AUTHN_USER),
        ok
    end).

builtin_authn_raw_config() ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"enable">> => true,
        <<"user_id_type">> => <<"username">>,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"bcrypt">>,
            <<"salt_rounds">> => 8
        }
    }.

builtin_authz_raw_config() ->
    #{
        <<"no_match">> => <<"deny">>,
        <<"cache">> => #{<<"enable">> => false},
        <<"sources">> => [
            #{
                <<"type">> => <<"file">>,
                <<"enable">> => false,
                <<"path">> => <<"${EMQX_ETC_DIR}/acl.conf">>
            },
            #{
                <<"type">> => <<"built_in_database">>,
                <<"enable">> => true
            }
        ]
    }.

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

make_upload_error_backup(Config, BackupName) ->
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
                        <<"cluster_config_sync_bad_import_rule">> => #{
                            <<"description">> => <<"invalid rule for import failure">>,
                            <<"sql">> => <<"SELECT * FROM">>,
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

make_import_error_backup(Config, BackupName) ->
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
    ok = erl_tar:create(
        BackupFile,
        [
            {filename:join(BackupRoot, "META.hocon"), Meta},
            {filename:join([BackupRoot, "mnesia", "emqx_banned"]), <<"not a mnesia backup">>}
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
    has_rule(Node, ?INTEGRATION_RULE_ID).

wait_rule_absent(Nodes) ->
    wait_until(
        fun() ->
            lists:all(fun(Node) -> not has_imported_rule(Node) end, Nodes)
        end,
        #{action => wait_rule_absent, nodes => Nodes}
    ).

has_rule(Node, RuleId) ->
    ?ON(Node, begin
        Rules = emqx_conf:get([rule_engine, rules]),
        RuleIdAtom = binary_to_existing_atom_or_undefined(RuleId),
        maps:is_key(RuleId, Rules) orelse
            maps:is_key(RuleIdAtom, Rules)
    end).

rule_description(Node, RuleId) ->
    ?ON(Node, begin
        Rules = emqx_conf:get([rule_engine, rules]),
        RuleIdAtom = binary_to_existing_atom_or_undefined(RuleId),
        case maps:get(RuleId, Rules, maps:get(RuleIdAtom, Rules, undefined)) of
            undefined ->
                undefined;
            Rule ->
                maps:get(description, Rule, maps:get(<<"description">>, Rule, undefined))
        end
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

wait_updated_rule(Nodes) ->
    wait_until(
        fun() ->
            lists:all(fun has_updated_rule/1, Nodes)
        end,
        #{action => wait_updated_rule, nodes => Nodes}
    ).

has_updated_rule(Node) ->
    has_rule(Node, ?INTEGRATION_UPDATED_RULE_ID) andalso
        rule_description(Node, ?INTEGRATION_RULE_ID) =:=
            <<"cluster config sync integration test updated">>.

wait_config_only_rule(Nodes) ->
    wait_until(
        fun() ->
            lists:all(fun(Node) -> has_rule(Node, ?INTEGRATION_CONFIG_ONLY_RULE_ID) end, Nodes)
        end,
        #{action => wait_config_only_rule, nodes => Nodes}
    ).

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

wait_retainer_data(Nodes) ->
    wait_until(
        fun() ->
            lists:all(
                fun(Node) -> has_retained_message(Node, ?INTEGRATION_RETAINER_TOPIC) end, Nodes
            )
        end,
        #{action => wait_retainer_data, nodes => Nodes}
    ).

wait_retainer_absent(Nodes) ->
    wait_until(
        fun() ->
            lists:all(
                fun(Node) -> not has_retained_message(Node, ?INTEGRATION_RETAINER_TOPIC) end,
                Nodes
            )
        end,
        #{action => wait_retainer_absent, nodes => Nodes}
    ).

wait_default_table_data(Nodes) ->
    wait_until(
        fun() ->
            lists:all(fun has_default_table_data/1, Nodes)
        end,
        #{action => wait_default_table_data, nodes => Nodes}
    ).

wait_default_table_data_absent(Nodes) ->
    wait_default_table_data_absent(
        Nodes,
        erlang:monotonic_time(millisecond) + 30_000
    ).

wait_default_table_data_absent(Nodes, Deadline) ->
    case lists:all(fun default_table_data_absent/1, Nodes) of
        true ->
            ok;
        false ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    error(
                        {timeout, #{
                            action => wait_default_table_data_absent,
                            nodes => Nodes,
                            status => default_table_data_status(Nodes)
                        }}
                    );
                false ->
                    receive
                    after 100 ->
                        wait_default_table_data_absent(Nodes, Deadline)
                    end
            end
    end.

assert_default_table_data_absent(Nodes) ->
    Status = default_table_data_status(Nodes),
    ?assert(lists:all(fun default_table_data_absent_status/1, Status), Status).

wait_table_data(Nodes) ->
    wait_until(
        fun() ->
            lists:all(fun has_all_table_data/1, Nodes)
        end,
        #{action => wait_table_data, nodes => Nodes}
    ).

wait_partial_default_table_data(Nodes) ->
    wait_until(
        fun() ->
            lists:all(fun has_partial_default_table_data/1, Nodes)
        end,
        #{action => wait_partial_default_table_data, nodes => Nodes}
    ).

wait_additional_default_table_data(Nodes) ->
    wait_until(
        fun() ->
            lists:all(fun has_additional_default_table_data/1, Nodes)
        end,
        #{action => wait_additional_default_table_data, nodes => Nodes}
    ).

assert_additional_default_table_data(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assertEqual(true, has_additional_default_table_data(Node))
        end,
        Nodes
    ).

assert_config_only_table_data_not_synced(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assertEqual(false, has_banned(Node, ?INTEGRATION_BANNED_CLIENTID_3)),
            ?assertEqual(false, has_authn_user(Node, ?INTEGRATION_AUTHN_USER_3)),
            ?assertEqual(false, has_authz_rules(Node, ?INTEGRATION_AUTHZ_USERNAME_3))
        end,
        Nodes
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

has_partial_default_table_data(Node) ->
    has_banned(Node, ?INTEGRATION_BANNED_CLIENTID) andalso
        not has_authn_user(Node, ?INTEGRATION_AUTHN_USER) andalso
        has_authz_rules(Node, ?INTEGRATION_AUTHZ_USERNAME).

default_table_data_absent(Node) ->
    default_table_data_absent_status(default_table_data_status(Node)).

default_table_data_absent_status(#{banned := false, authn := false, authz := false}) ->
    true;
default_table_data_absent_status(_) ->
    false.

default_table_data_status(Nodes) when is_list(Nodes) ->
    [default_table_data_status(Node) || Node <- Nodes];
default_table_data_status(Node) ->
    #{
        node => Node,
        banned => has_banned(Node, ?INTEGRATION_BANNED_CLIENTID),
        authn => has_authn_user(Node, ?INTEGRATION_AUTHN_USER),
        authz => has_authz_rules(Node, ?INTEGRATION_AUTHZ_USERNAME)
    }.

has_additional_default_table_data(Node) ->
    has_banned(Node, ?INTEGRATION_BANNED_CLIENTID_2) andalso
        has_authn_user(Node, ?INTEGRATION_AUTHN_USER_2) andalso
        has_authz_rules(Node, ?INTEGRATION_AUTHZ_USERNAME_2).

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

assert_http_connector_action_absent(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assertEqual(false, has_http_connector_action(Node))
        end,
        Nodes
    ).

has_http_connector_action(Node) ->
    ?ON(Node, http_connector_conf() =/= undefined andalso http_action_conf() =/= undefined).

wait_http_connector_action_cert_files(Nodes, PrimaryDataDir, Certs) ->
    wait_until(
        fun() ->
            lists:all(
                fun(Node) ->
                    http_connector_action_cert_files_synced(Node, PrimaryDataDir, Certs)
                end,
                Nodes
            )
        end,
        #{action => wait_http_connector_action_cert_files, nodes => Nodes}
    ).

http_connector_action_cert_files_synced(Node, PrimaryDataDir, Certs) ->
    ?ON(Node, begin
        case {http_connector_conf(), http_action_conf()} of
            {#{<<"enable">> := false, <<"ssl">> := SSL}, Action} when is_map(Action) ->
                action_connector_matches(Action) andalso
                    tls_cert_file_synced(cacertfile, SSL, PrimaryDataDir, Certs) andalso
                    tls_cert_file_synced(certfile, SSL, PrimaryDataDir, Certs) andalso
                    tls_cert_file_synced(keyfile, SSL, PrimaryDataDir, Certs);
            _ ->
                false
        end
    end).

http_connector_conf() ->
    emqx:get_raw_config([connectors, http, ?INTEGRATION_HTTP_CONNECTOR_NAME], undefined).

http_action_conf() ->
    emqx:get_raw_config([actions, http, ?INTEGRATION_HTTP_ACTION_NAME], undefined).

action_connector_matches(#{<<"connector">> := Connector, <<"parameters">> := Parameters}) ->
    Connector =:= ?INTEGRATION_HTTP_CONNECTOR_NAME andalso
        maps:get(<<"path">>, Parameters, undefined) =:= <<"/cluster-config-sync">>;
action_connector_matches(_) ->
    false.

assert_auth_runtime_absent(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assertEqual(false, has_builtin_auth_runtime_config(Node)),
            ?assertEqual(false, has_authn_user(Node, ?INTEGRATION_AUTHN_USER)),
            ?assertEqual(false, has_authz_rules(Node, ?INTEGRATION_AUTHN_USER))
        end,
        Nodes
    ).

has_builtin_auth_runtime_config(Node) ->
    ?ON(Node, begin
        AuthnConfigured =
            case emqx_authn_chains:list_authenticators('mqtt:global') of
                {ok, Authenticators} ->
                    lists:any(
                        fun(#{id := ID}) -> ID =:= <<"password_based:built_in_database">> end,
                        Authenticators
                    );
                _ ->
                    false
            end,
        AuthzConfigured =
            lists:any(
                fun(Source) ->
                    maps:get(type, Source, maps:get(<<"type">>, Source, undefined)) =:=
                        built_in_database orelse
                        maps:get(type, Source, maps:get(<<"type">>, Source, undefined)) =:=
                            <<"built_in_database">>
                end,
                emqx:get_config([authorization, sources], [])
            ),
        AuthnConfigured orelse AuthzConfigured
    end).

wait_auth_runtime_usable(Nodes) ->
    try
        wait_until(
            fun() ->
                lists:all(fun auth_runtime_usable/1, Nodes)
            end,
            #{action => wait_auth_runtime_usable, nodes => Nodes}
        )
    catch
        error:{timeout, Context} ->
            error({timeout, Context#{diagnostics => auth_runtime_diagnostics(Nodes)}})
    end.

auth_runtime_diagnostics(Nodes) ->
    [{Node, auth_runtime_status(Node)} || Node <- Nodes].

auth_runtime_status(Node) ->
    ?ON(Node, begin
        GoodCredentials = authn_credentials(?INTEGRATION_AUTHN_USER, <<"secret">>),
        BadCredentials = authn_credentials(?INTEGRATION_AUTHN_USER, <<"bad-secret">>),
        ClientInfo = authz_client_info(?INTEGRATION_AUTHN_USER),
        #{
            authenticators => safe_eval(fun() ->
                emqx_authn_chains:list_authenticators('mqtt:global')
            end),
            raw_authentication => safe_eval(fun() ->
                emqx:get_raw_config([authentication], undefined)
            end),
            authz_sources => safe_eval(fun() ->
                emqx:get_config([authorization, sources], [])
            end),
            authn_user => safe_eval(fun() ->
                emqx_authn_mnesia:lookup_user(?INTEGRATION_AUTHN_USER, authn_config())
            end),
            authz_rules => safe_eval(fun() ->
                emqx_authz_mnesia:get_rules({username, ?INTEGRATION_AUTHN_USER})
            end),
            authn_good => safe_eval(fun() ->
                emqx_access_control:authenticate(GoodCredentials)
            end),
            authn_bad => safe_eval(fun() ->
                emqx_access_control:authenticate(BadCredentials)
            end),
            authz_allow => safe_eval(fun() ->
                emqx_access_control:authorize(
                    ClientInfo, ?AUTHZ_PUBLISH, <<"cluster/config/sync">>
                )
            end),
            authz_deny => safe_eval(fun() ->
                emqx_access_control:authorize(
                    ClientInfo, ?AUTHZ_PUBLISH, <<"cluster/config/sync/denied">>
                )
            end)
        }
    end).

safe_eval(Fun) ->
    try Fun() of
        Result -> Result
    catch
        Class:Reason:Stacktrace ->
            {Class, Reason, Stacktrace}
    end.

auth_runtime_usable(Node) ->
    ?ON(Node, begin
        try
            GoodCredentials = authn_credentials(?INTEGRATION_AUTHN_USER, <<"secret">>),
            BadCredentials = authn_credentials(?INTEGRATION_AUTHN_USER, <<"bad-secret">>),
            AuthnOK =
                case emqx_access_control:authenticate(GoodCredentials) of
                    {ok, _} -> true;
                    _ -> false
                end,
            AuthnReject =
                case emqx_access_control:authenticate(BadCredentials) of
                    {ok, _} -> false;
                    _ -> true
                end,
            ClientInfo = authz_client_info(?INTEGRATION_AUTHN_USER),
            AuthzAllow =
                emqx_access_control:authorize(
                    ClientInfo, ?AUTHZ_PUBLISH, <<"cluster/config/sync">>
                ) =:= allow,
            AuthzDeny =
                emqx_access_control:authorize(
                    ClientInfo, ?AUTHZ_PUBLISH, <<"cluster/config/sync/denied">>
                ) =:= deny,
            AuthnOK andalso AuthnReject andalso AuthzAllow andalso AuthzDeny
        catch
            _:_ ->
                false
        end
    end).

authn_credentials(Username, Password) ->
    #{
        zone => default,
        listener => 'tcp:default',
        protocol => mqtt,
        clientid => <<"cluster_config_sync_auth_runtime_client">>,
        username => Username,
        password => Password,
        peerhost => {127, 0, 0, 1},
        peerport => 1883
    }.

authz_client_info(Username) ->
    #{
        zone => default,
        listener => 'tcp:default',
        protocol => mqtt,
        clientid => <<"cluster_config_sync_auth_runtime_client">>,
        username => Username,
        peerhost => {127, 0, 0, 1},
        peerport => 1883,
        is_superuser => false
    }.

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

assert_real_sync_finished(SelectedNode, RootKeys, TableSets) ->
    ?check_trace(
        begin
            trigger_sync(SelectedNode),
            ?block_until(#{?snk_kind := cluster_config_sync_result, stage := finished}, 60_000)
        end,
        fun(Trace) ->
            assert_success_trace(Trace, SelectedNode, RootKeys, TableSets)
        end
    ).

assert_failure_trace(Trace, SelectedNode, RootKeys, TableSets, Secret) ->
    Event = assert_failure_trace_stage(Trace, SelectedNode, RootKeys, TableSets, export),
    Reason = maps:get(reason, Event),
    ?assertMatch({http_error, post, _, _, _}, Reason),
    ?assertEqual(nomatch, binary:match(format_term(Reason), Secret)),
    ok.

assert_failure_trace_stage(Trace, SelectedNode, RootKeys, TableSets, Stage) ->
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
    ?assertEqual(Stage, maps:get(stage, Event)),
    ?assert(maps:is_key(reason, Event)),
    Event.

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

assert_real_sync_failure_stage(_Config, Nodes, RootKeys, Stage, FakePrimaryOpts, HealthFragment) ->
    [SelectedNode, OtherNode] = Nodes,
    {PrimaryPid, PrimaryUrl} = start_fake_primary(FakePrimaryOpts),
    try
        SyncConf = conf(PrimaryUrl, <<"key">>, <<"secret">>, RootKeys, []),
        ok = configure_sync_nodes([SelectedNode, OtherNode], SyncConf),
        ?check_trace(
            begin
                trigger_sync(SelectedNode),
                wait_remote_health_error(SelectedNode, HealthFragment),
                ?block_until(#{?snk_kind := cluster_config_sync_result, result := failed}, 10_000)
            end,
            fun(Trace) ->
                _Event = assert_failure_trace_stage(Trace, SelectedNode, RootKeys, [], Stage),
                ok
            end
        ),
        ok
    after
        stop_fake_primary(PrimaryPid)
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

fake_primary_opts(Overrides) ->
    maps:merge(
        #{
            backup_name => <<"backup.tar.gz">>,
            backup_bin => <<>>,
            export_status => 200,
            export_body => undefined,
            download_status => 200,
            download_body => undefined,
            download_wait => false,
            delete_status => 204,
            delete_body => <<>>
        },
        Overrides
    ).

install_import_gate(Node) ->
    Parent = self(),
    ?ON(Node, begin
        meck:new(emqx_mgmt_data_backup, [passthrough, no_link]),
        meck:expect(emqx_mgmt_data_backup, import, 1, fun(Filename) ->
            Parent ! {import_blocked, self()},
            receive
                continue_import ->
                    ok
            after 30_000 ->
                error(import_gate_timeout)
            end,
            meck:passthrough([Filename])
        end),
        meck:expect(emqx_mgmt_data_backup, import, 2, fun(Filename, Opts) ->
            Parent ! {import_blocked, self()},
            receive
                continue_import ->
                    ok
            after 30_000 ->
                error(import_gate_timeout)
            end,
            meck:passthrough([Filename, Opts])
        end)
    end),
    ok.

unload_import_gate(Nodes) ->
    lists:foreach(
        fun(Node) ->
            catch ?ON(Node, meck:unload(emqx_mgmt_data_backup))
        end,
        Nodes
    ),
    ok.

wait_import_blocked() ->
    receive
        {import_blocked, Pid} -> Pid
    after 10_000 ->
        error(import_not_blocked)
    end.

continue_import(Pid) ->
    Pid ! continue_import,
    ok.

corrupt_backup_mnesia_records(Config, BackupName, BackupBin, Tab, TransformFun) ->
    rewrite_backup_archive(
        Config,
        BackupName,
        BackupBin,
        fun(Path, Bin) ->
            case is_mnesia_backup_entry(Path, Tab) of
                true ->
                    transform_mnesia_backup_bin(Config, Bin, TransformFun);
                false ->
                    Bin
            end
        end
    ).

rewrite_backup_archive(Config, BackupName, BackupBin, RewriteFun) ->
    Prefix = filename:join(?config(priv_dir, Config), integer_to_list(erlang:unique_integer())),
    InFile = Prefix ++ ".tar.gz",
    OutFile = Prefix ++ "-rewritten.tar.gz",
    ok = file:write_file(InFile, BackupBin),
    {ok, Entries0} = erl_tar:extract(InFile, [compressed, memory]),
    BackupRoot = filename:basename(binary_to_list(BackupName), ".tar.gz"),
    Entries = [
        {replace_archive_root(Path, BackupRoot), RewriteFun(Path, Bin)}
     || {Path, Bin} <- Entries0
    ],
    ok = erl_tar:create(OutFile, Entries, [compressed]),
    {ok, OutBin} = file:read_file(OutFile),
    OutBin.

replace_archive_root(Path0, BackupRoot) ->
    [_OldRoot | Rest] = filename:split(unicode:characters_to_list(Path0)),
    filename:join([BackupRoot | Rest]).

transform_mnesia_backup_bin(Config, Bin, TransformFun) ->
    Prefix = filename:join(?config(priv_dir, Config), integer_to_list(erlang:unique_integer())),
    InFile = Prefix ++ ".mnesia.in",
    OutFile = Prefix ++ ".mnesia.out",
    ok = file:write_file(InFile, Bin),
    {ok, ok} = mnesia:traverse_backup(
        InFile,
        mnesia_backup,
        OutFile,
        mnesia_backup,
        fun(Item, Acc) ->
            {[TransformFun(Item)], Acc}
        end,
        ok
    ),
    {ok, OutBin} = file:read_file(OutFile),
    OutBin.

is_mnesia_backup_entry(Path0, Tab) ->
    Path = unicode:characters_to_list(Path0),
    lists:suffix(filename:join(["mnesia", atom_to_list(Tab)]), Path).

start_fake_primary(BackupName, BackupBin) ->
    start_fake_primary(fake_primary_opts(#{backup_name => BackupName, backup_bin => BackupBin})).

start_fake_primary(Opts) ->
    Parent = self(),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, {_Addr, Port}} = inet:sockname(LSock),
    Pid = spawn(fun() -> fake_primary_loop(LSock, Parent, Opts) end),
    BaseUrl = iolist_to_binary(io_lib:format("http://127.0.0.1:~B/api/v5/", [Port])),
    {Pid, BaseUrl}.

stop_fake_primary(Pid) ->
    exit(Pid, shutdown).

fake_primary_loop(LSock, Parent, Opts) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            _ = spawn(fun() -> handle_fake_primary_request(Sock, Parent, Opts) end),
            fake_primary_loop(LSock, Parent, Opts);
        {error, closed} ->
            ok
    end.

handle_fake_primary_request(Sock, Parent, Opts) ->
    try
        {Method, Path, Body} = read_http_request(Sock),
        Parent ! {fake_primary_request, Method, Path, Body},
        respond_fake_primary(Sock, Method, Path, Parent, Opts)
    after
        gen_tcp:close(Sock)
    end.

read_http_request(Sock) ->
    {Method, Path, _HeaderLines, Body} = read_http_request_with_headers(Sock),
    {Method, Path, Body}.

read_http_request_with_headers(Sock) ->
    {HeaderBin, Body0} = read_http_headers(Sock, <<>>),
    [RequestLine | HeaderLines] = binary:split(HeaderBin, <<"\r\n">>, [global]),
    [MethodBin, Path, _Version] = binary:split(RequestLine, <<" ">>, [global]),
    ContentLength = content_length(HeaderLines),
    Body = read_http_body(Sock, Body0, ContentLength),
    {method(MethodBin), Path, HeaderLines, Body}.

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

respond_fake_primary(Sock, post, _Path, _Parent, Opts) ->
    Status = maps:get(export_status, Opts),
    DefaultBody = emqx_utils_json:encode(#{<<"filename">> => maps:get(backup_name, Opts)}),
    Body = response_body(maps:get(export_body, Opts), DefaultBody),
    send_http_response(Sock, Status, http_reason(Status), Body);
respond_fake_primary(Sock, get, _Path, Parent, Opts) ->
    maybe_wait_fake_primary_download(Parent, Opts),
    Status = maps:get(download_status, Opts),
    Body = response_body(maps:get(download_body, Opts), maps:get(backup_bin, Opts)),
    send_http_response(Sock, Status, http_reason(Status), Body);
respond_fake_primary(Sock, delete, _Path, _Parent, Opts) ->
    Status = maps:get(delete_status, Opts),
    Body = maps:get(delete_body, Opts),
    send_http_response(Sock, Status, http_reason(Status), Body).

start_redirect_target() ->
    Parent = self(),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, {_Addr, Port}} = inet:sockname(LSock),
    Pid = spawn(fun() -> redirect_target_loop(LSock, Parent) end),
    Url = iolist_to_binary(io_lib:format("http://127.0.0.1:~B/redirect-target", [Port])),
    {Pid, Url}.

redirect_target_loop(LSock, Parent) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            _ = spawn(fun() -> handle_redirect_target_request(Sock, Parent) end),
            redirect_target_loop(LSock, Parent);
        {error, closed} ->
            ok
    end.

handle_redirect_target_request(Sock, Parent) ->
    try
        {Method, Path, HeaderLines, Body} = read_http_request_with_headers(Sock),
        Auth = header_value(<<"authorization">>, HeaderLines),
        Parent ! {redirect_target_request, Method, Path, Auth, Body},
        send_http_response(Sock, 500, http_reason(500), <<"redirect target">>)
    after
        gen_tcp:close(Sock)
    end.

start_redirect_primary(TargetUrl) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, {_Addr, Port}} = inet:sockname(LSock),
    Pid = spawn(fun() -> redirect_primary_loop(LSock, TargetUrl) end),
    BaseUrl = iolist_to_binary(io_lib:format("http://127.0.0.1:~B/api/v5/", [Port])),
    {Pid, BaseUrl}.

redirect_primary_loop(LSock, TargetUrl) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            _ = spawn(fun() -> handle_redirect_primary_request(Sock, TargetUrl) end),
            redirect_primary_loop(LSock, TargetUrl);
        {error, closed} ->
            ok
    end.

handle_redirect_primary_request(Sock, TargetUrl) ->
    try
        {_Method, _Path, _HeaderLines, _Body} = read_http_request_with_headers(Sock),
        send_http_redirect(Sock, TargetUrl)
    after
        gen_tcp:close(Sock)
    end.

header_value(HeaderName, HeaderLines) ->
    LowerName = string:lowercase(binary_to_list(HeaderName)),
    lists:foldl(
        fun(Line, Acc) ->
            case {Acc, binary:split(Line, <<":">>)} of
                {undefined, [Name, Value]} ->
                    case string:lowercase(binary_to_list(Name)) of
                        LowerName -> string:trim(Value);
                        _ -> undefined
                    end;
                _ ->
                    Acc
            end
        end,
        undefined,
        HeaderLines
    ).

maybe_wait_fake_primary_download(Parent, #{download_wait := true}) ->
    Parent ! {fake_primary_download_blocked, self()},
    receive
        continue_fake_primary_download ->
            ok
    after 30_000 ->
        error(fake_primary_download_wait_timeout)
    end;
maybe_wait_fake_primary_download(_Parent, _Opts) ->
    ok.

response_body(undefined, DefaultBody) ->
    DefaultBody;
response_body(Body, _DefaultBody) ->
    Body.

http_reason(200) -> <<"OK">>;
http_reason(204) -> <<"No Content">>;
http_reason(400) -> <<"Bad Request">>;
http_reason(401) -> <<"Unauthorized">>;
http_reason(404) -> <<"Not Found">>;
http_reason(500) -> <<"Internal Server Error">>;
http_reason(_Status) -> <<"Status">>.

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

send_http_redirect(Sock, Location) ->
    Response = [
        <<"HTTP/1.1 302 Found\r\nLocation: ">>,
        Location,
        <<"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n">>
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

wait_fake_primary_download_blocked() ->
    receive
        {fake_primary_download_blocked, Pid} -> Pid
    after 10_000 ->
        error(fake_primary_download_not_blocked)
    end.

continue_fake_primary_download(Pid) ->
    Pid ! continue_fake_primary_download,
    ok.

assert_no_fake_primary_request(Timeout) ->
    receive
        {fake_primary_request, Method, Path, Body} ->
            error({unexpected_fake_primary_request, Method, Path, Body})
    after Timeout ->
        ok
    end.

collect_redirect_target_requests(Timeout) ->
    collect_redirect_target_requests(Timeout, []).

collect_redirect_target_requests(Timeout, Acc) ->
    receive
        {redirect_target_request, Method, Path, Auth, Body} ->
            collect_redirect_target_requests(0, [{Method, Path, Auth, Body} | Acc])
    after Timeout ->
        lists:reverse(Acc)
    end.
