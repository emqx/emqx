%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_config_sync_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(CORE_NODES_TAB, emqx_cluster_config_sync_SUITE_core_nodes).
-define(INTEGRATION_RULE_ID, <<"cluster_config_sync_integration_rule">>).

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
        t_sync_once_exports_downloads_imports_and_cleans_up,
        t_sync_once_cleans_up_when_cancelled_after_export,
        t_sync_once_reports_import_errors,
        t_sync_once_reports_cleanup_errors,
        t_sync_once_reports_remote_export_errors,
        t_real_cluster_sync_runs_on_one_core_only
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
        assert_no_fake_primary_request(500)
    after
        stop_fake_primary(PrimaryPid),
        ok = emqx_cth_cluster:stop(Nodes)
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

default_root_keys() ->
    emqx_cluster_config_sync_client:default_root_keys().

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
    WorkDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
    Apps = real_cluster_apps(),
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

real_cluster_apps() ->
    Conf = #{
        listeners => #{
            tcp => #{default => <<"marked_for_deletion">>},
            ssl => #{default => <<"marked_for_deletion">>},
            ws => #{default => <<"marked_for_deletion">>},
            wss => #{default => <<"marked_for_deletion">>}
        }
    },
    [
        {emqx, #{config => Conf}},
        {emqx_conf, #{config => Conf}},
        emqx_management,
        emqx_rule_engine,
        emqx_cluster_config_sync
    ].

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
    wait_until(
        fun() ->
            lists:all(fun has_imported_rule/1, Nodes)
        end,
        #{action => wait_imported_rule, nodes => Nodes}
    ).

has_imported_rule(Node) ->
    ?ON(Node, begin
        Rules = emqx_conf:get([rule_engine, rules]),
        maps:is_key(?INTEGRATION_RULE_ID, Rules) orelse
            maps:is_key(binary_to_existing_atom(?INTEGRATION_RULE_ID, utf8), Rules)
    end).

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
