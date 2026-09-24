%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_tests).

-include("emqx_plugins.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(nowarn_export_all).
-compile(export_all).

ensure_configured_test_todo() ->
    meck_emqx(),
    try
        test_ensure_configured()
    after
        emqx_plugins:put_configured([])
    end,
    unmeck_emqx().

test_ensure_configured() ->
    ok = emqx_plugins:put_configured([]),
    P1 = #{name_vsn => "p-1", enable => true},
    P2 = #{name_vsn => "p-2", enable => true},
    P3 = #{name_vsn => "p-3", enable => false},
    emqx_plugins:ensure_configured(P1, front, local),
    emqx_plugins:ensure_configured(P2, {before, <<"p-1">>}, local),
    emqx_plugins:ensure_configured(P3, {before, <<"p-1">>}, local),
    ?assertEqual([P2, P3, P1], emqx_plugins:configured()),
    ?assertThrow(
        #{error := "position_anchor_plugin_not_configured"},
        emqx_plugins:ensure_configured(P3, {before, <<"unknown-x">>}, local)
    ).

ensure_configured_same_name_disables_other_versions_test() ->
    meck_emqx(),
    try
        ok = emqx_plugins:put_configured([]),
        P1 = #{name_vsn => "p-1.0.0", enable => true},
        P2 = #{name_vsn => "p-2.0.0", enable => true},
        ok = emqx_plugins:ensure_configured(P1, rear, local),
        ok = emqx_plugins:ensure_configured(P2, rear, local),
        ?assertEqual(
            [
                #{name_vsn => "p-1.0.0", enable => false},
                #{name_vsn => "p-2.0.0", enable => true}
            ],
            emqx_plugins:configured()
        )
    after
        emqx_plugins:put_configured([])
    end,
    unmeck_emqx().

normalize_enabled_versions_prefers_latest_test() ->
    ?assertEqual(
        [
            #{name_vsn => "p-1.0.0", enable => false},
            #{name_vsn => "p-2.0.0", enable => true},
            #{name_vsn => "q-1.0.0", enable => true}
        ],
        emqx_plugins:normalize_enabled_versions([
            #{name_vsn => "p-1.0.0", enable => true},
            #{name_vsn => "p-2.0.0", enable => true},
            #{name_vsn => "q-1.0.0", enable => true}
        ])
    ).

running_status_requires_matching_version_test() ->
    [{AppName, _Desc, Vsn} | _] = application:which_applications(infinity),
    ExactNameVsn = atom_to_list(AppName) ++ "-" ++ Vsn,
    MismatchedNameVsn = atom_to_list(AppName) ++ "-0.0.0",
    ?assertEqual(running, emqx_plugins_apps:running_status(ExactNameVsn)),
    ?assertEqual(stopped, emqx_plugins_apps:running_status(MismatchedNameVsn)).

is_package_present_requires_exact_version_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                Tar1 = emqx_plugins_fs:tar_file_path("p-1.0.0"),
                Tar2 = emqx_plugins_fs:tar_file_path("p-2.0.0"),
                ok = write_file(Tar1, <<"p1">>),
                ok = write_file(Tar2, <<"p2">>),
                ?assertEqual({true, [Tar1]}, emqx_plugins:is_package_present("p-1.0.0")),
                ?assertEqual({true, [Tar2]}, emqx_plugins:is_package_present("p-2.0.0")),
                ?assertEqual(false, emqx_plugins:is_package_present("p-3.0.0"))
            end
        )
    after
        unmeck_emqx()
    end.

configured_normalizes_binary_key_items_test() ->
    meck_emqx(),
    try
        ok = emqx_plugins:put_config_internal(states, [
            #{<<"name_vsn">> => <<"p-1.0.0">>, <<"enable">> => true}
        ]),
        ?assertEqual(
            [#{name_vsn => <<"p-1.0.0">>, enable => true}],
            emqx_plugins:configured()
        )
    after
        emqx_plugins:put_configured([]),
        unmeck_emqx()
    end.

read_plugin_test() ->
    meck_emqx(),
    with_rand_install_dir(
        fun(_Dir) ->
            NameVsn = "bar-5",
            InfoFile = emqx_plugins_fs:info_file_path(NameVsn),
            FakeInfo =
                "name=bar, rel_vsn=\"5\", rel_apps=[justname_no_vsn],"
                "description=\"desc bar\"",
            try
                ok = write_file(InfoFile, FakeInfo),
                ?assertMatch(
                    {error, #{msg := "bad_rel_apps"}},
                    emqx_plugins:read_plugin_info(NameVsn, #{})
                )
            after
                emqx_plugins:purge(NameVsn)
            end
        end
    ),
    unmeck_emqx().

with_rand_install_dir(F) ->
    N = rand:uniform(10000000),
    TmpDir = integer_to_list(N),
    OriginalInstallDir = emqx_plugins_fs:install_dir(),
    ok = filelib:ensure_dir(filename:join([TmpDir, "foo"])),
    ok = emqx_plugins:put_config_internal(install_dir, TmpDir),
    %% the plugin's configuration status is read from the config
    ok = emqx_plugins:put_config_internal(states, []),
    try
        F(TmpDir)
    after
        file:del_dir_r(TmpDir),
        ok = emqx_plugins:put_config_internal(install_dir, OriginalInstallDir)
    end.

write_file(Path, Content) ->
    ok = filelib:ensure_dir(Path),
    file:write_file(Path, Content).

%% The retry of a start which was refused the installation lock runs in its own
%% process (`emqx_plugins:enable_disable_plugin/2'), so the test waits for the
%% installation it makes instead of reading it directly.  The wait is bounded
%% well below the EUnit timeout of the test function, so a missing retry is
%% reported as a failure of this assertion instead of cancelling the run.
wait_for_extraction_complete(_NameVsn, 0) ->
    error(plugin_was_not_installed);
wait_for_extraction_complete(NameVsn, Attempts) ->
    case emqx_plugins_fs:is_extraction_complete(NameVsn) of
        true ->
            ok;
        false ->
            timer:sleep(50),
            wait_for_extraction_complete(NameVsn, Attempts - 1)
    end.

package_operations_validate_names_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(fun(Dir) ->
            InstallDir = filename:join(Dir, "plugins"),
            ok = emqx_plugins:put_config_internal(install_dir, InstallDir),
            Kept = filename:join(Dir, "kept-1"),
            Files = [filename:join(Kept, "marker"), Kept ++ ".tar.gz", Kept ++ ".tar.gz.md5sum"],
            lists:foreach(fun(F) -> ok = write_file(F, <<"original">>) end, Files),
            Operations = [
                fun emqx_plugins:ensure_installed/1,
                fun(N) -> emqx_plugins:ensure_installed(N, fresh_install) end,
                fun emqx_plugins:ensure_uninstalled/1,
                fun emqx_plugins:purge/1,
                fun emqx_plugins:safe_delete_package/1,
                fun emqx_plugins:purge_other_versions/1,
                fun(N) -> emqx_plugins:install_package(N, <<"replacement">>) end,
                fun(N) -> emqx_plugins:write_package(N, <<"replacement">>) end,
                fun emqx_plugins:delete_package/1,
                fun emqx_plugins:backup_package/1,
                fun(N) -> emqx_plugins:restore_package(N, #{tar => none, md5sum => none}) end,
                fun emqx_plugins_fs:prepare_replacement/1,
                fun emqx_plugins_fs:purge_installed/1,
                fun emqx_plugins_fs:delete_tar/1,
                fun emqx_plugins_fs:get_tar/1,
                fun emqx_plugins_fs:ensure_config_dir/1
            ],
            Names = [
                "../kept-1", filename:absname(Kept), "valid-1/../../kept-1", "..-1", ".-1", "-1"
            ],
            lists:foreach(
                fun(Name) ->
                    lists:foreach(
                        fun(N) ->
                            lists:foreach(
                                fun(Op) ->
                                    ?assertMatch(
                                        {error, #{msg := "bad_plugin_package_name"}}, Op(N)
                                    ),
                                    lists:foreach(
                                        fun(F) ->
                                            ?assertEqual({ok, <<"original">>}, file:read_file(F))
                                        end,
                                        Files
                                    )
                                end,
                                Operations
                            )
                        end,
                        [Name, list_to_binary(Name)]
                    )
                end,
                Names
            ),
            ?assertEqual(false, filelib:is_dir(InstallDir))
        end)
    after
        unmeck_emqx()
    end.

package_paths_validate_components_test() ->
    Paths = [
        fun emqx_plugins_fs:plugin_dir/1,
        fun emqx_plugins_fs:lib_dir/1,
        fun emqx_plugins_fs:tar_file_path/1
    ],
    lists:foreach(
        fun(Name) ->
            lists:foreach(
                fun(Path) ->
                    ?assertException(error, #{msg := "bad_plugin_package_name"}, Path(Name))
                end,
                Paths
            )
        end,
        [
            "/",
            <<"/">>,
            "../p-1",
            "/p-1",
            "p-1/",
            "p-1\\child",
            "p-1\n",
            "p-1" ++ [0],
            ".emqx-plugin-staging"
        ]
    ),
    lists:foreach(
        fun(Name) ->
            ?assertException(
                error, #{msg := "bad_plugin_package_name"}, emqx_plugins_fs:config_file_path(Name)
            )
        end,
        ["..-1", ".-1", "-1", "../p-1"]
    ),
    lists:foreach(
        fun(Path) ->
            ?assertEqual(Path("plugin-1"), Path(<<"plugin-1">>)),
            ?assert(is_list(Path("plugin-1")))
        end,
        Paths
    ).

%% delete package should mostly work and return ok
%% but it may fail in case the path is a directory
%% or if the file is read-only
delete_package_test() ->
    meck_emqx(),
    with_rand_install_dir(
        fun(_Dir) ->
            File = emqx_plugins_fs:tar_file_path("a-1"),
            ok = write_file(File, "a"),
            ok = emqx_plugins_fs:delete_tar("a-1"),
            %% delete again should be ok
            ok = emqx_plugins_fs:delete_tar("a-1"),
            Dir = File,
            ok = filelib:ensure_dir(filename:join([Dir, "foo"])),
            ?assertMatch({error, _}, emqx_plugins_fs:delete_tar("a-1"))
        end
    ),
    unmeck_emqx().

%% purge plugin's install dir should mostly work and return ok
%% but it may fail in case the dir is read-only
purge_test() ->
    meck_emqx(),
    with_rand_install_dir(
        fun(_Dir) ->
            File = emqx_plugins_fs:info_file_path("a-1"),
            Dir = emqx_plugins_fs:plugin_dir("a-1"),
            ok = filelib:ensure_dir(File),
            ?assertMatch({ok, _}, file:read_file_info(Dir)),
            ?assertEqual(ok, emqx_plugins:purge("a-1")),
            %% assert the dir is gone
            ?assertMatch({error, enoent}, file:read_file_info(Dir)),
            %% write a file for the dir path
            ok = file:write_file(Dir, "a"),
            ?assertEqual(ok, emqx_plugins:purge("a-1"))
        end
    ),
    unmeck_emqx().

meck_emqx() ->
    start_install_serializer(),
    meck:new(emqx, [passthrough]),
    meck:new(emqx_plugins_serde),
    meck:expect(
        emqx,
        update_config,
        fun(Path, Values, _Opts) ->
            emqx_config:put(Path, Values)
        end
    ),
    meck:expect(
        emqx_plugins_serde,
        add_schema,
        fun(_NameVsn, _AvscBin) -> ok end
    ),
    meck:expect(
        emqx_plugins_serde,
        delete_schema,
        fun(_NameVsn) -> ok end
    ),
    ok.

unmeck_emqx() ->
    meck:unload(emqx),
    meck:unload(emqx_plugins_serde),
    stop_install_serializer(),
    ok.

%% The installation path takes its lock from `emqx_plugins_install_serializer',
%% which the plugins application starts in a running node.  These tests run
%% without the plugins application (and without the rest of EMQX), so the server
%% is started here; a node without peers coordinates with itself, so the lock is
%% the same one a single node installation would take.
start_install_serializer() ->
    case whereis(emqx_plugins_install_serializer) of
        undefined ->
            {ok, _Pid} = emqx_plugins_install_serializer:start_link(),
            ok;
        _Pid ->
            ok
    end.

stop_install_serializer() ->
    case whereis(emqx_plugins_install_serializer) of
        undefined ->
            ok;
        Pid ->
            ok = gen_server:stop(Pid)
    end.

ensure_installed_from_tar_purges_leftovers_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "leftover-0.1.0",
                StaleFile = filename:join(emqx_plugins_fs:plugin_dir(NameVsn), "stale.txt"),
                ok = write_file(StaleFile, <<"stale">>),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                %% the directory alone makes `is_installed/1' true
                ?assert(emqx_plugins_fs:is_installed(NameVsn)),
                ?assertMatch({error, _}, (validator(NameVsn))()),
                ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, validator(NameVsn)),
                ?assertEqual(
                    {ok, release_json(NameVsn)},
                    file:read_file(emqx_plugins_fs:info_file_path(NameVsn))
                ),
                %% the leftovers are gone, not merged with the newly unpacked files
                ?assertEqual({error, enoent}, file:read_file_info(StaleFile)),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% Same as above, but with a `release.json' that cannot be read: it must be
%% replaced by the one from the package.
ensure_installed_from_tar_replaces_broken_info_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "broken-0.1.0",
                ok = write_file(emqx_plugins_fs:info_file_path(NameVsn), <<"bad-syntax">>),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ?assert(emqx_plugins_fs:is_installed(NameVsn)),
                ?assertMatch({error, _}, (validator(NameVsn))()),
                ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, validator(NameVsn)),
                ?assertEqual(
                    {ok, release_json(NameVsn)},
                    file:read_file(emqx_plugins_fs:info_file_path(NameVsn))
                ),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% A package which could not be installed because the cluster wide installation
%% lock was not available is refused before anything is written: the package file
%% and the unpack are one critical section, so a refused attempt leaves neither a
%% package file nor a plugin directory behind.
install_package_writes_nothing_when_the_install_lock_is_unavailable_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "locked-0.1.0",
                ok = meck:new(emqx_plugins_install_serializer, [passthrough]),
                try
                    ok = meck:expect(emqx_plugins_install_serializer, run, fun(_NameVsn, _Fun) ->
                        {error, #{
                            msg => "failed_to_acquire_plugin_install_lock",
                            reason => coordinator_unknown
                        }}
                    end),
                    ?assertMatch(
                        {error, #{msg := "failed_to_acquire_plugin_install_lock"}},
                        emqx_plugins:install_package(NameVsn, plugin_package(NameVsn, []))
                    ),
                    ?assertEqual(false, emqx_plugins:is_package_present(NameVsn)),
                    ?assertNot(emqx_plugins_fs:is_installed(NameVsn))
                after
                    ok = meck:unload(emqx_plugins_install_serializer)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% A plugin whose installation is already complete on this node is started even
%% when the cluster wide installation lock can not be taken: loading it unpacks
%% and writes nothing, and the node would otherwise run without a plugin its own
%% configuration enables.  The check is on the installation, not on the caller:
%% a package which still has to be unpacked keeps reporting the refusal.
start_package_of_an_installed_plugin_does_not_need_the_install_lock_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                Installed = "already_installed-0.1.0",
                ok = install_plugin_files(Installed, []),
                ok = meck:new(emqx_plugins_install_serializer, [passthrough]),
                try
                    ok = meck:expect(emqx_plugins_install_serializer, run, fun(_NameVsn, _Fun) ->
                        {error, #{
                            msg => "failed_to_acquire_plugin_install_lock",
                            reason => installation_in_progress
                        }}
                    end),
                    ?assertEqual(ok, emqx_plugins:ensure_start_package(Installed)),
                    ?assertMatch(
                        {error, #{msg := "failed_to_acquire_plugin_install_lock"}},
                        emqx_plugins:ensure_start_package("not_installed-0.1.0")
                    )
                after
                    ok = meck:unload(emqx_plugins_install_serializer)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% An installation which runs on this node can be between the publication and
%% the validation of its tree: the tree is already complete, but it can still be
%% rolled back.  A start which can not take the lock must keep the refusal while
%% such an installation is running, and only bypass it when no installation of
%% this application runs here (the installation on another node can not touch
%% this node's install directory).
start_package_keeps_the_refusal_while_a_local_install_runs_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "already_installed-0.1.0",
                ok = install_plugin_files(NameVsn, []),
                Parent = self(),
                {Holder, Ref} = spawn_monitor(fun() ->
                    emqx_plugins_fs:with_installation_lock(NameVsn, fun() ->
                        Parent ! {locked, self()},
                        receive
                            release -> ok
                        end
                    end)
                end),
                receive
                    {locked, Holder} -> ok
                after 5000 ->
                    error(lock_not_taken)
                end,
                ?assertMatch(
                    {error, #{reason := installation_in_progress}},
                    emqx_plugins:ensure_start_package(NameVsn)
                ),
                Holder ! release,
                receive
                    {'DOWN', Ref, process, Holder, _} -> ok
                after 5000 ->
                    error(install_did_not_finish)
                end,
                ?assertEqual(false, emqx_plugins_install_serializer:install_in_progress(NameVsn)),
                ?assertEqual(ok, emqx_plugins:ensure_start_package(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% Purging the other versions of a plugin only needs the two textual parts of
%% the name: the name comes from a request and does not become an atom.
purge_other_versions_keeps_the_name_textual_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                Name = "purge_textual_" ++ integer_to_list(erlang:unique_integer([positive])),
                ?assertNot(is_existing_atom(Name)),
                ?assertEqual(ok, emqx_plugins:purge_other_versions(Name ++ "-1.0")),
                ?assertNot(is_existing_atom(Name)),
                ?assertEqual(ok, emqx_plugins:purge_other_versions(list_to_binary(Name ++ "-1.0"))),
                ?assertNot(is_existing_atom(Name))
            end
        )
    after
        unmeck_emqx()
    end.

is_existing_atom(Name) ->
    try
        _ = binary_to_existing_atom(list_to_binary(Name), utf8),
        true
    catch
        error:badarg -> false
    end.

%% The boot path of an enabled plugin waits for a transient installation lock
%% contention instead of skipping the plugin and leaving the node running
%% without its hooks.  The lock is held for the time of one installation, so the
%% first attempts are refused and the retry has to bring the installation
%% forward.
start_enabled_plugin_waits_for_the_install_lock_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "booting-0.1.0",
                ok = emqx_plugins:put_config_internal(states, [
                    #{name_vsn => NameVsn, enable => true}
                ]),
                erase(install_lock_calls),
                erase(install_lock_refusals),
                ok = meck:new(emqx_plugins_install_serializer, [passthrough]),
                try
                    ok = meck:expect(emqx_plugins_install_serializer, run, fun(NV, Fun) ->
                        Calls =
                            case get(install_lock_calls) of
                                undefined -> 0;
                                Counted -> Counted
                            end,
                        put(install_lock_calls, Calls + 1),
                        Refusals =
                            case get(install_lock_refusals) of
                                undefined -> 0;
                                Refused -> Refused
                            end,
                        case Refusals < 2 of
                            true ->
                                put(install_lock_refusals, Refusals + 1),
                                {error, #{
                                    msg => "failed_to_acquire_plugin_install_lock",
                                    reason => installation_in_progress
                                }};
                            false ->
                                meck:passthrough([NV, Fun])
                        end
                    end),
                    ?assertEqual(ok, emqx_plugins:ensure_started()),
                    %% two refused attempts, then the retry which takes the lock
                    ?assertEqual(2, get(install_lock_refusals)),
                    ?assert(get(install_lock_calls) > 2)
                after
                    ok = meck:unload(emqx_plugins_install_serializer)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% Enabling an installed plugin through the configuration starts it even when
%% the cluster wide installation lock is held elsewhere for a moment.  This
%% asserts that the refused start is retried and runs the installation it needs;
%% the Common Test case of the same name asserts the plugin is running
%% afterwards.  The configuration is not applied again after it was stored, so a
%% start which was dropped here would leave the plugin loaded and stopped while
%% the stored configuration says enabled.
enable_disable_plugin_waits_for_the_install_lock_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "config_enable-0.1.0",
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                %% The retry of the start runs in its own process, so the
                %% refusals are counted with a shared counter.
                LockCalls = atomics:new(1, []),
                ok = meck:new(emqx_plugins_install_serializer, [passthrough]),
                try
                    ok = meck:expect(emqx_plugins_install_serializer, run, fun(NV, Fun) ->
                        case atomics:add_get(LockCalls, 1, 1) of
                            N when N =< 2 ->
                                {error, #{
                                    msg => "failed_to_acquire_plugin_install_lock",
                                    reason => installation_in_progress
                                }};
                            _Taken ->
                                meck:passthrough([NV, Fun])
                        end
                    end),
                    ok = emqx_plugins:post_config_update(
                        [plugins],
                        undefined,
                        %% the stored state, now enabling the plugin
                        #{states => [#{name_vsn => NameVsn, enable => true}]},
                        %% the stored state before the update
                        #{states => [#{name_vsn => NameVsn, enable => false}]},
                        #{}
                    ),
                    %% the refused start was retried, and the retry ran the
                    %% installation the start needs
                    ok = wait_for_extraction_complete(NameVsn, 40),
                    ?assert(atomics:get(LockCalls, 1) > 2)
                after
                    ok = meck:unload(emqx_plugins_install_serializer)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% A node which does not hold the package fetches it from another node, writes
%% it to the shared package file and unpacks it in one critical section: while
%% that runs, another installation of the same name-vsn is refused instead of
%% racing the package file which is about to be read back.
ensure_start_package_prepares_the_fetched_package_under_the_install_lock_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "fetched-0.1.0",
                Tar = plugin_package(NameVsn, []),
                Parent = self(),
                ok = meck:new(mria, [passthrough]),
                ok = meck:new(emqx_plugins_proto_v2, [passthrough]),
                try
                    ok = meck:expect(mria, running_nodes, fun() -> [node(), 'peer@host'] end),
                    ok = meck:expect(emqx_plugins_proto_v2, get_tar, fun(_Node, _NV, _Timeout) ->
                        Parent ! {concurrent_install, install_in_another_process(NameVsn)},
                        {ok, Tar}
                    end),
                    ?assertEqual(ok, emqx_plugins:ensure_start_package(NameVsn)),
                    receive
                        {concurrent_install, Result} ->
                            ?assertMatch(
                                {error, #{reason := installation_in_progress}},
                                Result
                            )
                    after 5000 ->
                        error(no_concurrent_install_result)
                    end,
                    ?assertEqual({ok, Tar}, emqx_plugins_fs:get_tar(NameVsn))
                after
                    ok = meck:unload(emqx_plugins_proto_v2),
                    ok = meck:unload(mria)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% The cluster install reads the package of this node in the same critical
%% section which writes the shared package file: a concurrent upload of the same
%% name-vsn could otherwise make it read half of its content and push that to
%% every node.  The snapshot is taken before the per-node calls, each of which
%% takes the installation lock for its own installation.
ensure_installed_cluster_snapshots_the_package_under_the_install_lock_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "cluster_snapshot-0.1.0",
                ok = emqx_plugins:write_package(NameVsn, plugin_package(NameVsn, [])),
                ok = meck:expect(emqx, running_nodes, fun() -> [node()] end),
                ok = meck:new(emqx_plugins_proto_v5, [passthrough]),
                ok = meck:new(emqx_plugins_fs, [passthrough]),
                try
                    ok = meck:expect(emqx_plugins_proto_v5, install_package, fun(_Nodes, _NV, _Bin) ->
                        %% `erpc:multicall/5' returns one `{ok, Result}' per node
                        [{ok, ok}]
                    end),
                    ok = meck:expect(emqx_plugins_fs, get_tar, fun(NV) ->
                        put(
                            snapshot_lock,
                            maps:get(owner, emqx_plugins_install_serializer:lock_status())
                        ),
                        meck:passthrough([NV])
                    end),
                    erase(snapshot_lock),
                    Json = emqx_plugins_cli_utils:ensure_installed_cluster(
                        NameVsn,
                        fun(Format, Args) -> iolist_to_binary(io_lib:format(Format, Args)) end
                    ),
                    ?assertMatch(#{<<"result">> := <<"ok">>}, emqx_utils_json:decode(Json)),
                    %% the read happened while this process held the lock
                    ?assert(is_pid(get(snapshot_lock)))
                after
                    ok = meck:unload(emqx_plugins_fs),
                    ok = meck:unload(emqx_plugins_proto_v5)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% The cluster install walks the nodes in order and stops at the first node
%% which can not take the cluster wide installation lock: installing on the
%% remaining nodes would update them while the refused node keeps its old
%% package, and a retry could then be refused as already installed by a node
%% which was updated.
ensure_installed_cluster_stops_at_the_first_node_without_the_lock_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "cluster_stop-0.1.0",
                ok = emqx_plugins:write_package(NameVsn, plugin_package(NameVsn, [])),
                Peer = 'sec376_cli_cluster_peer@nowhere',
                ok = meck:expect(emqx, running_nodes, fun() -> [node(), Peer] end),
                ok = meck:new(emqx_plugins, [passthrough]),
                ok = meck:new(emqx_plugins_proto_v5, [passthrough]),
                try
                    ok = meck:expect(emqx_plugins, node_supports_install_lock, fun(_Node) ->
                        true
                    end),
                    ok = meck:expect(emqx_plugins_proto_v5, install_package, fun([Node], _NV, _Bin) ->
                        case Node of
                            Peer ->
                                [{ok, ok}];
                            _ ->
                                [
                                    {ok,
                                        {error, #{
                                            msg => "failed_to_acquire_plugin_install_lock",
                                            reason => busy
                                        }}}
                                ]
                        end
                    end),
                    Json = emqx_plugins_cli_utils:ensure_installed_cluster(
                        NameVsn,
                        fun(Format, Args) -> iolist_to_binary(io_lib:format(Format, Args)) end
                    ),
                    ?assertMatch(#{<<"result">> := <<"not_ok">>}, emqx_utils_json:decode(Json)),
                    %% the peer was never asked to install
                    ?assertEqual(
                        1,
                        meck:num_calls(emqx_plugins_proto_v5, install_package, ['_', '_', '_'])
                    )
                after
                    ok = meck:unload(emqx_plugins_proto_v5),
                    ok = meck:unload(emqx_plugins)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% The cluster install also stops at a node which can not be reached: `erpc'
%% answers with the caught call exception for such a node instead of an
%% installation result, and installing on the remaining nodes would leave the
%% cluster split in the same way a refused lock does.
ensure_installed_cluster_stops_at_the_first_unreachable_node_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "cluster_unreachable-0.1.0",
                ok = emqx_plugins:write_package(NameVsn, plugin_package(NameVsn, [])),
                Peer = 'sec376_cli_unreachable_peer@nowhere',
                ok = meck:expect(emqx, running_nodes, fun() -> [node(), Peer] end),
                ok = meck:new(emqx_plugins, [passthrough]),
                ok = meck:new(emqx_plugins_proto_v5, [passthrough]),
                try
                    ok = meck:expect(emqx_plugins, node_supports_install_lock, fun(_Node) ->
                        true
                    end),
                    ok = meck:expect(emqx_plugins_proto_v5, install_package, fun([Node], _NV, _Bin) ->
                        case Node of
                            Peer -> [{ok, ok}];
                            _ -> [{error, {erpc, noconnection}}]
                        end
                    end),
                    Json = emqx_plugins_cli_utils:ensure_installed_cluster(
                        NameVsn,
                        fun(Format, Args) -> iolist_to_binary(io_lib:format(Format, Args)) end
                    ),
                    ?assertMatch(#{<<"result">> := <<"not_ok">>}, emqx_utils_json:decode(Json)),
                    %% the peer was never asked to install
                    ?assertEqual(
                        1,
                        meck:num_calls(emqx_plugins_proto_v5, install_package, ['_', '_', '_'])
                    )
                after
                    ok = meck:unload(emqx_plugins_proto_v5),
                    ok = meck:unload(emqx_plugins)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% The fresh-install overload is called by the CLI directly, so it takes the
%% installation lock itself: the configuration which follows the installation
%% runs in the same critical section, so another installation attempt can not
%% interleave with it.
ensure_installed_fresh_install_holds_the_install_lock_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "cli_fresh-0.1.0",
                ok = emqx_plugins:write_package(NameVsn, plugin_package(NameVsn, [])),
                %% the cluster wide configuration store of a running node is out
                %% of scope here
                ok = meck:new(emqx_conf, [passthrough]),
                ok = meck:expect(emqx_conf, update, fun(_Path, _Values, _Opts) -> {ok, #{}} end),
                erase(configure_owner),
                ok = meck:new(emqx_plugins_local_config, [passthrough]),
                try
                    ok = meck:expect(emqx_plugins_local_config, copy_default, fun(NV) ->
                        %% `configure' runs after the package was unpacked; the
                        %% lock has to still be held here.
                        put(
                            configure_owner,
                            maps:get(owner, emqx_plugins_install_serializer:lock_status())
                        ),
                        meck:passthrough([NV])
                    end),
                    ?assertEqual(ok, emqx_plugins:ensure_installed(NameVsn, fresh_install)),
                    ?assert(is_pid(get(configure_owner)))
                after
                    ok = meck:unload(emqx_plugins_local_config),
                    ok = meck:unload(emqx_conf)
                end,
                ?assertEqual(installed, emqx_plugins:install_state(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% A failed installation keeps the package which was on disk before the attempt:
%% the content which failed is not the one to keep, and the previous package may
%% be the only local copy which can repair the installation later.
install_package_restores_the_previous_package_on_failure_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "keep_previous-0.1.0",
                ok = emqx_plugins:write_package(NameVsn, <<"previous package">>),
                {ok, Before} = emqx_plugins:backup_package(NameVsn),
                ?assertMatch(
                    {error, _},
                    emqx_plugins:install_package(NameVsn, plugin_package("other-0.1.0", []))
                ),
                ?assertEqual({ok, Before}, emqx_plugins:backup_package(NameVsn)),
                ?assertNot(emqx_plugins_fs:is_installed(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

install_in_another_process(NameVsn) ->
    Parent = self(),
    Pid = spawn(fun() ->
        Parent ! {install_result, emqx_plugins:install_package(NameVsn, <<"another package">>)}
    end),
    receive
        {install_result, Result} ->
            Result
    after 5000 ->
        exit(Pid, kill),
        error(concurrent_install_timeout)
    end.

%% A healthy installation is kept as is: it is not purged, and the package is
%% not unpacked again (other nodes may be running the plugin).
ensure_installed_from_tar_keeps_healthy_install_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "healthy-0.1.0",
                PluginDir = emqx_plugins_fs:plugin_dir(NameVsn),
                ok = install_plugin_files(NameVsn, []),
                Marker = filename:join(PluginDir, "marker.txt"),
                ok = write_file(Marker, <<"keep me">>),
                %% this file is in the package but not in the install dir: it would
                %% show up if the package was unpacked again
                ok = emqx_plugins_fs:write_tar(
                    NameVsn, plugin_package(NameVsn, [{"extra.txt", <<"extra">>}])
                ),
                ?assertEqual(ok, (validator(NameVsn))()),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, validator(NameVsn)),
                ?assertEqual({ok, <<"keep me">>}, file:read_file(Marker)),
                ?assertEqual(
                    {error, enoent}, file:read_file_info(filename:join(PluginDir, "extra.txt"))
                )
            end
        )
    after
        unmeck_emqx()
    end.

%% A valid `release.json' without the application files it declares is not an
%% installation: the unpack stopped before it wrote the application, so the
%% package must be unpacked again instead of being skipped.
ensure_installed_from_tar_recovers_manifest_only_install_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "manifest_only-0.1.0",
                ok = install_plugin_files(NameVsn, []),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                %% simulate an unpack that stopped right after the manifest
                ok = file:del_dir_r(plugin_app_dir(NameVsn)),
                %% the metadata alone still looks fine
                ?assertEqual(ok, (validator(NameVsn))()),
                ?assertNot(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, validator(NameVsn)),
                %% the application files are there now
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ?assert(filelib:is_regular(app_file_path(NameVsn)))
            end
        )
    after
        unmeck_emqx()
    end.

%% Same, but the application file is there and one of the beam files it
%% declares is missing: the unpack stopped in the middle of the `ebin' dir.
ensure_installed_from_tar_recovers_missing_beam_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "missing_beam-0.1.0",
                Modules = [emqx_plugins_tests_fake_mod],
                ok = install_plugin_files(NameVsn, Modules),
                ok = emqx_plugins_fs:write_tar(
                    NameVsn, plugin_package_with_modules(NameVsn, Modules, [])
                ),
                Beam = beam_file_path(NameVsn, hd(Modules)),
                ?assert(filelib:is_regular(Beam)),
                ok = file:delete(Beam),
                ?assertEqual(ok, (validator(NameVsn))()),
                ?assertNot(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, validator(NameVsn)),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ?assert(filelib:is_regular(Beam))
            end
        )
    after
        unmeck_emqx()
    end.

%% A failed metadata read must not cause the files of a plugin whose
%% application is running to be deleted: the plugin would keep running from
%% replaced or missing code.
ensure_installed_from_tar_refuses_in_use_plugin_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "in_use-0.1.0",
                ok = install_plugin_files(NameVsn, []),
                Resource = filename:join(emqx_plugins_fs:plugin_dir(NameVsn), "resource.dat"),
                ok = write_file(Resource, <<"keep me">>),
                ok = start_plugin_app(NameVsn),
                try
                    %% the runtime state is detected without `release.json'
                    ?assertEqual(running, emqx_plugins_apps:running_status(NameVsn)),
                    ?assert(emqx_plugins_fs:is_in_use(NameVsn)),
                    %% the metadata of the running plugin stops being readable
                    ok = write_file(emqx_plugins_fs:info_file_path(NameVsn), <<"not json">>),
                    ?assertMatch(
                        {error, #{msg := "plugin_is_in_use"}},
                        emqx_plugins_fs:ensure_installed_from_tar(NameVsn, validator(NameVsn))
                    ),
                    %% nothing of the running plugin has been touched
                    ?assertMatch({error, _}, (validator(NameVsn))()),
                    ?assertEqual({ok, <<"keep me">>}, file:read_file(Resource)),
                    ?assert(filelib:is_regular(app_file_path(NameVsn))),
                    ?assertEqual(running, emqx_plugins_apps:running_status(NameVsn))
                after
                    ok = stop_plugin_app(NameVsn)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% An application that is loaded but not running does not make the installation
%% in use: it is unloaded before the leftovers are purged, so that the package
%% which is unpacked afterwards is loaded cleanly.
ensure_installed_from_tar_unloads_loaded_app_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "loaded-0.1.0",
                ok = install_plugin_files(NameVsn, []),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ok = start_plugin_app(NameVsn),
                %% stopped, but still loaded
                ok = emqx_plugins_apps:stop(#{rel_apps => [NameVsn]}),
                ?assertEqual(loaded, emqx_plugins_apps:running_status(NameVsn)),
                ?assertNot(emqx_plugins_fs:is_in_use(NameVsn)),
                %% the metadata is not readable, so the package must be unpacked
                ok = write_file(emqx_plugins_fs:info_file_path(NameVsn), <<"not json">>),
                ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, validator(NameVsn)),
                %% the old code is gone and the package is complete again
                ?assertEqual(stopped, emqx_plugins_apps:running_status(NameVsn)),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                stop_plugin_app(NameVsn)
            end
        )
    after
        unmeck_emqx()
    end.

%% A plugin with an unreadable `release.json' can still be stopped: its
%% applications are found in the install directory, so that the installation
%% can be replaced afterwards.
ensure_stopped_without_readable_metadata_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "stop_broken-0.1.0",
                ok = install_plugin_files(NameVsn, []),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ok = start_plugin_app(NameVsn),
                try
                    ?assertEqual(running, emqx_plugins_apps:running_status(NameVsn)),
                    %% the metadata is not readable, so `emqx ctl plugins stop'
                    %% can not take the applications from it
                    ok = write_file(emqx_plugins_fs:info_file_path(NameVsn), <<"not json">>),
                    ok = emqx_plugins:ensure_stopped(NameVsn),
                    ?assertEqual(stopped, emqx_plugins_apps:running_status(NameVsn)),
                    ?assertNot(emqx_plugins_fs:is_in_use(NameVsn)),
                    %% and the installation can be repaired
                    ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, validator(NameVsn)),
                    ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn))
                after
                    stop_plugin_app(NameVsn)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% An installation which is complete on disk does not become `incomplete'
%% because one of its applications happens to be loaded from another version of
%% the same plugin.  The runtime state must not decide whether the files of a
%% complete installation may be replaced: the caller used to purge them and to
%% fail with `plugin_tarball_not_found' when the package was gone.
install_state_ignores_other_version_loaded_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                V1 = "other_vsn-0.1.0",
                V2 = "other_vsn-0.2.0",
                ok = install_plugin_files(V1, []),
                ok = install_plugin_files(V2, []),
                ok = start_plugin_app(V1),
                try
                    %% the file check and the runtime validation disagree ...
                    ?assert(emqx_plugins_fs:is_extraction_complete(V2)),
                    ?assertMatch(
                        {error, #{msg := "plugin_app_loaded_outside_package"}},
                        emqx_plugins:validate_installation(V2)
                    ),
                    %% ... and the files on disk decide
                    ?assertEqual(installed, emqx_plugins:install_state(V2)),
                    V2Files = installed_plugin_files(V2),
                    %% the complete installation is kept, not purged
                    ?assertEqual(
                        ok,
                        emqx_plugins_fs:ensure_installed_from_tar(V2, fun() ->
                            emqx_plugins:validate_installation(V2)
                        end)
                    ),
                    assert_files_exist(V2Files),
                    %% and the CLI reports it as already installed
                    Output = cli_ensure_installed(V2),
                    ?assertNotEqual(
                        nomatch, binary:match(Output, <<"plugin_already_installed">>)
                    ),
                    assert_files_exist(V2Files)
                after
                    ok = stop_plugin_app(V1)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% Classifying an installation must not install its configuration schema: the
%% state is inspected before an upload is authorized, so a rejected upload must
%% not change how the plugin's configuration is decoded.
install_state_does_not_load_config_schema_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "schema_add-0.1.0",
                ok = install_plugin_files(NameVsn, []),
                ok = write_file(avsc_file_path(NameVsn), <<"{}">>),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
                ?assertNot(meck:called(emqx_plugins_serde, add_schema, '_')),
                %% (the installation validator does load it)
                ?assertEqual(ok, emqx_plugins:validate_installation(NameVsn)),
                ?assert(meck:called(emqx_plugins_serde, add_schema, '_'))
            end
        )
    after
        unmeck_emqx()
    end.

%% Same, for the error path: an unreadable installation must not make the
%% classification delete the schema of a plugin that is still running.
install_state_does_not_delete_config_schema_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "schema_delete-0.1.0",
                ok = install_plugin_files(NameVsn, []),
                ok = start_plugin_app(NameVsn),
                try
                    %% the application file of the running plugin disappears
                    ok = file:delete(app_file_path(NameVsn)),
                    ?assertEqual(incomplete, emqx_plugins:install_state(NameVsn)),
                    ?assertNot(meck:called(emqx_plugins_serde, delete_schema, '_')),
                    %% (the installation validator does delete it)
                    ?assertMatch(
                        {error, #{msg := "bad_plugin_app_file"}},
                        emqx_plugins:validate_installation(NameVsn)
                    ),
                    ?assert(meck:called(emqx_plugins_serde, delete_schema, '_'))
                after
                    ok = stop_plugin_app(NameVsn)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% The leftovers of a failed installation can not always be removed: that is
%% an error to report, not a `badmatch' crash while a configured plugin is
%% being recovered.
reinstall_reports_purge_failure_test() ->
    meck_emqx(),
    meck:new(emqx_plugins_fs, [passthrough]),
    try
        %% a read-only directory or a permission error, as the file system
        %% reports it (the test must not depend on the user it runs as)
        meck:expect(emqx_plugins_fs, purge_installed, fun(_NameVsn) -> {error, eacces} end),
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "unpurgeable-0.1.0",
                ok = write_file(emqx_plugins_fs:info_file_path(NameVsn), <<"not json">>),
                ?assertEqual(incomplete, emqx_plugins:install_state(NameVsn)),
                ?assertMatch(
                    {error, #{msg := "failed_to_purge_plugin_dir"}},
                    emqx_plugins:ensure_installed(NameVsn)
                )
            end
        )
    after
        meck:unload(emqx_plugins_fs),
        unmeck_emqx()
    end.

%% An application which can not be unloaded is still running from the
%% installation that is about to be replaced, so the replacement has to be
%% refused instead of deleting its files.
prepare_replacement_reports_unload_failure_test() ->
    meck_emqx(),
    meck:new(emqx_plugins_apps, [passthrough]),
    try
        meck:expect(emqx_plugins_apps, running_apps_from, fun(_Dir) -> [] end),
        meck:expect(
            emqx_plugins_apps,
            stop_and_unload_loaded,
            fun(_Dir) -> {error, "can not unload"} end
        ),
        ?assertMatch(
            {error, #{msg := "failed_to_unload_plugin_apps"}},
            emqx_plugins_fs:prepare_replacement("unloadable-0.1.0")
        )
    after
        meck:unload(emqx_plugins_apps),
        unmeck_emqx()
    end.

%% A package which does not contain the applications its own metadata declares
%% must not be reported as installed: the next state check would classify the
%% fresh installation as incomplete again.
install_from_local_tar_rejects_incomplete_package_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "incomplete_pkg-0.1.0",
                ok = emqx_plugins_fs:write_tar(
                    NameVsn, plugin_package_missing_beam(NameVsn, missing_mod)
                ),
                ?assertMatch(
                    {error, #{msg := "incomplete_plugin_package"}},
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end)
                ),
                %% nothing of the half installed package is left behind
                ?assertNot(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ?assertEqual({error, enoent}, file:read_file_info(app_file_path(NameVsn)))
            end
        )
    after
        unmeck_emqx()
    end.

%% The core regression: an authorized package which carries entries of another
%% plugin must not write (or delete) anything of that other plugin.  The whole
%% package is refused before a single byte is written.
install_from_local_tar_rejects_sibling_entries_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                Existing = "existing-1.0",
                ok = install_plugin_files(Existing, []),
                ok = write_file(
                    filename:join(emqx_plugins_fs:plugin_dir(Existing), "marker.txt"), <<
                        "keep me"
                    >>
                ),
                Before = snapshot_plugin_dir(Existing),
                NameVsn = "candidate-1.0",
                Package = tar_of([
                    {filename:join(NameVsn, "release.json"), release_json(NameVsn)},
                    {
                        filename:join([NameVsn, NameVsn, "ebin", "candidate.app"]),
                        app_file(app_name(NameVsn), app_vsn(NameVsn), [])
                    },
                    {filename:join(Existing, "release.json"), <<"replacement">>},
                    {filename:join([Existing, Existing, "ebin", "existing.app"]), <<"replacement">>}
                ]),
                ok = emqx_plugins_fs:write_tar(NameVsn, Package),
                ?assertMatch(
                    {error, #{msg := "plugin_package_entry_outside_root"}},
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end)
                ),
                %% every file of the sibling plugin is untouched, byte for byte
                ?assertEqual(Before, snapshot_plugin_dir(Existing)),
                ?assertNot(filelib:is_dir(emqx_plugins_fs:plugin_dir(NameVsn))),
                %% guard against the rejection point moving behind the staging
                %% directory creation (the package is refused before it)
                ?assertEqual([], staging_attempts(NameVsn)),
                %% nothing was written before the package was refused: the
                %% install dir only holds the existing plugin and the package file
                %% `write_tar/2' wrote itself
                ?assertEqual(
                    lists:sort([
                        emqx_plugins_fs:plugin_dir(Existing),
                        emqx_plugins_fs:tar_file_path(NameVsn),
                        emqx_plugins_fs:tar_file_path(NameVsn) ++ ".md5sum"
                    ]),
                    lists:sort(filelib:wildcard(filename:join(emqx_plugins_fs:install_dir(), "*")))
                )
            end
        )
    after
        unmeck_emqx()
    end.

%% A failed installation must only ever clean up after itself: the rollback of
%% a rejected replacement does not touch the directory of another plugin.
install_from_local_tar_rollback_touches_only_its_own_name_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                Existing = "existing-1.0",
                ok = install_plugin_files(Existing, []),
                ok = write_file(
                    filename:join(emqx_plugins_fs:plugin_dir(Existing), "marker.txt"), <<"keep me">>
                ),
                Before = snapshot_plugin_dir(Existing),
                NameVsn = "candidate-1.0",
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ?assertMatch(
                    {error, #{msg := "boom"}},
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() ->
                        {error, #{msg => "boom"}}
                    end)
                ),
                ?assertEqual(Before, snapshot_plugin_dir(Existing)),
                ?assertNot(filelib:is_dir(emqx_plugins_fs:plugin_dir(NameVsn))),
                ?assertEqual([], staging_attempts(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% An entry which resolves back to the package root (or to the install root)
%% would be written over a directory: it is refused, and no directory of the
%% install dir is left behind by the attempt.
install_from_local_tar_rejects_root_resolving_entry_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "candidate-1.0",
                ok = emqx_plugins_fs:write_tar(
                    NameVsn,
                    tar_of([
                        {filename:join(NameVsn, "release.json"), release_json(NameVsn)},
                        {filename:join(NameVsn, "dir/.."), <<"replacement">>}
                    ])
                ),
                ?assertMatch(
                    {error, #{
                        msg := "unsafe_tar_entry_path", reason := resolves_to_package_root
                    }},
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end)
                ),
                ?assertNot(filelib:is_dir(emqx_plugins_fs:plugin_dir(NameVsn))),
                ?assertNot(filelib:is_regular(emqx_plugins_fs:plugin_dir(NameVsn))),
                ?assertNot(
                    filelib:is_dir(filename:join(emqx_plugins_fs:plugin_dir(NameVsn), "dir"))
                ),
                %% the same for an entry which resolves to the install root
                ok = emqx_plugins_fs:write_tar(NameVsn, tar_of([{"dir/..", <<"replacement">>}])),
                ?assertMatch(
                    {error, #{
                        msg := "unsafe_tar_entry_path", reason := resolves_to_install_root
                    }},
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end)
                ),
                ?assertNot(filelib:is_dir(filename:join(emqx_plugins_fs:install_dir(), "dir"))),
                ?assertEqual([], staging_attempts(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% A successful installation leaves no staging directory behind, and the
%% staging root is not mistaken for a plugin.
install_from_local_tar_cleans_staging_on_success_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "staged-0.1.0",
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ?assertEqual([], staging_attempts(NameVsn)),
                ?assertEqual([NameVsn], lists:sort(emqx_plugins_fs:list_name_vsn()))
            end
        )
    after
        unmeck_emqx()
    end.

%% A failed replacement puts back whatever the plugin's install directory held
%% before: the previous target is moved aside before the new tree is published
%% and must survive a validator which refuses the new one.
%%
%% The target here is a leftover which is not a directory.  That is the shape
%% `install_state/1' reports as `absent' while something is still there, so it
%% is the replacement the public entry can actually be asked to do.
install_from_local_tar_rolls_back_replacement_on_validator_error_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "replaced-0.1.0",
                Target = emqx_plugins_fs:plugin_dir(NameVsn),
                ok = write_file(Target, <<"installed v1">>),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ?assertEqual(
                    {error, #{msg => "boom"}},
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() ->
                        {error, #{msg => "boom"}}
                    end)
                ),
                ?assertEqual({ok, <<"installed v1">>}, file:read_file(Target)),
                ?assertNot(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ?assertEqual([], staging_attempts(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% A validator which crashes must not lose the installation which was there
%% before: the previous target is put back before the exception is re-raised.
install_from_local_tar_rolls_back_when_validator_crashes_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "crashed_validator-0.1.0",
                Target = emqx_plugins_fs:plugin_dir(NameVsn),
                ok = write_file(Target, <<"installed v1">>),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ?assertException(
                    error,
                    boom,
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> error(boom) end)
                ),
                ?assertEqual({ok, <<"installed v1">>}, file:read_file(Target)),
                ?assertNot(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ?assertEqual([], staging_attempts(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% A write which fails in the middle is reported, instead of crashing the
%% caller, and the install directory is left exactly as it was.
install_from_local_tar_keeps_target_on_midwrite_failure_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                Existing = "existing-1.0",
                ok = install_plugin_files(Existing, []),
                Before = snapshot_plugin_dir(Existing),
                NameVsn = "conflict-1.0",
                ok = emqx_plugins_fs:write_tar(
                    NameVsn,
                    tar_of([
                        {filename:join(NameVsn, "x"), <<"a plain file">>},
                        {filename:join(NameVsn, "x/y"), <<"needs x to be a directory">>}
                    ])
                ),
                ?assertMatch(
                    {error, #{msg := "failed_to_write_plugin_package"}},
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end)
                ),
                ?assertEqual(Before, snapshot_plugin_dir(Existing)),
                ?assertNot(filelib:is_dir(emqx_plugins_fs:plugin_dir(NameVsn))),
                ?assertEqual([], staging_attempts(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% An installation never sweeps the staging directories of another one (a
%% concurrent installation of the same plugin may be writing its own attempt);
%% the leftovers of a crashed installation are swept when the node starts.
stale_staging_is_left_alone_by_installs_and_swept_at_startup_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "stale_target-0.1.0",
                Stale = filename:join([staging_root(), NameVsn, "crashed-attempt"]),
                OtherStale = filename:join([staging_root(), "other-0.1.0", "crashed-attempt"]),
                ok = write_file(filename:join([Stale, NameVsn, "release.json"]), <<"stale">>),
                ok = write_file(
                    filename:join([OtherStale, "other-0.1.0", "release.json"]), <<"stale">>
                ),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ?assert(filelib:is_dir(Stale)),
                ?assert(filelib:is_dir(OtherStale)),
                ?assertEqual([NameVsn], lists:sort(emqx_plugins_fs:list_name_vsn())),
                ?assertEqual([Stale], staging_attempts(NameVsn)),
                %% the same sweep the plugins application runs when it starts
                ok = emqx_plugins_fs:cleanup_stale_staging(),
                ?assertNot(filelib:is_dir(Stale)),
                ?assertNot(filelib:is_dir(OtherStale))
            end
        )
    after
        unmeck_emqx()
    end.

%% A name which is not a single, already normalized path component can not name
%% a package root: it must be refused before it is used to build the package
%% path.  For `"."' and `".."' this also has to happen before `install_state/1'
%% is consulted, because `is_installed(".")' sees the install directory itself
%% and recovering it would delete everything in there
%% (`file:del_dir_r("<dir>/.")' removes the contents and then fails with
%% `einval').
install_from_local_tar_rejects_multi_component_namevsn_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(Dir) ->
                Parent = filename:dirname(Dir),
                Sentinel = filename:join(Dir, "sentinel.txt"),
                ok = write_file(Sentinel, <<"keep me">>),
                lists:foreach(
                    fun(NameVsn) ->
                        ?assertMatch(
                            {error, #{msg := "bad_plugin_package_name"}},
                            emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end)
                        ),
                        %% the refused name did not touch the install directory
                        ?assertEqual({ok, <<"keep me">>}, file:read_file(Sentinel))
                    end,
                    %% `"escape-1.0/"' is refused as well: it names the directory
                    %% of `"escape-1.0"' but is a different package file name
                    ["/", <<"/">>, "../escape-1.0", "a/b-1.0", ".", "..", "escape-1.0/"]
                ),
                %% `tar_file_path/1' was never reached with the bad name
                ?assertNot(filelib:is_regular(filename:join(Parent, "escape-1.0.tar.gz"))),
                ?assertNot(filelib:is_dir(filename:join(Parent, "escape-1.0")))
            end
        )
    after
        unmeck_emqx()
    end.

%% Concurrent public installations of the same plugin are serialized by the
%% installation lock: the caller which takes it unpacks the package while the
%% others are either refused as busy or find the complete tree, so all of them
%% come back cleanly and the installation on disk is the one the package
%% describes, with no staging attempt left behind.  Half of the callers name the
%% plugin as a string, half as a binary: both spellings name the same plugin
%% directory.  (That concurrent writers can not interleave their bytes is the
%% staging design itself: only the lock holder publishes, by one `file:rename/2'.)
install_from_local_tar_concurrent_callers_leave_one_installation_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "concurrent-0.1.0",
                ExpectedMarker = <<"packaged content">>,
                ok = emqx_plugins_fs:write_tar(
                    NameVsn, plugin_package(NameVsn, [{"marker.txt", ExpectedMarker}])
                ),
                Names = [
                    case N rem 2 of
                        0 -> NameVsn;
                        1 -> emqx_plugins_utils:bin(NameVsn)
                    end
                 || N <- lists:seq(1, 8)
                ],
                Monitors = [
                    begin
                        {_Pid, Ref} = spawn_monitor(fun() ->
                            _ = emqx_plugins_fs:ensure_installed_from_tar(Name, fun() -> ok end),
                            ok
                        end),
                        Ref
                    end
                 || Name <- Names
                ],
                lists:foreach(
                    fun(Ref) ->
                        receive
                            {'DOWN', Ref, process, _Pid, Reason} ->
                                %% a failed attempt is allowed, a crash is not
                                ?assertEqual(normal, Reason)
                        after 30000 ->
                            ?assert(false)
                        end
                    end,
                    Monitors
                ),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ?assertEqual(
                    {ok, ExpectedMarker},
                    file:read_file(
                        filename:join(emqx_plugins_fs:plugin_dir(NameVsn), "marker.txt")
                    )
                ),
                ?assertEqual(
                    {ok, release_json(NameVsn)},
                    file:read_file(emqx_plugins_fs:info_file_path(NameVsn))
                ),
                ?assertEqual([], staging_attempts(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% The state check of the public entry runs under the same lock as the
%% publication: an attempt which has already published its tree but has not
%% validated it yet can still roll that tree back, so no concurrent caller of
%% `ensure_installed_from_tar/2' may be told that the plugin is installed while
%% the tree is in that state.
ensure_installed_from_tar_retries_after_a_concurrent_rollback_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "racing-0.1.0",
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                Parent = self(),
                %% A publishes the package and stops inside its validator: its
                %% tree is in place but not validated yet, and A holds the lock.
                PidA = spawn(fun() ->
                    Result = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() ->
                        Parent ! {a_in_validator, self()},
                        receive
                            release -> ok
                        end,
                        {error, #{msg => "rejected_by_test"}}
                    end),
                    Parent ! {a_result, self(), Result}
                end),
                %% Whatever happens below, A must be let go of: it holds the
                %% installation lock of NameVsn while it waits, and a test process
                %% which aborts before releasing it would leave that lock set
                %% for the rest of the VM.
                try
                    receive
                        {a_in_validator, PidA} ->
                            ok
                    after 5000 ->
                        error(a_validator_not_reached)
                    end,
                    %% B enters the public entry for the same plugin while A is
                    %% between publishing and rolling back.
                    PidB = spawn(fun() ->
                        Result = emqx_plugins_fs:ensure_installed_from_tar(
                            NameVsn, fun() -> ok end
                        ),
                        Parent ! {b_result, self(), Result}
                    end),
                    receive
                        {b_result, PidB, ResultB} ->
                            ?assertMatch({error, #{reason := installation_in_progress}}, ResultB)
                    after 5000 ->
                        error(b_result_timeout)
                    end,
                    PidA ! release,
                    receive
                        {a_result, PidA, {error, #{msg := "rejected_by_test"}}} ->
                            ok
                    after 5000 ->
                        error(a_result_timeout)
                    end,
                    %% After the first attempt finishes, a new request can install.
                    ?assertEqual(
                        ok,
                        emqx_plugins_fs:ensure_installed_from_tar(
                            NameVsn, fun() -> ok end
                        )
                    ),
                    ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                    ?assertEqual(
                        {ok, release_json(NameVsn)},
                        file:read_file(emqx_plugins_fs:info_file_path(NameVsn))
                    ),
                    ?assertEqual([], staging_attempts(NameVsn))
                after
                    PidA ! release
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% A failed installation attempt must leave the package which was installed
%% before it as it was: it may be the only local copy the installation can be
%% repaired from.
write_package_restores_partial_writes_test_() ->
    {timeout, 60, fun() ->
        meck_emqx(),
        try
            with_rand_install_dir(fun(_Dir) ->
                lists:foreach(
                    fun({Operation, Previous, FailedFile}) ->
                        assert_package_write_restored(Operation, Previous, FailedFile)
                    end,
                    [
                        {Operation, Previous, FailedFile}
                     || Operation <- [
                            fun emqx_plugins:write_package/2, fun emqx_plugins:install_package/2
                        ],
                        Previous <- [absent, tar_only, complete],
                        FailedFile <- [tar, checksum]
                    ]
                )
            end)
        after
            unmeck_emqx()
        end
    end}.

assert_package_write_restored(Operation, Previous, FailedFile) ->
    NameVsn = "write_restore-1.0",
    TarFile = emqx_plugins_fs:tar_file_path(NameVsn),
    ChecksumFile = TarFile ++ ".md5sum",
    ok = emqx_plugins:delete_package(NameVsn),
    case Previous of
        absent -> ok;
        tar_only -> ok = write_file(TarFile, <<"previous">>);
        complete -> ok = emqx_plugins:write_package(NameVsn, <<"previous">>)
    end,
    {ok, Before} = emqx_plugins:backup_package(NameVsn),
    Content = <<"replacement">>,
    {FailedPath, FailedContent} =
        case FailedFile of
            tar -> {TarFile, Content};
            checksum -> {ChecksumFile, emqx_utils:bin_to_hexstr(crypto:hash(md5, Content), lower)}
        end,
    ok = meck:new(file, [passthrough, unstick]),
    try
        ok = meck:expect(file, write_file, fun(Path, Bytes) ->
            case {Path, Bytes} of
                {FailedPath, FailedContent} ->
                    ok = meck:passthrough([Path, <<"partial">>]),
                    {error, enospc};
                _ ->
                    meck:passthrough([Path, Bytes])
            end
        end),
        ?assertMatch(
            {error, #{
                msg := "failed_to_write_plugin_package", path := FailedPath, reason := enospc
            }},
            Operation(NameVsn, Content)
        ),
        ?assertEqual({ok, Before}, emqx_plugins:backup_package(NameVsn)),
        ?assertEqual(absent, emqx_plugins:install_state(NameVsn))
    after
        ok = meck:unload(file)
    end.

restore_package_puts_back_the_replaced_package_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "package-0.1.0",
                TarFile = emqx_plugins_fs:tar_file_path(NameVsn),
                Md5File = TarFile ++ ".md5sum",
                ok = emqx_plugins:write_package(NameVsn, <<"installed package">>),
                {ok, InstalledChecksum} = file:read_file(Md5File),
                {ok, Backup} = emqx_plugins:backup_package(NameVsn),
                %% a new upload overwrites the installed package ...
                ok = emqx_plugins:write_package(NameVsn, <<"rejected package">>),
                ?assertEqual({ok, <<"rejected package">>}, file:read_file(TarFile)),
                %% ... and is put back when the installation attempt fails
                ok = emqx_plugins:restore_package(NameVsn, Backup),
                ?assertEqual({ok, <<"installed package">>}, file:read_file(TarFile)),
                ?assertEqual({ok, InstalledChecksum}, file:read_file(Md5File))
            end
        )
    after
        unmeck_emqx()
    end.

%% A hand copied package has no checksum, and a package which did not exist
%% before the failed attempt must not be left behind.
restore_package_restores_each_file_separately_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "orphan_package-0.1.0",
                TarFile = emqx_plugins_fs:tar_file_path(NameVsn),
                Md5File = TarFile ++ ".md5sum",
                %% a package which was not there before is deleted
                {ok, Backup} = emqx_plugins:backup_package(NameVsn),
                ok = emqx_plugins:write_package(NameVsn, <<"failed package">>),
                ok = emqx_plugins:restore_package(NameVsn, Backup),
                ?assertEqual(false, emqx_plugins:is_package_present(NameVsn)),
                ?assertEqual({error, enoent}, file:read_file(Md5File)),
                %% a package copied by hand is kept without a checksum
                ok = write_file(TarFile, <<"hand copied package">>),
                {ok, Backup1} = emqx_plugins:backup_package(NameVsn),
                ok = emqx_plugins:write_package(NameVsn, <<"failed package">>),
                ok = emqx_plugins:restore_package(NameVsn, Backup1),
                ?assertEqual({ok, <<"hand copied package">>}, file:read_file(TarFile)),
                ?assertEqual({error, enoent}, file:read_file(Md5File))
            end
        )
    after
        unmeck_emqx()
    end.

%% The package of the installation which is about to be replaced can not be
%% read: that is not an absent package, it must fail the snapshot.  Recording
%% it as `none' would make the restore of a failed attempt delete the checksum
%% it was meant to preserve.  The unreadable file is a directory here, which
%% fails whatever the user the tests run as (`eacces' can not be relied on,
%% the tests run as root in CI).
backup_package_reports_unreadable_package_file_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "unreadable_package-0.1.0",
                TarFile = emqx_plugins_fs:tar_file_path(NameVsn),
                Md5File = TarFile ++ ".md5sum",
                ok = emqx_plugins:write_package(NameVsn, <<"installed package">>),
                ok = file:delete(Md5File),
                ok = file:make_dir(Md5File),
                ?assertMatch(
                    {error, #{msg := "failed_to_backup_plugin_package"}},
                    emqx_plugins:backup_package(NameVsn)
                ),
                %% a package which is not there at all is still `none'
                ?assertEqual(
                    {ok, #{tar => none, md5sum => none}},
                    emqx_plugins:backup_package("absent-0.1.0")
                )
            end
        )
    after
        unmeck_emqx()
    end.

%% A module which is not an atom can not name a beam file: the application is
%% not extracted, instead of crashing the state check with a `badarg'.
is_extraction_complete_rejects_non_atom_modules_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "bad_modules-0.1.0",
                ok = write_file(emqx_plugins_fs:info_file_path(NameVsn), release_json(NameVsn)),
                ok = write_file(
                    app_file_path(NameVsn),
                    app_file(app_name(NameVsn), app_vsn(NameVsn), ["not_an_atom"])
                ),
                ?assertEqual(incomplete, emqx_plugins_fs:install_state(NameVsn)),
                ?assertEqual(incomplete, emqx_plugins:install_state(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% A simplified version of `emqx_plugins:validate_installation/1': the info
%% file must be there and must be readable.
validator(NameVsn) ->
    fun() ->
        case file:read_file(emqx_plugins_fs:info_file_path(NameVsn)) of
            {ok, Bin} ->
                case emqx_utils_json:safe_decode(Bin) of
                    {ok, Map} when is_map(Map) -> ok;
                    _ -> {error, #{msg => "bad_info_file", reason => invalid_json}}
                end;
            {error, Reason} ->
                {error, #{msg => "bad_info_file", reason => Reason}}
        end
    end.

release_json(NameVsn) ->
    {Name, Vsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    NameBin = atom_to_binary(Name, utf8),
    VsnBin = list_to_binary(Vsn),
    emqx_utils_json:encode(#{
        name => NameBin,
        rel_vsn => VsnBin,
        rel_apps => [<<NameBin/binary, "-", VsnBin/binary>>],
        description => <<"test plugin">>
    }).

%% The application resource file of a plugin's app.  Declaring the modules is
%% what makes an installation complete for `emqx_plugins_fs'.
app_file(AppName, AppVsn, Modules) ->
    iolist_to_binary(
        io_lib:format(
            "~p.~n",
            [
                {application, AppName, [
                    {description, "test plugin app"},
                    {vsn, AppVsn},
                    {modules, Modules},
                    {registered, []},
                    {applications, [kernel, stdlib]}
                ]}
            ]
        )
    ).

app_name(NameVsn) ->
    {Name, _Vsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    Name.

app_vsn(NameVsn) ->
    {_Name, Vsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    Vsn.

%% The directory of the plugin's (only) application, as the package unpacks it.
plugin_app_dir(NameVsn) ->
    filename:join(emqx_plugins_fs:plugin_dir(NameVsn), NameVsn).

plugin_app_ebin_dir(NameVsn) ->
    filename:join(plugin_app_dir(NameVsn), "ebin").

plugin_app_priv_dir(NameVsn) ->
    filename:join(plugin_app_dir(NameVsn), "priv").

avsc_file_path(NameVsn) ->
    filename:join(plugin_app_priv_dir(NameVsn), "config_schema.avsc").

app_file_path(NameVsn) ->
    filename:join(plugin_app_ebin_dir(NameVsn), atom_to_list(app_name(NameVsn)) ++ ".app").

beam_file_path(NameVsn, Module) ->
    filename:join(plugin_app_ebin_dir(NameVsn), atom_to_list(Module) ++ ".beam").

%% The files an unpack of `NameVsn' writes: they must all survive a refused
%% replacement.
installed_plugin_files(NameVsn) ->
    {ok, [{application, _AppName, Props}]} = file:consult(app_file_path(NameVsn)),
    Modules = proplists:get_value(modules, Props, []),
    [
        emqx_plugins_fs:info_file_path(NameVsn),
        app_file_path(NameVsn)
        | [beam_file_path(NameVsn, Module) || Module <- Modules]
    ].

assert_files_exist(Files) ->
    Missing = [File || File <- Files, not filelib:is_regular(File)],
    ?assertEqual([], Missing).

%% The JSON the CLI prints for `emqx_plugins_cli_utils:ensure_installed/2'.
cli_ensure_installed(NameVsn) ->
    LogFun = fun(Fmt, Args) -> iolist_to_binary(io_lib:format(Fmt, Args)) end,
    emqx_plugins_cli_utils:ensure_installed(NameVsn, LogFun).

%% Create what a completed unpack of `NameVsn' leaves on disk: the metadata,
%% the application resource file and one beam file per declared module.
install_plugin_files(NameVsn, Modules) ->
    ok = write_file(emqx_plugins_fs:info_file_path(NameVsn), release_json(NameVsn)),
    ok = write_file(app_file_path(NameVsn), app_file(app_name(NameVsn), app_vsn(NameVsn), Modules)),
    lists:foreach(
        fun(Module) ->
            ok = write_file(beam_file_path(NameVsn, Module), <<"beam">>)
        end,
        Modules
    ).

%% The same content as `install_plugin_files/2', packed as the plugin package.
plugin_package(NameVsn, ExtraFiles) ->
    plugin_package_with_modules(NameVsn, [], ExtraFiles).

plugin_package_with_modules(NameVsn, Modules, ExtraFiles) ->
    EbinRelDir = filename:join([NameVsn, NameVsn, "ebin"]),
    Files =
        [
            {filename:join(NameVsn, "release.json"), release_json(NameVsn)},
            {
                filename:join(EbinRelDir, atom_to_list(app_name(NameVsn)) ++ ".app"),
                app_file(app_name(NameVsn), app_vsn(NameVsn), Modules)
            }
            | [
                {filename:join(EbinRelDir, atom_to_list(Module) ++ ".beam"), <<"beam">>}
             || Module <- Modules
            ]
        ] ++ [{filename:join(NameVsn, Name), Bin} || {Name, Bin} <- ExtraFiles],
    tar_of(Files).

%% A package whose application file declares a module whose beam file is not
%% part of the package.
plugin_package_missing_beam(NameVsn, Module) ->
    EbinRelDir = filename:join([NameVsn, NameVsn, "ebin"]),
    tar_of([
        {filename:join(NameVsn, "release.json"), release_json(NameVsn)},
        {
            filename:join(EbinRelDir, atom_to_list(app_name(NameVsn)) ++ ".app"),
            app_file(app_name(NameVsn), app_vsn(NameVsn), [Module])
        }
    ]).

tar_of(Files) ->
    TmpFile = filename:join(emqx_plugins_fs:install_dir(), "tmp-test-package.tar.gz"),
    ok = erl_tar:create(TmpFile, Files, [compressed]),
    try
        {ok, Bin} = file:read_file(TmpFile),
        Bin
    after
        ok = file:delete(TmpFile)
    end.

%% The staging directory the installations under test use.
staging_root() ->
    emqx_plugins_fs:staging_root().

%% The installation attempts of `NameVsn' which are still on disk: `[]' means
%% that the attempt cleaned up after itself.
staging_attempts(NameVsn) ->
    filelib:wildcard(filename:join([staging_root(), NameVsn, "*"])).

%% Every regular file below the plugin's install directory, with its bytes.
snapshot_plugin_dir(NameVsn) ->
    lists:sort([
        {File, Content}
     || File <- filelib:wildcard(filename:join(emqx_plugins_fs:plugin_dir(NameVsn), "**")),
        filelib:is_regular(File),
        {ok, Content} <- [file:read_file(File)]
    ]).

%% Load and start the plugin's application, so that the plugin is in use.
start_plugin_app(NameVsn) ->
    Plugin = #{rel_apps => [NameVsn]},
    ok = emqx_plugins_apps:load(Plugin, emqx_plugins_fs:plugin_dir(NameVsn)),
    ok = emqx_plugins_apps:start(Plugin).

stop_plugin_app(NameVsn) ->
    Plugin = #{rel_apps => [NameVsn]},
    ok = emqx_plugins_apps:stop(Plugin),
    ok = emqx_plugins_apps:unload(Plugin),
    _ = code:del_path(plugin_app_ebin_dir(NameVsn)),
    ok.
