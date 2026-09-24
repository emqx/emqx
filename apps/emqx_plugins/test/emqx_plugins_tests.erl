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

%% delete package should mostly work and return ok
%% but it may fail in case the path is a directory
%% or if the file is read-only
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
    ok.

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

%% A failed installation attempt must leave the package which was installed
%% before it as it was: it may be the only local copy the installation can be
%% repaired from.
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
