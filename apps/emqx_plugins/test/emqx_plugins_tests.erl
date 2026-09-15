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

%% A directory left behind by an interrupted or failed installation (no
%% readable `release.json') does not count as an installation: the package is
%% unpacked again after the leftovers have been purged.
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
app_file(AppName, Modules) ->
    iolist_to_binary(
        io_lib:format(
            "~p.~n",
            [
                {application, AppName, [
                    {description, "test plugin app"},
                    {vsn, "1.0"},
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

%% The directory of the plugin's (only) application, as the package unpacks it.
plugin_app_dir(NameVsn) ->
    filename:join(emqx_plugins_fs:plugin_dir(NameVsn), NameVsn).

plugin_app_ebin_dir(NameVsn) ->
    filename:join(plugin_app_dir(NameVsn), "ebin").

app_file_path(NameVsn) ->
    filename:join(plugin_app_ebin_dir(NameVsn), atom_to_list(app_name(NameVsn)) ++ ".app").

beam_file_path(NameVsn, Module) ->
    filename:join(plugin_app_ebin_dir(NameVsn), atom_to_list(Module) ++ ".beam").

%% Create what a completed unpack of `NameVsn' leaves on disk: the metadata,
%% the application resource file and one beam file per declared module.
install_plugin_files(NameVsn, Modules) ->
    ok = write_file(emqx_plugins_fs:info_file_path(NameVsn), release_json(NameVsn)),
    ok = write_file(app_file_path(NameVsn), app_file(app_name(NameVsn), Modules)),
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
                app_file(app_name(NameVsn), Modules)
            }
            | [
                {filename:join(EbinRelDir, atom_to_list(Module) ++ ".beam"), <<"beam">>}
             || Module <- Modules
            ]
        ] ++ [{filename:join(NameVsn, Name), Bin} || {Name, Bin} <- ExtraFiles],
    tar_of(Files).

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

meck_emqx() ->
    meck:new(emqx, [unstick, passthrough]),
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
        delete_schema,
        fun(_NameVsn) -> ok end
    ),
    ok.

unmeck_emqx() ->
    meck:unload(emqx),
    meck:unload(emqx_plugins_serde),
    ok.
