%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Tests for the beam preflight and the atomic publishing of
%% `emqx_plugins_apps'.  Everything runs against a throw-away install directory
%% and the code server is restored after every test.
-module(emqx_plugins_apps_tests).

-include("emqx_plugins.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PLUGIN, "plug-1.0.0").

preflight_rejects_truncated_beam_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Bin = compile_beam(a_mod, plain_src(a_mod, ok)),
        _Ebin = write_plugin_app(LibDir, ?PLUGIN, [{a_mod, truncate(Bin)}]),
        Path0 = code:get_path(),
        Result = emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir),
        ?assertMatch({error, #{msg := _}}, Result),
        {error, #{msg := Msg}} = Result,
        ?assert(lists:member(Msg, ["plugin_beam_truncated", "plugin_beam_not_loadable"])),
        ?assertEqual(Path0, code:get_path()),
        ?assertNot(code:is_loaded(a_mod))
    end).

preflight_rejects_module_name_mismatch_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        %% The bytes of `a_mod' stored under the name `b_mod'.
        Bin = compile_beam(a_mod, plain_src(a_mod, ok)),
        _Ebin = write_plugin_app(LibDir, ?PLUGIN, [{b_mod, Bin}]),
        Path0 = code:get_path(),
        ?assertMatch(
            {error, #{msg := "plugin_beam_module_name_mismatch"}},
            emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)
        ),
        ?assertNot(code:is_loaded(b_mod)),
        ?assertEqual(Path0, code:get_path())
    end).

preflight_rejects_undeclared_module_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Ebin = ebin(LibDir, ?PLUGIN),
        ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
        Bin = compile_beam(a_mod, plain_src(a_mod, ok)),
        ok = write_beam(Ebin, a_mod, Bin),
        %% `modules' is a valid (empty) list, so the beam is simply undeclared.
        ok = write_app(Ebin, "plug.app", app_file(plug, "1.0.0", [])),
        Path0 = code:get_path(),
        ?assertMatch(
            {error, #{msg := "plugin_app_module_not_declared"}},
            emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)
        ),
        ?assertEqual(Path0, code:get_path())
    end).

preflight_rejects_missing_modules_declaration_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Ebin = ebin(LibDir, ?PLUGIN),
        ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
        Bin = compile_beam(a_mod, plain_src(a_mod, ok)),
        ok = write_beam(Ebin, a_mod, Bin),
        ok = write_app(Ebin, "plug.app", app_file_with(plug, [{vsn, "1.0.0"}])),
        Path0 = code:get_path(),
        ?assertMatch(
            {error, #{msg := "plugin_app_modules_not_declared"}},
            emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)
        ),
        ?assertEqual(Path0, code:get_path())
    end),
    %% A package whose ebin holds no beam at all may omit `modules'.
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Ebin = ebin(LibDir, ?PLUGIN),
        ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
        ok = write_app(Ebin, "plug.app", app_file_with(plug, [{vsn, "1.0.0"}])),
        ?assertEqual(ok, emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir))
    end).

%% A `modules' declaration which is there but malformed is rejected even when
%% the ebin holds no beam at all.
preflight_rejects_malformed_modules_declaration_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Ebin = ebin(LibDir, ?PLUGIN),
        ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
        ok = write_app(
            Ebin,
            "plug.app",
            app_file_with(plug, [{vsn, "1.0.0"}, {modules, not_a_list}])
        ),
        ?assertMatch(
            {error, #{msg := "plugin_app_modules_not_declared"}},
            emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)
        )
    end).

preflight_rejects_duplicate_module_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Bin = compile_beam(dup_mod, plain_src(dup_mod, ok)),
        _Ebin1 = write_plugin_app(LibDir, "plug-1.0.0", [{dup_mod, Bin}]),
        _Ebin2 = write_plugin_app(LibDir, "dep-1.0.0", [{dup_mod, Bin}]),
        ?assertMatch(
            {error, #{msg := "duplicate_plugin_module"}},
            emqx_plugins_apps:load(#{rel_apps => [<<"plug-1.0.0">>, <<"dep-1.0.0">>]}, LibDir)
        )
    end).

preflight_rejects_loaded_emqx_module_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Mod = emqx_plugins_apps_tests_owned,
        {Bin, Beam, OutsideDir} = load_outside_module(Mod),
        try
            _Ebin = write_plugin_app(LibDir, ?PLUGIN, [{Mod, Bin}]),
            Path0 = code:get_path(),
            ?assertMatch(
                {error, #{
                    msg := "plugin_beam_load_conflict",
                    conflict := loaded_outside_plugins
                }},
                emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)
            ),
            ?assertEqual(Beam, code:which(Mod)),
            ?assertEqual(Path0, code:get_path())
        after
            drop_module(Mod),
            file:del_dir_r(OutsideDir)
        end
    end).

%% A module which is merely resolvable on the code path is not in use: the
%% applications under `plugins/' are built into the same tree as the release
%% applications, so a package must still be allowed to load its own module.
preflight_allows_unloaded_module_found_on_the_code_path_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Mod = unloaded_path_mod,
        OutsideDir = filename:absname("emqx_plugins_apps_tests_unloaded"),
        Ebin = filename:join(OutsideDir, "ebin"),
        ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
        Bin = compile_beam(Mod, plain_src(Mod, ok)),
        ok = file:write_file(beam_path(Ebin, Mod), Bin),
        true = code:add_patha(Ebin),
        try
            ?assertNot(code:is_loaded(Mod)),
            _Ebin = write_plugin_app(LibDir, ?PLUGIN, [{Mod, Bin}]),
            ?assertEqual(ok, emqx_plugins_apps:validate(#{rel_apps => [?PLUGIN]}, LibDir))
        after
            drop_module(Mod),
            file:del_dir_r(OutsideDir)
        end
    end).

%% `emqx.ct' loads the modules of the project under test from the build tree
%% while the application itself is not loaded: the package must still be able to
%% replace the modules of its own applications.
preflight_allows_own_module_preloaded_from_build_tree_test() ->
    with_plugin_env(fun() ->
        Mod = preloaded_own_app_mod,
        OwnBase = filename:absname("emqx_plugins_apps_tests_preloaded"),
        OwnEbin = filename:join([OwnBase, ?PLUGIN, "ebin"]),
        ok = filelib:ensure_dir(filename:join(OwnEbin, "dummy")),
        Bin = compile_beam(Mod, plain_src(Mod, original)),
        ok = file:write_file(beam_path(OwnEbin, Mod), Bin),
        {module, Mod} = code:load_binary(Mod, beam_path(OwnEbin, Mod), Bin),
        try
            ?assertNot(lists:keymember(plug, 1, application:loaded_applications())),
            LibDir = lib_dir(?PLUGIN),
            _Ebin = write_plugin_app(LibDir, ?PLUGIN, [{Mod, Bin}]),
            ?assertEqual(ok, emqx_plugins_apps:validate(#{rel_apps => [?PLUGIN]}, LibDir))
        after
            drop_module(Mod),
            file:del_dir_r(OwnBase)
        end
    end).

%% ... but not when that application is loaded: then it belongs to the release.
preflight_rejects_preloaded_own_app_when_app_is_loaded_test() ->
    with_plugin_env(fun() ->
        Mod = loaded_own_app_mod,
        OwnBase = filename:absname("emqx_plugins_apps_tests_preloaded_loaded"),
        OwnEbin = filename:join([OwnBase, ?PLUGIN, "ebin"]),
        ok = filelib:ensure_dir(filename:join(OwnEbin, "dummy")),
        Bin = compile_beam(Mod, plain_src(Mod, original)),
        ok = file:write_file(beam_path(OwnEbin, Mod), Bin),
        {module, Mod} = code:load_binary(Mod, beam_path(OwnEbin, Mod), Bin),
        ok = application:load({application, plug, [{vsn, "1.0.0"}]}),
        try
            LibDir = lib_dir(?PLUGIN),
            _Ebin = write_plugin_app(LibDir, ?PLUGIN, [{Mod, Bin}]),
            ?assertMatch(
                {error, #{
                    msg := "plugin_beam_load_conflict",
                    conflict := loaded_outside_plugins
                }},
                emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)
            )
        after
            _ = application:unload(plug),
            drop_module(Mod),
            file:del_dir_r(OwnBase)
        end
    end).

%% A module left in the code server after its plugin directory was removed is
%% not protected by anything any more: a new package may take it over.
preflight_allows_module_left_from_removed_plugin_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Mod = removed_plugin_leftover_mod,
        {Bin, _Beam, OutsideDir} = load_outside_module(Mod),
        try
            ok = file:del_dir_r(OutsideDir),
            _Ebin = write_plugin_app(LibDir, ?PLUGIN, [{Mod, Bin}]),
            ?assertEqual(ok, emqx_plugins_apps:validate(#{rel_apps => [?PLUGIN]}, LibDir))
        after
            drop_module(Mod),
            _ = file:del_dir_r(OutsideDir)
        end
    end).

%% A module can stay loaded from a plugin installation made under another
%% install directory (the install directory is configurable).  It is still the
%% same plugin's module, so the new version may take it over.
preflight_allows_module_from_other_install_dir_of_same_plugin_test() ->
    with_plugin_env(fun() ->
        Mod = other_install_dir_mod,
        OldNameVsn = "otherdirplug-0.9.0",
        NewNameVsn = "otherdirplug-1.0.0",
        OtherBase = filename:absname("emqx_plugins_apps_tests_other_install"),
        OldPluginDir = filename:join(OtherBase, OldNameVsn),
        OldEbin = filename:join([OldPluginDir, OldNameVsn, "ebin"]),
        ok = filelib:ensure_dir(filename:join(OldEbin, "dummy")),
        ok = file:write_file(filename:join(OldPluginDir, "release.json"), <<"{}">>),
        {AppName, AppVsn} = emqx_plugins_utils:parse_name_vsn(OldNameVsn),
        ok = file:write_file(
            filename:join(OldEbin, atom_to_list(AppName) ++ ".app"),
            app_file(AppName, AppVsn, [Mod])
        ),
        OldBin = compile_beam(Mod, plain_src(Mod, original)),
        OldBeam = beam_path(OldEbin, Mod),
        ok = file:write_file(OldBeam, OldBin),
        {module, Mod} = code:load_binary(Mod, OldBeam, OldBin),
        try
            NewLibDir = lib_dir(NewNameVsn),
            NewBin = compile_beam(Mod, plain_src(Mod, plugin)),
            _Ebin = write_plugin_app(NewLibDir, NewNameVsn, [{Mod, NewBin}]),
            ?assertEqual(
                ok, emqx_plugins_apps:validate(#{rel_apps => [NewNameVsn]}, NewLibDir)
            )
        after
            drop_module(Mod),
            file:del_dir_r(OtherBase)
        end
    end).

preflight_rejects_sticky_module_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Which0 = code:which(lists),
        Bin = stdlib_beam(lists),
        _Ebin = write_plugin_app(LibDir, ?PLUGIN, [{lists, Bin}]),
        ?assertMatch(
            {error, #{msg := "plugin_beam_load_conflict", conflict := sticky_module}},
            emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)
        ),
        ?assertEqual(Which0, code:which(lists))
    end).

preflight_allows_own_module_reload_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Mod = plug_own_mod,
        Bin = compile_beam(Mod, plain_src(Mod, v1)),
        Ebin = write_plugin_app(LibDir, ?PLUGIN, [{Mod, Bin}]),
        Beam = beam_path(Ebin, Mod),
        {module, Mod} = code:load_binary(Mod, Beam, Bin),
        ?assertEqual(ok, emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)),
        ?assertEqual(Beam, code:which(Mod))
    end).

preflight_allows_sibling_version_reload_test() ->
    with_plugin_env(fun() ->
        OldNameVsn = "myplugin-1.0.0",
        NewNameVsn = "myplugin-1.1.0",
        Bin1 = compile_beam(myplugin, plain_src(myplugin, v1)),
        OldEbin = write_plugin_app(lib_dir(OldNameVsn), OldNameVsn, [{myplugin, Bin1}]),
        {module, myplugin} = code:load_binary(myplugin, beam_path(OldEbin, myplugin), Bin1),
        %% An upgrade unloads the old application but its modules stay in the
        %% code server, because `unload' only soft purges.
        ok = application:load({application, myplugin, [{vsn, "1.0.0"}, {modules, [myplugin]}]}),
        ok = application:unload(myplugin),
        Bin2 = compile_beam(myplugin, plain_src(myplugin, v2)),
        NewEbin = write_plugin_app(lib_dir(NewNameVsn), NewNameVsn, [{myplugin, Bin2}]),
        ?assertEqual(ok, emqx_plugins_apps:load(#{rel_apps => [NewNameVsn]}, lib_dir(NewNameVsn))),
        ?assertEqual(beam_path(NewEbin, myplugin), code:which(myplugin))
    end).

early_on_load_not_executed_when_package_rejected_test() ->
    with_on_load_marker(a_onload, fun() ->
        with_plugin_env(fun() ->
            LibDir = lib_dir(?PLUGIN),
            OnLoad = compile_beam(a_onload, onload_src(a_onload)),
            Good = compile_beam(z_bad, plain_src(z_bad, ok)),
            _Ebin = write_plugin_app(LibDir, ?PLUGIN, [
                {a_onload, OnLoad}, {z_bad, truncate(Good)}
            ]),
            Path0 = code:get_path(),
            ?assertMatch({error, _}, emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)),
            ?assertEqual(undefined, on_load_ran(a_onload)),
            ?assertNot(code:is_loaded(a_onload)),
            ?assertEqual(Path0, code:get_path())
        end)
    end).

rollback_on_application_load_failure_test() ->
    with_on_load_marker(a_onload, fun() ->
        with_plugin_env(fun() ->
            LibDir = lib_dir(?PLUGIN),
            Ebin = ebin(LibDir, ?PLUGIN),
            ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
            Bin = compile_beam(a_onload, onload_src(a_onload)),
            ok = write_beam(Ebin, a_onload, Bin),
            ok = write_app(
                Ebin,
                "plug.app",
                app_file_with(plug, [
                    {vsn, "1.0.0"},
                    {modules, [a_onload]},
                    %% `kernel' is already loaded and running: loading this spec
                    %% leaves it alone, and the rollback must not unload it.
                    {included_applications, [kernel, missing_app_xyz]}
                ])
            ),
            Path0 = code:get_path(),
            ?assertMatch(
                {error, #{msg := "failed_to_load_plugin_app"}},
                emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)
            ),
            ?assertEqual(undefined, on_load_ran(a_onload)),
            ?assertNot(code:is_loaded(a_onload)),
            ?assertEqual(Path0, code:get_path()),
            %% `kernel' survived the rollback, and the application controller
            %% is still usable.
            ?assert(lists:keymember(kernel, 1, application:loaded_applications())),
            ?assertMatch([_ | _], application:which_applications()),
            ?assertNot(lists:keymember(plug, 1, application:loaded_applications()))
        end)
    end).

rollback_restores_replaced_module_test() ->
    with_plugin_env(fun() ->
        OldNameVsn = "rollbackplug-0.9.0",
        NewNameVsn = "rollbackplug-1.0.0",
        Mod = rollback_mod,
        OldBin = compile_beam(Mod, plain_src(Mod, original)),
        OldEbin = write_plugin_app(lib_dir(OldNameVsn), OldNameVsn, [{Mod, OldBin}]),
        OldBeam = beam_path(OldEbin, Mod),
        {module, Mod} = code:load_binary(Mod, OldBeam, OldBin),
        NewBin = compile_beam(Mod, plain_src(Mod, plugin)),
        FailBin = compile_beam(z_fail, failing_onload_src(z_fail)),
        _NewEbin = write_plugin_app(lib_dir(NewNameVsn), NewNameVsn, [
            {Mod, NewBin}, {z_fail, FailBin}
        ]),
        ?assertMatch(
            {error, #{msg := "failed_to_load_plugin_beam"}},
            emqx_plugins_apps:load(#{rel_apps => [NewNameVsn]}, lib_dir(NewNameVsn))
        ),
        ?assertEqual(original, Mod:ping()),
        %% `restore_module/2' loads the old file by absolute path.
        ?assertEqual(filename:absname(OldBeam), code:which(Mod))
    end).

%% A fresh install (nothing to replace) which fails on a later beam must not
%% report rollback errors for the beams it did publish: the ebin added in phase 1
%% makes `code:which/1' find the package's own beam for a module which is not
%% loaded, which must not be mistaken for previous code of that module.
rollback_of_fresh_install_reports_no_errors_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        %% `a_ok' is published before `z_fail' (beams are enumerated in order).
        OkBin = compile_beam(a_ok, plain_src(a_ok, ok)),
        FailBin = compile_beam(z_fail, failing_onload_src(z_fail)),
        _Ebin = write_plugin_app(LibDir, ?PLUGIN, [{a_ok, OkBin}, {z_fail, FailBin}]),
        Result = emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir),
        ?assertMatch({error, #{msg := "failed_to_load_plugin_beam"}}, Result),
        {error, Reason} = Result,
        ?assertNot(maps:is_key(rollback_errors, Reason)),
        ?assertNot(code:is_loaded(a_ok)),
        ?assertNot(code:is_loaded(z_fail))
    end).

%% `code:del_path/1' returns `false' when the directory is not on the path any
%% more; the rollback must treat that as "already removed" instead of crashing
%% out of `load/2'.
rollback_tolerates_ebin_already_removed_from_path_test() ->
    with_plugin_env(fun() ->
        %% Inside the throw-away install directory, so that it is removed with
        %% it and no directory is left behind in the working directory.
        Ebin = filename:absname(
            filename:join(emqx_plugins_fs:install_dir(), "rollback_dup_ebin_test")
        ),
        ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
        Path0 = code:get_path(),
        true = code:add_patha(Ebin),
        %% The same ebin added twice, as it would be while the transaction
        %% recorded two additions: the second removal finds it already gone.
        Tx = #{
            prev_path => Path0,
            added_paths => [Ebin, Ebin],
            loaded_mods => [],
            loaded_apps => []
        },
        ?assertEqual([], emqx_plugins_apps:rollback(Tx)),
        ?assertNot(lists:member(Ebin, code:get_path()))
    end).

%% When the load which would have replaced a module fails (here in the new
%% module's `-on_load'), the failed code never becomes current: `code:load_binary/3'
%% only purges old code before loading, so the previously loaded module is still
%% the loaded one.  The rollback must therefore not touch it (and there is
%% nothing to record for it).
failed_publish_keeps_previously_loaded_module_test() ->
    with_plugin_env(fun() ->
        OldNameVsn = "failedpublishplug-0.9.0",
        NewNameVsn = "failedpublishplug-1.0.0",
        Mod = failed_publish_mod,
        OldBin = compile_beam(Mod, plain_src(Mod, original)),
        OldEbin = write_plugin_app(lib_dir(OldNameVsn), OldNameVsn, [{Mod, OldBin}]),
        OldBeam = beam_path(OldEbin, Mod),
        {module, Mod} = code:load_binary(Mod, OldBeam, OldBin),
        %% The very first beam of the new package is the one which fails, so no
        %% module of the package is ever published.
        FailBin = compile_beam(Mod, failing_onload_src(Mod)),
        _NewEbin = write_plugin_app(lib_dir(NewNameVsn), NewNameVsn, [{Mod, FailBin}]),
        Result = emqx_plugins_apps:load(#{rel_apps => [NewNameVsn]}, lib_dir(NewNameVsn)),
        ?assertMatch({error, #{msg := "failed_to_load_plugin_beam"}}, Result),
        {error, Reason} = Result,
        ?assertNot(maps:is_key(rollback_errors, Reason)),
        ?assertEqual(original, Mod:ping()),
        ?assertEqual(filename:absname(OldBeam), filename:absname(code:which(Mod)))
    end).

%% When the code being replaced is already gone from disk (its version
%% directory was purged), the rollback can not put it back; the rejected
%% package's code must not stay loaded either.
rollback_drops_module_when_previous_code_is_gone_test() ->
    with_plugin_env(fun() ->
        OldNameVsn = "goneplug-0.9.0",
        NewNameVsn = "goneplug-1.0.0",
        Mod = gone_mod,
        OldBin = compile_beam(Mod, plain_src(Mod, original)),
        OldEbin = write_plugin_app(lib_dir(OldNameVsn), OldNameVsn, [{Mod, OldBin}]),
        {module, Mod} = code:load_binary(Mod, beam_path(OldEbin, Mod), OldBin),
        ok = file:del_dir_r(filename:dirname(OldEbin)),
        NewBin = compile_beam(Mod, plain_src(Mod, plugin)),
        FailBin = compile_beam(z_fail, failing_onload_src(z_fail)),
        _NewEbin = write_plugin_app(lib_dir(NewNameVsn), NewNameVsn, [
            {Mod, NewBin}, {z_fail, FailBin}
        ]),
        Result = emqx_plugins_apps:load(#{rel_apps => [NewNameVsn]}, lib_dir(NewNameVsn)),
        ?assertMatch(
            {error, #{msg := "failed_to_load_plugin_beam", rollback_errors := [_ | _]}},
            Result
        ),
        ?assertNot(code:is_loaded(Mod))
    end).

%% On an in-place reinstall the package has already overwritten the file the
%% previous code was loaded from, so the rollback must not reload the rejected
%% bytes from it.  Dropping the module is the expected outcome of that case, so
%% it is logged rather than reported as a rollback failure.
rollback_drops_module_when_its_file_was_replaced_test() ->
    with_plugin_env(fun() ->
        NameVsn = "inplaceplug-1.0.0",
        LibDir = lib_dir(NameVsn),
        Mod = inplace_mod,
        OldBin = compile_beam(Mod, plain_src(Mod, original)),
        Ebin = write_plugin_app(LibDir, NameVsn, [{Mod, OldBin}]),
        {module, Mod} = code:load_binary(Mod, beam_path(Ebin, Mod), OldBin),
        NewBin = compile_beam(Mod, plain_src(Mod, plugin)),
        FailBin = compile_beam(z_fail, failing_onload_src(z_fail)),
        _Ebin2 = write_plugin_app(LibDir, NameVsn, [{Mod, NewBin}, {z_fail, FailBin}]),
        Result = emqx_plugins_apps:load(#{rel_apps => [NameVsn]}, LibDir),
        ?assertMatch({error, #{msg := "failed_to_load_plugin_beam"}}, Result),
        {error, Reason} = Result,
        ?assertNot(maps:is_key(rollback_errors, Reason)),
        ?assertNot(code:is_loaded(Mod))
    end).

%% `application:load/1' reads the `.app' of an included application from the
%% code path, so a package shipped application including an already loaded one
%% must be loaded with its own (sanitised) spec first, or the rollback would
%% recursively unload the already loaded application.
rollback_survives_nested_included_application_test() ->
    with_plugin_env(fun() ->
        NameVsn = "nestedplug-1.0.0",
        LibDir = lib_dir(NameVsn),
        ParentNameVsn = "pkgparent-1.0.0",
        ChildNameVsn = "pkgchild-1.0.0",
        ParentEbin = ebin(LibDir, ParentNameVsn),
        ChildEbin = ebin(LibDir, ChildNameVsn),
        ok = filelib:ensure_dir(filename:join(ParentEbin, "dummy")),
        ok = filelib:ensure_dir(filename:join(ChildEbin, "dummy")),
        ok = write_beam(
            ParentEbin, pkg_parent_mod, compile_beam(pkg_parent_mod, plain_src(pkg_parent_mod, ok))
        ),
        ok = write_beam(
            ChildEbin, pkg_child_mod, compile_beam(pkg_child_mod, plain_src(pkg_child_mod, ok))
        ),
        ok = write_app(
            ParentEbin,
            "pkgparent.app",
            app_file_with(pkgparent, [
                {vsn, "1.0.0"},
                {modules, [pkg_parent_mod]},
                {included_applications, [pkgchild]}
            ])
        ),
        ok = write_app(
            ChildEbin,
            "pkgchild.app",
            app_file_with(pkgchild, [
                {vsn, "1.0.0"},
                {modules, [pkg_child_mod]},
                %% `kernel' is loaded and running, and `missing_app_xyz' makes the
                %% load fail after `pkgchild' has been inserted.
                {included_applications, [kernel, missing_app_xyz]}
            ])
        ),
        Path0 = code:get_path(),
        ?assertMatch(
            {error, #{msg := "failed_to_load_plugin_app"}},
            emqx_plugins_apps:load(
                #{rel_apps => [<<"pkgparent-1.0.0">>, <<"pkgchild-1.0.0">>]}, LibDir
            )
        ),
        ?assert(lists:keymember(kernel, 1, application:loaded_applications())),
        ?assertMatch([_ | _], application:which_applications()),
        ?assertNot(lists:keymember(pkgparent, 1, application:loaded_applications())),
        ?assertNot(lists:keymember(pkgchild, 1, application:loaded_applications())),
        ?assertEqual(Path0, code:get_path())
    end).

preflight_rejects_unparsable_beam_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Ebin = ebin(LibDir, ?PLUGIN),
        ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
        Bin = <<"this is not a beam file">>,
        ok = write_beam(Ebin, a_mod, Bin),
        ok = write_app(Ebin, "plug.app", app_file(plug, "1.0.0", [a_mod])),
        %% The error must not carry the whole file, only its size.
        Result = emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir),
        ?assertMatch(
            {error, #{
                msg := "plugin_beam_unparsable",
                reason := {not_a_beam_file, {binary, _}}
            }},
            Result
        ),
        {error, #{reason := {not_a_beam_file, {binary, Size}}}} = Result,
        ?assertEqual(byte_size(Bin), Size)
    end).

preflight_rejects_missing_declared_beam_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Ebin = ebin(LibDir, ?PLUGIN),
        ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
        Bin = compile_beam(a_mod, plain_src(a_mod, ok)),
        ok = write_beam(Ebin, a_mod, Bin),
        ok = write_app(Ebin, "plug.app", app_file(plug, "1.0.0", [a_mod, missing_mod])),
        ?assertMatch(
            {error, #{msg := "plugin_app_beam_missing", module := missing_mod}},
            emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)
        )
    end).

%% `beam_lib' puts the whole beam in the source position of every error it
%% reports for a binary input; only its size may reach the log or the API
%% response.  The truncation sizes cut the beam before the first chunk's data,
%% inside its header, and after it.
preflight_error_reason_keeps_only_the_beam_size_test() ->
    Cases = [
        {12, missing_chunk, "plugin_beam_unparsable"},
        {16, invalid_beam_file, "plugin_beam_unparsable"},
        {20, chunk_too_big, "plugin_beam_truncated"}
    ],
    lists:foreach(
        fun({Size, Tag, Msg}) ->
            with_plugin_env(fun() ->
                LibDir = lib_dir(?PLUGIN),
                Ebin = ebin(LibDir, ?PLUGIN),
                ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
                Truncated = truncate(compile_beam(a_mod, plain_src(a_mod, ok)), Size),
                ok = write_beam(Ebin, a_mod, Truncated),
                ok = write_app(Ebin, "plug.app", app_file(plug, "1.0.0", [a_mod])),
                Result = emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir),
                {error, #{msg := ActualMsg, reason := ActualReason}} = Result,
                ?assertEqual(Msg, ActualMsg),
                %% The source is the second element of every `beam_lib' error
                %% tuple; it must be the size, not the file.
                ?assertEqual(Tag, element(1, ActualReason)),
                ?assertEqual({binary, byte_size(Truncated)}, element(2, ActualReason)),
                ?assertEqual([], binary_leaves(ActualReason))
            end)
        end,
        Cases
    ).

%% A module loaded from another plugin (its file is still there) may not be
%% replaced unless the application is shared.
preflight_rejects_other_plugin_module_test() ->
    with_plugin_env(fun() ->
        OtherNameVsn = "otherplug-1.0.0",
        NewNameVsn = "newplug-1.0.0",
        Mod = foreign_mod,
        OtherBin = compile_beam(Mod, plain_src(Mod, other)),
        OtherEbin = write_plugin_app(lib_dir(OtherNameVsn), OtherNameVsn, [{Mod, OtherBin}]),
        {module, Mod} = code:load_binary(Mod, beam_path(OtherEbin, Mod), OtherBin),
        NewBin = compile_beam(Mod, plain_src(Mod, plugin)),
        _NewEbin = write_plugin_app(lib_dir(NewNameVsn), NewNameVsn, [{Mod, NewBin}]),
        ?assertMatch(
            {error, #{
                msg := "plugin_beam_load_conflict",
                conflict := loaded_from_other_plugin
            }},
            emqx_plugins_apps:load(#{rel_apps => [NewNameVsn]}, lib_dir(NewNameVsn))
        )
    end).

%% ... but it may when both plugins ship the very same application spec.
preflight_allows_shared_app_module_test() ->
    with_plugin_env(fun() ->
        OtherNameVsn = "otherplug-1.0.0",
        NewNameVsn = "newplug-1.0.0",
        AppNameVsn = "sharedapp-1.0.0",
        Mod = shared_mod,
        OtherBin = compile_beam(Mod, plain_src(Mod, shared_v1)),
        OtherEbin = write_plugin_app(lib_dir(OtherNameVsn), AppNameVsn, [{Mod, OtherBin}]),
        {module, Mod} = code:load_binary(Mod, beam_path(OtherEbin, Mod), OtherBin),
        NewBin = compile_beam(Mod, plain_src(Mod, shared_v2)),
        NewEbin = write_plugin_app(lib_dir(NewNameVsn), AppNameVsn, [{Mod, NewBin}]),
        ?assertEqual(
            ok,
            emqx_plugins_apps:load(#{rel_apps => [AppNameVsn]}, lib_dir(NewNameVsn))
        ),
        ?assertEqual(beam_path(NewEbin, Mod), code:which(Mod))
    end).

on_load_failure_is_rejected_and_rolled_back_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        FailBin = compile_beam(z_fail, failing_onload_src(z_fail)),
        _Ebin = write_plugin_app(LibDir, ?PLUGIN, [{z_fail, FailBin}]),
        Path0 = code:get_path(),
        ?assertMatch(
            {error, #{msg := "failed_to_load_plugin_beam"}},
            emqx_plugins_apps:load(#{rel_apps => [?PLUGIN]}, LibDir)
        ),
        ?assertNot(code:is_loaded(z_fail)),
        ?assertEqual(Path0, code:get_path())
    end).

add_path_failure_leaves_code_server_untouched_test() ->
    with_plugin_env(fun() ->
        Path0 = code:get_path(),
        Apps0 = application:loaded_applications(),
        Tx = #{prev_path => Path0, added_paths => [], loaded_mods => [], loaded_apps => []},
        Plan = [
            #{
                app => no_such_app,
                ebin => "/nonexistent-plugin-ebin",
                spec => {application, no_such_app, [{vsn, "1.0.0"}]},
                entries => []
            }
        ],
        ?assertMatch(
            {error, #{msg := "failed_to_add_plugin_ebin_to_code_path"}, _},
            emqx_plugins_apps:do_load(Plan, Tx)
        ),
        ?assertEqual(Path0, code:get_path()),
        ?assertEqual(Apps0, application:loaded_applications())
    end).

preflight_is_idempotent_and_adds_path_once_test() ->
    with_plugin_env(fun() ->
        LibDir = lib_dir(?PLUGIN),
        Bin = compile_beam(a_mod, plain_src(a_mod, ok)),
        Ebin = write_plugin_app(LibDir, ?PLUGIN, [{a_mod, Bin}]),
        Plugin = #{rel_apps => [?PLUGIN]},
        ?assertEqual(ok, emqx_plugins_apps:validate(Plugin, LibDir)),
        ?assertEqual(ok, emqx_plugins_apps:validate(Plugin, LibDir)),
        ?assertEqual(ok, emqx_plugins_apps:load(Plugin, LibDir)),
        %% A second load of the same plugin is a no-op, it must not add the
        %% ebin again.
        ?assertEqual(ok, emqx_plugins_apps:load(Plugin, LibDir)),
        Path = code:get_path(),
        EbinAbs = filename:absname(Ebin),
        ?assertEqual(1, length([P || P <- Path, filename:absname(P) =:= EbinAbs])),
        ?assertEqual(EbinAbs, filename:absname(hd(Path)))
    end).

%%--------------------------------------------------------------------
%% Fixtures
%%--------------------------------------------------------------------

%% Run `F' inside a fresh random install directory, and put the code server
%% back the way it was afterwards.
with_plugin_env(F) ->
    emqx_plugins_tests:meck_emqx(),
    try
        emqx_plugins_tests:with_rand_install_dir(fun(Dir) ->
            PrevPath = code:get_path(),
            PrevApps = [App || {App, _, _} <- application:loaded_applications()],
            PrevMods = code:all_loaded(),
            try
                F()
            after
                restore_code_server(Dir, PrevPath, PrevApps, PrevMods)
            end
        end)
    after
        emqx_plugins_tests:unmeck_emqx()
    end.

restore_code_server(Dir, PrevPath, PrevApps, PrevMods) ->
    lists:foreach(
        fun({App, _, _}) ->
            case lists:member(App, PrevApps) of
                true -> ok;
                false -> _ = application:unload(App)
            end
        end,
        application:loaded_applications()
    ),
    DirAbs = filename:split(filename:absname(Dir)),
    lists:foreach(
        fun({Mod, File}) ->
            IsNew = not lists:keymember(Mod, 1, PrevMods),
            case IsNew andalso not code:is_sticky(Mod) andalso in_dir(File, DirAbs) of
                true -> drop_module(Mod);
                false -> ok
            end
        end,
        code:all_loaded()
    ),
    _ = code:set_path(PrevPath),
    ok.

in_dir(File, DirParts) when is_list(File) ->
    lists:prefix(DirParts, filename:split(filename:absname(File)));
in_dir(_File, _DirParts) ->
    false.

drop_module(Mod) ->
    _ = code:delete(Mod),
    _ = code:purge(Mod),
    _ = code:delete(Mod),
    _ = code:purge(Mod),
    ok.

with_on_load_marker(Mod, F) ->
    Key = {Mod, on_load_ran},
    persistent_term:erase(Key),
    try
        F()
    after
        persistent_term:erase(Key)
    end.

on_load_ran(Mod) ->
    persistent_term:get({Mod, on_load_ran}, undefined).

%%--------------------------------------------------------------------
%% Package fixtures
%%--------------------------------------------------------------------

lib_dir(NameVsn) ->
    emqx_plugins_fs:lib_dir(NameVsn).

ebin(LibDir, AppNameVsn) ->
    filename:join([LibDir, AppNameVsn, "ebin"]).

beam_path(Ebin, Mod) ->
    filename:join(Ebin, atom_to_list(Mod) ++ ".beam").

%% Write `<LibDir>/<AppNameVsn>/ebin' with the given beams and a matching `.app'.
write_plugin_app(LibDir, AppNameVsn, Mods) ->
    Ebin = ebin(LibDir, AppNameVsn),
    ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
    {AppName, AppVsn} = emqx_plugins_utils:parse_name_vsn(AppNameVsn),
    ok = write_app(Ebin, atom_to_list(AppName) ++ ".app", app_file(AppName, AppVsn, mods(Mods))),
    ok = lists:foreach(fun({Mod, Bin}) -> write_beam(Ebin, Mod, Bin) end, Mods),
    Ebin.

mods(Mods) ->
    [Mod || {Mod, _Bin} <- Mods].

write_beam(Ebin, Mod, Bin) ->
    ok = file:write_file(beam_path(Ebin, Mod), Bin).

write_app(Ebin, FileName, Content) ->
    ok = file:write_file(filename:join(Ebin, FileName), Content).

app_file(AppName, AppVsn, Modules) ->
    app_file_with(AppName, [
        {description, "test plugin app"},
        {vsn, AppVsn},
        {modules, Modules},
        {registered, []},
        {applications, [kernel, stdlib]}
    ]).

app_file_with(AppName, Props) ->
    iolist_to_binary(io_lib:format("~p.~n", [{application, AppName, Props}])).

truncate(Bin) ->
    binary:part(Bin, 0, byte_size(Bin) div 2).

truncate(Bin, Size) ->
    binary:part(Bin, 0, Size).

%% Every binary found in a term, to check that an error reason does not carry
%% the bytes of a beam file.
binary_leaves(Term) when is_tuple(Term) ->
    lists:append([binary_leaves(Element) || Element <- tuple_to_list(Term)]);
binary_leaves(Bin) when is_binary(Bin) ->
    [byte_size(Bin)];
binary_leaves(List) when is_list(List) ->
    lists:append([binary_leaves(Element) || Element <- List]);
binary_leaves(_Term) ->
    [].

%%--------------------------------------------------------------------
%% Beam fixtures
%%--------------------------------------------------------------------

compile_beam(Mod, Src) ->
    SrcDir = filename:join(emqx_plugins_fs:install_dir(), "src"),
    ok = filelib:ensure_dir(filename:join(SrcDir, "dummy")),
    SrcFile = filename:join(SrcDir, atom_to_list(Mod) ++ ".erl"),
    ok = file:write_file(SrcFile, Src),
    try
        {ok, Forms} = epp:parse_file(SrcFile, [], []),
        {ok, Mod, Bin} = compile:forms(Forms, [binary, return_errors]),
        Bin
    after
        _ = file:delete(SrcFile)
    end.

plain_src(Mod, Value) ->
    lists:flatten(
        io_lib:format(
            "-module(~s).~n-export([ping/0]).~nping() -> ~p.~n",
            [Mod, Value]
        )
    ).

onload_src(Mod) ->
    lists:flatten(
        io_lib:format(
            "-module(~s).~n-export([ping/0]).~n-on_load(init/0).~n"
            "init() -> persistent_term:put({~s, on_load_ran}, true), ok.~n"
            "ping() -> ok.~n",
            [Mod, Mod]
        )
    ).

failing_onload_src(Mod) ->
    lists:flatten(
        io_lib:format(
            "-module(~s).~n-export([ping/0]).~n-on_load(init/0).~n"
            "init() -> {error, nope}.~n"
            "ping() -> ok.~n",
            [Mod]
        )
    ).

stdlib_beam(Mod) ->
    File = filename:join([code:lib_dir(stdlib), "ebin", atom_to_list(Mod) ++ ".beam"]),
    {ok, Bin} = file:read_file(File),
    Bin.

%% Load a module which does not belong to any plugin, to stand in for an EMQX
%% module the package must not replace.
load_outside_module(Mod) ->
    OutsideDir = filename:absname("emqx_plugins_apps_tests_outside"),
    Ebin = filename:join(OutsideDir, "ebin"),
    ok = filelib:ensure_dir(filename:join(Ebin, "dummy")),
    Bin = compile_beam(Mod, plain_src(Mod, ok)),
    Beam = filename:join(Ebin, atom_to_list(Mod) ++ ".beam"),
    ok = file:write_file(Beam, Bin),
    {module, Mod} = code:load_binary(Mod, Beam, Bin),
    {Bin, Beam, OutsideDir}.
