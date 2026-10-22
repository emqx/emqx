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
            Info = fun(RelApps) ->
                "name=bar, rel_vsn=\"5\", rel_apps=" ++ RelApps ++
                    ", description=\"desc bar\""
            end,
            %% The entries name the directories of the package's applications,
            %% so an entry which is not a single path component must not be
            %% accepted: a directory built from it would leave the plugin's
            %% install directory.
            ProbeApp =
                "atom_probe_" ++ integer_to_list(erlang:unique_integer([positive])) ++ "/nested",
            BadRelApps = [
                "[justname_no_vsn]",
                "[\"../escape-1.0\"]",
                "[\"a/dep-1.0\"]",
                %% only the name part is validated: an empty application name and
                %% an absolute path are still refused
                "[\"-1\"]",
                "[\"/absolute-1.0\"]",
                %% the rejected entry must not create an atom for its name part
                "[\"" ++ ProbeApp ++ "-1.0\"]"
            ],
            ?assertError(badarg, binary_to_existing_atom(list_to_binary(ProbeApp), utf8)),
            try
                lists:foreach(
                    fun(RelApps) ->
                        ok = write_file(InfoFile, Info(RelApps)),
                        ?assertMatch(
                            {error, #{msg := "bad_rel_apps"}},
                            emqx_plugins:read_plugin_info(NameVsn, #{})
                        )
                    end,
                    BadRelApps
                ),
                ?assertError(badarg, binary_to_existing_atom(list_to_binary(ProbeApp), utf8)),
                %% A bundled application may carry a version which the plugin
                %% package identifier syntax does not allow.
                ok = write_file(InfoFile, Info("[\"dep-1.2.3+build.4\"]")),
                ?assertMatch({ok, _}, emqx_plugins:read_plugin_info(NameVsn, #{}))
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

%% Hold the installation lock of a plugin in another process, so that a test can
%% observe how an operation behaves while the lock is taken.  The caller releases
%% the holder with `Holder ! release', and the wait is shorter than the EUnit
%% timeout of a test, so a holder which never takes the lock fails this helper
%% instead of being cancelled.
hold_installation_lock(NameVsn) ->
    Parent = self(),
    Holder = spawn(fun() ->
        ok = emqx_plugins_fs:with_installation_lock(NameVsn, fun() ->
            Parent ! {locked, self()},
            receive
                release -> ok
            end
        end)
    end),
    receive
        {locked, Holder} -> Holder
    after 2000 ->
        error(installation_lock_not_held)
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
                "../kept-1",
                filename:absname(Kept),
                "valid-1/../../kept-1",
                "..-1",
                ".-1",
                "-1",
                %% the installation directory of such a name is the package file
                %% (or the checksum) of `kept-1'
                "kept-1.tar.gz",
                "kept-1.tar.gz.md5sum",
                %% the directory where installation attempts are unpacked
                ".emqx-plugin-staging"
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
            %% A name which is not a string at all is refused as well, and an
            %% improper list must not make the operation crash.
            ?assertMatch(
                {error, #{msg := "bad_plugin_package_name"}}, emqx_plugins:purge(123)
            ),
            ?assertMatch(
                {error, #{msg := "bad_plugin_package_name"}},
                emqx_plugins:purge([$p, $-, $1 | bad_tail])
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
    meck:expect(
        emqx,
        update_config,
        fun(Path, Values, _Opts) ->
            emqx_config:put(Path, Values)
        end
    ),
    ok.

unmeck_emqx() ->
    meck:unload(emqx),
    stop_install_serializer(),
    ok.

%% The installation path takes its lock from `emqx_plugins_install_serializer',
%% which the plugins application starts in a running node.  These tests run
%% without the plugins application (and without the rest of EMQX), so the server
%% is started here.
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

%%--------------------------------------------------------------------
%% plugin API response header filtering (allow-list)
%%--------------------------------------------------------------------

map_plugin_api_result_filters_forbidden_headers_test() ->
    Stripped = [
        %% auth
        <<"authorization">>,
        <<"www-authenticate">>,
        %% cookies
        <<"set-cookie">>,
        <<"cookie">>,
        %% CORS
        <<"access-control-allow-origin">>,
        <<"access-control-request-method">>,
        %% redirects
        <<"location">>,
        <<"refresh">>,
        %% security/policy
        <<"content-security-policy">>,
        <<"strict-transport-security">>,
        <<"x-frame-options">>,
        <<"x-content-type-options">>,
        <<"referrer-policy">>,
        %% custom header without the x-plugin- prefix
        <<"x-custom">>,
        %% non-binary key is dropped too
        content_type
    ],
    Allowed = [
        <<"content-type">>,
        <<"cache-control">>,
        <<"etag">>,
        <<"x-request-id">>,
        <<"x-plugin-custom">>,
        <<"x-plugin-set-cookie">>
    ],
    Headers = maps:from_list([{K, <<"1">>} || K <- Stripped ++ Allowed]),
    {200, RespHeaders, #{ok := true}} = emqx_plugins:map_plugin_api_result(
        {ok, 200, Headers, #{ok => true}}
    ),
    [?assertNot(maps:is_key(K, RespHeaders)) || K <- Stripped],
    [?assertEqual(<<"1">>, maps:get(K, RespHeaders)) || K <- Allowed],
    %% error clause is filtered too
    {401, ErrHeaders, #{error := true}} = emqx_plugins:map_plugin_api_result(
        {error, 401, Headers, #{error => true}}
    ),
    [?assertNot(maps:is_key(K, ErrHeaders)) || K <- Stripped],
    [?assertEqual(<<"1">>, maps:get(K, ErrHeaders)) || K <- Allowed].

map_plugin_api_result_case_insensitive_test() ->
    Headers = #{
        <<"Set-Cookie">> => <<"a">>,
        <<"LOCATION">> => <<"b">>,
        <<"Content-Type">> => <<"c">>,
        <<"X-Plugin-Foo">> => <<"d">>
    },
    {200, RespHeaders, #{}} = emqx_plugins:map_plugin_api_result({ok, 200, Headers, #{}}),
    %% Stripped regardless of input case
    [
        ?assertNot(maps:is_key(K, RespHeaders))
     || K <- [<<"set-cookie">>, <<"Set-Cookie">>, <<"location">>, <<"LOCATION">>]
    ],
    %% Allowed headers preserved (normalization keeps original case)
    ?assertEqual(<<"c">>, iolist_to_binary(maps:get(<<"Content-Type">>, RespHeaders))),
    %% custom header with x-plugin- prefix passes (prefix matched case-insensitively)
    ?assertEqual(<<"d">>, iolist_to_binary(maps:get(<<"X-Plugin-Foo">>, RespHeaders))).

%%--------------------------------------------------------------------
%% CLI audit logging (emqx#18717)
%%--------------------------------------------------------------------

%% None of the `emqx ctl plugins' arguments are sensitive, so the audit args
%% callback must keep them verbatim. Before the fix, `emqx_plugins_cli_utils'
%% did not export the callback at all and `emqx_ctl' masked every argument.
plugins_audit_args_preserves_arguments_test() ->
    Sha256 = "sha256:" ++ lists:duplicate(64, $a),
    ArgsList = [
        [],
        ["list"],
        ["install", "my_plugin-1.0.0"],
        ["install", "my_plugin-1.0.0", "--cluster"],
        ["uninstall", "my_plugin-1.0.0"],
        ["allow", "my_plugin-1.0.0", Sha256],
        ["enable", "my_plugin-1.0.0", "before", "other_plugin-0.1.0"]
    ],
    lists:foreach(
        fun(Args) ->
            ?assertEqual(Args, emqx_plugins_cli_utils:plugins_audit_args(Args))
        end,
        ArgsList
    ),
    %% `emqx_ctl' discovers the callback by name and arity; without the
    %% export it silently falls back to masking every argument again.
    ?assert(erlang:function_exported(emqx_plugins_cli_utils, plugins_audit_args, 1)).

%% `plugins install <Name-Vsn> --cluster' must reach the cluster install
%% path rather than falling through to the usage clause (the `--cluster'
%% branch was accidentally dropped when the CLI moved out of emqx_mgmt_cli).
plugins_cli_install_cluster_dispatches_test() ->
    catch meck:unload(emqx_plugins),
    catch meck:unload(emqx_ctl),
    ok = meck:new(emqx_plugins, [passthrough]),
    ok = meck:new(emqx_ctl, [passthrough]),
    try
        Parent = self(),
        ok = meck:expect(emqx_plugins, is_allowed_installation, fun(_NameVsn) -> false end),
        ok = meck:expect(emqx_ctl, print, fun(Fmt, Args) ->
            Parent ! {printed, lists:flatten(io_lib:format(Fmt, Args))},
            ok
        end),
        ok = emqx_plugins_cli_utils:plugins(["install", "my_plugin-1.0.0", "--cluster"]),
        %% Only the cluster install path prints `ensure_installed_cluster' as
        %% the action; the usage fallback would print the command list.
        receive
            {printed, Output} ->
                ?assertNotEqual(nomatch, string:find(Output, "ensure_installed_cluster"), Output)
        after 1000 ->
            error(cluster_install_not_dispatched)
        end
    after
        meck:unload(emqx_ctl),
        meck:unload(emqx_plugins)
    end.

%% A failed cluster install must stay observable: `emqx_ctl' derives the
%% audit level and the CLI exit code from the handler result, so
%% `ensure_installed_cluster/2' must not collapse failures into `ok'.
ensure_installed_cluster_reports_failure_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                catch meck:unload(emqx_plugins),
                ok = meck:new(emqx_plugins, [passthrough]),
                try
                    ok = meck:expect(
                        emqx_plugins, is_allowed_installation, fun(_NameVsn) -> true end
                    ),
                    LogFun = fun(_Fmt, _Args) -> ok end,
                    ?assertMatch(
                        {error, _},
                        emqx_plugins_cli_utils:ensure_installed_cluster(
                            "no_such_plugin-1.0.0", LogFun
                        )
                    )
                after
                    meck:unload(emqx_plugins)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% The single-node CLI install classifies the installation and verifies the
%% package of the install dir under the installation lock: while another
%% installation of the same plugin holds it, neither is read, and the CLI
%% reports the refusal.
cli_install_takes_the_installation_lock_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                catch meck:unload(emqx_plugins),
                ok = meck:new(emqx_plugins, [passthrough]),
                try
                    Parent = self(),
                    ok = meck:expect(emqx_plugins, is_allowed_installation, fun(_NameVsn) ->
                        true
                    end),
                    ok = meck:expect(emqx_plugins, install_state, fun(_NameVsn) ->
                        Parent ! installation_classified,
                        absent
                    end),
                    NameVsn = "locked_cli-0.1.0",
                    Holder = hold_installation_lock(NameVsn),
                    try
                        ok = emqx_plugins_cli_utils:ensure_installed(NameVsn, fun(Fmt, Args) ->
                            Parent ! {printed, lists:flatten(io_lib:format(Fmt, Args))},
                            ok
                        end),
                        receive
                            {printed, Output} ->
                                ?assertNotEqual(
                                    nomatch, string:find(Output, "installation_in_progress")
                                )
                        after 1000 ->
                            error(cli_install_not_reported)
                        end,
                        receive
                            installation_classified ->
                                error(cli_classified_the_installation_outside_the_lock)
                        after 200 ->
                            ok
                        end
                    after
                        Holder ! release
                    end
                after
                    meck:unload(emqx_plugins)
                end
            end
        )
    after
        unmeck_emqx()
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

%% An installation which can not take the lock of its plugin is refused before
%% anything is written: the package file and the unpack are one critical
%% section, so a refused attempt leaves neither a package file nor a plugin
%% directory behind.
install_package_writes_nothing_when_the_install_lock_is_unavailable_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "locked-0.1.0",
                Holder = hold_installation_lock(NameVsn),
                try
                    ?assertMatch(
                        {error, #{reason := installation_in_progress}},
                        emqx_plugins:install_package(NameVsn, plugin_package(NameVsn, []))
                    ),
                    ?assertEqual(false, emqx_plugins:is_package_present(NameVsn)),
                    ?assertNot(emqx_plugins_fs:is_installed(NameVsn))
                after
                    Holder ! release
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% An installation of the same plugin which runs on this node can be between the
%% publication and the validation of its tree: the tree is already complete, but
%% it can still be rolled back.  A start of that plugin keeps the refusal while
%% such an installation runs, and succeeds once it finished.
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
                %% with the installation gone, the start may take the lock path
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

%% The lock of the plugin is held for the whole fresh install, not only while
%% the package is unpacked: a second installation of the same plugin is refused
%% while the configuration which follows the unpack runs.
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
                erase(configure_refusal),
                ok = meck:new(emqx_plugins_local_config, [passthrough]),
                try
                    ok = meck:expect(emqx_plugins_local_config, copy_default, fun(NV) ->
                        %% `configure' runs after the package was unpacked, and
                        %% the lock has to still be held here.
                        put(configure_refusal, install_in_another_process(NV)),
                        meck:passthrough([NV])
                    end),
                    ?assertEqual(ok, emqx_plugins:ensure_installed(NameVsn, fresh_install)),
                    ?assertMatch(
                        {error, #{reason := installation_in_progress}},
                        get(configure_refusal)
                    )
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

%% The configuration stored for the plugin on this node is validated while the
%% newly unpacked tree can still be rolled back: a configuration which the package
%% can not read would otherwise fail the installation after that point, when the
%% previous tree is already gone.
install_package_rejects_a_config_which_the_package_cannot_read_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "bad_stored_config-0.1.0",
                ok = emqx_plugins:write_package(NameVsn, <<"previous package">>),
                ConfigFile = emqx_plugins_fs:config_file_path(NameVsn),
                ok = write_file(ConfigFile, <<"foo = {{{">>),
                try
                    ?assertMatch(
                        {error, #{msg := "invalid_plugin_config"}},
                        emqx_plugins:install_package(NameVsn, plugin_package(NameVsn, []))
                    ),
                    %% nothing was published, and the package which was there is back
                    ?assertNot(emqx_plugins_fs:is_installed(NameVsn)),
                    ?assertEqual({ok, <<"previous package">>}, emqx_plugins_fs:get_tar(NameVsn)),
                    ?assertEqual({ok, <<"foo = {{{">>}, file:read_file(ConfigFile))
                after
                    _ = file:del_dir_r(filename:dirname(ConfigFile))
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% Only a fresh installation keeps the configuration which is stored on this
%% node: a normal installation replaces it with the configuration of a peer, so
%% a stored configuration which does not fit the package must not keep the
%% repair from happening.
validate_stored_local_config_depends_on_the_install_mode_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "mode_config-0.1.0",
                ConfigFile = emqx_plugins_fs:config_file_path(NameVsn),
                ok = write_file(ConfigFile, <<"foo = {{{">>),
                try
                    ?assertMatch(
                        {error, #{msg := "invalid_plugin_config"}},
                        emqx_plugins:validate_stored_local_config(NameVsn, fresh_install)
                    ),
                    ?assertEqual(
                        ok, emqx_plugins:validate_stored_local_config(NameVsn, normal)
                    )
                after
                    _ = file:del_dir_r(filename:dirname(ConfigFile))
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% A failure after the tree was published keeps the package which was written:
%% the files on disk come from it, and the next attempt repeats the failing step
%% without unpacking the package again.
install_package_keeps_the_written_package_when_finalization_fails_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "keep_written-0.1.0",
                ok = emqx_plugins:write_package(NameVsn, <<"previous package">>),
                Package = plugin_package(NameVsn, []),
                ConfigDir = filename:dirname(emqx_plugins_fs:config_file_path(NameVsn)),
                ok = meck:new(emqx_plugins_local_config, [passthrough]),
                try
                    %% the configuration which follows the publication fails
                    ok = meck:expect(emqx_plugins_local_config, copy_default, fun(_NameVsn) ->
                        {error, eacces}
                    end),
                    ?assertMatch(
                        {error, #{msg := "plugin_installed_but_not_ready"}},
                        emqx_plugins:install_package(NameVsn, Package)
                    ),
                    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
                    ?assertEqual({ok, Package}, emqx_plugins_fs:get_tar(NameVsn))
                after
                    ok = meck:unload(emqx_plugins_local_config),
                    _ = file:del_dir_r(ConfigDir)
                end
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

%% A package which holds no regular file has no plugin to publish, and it is
%% refused before a staging attempt is written.
install_from_local_tar_rejects_a_package_without_files_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "empty-0.1.0",
                ok = emqx_plugins_fs:write_tar(NameVsn, tar_of([])),
                ?assertMatch(
                    {error, #{reason := empty_plugin_package}},
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() ->
                        error(unexpected_validator)
                    end)
                ),
                ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn))
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
                ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn)),
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
                ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% An entry which resolves back to the package root (or to the install root)
%% would be written over a directory: it is refused, and no directory of the
%% install dir is left behind by the attempt.  An entry whose name is not below
%% the package root at all (an absolute one) is refused just the same.
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
                %% an entry which names the package root itself would be the whole
                %% package rather than a file in it
                ok = emqx_plugins_fs:write_tar(NameVsn, tar_of([{NameVsn, <<"replacement">>}])),
                ?assertMatch(
                    {error, #{
                        msg := "unsafe_tar_entry_path", reason := resolves_to_package_root
                    }},
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end)
                ),
                ?assertNot(filelib:is_regular(emqx_plugins_fs:plugin_dir(NameVsn))),
                %% an absolute name addresses a place which is not below the
                %% package root either, and nothing is written there
                Absolute = filename:absname(
                    filename:join(emqx_plugins_fs:install_dir(), "absolute-marker")
                ),
                ok = emqx_plugins_fs:write_tar(NameVsn, tar_of([{Absolute, <<"replacement">>}])),
                ?assertMatch(
                    {error, #{msg := "unsafe_tar_entry_path"}},
                    emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end)
                ),
                ?assertNot(filelib:is_regular(Absolute)),
                ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% Entries which are spelled relative to the package root are resolved to the
%% place they name: a leading `./' and a `..' which stays inside the root are
%% normalized, and a duplicated entry is written twice (the last one wins).  A
%% package which uses them is installed normally, and its files are in the
%% normalized place.
install_from_local_tar_normalizes_entries_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "normalized-1.0",
                AppName = atom_to_list(app_name(NameVsn)),
                EbinRelDir = filename:join([NameVsn, NameVsn, "ebin"]),
                ok = emqx_plugins_fs:write_tar(
                    NameVsn,
                    tar_of([
                        {filename:join(NameVsn, "release.json"), release_json(NameVsn)},
                        {
                            filename:join(EbinRelDir, AppName ++ ".app"),
                            app_file(app_name(NameVsn), app_vsn(NameVsn), [])
                        },
                        {"./" ++ filename:join(NameVsn, "leading.txt"), <<"1">>},
                        {filename:join([NameVsn, "nested", "..", "resolved.txt"]), <<"2">>},
                        {filename:join(NameVsn, "duplicate.txt"), <<"first">>},
                        {filename:join(NameVsn, "duplicate.txt"), <<"second">>}
                    ])
                ),
                ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                Dir = emqx_plugins_fs:plugin_dir(NameVsn),
                ?assertEqual({ok, <<"1">>}, file:read_file(filename:join(Dir, "leading.txt"))),
                ?assertEqual({ok, <<"2">>}, file:read_file(filename:join(Dir, "resolved.txt"))),
                %% the last duplicate is the content of the file
                ?assertEqual(
                    {ok, <<"second">>}, file:read_file(filename:join(Dir, "duplicate.txt"))
                ),
                %% the `..' was resolved instead of creating the directory it names
                ?assertNot(filelib:is_dir(filename:join(Dir, "nested"))),
                ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn))
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
                ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn)),
                ?assertEqual([], filelib:wildcard(filename:join(staging_root(), "*"))),
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
                ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn))
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
                ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn))
            end
        )
    after
        unmeck_emqx()
    end.

%% When the validator refuses the tree and the previous tree can not be put
%% back, the refused tree is still published: the upload path keeps the package
%% it wrote and reports the installation as not ready.
install_package_keeps_the_written_package_when_the_rollback_fails_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "unrestored_upload-0.1.0",
                Package = plugin_package(NameVsn, []),
                ok = meck:new(emqx_plugins_fs, [passthrough]),
                try
                    ok = meck:expect(
                        emqx_plugins_fs,
                        ensure_installed_from_tar,
                        fun(_NameVsn, _Validator) ->
                            {error, #{
                                msg => "failed_to_rollback_plugin_install",
                                rollback => not_restored
                            }}
                        end
                    ),
                    ?assertMatch(
                        {error, #{
                            msg := "plugin_installed_but_not_ready",
                            reason := installation_not_ready
                        }},
                        emqx_plugins:install_package(NameVsn, Package)
                    ),
                    ?assertEqual({ok, Package}, emqx_plugins_fs:get_tar(NameVsn))
                after
                    ok = meck:unload(emqx_plugins_fs)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% The same through the real validator: it crashes, the refused tree can not be
%% removed either, and the package is kept while the original crash is raised
%% again.
install_package_keeps_the_written_package_when_the_validation_crashes_and_the_rollback_fails_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "unrestored_crash-0.1.0",
                Target = emqx_plugins_fs:plugin_dir(NameVsn),
                Package = plugin_package(NameVsn, []),
                ok = meck:new(emqx_plugins_apps, [passthrough]),
                ok = meck:new(file, [passthrough, unstick]),
                try
                    ok = meck:expect(emqx_plugins_apps, validate, fun(_, _) ->
                        error(boom)
                    end),
                    ok = meck:expect(file, del_dir_r, fun(Path) ->
                        case Path =:= Target of
                            true -> {error, eacces};
                            false -> meck:passthrough([Path])
                        end
                    end),
                    ?assertException(
                        error, boom, emqx_plugins:install_package(NameVsn, Package)
                    ),
                    ?assertEqual({ok, Package}, emqx_plugins_fs:get_tar(NameVsn))
                after
                    ok = meck:unload(file),
                    ok = meck:unload(emqx_plugins_apps)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% The validator's refusal with a rollback which did not happen is reported by
%% the filesystem layer with the tree state, so that the caller can keep the
%% package it wrote.
install_from_local_tar_reports_an_unrestored_tree_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "unrestored-0.1.0",
                Target = emqx_plugins_fs:plugin_dir(NameVsn),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ok = meck:new(file, [passthrough, unstick]),
                try
                    ok = meck:expect(file, del_dir_r, fun(Path) ->
                        case Path =:= Target of
                            true -> {error, eacces};
                            false -> meck:passthrough([Path])
                        end
                    end),
                    ?assertMatch(
                        {error, #{
                            msg := "failed_to_rollback_plugin_install",
                            rollback := not_restored
                        }},
                        emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() ->
                            {error, #{msg => "boom"}}
                        end)
                    ),
                    ?assert(filelib:is_dir(Target))
                after
                    ok = meck:unload(file)
                end
            end
        )
    after
        unmeck_emqx()
    end.

%% A validator crash with a rollback which did not happen is raised again
%% together with the tree state, instead of losing the state.
install_from_local_tar_reports_an_unrestored_tree_after_a_crash_test() ->
    meck_emqx(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "unrestored_crash-0.1.0",
                Target = emqx_plugins_fs:plugin_dir(NameVsn),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ok = meck:new(file, [passthrough, unstick]),
                try
                    ok = meck:expect(file, del_dir_r, fun(Path) ->
                        case Path =:= Target of
                            true -> {error, eacces};
                            false -> meck:passthrough([Path])
                        end
                    end),
                    ?assertException(
                        error,
                        {tree_not_restored, error, boom, #{
                            msg := "failed_to_rollback_plugin_install",
                            rollback := not_restored
                        }},
                        emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() ->
                            error(boom)
                        end)
                    ),
                    ?assert(filelib:is_dir(Target))
                after
                    ok = meck:unload(file)
                end
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
                ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn))
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
                StaleAttempt = filename:join(staging_root(), "crashed-attempt"),
                Stale = filename:join(StaleAttempt, NameVsn),
                OtherStale = filename:join([staging_root(), "crashed-other", "other-0.1.0"]),
                ok = write_file(filename:join(Stale, "release.json"), <<"stale">>),
                ok = write_file(filename:join(OtherStale, "release.json"), <<"stale">>),
                ok = emqx_plugins_fs:write_tar(NameVsn, plugin_package(NameVsn, [])),
                ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end),
                ?assert(emqx_plugins_fs:is_extraction_complete(NameVsn)),
                ?assert(filelib:is_dir(Stale)),
                ?assert(filelib:is_dir(OtherStale)),
                ?assertEqual([NameVsn], lists:sort(emqx_plugins_fs:list_name_vsn())),
                ?assertEqual([StaleAttempt], emqx_plugins_test_helpers:staging_attempts(NameVsn)),
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
                    %% `"escape-1.0/"' and `"escape-1.0//"' are refused as well:
                    %% they name the directory of `"escape-1.0"' but are a
                    %% different package file name.  An empty name and the
                    %% staging directory are not a package at all.
                    [
                        "/",
                        <<"/">>,
                        "../escape-1.0",
                        "a/b-1.0",
                        ".",
                        "..",
                        "escape-1.0/",
                        "escape-1.0//",
                        "",
                        <<>>,
                        filename:basename(emqx_plugins_fs:staging_root())
                    ]
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
                ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn))
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
ensure_installed_from_tar_refuses_while_a_published_tree_is_unvalidated_test() ->
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
                    ?assertEqual([], emqx_plugins_test_helpers:staging_attempts(NameVsn))
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
%% The CLI install path is gated by an `allow' grant.  These cases cover what
%% the path does once the gate is open, so the grant is mocked here.
cli_ensure_installed(NameVsn) ->
    LogFun = fun(Fmt, Args) -> iolist_to_binary(io_lib:format(Fmt, Args)) end,
    ok = meck:new(emqx_plugins, [passthrough]),
    try
        ok = meck:expect(emqx_plugins, is_allowed_installation, fun(_NameVsn) -> true end),
        emqx_plugins_cli_utils:ensure_installed(NameVsn, LogFun)
    after
        meck:unload(emqx_plugins)
    end.

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
