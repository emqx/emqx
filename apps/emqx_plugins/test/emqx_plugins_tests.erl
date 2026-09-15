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

meck_emqx() ->
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
    ok.

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
