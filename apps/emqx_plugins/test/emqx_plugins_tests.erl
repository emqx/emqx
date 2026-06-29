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

purge_unconfigured_removes_package_absent_from_cluster_config_test() ->
    meck_emqx(),
    meck_plugins_apps(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                NameVsn = "stale-1",
                ok = emqx_plugins:put_configured([]),
                ok = write_fake_plugin_release(NameVsn),
                ok = write_file(emqx_plugins_fs:tar_file_path(NameVsn), "tar"),

                ?assertMatch({ok, _}, file:read_file_info(emqx_plugins_fs:plugin_dir(NameVsn))),
                ?assertMatch({ok, _}, file:read_file_info(emqx_plugins_fs:tar_file_path(NameVsn))),

                ok = emqx_plugins:purge_unconfigured(),

                ?assertMatch(
                    {error, enoent}, file:read_file_info(emqx_plugins_fs:plugin_dir(NameVsn))
                ),
                ?assertMatch(
                    {error, enoent}, file:read_file_info(emqx_plugins_fs:tar_file_path(NameVsn))
                )
            end
        )
    after
        ok = emqx_plugins:put_configured([]),
        unmeck_plugins_apps(),
        unmeck_emqx()
    end.

purge_unconfigured_keeps_other_versions_of_configured_plugin_test() ->
    meck_emqx(),
    meck_plugins_apps(),
    try
        with_rand_install_dir(
            fun(_Dir) ->
                ok = emqx_plugins:put_configured([#{name_vsn => "demo-1", enable => false}]),
                ok = write_fake_plugin_release("demo-2"),

                ok = emqx_plugins:purge_unconfigured(),

                ?assertMatch({ok, _}, file:read_file_info(emqx_plugins_fs:plugin_dir("demo-2")))
            end
        )
    after
        ok = emqx_plugins:put_configured([]),
        unmeck_plugins_apps(),
        unmeck_emqx()
    end.

write_fake_plugin_release(NameVsn) ->
    {AppName, Vsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    AppNameStr = atom_to_list(AppName),
    write_file(
        emqx_plugins_fs:info_file_path(NameVsn),
        io_lib:format(
            "name=\"~s\", rel_vsn=\"~s\", rel_apps=[\"~s-~s\"], description=\"desc ~s\"",
            [AppNameStr, Vsn, AppNameStr, Vsn, AppNameStr]
        )
    ).

meck_emqx() ->
    meck:new(emqx, [unstick, passthrough]),
    meck:new(emqx_plugins_serde),
    meck:expect(
        emqx,
        update_config,
        fun(Path, Values, _Opts) ->
            emqx_config:put(Path, mock_schema_key_transform(Values))
        end
    ),
    meck:expect(
        emqx_plugins_serde,
        delete_schema,
        fun(_NameVsn) -> ok end
    ),
    ok.

mock_schema_key_transform(List = [#{} | _]) ->
    [mock_schema_key_transform(Item) || Item <- List];
mock_schema_key_transform(Map) when is_map(Map) ->
    maps:fold(
        fun(K, V, Acc) ->
            Acc#{mock_schema_key_transform(K) => mock_schema_key_transform(V)}
        end,
        #{},
        Map
    );
mock_schema_key_transform(<<"name_vsn">>) ->
    name_vsn;
mock_schema_key_transform(<<"enable">>) ->
    enable;
mock_schema_key_transform(Term) ->
    Term.

unmeck_emqx() ->
    meck:unload(emqx),
    meck:unload(emqx_plugins_serde),
    ok.

meck_plugins_apps() ->
    meck:new(emqx_plugins_apps, [passthrough]),
    meck:expect(emqx_plugins_apps, running_status, fun(_NameVsn) -> stopped end),
    meck:expect(emqx_plugins_apps, stop, fun(_Plugin) -> ok end),
    ok.

unmeck_plugins_apps() ->
    meck:unload(emqx_plugins_apps),
    ok.
