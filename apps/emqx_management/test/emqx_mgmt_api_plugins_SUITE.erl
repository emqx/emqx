%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_plugins_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(EMQX_PLUGIN_TEMPLATE_NAME, "my_emqx_plugin").
-define(EMQX_PLUGIN_TEMPLATE_APP_NAME, my_emqx_plugin).
-define(EMQX_PLUGIN_TEMPLATE_VSN, "5.9.0-beta.3").
-define(EMQX_PLUGIN_TEMPLATE_TAG, "5.9.0-beta.3").
-define(EMQX_PLUGIN_TEMPLATE_URL,
    "https://github.com/emqx/emqx-plugin-template/releases/download/"
).
-define(PACKAGE_SUFFIX, ".tar.gz").

-define(EMQX_PLUGIN, #{
    release_name => ?EMQX_PLUGIN_TEMPLATE_NAME,
    app_name => ?EMQX_PLUGIN_TEMPLATE_APP_NAME,
    git_url => ?EMQX_PLUGIN_TEMPLATE_URL,
    vsn => ?EMQX_PLUGIN_TEMPLATE_VSN,
    tag => ?EMQX_PLUGIN_TEMPLATE_TAG
}).

-define(CLUSTER_API_SERVER(PORT), ("http://127.0.0.1:" ++ (integer_to_list(PORT)))).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_plugins,
            {emqx_management, #{
                after_start => fun emqx_mgmt_cli:load/0
            }},
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(t_cluster_update_order = TestCase, Config0) ->
    Config = [{api_port, 18085} | Config0],
    Cluster = [Node1 | _] = cluster(TestCase, Config),
    {ok, API} = init_api(Node1),
    [
        {api, API},
        {cluster, Cluster}
        | Config
    ];
init_per_testcase(t_cluster_rejects_invalid_local_config_on_start = TestCase, Config0) ->
    Config = [{api_port, 28085} | Config0],
    Cluster = [Node1 | _] = cluster(TestCase, Config),
    {ok, API} = init_api(Node1),
    [
        {api, API},
        {cluster, Cluster}
        | Config
    ];
init_per_testcase(t_cluster_rolls_back_partial_start_failure = TestCase, Config0) ->
    Config = [{api_port, 38085} | Config0],
    Cluster = [Node1 | _] = cluster(TestCase, Config),
    {ok, API} = init_api(Node1),
    [
        {api, API},
        {cluster, Cluster}
        | Config
    ];
init_per_testcase(TestCase, Config) ->
    ToInstallDir = filename:join(emqx_cth_suite:work_dir(TestCase, Config), "emqx_plugins"),
    emqx_plugins:put_config_internal(install_dir, ToInstallDir),
    Config.

end_per_testcase(TestCase, Config) when
    TestCase =:= t_cluster_update_order;
    TestCase =:= t_cluster_rejects_invalid_local_config_on_start;
    TestCase =:= t_cluster_rolls_back_partial_start_failure
->
    Cluster = ?config(cluster, Config),
    emqx_cth_cluster:stop(Cluster),
    end_per_testcase(common, Config);
end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

t_sync_plugin_validates_name(Config) ->
    WorkDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
    Kept = filename:join(WorkDir, "kept-1"),
    Marker = filename:join(Kept, "marker"),
    ok = filelib:ensure_dir(Marker),
    ok = file:write_file(Marker, <<"original">>),
    Path = emqx_mgmt_api_test_util:api_path(["plugins", "cluster_sync"]),
    Names = [
        <<"../kept-1">>,
        list_to_binary(filename:absname(Kept)),
        <<"valid-1/../../kept-1">>,
        <<"..-1">>,
        <<".-1">>,
        <<"-1">>,
        <<"plugin-1\n">>,
        <<"plugin-1", 0>>,
        <<"plugin-1\\child">>,
        <<".emqx-plugin-staging">>,
        <<>>,
        123,
        [],
        #{},
        <<"p-", (binary:copy(<<"a">>, 255))/binary>>
    ],
    lists:foreach(
        fun(Name) ->
            ?assertMatch(
                {error, {_, 400, _}},
                emqx_mgmt_api_test_util:request_api(
                    post, Path, "", [], #{<<"name">> => Name}
                )
            ),
            ?assertEqual({ok, <<"original">>}, file:read_file(Marker))
        end,
        Names
    ),
    ?assertMatch(
        {error, {_, 400, _}}, emqx_mgmt_api_test_util:request_api(post, Path, "", [], #{})
    ),
    ?assertEqual({ok, <<"original">>}, file:read_file(Marker)),
    ?assertEqual([], emqx_plugins_fs:list_name_vsn()),
    ?assertMatch(
        {error, {_, 404, _}},
        emqx_mgmt_api_test_util:request_api(
            post, Path, "", [], #{<<"name">> => <<"missing_sync_plugin-1">>}
        )
    ).

t_install_callback_validates_name(_Config) ->
    ?assertMatch(
        {error, #{msg := "bad_plugin_package_name"}},
        emqx_mgmt_api_plugins:install_package_v4(<<"../plugin-1">>, <<>>)
    ).

t_sync_plugin_keeps_name_textual(_Config) ->
    Name = <<"sync_candidate_", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    NameVsn = <<Name/binary, "-1.0">>,
    ?assertError(badarg, binary_to_existing_atom(Name, utf8)),
    Path = emqx_mgmt_api_test_util:api_path(["plugins", "cluster_sync"]),
    ?assertMatch(
        {error, {_, 404, _}},
        emqx_mgmt_api_test_util:request_api(
            post, Path, "", [], #{<<"name">> => NameVsn}
        )
    ),
    ?assertError(badarg, binary_to_existing_atom(Name, utf8)).

t_plugins(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    %% Must allow via CLI first.
    ?assertMatch({ok, {{_, 403, _}, _, _}}, install_plugin(PackagePath)),
    ok = allow_installation(NameVsn),
    %% Test disallow
    ok = disallow_installation(NameVsn),
    ?assertMatch({ok, {{_, 403, _}, _, _}}, install_plugin(PackagePath)),
    %% Now really allow it.
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    Node = atom_to_binary(node()),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"status">> := <<"stopped">>}
            ]
        },
        describe_plugin(NameVsn)
    ),
    {ok, StopRes1} = update_plugin(NameVsn, "start"),
    ?assertEqual([], StopRes1),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"status">> := <<"running">>}
            ]
        },
        describe_plugin(NameVsn)
    ),
    {ok, []} = update_plugin(NameVsn, "stop"),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"status">> := <<"stopped">>}
            ]
        },
        describe_plugin(NameVsn)
    ),
    {ok, []} = uninstall_plugin(NameVsn),
    %% Should forget that we allowed installation after uninstall
    ?assertMatch({ok, {{_, 403, _}, _, _}}, install_plugin(PackagePath)),
    ok.

-doc """
`GET /plugins` tolerates unreachable nodes and remote crashes in the
cluster-wide plugin listing RPC instead of crashing with badmatch.
""".
t_list_plugins_rpc_failure(_Config) ->
    ok = meck:new(emqx_mgmt_api_plugins_proto_v4, [passthrough]),
    try
        meck:expect(
            emqx_mgmt_api_plugins_proto_v4,
            get_plugins,
            fun(_Nodes) ->
                {[{node(), []}, {badrpc, {'EXIT', boom}}], ['badnode@nohost']}
            end
        ),
        ?assertEqual([], list_plugins())
    after
        meck:unload(emqx_mgmt_api_plugins_proto_v4)
    end.

t_install_plugin_sha256_match(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    {ok, Bin} = file:read_file(PackagePath),
    Sha = binary_to_list(binary:encode_hex(crypto:hash(sha256, Bin), lowercase)),
    ok = allow_installation(NameVsn, Sha),
    ok = install_plugin(PackagePath),
    ?assertMatch(#{<<"name">> := <<"my_emqx_plugin">>}, describe_plugin(NameVsn)),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

t_install_plugin_sha256_mismatch(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    %% Bind a hash that intentionally doesn't match the file bytes.
    BogusSha = string:copies("a", 64),
    ok = allow_installation(NameVsn, BogusSha),
    ?assertMatch({ok, {{_, 403, _}, _, _}}, install_plugin(PackagePath)),
    %% Clean up the leftover allow entry so other cases aren't affected.
    ok = disallow_installation(NameVsn),
    ok.

t_install_plugin_with_invalid_schema_returns_bad_plugin_info(Config) ->
    PackagePath = make_test_plugin_package(
        Config,
        <<"not an avro schema">>,
        <<"foo = \"bar\"\n">>
    ),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    on_exit(fun() ->
        _ = disallow_installation(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    {ok, {{"HTTP/1.1", 400, "Bad Request"}, _, Body}} = install_plugin(PackagePath),
    #{<<"code">> := <<"BAD_PLUGIN_INFO">>, <<"message">> := Message} =
        emqx_utils_json:decode(Body),
    ?assertNotEqual(nomatch, binary:match(Message, <<"invalid_plugin_config_schema">>)),
    ?assertNotEqual(nomatch, binary:match(Message, <<"Rebuild or reinstall">>)),
    %% the package root was right and the semantic validation failed, so the
    %% rejected attempt left neither the plugin directory nor a staging
    %% directory behind
    ?assertNot(filelib:is_dir(emqx_plugins_fs:plugin_dir(NameVsn))),
    ?assertEqual([], staging_attempts(NameVsn)).

%% A package whose entries are not all below its own name-vsn must not write
%% (or delete) the files of another plugin: it is refused before a single byte
%% is written, and the other installation is untouched.
t_install_rejects_sibling_entries(Config) ->
    Victim = "invalid_plugin-1.0.0",
    VictimPackage = make_test_plugin_package(
        Config, "1.0.0", "invalid_plugin", "0.1.0", test_plugin_schema(), <<"foo = \"bar\"\n">>
    ),
    ok = emqx_plugins:ensure_uninstalled(Victim),
    ok = emqx_plugins:delete_package(Victim),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(Victim),
        _ = emqx_plugins:ensure_uninstalled(Victim),
        _ = emqx_plugins:delete_package(Victim),
        _ = disallow_installation(Victim)
    end),
    ok = allow_installation(Victim),
    ok = install_plugin(VictimPackage),
    ?assertEqual(installed, emqx_plugins:install_state(Victim)),
    VictimFiles = plugin_dir_files(Victim),

    %% The attacker package is a valid package of `invalid_plugin-2.0.0' which
    %% additionally carries entries of the plugin installed above.
    NameVsn = "invalid_plugin-2.0.0",
    AttackerPackage = make_test_plugin_package(
        Config,
        "2.0.0",
        "invalid_plugin",
        "0.1.0",
        test_plugin_schema(),
        <<"foo = \"bar\"\n">>,
        iolist_to_binary(io_lib:format("{application, invalid_plugin, [{vsn, \"0.1.0\"}]}.\n", [])),
        [
            {filename:join(Victim, "release.json"), <<"pwned">>},
            {filename:join([Victim, "invalid_plugin-0.1.0", "ebin", "invalid_plugin.app"]), <<
                "pwned"
            >>}
        ]
    ),
    on_exit(fun() ->
        _ = disallow_installation(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    {ok, {{"HTTP/1.1", 400, "Bad Request"}, _, Body}} = install_plugin(AttackerPackage),
    #{<<"code">> := <<"BAD_PLUGIN_INFO">>} = emqx_utils_json:decode(Body),
    %% the victim is untouched, byte for byte, and still installed
    ?assertEqual(VictimFiles, plugin_dir_files(Victim)),
    ?assertEqual(installed, emqx_plugins:install_state(Victim)),
    %% nothing of the refused package was written anywhere
    ?assertEqual(absent, emqx_plugins:install_state(NameVsn)),
    ?assertNot(filelib:is_dir(emqx_plugins_fs:plugin_dir(NameVsn))),
    ?assertEqual([], staging_attempts(NameVsn)).

%% A failed replacement must not destroy the package the installation can be
%% repaired from: the upload is refused and the previous package file (and its
%% checksum) are put back.
t_install_failure_keeps_previous_package_and_files(Config) ->
    NameVsn = "invalid_plugin-1.0.0",
    InstalledPackage = make_test_plugin_package(
        Config, "1.0.0", "invalid_plugin", "0.1.0", test_plugin_schema(), <<"foo = \"bar\"\n">>
    ),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    ok = install_plugin(InstalledPackage),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    TarFile = emqx_plugins_fs:tar_file_path(NameVsn),
    {ok, InstalledTar} = file:read_file(TarFile),
    {ok, InstalledChecksum} = file:read_file(TarFile ++ ".md5sum"),

    %% a complete installation is not replaced by an upload, so degrade it to
    %% the leftovers an interrupted unpack would leave
    ok = emqx_plugins_test_helpers:delete_plugin_app_dirs(NameVsn),
    ?assertEqual(incomplete, emqx_plugins:install_state(NameVsn)),

    %% the uploaded package has the right package root but an invalid schema
    RefusedPackage = make_test_plugin_package(
        Config,
        "1.0.0",
        "invalid_plugin",
        "0.1.0",
        <<"not an avro schema">>,
        <<"foo = \"bar\"\n">>
    ),
    %% a successful install revokes the allow entry, so the package has to be
    %% authorized again for the refused attempt
    ok = allow_installation(NameVsn),
    {ok, {{"HTTP/1.1", 400, "Bad Request"}, _, Body}} = install_plugin(RefusedPackage),
    #{<<"code">> := <<"BAD_PLUGIN_INFO">>} = emqx_utils_json:decode(Body),
    %% the previous package is back, so the installation can be repaired from it
    ?assertEqual({ok, InstalledTar}, file:read_file(TarFile)),
    ?assertEqual({ok, InstalledChecksum}, file:read_file(TarFile ++ ".md5sum")),
    ?assertEqual([], staging_attempts(NameVsn)).

t_install_plugin_with_bad_app_file_returns_consistent_error(Config) ->
    PackagePath = make_test_plugin_package(
        Config,
        test_plugin_schema(),
        <<"foo = \"bar\"\n">>,
        <<"not an Erlang application resource file">>
    ),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    on_exit(fun() ->
        _ = disallow_installation(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    {ok, {{"HTTP/1.1", 400, "Bad Request"}, _, Body}} = install_plugin(PackagePath),
    ?assertEqual(
        #{
            <<"code">> => <<"BAD_PLUGIN_INFO">>,
            <<"message">> =>
                iolist_to_binary([
                    "node ",
                    atom_to_binary(node()),
                    ": bad_plugin_app_file: Plugin package metadata is invalid or unreadable. ",
                    "Rebuild or reinstall a corrected plugin package and retry."
                ])
        },
        emqx_utils_json:decode(Body)
    ).

t_start_preflight_package_error_returns_400(Config) ->
    NameVsn = install_test_plugin(Config),
    [SchemaPath] = filelib:wildcard(
        filename:join([
            emqx_plugins_fs:plugin_dir(NameVsn),
            "*",
            "priv",
            "config_schema.avsc"
        ])
    ),
    ok = file:write_file(SchemaPath, <<"not an avro schema">>),
    {400, Response} = request_plugin_action(NameVsn, "start"),
    ?assertEqual(<<"PARAM_ERROR">>, maps:get(<<"code">>, Response)),
    ?assertNotEqual(
        nomatch,
        binary:match(maps:get(<<"message">>, Response), <<"invalid_plugin_config_schema:">>)
    ).

t_uninstall_conflicting_version_keeps_old_running(Config) ->
    OldPackagePath = make_test_plugin_package(
        Config,
        "1.0.0",
        "invalid_plugin_v1",
        "0.1.0",
        test_plugin_schema(),
        <<"foo = \"old\"\n">>
    ),
    NewPackagePath = make_test_plugin_package(
        Config,
        "2.0.0",
        "invalid_plugin_v2",
        "0.2.0",
        test_plugin_schema(),
        <<"foo = \"new\"\n">>
    ),
    OldNameVsn = filename:basename(OldPackagePath, ?PACKAGE_SUFFIX),
    NewNameVsn = filename:basename(NewPackagePath, ?PACKAGE_SUFFIX),
    on_exit(fun() ->
        lists:foreach(
            fun(NameVsn) ->
                _ = emqx_plugins:ensure_stopped(NameVsn),
                _ = emqx_plugins:ensure_disabled(NameVsn),
                _ = emqx_plugins:ensure_uninstalled(NameVsn),
                _ = emqx_plugins:delete_package(NameVsn)
            end,
            [OldNameVsn, NewNameVsn]
        )
    end),
    ok = allow_installation(OldNameVsn),
    ok = allow_installation(NewNameVsn),
    ok = install_plugin(OldPackagePath),
    ok = install_plugin(NewPackagePath),
    {ok, []} = update_plugin(OldNameVsn, "start"),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"status">> := <<"running">>}
            ]
        },
        describe_plugin(OldNameVsn)
    ),
    {400, ConflictResponse} = request_plugin_action(NewNameVsn, "start"),
    ?assertEqual(<<"PARAM_ERROR">>, maps:get(<<"code">>, ConflictResponse)),
    ?assertNotEqual(
        nomatch,
        binary:match(
            maps:get(<<"message">>, ConflictResponse),
            <<"conflicting_plugin_version_running:">>
        )
    ),
    {ok, []} = uninstall_plugin(NewNameVsn),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"status">> := <<"running">>}
            ]
        },
        describe_plugin(OldNameVsn)
    ).

t_update_config(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    OldConfig = emqx_plugins:get_config(NameVsn),
    %% Check config update when plugin is not started
    ?assertMatch(
        {ok, 400, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"bad.host">>})
    ),
    ?assertMatch(
        {ok, 204, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"localhost">>})
    ),
    %% Check config update when plugin is started
    {ok, _} = update_plugin(NameVsn, "start"),
    ?assertMatch(
        {ok, 400, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"bad.host">>})
    ),
    ?assertMatch(
        {ok, 204, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"localhost">>})
    ),
    {ok, []} = update_plugin(NameVsn, "stop"),
    %% Check config update when plugin is stopped
    ?assertMatch(
        {ok, 400, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"bad.host">>})
    ),
    ?assertMatch(
        {ok, 204, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"localhost">>})
    ),
    %% Clean up
    {ok, []} = uninstall_plugin(NameVsn).

t_update_config_with_invalid_type_returns_readable_error(Config0) ->
    PackagePath = make_test_plugin_package(
        Config0,
        test_plugin_schema(),
        <<"foo = \"bar\"\n">>
    ),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_disabled(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    PluginConfig = emqx_plugins:get_config(NameVsn),
    {ok, 400, Body} = update_plugin_config(NameVsn, PluginConfig#{<<"foo">> => 42}),
    ?assertEqual(
        #{
            <<"code">> => <<"BAD_CONFIG">>,
            <<"message">> =>
                <<"invalid_type: Invalid type for field 'foo': expected string, got integer">>
        },
        emqx_utils_json:decode(Body)
    ).

t_update_config_with_invalid_root_type_returns_readable_error(Config) ->
    NameVsn = install_test_plugin(Config),
    {ok, 400, Body} = update_plugin_config(NameVsn, 42),
    ?assertEqual(
        #{
            <<"code">> => <<"BAD_CONFIG">>,
            <<"message">> =>
                <<"invalid_type: Invalid type for field '$': expected invalid_plugin, got integer">>
        },
        emqx_utils_json:decode(Body)
    ).

t_upload_download_config(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    DownloadPath = emqx_mgmt_api_test_util:api_path(["plugins", NameVsn, "config", "download"]),
    ?assertMatch(
        {ok, _},
        emqx_mgmt_api_test_util:request_api(get, DownloadPath)
    ),
    OldConfig = emqx_plugins:get_config(NameVsn),
    JSONData = emqx_utils_json:encode(OldConfig),
    UploadPath = emqx_mgmt_api_test_util:api_path(["plugins", NameVsn, "config", "upload"]),
    ?assertMatch(
        {ok, 204, _},
        emqx_dashboard_api_test_helpers:multipart_formdata_request(UploadPath, [], [
            {config, "config.json", JSONData}
        ])
    ),
    %% Clean up
    {ok, []} = uninstall_plugin(NameVsn).

t_health_status(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    OldConfig = emqx_plugins:get_config(NameVsn),
    Node = atom_to_binary(node()),
    %% No health status for stopped plugin
    ?assertNotMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"health_status">> := _}
            ]
        },
        describe_plugin(NameVsn)
    ),
    %% Check health status when plugin is started
    {ok, _} = update_plugin(NameVsn, "start"),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"health_status">> := #{<<"status">> := <<"ok">>}}
            ]
        },
        describe_plugin(NameVsn)
    ),
    %% Check health status in /plugins
    ?assertMatch(
        [
            #{
                <<"running_status">> := [
                    #{<<"node">> := Node, <<"health_status">> := #{<<"status">> := <<"ok">>}}
                ]
            }
        ],
        list_plugins()
    ),
    %% Change config to make plugin unhealthy. Unhealthines for ports other than
    %% 3306 is baked in the plugin.
    ?assertMatch(
        {ok, 204, _},
        update_plugin_config(NameVsn, OldConfig#{<<"port">> => 3307})
    ),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"health_status">> := #{<<"status">> := <<"error">>}}
            ]
        },
        describe_plugin(NameVsn)
    ),
    {ok, []} = update_plugin(NameVsn, "stop"),
    %% Check health status when plugin is stopped
    ?assertNotMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"health_status">> := _}
            ]
        },
        describe_plugin(NameVsn)
    ),
    %% Clean up
    {ok, []} = uninstall_plugin(NameVsn).

%% A plugin package whose tarball is present but which has never been unpacked
%% (copied by hand, or left behind by an install that was refused) is not an
%% installation.  Uploading it must not short-circuit to ALREADY_INSTALLED
%% before the allow gate: without a grant the upload is rejected with the
%% actionable 403, and with a grant it installs.
t_install_orphan_package_requires_allow(Config) ->
    PackagePath = make_test_plugin_package(Config, test_plugin_schema(), <<"foo = \"bar\"\n">>),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    %% Simulate the orphan tarball: bytes present, nothing unpacked.
    {ok, Bin} = file:read_file(PackagePath),
    ok = emqx_plugins:write_package(NameVsn, Bin),
    ?assertEqual(absent, emqx_plugins:install_state(NameVsn)),
    ?assertMatch({true, [_]}, emqx_plugins:is_package_present(NameVsn)),
    %% Not allowed yet: the upload must ask for `plugins allow', not claim
    %% that the package is already installed.
    {ok, {{_, 403, _}, _, Body}} = install_plugin(PackagePath),
    #{<<"code">> := <<"FORBIDDEN">>, <<"message">> := Msg} = emqx_utils_json:decode(Body),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"plugins allow">>)),
    %% The rejected upload left no installation behind, and did not consume
    %% the orphan tarball.
    ?assertEqual(absent, emqx_plugins:install_state(NameVsn)),
    ?assertMatch({true, [_]}, emqx_plugins:is_package_present(NameVsn)),
    %% Once allowed, the very same upload installs the package.
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    ?assertMatch(#{<<"name">> := <<"invalid_plugin">>}, describe_plugin(NameVsn)),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

%% A failure which is not the package's fault is a server error: when the
%% cluster wide installation lock can not be acquired, the upload is refused
%% with a 500 instead of a 400 which would tell the caller to fix the package.
t_install_lock_unavailable_is_a_server_error(Config) ->
    PackagePath = make_test_plugin_package(Config, test_plugin_schema(), <<"foo = \"bar\"\n">>),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    ok = meck:new(emqx_plugins_install_serializer, [passthrough]),
    try
        ok = meck:expect(emqx_plugins_install_serializer, run, fun(_NameVsn, _Fun) ->
            {error, #{
                msg => "failed_to_acquire_plugin_install_lock",
                reason => coordinator_unknown
            }}
        end),
        {ok, {{"HTTP/1.1", 500, _}, _, Body}} = install_plugin(PackagePath),
        #{<<"code">> := <<"INTERNAL_ERROR">>} = emqx_utils_json:decode(Body),
        %% Nothing was installed, and the upload left no package behind.
        ?assertEqual(absent, emqx_plugins:install_state(NameVsn)),
        ?assertEqual(false, emqx_plugins:is_package_present(NameVsn))
    after
        ok = meck:unload(emqx_plugins_install_serializer)
    end,
    ok.

%% A complete installation is still an installation: uploading the same package
%% again is refused with ALREADY_INSTALLED and the installed files are kept.
t_install_over_complete_install_dir(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    %% the applications declared by the package have all been unpacked
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    AppFiles = emqx_plugins_test_helpers:plugin_app_files(NameVsn),
    ?assertMatch([_ | _], AppFiles),
    {ok, {{_, 400, _}, _, Body}} = install_plugin(PackagePath),
    #{<<"code">> := <<"ALREADY_INSTALLED">>} = emqx_utils_json:decode(Body),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    emqx_plugins_test_helpers:assert_files_exist(AppFiles),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

%% An unpack can stop right after `release.json' has been written, so the
%% directory holds a readable manifest without the applications it declares.
%% That is not an installation: the upload must go through the allow gate and
%% unpack the package instead of replying ALREADY_INSTALLED.
t_install_over_manifest_only_install_dir(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    %% keep `release.json' only, as if the unpack had been interrupted there
    ok = erl_tar:extract(PackagePath, [compressed, {cwd, emqx_plugins_fs:install_dir()}]),
    ok = emqx_plugins_test_helpers:delete_plugin_app_dirs(NameVsn),
    ?assertMatch({ok, _}, emqx_plugins:describe(NameVsn, #{})),
    ?assertEqual([], emqx_plugins_test_helpers:plugin_app_files(NameVsn)),
    %% Not allowed yet: the upload must ask for `plugins allow', not claim
    %% that the package is already installed.
    {ok, {{_, 403, _}, _, Body}} = install_plugin(PackagePath),
    #{<<"code">> := <<"FORBIDDEN">>, <<"message">> := Msg} = emqx_utils_json:decode(Body),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"plugins allow">>)),
    %% Once allowed, the very same upload unpacks the package.
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ?assertMatch(#{<<"name">> := <<"my_emqx_plugin">>}, describe_plugin(NameVsn)),
    ?assertMatch([_ | _], emqx_plugins_test_helpers:plugin_app_files(NameVsn)),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

%% The files of a plugin whose applications are running must not be replaced by
%% an upload, even when its metadata has become unreadable: the upload is
%% refused and the running plugin is left untouched.
t_install_running_plugin_broken_metadata(Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        %% restore the metadata, otherwise the plugin can not be stopped
        _ = emqx_plugins_test_helpers:restore_info_file_from_package(PackagePath, NameVsn),
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assert(plugin_is_running(NameVsn)),
    AppFiles = emqx_plugins_test_helpers:plugin_app_files(NameVsn),
    ?assertMatch([_ | _], AppFiles),
    %% the metadata of the running plugin becomes unreadable
    ok = file:write_file(emqx_plugins_fs:info_file_path(NameVsn), <<"not json">>),
    ?assertMatch({error, _}, emqx_plugins:describe(NameVsn, #{})),
    %% the package that is installed now
    TarFile = emqx_plugins_fs:tar_file_path(NameVsn),
    {ok, InstalledPackage} = file:read_file(TarFile),
    {ok, InstalledChecksum} = file:read_file(TarFile ++ ".md5sum"),
    ok = allow_installation(NameVsn),
    %% an upload which is refused must not destroy it: it may be the only
    %% local copy the broken installation can be repaired from
    RefusedPackage = create_modified_package(Config, PackagePath),
    {ok, {{_, 400, _}, _, Body}} = install_plugin(RefusedPackage),
    #{<<"code">> := <<"BAD_PLUGIN_INFO">>, <<"message">> := Msg} = emqx_utils_json:decode(Body),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"plugin_is_in_use">>)),
    %% the message tells the user how to get out of the situation
    ?assertNotEqual(nomatch, binary:match(Msg, <<"emqx ctl plugins stop">>)),
    ?assertEqual({ok, InstalledPackage}, file:read_file(TarFile)),
    ?assertEqual({ok, InstalledChecksum}, file:read_file(TarFile ++ ".md5sum")),
    %% the running plugin is still running, from the same files
    ?assert(plugin_is_running(NameVsn)),
    emqx_plugins_test_helpers:assert_files_exist(AppFiles),
    %% the plugin can be stopped even though its metadata is unreadable, and
    %% the upload then replaces the broken installation
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ?assertNot(plugin_is_running(NameVsn)),
    ok = install_plugin(PackagePath),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    ok.

%% The package of the installation which is about to be replaced can not be
%% read, so the snapshot a failed attempt would be recovered from can not be
%% taken: the upload has to be refused before it overwrites the package.  The
%% checksum file is a directory here, which can not be read as a file whatever
%% the user the tests run as.
t_install_refuses_unreadable_installed_package(Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    TarFile = emqx_plugins_fs:tar_file_path(NameVsn),
    ChecksumFile = TarFile ++ ".md5sum",
    {ok, InstalledPackage} = file:read_file(TarFile),
    %% keep the manifest only, as if the unpack had stopped there: the state
    %% is then incomplete, so the upload goes through the allow gate and
    %% reaches the node callback which replaces the package
    ok = emqx_plugins_test_helpers:delete_plugin_app_dirs(NameVsn),
    ?assertEqual(incomplete, emqx_plugins:install_state(NameVsn)),
    ok = file:delete(ChecksumFile),
    ok = file:make_dir(ChecksumFile),
    ok = allow_installation(NameVsn),
    {ok, {{_, 400, _}, _, Body}} = install_plugin(create_modified_package(Config, PackagePath)),
    #{<<"code">> := <<"BAD_PLUGIN_INFO">>, <<"message">> := Msg} = emqx_utils_json:decode(Body),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"failed_to_backup_plugin_package">>)),
    %% the message tells the user what to do
    ?assertNotEqual(
        nomatch, binary:match(Msg, <<"permissions of the plugins install directory">>)
    ),
    %% the package which could not be snapshotted is untouched
    ?assertEqual({ok, InstalledPackage}, file:read_file(TarFile)),
    ?assert(filelib:is_dir(ChecksumFile)),
    ok.

%% A node where the installation is complete does not unpack the upload
%% (`ensure_installed_from_tar/2' returns without touching the package), so
%% replacing the package would leave the uploaded bytes next to the files of
%% the previous one.  A cluster install runs the node callback on every node,
%% including the complete ones.
t_install_on_complete_installation_keeps_package(Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    TarFile = emqx_plugins_fs:tar_file_path(NameVsn),
    {ok, InstalledPackage} = file:read_file(TarFile),
    {ok, InstalledChecksum} = file:read_file(TarFile ++ ".md5sum"),
    AppFiles = emqx_plugins_test_helpers:plugin_app_files(NameVsn),
    ?assertMatch([_ | _], AppFiles),
    %% the same name-vsn with different bytes, as an upload which repairs
    %% another node of the cluster carries
    {ok, OtherPackage} = file:read_file(create_modified_package(Config, PackagePath)),
    ?assertNotEqual(InstalledPackage, OtherPackage),
    %% this is what the cluster install runs on this node
    ok = emqx_mgmt_api_plugins:install_package_v4(NameVsn, OtherPackage),
    %% the package still matches the files which are installed
    ?assertEqual({ok, InstalledPackage}, file:read_file(TarFile)),
    ?assertEqual({ok, InstalledChecksum}, file:read_file(TarFile ++ ".md5sum")),
    emqx_plugins_test_helpers:assert_files_exist(AppFiles),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

%% A complete installation is not a leftover to be replaced just because an
%% application of the same name is loaded from another version: the CLI reports
%% it as installed instead of purging it and failing for the vanished package.
t_install_state_ignores_other_version_running(Config) ->
    Schema = test_plugin_schema(),
    DefaultConfig = <<"foo = \"bar\"\n">>,
    Vsn1 = "1.0.0",
    Vsn2 = "2.0.0",
    Pkg1 = make_test_plugin_package(Config, Vsn1, "invalid_plugin", Vsn1, Schema, DefaultConfig),
    Pkg2 = make_test_plugin_package(Config, Vsn2, "invalid_plugin", Vsn2, Schema, DefaultConfig),
    NameVsn1 = "invalid_plugin-" ++ Vsn1,
    NameVsn2 = "invalid_plugin-" ++ Vsn2,
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn1),
        _ = emqx_plugins:ensure_uninstalled(NameVsn1),
        _ = emqx_plugins:ensure_uninstalled(NameVsn2),
        _ = emqx_plugins:delete_package(NameVsn1),
        _ = emqx_plugins:delete_package(NameVsn2),
        _ = disallow_installation(NameVsn1),
        _ = disallow_installation(NameVsn2)
    end),
    %% two complete installations, as a hand-made upgrade of a plugin leaves
    %% them behind
    ok = unpack_package(Pkg1),
    ok = unpack_package(Pkg2),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn1)),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn2)),
    %% version 1 is loaded and running, version 2 is complete on disk
    ok = emqx_plugins:ensure_started(NameVsn1),
    ?assert(plugin_is_running(NameVsn1)),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn2)),
    AppFiles = emqx_plugins_test_helpers:plugin_app_files(NameVsn2),
    ?assertMatch([_ | _], AppFiles),
    %% there is no local package left to unpack version 2 from: the pre-fix
    %% code purged the complete installation and then failed with
    %% `plugin_tarball_not_found'
    ok = emqx_plugins:delete_package(NameVsn2),
    Output = cli_ensure_installed(NameVsn2),
    ?assertNotEqual(nomatch, binary:match(Output, <<"plugin_already_installed">>)),
    %% the complete installation has been kept
    emqx_plugins_test_helpers:assert_files_exist(AppFiles),
    ok.

%% Simulate an installation that was unpacked by hand instead of by the
%% install code.
unpack_package(PackagePath) ->
    erl_tar:extract(PackagePath, [compressed, {cwd, emqx_plugins_fs:install_dir()}]).

t_install_plugin_matching_exisiting_name(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    NameVsn1 = ?EMQX_PLUGIN_TEMPLATE_NAME ++ "_a" ++ "-" ++ ?EMQX_PLUGIN_TEMPLATE_VSN,
    PackagePath1 = create_renamed_package(PackagePath, NameVsn1),
    NameVsn1 = filename:basename(PackagePath1, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn1),
    ok = emqx_plugins:delete_package(NameVsn1),
    %% First, install plugin "emqx_plugin_template_a", then:
    %% "emqx_plugin_template" which matches the beginning
    %% of the previously installed plugin name
    ok = allow_installation(NameVsn),
    ok = allow_installation(NameVsn1),
    ok = install_plugin(PackagePath1),
    ok = install_plugin(PackagePath),
    _ = describe_plugin(NameVsn),
    _ = describe_plugin(NameVsn1),
    {ok, _} = update_plugin(NameVsn1, "start"),
    {ok, _} = update_plugin(NameVsn, "start"),
    _ = uninstall_plugin(NameVsn),
    _ = uninstall_plugin(NameVsn1).

t_install_plugin_with_bad_form_data(_Config) ->
    AuthHeader = emqx_common_test_http:default_auth_header(),
    Path = emqx_mgmt_api_test_util:api_path(["plugins", "install"]),
    {error, {"HTTP/1.1", 400, "Bad Request"}} = emqx_mgmt_api_test_util:request_api(
        post, Path, "", AuthHeader, #{}
    ).

t_bad_plugin(_Config) ->
    PackagePathOrig = get_demo_plugin_package(),
    BackupPath = filename:join(["/tmp", [filename:basename(PackagePathOrig), ".backup"]]),
    {ok, _} = file:copy(PackagePathOrig, BackupPath),
    on_exit(fun() -> {ok, _} = file:rename(BackupPath, PackagePathOrig) end),
    PackagePath = filename:join([
        filename:dirname(PackagePathOrig),
        "bad_plugin-1.0.0.tar.gz"
    ]),
    on_exit(fun() -> file:delete(PackagePath) end),
    ct:pal("package_location:~p orig:~p", [PackagePath, PackagePathOrig]),
    %% rename plugin tarball
    file:copy(PackagePathOrig, PackagePath),
    file:delete(PackagePathOrig),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = allow_installation(NameVsn),
    {ok, {{"HTTP/1.1", 400, "Bad Request"}, _, _}} = install_plugin(PackagePath),
    ?assertEqual(
        {error, enoent},
        file:delete(
            filename:join([
                emqx_plugins_fs:install_dir(),
                filename:basename(PackagePath)
            ])
        )
    ).

t_preinstall_step1_without_plugins_states_is_ignored_by_http_api(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    StalePluginPath = emqx_mgmt_api_test_util:api_path(["plugins", NameVsn]),
    StaleConfigPath = emqx_mgmt_api_test_util:api_path(["plugins", NameVsn, "config"]),
    StaleSchemaPath = emqx_mgmt_api_test_util:api_path(["plugins", NameVsn, "schema"]),
    StaleMovePath = emqx_mgmt_api_test_util:api_path(["plugins", NameVsn, "move"]),
    ClusterSyncPath = emqx_mgmt_api_test_util:api_path(["plugins", "cluster_sync"]),
    NameVsnBin = list_to_binary(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = make_stale_plugin(PackagePath),
    on_exit(fun() ->
        emqx_plugins:put_configured([]),
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:purge(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn)
    end),
    ?assertMatch(
        {ok, #{config_status := not_configured, running_status := stopped}},
        emqx_plugins:describe(NameVsn, #{})
    ),

    ?assertNot(
        lists:any(fun(Plugin) -> plugin_json_name_vsn(Plugin) =:= NameVsnBin end, list_plugins())
    ),
    ?assertMatch({error, {_, 404, _}}, emqx_mgmt_api_test_util:request_api(get, StalePluginPath)),
    ?assertMatch({error, {_, 404, _}}, emqx_mgmt_api_test_util:request_api(get, StaleConfigPath)),
    ?assertMatch({error, {_, 404, _}}, emqx_mgmt_api_test_util:request_api(get, StaleSchemaPath)),
    ?assertMatch({error, {_, 404, _}}, update_plugin(NameVsn, "start")),
    ?assertMatch({error, {_, 404, _}}, uninstall_plugin(NameVsn)),
    ?assertMatch(
        {error, {_, 404, _}},
        emqx_mgmt_api_test_util:request_api(
            post, StaleMovePath, "", [], #{<<"position">> => <<"front">>}
        )
    ),
    ?assertMatch(
        {error, {_, 404, _}},
        emqx_mgmt_api_test_util:request_api(
            post, ClusterSyncPath, "", [], #{<<"name">> => NameVsnBin}
        )
    ),

    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ?assert(
        lists:any(fun(Plugin) -> plugin_json_name_vsn(Plugin) =:= NameVsnBin end, list_plugins())
    ),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

t_preinstall_with_plugins_states_is_visible_to_http_api(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    NameVsnBin = list_to_binary(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = preinstall_step1_extract_package(PackagePath),
    ok = emqx_plugins:put_configured([#{name_vsn => NameVsn, enable => false}]),
    ok = emqx_plugins:ensure_installed(),
    on_exit(fun() ->
        emqx_plugins:put_configured([]),
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:purge(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn)
    end),
    ?assertMatch(
        {ok, #{config_status := disabled, running_status := stopped}},
        emqx_plugins:describe(NameVsn, #{})
    ),

    ?assert(
        lists:any(fun(Plugin) -> plugin_json_name_vsn(Plugin) =:= NameVsnBin end, list_plugins())
    ),
    ?assertMatch(
        #{<<"running_status">> := [#{<<"status">> := <<"stopped">>}]},
        describe_plugin(NameVsn)
    ),
    ok.

t_delete_non_existing(_Config) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", "non_exists-1.0.0"]),
    ?assertMatch(
        {error, {_, 404, _}},
        emqx_mgmt_api_test_util:request_api(delete, Path)
    ),
    ok.

t_cluster_update_order(Config) ->
    [N1 | _] = ?config(cluster, Config),
    PackagePath1 = get_demo_plugin_package(),
    NameVsn1 = filename:basename(PackagePath1, ?PACKAGE_SUFFIX),
    Name2Str = ?EMQX_PLUGIN_TEMPLATE_NAME ++ "_a",
    NameVsn2 = Name2Str ++ "-" ++ ?EMQX_PLUGIN_TEMPLATE_VSN,
    PackagePath2 = create_renamed_package(PackagePath1, NameVsn2),
    Name1 = list_to_binary(?EMQX_PLUGIN_TEMPLATE_NAME),
    Name2 = list_to_binary(Name2Str),

    ?ON(N1, begin
        ok = allow_installation(NameVsn1),
        ok = allow_installation(NameVsn2)
    end),
    ok = install_plugin_into_cluster(Config, PackagePath1),
    ok = install_plugin_into_cluster(Config, PackagePath2),
    %% to get them configured...
    {ok, _} = update_plugin_in_cluster(Config, NameVsn1, "start"),
    {ok, _} = update_plugin_in_cluster(Config, NameVsn2, "start"),

    ?assertMatch(
        [
            #{<<"name">> := Name1},
            #{<<"name">> := Name2}
        ],
        list_plugins_from_cluster(Config)
    ),

    ct:pal("moving to rear"),
    ?assertMatch({ok, _}, update_boot_order(NameVsn1, #{position => rear}, Config)),
    ?assertMatch(
        [
            #{<<"name">> := Name2},
            #{<<"name">> := Name1}
        ],
        list_plugins_from_cluster(Config)
    ),

    ct:pal("moving to front"),
    ?assertMatch({ok, _}, update_boot_order(NameVsn1, #{position => front}, Config)),
    ?assertMatch(
        [
            #{<<"name">> := Name1},
            #{<<"name">> := Name2}
        ],
        list_plugins_from_cluster(Config)
    ),

    ct:pal("moving after"),
    NameVsn2Bin = list_to_binary(NameVsn2),
    ?assertMatch(
        {ok, _},
        update_boot_order(NameVsn1, #{position => <<"after:", NameVsn2Bin/binary>>}, Config)
    ),
    ?assertMatch(
        [
            #{<<"name">> := Name2},
            #{<<"name">> := Name1}
        ],
        list_plugins_from_cluster(Config)
    ),

    ct:pal("moving before"),
    ?assertMatch(
        {ok, _},
        update_boot_order(NameVsn1, #{position => <<"before:", NameVsn2Bin/binary>>}, Config)
    ),
    ?assertMatch(
        [
            #{<<"name">> := Name1},
            #{<<"name">> := Name2}
        ],
        list_plugins_from_cluster(Config)
    ),

    ok.

t_cluster_rejects_invalid_local_config_on_start(Config) ->
    [Initiator, InvalidNode] = ?config(cluster, Config),
    PackagePath = make_test_plugin_package(
        Config,
        test_plugin_schema(),
        <<"foo = \"bar\"\n">>
    ),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ?ON(Initiator, ok = allow_installation(NameVsn)),
    ok = install_plugin_into_cluster(Config, PackagePath),
    InvalidConfigPath = ?ON(InvalidNode, emqx_plugins_fs:config_file_path(NameVsn)),
    ok = file:write_file(InvalidConfigPath, <<"foo = 42\n">>),
    {400, StartResponse} = request_plugin_action_in_cluster(Config, NameVsn, "start"),
    ?assertEqual(<<"BAD_CONFIG">>, maps:get(<<"code">>, StartResponse)),
    ?assertNotEqual(
        nomatch,
        binary:match(maps:get(<<"message">>, StartResponse), <<"invalid_plugin_config:">>)
    ),
    ?assertEqual(
        [{ok, false}, {ok, false}],
        erpc:multicall(
            [Initiator, InvalidNode],
            fun() -> lists:keymember(invalid_plugin, 1, application:which_applications()) end
        )
    ),
    ok = file:write_file(InvalidConfigPath, <<"foo = \"bar\"\n">>),
    {ok, _} = update_plugin_in_cluster(Config, NameVsn, "start"),
    ?assertEqual(
        [{ok, true}, {ok, true}],
        erpc:multicall(
            [Initiator, InvalidNode],
            fun() -> lists:keymember(invalid_plugin, 1, application:which_applications()) end
        )
    ).

t_cluster_rolls_back_partial_start_failure(Config) ->
    [Initiator, FailingNode] = ?config(cluster, Config),
    PackagePath = make_test_plugin_package(
        Config,
        test_plugin_schema(),
        <<"foo = \"bar\"\n">>
    ),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ?ON(Initiator, ok = allow_installation(NameVsn)),
    ok = install_plugin_into_cluster(Config, PackagePath),
    FailingAppFile = ?ON(FailingNode, test_plugin_app_file(NameVsn)),
    ok = ?ON(
        FailingNode,
        file:write_file(
            FailingAppFile,
            <<
                "{application, invalid_plugin, ["
                "{vsn, \"0.1.0\"},"
                "{applications, [missing_plugin_dependency]}"
                "]}.\n"
            >>
        )
    ),
    ok = ?ON(FailingNode, application:unload(invalid_plugin)),
    {500, StartResponse} = request_plugin_action_in_cluster(Config, NameVsn, "start"),
    ?assertEqual(<<"INTERNAL_ERROR">>, maps:get(<<"code">>, StartResponse)),
    ?assertMatch(
        <<"plugin_start_failed: ", _/binary>>,
        maps:get(<<"message">>, StartResponse)
    ),
    #{
        <<"running_status">> := RunningStatus
    } = describe_plugin_in_cluster(Config, NameVsn),
    ?assertEqual(
        [<<"stopped">>, <<"stopped">>],
        lists:sort([Status || #{<<"status">> := Status} <- RunningStatus])
    ).

list_plugins_from_cluster(Config) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins"]),
    case emqx_mgmt_api_test_util:request_api(get, Path, Auth) of
        {ok, Apps} -> emqx_utils_json:decode(Apps);
        {error, Reason} -> error(Reason)
    end.

list_plugins() ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins"]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Apps} -> emqx_utils_json:decode(Apps);
        {error, Reason} -> error(Reason)
    end.

%% The applications of the plugin that are running from its install directory.
%% `emqx_plugins_apps:running_status/1' with a name-vsn compares the release vsn
%% with the application vsn, which differ for the demo package, so check the
%% directory instead.
plugin_is_running(NameVsn) ->
    emqx_plugins_apps:running_apps_from(emqx_plugins_fs:plugin_dir(NameVsn)) =/= [].

describe_plugin(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Res} -> emqx_utils_json:decode(Res);
        {error, Reason} -> error(Reason)
    end.

describe_plugin_in_cluster(Config, Name) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path, Auth) of
        {ok, Res} -> emqx_utils_json:decode(Res);
        {error, Reason} -> error(Reason)
    end.

update_plugin_config(Name, Config) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name, "config"]),
    case emqx_mgmt_api_test_util:request_api_with_body(put, Path, Config) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

install_plugin(FilePath) ->
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(<<"admin">>, <<"public">>),
    Path = emqx_mgmt_api_test_util:api_path(["plugins", "install"]),
    case
        emqx_mgmt_api_test_util:upload_request(
            Path,
            FilePath,
            "plugin",
            <<"application/gzip">>,
            [],
            Token
        )
    of
        {ok, {{"HTTP/1.1", 204, "No Content"}, _Headers, <<>>}} -> ok;
        Error -> Error
    end.

install_plugin_into_cluster(Config, FilePath) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins", "install"]),
    case
        emqx_mgmt_api_test_util:upload_request(
            Path,
            FilePath,
            "plugin",
            <<"application/gzip">>,
            [],
            Auth
        )
    of
        {ok, {{"HTTP/1.1", 204, "No Content"}, _Headers, <<>>}} -> ok;
        Error -> Error
    end.

update_plugin(Name, Action) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name, Action]),
    emqx_mgmt_api_test_util:request_api(put, Path).

request_plugin_action(Name, Action) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name, Action]),
    request_with_response(put, Path, emqx_mgmt_api_test_util:auth_header_()).

update_plugin_in_cluster(Config, Name, Action) when is_list(Config) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins", Name, Action]),
    emqx_mgmt_api_test_util:request_api(put, Path, Auth).

request_plugin_action_in_cluster(Config, Name, Action) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins", Name, Action]),
    request_with_response(put, Path, Auth).

request_with_response(Method, Path, Auth) ->
    Opts = #{return_all => true, httpc_req_opts => [{body_format, binary}]},
    Result = emqx_mgmt_api_test_util:request_api(Method, Path, [], Auth, [], Opts),
    case Result of
        {ok, {{"HTTP/1.1", StatusCode, _}, _Headers, Body}} ->
            {StatusCode, emqx_utils_json:decode(Body)};
        {error, {{"HTTP/1.1", StatusCode, _}, _Headers, Body}} ->
            {StatusCode, emqx_utils_json:decode(Body)}
    end.

update_boot_order(Name, MoveBody, Config) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins", Name, "move"]),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, MoveBody, Opts) of
        {ok, Res} ->
            Resp =
                case emqx_utils_json:safe_decode(Res) of
                    {ok, Decoded} -> Decoded;
                    {error, _} -> Res
                end,
            ct:pal("update_boot_order response:\n  ~p", [Resp]),
            {ok, Resp};
        Error ->
            Error
    end.

uninstall_plugin(Name) ->
    DeletePath = emqx_mgmt_api_test_util:api_path(["plugins", Name]),
    emqx_mgmt_api_test_util:request_api(delete, DeletePath).

install_test_plugin(Config) ->
    PackagePath = make_test_plugin_package(
        Config,
        test_plugin_schema(),
        <<"foo = \"bar\"\n">>
    ),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_disabled(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    NameVsn.

test_plugin_app_file(NameVsn) ->
    [AppFile] = filelib:wildcard(
        filename:join([
            emqx_plugins_fs:plugin_dir(NameVsn),
            "*",
            "ebin",
            "invalid_plugin.app"
        ])
    ),
    AppFile.

get_demo_plugin_package() ->
    PkgName = lists:flatten([
        ?EMQX_PLUGIN_TEMPLATE_NAME, "-", ?EMQX_PLUGIN_TEMPLATE_VSN, ?PACKAGE_SUFFIX
    ]),
    Pkg = filename:join(emqx_common_test_helpers:proj_root(), PkgName),
    case filelib:is_regular(Pkg) of
        true ->
            Pkg;
        false ->
            #{package := Pkg} = emqx_plugins_test_helpers:get_demo_plugin_package(
                maps:merge(?EMQX_PLUGIN, #{shdir => emqx_common_test_helpers:proj_root()})
            ),
            true = filelib:is_regular(Pkg),
            Pkg
    end.

make_stale_plugin(PackagePath) ->
    ok = preinstall_step1_extract_package(PackagePath),
    emqx_plugins:put_configured([]).

preinstall_step1_extract_package(PackagePath) ->
    InstallDir = emqx_plugins_fs:install_dir(),
    ok = filelib:ensure_dir(filename:join(InstallDir, "dummy")),
    {ok, _} = file:copy(PackagePath, filename:join(InstallDir, filename:basename(PackagePath))),
    ok = erl_tar:extract(PackagePath, [compressed, {cwd, InstallDir}]).

plugin_json_name_vsn(#{<<"name">> := Name, <<"rel_vsn">> := Vsn}) ->
    <<Name/binary, "-", Vsn/binary>>.

get_demo_plugin_package(Overrides) ->
    Opts = maps:merge(?EMQX_PLUGIN, Overrides),
    Result = emqx_plugins_test_helpers:get_demo_plugin_package(Opts),
    true = filelib:is_regular(maps:get(package, Result)),
    Result.

create_renamed_package(PackagePath, NewNameVsn) ->
    {ok, Content} = erl_tar:extract(PackagePath, [compressed, memory]),
    {NewName, _Vsn} = emqx_plugins_utils:parse_name_vsn(NewNameVsn),
    NewNameB = atom_to_binary(NewName, utf8),
    Content1 = lists:map(
        fun({F, B}) ->
            [_ | PathPart] = filename:split(F),
            B1 = update_release_json(PathPart, B, NewNameB),
            {filename:join([NewNameVsn | PathPart]), B1}
        end,
        Content
    ),
    NewPackagePath = filename:join(filename:dirname(PackagePath), NewNameVsn ++ ?PACKAGE_SUFFIX),
    ok = erl_tar:create(NewPackagePath, Content1, [compressed]),
    NewPackagePath.

%% The same plugin package with different bytes: a file is added, the name-vsn
%% stays the same, so an upload of it can only be told apart from the installed
%% package by comparing the bytes.
create_modified_package(Config, PackagePath) ->
    {ok, Content} = erl_tar:extract(PackagePath, [compressed, memory]),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ModifiedPath = filename:join([?config(priv_dir, Config), NameVsn ++ ?PACKAGE_SUFFIX]),
    ok = filelib:ensure_dir(ModifiedPath),
    ok = erl_tar:create(
        ModifiedPath,
        [{filename:join(NameVsn, "REVIEW_EXTRA.md"), <<"different bytes">>} | Content],
        [compressed]
    ),
    ModifiedPath.

%% The JSON the CLI prints for `emqx_plugins_cli_utils:ensure_installed/2'.
cli_ensure_installed(NameVsn) ->
    LogFun = fun(Fmt, Args) -> iolist_to_binary(io_lib:format(Fmt, Args)) end,
    emqx_plugins_cli_utils:ensure_installed(NameVsn, LogFun).

make_test_plugin_package(Config, Schema, DefaultConfig) ->
    make_test_plugin_package(
        Config,
        Schema,
        DefaultConfig,
        <<"{application, invalid_plugin, [{vsn, \"0.1.0\"}]}.\n">>
    ).

make_test_plugin_package(Config, Schema, DefaultConfig, AppFile) ->
    make_test_plugin_package(
        Config,
        "1.0.0",
        "invalid_plugin",
        "0.1.0",
        Schema,
        DefaultConfig,
        AppFile
    ).

make_test_plugin_package(Config, RelVsn, AppName, AppVsn, Schema, DefaultConfig) ->
    AppFile = iolist_to_binary(
        io_lib:format("{application, ~s, [{vsn, ~p}]}.\n", [AppName, AppVsn])
    ),
    make_test_plugin_package(Config, RelVsn, AppName, AppVsn, Schema, DefaultConfig, AppFile).

make_test_plugin_package(Config, RelVsn, AppName, AppVsn, Schema, DefaultConfig, AppFile) ->
    make_test_plugin_package(Config, RelVsn, AppName, AppVsn, Schema, DefaultConfig, AppFile, []).

%% The same, with entries appended to the package: used to build a package
%% which carries the entries of another plugin.
make_test_plugin_package(
    Config, RelVsn, AppName, AppVsn, Schema, DefaultConfig, AppFile, ExtraEntries
) ->
    NameVsn = "invalid_plugin-" ++ RelVsn,
    PluginApp = AppName ++ "-" ++ AppVsn,
    PrivDir = filename:join([NameVsn, PluginApp, "priv"]),
    PackagePath = filename:join(
        ?config(priv_dir, Config),
        NameVsn ++ ?PACKAGE_SUFFIX
    ),
    ok = filelib:ensure_dir(PackagePath),
    Info = emqx_utils_json:encode(#{
        <<"name">> => <<"invalid_plugin">>,
        <<"rel_vsn">> => list_to_binary(RelVsn),
        <<"rel_apps">> => [list_to_binary(PluginApp)],
        <<"description">> => <<"test">>,
        <<"with_config_schema">> => true
    }),
    ok = erl_tar:create(
        PackagePath,
        [
            {filename:join(NameVsn, "release.json"), Info},
            {
                filename:join([NameVsn, PluginApp, "ebin", AppName ++ ".app"]),
                AppFile
            },
            {filename:join(PrivDir, "config_schema.avsc"), Schema},
            {filename:join(PrivDir, "config.hocon"), DefaultConfig}
        ] ++ ExtraEntries,
        [compressed]
    ),
    PackagePath.

test_plugin_schema() ->
    <<
        "{\"type\":\"record\",\"name\":\"invalid_plugin\","
        "\"fields\":[{\"name\":\"foo\",\"type\":\"string\"}]}"
    >>.

update_release_json(["release.json"], FileContent, NewName) ->
    ContentMap = emqx_utils_json:decode(FileContent),
    emqx_utils_json:encode(ContentMap#{<<"name">> => NewName});
update_release_json(_FileName, FileContent, _NewName) ->
    FileContent.

cluster(TestCase, Config) ->
    APIPort = ?config(api_port, Config),
    AppSpecs = app_specs(Config),
    Node1Apps = AppSpecs ++ [app_spec_dashboard(APIPort)],
    Node2Apps = AppSpecs,
    Node1Name = emqx_mgmt_api_plugins_SUITE1,
    Node1 = emqx_cth_cluster:node_name(Node1Name),
    emqx_cth_cluster:start(
        [
            {Node1Name, #{role => core, apps => Node1Apps, join_to => Node1}},
            {emqx_mgmt_api_plugins_SUITE2, #{role => core, apps => Node2Apps, join_to => Node1}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ).

app_specs(_Config) ->
    [
        emqx,
        emqx_conf,
        {emqx_management, #{
            after_start => fun emqx_mgmt_cli:load/0
        }},
        emqx_plugins
    ].

app_spec_dashboard(APIPort) ->
    {emqx_dashboard, #{
        config =>
            #{
                dashboard =>
                    #{
                        listeners =>
                            #{
                                http =>
                                    #{bind => APIPort}
                            }
                    }
            }
    }}.

init_api(Node) ->
    erpc:call(Node, emqx_common_test_http, create_default_app, []).

get_host_and_auth(Config) when is_list(Config) ->
    API = ?config(api, Config),
    APIPort = ?config(api_port, Config),
    Host = ?CLUSTER_API_SERVER(APIPort),
    Auth = emqx_common_test_http:auth_header(API),
    #{host => Host, auth => Auth}.

allow_installation(NameVsn) ->
    emqx_ctl:run_command(["plugins", "allow", NameVsn]).

allow_installation(NameVsn, Sha256Hex) ->
    emqx_ctl:run_command(["plugins", "allow", NameVsn, "sha256:" ++ Sha256Hex]).

disallow_installation(NameVsn) ->
    emqx_ctl:run_command(["plugins", "disallow", NameVsn]).

%% Every regular file of the plugin's install directory, with its bytes.
plugin_dir_files(NameVsn) ->
    Dir = emqx_plugins_fs:plugin_dir(NameVsn),
    lists:sort([
        {File, Content}
     || File <- filelib:wildcard(filename:join(Dir, "**")),
        filelib:is_regular(File),
        {ok, Content} <- [file:read_file(File)]
    ]).

%% The installation attempts of `NameVsn' still on disk: `[]' means that the
%% attempt cleaned up after itself.
staging_attempts(NameVsn) ->
    %% `filename:join/1' returns a binary as soon as one part is a binary, and
    %% `filelib:wildcard/1' only takes a string.
    filelib:wildcard(
        to_list(filename:join([emqx_plugins_fs:staging_root(), NameVsn, "*"]))
    ).

to_list(Path) when is_binary(Path) -> unicode:characters_to_list(Path);
to_list(Path) -> Path.
