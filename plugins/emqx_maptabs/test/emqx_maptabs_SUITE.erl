%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TABLE, <<"can_signals">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    InstallDir = filename:join([WorkDir, "plugins"]),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_ctl,
            emqx_rule_engine,
            {emqx_plugins, #{config => #{plugins => #{install_dir => InstallDir}}}}
        ],
        #{work_dir => WorkDir}
    ),
    ok = filelib:ensure_path(filename:join([InstallDir, "dummy"])),
    %% rule SQL resolves function names with binary_to_existing_atom;
    %% make sure the builtin provider module's atoms exist
    {module, _} = code:ensure_loaded(emqx_rule_funcs),
    try
        Package = plugin_package(),
        {ok, PackageBin} = file:read_file(Package),
        NameVsn = filename:basename(Package, ".tar.gz"),
        [
            {apps, Apps},
            {plugin_name_vsn, NameVsn},
            {plugin_package_bin, PackageBin}
            | Config
        ]
    catch
        error:{plugin_package_build_failed, _Package, Output} ->
            ct:log("plugin_package build failed: ~s", [Output]),
            {skip, "Run 'make compile-emqx-enterprise' first to build plugin dependencies."}
    end.

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = cleanup_plugin(Config),
    _ = file:del_dir_r(tables_dir()),
    ok = install_and_start_plugin(Config),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = cleanup_plugin(Config),
    _ = file:del_dir_r(tables_dir()),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_lookup_semantics(_Config) ->
    ok = load_table(?TABLE, [
        #{key => 2, signal_name => <<"sig_f32">>, start_bit => 17, length => 32},
        #{key => <<"str">>, note => <<"string keyed">>}
    ]),
    %% hit: value map without the key field
    ?assertEqual(
        #{<<"signal_name">> => <<"sig_f32">>, <<"start_bit">> => 17, <<"length">> => 32},
        emqx_maptabs:lookup(?TABLE, 2)
    ),
    ?assertEqual(#{<<"note">> => <<"string keyed">>}, emqx_maptabs:lookup(?TABLE, <<"str">>)),
    %% single field and default
    ?assertEqual(<<"sig_f32">>, emqx_maptabs:lookup(?TABLE, 2, <<"signal_name">>)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 2, <<"no_such_field">>)),
    ?assertEqual(<<"dflt">>, emqx_maptabs:lookup(?TABLE, 999, <<"signal_name">>, <<"dflt">>)),
    ?assertEqual(17, emqx_maptabs:lookup(?TABLE, 2, <<"start_bit">>, 0)),
    %% miss: unknown key, exact-term matching (no coercion), unknown table
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 999)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, <<"2">>)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 2.0)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, true)),
    ?assertEqual(undefined, emqx_maptabs:lookup(<<"no_such_table">>, 2)),
    %% lookups never throw, even with unusual argument types
    ?assertEqual(undefined, emqx_maptabs:lookup(not_a_binary, 2)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 2, not_a_binary)),
    ok.

t_loader_validation(_Config) ->
    Parse = fun(Bin) -> emqx_maptabs_loader:parse(Bin) end,
    ?assertMatch({error, #{reason := invalid_json}}, Parse(<<"{oops">>)),
    ?assertMatch({error, #{reason := not_a_json_array}}, Parse(<<"{\"key\": 1}">>)),
    ?assertMatch(
        {error, #{reason := missing_key, row_number := 2}},
        Parse(<<"[{\"key\": 1}, {\"nokey\": 2}]">>)
    ),
    ?assertMatch(
        {error, #{reason := duplicate_key, key := 1}},
        Parse(<<"[{\"key\": 1}, {\"key\": 1, \"x\": 2}]">>)
    ),
    ?assertMatch(
        {error, #{reason := float_key, key := 50.0}},
        Parse(<<"[{\"key\": 50.0}]">>)
    ),
    ?assertMatch(
        {error, #{reason := invalid_key_type}},
        Parse(<<"[{\"key\": null}]">>)
    ),
    ?assertMatch(
        {error, #{reason := invalid_key_type}},
        Parse(<<"[{\"key\": true}]">>)
    ),
    ?assertMatch(
        {error, #{reason := invalid_key_type}},
        Parse(<<"[{\"key\": [1]}]">>)
    ),
    ?assertMatch(
        {error, #{reason := row_not_an_object, row_number := 1}},
        Parse(<<"[42]">>)
    ),
    %% native JSON types are preserved
    ?assertMatch(
        {ok, #{
            rows := [{50, #{<<"f">> := 1.5, <<"b">> := true, <<"s">> := <<"x">>}}],
            row_count := 1,
            version := <<_/binary>>
        }},
        Parse(<<"[{\"key\": 50, \"f\": 1.5, \"b\": true, \"s\": \"x\"}]">>)
    ),
    ?assertEqual(ok, emqx_maptabs_loader:validate_name(<<"Az0_-">>)),
    ?assertMatch({error, _}, emqx_maptabs_loader:validate_name(<<"bad.name">>)),
    ?assertMatch({error, _}, emqx_maptabs_loader:validate_name(<<"">>)),
    ?assertMatch({error, _}, emqx_maptabs_loader:validate_name(<<"a/b">>)),
    ?assertEqual({ok, <<"tab-1">>}, emqx_maptabs_loader:table_name_from_path("/x/tab-1.json")),
    ?assertMatch({error, _}, emqx_maptabs_loader:table_name_from_path("/x/tab-1.csv")),
    ?assertMatch({error, _}, emqx_maptabs_loader:table_name_from_path("/x/ta b.json")),
    ok.

t_load_fail_closed_keeps_previous(_Config) ->
    ok = load_table(?TABLE, [#{key => 1, v => <<"v1">>}]),
    [#{version := Version1}] = emqx_maptabs:list_local(),
    %% duplicate keys: whole file rejected, previous version kept
    ?assertMatch(
        {error, #{reason := duplicate_key}},
        load_table(?TABLE, [#{key => 2, v => <<"v2">>}, #{key => 2, v => <<"v2b">>}])
    ),
    ?assertEqual(#{<<"v">> => <<"v1">>}, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertMatch([#{version := Version1}], emqx_maptabs:list_local()),
    %% invalid file name is rejected before any replication
    ?assertMatch({error, #{reason := invalid_table_name}}, load_json_file("ba d", <<"[]">>)),
    ok.

t_reload_and_delete(_Config) ->
    ok = load_table(?TABLE, [#{key => 1, v => <<"v1">>}]),
    %% hand-edit the file on disk, then reconcile
    Path = filename:join(tables_dir(), binary_to_list(?TABLE) ++ ".json"),
    ok = file:write_file(Path, emqx_utils_json:encode([#{key => 1, v => <<"v2">>}])),
    ?assertEqual(#{<<"v">> => <<"v1">>}, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertMatch([{_, ok}], emqx_maptabs:reload_cluster(?TABLE)),
    ?assertEqual(#{<<"v">> => <<"v2">>}, emqx_maptabs:lookup(?TABLE, 1)),
    %% a table whose file disappeared is dropped on reload
    ok = file:delete(Path),
    _ = emqx_maptabs:reload_cluster(all),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertEqual([], emqx_maptabs:list_local()),
    %% delete removes both the file and the cache
    ok = load_table(?TABLE, [#{key => 1, v => <<"v3">>}]),
    ?assert(filelib:is_regular(Path)),
    ok = emqx_maptabs:delete(?TABLE),
    ?assertNot(filelib:is_regular(Path)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 1)),
    %% deleting a non-existent table is idempotent
    ?assertEqual(ok, emqx_maptabs:delete(?TABLE)),
    ok.

t_list_local(_Config) ->
    ?assertEqual([], emqx_maptabs:list_local()),
    ok = load_table(?TABLE, [#{key => 1, v => <<"v1">>}]),
    ?assertMatch(
        [#{name := ?TABLE, row_count := 1, version := <<_/binary>>, loaded_at := _}],
        emqx_maptabs:list_local()
    ),
    ok.

t_atomic_reload(_Config) ->
    RowsA = [#{key => 1, gen => <<"a">>, mark => <<"a">>}],
    RowsB = [#{key => 1, gen => <<"b">>, mark => <<"b">>}],
    ok = load_table(?TABLE, RowsA),
    Tester = self(),
    Reader = spawn_link(fun() -> reader_loop(Tester, 0) end),
    lists:foreach(
        fun(I) ->
            Rows =
                case I rem 2 of
                    0 -> RowsB;
                    1 -> RowsA
                end,
            ok = load_table(?TABLE, Rows)
        end,
        lists:seq(1, 50)
    ),
    Reader ! stop,
    receive
        {reader_done, N} ->
            ?assert(N > 0)
    after 5000 ->
        error(reader_did_not_finish)
    end.

reader_loop(Tester, N) ->
    receive
        stop -> Tester ! {reader_done, N}
    after 0 ->
        %% a reader must always see a complete generation, never a
        %% partial row set or a torn row
        case emqx_maptabs:lookup(?TABLE, 1) of
            #{<<"gen">> := G, <<"mark">> := G} when G =:= <<"a">>; G =:= <<"b">> ->
                reader_loop(Tester, N + 1);
            undefined ->
                %% transient miss during swap is acceptable
                reader_loop(Tester, N);
            Other ->
                error({partial_read, Other})
        end
    end.

t_rule_sql_lookup_and_subbits(_Config) ->
    ok = load_table(?TABLE, [
        #{
            key => 2,
            signal_name => <<"sig_f32">>,
            start_bit => 17,
            length => 32,
            type => <<"float">>,
            signedness => <<"unsigned">>,
            endian => <<"big">>
        },
        #{
            key => 3,
            signal_name => <<"sig_s8">>,
            start_bit => 17,
            length => 8,
            type => <<"integer">>,
            signedness => <<"signed">>,
            endian => <<"big">>
        }
    ]),
    FrameF32 = frame_hex(<<0:4, 2:12, 245.5:32/float-big, 0:16>>),
    FrameS8 = frame_hex(<<0:4, 3:12, (-5):8/signed-big, 0:40>>),
    SQL = <<
        "FOREACH payload.frames AS c "
        "DO "
        "subbits(hexstr2bin(c),5,12) AS item_id, "
        "maptab_lookup('can_signals', item_id) AS sig, "
        "maptab_lookup('can_signals', item_id, 'signal_name', 'Unknown') AS signal_name, "
        "subbits(hexstr2bin(c), sig.start_bit, sig.length, sig.type, sig.signedness, sig.endian) AS data "
        "INCASE regex_match(c,'^[0-9A-Fa-f]{16}$') "
        "FROM \"t/can\""
    >>,
    Context = #{
        payload => emqx_utils_json:encode(#{frames => [FrameF32, FrameS8, <<"zz">>]}),
        topic => <<"t/can">>
    },
    {ok, Results} = emqx_rule_sqltester:test(#{sql => SQL, context => Context}),
    ?assertMatch(
        [
            #{
                <<"item_id">> := 2,
                <<"signal_name">> := <<"sig_f32">>,
                <<"data">> := 245.5
            },
            #{
                <<"item_id">> := 3,
                <<"signal_name">> := <<"sig_s8">>,
                <<"data">> := -5
            }
        ],
        Results
    ),
    ok.

t_rule_sql_miss_path(_Config) ->
    ok = load_table(?TABLE, [#{key => 2, signal_name => <<"sig_f32">>}]),
    UnknownFrame = frame_hex(<<0:4, 999:12, 0:48>>),
    SQL = <<
        "FOREACH payload.frames AS c "
        "DO "
        "maptab_lookup('can_signals', subbits(hexstr2bin(c),5,12), 'signal_name', 'Unknown') "
        "AS signal_name "
        "FROM \"t/can\""
    >>,
    Context = #{
        payload => emqx_utils_json:encode(#{frames => [UnknownFrame]}),
        topic => <<"t/can">>
    },
    {ok, Results} = emqx_rule_sqltester:test(#{sql => SQL, context => Context}),
    ?assertMatch([#{<<"signal_name">> := <<"Unknown">>}], Results),
    ok.

t_plugin_lifecycle(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    ok = load_table(?TABLE, [#{key => 1, v => <<"v1">>}]),
    ?assertMatch(
        {ok, _, _},
        emqx_rule_engine:get_external_function(maptab_lookup)
    ),
    ?assertMatch(
        {ok, #{health_status := #{status := ok}}},
        emqx_plugins:describe(NameVsn, #{fill_readme => false, health_check => true})
    ),
    ok = emqx_plugins:ensure_stopped(NameVsn),
    %% everything is cleaned up on stop...
    ?assertEqual(undefined, whereis(emqx_maptabs)),
    ?assertEqual(undefined, ets:whereis(emqx_maptabs_registry)),
    ?assertEqual({error, not_found}, emqx_rule_engine:get_external_function(maptab_lookup)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertEqual([], emqx_maptabs:list_local()),
    ?assertEqual({error, cmd_not_found}, emqx_ctl:lookup_command(maptabs)),
    %% ...and the cache is rebuilt from disk on restart
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assertEqual(#{<<"v">> => <<"v1">>}, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertMatch({ok, _}, emqx_ctl:lookup_command(maptabs)),
    ok.

t_limits_config(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    %% defaults from priv/config.hocon
    ?assertEqual(100, emqx_maptabs:max_tables()),
    ?assertEqual(10000, emqx_maptabs:max_rows_per_table()),
    ok = load_table(?TABLE, [#{key => N} || N <- [1, 2, 3]]),
    try
        ok = emqx_plugins:update_config(
            NameVsn,
            #{<<"max_tables">> => 2, <<"max_rows_per_table">> => 2}
        ),
        ?assertEqual(2, emqx_maptabs:max_tables()),
        ?assertEqual(2, emqx_maptabs:max_rows_per_table()),
        %% row limit: whole file rejected, previous version kept
        ?assertMatch(
            {error, #{reason := too_many_rows, row_count := 3, max_rows_per_table := 2}},
            load_table(?TABLE, [#{key => N, v => N} || N <- [1, 2, 3]])
        ),
        ?assertEqual(#{}, emqx_maptabs:lookup(?TABLE, 3)),
        %% replacing an existing table is allowed even at the table limit
        ok = load_table(?TABLE, [#{key => 1, v => <<"v2">>}]),
        ok = load_table(<<"tab2">>, [#{key => 1}]),
        %% a third table exceeds max_tables = 2
        ?assertMatch(
            {error, #{reason := too_many_tables, table_count := 2, max_tables := 2}},
            load_table(<<"tab3">>, [#{key => 1}])
        ),
        %% deleting a table frees a slot
        ok = emqx_maptabs:delete(<<"tab2">>),
        ok = load_table(<<"tab3">>, [#{key => 1}])
    after
        %% the plugin config persists in the data dir across
        %% reinstalls: restore defaults for the other test cases
        ok = emqx_plugins:update_config(
            NameVsn,
            #{<<"max_tables">> => 100, <<"max_rows_per_table">> => 10000}
        )
    end,
    ok.

t_cli(_Config) ->
    mock_ctl_print(),
    try
        ?assertMatch({ok, _}, emqx_ctl:lookup_command(maptabs)),
        JsonPath = write_json_file(binary_to_list(?TABLE), [#{key => 1, v => <<"v1">>}]),
        _ = emqx_maptabs_cli:cmd(["load", JsonPath]),
        ?assertEqual(#{<<"v">> => <<"v1">>}, emqx_maptabs:lookup(?TABLE, 1)),
        _ = emqx_maptabs_cli:cmd(["list"]),
        _ = emqx_maptabs_cli:cmd(["status"]),
        _ = emqx_maptabs_cli:cmd(["get", binary_to_list(?TABLE)]),
        _ = emqx_maptabs_cli:cmd(["reload"]),
        _ = emqx_maptabs_cli:cmd(["reload", binary_to_list(?TABLE)]),
        _ = emqx_maptabs_cli:cmd(["delete", binary_to_list(?TABLE)]),
        ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 1)),
        _ = emqx_maptabs_cli:cmd(["get", "no_such_table"]),
        _ = emqx_maptabs_cli:cmd(["load", "/no/such/file.json"]),
        _ = emqx_maptabs_cli:cmd(["bogus"])
    after
        unmock_ctl_print()
    end,
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

tables_dir() ->
    filename:join([emqx:data_dir(), "plugins", "emqx_maptabs", "tables"]).

load_table(Name, Rows) ->
    load_json_file(binary_to_list(Name), emqx_utils_json:encode(Rows)).

load_json_file(BaseName, Json) ->
    Path = write_json_bin(BaseName, Json),
    emqx_maptabs:load_file(Path).

write_json_file(BaseName, Rows) ->
    write_json_bin(BaseName, emqx_utils_json:encode(Rows)).

write_json_bin(BaseName, Json) ->
    Dir = filename:join(emqx:data_dir(), "maptab_uploads"),
    ok = filelib:ensure_path(Dir),
    Path = filename:join(Dir, BaseName ++ ".json"),
    ok = file:write_file(Path, Json),
    Path.

frame_hex(FrameBin) when bit_size(FrameBin) =:= 64 ->
    binary:encode_hex(FrameBin).

mock_ctl_print() ->
    catch meck:unload(emqx_ctl),
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg, []) end),
    meck:expect(emqx_ctl, print, fun(Msg, Args) -> emqx_ctl:format(Msg, Args) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> Usages end).

unmock_ctl_print() ->
    meck:unload(emqx_ctl).

plugin_package() ->
    Root = emqx_common_test_helpers:proj_root(),
    Vsn = string:trim(read_file(filename:join([Root, "plugins", "emqx_maptabs", "VERSION"]))),
    Package = filename:join([Root, "_build", "plugins", "emqx_maptabs-" ++ Vsn ++ ".tar.gz"]),
    _ = file:delete(Package),
    build_in_tree_plugin_package(Root, Package).

build_in_tree_plugin_package(Root, Package) ->
    Output = os:cmd(
        "cd " ++ Root ++
            " && PROFILE=emqx-enterprise ./scripts/build-plugin.sh emqx_maptabs 2>&1"
    ),
    case filelib:is_regular(Package) of
        true ->
            Package;
        false ->
            error({plugin_package_build_failed, Package, Output})
    end.

read_file(Path) ->
    {ok, Bin} = file:read_file(Path),
    binary_to_list(Bin).

install_and_start_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    PackageBin = ?config(plugin_package_bin, Config),
    ok = emqx_plugins:write_package(NameVsn, PackageBin),
    ok = emqx_plugins:allow_installation(
        NameVsn,
        binary:encode_hex(crypto:hash(sha256, PackageBin), lowercase)
    ),
    ok = emqx_plugins:ensure_installed(NameVsn, fresh_install),
    ok = emqx_plugins:ensure_started(NameVsn),
    ok.

cleanup_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    case emqx_plugins:describe(NameVsn, #{fill_readme => false, health_check => false}) of
        {ok, _Plugin} ->
            _ = emqx_plugins:ensure_stopped(NameVsn),
            _ = emqx_plugins:ensure_disabled(NameVsn),
            _ = emqx_plugins:ensure_uninstalled(NameVsn);
        {error, _Reason} ->
            ok
    end,
    _ = emqx_plugins:delete_package(NameVsn),
    _ = emqx_plugins:forget_allowed_installation(NameVsn),
    ok.
