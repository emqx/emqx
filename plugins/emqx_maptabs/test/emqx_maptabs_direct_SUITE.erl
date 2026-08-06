%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Runs the plugin application directly from the umbrella build (no
%% plugin package install), so the module code under test is the
%% cover-compiled build output. Package install/uninstall integration
%% is covered by emqx_maptabs_SUITE.
-module(emqx_maptabs_direct_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(TABLE, <<"direct_tab">>).

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
            {emqx_plugins, #{config => #{plugins => #{install_dir => InstallDir}}}},
            emqx_maptabs
        ],
        #{work_dir => WorkDir}
    ),
    {module, _} = code:ensure_loaded(emqx_rule_funcs),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = ensure_app_running(),
    ok = wipe_tables(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    catch meck:unload(),
    ok = ensure_app_running(),
    ok = wipe_tables(),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_lookup_semantics(_Config) ->
    ok = load_table(?TABLE, [
        #{key => 2, signal_name => <<"sig_f32">>, start_bit => 17},
        #{key => <<"str">>, note => <<"string keyed">>}
    ]),
    ?assertEqual(
        #{<<"signal_name">> => <<"sig_f32">>, <<"start_bit">> => 17},
        emqx_maptabs:lookup(?TABLE, 2)
    ),
    ?assertEqual(<<"sig_f32">>, emqx_maptabs:lookup(?TABLE, 2, <<"signal_name">>)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 2, <<"nope">>)),
    ?assertEqual(<<"d">>, emqx_maptabs:lookup(?TABLE, 404, <<"signal_name">>, <<"d">>)),
    ?assertEqual(17, emqx_maptabs:lookup(?TABLE, 2, <<"start_bit">>, 0)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, <<"2">>)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 2.0)),
    ?assertEqual(undefined, emqx_maptabs:lookup(<<"no_such">>, 2)),
    ?assertEqual(undefined, emqx_maptabs:lookup(not_a_binary, 2)),
    %% rule SQL entrypoints
    ?assertEqual(
        #{<<"note">> => <<"string keyed">>},
        emqx_maptabs_rule_funcs:rsf_maptab_lookup([?TABLE, <<"str">>])
    ),
    ?assertEqual(
        <<"sig_f32">>,
        emqx_maptabs_rule_funcs:rsf_maptab_lookup([?TABLE, 2, <<"signal_name">>])
    ),
    ?assertEqual(
        <<"dflt">>,
        emqx_maptabs_rule_funcs:rsf_maptab_lookup([?TABLE, 404, <<"x">>, <<"dflt">>])
    ),
    %% metadata
    ?assertMatch(
        [#{name := ?TABLE, row_count := 2, version := <<_/binary>>, loaded_at := _}],
        emqx_maptabs:list_local()
    ),
    ?assertEqual(emqx_maptabs:list_local(), emqx_maptabs:list_on_node(node())),
    ?assertMatch({ok, <<_/binary>>}, emqx_maptabs:read_table_file(?TABLE)),
    ok.

t_loader_validation(_Config) ->
    Parse = fun(Bin) -> emqx_maptabs_loader:parse(Bin) end,
    ?assertMatch({error, #{reason := invalid_json}}, Parse(<<"{oops">>)),
    ?assertMatch({error, #{reason := not_a_json_array}}, Parse(<<"{}">>)),
    ?assertMatch({error, #{reason := missing_key}}, Parse(<<"[{\"k\": 1}]">>)),
    ?assertMatch(
        {error, #{reason := duplicate_key}},
        Parse(<<"[{\"key\": 1}, {\"key\": 1}]">>)
    ),
    ?assertMatch({error, #{reason := float_key}}, Parse(<<"[{\"key\": 1.5}]">>)),
    ?assertMatch({error, #{reason := invalid_key_type}}, Parse(<<"[{\"key\": null}]">>)),
    ?assertMatch({error, #{reason := invalid_key_type}}, Parse(<<"[{\"key\": true}]">>)),
    ?assertMatch({error, #{reason := invalid_key_type}}, Parse(<<"[{\"key\": [1]}]">>)),
    ?assertMatch({error, #{reason := invalid_key_type}}, Parse(<<"[{\"key\": {}}]">>)),
    ?assertMatch({error, #{reason := row_not_an_object}}, Parse(<<"[1]">>)),
    ?assertMatch({ok, #{row_count := 0, rows := []}}, Parse(<<"[]">>)),
    ?assertMatch(
        {ok, #{rows := [{1, #{}}, {<<"a">>, #{}}]}}, Parse(<<"[{\"key\":1},{\"key\":\"a\"}]">>)
    ),
    ?assertEqual(ok, emqx_maptabs_loader:validate_name(<<"A-z_09">>)),
    ?assertMatch({error, _}, emqx_maptabs_loader:validate_name(<<"sp ace">>)),
    ?assertMatch({error, _}, emqx_maptabs_loader:validate_name(<<>>)),
    ?assertMatch({error, _}, emqx_maptabs_loader:validate_name("not_binary")),
    ?assertEqual({ok, <<"n1">>}, emqx_maptabs_loader:table_name_from_path("/a/n1.json")),
    ?assertEqual({ok, <<"n1">>}, emqx_maptabs_loader:table_name_from_path("/a/n1.JSON")),
    ?assertMatch({error, _}, emqx_maptabs_loader:table_name_from_path("/a/n1.txt")),
    ?assertMatch({error, _}, emqx_maptabs_loader:table_name_from_path("/a/b ad.json")),
    ok.

t_limits(_Config) ->
    ok = load_table(?TABLE, [#{key => 1, v => <<"v1">>}]),
    mock_limits(#{
        <<"max_tables">> => 1,
        <<"max_rows_per_table">> => 2,
        <<"max_table_file_bytes">> => 100
    }),
    ?assertEqual(1, emqx_maptabs:max_tables()),
    ?assertEqual(2, emqx_maptabs:max_rows_per_table()),
    ?assertEqual(100, emqx_maptabs:max_table_file_bytes()),
    %% oversized file: rejected before reading
    Filler = binary:copy(<<"x">>, 200),
    ?assertMatch(
        {error, #{reason := table_file_too_large}},
        load_table(?TABLE, [#{key => 1, filler => Filler}])
    ),
    %% too many rows: whole file rejected, previous version kept
    ?assertMatch(
        {error, #{reason := too_many_rows}},
        load_table(?TABLE, [#{key => N} || N <- [1, 2, 3]])
    ),
    ?assertEqual(#{<<"v">> => <<"v1">>}, emqx_maptabs:lookup(?TABLE, 1)),
    %% replacing the only table is fine; a second one exceeds max_tables
    ok = load_table(?TABLE, [#{key => 1, v => <<"v2">>}]),
    ?assertMatch({error, #{reason := too_many_tables}}, load_table(<<"more">>, [#{key => 1}])),
    %% replicated transactions bypass the limits
    Rows3 = emqx_utils_json:encode([#{key => N} || N <- [1, 2, 3]]),
    ?assertEqual(ok, emqx_maptabs:do_load_v1(?TABLE, Rows3, #{kind => ?KIND_REPLICATE})),
    ?assertEqual(#{}, emqx_maptabs:lookup(?TABLE, 3)),
    %% invalid config values fall back to the defaults
    mock_limits(#{<<"max_tables">> => -1, <<"max_rows_per_table">> => <<"nan">>}),
    ?assertEqual(100, emqx_maptabs:max_tables()),
    ?assertEqual(10000, emqx_maptabs:max_rows_per_table()),
    ?assertEqual(10000000, emqx_maptabs:max_table_file_bytes()),
    meck:unload(emqx_plugins),
    ok.

t_reload_and_delete(_Config) ->
    ok = load_table(?TABLE, [#{key => 1, v => <<"v1">>}]),
    TabPath = table_file_path(?TABLE),
    %% hand-edit + reload one
    ok = file:write_file(TabPath, emqx_utils_json:encode([#{key => 1, v => <<"v2">>}])),
    ?assertMatch([{_, ok}], emqx_maptabs:reload_cluster(?TABLE)),
    ?assertEqual(#{<<"v">> => <<"v2">>}, emqx_maptabs:lookup(?TABLE, 1)),
    %% a hand-copied new file is picked up by named reload
    Extra = table_file_path(<<"extra">>),
    ok = file:write_file(Extra, emqx_utils_json:encode([#{key => 7}])),
    ?assertEqual(ok, emqx_maptabs:reload_local(<<"extra">>)),
    ?assertEqual(#{}, emqx_maptabs:lookup(<<"extra">>, 7)),
    %% reload all reconciles: gone file drops, invalid file is rejected
    ok = file:delete(Extra),
    ok = file:write_file(table_file_path(<<"bad">>), <<"{oops">>),
    Results = emqx_maptabs:reload_local(all),
    ?assertMatch({_, {error, #{reason := invalid_json}}}, lists:keyfind(<<"bad">>, 1, Results)),
    ?assertEqual(undefined, emqx_maptabs:lookup(<<"extra">>, 7)),
    ?assertEqual(#{<<"v">> => <<"v2">>}, emqx_maptabs:lookup(?TABLE, 1)),
    ok = file:delete(table_file_path(<<"bad">>)),
    %% reload of a never-existing table is a no-op drop
    ?assertEqual(ok, emqx_maptabs:reload_local(<<"ghost">>)),
    %% files beyond max_tables are skipped on full reload
    ok = file:write_file(table_file_path(<<"zzz">>), emqx_utils_json:encode([#{key => 9}])),
    mock_limits(#{<<"max_tables">> => 1}),
    _ = emqx_maptabs:reload_local(all),
    ?assertEqual(#{<<"v">> => <<"v2">>}, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertEqual(undefined, emqx_maptabs:lookup(<<"zzz">>, 9)),
    %% a new-to-cache table is refused by named reload at the limit
    ?assertMatch({error, #{reason := too_many_tables}}, emqx_maptabs:reload_local(<<"zzz">>)),
    meck:unload(emqx_plugins),
    %% delete: file + cache, idempotent, name validated
    ok = emqx_maptabs:delete(?TABLE),
    ?assertNot(filelib:is_regular(TabPath)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 1)),
    ok = emqx_maptabs:delete(?TABLE),
    ?assertMatch({error, #{reason := invalid_table_name}}, emqx_maptabs:delete(<<"b/d">>)),
    %% do_delete_v1 is also directly callable (replay path)
    ok = file:write_file(table_file_path(<<"zap">>), emqx_utils_json:encode([#{key => 1}])),
    ?assertEqual(ok, emqx_maptabs:do_delete_v1(<<"zap">>, #{kind => ?KIND_REPLICATE})),
    ok.

t_load_errors(_Config) ->
    ?assertMatch({error, #{reason := not_a_json_file}}, emqx_maptabs:load_file("/x/t.csv")),
    ?assertMatch(
        {error, #{reason := failed_to_read_file}},
        emqx_maptabs:load_file("/no/such/file.json")
    ),
    ?assertMatch(
        {error, #{reason := invalid_json}},
        load_json_file(binary_to_list(?TABLE), <<"{not json">>)
    ),
    ?assertMatch(
        {error, #{reason := failed_to_read_file}},
        emqx_maptabs:read_table_file(<<"no_such_table">>)
    ),
    ?assertMatch({error, #{reason := invalid_table_name}}, emqx_maptabs:read_table_file(<<"a b">>)),
    ok.

t_stopped_plugin(_Config) ->
    ok = load_table(?TABLE, [#{key => 1, v => <<"v1">>}]),
    Path = write_json_file("stopped_probe", [#{key => 1}]),
    ok = application:stop(emqx_maptabs),
    ?assertEqual({error, <<"Plugin is not running">>}, emqx_maptabs:health_check()),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertEqual([], emqx_maptabs:list_local()),
    ?assertEqual({error, plugin_not_running}, emqx_maptabs:reload_local(all)),
    ?assertMatch(
        {error, #{reason := preflight_failed, errors := [{_, plugin_not_running}]}},
        emqx_maptabs:load_file(Path)
    ),
    %% replicated cache ops degrade to ok while the file ops still work
    Rows = emqx_utils_json:encode([#{key => 1, v => <<"v2">>}]),
    ?assertEqual(ok, emqx_maptabs:do_load_v1(?TABLE, Rows, #{kind => ?KIND_REPLICATE})),
    ok = ensure_app_running(),
    %% the file written while stopped is loaded at start
    ?assertEqual(#{<<"v">> => <<"v2">>}, emqx_maptabs:lookup(?TABLE, 1)),
    ok.

t_readonly_dir(_Config) ->
    Dir = emqx_maptabs:tables_dir(),
    ok = filelib:ensure_path(Dir),
    ok = file:change_mode(Dir, 8#555),
    Probe = filename:join(Dir, ".probe"),
    case file:write_file(Probe, <<>>) of
        {error, eacces} ->
            try
                Path = write_json_file("ro_probe", [#{key => 1}]),
                ?assertMatch(
                    {error, #{reason := preflight_failed}},
                    emqx_maptabs:load_file(Path)
                ),
                %% write and delete failures surface as errors on the
                %% replicated path too
                Rows = emqx_utils_json:encode([#{key => 1}]),
                ?assertMatch(
                    {error, #{reason := failed_to_write_table_file}},
                    emqx_maptabs:do_load_v1(<<"ro_probe">>, Rows, #{kind => ?KIND_REPLICATE})
                )
            after
                ok = file:change_mode(Dir, 8#755)
            end;
        ok ->
            %% running as a privileged user: mode bits are not enforced
            _ = file:delete(Probe),
            ok = file:change_mode(Dir, 8#755),
            ct:comment("privileged user, read-only dir not enforceable")
    end,
    ok.

t_delete_readonly_dir(_Config) ->
    ok = load_table(?TABLE, [#{key => 1}]),
    Dir = emqx_maptabs:tables_dir(),
    ok = file:change_mode(Dir, 8#555),
    case file:delete(filename:join(Dir, "not_there.json")) of
        {error, enoent} ->
            try
                Result = emqx_maptabs:do_delete_v1(?TABLE, #{kind => ?KIND_REPLICATE}),
                case Result of
                    {error, #{reason := failed_to_delete_table_file}} -> ok;
                    %% privileged user: deletion succeeds despite the mode
                    ok -> ct:comment("privileged user, read-only dir not enforceable")
                end
            after
                ok = file:change_mode(Dir, 8#755)
            end;
        _ ->
            ok = file:change_mode(Dir, 8#755)
    end,
    ok.

t_server_edges(_Config) ->
    ?assertEqual({error, unexpected_call}, gen_server:call(emqx_maptabs_server, bogus)),
    ok = gen_server:cast(emqx_maptabs_server, bogus),
    emqx_maptabs_server ! bogus,
    ?assertEqual(ok, emqx_maptabs_server:health_check()),
    %% remote-call error mapping
    Fake = 'no_such_node@127.0.0.1',
    ?assertMatch({error, {nodedown, Fake}}, emqx_maptabs_server:list_tables(Fake)),
    ?assertMatch({error, {nodedown, Fake}}, emqx_maptabs_server:preflight(Fake)),
    ?assertMatch({error, {nodedown, Fake}}, emqx_maptabs_server:reload(Fake, all)),
    %% no tables on disk: full reload reconciles to an empty result
    ?assertMatch([{_, []}], emqx_maptabs:reload_cluster(all)),
    ok.

t_app_callbacks(_Config) ->
    ?assertEqual(ok, emqx_maptabs_app:on_config_changed(#{}, #{})),
    ?assertEqual(ok, emqx_maptabs_app:on_health_check(#{})),
    ?assertMatch({ok, _, _}, emqx_rule_engine:get_external_function(maptab_lookup)),
    ok.

t_cli(_Config) ->
    mock_ctl_print(),
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
    _ = emqx_maptabs_cli:cmd(["delete", "bad name"]),
    _ = emqx_maptabs_cli:cmd(["load", "/no/such/file.json"]),
    _ = emqx_maptabs_cli:cmd(["bogus"]),
    %% error entries in status and non-JSON-encodable reasons
    ok = application:stop(emqx_maptabs),
    _ = emqx_maptabs_cli:cmd(["status"]),
    _ = emqx_maptabs_cli:cmd(["load", JsonPath]),
    _ = emqx_maptabs_cli:cmd(["reload"]),
    ok = ensure_app_running(),
    ok.

t_sqltester_roundtrip(_Config) ->
    ok = load_table(?TABLE, [#{key => 3, signal_name => <<"sig_s8">>}]),
    SQL = <<
        "SELECT maptab_lookup('direct_tab', 3, 'signal_name', 'Unknown') AS s1, "
        "maptab_lookup('direct_tab', 4, 'signal_name', 'Unknown') AS s2 "
        "FROM \"t/1\""
    >>,
    ?assertMatch(
        {ok, #{<<"s1">> := <<"sig_s8">>, <<"s2">> := <<"Unknown">>}},
        emqx_rule_sqltester:test(#{
            sql => SQL, context => #{payload => <<"{}">>, topic => <<"t/1">>}
        })
    ),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

ensure_app_running() ->
    case whereis(emqx_maptabs_server) of
        undefined ->
            {ok, _} = application:ensure_all_started(emqx_maptabs),
            ok;
        _ ->
            ok
    end.

wipe_tables() ->
    _ = file:del_dir_r(emqx_maptabs:tables_dir()),
    _ = emqx_maptabs:reload_local(all),
    ok.

table_file_path(Name) ->
    ok = filelib:ensure_path(emqx_maptabs:tables_dir()),
    filename:join(emqx_maptabs:tables_dir(), binary_to_list(Name) ++ ".json").

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

mock_limits(ConfigMap) ->
    catch meck:unload(emqx_plugins),
    meck:new(emqx_plugins, [non_strict, passthrough]),
    meck:expect(emqx_plugins, get_config, fun(_NameVsn, _Default) -> ConfigMap end).

mock_ctl_print() ->
    catch meck:unload(emqx_ctl),
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg, []) end),
    meck:expect(emqx_ctl, print, fun(Msg, Args) -> emqx_ctl:format(Msg, Args) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> Usages end).
