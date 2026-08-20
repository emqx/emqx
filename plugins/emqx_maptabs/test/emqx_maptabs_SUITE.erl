%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

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
            {skip, "Run 'make emqx-enterprise' first to build plugin dependencies."}
    end.

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = cleanup_plugin(Config),
    ok = wipe_storage(),
    ok = install_and_start_plugin(Config),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = cleanup_plugin(Config),
    ok = wipe_storage(),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "Lookup arities, exact-term key matching, and miss behavior.".
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

-doc "Fail-closed JSON validation with row numbers, and name/path rules.".
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

-doc "A rejected file keeps the previously loaded table version.".
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

-doc "The ETS cache follows direct storage writes and deletes through table events.".
t_cache_follows_storage(_Config) ->
    ok = load_table(?TABLE, [#{key => 1, v => <<"v1">>}]),
    %% a direct storage write (as a replicated update from another node
    %% would be) reaches the cache through table events
    ok = emqx_maptabs_store:put(?TABLE, emqx_utils_json:encode([#{key => 1, v => <<"v2">>}])),
    ok = wait_lookup(?TABLE, 1, #{<<"v">> => <<"v2">>}),
    %% so does a direct storage delete
    ok = emqx_maptabs_store:delete(?TABLE),
    ok = wait_lookup(?TABLE, 1, undefined),
    ?assertEqual([], emqx_maptabs:list_local()),
    %% delete removes both the record and the cache
    ok = load_table(?TABLE, [#{key => 1, v => <<"v3">>}]),
    ok = emqx_maptabs:delete(?TABLE),
    ?assertEqual({error, not_found}, emqx_maptabs:read_table(?TABLE)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 1)),
    %% deleting a non-existent table is idempotent
    ?assertEqual(ok, emqx_maptabs:delete(?TABLE)),
    %% the operator reconcile fallback is a cheap no-op when in sync
    ?assertMatch([{_, ok}], emqx_maptabs:reconcile_cluster()),
    ok.

-doc "list_local returns per-table metadata.".
t_list_local(_Config) ->
    ?assertEqual([], emqx_maptabs:list_local()),
    ok = load_table(?TABLE, [#{key => 1, v => <<"v1">>}]),
    ?assertMatch(
        [#{name := ?TABLE, row_count := 1, version := <<_/binary>>, loaded_at := _}],
        emqx_maptabs:list_local()
    ),
    ok.

-doc "A concurrent reader sees complete generations only, never a torn row set.".
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

-doc "FOREACH decode with maptab_lookup feeding subbits, guarded against misses.".
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
    FrameUnknown = frame_hex(<<0:4, 999:12, 0:48>>),
    %% the CASE .. is_map(sig) guard is load-bearing: subbits on the
    %% undefined miss result throws, and a FOREACH item error drops the
    %% whole message, not just the unknown frame
    SQL = <<
        "FOREACH payload.frames AS c "
        "DO "
        "subbits(hexstr2bin(c),5,12) AS item_id, "
        "maptab_lookup('can_signals', item_id) AS sig, "
        "maptab_lookup('can_signals', item_id, 'signal_name', 'Unknown') AS signal_name, "
        "CASE WHEN is_map(sig) "
        "THEN subbits(hexstr2bin(c), sig.start_bit, sig.length, sig.type, sig.signedness, sig.endian) "
        "ELSE 0.0 END AS data "
        "INCASE regex_match(c,'^[0-9A-Fa-f]{16}$') "
        "FROM \"t/can\""
    >>,
    Context = #{
        payload => emqx_utils_json:encode(#{
            frames => [FrameF32, FrameS8, FrameUnknown, <<"zz">>]
        }),
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
            },
            #{
                <<"item_id">> := 999,
                <<"signal_name">> := <<"Unknown">>,
                <<"data">> := +0.0
            }
        ],
        Results
    ),
    ok.

-doc "A complete default row makes the decode guard-free.".
t_rule_sql_default_row(_Config) ->
    ok = load_table(?TABLE, [
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
    FrameS8 = frame_hex(<<0:4, 3:12, (-5):8/signed-big, 0:40>>),
    FrameUnknown = frame_hex(<<0:4, 999:12, 1:48>>),
    %% a complete default row makes the decode guard-free: every field
    %% subbits needs is present even on a miss
    SQL = <<
        "FOREACH payload.frames AS c "
        "DO "
        "maptab_lookup('can_signals', subbits(hexstr2bin(c),5,12), "
        "json_decode('{\"signal_name\":\"Unknown\",\"start_bit\":17,\"length\":48,"
        "\"type\":\"bits\",\"signedness\":\"unsigned\",\"endian\":\"big\"}')) AS sig, "
        "sig.signal_name AS signal_name, "
        "subbits(hexstr2bin(c), sig.start_bit, sig.length, sig.type, sig.signedness, sig.endian) AS data "
        "FROM \"t/can\""
    >>,
    Context = #{
        payload => emqx_utils_json:encode(#{frames => [FrameS8, FrameUnknown]}),
        topic => <<"t/can">>
    },
    {ok, Results} = emqx_rule_sqltester:test(#{sql => SQL, context => Context}),
    ?assertMatch(
        [
            #{<<"signal_name">> := <<"sig_s8">>, <<"data">> := -5},
            #{<<"signal_name">> := <<"Unknown">>, <<"data">> := <<1:48>>}
        ],
        Results
    ),
    ok.

-doc "A miss with a scalar default yields the default, not an error.".
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

-doc "Plugin stop cleans up everything; restart rebuilds the cache from storage.".
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
    ?assertEqual(undefined, whereis(emqx_maptabs_server)),
    ?assertEqual(undefined, ets:whereis(emqx_maptabs_registry)),
    ?assertEqual({error, not_found}, emqx_rule_engine:get_external_function(maptab_lookup)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertEqual([], emqx_maptabs:list_local()),
    ?assertEqual({error, cmd_not_found}, emqx_ctl:lookup_command(maptabs)),
    %% ...and the cache is rebuilt from storage on restart
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assertEqual(#{<<"v">> => <<"v1">>}, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertMatch({ok, _}, emqx_ctl:lookup_command(maptabs)),
    ok.

-doc "Limits from the plugin config apply at load time; replicated writes bypass them.".
t_limits_config(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    %% defaults from priv/config.hocon
    ?assertEqual(100, emqx_maptabs:max_tables()),
    ?assertEqual(10000, emqx_maptabs:max_rows_per_table()),
    ?assertEqual(10000000, emqx_maptabs:max_table_file_bytes()),
    ok = load_table(?TABLE, [#{key => N} || N <- [1, 2, 3]]),
    try
        ok = emqx_plugins:update_config(
            NameVsn,
            #{
                <<"max_tables">> => 2,
                <<"max_rows_per_table">> => 2,
                <<"max_table_file_bytes">> => 100
            }
        ),
        ?assertEqual(2, emqx_maptabs:max_tables()),
        ?assertEqual(2, emqx_maptabs:max_rows_per_table()),
        ?assertEqual(100, emqx_maptabs:max_table_file_bytes()),
        %% file-size limit: rejected before any replication
        BigRow = #{key => 1, filler => binary:copy(<<"x">>, 200)},
        ?assertMatch(
            {error, #{reason := table_file_too_large, max_table_file_bytes := 100}},
            load_table(?TABLE, [BigRow])
        ),
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
        ok = load_table(<<"tab3">>, [#{key => 1}]),
        %% the cache follows storage unconditionally: limits guard only
        %% the load entrypoint, a replicated record is applied as is
        Rows3 = emqx_utils_json:encode([#{key => N} || N <- [1, 2, 3]]),
        ?assertEqual(ok, emqx_maptabs_store:put(?TABLE, Rows3)),
        ok = wait_lookup(?TABLE, 3, #{})
    after
        %% the plugin config persists in the data dir across
        %% reinstalls: restore defaults for the other test cases
        ok = emqx_plugins:update_config(
            NameVsn,
            #{
                <<"max_tables">> => 100,
                <<"max_rows_per_table">> => 10000,
                <<"max_table_file_bytes">> => 10000000
            }
        )
    end,
    ok.

-doc "Non-positive limit values are rejected and the previous config stays in effect.".
t_config_validation(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    Defaults = #{
        <<"max_tables">> => 100,
        <<"max_rows_per_table">> => 10000,
        <<"max_table_file_bytes">> => 10000000
    },
    try
        %% establish a known-good non-default value to prove rejections keep it
        ok = emqx_plugins:update_config(NameVsn, Defaults#{<<"max_tables">> := 42}),
        ?assertEqual(42, emqx_maptabs:max_tables()),
        %% each field rejects 0, -1 and a non-integer
        lists:foreach(
            fun({Field, Bad}) ->
                ?assertMatch(
                    {error, #{reason := invalid_config_value, field := Field, value := Bad}},
                    emqx_plugins:update_config(NameVsn, Defaults#{Field := Bad}),
                    {Field, Bad}
                )
            end,
            [{F, V} || F <- maps:keys(Defaults), V <- [0, -1, <<"10">>]]
        ),
        %% the rejected updates left the previous config in place
        ?assertEqual(42, emqx_maptabs:max_tables()),
        ?assertEqual(10000, emqx_maptabs:max_rows_per_table()),
        ?assertEqual(10000000, emqx_maptabs:max_table_file_bytes()),
        %% a valid update is accepted and takes effect
        ok = emqx_plugins:update_config(NameVsn, Defaults#{<<"max_tables">> := 7}),
        ?assertEqual(7, emqx_maptabs:max_tables()),
        %% missing fields are valid and fall back to their defaults
        ok = emqx_plugins:update_config(NameVsn, #{<<"max_rows_per_table">> => 5000}),
        ?assertEqual(100, emqx_maptabs:max_tables()),
        ?assertEqual(5000, emqx_maptabs:max_rows_per_table()),
        ?assertEqual(10000000, emqx_maptabs:max_table_file_bytes())
    after
        %% the plugin config persists in the data dir across
        %% reinstalls: restore defaults for the other test cases
        ok = emqx_plugins:update_config(NameVsn, Defaults)
    end,
    ok.

-doc "A persisted invalid limit does not block plugin start; reads fall back to the defaults.".
t_persisted_invalid_config(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    Defaults = #{
        <<"max_tables">> => 100,
        <<"max_rows_per_table">> => 10000,
        <<"max_table_file_bytes">> => 10000000
    },
    try
        %% write the config file directly, bypassing validation, as a
        %% deployment from before the validation existed would have
        ok = emqx_plugins_local_config:update(NameVsn, Defaults#{<<"max_tables">> := 0}),
        %% reinstall: the config file persists and seeds the config cache
        ok = cleanup_plugin(Config),
        ok = install_and_start_plugin(Config),
        ?assertMatch(
            {ok, #{health_status := #{status := ok}}},
            emqx_plugins:describe(NameVsn, #{fill_readme => false, health_check => true})
        ),
        %% the invalid value did reach the cache...
        ?assertEqual(0, maps:get(<<"max_tables">>, emqx_plugins:get_config(NameVsn, #{}))),
        %% ...and reads fall back to the default
        ?assertEqual(100, emqx_maptabs:max_tables()),
        %% a valid update replaces the stored invalid value
        ok = emqx_plugins:update_config(NameVsn, Defaults#{<<"max_tables">> := 5}),
        ?assertEqual(5, emqx_maptabs:max_tables())
    after
        ok = emqx_plugins_local_config:update(NameVsn, Defaults)
    end,
    ok.

-doc "Every CLI command against the installed plugin package.".
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

%% the storage table survives plugin reinstalls; before the first
%% install it does not exist yet
wipe_storage() ->
    _ = catch mria:clear_table(emqx_maptabs_index),
    _ = catch mria:clear_table(emqx_maptabs),
    ok.

%% waits out the event-driven cache update
wait_lookup(Table, Key, Expected) ->
    wait_lookup(Table, Key, Expected, 50).

wait_lookup(Table, Key, Expected, Retries) ->
    case emqx_maptabs:lookup(Table, Key) of
        Expected ->
            ok;
        Other when Retries =< 0 ->
            error({lookup_did_not_converge, #{expected => Expected, got => Other}});
        _ ->
            timer:sleep(100),
            wait_lookup(Table, Key, Expected, Retries - 1)
    end.

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
            " && PROFILE=emqx-enterprise make plugin-emqx_maptabs 2>&1"
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
