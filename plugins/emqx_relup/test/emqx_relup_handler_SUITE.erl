%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_relup_handler_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% Loading is enough — `code:priv_dir/1` works without start/2 running.
    {ok, _} = application:ensure_all_started(crypto),
    ok = ensure_loaded(emqx_relup),
    Config.

end_per_suite(_Config) ->
    cleanup_test_catalog_entries(),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    cleanup_test_catalog_entries(),
    ok.

%%==============================================================================
%% validate_priv_catalog/0
%%==============================================================================
-doc "A well-formed .relup file appears in the Valid list.".
t_validate_priv_catalog_well_formed(_Config) ->
    File = write_test_relup(<<
        "#{from_version => \"1.0.0\","
        "  target_version => \"2.0.0\","
        "  code_changes => [],"
        "  post_upgrade_callbacks => []}."
    >>),
    {Valid, Errors} = emqx_relup_handler:validate_priv_catalog(),
    ?assert(
        lists:any(
            fun
                (#{from_version := "1.0.0", target_version := "2.0.0"}) -> true;
                (_) -> false
            end,
            Valid
        ),
        "valid entry must surface in Valid list"
    ),
    ?assertNot(
        lists:any(fun(#{file := F}) -> F =:= File end, Errors),
        "well-formed file must not appear in Errors"
    ).

-doc "A `.relup` file containing a non-map term is reported in Errors.".
t_validate_priv_catalog_invalid_term(_Config) ->
    File = write_test_relup(<<"not_a_map.">>),
    {_Valid, Errors} = emqx_relup_handler:validate_priv_catalog(),
    ?assert(
        lists:any(
            fun
                (#{err_type := invalid_relup_file, file := F}) -> F =:= File;
                (_) -> false
            end,
            Errors
        )
    ).

-doc "A `.relup` file that fails to parse is reported in Errors without crashing the scan.".
t_validate_priv_catalog_unparseable(_Config) ->
    File = write_test_relup(<<"this is not erlang at all">>),
    {_Valid, Errors} = emqx_relup_handler:validate_priv_catalog(),
    ?assert(
        lists:any(
            fun
                (#{file := F, err_type := failed_to_read_relup_file}) -> F =:= File;
                (#{file := F, err_type := relup_file_eval_crashed}) -> F =:= File;
                (_) -> false
            end,
            Errors
        )
    ).

-doc "The 5.10.4 -> 5.10.5 eredis hop uses a relup-plugin helper, not "
"`emqx_post_upgrade`, because these callbacks run in the `code_changes` "
"phase rather than the post-upgrade phase.".
t_5105_catalog_uses_plugin_eredis_upgrade_module(_Config) ->
    {Valid, _Errors} = emqx_relup_handler:validate_priv_catalog(),
    #{code_changes := CodeChanges} = find_relup_entry("5.10.4", "5.10.5", Valid),
    ?assertMatch([{load_module, emqx_release} | _], CodeChanges),
    ?assert(
        lists:member({apply, emqx_relup_eredis_upgrade, stop_redis_resources, []}, CodeChanges)
    ),
    ?assert(
        lists:member({apply, emqx_relup_eredis_upgrade, start_redis_resources, []}, CodeChanges)
    ),
    ?assertNot(lists:member({load_module, emqx_post_upgrade}, CodeChanges)),
    ?assertNot(
        lists:any(
            fun
                ({apply, emqx_post_upgrade, _Fun, _Args}) -> true;
                (_) -> false
            end,
            CodeChanges
        )
    ).

%%==============================================================================
%% Helpers
%%==============================================================================
ensure_loaded(App) ->
    case application:load(App) of
        ok -> ok;
        {error, {already_loaded, App}} -> ok;
        Other -> Other
    end.

write_test_relup(Content) ->
    Dir = filename:join([code:priv_dir(emqx_relup), "relup"]),
    ok = filelib:ensure_path(Dir),
    Name = "test-" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".relup",
    File = filename:join(Dir, Name),
    ok = file:write_file(File, Content),
    File.

find_relup_entry(FromVsn, TargetVsn, Entries) ->
    {value, Entry} = lists:search(
        fun(#{from_version := FromVsn0, target_version := TargetVsn0}) ->
            FromVsn0 =:= FromVsn andalso TargetVsn0 =:= TargetVsn
        end,
        Entries
    ),
    Entry.

cleanup_test_catalog_entries() ->
    Dir = filename:join([code:priv_dir(emqx_relup), "relup"]),
    case filelib:wildcard(filename:join(Dir, "test-*.relup")) of
        [] ->
            ok;
        Files ->
            lists:foreach(fun(F) -> _ = file:delete(F) end, Files),
            ok
    end.
