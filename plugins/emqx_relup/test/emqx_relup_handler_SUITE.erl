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

-doc "The 5.10.4 -> 5.10.5 hop also applies SAML XXE and backup download "
"authorization fixes without extending the Redis stop/start window.".
t_5105_catalog_covers_saml_xxe_and_backup_download_fixes(_Config) ->
    {Valid, _Errors} = emqx_relup_handler:validate_priv_catalog(),
    #{code_changes := CodeChanges} = find_relup_entry("5.10.4", "5.10.5", Valid),
    LoadRelease = {load_module, emqx_release},
    StopRedis = {apply, emqx_relup_eredis_upgrade, stop_redis_resources, []},
    StartRedis = {apply, emqx_relup_eredis_upgrade, start_redis_resources, []},
    AnnounceBPAPI = {apply, emqx_bpapi, announce, [node(), emqx]},
    ?assert(is_before(StopRedis, StartRedis, CodeChanges)),
    lists:foreach(
        fun(Instruction) ->
            ?assert(
                lists:member(Instruction, CodeChanges),
                #{missing_instruction => Instruction}
            ),
            ?assert(
                is_before(LoadRelease, Instruction, CodeChanges),
                #{instruction_must_run_after_emqx_release_load => Instruction}
            )
        end,
        [
            {restart_application, esaml},
            {load_module, emqx_dashboard},
            {load_module, emqx_mgmt_data_backup},
            {load_module, emqx_mgmt_data_backup_proto_v2},
            {load_module, emqx_mgmt_api_data_backup},
            AnnounceBPAPI
        ]
    ),
    ?assert(is_before({load_module, emqx_mgmt_data_backup_proto_v2}, AnnounceBPAPI, CodeChanges)).

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

is_before(Earlier, Later, List) ->
    index_of(Earlier, List) < index_of(Later, List).

index_of(Item, List) ->
    index_of(Item, List, 1).

index_of(Item, [Item | _], Index) ->
    Index;
index_of(Item, [_ | Rest], Index) ->
    index_of(Item, Rest, Index + 1);
index_of(Item, [], _Index) ->
    error({not_found, Item}).

cleanup_test_catalog_entries() ->
    Dir = filename:join([code:priv_dir(emqx_relup), "relup"]),
    case filelib:wildcard(filename:join(Dir, "test-*.relup")) of
        [] ->
            ok;
        Files ->
            lists:foreach(fun(F) -> _ = file:delete(F) end, Files),
            ok
    end.
