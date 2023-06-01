%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_config_SUITE).

-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

init_per_testcase(TestCase, Config) ->
    try
        ?MODULE:TestCase({init, Config})
    catch
        error:function_clause ->
            ok
    end,
    Config.

end_per_testcase(TestCase, Config) ->
    try
        ?MODULE:TestCase({'end', Config})
    catch
        error:function_clause ->
            ok
    end.

t_fill_default_values(C) when is_list(C) ->
    Conf = #{
        <<"broker">> => #{
            <<"perf">> => #{},
            <<"route_batch_clean">> => false
        }
    },
    WithDefaults = emqx_config:fill_defaults(Conf),
    ?assertMatch(
        #{
            <<"broker">> :=
                #{
                    <<"enable_session_registry">> := true,
                    <<"perf">> :=
                        #{
                            <<"route_lock_type">> := key,
                            <<"trie_compaction">> := true
                        },
                    <<"route_batch_clean">> := false,
                    <<"session_locking_strategy">> := quorum,
                    <<"shared_subscription_strategy">> := round_robin
                }
        },
        WithDefaults
    ),
    %% ensure JSON compatible
    _ = emqx_utils_json:encode(WithDefaults),
    ok.

t_init_load(C) when is_list(C) ->
    ConfFile = "./test_emqx.conf",
    ok = file:write_file(ConfFile, <<"">>),
    ExpectRootNames = lists:sort(hocon_schema:root_names(emqx_schema)),
    emqx_config:erase_all(),
    {ok, DeprecatedFile} = application:get_env(emqx, cluster_override_conf_file),
    ?assertEqual(false, filelib:is_regular(DeprecatedFile), DeprecatedFile),
    %% Don't has deprecated file
    ok = emqx_config:init_load(emqx_schema, [ConfFile]),
    ?assertEqual(ExpectRootNames, lists:sort(emqx_config:get_root_names())),
    ?assertMatch({ok, #{raw_config := 256}}, emqx:update_config([mqtt, max_topic_levels], 256)),
    emqx_config:erase_all(),
    %% Has deprecated file
    ok = file:write_file(DeprecatedFile, <<"{}">>),
    ok = emqx_config:init_load(emqx_schema, [ConfFile]),
    ?assertEqual(ExpectRootNames, lists:sort(emqx_config:get_root_names())),
    ?assertMatch({ok, #{raw_config := 128}}, emqx:update_config([mqtt, max_topic_levels], 128)),
    ok = file:delete(DeprecatedFile).

t_unknown_root_keys(C) when is_list(C) ->
    ?check_trace(
        #{timetrap => 1000},
        begin
            ok = emqx_config:init_load(
                emqx_schema, <<"test_1 {}\n test_2 {sub = 100}\n listeners {}">>
            ),
            ?block_until(#{?snk_kind := unknown_config_keys})
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{unknown_config_keys := "test_1,test_2"}],
                ?of_kind(unknown_config_keys, Trace)
            )
        end
    ),
    ok.

t_cluster_hocon_backup({init, C}) ->
    C;
t_cluster_hocon_backup({'end', _C}) ->
    File = "backup-test.hocon",
    Files = [File | filelib:wildcard(File ++ ".*.bak")],
    lists:foreach(fun file:delete/1, Files);
t_cluster_hocon_backup(C) when is_list(C) ->
    Write = fun(Path, Content) ->
        %% avoid name clash
        timer:sleep(1),
        emqx_config:backup_and_write(Path, Content)
    end,
    File = "backup-test.hocon",
    %% write 12 times, 10 backups should be kept
    %% the latest one is File itself without suffix
    %% the oldest one is expected to be deleted
    N = 12,
    Inputs = lists:seq(1, N),
    Backups = lists:seq(N - 10, N - 1),
    InputContents = [integer_to_binary(I) || I <- Inputs],
    BackupContents = [integer_to_binary(I) || I <- Backups],
    lists:foreach(
        fun(Content) ->
            Write(File, Content)
        end,
        InputContents
    ),
    LatestContent = integer_to_binary(N),
    ?assertEqual({ok, LatestContent}, file:read_file(File)),
    Re = "\\.[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{3}\\.bak$",
    Files = filelib:wildcard(File ++ ".*.bak"),
    ?assert(lists:all(fun(F) -> re:run(F, Re) =/= nomatch end, Files)),
    %% keep only the latest 10
    ?assertEqual(10, length(Files)),
    FilesSorted = lists:zip(lists:sort(Files), BackupContents),
    lists:foreach(
        fun({BackupFile, ExpectedContent}) ->
            ?assertEqual({ok, ExpectedContent}, file:read_file(BackupFile))
        end,
        FilesSorted
    ),
    ok.
