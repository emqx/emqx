%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_config_handler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(MOD, {mod}).
-define(WKEY, '?').

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

t_handler(_Config) ->
    BadCallBackMod = emqx,
    RootKey = sysmon,
    %% bad
    ?assertError(
        #{msg := "bad_emqx_config_handler_callback", module := BadCallBackMod},
        emqx_config_handler:add_handler([RootKey], BadCallBackMod)
    ),
    %% simple
    ok = emqx_config_handler:add_handler([RootKey], ?MODULE),
    #{handlers := Handlers0} = emqx_config_handler:info(),
    ?assertMatch(#{RootKey := #{?MOD := ?MODULE}}, Handlers0),
    ok = emqx_config_handler:remove_handler([RootKey]),
    #{handlers := Handlers1} = emqx_config_handler:info(),
    ct:pal("Key:~p simple: ~p~n", [RootKey, Handlers1]),
    ?assertEqual(false, maps:is_key(RootKey, Handlers1)),
    %% wildcard 1
    Wildcard1 = [RootKey, '?', cpu_check_interval],
    ok = emqx_config_handler:add_handler(Wildcard1, ?MODULE),
    #{handlers := Handlers2} = emqx_config_handler:info(),
    ?assertMatch(#{RootKey := #{?WKEY := #{cpu_check_interval := #{?MOD := ?MODULE}}}}, Handlers2),
    ok = emqx_config_handler:remove_handler(Wildcard1),
    #{handlers := Handlers3} = emqx_config_handler:info(),
    ct:pal("Key:~p wildcard1: ~p~n", [Wildcard1, Handlers3]),
    ?assertEqual(false, maps:is_key(RootKey, Handlers3)),

    %% can_override_a_wildcard_path
    ok = emqx_config_handler:add_handler(Wildcard1, ?MODULE),
    ?assertEqual(ok, emqx_config_handler:add_handler([RootKey, os, cpu_check_interval], ?MODULE)),
    ok = emqx_config_handler:remove_handler(Wildcard1),
    ok = emqx_config_handler:remove_handler([RootKey, os, cpu_check_interval]),

    ok = emqx_config_handler:add_handler([RootKey, os, cpu_check_interval], ?MODULE),
    ok = emqx_config_handler:add_handler(Wildcard1, ?MODULE),
    ok = emqx_config_handler:remove_handler([RootKey, os, cpu_check_interval]),
    ok = emqx_config_handler:remove_handler(Wildcard1),
    ok.

t_conflict_handler(_Config) ->
    ok = emqx_config_handler:add_handler([sysmon, '?', '?'], ?MODULE),
    ?assertMatch(
        {error, {conflict, _}},
        emqx_config_handler:add_handler([sysmon, '?', cpu_check_interval], ?MODULE)
    ),
    ok = emqx_config_handler:remove_handler([sysmon, '?', '?']),

    ok = emqx_config_handler:add_handler([sysmon, '?', cpu_check_interval], ?MODULE),
    ?assertMatch(
        {error, {conflict, _}},
        emqx_config_handler:add_handler([sysmon, '?', '?'], ?MODULE)
    ),
    ok = emqx_config_handler:remove_handler([sysmon, '?', cpu_check_interval]),

    %% override
    ok = emqx_config_handler:add_handler([sysmon], emqx_config_logger),
    ?assertMatch(
        #{handlers := #{sysmon := #{{mod} := emqx_config_logger}}},
        emqx_config_handler:info()
    ),
    ok.

t_root_key_update(_Config) ->
    PathKey = [sysmon],
    Opts = #{rawconf_with_defaults => true},
    ok = emqx_config_handler:add_handler(PathKey, ?MODULE),
    %% update
    Old = #{<<"os">> := OS} = emqx:get_raw_config(PathKey),
    {ok, Res} = emqx:update_config(
        PathKey,
        Old#{<<"os">> => OS#{<<"cpu_check_interval">> => <<"12s">>}},
        Opts
    ),
    ?assertMatch(
        #{
            config := #{os := #{cpu_check_interval := 12000}},
            post_config_update := #{?MODULE := ok},
            raw_config := #{<<"os">> := #{<<"cpu_check_interval">> := <<"12s">>}}
        },
        Res
    ),
    ?assertMatch(#{os := #{cpu_check_interval := 12000}}, emqx:get_config(PathKey)),

    %% update sub key
    SubKey = PathKey ++ [os, cpu_high_watermark],
    ?assertEqual(
        {ok, #{
            config => 0.81,
            post_config_update => #{?MODULE => ok},
            raw_config => <<"81%">>
        }},
        emqx:update_config(SubKey, "81%", Opts)
    ),
    ?assertEqual(0.81, emqx:get_config(SubKey)),
    ?assertEqual("81%", emqx:get_raw_config(SubKey)),
    %% remove
    ?assertEqual({error, "remove_root_is_forbidden"}, emqx:remove_config(PathKey)),
    ?assertMatch(true, is_map(emqx:get_raw_config(PathKey))),

    ok = emqx_config_handler:remove_handler(PathKey),
    ok.

t_sub_key_update_remove(_Config) ->
    KeyPath = [sysmon, os, cpu_check_interval],
    Opts = #{},
    ok = emqx_config_handler:add_handler(KeyPath, ?MODULE),
    {ok, Res} = emqx:update_config(KeyPath, <<"60s">>, Opts),
    ?assertMatch(
        #{
            config := 60000,
            post_config_update := #{?MODULE := ok},
            raw_config := <<"60s">>
        },
        Res
    ),
    ?assertMatch(60000, emqx:get_config(KeyPath)),

    KeyPath2 = [sysmon, os, cpu_low_watermark],
    ok = emqx_config_handler:add_handler(KeyPath2, ?MODULE),
    {ok, Res1} = emqx:update_config(KeyPath2, <<"40%">>, Opts),
    ?assertMatch(
        #{
            config := 0.4,
            post_config_update := #{},
            raw_config := <<"40%">>
        },
        Res1
    ),
    ?assertMatch(0.4, emqx:get_config(KeyPath2)),

    %% remove
    ?assertEqual(
        {ok, #{post_config_update => #{emqx_config_handler_SUITE => ok}}},
        emqx:remove_config(KeyPath)
    ),
    ?assertError({config_not_found, KeyPath}, emqx:get_raw_config(KeyPath)),
    OSKey = maps:keys(emqx:get_raw_config([sysmon, os])),
    ?assertEqual(false, lists:member(<<"cpu_check_interval">>, OSKey)),
    ?assert(length(OSKey) > 0),

    ?assertEqual(
        {ok, #{
            config => 60000,
            post_config_update => #{?MODULE => ok},
            raw_config => <<"60s">>
        }},
        emqx:reset_config(KeyPath, Opts)
    ),
    OSKey1 = maps:keys(emqx:get_raw_config([sysmon, os])),
    ?assertEqual(true, lists:member(<<"cpu_check_interval">>, OSKey1)),
    ?assert(length(OSKey1) > 1),

    ok = emqx_config_handler:remove_handler(KeyPath),
    ok = emqx_config_handler:remove_handler(KeyPath2),
    ok.

t_check_failed(_Config) ->
    KeyPath = [sysmon, os, cpu_check_interval],
    Opts = #{rawconf_with_defaults => true},
    Origin = emqx:get_raw_config(KeyPath),
    ok = emqx_config_handler:add_handler(KeyPath, ?MODULE),
    %% It should be a duration("1h"), but we set it as a percent.
    ?assertMatch({error, _Res}, emqx:update_config(KeyPath, <<"80%">>, Opts)),
    New = emqx:get_raw_config(KeyPath),
    ?assertEqual(Origin, New),
    ok = emqx_config_handler:remove_handler(KeyPath),
    ok.

t_stop(_Config) ->
    OldPid = erlang:whereis(emqx_config_handler),
    OldInfo = emqx_config_handler:info(),
    emqx_config_handler:stop(),
    NewPid = wait_for_new_pid(),
    NewInfo = emqx_config_handler:info(),
    ?assertNotEqual(OldPid, NewPid),
    ?assertEqual(OldInfo, NewInfo),
    ok.

t_callback_crash(_Config) ->
    CrashPath = [sysmon, os, cpu_high_watermark],
    Opts = #{rawconf_with_defaults => true},
    ok = emqx_config_handler:add_handler(CrashPath, ?MODULE),
    Old = emqx:get_raw_config(CrashPath),
    ?assertMatch(
        {error, {config_update_crashed, _}}, emqx:update_config(CrashPath, <<"89%">>, Opts)
    ),
    New = emqx:get_raw_config(CrashPath),
    ?assertEqual(Old, New),
    ok = emqx_config_handler:remove_handler(CrashPath),
    ok.

t_pre_callback_error(_Config) ->
    callback_error(
        [sysmon, os, mem_check_interval],
        <<"100s">>,
        {error, {pre_config_update, ?MODULE, pre_config_update_error}}
    ),
    ok.

t_post_update_error(_Config) ->
    callback_error(
        [sysmon, os, sysmem_high_watermark],
        <<"60%">>,
        {error, {post_config_update, ?MODULE, post_config_update_error}}
    ),
    ok.

t_handler_root() ->
    %% Don't rely on default emqx_config_handler's merge behaviour.
    RootKey = [],
    Opts = #{rawconf_with_defaults => true},
    ok = emqx_config_handler:add_handler(RootKey, ?MODULE),
    %% update
    Old = #{<<"sysmon">> := #{<<"os">> := OS}} = emqx:get_raw_config(RootKey),
    {ok, Res} = emqx:update_config(
        RootKey,
        Old#{<<"sysmon">> => #{<<"os">> => OS#{<<"cpu_check_interval">> => <<"12s">>}}},
        Opts
    ),
    ?assertMatch(
        #{
            config := #{os := #{cpu_check_interval := 12000}},
            post_config_update := #{?MODULE := ok},
            raw_config := #{<<"os">> := #{<<"cpu_check_interval">> := <<"12s">>}}
        },
        Res
    ),
    ?assertMatch(#{sysmon := #{os := #{cpu_check_interval := 12000}}}, emqx:get_config(RootKey)),
    ok = emqx_config_handler:remove_handler(RootKey),
    ok.

t_get_raw_cluster_override_conf(_Config) ->
    Raw0 = emqx_config:read_override_conf(#{override_to => cluster}),
    Raw1 = emqx_config_handler:get_raw_cluster_override_conf(),
    ?assertEqual(Raw0, Raw1),
    OldPid = erlang:whereis(emqx_config_handler),
    OldInfo = emqx_config_handler:info(),

    ?assertEqual(ok, gen_server:call(emqx_config_handler, bad_call_msg)),
    gen_server:cast(emqx_config_handler, bad_cast_msg),
    erlang:send(emqx_config_handler, bad_info_msg),

    NewPid = erlang:whereis(emqx_config_handler),
    NewInfo = emqx_config_handler:info(),
    ?assertEqual(OldPid, NewPid),
    ?assertEqual(OldInfo, NewInfo),
    ok.

t_save_config_failed(_Config) ->
    ok.

t_update_sub(_Config) ->
    PathKey = [sysmon],
    Opts = #{rawconf_with_defaults => true},
    ok = emqx_config_handler:add_handler(PathKey, ?MODULE),
    %% update sub key
    #{<<"os">> := OS1} = emqx:get_raw_config(PathKey),
    {ok, Res} = emqx:update_config(PathKey ++ [os, cpu_check_interval], <<"120s">>, Opts),
    ?assertMatch(
        #{
            config := 120000,
            post_config_update := #{?MODULE := ok},
            raw_config := <<"120s">>
        },
        Res
    ),
    ?assertMatch(#{os := #{cpu_check_interval := 120000}}, emqx:get_config(PathKey)),
    #{<<"os">> := OS2} = emqx:get_raw_config(PathKey),
    ?assertEqual(lists:sort(maps:keys(OS1)), lists:sort(maps:keys(OS2))),

    %% update sub key
    SubKey = PathKey ++ [os, cpu_high_watermark],
    ?assertEqual(
        {ok, #{
            config => 0.81,
            post_config_update => #{?MODULE => ok},
            raw_config => <<"81%">>
        }},
        emqx:update_config(SubKey, "81%", Opts)
    ),
    ?assertEqual(0.81, emqx:get_config(SubKey)),
    ?assertEqual("81%", emqx:get_raw_config(SubKey)),

    ok = emqx_config_handler:remove_handler(PathKey),
    ok.

pre_config_update([sysmon], UpdateReq, _RawConf) ->
    {ok, UpdateReq};
pre_config_update([sysmon, os], UpdateReq, _RawConf) ->
    {ok, UpdateReq};
pre_config_update([sysmon, os, cpu_check_interval], UpdateReq, _RawConf) ->
    {ok, UpdateReq};
pre_config_update([sysmon, os, cpu_low_watermark], UpdateReq, _RawConf) ->
    {ok, UpdateReq};
pre_config_update([sysmon, os, sysmem_high_watermark], UpdateReq, _RawConf) ->
    {ok, UpdateReq};
pre_config_update([sysmon, os, mem_check_interval], _UpdateReq, _RawConf) ->
    {error, pre_config_update_error}.

post_config_update([sysmon], _UpdateReq, _NewConf, _OldConf, _AppEnvs) ->
    {ok, ok};
post_config_update([sysmon, os], _UpdateReq, _NewConf, _OldConf, _AppEnvs) ->
    {ok, ok};
post_config_update([sysmon, os, cpu_check_interval], _UpdateReq, _NewConf, _OldConf, _AppEnvs) ->
    {ok, ok};
post_config_update([sysmon, os, cpu_low_watermark], _UpdateReq, _NewConf, _OldConf, _AppEnvs) ->
    ok;
post_config_update([sysmon, os, sysmem_high_watermark], _UpdateReq, _NewConf, _OldConf, _AppEnvs) ->
    {error, post_config_update_error}.

wait_for_new_pid() ->
    case erlang:whereis(emqx_config_handler) of
        undefined ->
            ct:sleep(10),
            wait_for_new_pid();
        Pid ->
            Pid
    end.

callback_error(FailedPath, Update, Error) ->
    Opts = #{rawconf_with_defaults => true},
    ok = emqx_config_handler:add_handler(FailedPath, ?MODULE),
    Old = emqx:get_raw_config(FailedPath),
    ?assertEqual(Error, emqx:update_config(FailedPath, Update, Opts)),
    New = emqx:get_raw_config(FailedPath),
    ?assertEqual(Old, New),
    ok = emqx_config_handler:remove_handler(FailedPath),
    ok.
