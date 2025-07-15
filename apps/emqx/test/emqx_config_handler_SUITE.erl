%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_config_handler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(MOD, '$mod').
-define(WKEY, '?').
-define(CLUSTER_CONF, "/tmp/cluster.conf").

-define(NS, <<"testns">>).
-define(global, global).
-define(namespace, namespace).

-define(pre_post_table, pre_post_table).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                after_start =>
                    fun() ->
                        ok = emqx_schema_hooks:inject_from_modules([?MODULE])
                    end
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    _ = file:delete(?CLUSTER_CONF),
    _ = ets:new(?pre_post_table, [named_table, bag, public]),
    maybe
        true ?= is_namespaced(Config),
        emqx_config:seed_defaults_for_all_roots_namespaced(emqx_schema, ?NS),
        on_exit(fun() -> ok = emqx_config:erase_namespaced_configs(?NS) end)
    end,
    Config.

end_per_testcase(_Case, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

injected_fields() ->
    #{
        'config.allowed_namespaced_roots' => [<<"sysmon">>]
    }.

update_config_opts(TCConfig) ->
    update_config_opts(TCConfig, _Overrides = #{}).
update_config_opts(TCConfig, Overrides) ->
    Opts = lists:foldl(
        fun
            (?namespace, Acc) ->
                Acc#{namespace => ?NS};
            (_Group, Acc) ->
                Acc
        end,
        #{},
        emqx_common_test_helpers:group_path(TCConfig)
    ),
    maps:merge(Opts, Overrides).

is_namespaced(TCConfig) ->
    case [true || ?namespace <- emqx_common_test_helpers:group_path(TCConfig)] of
        [] ->
            false;
        _ ->
            true
    end.

get_raw_config(TCConfig, KeyPath) when is_list(TCConfig) ->
    case is_namespaced(TCConfig) of
        true ->
            emqx:get_raw_namespaced_config(?NS, KeyPath);
        false ->
            emqx:get_raw_config(KeyPath)
    end.

get_raw_config(TCConfig, KeyPath, Default) when is_list(TCConfig) ->
    case is_namespaced(TCConfig) of
        true ->
            emqx:get_raw_namespaced_config(?NS, KeyPath, Default);
        false ->
            emqx:get_raw_config(KeyPath, Default)
    end.

get_config(TCConfig, KeyPath) when is_list(TCConfig) ->
    case is_namespaced(TCConfig) of
        true ->
            emqx:get_namespaced_config(?NS, KeyPath);
        false ->
            emqx:get_config(KeyPath)
    end.

remove_config(TCConfig, KeyPath) ->
    emqx:remove_config(KeyPath, update_config_opts(TCConfig)).

pre_config_update([sysmon], UpdateReq, _RawConf, ExtraContext) ->
    ets:insert(?pre_post_table, {pre, #{extra_context => ExtraContext}}),
    {ok, UpdateReq};
pre_config_update([sysmon, os], UpdateReq, _RawConf, _) ->
    {ok, UpdateReq};
pre_config_update([sysmon, os, cpu_check_interval], UpdateReq, _RawConf, _) ->
    {ok, UpdateReq};
pre_config_update([sysmon, os, cpu_low_watermark], UpdateReq, _RawConf, _) ->
    {ok, UpdateReq};
pre_config_update([sysmon, os, cpu_high_watermark], UpdateReq, _RawConf, _) ->
    {ok, UpdateReq};
pre_config_update([sysmon, os, sysmem_high_watermark], UpdateReq, _RawConf, _) ->
    {ok, UpdateReq};
pre_config_update([sysmon, os, mem_check_interval], _UpdateReq, _RawConf, _) ->
    {error, pre_config_update_error}.

propagated_pre_config_update(
    [<<"sysmon">>, <<"os">>, <<"cpu_check_interval">>],
    <<"333s">>,
    _RawConf,
    _ExtraContext
) ->
    {ok, <<"444s">>};
propagated_pre_config_update(
    [<<"sysmon">>, <<"os">>, <<"mem_check_interval">>],
    _UpdateReq,
    _RawConf,
    _ExtraContext
) ->
    {error, pre_config_update_error};
propagated_pre_config_update(_ConfKeyPath, _UpdateReq, _RawConf, ExtraContext) ->
    ets:insert(?pre_post_table, {propagated_pre, #{extra_context => ExtraContext}}),
    ok.

post_config_update(
    [sysmon], _UpdateReq, _NewConf, _OldConf, _AppEnvs, ExtraContext
) ->
    ets:insert(?pre_post_table, {post, #{extra_context => ExtraContext}}),
    {ok, ok};
post_config_update([sysmon, os], _UpdateReq, _NewConf, _OldConf, _AppEnvs, _) ->
    {ok, ok};
post_config_update(
    [sysmon, os, cpu_check_interval], _UpdateReq, _NewConf, _OldConf, _AppEnvs, _
) ->
    {ok, ok};
post_config_update([sysmon, os, cpu_low_watermark], _UpdateReq, _NewConf, _OldConf, _AppEnvs, _) ->
    ok;
post_config_update(
    [sysmon, os, cpu_high_watermark], _UpdateReq, _NewConf, _OldConf, _AppEnvs, _
) ->
    ok;
post_config_update(
    [sysmon, os, sysmem_high_watermark], _UpdateReq, _NewConf, _OldConf, _AppEnvs, _
) ->
    {error, post_config_update_error}.

propagated_post_config_update(
    [sysmon, os, sysmem_high_watermark],
    _UpdateReq,
    _NewConf,
    _OldConf,
    _AppEnvs,
    _ExtraContext
) ->
    {error, post_config_update_error};
propagated_post_config_update(
    _ConfKeyPath, _UpdateReq, _NewConf, _OldConf, _AppEnvs, ExtraContext
) ->
    ets:insert(?pre_post_table, {propagated_post, #{extra_context => ExtraContext}}),
    ok.

wait_for_new_pid() ->
    case erlang:whereis(emqx_config_handler) of
        undefined ->
            ct:sleep(10),
            wait_for_new_pid();
        Pid ->
            Pid
    end.

assert_update_result(TCConfig, FailedPath, Update, Expect) ->
    assert_update_result(TCConfig, [FailedPath], FailedPath, Update, Expect).

assert_update_result(TCConfig, Paths, UpdatePath, Update, Expect) ->
    with_update_result(TCConfig, Paths, UpdatePath, Update, fun(Old, Result) ->
        ?assertEqual(Expect, Result),
        New = get_raw_config(TCConfig, UpdatePath, undefined),
        ?assertEqual(Old, New)
    end).

with_update_result(TCConfig, Paths, UpdatePath, Update, Fun) ->
    ok = lists:foreach(
        fun(Path) -> emqx_config_handler:add_handler(Path, ?MODULE) end,
        Paths
    ),
    Opts = update_config_opts(TCConfig, #{rawconf_with_defaults => true}),
    Old = get_raw_config(TCConfig, UpdatePath, undefined),
    Result = emqx:update_config(UpdatePath, Update, Opts),
    _ = Fun(Old, Result),
    ok = lists:foreach(
        fun(Path) -> emqx_config_handler:remove_handler(Path) end,
        Paths
    ),
    ok.

add_handler(KeyPath, Module) ->
    Res = emqx_config_handler:add_handler(KeyPath, Module),
    on_exit(fun() -> emqx_config_handler:remove_handler(KeyPath) end),
    Res.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

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
    ct:pal("Key:~p wildcard1:\n ~p~n", [Wildcard1, Handlers3]),
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
        #{handlers := #{sysmon := #{?MOD := emqx_config_logger}}},
        emqx_config_handler:info()
    ),
    ok.

t_root_key_update() ->
    [{matrix, true}].
t_root_key_update(matrix) ->
    [[?global], [?namespace]];
t_root_key_update(TCConfig) when is_list(TCConfig) ->
    KeyPath = [sysmon],
    Opts = update_config_opts(TCConfig, #{rawconf_with_defaults => true}),
    ok = add_handler(KeyPath, ?MODULE),
    %% update
    Old = #{<<"os">> := OS} = get_raw_config(TCConfig, KeyPath),
    {ok, Res} = emqx:update_config(
        KeyPath,
        Old#{<<"os">> => OS#{<<"cpu_check_interval">> => <<"12s">>}},
        Opts
    ),
    ?assertMatch(
        #{
            config := #{os := #{cpu_check_interval := 12_000}},
            post_config_update := #{?MODULE := ok},
            raw_config := #{<<"os">> := #{<<"cpu_check_interval">> := <<"12s">>}}
        },
        Res
    ),
    ?assertMatch(#{os := #{cpu_check_interval := 12000}}, get_config(TCConfig, KeyPath)),

    %% update sub key
    SubKey = KeyPath ++ [os, cpu_high_watermark],
    ?assertMatch(
        {ok, #{
            config := 0.81,
            post_config_update := #{},
            raw_config := <<"81%">>
        }},
        emqx:update_config(SubKey, "81%", Opts)
    ),
    ?assertEqual(0.81, get_config(TCConfig, SubKey)),
    ?assertEqual("81%", get_raw_config(TCConfig, SubKey)),
    %% remove
    ?assertEqual({error, "remove_root_is_forbidden"}, remove_config(TCConfig, KeyPath)),
    ?assertMatch(true, is_map(get_raw_config(TCConfig, KeyPath))),

    ok.

t_sub_key_update_remove() ->
    [{matrix, true}].
t_sub_key_update_remove(matrix) ->
    [[?global], [?namespace]];
t_sub_key_update_remove(TCConfig) when is_list(TCConfig) ->
    KeyPath = [sysmon, os, cpu_check_interval],
    Opts = update_config_opts(TCConfig),
    ok = add_handler(KeyPath, ?MODULE),
    {ok, Res} = emqx:update_config(KeyPath, <<"60s">>, Opts),
    ?assertMatch(
        #{
            config := 60_000,
            post_config_update := #{?MODULE := ok},
            raw_config := <<"60s">>
        },
        Res
    ),
    ?assertMatch(60_000, get_config(TCConfig, KeyPath)),
    OriginalVal = get_raw_config(TCConfig, KeyPath),

    KeyPath2 = [sysmon, os, cpu_low_watermark],
    ok = add_handler(KeyPath2, ?MODULE),
    {ok, Res1} = emqx:update_config(KeyPath2, <<"40%">>, Opts),
    ?assertMatch(
        #{
            config := 0.4,
            post_config_update := #{},
            raw_config := <<"40%">>
        },
        Res1
    ),
    ?assertMatch(0.4, get_config(TCConfig, KeyPath2)),

    %% remove
    on_exit(fun() -> emqx:update_config(KeyPath, OriginalVal, Opts) end),
    ?assertMatch(
        {ok, #{post_config_update := #{?MODULE := ok}}},
        remove_config(TCConfig, KeyPath)
    ),
    try
        get_raw_config(TCConfig, KeyPath),
        ct:fail("should have raised a config_not_found error")
    catch
        error:{config_not_found, [<<"sysmon">>, os, cpu_check_interval]} ->
            %% Global config
            ok;
        error:{config_not_found, [<<"sysmon">>, <<"os">>, <<"cpu_check_interval">>]} ->
            %% Namespace config
            ok;
        K:E:S ->
            ct:fail("unexpected error:\n  ~p", [{K, E, S}])
    end,
    OSKey = maps:keys(get_raw_config(TCConfig, [sysmon, os])),
    ?assertEqual(false, lists:member(<<"cpu_check_interval">>, OSKey)),
    ?assert(length(OSKey) > 0),

    ok.

t_check_failed() ->
    [{matrix, true}].
t_check_failed(matrix) ->
    [[?global], [?namespace]];
t_check_failed(TCConfig) when is_list(TCConfig) ->
    KeyPath = [sysmon, os, cpu_check_interval],
    Opts = update_config_opts(TCConfig, #{rawconf_with_defaults => true}),
    Origin = get_raw_config(TCConfig, KeyPath),
    ok = add_handler(KeyPath, ?MODULE),
    %% It should be a duration("1h"), but we set it as a percent.
    ?assertMatch({error, _Res}, emqx:update_config(KeyPath, <<"80%">>, Opts)),
    New = get_raw_config(TCConfig, KeyPath),
    ?assertEqual(Origin, New),
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

t_callback_crash() ->
    [{matrix, true}].
t_callback_crash(matrix) ->
    [[?global], [?namespace]];
t_callback_crash(TCConfig) when is_list(TCConfig) ->
    CrashPath = [sysmon, os, procmem_high_watermark],
    Opts = update_config_opts(TCConfig, #{rawconf_with_defaults => true}),
    ok = add_handler(CrashPath, ?MODULE),
    Old = get_raw_config(TCConfig, CrashPath),
    ?assertMatch(
        {error, {config_update_crashed, _}}, emqx:update_config(CrashPath, <<"89%">>, Opts)
    ),
    New = get_raw_config(TCConfig, CrashPath),
    ?assertEqual(Old, New),
    ok.

t_pre_assert_update_result() ->
    [{matrix, true}].
t_pre_assert_update_result(matrix) ->
    [[?global], [?namespace]];
t_pre_assert_update_result(TCConfig) when is_list(TCConfig) ->
    assert_update_result(
        TCConfig,
        [sysmon, os, mem_check_interval],
        <<"100s">>,
        {error, {pre_config_update, ?MODULE, pre_config_update_error}}
    ),
    ok.

t_post_update_error() ->
    [{matrix, true}].
t_post_update_error(matrix) ->
    [[?global], [?namespace]];
t_post_update_error(TCConfig) when is_list(TCConfig) ->
    assert_update_result(
        TCConfig,
        [sysmon, os, sysmem_high_watermark],
        <<"60%">>,
        {error, {post_config_update, ?MODULE, post_config_update_error}}
    ),
    ok.

t_post_update_propagate_error_wkey() ->
    [{matrix, true}].
t_post_update_propagate_error_wkey(matrix) ->
    [[?global], [?namespace]];
t_post_update_propagate_error_wkey(TCConfig) when is_list(TCConfig) ->
    Conf0 = emqx_config:get_raw([sysmon]),
    Conf1 = emqx_utils_maps:deep_put([<<"os">>, <<"sysmem_high_watermark">>], Conf0, <<"60%">>),
    assert_update_result(
        TCConfig,
        [
            [sysmon, '?', sysmem_high_watermark],
            [sysmon]
        ],
        [sysmon],
        Conf1,
        {error, {post_config_update, ?MODULE, post_config_update_error}}
    ),
    ok.

t_post_update_propagate_error_key() ->
    [{matrix, true}].
t_post_update_propagate_error_key(matrix) ->
    [[?global], [?namespace]];
t_post_update_propagate_error_key(TCConfig) when is_list(TCConfig) ->
    Conf0 = emqx_config:get_raw([sysmon]),
    Conf1 = emqx_utils_maps:deep_put([<<"os">>, <<"sysmem_high_watermark">>], Conf0, <<"60%">>),
    assert_update_result(
        TCConfig,
        [
            [sysmon, os, sysmem_high_watermark],
            [sysmon]
        ],
        [sysmon],
        Conf1,
        {error, {post_config_update, ?MODULE, post_config_update_error}}
    ),
    ok.

t_pre_update_propagate_error_wkey() ->
    [{matrix, true}].
t_pre_update_propagate_error_wkey(matrix) ->
    [[?global], [?namespace]];
t_pre_update_propagate_error_wkey(TCConfig) when is_list(TCConfig) ->
    Conf0 = emqx_config:get_raw([sysmon]),
    Conf1 = emqx_utils_maps:deep_put([<<"os">>, <<"mem_check_interval">>], Conf0, <<"70s">>),
    assert_update_result(
        TCConfig,
        [
            [sysmon, '?', mem_check_interval],
            [sysmon]
        ],
        [sysmon],
        Conf1,
        {error, {pre_config_update, ?MODULE, pre_config_update_error}}
    ),
    ok.

t_pre_update_propagate_error_key() ->
    [{matrix, true}].
t_pre_update_propagate_error_key(matrix) ->
    [[?global], [?namespace]];
t_pre_update_propagate_error_key(TCConfig) when is_list(TCConfig) ->
    Conf0 = emqx_config:get_raw([sysmon]),
    Conf1 = emqx_utils_maps:deep_put([<<"os">>, <<"mem_check_interval">>], Conf0, <<"70s">>),
    assert_update_result(
        TCConfig,
        [
            [sysmon, os, mem_check_interval],
            [sysmon]
        ],
        [sysmon],
        Conf1,
        {error, {pre_config_update, ?MODULE, pre_config_update_error}}
    ),
    ok.

t_pre_update_propagate_key_rewrite() ->
    [{matrix, true}].
t_pre_update_propagate_key_rewrite(matrix) ->
    [[?global], [?namespace]];
t_pre_update_propagate_key_rewrite(TCConfig) when is_list(TCConfig) ->
    Conf0 = emqx_config:get_raw([sysmon]),
    Conf1 = emqx_utils_maps:deep_put([<<"os">>, <<"cpu_check_interval">>], Conf0, <<"333s">>),
    with_update_result(
        TCConfig,
        [
            [sysmon, '?', cpu_check_interval],
            [sysmon]
        ],
        [sysmon],
        Conf1,
        fun(_, Result) ->
            ?assertMatch(
                {ok, #{config := #{os := #{cpu_check_interval := 444000}}}},
                Result
            )
        end
    ),
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

t_independent_namespace_configs() ->
    [{matrix, true}].
t_independent_namespace_configs(matrix) ->
    [[?namespace]];
t_independent_namespace_configs(TCConfig) when is_list(TCConfig) ->
    OtherNS = <<"other_ns">>,
    on_exit(fun() -> ok = emqx_config:erase_namespaced_configs(OtherNS) end),
    emqx_config:seed_defaults_for_all_roots_namespaced(emqx_schema, OtherNS),
    KeyPath = [sysmon],
    ok = add_handler(KeyPath, ?MODULE),
    Wildcard = KeyPath ++ ['?', cpu_check_interval],
    ok = add_handler(Wildcard, ?MODULE),
    %% Initially, both are seeded with same defaults.
    ?assertEqual(
        emqx_config:get_namespaced(KeyPath, ?NS), emqx_config:get_namespaced(KeyPath, OtherNS)
    ),
    ?assertEqual(
        emqx_config:get_raw_namespaced(KeyPath, ?NS),
        emqx_config:get_raw_namespaced(KeyPath, OtherNS)
    ),
    %% They should update independently, and not affect globals as well.
    OriginalValGlobal = emqx_config:get(KeyPath),
    OriginalValRawGlobal = emqx_config:get_raw(KeyPath),
    OriginalVal = emqx_config:get_namespaced(KeyPath, ?NS),
    OriginalValRaw = emqx_config:get_raw_namespaced(KeyPath, ?NS),
    Val1 = emqx_utils_maps:deep_put(
        [<<"os">>, <<"cpu_check_interval">>], OriginalValRaw, <<"666s">>
    ),
    Opts = #{namespace => OtherNS},
    ?assertMatch(
        {ok, #{
            config := #{os := #{cpu_check_interval := 666_000}},
            raw_config := #{<<"os">> := #{<<"cpu_check_interval">> := <<"666s">>}}
        }},
        emqx:update_config(KeyPath, Val1, Opts)
    ),
    %% Global and other namespaces are untouched.
    ?assertEqual(OriginalValGlobal, emqx_config:get(KeyPath)),
    ?assertEqual(OriginalValRawGlobal, emqx_config:get_raw(KeyPath)),
    ?assertEqual(OriginalVal, emqx_config:get_namespaced(KeyPath, ?NS)),
    ?assertEqual(OriginalValRaw, emqx_config:get_raw_namespaced(KeyPath, ?NS)),
    %% Only target namespace is affected
    ?assertEqual(Val1, emqx_config:get_raw_namespaced(KeyPath, OtherNS)),
    ?assertEqual(666_000, emqx_config:get_namespaced(KeyPath ++ [os, cpu_check_interval], OtherNS)),
    KeyPathSpecific = KeyPath ++ [os, cpu_check_interval],
    KeyPathSpecificBin = lists:map(fun emqx_utils_conv:bin/1, KeyPathSpecific),
    %% Trigger propagated pre/post config update callbacks
    ?assertMatch(
        {ok, #{
            config := 432_000_000,
            raw_config := <<"5d">>
        }},
        emqx:update_config(KeyPathSpecific, <<"5d">>, Opts)
    ),
    %% Removing is also independent
    ?assertMatch({ok, _}, emqx:remove_config(KeyPathSpecific, Opts)),
    %% Global and other namespaces are untouched.
    ?assertEqual(OriginalValGlobal, emqx_config:get(KeyPath)),
    ?assertEqual(OriginalValRawGlobal, emqx_config:get_raw(KeyPath)),
    ?assertEqual(OriginalVal, emqx_config:get_namespaced(KeyPath, ?NS)),
    ?assertEqual(OriginalValRaw, emqx_config:get_raw_namespaced(KeyPath, ?NS)),
    %% Removed
    ?assertError(
        {config_not_found, KeyPathSpecificBin},
        emqx_config:get_raw_namespaced(KeyPathSpecific, OtherNS)
    ),
    %% Returns default again
    ?assertEqual(60_000, emqx_config:get_namespaced(KeyPathSpecific, OtherNS)),
    %% Pre/Post callbacks receive the namespace in extra context.
    ?assertMatch(
        [
            {post, #{extra_context := #{namespace := OtherNS}}},
            {pre, #{extra_context := #{namespace := OtherNS}}},
            {propagated_post, #{extra_context := #{namespace := OtherNS}}},
            {propagated_pre, #{extra_context := #{namespace := OtherNS}}}
        ],
        lists:sort(ets:tab2list(?pre_post_table))
    ),
    ok.
