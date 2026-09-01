%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_audit_api_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/logger.hrl").

all() ->
    [
        {group, audit, [sequence]}
    ].

groups() ->
    [
        {audit, [sequence], common_tests()}
    ].

common_tests() ->
    emqx_common_test_helpers:all(?MODULE).

-define(CONF_DEFAULT, #{
    node =>
        #{
            name => "emqx1@127.0.0.1",
            cookie => "emqxsecretcookie",
            data_dir => "data"
        },
    log => #{
        audit =>
            #{
                enable => true,
                ignore_high_frequency_request => true,
                level => info,
                cache_size => 15,
                rotation_count => 2,
                rotation_size => "10MB",
                time_offset => "system"
            }
    }
}).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_ctl,
            emqx,
            {emqx_conf, #{
                config => ?CONF_DEFAULT,
                schema_mod => emqx_conf_schema
            }},
            emqx_modules,
            emqx_license,
            emqx_audit,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

t_http_api(_) ->
    process_flag(trap_exit, true),
    AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    StartAt = erlang:system_time(microsecond),
    {ok, Zones} = emqx_mgmt_api_configs_SUITE:get_global_zone(),
    NewZones = emqx_utils_maps:deep_put([<<"mqtt">>, <<"max_qos_allowed">>], Zones, 1),
    {ok, #{<<"mqtt">> := Res}} = emqx_mgmt_api_configs_SUITE:update_global_zone(NewZones),
    ?assertMatch(#{<<"max_qos_allowed">> := 1}, Res),
    Query =
        lists:flatten(
            io_lib:format(
                "from=rest_api&operation_id=/configs/global_zone&gte_created_at=~B&limit=1",
                [StartAt]
            )
        ),
    Res1 = wait_for_matching_audit_entry(AuditPath, Query, AuthHeader, 2000),
    ?assertMatch(
        #{
            <<"data">> := [
                #{
                    <<"from">> := <<"rest_api">>,
                    <<"operation_id">> := <<"/configs/global_zone">>,
                    <<"source_ip">> := <<"127.0.0.1">>,
                    <<"source">> := _,
                    <<"http_request">> := #{
                        <<"method">> := <<"put">>,
                        <<"body">> := _,
                        <<"bindings">> := _,
                        <<"headers">> := #{<<"authorization">> := <<"******">>}
                    },
                    <<"http_status_code">> := 200,
                    <<"operation_result">> := <<"success">>,
                    <<"operation_type">> := <<"configs">>
                }
            ]
        },
        emqx_utils_json:decode(Res1)
    ),
    ok.

-doc """
GET /audit must not 500 when a page includes a record written by an
SSO-authenticated user, whose `auth_meta.source` is `{Backend, Name}`
instead of a plain binary.
""".
t_http_api_sso_source(_) ->
    process_flag(trap_exit, true),
    SsoBackend = saml,
    SsoUser = <<"jackson-http@example.com">>,
    Desc = <<"desc">>,
    SsoUsername = ?SSO_USERNAME(SsoBackend, SsoUser),
    {ok, _} = emqx_dashboard_admin:add_sso_user(SsoBackend, SsoUser, ?ROLE_SUPERUSER, Desc),
    {ok, #{role := ?ROLE_SUPERUSER, token := SsoToken}} =
        emqx_dashboard_admin:sign_token(SsoUsername, <<>>),
    SsoAuthHeader = {"Authorization", "Bearer " ++ binary_to_list(SsoToken)},
    StartAt = erlang:system_time(microsecond),
    {ok, Zones} = emqx_mgmt_api_configs_SUITE:get_global_zone(),
    NewZones = emqx_utils_maps:deep_put([<<"mqtt">>, <<"max_qos_allowed">>], Zones, 1),
    ConfigsPath = emqx_mgmt_api_test_util:api_path(["configs", "global_zone"]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(
        put, ConfigsPath, "", SsoAuthHeader, NewZones
    ),
    AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Query =
        lists:flatten(
            io_lib:format(
                "from=dashboard&operation_id=/configs/global_zone&gte_created_at=~B&limit=1",
                [StartAt]
            )
        ),
    %% Before the fix, this GET fails with 500 (invalid json term) because
    %% `format/1` passed the tuple `source` straight to the JSON encoder.
    Res = wait_for_matching_audit_entry(AuditPath, Query, AuthHeader, 2000),
    ?assertMatch(
        #{
            <<"data">> := [
                #{
                    <<"operation_id">> := <<"/configs/global_zone">>,
                    <<"source">> := <<"saml:jackson-http@example.com">>
                }
            ]
        },
        emqx_utils_json:decode(Res)
    ),
    ok.

t_disabled(_) ->
    Enable = [log, audit, enable],
    ?assertEqual(true, emqx:get_config(Enable)),
    AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    {ok, _} = emqx_mgmt_api_test_util:request_api(get, AuditPath, "limit=1", AuthHeader),
    Size1 = mnesia:table_info(emqx_audit, size),

    {ok, Logs} = emqx_mgmt_api_configs_SUITE:get_config("log"),
    Logs1 = emqx_utils_maps:deep_put([<<"audit">>, <<"cache_size">>], Logs, 199),
    NewLogs = emqx_utils_maps:deep_put([<<"audit">>, <<"enable">>], Logs1, false),
    {ok, _} = emqx_mgmt_api_configs_SUITE:update_config("log", NewLogs),
    {ok, GetLog1} = emqx_mgmt_api_configs_SUITE:get_config("log"),
    ?assertEqual(NewLogs, GetLog1),
    ?assertMatch(
        {error, _},
        emqx_mgmt_api_test_util:request_api(get, AuditPath, "limit=1", AuthHeader)
    ),

    Size2 = mnesia:table_info(emqx_audit, size),
    %% Record the audit disable action, so the size + 1
    ?assertEqual(Size1 + 1, Size2),

    {ok, Zones} = emqx_mgmt_api_configs_SUITE:get_global_zone(),
    NewZones = emqx_utils_maps:deep_put([<<"mqtt">>, <<"max_topic_levels">>], Zones, 111),
    {ok, #{<<"mqtt">> := Res}} = emqx_mgmt_api_configs_SUITE:update_global_zone(NewZones),
    ?assertMatch(#{<<"max_topic_levels">> := 111}, Res),
    Size3 = mnesia:table_info(emqx_audit, size),
    %% Don't record mqtt update request.
    ?assertEqual(Size2, Size3),
    %% enabled again
    {ok, _} = emqx_mgmt_api_configs_SUITE:update_config("log", Logs1),
    {ok, GetLog2} = emqx_mgmt_api_configs_SUITE:get_config("log"),
    ?assertEqual(Logs1, GetLog2),
    Size4 = mnesia:table_info(emqx_audit, size),
    ?assertEqual(Size3 + 1, Size4),
    ok.

t_cli(_Config) ->
    Size = mnesia:table_info(emqx_audit, size),
    TimeInt = erlang:system_time(microsecond) - 1000,
    Time = integer_to_list(TimeInt),
    DateStr = calendar:system_time_to_rfc3339(TimeInt, [{unit, microsecond}]),
    Date = emqx_http_lib:uri_encode(DateStr),
    ok = emqx_ctl:run_command(["conf", "show", "log"]),
    AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Res} = emqx_mgmt_api_test_util:request_api(get, AuditPath, "limit=1", AuthHeader),
    #{<<"data">> := Data} = emqx_utils_json:decode(Res),
    ?assertMatch(
        [
            #{
                <<"from">> := <<"cli">>,
                <<"operation_id">> := <<"">>,
                <<"source_ip">> := <<"">>,
                <<"operation_type">> := <<"conf">>,
                <<"args">> := [<<"show">>, <<"log">>],
                <<"node">> := _,
                <<"source">> := <<"">>,
                <<"http_request">> := <<"">>
            }
        ],
        Data
    ),
    [ShowLogEntry] = Data,
    %% check create at is valid
    [#{<<"created_at">> := CreateAtRaw}] = Data,
    CreateAt = calendar:rfc3339_to_system_time(binary_to_list(CreateAtRaw), [{unit, microsecond}]),
    ?assert(CreateAt > TimeInt, CreateAtRaw),
    ?assert(CreateAt < TimeInt + 5000000, CreateAtRaw),
    %% check cli filter
    {ok, Res1} = emqx_mgmt_api_test_util:request_api(get, AuditPath, "from=cli", AuthHeader),
    #{<<"data">> := Data1} = emqx_utils_json:decode(Res1),
    ?assertMatch(
        [ShowLogEntry, #{<<"operation_type">> := <<"emqx">>, <<"args">> := [<<"start">>]}],
        Data1
    ),
    {ok, Res2} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "from=erlang_console", AuthHeader
    ),
    ?assertMatch(#{<<"data">> := []}, emqx_utils_json:decode(Res2)),

    %% check created_at filter microsecond
    {ok, Res3} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "gte_created_at=" ++ Time, AuthHeader
    ),
    #{<<"data">> := Data3} = emqx_utils_json:decode(Res3),
    ?assertEqual(1, erlang:length(Data3)),
    %% check created_at filter rfc3339
    {ok, Res31} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "gte_created_at=" ++ Date, AuthHeader
    ),
    ?assertEqual(Res3, Res31),
    %% check created_at filter millisecond
    TimeMs = integer_to_list(TimeInt div 1000),
    {ok, Res32} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "gte_created_at=" ++ TimeMs, AuthHeader
    ),
    ?assertEqual(Res3, Res32),

    %% check created_at filter microsecond
    {ok, Res4} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "lte_created_at=" ++ Time, AuthHeader
    ),
    #{<<"data">> := Data4} = emqx_utils_json:decode(Res4),
    ?assertEqual(Size, erlang:length(Data4)),

    %% check created_at filter rfc3339
    {ok, Res41} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "lte_created_at=" ++ Date, AuthHeader
    ),
    ?assertEqual(Res4, Res41),
    %% check created_at filter millisecond
    {ok, Res42} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "lte_created_at=" ++ TimeMs, AuthHeader
    ),
    ?assertEqual(Res4, Res42),

    %% check duration_ms filter
    {ok, Res5} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "gte_duration_ms=0", AuthHeader
    ),
    #{<<"data">> := Data5} = emqx_utils_json:decode(Res5),
    ?assertEqual(Size + 1, erlang:length(Data5)),
    {ok, Res6} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "lte_duration_ms=-1", AuthHeader
    ),
    ?assertMatch(#{<<"data">> := []}, emqx_utils_json:decode(Res6)),
    ok.

t_cli_redaction(_Config) ->
    Secret = "12345678901234567890123456789012",
    Name = "audit-redaction-key",
    ok = emqx_ctl:run_command([
        "api_keys",
        "add",
        "--name",
        Name,
        "--api-secret",
        Secret,
        "--role",
        "viewer"
    ]),
    AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Res} = emqx_mgmt_api_test_util:request_api(get, AuditPath, "limit=1", AuthHeader),
    ?assertMatch(
        #{
            <<"data">> := [
                #{
                    <<"operation_type">> := <<"api_keys">>,
                    <<"args">> := [
                        <<"add">>,
                        <<"--name">>,
                        <<"audit-redaction-key">>,
                        <<"--api-secret">>,
                        <<"******">>,
                        <<"--role">>,
                        <<"viewer">>
                    ]
                }
            ]
        },
        emqx_utils_json:decode(Res)
    ),
    {ok, _} = emqx_mgmt_auth:delete(iolist_to_binary(Name)),
    ok.

-doc """
Regression test for #18588: `bin/node_dump' used to drive the node through
`emqx eval', which is audited as `eval_erl' carrying the raw Erlang
expression. Running the `node_dump' CLI command must write ordinary `cli'
audit records identifying the sub-command, and must not write any
`eval_erl' record.
""".
t_node_dump(_Config) ->
    %% Need to explicitly load the commands because they are loaded by `emqx_machine'.
    ok = emqx_mgmt_cli:load(),
    %% Sub-command output content is covered by `emqx_mgmt_cli_SUITE:t_node_dump/1';
    %% this only needs each call to succeed, to trigger the audit write below.
    lists:foreach(
        fun(SubCmd) ->
            ?assertEqual(ok, emqx_ctl:run_command(["node_dump", SubCmd]), SubCmd)
        end,
        ["sys_info", "app_env", "conf"]
    ),
    AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Res} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "operation_type=node_dump&limit=10", AuthHeader
    ),
    #{<<"data">> := Data} = emqx_utils_json:decode(Res),
    ?assertEqual(3, length(Data)),
    lists:foreach(
        fun(#{<<"from">> := From, <<"operation_type">> := OpType}) ->
            ?assertEqual(<<"cli">>, From),
            ?assertEqual(<<"node_dump">>, OpType)
        end,
        Data
    ),
    ?assertEqual(
        lists:sort([<<"app_env">>, <<"conf">>, <<"sys_info">>]),
        lists:sort([Arg || #{<<"args">> := [Arg]} <- Data])
    ),
    %% no `eval_erl' record was written for any of the sub-commands
    {ok, ResEval} = emqx_mgmt_api_test_util:request_api(
        get, AuditPath, "operation_type=eval_erl", AuthHeader
    ),
    ?assertMatch(#{<<"data">> := []}, emqx_utils_json:decode(ResEval)),
    ok.

t_max_size(_Config) ->
    {ok, _} = emqx:update_config([log, audit, cache_size], 999),
    %% Make sure this process is using latest cache_size.
    ?assertEqual(ignore, gen_server:call(emqx_audit, whatever)),
    SizeFun =
        fun() ->
            AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
            AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
            Limit = "limit=1000",
            {ok, Res} = emqx_mgmt_api_test_util:request_api(get, AuditPath, Limit, AuthHeader),
            #{<<"data">> := Data} = emqx_utils_json:decode(Res),
            erlang:length(Data)
        end,
    InitSize = SizeFun(),
    lists:foreach(
        fun(_) ->
            ok = emqx_ctl:run_command(["conf", "show", "log"])
        end,
        lists:duplicate(100, 1)
    ),
    _ = mnesia:dump_log(),
    LogCount = wait_for_dirty_write_log_done(1500),
    Size1 = SizeFun(),
    ?assert(Size1 - InitSize >= 100, #{
        api => Size1,
        init => InitSize,
        log_size => LogCount,
        config => emqx:get_config([log, audit, cache_size])
    }),
    {ok, _} = emqx:update_config([log, audit, cache_size], 10),
    %% wait for clean_expired
    timer:sleep(250),
    ExpectSize = emqx:get_config([log, audit, cache_size]),
    Size2 = SizeFun(),
    ?assertEqual(ExpectSize, Size2, {sys:get_state(emqx_audit)}),
    ok.

-doc """
The audit log records the `POST /license` request body as `******`
so the license key does not appear in cleartext in `GET /audit`.
""".
t_license_key_redacted(_) ->
    StartAt = erlang:system_time(microsecond),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    LicensePath = emqx_mgmt_api_test_util:api_path(["license"]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(
        post, LicensePath, "", AuthHeader, #{<<"key">> => <<"default">>}
    ),
    AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
    Query =
        lists:flatten(
            io_lib:format(
                "operation_id=/license&gte_created_at=~B&limit=1",
                [StartAt]
            )
        ),
    Res = wait_for_matching_audit_entry(AuditPath, Query, AuthHeader, 2000),
    ?assertMatch(
        #{
            <<"data">> := [
                #{
                    <<"operation_id">> := <<"/license">>,
                    <<"http_request">> := #{<<"method">> := <<"post">>, <<"body">> := _}
                }
            ]
        },
        emqx_utils_json:decode(Res)
    ),
    #{<<"data">> := [#{<<"http_request">> := #{<<"body">> := AuditBody}}]} =
        emqx_utils_json:decode(Res),
    %% the recorded body is redacted: no license key, only the redaction mark
    ?assertEqual(nomatch, binary:match(AuditBody, <<"default">>), AuditBody),
    ?assertNotEqual(nomatch, binary:match(AuditBody, <<"******">>), AuditBody),
    ok.

-doc """
Regression test for #18534: an audit event that `to_audit/1' has no
clause for must not crash the ?AUDIT caller — audit events are
emitted from within cluster_rpc transactions and CLI commands, where
a raise breaks the audited operation itself.
""".
t_malformed_event_does_not_crash(_) ->
    ?assertEqual(true, emqx:get_config([log, audit, enable])),
    ?assertMatch({ok, _}, logger_config:get(logger, emqx_audit)),
    ?assertEqual(ok, ?AUDIT(info, #{from => bogus_from, cmd => bogus})),
    ok.

t_kickout_clients_without_log(_) ->
    process_flag(trap_exit, true),
    AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
    {ok, AuditLogs1} = emqx_mgmt_api_test_util:request_api(get, AuditPath),
    kickout_clients(),
    {ok, AuditLogs2} = emqx_mgmt_api_test_util:request_api(get, AuditPath),
    ?assertEqual(AuditLogs1, AuditLogs2),
    ok.

kickout_clients() ->
    ClientId1 = <<"client1">>,
    ClientId2 = <<"client2">>,
    ClientId3 = <<"client3">>,

    {ok, C1} = emqtt:start_link(#{
        clientid => ClientId1,
        proto_ver => v5,
        properties => #{'Session-Expiry-Interval' => 120}
    }),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link(#{clientid => ClientId2}),
    {ok, _} = emqtt:connect(C2),
    {ok, C3} = emqtt:start_link(#{clientid => ClientId3}),
    {ok, _} = emqtt:connect(C3),

    timer:sleep(300),

    %% get /clients
    ClientsPath = emqx_mgmt_api_test_util:api_path(["clients"]),
    {ok, Clients} = emqx_mgmt_api_test_util:request_api(get, ClientsPath),
    ClientsResponse = emqx_utils_json:decode(Clients),
    ClientsMeta = maps:get(<<"meta">>, ClientsResponse),
    ClientsPage = maps:get(<<"page">>, ClientsMeta),
    ClientsLimit = maps:get(<<"limit">>, ClientsMeta),
    ClientsCount = maps:get(<<"count">>, ClientsMeta),
    ?assertEqual(ClientsPage, 1),
    ?assertEqual(ClientsLimit, emqx_mgmt:default_row_limit()),
    ?assertEqual(ClientsCount, 3),

    %% kickout clients
    KickoutPath = emqx_mgmt_api_test_util:api_path(["clients", "kickout", "bulk"]),
    KickoutBody = [ClientId1, ClientId2, ClientId3],
    {ok, 204, _} = emqx_mgmt_api_test_util:request_api_with_body(post, KickoutPath, KickoutBody),

    {ok, Clients2} = emqx_mgmt_api_test_util:request_api(get, ClientsPath),
    ClientsResponse2 = emqx_utils_json:decode(Clients2),
    ?assertMatch(#{<<"data">> := []}, ClientsResponse2).

wait_for_dirty_write_log_done(MaxMs) ->
    Size = mnesia:table_info(emqx_audit, size),
    wait_for_dirty_write_log_done(Size, MaxMs).

wait_for_dirty_write_log_done(Size, RemainMs) when RemainMs =< 0 -> Size;
wait_for_dirty_write_log_done(Prev, RemainMs) ->
    SleepMs = 100,
    ct:sleep(SleepMs),
    case mnesia:table_info(emqx_audit, size) of
        Prev ->
            ct:sleep(SleepMs * 2),
            Prev;
        New ->
            wait_for_dirty_write_log_done(New, RemainMs - SleepMs)
    end.

wait_for_matching_audit_entry(_AuditPath, _Query, _AuthHeader, RemainMs) when RemainMs =< 0 ->
    ct:fail(audit_entry_not_found_in_time);
wait_for_matching_audit_entry(AuditPath, Query, AuthHeader, RemainMs) ->
    case emqx_mgmt_api_test_util:request_api(get, AuditPath, Query, AuthHeader) of
        {ok, Res} ->
            case emqx_utils_json:decode(Res) of
                #{<<"data">> := [_ | _]} ->
                    Res;
                _ ->
                    SleepMs = 100,
                    ct:sleep(SleepMs),
                    wait_for_matching_audit_entry(AuditPath, Query, AuthHeader, RemainMs - SleepMs)
            end;
        _ ->
            SleepMs = 100,
            ct:sleep(SleepMs),
            wait_for_matching_audit_entry(AuditPath, Query, AuthHeader, RemainMs - SleepMs)
    end.
