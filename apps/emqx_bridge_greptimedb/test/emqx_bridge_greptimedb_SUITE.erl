%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, greptimedb).
-define(CONNECTOR_TYPE_BIN, <<"greptimedb">>).
-define(ACTION_TYPE, greptimedb).
-define(ACTION_TYPE_BIN, <<"greptimedb">>).

-define(PROXY_NAME_GRPC, "greptimedb_grpc").
%% -define(PROXY_NAME_TLS, "greptimedb_tls").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(tcp, tcp).
%% -define(tls, tls).
-define(async, async).
-define(sync, sync).
-define(with_batch, with_batch).
-define(without_batch, without_batch).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_greptimedb,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    EHttpcPoolName = <<(atom_to_binary(?MODULE))/binary, "_http">>,
    ok = start_ehttpc_pool(EHttpcPoolName),
    [
        {apps, Apps},
        {ehttpc_pool_name, EHttpcPoolName}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    reset_proxy(),
    Apps = get_config(apps, TCConfig),
    EHttpcPoolName = get_config(ehttpc_pool_name, TCConfig),
    ehttpc_sup:stop_pool(EHttpcPoolName),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(?tcp, TCConfig) ->
    [
        {server, <<"toxiproxy:4001">>},
        {enable_tls, false},
        {proxy_name, ?PROXY_NAME_GRPC},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT}
        | TCConfig
    ];
init_per_group(?async, TCConfig) ->
    [{query_mode, async} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, sync} | TCConfig];
init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{
        <<"server">> => get_config(server, TCConfig, <<"toxiproxy:4001">>),
        <<"ssl">> => #{<<"enable">> => get_config(enable_tls, TCConfig, false)}
    }),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>),
            <<"query_mode">> => get_config(query_mode, TCConfig, <<"sync">>)
        }
    }),
    clear_table(TCConfig),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:stop(),
    reset_proxy(),
    clear_table(TCConfig),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"server">> => <<"toxiproxy:4001">>,
        <<"dbname">> => <<"public">>,
        <<"username">> => <<"greptime_user">>,
        <<"password">> => <<"greptime_pwd">>,
        <<"ttl">> => <<"3 years">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"precision">> => <<"ns">>,
            <<"write_syntax">> => example_write_syntax()
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

example_write_syntax() ->
    %% N.B.: this single space character is relevant
    %% the measurement name should not be the topic
    %% from greptimedb:
    %% error => {error,{case_clause,{error,{<<"3">>,<<"Invalid%20table%20name:%20test/greptimedb">
    <<"mqtt,clientid=${clientid}", " ", "payload=${payload},",
        "${clientid}_int_value=${payload.int_key}i,",
        "uint_value=${payload.uint_key}u,"
        "float_value=${payload.float_key},", "undef_value=${payload.undef},",
        "${undef_key}=\"hard-coded-value\",", "bool=${payload.bool}">>.

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, TCConfig, Fn) ->
    ProxyName = get_config(proxy_name, TCConfig),
    emqx_common_test_helpers:with_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT, Fn).

start_ehttpc_pool(EHttpcPoolName) ->
    EHttpcTransport = tcp,
    EHttpcTransportOpts = [],
    EHttpcPoolOpts = [
        {host, "greptimedb"},
        {port, 4000},
        {pool_size, 1},
        {transport, EHttpcTransport},
        {transport_opts, EHttpcTransportOpts}
    ],
    {ok, _} = ehttpc_sup:start_pool(EHttpcPoolName, EHttpcPoolOpts),
    ok.

clear_table(TCConfig) ->
    query_by_sql(<<"drop table mqtt">>, TCConfig).

query_by_clientid(ClientId, TCConfig) ->
    SQL = <<"select * from \"mqtt\" where clientid='", ClientId/binary, "'">>,
    query_by_sql(SQL, TCConfig).

query_by_sql(SQL, TCConfig) ->
    GreptimedbHost = "toxiproxy",
    GreptimedbPort = 4000,
    EHttpcPoolName = get_config(ehttpc_pool_name, TCConfig),
    UseTLS = get_config(enable_tls, TCConfig, false),
    Path = <<"/v1/sql?db=public">>,
    Scheme =
        case UseTLS of
            true -> <<"https://">>;
            false -> <<"http://">>
        end,
    URI = iolist_to_binary([
        Scheme,
        list_to_binary(GreptimedbHost),
        ":",
        integer_to_binary(GreptimedbPort),
        Path
    ]),
    Headers = [
        {"Authorization", "Basic Z3JlcHRpbWVfdXNlcjpncmVwdGltZV9wd2Q="},
        {"Content-Type", "application/x-www-form-urlencoded"}
    ],
    Body = <<"sql=", SQL/binary>>,
    {ok, StatusCode, _Headers, RawBody0} =
        ehttpc:request(
            EHttpcPoolName,
            post,
            {URI, Headers, Body},
            _Timeout = 10_000,
            _Retry = 0
        ),

    case emqx_utils_json:decode(RawBody0) of
        #{
            <<"output">> := [
                #{
                    <<"records">> := #{
                        <<"rows">> := Rows,
                        <<"schema">> := Schema
                    }
                }
            ]
        } when StatusCode >= 200 andalso StatusCode =< 300 ->
            make_row(Schema, Rows);
        #{
            <<"code">> := _Code,
            <<"error">> := Error
        } when StatusCode > 300 ->
            %% TODO(dennis): check the error by code
            case binary:match(Error, <<"Table not found">>) of
                nomatch ->
                    {error, Error};
                _ ->
                    %% Table not found
                    #{}
            end;
        Error ->
            {error, Error}
    end.

make_row(null, _Rows) ->
    #{};
make_row(_Schema, []) ->
    #{};
make_row(#{<<"column_schemas">> := ColumnsSchemas}, [Row]) ->
    Columns = lists:map(fun(#{<<"name">> := Name}) -> Name end, ColumnsSchemas),
    maps:from_list(lists:zip(Columns, Row)).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_connector_api(TCConfig) ->
    #{connector_type := ConnectorType, connector_name := ConnectorName} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(
            ConnectorType, ConnectorName
        )
    ).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

json_encode(X) ->
    emqx_utils_json:encode(X).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    %% `greptimedb_worker' leaks atoms...  pids become atoms 🫠
    Opts = #{skip_atom_leak_check => true},
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, greptimedb_client_stopped, Opts).

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [?tcp, Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    StartClientOpts = #{
        clean_start => true,
        proto_ver => v5,
        clientid => ClientId
    },
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?retry(
            200,
            10,
            ?assertMatch(
                #{<<"payload">> := Payload},
                query_by_clientid(ClientId, TCConfig)
            )
        ),
        %% assert table options
        SQL =
            <<
                "SELECT create_options FROM information_schema.tables "
                "WHERE table_name='mqtt'"
            >>,
        TableOpts = query_by_sql(SQL, TCConfig),
        ?assertEqual(<<"ttl=3years">>, maps:get(<<"create_options">>, TableOpts)),
        ok
    end,
    TraceChecker = fun(Trace0) ->
        Trace = ?of_kind(greptimedb_connector_send_query, Trace0),
        ?assertMatch([#{points := [_]}], Trace),
        [#{points := [Point0]}] = Trace,
        {Measurement, [Point]} = Point0,
        ct:pal("sent point: ~p", [Point]),
        ?assertMatch(#{dbname := _, table := _, timeunit := _}, Measurement),
        ?assertMatch(
            #{
                fields := #{},
                tags := #{},
                timestamp := TS
            } when is_integer(TS),
            Point
        ),
        #{fields := Fields} = Point,
        ?assert(lists:all(fun is_binary/1, maps:keys(Fields))),
        ?assertNot(maps:is_key(<<"undefined">>, Fields)),
        ?assertNot(maps:is_key(<<"undef_value">>, Fields)),
        #{tags := Tags} = Point,
        ?assert(lists:all(fun is_binary/1, maps:keys(Tags))),
        ?assert(maps:is_key(<<"clientid">>, Tags)),
        ?assertMatch(
            #{
                value_data := {string_value, Val}
            } when is_binary(Val),
            maps:get(<<"clientid">>, Tags)
        ),
        ok
    end,
    Opts = #{
        trace_checkers => [TraceChecker],
        start_client_opts => StartClientOpts,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_start_ok_timestamp_write_syntax(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    WriteSyntax =
        %% N.B.: this single space characters are relevant
        <<"mqtt,clientid=${clientid}", " ", "payload=${payload},",
            "${clientid}_int_value=${payload.int_key}i,",
            "uint_value=${payload.uint_key}u,"
            "bool=${payload.bool}", " ", "${timestamp}">>,
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{<<"write_syntax">> => WriteSyntax}
        })
    ),
    ok.

t_start_ok_no_subject_tags_write_syntax(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    WriteSyntax =
        %% N.B.: this single space characters are relevant
        <<"mqtt", " ", "payload=${payload},", "${clientid}_int_value=${payload.int_key}i,",
            "uint_value=${payload.uint_key}u,"
            "bool=${payload.bool}", " ", "${timestamp}">>,
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{<<"write_syntax">> => WriteSyntax}
        })
    ),
    ok.

t_const_timestamp(TCConfig) ->
    Const = erlang:system_time(nanosecond),
    ConstBin = integer_to_binary(Const),
    WriteSyntax = <<"mqtt,clientid=${clientid} foo=${payload.foo}i,bar=5i ", ConstBin/binary>>,
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{<<"write_syntax">> => WriteSyntax}
        })
    ),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    C = start_client(#{clientid => ClientId}),
    Payload = json_encode(#{<<"foo">> => 123}),
    emqtt:publish(C, Topic, Payload),
    ?retry(200, 10, begin
        PersistedData = query_by_clientid(ClientId, TCConfig),
        ?assertMatch(
            #{<<"foo">> := 123, <<"greptime_timestamp">> := Const},
            PersistedData,
            #{const => Const}
        ),
        ok
    end),
    ok.

%% Smoke test for using `ts_column`.
t_ts_column(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        <<"ts_column">> => <<"event_time">>
    }),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = start_client(#{clientid => ClientId}),
    emqtt:publish(C, Topic, <<"hey">>),
    ?retry(
        200,
        10,
        ?assertMatch(
            #{<<"event_time">> := _},
            query_by_clientid(ClientId, TCConfig)
        )
    ),
    ok.

t_boolean_variants(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    C = start_client(#{clientid => ClientId}),
    BoolVariants = #{
        true => true,
        false => false,
        <<"t">> => true,
        <<"f">> => false,
        <<"T">> => true,
        <<"F">> => false,
        <<"TRUE">> => true,
        <<"FALSE">> => false,
        <<"True">> => true,
        <<"False">> => false
    },
    maps:foreach(
        fun(BoolVariant, Translation) ->
            Payload = json_encode(#{
                int_key => -123,
                bool => BoolVariant,
                uint_key => 123
            }),
            emqtt:publish(C, Topic, Payload),
            ?retry(
                _Sleep2 = 500,
                _Attempts2 = 20,
                ?assertMatch(
                    #{
                        <<"bool">> := Translation
                    },
                    query_by_clientid(ClientId, TCConfig),
                    #{variant => {BoolVariant, Translation}}
                )
            ),
            clear_table(TCConfig)
        end,
        BoolVariants
    ),
    ok.

t_bad_timestamp() ->
    [{matrix, true}].
t_bad_timestamp(matrix) ->
    [
        [?tcp, Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_bad_timestamp(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    WriteSyntax =
        %% N.B.: this single space characters are relevant
        <<"mqtt", " ", "payload=${payload},", "${clientid}_int_value=${payload.int_key}i,",
            "uint_value=${payload.uint_key}u,"
            "bool=${payload.bool}", " ", "bad_timestamp">>,
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"write_syntax">> => WriteSyntax}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    C = start_client(#{clientid => ClientId}),
    Payload = json_encode(#{
        int_key => -123,
        bool => false,
        uint_key => 123
    }),
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                emqtt:publish(C, Topic, Payload, [{qos, 1}]),
                #{?snk_kind := greptimedb_connector_send_query_error},
                10_000
            ),
        fun(Trace) ->
            IsBatch = get_config(batch_size, TCConfig, 1) > 1,
            case IsBatch of
                true ->
                    ?assertMatch(
                        [#{error := points_trans_failed}],
                        ?of_kind(greptimedb_connector_send_query_error, Trace)
                    );
                false ->
                    ?assertMatch(
                        [
                            #{
                                error := [
                                    {error, {bad_timestamp, <<"bad_timestamp">>}}
                                ]
                            }
                        ],
                        ?of_kind(greptimedb_connector_send_query_error, Trace)
                    )
            end,
            ok
        end
    ),
    ok.

t_create_disconnected() ->
    [{matrix, true}].
t_create_disconnected(matrix) ->
    [[?tcp]];
t_create_disconnected(TCConfig) ->
    ?check_trace(
        with_failure(down, TCConfig, fun() ->
            {201, #{<<"status">> := <<"disconnected">>}} = create_connector_api(TCConfig, #{})
        end),
        fun(Trace) ->
            ?assertMatch(
                [#{error := greptimedb_client_not_alive, reason := _SomeReason}],
                ?of_kind(greptimedb_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(TCConfig)
        )
    ),
    ok.

t_start_error(TCConfig) ->
    %% simulate client start error
    ?check_trace(
        emqx_common_test_helpers:with_mock(
            greptimedb,
            start_client,
            fun(_Config) -> {error, some_error} end,
            fun() ->
                {{201, _}, {ok, _}} =
                    ?wait_async_action(
                        create_connector_api(TCConfig, #{}),
                        #{?snk_kind := greptimedb_connector_start_failed},
                        10_000
                    )
            end
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{error := some_error}],
                ?of_kind(greptimedb_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    ok.

t_start_exception(TCConfig) ->
    %% simulate client start exception
    ?check_trace(
        emqx_common_test_helpers:with_mock(
            greptimedb,
            start_client,
            fun(_Config) -> error(boom) end,
            fun() ->
                {{201, _}, {ok, _}} =
                    ?wait_async_action(
                        create_connector_api(TCConfig, #{}),
                        #{?snk_kind := greptimedb_connector_start_exception},
                        10_000
                    )
            end
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{error := {error, boom}}],
                ?of_kind(greptimedb_connector_start_exception, Trace)
            ),
            ok
        end
    ),
    ok.

t_missing_field() ->
    [{matrix, true}].
t_missing_field(matrix) ->
    [
        [?tcp, ?sync, Batch]
     || Batch <- [?without_batch, ?with_batch]
    ];
t_missing_field(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    IsBatch = BatchSize > 1,
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"write_syntax">> => <<"mqtt,clientid=${clientid} foo=${foo}i">>},
        <<"resource_opts">> => #{<<"worker_pool_size">> => 1}
    }),
    %% note: we don't select foo here, but we interpolate it in the
    %% fields, so it'll become undefined.
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    ClientId0 = emqx_guid:to_hexstr(emqx_guid:gen()),
    ClientId1 = emqx_guid:to_hexstr(emqx_guid:gen()),
    %% Message with the field that we "forgot" to select in the rule
    Msg0 = emqx_message:make(ClientId0, Topic, emqx_utils_json:encode(#{foo => 123})),
    %% Message without any fields
    Msg1 = emqx_message:make(ClientId1, Topic, emqx_utils_json:encode(#{})),
    ?check_trace(
        begin
            emqx:publish(Msg0),
            emqx:publish(Msg1),
            NEvents = 1,
            {ok, _} =
                snabbkaffe:block_until(
                    ?match_n_events(NEvents, #{
                        ?snk_kind := greptimedb_connector_send_query_error
                    }),
                    _Timeout1 = 16_000
                ),
            ok
        end,
        fun(Trace) ->
            PersistedData0 = query_by_clientid(ClientId0, TCConfig),
            PersistedData1 = query_by_clientid(ClientId1, TCConfig),
            case IsBatch of
                true ->
                    ?assertMatch(
                        [#{error := points_trans_failed} | _],
                        ?of_kind(greptimedb_connector_send_query_error, Trace)
                    );
                false ->
                    ?assertMatch(
                        [#{error := [{error, no_fields}]} | _],
                        ?of_kind(greptimedb_connector_send_query_error, Trace)
                    )
            end,
            %% nothing should have been persisted
            ?assertEqual(#{}, PersistedData0),
            ?assertEqual(#{}, PersistedData1),
            ok
        end
    ),
    ok.

t_authentication_error_on_send_message(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        <<"password">> => <<"wrong_password">>
    }),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = json_encode(#{
        int_key => -123,
        bool => true,
        float_key => 24.5,
        uint_key => 123
    }),
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                emqtt:publish(C, Topic, Payload, [{qos, 1}]),
                #{?snk_kind := greptimedb_connector_do_query_failure},
                1_000
            ),
        fun(Trace) ->
            ?assertMatch(
                [#{error := <<"authorization failure">>} | _],
                ?of_kind(greptimedb_connector_do_query_failure, Trace)
            ),
            ok
        end
    ),
    ok.
