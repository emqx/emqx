%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_doris_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("../src/emqx_bridge_doris.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("mysql/include/protocol.hrl").

%% -import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(HOST_PLAIN, <<"toxiproxy">>).
-define(HOST_TLS, <<"toxiproxy">>).
-define(PORT_PLAIN, 9030).
-define(PORT_TLS, 9130).
-define(USERNAME, <<"root">>).
-define(PROXY_NAME, "doris-fe").
-define(PROXY_NAME_TLS, "doris-fe-tls").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_cases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.

matrix_cases() ->
    lists:filter(
        fun(TestCase) ->
            get_tc_prop(TestCase, matrix, false)
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

groups() ->
    emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_doris,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT}
        | TCConfig
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:print(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    IsTLS = is_tls(TCConfig),
    {Port, ProxyName} =
        case IsTLS of
            true -> {?PORT_TLS, ?PROXY_NAME_TLS};
            false -> {?PORT_PLAIN, ?PROXY_NAME}
        end,
    Host = host(TCConfig),
    Server = server(Host, Port),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{<<"server">> => Server}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
    }),
    wait_for_be_ready(TCConfig),
    create_database(TCConfig),
    create_table(TCConfig),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {proxy_name, ProxyName}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    drop_table(TCConfig),
    ok.

is_tls(TCConfig) ->
    case [tls || tls <- group_path(TCConfig, [])] of
        [] ->
            false;
        _ ->
            true
    end.

host(TCConfig) when is_list(TCConfig) -> host(is_tls(TCConfig));
host(_IsTLS = true) -> ?HOST_TLS;
host(_IsTLS = false) -> ?HOST_PLAIN.

port(TCConfig) when is_list(TCConfig) -> port(is_tls(TCConfig));
port(_IsTLS = true) -> ?PORT_TLS;
port(_IsTLS = false) -> ?PORT_PLAIN.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

server(Host, Port) ->
    iolist_to_binary(io_lib:format("~s:~b", [Host, Port])).

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"database">> => <<"mqtt">>,
        <<"server">> => server(?HOST_PLAIN, ?PORT_PLAIN),
        <<"pool_size">> => 8,
        <<"username">> => ?USERNAME,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"parameters">> => #{
            <<"sql">> => action_sql()
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
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

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

create_table_ddl() ->
    <<
        "create table if not exists t_mqtt_msg "
        "(msgid varchar, topic string, qos tinyint, "
        "payload string, arrived datetime) "
        "properties (\"replication_num\" = \"1\")"
    >>.

action_sql() ->
    <<
        "insert into t_mqtt_msg(msgid, topic, qos, payload, arrived)"
        " values (${id}, ${topic}, ${qos}, ${payload},"
        " FROM_UNIXTIME(${timestamp}/1000))"
    >>.

eval_query(SQL, TCConfig) ->
    Opts = #{
        host => emqx_utils_conv:str(host(TCConfig)),
        port => port(TCConfig),
        user => ?USERNAME,
        basic_capabilities => #{?CLIENT_TRANSACTIONS => false}
    },
    {ok, C} = mysql:start_link(maps:to_list(Opts)),
    Res = mysql:query(C, iolist_to_binary(SQL)),
    mysql:stop(C),
    Res.

create_database(TCConfig) ->
    ok = eval_query(["create database if not exists mqtt"], TCConfig).

create_table(TCConfig) ->
    %% Even after `wait_for_be_ready/1`, the FE may briefly still reject the
    %% `CREATE TABLE` with "Failed to find enough backend" while it finishes
    %% accounting the freshly-reported BE disks.  This retry is the safety net
    %% that complements the readiness poll.
    ?retry(
        _Interval = 500,
        _Tries = 60,
        ok = eval_query(["use mqtt; ", create_table_ddl()], TCConfig)
    ).

%% Doris reports the BE as "healthy" (docker healthcheck / FE registration) as
%% soon as the BE process is up, but the BE only publishes its storage disks to
%% the FE asynchronously afterwards.  Issuing DDL before disks are published
%% fails with "Failed to find enough backend ... hdd disks count={}".  Wait
%% until at least one BE row reports both `Alive=true` and non-zero total
%% capacity before proceeding.
wait_for_be_ready(TCConfig) ->
    ?retry(
        _Interval = 500,
        _Tries = 60,
        ok = assert_be_ready(TCConfig)
    ).

assert_be_ready(TCConfig) ->
    case eval_query(<<"SHOW BACKENDS">>, TCConfig) of
        {ok, Cols, Rows} when Rows =/= [] ->
            case lists:any(fun(Row) -> is_be_ready(Cols, Row) end, Rows) of
                true -> ok;
                false -> error({be_not_ready, Rows})
            end;
        Other ->
            error({show_backends_failed, Other})
    end.

%% `SHOW BACKENDS` returns ~20 columns whose order varies across Doris
%% versions, so look the fields up by name rather than by index.
is_be_ready(Cols, Row) ->
    Fields = maps:from_list(lists:zip(Cols, Row)),
    is_true(maps:get(<<"Alive">>, Fields, undefined)) andalso
        has_capacity(maps:get(<<"TotalCapacity">>, Fields, undefined)).

is_true(<<"true">>) -> true;
is_true("true") -> true;
is_true(true) -> true;
is_true(_) -> false.

%% Capacity columns come back as human-readable strings such as
%% `<<"930.635 GB">>`; a BE that hasn't reported disks yet shows `<<"0.000 ">>`.
has_capacity(undefined) ->
    false;
has_capacity(Bin) ->
    case string:to_float(string:trim(iolist_to_binary([Bin]))) of
        {F, _} when F > 0.0 -> true;
        _ -> false
    end.

drop_table(TCConfig) ->
    ok = eval_query(["use mqtt; drop table t_mqtt_msg"], TCConfig).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [[plain], [tls]];
t_start_stop(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, "doris_connector_stop").

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [[plain], [tls]];
t_on_get_status(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => [connecting, disconnected]}).

t_rule_test_trace(Config) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(Config, Opts).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [[plain], [tls]];
t_rule_action(Config) when is_list(Config) ->
    PostPublishFn = fun(Context) ->
        #{rule_topic := RuleTopic, payload := Payload} = Context,
        QoS = 2,
        ?assertMatch(
            {ok, _ColNames, [[_MsgId, RuleTopic, QoS, Payload, {{_, _, _}, {_, _, _}}]]},
            eval_query(<<"use mqtt; select * from t_mqtt_msg">>, Config)
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(Config, Opts).

t_batch_rule_action(Config) ->
    ActionConfig0 = ?config(action_config, Config),
    ActionConfig = emqx_utils_maps:deep_merge(ActionConfig0, #{
        <<"parameters">> => #{
            <<"sql">> => <<
                "insert into t_mqtt_msg(msgid, topic, qos, payload, arrived)"
                " values (${id}, ${topic}, ${qos}, ${payload}, CURRENT_TIMESTAMP)"
            >>
        },
        <<"resource_opts">> => #{<<"batch_size">> => 2, <<"batch_time">> => 10}
    }),
    BatchConfig = [{action_config, ActionConfig} | proplists:delete(action_config, Config)],
    Payload = <<"a'b\\c", 0, 255>>,
    Expected = binary:encode_hex(Payload),
    emqx_bridge_v2_testlib:t_rule_action(BatchConfig, #{
        payload_fn => fun() -> Payload end,
        post_publish_fn => fun(_Context) ->
            ?retry(
                100,
                50,
                ?assertMatch(
                    {ok, _, [[Expected]]},
                    eval_query(<<"SELECT HEX(payload) FROM mqtt.t_mqtt_msg">>, Config)
                )
            )
        end
    }).

t_batch_values() ->
    [{matrix, true}].
t_batch_values(matrix) ->
    [[null_missing], [text_missing]];
t_batch_values(Config) when is_list(Config) ->
    AsNull = lists:member(null_missing, group_path(Config, [])),
    SQL =
        ~B"""
    INSERT INTO t_mqtt_msg(msgid, topic, qos, payload) VALUES (
        ${missing}, '${id}/${payload.n}/${missing}',
        CASE
            WHEN ${payload.n} IS NULL THEN 0
            WHEN NOT ${payload.n} <=> 2 THEN (${payload.n} + 1) * 3
            ELSE 1 + 2 * 3
        END,
        CONCAT(R'path\${payload.v}', '${$}{v}')
    )
    """,
    ?assertMatch(
        {ok, {{_, 201, _}, _, _}},
        emqx_bridge_v2_testlib:create_bridge_api(Config, #{
            <<"parameters">> => #{<<"sql">> => SQL, <<"undefined_vars_as_null">> => AsNull},
            <<"resource_opts">> => #{
                <<"batch_size">> => 3,
                <<"batch_time">> => 1000,
                <<"worker_pool_size">> => 1,
                <<"query_mode">> => <<"async">>
            }
        })
    ),
    Name = ?config(action_name, Config),
    _ = emqx_bridge_v2_testlib:kickoff_action_health_check(?ACTION_TYPE, Name),
    Unicode = unicode:characters_to_binary([16#E9, 16#1F642]),
    Messages = [
        #{id => <<"a">>, payload => #{n => 2, v => <<"a'b\\c">>}},
        #{id => <<"b">>, payload => emqx_utils_json:encode(#{n => 4, v => Unicode})},
        #{id => <<"c">>, payload => #{n => 0, v => <<0, 255>>}}
    ],
    {Missing, Text} =
        case AsNull of
            true -> {null, <<"null">>};
            false -> {<<"undefined">>, <<"undefined">>}
        end,
    ?check_trace(
        begin
            {_, {ok, _}} = ?wait_async_action(
                lists:foreach(
                    fun(Msg) ->
                        ?assertEqual(
                            ok,
                            emqx_bridge_v2:send_message(?global_ns, ?ACTION_TYPE, Name, Msg, #{})
                        )
                    end,
                    Messages
                ),
                #{?snk_kind := mysql_connector_on_batch_query_return, result := ok},
                10_000
            ),
            Expected = [
                [Missing, <<"a/2/", Text/binary>>, 7, binary:encode_hex(<<"path\\a'b\\c${v}">>)],
                [
                    Missing,
                    <<"b/4/", Text/binary>>,
                    15,
                    binary:encode_hex(<<"path\\", Unicode/binary, "${v}">>)
                ],
                [
                    Missing,
                    <<"c/0/", Text/binary>>,
                    3,
                    binary:encode_hex(<<"path\\", 0, 255, "${v}">>)
                ]
            ],
            ?assertMatch(
                {ok, _, Expected},
                eval_query(
                    <<"SELECT msgid, topic, qos, HEX(payload) FROM mqtt.t_mqtt_msg ORDER BY topic">>,
                    Config
                )
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{sql_func := query, data := no_params}],
                ?of_kind(mysql_connector_send_query, Trace)
            )
        end
    ).

t_batch_render_failure(Config) ->
    ?assertMatch(
        {ok, {{_, 201, _}, _, _}},
        emqx_bridge_v2_testlib:create_bridge_api(Config, #{
            <<"parameters">> => #{
                <<"sql">> => <<"INSERT INTO t_mqtt_msg(payload) VALUES ('value ${payload.v}')">>
            },
            <<"resource_opts">> => #{
                <<"batch_size">> => 2,
                <<"batch_time">> => 1000,
                <<"worker_pool_size">> => 1,
                <<"query_mode">> => <<"async">>
            }
        })
    ),
    Name = ?config(action_name, Config),
    _ = emqx_bridge_v2_testlib:kickoff_action_health_check(?ACTION_TYPE, Name),
    Send = fun(Value) ->
        ?assertEqual(
            ok,
            emqx_bridge_v2:send_message(
                ?global_ns, ?ACTION_TYPE, Name, #{payload => #{v => Value}}, #{}
            )
        )
    end,
    ?check_trace(
        begin
            {_, {ok, _}} = ?wait_async_action(
                begin
                    Send(1),
                    Send({unsupported})
                end,
                #{
                    ?snk_kind := mysql_connector_on_batch_query_return,
                    result :=
                        {error,
                            {unrecoverable_error,
                                {doris_template_render_failed, #{
                                    batch_index := 2,
                                    reason :=
                                        {invalid_sql_template_value, #{placeholder := "payload.v"}}
                                }}}}
                },
                10_000
            ),
            ?assertMatch(
                {ok, _, [[0]]}, eval_query(<<"SELECT COUNT(*) FROM mqtt.t_mqtt_msg">>, Config)
            )
        end,
        fun(Trace) -> ?assertEqual([], ?of_kind(mysql_connector_send_query, Trace)) end
    ),
    {_, {ok, _}} = ?wait_async_action(
        begin
            Send(2),
            Send(3)
        end,
        #{?snk_kind := mysql_connector_on_batch_query_return, result := ok},
        10_000
    ),
    ?assertMatch(
        {ok, _, [[<<"value 2">>], [<<"value 3">>]]},
        eval_query(<<"SELECT payload FROM mqtt.t_mqtt_msg ORDER BY payload">>, Config)
    ).

t_connection_initialization(Config) ->
    ok = meck:new(mysql, [passthrough, no_link]),
    emqx_common_test_helpers:on_exit(fun() -> meck:unload(mysql) end),
    ok = meck:expect(mysql, start_link, fun(Opts) ->
        Queries = [
            <<"SET SESSION sql_mode = 'ANSI_QUOTES,NO_BACKSLASH_ESCAPES'">>
            | proplists:get_value(queries, Opts, [])
        ],
        meck:passthrough([[{queries, Queries} | proplists:delete(queries, Opts)]])
    end),
    ConnectorConfig = emqx_utils_maps:deep_merge(?config(connector_config, Config), #{
        <<"pool_size">> => 1,
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"1h">>}
    }),
    {ok, {{_, 201, _}, _, _}} = emqx_bridge_v2_testlib:create_bridge_api([
        {connector_config, ConnectorConfig} | Config
    ]),
    Name = ?config(connector_name, Config),
    [{_, Worker}] = ecpool:workers(<<"connector:doris:", Name/binary>>),
    {ok, Conn} = ecpool_worker:client(Worker),
    assert_session_initialized(Conn),
    ok = mysql:stop(Conn),
    ?retry(
        200,
        100,
        begin
            {ok, NewConn} = ecpool_worker:client(Worker),
            ?assertNotEqual(Conn, NewConn),
            assert_session_initialized(NewConn)
        end
    ).

assert_session_initialized(Conn) ->
    ?assert(meck:called(mysql, query, [Conn, <<"SET enable_nereids_planner = true">>])),
    ?assert(
        meck:called(mysql, query, [Conn, <<"SET enable_fallback_to_original_planner = false">>])
    ),
    ?assertMatch(
        {ok, _, [[1, 0]]},
        mysql:query(
            Conn,
            <<
                "SELECT @@SESSION.enable_nereids_planner, "
                "@@SESSION.enable_fallback_to_original_planner"
            >>
        )
    ),
    {ok, _, [[Modes]]} = mysql:query(Conn, <<"SELECT @@SESSION.sql_mode">>),
    ?assertEqual(nomatch, binary:match(Modes, <<"ANSI_QUOTES">>)),
    ?assertEqual(nomatch, binary:match(Modes, <<"NO_BACKSLASH_ESCAPES">>)).

t_sql_compiler_roundtrip(Config) ->
    {ok, C} = mysql:start_link([
        {host, emqx_utils_conv:str(host(Config))},
        {port, port(Config)},
        {user, ?USERNAME},
        {basic_capabilities, #{?CLIENT_TRANSACTIONS => false}}
    ]),
    ok = mysql:query(C, <<"SET SESSION sql_mode = 'ANSI_QUOTES,NO_BACKSLASH_ESCAPES'">>),
    ok = emqx_mysql:prepare_sql_to_conn(C, [], fun emqx_bridge_doris_impl:prepare_conn/1),
    {ok, _, [[Modes]]} = mysql:query(C, <<"SELECT @@SESSION.sql_mode">>),
    ?assertEqual(nomatch, binary:match(Modes, <<"ANSI_QUOTES">>)),
    ?assertEqual(nomatch, binary:match(Modes, <<"NO_BACKSLASH_ESCAPES">>)),
    {ok, Plan} = emqx_sql_plan:compile(
        emqx_doris_sql,
        <<"INSERT INTO mqtt.t_mqtt_msg(payload) VALUES (${payload})">>
    ),
    Values = [
        <<"hello">>,
        <<"a'b\\c">>,
        <<"a", 0, "b">>,
        <<255>>,
        <<>>,
        <<"a''b\"c\\">>,
        <<"%_\\%\\_\\n\\0\\q">>,
        list_to_binary(lists:seq(0, 127)),
        unicode:characters_to_binary([16#E9, 16#4E2D, 16#1F642]),
        <<16#C3>>,
        <<16#C0, 16#80>>,
        <<16#ED, 16#A0, 16#80>>,
        <<16#F4, 16#90, 16#80, 16#80>>
    ],
    {ok, SQL} = emqx_sql_plan:render_batch(
        Plan, [#{payload => Value} || Value <- Values], #{}
    ),
    ?assertEqual(ok, mysql:query(C, iolist_to_binary(SQL))),
    {ok, _, Rows} = eval_query(<<"SELECT HEX(payload) FROM mqtt.t_mqtt_msg">>, Config),
    ?assertEqual(
        lists:sort([[binary:encode_hex(Value)] || Value <- Values]),
        lists:sort(Rows)
    ),
    {ok, TextPlan} = emqx_sql_plan:compile(
        emqx_doris_sql,
        <<"INSERT INTO mqtt.t_mqtt_msg(payload) VALUES ('prefix ${payload} suffix')">>
    ),
    {ok, TextSQL} = emqx_sql_plan:render_batch(
        TextPlan, [#{payload => Value} || Value <- Values], #{}
    ),
    ?assertEqual(ok, mysql:query(C, iolist_to_binary(TextSQL))),
    {ok, _, TextRows} = eval_query(<<"SELECT HEX(payload) FROM mqtt.t_mqtt_msg">>, Config),
    ?assertEqual(
        lists:sort(
            Rows ++ [[binary:encode_hex(<<"prefix ", V/binary, " suffix">>)] || V <- Values]
        ),
        lists:sort(TextRows)
    ),
    {ok, ExpressionPlan} = emqx_sql_plan:compile(
        emqx_doris_sql,
        ~B"""
        INSERT INTO mqtt.t_mqtt_msg(payload)
        VALUES (CONCAT('a''b', R'c\', ${value}))
        """
    ),
    {ok, ExpressionSQL} = emqx_sql_plan:render(ExpressionPlan, #{value => <<"d">>}, #{}),
    ?assertEqual(ok, mysql:query(C, iolist_to_binary(ExpressionSQL))),
    %% Ordinary literals keep server semantics; raw bodies keep their bytes.
    {ok, _, [[Expected]]} = eval_query(<<"SELECT HEX(CONCAT('a''b', 'c\\\\', 'd'))">>, Config),
    {ok, _, AllRows} = eval_query(<<"SELECT HEX(payload) FROM mqtt.t_mqtt_msg">>, Config),
    ?assertEqual(lists:sort([[Expected] | TextRows]), lists:sort(AllRows)),
    ok = mysql:stop(C).

t_escaped_dollar_roundtrip(Config) ->
    {ok, C} = mysql:start_link([
        {host, emqx_utils_conv:str(host(Config))},
        {port, port(Config)},
        {user, ?USERNAME},
        {basic_capabilities, #{?CLIENT_TRANSACTIONS => false}}
    ]),
    ok = emqx_mysql:prepare_sql_to_conn(C, [], fun emqx_bridge_doris_impl:prepare_conn/1),
    Cases = [
        {<<"${$}">>, <<"$">>},
        {<<"cost: ${$}{amount}">>, <<"cost: ${amount}">>},
        {<<"${$}${$}{amount}${$}">>, <<"$${amount}$">>},
        {<<"${$}{$}">>, <<"${$}">>},
        {<<"\\${$}{amount}">>, <<"${amount}">>},
        {<<"\\\\${$}{amount}">>, <<"\\${amount}">>},
        {<<"${$}{amount}${v}${$}{$}">>, <<"${amount}x${$}">>}
    ],
    ExpectedRows = [
        begin
            {ok, Plan} = emqx_sql_plan:compile(
                emqx_doris_sql,
                <<"INSERT INTO mqtt.t_mqtt_msg(payload) VALUES (", Quote, Body/binary, Quote, ")">>
            ),
            {ok, SQL} = emqx_sql_plan:render(Plan, #{v => <<"x">>, amount => 99}, #{}),
            ?assertEqual(ok, mysql:query(C, iolist_to_binary(SQL))),
            [Expected]
        end
     || Quote <- "'\"", {Body, Expected} <- Cases
    ],
    {ok, _, Rows} = mysql:query(C, <<"SELECT payload FROM mqtt.t_mqtt_msg">>),
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)),
    ok = mysql:stop(C).

t_raw_string_roundtrip(Config) ->
    {ok, C} = mysql:start_link([
        {host, emqx_utils_conv:str(host(Config))},
        {port, port(Config)},
        {user, ?USERNAME},
        {basic_capabilities, #{?CLIENT_TRANSACTIONS => false}}
    ]),
    ok = mysql:query(C, <<"SET SESSION sql_mode = 'ANSI_QUOTES,NO_BACKSLASH_ESCAPES'">>),
    ok = emqx_mysql:prepare_sql_to_conn(C, [], fun emqx_bridge_doris_impl:prepare_conn/1),
    %% Record native raw behavior separately from the compiler's byte-preserving semantics.
    Native = mysql:query(C, <<"SELECT HEX(R'a\\n'), HEX(r\"a\\n\"), HEX(R'a\\'), HEX(R'')">>),
    ct:pal("Native Doris raw literals: ~p", [Native]),
    ?assertMatch(
        {ok, _, [[<<"27610A">>, <<"22610A">>, <<"27615C">>, <<"27">>]]}, Native
    ),
    Values = [<<>>, <<"a'b\"c\\">>, <<0, 255>>, <<"\\n">>],
    Cases = [
        {<<>>, fun(_) -> <<>> end},
        {<<"static\\n${$}">>, fun(_) -> <<"static\\n$">> end},
        {<<"cost: ${$}{v}">>, fun(_) -> <<"cost: ${v}">> end},
        {<<"${$}${$}{v}${$}">>, fun(_) -> <<"$${v}$">> end},
        {<<"${$}{$}">>, fun(_) -> <<"${$}">> end},
        {<<"\\${$}{v}\\">>, fun(_) -> <<"\\${v}\\">> end},
        {<<0, 255, "${$}">>, fun(_) -> <<0, 255, "$">> end},
        {<<"${v}">>, fun(V) -> V end},
        {<<"${v}${v}">>, fun(V) -> <<V/binary, V/binary>> end},
        {<<"${$}${v}${$}">>, fun(V) -> <<"$", V/binary, "$">> end},
        {<<"\\${v}\\">>, fun(V) -> <<"\\", V/binary, "\\">> end},
        {<<"\\\\${v}\\n">>, fun(V) -> <<"\\\\", V/binary, "\\n">> end},
        {<<0, 255, "${v}">>, fun(V) -> <<0, 255, V/binary>> end},
        {<<"'${v}'">>, fun(V) -> <<"'", V/binary, "'">> end},
        {<<"\"${v}\"">>, fun(V) -> <<"\"", V/binary, "\"">> end}
    ],
    Expected = lists:append([
        begin
            {ok, Plan} = emqx_sql_plan:compile(
                emqx_doris_sql,
                <<"INSERT INTO mqtt.t_mqtt_msg(payload) VALUES (", R, Quote, Body/binary, Quote,
                    ")">>
            ),
            {ok, SQL} = emqx_sql_plan:render_batch(Plan, [#{v => V} || V <- Values], #{}),
            ?assertEqual(ok, mysql:query(C, iolist_to_binary(SQL)), {R, Quote, Body}),
            [[binary:encode_hex(ToExpected(V))] || V <- Values]
        end
     || R <- "Rr",
        Quote <- "'\"",
        {Body, ToExpected} <- Cases,
        binary:match(Body, <<Quote>>) =:= nomatch
    ]),
    {ok, _, Rows} = mysql:query(C, <<"SELECT HEX(payload) FROM mqtt.t_mqtt_msg">>),
    ?assertEqual(lists:sort(Expected), lists:sort(Rows)),
    ok = mysql:stop(C).
