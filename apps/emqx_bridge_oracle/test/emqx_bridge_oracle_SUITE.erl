%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_oracle_SUITE).

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

-define(CONNECTOR_TYPE, oracle).
-define(CONNECTOR_TYPE_BIN, <<"oracle">>).
-define(ACTION_TYPE, oracle).
-define(ACTION_TYPE_BIN, <<"oracle">>).

-define(PROXY_NAME, "oracle").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(SID, "XE").

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
            emqx_bridge_oracle,
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
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME}
        | TCConfig
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>),
            <<"query_mode">> => get_config(query_mode, TCConfig, <<"sync">>)
        }
    }),
    reset_table(TCConfig),
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

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
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
        <<"sid">> => bin(?SID),
        <<"server">> => <<"toxiproxy.emqx.net:1521">>,
        <<"username">> => <<"system">>,
        <<"password">> => <<"oracle">>,
        <<"pool_size">> => 1,
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
            <<"sql">> => bin(sql_insert_template_for_bridge())
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

bin(X) -> emqx_utils_conv:bin(X).
str(X) -> emqx_utils_conv:str(X).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

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

sql_insert_template_for_bridge() ->
    "INSERT INTO mqtt_test(topic, msgid, payload, retain) VALUES (${topic}, ${id}, ${payload}, ${retain})".

sql_insert_template_with_nested_token_for_bridge() ->
    "INSERT INTO mqtt_test(topic, msgid, payload, retain) VALUES (${topic}, ${id}, ${payload.msg}, ${retain})".

sql_insert_template_with_large_value() ->
    "INSERT INTO mqtt_test(topic, msgid, payload, retain) VALUES (${topic}, ${id}, ${payload}, ${id})".

sql_insert_template_with_null_value() ->
    "INSERT INTO mqtt_test(topic, msgid, payload, retain) VALUES (${topic}, ${id}, ${payload.nullkey}, ${retain})".

sql_insert_template_with_inconsistent_datatype() ->
    "INSERT INTO mqtt_test(topic, msgid, payload, retain) VALUES (${topic}, ${id}, ${payload}, ${flags})".

sql_create_table() ->
    "CREATE TABLE mqtt_test (topic VARCHAR2(255), msgid VARCHAR2(64), payload NCLOB, retain NUMBER(1))".

sql_drop_table() ->
    "BEGIN\n"
    "        EXECUTE IMMEDIATE 'DROP TABLE mqtt_test';\n"
    "     EXCEPTION\n"
    "        WHEN OTHERS THEN\n"
    "            IF SQLCODE = -942 THEN\n"
    "                NULL;\n"
    "            ELSE\n"
    "                RAISE;\n"
    "            END IF;\n"
    "     END;".

sql_check_table_exist() ->
    "SELECT COUNT(*) FROM user_tables WHERE table_name = 'MQTT_TEST'".

new_jamdb_connection(Config) ->
    JamdbOpts = [
        {host, get_config(oracle_host, Config, "toxiproxy.emqx.net")},
        {port, get_config(oracle_port, Config, 1521)},
        {user, "system"},
        {password, "oracle"},
        {sid, ?SID}
    ],
    jamdb_oracle:start(JamdbOpts).

close_jamdb_connection(Conn) ->
    jamdb_oracle:stop(Conn).

reset_table(Config) ->
    {ok, Conn} = new_jamdb_connection(Config),
    try
        ok = drop_table_if_exists(Conn),
        {ok, [{proc_result, 0, _}]} = jamdb_oracle:sql_query(Conn, sql_create_table())
    after
        close_jamdb_connection(Conn)
    end,
    ok.

drop_table_if_exists(Conn) when is_pid(Conn) ->
    {ok, [{proc_result, 0, _}]} = jamdb_oracle:sql_query(Conn, sql_drop_table()),
    ok;
drop_table_if_exists(Config) ->
    {ok, Conn} = new_jamdb_connection(Config),
    try
        ok = drop_table_if_exists(Conn)
    after
        close_jamdb_connection(Conn)
    end,
    ok.

scan_table(TCConfig) ->
    {ok, Conn} = new_jamdb_connection(TCConfig),
    try
        jamdb_oracle:sql_query(Conn, "select * from mqtt_test")
    after
        close_jamdb_connection(Conn)
    end.

probe_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:probe_bridge_api(TCConfig, Overrides)
    ).

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_connector_api2(Config, Overrides).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_action_api2(Config, Overrides).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, oracle_bridge_stopped).

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [[?without_batch], [?with_batch]];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        PayloadStr = str(Payload),
        ?retry(
            200,
            20,
            ?assertMatch(
                {ok, [{result_set, _ColNames, _, [[_, _MsgId, PayloadStr, _]]}]},
                scan_table(TCConfig),
                #{payload => Payload}
            )
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_update_with_invalid_prepare(TCConfig) ->
    Opts = #{
        bad_sql =>
            %% retainx is a bad column name
            <<
                "INSERT INTO mqtt_test(topic, msgid, payload, retainx) "
                "VALUES (${topic}, ${id}, ${payload}, ${retain})"
            >>,
        reconnect_cb => {emqx_oracle, prepare_sql_to_conn},
        get_sig_fn => fun emqx_postgresql:get_reconnect_callback_signature/1,
        check_expected_error_fn => fun(Error) ->
            case re:run(Error, <<"unhealthy_target">>, [{capture, none}]) of
                match ->
                    ok;
                nomatch ->
                    ct:fail(#{
                        expected_pattern => "undefined_column",
                        got => Error
                    })
            end
        end
    },
    emqx_bridge_v2_testlib:t_update_with_invalid_prepare(TCConfig, Opts),
    ok.

t_probe_with_nested_tokens(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {204, _},
        probe_action_api(
            TCConfig,
            #{
                <<"parameters">> => #{
                    <<"sql">> => sql_insert_template_with_nested_token_for_bridge()
                }
            }
        )
    ).

t_probe_with_large_value(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {204, _},
        probe_action_api(
            TCConfig,
            #{<<"parameters">> => #{<<"sql">> => sql_insert_template_with_large_value()}}
        )
    ).

t_probe_with_inconsistent_datatype(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {204, _},
        probe_action_api(
            TCConfig,
            #{<<"parameters">> => #{<<"sql">> => sql_insert_template_with_inconsistent_datatype()}}
        )
    ).

t_no_sid_nor_service_name(TCConfig0) ->
    TCConfig = emqx_bridge_v2_testlib:proplist_update(
        TCConfig0,
        connector_config,
        fun(Cfg0) ->
            {_, Cfg} = maps:take(<<"sid">>, Cfg0),
            Cfg
        end
    ),
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"reason">> := <<"neither SID nor Service Name was set">>,
                %% should be censored as it contains secrets
                <<"value">> := #{<<"password">> := <<"******">>}
            }
        }},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"reason">> := <<"neither SID nor Service Name was set">>,
                %% should be censored as it contains secrets
                <<"value">> := #{<<"password">> := <<"******">>}
            }
        }},
        emqx_bridge_v2_testlib:probe_connector_api2(TCConfig, #{})
    ),
    ok.

t_table_removed(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            ct:pal("dropping table"),
            drop_table_if_exists(TCConfig),
            Payload = unique_payload(),
            ct:pal("sending query"),
            {_, {ok, Event}} =
                ?wait_async_action(
                    emqtt:publish(C, Topic, Payload, [{qos, 1}]),
                    #{?snk_kind := buffer_worker_flush_ack},
                    15_000
                ),
            ?assertMatch(
                #{result := {error, {unrecoverable_error, _}}},
                Event
            ),
            ok
        end,
        []
    ),
    ok.

t_missing_table(TCConfig) ->
    ?check_trace(
        begin
            drop_table_if_exists(TCConfig),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{}),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"status">> := Status,
                        <<"status_reason">> := <<"{unhealthy_target,", _/binary>>
                    }} when
                        Status == <<"connecting">> orelse Status == <<"disconnected">>,
                    get_action_api(TCConfig)
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_message_with_null_value(TCConfig) ->
    reset_table(TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"sql">> => sql_insert_template_with_null_value()}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    %% no nullkey in payload
    Payload = emqx_utils_json:encode(#{}),
    emqtt:publish(C, Topic, Payload),
    ?retry(
        _Sleep = 200,
        _Attempts = 20,
        ?assertMatch(
            {ok, [{result_set, _, _, [[_, _, null, _]]}]},
            scan_table(TCConfig)
        )
    ),
    ok.

t_message_with_nested_tokens(TCConfig) ->
    reset_table(TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"sql">> => sql_insert_template_with_nested_token_for_bridge()}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = emqx_utils_json:encode(#{<<"msg">> => <<"hello">>}),
    emqtt:publish(C, Topic, Payload),
    ?retry(
        _Sleep = 200,
        _Attempts = 20,
        ?assertMatch(
            {ok, [{result_set, _, _, [[_, _, "hello", _]]}]},
            scan_table(TCConfig)
        )
    ),
    ok.
