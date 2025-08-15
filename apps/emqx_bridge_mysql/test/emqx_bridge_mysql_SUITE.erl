%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mysql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, mysql).
-define(CONNECTOR_TYPE_BIN, <<"mysql">>).
-define(ACTION_TYPE, mysql).
-define(ACTION_TYPE_BIN, <<"mysql">>).

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(DATABASE, <<"mqtt">>).
-define(USERNAME, <<"root">>).
-define(PASSWORD, <<"public">>).

-define(async, async).
-define(sync, sync).
-define(tcp, tcp).
-define(tls, tls).
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
            emqx_bridge_mysql,
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

init_per_group(?tcp, TCConfig) ->
    MysqlHost = os:getenv("MYSQL_TCP_HOST", "toxiproxy"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TCP_PORT", "3306")),
    [
        {mysql_host, MysqlHost},
        {mysql_port, MysqlPort},
        {enable_tls, false},
        {proxy_name, "mysql_tcp"}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    MysqlHost = os:getenv("MYSQL_TLS_HOST", "toxiproxy"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TLS_PORT", "3307")),
    [
        {mysql_host, MysqlHost},
        {mysql_port, MysqlPort},
        {enable_tls, true},
        {proxy_name, "mysql_tls"}
        | TCConfig
    ];
init_per_group(?async, TCConfig) ->
    [{query_mode, async} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, sync} | TCConfig];
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
    ConnectorConfig = connector_config(#{
        <<"server">> => server(TCConfig),
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
    connect_and_drop_table(TCConfig),
    connect_and_create_table(TCConfig),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
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
    reset_proxy(),
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

server(TCConfig) ->
    Host = get_config(mysql_host, TCConfig, <<"toxiproxy">>),
    Port = get_config(mysql_port, TCConfig, 3306),
    emqx_bridge_v2_testlib:fmt(<<"${h}:${p}">>, #{h => Host, p => Port}).

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"server">> => <<"toxiproxy:3306">>,
        <<"database">> => ?DATABASE,
        <<"username">> => ?USERNAME,
        <<"password">> => ?PASSWORD,
        <<"pool_size">> => 4,
        <<"ssl">> => #{<<"enable">> => false},
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
            <<"sql">> => <<
                "INSERT INTO mqtt_test(payload, arrived) "
                "VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))"
            >>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

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

with_failure(FailureType, TCConfig, Fn) ->
    ProxyName = get_config(proxy_name, TCConfig, "mysql_tcp"),
    emqx_common_test_helpers:with_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT, Fn).

%% We need to create and drop the test table outside of using bridges
%% since a bridge expects the table to exist when enabling it. We
%% therefore call the mysql module directly, in addition to using it
%% for querying the DB directly.
connect_direct_mysql(Config) ->
    Opts = [
        {host, get_config(mysql_host, Config, "toxiproxy")},
        {port, get_config(mysql_port, Config, 3306)},
        {user, ?USERNAME},
        {password, ?PASSWORD},
        {database, ?DATABASE}
    ],
    SslOpts =
        case get_config(enable_tls, Config, false) of
            true ->
                [{ssl, emqx_tls_lib:to_client_opts(#{enable => true})}];
            false ->
                []
        end,
    {ok, Pid} = mysql:start_link(Opts ++ SslOpts),
    Pid.

query_direct_mysql(Config, Query) ->
    Pid = connect_direct_mysql(Config),
    try
        mysql:query(Pid, Query)
    after
        mysql:stop(Pid)
    end.

connect_and_create_table(Config) ->
    SQL = <<
        "CREATE TABLE IF NOT EXISTS mqtt_test (payload blob, arrived datetime NOT NULL) "
        "DEFAULT CHARSET=utf8MB4;"
    >>,
    query_direct_mysql(Config, SQL).

connect_and_drop_table(Config) ->
    SQL = <<"DROP TABLE mqtt_test">>,
    query_direct_mysql(Config, SQL).

connect_and_get_payload(Config) ->
    SQL = <<"SELECT payload FROM mqtt_test">>,
    query_direct_mysql(Config, SQL).

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

delete_action_api(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(action, Type, Name).

get_connector_api(Config) ->
    ConnectorType = ?config(connector_type, Config),
    ConnectorName = ?config(connector_name, Config),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(
            ConnectorType, ConnectorName
        )
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_action_api(TCConfig)
    ).

probe_action_api(TCConfig, Overrides) ->
    #{
        type := Type,
        name := Name,
        config := ActionConfig0
    } = emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    ActionConfig = emqx_utils_maps:deep_merge(ActionConfig0, Overrides),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:probe_bridge_api(action, Type, Name, ActionConfig)
    ).

simple_create_rule_api(TCConfig) ->
    simple_create_rule_api(
        <<
            "select payload,"
            " 1668602148000 as timestamp from \"${t}\" "
        >>,
        TCConfig
    ).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

full_matrix() ->
    [
        [Conn, Sync, Batch]
     || Conn <- [?tcp, ?tls],
        Sync <- [?sync, ?async],
        Batch <- [?with_batch, ?without_batch]
    ].

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

action_res_id(TCConfig) ->
    emqx_bridge_v2_testlib:lookup_chan_id_in_conf(
        emqx_bridge_v2_testlib:get_common_values(TCConfig)
    ).

unprepare(TCConfig, Key) ->
    ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    {ok, _, #{state := #{connector_state := #{pool_name := PoolName}}}} =
        emqx_resource:get_instance(ConnResId),
    [
        begin
            {ok, Conn} = ecpool_worker:client(Worker),
            ok = mysql:unprepare(Conn, Key)
        end
     || {_Name, Worker} <- ecpool:workers(PoolName)
    ].

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

-doc """
N.B.: directly querying the resource like this is bad for refactoring, besides not
actually exercising the full real path that leads to bridge requests.  Thus, it should be
avoid as much as possible.  This function is given an ugly name to remind callers of that.

This connector, however, also serves non-bridge use cases such as authn/authz, and thus
some requests are made directly and do not follow the bridge request format.
""".
directly_query_resource_even_if_it_should_be_avoided(TCConfig, Request) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2:query(?global_ns, Type, Name, Request, #{timeout => 500}).

-doc "See docs on `directly_query_resource_even_if_it_should_be_avoided`".
directly_query_resource_async_even_if_it_should_be_avoided(TCConfig, Request) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    Ref = alias([reply]),
    AsyncReplyFun = fun(#{result := Result}) -> Ref ! {result, Ref, Result} end,
    Return = emqx_bridge_v2:query(?global_ns, Type, Name, Request, #{
        timeout => 500,
        query_mode => async,
        async_reply_fun => {AsyncReplyFun, []}
    }),
    {Return, Ref}.

receive_result(Ref, Timeout) ->
    receive
        {result, Ref, Result} ->
            {ok, Result}
    after Timeout ->
        timeout
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_create_via_http() ->
    [{matrix, true}].
t_create_via_http(matrix) ->
    full_matrix();
t_create_via_http(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_create_via_http(TCConfig),
    ok.

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [
        [Conn, ?sync, ?without_batch]
     || Conn <- [?tcp, ?tls]
    ];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    %% Depending on the exact way that the connection cut manifests, it may report as
    %% connecting or disconnected (if `mysql_protocol:send_packet` crashes with `badmatch`
    %% error...)
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{failure_status => [connecting, disconnected]}),
    ok.

t_start_action_or_source_with_disabled_connector(TCConfig) ->
    ok = emqx_bridge_v2_testlib:t_start_action_or_source_with_disabled_connector(TCConfig),
    ok.

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    full_matrix();
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?retry(
            200,
            10,
            ?assertMatch(
                {ok, [<<"payload">>], [[Payload]]},
                connect_and_get_payload(TCConfig)
            )
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_undefined_vars_as_null() ->
    [{matrix, true}].
t_undefined_vars_as_null(matrix) ->
    full_matrix();
t_undefined_vars_as_null(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"undefined_vars_as_null">> => true
        }
    }),
    #{topic := RuleTopic} = simple_create_rule_api(
        <<
            "select null as payload,"
            " 1668602148000 as timestamp from \"${t}\" "
        >>,
        TCConfig
    ),
    C = start_client(),
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            ?wait_async_action(
                emqtt:publish(C, RuleTopic, <<"">>),
                #{?snk_kind := mysql_connector_query_return}
            ),
            ?assertMatch(
                {ok, [<<"payload">>], [[null]]},
                connect_and_get_payload(TCConfig)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(mysql_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_create_disconnected() ->
    [{matrix, true}].
t_create_disconnected(matrix) ->
    [[?tcp], [?tls]];
t_create_disconnected(TCConfig) when is_list(TCConfig) ->
    ?check_trace(
        with_failure(down, TCConfig, fun() ->
            {201, _} = create_connector_api(TCConfig, #{})
        end),
        fun(Trace) ->
            ?assertMatch(
                [#{error := {start_pool_failed, _, _}}],
                ?of_kind(mysql_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    ok.

t_write_failure() ->
    [{matrix, true}].
t_write_failure(matrix) ->
    full_matrix();
t_write_failure(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Val = unique_payload(),
    ?check_trace(
        begin
            %% for some unknown reason, `?wait_async_action' and `subscribe'
            %% hang and timeout if called inside `with_failure', but the event
            %% happens and is emitted after the test pid dies!?
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := buffer_worker_flush_nack}),
                2_000
            ),
            with_failure(down, TCConfig, fun() ->
                emqtt:publish(C, RuleTopic, Val),
                ?assertMatch({ok, [#{result := {error, _}}]}, snabbkaffe:receive_events(SRef)),
                ok
            end),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(buffer_worker_flush_nack, Trace0),
            ?assertMatch([#{result := {error, _}} | _], Trace),
            [#{result := {error, Error}} | _] = Trace,
            case Error of
                {resource_error, _} ->
                    ok;
                {recoverable_error, disconnected} ->
                    ok;
                _ ->
                    ct:fail("unexpected error: ~p", [Error])
            end
        end
    ),
    ok.

%% This test doesn't work with batch enabled since it is not possible
%% to set the timeout directly for batch queries
t_write_timeout() ->
    [{matrix, true}].
t_write_timeout(matrix) ->
    [
        [Conn, Sync, ?without_batch]
     || Conn <- [?tcp, ?tls],
        Sync <- [?sync, ?async]
    ];
t_write_timeout(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Val = unique_payload(),
    Timeout = 1000,
    %% for some unknown reason, `?wait_async_action' and `subscribe'
    %% hang and timeout if called inside `with_failure', but the event
    %% happens and is emitted after the test pid dies!?
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := buffer_worker_flush_nack}),
        2 * Timeout
    ),
    with_failure(timeout, TCConfig, fun() ->
        emqx_common_test_helpers:with_mock(
            mysql,
            query,
            fun(Conn, SQLOrKey, Params, FilterMap, _Timeout) ->
                meck:passthrough([Conn, SQLOrKey, Params, FilterMap, Timeout])
            end,
            fun() ->
                emqtt:publish(C, RuleTopic, Val)
            end
        )
    end),
    ?assertMatch({ok, [#{result := {error, _}}]}, snabbkaffe:receive_events(SRef)),
    ok.

t_missing_data() ->
    [{matrix, true}].
t_missing_data(matrix) ->
    [
        [?tcp, ?sync, Batch]
     || Batch <- [?without_batch, ?with_batch]
    ];
t_missing_data(TCConfig) when is_list(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(<<"select null as x from \"${t}\" ">>, TCConfig),
    C = start_client(),
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := buffer_worker_flush_ack}),
        2_000
    ),
    emqtt:publish(C, RuleTopic, <<"hey">>),
    {ok, [Event]} = snabbkaffe:receive_events(SRef),
    case BatchSize of
        N when N > 1 ->
            ?assertMatch(
                #{
                    result :=
                        {error,
                            {unrecoverable_error,
                                {1292, _, <<"Truncated incorrect DOUBLE value: 'undefined'">>}}}
                },
                Event
            );
        1 ->
            ?assertMatch(
                #{
                    result :=
                        {error,
                            {unrecoverable_error, {1048, _, <<"Column 'arrived' cannot be null">>}}}
                },
                Event
            )
    end,
    ok.

t_nasty_sql_string(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = list_to_binary(lists:seq(0, 255)),
    {ok, {ok, _}} =
        ?wait_async_action(
            emqtt:publish(C, RuleTopic, Payload),
            #{?snk_kind := mysql_connector_query_return},
            1_000
        ),
    ?assertMatch(
        {ok, [<<"payload">>], [[Payload]]},
        connect_and_get_payload(TCConfig)
    ),
    ok.

t_undefined_field_in_sql(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {400, #{<<"message">> := Msg}} =
                probe_action_api(TCConfig, #{
                    <<"parameters">> => #{
                        <<"sql">> =>
                            <<
                                "INSERT INTO mqtt_test(wrong_column, arrived) "
                                "VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))"
                            >>
                    }
                }),
            ?assertEqual(
                match,
                re:run(
                    Msg,
                    <<"Unknown column 'wrong_column' in 'field list'">>,
                    [{capture, none}]
                ),
                #{error_msg => Msg}
            ),
            ok
        end,
        []
    ),
    ok.

t_delete_with_undefined_field_in_sql(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, #{<<"status">> := <<"disconnected">>}} = create_action_api(TCConfig, #{
                <<"parameters">> => #{
                    <<"sql">> =>
                        <<
                            "INSERT INTO mqtt_test(wrong_column, arrived) "
                            "VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))"
                        >>
                }
            }),
            ?assertMatch({204, _}, delete_action_api(TCConfig)),
            ok
        end,
        []
    ),
    ok.

t_workload_fits_prepared_statement_limit(TCConfig) ->
    N = 50,
    ConnPoolSize = 4,
    BufferWorkerPoolSize = 1,
    {201, _} = create_connector_api(TCConfig, #{<<"pool_size">> => ConnPoolSize}),
    {201, _} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{
            <<"worker_pool_size">> => BufferWorkerPoolSize
        }
    }),
    #{topic := RuleTopic} = simple_create_rule_api(
        <<
            "select payload.time as timestamp,"
            " payload.val as payload from \"${t}\" "
        >>,
        TCConfig
    ),
    emqx_utils:pforeach(
        fun(M) ->
            {ok, C} = emqtt:start_link(#{clientid => integer_to_binary(M)}),
            {ok, _} = emqtt:connect(C),
            lists:foreach(
                fun(_) ->
                    Payload = unique_payload(),
                    Timestamp = erlang:system_time(millisecond),
                    emqtt:publish(
                        C,
                        RuleTopic,
                        emqx_utils_json:encode(#{
                            <<"time">> => Timestamp,
                            <<"val">> => Payload
                        })
                    )
                end,
                lists:seq(1, N)
            ),
            emqtt:stop(C)
        end,
        lists:seq(1, BufferWorkerPoolSize * ConnPoolSize),
        _Timeout = 10_000
    ),
    {ok, _, [[_Var, Count]]} =
        query_direct_mysql(TCConfig, "SHOW GLOBAL STATUS LIKE 'Prepared_stmt_count'"),
    ?assertEqual(
        ConnPoolSize,
        binary_to_integer(Count)
    ).

%% Test doesn't work with batch enabled since batch doesn't use
%% prepared statements as such; it has its own query generation process
t_uninitialized_prepared_statement(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Val = unique_payload(),
    ActionResId = action_res_id(TCConfig),
    unprepare(TCConfig, ActionResId),
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        ?wait_async_action(
            emqtt:publish(C, RuleTopic, Val),
            #{?snk_kind := mysql_connector_query_return}
        ),
        fun(Trace) ->
            ?assert(
                ?strict_causality(
                    #{?snk_kind := mysql_connector_prepare_query_failed, error := not_prepared},
                    #{
                        ?snk_kind := mysql_connector_on_query_prepared_sql,
                        type_or_key := ActionResId
                    },
                    Trace
                )
            ),
            SendQueryTrace = ?of_kind(mysql_connector_send_query, Trace),
            ?assertMatch([#{data := [Val, _]}, #{data := [Val, _]}], SendQueryTrace),
            ReturnTrace = ?of_kind(mysql_connector_query_return, Trace),
            ?assertMatch([#{result := ok}], ReturnTrace),
            ok
        end
    ),
    ok.

t_non_batch_update_is_allowed(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?check_trace(
        begin
            Overrides = #{
                <<"resource_opts">> => #{<<"metrics_flush_interval">> => <<"500ms">>},
                <<"parameters">> => #{
                    <<"sql">> =>
                        <<
                            "UPDATE mqtt_test "
                            "SET arrived = FROM_UNIXTIME(${timestamp}/1000) "
                            "WHERE payload = ${payload.value}"
                        >>
                }
            },
            ?assertMatch({204, _}, probe_action_api(TCConfig, Overrides)),
            ?assertMatch({201, _}, create_action_api(TCConfig, Overrides)),
            #{id := RuleId, topic := RuleTopic} = simple_create_rule_api(
                <<"select * from \"${t}\" ">>,
                TCConfig
            ),
            Payload = emqx_utils_json:encode(#{value => <<"aaaa">>}),
            Message = emqx_message:make(RuleTopic, Payload),
            {_, {ok, _}} =
                ?wait_async_action(
                    emqx:publish(Message),
                    #{?snk_kind := mysql_connector_query_return},
                    10_000
                ),
            ActionId = action_res_id(TCConfig),
            ?assertEqual(1, emqx_resource_metrics:matched_get(ActionId)),
            ?retry(
                _Sleep0 = 200,
                _Attempts0 = 10,
                ?assertEqual(1, emqx_resource_metrics:success_get(ActionId))
            ),

            ?assertEqual(1, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.success')),
            ok
        end,
        []
    ),
    ok.

t_batch_update_is_forbidden() ->
    [{matrix, true}].
t_batch_update_is_forbidden(matrix) ->
    [[?tcp, ?sync, ?with_batch]];
t_batch_update_is_forbidden(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?check_trace(
        begin
            Overrides = #{
                <<"parameters">> => #{
                    <<"sql">> =>
                        <<
                            "UPDATE mqtt_test "
                            "SET arrived = FROM_UNIXTIME(${timestamp}/1000) "
                            "WHERE payload = ${payload.value}"
                        >>
                }
            },
            {400, #{<<"message">> := Msg}} =
                probe_action_api(TCConfig, Overrides),
            ?assertEqual(
                match,
                re:run(
                    Msg,
                    <<"UPDATE statements are not supported for batch operations">>,
                    [global, {capture, none}]
                )
            ),
            {201, #{
                <<"status">> := <<"disconnected">>,
                <<"status_reason">> := Msg2
            }} = create_action_api(TCConfig, Overrides),
            ?assertEqual(
                match,
                re:run(
                    Msg2,
                    <<"UPDATE statements are not supported for batch operations">>,
                    [global, {capture, none}]
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_nested_payload_template_1(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    SQL = <<
        "INSERT INTO mqtt_test(payload, arrived) "
        "VALUES (${payload.value}, FROM_UNIXTIME(${timestamp}/1000))"
    >>,
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"sql">> => SQL}
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    Value = unique_payload(),
    Payload = emqx_utils_json:encode(#{value => Value}),
    C = start_client(),
    {_, {ok, _}} =
        ?wait_async_action(
            emqtt:publish(C, RuleTopic, Payload),
            #{?snk_kind := mysql_connector_query_return},
            10_000
        ),
    ?retry(200, 10, begin
        SelectResult = connect_and_get_payload(TCConfig),
        ?assertEqual({ok, [<<"payload">>], [[Value]]}, SelectResult)
    end),
    ok.

t_nested_payload_template_2(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    SQL = <<
        "INSERT INTO mqtt_test(payload, arrived) "
        "VALUES (${payload.value}, FROM_UNIXTIME(${timestamp}/1000)) "
        "ON DUPLICATE KEY UPDATE arrived=NOW()"
    >>,
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"sql">> => SQL}
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    Value = unique_payload(),
    Payload = emqx_utils_json:encode(#{value => Value}),
    C = start_client(),
    MsgCnt = 20,
    {_, {ok, _}} =
        ?wait_async_action(
            [emqtt:publish(C, RuleTopic, Payload) || _ <- lists:seq(1, MsgCnt)],
            #{?snk_kind := mysql_connector_query_return},
            10_000
        ),
    ?retry(200, 10, begin
        {ok, [<<"payload">>], Results} = connect_and_get_payload(TCConfig),
        ct:pal("value:\n  ~p", [Value]),
        ct:pal("results:\n  ~p", [Results]),
        ?assert(lists:all(fun([V]) -> V == Value end, Results)),
        ?assert(length(Results) >= MsgCnt)
    end),
    ok.

t_table_removed(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    connect_and_drop_table(TCConfig),
    Val = unique_payload(),
    C = start_client(),
    ?assertMatch(
        {_,
            {ok, #{
                reason := {1146, <<"42S02">>, <<"Table 'mqtt.mqtt_test' doesn't exist">>}
            }}},
        ?wait_async_action(
            emqtt:publish(C, RuleTopic, Val),
            #{?snk_kind := "mysql_connector_do_sql_query_failed"},
            10_000
        )
    ),
    ok.

t_missing_table(TCConfig) ->
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            connect_and_drop_table(TCConfig),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{}),
            #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
            Val = unique_payload(),
            C = start_client(),
            ?wait_async_action(
                emqtt:publish(C, RuleTopic, Val),
                #{?snk_kind := mysql_undefined_table}
            ),
            ok
        end,
        []
    ).

t_unprepared_statement_query() ->
    [{matrix, true}].
t_unprepared_statement_query(matrix) ->
    [
        [?tcp, Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_unprepared_statement_query(TCConfig) when is_list(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    Request = {prepared_query, unprepared_query, []},
    {_, {ok, Event}} =
        ?wait_async_action(
            directly_query_resource_even_if_it_should_be_avoided(TCConfig, Request),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    IsBatch = BatchSize > 1,
    case IsBatch of
        true ->
            ?assertMatch(#{result := {error, {unrecoverable_error, invalid_request}}}, Event);
        false ->
            ?assertMatch(
                #{result := {error, {unrecoverable_error, prepared_statement_invalid}}},
                Event
            )
    end,
    ok.

t_bad_sql_parameter() ->
    [{matrix, true}].
t_bad_sql_parameter(matrix) ->
    [
        [?tcp, Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_bad_sql_parameter(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    Request = {sql, <<"">>, [bad_parameter]},
    {_, {ok, Event}} =
        ?wait_async_action(
            directly_query_resource_even_if_it_should_be_avoided(TCConfig, Request),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    IsBatch = BatchSize > 1,
    case IsBatch of
        true ->
            ?assertMatch(#{result := {error, {unrecoverable_error, invalid_request}}}, Event);
        false ->
            ?assertMatch(
                #{result := {error, {unrecoverable_error, {invalid_params, [bad_parameter]}}}},
                Event
            )
    end,
    ok.

t_simple_sql_query() ->
    [{matrix, true}].
t_simple_sql_query(matrix) ->
    [
        [?tcp, Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_simple_sql_query(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    QueryMode = get_config(query_mode, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    Request = {sql, <<"SELECT count(1) AS T">>},
    Result =
        case QueryMode of
            sync ->
                directly_query_resource_even_if_it_should_be_avoided(TCConfig, Request);
            async ->
                {_, Ref} =
                    directly_query_resource_async_even_if_it_should_be_avoided(TCConfig, Request),
                {ok, Res} = receive_result(Ref, 2_000),
                Res
        end,
    IsBatch = BatchSize > 1,
    case IsBatch of
        true -> ?assertEqual({error, {unrecoverable_error, batch_select_not_implemented}}, Result);
        false -> ?assertEqual({ok, [<<"T">>], [[1]]}, Result)
    end,
    ok.

t_update_with_invalid_prepare(Config) ->
    ConnectorName = ?config(connector_name, Config),
    BridgeName = ?config(action_name, Config),
    {ok, _} = emqx_bridge_v2_testlib:create_bridge_api(Config),
    %% arrivedx is a bad column name
    BadSQL = <<
        "INSERT INTO mqtt_test(payload, arrivedx) "
        "VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))"
    >>,
    Override = #{<<"parameters">> => #{<<"sql">> => BadSQL}},
    {ok, {{_, 200, "OK"}, _Headers1, Body1}} =
        emqx_bridge_v2_testlib:update_bridge_api(Config, Override),
    ?assertMatch(#{<<"status">> := <<"disconnected">>}, Body1),
    Error1 = maps:get(<<"error">>, Body1),
    case re:run(Error1, <<"Unknown column">>, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            ct:fail(#{
                expected_pattern => "undefined_column",
                got => Error1
            })
    end,
    %% assert that although there was an error returned, the invliad SQL is actually put
    {200, Action} = get_action_api(Config),
    #{<<"parameters">> := #{<<"sql">> := FetchedSQL}} = Action,
    ?assertEqual(FetchedSQL, BadSQL),

    %% update again with the original sql
    {ok, {{_, 200, "OK"}, _Headers2, Body2}} =
        emqx_bridge_v2_testlib:update_bridge_api(Config, #{}),
    %% the error should be gone now, and status should be 'connected'
    ?assertMatch(#{<<"error">> := <<>>, <<"status">> := <<"connected">>}, Body2),
    %% finally check if ecpool worker should have exactly one of reconnect callback
    ConnectorResId = <<"connector:mysql:", ConnectorName/binary>>,
    Workers = ecpool:workers(ConnectorResId),
    [_ | _] = WorkerPids = lists:map(fun({_, Pid}) -> Pid end, Workers),
    lists:foreach(
        fun(Pid) ->
            [{emqx_mysql, prepare_sql_to_conn, Args}] =
                ecpool_worker:get_reconnect_callbacks(Pid),
            Sig = emqx_mysql:get_reconnect_callback_signature(Args),
            BridgeResId = <<"action:mysql:", BridgeName/binary, $:, ConnectorResId/binary>>,
            ?assertEqual(BridgeResId, Sig)
        end,
        WorkerPids
    ),
    ok.

t_timeout_disconnected_then_recover() ->
    [{matrix, true}].
t_timeout_disconnected_then_recover(matrix) ->
    [[?tcp, ?sync, ?without_batch]];
t_timeout_disconnected_then_recover(Config) ->
    ProxyName = ?config(proxy_name, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            %% 0) Bridge is initially healthy.
            {201, _} = create_connector_api(
                Config,
                #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"500ms">>}}
            ),
            {201, _} = create_action_api(Config, #{}),
            ?retry(
                500,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(Config)
                )
            ),
            RuleTopic = <<"t/mysql">>,
            {ok, _} = emqx_bridge_v2_testlib:create_rule_and_action_http(
                ?ACTION_TYPE_BIN, RuleTopic, Config
            ),
            {ok, C} = emqtt:start_link(),
            {ok, _} = emqtt:connect(C),
            Publisher = spawn_link(fun Rec() ->
                emqtt:publish(C, RuleTopic, <<"aaa">>),
                timer:sleep(20),
                Rec()
            end),
            %% 1) A connector health check times out, connector becomes `connecting`.
            ct:pal("starting timeout failure..."),
            emqx_common_test_helpers:enable_failure(timeout, ProxyName, ProxyHost, ProxyPort),
            ?retry(
                500,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := Status}} when
                        Status == <<"connecting">> orelse Status == <<"disconnected">>,
                    get_connector_api(Config)
                )
            ),
            %% 2) Connection is closed by MySQL, crashing the ecpool workers.
            ct:pal("cutting connection..."),
            emqx_common_test_helpers:enable_failure(down, ProxyName, ProxyHost, ProxyPort),
            ?retry(
                500,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := Status}} when
                        Status == <<"connecting">> orelse Status == <<"disconnected">>,
                    get_connector_api(Config)
                )
            ),
            %% 3) Now recover but drop table, so that it becomes unhealthy.
            ct:pal("restoring down and timeout failures..."),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            ct:pal("dropping table..."),
            connect_and_drop_table(Config),
            ?retry(
                500,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"disconnected">>}},
                    get_connector_api(Config)
                )
            ),
            %% 4) MySQL becomes healthy again later after table is recreated, should
            %% resume operations automatically.
            ct:pal("recreating table..."),
            connect_and_create_table(Config),
            ?retry(
                500,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(Config)
                )
            ),
            unlink(Publisher),
            exit(Publisher, kill),
            emqtt:stop(C),
            ok
        end,
        []
    ),
    ok.

t_rule_test_trace(Config) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(Config, Opts).
