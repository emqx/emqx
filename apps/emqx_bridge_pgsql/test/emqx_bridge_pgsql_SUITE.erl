%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pgsql_SUITE).

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

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).
-define(PROXY_NAME_TCP, "pgsql_tcp").
-define(PROXY_NAME_TLS, "pgsql_tls").

-define(DATABASE, <<"mqtt">>).
-define(USERNAME, <<"root">>).
-define(PASSWORD, <<"public">>).

-define(async, async).
-define(sync, sync).
-define(tcp, tcp).
-define(tls, tls).
-define(with_batch, with_batch).
-define(without_batch, without_batch).

-define(postgres, pgsql).
-define(matrix, matrix).
-define(timescale, timescale).

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
            emqx_bridge_pgsql,
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
    PgsqlHost = os:getenv("PGSQL_TCP_HOST", "toxiproxy"),
    PgsqlPort = list_to_integer(os:getenv("PGSQL_TCP_PORT", "5432")),
    [
        {postgres_host, PgsqlHost},
        {postgres_port, PgsqlPort},
        {enable_tls, false},
        {proxy_name, ?PROXY_NAME_TCP}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    PgsqlHost = os:getenv("PGSQL_TLS_HOST", "toxiproxy"),
    PgsqlPort = list_to_integer(os:getenv("PGSQL_TLS_PORT", "5433")),
    [
        {postgres_host, PgsqlHost},
        {postgres_port, PgsqlPort},
        {enable_tls, true},
        {proxy_name, ?PROXY_NAME_TLS}
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
    ConnectorType = connector_type_of(TCConfig),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(ConnectorType, #{
        <<"server">> => server(TCConfig),
        <<"ssl">> => #{<<"enable">> => get_config(enable_tls, TCConfig, false)}
    }),
    ActionType = action_type_of(TCConfig),
    ActionName = ConnectorName,
    ActionConfig = action_config(ActionType, #{
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
        {connector_type, ConnectorType},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ActionType},
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

server(TCConfig) ->
    Host = get_config(postgres_host, TCConfig, <<"toxiproxy">>),
    Port = get_config(postgres_port, TCConfig, 5432),
    emqx_bridge_v2_testlib:fmt(<<"${h}:${p}">>, #{h => Host, p => Port}).

connector_type_of(TCConfig) ->
    postgres_flavor_of(TCConfig).

action_type_of(TCConfig) ->
    postgres_flavor_of(TCConfig).

postgres_flavor_of(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?postgres, ?matrix, ?timescale], ?postgres).

connector_config(Type, Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"database">> => <<"mqtt">>,
        <<"server">> => <<"toxiproxy:5432">>,
        <<"username">> => ?USERNAME,
        <<"password">> => ?PASSWORD,
        <<"disable_prepared_statements">> => false,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(bin(Type), <<"x">>, InnerConfigMap).

action_config(Type, Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"sql">> => <<
                "INSERT INTO mqtt_test(payload, arrived) "
                "VALUES (${payload}, TO_TIMESTAMP((${timestamp} :: bigint)/1000))"
            >>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, bin(Type), <<"x">>, InnerConfigMap).

bin(X) -> emqx_utils_conv:bin(X).

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
    ProxyName = get_config(proxy_name, TCConfig, ?PROXY_NAME_TCP),
    emqx_common_test_helpers:with_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT, Fn).

connect_direct_pgsql(Config) ->
    Opts = #{
        host => get_config(postgres_host, Config, "toxiproxy"),
        port => get_config(postgres_port, Config, 5432),
        username => ?USERNAME,
        password => ?PASSWORD,
        database => ?DATABASE
    },
    SslOpts =
        case get_config(enable_tls, Config, false) of
            true ->
                Opts#{
                    ssl => true,
                    ssl_opts => emqx_tls_lib:to_client_opts(#{enable => true})
                };
            false ->
                Opts
        end,
    {ok, Conn} = epgsql:connect(SslOpts),
    Conn.

connect_and_create_table(TCConfig) ->
    SQL = <<"CREATE TABLE IF NOT EXISTS mqtt_test (payload text, arrived timestamp NOT NULL) ">>,
    Conn = connect_direct_pgsql(TCConfig),
    {ok, _, _} = epgsql:squery(Conn, SQL),
    ok = epgsql:close(Conn).

connect_and_drop_table(TCConfig) ->
    SQL = <<"DROP TABLE if exists  mqtt_test">>,
    Conn = connect_direct_pgsql(TCConfig),
    {ok, _, _} = epgsql:squery(Conn, SQL),
    ok = epgsql:close(Conn).

connect_and_clear_table(TCConfig) ->
    SQL = <<"DELETE from mqtt_test">>,
    Conn = connect_direct_pgsql(TCConfig),
    {ok, _} = epgsql:squery(Conn, SQL),
    ok = epgsql:close(Conn).

connect_and_get_payload(TCConfig) ->
    SQL = <<"SELECT payload FROM mqtt_test">>,
    Conn = connect_direct_pgsql(TCConfig),
    {ok, _, [{Result}]} = epgsql:squery(Conn, SQL),
    ok = epgsql:close(Conn),
    Result.

full_matrix() ->
    [
        [Conn, Sync, Batch]
     || Conn <- [?tcp, ?tls],
        Sync <- [?sync, ?async],
        Batch <- [?with_batch, ?without_batch]
    ].

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

update_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_bridge_api(Config, Overrides)
    ).

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

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

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

force_health_check(TCConfig) ->
    emqx_bridge_v2_testlib:force_health_check(
        emqx_bridge_v2_testlib:get_common_values(TCConfig)
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, postgres_stopped).

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [
        [Conn, ?sync, ?without_batch]
     || Conn <- [?tcp, ?tls]
    ];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

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
            ?assertEqual(Payload, connect_and_get_payload(TCConfig))
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_create_disconnected() ->
    [{matrix, true}].
t_create_disconnected(matrix) ->
    [
        [Conn, ?sync, ?without_batch]
     || Conn <- [?tcp, ?tls]
    ];
t_create_disconnected(TCConfig) when is_list(TCConfig) ->
    ?check_trace(
        with_failure(down, TCConfig, fun() ->
            {201, _} = create_connector_api(TCConfig, #{})
        end),
        fun(Trace) ->
            ?assertMatch(
                [#{error := {start_pool_failed, _, _}}],
                ?of_kind(pgsql_connector_start_failed, Trace)
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
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = unique_payload(),
    ?check_trace(
        with_failure(down, TCConfig, fun() ->
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, Topic, Payload, [{qos, 0}]),
                    #{?snk_kind := buffer_worker_flush_nack},
                    15_000
                )
        end),
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
    [[?tcp, ?async, ?without_batch]];
t_write_timeout(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"100ms">>
        }
    }),
    {201, _} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{
            <<"resume_interval">> => <<"100ms">>,
            <<"health_check_interval">> => <<"100ms">>,
            <<"request_ttl">> => <<"500ms">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = unique_payload(),
    ?check_trace(
        with_failure(timeout, TCConfig, fun() ->
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, Topic, Payload, [{qos, 0}]),
                    #{?snk_kind := buffer_worker_flush_nack},
                    20_000
                )
        end),
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
    Request = {query, <<"SELECT count(1) AS T">>},
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
        true -> ?assertEqual({error, {unrecoverable_error, batch_prepare_not_implemented}}, Result);
        false -> ?assertMatch({ok, _, [{1}]}, Result)
    end,
    ok.

t_missing_data(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(
        <<"select nope from \"${t}\" ">>,
        TCConfig
    ),
    C = start_client(),
    Payload = unique_payload(),
    {_, {ok, Event}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, Payload, [{qos, 1}]),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    ?assertMatch(
        #{
            result :=
                {error,
                    {unrecoverable_error, #{
                        error_code := <<"23502">>,
                        error_codename := not_null_violation,
                        severity := error
                    }}}
        },
        Event
    ),
    ok.

t_bad_sql_parameter() ->
    [{matrix, true}].
t_bad_sql_parameter(matrix) ->
    [
        [?tcp, Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?with_batch, ?without_batch]
    ];
t_bad_sql_parameter(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    QueryMode = get_config(query_mode, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    Request = {query, <<"">>, [bad_parameter]},
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
        true ->
            ?assertEqual({error, {unrecoverable_error, invalid_request}}, Result);
        false ->
            ?assertMatch({error, {unrecoverable_error, _}}, Result)
    end,
    ok.

t_nasty_sql_string(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = list_to_binary(lists:seq(1, 127)),
    {_, {ok, _}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, Payload, [{qos, 1}]),
            #{?snk_kind := pgsql_connector_query_return},
            1_000
        ),
    ?assertEqual(Payload, connect_and_get_payload(TCConfig)).

t_missing_table(TCConfig) ->
    ?check_trace(
        begin
            connect_and_drop_table(TCConfig),
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
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(pgsql_undefined_table, Trace)),
            ok
        end
    ),
    ok.

%% We test that we can handle when the prepared statement with the channel
%% name already exists in the connection instance when we try to make a new
%% prepared statement. It is unknown in which scenario this can happen but it
%% has been observed in a production log file.
%% See:
%% https://emqx.atlassian.net/browse/EEC-1036
t_prepared_statement_exists(TCConfig) ->
    emqx_common_test_helpers:on_exit(fun meck:unload/0),
    meck:new(epgsql, [passthrough, no_link, no_history]),
    InsertPrepStatementDupAndThenRemoveMeck =
        fun(Conn, Key, SQL, List) ->
            meck:passthrough([Conn, Key, SQL, List]),
            meck:delete(
                epgsql,
                parse2,
                4
            ),
            meck:passthrough([Conn, Key, SQL, List])
        end,
    meck:expect(
        epgsql,
        parse2,
        InsertPrepStatementDupAndThenRemoveMeck
    ),
    %% We should recover if the prepared statement name already exists in the
    %% driver
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(pgsql_prepared_statement_exists, Trace)),
            ok
        end
    ),
    InsertPrepStatementDup =
        fun(Conn, Key, SQL, List) ->
            meck:passthrough([Conn, Key, SQL, List]),
            meck:passthrough([Conn, Key, SQL, List])
        end,
    meck:expect(
        epgsql,
        parse2,
        InsertPrepStatementDup
    ),
    %% We should get status disconnected if removing already existing statment don't help
    ?check_trace(
        begin
            ?assertMatch(
                {200, #{<<"status">> := <<"disconnected">>}},
                update_action_api(TCConfig, #{})
            ),
            snabbkaffe_nemesis:cleanup(),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(pgsql_prepared_statement_exists, Trace)),
            ok
        end
    ),
    meck:unload(),
    ok.

t_table_removed(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"200ms">>}
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            ct:pal("dropping table"),
            connect_and_drop_table(TCConfig),
            ?retry(
                500,
                10,
                ?assertMatch(
                    {200, #{
                        <<"status">> := <<"disconnected">>,
                        <<"status_reason">> := <<"{unhealthy_target,", _/binary>>
                    }},
                    get_action_api(TCConfig)
                )
            ),
            Payload = unique_payload(),
            ct:pal("sending query"),
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, Topic, Payload, [{qos, 1}]),
                    #{?snk_kind := "rule_runtime_unhealthy_target"},
                    15_000
                ),
            ok
        end,
        []
    ),
    ok.

t_concurrent_health_checks(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
            emqx_utils:pmap(
                fun(_) ->
                    ?assertMatch(
                        #{status := connected},
                        force_health_check(TCConfig)
                    )
                end,
                lists:seq(1, 20)
            ),
            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(postgres_connector_bad_parse2, Trace)),
            ok
        end
    ),
    ok.

t_start_action_or_source_with_disabled_connector(TCConfig) ->
    ok = emqx_bridge_v2_testlib:t_start_action_or_source_with_disabled_connector(TCConfig),
    ok.

t_disable_prepared_statements() ->
    [{matrix, true}].
t_disable_prepared_statements(matrix) ->
    [
        [?postgres, ?async, ?without_batch],
        [?postgres, ?async, ?with_batch],
        [?timescale, ?async, ?without_batch],
        [?timescale, ?async, ?with_batch],
        [?matrix, ?async, ?without_batch],
        [?matrix, ?async, ?with_batch]
    ];
t_disable_prepared_statements(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    ?check_trace(
        #{timetrap => 5_000},
        begin
            {201, _} = create_connector_api(TCConfig, #{
                <<"disable_prepared_statements">> => true
            }),
            {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            lists:foreach(
                fun(N) ->
                    emqtt:publish(C, Topic, integer_to_binary(N), [{qos, 0}])
                end,
                lists:seq(1, BatchSize)
            ),
            case BatchSize > 1 of
                true ->
                    ?block_until(#{
                        ?snk_kind := "postgres_success_batch_result",
                        row_count := BatchSize
                    }),
                    ok;
                false ->
                    ok
            end,
            ok
        end,
        []
    ),
    ok.

t_update_with_invalid_prepare(TCConfig) ->
    Opts = #{
        bad_sql =>
            %% arrivedx is a bad column name
            <<
                "INSERT INTO mqtt_test(payload, arrivedx) "
                "VALUES (${payload}, TO_TIMESTAMP((${timestamp} :: bigint)/1000))"
            >>,
        reconnect_cb => {emqx_postgresql, prepare_sql_to_conn},
        get_sig_fn => fun emqx_postgresql:get_reconnect_callback_signature/1,
        check_expected_error_fn => fun(Error) ->
            case re:run(Error, <<"undefined_column">>, [{capture, none}]) of
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

%% Checks that furnishing `epgsql' a value that cannot be encoded to a timestamp results
%% in a pretty error instead of a crash.
t_bad_datetime_param(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"sql">> => <<
                "INSERT INTO mqtt_test(payload, arrived) "
                "VALUES (${payload}, ${payload})"
            >>
        }
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    Payload = <<"2024-06-30 00:10:00">>,
    C = start_client(),
    ?assertMatch(
        {_,
            {ok, #{
                context := #{
                    reason := bad_param,
                    type := timestamp,
                    index := 1,
                    value := Payload
                }
            }}},
        ?wait_async_action(
            emqtt:publish(C, RuleTopic, Payload),
            #{?snk_kind := "postgres_bad_param_error"}
        )
    ),
    ok.

%% Checks that we trigger a full reconnection if the health check times out, by declaring
%% the connector `?status_disconnected`.  We choose to do this because there have been
%% issues where the connection process does not die and the connection itself unusable.
t_reconnect_on_connector_health_check_timeout(TCConfig) ->
    {201, _} = create_connector_api(
        TCConfig,
        #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"750ms">>}}
    ),
    ?assertMatch(
        {200, #{<<"status">> := <<"connected">>}},
        get_connector_api(TCConfig)
    ),
    meck:new(epgsql, [passthrough]),
    with_failure(timeout, TCConfig, fun() ->
        ?retry(
            750,
            5,
            ?assertMatch(
                {200, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> := <<"health_check_timeout">>
                }},
                get_connector_api(TCConfig)
            )
        )
    end),
    %% Recovery
    ?retry(
        750,
        5,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(TCConfig)
        )
    ),
    %% Should have triggered a reconnection
    ?assertMatch(
        [_ | _],
        [
            1
         || {_, {_, connect, _}, _} <- meck:history(epgsql)
        ]
    ),
    ok.

%% Similar to `t_reconnect_on_connector_health_check_timeout`, but the timeout occurs in
%% the `check_prepares` call.
t_reconnect_on_connector_health_check_timeout_check_prepares(TCConfig) ->
    {201, _} = create_connector_api(
        TCConfig,
        #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"750ms">>}}
    ),
    ?assertMatch(
        {200, #{<<"status">> := <<"connected">>}},
        get_connector_api(TCConfig)
    ),
    TestPid = self(),
    emqx_common_test_helpers:with_mock(
        emqx_postgresql,
        on_get_status_prepares,
        fun(_ConnState) ->
            TestPid ! get_prepares_called,
            timer:sleep(infinity)
        end,
        fun() ->
            ?retry(
                750,
                5,
                ?assertMatch(
                    {200, #{
                        <<"status">> := <<"disconnected">>,
                        <<"status_reason">> := <<"resource_health_check_timed_out">>
                    }},
                    get_connector_api(TCConfig)
                )
            )
        end
    ),
    ?assertReceive(get_prepares_called),
    %% Recovery
    ?retry(
        750,
        10,
        ?assertMatch(
            {200, #{
                <<"status">> := <<"connected">>
            }},
            get_connector_api(TCConfig)
        )
    ),
    ok.
