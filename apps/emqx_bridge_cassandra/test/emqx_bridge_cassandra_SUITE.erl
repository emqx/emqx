%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_cassandra_SUITE).

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

-define(CONNECTOR_TYPE, cassandra).
-define(CONNECTOR_TYPE_BIN, <<"cassandra">>).
-define(ACTION_TYPE, cassandra).
-define(ACTION_TYPE_BIN, <<"cassandra">>).

-define(PROXY_NAME_TCP, "cassandra_tcp").
-define(PROXY_NAME_TLS, "cassandra_tls").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

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
            emqx_bridge_cassandra,
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
    Host = os:getenv("CASSANDRA_TCP_HOST", "toxiproxy"),
    Port = list_to_integer(os:getenv("CASSANDRA_TCP_PORT", "9042")),
    [
        {cassandra_host, Host},
        {cassandra_port, Port},
        {enable_tls, false},
        {proxy_name, ?PROXY_NAME_TCP}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    Host = os:getenv("CASSANDRA_TLS_HOST", "toxiproxy"),
    Port = list_to_integer(os:getenv("CASSANDRA_TLS_PORT", "9142")),
    [
        {cassandra_host, Host},
        {cassandra_port, Port},
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
    ConnectorName = atom_to_binary(TestCase),
    SSL =
        case get_config(enable_tls, TCConfig, false) of
            true ->
                #{
                    <<"enable">> => true,
                    <<"cacertfile">> => bin(cacertfile()),
                    <<"certfile">> => bin(certfile()),
                    <<"keyfile">> => bin(keyfile())
                };
            false ->
                #{<<"enable">> => false}
        end,
    ConnectorConfig = connector_config(#{
        <<"servers">> => server(TCConfig),
        <<"ssl">> => SSL
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
    catch connect_and_drop_table(TCConfig),
    connect_and_create_table(TCConfig),
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

server(TCConfig) ->
    Host = get_config(cassandra_host, TCConfig, <<"toxiproxy">>),
    Port = get_config(cassandra_port, TCConfig, 9042),
    emqx_bridge_v2_testlib:fmt(<<"${h}:${p}">>, #{h => Host, p => Port}).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"servers">> => <<"toxiproxy:9042">>,
        <<"keyspace">> => <<"mqtt">>,
        <<"username">> => <<"cassandra">>,
        <<"password">> => <<"cassandra">>,
        <<"ssl">> => #{
            <<"server_name_indication">> => <<"disable">>
        },
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
            <<"cql">> => <<
                "insert into mqtt_msg_test(topic, payload, arrived) "
                "values (${topic}, ${payload}, ${timestamp})"
            >>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

bin(X) -> emqx_utils_conv:bin(X).

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
    ProxyName = get_config(proxy_name, TCConfig),
    emqx_common_test_helpers:with_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT, Fn).

cert_root() ->
    filename:join([emqx_common_test_helpers:proj_root(), ".ci", "docker-compose-file", "certs"]).

cacertfile() ->
    filename:join(cert_root(), ["ca.crt"]).

certfile() ->
    filename:join(cert_root(), ["client.pem"]).

keyfile() ->
    filename:join(cert_root(), ["client.key"]).

connect_direct_cassandra(TCConfig) ->
    Opts = #{
        nodes => [
            {
                get_config(cassandra_host, TCConfig, "toxiproxy"),
                get_config(cassandra_port, TCConfig, 9042)
            }
        ],
        username => <<"cassandra">>,
        password => <<"cassandra">>,
        keyspace => <<"mqtt">>
    },
    SslOpts =
        case get_config(enable_tls, TCConfig, false) of
            true ->
                Opts#{
                    ssl => emqx_tls_lib:to_client_opts(
                        #{
                            enable => true,
                            cacertfile => cacertfile(),
                            certfile => certfile(),
                            keyfile => keyfile()
                        }
                    )
                };
            false ->
                Opts
        end,
    {ok, Con} = ecql:connect(maps:to_list(SslOpts)),
    Con.

connect_and_create_table(Config) ->
    SQL = iolist_to_binary([
        "CREATE TABLE mqtt.mqtt_msg_test (\n"
        "    topic text,\n"
        "    payload text,\n"
        "    arrived timestamp,\n"
        "    PRIMARY KEY (topic)\n"
        ")"
    ]),
    connect_and_create_table(Config, SQL).

connect_and_create_table(Config, SQL) ->
    with_direct_conn(Config, fun(Conn) ->
        {ok, _} = ecql:query(Conn, SQL)
    end).

connect_and_drop_table(Config) ->
    SQL = <<"DROP TABLE mqtt.mqtt_msg_test">>,
    connect_and_drop_table(Config, SQL).

connect_and_drop_table(Config, SQL) ->
    with_direct_conn(Config, fun(Conn) ->
        {ok, _} = ecql:query(Conn, SQL)
    end).

connect_and_clear_table(Config) ->
    SQL = <<"TRUNCATE mqtt.mqtt_msg_test">>,
    connect_and_clear_table(Config, SQL).

connect_and_clear_table(Config, SQL) ->
    with_direct_conn(Config, fun(Conn) ->
        ok = ecql:query(Conn, SQL)
    end).

connect_and_get_payload(Config) ->
    SQL = <<"SELECT payload FROM mqtt.mqtt_msg_test">>,
    connect_and_get_payload(Config, SQL).

connect_and_get_payload(Config, SQL) ->
    with_direct_conn(Config, fun(Conn) ->
        {ok, {_Keyspace, _ColsSpec, [[Result]]}} = ecql:query(Conn, SQL),
        Result
    end).

with_direct_conn(Config, Fn) ->
    Conn = connect_direct_cassandra(Config),
    try
        Fn(Conn)
    after
        ok = ecql:close(Conn)
    end.

-doc """
N.B.: directly querying the resource like this is bad for refactoring, besides not
actually exercising the full real path that leads to bridge requests.  Thus, it should be
avoid as much as possible.  This function is given an ugly name to remind callers of that.
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

update_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:update_bridge_api2(Config, Overrides).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [
        [Conn, ?sync, ?without_batch]
     || Conn <- [?tcp, ?tls]
    ];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "cassandra_connector_stop").

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [
        [Conn, ?sync, ?without_batch]
     || Conn <- [?tcp, ?tls]
    ];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{
        failure_status => [?status_connecting, ?status_disconnected]
    }).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    full_matrix();
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?retry(200, 10, ?assertMatch(Payload, connect_and_get_payload(TCConfig)))
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_insert_null_into_int_column(TCConfig) ->
    connect_and_create_table(
        TCConfig,
        <<
            "CREATE TABLE mqtt.mqtt_msg_test2 (\n"
            "  topic text,\n"
            "  payload text,\n"
            "  arrived timestamp,\n"
            "  x int,\n"
            "  PRIMARY KEY (topic)\n"
            ")"
        >>
    ),
    on_exit(fun() -> connect_and_drop_table(TCConfig, "DROP TABLE mqtt.mqtt_msg_test2") end),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"cql">> => <<
                "insert into mqtt_msg_test2(topic, payload, x, arrived) "
                "values (${topic}, ${payload}, ${x}, ${timestamp})"
            >>
        }
    }),
    #{topic := RuleTopic} = simple_create_rule_api(
        <<"select *, first(jq('null', payload)) as x from \"${t}\"">>,
        TCConfig
    ),
    Payload = <<"{}">>,
    Msg = emqx_message:make(RuleTopic, Payload),
    {_, {ok, _}} =
        ?wait_async_action(
            emqx:publish(Msg),
            #{?snk_kind := cassandra_connector_query_return},
            10_000
        ),

    %% Would return `1853189228' if it encodes `null' as an integer...
    ?assertEqual(null, connect_and_get_payload(TCConfig, "select x from mqtt.mqtt_msg_test2")),
    ok.

t_update_action_sql(TCConfig) ->
    connect_and_create_table(
        TCConfig,
        <<
            "CREATE TABLE mqtt.mqtt_msg_test2 (\n"
            "  topic text,\n"
            "  qos int,\n"
            "  payload text,\n"
            "  arrived timestamp,\n"
            "  PRIMARY KEY (topic)\n"
            ")"
        >>
    ),
    on_exit(fun() -> connect_and_drop_table(TCConfig, "DROP TABLE mqtt.mqtt_msg_test2") end),

    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"cql">> => <<
                "insert into mqtt_msg_test2(topic, payload, arrived) "
                "values (${topic}, ${payload}, ${timestamp})"
            >>
        }
    }),

    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),

    Payload = <<"{}">>,
    Msg = emqx_message:make(RuleTopic, Payload),
    {_, {ok, _}} =
        ?wait_async_action(
            emqx:publish(Msg),
            #{?snk_kind := cassandra_connector_query_return},
            10_000
        ),

    ?assertEqual(
        null,
        connect_and_get_payload(TCConfig, "select qos from mqtt.mqtt_msg_test2")
    ),
    connect_and_clear_table(TCConfig, <<"truncate mqtt.mqtt_msg_test2">>),

    %% Update a correct SQL Teamplate
    {200, _} =
        update_action_api(
            TCConfig,
            #{
                <<"parameters">> => #{
                    <<"cql">> => <<
                        "insert into mqtt_msg_test2(topic, qos, payload, arrived) "
                        "values (${topic}, ${qos}, ${payload}, ${timestamp})"
                    >>
                }
            }
        ),

    {_, {ok, _}} =
        ?wait_async_action(
            emqx:publish(Msg),
            #{?snk_kind := cassandra_connector_query_return},
            10_000
        ),

    ?assertEqual(
        0,
        connect_and_get_payload(TCConfig, "select qos from mqtt.mqtt_msg_test2")
    ),
    connect_and_clear_table(TCConfig, <<"truncate mqtt.mqtt_msg_test2">>),

    %% Update a wrong SQL Teamplate
    BadSQL =
        <<
            "insert into mqtt_msg_test2(topic, qos, payload, bad_col_name) "
            "values (${topic}, ${qos}, ${payload}, ${timestamp})"
        >>,
    {200, #{
        <<"status">> := <<"connecting">>,
        <<"status_reason">> := Error1,
        <<"parameters">> := #{<<"cql">> := BadSQL}
    }} =
        update_action_api(
            TCConfig,
            #{
                <<"parameters">> => #{
                    <<"cql">> => BadSQL
                }
            }
        ),
    case re:run(Error1, <<"Undefined column name bad_col_name">>, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            ct:fail(#{
                expected_pattern => "undefined_column",
                got => Error1
            })
    end,

    %% Update again with a correct SQL Teamplate
    {200, #{<<"status">> := <<"connected">>}} =
        update_action_api(
            TCConfig,
            #{
                <<"parameters">> => #{
                    <<"cql">> => <<
                        "insert into mqtt_msg_test2(topic, qos, payload, arrived) "
                        "values (${topic}, ${qos}, ${payload}, ${timestamp})"
                    >>
                }
            }
        ),

    {_, {ok, _}} =
        ?wait_async_action(
            emqx:publish(Msg),
            #{?snk_kind := cassandra_connector_query_return},
            10_000
        ),

    ?assertEqual(
        0,
        connect_and_get_payload(TCConfig, "select qos from mqtt.mqtt_msg_test2")
    ),
    ok.

t_create_disconnected() ->
    [{matrix, true}].
t_create_disconnected(matrix) ->
    [
        [Conn, ?sync, ?without_batch]
     || Conn <- [?tcp, ?tls]
    ];
t_create_disconnected(TCConfig) ->
    ?check_trace(
        with_failure(down, TCConfig, fun() ->
            ?assertMatch({201, _}, create_connector_api(TCConfig, #{}))
        end),
        fun(Trace) ->
            ?assertMatch(
                [#{error := {start_pool_failed, _, _}}],
                ?of_kind(cassandra_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    ok.

t_write_failure() ->
    [{matrix, true}].
t_write_failure(matrix) ->
    full_matrix();
t_write_failure(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = unique_payload(),
    ?check_trace(
        with_failure(down, TCConfig, fun() ->
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, Payload),
                    #{?snk_kind := Evt} when
                        Evt =:= buffer_worker_flush_nack orelse
                            Evt =:= buffer_worker_retry_inflight_failed,
                    10_000
                )
        end),
        fun(Trace0) ->
            Trace = ?of_kind(
                [buffer_worker_flush_nack, buffer_worker_retry_inflight_failed], Trace0
            ),
            [#{result := Result} | _] = Trace,
            case Result of
                {async_return, {error, {resource_error, _}}} ->
                    ok;
                {async_return, {error, {recoverable_error, disconnected}}} ->
                    ok;
                {error, {resource_error, _}} ->
                    ok;
                _ ->
                    ct:fail("unexpected error: ~p", [Result])
            end
        end
    ),
    ok.

%% Test for ad-hoc queries, even though this bridge does not serve authn/authz....
t_simple_sql_query() ->
    [{matrix, true}].
t_simple_sql_query(matrix) ->
    [
        [?tcp, Sync, ?without_batch]
     || Sync <- [?sync, ?async]
    ];
t_simple_sql_query(TCConfig) ->
    QueryMode = get_config(query_mode, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    Request = {query, <<"SELECT count(1) AS T FROM system.local">>},
    Result =
        case QueryMode of
            sync ->
                directly_query_resource_even_if_it_should_be_avoided(TCConfig, Request);
            async ->
                {_, Ref} = directly_query_resource_async_even_if_it_should_be_avoided(
                    TCConfig, Request
                ),
                {ok, Res} = receive_result(Ref, 2_000),
                Res
        end,
    ?assertMatch({ok, {<<"system.local">>, _, [[1]]}}, Result),
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
    %% emqx_bridge_cassandra_connector will send missed data as a `null` atom
    %% to ecql driver
    ?check_trace(
        begin
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, Topic, Payload, [{qos, 1}]),
                    #{?snk_kind := cassandra_connector_query_return},
                    2_000
                ),
            ok
        end,
        fun(Trace0) ->
            %% 1. ecql driver will return `ok` first in async query
            Trace = ?of_kind(cassandra_connector_query_return, Trace0),
            ?assertMatch([#{result := {ok, _Pid}}], Trace),
            %% 2. then it will return an error in callback function
            Trace1 = ?of_kind(handle_async_reply, Trace0),
            ?assertMatch([#{result := {error, {8704, _}}}], Trace1),
            ok
        end
    ),
    ok.

%% Test for ad-hoc queries, even though this bridge does not serve authn/authz....
t_bad_sql_parameter() ->
    [{matrix, true}].
t_bad_sql_parameter(matrix) ->
    [
        [?tcp, Sync, ?without_batch]
     || Sync <- [?sync, ?async]
    ];
t_bad_sql_parameter(TCConfig) ->
    QueryMode = get_config(query_mode, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    Request = {query, <<"">>, [bad_parameter]},
    Result =
        case QueryMode of
            sync ->
                directly_query_resource_even_if_it_should_be_avoided(TCConfig, Request);
            async ->
                {_, Ref} = directly_query_resource_async_even_if_it_should_be_avoided(
                    TCConfig, Request
                ),
                case receive_result(Ref, 5_000) of
                    {ok, Res} ->
                        Res;
                    timeout ->
                        ct:pal("mailbox:\n  ~p", [process_info(self(), messages)]),
                        ct:fail("no response received")
                end
        end,
    ?assertMatch({error, _}, Result),
    ok.

t_nasty_sql_string(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = list_to_binary(lists:seq(1, 127)),
    emqtt:publish(C, RuleTopic, Payload),
    ?assertEqual(Payload, connect_and_get_payload(TCConfig)).
