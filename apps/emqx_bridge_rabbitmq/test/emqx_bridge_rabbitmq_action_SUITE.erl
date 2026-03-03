%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rabbitmq_action_SUITE).

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

-define(CONNECTOR_TYPE, rabbitmq).
-define(CONNECTOR_TYPE_BIN, <<"rabbitmq">>).
-define(ACTION_TYPE, rabbitmq).
-define(ACTION_TYPE_BIN, <<"rabbitmq">>).

-define(USER, <<"guest">>).
-define(PASSWORD, <<"guest">>).
-define(EXCHANGE, <<"messages">>).
-define(QUEUE, <<"test_queue">>).
-define(ROUTING_KEY, <<"test_routing_key">>).

-define(tcp, tcp).
-define(tls, tls).
-define(without_batch, without_batch).
-define(with_batch, with_batch).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_rabbitmq,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(?tcp, TCConfig) ->
    [
        {host, <<"rabbitmq">>},
        {port, 5672},
        {enable_tls, false}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    [
        {host, <<"rabbitmq">>},
        {port, 5671},
        {enable_tls, true}
        | TCConfig
    ];
init_per_group(?with_batch, TCConfig) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig];
init_per_group(?without_batch, TCConfig) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    SSL =
        case get_config(enable_tls, TCConfig, false) of
            true ->
                emqx_utils_maps:binary_key_map(
                    emqx_bridge_rabbitmq_testlib:ssl_options(true)
                );
            false ->
                emqx_utils_maps:binary_key_map(
                    emqx_bridge_rabbitmq_testlib:ssl_options(false)
                )
        end,
    ConnectorConfig = connector_config(#{
        <<"server">> => get_config(host, TCConfig, <<"rabbitmq">>),
        <<"port">> => get_config(port, TCConfig, 5672),
        <<"ssl">> => SSL
    }),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>)
        }
    }),
    ClientOpts = #{
        host => get_config(host, TCConfig, <<"rabbitmq">>),
        port => get_config(port, TCConfig, 5672),
        use_tls => get_config(enable_tls, TCConfig, false),
        exchange => ?EXCHANGE,
        queue => ?QUEUE,
        routing_key => ?ROUTING_KEY
    },
    emqx_bridge_rabbitmq_testlib:connect_and_setup_exchange_and_queue(ClientOpts),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {client_opts, ClientOpts}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:stop(),
    ClientOpts = get_config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:cleanup_client_and_queue(ClientOpts),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:connector_config(Overrides).

action_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:action_config(Overrides).

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

receive_message(TCConfig) ->
    ClientOpts = get_config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:receive_message(ClientOpts).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

delete_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(Kind, Type, Name).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts) ->
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

prepare_conf_file(Name, Content, TCConfig) ->
    emqx_config_SUITE:prepare_conf_file(Name, Content, TCConfig).

payload() ->
    payload(_Overrides = #{}).

payload(Overrides) ->
    maps:merge(
        #{<<"key">> => 42, <<"data">> => <<"RabbitMQ">>, <<"timestamp">> => 10000},
        Overrides
    ).

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [[?tcp], [?tls]];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "rabbitmq_connector_stop").

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [[?tcp, ?without_batch], [?tcp, ?with_batch], [?tls, ?without_batch]];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?assertMatch(
            #{payload := Payload},
            receive_message(TCConfig)
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_send_message_query_with_template() ->
    [{matrix, true}].
t_send_message_query_with_template(matrix) ->
    [[?tcp, ?without_batch], [?tcp, ?with_batch]];
t_send_message_query_with_template(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"payload_template">> =>
                <<
                    "{"
                    "      \"key\": ${payload.key},"
                    "      \"data\": \"${payload.data}\","
                    "      \"timestamp\": ${payload.timestamp},"
                    "      \"secret\": 42"
                    "}"
                >>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = #{
        <<"key">> => 7,
        <<"data">> => <<"RabbitMQ">>,
        <<"timestamp">> => 10_000
    },
    PayloadBin = emqx_utils_json:encode(Payload),
    emqtt:publish(C, Topic, PayloadBin, [{qos, 1}]),
    %% Check that the data got to the database
    ExpectedResult = Payload#{<<"secret">> => 42},
    ?assertMatch(
        #{payload := ExpectedResult},
        receive_message(TCConfig),
        #{expected => ExpectedResult}
    ),
    ok.

t_heavy_batching() ->
    [{matrix, true}].
t_heavy_batching(matrix) ->
    [[?tcp, ?with_batch]];
t_heavy_batching(TCConfig) ->
    ClientOpts = get_config(client_opts, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    NumberOfMessages = 1_000,
    emqx_utils:pforeach(
        fun(N) ->
            emqx:publish(emqx_message:make(Topic, integer_to_binary(N)))
        end,
        lists:seq(1, NumberOfMessages)
    ),
    {Connection, Channel} = emqx_bridge_rabbitmq_testlib:connect_client(ClientOpts),
    AllMessages = lists:foldl(
        fun(_, Acc) ->
            #{payload := Key} = emqx_bridge_rabbitmq_testlib:receive_message(
                Connection, Channel, ClientOpts
            ),
            Acc#{Key => true}
        end,
        #{},
        lists:seq(1, NumberOfMessages)
    ),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ?assertEqual(NumberOfMessages, maps:size(AllMessages)),
    ok.

t_action_stop(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    %% Emulate channel close hitting the timeout
    on_exit(fun meck:unload/0),
    meck:new(amqp_channel, [passthrough, no_history]),
    meck:expect(amqp_channel, close, fun(_Pid) -> timer:sleep(infinity) end),
    %% Delete action should not exceed connector's ?CHANNEL_CLOSE_TIMEOUT
    ct:timetrap({seconds, 30}),
    {Time, _} = timer:tc(fun() -> delete_action_api(TCConfig) end),
    ?assert(Time < 4_500_000),
    meck:unload(),
    ok.

t_action_inexistent_exchange(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"exchange">> => <<"inexistent_exchange">>},
        <<"resource_opts">> => #{<<"request_ttl">> => <<"1s">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = unique_payload(),
    ?check_trace(
        begin
            emqtt:publish(C, Topic, Payload, [{qos, 1}]),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"disconnected">>}},
                    get_action_api(TCConfig)
                )
            ),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"dropped">> := 1,
                            <<"matched">> := 1,
                            <<"success">> := 0,
                            <<"failed">> := 0,
                            <<"received">> := 0
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [_ | _],
                ?of_kind(
                    [
                        emqx_bridge_rabbitmq_connector_rabbit_publish_failed_with_msg,
                        emqx_bridge_rabbitmq_connector_rabbit_publish_failed_con_not_ready
                    ],
                    Trace
                )
            ),
            ok
        end
    ),
    ok.

t_action_use_default_exchange(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"exchange">> => <<"">>,
            <<"routing_key">> => ?QUEUE
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = payload(#{<<"e">> => <<"">>, <<"r">> => <<"test_queue">>}),
    PayloadBin = emqx_utils_json:encode(Payload),
    emqtt:publish(C, Topic, PayloadBin, [{qos, 1}]),
    ?assertMatch(#{payload := Payload}, receive_message(TCConfig)),
    ?retry(
        _Interval0 = 500,
        _NAttempts0 = 10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"dropped">> := 0,
                    <<"matched">> := 1,
                    <<"success">> := 1,
                    <<"failed">> := 0,
                    <<"received">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

-doc """
Smoke tests for settings message header and property templates.
""".
t_header_props_templates(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"headers_template">> => [
                #{<<"key">> => <<"${.payload.hk1}">>, <<"value">> => <<"${.payload.hv1}">>},
                #{<<"key">> => <<"${.payload.hk2}">>, <<"value">> => <<"${.payload.hv2}">>}
            ],
            <<"properties_template">> => [
                #{<<"key">> => <<"app_id">>, <<"value">> => <<"myapp">>},
                #{<<"key">> => <<"cluster_id">>, <<"value">> => <<"mycluster">>},
                #{<<"key">> => <<"content_encoding">>, <<"value">> => <<"utf8">>},
                #{<<"key">> => <<"content_type">>, <<"value">> => <<"application/json">>},
                #{<<"key">> => <<"correlation_id">>, <<"value">> => <<"${.payload.pv1}">>},
                #{<<"key">> => <<"expiration">>, <<"value">> => <<"600000">>},
                #{<<"key">> => <<"message_id">>, <<"value">> => <<"${.payload.pv2}">>},
                #{<<"key">> => <<"reply_to">>, <<"value">> => <<"reply.to.me">>},
                #{<<"key">> => <<"timestamp">>, <<"value">> => <<"${publish_received_at}">>},
                #{<<"key">> => <<"type">>, <<"value">> => <<"orders.created">>},
                #{<<"key">> => <<"user_id">>, <<"value">> => ?USER}
            ]
        },
        <<"resource_opts">> => #{<<"request_ttl">> => <<"1s">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    CorrelationId = <<"123456_correlation">>,
    MessageId = <<"some_message_id">>,
    Payload = payload(#{
        <<"hk1">> => <<"header_key1">>,
        <<"hv1">> => <<"header_val1">>,
        <<"hk2">> => <<"header_key2">>,
        <<"hv2">> => <<"header_val2">>,
        <<"pk1">> => <<"correlation_id">>,
        <<"pv1">> => CorrelationId,
        <<"pk2">> => <<"message_id">>,
        <<"pv2">> => MessageId
    }),
    PayloadBin = emqx_utils_json:encode(Payload),
    {ok, _} = emqtt:publish(C, Topic, PayloadBin, [{qos, 1}]),
    Msg = receive_message(TCConfig),
    ?assertMatch(
        #{
            payload := Payload,
            props := #{
                app_id := <<"myapp">>,
                cluster_id := <<"mycluster">>,
                content_encoding := <<"utf8">>,
                content_type := <<"application/json">>,
                correlation_id := CorrelationId,
                expiration := <<"600000">>,
                message_id := MessageId,
                reply_to := <<"reply.to.me">>,
                timestamp := TS,
                type := <<"orders.created">>,
                user_id := ?USER
            },
            headers := [
                {<<"header_key1">>, binary, <<"header_val1">>},
                {<<"header_key2">>, binary, <<"header_val2">>}
            ]
        } when TS /= undefined,
        Msg
    ),
    ok.

t_action_dynamic(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"exchange">> => <<"${payload.e}">>,
            <<"routing_key">> => <<"${payload.r}">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = payload(#{<<"e">> => ?EXCHANGE, <<"r">> => ?ROUTING_KEY}),
    PayloadBin = emqx_utils_json:encode(Payload),
    {ok, _} = emqtt:publish(C, Topic, PayloadBin, [{qos, 1}]),
    #{payload := Msg} = receive_message(TCConfig),
    ?assertMatch(Payload, Msg),
    ?retry(
        _Interval0 = 500,
        _NAttempts0 = 10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"dropped">> := 0,
                    <<"matched">> := 1,
                    <<"success">> := 1,
                    <<"failed">> := 0,
                    <<"received">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.
