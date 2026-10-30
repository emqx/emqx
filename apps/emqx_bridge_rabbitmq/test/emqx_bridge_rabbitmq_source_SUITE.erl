%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rabbitmq_source_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, rabbitmq).
-define(CONNECTOR_TYPE_BIN, <<"rabbitmq">>).
-define(SOURCE_TYPE, rabbitmq).
-define(SOURCE_TYPE_BIN, <<"rabbitmq">>).

-define(USER, <<"guest">>).
-define(PASSWORD, <<"guest">>).
-define(EXCHANGE, <<"messages">>).
-define(QUEUE, <<"test_queue">>).
-define(ROUTING_KEY, <<"test_routing_key">>).

-define(tcp, tcp).
-define(tls, tls).
-define(ack, ack).
-define(no_ack, no_ack).

-define(POOL_SIZE, 3).

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
init_per_group(?ack, TCConfig) ->
    [{no_ack, false} | TCConfig];
init_per_group(?no_ack, TCConfig) ->
    [{no_ack, true} | TCConfig];
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
    SourceName = ConnectorName,
    SourceConfig = source_config(#{
        <<"connector">> => ConnectorName
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
        {bridge_kind, source},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {source_type, ?SOURCE_TYPE},
        {source_name, SourceName},
        {source_config, SourceConfig},
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

source_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:source_config(Overrides).

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

create_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api([{bridge_kind, source} | TCConfig], Overrides).

get_source_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_source_metrics_api(TCConfig).

get_source_api(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(emqx_bridge_v2_testlib:get_source_api(Type, Name)).

get_connector_api(TCConfig) ->
    Type = get_config(connector_type, TCConfig),
    Name = get_config(connector_name, TCConfig),
    emqx_bridge_v2_testlib:simplify_result(emqx_bridge_v2_testlib:get_connector_api(Type, Name)).

delete_source_api(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(source, Type, Name).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

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

publish_message(Payload, TCConfig) ->
    ClientOpts = get_config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:publish_message(Payload, ClientOpts).

%% Ready messages and consumers of the test queue, from a passive `queue.declare'.
queue_counts(TCConfig) ->
    #{queue := Queue} = ClientOpts = get_config(client_opts, TCConfig),
    {Connection, Channel} = emqx_bridge_rabbitmq_testlib:connect_client(ClientOpts),
    #'queue.declare_ok'{message_count = Messages, consumer_count = Consumers} =
        amqp_channel:call(Channel, #'queue.declare'{queue = Queue, passive = true}),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    #{messages => Messages, consumers => Consumers}.

receive_payloads(Topic, N) ->
    receive_payloads(Topic, N, []).

receive_payloads(_Topic, 0, Acc) ->
    lists:sort(Acc);
receive_payloads(Topic, N, Acc) ->
    receive
        {publish, #{topic := Topic, payload := Payload}} ->
            #{<<"payload">> := P} = emqx_utils_json:decode(Payload),
            receive_payloads(Topic, N - 1, [P | Acc])
    after 10_000 ->
        ct:fail({missing_publishes, N, Acc})
    end.

mark_ready_and_wait_waiter() ->
    ?wait_async_action(
        emqx_node_readiness:mark_ready(),
        #{?snk_kind := resource_ready_waiter_ran},
        5_000
    ).

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

t_source(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_source_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(
        <<
            "select *, queue as payload.queue, exchange as payload.exchange,"
            "routing_key as payload.routing_key from \"${t}\""
        >>,
        TCConfig
    ),
    C = start_client(),
    {ok, #{}, [0]} = emqtt:subscribe(C, Topic, [{qos, 0}, {rh, 0}]),
    Payload = payload(),
    PayloadBin = emqx_utils_json:encode(Payload),
    publish_message(PayloadBin, TCConfig),
    {publish, #{payload := ReceivedPayload}} = ?assertReceive(
        {publish, #{
            dup := false,
            properties := undefined,
            topic := Topic,
            qos := 0,
            payload := _,
            retain := false
        }}
    ),
    Meta = #{
        <<"exchange">> => ?EXCHANGE,
        <<"routing_key">> => ?ROUTING_KEY,
        <<"queue">> => ?QUEUE
    },
    ExpectedPayload = maps:merge(payload(), Meta),
    ?assertMatch(
        #{<<"payload">> := ExpectedPayload},
        emqx_utils_json:decode(ReceivedPayload),
        #{expected => ExpectedPayload}
    ),
    ?retry(
        500,
        20,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"received">> := 1,
                    <<"matched">> := 0,
                    <<"success">> := 0,
                    <<"failed">> := 0,
                    <<"dropped">> := 0
                }
            }},
            get_source_metrics_api(TCConfig)
        )
    ),
    ok.

%% Checks that a source added while the node is not ready leaves the messages in the
%% queue, with no consumer, and reports connected. Once the node is ready, every pool
%% connection consumes and the messages reach the rule.
t_consume_after_node_ready() ->
    [{matrix, true}].
t_consume_after_node_ready(matrix) ->
    [[?ack], [?no_ack]];
t_consume_after_node_ready(TCConfig) when is_list(TCConfig) ->
    NumMessages = 5,
    Payloads = lists:sort([
        <<"msg-", (integer_to_binary(I))/binary>>
     || I <- lists:seq(1, NumMessages)
    ]),
    %% Stands for a subscription that exists when the node becomes ready; the node
    %% refuses new MQTT connections while it is not ready.
    C = start_client(),
    on_exit(fun emqx_node_readiness:mark_ready/0),
    ok = emqx_node_readiness:mark_not_ready(),
    lists:foreach(fun(P) -> publish_message(P, TCConfig) end, Payloads),
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(TCConfig, #{<<"pool_size">> => ?POOL_SIZE}),
    {201, #{<<"status">> := <<"connected">>}} =
        create_source_api(TCConfig, #{
            <<"parameters">> => #{<<"no_ack">> => get_config(no_ack, TCConfig)}
        }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, 1),
    ?assertEqual(#{messages => NumMessages, consumers => 0}, queue_counts(TCConfig)),
    ?assertMatch({200, #{<<"status">> := <<"connected">>}}, get_source_api(TCConfig)),
    ?assertMatch({ok, {ok, #{keys := [_, _, _]}}}, mark_ready_and_wait_waiter()),
    ?assertEqual(Payloads, receive_payloads(Topic, NumMessages)),
    ?retry(
        200,
        20,
        ?assertEqual(#{messages => 0, consumers => ?POOL_SIZE}, queue_counts(TCConfig))
    ),
    ok.

%% Checks that a source deleted while the node is not ready never consumes.
t_remove_before_node_ready(TCConfig) ->
    NumMessages = 3,
    lists:foreach(
        fun(I) -> publish_message(integer_to_binary(I), TCConfig) end,
        lists:seq(1, NumMessages)
    ),
    on_exit(fun emqx_node_readiness:mark_ready/0),
    ok = emqx_node_readiness:mark_not_ready(),
    {201, _} = create_connector_api(TCConfig, #{<<"pool_size">> => ?POOL_SIZE}),
    {201, _} = create_source_api(TCConfig, #{<<"parameters">> => #{<<"no_ack">> => false}}),
    {204, _} = delete_source_api(TCConfig),
    ?assertMatch({ok, {ok, #{keys := [_, _, _]}}}, mark_ready_and_wait_waiter()),
    ?assertEqual(#{messages => NumMessages, consumers => 0}, queue_counts(TCConfig)),
    ok.

%% Checks that a source on a missing queue fails to be added while the node is not
%% ready, and that the connector stays connected.
t_missing_queue_before_node_ready(TCConfig) ->
    on_exit(fun emqx_node_readiness:mark_ready/0),
    ok = emqx_node_readiness:mark_not_ready(),
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(TCConfig, #{<<"pool_size">> => ?POOL_SIZE}),
    {201, #{<<"status">> := <<"disconnected">>, <<"error">> := Error}} =
        create_source_api(TCConfig, #{
            <<"parameters">> => #{<<"queue">> => <<"missing_queue">>}
        }),
    ?assertMatch({_, _}, binary:match(Error, <<"NOT_FOUND">>), Error),
    ?assertMatch({200, #{<<"status">> := <<"connected">>}}, get_connector_api(TCConfig)),
    ok.
