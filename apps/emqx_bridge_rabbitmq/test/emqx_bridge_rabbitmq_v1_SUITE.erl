%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_v1_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% See comment in
%% apps/emqx_bridge_rabbitmq/test/emqx_bridge_rabbitmq_connector_SUITE.erl for how to
%% run this without bringing up the whole CI infrastructure
-define(TYPE, <<"rabbitmq">>).
-import(emqx_bridge_rabbitmq_test_utils, [
    rabbit_mq_exchange/0,
    rabbit_mq_routing_key/0,
    rabbit_mq_queue/0,
    rabbit_mq_host/0,
    rabbit_mq_port/0,
    get_rabbitmq/1,
    ssl_options/1,
    get_channel_connection/1,
    parse_and_check/4,
    receive_message_from_rabbitmq/1
]).
%%------------------------------------------------------------------------------
%% Common Test Setup, Tear down and Testcase List
%%------------------------------------------------------------------------------

all() ->
    [
        {group, tcp},
        {group, tls}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {tcp, AllTCs},
        {tls, AllTCs}
    ].

init_per_group(Group, Config) ->
    emqx_bridge_rabbitmq_test_utils:init_per_group(Group, Config).

end_per_group(Group, Config) ->
    emqx_bridge_rabbitmq_test_utils:end_per_group(Group, Config).

create_bridge(Name, Config) ->
    BridgeConfig = rabbitmq_config(Config),
    {ok, _} = emqx_bridge:create(?TYPE, Name, BridgeConfig),
    emqx_bridge_resource:bridge_id(?TYPE, Name).

delete_bridge(Name) ->
    ok = emqx_bridge:remove(?TYPE, Name).

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------

t_create_delete_bridge(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    RabbitMQ = get_rabbitmq(Config),
    create_bridge(Name, RabbitMQ),
    Bridges = emqx_bridge:list(),
    Any = fun(#{name := BName}) -> BName =:= Name end,
    ?assert(lists:any(Any, Bridges), Bridges),
    ok = delete_bridge(Name),
    BridgesAfterDelete = emqx_bridge:list(),
    ?assertNot(lists:any(Any, BridgesAfterDelete), BridgesAfterDelete),
    ok.

t_create_delete_bridge_non_existing_server(_Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_bridge(Name, #{server => <<"non_existing_server">>, port => 3174}),
    %% Check that the new bridge is in the list of bridges
    Bridges = emqx_bridge:list(),
    Any = fun(#{name := BName}) -> BName =:= Name end,
    ?assert(lists:any(Any, Bridges)),
    ok = delete_bridge(Name),
    BridgesAfterDelete = emqx_bridge:list(),
    ?assertNot(lists:any(Any, BridgesAfterDelete)),
    ok.

t_send_message_query(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    RabbitMQ = get_rabbitmq(Config),
    BridgeID = create_bridge(Name, RabbitMQ#{batch_size => 1}),
    Payload = #{<<"key">> => 42, <<"data">> => <<"RabbitMQ">>, <<"timestamp">> => 10000},
    %% This will use the SQL template included in the bridge
    emqx_bridge:send_message(BridgeID, Payload),
    %% Check that the data got to the database
    ?assertEqual(Payload, receive_message_from_rabbitmq(Config)),
    ok = delete_bridge(Name),
    ok.

t_send_message_query_with_template(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    RabbitMQ = get_rabbitmq(Config),
    BridgeID = create_bridge(Name, RabbitMQ#{
        batch_size => 1,
        payload_template => payload_template()
    }),
    Payload = #{
        <<"key">> => 7,
        <<"data">> => <<"RabbitMQ">>,
        <<"timestamp">> => 10000
    },
    emqx_bridge:send_message(BridgeID, Payload),
    %% Check that the data got to the database
    ExpectedResult = Payload#{
        <<"secret">> => 42
    },
    ?assertEqual(ExpectedResult, receive_message_from_rabbitmq(Config)),
    ok = delete_bridge(Name),
    ok.

t_send_simple_batch(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    RabbitMQ = get_rabbitmq(Config),
    BridgeConf = RabbitMQ#{batch_size => 100},
    BridgeID = create_bridge(Name, BridgeConf),
    Payload = #{<<"key">> => 42, <<"data">> => <<"RabbitMQ">>, <<"timestamp">> => 10000},
    emqx_bridge:send_message(BridgeID, Payload),
    ?assertEqual(Payload, receive_message_from_rabbitmq(Config)),
    ok = delete_bridge(Name),
    ok.

t_send_simple_batch_with_template(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    RabbitMQ = get_rabbitmq(Config),
    BridgeConf =
        RabbitMQ#{
            batch_size => 100,
            payload_template => payload_template()
        },
    BridgeID = create_bridge(Name, BridgeConf),
    Payload = #{
        <<"key">> => 7,
        <<"data">> => <<"RabbitMQ">>,
        <<"timestamp">> => 10000
    },
    emqx_bridge:send_message(BridgeID, Payload),
    ExpectedResult = Payload#{<<"secret">> => 42},
    ?assertEqual(ExpectedResult, receive_message_from_rabbitmq(Config)),
    ok = delete_bridge(Name),
    ok.

t_heavy_batching(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    NumberOfMessages = 20000,
    RabbitMQ = get_rabbitmq(Config),
    BridgeConf = RabbitMQ#{
        batch_size => 10173,
        batch_time_ms => 50
    },
    BridgeID = create_bridge(Name, BridgeConf),
    SendMessage = fun(Key) ->
        Payload = #{<<"key">> => Key},
        emqx_bridge:send_message(BridgeID, Payload)
    end,
    [SendMessage(Key) || Key <- lists:seq(1, NumberOfMessages)],
    AllMessages = lists:foldl(
        fun(_, Acc) ->
            Message = receive_message_from_rabbitmq(Config),
            #{<<"key">> := Key} = Message,
            Acc#{Key => true}
        end,
        #{},
        lists:seq(1, NumberOfMessages)
    ),
    ?assertEqual(NumberOfMessages, maps:size(AllMessages)),
    ok = delete_bridge(Name),
    ok.

rabbitmq_config(Config) ->
    UseTLS = maps:get(tls, Config, false),
    BatchSize = maps:get(batch_size, Config, 1),
    BatchTime = maps:get(batch_time_ms, Config, 0),
    Name = atom_to_binary(?MODULE),
    Server = maps:get(server, Config, rabbit_mq_host()),
    Port = maps:get(port, Config, rabbit_mq_port()),
    Template = maps:get(payload_template, Config, <<"">>),
    Bridge =
        #{
            <<"bridges">> => #{
                <<"rabbitmq">> => #{
                    Name => #{
                        <<"enable">> => true,
                        <<"ssl">> => ssl_options(UseTLS),
                        <<"server">> => Server,
                        <<"port">> => Port,
                        <<"username">> => <<"guest">>,
                        <<"password">> => <<"guest">>,
                        <<"routing_key">> => rabbit_mq_routing_key(),
                        <<"exchange">> => rabbit_mq_exchange(),
                        <<"payload_template">> => Template,
                        <<"resource_opts">> => #{
                            <<"batch_size">> => BatchSize,
                            <<"batch_time">> => BatchTime
                        }
                    }
                }
            }
        },
    parse_and_check(<<"bridges">>, emqx_bridge_schema, Bridge, Name).

payload_template() ->
    <<
        "{"
        "      \"key\": ${key},"
        "      \"data\": \"${data}\","
        "      \"timestamp\": ${timestamp},"
        "      \"secret\": 42"
        "}"
    >>.
