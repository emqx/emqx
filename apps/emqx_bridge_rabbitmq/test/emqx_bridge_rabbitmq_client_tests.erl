%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_client_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

servers_from_config_test_() ->
    [
        {"uses inline ports",
            ?_assertEqual(
                [{"rmq1", 5672}, {"rmq2", 5673}],
                emqx_bridge_rabbitmq_client:servers_from_config(#{
                    servers => <<"rmq1:5672,rmq2:5673">>,
                    port => 1111
                })
            )},
        {"uses configured default port",
            ?_assertEqual(
                [{"rmq-legacy", 5671}],
                emqx_bridge_rabbitmq_client:servers_from_config(#{
                    servers => <<"rmq-legacy">>,
                    port => 5671
                })
            )},
        {"default port in servers list",
            ?_assertEqual(
                [{"rmq1", 5671}, {"rmq2", 5673}],
                emqx_bridge_rabbitmq_client:servers_from_config(#{
                    servers => <<"rmq1,rmq2:5673">>,
                    port => 5671
                })
            )}
    ].

rotate_servers_test() ->
    Servers = [{"a", 1}, {"b", 2}, {"c", 3}],
    ?assertEqual(Servers, emqx_bridge_rabbitmq_client:rotate_servers(Servers, 1)),
    ?assertEqual(
        [{"b", 2}, {"c", 3}, {"a", 1}], emqx_bridge_rabbitmq_client:rotate_servers(Servers, 2)
    ),
    ?assertEqual(
        [{"c", 3}, {"a", 1}, {"b", 2}], emqx_bridge_rabbitmq_client:rotate_servers(Servers, 3)
    ),
    ?assertEqual(
        [{"b", 2}, {"c", 3}, {"a", 1}], emqx_bridge_rabbitmq_client:rotate_servers(Servers, 5)
    ).

start_connection_failover_test() ->
    meck:new(amqp_connection, [passthrough, no_link]),
    try
        meck:expect(
            amqp_connection,
            start,
            fun(#amqp_params_network{host = Host, port = Port}) ->
                case {Host, Port} of
                    {"bad", 1} -> {error, econnrefused};
                    {"good", 5672} -> {ok, rabbitmq_conn_stub}
                end
            end
        ),
        ?assertEqual(
            {ok, rabbitmq_conn_stub},
            emqx_bridge_rabbitmq_client:start_connection(
                [{"bad", 1}, {"good", 5672}],
                #amqp_params_network{}
            )
        ),
        History = [
            {Host, Port}
         || {_Pid, {amqp_connection, start, [#amqp_params_network{host = Host, port = Port}]}, _Res} <-
                meck:history(amqp_connection)
        ],
        ?assertEqual([{"bad", 1}, {"good", 5672}], History)
    after
        meck:unload(amqp_connection)
    end.

start_connection_all_failed_test() ->
    meck:new(amqp_connection, [passthrough, no_link]),
    try
        meck:expect(amqp_connection, start, fun(#amqp_params_network{}) -> {error, econnrefused} end),
        ?assertMatch(
            {error, #{
                reason := all_nodes_failed,
                tried := [{"a", 1, econnrefused}, {"b", 2, econnrefused}]
            }},
            emqx_bridge_rabbitmq_client:start_connection(
                [{"a", 1}, {"b", 2}], #amqp_params_network{}
            )
        )
    after
        meck:unload(amqp_connection)
    end.

schema_test_() ->
    [
        {"accepts servers",
            ?_assertMatch(
                #{<<"servers">> := <<"rmq1:5672,rmq2:5672">>},
                emqx_bridge_rabbitmq_testlib:connector_config(#{
                    <<"servers">> => <<"rmq1:5672,rmq2:5672">>
                })
            )},
        {"accepts legacy server+port",
            ?_assertMatch(
                #{<<"servers">> := <<"rmq-legacy">>, <<"port">> := 5671},
                emqx_bridge_rabbitmq_testlib:connector_config(#{
                    <<"server">> => <<"rmq-legacy">>,
                    <<"port">> => 5671
                })
            )}
    ].
