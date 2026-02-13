%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_nats_SUITE).

-include("emqx_nats.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_gateway_nats),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_auth,
            emqx_gateway,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"},
            emqx_gateway_nats
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_common_test_http:create_default_app(),
    _ = application:ensure_all_started(emqx_gateway_nats),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_TestCase, Config) ->
    _ = emqx_gateway_conf:unload_gateway(nats),
    ct:sleep(100),
    case needs_gateway(_TestCase) of
        true ->
            Port = emqx_common_test_helpers:select_free_port(tcp),
            Conf = nats_conf(Port),
            {ok, _} = emqx_gateway_conf:load_gateway(nats, Conf),
            ok = assert_can_connect(Port, 10),
            _ = emqx_gateway_test_utils:disable_gateway_auth(<<"nats">>),
            [
                {client_opts, default_client_opts(Port)},
                {group_name, tcp},
                {nats_port, Port}
                | Config
            ];
        false ->
            Config
    end.

end_per_testcase(TestCase, _Config) ->
    case needs_gateway(TestCase) of
        true ->
            _ = emqx_gateway_conf:unload_gateway(nats),
            ok;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_load_badconf_listener_in_use(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    {ok, LSock} = gen_tcp:listen(Port, [binary, {active, false}]),
    Conf = nats_conf(Port),
    try
        ?assertMatch({error, {badconf, _}}, emqx_gateway_conf:load_gateway(nats, Conf))
    after
        gen_tcp:close(LSock)
    end.

t_load_badconf_partial_authn_jwt(_Config) ->
    BaseConf1 = nats_conf(emqx_common_test_helpers:select_free_port(tcp)),
    MissingTrustedOperators = BaseConf1#{
        <<"authn_jwt">> => #{
            <<"resolver">> => #{
                <<"type">> => <<"memory">>,
                <<"resolver_preload">> => [
                    #{
                        <<"pubkey">> => <<"A">>,
                        <<"jwt">> => <<"jwt-account">>
                    }
                ]
            }
        }
    },
    ?assertMatch(
        {error, #{kind := validation_error, path := "gateway.nats.authn_jwt"}},
        emqx_gateway_conf:load_gateway(nats, MissingTrustedOperators)
    ),

    BaseConf2 = nats_conf(emqx_common_test_helpers:select_free_port(tcp)),
    MissingResolverPreload = BaseConf2#{
        <<"authn_jwt">> => #{
            <<"trusted_operators">> => [<<"OP">>]
        }
    },
    ?assertMatch(
        {error, #{kind := validation_error, path := "gateway.nats.authn_jwt"}},
        emqx_gateway_conf:load_gateway(nats, MissingResolverPreload)
    ).

t_load_badconf_authn_jwt_cache_ttl(_Config) ->
    BaseConf = nats_conf(emqx_common_test_helpers:select_free_port(tcp)),
    UnsupportedCacheTTL = BaseConf#{
        <<"authn_jwt">> => #{
            <<"trusted_operators">> => [<<"OP">>],
            <<"resolver">> => #{
                <<"type">> => <<"memory">>,
                <<"resolver_preload">> => [
                    #{
                        <<"pubkey">> => <<"A">>,
                        <<"jwt">> => <<"jwt-account">>
                    }
                ]
            },
            <<"cache_ttl">> => <<"5m">>
        }
    },
    ?assertMatch(
        {error, #{kind := validation_error, path := "gateway.nats.authn_jwt"}},
        emqx_gateway_conf:load_gateway(nats, UnsupportedCacheTTL)
    ).

t_load_update_unload(_Config) ->
    Port1 = emqx_common_test_helpers:select_free_port(tcp),
    Port2 = emqx_common_test_helpers:select_free_port(tcp),
    Conf1 = nats_conf(Port1),
    {ok, _} = emqx_gateway_conf:load_gateway(nats, Conf1),
    ok = assert_can_connect(Port1, 10),
    Raw0 = emqx:get_raw_config([gateway]),
    Raw1 = Raw0#{<<"nats">> => nats_raw_conf(Port2)},
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw1)),
    ok = assert_can_connect(Port2, 10),
    ok = emqx_gateway_conf:unload_gateway(nats),
    ?assertMatch({error, _}, gen_tcp:connect("127.0.0.1", Port2, [binary], 1000)).

t_update_error_listener_in_use(_Config) ->
    Port1 = emqx_common_test_helpers:select_free_port(tcp),
    Port2 = emqx_common_test_helpers:select_free_port(tcp),
    Conf1 = nats_conf(Port1),
    {ok, _} = emqx_gateway_conf:load_gateway(nats, Conf1),
    {ok, LSock} = gen_tcp:listen(Port2, [binary, {active, false}]),
    try
        GwConf0 = emqx:get_config([gateway, nats]),
        GwConf1 = emqx_utils_maps:deep_put([listeners, tcp, default, bind], GwConf0, Port2),
        ?assertMatch({error, _}, emqx_gateway:update(nats, GwConf1))
    after
        gen_tcp:close(LSock),
        _ = emqx_gateway_conf:unload_gateway(nats)
    end.

t_gateway_client_management(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => <<"test_user">>,
            verbose => true
        }
    ),

    %% Start a client
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    %% Test list clients
    [ClientInfo0] = emqx_gateway_test_utils:list_gateway_clients(<<"nats">>),
    ?assertEqual(<<"test_user">>, maps:get(username, ClientInfo0)),
    %% ClientId assigned by emqx_gateway_nats, it's a random string.
    %% We can get it from the client info.
    ClientId = maps:get(clientid, ClientInfo0),

    %% Test get client info
    ClientInfo = emqx_gateway_test_utils:get_gateway_client(<<"nats">>, ClientId),
    ?assertEqual(ClientId, maps:get(clientid, ClientInfo)),
    ?assertEqual(true, maps:get(connected, ClientInfo)),

    %% Test kick client
    ok = emqx_gateway_test_utils:kick_gateway_client(<<"nats">>, ClientId),
    {ok, [ErrorMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR,
            message = <<"Kicked out">>
        },
        ErrorMsg
    ),

    emqx_nats_client:stop(Client).

t_gateway_client_subscription_management(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => <<"test_user">>,
            verbose => true
        }
    ),
    Topic = <<"test/subject">>,
    Subject = <<"test.subject">>,
    QueueSubject = <<"test.subject.queue">>,
    Queue = <<"queue-1">>,
    %% Start a client
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    [ClientInfo0] = emqx_gateway_test_utils:list_gateway_clients(<<"nats">>),
    ?assertEqual(<<"test_user">>, maps:get(username, ClientInfo0)),
    ClientId = maps:get(clientid, ClientInfo0),

    %% Create subscription by client
    ok = emqx_nats_client:subscribe(Client, Subject, <<"sid-1">>),
    {ok, [SubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        SubAck
    ),

    %% Create queue subscription by client
    ok = emqx_nats_client:subscribe(Client, QueueSubject, <<"sid-2">>, Queue),
    {ok, [SubAck2]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        SubAck2
    ),

    %% Verify subscription list
    Subscriptions = emqx_gateway_test_utils:get_gateway_client_subscriptions(<<"nats">>, ClientId),
    ?assertEqual(2, length(Subscriptions)),

    %% XXX: Not implemented yet
    ?assertMatch(
        {400, _},
        emqx_gateway_test_utils:create_gateway_client_subscription(<<"nats">>, ClientId, Topic)
    ),

    %% XXX: Not implemented yet
    ?assertMatch(
        {400, _},
        emqx_gateway_test_utils:delete_gateway_client_subscription(<<"nats">>, ClientId, Topic)
    ),

    %% Delete subscription by client
    ok = emqx_nats_client:unsubscribe(Client, <<"sid-1">>),
    {ok, [UnsubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        UnsubAck
    ),
    %% Delete queue subscription by client
    ok = emqx_nats_client:unsubscribe(Client, <<"sid-2">>),
    {ok, [UnsubAck2]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        UnsubAck2
    ),

    ?assertEqual(
        [], emqx_gateway_test_utils:get_gateway_client_subscriptions(<<"nats">>, ClientId)
    ),

    emqx_nats_client:stop(Client).

t_clientinfo_override_with_empty_clientid(Config) ->
    update_nats_with_clientinfo_override(#{<<"clientid">> => <<>>}),

    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => <<"test_user">>,
            pass => <<"password">>,
            verbose => true
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    wait_for_client_info(Config),
    [ClientInfo] = find_client_by_username(<<"test_user">>),
    ?assertNotEqual(undefined, maps:get(clientid, ClientInfo)),
    ?assertNotEqual(<<>>, maps:get(clientid, ClientInfo)),

    emqx_nats_client:stop(Client).

t_clientinfo_override_with_prefix_and_empty_clientid(Config) ->
    update_nats_with_clientinfo_override(#{<<"clientid">> => <<"prefix-${Packet.clientid}">>}),

    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => <<"test_user">>,
            pass => <<"password">>,
            verbose => true
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    wait_for_client_info(Config),
    [ClientInfo] = find_client_by_username(<<"test_user">>),
    ?assertNotEqual(undefined, maps:get(clientid, ClientInfo)),
    ?assertEqual(<<"prefix-">>, maps:get(clientid, ClientInfo)),

    update_nats_with_clientinfo_override(#{}),
    emqx_nats_client:stop(Client).

t_schema_coverage(_Config) ->
    _ = emqx_nats_schema:fields(wss_listener),
    _ = emqx_nats_schema:desc(wss_listener),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

nats_conf(Port) ->
    nats_conf_list([listener(<<"default">>, Port)]).

nats_conf_list(Listeners) ->
    #{
        <<"server_id">> => <<"emqx_nats_gateway">>,
        <<"server_name">> => <<"emqx_nats_gateway">>,
        <<"default_heartbeat_interval">> => <<"2s">>,
        <<"heartbeat_wait_timeout">> => <<"1s">>,
        <<"protocol">> => #{<<"max_payload_size">> => 1024},
        <<"listeners">> => Listeners
    }.

listener(Name, Port) ->
    #{
        <<"type">> => <<"tcp">>,
        <<"name">> => Name,
        <<"bind">> => Port
    }.

nats_raw_conf(Port) ->
    #{
        <<"server_id">> => <<"emqx_nats_gateway">>,
        <<"server_name">> => <<"emqx_nats_gateway">>,
        <<"default_heartbeat_interval">> => <<"2s">>,
        <<"heartbeat_wait_timeout">> => <<"1s">>,
        <<"protocol">> => #{<<"max_payload_size">> => 1024},
        <<"listeners">> => #{
            <<"tcp">> => #{
                <<"default">> => #{<<"bind">> => Port}
            }
        }
    }.

assert_can_connect(_Port, 0) ->
    exit({connect_failed, timeout});
assert_can_connect(Port, Attempts) ->
    case gen_tcp:connect("127.0.0.1", Port, [binary], 1000) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        _Error ->
            timer:sleep(200),
            assert_can_connect(Port, Attempts - 1)
    end.

default_client_opts(Port) ->
    #{
        host => "tcp://127.0.0.1",
        port => Port,
        verbose => false
    }.

needs_gateway(TestCase) ->
    lists:member(
        TestCase,
        [
            t_gateway_client_management,
            t_gateway_client_subscription_management,
            t_clientinfo_override_with_empty_clientid,
            t_clientinfo_override_with_prefix_and_empty_clientid
        ]
    ).

update_nats_with_clientinfo_override(ClientInfoOverride) ->
    DefaultOverride = #{
        <<"username">> => <<"${Packet.user}">>,
        <<"password">> => <<"${Packet.pass}">>
    },
    ClientInfoOverride1 = maps:merge(DefaultOverride, ClientInfoOverride),
    Conf = emqx:get_raw_config([gateway, nats]),
    emqx_gateway_conf:update_gateway(
        nats,
        Conf#{<<"clientinfo_override">> => ClientInfoOverride1}
    ).

find_client_by_username(Username) ->
    ClientInfos = emqx_gateway_test_utils:list_gateway_clients(<<"nats">>),
    lists:filter(
        fun(ClientInfo) ->
            maps:get(username, ClientInfo) =:= Username
        end,
        ClientInfos
    ).

wait_for_client_info(Config) ->
    case ?config(group_name, Config) of
        ws ->
            timer:sleep(1000);
        wss ->
            timer:sleep(1000);
        _ ->
            ok
    end.
