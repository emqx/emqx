%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ocpp_SUITE).

-include("emqx_ocpp.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_gateway_test_utils,
    [
        assert_fields_exist/2,
        request/2,
        request/3
    ]
).

-define(CONF_DEFAULT,
    ~b"""
    authorization.include_mountpoint = true
    gateway.ocpp {
        mountpoint = "ocpp/"
        default_heartbeat_interval = "60s"
        heartbeat_checking_times_backoff = 1
        message_format_checking = disable
        upstream {
            topic = "cp/${clientid}"
            reply_topic = "cp/${clientid}/Reply"
            error_topic = "cp/${clientid}/Reply"
        }
        dnstream {
            topic = "cs/${clientid}"
        }
        listeners.ws.default {
            bind = "0.0.0.0:33033"
            enable_authn = false
            websocket.path = "/ocpp"
        }
    }
    """
).

all() -> [{group, legacy}, {group, hardened}].

groups() ->
    Tests = emqx_common_test_helpers:all(?MODULE),
    [{legacy, [], Tests}, {hardened, [], Tests}].

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    ok = emqx_common_test_helpers:set_security_profile(Profile),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, ?CONF_DEFAULT},
            emqx_gateway_ocpp,
            emqx_gateway,
            emqx_auth,
            emqx_auth_mnesia,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Profile, Config)}
    ),
    emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps}, {security_profile, Profile} | Config].

end_per_group(_Profile, Config) ->
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_common_test_helpers:clear_security_profile().

init_per_testcase(TestCase, Config) when
    TestCase == t_auth_expire;
    TestCase == t_listener_enable_authn_false_skips_authentication;
    TestCase == t_update_listeners
->
    ok = set_ocpp_listener_enable_authn(true),
    snabbkaffe:start_trace(),
    Config;
init_per_testcase(_TestCase, Config) ->
    ok = set_ocpp_listener_enable_authn(false),
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    snabbkaffe:stop(),
    ok.

default_config() ->
    ?CONF_DEFAULT.

update_ocpp_with_idle_timeout(IdleTimeout) ->
    Conf = emqx:get_raw_config([gateway, ocpp]),
    emqx_gateway_conf:update_gateway(
        ocpp,
        Conf#{<<"idle_timeout">> => IdleTimeout}
    ).

boot_notification(UniqueId) ->
    #{
        id => UniqueId,
        type => ?OCPP_MSG_TYPE_ID_CALL,
        action => <<"BootNotification">>,
        payload => #{
            <<"chargePointVendor">> => <<"vendor1">>,
            <<"chargePointModel">> => <<"model1">>
        }
    }.

ack_payload(UniqueId) ->
    emqx_utils_json:encode(#{
        <<"MessageTypeId">> => ?OCPP_MSG_TYPE_ID_CALLRESULT,
        <<"UniqueId">> => UniqueId,
        <<"Payload">> => #{
            <<"currentTime">> => "2026-06-11T00:00:00+00:00",
            <<"interval">> => 300,
            <<"status">> => <<"Accepted">>
        }
    }).

receive_broker_message(Topic, Timeout) ->
    receive
        {deliver, Topic, #message{payload = Payload}} ->
            {ok, emqx_utils_json:decode(Payload)}
    after Timeout ->
        {error, timeout}
    end.

get_subscriptions() ->
    lists:map(fun({_, Topic}) -> Topic end, ets:tab2list(emqx_subscription)).

with_gateway_authz_result(Protocol, ActionType, Topic, Result, Fun) ->
    Action = {?MODULE, gateway_authz_result, [Protocol, ActionType, Topic, Result]},
    ok = emqx_hooks:put('client.authorize', Action, ?HP_HIGHEST),
    try
        Fun()
    after
        ok = emqx_hooks:del('client.authorize', Action)
    end.

gateway_authz_result(
    #{protocol := Protocol},
    #{action_type := ActionType},
    Topic,
    _DefaultResult,
    Protocol,
    ActionType,
    Topic,
    Result
) ->
    {stop, #{result => Result, from => test, is_cacheable => false}};
gateway_authz_result(
    _ClientInfo,
    _Action,
    _Topic,
    DefaultResult,
    _Protocol,
    _ActionType,
    _ExpectedTopic,
    _Result
) ->
    {ok, DefaultResult}.

create_authenticator() ->
    AuthnConfig = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"username">>,
        <<"autogenerate_password">> => false,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"sha256">>,
            <<"salt_position">> => <<"prefix">>
        }
    },
    {ok, _} = emqx_gateway_conf:add_authn(<<"ocpp">>, AuthnConfig),
    ok.

delete_authenticators() ->
    _ = emqx_gateway_conf:remove_authn(<<"ocpp">>),
    emqx_authn_test_lib:delete_authenticators([gateway, ocpp, authentication], 'ocpp:global').

add_authenticator_user(UserId, Password) ->
    {ok, _} = emqx_authn_chains:add_user(
        'ocpp:global',
        <<"password_based:built_in_database">>,
        #{user_id => UserId, password => Password}
    ),
    ok.

set_ocpp_listener_enable_authn(EnableAuthn) ->
    RawCfg = emqx_conf:get_raw([gateway, ocpp], #{}),
    ListenerCfg = emqx_utils_maps:deep_get([<<"listeners">>, <<"ws">>, <<"default">>], RawCfg),
    {ok, _} = emqx_gateway_conf:update_listener(ocpp, {ws, default}, ListenerCfg#{
        <<"enable_authn">> => EnableAuthn
    }),
    ok.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_update_listeners(_Config) ->
    {200, [DefaultListener]} = request(get, "/gateways/ocpp/listeners"),

    ListenerConfKeys =
        [
            id,
            type,
            name,
            enable,
            enable_authn,
            bind,
            acceptors,
            max_connections,
            max_conn_rate,
            proxy_protocol,
            proxy_protocol_timeout,
            websocket,
            tcp_options
        ],
    StatusKeys = [status, node_status],

    assert_fields_exist(ListenerConfKeys ++ StatusKeys, DefaultListener),
    ?assertMatch(
        #{
            id := <<"ocpp:ws:default">>,
            type := <<"ws">>,
            name := <<"default">>,
            enable := true,
            enable_authn := true,
            bind := <<"0.0.0.0:33033">>,
            websocket := #{path := <<"/ocpp">>}
        },
        DefaultListener
    ),

    UpdateBody = emqx_utils_maps:deep_put(
        [websocket, path],
        maps:with(ListenerConfKeys, DefaultListener),
        <<"/ocpp2">>
    ),
    {200, _} = request(put, "/gateways/ocpp/listeners/ocpp:ws:default", UpdateBody),

    {200, [UpdatedListener]} = request(get, "/gateways/ocpp/listeners"),
    ?assertMatch(#{websocket := #{path := <<"/ocpp2">>}}, UpdatedListener),

    %% update listener back to default
    UpdateBody2 = emqx_utils_maps:deep_put(
        [websocket, path],
        maps:with(ListenerConfKeys, DefaultListener),
        <<"/ocpp">>
    ),
    {200, _} = request(put, "/gateways/ocpp/listeners/ocpp:ws:default", UpdateBody2),

    {200, [UpdatedListener2]} = request(get, "/gateways/ocpp/listeners"),
    ?assertMatch(#{websocket := #{path := <<"/ocpp">>}}, UpdatedListener2),
    ok.

t_enable_disable_gw_ocpp(_Config) ->
    AssertEnabled = fun(Enabled) ->
        {200, R} = request(get, "/gateways/ocpp"),
        E = maps:get(enable, R),
        ?assertEqual(E, Enabled),
        timer:sleep(500),
        ?assertEqual(E, emqx:get_config([gateway, ocpp, enable]))
    end,
    ?assertEqual({204, #{}}, request(put, "/gateways/ocpp/enable/false", <<>>)),
    AssertEnabled(false),
    ?assertEqual({204, #{}}, request(put, "/gateways/ocpp/enable/true", <<>>)),
    AssertEnabled(true).

t_init_respects_listener_enable_authn(_Config) ->
    Channel = emqx_ocpp_channel:init(
        #{
            peername => {{127, 0, 0, 1}, 12345},
            sockname => {{127, 0, 0, 1}, 33033}
        },
        #{
            ctx => undefined,
            listener => {ocpp, ws, default},
            enable_authn => false
        }
    ),
    ClientInfo = emqx_ocpp_channel:info(clientinfo, Channel),
    ?assertMatch(#{enable_authn := false}, ClientInfo),
    ?assertEqual(false, maps:is_key(enalbe_authn, ClientInfo)).

t_listener_enable_authn_false_skips_authentication(_Config) ->
    ClientId = <<"authn-disabled-ocpp-client">>,
    Username = <<"ocpp-user">>,
    AuthHeaders = [basic_auth_header(Username, <<"bad-password">>)],
    try
        ok = create_authenticator(),
        ok = add_authenticator_user(Username, <<"good-password">>),
        assert_connect_failed(connect("127.0.0.1", 33033, ClientId, AuthHeaders)),
        ok = set_ocpp_listener_enable_authn(false),
        {ok, Client} = connect("127.0.0.1", 33033, ClientId, AuthHeaders),
        close(Client)
    after
        ok = set_ocpp_listener_enable_authn(true),
        delete_authenticators()
    end.

t_adjust_keepalive_timer(_Config) ->
    {ok, Client} = connect("127.0.0.1", 33033, <<"client1">>),
    UniqueId = <<"3335862321">>,
    BootNotification = #{
        id => UniqueId,
        type => ?OCPP_MSG_TYPE_ID_CALL,
        action => <<"BootNotification">>,
        payload => #{
            <<"chargePointVendor">> => <<"vendor1">>,
            <<"chargePointModel">> => <<"model1">>
        }
    },
    ok = send_msg(Client, BootNotification),
    %% check the default keepalive timer
    timer:sleep(1000),
    ?assertMatch(
        #{conninfo := #{keepalive := 60}}, emqx_gateway_cm:get_chan_info(ocpp, <<"client1">>)
    ),
    %% publish the BootNotification.ack
    AckPayload = emqx_utils_json:encode(#{
        <<"MessageTypeId">> => ?OCPP_MSG_TYPE_ID_CALLRESULT,
        <<"UniqueId">> => UniqueId,
        <<"Payload">> => #{
            <<"currentTime">> => "2023-06-21T14:20:39+00:00",
            <<"interval">> => 300,
            <<"status">> => <<"Accepted">>
        }
    }),
    _ = emqx:publish2(emqx_message:make(<<"ocpp/cs/client1">>, AckPayload)),
    {ok, _Resp} = receive_msg(Client),
    %% assert: check the keepalive timer is adjusted
    ?assertMatch(
        #{conninfo := #{keepalive := 300}}, emqx_gateway_cm:get_chan_info(ocpp, <<"client1">>)
    ),
    %% close conns
    close(Client),
    timer:sleep(1000),
    %% assert:
    ?assertEqual(undefined, emqx_gateway_cm:get_chan_info(ocpp, <<"client1">>)),
    ok.

-doc "OCPP uses version 1.6 without a response subprotocol when the supported list is empty.".
t_optional_subprotocol_empty(_Config) ->
    check_optional_subprotocol(<<>>).

-doc "OCPP uses version 1.6 when the client omits an optional subprotocol header.".
t_optional_subprotocol_absent(_Config) ->
    lists:foreach(
        fun check_optional_subprotocol/1,
        [<<"ocpp1.6, ocpp2.0">>, <<"ocpp2.0, ocpp1.6">>]
    ).

check_optional_subprotocol(Supported) ->
    with_handshake_config(Supported, false, fun() ->
        with_handshake_client(<<"optional-protocol">>, [], valid_credentials(), fun(
            {ok, Client, Headers}
        ) ->
            ?assertEqual(undefined, proplists:get_value(<<"sec-websocket-protocol">>, Headers)),
            assert_authenticated_exchange(Client, <<"optional-protocol">>)
        end)
    end).

-doc "OCPP rejects wrong or missing credentials when the client omits an optional subprotocol.".
t_optional_subprotocol_authentication(_Config) ->
    lists:foreach(
        fun(Supported) ->
            with_handshake_config(Supported, false, fun check_handshake_credentials/0)
        end,
        [<<>>, <<"ocpp1.6, ocpp2.0">>]
    ).

check_handshake_credentials() ->
    lists:foreach(
        fun(Credentials) ->
            with_handshake_client(<<"rejected-credentials">>, [], Credentials, fun(Result) ->
                ?assertMatch({error, 400, _}, Result),
                ?assertEqual(
                    undefined, emqx_gateway_cm:get_chan_info(ocpp, <<"rejected-credentials">>)
                )
            end)
        end,
        [undefined, {<<"optional-user">>, <<"wrong-password">>}]
    ).

-doc "OCPP rejects a request that omits a required subprotocol header.".
t_required_subprotocol_absent(_Config) ->
    lists:foreach(
        fun(Supported) ->
            with_handshake_config(Supported, true, fun() ->
                assert_handshake_rejected([])
            end)
        end,
        [<<>>, <<"ocpp1.6, ocpp2.0">>]
    ).

-doc "OCPP selects a supported subprotocol in the client's preference order.".
t_subprotocol_negotiation(_Config) ->
    with_handshake_config(<<"ocpp2.0, ocpp1.6">>, true, fun() ->
        with_handshake_client(
            <<"negotiated-protocol">>,
            [<<"ocpp1.6">>, <<"ocpp2.0">>],
            valid_credentials(),
            fun({ok, Client, Headers}) ->
                ?assertEqual(
                    <<"ocpp1.6">>, proplists:get_value(<<"sec-websocket-protocol">>, Headers)
                ),
                assert_authenticated_exchange(Client, <<"negotiated-protocol">>)
            end
        )
    end).

-doc "OCPP rejects a request when no requested subprotocol is supported.".
t_subprotocol_not_supported(_Config) ->
    lists:foreach(
        fun(Supported) ->
            with_handshake_config(Supported, false, fun() ->
                assert_handshake_rejected([<<"unsupported">>])
            end)
        end,
        [<<>>, <<"ocpp1.6">>]
    ).

-doc "Only an absent subprotocol uses the internal OCPP 1.6 default.".
t_protocol_version_default(_Config) ->
    ?assertEqual({<<"OCPP">>, <<"1.6">>}, emqx_ocpp_connection:parse_protocol_name(undefined)),
    ?assertEqual({<<"OCPP">>, <<"1.6">>}, emqx_ocpp_connection:parse_protocol_name(<<"ocpp1.6">>)),
    ?assertError(function_clause, emqx_ocpp_connection:parse_protocol_name(<<"ocpp2.0">>)),
    ?assertError(function_clause, emqx_ocpp_connection:parse_protocol_name(<<"unknown">>)).

with_handshake_config(Supported, Required, Fun) ->
    Saved = emqx:get_raw_config([gateway, ocpp]),
    Listener = emqx_utils_maps:deep_get([<<"listeners">>, <<"ws">>, <<"default">>], Saved),
    ok = create_authenticator(),
    try
        ok = add_authenticator_user(<<"optional-user">>, <<"optional-password">>),
        Websocket = maps:get(<<"websocket">>, Listener, #{}),
        {ok, _} = emqx_gateway_conf:update_listener(ocpp, {ws, default}, Listener#{
            <<"enable_authn">> => true,
            <<"websocket">> => Websocket#{
                <<"supported_subprotocols">> => Supported,
                <<"fail_if_no_subprotocol">> => Required
            }
        }),
        Fun()
    after
        restore_handshake_config(Listener)
    end.

restore_handshake_config(Listener) ->
    try
        delete_authenticators()
    after
        {ok, _} = emqx_gateway_conf:update_listener(ocpp, {ws, default}, Listener)
    end.

valid_credentials() ->
    {<<"optional-user">>, <<"optional-password">>}.

with_handshake_client(ClientId, Protocols, Credentials, Fun) ->
    {ok, ConnPid} = gun:open("127.0.0.1", 33033, #{connect_timeout => 5000, retry => 0}),
    try
        {ok, _} = gun:await_up(ConnPid, 5000),
        Headers = handshake_auth_header(Credentials),
        Path = binary_to_list(<<"/ocpp/", ClientId/binary>>),
        WsOpts =
            case Protocols of
                [] -> #{};
                _ -> #{protocols => [{Protocol, gun_ws_h} || Protocol <- Protocols]}
            end,
        StreamRef = gun:ws_upgrade(ConnPid, Path, Headers, WsOpts),
        Fun(await_handshake(ConnPid, StreamRef))
    after
        gun:close(ConnPid),
        ?retry(100, 50, ?assertEqual(undefined, emqx_gateway_cm:get_chan_info(ocpp, ClientId)))
    end.

handshake_auth_header(undefined) ->
    [];
handshake_auth_header({Username, Password}) ->
    Encoded = base64:encode(<<Username/binary, ":", Password/binary>>),
    [{<<"authorization">>, <<"Basic ", Encoded/binary>>}].

await_handshake(ConnPid, StreamRef) ->
    receive
        {gun_upgrade, ConnPid, StreamRef, [<<"websocket">>], Headers} ->
            {ok, {ConnPid, StreamRef}, Headers};
        {gun_response, ConnPid, StreamRef, _, Status, Headers} ->
            {error, Status, Headers};
        {gun_error, ConnPid, StreamRef, Reason} ->
            ct:fail({upgrade_error, Reason})
    after 5000 ->
        ct:fail(upgrade_timeout)
    end.

assert_handshake_rejected(Protocols) ->
    with_handshake_client(<<"rejected-protocol">>, Protocols, valid_credentials(), fun(Result) ->
        ?assertMatch({error, 400, _}, Result),
        ?assertEqual(undefined, emqx_gateway_cm:get_chan_info(ocpp, <<"rejected-protocol">>))
    end).

assert_authenticated_exchange(Client, ClientId) ->
    ?retry(
        100,
        50,
        ?assertMatch(
            #{
                conninfo := #{proto_name := <<"OCPP">>, proto_ver := <<"1.6">>},
                clientinfo := #{
                    protocol := ocpp, clientid := ClientId, username := <<"optional-user">>
                }
            },
            emqx_gateway_cm:get_chan_info(ocpp, ClientId)
        )
    ),
    Upstream = <<"ocpp/cp/", ClientId/binary>>,
    Downstream = <<"ocpp/cs/", ClientId/binary>>,
    ?retry(100, 50, ?assert(lists:member(Downstream, get_subscriptions()))),
    ok = emqx:subscribe(Upstream),
    try
        UniqueId = <<"optional-handshake-message">>,
        ok = send_msg(Client, boot_notification(UniqueId)),
        ?assertReceive(
            {deliver, Upstream, #message{
                from = ClientId,
                headers = #{
                    protocol := ocpp, proto_ver := <<"1.6">>, username := <<"optional-user">>
                }
            }}
        ),
        _ = emqx:publish(emqx_message:make(Downstream, ack_payload(UniqueId))),
        ?assertMatch(
            {ok, #{type := ?OCPP_MSG_TYPE_ID_CALLRESULT, id := UniqueId}}, receive_msg(Client)
        )
    after
        emqx:unsubscribe(Upstream)
    end.

t_auth_expire(_Config) ->
    ok = meck:new(emqx_access_control, [passthrough, no_history]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) ->
            {ok, #{is_superuser => false, expire_at => erlang:system_time(millisecond) + 500}}
        end
    ),

    ?assertWaitEvent(
        {ok, _Client} = connect("127.0.0.1", 33033, <<"client1">>),
        #{
            ?snk_kind := conn_process_terminated,
            clientid := <<"client1">>,
            reason := {shutdown, expired}
        },
        5000
    ),

    meck:unload(emqx_access_control).

t_authz_denies_upstream_publish(_Config) ->
    ClientId = <<"authz-up-denied">>,
    AuthzTopic = <<"cp/", ClientId/binary>>,
    BrokerTopic = <<"ocpp/", AuthzTopic/binary>>,
    ok = emqx:subscribe(BrokerTopic),
    try
        with_gateway_authz_result(ocpp, publish, BrokerTopic, deny, fun() ->
            {ok, Client} = connect("127.0.0.1", 33033, ClientId),
            try
                ok = send_msg(Client, boot_notification(<<"authz-up-denied-id">>)),
                ?assertEqual({error, timeout}, receive_broker_message(BrokerTopic, 500))
            after
                close(Client)
            end
        end)
    after
        emqx:unsubscribe(BrokerTopic)
    end.

t_authz_allows_upstream_publish(_Config) ->
    ClientId = <<"authz-up-allowed">>,
    UniqueId = <<"authz-up-allowed-id">>,
    AuthzTopic = <<"cp/", ClientId/binary>>,
    BrokerTopic = <<"ocpp/", AuthzTopic/binary>>,
    ok = emqx:subscribe(BrokerTopic),
    try
        with_gateway_authz_result(ocpp, publish, BrokerTopic, allow, fun() ->
            {ok, Client} = connect("127.0.0.1", 33033, ClientId),
            try
                ok = send_msg(Client, boot_notification(UniqueId)),
                ?assertMatch(
                    {ok, #{
                        <<"MessageTypeId">> := ?OCPP_MSG_TYPE_ID_CALL,
                        <<"UniqueId">> := UniqueId,
                        <<"Action">> := <<"BootNotification">>
                    }},
                    receive_broker_message(BrokerTopic, 1000)
                )
            after
                close(Client)
            end
        end)
    after
        emqx:unsubscribe(BrokerTopic)
    end.

t_authz_denies_auto_subscribe(_Config) ->
    ClientId = <<"authz-dn-denied">>,
    UniqueId = <<"authz-dn-denied-id">>,
    AuthzTopic = <<"cs/", ClientId/binary>>,
    BrokerTopic = <<"ocpp/", AuthzTopic/binary>>,
    with_gateway_authz_result(ocpp, subscribe, BrokerTopic, deny, fun() ->
        {ok, Client} = connect("127.0.0.1", 33033, ClientId),
        try
            timer:sleep(100),
            ?assertEqual(false, lists:member(BrokerTopic, get_subscriptions())),
            _ = emqx:publish2(emqx_message:make(BrokerTopic, ack_payload(UniqueId))),
            ?assertMatch({error, {timeout, _}}, receive_msg(Client))
        after
            close(Client)
        end
    end).

t_authz_allows_auto_subscribe(_Config) ->
    ClientId = <<"authz-dn-allowed">>,
    UniqueId = <<"authz-dn-allowed-id">>,
    AuthzTopic = <<"cs/", ClientId/binary>>,
    BrokerTopic = <<"ocpp/", AuthzTopic/binary>>,
    with_gateway_authz_result(ocpp, subscribe, BrokerTopic, allow, fun() ->
        {ok, Client} = connect("127.0.0.1", 33033, ClientId),
        try
            timer:sleep(100),
            ?assertEqual(true, lists:member(BrokerTopic, get_subscriptions())),
            _ = emqx:publish2(emqx_message:make(BrokerTopic, ack_payload(UniqueId))),
            ?assertMatch(
                {ok, #{
                    type := ?OCPP_MSG_TYPE_ID_CALLRESULT,
                    id := UniqueId
                }},
                receive_msg(Client)
            )
        after
            close(Client)
        end
    end).

t_update_not_restart_listener(_Config) ->
    {ok, Client} = connect("127.0.0.1", 33033, <<"client1">>),
    %% update ocpp gateway config
    update_ocpp_with_idle_timeout(<<"20s">>),
    %% send BootNotification
    UniqueId = <<"3335862321">>,
    BootNotification = #{
        id => UniqueId,
        type => ?OCPP_MSG_TYPE_ID_CALL,
        action => <<"BootNotification">>,
        payload => #{
            <<"chargePointVendor">> => <<"vendor1">>,
            <<"chargePointModel">> => <<"model1">>
        }
    },
    ok = send_msg(Client, BootNotification),
    %% publish the BootNotification.ack
    AckPayload = emqx_utils_json:encode(#{
        <<"MessageTypeId">> => ?OCPP_MSG_TYPE_ID_CALLRESULT,
        <<"UniqueId">> => UniqueId,
        <<"Payload">> => #{
            <<"currentTime">> => "2023-06-21T14:20:39+00:00",
            <<"interval">> => 300,
            <<"status">> => <<"Accepted">>
        }
    }),
    _ = emqx:publish2(emqx_message:make(<<"ocpp/cs/client1">>, AckPayload)),
    %% receive the BootNotification.ack
    {ok, _Resp} = receive_msg(Client),

    close(Client),
    ok.

t_listeners_status(_Config) ->
    {200, [Listener]} = request(get, "/gateways/ocpp/listeners"),
    ?assertMatch(
        #{
            status := #{running := true, current_connections := 0}
        },
        Listener
    ),
    %% add a connection
    {ok, Client} = connect("127.0.0.1", 33033, <<"client1">>),
    UniqueId = <<"3335862321">>,
    BootNotification = #{
        id => UniqueId,
        type => ?OCPP_MSG_TYPE_ID_CALL,
        action => <<"BootNotification">>,
        payload => #{
            <<"chargePointVendor">> => <<"vendor1">>,
            <<"chargePointModel">> => <<"model1">>
        }
    },
    ok = send_msg(Client, BootNotification),
    timer:sleep(1000),
    %% assert: the current_connections is 1
    {200, [Listener1]} = request(get, "/gateways/ocpp/listeners"),
    ?assertMatch(
        #{
            status := #{running := true, current_connections := 1}
        },
        Listener1
    ),
    %% close conns
    close(Client),
    timer:sleep(1000),
    %% assert: the current_connections is 0
    {200, [Listener2]} = request(get, "/gateways/ocpp/listeners"),
    ?assertMatch(
        #{
            status := #{running := true, current_connections := 0}
        },
        Listener2
    ).

t_active_n(_Config) ->
    {ok, Client} = connect("127.0.0.1", 33033, <<"client1">>),
    UniqueId = <<"3335862321">>,
    BootNotification = #{
        id => UniqueId,
        type => ?OCPP_MSG_TYPE_ID_CALL,
        action => <<"BootNotification">>,
        payload => #{
            <<"chargePointVendor">> => <<"vendor1">>,
            <<"chargePointModel">> => <<"model1">>
        }
    },
    ok = send_msg(Client, BootNotification),
    timer:sleep(1000),
    AckPayload = emqx_utils_json:encode(#{
        <<"MessageTypeId">> => ?OCPP_MSG_TYPE_ID_CALLRESULT,
        <<"UniqueId">> => UniqueId,
        <<"Payload">> => #{
            <<"currentTime">> => "2023-06-21T14:20:39+00:00",
            <<"interval">> => 300,
            <<"status">> => <<"Accepted">>
        }
    }),
    lists:foreach(
        fun(_) ->
            _ = emqx:publish2(emqx_message:make(<<"ocpp/cs/client1">>, AckPayload)),
            {ok, _Resp} = receive_msg(Client)
        end,
        lists:seq(1, 20)
    ),
    %% close conns
    close(Client),
    timer:sleep(1000),
    %% assert:
    ?assertEqual(undefined, emqx_gateway_cm:get_chan_info(ocpp, <<"client1">>)),
    ok.

-doc """
A WebSocket message larger than the listener's `websocket.max_frame_size`
closes the connection with status code 1009. The default limit applies when
the option is not set, and an explicit value replaces it.
""".
t_ws_max_frame_size(_Config) ->
    {ok, Client1} = connect("127.0.0.1", 33033, <<"client_frame_default">>),
    ok = send_text(Client1, binary:copy(<<"a">>, 2097152 + 1)),
    ?assertEqual(1009, receive_close_code(Client1)),
    close(Client1),
    RawCfg = emqx_conf:get_raw([gateway, ocpp], #{}),
    ListenerCfg = emqx_utils_maps:deep_get([<<"listeners">>, <<"ws">>, <<"default">>], RawCfg),
    WsCfg = maps:get(<<"websocket">>, ListenerCfg, #{}),
    {ok, _} = emqx_gateway_conf:update_listener(ocpp, {ws, default}, ListenerCfg#{
        <<"websocket">> => WsCfg#{<<"max_frame_size">> => 1024}
    }),
    try
        {ok, Client2} = connect("127.0.0.1", 33033, <<"client_frame_explicit">>),
        ok = send_text(Client2, binary:copy(<<"a">>, 1025)),
        ?assertEqual(1009, receive_close_code(Client2)),
        close(Client2)
    after
        {ok, _} = emqx_gateway_conf:update_listener(ocpp, {ws, default}, ListenerCfg)
    end.

%%--------------------------------------------------------------------
%% ocpp simple client

connect(Host, Port, ClientId) ->
    connect(Host, Port, ClientId, []).

send_text({ConnPid, StreamRef}, Text) ->
    gun:ws_send(ConnPid, StreamRef, {text, Text}).

receive_close_code({ConnPid, StreamRef}) ->
    receive
        {gun_ws, ConnPid, StreamRef, {close, Code, _}} -> Code
    after 5000 ->
        ct:fail({no_ws_close, ?drainMailbox()})
    end.

connect(Host, Port, ClientId, ExtraHeaders) ->
    Timeout = 5000,
    ConnOpts = #{connect_timeout => 5000},
    case gun:open(Host, Port, ConnOpts) of
        {ok, ConnPid} ->
            {ok, _} = gun:await_up(ConnPid, Timeout),
            case upgrade(ConnPid, ClientId, Timeout, ExtraHeaders) of
                {ok, StreamRef} -> {ok, {ConnPid, StreamRef}};
                Error -> Error
            end;
        Error ->
            Error
    end.

upgrade(ConnPid, ClientId, Timeout, ExtraHeaders) ->
    Path = binary_to_list(<<"/ocpp/", ClientId/binary>>),
    WsHeaders = [{<<"cache-control">>, <<"no-cache">>} | ExtraHeaders],
    StreamRef = gun:ws_upgrade(ConnPid, Path, WsHeaders, #{protocols => [{<<"ocpp1.6">>, gun_ws_h}]}),
    receive
        {gun_upgrade, ConnPid, StreamRef, [<<"websocket">>], _Headers} ->
            {ok, StreamRef};
        {gun_response, ConnPid, _, _, Status, Headers} ->
            {error, {ws_upgrade_failed, Status, Headers}};
        {gun_error, ConnPid, StreamRef, Reason} ->
            {error, {ws_upgrade_failed, Reason}}
    after Timeout ->
        {error, timeout}
    end.

basic_auth_header(Username, Password) ->
    Credential = <<Username/binary, ":", Password/binary>>,
    Encoded = base64:encode(Credential),
    {<<"authorization">>, <<"Basic ", Encoded/binary>>}.

assert_connect_failed({ok, Client}) ->
    close(Client),
    ct:fail(expected_ocpp_websocket_authentication_failure);
assert_connect_failed({error, {ws_upgrade_failed, _Status, _Headers}}) ->
    ok;
assert_connect_failed(Error) ->
    ct:fail({unexpected_ocpp_websocket_connect_result, Error}).

send_msg({ConnPid, StreamRef}, Frame) when is_map(Frame) ->
    Opts = emqx_ocpp_frame:serialize_opts(),
    Msg = emqx_ocpp_frame:serialize_pkt(Frame, Opts),
    gun:ws_send(ConnPid, StreamRef, {text, Msg}).

receive_msg({ConnPid, StreamRef}) ->
    receive
        {gun_ws, ConnPid, StreamRef, {_Type, Msg}} ->
            ParseState = emqx_ocpp_frame:initial_parse_state(#{}),
            {ok, Frame, _Rest, _NewParseStaet} = emqx_ocpp_frame:parse(Msg, ParseState),
            {ok, Frame}
    after 5000 ->
        {error, {timeout, ?drainMailbox()}}
    end.

close({ConnPid, _StreamRef}) ->
    gun:shutdown(ConnPid).

-doc "OCPP returns HTTP 400 for a numeric hook rejection and upgrades after recovery.".
t_connect_hook_rejection(_) ->
    check_ocpp_hook_reason(?RC_QUOTA_EXCEEDED, <<"Quota exceeded">>).

-doc "OCPP preserves UTF-8 byte lists, binaries, and Unicode character-list hook reasons.".
t_ocpp_utf8_reason(_) ->
    Text = <<"拒绝"/utf8>>,
    Reasons = [
        binary_to_list(Text),
        [binary_to_list(<<"拒"/utf8>>), <<"绝"/utf8>>],
        Text,
        [16#62D2, 16#7EDD],
        [<<"拒"/utf8>>, [16#7EDD]]
    ],
    lists:foreach(fun(Reason) -> check_ocpp_hook_reason(Reason, Text) end, Reasons),
    check_ocpp_hook_reason([16#E9], <<"é"/utf8>>).

-doc "OCPP returns readable HTTP 400 text for a hook reason containing invalid UTF-8 bytes.".
t_ocpp_invalid_utf8_reason(_) ->
    check_ocpp_hook_reason(<<255>>, <<"<<255>>">>).

-doc "OCPP returns readable HTTP 400 text for a hook reason containing incomplete UTF-8 bytes.".
t_ocpp_incomplete_utf8_reason(_) ->
    check_ocpp_hook_reason(<<16#E4, 16#B8>>, <<"<<228,184>>">>).

-doc "OCPP returns readable HTTP 400 text when a hook reason is not valid character data.".
t_ocpp_invalid_chardata_reason(_) ->
    check_ocpp_hook_reason([invalid], <<"[invalid]">>).

check_ocpp_hook_reason(Reason, ExpectedText) ->
    emqx_gateway_test_utils:with_connect_rejection(Reason, fun() ->
        check_hook_connect({rejected, ExpectedText})
    end),
    check_hook_connect(accepted).

check_hook_connect(Expected) ->
    {ok, Conn} = gun:open("127.0.0.1", 33033, #{retry => 0}),
    try
        {ok, _} = gun:await_up(Conn, 5000),
        Stream = gun:ws_upgrade(
            Conn,
            <<"/ocpp/hook-rejection">>,
            handshake_auth_header(valid_credentials()),
            #{protocols => [{<<"ocpp1.6">>, gun_ws_h}]}
        ),
        case Expected of
            accepted ->
                ?assertMatch({ok, _, _}, await_handshake(Conn, Stream));
            {rejected, ExpectedText} ->
                ?assertMatch({response, _, 400, _}, gun:await(Conn, Stream, 5000)),
                ?assertEqual({ok, ExpectedText}, gun:await_body(Conn, Stream, 5000))
        end
    after
        gun:close(Conn),
        ?retry(
            100,
            50,
            ?assertEqual(undefined, emqx_gateway_cm:get_chan_info(ocpp, <<"hook-rejection">>))
        )
    end.
