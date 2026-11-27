%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_gateway_test_utils,
    [
        request/2,
        request/3
    ]
).

-include_lib("er_coap_client/include/coap.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, <<
    "\n"
    "gateway.coap\n"
    "{\n"
    "    idle_timeout = 30s\n"
    "    enable_stats = false\n"
    "    mountpoint = \"\"\n"
    "    notify_type = qos\n"
    "    connection_required = true\n"
    "    subscribe_qos = qos1\n"
    "    publish_qos = qos1\n"
    "\n"
    "    listeners.udp.default\n"
    "    {bind = 5683}\n"
    "}\n"
>>).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).
-define(PS_PREFIX, "coap://127.0.0.1/ps").
-define(MQTT_PREFIX, "coap://127.0.0.1/mqtt").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_gateway_coap),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, ?CONF_DEFAULT},
            emqx_gateway,
            emqx_auth,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

init_per_testcase(t_connection_with_authn_failed, Config) ->
    ok = meck:new(emqx_access_control, [passthrough]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) -> {error, bad_username_or_password} end
    ),
    Config;
init_per_testcase(t_connection_with_expire, Config) ->
    ok = meck:new(emqx_access_control, [passthrough, no_history]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) ->
            {ok, #{is_superuser => false, expire_at => erlang:system_time(millisecond) + 100}}
        end
    ),
    snabbkaffe:start_trace(),
    Config;
init_per_testcase(t_heartbeat, Config) ->
    NewHeartbeat = 800,
    OldConf = emqx:get_raw_config([gateway, coap]),
    {ok, _} = emqx_gateway_conf:update_gateway(
        coap,
        OldConf#{<<"heartbeat">> => <<"1s">>}
    ),
    [
        {old_conf, OldConf},
        {new_heartbeat, NewHeartbeat}
        | Config
    ];
init_per_testcase(_, Config) ->
    ok = meck:new(emqx_access_control, [passthrough]),
    Config.

end_per_testcase(t_heartbeat, Config) ->
    OldConf = ?config(old_conf, Config),
    {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf),
    ok;
end_per_testcase(t_connection_with_expire, Config) ->
    snabbkaffe:stop(),
    meck:unload(emqx_access_control),
    Config;
end_per_testcase(_, Config) ->
    ok = meck:unload(emqx_access_control),
    Config.

default_config() ->
    ?CONF_DEFAULT.

mqtt_prefix() ->
    ?MQTT_PREFIX.

ps_prefix() ->
    ?PS_PREFIX.

update_coap_with_connection_mode(Bool) ->
    Conf = emqx:get_raw_config([gateway, coap]),
    emqx_gateway_conf:update_gateway(
        coap,
        Conf#{<<"connection_required">> => atom_to_binary(Bool)}
    ).

update_coap_with_mountpoint(Mp) ->
    Conf = emqx:get_raw_config([gateway, coap]),
    emqx_gateway_conf:update_gateway(
        coap,
        Conf#{<<"mountpoint">> => Mp}
    ).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connection(_) ->
    Action = fun(Channel) ->
        emqx_gateway_test_utils:meck_emqx_hook_calls(),

        %% connection
        Token = connection(Channel),

        timer:sleep(100),
        ?assertNotEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),

        ?assertMatch(
            ['client.connect' | _],
            emqx_gateway_test_utils:collect_emqx_hooks_calls()
        ),

        %% heartbeat
        {ok, changed, _} = send_heartbeat(Token),

        disconnection(Channel, Token),

        timer:sleep(100),
        ?assertEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        )
    end,
    do(Action),
    ok.

t_mountpoint_after_authn(_) ->
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) ->
            {ok, #{client_attrs => #{<<"tenant">> => <<"tenant-1">>}}}
        end
    ),
    OldConf = emqx:get_raw_config([gateway, coap]),
    {ok, _} = emqx_gateway_conf:update_gateway(
        coap,
        OldConf#{<<"mountpoint">> => <<"coap/${client_attrs.tenant}/">>}
    ),
    try
        Action = fun(Channel) ->
            Token = connection(Channel),
            timer:sleep(100),
            ?assertNotEqual(
                [],
                emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
            ),
            #{clientinfo := ClientInfo} = emqx_gateway_cm:get_chan_info(coap, <<"client1">>),
            ?assertEqual(<<"coap/tenant-1/">>, maps:get(mountpoint, ClientInfo)),
            ?assertMatch(
                {ok, changed, _},
                send_heartbeat(Token)
            ),
            ?assertMatch(
                {ok, deleted, _},
                disconnection(Channel, Token)
            ),
            ok
        end,
        do(Action)
    after
        {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf)
    end,
    ok.

t_clientid_override_not_retained(_) ->
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) ->
            {ok, #{
                clientid_override => <<"trusted-id">>,
                client_attrs => #{<<"tenant">> => <<"tenant-1">>}
            }}
        end
    ),
    OldConf = emqx:get_raw_config([gateway, coap]),
    {ok, _} = emqx_gateway_conf:update_gateway(
        coap,
        OldConf#{<<"mountpoint">> => <<"coap/${client_attrs.tenant}/${clientid}/">>}
    ),
    try
        Action = fun(Channel) ->
            Token = connection(Channel),
            timer:sleep(100),
            #{clientinfo := ClientInfo} = emqx_gateway_cm:get_chan_info(coap, <<"client1">>),
            ?assertEqual(<<"client1">>, maps:get(clientid, ClientInfo)),
            ?assertEqual(false, maps:is_key(clientid_override, ClientInfo)),
            ?assertEqual(<<"coap/tenant-1/client1/">>, maps:get(mountpoint, ClientInfo)),
            disconnection(Channel, Token),
            ok
        end,
        do(Action)
    after
        {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf)
    end,
    ok.

t_connection_token_not_logged(_) ->
    Reports = emqx_cth_log_capture:capture(debug, fun() ->
        do(fun(Channel) ->
            Token = connection(Channel),
            put(coap_session_token, Token),
            disconnection(Channel, Token)
        end)
    end),
    TokenBin = list_to_binary(get(coap_session_token)),
    ?assertNotEqual([], Reports),
    %% No debug log may carry the token. Checking every report also covers the
    %% raw datagram dumps, which would otherwise defeat frame-level redaction.
    Leaks = [
        Report
     || Report <- Reports,
        binary:match(term_to_binary(Report), TokenBin) =/= nomatch
    ],
    ?assertEqual([], Leaks).

t_connection_with_short_param_name(_) ->
    Action = fun(Channel) ->
        %% connection
        Token = connection(Channel, true),

        timer:sleep(100),
        ?assertNotEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),

        %% heartbeat
        {ok, changed, _} = send_heartbeat(Token, true),

        disconnection(Channel, Token, true),

        timer:sleep(100),
        ?assertEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        )
    end,
    do(Action).

t_heartbeat(Config) ->
    Heartbeat = ?config(new_heartbeat, Config),
    Action = fun(Channel) ->
        Token = connection(Channel),

        timer:sleep(100),
        ?assertNotEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),

        %% must keep client connection alive
        Delay = Heartbeat div 2,
        lists:foreach(
            fun(_) ->
                ?assertMatch({ok, changed, _}, send_heartbeat(Token)),
                timer:sleep(Delay)
            end,
            lists:seq(1, 5)
        ),

        ?assertNotEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),
        %% The minimum timeout time is 1 second.
        %% 1.5 * Heartbeat + 0.5 * Heartbeat(< 1s) = 1.5 * 1 + 1 = 2.5
        timer:sleep(Heartbeat * 2 + 1000),
        ?assertEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        ),

        disconnection(Channel, Token),

        timer:sleep(100),
        ?assertEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        )
    end,
    do(Action).

t_connection_optional_params(_) ->
    UsernamePasswordAreOptional =
        fun(Channel) ->
            URI =
                ?MQTT_PREFIX ++
                    "/connection?clientid=client1",
            Req = make_req(post),
            {ok, created, Data} = do_request(Channel, URI, Req),
            #coap_content{payload = Token0} = Data,
            Token = binary_to_list(Token0),

            timer:sleep(100),
            ?assertNotEqual(
                [],
                emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
            ),

            disconnection(Channel, Token),

            timer:sleep(100),
            ?assertEqual(
                [],
                emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
            )
        end,
    ClientIdIsRequired =
        fun(Channel) ->
            URI =
                ?MQTT_PREFIX ++
                    "/connection",
            Req = make_req(post),
            {error, bad_request, _} = do_request(Channel, URI, Req)
        end,
    do(UsernamePasswordAreOptional),
    do(ClientIdIsRequired).

t_connection_with_authn_failed(_) ->
    {ok, _Sock, Channel} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    URI =
        ?MQTT_PREFIX ++
            "/connection?clientid=client1&username=admin&password=public",
    Req = make_req(post),
    ?assertMatch({error, bad_request, _}, do_request(Channel, URI, Req)),

    timer:sleep(100),
    ?assertEqual(
        [],
        emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
    ),
    ok.

t_connection_with_expire(_) ->
    {ok, _Sock, Channel} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),

    URI = ?MQTT_PREFIX ++ "/connection?clientid=client1",

    ?assertWaitEvent(
        begin
            Req = make_req(post),
            {ok, created, _Data} = do_request(Channel, URI, Req)
        end,
        #{
            ?snk_kind := conn_process_terminated,
            clientid := <<"client1">>,
            reason := {shutdown, expired}
        },
        5000
    ).

t_update_not_restart_listener(_) ->
    update_coap_with_mountpoint(<<"mp/">>),
    with_connection(fun(_Channel, Token) ->
        ?assertMatch({ok, changed, _}, send_heartbeat(Token)),
        update_coap_with_mountpoint(<<>>),
        ?assertMatch({ok, changed, _}, send_heartbeat(Token)),
        true
    end).

t_publish(_) ->
    %% can publish to a normal topic
    Topics = [
        <<"abc">>,
        %% can publish to a `/` leading topic
        <<"/abc">>
    ],
    Action = fun(Topic, Channel, Token) ->
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic), Token),

        %% Sub topic first
        emqx:subscribe(Topic),

        Req = make_req(post, Payload),
        {ok, changed, _} = do_request(Channel, URI, Req),

        receive
            {deliver, Topic, Msg} ->
                ?assertEqual(Topic, Msg#message.topic),
                ?assertEqual(Payload, Msg#message.payload)
        after 500 ->
            ?assert(false)
        end,
        true
    end,
    with_connection(Topics, Action).

t_publish_with_retain_qos_expiry(_) ->
    _ = meck:expect(
        emqx_access_control,
        authorize,
        fun(_, #{action_type := publish, qos := 1, retain := true}, _) ->
            allow
        end
    ),

    Topics = [<<"abc">>],
    Action = fun(Topic, Channel, Token) ->
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic), Token) ++ "&retain=true&qos=1&expiry=60",

        %% Sub topic first
        emqx:subscribe(Topic),

        Req = make_req(post, Payload),
        {ok, changed, _} = do_request(Channel, URI, Req),

        receive
            {deliver, Topic, Msg} ->
                ?assertEqual(Topic, Msg#message.topic),
                ?assertEqual(Payload, Msg#message.payload)
        after 500 ->
            ?assert(false)
        end,

        true
    end,
    with_connection(Topics, Action),

    _ = meck:validate(emqx_access_control).

t_subscribe(_) ->
    %% can subscribe to a normal topic
    Topics = [
        <<"abc">>,
        %% can subscribe to a `/` leading topic
        <<"/abc">>
    ],
    Fun = fun(Topic, Channel, Token) ->
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic), Token),
        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        %% ensure subscribe succeed
        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        %% publish a message
        emqx:publish(emqx_message:make(Topic, Payload)),
        {ok, content, Notify} = with_response(Channel),
        ?LOGT("observer get Notif=~p", [Notify]),

        #coap_content{payload = PayloadRecv} = Notify,

        ?assertEqual(Payload, PayloadRecv),
        true
    end,

    with_connection(Topics, Fun),

    %% subscription removed if coap client disconnected
    timer:sleep(100),
    lists:foreach(
        fun(Topic) ->
            ?assertEqual([], emqx:subscribers(Topic))
        end,
        Topics
    ).

t_subscribe_with_qos_opt(_) ->
    Topics = [
        {<<"abc">>, 0},
        {<<"/abc">>, 1},
        {<<"abc/d">>, 2}
    ],
    Fun = fun({Topic, Qos}, Channel, Token) ->
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic), Token) ++ "&qos=" ++ integer_to_list(Qos),
        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        %% ensure subscribe succeed
        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),
        %% publish a message
        emqx:publish(emqx_message:make(Topic, Payload)),
        {ok, content, Notify} = with_response(Channel),
        ?LOGT("observer get Notif=~p", [Notify]),

        #coap_content{payload = PayloadRecv} = Notify,

        ?assertEqual(Payload, PayloadRecv),
        true
    end,

    with_connection(Topics, Fun),

    %% subscription removed if coap client disconnected
    timer:sleep(100),
    lists:foreach(
        fun({Topic, _Qos}) ->
            ?assertEqual([], emqx:subscribers(Topic))
        end,
        Topics
    ).

t_un_subscribe(_) ->
    %% can unsubscribe to a normal topic
    Topics = [
        <<"abc">>,
        %% can unsubscribe to a `/` leading topic
        <<"/abc">>
    ],
    Fun = fun(Topic, Channel, Token) ->
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic), Token),

        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        UnReq = make_req(get, Payload, [{observe, 1}]),
        {ok, nocontent, _} = do_request(Channel, URI, UnReq),
        ?LOGT("un observer topic:~ts~n", [Topic]),
        timer:sleep(100),
        ?assertEqual([], emqx:subscribers(Topic)),
        true
    end,

    with_connection(Topics, Fun).

t_observe_wildcard(_) ->
    Fun = fun(Channel, Token) ->
        %% resolve_url can't process wildcard with #
        Topic = <<"abc/+">>,
        Payload = <<"123">>,

        URI = pubsub_uri(binary_to_list(Topic), Token),
        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        %% Publish a message
        PubTopic = <<"abc/def">>,
        emqx:publish(emqx_message:make(PubTopic, Payload)),
        {ok, content, Notify} = with_response(Channel),

        ?LOGT("observer get Notif=~p", [Notify]),

        #coap_content{payload = PayloadRecv} = Notify,

        ?assertEqual(Payload, PayloadRecv),
        true
    end,

    with_connection(Fun).

t_clients_api(_) ->
    Fun = fun(_Channel, _Token) ->
        ClientId = <<"client1">>,
        %% list
        {200, #{data := [Client1]}} = request(get, "/gateways/coap/clients"),
        #{clientid := ClientId} = Client1,
        %% searching
        {200, #{data := [Client2]}} =
            request(
                get,
                "/gateways/coap/clients",
                [{<<"clientid">>, ClientId}]
            ),
        {200, #{data := [Client3]}} =
            request(
                get,
                "/gateways/coap/clients",
                [{<<"like_clientid">>, <<"cli">>}]
            ),
        %% lookup
        {200, Client4} =
            request(get, "/gateways/coap/clients/client1"),
        %% assert
        Client1 = Client2 = Client3 = Client4,
        %% assert keepalive
        ?assertEqual(15, maps:get(keepalive, Client4)),
        %% kickout
        {204, _} =
            request(delete, "/gateways/coap/clients/client1"),
        timer:sleep(200),
        {200, #{data := []}} = request(get, "/gateways/coap/clients"),
        false
    end,
    with_connection(Fun).

t_clients_subscription_api(_) ->
    Fun = fun(_Channel, _Token) ->
        Path = "/gateways/coap/clients/client1/subscriptions",
        %% list
        {200, []} = request(get, Path),
        %% create
        SubReq = #{
            topic => <<"tx">>,
            qos => 0,
            nl => 0,
            rap => 0,
            rh => 0
        },

        {201, SubsResp} = request(post, Path, SubReq),
        {200, [SubsResp2]} = request(get, Path),
        ?assertEqual(
            maps:get(topic, SubsResp),
            maps:get(topic, SubsResp2)
        ),

        %% check subscription_cnt
        {200, #{subscriptions_cnt := 1}} = request(get, "/gateways/coap/clients/client1"),

        {204, _} = request(delete, Path ++ "/tx"),

        {200, []} = request(get, Path),
        true
    end,
    with_connection(Fun).

t_clients_get_subscription_api(_) ->
    Fun = fun(Channel, Token) ->
        Path = "/gateways/coap/clients/client1/subscriptions",
        %% list
        {200, []} = request(get, Path),

        observe(Channel, Token, true),

        {200, [Subs]} = request(get, Path),

        ?assertEqual(<<"coap/observe">>, maps:get(topic, Subs)),

        observe(Channel, Token, false),

        {200, []} = request(get, Path),
        true
    end,
    with_connection(Fun).

t_on_offline_event(_) ->
    Fun = fun(Channel) ->
        emqx_hooks:add('client.connected', {emqx_sys, on_client_connected, []}, 1000),
        emqx_hooks:add('client.disconnected', {emqx_sys, on_client_disconnected, []}, 1000),

        ConnectedSub = <<"$SYS/brokers/+/gateway/coap/clients/+/connected">>,
        emqx_broker:subscribe(ConnectedSub),
        timer:sleep(100),

        Token = connection(Channel),
        ?assertMatch(#message{}, receive_deliver(500)),

        DisconnectedSub = <<"$SYS/brokers/+/gateway/coap/clients/+/disconnected">>,
        emqx_broker:subscribe(DisconnectedSub),
        timer:sleep(100),

        disconnection(Channel, Token),

        ?assertMatch(#message{}, receive_deliver(500)),

        emqx_broker:unsubscribe(ConnectedSub),
        emqx_broker:unsubscribe(DisconnectedSub),

        emqx_hooks:del('client.connected', {emqx_sys, on_client_connected}),
        emqx_hooks:del('client.disconnected', {emqx_sys, on_client_disconnected}),
        timer:sleep(500)
    end,
    do(Fun).

t_connectionless_pubsub(_) ->
    update_coap_with_connection_mode(false),
    Fun = fun(Channel) ->
        Topic = <<"t/a">>,
        Payload = <<"123">>,
        URI = pubsub_uri(binary_to_list(Topic)),
        Req = make_req(get, Payload, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        %% ensure subscribe succeed
        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        %% publish a message
        Req2 = make_req(post, Payload),
        {ok, changed, _} = do_request(Channel, URI, Req2),

        {ok, content, Notify} = with_response(Channel),
        ?LOGT("observer get Notif=~p", [Notify]),

        #coap_content{payload = PayloadRecv} = Notify,

        ?assertEqual(Payload, PayloadRecv)
    end,
    do(Fun),
    update_coap_with_connection_mode(true).

t_request_with_partial_token_params(_) ->
    with_connection(fun(Channel, Token) ->
        URI1 = compose_uri(
            ?PS_PREFIX ++ "/only_clientid",
            #{"clientid" => <<"client1">>},
            false
        ),
        Req1 = make_req(post, <<"x">>),
        ?assertMatch({error, bad_request, _}, do_request(Channel, URI1, Req1)),

        URI2 = compose_uri(
            ?PS_PREFIX ++ "/only_token",
            #{"token" => list_to_binary(Token)},
            false
        ),
        Req2 = make_req(post, <<"x">>),
        ?assertMatch({error, bad_request, _}, do_request(Channel, URI2, Req2)),
        true
    end).

%% A request rejected in connection mode must not leak credentials sent with the
%% short query aliases (`t', `p') into the debug log.
t_rejected_request_does_not_log_short_credentials(_) ->
    Secret = <<"short-password-value">>,
    Reports = emqx_cth_log_capture:capture(debug, fun() ->
        with_connection(fun(Channel, _Token) ->
            URI = compose_uri(
                ?PS_PREFIX ++ "/short_credential",
                #{"p" => Secret},
                false
            ),
            ?assertMatch(
                {error, bad_request, _},
                do_request(Channel, URI, make_req(post, <<"x">>))
            ),
            put(coap_rejected_secret, Secret),
            true
        end)
    end),
    %% The rejection path must have been exercised.
    ?assertNotEqual(
        [],
        [R || R = #{msg := "token_required_in_conn_mode"} <- Reports]
    ),
    RejectedSecret = get(coap_rejected_secret),
    Leaks = [
        Report
     || Report <- Reports,
        binary:match(term_to_binary(Report), RejectedSecret) =/= nomatch
    ],
    ?assertEqual([], Leaks).

t_token_takeover_across_udp_sessions(_) ->
    {ok, Sock1, Channel1} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    Token = connection(Channel1),
    timer:sleep(100),
    ?assertNotEqual([], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)),
    er_coap_channel:close(Channel1),
    er_coap_udp_socket:close(Sock1),
    timer:sleep(100),
    ?assertNotEqual([], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)),

    {ok, Sock2, Channel2} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    URI = compose_uri(
        ?PS_PREFIX ++ "/coap/udp_takeover",
        #{
            "clientid" => <<"client1">>,
            "token" => list_to_binary(Token)
        },
        false
    ),
    Req = make_req(post, <<"x">>),
    ?assertMatch({ok, changed, _}, do_request(Channel2, URI, Req)),
    ?assertEqual(1, length(emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>))),
    disconnection(Channel2, Token),
    er_coap_channel:close(Channel2),
    er_coap_udp_socket:close(Sock2).

t_invalid_token_rejected_across_udp_sessions(_) ->
    {ok, Sock1, Channel1} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    Token = connection(Channel1),
    er_coap_channel:close(Channel1),
    er_coap_udp_socket:close(Sock1),
    timer:sleep(100),
    ?assertNotEqual([], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)),

    {ok, Sock2, Channel2} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    URI = compose_uri(
        ?PS_PREFIX ++ "/coap/udp_invalid_token",
        #{
            "clientid" => <<"client1">>,
            "token" => <<"wrong-token">>
        },
        false
    ),
    Req = make_req(post, <<"x">>),
    ?assertMatch({error, unauthorized, _}, do_request(Channel2, URI, Req)),
    disconnection(Channel2, Token),
    er_coap_channel:close(Channel2),
    er_coap_udp_socket:close(Sock2).

t_invalid_token_does_not_take_over_udp_downlink(_) ->
    Topic = <<"coap/udp_invalid_token_downlink">>,
    {ok, Sock1, Channel1} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    Token = connection(Channel1),
    observe_topic(Channel1, Token, Topic, <<"original-peer">>),
    timer:sleep(100),
    [ConnPid] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
    OriginalProxyId = current_udp_proxy_id(ConnPid),

    BaselinePayload = <<"before-invalid-request">>,
    publish(Topic, ?QOS_0, BaselinePayload),
    _ = assert_notify(Channel1, non, BaselinePayload),

    {ok, Sock2} = gen_udp:open(0, [binary, {active, false}]),
    InvalidURI = compose_uri(
        pubsub_uri(binary_to_list(Topic)),
        #{
            "clientid" => <<"client1">>,
            "token" => <<"wrong-token">>
        },
        false
    ),
    InvalidReq = make_req(post, <<"rejected">>),
    try
        ?assertMatch({error, unauthorized, _}, raw_udp_request(Sock2, InvalidURI, InvalidReq)),
        ?assertEqual(OriginalProxyId, current_udp_proxy_id(ConnPid)),

        DownlinkPayload = <<"after-invalid-request">>,
        publish(Topic, ?QOS_0, DownlinkPayload),
        ?assertEqual({error, timeout}, gen_udp:recv(Sock2, 0, 300)),
        _ = assert_notify(Channel1, non, DownlinkPayload),

        MissingTokenURI = compose_uri(
            pubsub_uri(binary_to_list(Topic)),
            #{"clientid" => <<"client1">>},
            false
        ),
        ?assertMatch(
            {error, bad_request, _},
            raw_udp_request(Sock2, MissingTokenURI, make_req(post, <<"rejected-again">>))
        ),
        AfterMissingTokenPayload = <<"after-missing-token">>,
        publish(Topic, ?QOS_0, AfterMissingTokenPayload),
        ?assertEqual({error, timeout}, gen_udp:recv(Sock2, 0, 300)),
        _ = assert_notify(Channel1, non, AfterMissingTokenPayload),

        ValidURI = compose_uri(
            pubsub_uri("coap/udp_valid_migration"),
            #{
                "clientid" => <<"client1">>,
                "token" => list_to_binary(Token)
            },
            false
        ),
        {ok, changed, _} = raw_udp_request(Sock2, ValidURI, make_req(post, <<"accepted">>)),
        ?assertNotEqual(OriginalProxyId, current_udp_proxy_id(ConnPid)),
        MigratedPayload = <<"after-valid-migration">>,
        publish(Topic, ?QOS_0, MigratedPayload),
        ?assertEqual({error, timeout}, with_message_response(Channel1, 300)),
        {ok, {_Host, _Port, MigratedData}} = gen_udp:recv(Sock2, 0, 2000),
        #coap_message{payload = MigratedPayload} = parse_udp_message(MigratedData),
        ok
    after
        _ = catch disconnection(Channel1, Token),
        gen_udp:close(Sock2),
        er_coap_channel:close(Channel1),
        er_coap_udp_socket:close(Sock1)
    end.

t_valid_udp_migration_does_not_wait_for_old_proxy(_) ->
    {ok, Sock1, Channel1} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    Token = connection(Channel1),
    [ConnPid] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
    OriginalProxyId = current_udp_proxy_id(ConnPid),
    {ok, Sock2} = gen_udp:open(0, [binary, {active, false}]),
    ValidURI = compose_uri(
        pubsub_uri("coap/udp_nonblocking_migration"),
        #{
            "clientid" => <<"client1">>,
            "token" => list_to_binary(Token)
        },
        false
    ),
    try
        ok = sys:suspend(OriginalProxyId),
        ?assertMatch(
            {ok, changed, _},
            raw_udp_request(Sock2, ValidURI, make_req(post, <<"accepted">>))
        )
    after
        _ = catch sys:resume(OriginalProxyId),
        _ = catch disconnection(Channel1, Token),
        gen_udp:close(Sock2),
        er_coap_channel:close(Channel1),
        er_coap_udp_socket:close(Sock1)
    end.

t_queued_valid_source_keeps_a_live_downlink_proxy(_) ->
    Topic = <<"coap/udp_queued_migration">>,
    {ok, Sock1, Channel1} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    Token = connection(Channel1),
    observe_topic(Channel1, Token, Topic, <<"queued-migration-peer">>),
    [ConnPid] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
    {ok, Sock2} = gen_udp:open(0, [binary, {active, false}]),
    ValidURI = compose_uri(
        pubsub_uri("coap/udp_queued_migration_request"),
        #{
            "clientid" => <<"client1">>,
            "token" => list_to_binary(Token)
        },
        false
    ),
    try
        true = erlang:suspend_process(ConnPid),
        ok = send_raw_udp_request(Sock2, ValidURI, make_req(post, <<"new-source">>)),
        ok = wait_for_udp_proxy_messages(ConnPid, 1, 20),
        ok = send_request(Channel1, ValidURI, make_req(post, <<"old-source">>)),
        ok = wait_for_udp_proxy_messages(ConnPid, 2, 20),
        true = erlang:resume_process(ConnPid),

        ?assertMatch({ok, changed, _}, recv_raw_udp_response(Sock2)),
        ?assertMatch({ok, changed, _}, with_response(Channel1)),
        DownlinkPayload = <<"after-queued-migrations">>,
        publish(Topic, ?QOS_0, DownlinkPayload),
        _ = assert_notify(Channel1, non, DownlinkPayload),
        ?assertEqual({error, timeout}, gen_udp:recv(Sock2, 0, 300))
    after
        _ = catch erlang:resume_process(ConnPid),
        _ = catch disconnection(Channel1, Token),
        gen_udp:close(Sock2),
        er_coap_channel:close(Channel1),
        er_coap_udp_socket:close(Sock1)
    end.

-doc """
In connection mode, only a request can be checked against the client ID and
token of a channel. ACK and reset messages are accepted only from the UDP
source that the channel uses as its downlink: a confirmable request that the
gateway sends to the device through the REST API stays open until the device
itself answers.
""".
t_ack_reset_from_other_source_ignored(_) ->
    {ok, VSock} = gen_udp:open(0, [binary, {active, false}]),
    {ok, OSock} = gen_udp:open(0, [binary, {active, false}]),
    _Token = raw_connection(VSock, <<"client1">>),
    try
        %% The other source names client1 in a request that is refused.
        ?assertMatch(
            {error, unauthorized, _},
            raw_udp_request(
                OSock,
                client_uri(pubsub_uri("coap/other_source"), <<"client1">>, <<"wrong">>),
                make_req(post, <<"refused">>)
            )
        ),
        lists:foreach(
            fun(Kind) ->
                Payload = atom_to_binary(Kind),
                Ref = async_api_request(<<"client1">>, Payload, <<"10s">>),
                Req = recv_raw_message(VSock, 5000),
                ?assertMatch(#coap_message{type = con, method = get, payload = Payload}, Req),
                ok = send_raw_message(OSock, other_source_reply(Kind, Req)),
                %% The exchange is still open: the API caller has no result yet.
                receive
                    {api_result, Ref, Early} -> error({unexpected_api_result, Kind, Early})
                after 500 -> ok
                end,
                ok = send_raw_message(
                    VSock, emqx_coap_message:piggyback({ok, content}, Payload, Req)
                ),
                {ok, 200, Body} = wait_api_result(Ref),
                ?assertMatch(#{<<"payload">> := Payload}, emqx_utils_json:decode(Body))
            end,
            [reset, ack]
        ),
        ?assertEqual({error, timeout}, gen_udp:recv(OSock, 0, 0))
    after
        _ = catch emqx_gateway_cm:kick_session(coap, <<"client1">>),
        gen_udp:close(OSock),
        gen_udp:close(VSock)
    end.

-doc """
A reply to a request that is sent to a device through the REST API is accepted
only from the UDP source that the device uses as its downlink.
""".
t_api_request_reply_from_other_source_ignored(_) ->
    {ok, VSock} = gen_udp:open(0, [binary, {active, false}]),
    {ok, OSock} = gen_udp:open(0, [binary, {active, false}]),
    _Token = raw_connection(VSock, <<"client1">>),
    try
        ?assertMatch(
            {error, unauthorized, _},
            raw_udp_request(
                OSock,
                client_uri(pubsub_uri("coap/api_reply"), <<"client1">>, <<"wrong">>),
                make_req(post, <<"refused">>)
            )
        ),
        Ref = async_api_request(<<"client1">>, <<"ping">>, <<"2s">>),
        Req = recv_raw_message(VSock, 5000),
        ?assertMatch(#coap_message{type = con, method = get}, Req),
        %% A piggybacked response and a separate response, both from the other
        %% source.  The device itself does not answer.
        ok = send_raw_message(
            OSock, emqx_coap_message:piggyback({ok, content}, <<"forged">>, Req)
        ),
        ok = send_raw_message(
            OSock,
            (emqx_coap_message:response({ok, content}, <<"forged">>, Req))#coap_message{
                type = non, id = (Req#coap_message.id + 1) band 16#FFFF
            }
        ),
        ?assertMatch({ok, 504, _}, wait_api_result(Ref))
    after
        _ = catch emqx_gateway_cm:kick_session(coap, <<"client1">>),
        gen_udp:close(OSock),
        gen_udp:close(VSock)
    end.

-doc """
A UDP source that has named one client stays with the channel of that client.
The channel serves only its own client: a later request of the source that names
another client ID is refused, also when it carries the valid token of that other
client.  Neither client's channel, identity or downlink changes.
""".
t_request_for_other_client_refused_on_bound_source(_) ->
    Topic = <<"coap/refused_then_own">>,
    {ok, Sock1, Channel1} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    {ok, SockB0} = gen_udp:open(0, [binary, {active, false}]),
    {ok, SockB1} = gen_udp:open(0, [binary, {active, false}]),
    Token = connection(Channel1),
    try
        observe_topic(Channel1, Token, Topic, <<"client1-observe">>),
        [PidA] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
        ProxyA = current_udp_proxy_id(PidA),
        TokenB = raw_connection(SockB0, <<"client2">>),
        [PidB] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client2">>),
        ProxyB0 = current_udp_proxy_id(PidB),
        OwnURI = client_uri(pubsub_uri("coap/client2_own"), <<"client2">>, TokenB),

        %% The new source names client1 and is refused.
        ?assertMatch(
            {error, unauthorized, _},
            raw_udp_request(
                SockB1,
                client_uri(pubsub_uri(binary_to_list(Topic)), <<"client1">>, TokenB),
                make_req(post, <<"refused">>)
            )
        ),
        %% Its next request reaches client1's channel too, which does not serve
        %% client2.
        ?assertMatch(
            {error, unauthorized, _},
            raw_udp_request(SockB1, OwnURI, make_req(post, <<"own">>))
        ),

        %% client1 keeps its channel, identity and downlink.
        ?assertEqual([PidA], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)),
        ?assertEqual(ProxyA, current_udp_proxy_id(PidA)),
        ?assertMatch(
            #{clientinfo := #{clientid := <<"client1">>}}, emqx_gateway_conn:info(PidA)
        ),
        %% So does client2, which is still served through its first source.
        ?assertEqual([PidB], emqx_gateway_cm_registry:lookup_channels(coap, <<"client2">>)),
        ?assertEqual(ProxyB0, current_udp_proxy_id(PidB)),
        ?assertMatch(
            #{clientinfo := #{clientid := <<"client2">>}}, emqx_gateway_conn:info(PidB)
        ),
        ?assertMatch(
            {ok, changed, _},
            raw_udp_request(SockB0, OwnURI, make_req(post, <<"own">>))
        ),

        Payload = <<"for-client1-only">>,
        publish(Topic, ?QOS_0, Payload),
        _ = assert_notify(Channel1, non, Payload),
        ?assertEqual({error, timeout}, gen_udp:recv(SockB1, 0, 300)),
        ?assertEqual({error, timeout}, gen_udp:recv(SockB0, 0, 0))
    after
        _ = catch emqx_gateway_cm:kick_session(coap, <<"client2">>),
        _ = catch disconnection(Channel1, Token),
        gen_udp:close(SockB1),
        gen_udp:close(SockB0),
        er_coap_channel:close(Channel1),
        er_coap_udp_socket:close(Sock1)
    end.

-doc """
A refused request that names another client leaves the UDP proxy of its source
with the client that owns it.  When the named client closes later, the owner
still has its downlink and receives its notifications.
""".
t_refused_request_for_other_client_keeps_downlink(_) ->
    Topic = <<"coap/keeps_downlink">>,
    {ok, Sock1, Channel1} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    {ok, SockB} = gen_udp:open(0, [binary, {active, false}]),
    Token = connection(Channel1),
    try
        observe_topic(Channel1, Token, Topic, <<"client1-observe">>),
        [PidA] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
        ProxyA = current_udp_proxy_id(PidA),
        TokenB = raw_connection(SockB, <<"client2">>),
        [PidB] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client2">>),

        %% client1's source names client2 with a token that is not client2's.
        ?assertMatch(
            {error, unauthorized, _},
            do_request(
                Channel1,
                client_uri(pubsub_uri("coap/other_client"), <<"client2">>, <<"not-the-token">>),
                make_req(post, <<"refused">>)
            )
        ),

        %% client2 closes its connection.
        MRefB = erlang:monitor(process, PidB),
        ?assertMatch(
            {ok, deleted, _},
            raw_udp_request(SockB, connection_uri(<<"client2">>, TokenB), make_req(delete))
        ),
        receive
            {'DOWN', MRefB, process, PidB, _} -> ok
        after 5000 -> error(client2_channel_still_alive)
        end,

        %% The proxy of client1's source is still running and is still the
        %% downlink of client1.
        ?assert(is_map(sys:get_state(ProxyA))),
        ?assertEqual([PidA], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)),
        ?assertEqual(ProxyA, current_udp_proxy_id(PidA)),
        Payload = <<"still-delivered">>,
        publish(Topic, ?QOS_0, Payload),
        _ = assert_notify(Channel1, non, Payload),
        ?assert(is_process_alive(ProxyA))
    after
        _ = catch emqx_gateway_cm:kick_session(coap, <<"client2">>),
        _ = catch disconnection(Channel1, Token),
        gen_udp:close(SockB),
        er_coap_channel:close(Channel1),
        er_coap_udp_socket:close(Sock1)
    end.

-doc """
A UDP source serves one client at a time.  A second client that connects from a
source whose first client is still connected is refused and does not share the
proxy of that client.  Once the first client has closed its connection, the same
source can connect as the second client.
""".
t_udp_source_serves_one_client_at_a_time(_) ->
    {ok, Sock} = gen_udp:open(0, [binary, {active, false}]),
    try
        TokenA = raw_connection(Sock, <<"client1">>),
        [PidA] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
        ProxyA = current_udp_proxy_id(PidA),

        ?assertMatch(
            {error, bad_request, _},
            raw_udp_request(Sock, connection_uri(<<"client2">>), make_req(post))
        ),
        ?assertEqual([], emqx_gateway_cm_registry:lookup_channels(coap, <<"client2">>)),
        ?assertEqual([PidA], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)),
        ?assertEqual(ProxyA, current_udp_proxy_id(PidA)),
        ?assertMatch(
            {ok, changed, _},
            raw_udp_request(
                Sock,
                client_uri(pubsub_uri("coap/one_at_a_time"), <<"client1">>, TokenA),
                make_req(post, <<"a">>)
            )
        ),

        %% client1 closes its connection, which ends the proxy of the source.
        MRef = erlang:monitor(process, ProxyA),
        ?assertMatch(
            {ok, deleted, _},
            raw_udp_request(Sock, connection_uri(<<"client1">>, TokenA), make_req(delete))
        ),
        receive
            {'DOWN', MRef, process, ProxyA, _} -> ok
        after 5000 -> error(udp_proxy_still_alive)
        end,

        %% The listener may still hold the stopped proxy for this source when
        %% the next datagram arrives, so allow a retry.
        TokenB = ?retry(200, 5, raw_connection(Sock, <<"client2">>)),
        [PidB] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client2">>),
        ?assertMatch(
            {ok, changed, _},
            raw_udp_request(
                Sock,
                client_uri(pubsub_uri("coap/one_at_a_time"), <<"client2">>, TokenB),
                make_req(post, <<"b">>)
            )
        ),
        ?assert(is_process_alive(current_udp_proxy_id(PidB)))
    after
        _ = catch emqx_gateway_cm:kick_session(coap, <<"client1">>),
        _ = catch emqx_gateway_cm:kick_session(coap, <<"client2">>),
        gen_udp:close(Sock)
    end.

t_wrong_clientid_with_valid_token_rejected_across_udp_sessions(_) ->
    {ok, Sock1, Channel1} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    Token = connection(Channel1),
    er_coap_channel:close(Channel1),
    er_coap_udp_socket:close(Sock1),
    timer:sleep(100),
    ?assertNotEqual([], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)),

    {ok, Sock2, Channel2} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    URI = compose_uri(
        ?PS_PREFIX ++ "/coap/udp_wrong_clientid",
        #{
            "clientid" => <<"client2">>,
            "token" => list_to_binary(Token)
        },
        false
    ),
    Req = make_req(post, <<"x">>),
    ?assertMatch({error, unauthorized, _}, do_request(Channel2, URI, Req)),
    disconnection(Channel2, Token),
    er_coap_channel:close(Channel2),
    er_coap_udp_socket:close(Sock2).

t_proxy_conn_reuse_bound_clientid_when_missing(_) ->
    Peer = {{127, 0, 0, 1}, 12345},
    State0 = emqx_coap_proxy_conn:initialize([]),
    Msg1 = #coap_message{
        type = con,
        method = post,
        id = 1,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => <<"client1">>}
        }
    },
    Bin1 = emqx_coap_frame:serialize_pkt(Msg1, emqx_coap_frame:serialize_opts()),
    {ok, {coap_udp_proxy, Peer, <<"client1">>}, _Packets1, State1} =
        emqx_coap_proxy_conn:get_connection_id(dummy, Peer, State0, Bin1),
    Msg2 = #coap_message{
        type = con,
        method = put,
        id = 2,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{}
        }
    },
    Bin2 = emqx_coap_frame:serialize_pkt(Msg2, emqx_coap_frame:serialize_opts()),
    ?assertMatch(
        {ok, {coap_udp_proxy, Peer, <<"client1">>}, _Packets2, _State2},
        emqx_coap_proxy_conn:get_connection_id(dummy, Peer, State1, Bin2)
    ).

t_proxy_conn_keep_bound_clientid_on_different_clientid(_) ->
    Peer = {{127, 0, 0, 1}, 12345},
    State0 = emqx_coap_proxy_conn:initialize([]),
    Msg1 = #coap_message{
        type = con,
        method = post,
        id = 1,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => <<"client1">>}
        }
    },
    Bin1 = emqx_coap_frame:serialize_pkt(Msg1, emqx_coap_frame:serialize_opts()),
    {ok, {coap_udp_proxy, Peer, <<"client1">>}, _Packets1, State1} =
        emqx_coap_proxy_conn:get_connection_id(dummy, Peer, State0, Bin1),
    Msg2 = #coap_message{
        type = con,
        method = post,
        id = 2,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => <<"client2">>}
        }
    },
    Bin2 = emqx_coap_frame:serialize_pkt(Msg2, emqx_coap_frame:serialize_opts()),
    ?assertMatch(
        {ok, {coap_udp_proxy, Peer, <<"client1">>}, _Packets2, _State2},
        emqx_coap_proxy_conn:get_connection_id(dummy, Peer, State1, Bin2)
    ).

t_proxy_conn_scopes_clientid_by_peer(_) ->
    Peer1 = {{127, 0, 0, 1}, 10001},
    Peer2 = {{127, 0, 0, 1}, 10002},
    Msg = #coap_message{
        type = con,
        method = post,
        id = 1,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => <<"client1">>}
        }
    },
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    State1 = emqx_coap_proxy_conn:initialize([]),
    State2 = emqx_coap_proxy_conn:initialize([]),
    {ok, ProxyCId1, [_], _} =
        emqx_coap_proxy_conn:get_connection_id(dummy, Peer1, State1, Bin),
    {ok, ProxyCId2, [_], _} =
        emqx_coap_proxy_conn:get_connection_id(dummy, Peer2, State2, Bin),
    ?assertEqual({coap_udp_proxy, Peer1, <<"client1">>}, ProxyCId1),
    ?assertEqual({coap_udp_proxy, Peer2, <<"client1">>}, ProxyCId2),
    ?assertNotEqual(ProxyCId1, ProxyCId2).

t_proxy_conn_close_passes_proxy_owner(_) ->
    ProxyId = self(),
    ?assertEqual(ok, emqx_coap_proxy_conn:close(self(), ProxyId, #{})),
    receive
        {udp_proxy_closed, ProxyId} ->
            ok
    after 100 ->
        error(missing_udp_proxy_closed)
    end.

t_legacy_proxy_close_closes_transport(_) ->
    {ok, Sock, Channel} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    _Token = connection(Channel),
    [Pid] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
    try
        ?assertEqual(ok, emqx_coap_proxy_conn:close(Pid, #{})),
        ?retry(
            100,
            10,
            ?assertMatch(
                #{sockinfo := #{sockstate := closed}},
                emqx_gateway_conn:info(Pid)
            )
        )
    after
        _ = catch emqx_gateway_cm:kick_session(coap, <<"client1">>),
        ?retry(
            100,
            10,
            ?assertEqual([], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>))
        ),
        er_coap_channel:close(Channel),
        er_coap_udp_socket:close(Sock)
    end.

%%--------------------------------------------------------------------
%% helpers

send_heartbeat(Token) ->
    send_heartbeat(Token, false).

send_heartbeat(Token, ShortenParamName) ->
    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    ?LOGT("send heartbeat request:~ts~n", [URI]),
    er_coap_client:request(put, URI).

connection(Channel) ->
    connection(Channel, false).

connection(Channel, ShortenParamName) ->
    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries = #{
        "clientid" => <<"client1">>,
        "username" => <<"admin">>,
        "password" => <<"public">>
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    Req = make_req(post),
    {ok, created, Data} = do_request(Channel, URI, Req),
    #coap_content{payload = BinToken} = Data,
    binary_to_list(BinToken).

disconnection(Channel, Token) ->
    disconnection(Channel, Token, false).

disconnection(Channel, Token, ShortenParamName) ->
    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    Req = make_req(delete),
    {ok, deleted, _} = do_request(Channel, URI, Req).

observe(Channel, Token, Observe) ->
    observe(Channel, Token, Observe, false).

observe(Channel, Token, true, ShortenParamName) ->
    Prefix = ?PS_PREFIX ++ "/coap/observe",
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    Req = make_req(get, <<>>, [{observe, 0}]),
    {ok, content, _Data} = do_request(Channel, URI, Req),
    ok;
observe(Channel, Token, false, ShortenParamName) ->
    Prefix = ?PS_PREFIX ++ "/coap/observe",
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    Req = make_req(get, <<>>, [{observe, 1}]),
    {ok, nocontent, _Data} = do_request(Channel, URI, Req),
    ok.

pubsub_uri(Topic) when is_list(Topic) ->
    ?PS_PREFIX ++ "/" ++ Topic.

pubsub_uri(Topic, Token) ->
    pubsub_uri(Topic, Token, false).

pubsub_uri(Topic, Token, ShortenParamName) when is_list(Topic), is_list(Token) ->
    Prefix = ?PS_PREFIX ++ "/" ++ Topic,
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    compose_uri(Prefix, Queries, ShortenParamName).

make_req(Method) ->
    make_req(Method, <<>>).

make_req(Method, Payload) ->
    make_req(Method, Payload, []).

make_req(Method, Payload, Opts) ->
    er_coap_message:request(con, Method, Payload, Opts).

do_request(Channel, URI, Req) ->
    ok = send_request(Channel, URI, Req),
    with_response(Channel).

send_request(Channel, URI, #coap_message{options = Opts} = Req) ->
    {_, _, Path, Query} = er_coap_client:resolve_uri(URI),
    Opts2 = [{uri_path, Path}, {uri_query, Query} | Opts],
    Req2 = Req#coap_message{options = Opts2},
    ?LOGT("send request:~ts~nReq:~p~n", [URI, Req2]),
    {ok, _} = er_coap_channel:send(Channel, Req2),
    ok.

send_raw_udp_request(Socket, URI, #coap_message{options = Opts} = Req) ->
    {_, _, Path, Query} = er_coap_client:resolve_uri(URI),
    MsgId = erlang:unique_integer([positive]) band 16#FFFF,
    Token = crypto:strong_rand_bytes(4),
    Req2 = Req#coap_message{
        id = MsgId,
        token = Token,
        options = [{uri_path, Path}, {uri_query, Query} | Opts]
    },
    Data = er_coap_message_parser:encode(Req2),
    gen_udp:send(Socket, {127, 0, 0, 1}, 5683, Data).

raw_udp_request(Socket, URI, Req) ->
    ok = send_raw_udp_request(Socket, URI, Req),
    recv_raw_udp_response(Socket).

recv_raw_udp_response(Socket) ->
    {ok, {_Host, _Port, ResponseData}} = gen_udp:recv(Socket, 0, 2000),
    #coap_message{method = {Class, Code}, payload = Payload} = parse_udp_message(ResponseData),
    {Class, Code, Payload}.

raw_connection(Socket, ClientId) ->
    {ok, created, Token} = raw_udp_request(Socket, connection_uri(ClientId), make_req(post)),
    Token.

connection_uri(ClientId) ->
    compose_uri(
        ?MQTT_PREFIX ++ "/connection",
        #{"clientid" => ClientId, "username" => <<"admin">>, "password" => <<"public">>},
        false
    ).

connection_uri(ClientId, Token) ->
    client_uri(?MQTT_PREFIX ++ "/connection", ClientId, Token).

raw_observe(Socket, ClientId, Token, Topic) ->
    URI = client_uri(pubsub_uri(binary_to_list(Topic)), ClientId, Token),
    {ok, content, _} = raw_udp_request(Socket, URI, make_req(get, <<>>, [{observe, 0}])),
    ok.

client_uri(Prefix, ClientId, Token) ->
    compose_uri(Prefix, #{"clientid" => ClientId, "token" => Token}, false).

send_raw_message(Socket, Message) ->
    Data = emqx_coap_frame:serialize_pkt(Message, emqx_coap_frame:serialize_opts()),
    gen_udp:send(Socket, {127, 0, 0, 1}, 5683, Data).

recv_raw_message(Socket, Timeout) ->
    {ok, {_Host, _Port, Data}} = gen_udp:recv(Socket, 0, Timeout),
    parse_udp_message(Data).

other_source_reply(reset, Req) ->
    emqx_coap_message:reset(Req);
other_source_reply(ack, Req) ->
    emqx_coap_message:ack(Req).

async_api_request(ClientId, Payload, Timeout) ->
    Uri = emqx_mgmt_api_test_util:uri([
        "gateways", "coap", "clients", binary_to_list(ClientId), "request"
    ]),
    Body = #{
        token => <<"apitoken">>,
        payload => Payload,
        timeout => Timeout,
        content_type => <<"text/plain">>,
        method => <<"get">>
    },
    Parent = self(),
    Ref = make_ref(),
    _ = spawn_link(fun() ->
        Parent ! {api_result, Ref, emqx_mgmt_api_test_util:request(post, Uri, Body)}
    end),
    Ref.

wait_api_result(Ref) ->
    receive
        {api_result, Ref, Result} -> Result
    after 15000 ->
        error(api_request_timeout)
    end.

wait_for_udp_proxy_messages(Pid, MinCount, 0) ->
    ?assert(udp_proxy_message_count(Pid) >= MinCount),
    ok;
wait_for_udp_proxy_messages(Pid, MinCount, Retries) ->
    case udp_proxy_message_count(Pid) of
        Count when Count >= MinCount ->
            ok;
        _ ->
            timer:sleep(10),
            wait_for_udp_proxy_messages(Pid, MinCount, Retries - 1)
    end.

udp_proxy_message_count(Pid) ->
    {messages, Messages} = process_info(Pid, messages),
    length([
        Message
     || Message = {{esockd_udp_proxy, _ProxyId, _Socket}, _Data, _Packets} <- Messages
    ]).

parse_udp_message(Data) ->
    {ok, Message, <<>>, _State} =
        emqx_coap_frame:parse(Data, emqx_coap_frame:initial_parse_state(#{})),
    Message.

with_response(Channel) ->
    receive
        {coap_response, _ChId, Channel, _Ref, Message = #coap_message{method = Code}} ->
            return_response(Code, Message);
        {coap_error, _ChId, Channel, _Ref, reset} ->
            {error, reset}
    after 10_000 ->
        {error, timeout}
    end.

return_response({ok, Code}, Message) ->
    {ok, Code, er_coap_message:get_content(Message)};
return_response({error, Code}, #coap_message{payload = <<>>}) ->
    {error, Code};
return_response({error, Code}, Message) ->
    {error, Code, er_coap_message:get_content(Message)}.

do(Fun) ->
    {ok, Sock, Channel} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    %% send and receive
    Res = Fun(Channel),
    %% terminate the processes
    er_coap_channel:close(Channel),
    er_coap_udp_socket:close(Sock),
    Res.

with_connection(Action) ->
    Fun = fun(Channel) ->
        Token = connection(Channel),
        timer:sleep(100),
        _ = Action(Channel, Token) andalso disconnection(Channel, Token),
        timer:sleep(100)
    end,
    do(Fun).

with_connection(Checks, Action) ->
    Fun = fun(Channel) ->
        Token = connection(Channel),
        timer:sleep(100),
        lists:foreach(fun(E) -> Action(E, Channel, Token) end, Checks),
        disconnection(Channel, Token),
        timer:sleep(100)
    end,
    do(Fun).

observe_topic(Channel, Token, Topic, ObserveToken) ->
    URI = pubsub_uri(binary_to_list(Topic), Token),
    Req = (make_req(get, <<>>, [{observe, 0}]))#coap_message{token = ObserveToken},
    {ok, content, _} = do_request(Channel, URI, Req),
    ok.

assert_notify(Channel, Type, Payload) ->
    Notify = assert_notify(Channel, Type),
    ?assertEqual(Payload, notify_payload(Notify)),
    Notify.

publish(Topic, QoS, Payload) ->
    emqx:publish(emqx_message:make(<<"coap">>, QoS, Topic, Payload)).

with_message_response(Channel) ->
    with_message_response(Channel, 2000).

return_message_response({ok, Code}, Message) ->
    {ok, Code, Message};
return_message_response({error, Code}, Message) ->
    {error, Code, Message}.

notify_payload(Notify) ->
    #coap_content{payload = Payload} = er_coap_message:get_content(Notify),
    Payload.

current_udp_proxy_id(Pid) ->
    {esockd_udp_proxy, ProxyId, _Sock} = element(2, sys:get_state(Pid)),
    ProxyId.

assert_notify(Channel, Type) ->
    {ok, content, Notify = #coap_message{type = Type}} = with_message_response(Channel),
    Notify.

with_message_response(Channel, Timeout) ->
    receive
        {coap_response, _ChId, Channel, _Ref, Message = #coap_message{method = Code}} ->
            return_message_response(Code, Message);
        {coap_error, _ChId, Channel, _Ref, reset} ->
            {error, reset}
    after Timeout ->
        {error, timeout}
    end.

receive_deliver(Wait) ->
    receive
        {deliver, _, Msg} ->
            Msg
    after Wait ->
        {error, timeout}
    end.

get_field(type, #coap_message{type = Type}) ->
    Type;
get_field(method, #coap_message{method = Method}) ->
    Method.

compose_uri(URI, Queries, ShortenParamName) ->
    Queries1 = shorten_param_name(ShortenParamName, Queries),
    case maps:size(Queries1) of
        0 ->
            URI;
        _ ->
            URI ++ "?" ++ uri_string:compose_query(maps:to_list(Queries1))
    end.

shorten_param_name(false, Queries) ->
    Queries;
shorten_param_name(true, Queries) ->
    lists:foldl(
        fun({Short, Long}, Acc) ->
            case maps:take(Long, Acc) of
                error ->
                    Acc;
                {Value, Acc1} ->
                    maps:put(Short, Value, Acc1)
            end
        end,
        Queries,
        emqx_coap_message:query_params_mapping_table()
    ).

-doc "CoAP returns a readable numeric hook rejection and connects after recovery.".
t_connect_hook_rejection(_) ->
    emqx_gateway_test_utils:with_connect_rejection(?RC_QUOTA_EXCEEDED, fun() ->
        do(fun(Channel) ->
            URI = compose_uri(
                ?MQTT_PREFIX ++ "/connection", #{"clientid" => <<"rejected">>}, false
            ),
            ?assertMatch(
                {error, bad_request, #coap_content{payload = <<"Login Failed: Quota exceeded">>}},
                do_request(Channel, URI, make_req(post))
            )
        end)
    end),
    do(fun(Channel) ->
        Token = connection(Channel),
        disconnection(Channel, Token)
    end).
