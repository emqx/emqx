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
-include_lib("common_test/include/ct.hrl").

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).
-define(PS_PREFIX, "coap://127.0.0.1/ps").
-define(MQTT_PREFIX, "coap://127.0.0.1/mqtt").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config1 = emqx_coap_test_helpers:start_gateway(Config),
    emqx_common_test_http:create_default_app(),
    Config1.

end_per_suite(Config) ->
    emqx_coap_test_helpers:stop_gateway(Config).

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
init_per_testcase(t_connection_open_session_error, Config) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough]),
    ok = meck:expect(
        emqx_gateway_ctx,
        open_session,
        fun(_, _, _, _, _, _) -> {error, session_error} end
    ),
    Config;
init_per_testcase(t_publish_with_retain_qos_expiry, Config) ->
    ok = meck:new(emqx_access_control, [passthrough]),
    Config;
init_per_testcase(t_pubsub_unauthorized, Config) ->
    ok = meck:new(emqx_access_control, [passthrough]),
    ok = meck:expect(emqx_access_control, authorize, fun(_, _, _) -> deny end),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(t_heartbeat, Config) ->
    OldConf = ?config(old_conf, Config),
    {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf),
    ok;
end_per_testcase(t_connection_with_expire, Config) ->
    snabbkaffe:stop(),
    meck:unload(emqx_access_control),
    Config;
end_per_testcase(t_connection_open_session_error, Config) ->
    ok = meck:unload(emqx_gateway_ctx),
    Config;
end_per_testcase(t_connection_with_authn_failed, Config) ->
    ok = meck:unload(emqx_access_control),
    Config;
end_per_testcase(t_publish_with_retain_qos_expiry, Config) ->
    ok = meck:unload(emqx_access_control),
    Config;
end_per_testcase(t_pubsub_unauthorized, Config) ->
    ok = meck:unload(emqx_access_control),
    Config;
end_per_testcase(_, Config) ->
    Config.

default_config() ->
    emqx_coap_test_helpers:default_conf().

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
        HookPoint = 'client.connect',
        HookAction = {emqx_coap_test_helpers, hook_capture, [self(), HookPoint]},
        ok = emqx_coap_test_helpers:add_test_hook(HookPoint, HookAction),
        try
            %% connection
            Token = connection(Channel),

            timer:sleep(100),
            ?assertNotEqual(
                [],
                emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
            ),

            receive
                {hook_call, 'client.connect'} -> ok
            after 1000 ->
                ct:fail({missing_hook_call, 'client.connect'})
            end,

            %% heartbeat
            {ok, changed, _} = send_heartbeat(Token),

            disconnection(Channel, Token),

            timer:sleep(100),
            ?assertEqual(
                [],
                emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
            )
        after
            ok = emqx_coap_test_helpers:del_test_hook(HookPoint, HookAction)
        end
    end,
    do(Action),
    ok.

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

t_duplicate_connection(_) ->
    Action = fun(Channel) ->
        Token = connection(Channel),
        URI =
            ?MQTT_PREFIX ++
                "/connection?clientid=client1&username=admin&password=public",
        Req = make_req(post),
        {error, bad_request, _} = do_request(Channel, URI, Req),
        disconnection(Channel, Token)
    end,
    do(Action).

t_connection_missing_clientid(_) ->
    Action = fun(Channel) ->
        Prefix = ?MQTT_PREFIX ++ "/connection",
        Queries = #{
            "username" => <<"admin">>,
            "password" => <<"public">>
        },
        URI = compose_uri(Prefix, Queries, false),
        Req = make_req(post),
        case do_request(Channel, URI, Req) of
            {error, bad_request, _} -> ok;
            {error, bad_request} -> ok
        end
    end,
    do(Action).

t_connection_hooks_error(_) ->
    HookPoint = 'client.connect',
    HookAction = {emqx_coap_test_helpers, hook_return_error, [hook_failed]},
    ok = emqx_coap_test_helpers:add_test_hook(HookPoint, HookAction),
    Action = fun(Channel) ->
        Prefix = ?MQTT_PREFIX ++ "/connection",
        Queries = #{
            "clientid" => <<"client1">>,
            "username" => <<"admin">>,
            "password" => <<"public">>
        },
        URI = compose_uri(Prefix, Queries, false),
        Req = make_req(post),
        case do_request(Channel, URI, Req) of
            {error, bad_request, _} -> ok;
            {error, bad_request} -> ok
        end
    end,
    try
        do(Action)
    after
        ok = emqx_coap_test_helpers:del_test_hook(HookPoint, HookAction)
    end.

t_connection_open_session_error(_) ->
    Action = fun(Channel) ->
        Prefix = ?MQTT_PREFIX ++ "/connection",
        Queries = #{
            "clientid" => <<"client1">>,
            "username" => <<"admin">>,
            "password" => <<"public">>
        },
        URI = compose_uri(Prefix, Queries, false),
        Req = make_req(post),
        case do_request(Channel, URI, Req) of
            {error, bad_request, _} -> ok;
            {error, bad_request} -> ok
        end
    end,
    do(Action).

t_duplicate_connection_other_clientid(_) ->
    Action = fun(Channel) ->
        Token = connection(Channel),
        Prefix = ?MQTT_PREFIX ++ "/connection",
        Queries = #{
            "clientid" => <<"client2">>,
            "username" => <<"admin">>,
            "password" => <<"public">>
        },
        URI = compose_uri(Prefix, Queries, false),
        Req = make_req(post),
        {error, bad_request, _} = do_request(Channel, URI, Req),
        disconnection(Channel, Token)
    end,
    do(Action).

t_duplicate_connection_invalid_queries(_) ->
    Action = fun(Channel) ->
        Token = connection(Channel),
        Prefix = ?MQTT_PREFIX ++ "/connection",
        Queries = #{
            "username" => <<"admin">>,
            "password" => <<"public">>
        },
        URI = compose_uri(Prefix, Queries, false),
        Req = make_req(post),
        {error, bad_request, _} = do_request(Channel, URI, Req),
        disconnection(Channel, Token)
    end,
    do(Action).

t_request_with_token_no_connection(_) ->
    Action = fun(Channel) ->
        URI = pubsub_uri("no_connection", "tok"),
        Req = make_req(get, <<>>, [{observe, 0}]),
        {error, bad_request, _} = do_request(Channel, URI, Req)
    end,
    do(Action).

t_delete_without_connection(_) ->
    Action = fun(Channel) ->
        URI =
            ?MQTT_PREFIX ++
                "/connection?clientid=client1&token=badtoken",
        Req = make_req(delete),
        {ok, deleted, _} = do_request(Channel, URI, Req)
    end,
    do(Action).

t_delete_connection_missing_clientid(_) ->
    update_coap_with_connection_mode(false),
    Action = fun(Channel) ->
        URI = ?MQTT_PREFIX ++ "/connection",
        Req = make_req(delete),
        {ok, deleted, _} = do_request(Channel, URI, Req)
    end,
    do(Action),
    update_coap_with_connection_mode(true).

t_request_without_token_times_out(_) ->
    Action = fun(Channel) ->
        URI = ?PS_PREFIX ++ "/no_token_topic",
        Req = make_req(get, <<>>, [{observe, 0}]),
        case do_request(Channel, URI, Req) of
            {error, bad_request} -> ok;
            {error, bad_request, _} -> ok
        end
    end,
    do(Action).

t_unknown_path_bad_request(_) ->
    Action = fun(Channel) ->
        Token = connection(Channel),
        Prefix = "coap://127.0.0.1/unknown",
        Queries = #{
            "clientid" => <<"client1">>,
            "token" => list_to_binary(Token)
        },
        URI = compose_uri(Prefix, Queries, false),
        Req = make_req(get),
        case do_request(Channel, URI, Req) of
            {error, bad_request, _} -> ok;
            {error, bad_request} -> ok
        end,
        disconnection(Channel, Token)
    end,
    do(Action).

t_invalid_token_request(_) ->
    Action = fun(Channel) ->
        Token = connection(Channel),
        URI = pubsub_uri("abc", "badtoken"),
        Req = make_req(get, <<>>, [{observe, 0}]),
        {error, bad_request, _} = do_request(Channel, URI, Req),
        disconnection(Channel, Token)
    end,
    do(Action).

t_mqtt_handler_errors(_) ->
    Action = fun(Channel) ->
        Token = connection(Channel),
        URI1 = ?MQTT_PREFIX ++ "/invalid?clientid=client1&token=" ++ Token,
        Req1 = make_req(get),
        case do_request(Channel, URI1, Req1) of
            {error, bad_request, _} -> ok;
            {error, bad_request} -> ok
        end,
        disconnection(Channel, Token)
    end,
    do(Action).

t_pubsub_handler_errors(_) ->
    Action = fun(Channel) ->
        Token = connection(Channel),
        URI1 = ?PS_PREFIX ++ "?clientid=client1&token=" ++ Token,
        Req1 = make_req(get),
        {error, bad_request, _} = do_request(Channel, URI1, Req1),
        URI2 = pubsub_uri("abc", Token),
        Req2 = make_req(get, <<>>, [{observe, 2}]),
        {error, bad_request, _} = do_request(Channel, URI2, Req2),
        disconnection(Channel, Token)
    end,
    do(Action).

t_pubsub_unauthorized(_) ->
    Action = fun(Channel) ->
        Token = connection(Channel),
        URI = pubsub_uri("deny", Token),
        Req1 = make_req(post, <<"payload">>),
        case do_request(Channel, URI, Req1) of
            {error, uauthorized} -> ok;
            {error, uauthorized, _} -> ok;
            {error, unauthorized, _} -> ok;
            {error, unauthorized} -> ok
        end,
        Req2 = make_req(get, <<>>, [{observe, 0}]),
        case do_request(Channel, URI, Req2) of
            {error, uauthorized} -> ok;
            {error, uauthorized, _} -> ok;
            {error, unauthorized, _} -> ok;
            {error, unauthorized} -> ok
        end,
        disconnection(Channel, Token)
    end,
    do(Action).

t_subscribe_opts_nl_rh(_) ->
    Fun = fun(Channel, Token) ->
        Topic = <<"nlrh">>,
        URI = pubsub_uri(binary_to_list(Topic), Token) ++ "&nl=1&rh=2",
        Req = make_req(get, <<>>, [{observe, 0}]),
        {ok, content, _} = do_request(Channel, URI, Req),
        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        {_, SubOpts} = lists:keyfind(Topic, 1, emqx_broker:subscriptions(SubPid)),
        ?assertEqual(1, maps:get(nl, SubOpts)),
        ?assertEqual(2, maps:get(rh, SubOpts)),
        UnReq = make_req(get, <<>>, [{observe, 1}]),
        {ok, nocontent, _} = do_request(Channel, URI, UnReq),
        true
    end,
    with_connection(Fun).

t_subscribe_qos_from_config(_) ->
    OldConf = emqx:get_raw_config([gateway, coap]),
    Fun = fun(Channel, Token) ->
        Cases = [
            {qos0, con, 0},
            {qos1, con, 1},
            {qos2, con, 2},
            {coap, non, 0},
            {coap, con, 1}
        ],
        lists:foreach(
            fun({CfgType, ReqType, ExpectQos}) ->
                {ok, _} =
                    emqx_gateway_conf:update_gateway(
                        coap,
                        OldConf#{<<"subscribe_qos">> => atom_to_binary(CfgType)}
                    ),
                Topic = list_to_binary(io_lib:format("cfg/sub/~p", [CfgType])),
                URI = pubsub_uri(binary_to_list(Topic), Token),
                Req = make_req_type(ReqType, get, <<>>, [{observe, 0}]),
                {ok, content, _} = do_request(Channel, URI, Req),
                timer:sleep(100),
                [SubPid] = emqx:subscribers(Topic),
                {_, SubOpts} =
                    lists:keyfind(Topic, 1, emqx_broker:subscriptions(SubPid)),
                ?assertEqual(ExpectQos, maps:get(qos, SubOpts)),
                UnReq = make_req_type(ReqType, get, <<>>, [{observe, 1}]),
                {ok, nocontent, _} = do_request(Channel, URI, UnReq)
            end,
            Cases
        ),
        true
    end,
    try
        with_connection(Fun)
    after
        _ = emqx_gateway_conf:update_gateway(coap, OldConf)
    end.

t_publish_qos_from_config(_) ->
    OldConf = emqx:get_raw_config([gateway, coap]),
    Fun = fun(Channel, Token) ->
        Cases = [
            {qos0, con, 0},
            {qos1, con, 1},
            {qos2, con, 2},
            {coap, non, 0},
            {coap, con, 1}
        ],
        lists:foreach(
            fun({CfgType, ReqType, ExpectQos}) ->
                {ok, _} =
                    emqx_gateway_conf:update_gateway(
                        coap,
                        OldConf#{<<"publish_qos">> => atom_to_binary(CfgType)}
                    ),
                Topic = list_to_binary(io_lib:format("cfg/pub/~p", [CfgType])),
                URI = pubsub_uri(binary_to_list(Topic), Token),
                emqx:subscribe(Topic),
                Req = make_req_type(ReqType, post, <<"qos">>, []),
                {ok, changed, _} = do_request(Channel, URI, Req),
                receive
                    {deliver, Topic, Msg} ->
                        ?assertEqual(ExpectQos, Msg#message.qos)
                after 500 ->
                    ?assert(false)
                end
            end,
            Cases
        ),
        true
    end,
    try
        with_connection(Fun)
    after
        _ = emqx_gateway_conf:update_gateway(coap, OldConf)
    end.

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
                ?assertEqual(Payload, Msg#message.payload),
                Props = emqx_message:get_header(properties, Msg, #{}),
                ?assertEqual(60, maps:get('Message-Expiry-Interval', Props))
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

t_ignore_unknown_version(_) ->
    Packet = <<2:2, 0:2, 0:4, 0:3, 1:5, 16#2000:16>>,
    ?assertEqual({error, timeout}, send_raw(Packet, 500)),
    ok.

t_invalid_tkl_reset(_) ->
    Packet = <<1:2, 0:2, 9:4, 0:3, 1:5, 16#2001:16>>,
    {ok, Bin} = send_raw(Packet, 1000),
    Msg = er_coap_message_parser:decode(Bin),
    ?assertMatch(#coap_message{type = reset, id = 16#2001}, Msg),
    ok.

t_payload_marker_empty_reset(_) ->
    Packet = <<1:2, 0:2, 0:4, 0:3, 1:5, 16#2002:16, 16#FF>>,
    {ok, Bin} = send_raw(Packet, 1000),
    Msg = er_coap_message_parser:decode(Bin),
    ?assertMatch(#coap_message{type = reset, id = 16#2002}, Msg),
    ok.

t_unknown_critical_option_bad_option(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 16#2003,
        token = <<1>>,
        options = [{uri_path, [<<"ps">>, <<"topic">>]}, {9, <<1>>}]
    },
    Packet = er_coap_message_parser:encode(Msg),
    {ok, Bin} = send_raw(Packet, 1000),
    Resp = er_coap_message_parser:decode(Bin),
    ?assertMatch(#coap_message{method = {error, bad_option}}, Resp),
    ok.

t_unknown_method_code_405(_) ->
    Packet = <<1:2, 0:2, 1:4, 0:3, 31:5, 16#2004:16, 16#AA>>,
    {ok, Bin} = send_raw(Packet, 1000),
    Resp = er_coap_message_parser:decode(Bin),
    ?assertMatch(#coap_message{method = {error, method_not_allowed}}, Resp),
    ok.

t_invalid_block_szx_400(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 16#2005,
        token = <<2>>,
        options = [{uri_path, [<<"ps">>, <<"topic">>]}, {23, <<7>>}]
    },
    Packet = er_coap_message_parser:encode(Msg),
    {ok, Bin} = send_raw(Packet, 1000),
    Resp = er_coap_message_parser:decode(Bin),
    ?assertMatch(#coap_message{method = {error, bad_request}}, Resp),
    ok.

t_reserved_class_non_confirmable_ignored(_) ->
    Packet = <<1:2, 1:2, 0:4, 1:3, 0:5, 16#2006:16>>,
    ?assertEqual({error, timeout}, send_raw(Packet, 500)),
    ok.

t_empty_message_with_data_reset(_) ->
    Packet = <<1:2, 0:2, 0:4, 0:3, 0:5, 16#2007:16, 16#FF>>,
    {ok, Bin} = send_raw(Packet, 1000),
    Msg = er_coap_message_parser:decode(Bin),
    ?assertMatch(#coap_message{type = reset, id = 16#2007}, Msg),
    ok.

t_duplicate_critical_option_bad_option(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 16#2008,
        token = <<3>>,
        options = [{uri_host, <<"a.example">>}, {uri_host, <<"b.example">>}]
    },
    Packet = er_coap_message_parser:encode(Msg),
    {ok, Bin} = send_raw(Packet, 1000),
    Resp = er_coap_message_parser:decode(Bin),
    ?assertMatch(#coap_message{method = {error, bad_option}}, Resp),
    ok.

t_invalid_if_none_match_bad_option(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 16#2009,
        token = <<4>>,
        options = [{5, <<1>>}]
    },
    Packet = er_coap_message_parser:encode(Msg),
    {ok, Bin} = send_raw(Packet, 1000),
    Resp = er_coap_message_parser:decode(Bin),
    ?assertMatch(#coap_message{method = {error, bad_option}}, Resp),
    ok.

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

make_req_type(Type, Method, Payload, Opts) ->
    er_coap_message:request(Type, Method, Payload, Opts).

do_request(Channel, URI, #coap_message{options = Opts} = Req) ->
    {_, _, Path, Query} = er_coap_client:resolve_uri(URI),
    Opts2 = [{uri_path, Path}, {uri_query, Query} | Opts],
    Req2 = Req#coap_message{options = Opts2},
    ?LOGT("send request:~ts~nReq:~p~n", [URI, Req2]),

    {ok, _} = er_coap_channel:send(Channel, Req2),
    with_response(Channel).

with_response(Channel) ->
    receive
        {coap_response, _ChId, Channel, _Ref, Message = #coap_message{method = Code}} ->
            return_response(Code, Message);
        {coap_error, _ChId, Channel, _Ref, reset} ->
            {error, reset}
    after 2000 ->
        {error, timeout}
    end.

return_response({ok, Code}, Message) ->
    {ok, Code, er_coap_message:get_content(Message)};
return_response({error, Code}, #coap_message{payload = <<>>}) ->
    {error, Code};
return_response({error, Code}, Message) ->
    {error, Code, er_coap_message:get_content(Message)}.

do(Fun) ->
    emqx_coap_test_helpers:with_udp_channel(Fun).

send_raw(Packet, Timeout) ->
    emqx_coap_test_helpers:send_raw(Packet, Timeout).

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
