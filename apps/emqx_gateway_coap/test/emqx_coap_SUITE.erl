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
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).
-define(PS_PREFIX, "coap://127.0.0.1/ps").
-define(MQTT_PREFIX, "coap://127.0.0.1/mqtt").
-define(OBSERVE_NOTIFICATION_QUEUE_MAX_LEN, 100).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config1 = emqx_coap_test_helpers:start_gateway(Config),
    emqx_common_test_http:create_default_app(),
    {ok, _} = emqx_gateway_auth_ct:start(),
    Config1.

end_per_suite(Config) ->
    ok = emqx_gateway_auth_ct:stop(),
    emqx_coap_test_helpers:stop_gateway(Config).

init_per_testcase(t_connection_with_expire, Config) ->
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
    Config.

end_per_testcase(t_heartbeat, Config) ->
    OldConf = ?config(old_conf, Config),
    {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf),
    ok;
end_per_testcase(t_connection_with_expire, Config) ->
    snabbkaffe:stop(),
    Config;
end_per_testcase(_, Config) ->
    %% Cases that stub `emqx_access_control:authenticate/1' must not leave
    %% the stub behind: the cases that follow authenticate for real.
    catch meck:unload(emqx_access_control),
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

with_connectionless_coap(EnableAuthn, Fun) ->
    OldConf = emqx:get_raw_config([gateway, coap]),
    OldListenerConf = emqx_conf:get([gateway, coap, listeners, udp, default]),
    try
        {ok, _} = emqx_gateway_conf:update_gateway(
            coap,
            OldConf#{<<"connection_required">> => <<"false">>}
        ),
        ok = update_coap_udp_listener_authn(EnableAuthn),
        Fun()
    after
        ok = update_coap_udp_listener(OldListenerConf),
        {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf)
    end.

update_coap_udp_listener_authn(EnableAuthn) ->
    ListenerPath = [gateway, coap, listeners, udp, default],
    ListenerConf0 = emqx_conf:get(ListenerPath),
    ListenerConf1 = ListenerConf0#{enable_authn => EnableAuthn},
    ok = update_coap_udp_listener(ListenerConf1),
    ?assertEqual(
        EnableAuthn,
        emqx_conf:get([gateway, coap, listeners, udp, default, enable_authn], undefined)
    ),
    ok.

update_coap_udp_listener(ListenerConf) ->
    case emqx_gateway_conf:update_listener(coap, {udp, default}, ListenerConf) of
        ok -> ok;
        {ok, _} -> ok
    end.

with_notify_type(NotifyType, Fun) ->
    OldConf = emqx:get_raw_config([gateway, coap]),
    {ok, _} = emqx_gateway_conf:update_gateway(
        coap,
        OldConf#{<<"notify_type">> => atom_to_binary(NotifyType)}
    ),
    try
        Fun()
    after
        {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf)
    end.

with_blockwise_max_size(MaxBlockSize, Fun) ->
    with_blockwise_config(#{<<"max_block_size">> => MaxBlockSize}, Fun).

with_blockwise_config(Overrides, Fun) ->
    OldConf = emqx:get_raw_config([gateway, coap]),
    OldBlockwise = maps:get(<<"blockwise">>, OldConf, #{}),
    {ok, _} = emqx_gateway_conf:update_gateway(
        coap,
        OldConf#{<<"blockwise">> => maps:merge(OldBlockwise, Overrides)}
    ),
    try
        Fun()
    after
        {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf)
    end.

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
            {ok, changed, _} = send_heartbeat(Channel, Token),

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

t_mountpoint_after_authn(_) ->
    ok = meck:new(emqx_access_control, [passthrough, no_history]),
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
                send_heartbeat(Channel, Token)
            ),
            disconnection(Channel, Token),
            ok
        end,
        do(Action)
    after
        meck:unload(emqx_access_control),
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
        {ok, changed, _} = send_heartbeat(Channel, Token, true),

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
                ?assertMatch({ok, changed, _}, send_heartbeat(Channel, Token)),
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
    ok = emqx_gateway_auth_ct:start_auth(authn_http),
    try
        {ok, _Sock, Channel} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
        URI =
            ?MQTT_PREFIX ++
                "/connection?clientid=client1&username=deny&password=public",
        Req = make_req(post),
        ?assertMatch({error, bad_request, _}, do_request(Channel, URI, Req)),

        timer:sleep(100),
        ?assertEqual(
            [],
            emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
        )
    after
        ok = emqx_gateway_auth_ct:stop_auth(authn_http)
    end,
    ok.

t_connection_with_expire(_) ->
    ok = emqx_gateway_auth_ct:start_auth(authn_http),
    try
        {ok, _Sock, Channel} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),

        URI =
            ?MQTT_PREFIX ++
                "/connection?clientid=client1&username=expire&password=public",

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
        )
    after
        ok = emqx_gateway_auth_ct:stop_auth(authn_http)
    end.

t_update_not_restart_listener(_) ->
    update_coap_with_mountpoint(<<"mp/">>),
    with_connection(fun(Channel, Token) ->
        ?assertMatch({ok, changed, _}, send_heartbeat(Channel, Token)),
        update_coap_with_mountpoint(<<>>),
        ?assertMatch({ok, changed, _}, send_heartbeat(Channel, Token)),
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
        ClientId = <<"client1">>,
        Locker = list_to_atom("emqx_gateway_coap_locker"),
        {true, _} = ekka_locker:acquire(Locker, ClientId, quorum),
        Prefix = ?MQTT_PREFIX ++ "/connection",
        Queries = #{
            "clientid" => ClientId,
            "username" => <<"admin">>,
            "password" => <<"public">>
        },
        URI = compose_uri(Prefix, Queries, false),
        Req = make_req(post),
        try
            case do_request(Channel, URI, Req) of
                {error, bad_request, _} -> ok;
                {error, bad_request} -> ok
            end
        after
            _ = ekka_locker:release(Locker, ClientId, quorum)
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
        {ok, Sock2, Channel2} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
        Prefix = ?MQTT_PREFIX ++ "/connection",
        Queries = #{
            "username" => <<"admin">>,
            "password" => <<"public">>
        },
        URI = compose_uri(Prefix, Queries, false),
        Req = make_req(post),
        {error, bad_request, _} = do_request(Channel2, URI, Req),
        er_coap_channel:close(Channel2),
        er_coap_udp_socket:close(Sock2),
        disconnection(Channel, Token)
    end,
    do(Action).

t_request_with_token_no_connection(_) ->
    Action = fun(Channel) ->
        URI = pubsub_uri("no_connection", "tok"),
        Req = make_req(get, <<>>, [{observe, 0}]),
        ?assertMatch({error, unauthorized, _}, do_request(Channel, URI, Req))
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
        try
            ?assertMatch({error, unauthorized, _}, do_request(Channel, URI, Req))
        after
            disconnection(Channel, Token)
        end
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
    OldAuthz = emqx:get_raw_config([authorization]),
    ok = emqx_gateway_auth_ct:start_auth(authz_http),
    try
        Action = fun(Channel) ->
            Token = connection(Channel),
            URI = pubsub_uri("deny", Token),
            Req1 = make_req(post, <<"payload">>),
            case do_request(Channel, URI, Req1) of
                {error, unauthorized} -> ok;
                {error, unauthorized, _} -> ok
            end,
            Req2 = make_req(get, <<>>, [{observe, 0}]),
            case do_request(Channel, URI, Req2) of
                {error, unauthorized} -> ok;
                {error, unauthorized, _} -> ok
            end,
            disconnection(Channel, Token)
        end,
        do(Action)
    after
        ok = emqx_gateway_auth_ct:stop_auth(authz_http),
        {ok, _} = emqx:update_config([authorization], OldAuthz)
    end.

t_connectionless_hardened_no_auth_config_rejects_pub_sub(_) ->
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        with_connectionless_coap(true, fun() ->
            PublishTopic = <<"security/coap/hardened/publish">>,
            SubscribeTopic = <<"security/coap/hardened/subscribe">>,
            Payload = <<"blocked">>,
            emqx:subscribe(PublishTopic),
            try
                do(fun(Channel) ->
                    PublishURI = pubsub_uri(binary_to_list(PublishTopic)),
                    assert_coap_unauthorized(
                        do_request(Channel, PublishURI, make_req(post, Payload))
                    ),
                    receive
                        {deliver, PublishTopic, _Msg} ->
                            ct:fail(connectionless_publish_was_published)
                    after 500 ->
                        ok
                    end,

                    SubscribeURI = pubsub_uri(binary_to_list(SubscribeTopic)),
                    assert_coap_unauthorized(
                        do_request(Channel, SubscribeURI, make_req(get, <<>>, [{observe, 0}]))
                    ),
                    timer:sleep(100),
                    ?assertEqual([], emqx:subscribers(SubscribeTopic))
                end)
            after
                emqx:unsubscribe(PublishTopic)
            end
        end)
    end).

t_connectionless_legacy_no_auth_config_allows_pub_sub(_) ->
    emqx_common_test_helpers:with_security_profile("legacy", fun() ->
        with_connectionless_coap(true, fun() ->
            assert_connectionless_pub_sub_allowed(
                <<"security/coap/legacy/publish">>,
                <<"security/coap/legacy/subscribe">>
            )
        end)
    end).

t_connectionless_observe_skips_session_subscribed_hook(_) ->
    emqx_common_test_helpers:with_security_profile("legacy", fun() ->
        with_connectionless_coap(true, fun() ->
            Parent = self(),
            SubscribedHook = {?MODULE, forward_session_subscribed, [Parent]},
            ok = emqx_hooks:add('session.subscribed', SubscribedHook, ?HP_HIGHEST),
            try
                do(fun(Channel) ->
                    Topic = <<"coap/connectionless/no-session-subscribed">>,
                    ClientId = <<"coap-client-observe">>,
                    URI = pubsub_uri(binary_to_list(Topic), #{
                        "clientid" => ClientId
                    }),
                    Req = (make_req(get, <<>>, [{observe, 0}]))#coap_message{
                        token = <<"obs-no-session-subscribed">>
                    },
                    {ok, content, _} = do_request(Channel, URI, Req),

                    timer:sleep(100),
                    [SubPid] = emqx:subscribers(Topic),
                    ?assert(is_pid(SubPid)),
                    receive
                        {coap_session_subscribed, _SubClientInfo, Topic, _SubOpts} ->
                            ct:fail(connectionless_session_subscribed_hook_called)
                    after 500 ->
                        ok
                    end
                end)
            after
                ok = emqx_hooks:del('session.subscribed', {?MODULE, forward_session_subscribed})
            end
        end)
    end).

t_connectionless_hardened_listener_authn_disabled_allows_pub_sub(_) ->
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        with_connectionless_coap(false, fun() ->
            assert_connectionless_pub_sub_allowed(
                <<"security/coap/authn-disabled/publish">>,
                <<"security/coap/authn-disabled/subscribe">>
            )
        end)
    end).

t_connectionless_hardened_authn_allows_pub_sub(_) ->
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        ok = emqx_gateway_auth_ct:start_auth(authn_http),
        try
            with_connectionless_coap(true, fun() ->
                Query = #{
                    "clientid" => <<"connless-auth-ok">>,
                    "username" => <<"admin">>,
                    "password" => <<"public">>
                },
                assert_connectionless_pub_sub_allowed(
                    <<"security/coap/authn/publish">>,
                    <<"security/coap/authn/subscribe">>,
                    Query
                )
            end)
        after
            ok = emqx_gateway_auth_ct:stop_auth(authn_http)
        end
    end).

t_connectionless_hardened_bad_authn_rejects_pub_sub(_) ->
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        ok = emqx_gateway_auth_ct:start_auth(authn_http),
        try
            with_connectionless_coap(true, fun() ->
                Query = #{
                    "clientid" => <<"connless-auth-bad">>,
                    "username" => <<"deny">>,
                    "password" => <<"public">>
                },
                assert_connectionless_pub_sub_rejected(
                    <<"security/coap/authn-bad/publish">>,
                    <<"security/coap/authn-bad/subscribe">>,
                    Query
                )
            end)
        after
            ok = emqx_gateway_auth_ct:stop_auth(authn_http)
        end
    end).

t_connectionless_hardened_authn_is_request_scoped(_) ->
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        ok = emqx_gateway_auth_ct:start_auth(authn_http),
        try
            with_connectionless_coap(true, fun() ->
                Query = #{
                    "clientid" => <<"connless-auth-scoped">>,
                    "username" => <<"admin">>,
                    "password" => <<"public">>
                },
                assert_connectionless_auth_is_request_scoped(
                    <<"security/coap/authn-scoped/allowed">>,
                    <<"security/coap/authn-scoped/rejected-publish">>,
                    <<"security/coap/authn-scoped/rejected-subscribe">>,
                    Query
                )
            end)
        after
            ok = emqx_gateway_auth_ct:stop_auth(authn_http)
        end
    end).

t_connectionless_mountpoint_from_authn_client_attrs(_) ->
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        update_coap_with_mountpoint(<<"mp/${client_attrs.group}/">>),
        ok = meck:new(emqx_access_control, [passthrough, no_history]),
        ok = meck:expect(
            emqx_access_control,
            authenticate,
            fun
                (#{username := <<"admin">>}) ->
                    {ok, #{client_attrs => #{<<"group">> => <<"g1">>}}};
                (ClientInfo) ->
                    meck:passthrough([ClientInfo])
            end
        ),
        Query = #{
            "clientid" => <<"connless-auth-mountpoint">>,
            "username" => <<"admin">>,
            "password" => <<"public">>
        },
        PublishTopic = <<"security/coap/authn-mountpoint/publish">>,
        MountedPublishTopic = <<"mp/g1/security/coap/authn-mountpoint/publish">>,
        SubscribeTopic = <<"security/coap/authn-mountpoint/subscribe">>,
        MountedSubscribeTopic = <<"mp/g1/security/coap/authn-mountpoint/subscribe">>,
        Payload = <<"mounted">>,
        emqx:subscribe(MountedPublishTopic),
        try
            with_connectionless_coap(true, fun() ->
                do(fun(Channel) ->
                    PublishURI = pubsub_uri(binary_to_list(PublishTopic), Query),
                    {ok, changed, _} = do_request(Channel, PublishURI, make_req(post, Payload)),
                    receive
                        {deliver, MountedPublishTopic, Msg} ->
                            ?assertEqual(Payload, Msg#message.payload)
                    after 500 ->
                        ct:fail(connectionless_mounted_publish_not_delivered)
                    end,

                    SubscribeURI = pubsub_uri(binary_to_list(SubscribeTopic), Query),
                    {ok, content, _} = do_request(
                        Channel,
                        SubscribeURI,
                        make_req(get, <<>>, [{observe, 0}])
                    ),
                    timer:sleep(100),
                    ?assertEqual([], emqx:subscribers(SubscribeTopic)),
                    [_SubPid] = emqx:subscribers(MountedSubscribeTopic),
                    ok
                end)
            end)
        after
            emqx:unsubscribe(MountedPublishTopic),
            update_coap_with_mountpoint(<<>>),
            meck:unload(emqx_access_control)
        end
    end).

t_subscribe_opts_nl_rh(_) ->
    Fun = fun(Channel, Token) ->
        Topic = <<"nlrh">>,
        URI = pubsub_uri(binary_to_list(Topic), Token) ++ "&nl=1&rh=2",
        Req = make_req(get, <<>>, [{observe, 0}]),
        {ok, content, ObserveReply} = do_observe_request(Channel, URI, Req),
        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        {_, SubOpts} = lists:keyfind(Topic, 1, emqx_broker:subscriptions(SubPid)),
        ?assertEqual(1, maps:get(nl, SubOpts)),
        ?assertEqual(2, maps:get(rh, SubOpts)),
        UnReq = (make_req(get, <<>>, [{observe, 1}]))#coap_message{
            token = ObserveReply#coap_message.token
        },
        {ok, nocontent, _} = do_message_request(Channel, URI, UnReq),
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
                {ok, content, ObserveReply} = do_observe_request(Channel, URI, Req),
                timer:sleep(100),
                [SubPid] = emqx:subscribers(Topic),
                {_, SubOpts} =
                    lists:keyfind(Topic, 1, emqx_broker:subscriptions(SubPid)),
                ?assertEqual(ExpectQos, maps:get(qos, SubOpts)),
                UnReq = (make_req_type(ReqType, get, <<>>, [{observe, 1}]))#coap_message{
                    token = ObserveReply#coap_message.token
                },
                {ok, nocontent, _} = do_message_request(Channel, URI, UnReq)
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

t_publish_mountpoint_from_authn_client_attrs(_) ->
    update_coap_with_mountpoint(<<"mp/${client_attrs.group}/">>),
    ok = meck:new(emqx_access_control, [passthrough, no_history]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun
            (#{username := <<"admin">>}) ->
                {ok, #{client_attrs => #{<<"group">> => <<"g1">>}}};
            (ClientInfo) ->
                meck:passthrough([ClientInfo])
        end
    ),
    MountedTopic = <<"mp/g1/abc">>,
    Payload = <<"123">>,
    try
        emqx:subscribe(MountedTopic),
        with_connection(fun(Channel, Token) ->
            URI = pubsub_uri("abc", Token),
            Req = make_req(post, Payload),
            {ok, changed, _} = do_request(Channel, URI, Req),
            receive
                {deliver, MountedTopic, Msg} ->
                    ?assertEqual(Payload, Msg#message.payload)
            after 500 ->
                ?assert(false)
            end,
            true
        end)
    after
        update_coap_with_mountpoint(<<>>),
        emqx:unsubscribe(MountedTopic),
        meck:unload(emqx_access_control)
    end.

t_publish_with_retain_qos_expiry(_) ->
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
    ok.

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
        {ok, content, ObserveReply} = do_observe_request(Channel, URI, Req),
        ?LOGT("observer topic:~ts~n", [Topic]),

        timer:sleep(100),
        [SubPid] = emqx:subscribers(Topic),
        ?assert(is_pid(SubPid)),

        UnReq = (make_req(get, Payload, [{observe, 1}]))#coap_message{
            token = ObserveReply#coap_message.token
        },
        {ok, nocontent, _} = do_message_request(Channel, URI, UnReq),
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

t_observe_notify_type_qos(_) ->
    with_notify_type(qos, fun() ->
        Fun = fun(Channel, Token) ->
            Topic = <<"coap/observe_notify_qos">>,
            URI = pubsub_uri(binary_to_list(Topic), Token),
            Req = make_req(get, <<>>, [{observe, 0}]),
            {ok, content, _} = do_request(Channel, URI, Req),

            Payload0 = <<"qos0">>,
            publish(Topic, ?QOS_0, Payload0),
            Notify0 = assert_notify(Channel, non, Payload0),
            ack_if_con(Channel, Notify0),

            Payload1 = <<"qos1">>,
            publish(Topic, ?QOS_1, Payload1),
            Notify1 = assert_notify(Channel, con, Payload1),
            ack_if_con(Channel, Notify1),
            true
        end,
        with_connection(Fun)
    end).

t_observe_notify_type_con(_) ->
    with_notify_type(con, fun() ->
        Fun = fun(Channel, Token) ->
            Topic = <<"coap/observe_notify_con">>,
            Payload = <<"confirmable">>,
            URI = pubsub_uri(binary_to_list(Topic), Token),
            Req = make_req(get, <<>>, [{observe, 0}]),
            {ok, content, _} = do_request(Channel, URI, Req),

            publish(Topic, ?QOS_0, Payload),
            Notify = assert_notify(Channel, con, Payload),
            ack_if_con(Channel, Notify),
            true
        end,
        with_connection(Fun)
    end).

t_observe_notify_type_non(_) ->
    with_notify_type(non, fun() ->
        Fun = fun(Channel, Token) ->
            Topic = <<"coap/observe_notify_non">>,
            Payload = <<"non-confirmable">>,
            URI = pubsub_uri(binary_to_list(Topic), Token),
            Req = make_req(get, <<>>, [{observe, 0}]),
            {ok, content, _} = do_request(Channel, URI, Req),

            publish(Topic, ?QOS_1, Payload),
            Notify = assert_notify(Channel, non, Payload),
            ack_if_con(Channel, Notify),
            true
        end,
        with_connection(Fun)
    end).

t_observe_non_notify_waits_for_con_inflight(_) ->
    with_notify_type(qos, fun() ->
        Fun = fun(Channel, Token) ->
            Topic = <<"coap/observe_notify_non_queue">>,
            observe_topic(Channel, Token, Topic, <<"obsnq">>),

            publish(Topic, ?QOS_1, <<"qos1-con">>),
            Notify1 = assert_notify(Channel, con, <<"qos1-con">>),
            publish(Topic, ?QOS_0, <<"qos0-non">>),
            ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

            ack_if_con(Channel, Notify1),
            Notify2 = assert_notify(Channel, non, <<"qos0-non">>),
            ack_if_con(Channel, Notify2),
            true
        end,
        with_connection(Fun)
    end).

t_observe_con_notify_uses_shared_session_queue(_) ->
    with_notify_type(con, fun() ->
        Fun = fun(Channel, Token) ->
            TopicA = <<"coap/observe_notify_queue/a">>,
            TopicB = <<"coap/observe_notify_queue/b">>,
            observe_topic(Channel, Token, TopicA, <<"obsqa">>),
            observe_topic(Channel, Token, TopicB, <<"obsqb">>),

            Payload1 = <<"first-a">>,
            publish(TopicA, ?QOS_0, Payload1),
            Notify1 = assert_notify(Channel, con, Payload1),

            Payload2 = <<"second-b">>,
            publish(TopicB, ?QOS_0, Payload2),
            ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

            ?assertNotEqual(
                [],
                emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
            ),
            ack_if_con(Channel, Notify1),
            Notify2 = assert_notify(Channel, con, Payload2),

            Payload3 = <<"third-a">>,
            publish(TopicA, ?QOS_0, Payload3),
            ?assertEqual({error, timeout}, with_message_response(Channel, 300)),
            ack_if_con(Channel, Notify2),
            Notify3 = assert_notify(Channel, con, Payload3),
            ack_if_con(Channel, Notify3),
            ?assertMatch({ok, changed, _}, send_heartbeat(Channel, Token)),
            true
        end,
        with_connection(Fun)
    end).

-doc "Retrieve an Observe representation with a new Token for each continuation GET.".
t_observe_block2_new_tokens(_) ->
    with_notify_type(non, fun() ->
        with_blockwise_max_size(16, fun() ->
            with_block2_connection(fun(Channel, Token) ->
                Topic = <<"coap/block2_new_tokens">>,
                ObserveToken = observe_block2_topic(Channel, Token, Topic),
                publish(Topic, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
                Notify = assert_block2_notify(
                    Channel, non, binary:copy(<<"A">>, 16), {0, true, 16}
                ),
                ?assertEqual(ObserveToken, Notify#coap_message.token),
                URI = pubsub_uri(binary_to_list(Topic), Token),
                FollowResp = assert_block2_followup(
                    Channel, URI, <<"block-1">>, 1, binary:copy(<<"B">>, 16), {1, true, 16}
                ),
                ?assertEqual(undefined, coap_option(observe, FollowResp, undefined)),
                _ = assert_block2_followup(
                    Channel, URI, <<"block-2">>, 2, binary:copy(<<"C">>, 8), {2, false, 16}
                ),
                Req = (make_req(get, <<>>, [{block2, {1, false, 16}}]))#coap_message{
                    token = <<"expired">>
                },
                ?assertMatch({error, _, _}, do_message_request(Channel, URI, Req)),
                true
            end)
        end)
    end).

-doc "Match continuation GETs to their resource even when another Observe Token is used.".
t_observe_block2_resource_isolation(_) ->
    with_block2_observe(non, fun(Channel, Token) ->
        TopicA = <<"coap/block2_resources/a">>,
        TopicB = <<"coap/block2_resources/b">>,
        ObserveTokenA = observe_block2_topic(Channel, Token, TopicA),
        ObserveTokenB = observe_block2_topic(Channel, Token, TopicB),
        publish(TopicA, ?QOS_0, blockwise_payload(<<"A">>, <<"a">>, <<"1">>)),
        _ = assert_block2_notify(Channel, non, binary:copy(<<"A">>, 16), {0, true, 16}),
        publish(TopicB, ?QOS_0, blockwise_payload(<<"B">>, <<"b">>, <<"2">>)),
        _ = assert_block2_notify(Channel, non, binary:copy(<<"B">>, 16), {0, true, 16}),
        URIa = pubsub_uri(binary_to_list(TopicA), Token),
        URIb = pubsub_uri(binary_to_list(TopicB), Token),
        WrongURI = pubsub_uri("coap/block2_resources/unknown", Token),
        Req = (make_req(get, <<>>, [{block2, {1, false, 16}}]))#coap_message{token = ObserveTokenA},
        ?assertMatch({error, _, _}, do_message_request(Channel, WrongURI, Req)),
        ?assertMatch(
            {error, _, _}, do_message_request(Channel, URIa, Req#coap_message{method = put})
        ),
        _ = assert_block2_followup(
            Channel, URIb, ObserveTokenA, 1, binary:copy(<<"b">>, 16), {1, true, 16}
        ),
        _ = assert_block2_followup(
            Channel, URIa, ObserveTokenB, 1, binary:copy(<<"a">>, 16), {1, true, 16}
        ),
        _ = assert_block2_followup(
            Channel, URIb, <<"tail-b">>, 2, binary:copy(<<"2">>, 8), {2, false, 16}
        ),
        _ = assert_block2_followup(
            Channel, URIa, <<"tail-a">>, 2, binary:copy(<<"1">>, 8), {2, false, 16}
        ),
        true
    end).

-doc "Keep error response blocks separate from Observe continuations using new Tokens.".
t_observe_block2_error_response_isolation(_) ->
    lists:foreach(fun observe_block2_error_response_isolation/1, [before_notify, after_notify]).

observe_block2_error_response_isolation(Order) ->
    with_block2_observe(non, fun(Channel, Token) ->
        Topic = <<"coap/block2_errors/", (atom_to_binary(Order))/binary>>,
        ObserveToken = observe_block2_topic(Channel, Token, Topic),
        URI = pubsub_uri(binary_to_list(Topic), Token),
        ErrorToken = <<"error">>,
        ErrorRequest = fun() ->
            Req = (make_req(get))#coap_message{token = ErrorToken},
            {error, bad_request, Reply} = do_message_request(Channel, URI, Req),
            ?assertEqual({0, true, 16}, coap_option(block2, Reply, undefined)),
            ?assertEqual(<<"invalid observe ">>, Reply#coap_message.payload)
        end,
        case Order of
            before_notify -> ErrorRequest();
            after_notify -> ok
        end,
        publish(Topic, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
        _ = assert_block2_notify(Channel, non, binary:copy(<<"A">>, 16), {0, true, 16}),
        case Order of
            before_notify -> ok;
            after_notify -> ErrorRequest()
        end,
        _ = assert_block2_followup(
            Channel, URI, ObserveToken, 1, binary:copy(<<"B">>, 16), {1, true, 16}
        ),
        _ = assert_block2_followup(
            Channel, URI, <<"new-1">>, 1, binary:copy(<<"B">>, 16), {1, true, 16}
        ),
        _ = assert_block2_followup(
            Channel, URI, <<"new-2">>, 2, binary:copy(<<"C">>, 8), {2, false, 16}
        ),
        ErrorFollowup = (make_req(get, <<>>, [{block2, {1, false, 16}}]))#coap_message{
            token = ErrorToken
        },
        {error, bad_request, ErrorReply} = do_message_request(Channel, URI, ErrorFollowup),
        ?assertEqual(<<"value">>, ErrorReply#coap_message.payload),
        ?assertEqual({1, false, 16}, coap_option(block2, ErrorReply, undefined)),
        true
    end).

-doc "Keep an unfinished representation stable and drain the next notification after its last block.".
t_observe_block2_new_token_queue(_) ->
    with_block2_observe(con, fun(Channel, Token) ->
        Topic = <<"coap/block2_new_token_queue">>,
        ObserveToken = observe_block2_topic(Channel, Token, Topic),
        publish(Topic, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
        Notify = assert_block2_notify(Channel, con, binary:copy(<<"A">>, 16), {0, true, 16}),
        ack_if_con(Channel, Notify),
        publish(Topic, ?QOS_0, blockwise_payload(<<"X">>, <<"Y">>, <<"Z">>)),
        ?assertEqual({error, timeout}, with_message_response(Channel, 300)),
        URI = pubsub_uri(binary_to_list(Topic), Token),
        _ = assert_block2_followup(
            Channel, URI, <<"next-1">>, 1, binary:copy(<<"B">>, 16), {1, true, 16}
        ),
        ?assertEqual({error, timeout}, with_message_response(Channel, 300)),
        _ = assert_block2_followup(
            Channel, URI, <<"next-2">>, 2, binary:copy(<<"C">>, 8), {2, false, 16}
        ),
        Next = assert_block2_notify(Channel, con, binary:copy(<<"X">>, 16), {0, true, 16}),
        ?assertEqual(ObserveToken, Next#coap_message.token),
        ack_if_con(Channel, Next),
        _ = assert_block2_followup(
            Channel, URI, <<"next-3">>, 1, binary:copy(<<"Y">>, 16), {1, true, 16}
        ),
        _ = assert_block2_followup(
            Channel, URI, <<"next-4">>, 2, binary:copy(<<"Z">>, 8), {2, false, 16}
        ),
        true
    end).

-doc "Replace an observation and discard its old transfer and queued notifications.".
t_observe_replace(_) ->
    lists:foreach(
        fun({NotifyType, ReuseToken, ChangeOptions}) ->
            with_block2_observe(NotifyType, fun(Sock, Channel, Token) ->
                Topic = <<"coap/observe_replace">>,
                OtherTopic = <<"coap/observe_replace/other">>,
                URI = pubsub_uri(binary_to_list(Topic), Token) ++ "&qos=0",
                NewURI =
                    case ChangeOptions of
                        true -> pubsub_uri(binary_to_list(Topic), Token) ++ "&qos=1";
                        false -> URI
                    end,
                OldToken = observe_block2_uri(Channel, URI),
                _ = observe_block2_topic(Channel, Token, OtherTopic),
                publish(Topic, ?QOS_1, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
                Old = assert_block2_notify(
                    Channel, NotifyType, binary:copy(<<"A">>, 16), {0, true, 16}
                ),
                publish(Topic, ?QOS_1, blockwise_payload(<<"D">>, <<"E">>, <<"F">>)),
                ?assertEqual({error, timeout}, with_message_response(Channel, 100)),
                NewToken =
                    case ReuseToken of
                        true ->
                            _ = register_observe(Channel, NewURI, OldToken),
                            OldToken;
                        false ->
                            observe_block2_uri(Channel, NewURI)
                    end,
                Tail = (make_req(get, <<>>, [{block2, {2, false, 16}}]))#coap_message{
                    token = <<"old-tail">>
                },
                ?assertMatch({error, _, _}, do_message_request(Channel, URI, Tail)),
                publish(OtherTopic, ?QOS_0, <<"other works">>),
                Other = assert_notify(Channel, NotifyType, <<"other works">>),
                ack_if_con(Channel, Other),
                publish(Topic, ?QOS_1, blockwise_payload(<<"X">>, <<"Y">>, <<"Z">>)),
                Next = assert_block2_notify(
                    Channel, NotifyType, binary:copy(<<"X">>, 16), {0, true, 16}
                ),
                ?assertEqual(NewToken, Next#coap_message.token),
                Sock !
                    {datagram, {{127, 0, 0, 1}, 5683},
                        er_coap_message_parser:encode(emqx_coap_message:reset(Old))},
                Cancel = (make_req(get, <<>>, [{observe, 1}]))#coap_message{token = OldToken},
                ?assertMatch({error, _, _}, do_message_request(Channel, URI, Cancel)),
                {200, Subscriptions} = request(get, "/gateways/coap/clients/client1/subscriptions"),
                [Subscription] = [Sub || #{topic := T} = Sub <- Subscriptions, T =:= Topic],
                ?assertEqual(
                    case ChangeOptions of
                        true -> 1;
                        false -> 0
                    end,
                    maps:get(qos, Subscription)
                ),
                ack_if_con(Channel, Next),
                _ = assert_block2_followup(
                    Channel, NewURI, <<"new-mid">>, 1, binary:copy(<<"Y">>, 16), {1, true, 16}
                ),
                _ = assert_block2_followup(
                    Channel, NewURI, <<"new-tail">>, 2, binary:copy(<<"Z">>, 8), {2, false, 16}
                ),
                Wait =
                    case NotifyType of
                        con -> 3500;
                        non -> 300
                    end,
                ?assertEqual({error, timeout}, with_message_response(Channel, Wait))
            end)
        end,
        [
            {con, false, true},
            {non, false, true},
            {con, true, true},
            {non, true, true},
            {con, false, false},
            {non, false, false}
        ]
    ).

-doc "Draining one topic does not discard another topic's unfinished representation.".
t_observe_block2_cross_topic_queue(_) ->
    with_block2_observe(con, fun(Channel, Token) ->
        TopicA = <<"coap/block2_cross_queue/a">>,
        TopicB = <<"coap/block2_cross_queue/b">>,
        _ = observe_block2_topic(Channel, Token, TopicA),
        _ = observe_block2_topic(Channel, Token, TopicB),
        publish(TopicA, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
        First = assert_block2_notify(Channel, con, binary:copy(<<"A">>, 16), {0, true, 16}),
        publish(TopicB, ?QOS_0, blockwise_payload(<<"X">>, <<"Y">>, <<"Z">>)),
        ?assertEqual({error, timeout}, with_message_response(Channel, 100)),
        ack_if_con(Channel, First),
        Next = assert_block2_notify(Channel, con, binary:copy(<<"X">>, 16), {0, true, 16}),
        ack_if_con(Channel, Next),
        URIa = pubsub_uri(binary_to_list(TopicA), Token),
        URIb = pubsub_uri(binary_to_list(TopicB), Token),
        _ = assert_block2_followup(
            Channel, URIa, <<"cross-a1">>, 1, binary:copy(<<"B">>, 16), {1, true, 16}
        ),
        _ = assert_block2_followup(
            Channel, URIb, <<"cross-b1">>, 1, binary:copy(<<"Y">>, 16), {1, true, 16}
        ),
        _ = assert_block2_followup(
            Channel, URIa, <<"cross-a2">>, 2, binary:copy(<<"C">>, 8), {2, false, 16}
        ),
        _ = assert_block2_followup(
            Channel, URIb, <<"cross-b2">>, 2, binary:copy(<<"Z">>, 8), {2, false, 16}
        )
    end).

-doc "Management option updates preserve the client's observation and unfinished notifications.".
t_observe_update_by_api(_) ->
    lists:foreach(
        fun({NotifyType, Mountpoint}) ->
            OldConf = emqx:get_raw_config([gateway, coap]),
            {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf#{
                <<"mountpoint">> => Mountpoint
            }),
            try
                with_block2_observe(NotifyType, fun(Channel, Token) ->
                    Topic = <<"api-update">>,
                    MountedTopic = <<Mountpoint/binary, Topic/binary>>,
                    URI = pubsub_uri(binary_to_list(Topic), Token) ++ "&qos=0",
                    ObserveToken = observe_block2_uri(Channel, URI),
                    FirstType =
                        case NotifyType of
                            qos -> non;
                            _ -> NotifyType
                        end,
                    publish(MountedTopic, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
                    First = assert_block2_notify(
                        Channel, FirstType, binary:copy(<<"A">>, 16), {0, true, 16}
                    ),
                    publish(MountedTopic, ?QOS_0, blockwise_payload(<<"X">>, <<"Y">>, <<"Z">>)),
                    ?assertEqual({error, timeout}, with_message_response(Channel, 100)),
                    Path = "/gateways/coap/clients/client1/subscriptions",
                    {201, _} = request(post, Path, #{topic => Topic, qos => 1}),
                    {200, [Subscription]} = request(get, Path),
                    ?assertEqual(MountedTopic, maps:get(topic, Subscription)),
                    ?assertEqual(1, maps:get(qos, Subscription)),
                    ?assertEqual({error, timeout}, with_message_response(Channel, 100)),
                    ack_if_con(Channel, First),
                    _ = assert_block2_followup(
                        Channel, URI, <<"api-mid">>, 1, binary:copy(<<"B">>, 16), {1, true, 16}
                    ),
                    _ = assert_block2_followup(
                        Channel, URI, <<"api-tail">>, 2, binary:copy(<<"C">>, 8), {2, false, 16}
                    ),
                    Next = assert_block2_notify(
                        Channel, FirstType, binary:copy(<<"X">>, 16), {0, true, 16}
                    ),
                    ?assertEqual(ObserveToken, Next#coap_message.token),
                    ack_if_con(Channel, Next),
                    _ = assert_block2_followup(
                        Channel, URI, <<"api-next">>, 1, binary:copy(<<"Y">>, 16), {1, true, 16}
                    ),
                    _ = assert_block2_followup(
                        Channel, URI, <<"api-last">>, 2, binary:copy(<<"Z">>, 8), {2, false, 16}
                    ),
                    publish(MountedTopic, ?QOS_1, <<"updated qos">>),
                    NewType =
                        case NotifyType of
                            qos -> con;
                            _ -> NotifyType
                        end,
                    Updated = assert_notify(Channel, NewType, <<"updated qos">>),
                    ?assertEqual(ObserveToken, Updated#coap_message.token),
                    ack_if_con(Channel, Updated),
                    Cancel = (make_req(get, <<>>, [{observe, 1}]))#coap_message{
                        token = ObserveToken
                    },
                    ?assertMatch({ok, nocontent, _}, do_message_request(Channel, URI, Cancel)),
                    publish(MountedTopic, ?QOS_1, <<"cancelled">>),
                    ?assertEqual({error, timeout}, with_message_response(Channel, 300)),
                    {200, []} = request(get, Path)
                end)
            after
                {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf)
            end
        end,
        [{Type, Prefix} || Type <- [con, non, qos], Prefix <- [<<>>, <<"mounted/">>]]
    ).

-doc "Management cancellation clears a mounted observation and its queued notifications.".
t_observe_cancel_by_api(_) ->
    OldConf = emqx:get_raw_config([gateway, coap]),
    {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf#{<<"mountpoint">> => <<"mounted/">>}),
    try
        with_block2_observe(con, fun(Sock, Channel, Token) ->
            Topic = <<"api-cancel">>,
            URI = pubsub_uri(binary_to_list(Topic), Token),
            _ = observe_block2_uri(Channel, URI),
            publish(<<"mounted/api-cancel">>, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
            First = assert_block2_notify(Channel, con, binary:copy(<<"A">>, 16), {0, true, 16}),
            publish(<<"mounted/api-cancel">>, ?QOS_0, blockwise_payload(<<"X">>, <<"Y">>, <<"Z">>)),
            ?assertEqual({error, timeout}, with_message_response(Channel, 100)),
            {204, _} = request(delete, "/gateways/coap/clients/client1/subscriptions/api-cancel"),
            Req = (make_req(get, <<>>, [{block2, {1, false, 16}}]))#coap_message{
                token = <<"api-tail">>
            },
            ?assertMatch({error, _, _}, do_message_request(Channel, URI, Req)),
            Sock !
                {datagram, {{127, 0, 0, 1}, 5683},
                    er_coap_message_parser:encode(emqx_coap_message:reset(First))},
            publish(<<"mounted/api-cancel">>, ?QOS_0, <<"cancelled">>),
            ?assertEqual({error, timeout}, with_message_response(Channel, 300)),
            {200, []} = request(get, "/gateways/coap/clients/client1/subscriptions")
        end)
    after
        {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf)
    end.

-doc "An identical renewal and a rejected registration preserve an unfinished notification.".
t_observe_renew_preserves_block2(_) ->
    with_block2_observe(non, fun(Channel, Token) ->
        Topic = <<"coap/observe_renew">>,
        URI = pubsub_uri(binary_to_list(Topic), Token),
        ObserveToken = observe_block2_uri(Channel, URI),
        publish(Topic, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
        _ = assert_block2_notify(Channel, non, binary:copy(<<"A">>, 16), {0, true, 16}),
        _ = register_observe(Channel, URI, ObserveToken),
        Rejected = (make_req(get, <<>>, [{observe, 0}]))#coap_message{token = <<>>},
        ?assertMatch({error, bad_request, _}, do_message_request(Channel, URI, Rejected)),
        _ = assert_block2_followup(
            Channel, URI, <<"renew-1">>, 1, binary:copy(<<"B">>, 16), {1, true, 16}
        ),
        _ = assert_block2_followup(
            Channel, URI, <<"renew-2">>, 2, binary:copy(<<"C">>, 8), {2, false, 16}
        )
    end).

register_observe(Channel, URI, Token) ->
    Req = (make_req(get, <<>>, [{observe, 0}]))#coap_message{token = Token},
    {ok, content, Reply} = do_message_request(Channel, URI, Req),
    ?assertEqual(Token, Reply#coap_message.token),
    ?assert(is_integer(coap_option(observe, Reply, undefined))),
    Reply.

-doc "Release the cached body and queued notification after real CON retransmission exhaustion.".
t_observe_block2_retransmit_exhaustion(_) ->
    ct:timetrap({minutes, 3}),
    OldConf = emqx:get_raw_config([gateway, coap]),
    {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf#{
        <<"connection_required">> => false,
        <<"idle_timeout">> => <<"180s">>,
        <<"heartbeat">> => <<"180s">>,
        <<"notify_type">> => <<"con">>,
        <<"blockwise">> => #{<<"max_block_size">> => 16, <<"exchange_lifetime">> => <<"180s">>}
    }),
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    try
        Topic = <<"coap/block2_retransmit_exhaustion">>,
        URI = pubsub_uri(binary_to_list(Topic)),
        ObserveToken = <<"retryobs">>,
        Observe = (make_req(get, <<>>, [{observe, 0}]))#coap_message{token = ObserveToken},
        ?assertMatch(#coap_message{method = {ok, content}}, raw_coap_request(Socket, URI, Observe)),
        publish(Topic, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
        First = raw_coap_receive(Socket, 2000),
        ?assertEqual(con, First#coap_message.type),
        ?assertEqual(binary:copy(<<"A">>, 16), First#coap_message.payload),
        Mid = (make_req(get, <<>>, [{block2, {1, false, 16}}]))#coap_message{
            token = <<"retrymid">>
        },
        ?assertMatch(
            #coap_message{payload = <<"BBBBBBBBBBBBBBBB">>}, raw_coap_request(Socket, URI, Mid)
        ),
        publish(Topic, ?QOS_0, <<"next notice">>),
        Next = wait_observe_retries(Socket, First, 0),
        ?assertEqual(ObserveToken, Next#coap_message.token),
        raw_coap_send(Socket, #coap_message{type = ack, id = Next#coap_message.id}),
        Tail = (make_req(get, <<>>, [{block2, {2, false, 16}}]))#coap_message{
            token = <<"retryend">>
        },
        ?assertMatch(#coap_message{method = {error, _}}, raw_coap_request(Socket, URI, Tail))
    after
        gen_udp:close(Socket),
        {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf)
    end.

wait_observe_retries(Socket, First, Count) ->
    Msg = raw_coap_receive(Socket, 100000),
    ct:pal("Received notification after ~p retransmissions", [Count]),
    case Msg#coap_message.id =:= First#coap_message.id of
        true ->
            ?assertEqual(First#coap_message.payload, Msg#coap_message.payload),
            wait_observe_retries(Socket, First, Count + 1);
        false ->
            ?assertEqual(4, Count),
            ?assertEqual(<<"next notice">>, Msg#coap_message.payload),
            Msg
    end.

raw_coap_request(Socket, URI, #coap_message{options = Options} = Req) ->
    {_, _, Path, Query} = er_coap_client:resolve_uri(URI),
    Msg = Req#coap_message{
        id = erlang:unique_integer([positive]) rem 65536,
        options = [{uri_path, Path}, {uri_query, Query} | Options]
    },
    raw_coap_send(Socket, Msg),
    raw_coap_receive(Socket, 2000).

raw_coap_send(Socket, Msg) ->
    gen_udp:send(Socket, {127, 0, 0, 1}, 5683, er_coap_message_parser:encode(Msg)).

raw_coap_receive(Socket, Timeout) ->
    {ok, {_, _, Data}} = gen_udp:recv(Socket, 0, Timeout),
    er_coap_message_parser:decode(Data).

-doc "Remove the representation when its Observe relationship is cancelled.".
t_observe_block2_new_token_cancel(_) ->
    with_block2_observe(non, fun(Channel, Token) ->
        Topic = <<"coap/block2_new_token_cancel">>,
        ObserveToken = observe_block2_topic(Channel, Token, Topic),
        publish(Topic, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
        _ = assert_block2_notify(Channel, non, binary:copy(<<"A">>, 16), {0, true, 16}),
        URI = pubsub_uri(binary_to_list(Topic), Token),
        _ = assert_block2_followup(
            Channel, URI, <<"cancel-1">>, 1, binary:copy(<<"B">>, 16), {1, true, 16}
        ),
        Cancel = (make_req(get, <<>>, [{observe, 1}]))#coap_message{token = ObserveToken},
        ?assertMatch({ok, nocontent, _}, do_message_request(Channel, URI, Cancel)),
        Req = (make_req(get, <<>>, [{block2, {2, false, 16}}]))#coap_message{
            token = <<"cancel-2">>
        },
        ?assertMatch({error, _, _}, do_message_request(Channel, URI, Req)),
        true
    end).

-doc "Reset discards the representation while retaining the 6.x Observe relationship.".
t_observe_block2_new_token_reset(_) ->
    with_block2_observe(con, fun(Channel, Token) ->
        Topic = <<"coap/block2_new_token_reset">>,
        observe_block2_topic(Channel, Token, Topic),
        publish(Topic, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
        Notify = assert_block2_notify(Channel, con, binary:copy(<<"A">>, 16), {0, true, 16}),
        URI = pubsub_uri(binary_to_list(Topic), Token),
        _ = assert_block2_followup(
            Channel, URI, <<"reset-1">>, 1, binary:copy(<<"B">>, 16), {1, true, 16}
        ),
        reset_if_con(Channel, Notify),
        Req = (make_req(get, <<>>, [{block2, {2, false, 16}}]))#coap_message{token = <<"reset-2">>},
        ?assertMatch({error, _, _}, do_message_request(Channel, URI, Req)),
        publish(Topic, ?QOS_0, <<"still subscribed">>),
        Next = assert_notify(Channel, con, <<"still subscribed">>),
        ack_if_con(Channel, Next),
        true
    end).

-doc "Expire a cached representation before serving a continuation with a new Token.".
t_observe_block2_new_token_expiry(_) ->
    with_notify_type(non, fun() ->
        with_blockwise_config(
            #{<<"max_block_size">> => 16, <<"exchange_lifetime">> => <<"300ms">>},
            fun() ->
                with_block2_connection(fun(Channel, Token) ->
                    Topic = <<"coap/block2_new_token_expiry">>,
                    observe_block2_topic(Channel, Token, Topic),
                    publish(Topic, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
                    _ = assert_block2_notify(Channel, non, binary:copy(<<"A">>, 16), {0, true, 16}),
                    URI = pubsub_uri(binary_to_list(Topic), Token),
                    _ = assert_block2_followup(
                        Channel, URI, <<"expire-1">>, 1, binary:copy(<<"B">>, 16), {1, true, 16}
                    ),
                    timer:sleep(400),
                    Req = (make_req(get, <<>>, [{block2, {2, false, 16}}]))#coap_message{
                        token = <<"expire-2">>
                    },
                    ?assertMatch({error, _, _}, do_message_request(Channel, URI, Req)),
                    true
                end)
            end
        )
    end).

-doc "Keep a connectionless Observe representation private to the requesting UDP endpoint.".
t_observe_block2_peer_isolation(_) ->
    with_notify_type(non, fun() ->
        with_blockwise_max_size(16, fun() ->
            {ok, _} = update_coap_with_connection_mode(false),
            try
                do(fun(Channel) ->
                    Topic = <<"coap/block2_peer_isolation">>,
                    URI = pubsub_uri(binary_to_list(Topic)),
                    ObserveToken = observe_block2_uri(Channel, URI),
                    publish(Topic, ?QOS_0, blockwise_payload(<<"A">>, <<"B">>, <<"C">>)),
                    _ = assert_block2_notify(Channel, non, binary:copy(<<"A">>, 16), {0, true, 16}),
                    do(fun(OtherChannel) ->
                        Req = (make_req(get, <<>>, [{block2, {1, false, 16}}]))#coap_message{
                            token = ObserveToken
                        },
                        ?assertMatch({error, _, _}, do_message_request(OtherChannel, URI, Req))
                    end),
                    _ = assert_block2_followup(
                        Channel, URI, <<"peer-1">>, 1, binary:copy(<<"B">>, 16), {1, true, 16}
                    ),
                    _ = assert_block2_followup(
                        Channel, URI, <<"peer-2">>, 2, binary:copy(<<"C">>, 8), {2, false, 16}
                    )
                end)
            after
                {ok, _} = update_coap_with_connection_mode(true)
            end
        end)
    end).

with_block2_observe(NotifyType, Fun) ->
    with_notify_type(NotifyType, fun() ->
        with_blockwise_max_size(16, fun() -> with_block2_connection(Fun) end)
    end).

observe_block2_topic(Channel, Token, Topic) ->
    observe_block2_uri(Channel, pubsub_uri(binary_to_list(Topic), Token)).

observe_block2_uri(Channel, URI) ->
    {_, _, Path, Query} = er_coap_client:resolve_uri(URI),
    Req = make_req(get, <<>>, [{uri_path, Path}, {uri_query, Query}, {observe, 0}]),
    {ok, _} = er_coap_channel:send(Channel, Req),
    {ok, content, Reply} = with_message_response(Channel),
    Reply#coap_message.token.

with_block2_connection(Fun) ->
    {ok, Sock, Channel} = er_coap_udp_socket:connect({127, 0, 0, 1}, 5683),
    Token = connection(Channel),
    try
        case is_function(Fun, 3) of
            true -> Fun(Sock, Channel, Token);
            false -> Fun(Channel, Token)
        end
    after
        _ = catch disconnection(Channel, Token),
        er_coap_channel:close(Channel),
        er_coap_udp_socket:close(Sock)
    end.

t_observe_con_notify_queue_preserves_block2_until_drained(_) ->
    with_notify_type(con, fun() ->
        with_blockwise_max_size(16, fun() ->
            Fun = fun(Channel, Token) ->
                Topic = <<"coap/observe_notify_queue_block2">>,
                ObserveToken = <<"obsbq">>,
                observe_topic(Channel, Token, Topic, ObserveToken),

                PayloadA = blockwise_payload(<<"A">>, <<"B">>, <<"C">>),
                PayloadB = blockwise_payload(<<"X">>, <<"Y">>, <<"Z">>),
                publish(Topic, ?QOS_0, PayloadA),
                NotifyA0 = assert_block2_notify(
                    Channel, con, binary:copy(<<"A">>, 16), {0, true, 16}
                ),

                publish(Topic, ?QOS_0, PayloadB),
                ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

                URI = pubsub_uri(binary_to_list(Topic), Token),
                FollowReq = (make_req(get, <<>>, [{block2, {1, false, 16}}]))#coap_message{
                    token = NotifyA0#coap_message.token
                },
                {ok, content, FollowResp} = do_message_request(Channel, URI, FollowReq),
                ?assertEqual(binary:copy(<<"B">>, 16), notify_payload(FollowResp)),
                ?assertEqual(
                    {1, true, 16},
                    coap_option(block2, FollowResp, undefined)
                ),

                reset_if_con(Channel, NotifyA0),
                NotifyB0 = assert_block2_notify(
                    Channel, con, binary:copy(<<"X">>, 16), {0, true, 16}
                ),
                reset_if_con(Channel, NotifyB0),
                true
            end,
            with_connection(Fun)
        end)
    end).

t_observe_non_blockwise_queue_preserves_all_drained_tokens(_) ->
    with_notify_type(qos, fun() ->
        with_blockwise_max_size(16, fun() ->
            Fun = fun(Channel, Token) ->
                TopicA = <<"coap/observe_notify_queue_block2_multi/a">>,
                TopicB = <<"coap/observe_notify_queue_block2_multi/b">>,
                TopicC = <<"coap/observe_notify_queue_block2_multi/c">>,
                observe_topic(Channel, Token, TopicA, <<"obsbqa">>),
                observe_topic(Channel, Token, TopicB, <<"obsbqb">>),
                observe_topic(Channel, Token, TopicC, <<"obsbqc">>),

                publish(TopicA, ?QOS_1, <<"inflight-a">>),
                NotifyA = assert_notify(Channel, con, <<"inflight-a">>),

                PayloadB = blockwise_payload(<<"B">>, <<"b">>, <<"1">>),
                PayloadC = blockwise_payload(<<"C">>, <<"c">>, <<"2">>),
                publish(TopicB, ?QOS_0, PayloadB),
                publish(TopicC, ?QOS_0, PayloadC),
                ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

                ack_if_con(Channel, NotifyA),
                Notify1 = assert_notify(Channel, non),
                Notify2 = assert_notify(Channel, non),
                NotifyB = find_notify_by_payload(binary:copy(<<"B">>, 16), Notify1, Notify2),
                NotifyC = find_notify_by_payload(binary:copy(<<"C">>, 16), Notify1, Notify2),
                ?assertEqual({0, true, 16}, coap_option(block2, NotifyB, undefined)),
                ?assertEqual({0, true, 16}, coap_option(block2, NotifyC, undefined)),

                URIb = pubsub_uri(binary_to_list(TopicB), Token),
                FollowReqB = (make_req(get, <<>>, [{block2, {1, false, 16}}]))#coap_message{
                    token = NotifyB#coap_message.token
                },
                {ok, content, FollowRespB} = do_message_request(Channel, URIb, FollowReqB),
                ?assertEqual(binary:copy(<<"b">>, 16), notify_payload(FollowRespB)),
                ?assertEqual(
                    {1, true, 16},
                    coap_option(block2, FollowRespB, undefined)
                ),

                URIc = pubsub_uri(binary_to_list(TopicC), Token),
                FollowReqC = (make_req(get, <<>>, [{block2, {1, false, 16}}]))#coap_message{
                    token = NotifyC#coap_message.token
                },
                {ok, content, FollowRespC} = do_message_request(Channel, URIc, FollowReqC),
                ?assertEqual(binary:copy(<<"c">>, 16), notify_payload(FollowRespC)),
                ?assertEqual(
                    {1, true, 16},
                    coap_option(block2, FollowRespC, undefined)
                ),
                true
            end,
            with_connection(Fun)
        end)
    end).

t_observe_same_token_blockwise_queue_serializes_drained_notifications(_) ->
    with_notify_type(qos, fun() ->
        with_blockwise_max_size(16, fun() ->
            Fun = fun(Channel, Token) ->
                Topic = <<"coap/observe_notify_queue_block2_same_token">>,
                ObserveToken = <<"obsbst">>,
                observe_topic(Channel, Token, Topic, ObserveToken),

                publish(Topic, ?QOS_1, <<"inflight">>),
                NotifyA = assert_notify(Channel, con, <<"inflight">>),

                PayloadB = blockwise_payload(<<"B">>, <<"b">>, <<"1">>),
                PayloadC = blockwise_payload(<<"C">>, <<"c">>, <<"2">>),
                publish(Topic, ?QOS_0, PayloadB),
                publish(Topic, ?QOS_0, PayloadC),
                ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

                ack_if_con(Channel, NotifyA),
                Notify0 = assert_notify(Channel, non),
                ?assertEqual({0, true, 16}, coap_option(block2, Notify0, undefined)),
                FirstPayload = notify_payload(Notify0),
                ?assert(
                    lists:member(FirstPayload, [binary:copy(<<"B">>, 16), binary:copy(<<"C">>, 16)])
                ),
                {FirstMid, FirstTail, SecondHead, SecondMid, SecondTail} =
                    case FirstPayload of
                        <<"BBBBBBBBBBBBBBBB">> ->
                            {<<"b">>, <<"1">>, <<"C">>, <<"c">>, <<"2">>};
                        <<"CCCCCCCCCCCCCCCC">> ->
                            {<<"c">>, <<"2">>, <<"B">>, <<"b">>, <<"1">>}
                    end,
                ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

                URI = pubsub_uri(binary_to_list(Topic), Token),
                _ = assert_block2_followup(
                    Channel,
                    URI,
                    Notify0#coap_message.token,
                    1,
                    binary:copy(FirstMid, 16),
                    {1, true, 16}
                ),
                ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

                _ = assert_block2_followup(
                    Channel,
                    URI,
                    Notify0#coap_message.token,
                    2,
                    binary:copy(FirstTail, 8),
                    {2, false, 16}
                ),
                Notify1 = assert_block2_notify(
                    Channel, non, binary:copy(SecondHead, 16), {0, true, 16}
                ),
                _ = assert_block2_followup(
                    Channel,
                    URI,
                    Notify1#coap_message.token,
                    1,
                    binary:copy(SecondMid, 16),
                    {1, true, 16}
                ),
                _ = assert_block2_followup(
                    Channel,
                    URI,
                    Notify1#coap_message.token,
                    2,
                    binary:copy(SecondTail, 8),
                    {2, false, 16}
                ),
                true
            end,
            with_connection(Fun)
        end)
    end).

t_observe_con_notify_queue_drops_oldest_when_full(_) ->
    with_notify_type(con, fun() ->
        Fun = fun(Channel, Token) ->
            Topic = <<"coap/observe_notify_queue_full">>,
            observe_topic(Channel, Token, Topic, <<"obsqf">>),
            Before = gateway_metric('delivery.dropped.queue_full'),

            publish(Topic, ?QOS_0, <<"inflight">>),
            Notify = assert_notify(Channel, con, <<"inflight">>),

            QueueOverflow = 1,
            lists:foreach(
                fun(N) ->
                    Payload = iolist_to_binary(["queued-", integer_to_binary(N)]),
                    publish(Topic, ?QOS_0, Payload)
                end,
                lists:seq(1, ?OBSERVE_NOTIFICATION_QUEUE_MAX_LEN + QueueOverflow)
            ),
            wait_gateway_metric(
                'delivery.dropped.queue_full',
                Before + QueueOverflow,
                20
            ),
            ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

            ack_if_con(Channel, Notify),
            %% MQTT delivery scheduling may slightly reorder the published
            %% payloads by the time they reach the CoAP session, so this
            %% integration case asserts the observable queue-full drop metric
            %% and that the queue remains drainable.
            Notify2 = assert_notify(Channel, con),
            ?assert(is_queued_payload(notify_payload(Notify2))),
            true
        end,
        with_connection(Fun)
    end).

t_observe_con_notify_queue_drains_after_reset(_) ->
    with_notify_type(con, fun() ->
        Fun = fun(Channel, Token) ->
            TopicA = <<"coap/observe_notify_queue_reset/a">>,
            TopicB = <<"coap/observe_notify_queue_reset/b">>,
            observe_topic(Channel, Token, TopicA, <<"obsra">>),
            observe_topic(Channel, Token, TopicB, <<"obsrb">>),

            publish(TopicA, ?QOS_0, <<"first-a">>),
            Notify1 = assert_notify(Channel, con, <<"first-a">>),
            publish(TopicB, ?QOS_0, <<"second-b">>),
            ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

            reset_if_con(Channel, Notify1),
            Notify2 = assert_notify(Channel, con, <<"second-b">>),
            ack_if_con(Channel, Notify2),
            true
        end,
        with_connection(Fun)
    end).

t_observe_cancel_drops_pending_notifications(_) ->
    with_notify_type(con, fun() ->
        Fun = fun(Channel, Token) ->
            Topic = <<"coap/observe_notify_queue_cancel">>,
            ObserveToken = observe_block2_topic(Channel, Token, Topic),

            publish(Topic, ?QOS_0, <<"first">>),
            Notify1 = assert_notify(Channel, con, <<"first">>),
            publish(Topic, ?QOS_0, <<"queued-after-cancel">>),
            ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

            URI = pubsub_uri(binary_to_list(Topic), Token),
            UnReq = (make_req(get, <<>>, [{observe, 1}]))#coap_message{
                token = ObserveToken
            },
            {ok, nocontent, _} = do_message_request(Channel, URI, UnReq),

            ack_if_con(Channel, Notify1),
            ?assertEqual({error, timeout}, with_message_response(Channel, 500)),
            true
        end,
        with_connection(Fun)
    end).

t_observe_cancel_wildcard_drops_pending_notifications(_) ->
    with_notify_type(con, fun() ->
        Fun = fun(Channel, Token) ->
            ObserveTopic = <<"coap/observe_notify_queue_cancel_wildcard/+">>,
            PublishTopic1 = <<"coap/observe_notify_queue_cancel_wildcard/1">>,
            PublishTopic2 = <<"coap/observe_notify_queue_cancel_wildcard/2">>,
            ObserveToken = observe_block2_topic(Channel, Token, ObserveTopic),

            publish(PublishTopic1, ?QOS_0, <<"first">>),
            Notify1 = assert_notify(Channel, con, <<"first">>),
            publish(PublishTopic2, ?QOS_0, <<"queued-after-wildcard-cancel">>),
            ?assertEqual({error, timeout}, with_message_response(Channel, 300)),

            URI = pubsub_uri(binary_to_list(ObserveTopic), Token),
            UnReq = (make_req(get, <<>>, [{observe, 1}]))#coap_message{
                token = ObserveToken
            },
            {ok, nocontent, _} = do_message_request(Channel, URI, UnReq),

            ack_if_con(Channel, Notify1),
            ?assertEqual({error, timeout}, with_message_response(Channel, 500)),
            true
        end,
        with_connection(Fun)
    end).

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

        ObserveToken = observe(Channel, Token, true),

        {200, [Subs]} = request(get, Path),

        ?assertEqual(<<"coap/observe">>, maps:get(topic, Subs)),

        observe(Channel, Token, {false, ObserveToken}),

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
    {ok, <<"client1">>, _Packets1, State1} =
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
        {ok, <<"client1">>, _Packets2, _State2},
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
    {ok, <<"client1">>, _Packets1, State1} =
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
        {ok, <<"client1">>, _Packets2, _State2},
        emqx_coap_proxy_conn:get_connection_id(dummy, Peer, State1, Bin2)
    ).

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

send_heartbeat(Channel, Token) ->
    send_heartbeat(Channel, Token, false).

send_heartbeat(Channel, Token, ShortenParamName) ->
    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    ?LOGT("send heartbeat request:~ts~n", [URI]),
    Req = make_req(put),
    do_request(Channel, URI, Req).

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
    case do_request(Channel, URI, Req) of
        {ok, deleted, _} ->
            ok;
        {error, timeout} ->
            %% Use a fresh socket if the channel got closed during gateway updates.
            {ok, deleted, _} = er_coap_client:request(delete, URI),
            ok
    end.

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
    {ok, content, Reply} = do_observe_request(Channel, URI, Req),
    Reply#coap_message.token;
observe(Channel, Token, {false, ObserveToken}, ShortenParamName) ->
    Prefix = ?PS_PREFIX ++ "/coap/observe",
    Queries = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI = compose_uri(Prefix, Queries, ShortenParamName),
    Req = (make_req(get, <<>>, [{observe, 1}]))#coap_message{token = ObserveToken},
    {ok, nocontent, _Data} = do_message_request(Channel, URI, Req),
    ok.

observe_topic(Channel, Token, Topic, ObserveToken) ->
    URI = pubsub_uri(binary_to_list(Topic), Token),
    Req = (make_req(get, <<>>, [{observe, 0}]))#coap_message{token = ObserveToken},
    {ok, content, _} = do_request(Channel, URI, Req),
    ok.

pubsub_uri(Topic) when is_list(Topic) ->
    ?PS_PREFIX ++ "/" ++ Topic.

pubsub_uri(Topic, Query) when is_list(Topic), is_map(Query) ->
    compose_uri(pubsub_uri(Topic), Query, false);
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

publish(Topic, QoS, Payload) ->
    emqx:publish(emqx_message:make(<<"coap">>, QoS, Topic, Payload)).

gateway_metric(Name) ->
    proplists:get_value(Name, emqx_gateway_metrics:lookup(coap), 0).

wait_gateway_metric(Name, Expected, 0) ->
    ?assertEqual(Expected, gateway_metric(Name));
wait_gateway_metric(Name, Expected, Retries) ->
    case gateway_metric(Name) of
        Value when Value >= Expected ->
            ok;
        _ ->
            timer:sleep(100),
            wait_gateway_metric(Name, Expected, Retries - 1)
    end.

assert_notify(Channel, Type, Payload) ->
    Notify = assert_notify(Channel, Type),
    ?assertEqual(Payload, notify_payload(Notify)),
    Notify.

assert_notify(Channel, Type) ->
    {ok, content, Notify = #coap_message{type = Type}} = with_message_response(Channel),
    Notify.

assert_block2_notify(Channel, Type, Payload, Block2) ->
    Notify = assert_notify(Channel, Type, Payload),
    ?assertEqual(Block2, coap_option(block2, Notify, undefined)),
    Notify.

assert_block2_followup(Channel, URI, Token, Num, Payload, Block2) ->
    FollowReq = (make_req(get, <<>>, [{block2, {Num, false, 16}}]))#coap_message{
        token = Token
    },
    {ok, content, FollowResp} = do_message_request(Channel, URI, FollowReq),
    ?assertEqual(Token, FollowResp#coap_message.token),
    ?assertEqual(Payload, notify_payload(FollowResp)),
    ?assertEqual(Block2, coap_option(block2, FollowResp, undefined)),
    FollowResp.

notify_payload(Notify) ->
    #coap_content{payload = Payload} = er_coap_message:get_content(Notify),
    Payload.

coap_option(Option, #coap_message{options = Options}, Default) when is_map(Options) ->
    maps:get(Option, Options, Default);
coap_option(Option, #coap_message{options = Options}, Default) when is_list(Options) ->
    proplists:get_value(Option, Options, Default).

blockwise_payload(First, Second, Third) ->
    iolist_to_binary([
        binary:copy(First, 16),
        binary:copy(Second, 16),
        binary:copy(Third, 8)
    ]).

find_notify_by_payload(Payload, Notify1, Notify2) ->
    case {notify_payload(Notify1), notify_payload(Notify2)} of
        {Payload, _} ->
            Notify1;
        {_, Payload} ->
            Notify2
    end.

is_queued_payload(<<"queued-", _/binary>>) ->
    true;
is_queued_payload(_) ->
    false.

ack_if_con(Channel, #coap_message{type = con} = Message) ->
    {ok, _} = er_coap_channel:send(Channel, er_coap_message:ack(Message)),
    ok;
ack_if_con(_Channel, #coap_message{}) ->
    ok.

reset_if_con(Channel, #coap_message{type = con} = Message) ->
    {ok, _} = er_coap_channel:send(Channel, emqx_coap_message:reset(Message)),
    ok.

do_request(Channel, URI, #coap_message{options = Opts} = Req) ->
    {_, _, Path, Query} = er_coap_client:resolve_uri(URI),
    Opts2 = [{uri_path, Path}, {uri_query, Query} | Opts],
    Req2 = Req#coap_message{options = Opts2},
    ?LOGT("send request:~ts~nReq:~p~n", [URI, Req2]),

    {ok, _} = er_coap_channel:send(Channel, Req2),
    with_response(Channel).

do_observe_request(Channel, URI, #coap_message{options = Opts} = Req) ->
    {_, _, Path, Query} = er_coap_client:resolve_uri(URI),
    {ok, _} = er_coap_channel:send(Channel, Req#coap_message{
        options = [{uri_path, Path}, {uri_query, Query} | Opts]
    }),
    with_message_response(Channel).

do_message_request(Channel, URI, #coap_message{options = Opts} = Req) ->
    {_, _, Path, Query} = er_coap_client:resolve_uri(URI),
    Opts2 = [{uri_path, Path}, {uri_query, Query} | Opts],
    Req2 = Req#coap_message{options = Opts2},
    ?LOGT("send request:~ts~nReq:~p~n", [URI, Req2]),

    {ok, _} = er_coap_channel:send_message(Channel, make_ref(), Req2),
    with_message_response(Channel).

with_response(Channel) ->
    receive
        {coap_response, _ChId, Channel, _Ref, Message = #coap_message{method = Code}} ->
            return_response(Code, Message);
        {coap_error, _ChId, Channel, _Ref, reset} ->
            {error, reset}
    after 2000 ->
        {error, timeout}
    end.

with_message_response(Channel) ->
    with_message_response(Channel, 2000).

with_message_response(Channel, Timeout) ->
    receive
        {coap_response, _ChId, Channel, _Ref, Message = #coap_message{method = Code}} ->
            return_message_response(Code, Message);
        {coap_error, _ChId, Channel, _Ref, reset} ->
            {error, reset}
    after Timeout ->
        {error, timeout}
    end.

return_response({ok, Code}, Message) ->
    {ok, Code, er_coap_message:get_content(Message)};
return_response({error, Code}, #coap_message{payload = <<>>}) ->
    {error, Code};
return_response({error, Code}, Message) ->
    {error, Code, er_coap_message:get_content(Message)}.

return_message_response({ok, Code}, Message) ->
    {ok, Code, Message};
return_message_response({error, Code}, Message) ->
    {error, Code, Message}.

do(Fun) ->
    emqx_coap_test_helpers:with_udp_channel(Fun).

forward_session_subscribed(#{protocol := coap} = ClientInfo, Topic, SubOpts, Parent) ->
    Parent ! {coap_session_subscribed, ClientInfo, Topic, SubOpts},
    ok;
forward_session_subscribed(_ClientInfo, _Topic, _SubOpts, _Parent) ->
    ok.

assert_connectionless_pub_sub_allowed(PublishTopic, SubscribeTopic) ->
    assert_connectionless_pub_sub_allowed(PublishTopic, SubscribeTopic, #{}).

assert_connectionless_pub_sub_allowed(PublishTopic, SubscribeTopic, Query) ->
    Payload = <<"allowed">>,
    emqx:subscribe(PublishTopic),
    try
        do(fun(Channel) ->
            PublishURI = pubsub_uri(binary_to_list(PublishTopic), Query),
            {ok, changed, _} = do_request(Channel, PublishURI, make_req(post, Payload)),
            receive
                {deliver, PublishTopic, Msg} ->
                    ?assertEqual(Payload, Msg#message.payload)
            after 500 ->
                ct:fail(connectionless_publish_not_delivered)
            end,

            SubscribeURI = pubsub_uri(binary_to_list(SubscribeTopic), Query),
            {ok, content, _} = do_request(
                Channel,
                SubscribeURI,
                make_req(get, <<>>, [{observe, 0}])
            ),
            timer:sleep(100),
            [_SubPid] = emqx:subscribers(SubscribeTopic),
            ok
        end)
    after
        emqx:unsubscribe(PublishTopic)
    end.

assert_connectionless_pub_sub_rejected(PublishTopic, SubscribeTopic, Query) ->
    Payload = <<"blocked">>,
    emqx:subscribe(PublishTopic),
    try
        do(fun(Channel) ->
            PublishURI = pubsub_uri(binary_to_list(PublishTopic), Query),
            assert_coap_unauthorized(
                do_request(Channel, PublishURI, make_req(post, Payload))
            ),
            receive
                {deliver, PublishTopic, _Msg} ->
                    ct:fail(connectionless_publish_was_published)
            after 500 ->
                ok
            end,

            SubscribeURI = pubsub_uri(binary_to_list(SubscribeTopic), Query),
            assert_coap_unauthorized(
                do_request(Channel, SubscribeURI, make_req(get, <<>>, [{observe, 0}]))
            ),
            timer:sleep(100),
            ?assertEqual([], emqx:subscribers(SubscribeTopic))
        end)
    after
        emqx:unsubscribe(PublishTopic)
    end.

assert_connectionless_auth_is_request_scoped(
    AllowedTopic, RejectedPublishTopic, RejectedSubscribeTopic, Query
) ->
    AllowedPayload = <<"allowed">>,
    BlockedPayload = <<"blocked">>,
    emqx:subscribe(AllowedTopic),
    emqx:subscribe(RejectedPublishTopic),
    try
        do(fun(Channel) ->
            AllowedURI = pubsub_uri(binary_to_list(AllowedTopic), Query),
            {ok, changed, _} = do_request(Channel, AllowedURI, make_req(post, AllowedPayload)),
            receive
                {deliver, AllowedTopic, Msg} ->
                    ?assertEqual(AllowedPayload, Msg#message.payload)
            after 500 ->
                ct:fail(connectionless_publish_not_delivered)
            end,

            RejectedPublishURI = pubsub_uri(binary_to_list(RejectedPublishTopic)),
            assert_coap_unauthorized(
                do_request(Channel, RejectedPublishURI, make_req(post, BlockedPayload))
            ),
            receive
                {deliver, RejectedPublishTopic, _Msg} ->
                    ct:fail(connectionless_publish_was_published)
            after 500 ->
                ok
            end,

            RejectedSubscribeURI = pubsub_uri(binary_to_list(RejectedSubscribeTopic)),
            assert_coap_unauthorized(
                do_request(Channel, RejectedSubscribeURI, make_req(get, <<>>, [{observe, 0}]))
            ),
            timer:sleep(100),
            ?assertEqual([], emqx:subscribers(RejectedSubscribeTopic))
        end)
    after
        emqx:unsubscribe(AllowedTopic),
        emqx:unsubscribe(RejectedPublishTopic)
    end.

assert_coap_unauthorized({error, unauthorized}) ->
    ok;
assert_coap_unauthorized({error, unauthorized, _}) ->
    ok.

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
