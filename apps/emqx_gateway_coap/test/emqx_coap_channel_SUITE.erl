%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_channel_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include("emqx_coap_test.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_coap_test_helpers:start_gateway(Config).

end_per_suite(Config) ->
    emqx_coap_test_helpers:stop_gateway(Config).

t_pubsub_handler_direct(_) ->
    Ctx = coap_ctx(),
    CInfo = #{clientid => <<"client">>, mountpoint => <<>>},
    Msg1 = #coap_message{method = get, token = <<>>, options = #{observe => 0}},
    #{reply := Reply1} =
        emqx_coap_pubsub_handler:handle_request([<<"topic">>], Msg1, Ctx, CInfo),
    ?assertMatch(#coap_message{method = {error, bad_request}}, Reply1),
    Msg2 = #coap_message{method = put, token = <<"t">>},
    #{reply := Reply2} =
        emqx_coap_pubsub_handler:handle_request([<<"topic">>], Msg2, Ctx, CInfo),
    ?assertMatch(#coap_message{method = {error, method_not_allowed}}, Reply2),
    ok.

t_mqtt_handler_direct(_) ->
    Msg1 = #coap_message{method = get},
    #{reply := Reply1} =
        emqx_coap_mqtt_handler:handle_request([<<"not_connection">>], Msg1, #{}, #{}),
    ?assertMatch(#coap_message{method = {error, bad_request}}, Reply1),
    Msg2 = #coap_message{method = get},
    #{reply := Reply2} =
        emqx_coap_mqtt_handler:handle_request([<<"connection">>], Msg2, #{}, #{}),
    ?assertMatch(#coap_message{method = {error, method_not_allowed}}, Reply2),
    ok.

t_channel_direct(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683},
        peercert => [{pp2_ssl_cn, <<"CN">>}]
    },
    Channel0 = emqx_coap_channel:init(ConnInfo, #{ctx => coap_ctx()}),
    ChannelRequired =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => coap_ctx(),
            connection_required => false
        }),
    _ = emqx_coap_channel:info(ctx, Channel0),
    {shutdown, bad_frame, _} = emqx_coap_channel:handle_frame_error(bad_frame, Channel0),
    {ok, _} = emqx_coap_channel:handle_timeout(foo, unknown, Channel0),
    {shutdown, normal, _} = emqx_coap_channel:handle_timeout(foo, disconnect, Channel0),
    _ = emqx_coap_channel:handle_timeout(
        foo, {transport, {1, state_timeout, ack_timeout}}, Channel0
    ),
    KeepAlive0 = Channel0#channel.keepalive,
    KeepAlive = KeepAlive0#keepalive{idle_milliseconds = KeepAlive0#keepalive.max_idle_millisecond},
    {shutdown, timeout, _} = emqx_coap_channel:handle_timeout(
        foo, {keepalive, KeepAlive#keepalive.statval}, Channel0#channel{keepalive = KeepAlive}
    ),
    {ok, _} = emqx_coap_channel:handle_cast(inc_recv_pkt, Channel0),
    {ok, _} = emqx_coap_channel:handle_cast(unexpected_cast, Channel0),
    {shutdown, normal, _} = emqx_coap_channel:handle_cast(close, Channel0),
    {ok, _} = emqx_coap_channel:handle_info({subscribe, []}, Channel0),
    {ok, _} = emqx_coap_channel:handle_info(unexpected_info, Channel0),
    _ = emqx_coap_channel:handle_in(
        #coap_message{
            type = con,
            method = get,
            id = 1,
            options = #{uri_path => [<<"ps">>, <<"topic">>], uri_query => #{}}
        },
        ChannelRequired
    ),
    Channel1 = Channel0#channel{token = <<"token">>},
    {reply, true, _} = emqx_coap_channel:handle_call({check_token, <<"token">>}, none, Channel1),
    {reply, false, _} = emqx_coap_channel:handle_call({check_token, <<"bad">>}, none, Channel1),
    {reply, ignored, _} = emqx_coap_channel:handle_call(unexpected_call, none, Channel1),
    {shutdown, discarded, _, _} = emqx_coap_channel:handle_call(discard, none, Channel1),
    {noreply, [{outgoing, [OutMsg | _]}], Channel2} = emqx_coap_channel:handle_call(
        {send_request, #coap_message{type = con, method = get, token = <<"t">>}},
        self(),
        Channel0
    ),
    _ = emqx_coap_channel:handle_in(
        #coap_message{type = reset, id = OutMsg#coap_message.id, token = OutMsg#coap_message.token},
        Channel2
    ),
    ok.

t_channel_frame_error_handling(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    Channel0 = emqx_coap_channel:init(ConnInfo, #{ctx => coap_ctx()}),
    {ok, Channel1} = emqx_coap_channel:handle_in({coap_ignore, ignore}, Channel0),
    {ok, [{outgoing, Reset}], Channel2} = emqx_coap_channel:handle_in(
        {coap_format_error, con, 300, bad_format},
        Channel1
    ),
    ?assertMatch(#coap_message{type = reset, id = 300}, Reset),
    {ok, Channel3} = emqx_coap_channel:handle_in(
        {coap_format_error, non, 301, bad_format},
        Channel2
    ),
    {ok, [{outgoing, ErrReply}], _} = emqx_coap_channel:handle_in(
        {coap_request_error, #coap_message{type = con, id = 302, token = <<>>},
            {error, bad_option}},
        Channel3
    ),
    ?assertMatch(#coap_message{method = {error, bad_option}}, ErrReply),
    ok.

t_channel_block1_connection(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    Channel0 =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => coap_ctx(),
            connection_required => false
        }),
    Msg0 = #coap_message{
        type = con,
        method = post,
        id = 100,
        token = <<"blk1">>,
        payload = <<"part-a">>,
        options = #{
            uri_path => [<<"ps">>, <<"topic">>],
            uri_query => #{},
            block1 => {0, true, 16}
        }
    },
    {ok, [{outgoing, [Continue]}], Channel1} = emqx_coap_channel:handle_in(Msg0, Channel0),
    ?assertEqual({ok, continue}, Continue#coap_message.method),
    Msg1 = Msg0#coap_message{
        id = 101,
        payload = <<"part-b">>,
        options = (Msg0#coap_message.options)#{block1 => {1, false, 16}}
    },
    {ok, Replies, _Channel2} = emqx_coap_channel:handle_in(Msg1, Channel1),
    ?assert(
        lists:any(
            fun
                ({outgoing, [#coap_message{method = Method} | _]}) -> Method =/= {ok, continue};
                (_) -> false
            end,
            Replies
        )
    ),
    ok.

t_channel_connection_hooks_error_direct(_) ->
    HookPoint = 'client.connect',
    HookAction = {emqx_coap_test_helpers, hook_return_error, [hook_failed]},
    ok = emqx_coap_test_helpers:add_test_hook(HookPoint, HookAction),
    try
        ConnInfo = #{
            peername => {{127, 0, 0, 1}, 9999},
            sockname => {{127, 0, 0, 1}, 5683}
        },
        Channel0 =
            emqx_coap_channel:init(ConnInfo, #{
                ctx => coap_ctx(),
                connection_required => true
            }),
        ConnReq = #coap_message{
            type = con,
            method = post,
            id = 6,
            options = #{
                uri_path => [<<"mqtt">>, <<"connection">>],
                uri_query => #{<<"clientid">> => <<"client1">>}
            }
        },
        {shutdown, normal, _, _} = emqx_coap_channel:handle_in(ConnReq, Channel0)
    after
        ok = emqx_coap_test_helpers:del_test_hook(HookPoint, HookAction)
    end,
    ok.

t_channel_connection_open_session_error_direct(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683},
        conn_mod => emqx_gateway_conn
    },
    Channel0 =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => coap_ctx(),
            connection_required => true
        }),
    ClientId = <<"client1">>,
    Locker = list_to_atom("emqx_gateway_coap_locker"),
    Parent = self(),
    LockPid =
        spawn_link(fun() ->
            {true, _} = ekka_locker:acquire(Locker, ClientId, quorum),
            Parent ! {locked, self()},
            receive
                release -> ok
            after 30000 ->
                ok
            end,
            _ = ekka_locker:release(Locker, ClientId, quorum),
            Parent ! {released, self()}
        end),
    try
        receive
            {locked, LockPid} -> ok
        after 1000 ->
            ?assert(false)
        end,
        ConnReq = #coap_message{
            type = con,
            method = post,
            id = 8,
            options = #{
                uri_path => [<<"mqtt">>, <<"connection">>],
                uri_query => #{<<"clientid">> => ClientId}
            }
        },
        {ok, Replies, _} = emqx_coap_channel:handle_in(ConnReq, Channel0),
        ?assert(
            lists:any(
                fun
                    ({outgoing, [#coap_message{method = {error, bad_request}} | _]}) -> true;
                    (_) -> false
                end,
                Replies
            )
        )
    after
        LockPid ! release,
        receive
            {released, LockPid} -> ok
        after 1000 ->
            ok
        end
    end,
    ok.

t_channel_check_token_paths(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    Channel0 =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => coap_ctx(),
            connection_required => true
        }),
    DelReq = #coap_message{
        type = con,
        method = delete,
        id = 9,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => <<"client1">>, <<"token">> => <<"tok">>}
        }
    },
    {shutdown, normal, _, _} = emqx_coap_channel:handle_in(DelReq, Channel0),
    BadReq = #coap_message{
        type = con,
        method = get,
        id = 10,
        options = #{
            uri_path => [<<"ps">>, <<"topic">>],
            uri_query => #{<<"clientid">> => <<"client1">>, <<"token">> => <<"tok">>}
        }
    },
    {ok, {outgoing, BadReply}, _} = emqx_coap_channel:handle_in(BadReq, Channel0),
    ?assertEqual({error, unauthorized}, BadReply#coap_message.method),
    ?assertEqual(
        <<"Invalid token or clientid in connection mode">>,
        BadReply#coap_message.payload
    ),
    ResetReq = #coap_message{type = reset, id = 999, token = <<>>},
    {ok, _} = emqx_coap_channel:handle_in(ResetReq, Channel0),
    ok.

t_channel_check_token_and_get_clientinfo_sanitized(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    BaseChannel = emqx_coap_channel:init(
        ConnInfo,
        #{ctx => coap_ctx(), connection_required => true}
    ),
    Channel0 = BaseChannel#channel{
        token = <<"tok">>,
        clientinfo = (BaseChannel#channel.clientinfo)#{
            clientid => <<"client1">>,
            username => <<"admin">>,
            password => <<"public">>,
            is_superuser => true
        }
    },
    {reply, {ok, ClientInfo}, _} = emqx_coap_channel:handle_call(
        {check_token_and_get_clientinfo, <<"tok">>},
        none,
        Channel0
    ),
    ?assertEqual(<<"client1">>, maps:get(clientid, ClientInfo)),
    ?assertEqual(<<"admin">>, maps:get(username, ClientInfo)),
    ?assertEqual(true, maps:get(is_superuser, ClientInfo)),
    ?assertEqual(false, maps:is_key(password, ClientInfo)),
    {reply, false, _} = emqx_coap_channel:handle_call(
        {check_token_and_get_clientinfo, <<"bad">>},
        none,
        Channel0
    ),
    ok.

t_channel_same_clientid_invalid_token_no_self_cm_call(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    BaseChannel = emqx_coap_channel:init(
        ConnInfo,
        #{ctx => coap_ctx(), connection_required => true}
    ),
    Channel0 = BaseChannel#channel{
        conn_state = connected,
        token = <<"tok">>,
        clientinfo = (BaseChannel#channel.clientinfo)#{clientid => <<"client1">>}
    },
    Req = #coap_message{
        type = con,
        method = get,
        id = 11,
        options = #{
            uri_path => [<<"ps">>, <<"topic">>],
            uri_query => #{<<"clientid">> => <<"client1">>, <<"token">> => <<"bad-token">>}
        }
    },
    ok = meck:new(emqx_gateway_cm, [no_link, passthrough]),
    try
        {ok, {outgoing, Reply}, _} = emqx_coap_channel:handle_in(Req, Channel0),
        ?assertEqual({error, unauthorized}, Reply#coap_message.method),
        ?assertEqual(
            <<"Invalid token or clientid in connection mode">>,
            Reply#coap_message.payload
        ),
        ?assertEqual(0, meck:num_calls(emqx_gateway_cm, call, 3))
    after
        ok = meck:unload(emqx_gateway_cm)
    end,
    ok.

t_channel_connection_mode_sock_closed(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    ConnRequired0 = emqx_coap_channel:init(
        ConnInfo,
        #{ctx => coap_ctx(), connection_required => true}
    ),
    {shutdown, ssl_closed, _} = emqx_coap_channel:handle_info(
        {sock_closed, ssl_closed}, ConnRequired0
    ),
    ConnRequired1 = ConnRequired0#channel{conn_state = connected, token = <<"tok">>},
    {ok, _} = emqx_coap_channel:handle_info({sock_closed, ssl_closed}, ConnRequired1),

    ConnOptional = emqx_coap_channel:init(
        ConnInfo,
        #{ctx => coap_ctx(), connection_required => false}
    ),
    {shutdown, ssl_closed, _} = emqx_coap_channel:handle_info(
        {sock_closed, ssl_closed}, ConnOptional
    ),
    ok.

t_channel_takeover_resume_enriches_clientinfo(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    ReqClientId = <<"client1">>,
    ReqToken = <<"tok">>,
    ExpireAt = erlang:system_time(millisecond) + 5000,
    BaseChannel = emqx_coap_channel:init(
        ConnInfo,
        #{ctx => coap_ctx(), connection_required => true}
    ),
    Channel0 = BaseChannel#channel{
        conn_state = connected,
        token = <<"local-token">>,
        clientinfo = (BaseChannel#channel.clientinfo)#{
            clientid => <<"local-client">>,
            username => undefined,
            is_superuser => false,
            auth_expire_at => undefined
        }
    },
    ResumeClientInfo = #{
        clientid => ReqClientId,
        username => <<"admin">>,
        is_superuser => true,
        auth_expire_at => ExpireAt
    },
    Req = #coap_message{
        type = con,
        method = put,
        id = 20,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => ReqClientId, <<"token">> => ReqToken}
        }
    },
    ok = meck:new(emqx_gateway_cm, [no_link, passthrough]),
    ok = meck:new(emqx_gateway_ctx, [no_link, passthrough]),
    ok = meck:expect(
        emqx_gateway_cm,
        call,
        fun
            (GwName, ClientId, {check_token_and_get_clientinfo, Token}) when
                GwName =:= coap, ClientId =:= ReqClientId, Token =:= ReqToken
            ->
                {ok, ResumeClientInfo};
            (GwName, ClientId, Request) ->
                meck:passthrough([GwName, ClientId, Request])
        end
    ),
    ok = meck:expect(
        emqx_gateway_ctx,
        open_session,
        fun
            (
                _Ctx,
                false,
                ClientInfo,
                _ConnInfo0,
                _CreateSessionFun,
                emqx_coap_session
            ) ->
                ?assertEqual(ReqClientId, maps:get(clientid, ClientInfo)),
                ?assertEqual(<<"admin">>, maps:get(username, ClientInfo)),
                ?assertEqual(true, maps:get(is_superuser, ClientInfo)),
                ?assertEqual(ExpireAt, maps:get(auth_expire_at, ClientInfo)),
                {ok, #{session => emqx_coap_session:new(), present => true}};
            (Ctx, CleanStart, ClientInfo, ConnInfo0, CreateSessionFun, SessionMod) ->
                meck:passthrough([
                    Ctx,
                    CleanStart,
                    ClientInfo,
                    ConnInfo0,
                    CreateSessionFun,
                    SessionMod
                ])
        end
    ),
    try
        {ok, [{outgoing, [Reply]}], Channel1} = emqx_coap_channel:handle_in(Req, Channel0),
        ?assertEqual({ok, changed}, Reply#coap_message.method),
        ?assertEqual(connected, Channel1#channel.conn_state),
        ?assertEqual(ReqToken, Channel1#channel.token),
        ?assertEqual(<<"admin">>, maps:get(username, Channel1#channel.clientinfo)),
        ?assertEqual(true, maps:get(is_superuser, Channel1#channel.clientinfo)),
        ?assertEqual(ExpireAt, maps:get(auth_expire_at, Channel1#channel.clientinfo)),
        ?assertEqual(<<"CoAP">>, maps:get(proto_name, Channel1#channel.conninfo)),
        ?assertEqual(<<"1">>, maps:get(proto_ver, Channel1#channel.conninfo)),
        ?assertMatch(#{connection_expire_timer := _}, Channel1#channel.timers)
    after
        ok = meck:unload(emqx_gateway_ctx),
        ok = meck:unload(emqx_gateway_cm)
    end,
    ok.

t_channel_takeover_open_session_fallback_cleanup(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    ReqClientId = <<"client1">>,
    ReqToken = <<"tok">>,
    BaseChannel = emqx_coap_channel:init(
        ConnInfo,
        #{ctx => coap_ctx(), connection_required => true}
    ),
    Channel0 = BaseChannel#channel{
        conn_state = connected,
        token = <<"local-token">>,
        clientinfo = (BaseChannel#channel.clientinfo)#{clientid => <<"local-client">>}
    },
    ResumeClientInfo = #{clientid => ReqClientId, username => <<"admin">>},
    Req = #coap_message{
        type = con,
        method = put,
        id = 21,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => ReqClientId, <<"token">> => ReqToken}
        }
    },
    ok = meck:new(emqx_gateway_cm, [no_link, passthrough]),
    ok = meck:new(emqx_gateway_ctx, [no_link, passthrough]),
    ok = meck:expect(
        emqx_gateway_cm,
        call,
        fun
            (GwName, ClientId, {check_token_and_get_clientinfo, Token}) when
                GwName =:= coap, ClientId =:= ReqClientId, Token =:= ReqToken
            ->
                {ok, ResumeClientInfo};
            (GwName, ClientId, Request) ->
                meck:passthrough([GwName, ClientId, Request])
        end
    ),
    ok = meck:expect(
        emqx_gateway_cm,
        unregister_channel,
        fun
            (GwName, ClientId) when GwName =:= coap, ClientId =:= ReqClientId ->
                self() ! {channel_unregistered, ReqClientId},
                ok;
            (GwName, ClientId) ->
                meck:passthrough([GwName, ClientId])
        end
    ),
    ok = meck:expect(
        emqx_gateway_ctx,
        open_session,
        fun
            (
                _Ctx,
                false,
                _ClientInfo,
                _ConnInfo0,
                _CreateSessionFun,
                emqx_coap_session
            ) ->
                {ok, #{session => emqx_coap_session:new(), present => false}};
            (Ctx, CleanStart, ClientInfo, ConnInfo0, CreateSessionFun, SessionMod) ->
                meck:passthrough([
                    Ctx,
                    CleanStart,
                    ClientInfo,
                    ConnInfo0,
                    CreateSessionFun,
                    SessionMod
                ])
        end
    ),
    try
        {ok, {outgoing, Reply}, _} = emqx_coap_channel:handle_in(Req, Channel0),
        ?assertEqual({error, unauthorized}, Reply#coap_message.method),
        ?assertEqual(
            <<"Invalid token or clientid in connection mode">>,
            Reply#coap_message.payload
        ),
        receive
            {channel_unregistered, ReqClientId} -> ok
        after 1000 ->
            ?assert(false)
        end
    after
        ok = meck:unload(emqx_gateway_ctx),
        ok = meck:unload(emqx_gateway_cm)
    end,
    ok.

t_channel_connected_invalid_queries(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    BaseChannel =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => coap_ctx(),
            connection_required => true
        }),
    Channel1 = BaseChannel#channel{
        conn_state = connected,
        token = <<"tok">>,
        clientinfo = (BaseChannel#channel.clientinfo)#{clientid => <<"client1">>}
    },
    ConnReqDiff = #coap_message{
        type = con,
        method = post,
        id = 11,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => <<"client2">>}
        }
    },
    {ok, [{outgoing, [_]} | _], _} = emqx_coap_channel:handle_in(ConnReqDiff, Channel1),
    ConnReqMissing = #coap_message{
        type = con,
        method = post,
        id = 12,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"username">> => <<"admin">>}
        }
    },
    {ok, [{outgoing, [_]} | _], _} = emqx_coap_channel:handle_in(ConnReqMissing, Channel1),
    CloseReq = #coap_message{
        type = con,
        method = delete,
        id = 13,
        options = #{uri_path => [<<"mqtt">>, <<"connection">>], uri_query => #{}}
    },
    {shutdown, normal, _, _} = emqx_coap_channel:handle_in(
        CloseReq, Channel1#channel{connection_required = false}
    ),
    ok.

t_channel_block2_followup(_) ->
    Channel0 = new_block2_channel(#{max_block_size => 16}),
    Channel1 = Channel0#channel{
        conn_state = connected,
        clientinfo = (Channel0#channel.clientinfo)#{clientid => <<"client1">>}
    },
    Req0 = #coap_message{
        type = con,
        method = post,
        id = 500,
        token = <<"b2tok">>,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => <<"client2">>}
        }
    },
    {ok, [{outgoing, [Reply0]}], Channel2} = emqx_coap_channel:handle_in(Req0, Channel1),
    ?assertEqual({0, true, 16}, emqx_coap_message:get_option(block2, Reply0, undefined)),

    Req1 = Req0#coap_message{
        id = 501,
        options = (Req0#coap_message.options)#{block2 => {1, false, 16}}
    },
    {ok, [{outgoing, [Reply1]}], Channel3} = emqx_coap_channel:handle_in(Req1, Channel2),
    ?assertEqual({1, true, 16}, emqx_coap_message:get_option(block2, Reply1, undefined)),

    Req2 = Req0#coap_message{
        id = 502,
        options = (Req0#coap_message.options)#{block2 => {2, false, 16}}
    },
    {ok, [{outgoing, [Reply2]}], _Channel4} = emqx_coap_channel:handle_in(Req2, Channel3),
    ?assertEqual({2, false, 16}, emqx_coap_message:get_option(block2, Reply2, undefined)),
    ok.

t_channel_query_value_normalization(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    Channel0 = emqx_coap_channel:init(
        ConnInfo,
        #{ctx => coap_ctx(), connection_required => true}
    ),
    Channel1 = Channel0#channel{
        connection_required = true,
        conn_state = connected,
        token = <<"tok">>,
        clientinfo = (Channel0#channel.clientinfo)#{clientid => <<"client1">>}
    },
    Base = #coap_message{
        type = con, method = get, options = #{uri_path => [<<"ps">>, <<"topic">>]}
    },

    ReqMissing = Base#coap_message{
        id = 600,
        options = #{uri_path => [<<"ps">>, <<"topic">>], uri_query => #{<<"token">> => <<"tok">>}}
    },
    {ok, _Replies1, _} = emqx_coap_channel:handle_in(ReqMissing, Channel1),

    ReqList = Base#coap_message{
        id = 601,
        options = #{
            uri_path => [<<"ps">>, <<"topic">>],
            uri_query => #{<<"token">> => "tok", <<"clientid">> => "client1"}
        }
    },
    {ok, _Replies2, _} = emqx_coap_channel:handle_in(ReqList, Channel1),

    ReqInt = Base#coap_message{
        id = 602,
        options = #{
            uri_path => [<<"ps">>, <<"topic">>],
            uri_query => #{<<"token">> => 123, <<"clientid">> => 456}
        }
    },
    {ok, _Replies3, _} = emqx_coap_channel:handle_in(ReqInt, Channel1),

    ReqOther = Base#coap_message{
        id = 603,
        options = #{
            uri_path => [<<"ps">>, <<"topic">>],
            uri_query => #{<<"token">> => 1.2, <<"clientid">> => 3.4}
        }
    },
    {ok, _Replies4, _} = emqx_coap_channel:handle_in(ReqOther, Channel1),
    ok.

t_channel_blockwise_followup_error(_) ->
    Channel0 = new_block2_channel(#{max_block_size => 16}),
    Channel1 = Channel0#channel{
        conn_state = connected,
        clientinfo = (Channel0#channel.clientinfo)#{clientid => <<"client1">>}
    },
    Req0 = #coap_message{
        type = con,
        method = post,
        id = 700,
        token = <<"f1">>,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => <<"client2">>}
        }
    },
    {ok, [{outgoing, [Reply0]}], Channel2} = emqx_coap_channel:handle_in(Req0, Channel1),
    ?assertMatch({0, true, 16}, emqx_coap_message:get_option(block2, Reply0, undefined)),
    FollowBad = Req0#coap_message{
        id = 701,
        options = (Req0#coap_message.options)#{block2 => {1, false, 32}}
    },
    {ok, [{outgoing, [ReplyErr]}], _Channel3} = emqx_coap_channel:handle_in(FollowBad, Channel2),
    ?assertEqual({error, bad_option}, ReplyErr#coap_message.method).

t_channel_blockwise_server_in_error(_) ->
    Channel0 = new_block2_channel(#{max_block_size => 16}),
    ReqBad = #coap_message{
        type = con,
        method = post,
        id = 710,
        token = <<"b1">>,
        payload = <<"x">>,
        options = #{uri_path => [<<"ps">>, <<"topic">>], block1 => {1, false, 16}}
    },
    {ok, [{outgoing, [Reply]}], _Channel1} = emqx_coap_channel:handle_in(ReqBad, Channel0),
    ?assertEqual({error, request_entity_incomplete}, Reply#coap_message.method).

t_channel_block2_reply_error(_) ->
    Channel0 = new_block2_channel(#{max_block_size => 16}),
    Channel1 = Channel0#channel{
        conn_state = connected,
        clientinfo = (Channel0#channel.clientinfo)#{clientid => <<"client1">>}
    },
    Req = #coap_message{
        type = con,
        method = post,
        id = 720,
        token = <<"br">>,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"clientid">> => <<"client2">>},
            block2 => {5, false, 16}
        }
    },
    {ok, [{outgoing, [Reply]}], _Channel2} = emqx_coap_channel:handle_in(Req, Channel1),
    ?assertEqual({error, bad_option}, Reply#coap_message.method).

new_block2_channel(Opts) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    Channel0 =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => coap_ctx(),
            connection_required => false
        }),
    BW = emqx_coap_blockwise:new(
        maps:merge(
            #{
                enable => true,
                max_block_size => 16,
                max_body_size => 4 * 1024 * 1024,
                exchange_lifetime => 247000
            },
            Opts
        )
    ),
    Channel0#channel{connection_required = false, blockwise = BW}.

ps_get_request(Id, Token, ExtraOpts) ->
    #coap_message{
        type = con,
        method = get,
        id = Id,
        token = Token,
        options = maps:merge(#{uri_path => [<<"ps">>, <<"topic">>], uri_query => #{}}, ExtraOpts)
    }.

coap_ctx() ->
    #{
        gwname => coap,
        cm => self(),
        metrics_tab => emqx_gateway_metrics:tabname(coap)
    }.
