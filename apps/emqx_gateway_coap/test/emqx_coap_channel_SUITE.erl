%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_channel_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include("emqx_coap_test.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_coap_test_helpers:start_gateway(Config).

end_per_suite(Config) ->
    emqx_coap_test_helpers:stop_gateway(Config).

t_pubsub_handler_direct(_) ->
    Ctx = #{gwname => coap, cm => self()},
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
    Channel0 = emqx_coap_channel:init(ConnInfo, #{ctx => #{gwname => coap, cm => self()}}),
    ChannelRequired =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => #{gwname => coap, cm => self()},
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
    Channel0 = emqx_coap_channel:init(ConnInfo, #{ctx => #{gwname => coap, cm => self()}}),
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
            ctx => #{gwname => coap, cm => self()},
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
    Msg1 = Msg0#coap_message{id = 101, payload = <<"part-b">>, options = (Msg0#coap_message.options)#{block1 => {1, false, 16}}},
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
                ctx => #{gwname => coap, cm => self()},
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
    ok = meck:new(emqx_gateway_ctx, [passthrough]),
    ok = meck:expect(
        emqx_gateway_ctx,
        open_session,
        fun(_, _, _, _, _, _) ->
            {error, session_error}
        end
    ),
    try
        ConnInfo = #{
            peername => {{127, 0, 0, 1}, 9999},
            sockname => {{127, 0, 0, 1}, 5683}
        },
        Channel0 =
            emqx_coap_channel:init(ConnInfo, #{
                ctx => #{gwname => coap, cm => self()},
                connection_required => true
            }),
        ConnReq = #coap_message{
            type = con,
            method = post,
            id = 8,
            options = #{
                uri_path => [<<"mqtt">>, <<"connection">>],
                uri_query => #{<<"clientid">> => <<"client1">>}
            }
        },
        {ok, [{outgoing, [_]}], _} = emqx_coap_channel:handle_in(ConnReq, Channel0)
    after
        ok = meck:unload(emqx_gateway_ctx)
    end,
    ok.

t_channel_check_token_paths(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    Channel0 =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => #{gwname => coap, cm => self()},
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
    {ok, {outgoing, _}, _} = emqx_coap_channel:handle_in(BadReq, Channel0),
    ResetReq = #coap_message{type = reset, id = 999, token = <<>>},
    {ok, _} = emqx_coap_channel:handle_in(ResetReq, Channel0),
    ok.

t_channel_connected_invalid_queries(_) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    BaseChannel =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => #{gwname => coap, cm => self()},
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

t_channel_auto_tx_block2_enabled_followup(_) ->
    ok = meck:new(emqx_coap_pubsub_handler, [passthrough]),
    ok = meck:expect(
        emqx_coap_pubsub_handler,
        handle_request,
        fun(_RestPath, Msg, _Ctx, _CInfo) ->
            case emqx_coap_message:get_option(block2, Msg, undefined) of
                undefined ->
                    Payload = binary:copy(<<"X">>, 40),
                    #{reply => emqx_coap_message:piggyback({ok, content}, Payload, Msg)};
                _ ->
                    erlang:error(unexpected_followup_handler_call)
            end
        end
    ),
    try
        Channel0 = new_block2_channel(#{max_block_size => 16, auto_tx_block2 => true}),
        Req0 = ps_get_request(500, <<"b2tok">>, #{}),
        {ok, [{outgoing, [Reply0]}], Channel1} = emqx_coap_channel:handle_in(Req0, Channel0),
        ?assertEqual({0, true, 16}, emqx_coap_message:get_option(block2, Reply0, undefined)),

        Req1 = ps_get_request(501, <<"b2tok">>, #{block2 => {1, false, 16}}),
        {ok, [{outgoing, [Reply1]}], Channel2} = emqx_coap_channel:handle_in(Req1, Channel1),
        ?assertEqual({1, true, 16}, emqx_coap_message:get_option(block2, Reply1, undefined)),

        Req2 = ps_get_request(502, <<"b2tok">>, #{block2 => {2, false, 16}}),
        {ok, [{outgoing, [Reply2]}], _Channel3} = emqx_coap_channel:handle_in(Req2, Channel2),
        ?assertEqual({2, false, 16}, emqx_coap_message:get_option(block2, Reply2, undefined))
    after
        ok = meck:unload(emqx_coap_pubsub_handler)
    end,
    ok.

t_channel_auto_tx_block2_disabled(_) ->
    ok = meck:new(emqx_coap_pubsub_handler, [passthrough]),
    ok = meck:expect(
        emqx_coap_pubsub_handler,
        handle_request,
        fun(_RestPath, Msg, _Ctx, _CInfo) ->
            Payload = binary:copy(<<"Y">>, 40),
            #{reply => emqx_coap_message:piggyback({ok, content}, Payload, Msg)}
        end
    ),
    try
        Channel0 = new_block2_channel(#{max_block_size => 16, auto_tx_block2 => false}),
        Req0 = ps_get_request(510, <<"b2off">>, #{}),
        {ok, [{outgoing, [Reply0]}], _Channel1} = emqx_coap_channel:handle_in(Req0, Channel0),
        ?assertEqual(undefined, emqx_coap_message:get_option(block2, Reply0, undefined)),
        ?assertEqual(40, byte_size(Reply0#coap_message.payload))
    after
        ok = meck:unload(emqx_coap_pubsub_handler)
    end,
    ok.

new_block2_channel(Opts) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    Channel0 =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => #{gwname => coap, cm => self()},
            connection_required => false
        }),
    BW = emqx_coap_blockwise:new(
        maps:merge(
            #{
                enable => true,
                max_block_size => 16,
                max_body_size => 4 * 1024 * 1024,
                exchange_lifetime => 247000,
                auto_tx_block1 => true,
                auto_rx_block2 => true,
                auto_tx_block2 => true
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
