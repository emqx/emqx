%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_blockwise_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

t_server_inorder_reassemble(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Msg0 =
        (emqx_coap_message:request(con, post, <<"hello">>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            token = <<"t1">>,
            options = #{uri_path => [<<"ps">>, <<"topic">>], block1 => {0, true, 16}}
        },
    {continue, Continue, BW1} = emqx_coap_blockwise:server_in(Msg0, {peer, 1}, BW0),
    ?assertEqual({ok, continue}, Continue#coap_message.method),
    Msg1 = Msg0#coap_message{
        payload = <<" world">>,
        options = #{uri_path => [<<"ps">>, <<"topic">>], block1 => {1, false, 16}}
    },
    {complete, Full, _BW2} = emqx_coap_blockwise:server_in(Msg1, {peer, 1}, BW1),
    ?assertEqual(<<"hello world">>, Full#coap_message.payload),
    ?assertEqual(undefined, emqx_coap_message:get_option(block1, Full, undefined)).

t_server_out_of_order(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Msg0 =
        (emqx_coap_message:request(con, post, <<"a">>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            token = <<"t2">>,
            options = #{uri_path => [<<"ps">>, <<"topic">>], block1 => {0, true, 16}}
        },
    {continue, _, BW1} = emqx_coap_blockwise:server_in(Msg0, {peer, 2}, BW0),
    Msg2 = Msg0#coap_message{
        payload = <<"b">>,
        options = #{uri_path => [<<"ps">>, <<"topic">>], block1 => {2, false, 16}}
    },
    {error, Reply, _BW2} = emqx_coap_blockwise:server_in(Msg2, {peer, 2}, BW1),
    ?assertEqual({error, request_entity_incomplete}, Reply#coap_message.method).

t_client_tx_continue(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Payload = binary:copy(<<"A">>, 40),
    Req =
        (emqx_coap_message:request(con, put, Payload, [{uri_path, [<<"3">>, <<"0">>, <<"1">>]}]))#coap_message{
            token = <<"tx1">>
        },
    Ctx = #{ctx => <<"tx">>},
    {first_block, FirstReq, BW1} = emqx_coap_blockwise:client_prepare_out_request(Ctx, Req, BW0),
    ?assertMatch({0, true, 16}, emqx_coap_message:get_option(block1, FirstReq, undefined)),
    Continue = emqx_coap_message:piggyback({ok, continue}, FirstReq),
    {send_next, NextReq, _BW2} = emqx_coap_blockwise:client_in_response(Ctx, Continue, BW1),
    ?assertMatch({1, _, 16}, emqx_coap_message:get_option(block1, NextReq, undefined)).

t_client_tx_continue_out_of_range(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Payload = binary:copy(<<"B">>, 32),
    Req =
        (emqx_coap_message:request(con, put, Payload, [{uri_path, [<<"3">>, <<"0">>, <<"1">>]}]))#coap_message{
            token = <<"tx2">>
        },
    Ctx = #{ctx => <<"tx2">>},
    {first_block, FirstReq, BW1} = emqx_coap_blockwise:client_prepare_out_request(Ctx, Req, BW0),
    Continue0 = emqx_coap_message:piggyback({ok, continue}, FirstReq),
    {send_next, SecondReq, BW2} = emqx_coap_blockwise:client_in_response(Ctx, Continue0, BW1),
    Continue1 = emqx_coap_message:piggyback({ok, continue}, SecondReq),
    {deliver, Reply, BW3} = emqx_coap_blockwise:client_in_response(Ctx, Continue1, BW2),
    ?assertEqual({error, request_entity_incomplete}, Reply#coap_message.method),
    ?assertEqual(#{}, maps:get(client_tx_block1, BW3)).

t_client_rx_block2_reassemble(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"3">>, <<"0">>, <<"1">>]}]))#coap_message{
            token = <<"rx1">>
        },
    Ctx = #{request => Req},
    Resp0 = #coap_message{
        type = ack,
        method = {ok, content},
        token = <<"rx1">>,
        payload = <<"hello">>,
        options = #{block2 => {0, true, 16}}
    },
    {send_next, NextReq, BW1} = emqx_coap_blockwise:client_in_response(Ctx, Resp0, BW0),
    ?assertEqual({1, false, 16}, emqx_coap_message:get_option(block2, NextReq, undefined)),
    Resp1 = Resp0#coap_message{payload = <<" world">>, options = #{block2 => {1, false, 16}}},
    {deliver, FullResp, _BW2} = emqx_coap_blockwise:client_in_response(Ctx, Resp1, BW1),
    ?assertEqual(<<"hello world">>, FullResp#coap_message.payload),
    ?assertEqual(undefined, emqx_coap_message:get_option(block2, FullResp, undefined)).

t_server_tx_block2_followup(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            id = 100,
            token = <<"s1">>,
            options = #{uri_path => [<<"ps">>, <<"topic">>], uri_query => #{}}
        },
    Reply0 = emqx_coap_message:piggyback({ok, content}, binary:copy(<<"A">>, 40), Req),
    {chunked, Reply1, BW1} = emqx_coap_blockwise:server_prepare_out_response(
        Req, Reply0, {peer, 1}, BW0
    ),
    ?assertEqual({0, true, 16}, emqx_coap_message:get_option(block2, Reply1, undefined)),
    FollowReq1 = Req#coap_message{
        id = 101, options = (Req#coap_message.options)#{block2 => {1, false, 16}}
    },
    {reply, Reply2, BW2} = emqx_coap_blockwise:server_followup_in(FollowReq1, {peer, 1}, BW1),
    ?assertEqual({1, true, 16}, emqx_coap_message:get_option(block2, Reply2, undefined)),
    FollowReq2 = Req#coap_message{
        id = 102, options = (Req#coap_message.options)#{block2 => {2, false, 16}}
    },
    {reply, Reply3, BW3} = emqx_coap_blockwise:server_followup_in(FollowReq2, {peer, 1}, BW2),
    ?assertEqual({2, false, 16}, emqx_coap_message:get_option(block2, Reply3, undefined)),
    FollowReq3 = Req#coap_message{
        id = 103, options = (Req#coap_message.options)#{block2 => {3, false, 16}}
    },
    {pass, _Msg, _BW4} = emqx_coap_blockwise:server_followup_in(FollowReq3, {peer, 1}, BW3).

t_server_tx_block2_observe_overwrite(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Notify0 = #coap_message{
        type = con,
        id = 200,
        token = <<"ob1">>,
        method = {ok, content},
        payload = <<"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA">>,
        options = #{observe => 10}
    },
    {chunked, _First0, BW1} = emqx_coap_blockwise:server_prepare_out_response(
        undefined, Notify0, {peer, 9}, BW0
    ),
    Notify1 = Notify0#coap_message{
        id = 201,
        payload = <<"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB">>,
        options = #{observe => 11}
    },
    {chunked, _First1, BW2} = emqx_coap_blockwise:server_prepare_out_response(
        undefined, Notify1, {peer, 9}, BW1
    ),
    FollowReq = #coap_message{
        type = con,
        method = get,
        id = 202,
        token = <<"ob1">>,
        options = #{uri_path => [<<"ps">>, <<"topic">>], block2 => {1, false, 16}}
    },
    {reply, Reply, _BW3} = emqx_coap_blockwise:server_followup_in(FollowReq, {peer, 9}, BW2),
    ?assertEqual(<<"BBBBBBBBBBBBBBBB">>, Reply#coap_message.payload).

t_enable_false_disables_client_and_server_blockwise(_) ->
    BW0 = emqx_coap_blockwise:new(#{
        enable => false,
        max_block_size => 16
    }),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"3">>, <<"0">>, <<"1">>]}]))#coap_message{
            token = <<"off1">>
        },
    Ctx = #{ctx => <<"disabled">>},
    {single, _Req2, BW1} = emqx_coap_blockwise:client_prepare_out_request(Ctx, Req, BW0),
    Resp0 = #coap_message{
        type = ack,
        method = {ok, content},
        token = <<"off1">>,
        payload = <<"hello">>,
        options = #{block2 => {0, true, 16}}
    },
    {deliver, Resp0, BW2} = emqx_coap_blockwise:client_in_response(Ctx, Resp0, BW1),
    ?assertEqual(#{}, maps:get(client_req, BW2)),
    Reply0 = emqx_coap_message:piggyback({ok, content}, binary:copy(<<"A">>, 40), Req),
    {single, Reply1, _BW3} = emqx_coap_blockwise:server_prepare_out_response(
        Req, Reply0, {peer, 10}, BW2
    ),
    ?assertEqual(undefined, emqx_coap_message:get_option(block2, Reply1, undefined)).

t_block1_then_block2_followup_request_has_empty_payload(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Payload = binary:copy(<<"A">>, 40),
    Req =
        (emqx_coap_message:request(con, put, Payload, [{uri_path, [<<"3">>, <<"0">>, <<"1">>]}]))#coap_message{
            token = <<"mix1">>
        },
    Ctx = #{ctx => <<"mix">>},
    {first_block, FirstReq, BW1} = emqx_coap_blockwise:client_prepare_out_request(Ctx, Req, BW0),
    Continue0 = emqx_coap_message:piggyback({ok, continue}, FirstReq),
    {send_next, SecondReq, BW2} = emqx_coap_blockwise:client_in_response(Ctx, Continue0, BW1),
    Continue1 = emqx_coap_message:piggyback({ok, continue}, SecondReq),
    {send_next, ThirdReq, BW3} = emqx_coap_blockwise:client_in_response(Ctx, Continue1, BW2),
    ?assertMatch({2, false, 16}, emqx_coap_message:get_option(block1, ThirdReq, undefined)),
    Resp0 = emqx_coap_message:piggyback({ok, content}, <<"resp-part-1">>, ThirdReq),
    Resp1 = Resp0#coap_message{options = #{block2 => {0, true, 16}}},
    {send_next, NextReq, _BW4} = emqx_coap_blockwise:client_in_response(Ctx, Resp1, BW3),
    ?assertEqual(<<>>, NextReq#coap_message.payload),
    ?assertEqual(undefined, emqx_coap_message:get_option(block1, NextReq, undefined)),
    ?assertEqual({1, false, 16}, emqx_coap_message:get_option(block2, NextReq, undefined)).

t_client_rx_invalid_or_mismatched_block2_clears_client_request(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"3">>, <<"0">>, <<"1">>]}]))#coap_message{
            token = <<"rx2">>
        },
    CtxInvalid = #{ctx => <<"invalid">>},
    {single, _Req1, BW1} = emqx_coap_blockwise:client_prepare_out_request(CtxInvalid, Req, BW0),
    InvalidResp = #coap_message{
        type = ack,
        method = {ok, content},
        token = <<"rx2">>,
        payload = <<"chunk">>,
        options = #{block2 => {0, true, 2048}}
    },
    {deliver, InvalidResp, BW2} = emqx_coap_blockwise:client_in_response(
        CtxInvalid, InvalidResp, BW1
    ),
    ?assertEqual(#{}, maps:get(client_req, BW2)),
    CtxMismatch = #{ctx => <<"mismatch">>},
    {single, _Req2, BW3} = emqx_coap_blockwise:client_prepare_out_request(CtxMismatch, Req, BW2),
    Resp0 = #coap_message{
        type = ack,
        method = {ok, content},
        token = <<"rx2">>,
        payload = <<"hello">>,
        options = #{block2 => {0, true, 16}}
    },
    {send_next, _NextReq, BW4} = emqx_coap_blockwise:client_in_response(CtxMismatch, Resp0, BW3),
    RespBad = Resp0#coap_message{payload = <<"world">>, options = #{block2 => {2, false, 16}}},
    {deliver, RespBad, BW5} = emqx_coap_blockwise:client_in_response(CtxMismatch, RespBad, BW4),
    ?assertEqual(#{}, maps:get(client_req, BW5)),
    ?assertEqual(#{}, maps:get(client_rx_block2, BW5)).

t_blockwise_opts_and_helpers(_) ->
    BW0 = emqx_coap_blockwise:new(#{
        enable => undefined,
        max_block_size => 15,
        max_body_size => 32,
        exchange_lifetime => 1000
    }),
    ?assert(emqx_coap_blockwise:enabled(BW0)),
    ?assertEqual(1024, emqx_coap_blockwise:blockwise_size(BW0)),
    ?assertEqual(32, emqx_coap_blockwise:max_body_size(BW0)),
    ?assertEqual(1000, maps:get(exchange_lifetime, maps:get(opts, BW0))),
    BW1 = emqx_coap_blockwise:new(#{
        max_block_size => <<"bad">>,
        max_body_size => <<"bad">>,
        exchange_lifetime => <<"bad">>
    }),
    ?assertEqual(1024, emqx_coap_blockwise:blockwise_size(BW1)),
    ?assertEqual(4 * 1024 * 1024, emqx_coap_blockwise:max_body_size(BW1)),
    ?assertEqual(247000, maps:get(exchange_lifetime, maps:get(opts, BW1))),
    BW2 = emqx_coap_blockwise:new(#{
        max_body_size => bad,
        exchange_lifetime => bad
    }),
    ?assertEqual(4 * 1024 * 1024, emqx_coap_blockwise:max_body_size(BW2)),
    ?assertEqual(247000, maps:get(exchange_lifetime, maps:get(opts, BW2))),
    ?assertEqual(false, emqx_coap_blockwise:has_active_client_tx(<<"raw">>, BW2)),
    ok.

t_blockwise_server_in_invalids(_) ->
    BaseOpts = #{uri_path => [<<"ps">>, <<"topic">>]},
    Msg0 =
        (emqx_coap_message:request(con, post, <<"a">>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            token = <<"s1">>,
            options = BaseOpts
        },
    BWDisabled = emqx_coap_blockwise:new(#{enable => false, max_block_size => 16}),
    {pass, _Msg0, _BW0} = emqx_coap_blockwise:server_in(Msg0, {peer, 1}, BWDisabled),
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    {pass, _Msg1, _BW1} = emqx_coap_blockwise:server_in(Msg0, {peer, 1}, BW0),
    MsgBadBlock = Msg0#coap_message{options = BaseOpts#{block1 => <<"bad">>}},
    {error, ReplyBad, _BW2} = emqx_coap_blockwise:server_in(MsgBadBlock, {peer, 1}, BW0),
    ?assertEqual({error, bad_option}, ReplyBad#coap_message.method),
    MsgBadSize = Msg0#coap_message{options = BaseOpts#{block1 => {0, true, 7}}},
    {error, ReplySize, _BW3} = emqx_coap_blockwise:server_in(MsgBadSize, {peer, 1}, BW0),
    ?assertEqual({error, bad_option}, ReplySize#coap_message.method),
    ok.

t_blockwise_server_in_sequences(_) ->
    BaseOpts = #{uri_path => [<<"ps">>, <<"topic">>]},
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16, max_body_size => 7}),
    Msg0 =
        (emqx_coap_message:request(con, post, <<"1234">>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            token = <<"s2">>,
            options = BaseOpts#{block1 => {0, true, 16}}
        },
    {continue, _, BW1} = emqx_coap_blockwise:server_in(Msg0, {peer, 2}, BW0),
    Msg1 = Msg0#coap_message{payload = <<"5678">>, options = BaseOpts#{block1 => {1, true, 16}}},
    {error, ReplyTooLarge, _BW2} = emqx_coap_blockwise:server_in(Msg1, {peer, 2}, BW1),
    ?assertEqual({error, request_entity_too_large}, ReplyTooLarge#coap_message.method),
    BW2 = emqx_coap_blockwise:new(#{max_block_size => 16, max_body_size => 100}),
    MsgComplete = Msg0#coap_message{
        payload = <<"ok">>, options = BaseOpts#{block1 => {0, false, 16}}
    },
    {complete, Full, _BW3} = emqx_coap_blockwise:server_in(MsgComplete, {peer, 2}, BW2),
    ?assertEqual(undefined, emqx_coap_message:get_option(block1, Full, undefined)),
    MsgMissing = Msg0#coap_message{
        payload = <<"x">>, options = BaseOpts#{block1 => {1, false, 16}}
    },
    {error, ReplyMissing, _BW4} = emqx_coap_blockwise:server_in(MsgMissing, {peer, 2}, BW2),
    ?assertEqual({error, request_entity_incomplete}, ReplyMissing#coap_message.method),
    MsgA = Msg0#coap_message{payload = <<"aa">>, options = BaseOpts#{block1 => {0, true, 16}}},
    {continue, ContA, BW3} = emqx_coap_blockwise:server_in(MsgA, {peer, 2}, BW2),
    ?assertEqual({ok, continue}, ContA#coap_message.method),
    MsgB = Msg0#coap_message{payload = <<"bb">>, options = BaseOpts#{block1 => {1, true, 16}}},
    {continue, ContB, _BW5} = emqx_coap_blockwise:server_in(MsgB, {peer, 2}, BW3),
    ?assertEqual({ok, continue}, ContB#coap_message.method),
    ok.

t_blockwise_server_followup_variants(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    {pass, _Msg0, _BW1} = emqx_coap_blockwise:server_followup_in(<<"bad">>, {peer, 3}, BW0),
    BadTuple = #coap_message{options = #{block2 => <<"bad">>}},
    {error, ReplyBadTuple, _BW2} = emqx_coap_blockwise:server_followup_in(BadTuple, {peer, 3}, BW0),
    ?assertEqual({error, bad_option}, ReplyBadTuple#coap_message.method),
    BadSize = #coap_message{options = #{block2 => {0, false, 32}}},
    {error, ReplyBadSize, _BW3} = emqx_coap_blockwise:server_followup_in(BadSize, {peer, 3}, BW0),
    ?assertEqual({error, bad_option}, ReplyBadSize#coap_message.method),
    NoToken = #coap_message{token = <<>>, options = #{block2 => {0, false, 16}}},
    {pass, _Msg1, _BW4} = emqx_coap_blockwise:server_followup_in(NoToken, {peer, 3}, BW0),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            id = 300,
            token = <<"t3">>,
            options = #{uri_path => [<<"ps">>, <<"topic">>], uri_query => #{}}
        },
    Reply0 = emqx_coap_message:piggyback({ok, content}, binary:copy(<<"A">>, 40), Req),
    {chunked, _Reply1, BW5} = emqx_coap_blockwise:server_prepare_out_response(
        Req, Reply0, {peer, 3}, BW0
    ),
    FollowMismatch = Req#coap_message{
        options = (Req#coap_message.options)#{block2 => {1, false, 32}}
    },
    {error, _ReplyMismatch, _BW6} = emqx_coap_blockwise:server_followup_in(
        FollowMismatch, {peer, 3}, BW5
    ),
    {chunked, _Reply2, BW7} = emqx_coap_blockwise:server_prepare_out_response(
        Req, Reply0, {peer, 3}, BW0
    ),
    FollowOut = Req#coap_message{options = (Req#coap_message.options)#{block2 => {99, false, 16}}},
    {error, _ReplyOut, _BW8} = emqx_coap_blockwise:server_followup_in(FollowOut, {peer, 3}, BW7),
    ok.

t_blockwise_server_prepare_out_response_misc(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    {single, <<"bad">>, _BW1} =
        emqx_coap_blockwise:server_prepare_out_response(undefined, <<"bad">>, {peer, 4}, BW0),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            token = <<"t4">>,
            options = #{uri_path => [<<"ps">>, <<"topic">>], uri_query => #{}}
        },
    ReplyAtom = #coap_message{method = get, payload = <<"aaa">>, options = #{}},
    {single, _ReplyAtom, _BW2} =
        emqx_coap_blockwise:server_prepare_out_response(Req, ReplyAtom, {peer, 4}, BW0),
    ReqBadSize = Req#coap_message{options = (Req#coap_message.options)#{block2 => {0, false, 3}}},
    ReplyOk = #coap_message{method = {ok, content}, payload = <<"aaa">>, options = #{}},
    {single, _ReplyOk, _BW3} =
        emqx_coap_blockwise:server_prepare_out_response(ReqBadSize, ReplyOk, {peer, 4}, BW0),
    ReplyNonBin = #coap_message{method = {ok, content}, payload = 1, options = #{}},
    {single, _ReplyNonBin, _BW4} =
        emqx_coap_blockwise:server_prepare_out_response(Req, ReplyNonBin, {peer, 4}, BW0),
    ReqOutRange = Req#coap_message{options = (Req#coap_message.options)#{block2 => {5, false, 16}}},
    {error, _ReplyErr, _BW5} =
        emqx_coap_blockwise:server_prepare_out_response(ReqOutRange, ReplyOk, {peer, 4}, BW0),
    ReplyNoToken = ReplyOk#coap_message{token = <<>>, payload = binary:copy(<<"B">>, 40)},
    {chunked, _ReplyChunk, _BW6} =
        emqx_coap_blockwise:server_prepare_out_response(undefined, ReplyNoToken, {peer, 4}, BW0),
    ok.

t_blockwise_client_in_response_branches(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Ctx = #{ctx => <<"rx">>},
    Resp0 = #coap_message{
        type = ack,
        method = {ok, content},
        token = <<"r1">>,
        payload = <<"hello">>,
        options = #{block2 => {0, true, 16}}
    },
    {send_next, _NextReq0, _BW1} = emqx_coap_blockwise:client_in_response(Ctx, Resp0, BW0),
    BW1 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    BadResp = Resp0#coap_message{options = #{block2 => <<"bad">>}},
    {deliver, _Resp2, _BW2} = emqx_coap_blockwise:client_in_response(Ctx, BadResp, BW1),
    BW2 = emqx_coap_blockwise:new(#{max_block_size => 16, max_body_size => 1}),
    BigResp = Resp0#coap_message{payload = <<"too-big">>},
    {deliver, _Resp3, _BW3} = emqx_coap_blockwise:client_in_response(Ctx, BigResp, BW2),
    BW3 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    DoneResp = Resp0#coap_message{options = #{block2 => {0, false, 16}}},
    {deliver, DoneResp2, _BW4} = emqx_coap_blockwise:client_in_response(Ctx, DoneResp, BW3),
    ?assertEqual(undefined, emqx_coap_message:get_option(block2, DoneResp2, undefined)),
    BW4 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    SkipResp = Resp0#coap_message{options = #{block2 => {1, false, 16}}},
    {deliver, _Resp4, _BW5} = emqx_coap_blockwise:client_in_response(Ctx, SkipResp, BW4),
    BW5 = emqx_coap_blockwise:new(#{max_block_size => 16, max_body_size => 5}),
    StartResp = Resp0#coap_message{payload = <<"1234">>, options = #{block2 => {0, true, 16}}},
    {send_next, NextReq, BW6} = emqx_coap_blockwise:client_in_response(Ctx, StartResp, BW5),
    ?assertMatch(#coap_message{}, NextReq),
    NextResp = Resp0#coap_message{payload = <<"5678">>, options = #{block2 => {1, true, 16}}},
    {deliver, _Resp5, _BW7} = emqx_coap_blockwise:client_in_response(Ctx, NextResp, BW6),
    ok.

t_blockwise_followup_size_mismatch(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 32}),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            id = 200,
            token = <<"sm1">>,
            options = #{
                uri_path => [<<"ps">>, <<"topic">>], uri_query => #{}, block2 => {0, false, 16}
            }
        },
    Reply0 = emqx_coap_message:piggyback({ok, content}, binary:copy(<<"A">>, 40), Req),
    {chunked, _Reply1, BW1} = emqx_coap_blockwise:server_prepare_out_response(
        Req, Reply0, {peer, 9}, BW0
    ),
    FollowBad = Req#coap_message{
        id = 201,
        options = (Req#coap_message.options)#{block2 => {1, false, 32}}
    },
    {error, ReplyErr, _BW2} = emqx_coap_blockwise:server_followup_in(FollowBad, {peer, 9}, BW1),
    ?assertEqual({error, bad_option}, ReplyErr#coap_message.method).

t_blockwise_invalid_block2_request(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            id = 210,
            token = <<"ib2">>,
            options = #{uri_path => [<<"ps">>, <<"topic">>], block2 => {0, true, 15}}
        },
    Reply0 = emqx_coap_message:piggyback({ok, content}, <<"hi">>, Req),
    {single, _Reply1, _BW1} = emqx_coap_blockwise:server_prepare_out_response(
        Req, Reply0, {peer, 10}, BW0
    ),
    ok.

t_blockwise_invalid_block2_option_type(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            id = 211,
            token = <<"ib2t">>,
            options = #{uri_path => [<<"ps">>, <<"topic">>], block2 => <<"bad">>}
        },
    Reply0 = emqx_coap_message:piggyback({ok, content}, <<"hi">>, Req),
    {single, _Reply1, _BW1} = emqx_coap_blockwise:server_prepare_out_response(
        Req, Reply0, {peer, 10}, BW0
    ),
    ok.

t_blockwise_has_block2_option_true(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            id = 220,
            token = <<"hb2">>,
            options = #{uri_path => [<<"ps">>, <<"topic">>], block2 => {0, false, 16}}
        },
    Reply0 = emqx_coap_message:piggyback({ok, content}, <<"short">>, Req),
    {chunked, Reply1, _BW1} = emqx_coap_blockwise:server_prepare_out_response(
        Req, Reply0, {peer, 11}, BW0
    ),
    ?assertEqual({0, false, 16}, emqx_coap_message:get_option(block2, Reply1, undefined)).

t_blockwise_has_block2_option_false(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Reply0 = #coap_message{
        type = ack,
        method = {ok, content},
        payload = <<"short">>,
        options = #{}
    },
    {single, _Reply1, _BW1} = emqx_coap_blockwise:server_prepare_out_response(
        undefined, Reply0, {peer, 11}, BW0
    ),
    ok.

t_blockwise_normalize_binary_opts(_) ->
    BW0 = emqx_coap_blockwise:new(#{
        max_body_size => <<"1KB">>,
        exchange_lifetime => <<"10s">>
    }),
    ?assertEqual(4 * 1024 * 1024, emqx_coap_blockwise:max_body_size(BW0)),
    ?assertEqual(247000, maps:get(exchange_lifetime, maps:get(opts, BW0))),
    ok.

t_blockwise_server_block1_too_large(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16, max_body_size => 4}),
    Msg0 =
        (emqx_coap_message:request(con, post, <<"12345">>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            token = <<"b1big">>,
            options = #{uri_path => [<<"ps">>, <<"topic">>], block1 => {0, true, 16}}
        },
    {error, Reply, _BW1} = emqx_coap_blockwise:server_in(Msg0, {peer, 12}, BW0),
    ?assertEqual({error, request_entity_too_large}, Reply#coap_message.method).

t_blockwise_block2_token_empty(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"3">>, <<"0">>, <<"1">>]}]))#coap_message{
            token = <<"reqtok">>
        },
    Ctx = #{request => Req},
    Resp0 = #coap_message{
        type = ack,
        method = {ok, content},
        token = <<>>,
        payload = <<"hello">>,
        options = #{block2 => {0, true, 16}}
    },
    {send_next, NextReq, _BW1} = emqx_coap_blockwise:client_in_response(Ctx, Resp0, BW0),
    ?assertEqual(<<"reqtok">>, NextReq#coap_message.token).
