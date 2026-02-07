%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_blockwise_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include_lib("eunit/include/eunit.hrl").

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
    Msg1 = Msg0#coap_message{payload = <<" world">>, options = #{uri_path => [<<"ps">>, <<"topic">>], block1 => {1, false, 16}}},
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
    Msg2 = Msg0#coap_message{payload = <<"b">>, options = #{uri_path => [<<"ps">>, <<"topic">>], block1 => {2, false, 16}}},
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
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16, auto_tx_block2 => true}),
    Req =
        (emqx_coap_message:request(con, get, <<>>, [{uri_path, [<<"ps">>, <<"topic">>]}]))#coap_message{
            id = 100,
            token = <<"s1">>,
            options = #{uri_path => [<<"ps">>, <<"topic">>], uri_query => #{}}
        },
    Reply0 = emqx_coap_message:piggyback({ok, content}, binary:copy(<<"A">>, 40), Req),
    {chunked, Reply1, BW1} = emqx_coap_blockwise:server_prepare_out_response(Req, Reply0, {peer, 1}, BW0),
    ?assertEqual({0, true, 16}, emqx_coap_message:get_option(block2, Reply1, undefined)),
    FollowReq1 = Req#coap_message{id = 101, options = (Req#coap_message.options)#{block2 => {1, false, 16}}},
    {reply, Reply2, BW2} = emqx_coap_blockwise:server_followup_in(FollowReq1, {peer, 1}, BW1),
    ?assertEqual({1, true, 16}, emqx_coap_message:get_option(block2, Reply2, undefined)),
    FollowReq2 = Req#coap_message{id = 102, options = (Req#coap_message.options)#{block2 => {2, false, 16}}},
    {reply, Reply3, BW3} = emqx_coap_blockwise:server_followup_in(FollowReq2, {peer, 1}, BW2),
    ?assertEqual({2, false, 16}, emqx_coap_message:get_option(block2, Reply3, undefined)),
    FollowReq3 = Req#coap_message{id = 103, options = (Req#coap_message.options)#{block2 => {3, false, 16}}},
    {pass, _Msg, _BW4} = emqx_coap_blockwise:server_followup_in(FollowReq3, {peer, 1}, BW3).

t_server_tx_block2_observe_overwrite(_) ->
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16, auto_tx_block2 => true}),
    Notify0 = #coap_message{
        type = con,
        id = 200,
        token = <<"ob1">>,
        method = {ok, content},
        payload = <<"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA">>,
        options = #{observe => 10}
    },
    {chunked, _First0, BW1} = emqx_coap_blockwise:server_prepare_out_response(undefined, Notify0, {peer, 9}, BW0),
    Notify1 = Notify0#coap_message{
        id = 201,
        payload = <<"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB">>,
        options = #{observe => 11}
    },
    {chunked, _First1, BW2} = emqx_coap_blockwise:server_prepare_out_response(undefined, Notify1, {peer, 9}, BW1),
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
        max_block_size => 16,
        auto_rx_block2 => true,
        auto_tx_block2 => true
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
    {single, Reply1, _BW3} = emqx_coap_blockwise:server_prepare_out_response(Req, Reply0, {peer, 10}, BW2),
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
    {deliver, InvalidResp, BW2} = emqx_coap_blockwise:client_in_response(CtxInvalid, InvalidResp, BW1),
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
