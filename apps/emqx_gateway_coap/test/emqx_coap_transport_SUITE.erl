%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_transport_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include("emqx_coap_test.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_transport_paths(_) ->
    Msg = #coap_message{type = non, method = undefined, id = 1},
    _ = emqx_coap_transport:idle(in, Msg, emqx_coap_transport:new()),
    Msg2 = #coap_message{type = con, method = undefined, id = 2},
    _ = emqx_coap_transport:idle(in, Msg2, emqx_coap_transport:new()),
    Msg3 = #coap_message{type = non, method = get, id = 3},
    _ = emqx_coap_transport:idle(out, Msg3, emqx_coap_transport:new()),
    Cache = #coap_message{type = con, method = get, id = 4},
    Transport0 = #transport{cache = Cache, retry_interval = 1, retry_count = 0},
    _ = emqx_coap_transport:maybe_resend_4request(in, Cache, Transport0),
    _ = emqx_coap_transport:maybe_resend_4response(in, Cache, Transport0),
    _ = emqx_coap_transport:maybe_resend_4request(in, Msg3, #transport{cache = undefined}),
    _ = emqx_coap_transport:maybe_resend_4response(in, Msg3, #transport{cache = undefined}),
    _ = emqx_coap_transport:maybe_reset(in, #coap_message{type = reset, id = 5}, Transport0),
    _ = emqx_coap_transport:maybe_reset(
        in, #coap_message{type = con, method = {ok, content}, id = 6}, Transport0
    ),
    _ = emqx_coap_transport:maybe_reset(
        in, #coap_message{type = con, method = get, id = 7}, Transport0
    ),
    _ = emqx_coap_transport:maybe_reset(
        in, #coap_message{type = non, method = {ok, content}, id = 7}, Transport0
    ),
    _ = emqx_coap_transport:maybe_reset(
        in, #coap_message{type = ack, method = {ok, content}, id = 7}, Transport0
    ),
    _ = emqx_coap_transport:wait_ack(
        in, #coap_message{type = reset, id = 8}, Transport0
    ),
    _ = emqx_coap_transport:wait_ack(
        in, #coap_message{type = con, method = undefined, id = 9}, Transport0
    ),
    _ = emqx_coap_transport:wait_ack(
        in, #coap_message{type = con, method = {ok, content}, id = 10}, Transport0
    ),
    _ = emqx_coap_transport:wait_ack(
        in, #coap_message{type = con, method = get, id = 11}, Transport0
    ),
    _ = emqx_coap_transport:wait_ack(
        state_timeout, ack_timeout, #transport{cache = Cache, retry_interval = 1, retry_count = 0}
    ),
    _ = emqx_coap_transport:wait_ack(
        state_timeout, ack_timeout, #transport{cache = Cache, retry_interval = 1, retry_count = 4}
    ),
    ObserveMsg = #coap_message{
        type = con, method = {ok, content}, id = 12, options = #{observe => 1}
    },
    _ = emqx_coap_transport:observe(in, ObserveMsg, #transport{observe = 1}),
    _ = emqx_coap_transport:observe(in, ObserveMsg, #transport{observe = 0}),
    _ = emqx_coap_transport:observe(in, #coap_message{method = {error, bad_request}}, Transport0),
    _ = emqx_coap_transport:observe(in, #coap_message{method = get}, Transport0),
    _ = emqx_coap_transport:maybe_resend_4request(
        in, #coap_message{type = con, method = get}, Transport0
    ),
    _ = emqx_coap_transport:maybe_resend_4response(
        in, #coap_message{type = con, method = get}, Transport0
    ),
    _ = emqx_coap_transport:until_stop(in, Msg3, Transport0),
    ok.

t_transport_retry_interval_initial(_) ->
    Msg = #coap_message{type = con, method = get, id = 200},
    #{transport := Transport, timeouts := Timeouts} =
        emqx_coap_transport:idle(out, Msg, emqx_coap_transport:new()),
    {state_timeout, Timeout, ack_timeout} = lists:keyfind(state_timeout, 1, Timeouts),
    ?assertEqual(Timeout, Transport#transport.retry_interval),
    ok.

t_transport_observe_retransmit_update(_) ->
    Msg = #coap_message{
        type = con,
        method = {ok, content},
        id = 201,
        options = #{observe => 1}
    },
    Transport = #transport{cache = Msg, retry_interval = 1, retry_count = 0},
    Lower = emqx_coap_observe_res:current_value(),
    #{out := [OutMsg]} = emqx_coap_transport:wait_ack(state_timeout, ack_timeout, Transport),
    Upper = emqx_coap_observe_res:current_value(),
    Observe = emqx_coap_message:get_option(observe, OutMsg),
    case Lower =< Upper of
        true -> ?assert(Observe >= Lower andalso Observe =< Upper);
        false -> ?assert(Observe >= Lower orelse Observe =< Upper)
    end,
    ok.

t_transport_observe_retransmit_request_no_update(_) ->
    Msg = #coap_message{
        type = con,
        method = get,
        id = 202,
        options = #{observe => 1}
    },
    Transport = #transport{cache = Msg, retry_interval = 1, retry_count = 0},
    #{out := [OutMsg]} = emqx_coap_transport:wait_ack(state_timeout, ack_timeout, Transport),
    ?assertEqual(1, emqx_coap_message:get_option(observe, OutMsg)),
    ok.

t_transport_observe_retransmit_response_no_observe(_) ->
    Msg = #coap_message{
        type = con,
        method = {ok, content},
        id = 203,
        token = <<"rtok">>
    },
    Transport = #transport{cache = Msg, retry_interval = 1, retry_count = 0},
    #{out := [OutMsg]} = emqx_coap_transport:wait_ack(state_timeout, ack_timeout, Transport),
    ?assertEqual(undefined, emqx_coap_message:get_option(observe, OutMsg, undefined)),
    ok.

t_tm_paths(_) ->
    TM0 = emqx_coap_tm:new(),
    Msg = #coap_message{type = con, method = get, token = <<"tok">>, id = 1},
    #{tm := TM1} = emqx_coap_tm:handle_out(Msg, TM0),
    ?assertEqual(#{}, emqx_coap_tm:handle_out(Msg, TM1)),
    ?assertEqual(TM0, emqx_coap_tm:set_reply(#coap_message{id = 999}, TM0)),
    ObserveReqOk = #coap_message{
        type = con, method = get, token = <<"obs1">>, options = #{observe => 0}
    },
    #{out := [ObserveOutOk], tm := TMObsOk} = emqx_coap_tm:handle_out(ObserveReqOk, TM0),
    _ = emqx_coap_tm:handle_response(
        #coap_message{
            type = ack,
            method = {ok, content},
            id = ObserveOutOk#coap_message.id,
            token = <<"obs1">>
        },
        TMObsOk
    ),
    ObserveReqErr = #coap_message{
        type = con, method = get, token = <<"obs2">>, options = #{observe => 0}
    },
    #{out := [ObserveOutErr], tm := TMObsErr} = emqx_coap_tm:handle_out(ObserveReqErr, TM0),
    _ = emqx_coap_tm:handle_response(
        #coap_message{
            type = ack,
            method = {error, bad_request},
            id = ObserveOutErr#coap_message.id,
            token = <<"obs2">>
        },
        TMObsErr
    ),
    TokenKey = {token, <<"tok">>},
    SeqId = maps:get(TokenKey, TM1),
    Machine = maps:get(SeqId, TM1),
    TMNoTimers = TM1#{SeqId => Machine#state_machine{timers = #{}}},
    ?assertEqual(#{}, emqx_coap_tm:timeout({SeqId, state_timeout, ack_timeout}, TMNoTimers)),
    TMStop = TM1#{SeqId => Machine#state_machine{timers = #{stop_timeout => make_ref()}}},
    _ = emqx_coap_tm:timeout({SeqId, stop_timeout, stop}, TMStop),
    SeqId = 1,
    _ = emqx_coap_tm:timeout({SeqId, state_timeout, ack_timeout}, TM1),
    _ = emqx_coap_tm:timeout({999, state_timeout, ack_timeout}, TM1),
    TMConflict = maps:remove(maps:get(TokenKey, TM1), TM1),
    ?assertThrow("token conflict", emqx_coap_tm:handle_out(Msg, TMConflict)),
    Empty = emqx_coap_tm:handle_response(#coap_message{type = reset, id = 99, token = <<>>}, TM0),
    ?assertEqual(#{}, Empty),
    #{out := _} =
        emqx_coap_tm:handle_response(#coap_message{type = ack, id = 98, token = <<>>}, TM0),
    _ = emqx_coap_tm:handle_out(
        #coap_message{type = non, method = get, token = <<"tok2">>},
        TM0
    ),
    NextMsgId = maps:get(next_msg_id, TM0),
    TMUsed = TM0#{{out, NextMsgId} => 1},
    _ = emqx_coap_tm:handle_out(
        #coap_message{type = con, method = get, token = <<"tok3">>},
        TMUsed
    ),
    TMMax = TM0#{next_msg_id => ?MAX_MESSAGE_ID},
    #{tm := TMMax1} = emqx_coap_tm:handle_out(
        #coap_message{type = con, method = get, token = <<"tok4">>},
        TMMax
    ),
    ?assertEqual(1, maps:get(next_msg_id, TMMax1)),
    ok.

t_tm_cancel_state_timer(_) ->
    TM0 = emqx_coap_tm:new(),
    Msg = #coap_message{type = con, method = get, token = <<"ctok">>, id = 10},
    #{tm := TM1} = emqx_coap_tm:handle_out(Msg, TM0),
    TokenKey = {token, <<"ctok">>},
    SeqId = maps:get(TokenKey, TM1),
    Machine = maps:get(SeqId, TM1),
    Timers = (Machine#state_machine.timers)#{state_timer => make_ref()},
    TM2 = TM1#{SeqId => Machine#state_machine{timers = Timers}},
    Resp = #coap_message{
        type = ack,
        method = {ok, content},
        id = Msg#coap_message.id,
        token = <<"ctok">>
    },
    Result = emqx_coap_tm:handle_response(Resp, TM2),
    TM3 = maps:get(tm, Result, TM2),
    case maps:get(SeqId, TM3, undefined) of
        undefined ->
            ok;
        Machine2 ->
            ?assertEqual(false, maps:is_key(state_timer, Machine2#state_machine.timers))
    end,
    ok.

t_tm_observe_delete_token(_) ->
    TM0 = emqx_coap_tm:new(),
    Msg = #coap_message{
        type = con, method = get, token = <<"obsdel">>, id = 20, options = #{observe => 1}
    },
    #{tm := TM1} = emqx_coap_tm:handle_out(Msg, TM0),
    ?assertEqual(false, maps:is_key({token, <<"obsdel">>}, TM1)),
    ok.

t_tm_cancel_state_timer_manual(_) ->
    TM0 = emqx_coap_tm:new(),
    MsgId = 77,
    Machine = #state_machine{
        seq_id = 1,
        id = {in, MsgId},
        state = idle,
        timers = #{state_timer => make_ref()},
        transport = emqx_coap_transport:new()
    },
    TM1 = TM0#{seq_id := 2, 1 => Machine, {in, MsgId} => 1},
    Msg = #coap_message{type = con, method = get, id = MsgId},
    Result = emqx_coap_tm:handle_request(Msg, TM1),
    TM2 = maps:get(tm, Result, TM1),
    Machine2 = maps:get(1, TM2),
    ?assertEqual(false, maps:is_key(state_timer, Machine2#state_machine.timers)),
    ok.

t_tm_timer_cleanup(_) ->
    TM0 = emqx_coap_tm:new(),
    Msg = #coap_message{type = con, method = get, token = <<"tok">>, id = 1},
    #{tm := TM1} = emqx_coap_tm:handle_out(Msg, TM0),
    TokenKey = {token, <<"tok">>},
    SeqId = maps:get(TokenKey, TM1),
    Machine = maps:get(SeqId, TM1),
    Timers = #{state_timeout => make_ref(), state_timer => make_ref()},
    TMWithTimer = TM1#{SeqId => Machine#state_machine{state = wait_ack, timers = Timers}},
    _ = emqx_coap_tm:timeout({SeqId, state_timeout, ack_timeout}, TMWithTimer),
    ok.
