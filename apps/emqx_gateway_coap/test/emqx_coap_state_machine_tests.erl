%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_state_machine_tests).

-include("../include/emqx_coap.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(channel, {
    ctx,
    conninfo,
    clientinfo,
    session,
    keepalive,
    timers,
    connection_required,
    conn_state,
    token
}).

-record(transport, {
    cache,
    req_context,
    retry_interval,
    retry_count,
    observe
}).

retry_interval_and_exhaustion_test() ->
    Msg = #coap_message{
        type = con,
        method = {ok, content},
        id = 100,
        token = <<"token">>,
        payload = <<"payload">>
    },
    Transport0 = emqx_coap_transport:new(),
    #{
        transport := Transport1,
        timeouts := [
            {state_timeout, InitialTimeout, ack_timeout},
            {stop_timeout, 247000}
        ]
    } = emqx_coap_transport:idle(out, Msg, Transport0),
    ?assert(InitialTimeout >= 2001),
    ?assert(InitialTimeout =< 3000),
    ?assertEqual(InitialTimeout, Transport1#transport.retry_interval),
    ?assertEqual(0, Transport1#transport.retry_count),

    #{
        out := [Msg],
        transport := Transport2,
        timeouts := [{state_timeout, NextTimeout, ack_timeout}]
    } = emqx_coap_transport:wait_ack(
        state_timeout,
        ack_timeout,
        Transport1
    ),
    ?assertEqual(InitialTimeout * 2, NextTimeout),
    ?assertEqual(NextTimeout, Transport2#transport.retry_interval),
    ?assertEqual(1, Transport2#transport.retry_count),

    Exhausted = Transport1#transport{retry_count = 4},
    #{next := stop, proto := {ack_failure, Msg}} =
        emqx_coap_transport:wait_ack(
            state_timeout,
            ack_timeout,
            Exhausted
        ).

empty_ack_completes_server_response_test() ->
    Ack = #coap_message{
        type = ack,
        method = undefined,
        id = 100,
        token = <<"token">>
    },
    Response = #coap_message{
        type = con,
        method = {ok, content},
        id = 100,
        token = <<"token">>
    },
    Request = Response#coap_message{method = get},
    ResponseTransport = (emqx_coap_transport:new())#transport{cache = Response},
    RequestTransport = ResponseTransport#transport{cache = Request},
    ResponseResult = emqx_coap_transport:wait_ack(in, Ack, ResponseTransport),
    ?assertEqual(stop, maps:get(next, ResponseResult, undefined)),
    RequestResult = emqx_coap_transport:wait_ack(in, Ack, RequestTransport),
    ?assertEqual(false, maps:is_key(next, RequestResult)).

state_machine_timeout_retransmits_test() ->
    {Original, Channel0} = new_response_channel(),
    receive
        {
            timeout,
            TRef,
            {state_machine, {SeqId, state_timeout, ack_timeout}} = Payload
        } ->
            {ok, [{outgoing, [Retry]}], Channel1} =
                emqx_coap_channel:handle_timeout(TRef, Payload, Channel0),
            ?assertEqual(Original#coap_message.id, Retry#coap_message.id),
            ?assertEqual(Original#coap_message.token, Retry#coap_message.token),
            ?assertEqual(Original#coap_message.payload, Retry#coap_message.payload),
            Ack = #coap_message{
                type = ack,
                method = undefined,
                id = Retry#coap_message.id,
                token = Retry#coap_message.token
            },
            #{session := _} =
                emqx_coap_session:handle_response(
                    Ack,
                    Channel1#channel.session
                ),
            ?assert(is_integer(SeqId))
    after 5000 ->
        ?assert(false)
    end.

state_machine_stop_timeout_is_forwarded_test() ->
    Session = session_marker,
    Channel =
        {channel, #{}, #{}, #{}, Session, undefined, #{}, false, connected, undefined},
    TimerMsg = {1, stop_timeout, stop},
    ok = meck:new(emqx_coap_session),
    try
        ok = meck:expect(
            emqx_coap_session,
            timeout,
            fun(Received, ReceivedSession) ->
                ?assertEqual(TimerMsg, Received),
                ?assertEqual(Session, ReceivedSession),
                #{session => ReceivedSession}
            end
        ),
        TRef = emqx_utils:start_timer(0, {state_machine, TimerMsg}),
        receive
            {timeout, TRef, Payload} ->
                {ok, _} = emqx_coap_channel:handle_timeout(TRef, Payload, Channel)
        after 1000 ->
            ?assert(false)
        end,
        ?assert(meck:called(emqx_coap_session, timeout, [TimerMsg, Session]))
    after
        meck:unload(emqx_coap_session)
    end.

late_ack_and_reset_do_not_match_new_transaction_test() ->
    Token = <<"shared-token">>,
    Response = #coap_message{
        type = con,
        method = {ok, content},
        token = Token,
        payload = <<"payload">>
    },
    TM0 = emqx_coap_tm:new(),
    #{out := [First], tm := TM1} = emqx_coap_tm:handle_out(Response, TM0),
    FirstSeqId = maps:get({token, Token}, TM1),
    #{tm := TM2} = emqx_coap_tm:timeout({FirstSeqId, stop_timeout, stop}, TM1),
    #{out := [Second], tm := TM3} = emqx_coap_tm:handle_out(Response, TM2),
    LateAck = #coap_message{
        type = ack,
        method = undefined,
        id = First#coap_message.id,
        token = Token
    },
    LateReset = LateAck#coap_message{type = reset},
    ?assertEqual(#{}, emqx_coap_tm:handle_response(LateAck, TM3)),
    ?assertEqual(#{}, emqx_coap_tm:handle_response(LateReset, TM3)),
    CurrentAck = LateAck#coap_message{id = Second#coap_message.id},
    #{tm := TM4} = emqx_coap_tm:handle_response(CurrentAck, TM3),
    ?assertEqual(undefined, maps:get({out, Second#coap_message.id}, TM4, undefined)),
    ?assertEqual(undefined, maps:get({token, Token}, TM4, undefined)).

new_response_channel() ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683}
    },
    Channel0 = {
        channel,
        #{gwname => coap, cm => self()},
        ConnInfo,
        #{},
        emqx_coap_session:new(),
        undefined,
        #{},
        false,
        connected,
        undefined
    },
    Msg = #coap_message{
        type = con,
        method = {ok, content},
        token = <<"response-token">>,
        payload = <<"response-payload">>
    },
    #{out := [Out], session := Session1} =
        emqx_coap_session:handle_out(Msg, Channel0#channel.session),
    {Out, Channel0#channel{session = Session1}}.
