%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_session_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_coap_test_helpers:start_gateway(Config).

end_per_suite(Config) ->
    emqx_coap_test_helpers:stop_gateway(Config).

t_session_info_and_deliver(_) ->
    Session0 = emqx_coap_session:new(),
    _ = emqx_coap_session:info([inflight, mqueue, awaiting_rel], Session0),
    _ = emqx_coap_session:handle_request(#coap_message{type = con, method = get, id = 1}, Session0),
    _ = emqx_coap_session:handle_response(#coap_message{type = ack, id = 1}, Session0),
    _ = emqx_coap_session:handle_out(#coap_message{type = con, method = get}, Session0),
    _ = emqx_coap_session:timeout({1, state_timeout, ack_timeout}, Session0),
    SubData = #{topic => <<"t1">>, token => <<"tok">>, subopts => #{qos => 0}},
    Msg = #coap_message{type = con, method = get, id = 1, token = <<"tok">>},
    _ = emqx_coap_session:process_subscribe(undefined, Msg, #{}, Session0),
    Result = emqx_coap_session:process_subscribe(SubData, Msg, #{}, Session0),
    Session1 = maps:get(session, Result),
    Ctx = coap_ctx(),
    Deliver1 = {deliver, <<"t0">>, emqx_message:make(<<"t0">>, <<"p0">>)},
    Deliver2 = {deliver, <<"t1">>, emqx_message:make(<<"t1">>, <<"p1">>)},
    #{out := Out} = emqx_coap_session:deliver([Deliver1, Deliver2], Ctx, Session1),
    ?assertEqual(1, length(Out)),
    ok.

t_session_notify_qos_types(_) ->
    KeyPath = [gateway, coap, notify_qos],
    OldValue = emqx_config:find(KeyPath),
    ok = emqx_config:force_put(KeyPath, qos),
    try
        Session0 = emqx_coap_session:new(),
        SubData = #{topic => <<"tq">>, token => <<"tok">>, subopts => #{qos => 0}},
        Msg = #coap_message{type = con, method = get, id = 1, token = <<"tok">>},
        Result = emqx_coap_session:process_subscribe(SubData, Msg, #{}, Session0),
        Session1 = maps:get(session, Result),
        Ctx = coap_ctx(),
        Deliver0 = {deliver, <<"tq">>, emqx_message:make(undefined, 0, <<"tq">>, <<"p0">>)},
        Deliver1 = {deliver, <<"tq">>, emqx_message:make(undefined, 1, <<"tq">>, <<"p1">>)},
        #{out := [Out0], session := Session2} =
            emqx_coap_session:deliver([Deliver0], Ctx, Session1),
        ?assertEqual(non, Out0#coap_message.type),
        #{out := [Out1]} = emqx_coap_session:deliver([Deliver1], Ctx, Session2),
        ?assertEqual(con, Out1#coap_message.type)
    after
        case OldValue of
            {ok, Val} ->
                ok = emqx_config:force_put(KeyPath, Val);
            _ ->
                ok = emqx_config:force_put(KeyPath, non)
        end
    end,
    ok.

t_session_deliver_block2_notify(_) ->
    Session0 = emqx_coap_session:new(),
    SubData = #{topic => <<"tb2">>, token => <<"tokb2">>, subopts => #{qos => 0}},
    Msg = #coap_message{type = con, method = get, id = 1, token = <<"tokb2">>},
    Result = emqx_coap_session:process_subscribe(SubData, Msg, #{}, Session0),
    Session1 = maps:get(session, Result),
    BW0 = emqx_coap_blockwise:new(#{max_block_size => 16}),
    Ctx = coap_ctx(),
    Deliver = {deliver, <<"tb2">>, emqx_message:make(<<"tb2">>, binary:copy(<<"Z">>, 40))},
    #{out := [Out0], blockwise := BW1} = emqx_coap_session:deliver(
        [Deliver], Ctx, Session1, BW0, {peer, 7}
    ),
    ?assertEqual({0, true, 16}, emqx_coap_message:get_option(block2, Out0, undefined)),
    FollowReq = #coap_message{
        type = con,
        method = get,
        id = 2,
        token = <<"tokb2">>,
        options = #{uri_path => [<<"ps">>, <<"topic">>], block2 => {1, false, 16}}
    },
    {reply, Out1, _BW2} = emqx_coap_blockwise:server_followup_in(FollowReq, {peer, 7}, BW1),
    ?assertEqual({1, true, 16}, emqx_coap_message:get_option(block2, Out1, undefined)),
    ok.

t_session_notify_block2_prepare_error_mapping(_) ->
    Msg = #coap_message{
        type = con,
        method = {ok, content},
        token = <<"tok-map">>,
        payload = <<"payload">>,
        options = #{}
    },
    BW = emqx_coap_blockwise:new(#{max_block_size => 16}),
    {MappedMsg, MappedBW} = emqx_coap_session:map_notify_block2_prepare_result(
        {error, fake_reply, BW},
        Msg,
        {peer, 8},
        coap_ctx()
    ),
    ?assertEqual(Msg, MappedMsg),
    ?assertEqual(BW, MappedBW).

coap_ctx() ->
    #{
        gwname => coap,
        cm => self(),
        metrics_tab => emqx_gateway_metrics:tabname(coap)
    }.
