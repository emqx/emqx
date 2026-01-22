%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

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

-record(state_machine, {
    seq_id,
    id,
    token,
    observe,
    state,
    timers,
    transport
}).

-record(keepalive, {
    check_interval,
    statval,
    stat_reader,
    idle_milliseconds = 0,
    max_idle_millisecond
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

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
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

t_frame_encode_decode_options(_) ->
    Options = #{
        if_match => [<<"a">>],
        uri_host => <<"example.com">>,
        etag => [<<"tag">>],
        if_none_match => true,
        uri_port => 5683,
        location_path => [<<"loc">>, undefined],
        uri_path => [<<"path">>, <<"sub">>],
        content_format => <<"application/json">>,
        max_age => 61,
        uri_query => [<<"a=1">>, <<"b=2">>],
        'accept' => 0,
        location_query => [<<"x=1">>],
        proxy_uri => <<"coap://example.com">>,
        proxy_scheme => <<"coap">>,
        size1 => 10,
        observe => 1,
        block1 => {0, true, 16},
        block2 => {1, false, 32}
    },
    Msg = #coap_message{
        type = con,
        method = get,
        id = 1,
        token = <<>>,
        options = Options,
        payload = <<"p">>
    },
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Bin, #{}),
    Opts = Decoded#coap_message.options,
    ?assertEqual([<<"a">>], maps:get(if_match, Opts)),
    ?assertEqual(<<"example.com">>, maps:get(uri_host, Opts)),
    ?assertEqual([<<"path">>, <<"sub">>], maps:get(uri_path, Opts)),
    ?assertEqual(#{<<"a">> => <<"1">>, <<"b">> => <<"2">>}, maps:get(uri_query, Opts)),
    ok.

t_frame_encode_extended_values(_) ->
    LongVal = binary:copy(<<"a">>, 300),
    Msg1 = #coap_message{
        type = con,
        method = get,
        id = 10,
        options = #{uri_query => #{<<"k">> => LongVal}},
        payload = <<>>
    },
    _ = emqx_coap_frame:serialize_pkt(Msg1, emqx_coap_frame:serialize_opts()),
    Msg2 = #coap_message{
        type = con,
        method = get,
        id = 11,
        options = #{if_match => [<<"a">>], size1 => 10},
        payload = <<>>
    },
    _ = emqx_coap_frame:serialize_pkt(Msg2, emqx_coap_frame:serialize_opts()),
    Msg3 = #coap_message{
        type = con,
        method = get,
        id = 12,
        options = #{if_match => [<<"a">>], 300 => <<1>>},
        payload = <<>>
    },
    _ = emqx_coap_frame:serialize_pkt(Msg3, emqx_coap_frame:serialize_opts()),
    Types = [
        <<"application/link-format">>,
        <<"application/xml">>,
        <<"application/exi">>,
        <<"application/json">>,
        <<"application/cbor">>,
        <<"application/vnd.oma.lwm2m+tlv">>,
        <<"application/vnd.oma.lwm2m+json">>,
        <<"application/unknown">>
    ],
    lists:foreach(
        fun(Type) ->
            Msg = #coap_message{
                type = con,
                method = get,
                id = 20,
                options = #{content_format => Type},
                payload = <<>>
            },
            _ = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts())
        end,
        Types
    ),
    _ = emqx_coap_frame:serialize_pkt(
        #coap_message{type = con, method = {ok, valid}, id = 21},
        emqx_coap_frame:serialize_opts()
    ),
    _ = emqx_coap_frame:serialize_pkt(
        #coap_message{type = con, method = {ok, continue}, id = 22},
        emqx_coap_frame:serialize_opts()
    ),
    Methods = [
        put,
        delete,
        {error, bad_option},
        {error, forbidden},
        {error, not_found},
        {error, method_not_allowed},
        {error, not_acceptable},
        {error, request_entity_incomplete},
        {error, precondition_failed},
        {error, request_entity_too_large},
        {error, unsupported_content_format},
        {error, internal_server_error},
        {error, not_implemented},
        {error, bad_gateway},
        {error, service_unavailable}
    ],
    lists:foreach(
        fun(Method) ->
            _ = emqx_coap_frame:serialize_pkt(
                #coap_message{type = con, method = Method, id = 23},
                emqx_coap_frame:serialize_opts()
            )
        end,
        Methods
    ),
    ?assertThrow(
        {bad_method, bad_method},
        emqx_coap_frame:serialize_pkt(
            #coap_message{type = con, method = bad_method, id = 23},
            emqx_coap_frame:serialize_opts()
        )
    ),
    ?assertThrow(
        {bad_option, bad_opt, <<"x">>},
        emqx_coap_frame:serialize_pkt(
            #coap_message{type = con, method = get, id = 24, options = #{bad_opt => <<"x">>}},
            emqx_coap_frame:serialize_opts()
        )
    ),
    MsgInt = #coap_message{
        type = con,
        method = get,
        id = 25,
        options = #{content_format => 11542},
        payload = <<>>
    },
    BinInt = emqx_coap_frame:serialize_pkt(MsgInt, emqx_coap_frame:serialize_opts()),
    {ok, DecodedInt, <<>>, _} = emqx_coap_frame:parse(BinInt, #{}),
    ?assertEqual(
        <<"application/vnd.oma.lwm2m+tlv">>,
        maps:get(content_format, DecodedInt#coap_message.options)
    ),
    MsgInt2 = #coap_message{
        type = con,
        method = get,
        id = 26,
        options = #{content_format => 11543},
        payload = <<>>
    },
    BinInt2 = emqx_coap_frame:serialize_pkt(MsgInt2, emqx_coap_frame:serialize_opts()),
    {ok, DecodedInt2, <<>>, _} = emqx_coap_frame:parse(BinInt2, #{}),
    ?assertEqual(
        <<"application/vnd.oma.lwm2m+json">>,
        maps:get(content_format, DecodedInt2#coap_message.options)
    ),
    _ = emqx_coap_frame:serialize_pkt(
        #coap_message{type = con, method = {error, gateway_timeout}, id = 27},
        emqx_coap_frame:serialize_opts()
    ),
    _ = emqx_coap_frame:serialize_pkt(
        #coap_message{type = con, method = {error, proxying_not_supported}, id = 28},
        emqx_coap_frame:serialize_opts()
    ),
    DecodeCodes = [
        {40, <<"application/link-format">>},
        {41, <<"application/xml">>},
        {47, <<"application/exi">>},
        {60, <<"application/cbor">>},
        {9999, <<"application/octet-stream">>}
    ],
    lists:foreach(
        fun({Code, Expected}) ->
            Msg = #coap_message{
                type = con,
                method = get,
                id = 30,
                options = #{content_format => Code},
                payload = <<>>
            },
            Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
            {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Bin, #{}),
            ?assertEqual(Expected, maps:get(content_format, Decoded#coap_message.options))
        end,
        DecodeCodes
    ),
    ok.

t_frame_empty_reset_and_undefined_option(_) ->
    Msg0 = #coap_message{type = reset, method = undefined, id = 42},
    Bin0 = emqx_coap_frame:serialize_pkt(Msg0, emqx_coap_frame:serialize_opts()),
    {ok, Decoded0, <<>>, _} = emqx_coap_frame:parse(Bin0, #{}),
    ?assertEqual(reset, Decoded0#coap_message.type),
    ?assertEqual(42, Decoded0#coap_message.id),
    ?assertEqual(undefined, Decoded0#coap_message.method),
    Msg1 = #coap_message{
        type = con,
        method = get,
        id = 43,
        options = #{uri_host => undefined},
        payload = <<>>
    },
    _ = emqx_coap_frame:serialize_pkt(Msg1, emqx_coap_frame:serialize_opts()),
    ok.

t_frame_decode_extended_values(_) ->
    OptVal13 = binary:copy(<<"x">>, 13),
    Header13 = <<1:2, 0:2, 0:4, 0:3, 1:5, 2:16>>,
    Opt13 = <<13:4, 13:4, 0:8, 0:8, OptVal13/binary>>,
    Packet13 = <<Header13/binary, Opt13/binary>>,
    {ok, Decoded13, <<>>, _} = emqx_coap_frame:parse(Packet13, #{}),
    Opts13 = Decoded13#coap_message.options,
    ?assertEqual(OptVal13, maps:get(13, Opts13)),
    OptVal = binary:copy(<<"z">>, 269),
    Header = <<1:2, 0:2, 0:4, 0:3, 1:5, 1:16>>,
    Opt = <<14:4, 14:4, 0:16, 0:16, OptVal/binary>>,
    Packet = <<Header/binary, Opt/binary>>,
    {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    Opts = Decoded#coap_message.options,
    ?assertEqual(OptVal, maps:get(269, Opts)),
    ok.

t_frame_parse_codes(_) ->
    ResetPacket = <<1:2, 3:2, 0:4, 0:3, 0:5, 10:16>>,
    {ok, ResetMsg, <<>>, _} = emqx_coap_frame:parse(ResetPacket, #{}),
    ?assertEqual(reset, ResetMsg#coap_message.type),
    Codes = [
        {2, 2, {ok, deleted}},
        {2, 3, {ok, valid}},
        {2, 4, {ok, changed}},
        {2, 7, {ok, nocontent}},
        {2, 31, {ok, continue}},
        {4, 0, {error, bad_request}},
        {4, 1, {error, unauthorized}},
        {4, 2, {error, bad_option}},
        {4, 3, {error, forbidden}},
        {4, 4, {error, not_found}},
        {4, 5, {error, method_not_allowed}},
        {4, 6, {error, not_acceptable}},
        {4, 8, {error, request_entity_incomplete}},
        {4, 12, {error, precondition_failed}},
        {4, 13, {error, request_entity_too_large}},
        {4, 15, {error, unsupported_content_format}},
        {5, 0, {error, internal_server_error}},
        {5, 1, {error, not_implemented}},
        {5, 2, {error, bad_gateway}},
        {5, 3, {error, service_unavailable}},
        {5, 4, {error, gateway_timeout}},
        {5, 5, {error, proxying_not_supported}},
        {1, 0, undefined}
    ],
    lists:foreach(
        fun({Class, Code, Expected}) ->
            Packet = <<1:2, 0:2, 0:4, Class:3, Code:5, 11:16>>,
            {ok, Msg, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
            ?assertEqual(Expected, Msg#coap_message.method)
        end,
        Codes
    ),
    ok.

t_frame_query_and_truncated_option(_) ->
    QueryMsg = #coap_message{
        type = con,
        method = get,
        id = 30,
        options = #{uri_query => [<<"plain">>]}
    },
    QueryBin = emqx_coap_frame:serialize_pkt(QueryMsg, emqx_coap_frame:serialize_opts()),
    {ok, QueryDecoded, <<>>, _} = emqx_coap_frame:parse(QueryBin, #{}),
    ?assertEqual(#{}, maps:get(uri_query, QueryDecoded#coap_message.options)),
    Header = <<1:2, 0:2, 0:4, 0:3, 1:5, 31:16>>,
    Opt = <<0:4, 1:4>>,
    Packet = <<Header/binary, Opt/binary>>,
    {ok, TruncatedDecoded, <<>>, _} = emqx_coap_frame:parse(Packet, #{}),
    ?assertEqual(<<>>, maps:get(0, TruncatedDecoded#coap_message.options)),
    ok.

t_frame_block_roundtrip(_) ->
    Cases = [
        {block1, {0, true, 16}, 40},
        {block1, {16, false, 16}, 41},
        {block2, {4096, true, 16}, 42}
    ],
    lists:foreach(
        fun({Opt, Val, Id}) ->
            Msg = #coap_message{type = con, method = get, id = Id, options = #{Opt => Val}},
            Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
            {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Bin, #{}),
            ?assertEqual(Val, maps:get(Opt, Decoded#coap_message.options))
        end,
        Cases
    ),
    ok.

t_frame_misc_helpers(_) ->
    Msg = #coap_message{type = con, method = get, id = 50},
    _ = emqx_coap_frame:format(Msg),
    ?assert(emqx_coap_frame:is_message(Msg)),
    ?assertEqual(false, emqx_coap_frame:is_message(#{bad => msg})),
    ok.

t_message_helpers(_) ->
    Msg1 = emqx_coap_message:request(con, get),
    Msg2 = emqx_coap_message:request(con, post, <<"p">>),
    Resp1 = emqx_coap_message:response(Msg1),
    Resp2 = emqx_coap_message:response({ok, content}, Msg1),
    Resp3 = emqx_coap_message:response({ok, content}, <<"p">>, Msg1),
    ?assertMatch(#coap_message{}, Resp1),
    ?assertMatch(#coap_message{}, Resp2),
    ?assertMatch(#coap_message{}, Resp3),
    ?assertEqual(Msg1, emqx_coap_message:set(max_age, ?DEFAULT_MAX_AGE, Msg1)),
    QueryMsg = #coap_message{options = #{uri_query => #{<<"c">> => <<"client1">>}}},
    ?assertMatch(
        #{<<"clientid">> := <<"client1">>},
        emqx_coap_message:extract_uri_query(QueryMsg)
    ),
    _ = emqx_coap_message:set_payload("list", Msg2),
    BlockMsg1 =
        emqx_coap_message:set_payload_block(<<"abcdefghij">>, {0, true, 4}, Msg2),
    BlockMsg2 =
        emqx_coap_message:set_payload_block(<<"abcdefghij">>, {1, false, 5}, Resp2),
    ?assertMatch(#coap_message{}, BlockMsg1),
    ?assertMatch(#coap_message{}, BlockMsg2),
    ok.

t_medium_helpers(_) ->
    Msg = #coap_message{type = con, method = get, id = 1},
    Result0 = emqx_coap_medium:out(Msg),
    Result1 = emqx_coap_medium:out(Msg, Result0),
    Result2 = emqx_coap_medium:reset(Msg, #{}),
    Result3 = emqx_coap_medium:proto_out({request, Msg}, #{}),
    ReplyMsg = #coap_message{type = ack, method = {ok, content}, id = 2},
    Result4 = emqx_coap_medium:reply(ReplyMsg, #{}),
    ?assertMatch(#{out := _}, Result1),
    ?assertMatch(#{out := _}, Result2),
    ?assertMatch(#{proto := _}, Result3),
    ?assertMatch(#{reply := _}, Result4),
    ok.

t_observe_manager(_) ->
    Manager0 = emqx_coap_observe_res:new_manager(),
    Sub = #{topic => <<"t">>, token => <<"tok">>, subopts => #{qos => 0}},
    {SeqId, Manager1} = emqx_coap_observe_res:insert(Sub, Manager0),
    {SeqId2, Manager2} = emqx_coap_observe_res:insert(Sub, Manager1),
    ?assertEqual(SeqId, SeqId2),
    _ = emqx_coap_observe_res:remove(<<"t">>, Manager2),
    Manager3 = #{
        <<"wrap">> => #{token => <<"t">>, seq_id => 16777215, subopts => #{qos => 0}}
    },
    {_, 1, _} = emqx_coap_observe_res:res_changed(<<"wrap">>, Manager3),
    ok = emqx_coap_observe_res:foreach(fun(_, _) -> ok end, Manager2),
    _ = emqx_coap_observe_res:subscriptions(Manager2),
    ok.

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
    Ctx = #{gwname => coap, cm => self()},
    Deliver1 = {deliver, <<"t0">>, emqx_message:make(<<"t0">>, <<"p0">>)},
    Deliver2 = {deliver, <<"t1">>, emqx_message:make(<<"t1">>, <<"p1">>)},
    #{out := Out} = emqx_coap_session:deliver([Deliver1, Deliver2], Ctx, Session1),
    ?assertEqual(1, length(Out)),
    ok.

t_session_notify_qos_types(_) ->
    ok = meck:new(emqx_conf, [passthrough]),
    ok = meck:expect(emqx_conf, get, fun(_, _) -> qos end),
    try
        Session0 = emqx_coap_session:new(),
        SubData = #{topic => <<"tq">>, token => <<"tok">>, subopts => #{qos => 0}},
        Msg = #coap_message{type = con, method = get, id = 1, token = <<"tok">>},
        Result = emqx_coap_session:process_subscribe(SubData, Msg, #{}, Session0),
        Session1 = maps:get(session, Result),
        Ctx = #{gwname => coap, cm => self()},
        Deliver0 = {deliver, <<"tq">>, emqx_message:make(undefined, 0, <<"tq">>, <<"p0">>)},
        Deliver1 = {deliver, <<"tq">>, emqx_message:make(undefined, 1, <<"tq">>, <<"p1">>)},
        #{out := [Out0], session := Session2} =
            emqx_coap_session:deliver([Deliver0], Ctx, Session1),
        ?assertEqual(non, Out0#coap_message.type),
        #{out := [Out1]} = emqx_coap_session:deliver([Deliver1], Ctx, Session2),
        ?assertEqual(con, Out1#coap_message.type)
    after
        ok = meck:unload(emqx_conf)
    end,
    ok.

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

t_tm_timer_cleanup(_) ->
    ok = meck:new(emqx_coap_transport, [passthrough]),
    ok = meck:expect(emqx_coap_transport, idle, fun(_, _, _) ->
        #{next => wait_ack, timeouts => []}
    end),
    try
        TM0 = emqx_coap_tm:new(),
        Msg = #coap_message{type = con, method = get, token = <<"tok">>, id = 1},
        #{tm := TM1} = emqx_coap_tm:handle_out(Msg, TM0),
        TokenKey = {token, <<"tok">>},
        SeqId = maps:get(TokenKey, TM1),
        Machine = maps:get(SeqId, TM1),
        Timers = #{state_timeout => make_ref(), state_timer => make_ref()},
        TMWithTimer = TM1#{SeqId => Machine#state_machine{state = idle, timers = Timers}},
        _ = emqx_coap_tm:timeout({SeqId, state_timeout, ack_timeout}, TMWithTimer)
    after
        ok = meck:unload(emqx_coap_transport)
    end,
    ok.

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

t_api_namespace(_) ->
    ?assertEqual("gateway_coap", emqx_coap_api:namespace()),
    ok.

t_channel_direct(_) ->
    ok = meck:new(esockd_peercert, [passthrough, no_history, no_link]),
    ok = meck:expect(esockd_peercert, subject, fun(_) -> <<"DN">> end),
    ok = meck:expect(esockd_peercert, common_name, fun(_) -> <<"CN">> end),
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 9999},
        sockname => {{127, 0, 0, 1}, 5683},
        peercert => dummy
    },
    Channel0 = emqx_coap_channel:init(ConnInfo, #{ctx => #{gwname => coap, cm => self()}}),
    ChannelRequired =
        emqx_coap_channel:init(ConnInfo, #{
            ctx => #{gwname => coap, cm => self()},
            connection_required => true
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
    meck:unload(esockd_peercert),
    ok.

t_channel_connection_success(_) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough]),
    ok = meck:new(emqx_hooks, [passthrough]),
    ok = meck:expect(
        emqx_gateway_ctx,
        authenticate,
        fun(_, ClientInfo) -> {ok, ClientInfo#{auth_expire_at => undefined}} end
    ),
    ok = meck:expect(emqx_gateway_ctx, open_session, fun(_, _, _, _, _, _) -> {ok, #{}} end),
    ok = meck:expect(emqx_gateway_ctx, connection_expire_interval, fun(_, _) -> 1 end),
    ok = meck:expect(emqx_hooks, run, fun(_, _) -> ok end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_, _, Acc) -> {ok, Acc} end),
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
            id = 1,
            options = #{
                uri_path => [<<"mqtt">>, <<"connection">>],
                uri_query => #{
                    <<"clientid">> => <<"client1">>,
                    <<"username">> => <<"admin">>,
                    <<"password">> => <<"public">>
                }
            }
        },
        {ok, [{outgoing, [Reply]} | _], Channel1} =
            emqx_coap_channel:handle_in(ConnReq, Channel0),
        ?assertMatch(#coap_message{method = {ok, created}}, Reply),
        Token = Channel1#channel.token,
        HeartbeatReq = #coap_message{
            type = con,
            method = put,
            id = 2,
            options = #{
                uri_path => [<<"mqtt">>, <<"connection">>],
                uri_query => #{<<"clientid">> => <<"client1">>, <<"token">> => Token}
            }
        },
        {ok, [{outgoing, [_]}], Channel2} =
            emqx_coap_channel:handle_in(HeartbeatReq, Channel1),
        CloseReq = #coap_message{
            type = con,
            method = delete,
            id = 3,
            options = #{
                uri_path => [<<"mqtt">>, <<"connection">>],
                uri_query => #{<<"clientid">> => <<"client1">>, <<"token">> => Token}
            }
        },
        {shutdown, normal, _, _} = emqx_coap_channel:handle_in(CloseReq, Channel2)
    after
        ok = meck:unload(emqx_hooks),
        ok = meck:unload(emqx_gateway_ctx)
    end,
    ok.

t_channel_connection_no_expire(_) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough]),
    ok = meck:new(emqx_hooks, [passthrough]),
    ok = meck:expect(
        emqx_gateway_ctx,
        authenticate,
        fun(_, ClientInfo) -> {ok, ClientInfo#{auth_expire_at => undefined}} end
    ),
    ok = meck:expect(emqx_gateway_ctx, open_session, fun(_, _, _, _, _, _) -> {ok, #{}} end),
    ok = meck:expect(emqx_gateway_ctx, connection_expire_interval, fun(_, _) -> undefined end),
    ok = meck:expect(emqx_hooks, run, fun(_, _) -> ok end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_, _, Acc) -> {ok, Acc} end),
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
            id = 4,
            options = #{
                uri_path => [<<"mqtt">>, <<"connection">>],
                uri_query => #{
                    <<"clientid">> => <<"client1">>,
                    <<"username">> => <<"admin">>,
                    <<"password">> => <<"public">>
                }
            }
        },
        {ok, [{outgoing, [_]} | _], _} = emqx_coap_channel:handle_in(ConnReq, Channel0)
    after
        ok = meck:unload(emqx_hooks),
        ok = meck:unload(emqx_gateway_ctx)
    end,
    ok.

t_channel_connection_missing_clientid_direct(_) ->
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
        id = 5,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => #{<<"username">> => <<"admin">>}
        }
    },
    {shutdown, normal, _, _} = emqx_coap_channel:handle_in(ConnReq, Channel0),
    ok.

t_channel_connection_hooks_error_direct(_) ->
    ok = meck:new(emqx_hooks, [passthrough]),
    ok = meck:expect(emqx_hooks, run_fold, fun(_, _, _) -> {error, hook_failed} end),
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
        ok = meck:unload(emqx_hooks)
    end,
    ok.

t_channel_connection_auth_error_direct(_) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough]),
    ok = meck:new(emqx_hooks, [passthrough]),
    ok = meck:expect(emqx_gateway_ctx, authenticate, fun(_, _) -> {error, bad_password} end),
    ok = meck:expect(emqx_hooks, run, fun(_, _) -> ok end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_, _, Acc) -> {ok, Acc} end),
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
            id = 7,
            options = #{
                uri_path => [<<"mqtt">>, <<"connection">>],
                uri_query => #{<<"clientid">> => <<"client1">>}
            }
        },
        {shutdown, normal, _, _} = emqx_coap_channel:handle_in(ConnReq, Channel0)
    after
        ok = meck:unload(emqx_hooks),
        ok = meck:unload(emqx_gateway_ctx)
    end,
    ok.

t_channel_connection_open_session_error_direct(_) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough]),
    ok = meck:new(emqx_hooks, [passthrough]),
    ok = meck:expect(
        emqx_gateway_ctx,
        authenticate,
        fun(_, ClientInfo) -> {ok, ClientInfo#{auth_expire_at => undefined}} end
    ),
    ok = meck:expect(emqx_gateway_ctx, open_session, fun(_, _, _, _, _, _) ->
        {error, session_error}
    end),
    ok = meck:expect(emqx_hooks, run, fun(_, _) -> ok end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_, _, Acc) -> {ok, Acc} end),
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
        ok = meck:unload(emqx_hooks),
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

t_proxy_conn_paths(_) ->
    State = emqx_coap_frame:initial_parse_state(#{}),
    ?assertEqual(invalid, emqx_coap_proxy_conn:get_connection_id(dummy, dummy, State, <<>>)),
    Msg = #coap_message{
        type = con,
        method = post,
        id = 1,
        options = #{uri_path => [<<"mqtt">>, <<"connection">>]}
    },
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {error, ErrBin} = emqx_coap_proxy_conn:get_connection_id(dummy, dummy, State, Bin),
    ?assert(is_binary(ErrBin)),
    ok.

t_schema_and_gateway_paths(_) ->
    _ = emqx_coap_schema:namespace(),
    _ = emqx_coap_schema:desc(other),
    ok = meck:new(emqx_gateway_utils, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_gateway_utils, normalize_config, fun(_) -> [] end),
    ok = meck:expect(
        emqx_gateway_utils,
        start_listeners,
        fun(_, _, _, _) -> {error, {bad_listener, #{}}} end
    ),
    ?assertThrow(
        {badconf, _},
        emqx_gateway_coap:on_gateway_load(
            #{name => coap, config => #{}},
            #{}
        )
    ),
    ok = meck:expect(
        emqx_gateway_utils,
        update_gateway,
        fun(_, _, _, _, _) -> erlang:error(update_failed) end
    ),
    {error, update_failed} =
        emqx_gateway_coap:on_gateway_update(
            #{},
            #{name => coap, config => #{}},
            #{ctx => #{gwname => coap, cm => self()}}
        ),
    meck:unload(emqx_gateway_utils),
    ok.
