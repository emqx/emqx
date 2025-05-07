%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_frame_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_nats.hrl").

%%--------------------------------------------------------------------
%% CT Callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% Control Frames Tests
%%--------------------------------------------------------------------

t_ping_pong(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),

    % Test PING
    {ok, Frame1, Rest1, NewState1} = emqx_nats_frame:parse(<<"PING\r\n">>, State),
    ?assertEqual(?OP_PING, emqx_nats_frame:type(Frame1)),
    ?assertEqual(<<>>, Rest1),

    % Test pong
    {ok, Frame2, Rest2, _} = emqx_nats_frame:parse(<<"PONG\r\n">>, NewState1),
    ?assertEqual(?OP_PONG, emqx_nats_frame:type(Frame2)),
    ?assertEqual(<<>>, Rest2).

t_connect(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    ConnectJson = <<"{\"verbose\":false,\"pedantic\":false,\"tls_required\":false}">>,
    ConnectFrame = <<"CONNECT ", ConnectJson/binary, "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(ConnectFrame, State),
    ?assertEqual(?OP_CONNECT, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{<<"verbose">> := false}, emqx_nats_frame:message(Frame)).

t_info(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    InfoJson = <<"{\"server_id\":\"test\",\"version\":\"1.0.0\"}">>,
    InfoFrame = <<"INFO ", InfoJson/binary, "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(InfoFrame, State),
    ?assertEqual(?OP_INFO, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{<<"server_id">> := <<"test">>}, emqx_nats_frame:message(Frame)).

t_ok(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(<<"+OK\r\n">>, State),
    ?assertEqual(?OP_OK, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest).

t_err(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    ErrorMsg = <<"Invalid subject">>,
    ErrorFrame = <<"-ERR ", ErrorMsg/binary, "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(ErrorFrame, State),
    ?assertEqual(?OP_ERR, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(ErrorMsg, emqx_nats_frame:message(Frame)).

%%--------------------------------------------------------------------
%% Message Frames Tests
%%--------------------------------------------------------------------

t_pub(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    Subject = <<"test.subject">>,
    Payload = <<"test payload">>,
    PayloadSize = integer_to_binary(byte_size(Payload)),
    PubFrame = <<"PUB ", Subject/binary, " ", PayloadSize/binary, "\r\n", Payload/binary, "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(PubFrame, State),
    ?assertEqual(?OP_PUB, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(Subject, emqx_nats_frame:subject(Frame)),
    ?assertEqual(Payload, emqx_nats_frame:payload(Frame)).

t_sub(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    Subject = <<"test.subject">>,
    Sid = <<"123">>,
    SubFrame = <<"SUB ", Subject/binary, " ", Sid/binary, "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(SubFrame, State),
    ?assertEqual(?OP_SUB, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(Subject, emqx_nats_frame:subject(Frame)),
    ?assertEqual(Sid, emqx_nats_frame:sid(Frame)).

t_unsub(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    Sid = <<"123">>,
    MaxMsgs = <<"10">>,
    UnsubFrame = <<"UNSUB ", Sid/binary, " ", MaxMsgs/binary, "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(UnsubFrame, State),
    ?assertEqual(?OP_UNSUB, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(Sid, emqx_nats_frame:sid(Frame)).

t_msg(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    Subject = <<"test.subject">>,
    Sid = <<"123">>,
    Payload = <<"test payload">>,
    PayloadSize = integer_to_binary(byte_size(Payload)),
    MsgFrame =
        <<"MSG ", Subject/binary, " ", Sid/binary, " ", PayloadSize/binary, "\r\n", Payload/binary,
            "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(MsgFrame, State),
    ?assertEqual(?OP_MSG, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(Subject, emqx_nats_frame:subject(Frame)),
    ?assertEqual(Sid, emqx_nats_frame:sid(Frame)),
    ?assertEqual(Payload, emqx_nats_frame:payload(Frame)).

t_hpub(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    Subject = <<"test.subject">>,
    Headers = <<"NATS/1.0\r\nHeader1: Value1\r\nHeader2: Value2\r\n\r\n">>,
    HeadersSize = integer_to_binary(byte_size(Headers)),
    Payload = <<"test payload">>,
    TotalSize = integer_to_binary(byte_size(Payload) + byte_size(Headers)),
    HpubFrame =
        <<"HPUB ", Subject/binary, " ", HeadersSize/binary, " ", TotalSize/binary, "\r\n",
            Headers/binary, Payload/binary, "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(HpubFrame, State),
    ?assertEqual(?OP_HPUB, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(Subject, emqx_nats_frame:subject(Frame)),
    ?assertEqual(Payload, emqx_nats_frame:payload(Frame)),
    ?assertMatch(
        #{<<"Header1">> := <<"Value1">>, <<"Header2">> := <<"Value2">>},
        emqx_nats_frame:headers(Frame)
    ).

t_hmsg(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    Subject = <<"test.subject">>,
    Sid = <<"123">>,
    Headers = <<"NATS/1.0\r\nHeader1: Value1\r\nHeader2: Value2\r\n\r\n">>,
    HeadersSize = integer_to_binary(byte_size(Headers)),
    Payload = <<"test payload">>,
    TotalSize = integer_to_binary(byte_size(Payload) + byte_size(Headers)),
    HmsgFrame =
        <<"HMSG ", Subject/binary, " ", Sid/binary, " ", HeadersSize/binary, " ", TotalSize/binary,
            "\r\n", Headers/binary, Payload/binary, "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(HmsgFrame, State),
    ?assertEqual(?OP_HMSG, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(Subject, emqx_nats_frame:subject(Frame)),
    ?assertEqual(Sid, emqx_nats_frame:sid(Frame)),
    ?assertEqual(Payload, emqx_nats_frame:payload(Frame)),
    ?assertMatch(
        #{<<"Header1">> := <<"Value1">>, <<"Header2">> := <<"Value2">>},
        emqx_nats_frame:headers(Frame)
    ).

t_hpub_with_reply_to(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    Subject = <<"test.subject">>,
    ReplyTo = <<"reply.subject">>,
    Headers = <<"NATS/1.0\r\nHeader1: Value1\r\nHeader2: Value2\r\n\r\n">>,
    HeadersSize = integer_to_binary(byte_size(Headers)),
    Payload = <<"test payload">>,
    TotalSize = integer_to_binary(byte_size(Payload) + byte_size(Headers)),
    HpubFrame =
        <<"HPUB ", Subject/binary, " ", ReplyTo/binary, " ", HeadersSize/binary, " ",
            TotalSize/binary, "\r\n", Headers/binary, Payload/binary, "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(HpubFrame, State),
    ?assertEqual(?OP_HPUB, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(Subject, emqx_nats_frame:subject(Frame)),
    ?assertEqual(ReplyTo, emqx_nats_frame:reply_to(Frame)),
    ?assertEqual(Payload, emqx_nats_frame:payload(Frame)),
    ?assertMatch(
        #{<<"Header1">> := <<"Value1">>, <<"Header2">> := <<"Value2">>},
        emqx_nats_frame:headers(Frame)
    ).

t_hmsg_with_reply_to(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    Subject = <<"test.subject">>,
    Sid = <<"123">>,
    ReplyTo = <<"reply.subject">>,
    Headers = <<"NATS/1.0\r\nHeader1: Value1\r\nHeader2: Value2\r\n\r\n">>,
    HeadersSize = integer_to_binary(byte_size(Headers)),
    Payload = <<"test payload">>,
    TotalSize = integer_to_binary(byte_size(Payload) + byte_size(Headers)),
    HmsgFrame =
        <<"HMSG ", Subject/binary, " ", Sid/binary, " ", ReplyTo/binary, " ", HeadersSize/binary,
            " ", TotalSize/binary, "\r\n", Headers/binary, Payload/binary, "\r\n">>,

    {ok, Frame, Rest, _} = emqx_nats_frame:parse(HmsgFrame, State),
    ?assertEqual(?OP_HMSG, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(Subject, emqx_nats_frame:subject(Frame)),
    ?assertEqual(Sid, emqx_nats_frame:sid(Frame)),
    ?assertEqual(ReplyTo, emqx_nats_frame:reply_to(Frame)),
    ?assertEqual(Payload, emqx_nats_frame:payload(Frame)),
    ?assertMatch(
        #{<<"Header1">> := <<"Value1">>, <<"Header2">> := <<"Value2">>},
        emqx_nats_frame:headers(Frame)
    ).

t_invalid_headers(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    Subject = <<"test.subject">>,
    Headers = <<"Invalid-Headers\r\nHeader1: Value1\r\n\r\n">>,
    HeadersSize = integer_to_binary(byte_size(Headers)),
    Payload = <<"test payload">>,
    TotalSize = integer_to_binary(byte_size(Payload) + byte_size(Headers)),
    HpubFrame =
        <<"HPUB ", Subject/binary, " ", HeadersSize/binary, " ", TotalSize/binary, "\r\n",
            Headers/binary, Payload/binary, "\r\n">>,

    ?assertError(_, emqx_nats_frame:parse(HpubFrame, State)).

%%--------------------------------------------------------------------
%% State Management Tests
%%--------------------------------------------------------------------

t_initial_state(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    ?assertMatch(#{state := init, buffer := <<>>, headers := false}, State).

t_state_transitions(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),

    % Test transition from init to args
    {more, ArgsState} = emqx_nats_frame:parse(<<"PUB ">>, State),
    ?assertMatch(#{state := args}, ArgsState),

    % Test transition from args to payload
    {more, PayloadState} = emqx_nats_frame:parse(<<"test.subject 10\r\n">>, ArgsState),
    ?assertMatch(#{state := payload}, PayloadState).

t_buffer_handling(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),

    % Test partial frame
    {more, State1} = emqx_nats_frame:parse(<<"PI">>, State),
    ?assertMatch(#{buffer := <<"PI">>}, State1),

    % Test completing the frame
    {ok, Frame, Rest, _} = emqx_nats_frame:parse(<<"NG\r\n">>, State1),
    ?assertEqual(?OP_PING, emqx_nats_frame:type(Frame)),
    ?assertEqual(<<>>, Rest).

%%--------------------------------------------------------------------
%% Getter Tests
%%--------------------------------------------------------------------

t_subject(_Config) ->
    Frame = #nats_frame{
        operation = ?OP_PUB,
        message = #{subject => <<"test.subject">>}
    },
    ?assertEqual(<<"test.subject">>, emqx_nats_frame:subject(Frame)),
    ?assertError(badarg, emqx_nats_frame:subject(#nats_frame{operation = ?OP_PING})).

t_sid(_Config) ->
    Frame = #nats_frame{
        operation = ?OP_SUB,
        message = #{sid => <<"123">>}
    },
    ?assertEqual(<<"123">>, emqx_nats_frame:sid(Frame)),
    ?assertError(badarg, emqx_nats_frame:sid(#nats_frame{operation = ?OP_PING})).

t_queue_group(_Config) ->
    Frame = #nats_frame{
        operation = ?OP_SUB,
        message = #{queue_group => <<"group1">>}
    },
    ?assertEqual(<<"group1">>, emqx_nats_frame:queue_group(Frame)),
    ?assertError(badarg, emqx_nats_frame:queue_group(#nats_frame{operation = ?OP_PING})).

t_reply_to(_Config) ->
    Frame = #nats_frame{
        operation = ?OP_PUB,
        message = #{reply_to => <<"reply.subject">>}
    },
    ?assertEqual(<<"reply.subject">>, emqx_nats_frame:reply_to(Frame)),
    ?assertError(badarg, emqx_nats_frame:reply_to(#nats_frame{operation = ?OP_PING})).

t_headers(_Config) ->
    Frame = #nats_frame{
        operation = ?OP_HMSG,
        message = #{headers => #{<<"header1">> => <<"value1">>, <<"header2">> => <<"value2">>}}
    },
    ?assertEqual(
        #{<<"header1">> => <<"value1">>, <<"header2">> => <<"value2">>},
        emqx_nats_frame:headers(Frame)
    ),
    ?assertEqual(#{}, emqx_nats_frame:headers(#nats_frame{operation = ?OP_PING})).

t_payload(_Config) ->
    Frame = #nats_frame{
        operation = ?OP_PUB,
        message = #{payload => <<"test payload">>}
    },
    ?assertEqual(<<"test payload">>, emqx_nats_frame:payload(Frame)),
    ?assertError(badarg, emqx_nats_frame:payload(#nats_frame{operation = ?OP_PING})).

%%--------------------------------------------------------------------
%% Error Scenarios Tests
%%--------------------------------------------------------------------

t_invalid_operation(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    InvalidFrame = <<"INVALID\r\n">>,
    ?assertError(_, emqx_nats_frame:parse(InvalidFrame, State)).

t_incomplete_frame(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    IncompleteFrame = <<"PUB test.subject">>,
    ?assertMatch({more, _}, emqx_nats_frame:parse(IncompleteFrame, State)).

t_invalid_json(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    InvalidJson = <<"CONNECT {invalid json}\r\n">>,
    ?assertError(_, emqx_nats_frame:parse(InvalidJson, State)).

t_invalid_payload_size(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    InvalidSizeFrame = <<"PUB test.subject invalid_size\r\npayload\r\n">>,
    ?assertError(_, emqx_nats_frame:parse(InvalidSizeFrame, State)).

t_missing_required_fields(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    % PUB without subject
    MissingSubjectFrame = <<"PUB \r\n">>,
    ?assertError(_, emqx_nats_frame:parse(MissingSubjectFrame, State)),

    % SUB without sid
    MissingSidFrame = <<"SUB test.subject\r\n">>,
    ?assertError(_, emqx_nats_frame:parse(MissingSidFrame, State)).

t_malformed_frame(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    % Missing CRLF
    MalformedFrame1 = <<"PING">>,
    ?assertMatch({more, _}, emqx_nats_frame:parse(MalformedFrame1, State)),

    % Extra spaces
    MalformedFrame2 = <<"PING  \r\n">>,
    ?assertError(_, emqx_nats_frame:parse(MalformedFrame2, State)).

t_invalid_state_transition(_Config) ->
    State = emqx_nats_frame:initial_parse_state(#{}),
    % Try to parse payload without proper state
    ?assertError(_, emqx_nats_frame:parse(<<"payload\r\n">>, State)).

%%--------------------------------------------------------------------
%% Serialization Tests
%%--------------------------------------------------------------------

t_serialize_hpub(_Config) ->
    Message = #{
        subject => <<"test.subject">>,
        headers => #{
            <<"Header1">> => <<"Value1">>,
            <<"Header2">> => <<"Value2">>
        },
        payload => <<"test payload">>
    },
    Frame = #nats_frame{operation = ?OP_HPUB, message = Message},
    Serialized = iolist_to_binary(emqx_nats_frame:serialize_pkt(Frame, #{})),

    % Parse the serialized frame
    State = emqx_nats_frame:initial_parse_state(#{}),
    {ok, ParsedFrame, Rest, _} = emqx_nats_frame:parse(Serialized, State),

    % Verify the parsed frame matches the original
    ?assertEqual(?OP_HPUB, emqx_nats_frame:type(ParsedFrame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(<<"test.subject">>, emqx_nats_frame:subject(ParsedFrame)),
    ?assertEqual(<<"test payload">>, emqx_nats_frame:payload(ParsedFrame)),
    ?assertMatch(
        #{<<"Header1">> := <<"Value1">>, <<"Header2">> := <<"Value2">>},
        emqx_nats_frame:headers(ParsedFrame)
    ).

t_serialize_hmsg(_Config) ->
    Message = #{
        subject => <<"test.subject">>,
        sid => <<"123">>,
        headers => #{
            <<"Header1">> => <<"Value1">>,
            <<"Header2">> => <<"Value2">>
        },
        payload => <<"test payload">>
    },
    Frame = #nats_frame{operation = ?OP_HMSG, message = Message},
    Serialized = iolist_to_binary(emqx_nats_frame:serialize_pkt(Frame, #{})),

    % Parse the serialized frame
    State = emqx_nats_frame:initial_parse_state(#{}),
    {ok, ParsedFrame, Rest, _} = emqx_nats_frame:parse(Serialized, State),

    % Verify the parsed frame matches the original
    ?assertEqual(?OP_HMSG, emqx_nats_frame:type(ParsedFrame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(<<"test.subject">>, emqx_nats_frame:subject(ParsedFrame)),
    ?assertEqual(<<"123">>, emqx_nats_frame:sid(ParsedFrame)),
    ?assertEqual(<<"test payload">>, emqx_nats_frame:payload(ParsedFrame)),
    ?assertMatch(
        #{<<"Header1">> := <<"Value1">>, <<"Header2">> := <<"Value2">>},
        emqx_nats_frame:headers(ParsedFrame)
    ).

t_serialize_hpub_with_reply_to(_Config) ->
    Message = #{
        subject => <<"test.subject">>,
        reply_to => <<"reply.subject">>,
        headers => #{
            <<"Header1">> => <<"Value1">>,
            <<"Header2">> => <<"Value2">>
        },
        payload => <<"test payload">>
    },
    Frame = #nats_frame{operation = ?OP_HPUB, message = Message},
    Serialized = iolist_to_binary(emqx_nats_frame:serialize_pkt(Frame, #{})),

    % Parse the serialized frame
    State = emqx_nats_frame:initial_parse_state(#{}),
    {ok, ParsedFrame, Rest, _} = emqx_nats_frame:parse(Serialized, State),

    % Verify the parsed frame matches the original
    ?assertEqual(?OP_HPUB, emqx_nats_frame:type(ParsedFrame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(<<"test.subject">>, emqx_nats_frame:subject(ParsedFrame)),
    ?assertEqual(<<"reply.subject">>, emqx_nats_frame:reply_to(ParsedFrame)),
    ?assertEqual(<<"test payload">>, emqx_nats_frame:payload(ParsedFrame)),
    ?assertMatch(
        #{<<"Header1">> := <<"Value1">>, <<"Header2">> := <<"Value2">>},
        emqx_nats_frame:headers(ParsedFrame)
    ).

t_serialize_hmsg_with_reply_to(_Config) ->
    Message = #{
        subject => <<"test.subject">>,
        sid => <<"123">>,
        reply_to => <<"reply.subject">>,
        headers => #{
            <<"Header1">> => <<"Value1">>,
            <<"Header2">> => <<"Value2">>
        },
        payload => <<"test payload">>
    },
    Frame = #nats_frame{operation = ?OP_HMSG, message = Message},
    Serialized = iolist_to_binary(emqx_nats_frame:serialize_pkt(Frame, #{})),

    % Parse the serialized frame
    State = emqx_nats_frame:initial_parse_state(#{}),
    {ok, ParsedFrame, Rest, _} = emqx_nats_frame:parse(Serialized, State),

    % Verify the parsed frame matches the original
    ?assertEqual(?OP_HMSG, emqx_nats_frame:type(ParsedFrame)),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(<<"test.subject">>, emqx_nats_frame:subject(ParsedFrame)),
    ?assertEqual(<<"123">>, emqx_nats_frame:sid(ParsedFrame)),
    ?assertEqual(<<"reply.subject">>, emqx_nats_frame:reply_to(ParsedFrame)),
    ?assertEqual(<<"test payload">>, emqx_nats_frame:payload(ParsedFrame)),
    ?assertMatch(
        #{<<"Header1">> := <<"Value1">>, <<"Header2">> := <<"Value2">>},
        emqx_nats_frame:headers(ParsedFrame)
    ).

t_serialize_parse_roundtrip(_Config) ->
    % Test HPUB roundtrip
    HpubMessage = #{
        subject => <<"test.subject">>,
        reply_to => <<"reply.subject">>,
        headers => #{
            <<"Header1">> => <<"Value1">>,
            <<"Header2">> => <<"Value2">>
        },
        payload => <<"test payload">>
    },
    HpubFrame = #nats_frame{operation = ?OP_HPUB, message = HpubMessage},
    HpubSerialized = iolist_to_binary(emqx_nats_frame:serialize_pkt(HpubFrame, #{})),

    % Parse HPUB
    State1 = emqx_nats_frame:initial_parse_state(#{}),
    {ok, ParsedHpubFrame, Rest1, _} = emqx_nats_frame:parse(HpubSerialized, State1),

    % Verify HPUB roundtrip
    ?assertEqual(HpubMessage, emqx_nats_frame:message(ParsedHpubFrame)),
    ?assertEqual(<<>>, Rest1),

    % Test HMSG roundtrip
    HmsgMessage = #{
        subject => <<"test.subject">>,
        sid => <<"123">>,
        reply_to => <<"reply.subject">>,
        headers => #{
            <<"Header1">> => <<"Value1">>,
            <<"Header2">> => <<"Value2">>
        },
        payload => <<"test payload">>
    },
    HmsgFrame = #nats_frame{operation = ?OP_HMSG, message = HmsgMessage},
    HmsgSerialized = iolist_to_binary(emqx_nats_frame:serialize_pkt(HmsgFrame, #{})),

    % Parse HMSG
    State2 = emqx_nats_frame:initial_parse_state(#{}),
    {ok, ParsedHmsgFrame, Rest2, _} = emqx_nats_frame:parse(HmsgSerialized, State2),

    % Verify HMSG roundtrip
    ?assertEqual(HmsgMessage, emqx_nats_frame:message(ParsedHmsgFrame)),
    ?assertEqual(<<>>, Rest2).

t_serialize_empty_headers(_Config) ->
    % Test HPUB with empty headers
    HpubMessage = #{
        subject => <<"test.subject">>,
        headers => #{},
        payload => <<"test payload">>
    },
    HpubFrame = #nats_frame{operation = ?OP_HPUB, message = HpubMessage},
    HpubSerialized = iolist_to_binary(emqx_nats_frame:serialize_pkt(HpubFrame, #{})),

    % Parse HPUB
    State1 = emqx_nats_frame:initial_parse_state(#{}),
    {ok, ParsedHpubFrame, Rest1, _} = emqx_nats_frame:parse(HpubSerialized, State1),

    % Verify HPUB with empty headers
    ?assertEqual(HpubMessage, emqx_nats_frame:message(ParsedHpubFrame)),
    ?assertEqual(<<>>, Rest1),

    % Test HMSG with empty headers
    HmsgMessage = #{
        subject => <<"test.subject">>,
        sid => <<"123">>,
        headers => #{},
        payload => <<"test payload">>
    },
    HmsgFrame = #nats_frame{operation = ?OP_HMSG, message = HmsgMessage},
    HmsgSerialized = iolist_to_binary(emqx_nats_frame:serialize_pkt(HmsgFrame, #{})),

    % Parse HMSG
    State2 = emqx_nats_frame:initial_parse_state(#{}),
    {ok, ParsedHmsgFrame, Rest2, _} = emqx_nats_frame:parse(HmsgSerialized, State2),

    % Verify HMSG with empty headers
    ?assertEqual(HmsgMessage, emqx_nats_frame:message(ParsedHmsgFrame)),
    ?assertEqual(<<>>, Rest2).
