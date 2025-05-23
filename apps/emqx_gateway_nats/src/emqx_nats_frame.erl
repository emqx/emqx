%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_frame).

-behaviour(emqx_gateway_frame).

-include("emqx_nats.hrl").

-export([
    initial_parse_state/1,
    serialize_opts/0,
    serialize_pkt/2,
    parse/2,
    format/1,
    type/1,
    is_message/1
]).

-export([
    message/1,
    subject/1,
    sid/1,
    queue_group/1,
    reply_to/1,
    headers/1,
    payload/1,
    max_msgs/1
]).

-define(INIT_STATE(S, B), #{state := init, buffer := B} = S).
-define(ARGS_STATE(S, B), #{state := args, buffer := B} = S).
-define(HEADERS_STATE(S, B), #{state := headers, buffer := B} = S).
-define(PAYLOAD_STATE(S, B), #{state := payload, buffer := B} = S).

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

initial_parse_state(_) ->
    #{
        buffer => <<>>,
        headers => false,
        %% init | args | headers | payload
        state => init,
        %% in parsing frame, the current frame is stored in iframe
        iframe => undefined
    }.

parse(Data0, ?INIT_STATE(State, Buffer)) ->
    Data = <<Buffer/binary, Data0/binary>>,
    parse_operation(Data, State);
parse(Data0, ?ARGS_STATE(State, Buffer)) ->
    Data = <<Buffer/binary, Data0/binary>>,
    parse_args(Data, State#{buffer => <<>>});
parse(Data0, ?HEADERS_STATE(State, Buffer)) ->
    Data = <<Buffer/binary, Data0/binary>>,
    parse_headers(Data, State#{buffer => <<>>});
parse(Data0, ?PAYLOAD_STATE(State, Buffer)) ->
    Data = <<Buffer/binary, Data0/binary>>,
    parse_payload(Data, State#{buffer => <<>>}).

%%--------------------------------------------------------------------
%% Control frames

parse_operation(<<"PING\r\n", Rest/binary>>, State) ->
    return_ping(Rest, State);
parse_operation(<<"ping\r\n", Rest/binary>>, State) ->
    return_ping(Rest, State);
parse_operation(<<"PONG\r\n", Rest/binary>>, State) ->
    return_pong(Rest, State);
parse_operation(<<"pong\r\n", Rest/binary>>, State) ->
    return_pong(Rest, State);
parse_operation(<<"CONNECT ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_CONNECT));
parse_operation(<<"connect ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_CONNECT));
parse_operation(<<"INFO ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_INFO));
parse_operation(<<"info ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_INFO));
parse_operation(<<"+OK\r\n", Rest/binary>>, State) ->
    return_ok(Rest, State);
parse_operation(<<"+ok\r\n", Rest/binary>>, State) ->
    return_ok(Rest, State);
parse_operation(<<"-ERR ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_ERR));
parse_operation(<<"-err ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_ERR));
%%--------------------------------------------------------------------
%% Message frames

parse_operation(<<"PUB ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_PUB));
parse_operation(<<"pub ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_PUB));
parse_operation(<<"SUB ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_SUB));
parse_operation(<<"sub ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_SUB));
parse_operation(<<"UNSUB ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_UNSUB));
parse_operation(<<"unsub ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_UNSUB));
parse_operation(<<"MSG ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_MSG));
parse_operation(<<"msg ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_MSG));
parse_operation(<<"HPUB ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_HPUB));
parse_operation(<<"hpub ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_HPUB));
parse_operation(<<"HMSG ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_HMSG));
parse_operation(<<"hmsg ", Rest/binary>>, State) ->
    parse_args(Rest, to_args_state(State, ?OP_HMSG));
parse_operation(Data, State) ->
    case is_prefix_of_any(Data, ?ALL_OPS) of
        true ->
            {more, State#{buffer => Data}};
        false ->
            error({unknown_operation, Data})
    end.

is_prefix_of_any(Data, Ops) ->
    DataLen = byte_size(Data),
    lists:any(
        fun(Op) ->
            case Op of
                <<Data:DataLen/binary, _/binary>> ->
                    true;
                _ ->
                    false
            end
        end,
        Ops
    ).

%%--------------------------------------------------------------------
%% Serialize frames
%%--------------------------------------------------------------------

serialize_opts() ->
    #{}.

serialize_pkt(#nats_frame{operation = Op, message = Message}, Opts) ->
    UseLowerHeader = maps:get(use_lower_header, Opts, false),
    Bin1 = serialize_operation(Op, UseLowerHeader),
    case Message of
        undefined ->
            [Bin1, "\r\n"];
        _ ->
            Bin2 = serialize_message(Op, Message),
            [Bin1, " ", Bin2, "\r\n"]
    end.

serialize_operation(?OP_OK, _UseLowerHeader = false) ->
    [?OP_RAW_OK];
serialize_operation(?OP_OK, _UseLowerHeader = true) ->
    [?OP_RAW_OK_L];
serialize_operation(?OP_ERR, _UseLowerHeader = false) ->
    [?OP_RAW_ERR];
serialize_operation(?OP_ERR, _UseLowerHeader = true) ->
    [?OP_RAW_ERR_L];
serialize_operation(Op, _UseLowerHeader = false) when is_atom(Op) ->
    [string:to_upper(atom_to_list(Op))];
serialize_operation(Op, _UseLowerHeader = true) ->
    [string:to_lower(atom_to_list(Op))].

serialize_message(?OP_INFO, Message) ->
    [emqx_utils_json:encode(Message)];
serialize_message(?OP_CONNECT, Message) ->
    [emqx_utils_json:encode(Message)];
serialize_message(?OP_ERR, Message) ->
    [Message];
serialize_message(?OP_PUB, Message) ->
    Subject = maps:get(subject, Message),
    Payload = maps:get(payload, Message),
    PayloadSize = integer_to_list(byte_size(Payload)),
    ok = validate_non_wildcard_subject(Subject),
    case maps:get(reply_to, Message, undefined) of
        undefined ->
            [Subject, " ", PayloadSize, "\r\n", Payload];
        ReplyTo ->
            [Subject, " ", ReplyTo, " ", PayloadSize, "\r\n", Payload]
    end;
serialize_message(?OP_HPUB, Message) ->
    Subject = maps:get(subject, Message),
    Headers = maps:get(headers, Message, #{}),
    HeadersBin0 = serialize_headers(Headers),
    HeadersBin = <<HeadersBin0/binary, "\r\n">>,
    HeadersSize = integer_to_list(byte_size(HeadersBin)),
    Payload = maps:get(payload, Message),
    TotalSize = integer_to_list(byte_size(Payload) + byte_size(HeadersBin)),
    ok = validate_non_wildcard_subject(Subject),
    case maps:get(reply_to, Message, undefined) of
        undefined ->
            [Subject, " ", HeadersSize, " ", TotalSize, "\r\n", HeadersBin, Payload];
        ReplyTo ->
            [Subject, " ", ReplyTo, " ", HeadersSize, " ", TotalSize, "\r\n", HeadersBin, Payload]
    end;
serialize_message(?OP_SUB, Message) ->
    Subject = maps:get(subject, Message),
    Sid = maps:get(sid, Message),
    ok = validate_subject(Subject),
    case maps:get(queue_group, Message, undefined) of
        undefined ->
            [Subject, " ", Sid];
        QGroup ->
            [Subject, " ", QGroup, " ", Sid]
    end;
serialize_message(?OP_UNSUB, Message) ->
    Sid = maps:get(sid, Message),
    case maps:get(max_msgs, Message, 0) of
        0 ->
            [Sid];
        MaxMsgs when is_integer(MaxMsgs) ->
            [Sid, " ", integer_to_list(MaxMsgs)]
    end;
serialize_message(?OP_MSG, Message) ->
    Subject = maps:get(subject, Message),
    Sid = maps:get(sid, Message),
    Payload = maps:get(payload, Message),
    PayloadSize = integer_to_list(byte_size(Payload)),
    ok = validate_non_wildcard_subject(Subject),
    case maps:get(reply_to, Message, undefined) of
        undefined ->
            [Subject, " ", Sid, " ", PayloadSize, "\r\n", Payload];
        ReplyTo ->
            [Subject, " ", Sid, " ", ReplyTo, " ", PayloadSize, "\r\n", Payload]
    end;
serialize_message(?OP_HMSG, Message) ->
    Subject = maps:get(subject, Message),
    Sid = maps:get(sid, Message),
    Headers = maps:get(headers, Message, #{}),
    HeadersBin0 = serialize_headers(Headers),
    HeadersBin = <<HeadersBin0/binary, "\r\n">>,
    HeadersSize = integer_to_list(byte_size(HeadersBin)),
    Payload = maps:get(payload, Message),
    TotalSize = integer_to_list(byte_size(Payload) + byte_size(HeadersBin)),
    case maps:get(reply_to, Message, undefined) of
        undefined ->
            [Subject, " ", Sid, " ", HeadersSize, " ", TotalSize, "\r\n", HeadersBin, Payload];
        ReplyTo ->
            [
                Subject,
                " ",
                Sid,
                " ",
                ReplyTo,
                " ",
                HeadersSize,
                " ",
                TotalSize,
                "\r\n",
                HeadersBin,
                Payload
            ]
    end;
serialize_message(Op, _Message) ->
    error({unknown_operation, Op}).

serialize_headers(Headers) when is_map(Headers) ->
    HeaderPrefix =
        case maps:get(<<"code">>, Headers, false) of
            false ->
                <<"NATS/1.0\r\n">>;
            503 ->
                %% Only 503 is supported for the no_responders fast fail
                <<"NATS/1.0 503\r\n">>
        end,
    serialize_headers(maps:to_list(maps:remove(<<"code">>, Headers)), HeaderPrefix).

serialize_headers([], Acc) ->
    Acc;
serialize_headers([{Key, Value} | Rest], Acc) ->
    serialize_headers(Rest, <<Acc/binary, Key/binary, ": ", Value/binary, "\r\n">>).

format(#nats_frame{operation = Op, message = undefined}) ->
    io_lib:format("~s", [Op]);
format(#nats_frame{operation = Op, message = Message}) ->
    io_lib:format("~s: ~0p", [Op, Message]).

type(#nats_frame{operation = Op}) ->
    Op.

is_message(#nats_frame{operation = Op}) ->
    Op =:= ?OP_MSG orelse
        Op =:= ?OP_HMSG orelse
        Op =:= ?OP_PUB orelse
        Op =:= ?OP_HPUB.

%%--------------------------------------------------------------------
%% Getter

message(#nats_frame{message = M}) ->
    M.

subject(#nats_frame{operation = Op, message = M}) when ?HAS_SUBJECT_OP(Op) ->
    maps:get(subject, M, undefined);
subject(_) ->
    error(badarg).

sid(#nats_frame{operation = Op, message = M}) when ?HAS_SID_OP(Op) ->
    maps:get(sid, M, undefined);
sid(_) ->
    error(badarg).

queue_group(#nats_frame{operation = Op, message = M}) when ?HAS_QUEUE_GROUP_OP(Op) ->
    maps:get(queue_group, M, undefined);
queue_group(_) ->
    error(badarg).

reply_to(#nats_frame{operation = Op, message = M}) when ?HAS_REPLY_TO_OP(Op) ->
    maps:get(reply_to, M, undefined);
reply_to(_) ->
    error(badarg).

headers(#nats_frame{operation = Op, message = M}) when ?HAS_HEADERS_OP(Op) ->
    maps:get(headers, M, #{});
headers(_) ->
    #{}.

payload(#nats_frame{operation = Op, message = M}) when ?HAS_PAYLOAD_OP(Op) ->
    maps:get(payload, M);
payload(_) ->
    error(badarg).

max_msgs(#nats_frame{operation = ?OP_UNSUB, message = M}) ->
    maps:get(max_msgs, M, 0);
max_msgs(_) ->
    error(badarg).

%%--------------------------------------------------------------------
%% utils
%%--------------------------------------------------------------------

reset(State) ->
    State#{state => init, iframe => undefined, buffer => <<>>}.

to_args_state(State = #{state := init}, Op) ->
    State#{state => args, iframe => #nats_frame{operation = Op}}.

to_payload_state(State = #{state := S, iframe := Frame0}, M0) when
    S == args;
    S == headers
->
    Frame = Frame0#nats_frame{message = M0},
    State#{state => payload, iframe => Frame}.

to_header_state(State = #{state := args, iframe := Frame0}, M0) ->
    Frame = Frame0#nats_frame{message = M0},
    State#{state => headers, iframe => Frame}.

return_ping(Rest, State) ->
    {ok, #nats_frame{operation = ?OP_PING}, Rest, reset(State)}.

return_pong(Rest, State) ->
    {ok, #nats_frame{operation = ?OP_PONG}, Rest, reset(State)}.

return_ok(Rest, State) ->
    {ok, #nats_frame{operation = ?OP_OK}, Rest, reset(State)}.

parse_args(Data, State = #{state := args, iframe := #nats_frame{operation = Op}}) ->
    case split_to_first_linefeed(Data) of
        false ->
            {more, State#{buffer => Data}};
        {Line, Rest} ->
            pre_do_parse_args(Op, Line, Rest, State)
    end.

pre_do_parse_args(Op, JsonStr, Rest, State) when Op == ?OP_INFO; Op == ?OP_CONNECT ->
    case emqx_utils_json:safe_decode(JsonStr, [return_maps]) of
        {ok, Message} ->
            {ok, #nats_frame{operation = Op, message = Message}, Rest, reset(State)};
        {error, _} ->
            error(invalid_json_payload)
    end;
pre_do_parse_args(?OP_ERR, ErrorMessage, Rest, State) ->
    {ok, #nats_frame{operation = ?OP_ERR, message = ErrorMessage}, Rest, reset(State)};
pre_do_parse_args(Op, Line, Rest, State) ->
    Args = binary:split(Line, <<" ">>, [global]),
    do_parse_args(Op, Args, Rest, State).

do_parse_args(pub, [Subject, PayloadSize], Rest, State) ->
    ok = validate_non_wildcard_subject(Subject),
    M0 = #{subject => Subject, payload_size => binary_to_integer(PayloadSize)},
    parse_payload(Rest, to_payload_state(State, M0));
do_parse_args(pub, [Subject, ReplyTo, PayloadSize], Rest, State) ->
    ok = validate_non_wildcard_subject(Subject),
    M0 = #{subject => Subject, reply_to => ReplyTo, payload_size => binary_to_integer(PayloadSize)},
    parse_payload(Rest, to_payload_state(State, M0));
do_parse_args(hpub, [Subject, HeadersSize0, TotalSize0], Rest, State) ->
    ok = validate_non_wildcard_subject(Subject),
    HeadersSize = binary_to_integer(HeadersSize0),
    TotalSize = binary_to_integer(TotalSize0),
    M0 = #{
        subject => Subject,
        headers_size => HeadersSize,
        payload_size => TotalSize - HeadersSize
    },
    parse_headers(Rest, to_header_state(State, M0));
do_parse_args(hpub, [Subject, ReplyTo, HeadersSize0, TotalSize0], Rest, State) ->
    ok = validate_non_wildcard_subject(Subject),
    HeadersSize = binary_to_integer(HeadersSize0),
    TotalSize = binary_to_integer(TotalSize0),
    M0 = #{
        subject => Subject,
        reply_to => ReplyTo,
        headers_size => HeadersSize,
        payload_size => TotalSize - HeadersSize
    },
    parse_headers(Rest, to_header_state(State, M0));
do_parse_args(sub, [Subject, Sid], Rest, State) ->
    ok = validate_subject(Subject),
    Msg = #{subject => Subject, sid => Sid},
    Frame = #nats_frame{operation = ?OP_SUB, message = Msg},
    {ok, Frame, Rest, reset(State)};
do_parse_args(sub, [Subject, QGroup, Sid], Rest, State) ->
    ok = validate_subject(Subject),
    Msg =
        case QGroup of
            <<>> ->
                #{subject => Subject, sid => Sid};
            QGroup ->
                #{subject => Subject, sid => Sid, queue_group => QGroup}
        end,
    Frame = #nats_frame{operation = ?OP_SUB, message = Msg},
    {ok, Frame, Rest, reset(State)};
do_parse_args(unsub, [Sid], Rest, State) ->
    Msg = #{sid => Sid},
    Frame = #nats_frame{operation = ?OP_UNSUB, message = Msg},
    {ok, Frame, Rest, reset(State)};
do_parse_args(unsub, [Sid, MaxMsgs], Rest, State) ->
    Msg = #{sid => Sid, max_msgs => binary_to_integer(MaxMsgs)},
    Frame = #nats_frame{operation = ?OP_UNSUB, message = Msg},
    {ok, Frame, Rest, reset(State)};
do_parse_args(msg, [Subject, Sid, PayloadSize], Rest, State) ->
    M0 = #{subject => Subject, sid => Sid, payload_size => binary_to_integer(PayloadSize)},
    parse_payload(Rest, to_payload_state(State, M0));
do_parse_args(msg, [Subject, Sid, ReplyTo, PayloadSize], Rest, State) ->
    M0 = #{
        subject => Subject,
        sid => Sid,
        reply_to => ReplyTo,
        payload_size => binary_to_integer(PayloadSize)
    },
    parse_payload(Rest, to_payload_state(State, M0));
do_parse_args(hmsg, [Subject, Sid, HeadersSize0, TotalSize0], Rest, State) ->
    HeadersSize = binary_to_integer(HeadersSize0),
    TotalSize = binary_to_integer(TotalSize0),
    M0 = #{
        subject => Subject,
        sid => Sid,
        headers_size => HeadersSize,
        payload_size => TotalSize - HeadersSize
    },
    parse_headers(Rest, to_header_state(State, M0));
do_parse_args(hmsg, [Subject, Sid, ReplyTo, HeadersSize0, TotalSize0], Rest, State) ->
    HeadersSize = binary_to_integer(HeadersSize0),
    TotalSize = binary_to_integer(TotalSize0),
    M0 = #{
        subject => Subject,
        sid => Sid,
        reply_to => ReplyTo,
        headers_size => HeadersSize,
        payload_size => TotalSize - HeadersSize
    },
    parse_headers(Rest, to_header_state(State, M0));
do_parse_args(_Op, _Args, _Rest, _State) ->
    error(invalid_args).

parse_headers(
    Data,
    State = #{
        state := headers,
        iframe := #nats_frame{message = #{headers_size := HeadersSize} = Message0}
    }
) ->
    %% The size of the headers section in bytes
    %% including the \r\n\r\n delimiter before the payload.
    Len = byte_size(Data),
    case Len >= HeadersSize of
        false ->
            {more, State#{state => headers, buffer => Data}};
        true ->
            HeadersBin = binary:part(Data, 0, HeadersSize),
            Headers = parse_headers_binary(HeadersBin),
            Message1 = maps:remove(headers_size, Message0),
            Message = Message1#{headers => Headers},
            Rest0 = binary:part(Data, HeadersSize, Len - HeadersSize),
            parse_payload(Rest0, to_payload_state(State, Message))
    end.

parse_headers_binary(Bin) ->
    {Headers0, HeaderAndValuePart} = parse_headers_version_and_code(Bin),
    case binary:split(HeaderAndValuePart, [<<"\r\n">>, <<": ">>, <<":">>], [global]) of
        L when length(L) rem 2 == 0 ->
            convert_header_list_to_map(L, Headers0);
        _ ->
            error(invalid_headers_binary)
    end.

parse_headers_version_and_code(Bin) ->
    case binary:split(Bin, [<<"\r\n">>]) of
        [VersionAndCode, HeaderAndValuePart] ->
            case binary:split(VersionAndCode, [<<" ">>]) of
                [<<"NATS/1.0">>, Code] ->
                    {#{<<"code">> => binary_to_integer(Code)}, HeaderAndValuePart};
                [<<"NATS/1.0">>] ->
                    {#{}, HeaderAndValuePart};
                _ ->
                    error(invalid_headers_binary)
            end;
        _ ->
            error(invalid_headers_binary)
    end.

%% End with [<<>>, <<>>] due to the header section is end with \r\n\r\n
convert_header_list_to_map([<<>>, <<>>], Map) ->
    Map;
convert_header_list_to_map([Header, Value | Rest], Map) ->
    convert_header_list_to_map(Rest, Map#{Header => Value}).

parse_payload(
    Data,
    State = #{
        state := payload,
        iframe := #nats_frame{message = #{payload_size := PayloadSize} = Message0} = Frame0
    }
) ->
    Len = byte_size(Data),
    case Len >= (PayloadSize + 2) of
        false ->
            {more, State#{state => payload, buffer => Data}};
        true ->
            Message1 = maps:remove(payload_size, Message0),
            Message = Message1#{payload => binary:part(Data, 0, PayloadSize)},
            Rest0 = binary:part(Data, PayloadSize + 2, Len - PayloadSize - 2),
            {ok, Frame0#nats_frame{message = Message}, Rest0, reset(State)}
    end.

split_to_first_linefeed(Bin) when is_binary(Bin) ->
    case binary:match(Bin, <<"\r\n">>) of
        {Pos, 2} ->
            {binary:part(Bin, 0, Pos), binary:part(Bin, Pos + 2, byte_size(Bin) - Pos - 2)};
        nomatch ->
            false
    end.

validate_non_wildcard_subject(Subject) ->
    case emqx_nats_topic:validate_nats_subject(Subject) of
        {ok, false} ->
            ok;
        {ok, true} ->
            error({invalid_subject, wildcard_subject_not_allowed_in_pub_message});
        {error, Reason} ->
            error({invalid_subject, Reason})
    end.

validate_subject(Subject) ->
    case emqx_nats_topic:validate_nats_subject(Subject) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            error({invalid_subject, Reason})
    end.
