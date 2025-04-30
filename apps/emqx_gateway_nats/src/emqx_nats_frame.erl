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
    payload/1
]).

-define(INIT_STATE(S, B), #{state := init, buffer := B} = S).
-define(ARGS_STATE(S, B), #{state := args, buffer := B} = S).
-define(PAYLOAD_STATE(S, B), #{state := payload, buffer := B} = S).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

initial_parse_state(_) ->
    #{
        buffer => <<>>,
        %% TODO: support to parse HPUB and HMSG
        headers => false,
        %% init | args | payload
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

serialize_pkt(#nats_frame{operation = Op, message = Message}, _Opts) ->
    Bin1 = serialize_operation(Op),
    case Message of
        undefined ->
            [Bin1, "\r\n"];
        _ ->
            Bin2 = serialize_message(Op, Message),
            [Bin1, " ", Bin2, "\r\n"]
    end.

serialize_operation(?OP_OK) ->
    [?OP_RAW_OK];
serialize_operation(?OP_ERR) ->
    [?OP_RAW_ERR];
serialize_operation(Op) when is_atom(Op) ->
    [string:to_upper(atom_to_list(Op))].

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
    case maps:get(reply_to, Message, undefined) of
        undefined ->
            [Subject, " ", PayloadSize, "\r\n", Payload];
        ReplyTo ->
            [Subject, " ", ReplyTo, " ", PayloadSize, "\r\n", Payload]
    end;
serialize_message(?OP_SUB, Message) ->
    Subject = maps:get(subject, Message),
    Sid = maps:get(sid, Message),
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
    case maps:get(reply_to, Message, undefined) of
        undefined ->
            [Subject, " ", Sid, " ", PayloadSize, "\r\n", Payload];
        ReplyTo ->
            [Subject, " ", Sid, " ", ReplyTo, " ", PayloadSize, "\r\n", Payload]
    end.

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
    maps:get(headers, M, []);
headers(_) ->
    [].

payload(#nats_frame{operation = Op, message = M}) when ?HAS_PAYLOAD_OP(Op) ->
    maps:get(payload, M);
payload(_) ->
    error(badarg).

%%--------------------------------------------------------------------
%% utils
%%--------------------------------------------------------------------

reset(State) ->
    State#{state => init, iframe => undefined, buffer => <<>>}.

to_args_state(State = #{state := init}, Op) ->
    State#{state => args, iframe => #nats_frame{operation = Op}}.

to_payload_state(State = #{state := args, iframe := Frame0}, M0) ->
    Frame = Frame0#nats_frame{message = M0},
    State#{state => payload, iframe => Frame}.

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
    M0 = #{subject => Subject, payload_size => binary_to_integer(PayloadSize)},
    parse_payload(Rest, to_payload_state(State, M0));
do_parse_args(pub, [Subject, ReplyTo, PayloadSize], Rest, State) ->
    M0 = #{subject => Subject, reply_to => ReplyTo, payload_size => binary_to_integer(PayloadSize)},
    parse_payload(Rest, to_payload_state(State, M0));
do_parse_args(sub, [Subject, Sid], Rest, State) ->
    Msg = #{subject => Subject, sid => Sid},
    Frame = #nats_frame{operation = ?OP_SUB, message = Msg},
    {ok, Frame, Rest, reset(State)};
do_parse_args(sub, [Subject, QGroup, Sid], Rest, State) ->
    Msg = #{subject => Subject, sid => Sid, queue_group => QGroup},
    Frame = #nats_frame{operation = ?OP_SUB, message = Msg},
    {ok, Frame, Rest, reset(State)};
do_parse_args(unsub, [Sid], Rest, State) ->
    Msg = #{sid => Sid},
    Frame = #nats_frame{operation = ?OP_UNSUB, message = Msg},
    {ok, Frame, Rest, reset(State)};
do_parse_args(unsub, [Sid, MaxMsgs], Rest, State) ->
    Msg = #{sid => Sid, max_msgs => MaxMsgs},
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
do_parse_args(_Op, _Args, _Rest, _State) ->
    error(invalid_args).

parse_payload(
    Data,
    State = #{
        iframe := #nats_frame{message = #{payload_size := PayloadSize} = Message0} = Frame0
    }
) ->
    Len = byte_size(Data),
    case Len >= (PayloadSize + 2) of
        false ->
            {more, State#{state => payload, buffer => Data}};
        true ->
            Message = Message0#{payload => binary:part(Data, 0, PayloadSize)},
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
