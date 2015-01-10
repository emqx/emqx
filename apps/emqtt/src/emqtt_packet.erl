%%------------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------
%%
%% The Original Code is from RabbitMQ.
%%

-module(emqtt_packet).

-include("emqtt_packet.hrl").

-export([initial_state/0]).

-export([parse/2, serialise/1]).

-define(MAX_LEN, 16#fffffff).
-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).

initial_state() -> none.

parse(<<>>, none) ->
    {more, fun(Bin) -> parse(Bin, none) end};
parse(<<PacketType:4, Dup:1, QoS:2, Retain:1, Rest/binary>>, none) ->
    parse_remaining_len(Rest, #mqtt_packet_header{type   = PacketType,
                                                  dup    = bool(Dup),
                                                  qos    = QoS,
                                                  retain = bool(Retain) });
parse(Bin, Cont) -> Cont(Bin).

parse_remaining_len(<<>>, Header) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Header) end};
parse_remaining_len(Rest, Header) ->
    parse_remaining_len(Rest, Header, 1, 0).

parse_remaining_len(_Bin, _Header, _Multiplier, Length)
  when Length > ?MAX_LEN ->
    {error, invalid_mqtt_frame_len};
parse_remaining_len(<<>>, Header, Multiplier, Length) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Header, Multiplier, Length) end};
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Header, Multiplier, Value) ->
    parse_remaining_len(Rest, Header, Multiplier * ?HIGHBIT, Value + Len * Multiplier);
parse_remaining_len(<<0:1, Len:7, Rest/binary>>, Header,  Multiplier, Value) ->
    parse_frame(Rest, Header, Value + Len * Multiplier).

parse_frame(Bin, #mqtt_packet_header{ type = Type,
                                      qos  = Qos } = Header, Length) ->
    case {Type, Bin} of
        {?CONNECT, <<FrameBin:Length/binary, Rest/binary>>} ->
            {ProtoName, Rest1} = parse_utf(FrameBin),
            <<ProtoVersion : 8, Rest2/binary>> = Rest1,
            <<UsernameFlag : 1,
              PasswordFlag : 1,
              WillRetain   : 1,
              WillQos      : 2,
              WillFlag     : 1,
              CleanSession : 1,
              _Reserved    : 1,
              KeepAlive    : 16/big,
              Rest3/binary>>   = Rest2,
            {ClientId,  Rest4} = parse_utf(Rest3),
            {WillTopic, Rest5} = parse_utf(Rest4, WillFlag),
            {WillMsg,   Rest6} = parse_msg(Rest5, WillFlag),
            {UserName,  Rest7} = parse_utf(Rest6, UsernameFlag),
            {PasssWord, <<>>}  = parse_utf(Rest7, PasswordFlag),
            case protocol_name_approved(ProtoVersion, ProtoName) of
                true ->
                    wrap(Header,
                         #mqtt_packet_connect{
                           proto_ver   = ProtoVersion,
                           will_retain = bool(WillRetain),
                           will_qos    = WillQos,
                           will_flag   = bool(WillFlag),
                           clean_sess  = bool(CleanSession),
                           keep_alive  = KeepAlive,
                           client_id   = ClientId,
                           will_topic  = WillTopic,
                           will_msg    = WillMsg,
                           username    = UserName,
                           password    = PasssWord}, Rest);
               false ->
                    {error, protocol_header_corrupt}
            end;
        {?PUBLISH, <<FrameBin:Length/binary, Rest/binary>>} ->
            {TopicName, Rest1} = parse_utf(FrameBin),
            {PacketId, Payload} = case Qos of
                                       0 -> {undefined, Rest1};
                                       _ -> <<Id:16/big, R/binary>> = Rest1,
                                            {Id, R}
                                   end,
            wrap(Header, #mqtt_packet_publish {topic_name = TopicName,
                                              packet_id = PacketId },
                 Payload, Rest);
        {?PUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{packet_id = PacketId}, Rest);
        {?PUBREC, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{packet_id = PacketId}, Rest);
        {?PUBREL, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{ packet_id = PacketId }, Rest);
        {?PUBCOMP, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{ packet_id = PacketId }, Rest);
        {Subs, <<FrameBin:Length/binary, Rest/binary>>}
          when Subs =:= ?SUBSCRIBE orelse Subs =:= ?UNSUBSCRIBE ->
            1 = Qos,
            <<PacketId:16/big, Rest1/binary>> = FrameBin,
            Topics = parse_topics(Subs, Rest1, []),
            wrap(Header, #mqtt_packet_subscribe { packet_id  = PacketId,
                                                topic_table = Topics }, Rest);
        {Minimal, Rest}
          when Minimal =:= ?DISCONNECT orelse Minimal =:= ?PINGREQ ->
            Length = 0,
            wrap(Header, Rest);
        {_, TooShortBin} ->
            {more, fun(BinMore) ->
                           parse_frame(<<TooShortBin/binary, BinMore/binary>>,
                                       Header, Length)
                   end}
     end.

parse_topics(_, <<>>, Topics) ->
    Topics;
parse_topics(?SUBSCRIBE = Sub, Bin, Topics) ->
    {Name, <<_:6, QoS:2, Rest/binary>>} = parse_utf(Bin),
    parse_topics(Sub, Rest, [#mqtt_topic { name = Name, qos = QoS } | Topics]);
parse_topics(?UNSUBSCRIBE = Sub, Bin, Topics) ->
    {Name, <<Rest/binary>>} = parse_utf(Bin),
    parse_topics(Sub, Rest, [#mqtt_topic { name = Name } | Topics]).

wrap(Header, Variable, Payload, Rest) ->
    {ok, #mqtt_packet{ header = Header, variable = Variable, payload = Payload }, Rest}.
wrap(Header, Variable, Rest) ->
    {ok, #mqtt_packet { header = Header, variable = Variable }, Rest}.
wrap(Header, Rest) ->
    {ok, #mqtt_packet { header = Header }, Rest}.

parse_utf(Bin, 0) ->
    {undefined, Bin};
parse_utf(Bin, _) ->
    parse_utf(Bin).

parse_utf(<<Len:16/big, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest}.

parse_msg(Bin, 0) ->
    {undefined, Bin};
parse_msg(<<Len:16/big, Msg:Len/binary, Rest/binary>>, _) ->
    {Msg, Rest}.

bool(0) -> false;
bool(1) -> true.

%% serialisation

serialise(#mqtt_packet{ header = Header,
                        variable = Variable,
                        payload  = Payload }) ->
    serialise_header(Header, 
                     serialise_variable(Header, Variable, 
                                        serialise_payload(Payload))).

serialise_payload(undefined)           -> <<>>;
serialise_payload(B) when is_binary(B) -> B.

serialise_variable(#mqtt_packet_header  { type        = ?CONNACK },
                   #mqtt_packet_connack { ack_flags = AckFlags,
                                          return_code = ReturnCode },
                   <<>> = PayloadBin) ->
    VariableBin = <<AckFlags:8, ReturnCode:8>>,
    {VariableBin, PayloadBin};

serialise_variable(#mqtt_packet_header { type       = SubAck },
                   #mqtt_packet_suback { packet_id = PacketId,
                                         qos_table  = Qos },
                   <<>> = _PayloadBin)
  when SubAck =:= ?SUBACK orelse SubAck =:= ?UNSUBACK ->
    VariableBin = <<PacketId:16/big>>,
    QosBin = << <<Q:8>> || Q <- Qos >>,
    {VariableBin, QosBin};

serialise_variable(#mqtt_packet_header { type       = ?PUBLISH,
                                         qos        = Qos },
                   #mqtt_packet_publish { topic_name = TopicName,
                                          packet_id  = PacketId },
                   PayloadBin) ->
    TopicBin = serialise_utf(TopicName),
    PacketIdBin = case Qos of
                       0 -> <<>>;
                       1 -> <<PacketId:16/big>>;
                       2 -> <<PacketId:16/big>>
                   end,
    {<<TopicBin/binary, PacketIdBin/binary>>, PayloadBin};

serialise_variable(#mqtt_packet_header { type      = PubAck },
                   #mqtt_packet_puback { packet_id = PacketId },
                   <<>> = _Payload) 
  when PubAck =:= ?PUBACK; PubAck =:= ?PUBREC; 
       PubAck =:= ?PUBREL; PubAck =:= ?PUBCOMP ->
    {PacketIdBin = <<PacketId:16/big>>, <<>>};

serialise_variable(#mqtt_packet_header { },
                   undefined,
                   <<>> = _PayloadBin) ->
    {<<>>, <<>>}.

serialise_header(#mqtt_packet_header{ type   = Type,
                                     dup    = Dup,
                                     qos    = Qos,
                                     retain = Retain }, 
                {VariableBin, PayloadBin})
  when is_integer(Type) andalso ?CONNECT =< Type andalso Type =< ?DISCONNECT ->
    Len = size(VariableBin) + size(PayloadBin),
    true = (Len =< ?MAX_LEN),
    LenBin = serialise_len(Len),
    <<Type:4, (opt(Dup)):1, (opt(Qos)):2, (opt(Retain)):1,
      LenBin/binary, VariableBin/binary, PayloadBin/binary>>.

serialise_utf(String) ->
    StringBin = unicode:characters_to_binary(String),
    Len = size(StringBin),
    true = (Len =< 16#ffff),
    <<Len:16/big, StringBin/binary>>.

serialise_len(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialise_len(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialise_len(N div ?HIGHBIT))/binary>>.

opt(undefined)            -> ?RESERVED;
opt(false)                -> 0;
opt(true)                 -> 1;
opt(X) when is_integer(X) -> X.

protocol_name_approved(Ver, Name) ->
    lists:member({Ver, Name}, ?PROTOCOL_NAMES).

