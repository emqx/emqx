%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqtt packet parser.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqtt_parser).

-author("feng@emqtt.io").

-include("emqtt.hrl").

-include("emqtt_packet.hrl").

%% API
-export([init/1, parse/2]).

-record(mqtt_packet_limit, {max_packet_size}).

-type option() :: {atom(),  any()}.

%%%-----------------------------------------------------------------------------
%% @doc
%% Initialize a parser.
%%
%% @end
%%%-----------------------------------------------------------------------------
-spec init(Opts :: [option()]) -> {none, #mqtt_packet_limit{}}.
init(Opts) -> {none, limit(Opts)}.

limit(Opts) ->
    #mqtt_packet_limit{max_packet_size = proplists:get_value(max_packet_size, Opts, ?MAX_LEN)}.

%%%-----------------------------------------------------------------------------
%% @doc
%% Parse MQTT Packet.
%%
%% @end
%%%-----------------------------------------------------------------------------
-spec parse(binary(), {none, [option()]} | fun()) -> {ok, mqtt_packet()} | {error, any()} | {more, fun()}.
parse(<<>>, {none, Limit}) ->
    {more, fun(Bin) -> parse(Bin, {none, Limit}) end};
parse(<<PacketType:4, Dup:1, QoS:2, Retain:1, Rest/binary>>, {none, Limit}) ->
    parse_remaining_len(Rest, #mqtt_packet_header{type   = PacketType,
                                                  dup    = bool(Dup),
                                                  qos    = QoS,
                                                  retain = bool(Retain)}, Limit);
parse(Bin, Cont) -> Cont(Bin).

parse_remaining_len(<<>>, Header, Limit) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Header, Limit) end};
parse_remaining_len(Rest, Header, Limit) ->
    parse_remaining_len(Rest, Header, 1, 0, Limit).

parse_remaining_len(_Bin, _Header, _Multiplier, Length, #mqtt_packet_limit{max_packet_size = MaxLen})
    when Length > MaxLen ->
    {error, invalid_mqtt_frame_len};
parse_remaining_len(<<>>, Header, Multiplier, Length, Limit) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Header, Multiplier, Length, Limit) end};
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Header, Multiplier, Value, Limit) ->
    parse_remaining_len(Rest, Header, Multiplier * ?HIGHBIT, Value + Len * Multiplier, Limit);
parse_remaining_len(<<0:1, Len:7, Rest/binary>>, Header,  Multiplier, Value, #mqtt_packet_limit{max_packet_size = MaxLen}) ->
    FrameLen = Value + Len * Multiplier,
    if
        FrameLen > MaxLen -> {error, invalid_mqtt_frame_len};
        true -> parse_frame(Rest, Header, FrameLen)
    end.

parse_frame(Bin, #mqtt_packet_header{type = Type, qos  = Qos} = Header, Length) ->
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
                           proto_name  = ProtoName,
                           will_retain = bool(WillRetain),
                           will_qos    = WillQos,
                           will_flag   = bool(WillFlag),
                           clean_sess  = bool(CleanSession),
                           keep_alive  = KeepAlive,
                           clientid   = ClientId,
                           will_topic  = WillTopic,
                           will_msg    = WillMsg,
                           username    = UserName,
                           password    = PasssWord}, Rest);
               false ->
                    {error, protocol_header_corrupt}
            end;
        %{?CONNACK, <<FrameBin:Length/binary, Rest/binary>>} ->
        %    <<_Reserved:7, SP:1, ReturnCode:8>> = FrameBin,
        %    wrap(Header, #mqtt_packet_connack{ack_flags = SP,
        %                                      return_code = ReturnCode }, Rest);
        {?PUBLISH, <<FrameBin:Length/binary, Rest/binary>>} ->
            {TopicName, Rest1} = parse_utf(FrameBin),
            {PacketId, Payload} = case Qos of
                                      0 -> {undefined, Rest1};
                                      _ -> <<Id:16/big, R/binary>> = Rest1,
                                          {Id, R}
                                  end,
            wrap(Header, #mqtt_packet_publish{topic_name = TopicName,
                                              packet_id = PacketId},
                 Payload, Rest);
        {?PUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{packet_id = PacketId}, Rest);
        {?PUBREC, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{packet_id = PacketId}, Rest);
        {?PUBREL, <<FrameBin:Length/binary, Rest/binary>>} ->
            1 = Qos,
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{packet_id = PacketId}, Rest);
        {?PUBCOMP, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{packet_id = PacketId}, Rest);
        {?SUBSCRIBE, <<FrameBin:Length/binary, Rest/binary>>} ->
            1 = Qos,
            <<PacketId:16/big, Rest1/binary>> = FrameBin,
            TopicTable = parse_topics(?SUBSCRIBE, Rest1, []),
            wrap(Header, #mqtt_packet_subscribe{packet_id   = PacketId,
                                                topic_table = TopicTable}, Rest);
        %{?SUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
        %    <<PacketId:16/big, Rest1/binary>> = FrameBin,
        %    wrap(Header, #mqtt_packet_suback{packet_id = PacketId,
        %                                     qos_table = parse_qos(Rest1, []) }, Rest);
        {?UNSUBSCRIBE, <<FrameBin:Length/binary, Rest/binary>>} ->
            1 = Qos,
            <<PacketId:16/big, Rest1/binary>> = FrameBin,
            Topics = parse_topics(?UNSUBSCRIBE, Rest1, []),
            wrap(Header, #mqtt_packet_unsubscribe{packet_id = PacketId,
                                                  topics    = Topics}, Rest);
        %{?UNSUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
        %    <<PacketId:16/big>> = FrameBin,
        %    wrap(Header, #mqtt_packet_unsuback { packet_id = PacketId }, Rest);
        {?PINGREQ, Rest} ->
            Length = 0,
            wrap(Header, Rest);
        %{?PINGRESP, Rest} ->
        %    Length = 0,
        %    wrap(Header, Rest);
        {?DISCONNECT, Rest} ->
            Length = 0,
            wrap(Header, Rest);
        {_, TooShortBin} ->
            {more, fun(BinMore) ->
                parse_frame(<<TooShortBin/binary, BinMore/binary>>,
                    Header, Length)
            end}
    end.

wrap(Header, Variable, Payload, Rest) ->
    {ok, #mqtt_packet{header = Header, variable = Variable, payload = Payload}, Rest}.
wrap(Header, Variable, Rest) ->
    {ok, #mqtt_packet{header = Header, variable = Variable}, Rest}.
wrap(Header, Rest) ->
    {ok, #mqtt_packet{header = Header}, Rest}.

%client function
%parse_qos(<<>>, Acc) ->
%    lists:reverse(Acc);
%parse_qos(<<QoS:8/unsigned, Rest/binary>>, Acc) ->
%    parse_qos(Rest, [QoS | Acc]).

parse_topics(_, <<>>, Topics) ->
    Topics;
parse_topics(?SUBSCRIBE = Sub, Bin, Topics) ->
    {Name, <<_:6, QoS:2, Rest/binary>>} = parse_utf(Bin),
    parse_topics(Sub, Rest, [{Name, QoS}| Topics]);
parse_topics(?UNSUBSCRIBE = Sub, Bin, Topics) ->
    {Name, <<Rest/binary>>} = parse_utf(Bin),
    parse_topics(Sub, Rest, [Name | Topics]).

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

protocol_name_approved(Ver, Name) ->
    lists:member({Ver, Name}, ?PROTOCOL_NAMES).

