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
%%% emqttc received packet parser.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttc_parser).

-author("feng@emqtt.io").

-include("emqttc_packet.hrl").

%% API
-export([new/0, parse/2]).

%% TODO: REFACTOR...

new() -> none.

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
        {?CONNACK, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<_Reserved:7, SP:1, ReturnCode:8>> = FrameBin,
            wrap(Header, #mqtt_packet_connack{
                ack_flags = SP,
                return_code = ReturnCode }, Rest);
        {?PUBLISH, <<FrameBin:Length/binary, Rest/binary>>} ->
            {TopicName, Rest1} = parse_utf(FrameBin),
            {PacketId, Payload} = case Qos of
                                      0 -> {undefined, Rest1};
                                      _ -> <<Id:16/big, R/binary>> = Rest1,
                                          {Id, R}
                                  end,
            wrap(Header, #mqtt_packet_publish {
                topic_name = TopicName,
                packet_id = PacketId }, Payload, Rest);
        {?PUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{packet_id = PacketId}, Rest);
        {?PUBREC, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{packet_id = PacketId}, Rest);
        {?PUBREL, <<FrameBin:Length/binary, Rest/binary>>} ->
            1 = Qos,
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{ packet_id = PacketId }, Rest);
        {?PUBCOMP, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_puback{ packet_id = PacketId }, Rest);
        {?SUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big, Rest1/binary>> = FrameBin,
            wrap(Header, #mqtt_packet_suback { packet_id = PacketId,
                qos_table = parse_qos(Rest1, []) }, Rest);
        {?UNSUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<PacketId:16/big>> = FrameBin,
            wrap(Header, #mqtt_packet_unsuback { packet_id = PacketId }, Rest);
        {?PINGRESP, Rest} ->
            Length = 0,
            wrap(Header, Rest);
        {_, TooShortBin} ->
            {more, fun(BinMore) ->
                parse_frame(<<TooShortBin/binary, BinMore/binary>>,
                    Header, Length)
            end}
    end.

parse_qos(<<>>, Acc) ->
    lists:reverse(Acc);
parse_qos(<<QoS:8/unsigned, Rest/binary>>, Acc) ->
    parse_qos(Rest, [QoS | Acc]).

%parse_utf(Bin, 0) ->
%    {undefined, Bin};
%parse_utf(Bin, _) ->
%    parse_utf(Bin).

parse_utf(<<Len:16/big, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest}.

%parse_msg(Bin, 0) ->
%    {undefined, Bin};
%parse_msg(<<Len:16/big, Msg:Len/binary, Rest/binary>>, _) ->
%    {Msg, Rest}.

wrap(Header, Variable, Payload, Rest) ->
    {ok, #mqtt_packet{ header = Header,
        variable = Variable,
        payload = Payload }, Rest}.
wrap(Header, Variable, Rest) ->
    {ok, #mqtt_packet { header = Header,
        variable = Variable }, Rest}.
wrap(Header, Rest) ->
    {ok, #mqtt_packet { header = Header }, Rest}.

bool(0) -> false;
bool(1) -> true.
