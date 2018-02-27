%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_parser).

-author("Feng Lee <feng@emqtt.io>").

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

%% API
-export([initial_state/0, initial_state/1, parse/2]).

-type(max_packet_size() :: 1..?MAX_PACKET_SIZE).

-spec(initial_state() -> {none, max_packet_size()}).
initial_state() ->
    initial_state(?MAX_PACKET_SIZE).

%% @doc Initialize a parser
-spec(initial_state(max_packet_size()) -> {none, max_packet_size()}).
initial_state(MaxSize) ->
    {none, MaxSize}.

%% @doc Parse MQTT Packet
-spec(parse(binary(), {none, pos_integer()} | fun())
            -> {ok, mqtt_packet()} | {error, term()} | {more, fun()}).
parse(<<>>, {none, MaxLen}) ->
    {more, fun(Bin) -> parse(Bin, {none, MaxLen}) end};
parse(<<Type:4, Dup:1, QoS:2, Retain:1, Rest/binary>>, {none, Limit}) ->
    parse_remaining_len(Rest, #mqtt_packet_header{type   = Type,
                                                  dup    = bool(Dup),
                                                  qos    = fixqos(Type, QoS),
                                                  retain = bool(Retain)}, Limit);
parse(Bin, Cont) -> Cont(Bin).

parse_remaining_len(<<>>, Header, Limit) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Header, Limit) end};
parse_remaining_len(Rest, Header, Limit) ->
    parse_remaining_len(Rest, Header, 1, 0, Limit).

parse_remaining_len(_Bin, _Header, _Multiplier, Length, MaxLen)
    when Length > MaxLen ->
    {error, invalid_mqtt_frame_len};
parse_remaining_len(<<>>, Header, Multiplier, Length, Limit) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Header, Multiplier, Length, Limit) end};
%% optimize: match PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK...
parse_remaining_len(<<0:1, 2:7, Rest/binary>>, Header, 1, 0, _Limit) ->
    parse_frame(Rest, Header, 2);
%% optimize: match PINGREQ...
parse_remaining_len(<<0:8, Rest/binary>>, Header, 1, 0, _Limit) ->
    parse_frame(Rest, Header, 0);
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Header, Multiplier, Value, Limit) ->
    parse_remaining_len(Rest, Header, Multiplier * ?HIGHBIT, Value + Len * Multiplier, Limit);
parse_remaining_len(<<0:1, Len:7, Rest/binary>>, Header,  Multiplier, Value, MaxLen) ->
    FrameLen = Value + Len * Multiplier,
    if
        FrameLen > MaxLen -> {error, invalid_mqtt_frame_len};
        true -> parse_frame(Rest, Header, FrameLen)
    end.

parse_frame(Bin, #mqtt_packet_header{type = Type, qos = Qos} = Header, Length) ->
    case {Type, Bin} of
        {?CONNECT, <<FrameBin:Length/binary, Rest/binary>>} ->
            {ProtoName, Rest1} = parse_utf(FrameBin),
            %% Fix mosquitto bridge: 0x83, 0x84
            <<BridgeTag:4, ProtoVer:4, Rest2/binary>> = Rest1,
            <<UsernameFlag : 1,
              PasswordFlag : 1,
              WillRetain   : 1,
              WillQos      : 2,
              WillFlag     : 1,
              CleanSess    : 1,
              _Reserved    : 1,
              KeepAlive    : 16/big,
              Rest3/binary>>   = Rest2,
            {Properties, Rest4} = parse_properties(ProtoVer, Rest3),
            {ClientId,  Rest5} = parse_utf(Rest4),
            {WillProps, Rest6} = parse_will_props(Rest5, ProtoVer, WillFlag),
            {WillTopic, Rest7} = parse_utf(Rest6, WillFlag),
            {WillMsg,   Rest8} = parse_msg(Rest7, WillFlag),
            {UserName,  Rest9} = parse_utf(Rest8, UsernameFlag),
            {PasssWord, <<>>}  = parse_utf(Rest9, PasswordFlag),
            case protocol_name_approved(ProtoVersion, ProtoName) of
                true ->
                    wrap(Header,
                         #mqtt_packet_connect{
                           proto_ver   = ProtoVer,
                           proto_name  = ProtoName,
                           will_retain = bool(WillRetain),
                           will_qos    = WillQos,
                           will_flag   = bool(WillFlag),
                           clean_sess  = bool(CleanSess),
                           keep_alive  = KeepAlive,
                           client_id   = ClientId,
                           will_props  = WillProps,
                           will_topic  = WillTopic,
                           will_msg    = WillMsg,
                           username    = UserName,
                           password    = PasssWord,
                           is_bridge   = (BridgeTag =:= 8),
                           properties  = Properties}, Rest);
               false ->
                    {error, protocol_header_corrupt}
            end;
        %{?CONNACK, <<FrameBin:Length/binary, Rest/binary>>} ->
        %    <<_Reserved:7, SP:1, ReturnCode:8>> = FrameBin,
        %    wrap(Header, #mqtt_packet_connack{ack_flags = SP,
        %                                      return_code = ReturnCode }, Rest);
        {?PUBLISH, <<FrameBin:Length/binary, Rest/binary>>} ->
            {TopicName, Rest1} = parse_utf(FrameBin),
            {PacketId, Rest2} = case Qos of
                                    0 -> {undefined, Rest1};
                                    _ -> <<Id:16/big, R/binary>> = Rest1,
                                         {Id, R}
                                end,
            {Properties, Payload} = parse_properties(ProtoVer, Rest),
            wrap(fixdup(Header), #mqtt_packet_publish{topic_name = TopicName,
                                                      packet_id  = PacketId,
                                                      properties = Properties},
                 Payload, Rest);
        {PubAck, <<FrameBin:Length/binary, Rest/binary>>}
          when PubAck == ?PUBACK; PubAck == ?PUBREC; PubAck == ?PUBREL; PubAck == ?PUBCOMP ->
            <<PacketId:16/big, Rest1/binary>> = FrameBin,
            case ProtoVer == ?MQTT_PROTO_V5 of
                true ->
                    <<ReasonCode, Rest2/binary>> = Rest1,
                    {Properties, Rest3} = parse_properties(ProtoVer, Rest2),
                    wrap(Header, #mqtt_packet_puback{packet_id   = PacketId,
                                                     reason_code = ReasonCode,
                                                     properties  = Properties}, Rest3);
                false ->
                    wrap(Header, #mqtt_packet_puback{packet_id = PacketId}, Rest)
            end;
        {?SUBSCRIBE, <<FrameBin:Length/binary, Rest/binary>>} ->
            %% 1 = Qos,
            <<PacketId:16/big, Rest1/binary>> = FrameBin,
            {Properties, Rest2} = parse_properties(ProtoVer, Rest1),
            TopicTable = parse_topics(?SUBSCRIBE, Rest1, []),
            wrap(Header, #mqtt_packet_subscribe{packet_id   = PacketId,
                                                properties  = Properties,
                                                topic_table = TopicTable}, Rest);
        %{?SUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
        %    <<PacketId:16/big, Rest1/binary>> = FrameBin,
        %    {Properties, Rest2/binary>> = parse_properties(ProtoVer, Rest1),
        %    wrap(Header, #mqtt_packet_suback{packet_id = PacketId, properties = Properties,
        %                                     reason_codes = parse_qos(Rest1, [])}, Rest);
        {?UNSUBSCRIBE, <<FrameBin:Length/binary, Rest/binary>>} ->
            %% 1 = Qos,
            <<PacketId:16/big, Rest1/binary>> = FrameBin,
            {Properties, Rest2} = parse_properties(ProtoVer, Rest1),
            Topics = parse_topics(?UNSUBSCRIBE, Rest2, []),
            wrap(Header, #mqtt_packet_unsubscribe{packet_id  = PacketId,
                                                  properties = Properties,
                                                  topics     = Topics}, Rest);
        %{?UNSUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
        %    <<PacketId:16/big, Rest1/binary>> = FrameBin,
        %    {Properties, Rest2} = parse_properties(ProtoVer, Rest1),
        %    wrap(Header, #mqtt_packet_unsuback {
        %       packet_id = PacketId,
        %       properties = Properties }, Rest);
        {?PINGREQ, Rest} ->
            Length = 0,
            wrap(Header, Rest);
        %{?PINGRESP, Rest} ->
        %    Length = 0,
        %    wrap(Header, Rest);
        {?DISCONNECT, <<FrameBin:Length/binary, Rest/binary>>} ->
            case ProtoVer == ?MQTT_PROTO_V5 of
                true ->
                    <<ReasonCode, Rest1/binary>> = Rest,
                    {Properties, Rest2} = parse_properties(ProtoVer, Rest1),
                    wrap(Header, #mqtt_packet_disconnect{reason_code = Reason,
                                                         properties = Properties}, Rest2);
                false ->
                    Lenght = 0, wrap(Header, Rest)
            end;
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

parse_will_props(Bin, ProtoVer = ?MQTT_PROTO_V5, 1) ->
    parse_properties(ProtoVer, Bin);
parse_will_props(Bin, _ProtoVer, _WillFlag),
    {#{}, Bin}.

parse_properties(?MQTT_PROTO_V5, Bin) ->
    {Len, Rest} = parse_variable_byte_integer(Bin),
    <<PropsBin:Len/binary, Rest1} = Rest,
    {parse_property(PropsBin, #{}), Rest1};
parse_properties(_MQTT_PROTO_V3, Bin) ->
    {#{}, Bin}. %% No properties.

parse_property(<<>>, Props) ->
    Props;
%% 01: 'Payload-Format-Indicator', Byte;
parse_property(<<16#01, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Payload-Format-Indicator' => Val});
%% 02: 'Message-Expiry-Interval', Four Byte Integer;
parse_property(<<16#02, Val:32/big, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Message-Expiry-Interval' => Val});
%% 03: 'Content-Type', UTF-8 Encoded String;
parse_property(<<16#03, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf(Bin),
    parse_property(Rest, Props#{'Content-Type' => Val});
%% 08: 'Response-Topic', UTF-8 Encoded String;
parse_property(<<16#08, Bin/binary>>) ->
    {Val, Rest} = parse_utf(Bin),
    parse_property(Rest, Props#{'Response-Topic' => Val});
%% 09: 'Correlation-Data', Binary Data;
parse_property(<<16#09, Len:16/big, Val:Len/binary, Bin/binary>>) ->
    parse_property(Bin, Props#{'Correlation-Data' => Val});
%% 11: 'Subscription-Identifier', Variable Byte Integer;
parse_property(<<16#0B, Bin/binary>>, Props) ->
    {Val, Rest} = parse_variable_byte_integer(Bin),
    parse_property(Rest, Props#{'Subscription-Identifier' => Val});
%% 17: 'Session-Expiry-Interval', Four Byte Integer;
parse_property(<<16#11, Val:32/big, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Session-Expiry-Interval' => Val});
%% 18: 'Assigned-Client-Identifier', UTF-8 Encoded String;
parse_property(<<16#12, Bin/binary>>) ->
    {Val, Rest} = parse_utf(Bin),
    parse_property(Rest, Props#{'Assigned-Client-Identifier' => Val});
%% 19: 'Server-Keep-Alive', Two Byte Integer;
parse_property(<<16#13, Val:16, Bin/binary>>) ->
    parse_property(Bin, Props#{'Server-Keep-Alive' => Val});
%% 21: 'Authentication-Method', UTF-8 Encoded String;
parse_property(<<16#15, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf(Bin),
    parse_property(Rest, Props#{'Authentication-Method' => Val})
%% 22: 'Authentication-Data', Binary Data;
parse_property(<<16#16, Len:16/big, Val:Len/binary, Bin/binary>>) ->
    parse_property(Bin, Props#{'Authentication-Data' => Val});
%% 23: 'Request-Problem-Information', Byte;
parse_property(<<16#17, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Request-Problem-Information' => Val});
%% 24: 'Will-Delay-Interval', Four Byte Integer;
parse_property(<<16#18, Val:32, Bin/binary>>, Props) -> 
    parse_property(Bin, Props#{'Will-Delay-Interval' => Val});
%% 25: 'Request-Response-Information', Byte;
parse_property(<<16#19, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Request-Response-Information' => Val});
%% 26: 'Response Information', UTF-8 Encoded String;
parse_property(<<16#1A, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf(Bin),
    parse_property(Rest, Props#{'Response-Information' => Val});
%% 28: 'Server-Reference', UTF-8 Encoded String;
parse_property(<<16#1C, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf(Bin),
    parse_property(Rest, Props#{'Server-Reference' => Val});
%% 31: 'Reason-String', UTF-8 Encoded String;
parse_property(<<16#1F, Bin/binary, Props) ->
    {Val, Rest} = parse_utf(Bin),
    parse_property(Rest, Props#{'Reason-String' => Val});
%% 33: 'Receive-Maximum', Two Byte Integer;
parse_property(<<16#21, Val:16/big, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Receive-Maximum' => Val});
%% 34: 'Topic-Alias-Maximum', Two Byte Integer;
parse_property(<<16#22, Val:16/big, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Topic-Alias-Maximum' => Val});
%% 35: 'Topic-Alias', Two Byte Integer;
parse_property(<<16#23, Val:16/big, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Topic-Alias' => Val});
%% 36: 'Maximum-QoS', Byte;
parse_property(<<16#24, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Maximum-QoS' => Val});
%% 37: 'Retain-Available', Byte;
parse_property(<<16#25, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Retain-Available' => Val});
%% 38: 'User-Property', UTF-8 String Pair;
parse_property(<<16#26, Bin/binary>>, Props) ->
    {Pair, Rest} = parse_utf_pair(Bin),
    parse_property(Rest, case maps:find('User-Property', Props) of
                             {ok, UserProps} -> Props#{'User-Property' := [Pair | UserProps]};
                             error -> Props#{'User-Property' := [Pair]}
                         end);
%% 39: 'Maximum-Packet-Size', Four Byte Integer;
parse_property(<<16#27, Val:32, Bin/binary>>, Props) ->
    parse_property(Rest, Props#{'Maximum-Packet-Size' => Val});
%% 40: 'Wildcard-Subscription-Available', Byte;
parse_property(<<16#28, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Wildcard-Subscription-Available' => Val});
%% 41: 'Subscription-Identifier-Available', Byte;
parse_property(<<16#29, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Subscription-Identifier-Available' => Val});
%% 42: 'Shared-Subscription-Available', Byte;
parse_property(<<16#2A, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Shared-Subscription-Available' => Val}).

parse_variable_byte_integer(Bin) ->
    parse_variable_byte_integer(Bin, 1, 0).
parse_variable_byte_integer(<<1:1, Len:7, Rest/binary>>, Multiplier, Value) ->
    parse_variable_byte_integer(Rest, Multiplier * ?HIGHBIT, Value + Len * Multiplier);
parse_variable_byte_integer(<<0:1, Len:7, Rest/binary>>, Multiplier, Value) ->
    {Value + Len * Multiplier, Rest}.

parse_topics(_Packet, <<>>, Topics) ->
    lists:reverse(Topics);
parse_topics(?SUBSCRIBE = Sub, Bin, Topics) ->
    {Name, <<<<_Reserved:2, RetainHandling:2, KeepRetain:1, NoLocal:1, QoS:2>>, Rest/binary>>} = parse_utf(Bin),
    SubOpts = [{qos, Qos}, {retain_handling, RetainHandling}, {keep_retain, KeepRetain}, {no_local, NoLocal}],
    parse_topics(Sub, Rest, [{Name, SubOpts}| Topics]);
parse_topics(?UNSUBSCRIBE = Sub, Bin, Topics) ->
    {Name, <<Rest/binary>>} = parse_utf(Bin),
    parse_topics(Sub, Rest, [Name | Topics]).

parse_utf_pair(Bin) ->
    [{Name, Value} || <<Len:16/big, Name:Len/binary, Len2:16/big, Value:Len2/binary>> <= Bin].

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

%% Fix Issue#575
fixqos(?PUBREL, 0)      -> 1;
fixqos(?SUBSCRIBE, 0)   -> 1;
fixqos(?UNSUBSCRIBE, 0) -> 1;
fixqos(_Type, QoS)      -> QoS.

%% Fix Issue#1319
fixdup(Header = #mqtt_packet_header{qos = ?QOS0, dup = true}) ->
    Header#mqtt_packet_header{dup = false};
fixdup(Header = #mqtt_packet_header{qos = ?QOS2, dup = true}) ->
    Header#mqtt_packet_header{dup = false};
fixdup(Header) -> Header.

