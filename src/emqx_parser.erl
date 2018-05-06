%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(emqx_parser).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

-export([initial_state/0, initial_state/1, parse/2]).

-type(max_packet_size() :: 1..?MAX_PACKET_SIZE).

-type(option() :: {max_len, max_packet_size()}
                | {version, mqtt_version()}).

-type(state() :: {none, map()} | {more, fun()}).

-export_type([option/0, state/0]).

%% @doc Initialize a parser
-spec(initial_state() -> {none, map()}).
initial_state() -> initial_state([]).

-spec(initial_state([option()]) -> {none, map()}).
initial_state(Options) when is_list(Options) ->
    {none, parse_opt(Options, #{max_len => ?MAX_PACKET_SIZE,
                                version => ?MQTT_PROTO_V4})}.

parse_opt([], Map) ->
    Map;
parse_opt([{version, Ver}|Opts], Map) ->
    parse_opt(Opts, Map#{version := Ver});
parse_opt([{max_len, Len}|Opts], Map) ->
    parse_opt(Opts, Map#{max_len := Len});
parse_opt([_|Opts], Map) ->
    parse_opt(Opts, Map).

%% @doc Parse MQTT Packet
-spec(parse(binary(), {none, map()} | fun())
      -> {ok, mqtt_packet()} | {error, term()} | {more, fun()}).
parse(<<>>, {none, Options}) ->
    {more, fun(Bin) -> parse(Bin, {none, Options}) end};
parse(<<Type:4, Dup:1, QoS:2, Retain:1, Rest/binary>>, {none, Options}) ->
    parse_remaining_len(Rest, #mqtt_packet_header{type   = Type,
                                                  dup    = bool(Dup),
                                                  qos    = fixqos(Type, QoS),
                                                  retain = bool(Retain)}, Options);
parse(Bin, Cont) -> Cont(Bin).

parse_remaining_len(<<>>, Header, Options) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Header, Options) end};
parse_remaining_len(Rest, Header, Options) ->
    parse_remaining_len(Rest, Header, 1, 0, Options).

parse_remaining_len(_Bin, _Header, _Multiplier, Length, #{max_len := MaxLen})
    when Length > MaxLen ->
    {error, mqtt_frame_too_long};
parse_remaining_len(<<>>, Header, Multiplier, Length, Options) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Header, Multiplier, Length, Options) end};
%% Optimize: match PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK...
parse_remaining_len(<<0:1, 2:7, Rest/binary>>, Header, 1, 0, Options) ->
    parse_frame(Rest, Header, 2, Options);
%% optimize: match PINGREQ...
parse_remaining_len(<<0:8, Rest/binary>>, Header, 1, 0, Options) ->
    parse_frame(Rest, Header, 0, Options);
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Header, Multiplier, Value, Options) ->
    parse_remaining_len(Rest, Header, Multiplier * ?HIGHBIT, Value + Len * Multiplier, Options);
parse_remaining_len(<<0:1, Len:7, Rest/binary>>, Header,  Multiplier, Value,
                    Options = #{max_len := MaxLen}) ->
    FrameLen = Value + Len * Multiplier,
    if
        FrameLen > MaxLen -> error(mqtt_frame_too_long);
        true -> parse_frame(Rest, Header, FrameLen, Options)
    end.

parse_frame(Bin, Header, 0, _Options) ->
    wrap(Header, Bin);

parse_frame(Bin, Header, Length, Options) ->
    case Bin of
        <<FrameBin:Length/binary, Rest/binary>> ->
            case parse_packet(Header, FrameBin, Options) of
                {Variable, Payload} ->
                    wrap(Header, Variable, Payload, Rest);
                Variable ->
                    wrap(Header, Variable, Rest)
            end;
        TooShortBin ->
            {more, fun(BinMore) ->
                       parse_frame(<<TooShortBin/binary, BinMore/binary>>, Header, Length, Options)
                   end}
    end.

parse_packet(#mqtt_packet_header{type = ?CONNECT}, FrameBin, _Options) ->
    {ProtoName, Rest} = parse_utf8_string(FrameBin),
    <<BridgeTag:4, ProtoVer:4, Rest1/binary>> = Rest,
    <<UsernameFlag : 1,
      PasswordFlag : 1,
      WillRetain   : 1,
      WillQos      : 2,
      WillFlag     : 1,
      CleanStart   : 1,
      _Reserved    : 1,
      KeepAlive    : 16/big,
      Rest2/binary>> = Rest1,
    case protocol_name_approved(ProtoVer, ProtoName) of
        true  -> ok;
        false -> error(protocol_name_unapproved)
    end,
    {Properties, Rest3} = parse_properties(Rest2, ProtoVer),
    {ClientId, Rest4} = parse_utf8_string(Rest3),
    ConnPacket = #mqtt_packet_connect{proto_name   = ProtoName,
                                      proto_ver    = ProtoVer,
                                      is_bridge    = (BridgeTag =:= 8),
                                      clean_start  = bool(CleanStart),
                                      will_flag    = bool(WillFlag),
                                      will_qos     = WillQos,
                                      will_retain  = bool(WillRetain),
                                      keepalive    = KeepAlive,
                                      properties   = Properties,
                                      client_id    = ClientId},
    {ConnPacket1, Rest5} = parse_will_message(ConnPacket, Rest4),
    {Username, Rest6} = parse_utf8_string(Rest5, bool(UsernameFlag)),
    {Passsword, <<>>} = parse_utf8_string(Rest6, bool(PasswordFlag)),
    ConnPacket1#mqtt_packet_connect{username = Username, password = Passsword};

parse_packet(#mqtt_packet_header{type = ?CONNACK},
             <<AckFlags:8, ReasonCode:8, Rest/binary>>, #{version := Ver}) ->
    {Properties, <<>>} = parse_properties(Rest, Ver),
    #mqtt_packet_connack{ack_flags   = AckFlags,
                         reason_code = ReasonCode,
                         properties  = Properties};

parse_packet(#mqtt_packet_header{type = ?PUBLISH, qos = QoS}, Bin,
             #{version := Ver}) ->
    {TopicName, Rest} = parse_utf8_string(Bin),
    {PacketId, Rest1} = case QoS of
                            ?QOS_0 -> {undefined, Rest};
                            _ -> parse_packet_id(Rest)
                        end,
    {Properties, Payload} = parse_properties(Rest1, Ver),
    {#mqtt_packet_publish{topic_name = TopicName,
                          packet_id  = PacketId,
                          properties = Properties}, Payload};

parse_packet(#mqtt_packet_header{type = PubAck}, <<PacketId:16/big>>, _Options)
    when ?PUBACK =< PubAck, PubAck =< ?PUBCOMP ->
    #mqtt_packet_puback{packet_id = PacketId, reason_code = 0};
parse_packet(#mqtt_packet_header{type = PubAck}, <<PacketId:16/big, ReasonCode, Rest/binary>>,
             #{version := Ver = ?MQTT_PROTO_V5})
    when ?PUBACK =< PubAck, PubAck =< ?PUBCOMP ->
    {Properties, <<>>} = parse_properties(Rest, Ver),
    #mqtt_packet_puback{packet_id   = PacketId,
                        reason_code = ReasonCode,
                        properties  = Properties};

parse_packet(#mqtt_packet_header{type = ?SUBSCRIBE}, <<PacketId:16/big, Rest/binary>>,
             #{version := Ver}) ->
    {Properties, Rest1} = parse_properties(Rest, Ver),
    TopicFilters = parse_topic_filters(subscribe, Rest1),
    #mqtt_packet_subscribe{packet_id     = PacketId,
                           properties    = Properties,
                           topic_filters = TopicFilters};

parse_packet(#mqtt_packet_header{type = ?SUBACK}, <<PacketId:16/big, Rest/binary>>,
             #{version := Ver}) ->
    {Properties, Rest1} = parse_properties(Rest, Ver),
    #mqtt_packet_suback{packet_id    = PacketId,
                        properties   = Properties,
                        reason_codes = parse_reason_codes(Rest1)};

parse_packet(#mqtt_packet_header{type = ?UNSUBSCRIBE}, <<PacketId:16/big, Rest/binary>>,
             #{version := Ver}) ->
    {Properties, Rest1} = parse_properties(Rest, Ver),
    TopicFilters = parse_topic_filters(unsubscribe, Rest1),
    #mqtt_packet_unsubscribe{packet_id     = PacketId,
                             properties    = Properties,
                             topic_filters = TopicFilters};

parse_packet(#mqtt_packet_header{type = ?UNSUBACK}, <<PacketId:16/big>>, _Options) ->
    #mqtt_packet_unsuback{packet_id = PacketId};
parse_packet(#mqtt_packet_header{type = ?UNSUBACK}, <<PacketId:16/big, Rest/binary>>,
             #{version := Ver}) ->
    {Properties, Rest1} = parse_properties(Rest, Ver),
    ReasonCodes = parse_reason_codes(Rest1),
    #mqtt_packet_unsuback{packet_id    = PacketId,
                          properties   = Properties,
                          reason_codes = ReasonCodes};

parse_packet(#mqtt_packet_header{type = ?DISCONNECT}, <<ReasonCode, Rest/binary>>,
             #{version := ?MQTT_PROTO_V5}) ->
    {Properties, <<>>} = parse_properties(Rest, ?MQTT_PROTO_V5),
    #mqtt_packet_disconnect{reason_code = ReasonCode,
                            properties  = Properties};

parse_packet(#mqtt_packet_header{type = ?AUTH}, <<ReasonCode, Rest/binary>>,
             #{version := ?MQTT_PROTO_V5}) ->
    {Properties, <<>>} = parse_properties(Rest, ?MQTT_PROTO_V5),
    #mqtt_packet_auth{reason_code = ReasonCode, properties = Properties}.

wrap(Header, Variable, Payload, Rest) ->
    {ok, #mqtt_packet{header = Header, variable = Variable, payload = Payload}, Rest}.
wrap(Header, Variable, Rest) ->
    {ok, #mqtt_packet{header = Header, variable = Variable}, Rest}.
wrap(Header, Rest) ->
    {ok, #mqtt_packet{header = Header}, Rest}.

protocol_name_approved(Ver, Name) ->
    lists:member({Ver, Name}, ?PROTOCOL_NAMES).

parse_will_message(Packet = #mqtt_packet_connect{will_flag = true,
                                                 proto_ver = Ver}, Bin) ->
    {Props, Rest} = parse_properties(Bin, Ver),
    {Topic, Rest1} = parse_utf8_string(Rest),
    {Payload, Rest2} = parse_binary_data(Rest1),
    {Packet#mqtt_packet_connect{will_props   = Props,
                                will_topic   = Topic,
                                will_payload = Payload}, Rest2};
parse_will_message(Packet, Bin) ->
    {Packet, Bin}.

parse_packet_id(<<PacketId:16/big, Rest/binary>>) ->
    {PacketId, Rest}.

parse_properties(Bin, Ver) when Ver =/= ?MQTT_PROTO_V5 ->
    {undefined, Bin};
parse_properties(<<0, Rest/binary>>, ?MQTT_PROTO_V5) ->
    {#{}, Rest};
parse_properties(Bin, ?MQTT_PROTO_V5) ->
    {Len, Rest} = parse_variable_byte_integer(Bin),
    <<PropsBin:Len/binary, Rest1/binary>> = Rest,
    {parse_property(PropsBin, #{}), Rest1}.

parse_property(<<>>, Props) ->
    Props;
parse_property(<<16#01, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Payload-Format-Indicator' => Val});
parse_property(<<16#02, Val:32/big, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Message-Expiry-Interval' => Val});
parse_property(<<16#03, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf8_string(Bin),
    parse_property(Rest, Props#{'Content-Type' => Val});
parse_property(<<16#08, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf8_string(Bin),
    parse_property(Rest, Props#{'Response-Topic' => Val});
parse_property(<<16#09, Len:16/big, Val:Len/binary, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Correlation-Data' => Val});
parse_property(<<16#0B, Bin/binary>>, Props) ->
    {Val, Rest} = parse_variable_byte_integer(Bin),
    parse_property(Rest, Props#{'Subscription-Identifier' => Val});
parse_property(<<16#11, Val:32/big, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Session-Expiry-Interval' => Val});
parse_property(<<16#12, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf8_string(Bin),
    parse_property(Rest, Props#{'Assigned-Client-Identifier' => Val});
parse_property(<<16#13, Val:16, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Server-Keep-Alive' => Val});
parse_property(<<16#15, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf8_string(Bin),
    parse_property(Rest, Props#{'Authentication-Method' => Val});
parse_property(<<16#16, Len:16/big, Val:Len/binary, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Authentication-Data' => Val});
parse_property(<<16#17, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Request-Problem-Information' => Val});
parse_property(<<16#18, Val:32, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Will-Delay-Interval' => Val});
parse_property(<<16#19, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Request-Response-Information' => Val});
parse_property(<<16#1A, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf8_string(Bin),
    parse_property(Rest, Props#{'Response-Information' => Val});
parse_property(<<16#1C, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf8_string(Bin),
    parse_property(Rest, Props#{'Server-Reference' => Val});
parse_property(<<16#1F, Bin/binary>>, Props) ->
    {Val, Rest} = parse_utf8_string(Bin),
    parse_property(Rest, Props#{'Reason-String' => Val});
parse_property(<<16#21, Val:16/big, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Receive-Maximum' => Val});
parse_property(<<16#22, Val:16/big, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Topic-Alias-Maximum' => Val});
parse_property(<<16#23, Val:16/big, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Topic-Alias' => Val});
parse_property(<<16#24, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Maximum-QoS' => Val});
parse_property(<<16#25, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Retain-Available' => Val});
parse_property(<<16#26, Bin/binary>>, Props) ->
    {Pair, Rest} = parse_utf8_pair(Bin),
    case maps:find('User-Property', Props) of
        {ok, UserProps} ->
            parse_property(Rest,Props#{'User-Property' := [Pair|UserProps]});
        error ->
            parse_property(Rest, Props#{'User-Property' => [Pair]})
    end;
parse_property(<<16#27, Val:32, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Maximum-Packet-Size' => Val});
parse_property(<<16#28, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Wildcard-Subscription-Available' => Val});
parse_property(<<16#29, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Subscription-Identifier-Available' => Val});
parse_property(<<16#2A, Val, Bin/binary>>, Props) ->
    parse_property(Bin, Props#{'Shared-Subscription-Available' => Val}).

parse_variable_byte_integer(Bin) ->
    parse_variable_byte_integer(Bin, 1, 0).
parse_variable_byte_integer(<<1:1, Len:7, Rest/binary>>, Multiplier, Value) ->
    parse_variable_byte_integer(Rest, Multiplier * ?HIGHBIT, Value + Len * Multiplier);
parse_variable_byte_integer(<<0:1, Len:7, Rest/binary>>, Multiplier, Value) ->
    {Value + Len * Multiplier, Rest}.

parse_topic_filters(subscribe, Bin) ->
    [{Topic,  #mqtt_subopts{rh = Rh, rap = Rap, nl = Nl, qos = QoS}}
     || <<Len:16/big, Topic:Len/binary, _:2, Rh:2, Rap:1, Nl:1, QoS:2>> <= Bin];

parse_topic_filters(unsubscribe, Bin) ->
    [Topic || <<Len:16/big, Topic:Len/binary>> <= Bin].

parse_reason_codes(Bin) ->
    [Code || <<Code>> <= Bin].

parse_utf8_pair(Bin) ->
    [{Name, Value} || <<Len:16/big, Name:Len/binary, Len2:16/big, Value:Len2/binary>> <= Bin].

parse_utf8_string(Bin, false) ->
    {undefined, Bin};
parse_utf8_string(Bin, true) ->
    parse_utf8_string(Bin).

parse_utf8_string(<<Len:16/big, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest}.

parse_binary_data(<<Len:16/big, Data:Len/binary, Rest/binary>>) ->
    {Data, Rest}.

bool(0) -> false;
bool(1) -> true.

%% Fix Issue#575
fixqos(?PUBREL, 0)      -> 1;
fixqos(?SUBSCRIBE, 0)   -> 1;
fixqos(?UNSUBSCRIBE, 0) -> 1;
fixqos(_Type, QoS)      -> QoS.

