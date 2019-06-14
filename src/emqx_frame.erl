%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_frame).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-export([ initial_parse_state/0
        , initial_parse_state/1
        ]).

-export([ parse/1
        , parse/2
        , serialize/1
        , serialize/2
        ]).

-type(options() :: #{max_size => 1..?MAX_PACKET_SIZE,
                     version  => emqx_mqtt_types:version()
                    }).

-opaque(parse_state() :: {none, options()} | {more, cont_fun()}).

-opaque(parse_result() :: {ok, parse_state()}
                        | {ok, emqx_mqtt_types:packet(), binary(), parse_state()}).

-type(cont_fun() :: fun((binary()) -> parse_result())).

-export_type([ options/0
             , parse_state/0
             , parse_result/0
             ]).

-define(none(Opts), {none, Opts}).
-define(more(Cont), {more, Cont}).
-define(DEFAULT_OPTIONS,
        #{max_size => ?MAX_PACKET_SIZE,
          version  => ?MQTT_PROTO_V4
         }).

%%--------------------------------------------------------------------
%% Init Parse State
%%--------------------------------------------------------------------

-spec(initial_parse_state() -> {none, options()}).
initial_parse_state() ->
    initial_parse_state(#{}).

-spec(initial_parse_state(options()) -> {none, options()}).
initial_parse_state(Options) when is_map(Options) ->
    ?none(merge_opts(Options)).

%% @pivate
merge_opts(Options) ->
    maps:merge(?DEFAULT_OPTIONS, Options).

%%--------------------------------------------------------------------
%% Parse MQTT Frame
%%--------------------------------------------------------------------

-spec(parse(binary()) -> parse_result()).
parse(Bin) ->
    parse(Bin, initial_parse_state()).

-spec(parse(binary(), parse_state()) -> parse_result()).
parse(<<>>, {none, Options}) ->
    {ok, ?more(fun(Bin) -> parse(Bin, {none, Options}) end)};
parse(<<Type:4, Dup:1, QoS:2, Retain:1, Rest/binary>>, {none, Options}) ->
    parse_remaining_len(Rest, #mqtt_packet_header{type   = Type,
                                                  dup    = bool(Dup),
                                                  qos    = fixqos(Type, QoS),
                                                  retain = bool(Retain)}, Options);
parse(Bin, {more, Cont}) when is_binary(Bin), is_function(Cont) ->
    Cont(Bin).

parse_remaining_len(<<>>, Header, Options) ->
    {ok, ?more(fun(Bin) -> parse_remaining_len(Bin, Header, Options) end)};
parse_remaining_len(Rest, Header, Options) ->
    parse_remaining_len(Rest, Header, 1, 0, Options).

parse_remaining_len(_Bin, _Header, _Multiplier, Length, #{max_size := MaxSize})
  when Length > MaxSize ->
    error(mqtt_frame_too_large);
parse_remaining_len(<<>>, Header, Multiplier, Length, Options) ->
    {ok, ?more(fun(Bin) -> parse_remaining_len(Bin, Header, Multiplier, Length, Options) end)};
%% Match DISCONNECT without payload
parse_remaining_len(<<0:8, Rest/binary>>, Header = #mqtt_packet_header{type = ?DISCONNECT}, 1, 0, Options) ->
    Packet = packet(Header, #mqtt_packet_disconnect{reason_code = ?RC_SUCCESS}),
    {ok, Packet, Rest, ?none(Options)};
%% Match PINGREQ.
parse_remaining_len(<<0:8, Rest/binary>>, Header, 1, 0, Options) ->
    parse_frame(Rest, Header, 0, Options);
%% Match PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK...
parse_remaining_len(<<0:1, 2:7, Rest/binary>>, Header, 1, 0, Options) ->
    parse_frame(Rest, Header, 2, Options);
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Header, Multiplier, Value, Options) ->
    parse_remaining_len(Rest, Header, Multiplier * ?HIGHBIT, Value + Len * Multiplier, Options);
parse_remaining_len(<<0:1, Len:7, Rest/binary>>, Header, Multiplier, Value,
                    Options = #{max_size := MaxSize}) ->
    FrameLen = Value + Len * Multiplier,
    if
        FrameLen > MaxSize -> error(mqtt_frame_too_large);
        true -> parse_frame(Rest, Header, FrameLen, Options)
    end.

parse_frame(Bin, Header, 0, Options) ->
    {ok, packet(Header), Bin, ?none(Options)};

parse_frame(Bin, Header, Length, Options) ->
    case Bin of
        <<FrameBin:Length/binary, Rest/binary>> ->
            case parse_packet(Header, FrameBin, Options) of
                {Variable, Payload} ->
                    {ok, packet(Header, Variable, Payload), Rest, ?none(Options)};
                Variable = #mqtt_packet_connect{proto_ver = Ver} ->
                    {ok, packet(Header, Variable), Rest, ?none(Options#{version := Ver})};
                Variable ->
                    {ok, packet(Header, Variable), Rest, ?none(Options)}
            end;
        TooShortBin ->
            {ok, ?more(fun(BinMore) ->
                               parse_frame(<<TooShortBin/binary, BinMore/binary>>, Header, Length, Options)
                       end)}
    end.

packet(Header) ->
    #mqtt_packet{header = Header}.
packet(Header, Variable) ->
    #mqtt_packet{header = Header, variable = Variable}.
packet(Header, Variable, Payload) ->
    #mqtt_packet{header = Header, variable = Variable, payload = Payload}.

parse_packet(#mqtt_packet_header{type = ?CONNECT}, FrameBin, _Options) ->
    {ProtoName, Rest} = parse_utf8_string(FrameBin),
    <<BridgeTag:4, ProtoVer:4, Rest1/binary>> = Rest,
    % Note: Crash when reserved flag doesn't equal to 0, there is no strict compliance with the MQTT5.0.
    <<UsernameFlag : 1,
      PasswordFlag : 1,
      WillRetain   : 1,
      WillQoS      : 2,
      WillFlag     : 1,
      CleanStart   : 1,
      0            : 1,
      KeepAlive    : 16/big,
      Rest2/binary>> = Rest1,

    {Properties, Rest3} = parse_properties(Rest2, ProtoVer),
    {ClientId, Rest4} = parse_utf8_string(Rest3),
    ConnPacket = #mqtt_packet_connect{proto_name  = ProtoName,
                                      proto_ver   = ProtoVer,
                                      is_bridge   = (BridgeTag =:= 8),
                                      clean_start = bool(CleanStart),
                                      will_flag   = bool(WillFlag),
                                      will_qos    = WillQoS,
                                      will_retain = bool(WillRetain),
                                      keepalive   = KeepAlive,
                                      properties  = Properties,
                                      client_id   = ClientId},
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

% protocol_approved(Ver, Name) ->
%     lists:member({Ver, Name}, ?PROTOCOL_NAMES).

parse_packet_id(<<PacketId:16/big, Rest/binary>>) ->
    {PacketId, Rest}.

parse_properties(Bin, Ver) when Ver =/= ?MQTT_PROTO_V5 ->
    {undefined, Bin};
%% TODO: version mess?
parse_properties(<<>>, ?MQTT_PROTO_V5) ->
    {#{}, <<>>};
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
    [{Topic, #{rh => Rh, rap => Rap, nl => Nl, qos => QoS, rc => 0}}
     || <<Len:16/big, Topic:Len/binary, _:2, Rh:2, Rap:1, Nl:1, QoS:2>> <= Bin];

parse_topic_filters(unsubscribe, Bin) ->
    [Topic || <<Len:16/big, Topic:Len/binary>> <= Bin].

parse_reason_codes(Bin) ->
    [Code || <<Code>> <= Bin].

parse_utf8_pair(<<Len1:16/big, Key:Len1/binary,
                  Len2:16/big, Val:Len2/binary, Rest/binary>>) ->
    {{Key, Val}, Rest}.

parse_utf8_string(Bin, false) ->
    {undefined, Bin};
parse_utf8_string(Bin, true) ->
    parse_utf8_string(Bin).

parse_utf8_string(<<Len:16/big, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest}.

parse_binary_data(<<Len:16/big, Data:Len/binary, Rest/binary>>) ->
    {Data, Rest}.

%%--------------------------------------------------------------------
%% Serialize MQTT Packet
%%--------------------------------------------------------------------

-spec(serialize(emqx_mqtt_types:packet()) -> iodata()).
serialize(Packet) ->
    serialize(Packet, ?DEFAULT_OPTIONS).

-spec(serialize(emqx_mqtt_types:packet(), options()) -> iodata()).
serialize(#mqtt_packet{header   = Header,
                       variable = Variable,
                       payload  = Payload}, Options) when is_map(Options) ->
    serialize(Header, serialize_variable(Variable, merge_opts(Options)), serialize_payload(Payload)).

serialize(#mqtt_packet_header{type   = Type,
                              dup    = Dup,
                              qos    = QoS,
                              retain = Retain}, VariableBin, PayloadBin)
    when ?CONNECT =< Type andalso Type =< ?AUTH ->
    Len = iolist_size(VariableBin) + iolist_size(PayloadBin),
    (Len =< ?MAX_PACKET_SIZE) orelse error(mqtt_frame_too_large),
    [<<Type:4, (flag(Dup)):1, (flag(QoS)):2, (flag(Retain)):1>>,
     serialize_remaining_len(Len), VariableBin, PayloadBin].

serialize_variable(#mqtt_packet_connect{
                      proto_name   = ProtoName,
                      proto_ver    = ProtoVer,
                      is_bridge    = IsBridge,
                      clean_start  = CleanStart,
                      will_flag    = WillFlag,
                      will_qos     = WillQoS,
                      will_retain  = WillRetain,
                      keepalive    = KeepAlive,
                      properties   = Properties,
                      client_id    = ClientId,
                      will_props   = WillProps,
                      will_topic   = WillTopic,
                      will_payload = WillPayload,
                      username     = Username,
                      password     = Password}, _Options) ->
    [serialize_binary_data(ProtoName),
     <<(case IsBridge of
           true  -> 16#80 + ProtoVer;
           false -> ProtoVer
       end):8,
       (flag(Username)):1,
       (flag(Password)):1,
       (flag(WillRetain)):1,
       WillQoS:2,
       (flag(WillFlag)):1,
       (flag(CleanStart)):1,
       0:1,
       KeepAlive:16/big-unsigned-integer>>,
     serialize_properties(Properties, ProtoVer),
     serialize_utf8_string(ClientId),
     case WillFlag of
         true -> [serialize_properties(WillProps, ProtoVer),
                  serialize_utf8_string(WillTopic),
                  serialize_binary_data(WillPayload)];
         false -> <<>>
     end,
     serialize_utf8_string(Username, true),
     serialize_utf8_string(Password, true)];

serialize_variable(#mqtt_packet_connack{ack_flags   = AckFlags,
                                        reason_code = ReasonCode,
                                        properties  = Properties},
                   #{version := Ver}) ->
    [AckFlags, ReasonCode, serialize_properties(Properties, Ver)];

serialize_variable(#mqtt_packet_publish{topic_name = TopicName,
                                        packet_id  = PacketId,
                                        properties = Properties},
                   #{version := Ver}) ->
    [serialize_utf8_string(TopicName),
     if
         PacketId =:= undefined -> <<>>;
         true -> <<PacketId:16/big-unsigned-integer>>
     end,
     serialize_properties(Properties, Ver)];

serialize_variable(#mqtt_packet_puback{packet_id = PacketId},
                   #{version := Ver})
    when Ver == ?MQTT_PROTO_V3; Ver == ?MQTT_PROTO_V4 ->
    <<PacketId:16/big-unsigned-integer>>;
serialize_variable(#mqtt_packet_puback{packet_id   = PacketId,
                                       reason_code = ReasonCode,
                                       properties  = Properties},
                   #{version := ?MQTT_PROTO_V5}) ->
    [<<PacketId:16/big-unsigned-integer>>, ReasonCode,
     serialize_properties(Properties, ?MQTT_PROTO_V5)];

serialize_variable(#mqtt_packet_subscribe{packet_id     = PacketId,
                                          properties    = Properties,
                                          topic_filters = TopicFilters},
                   #{version := Ver}) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     serialize_topic_filters(subscribe, TopicFilters, Ver)];

serialize_variable(#mqtt_packet_suback{packet_id    = PacketId,
                                       properties   = Properties,
                                       reason_codes = ReasonCodes},
                   #{version := Ver}) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     serialize_reason_codes(ReasonCodes)];

serialize_variable(#mqtt_packet_unsubscribe{packet_id     = PacketId,
                                            properties    = Properties,
                                            topic_filters = TopicFilters},
                   #{version := Ver}) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     serialize_topic_filters(unsubscribe, TopicFilters, Ver)];

serialize_variable(#mqtt_packet_unsuback{packet_id    = PacketId,
                                         properties   = Properties,
                                         reason_codes = ReasonCodes},
                   #{version := Ver}) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     serialize_reason_codes(ReasonCodes)];

serialize_variable(#mqtt_packet_disconnect{}, #{version := Ver})
    when Ver == ?MQTT_PROTO_V3; Ver == ?MQTT_PROTO_V4 ->
    <<>>;

serialize_variable(#mqtt_packet_disconnect{reason_code = ReasonCode,
                                           properties  = Properties},
                   #{version := Ver = ?MQTT_PROTO_V5}) ->
    [ReasonCode, serialize_properties(Properties, Ver)];
serialize_variable(#mqtt_packet_disconnect{}, _Ver) ->
    <<>>;

serialize_variable(#mqtt_packet_auth{reason_code = ReasonCode,
                                     properties  = Properties},
                   #{version := Ver = ?MQTT_PROTO_V5}) ->
    [ReasonCode, serialize_properties(Properties, Ver)];

serialize_variable(PacketId, ?MQTT_PROTO_V3) when is_integer(PacketId) ->
    <<PacketId:16/big-unsigned-integer>>;
serialize_variable(PacketId, ?MQTT_PROTO_V4) when is_integer(PacketId) ->
    <<PacketId:16/big-unsigned-integer>>;
serialize_variable(undefined, _Ver) ->
    <<>>.

serialize_payload(undefined) -> <<>>;
serialize_payload(Bin)       -> Bin.

serialize_properties(_Props, Ver) when Ver =/= ?MQTT_PROTO_V5 ->
    <<>>;
serialize_properties(Props, ?MQTT_PROTO_V5) ->
    serialize_properties(Props).

serialize_properties(undefined) ->
    <<0>>;
serialize_properties(Props) when map_size(Props) == 0 ->
    <<0>>;
serialize_properties(Props) when is_map(Props) ->
    Bin = << <<(serialize_property(Prop, Val))/binary>> || {Prop, Val} <- maps:to_list(Props) >>,
    [serialize_variable_byte_integer(byte_size(Bin)), Bin].

serialize_property(_, undefined) ->
    <<>>;
serialize_property('Payload-Format-Indicator', Val) ->
    <<16#01, Val>>;
serialize_property('Message-Expiry-Interval', Val) ->
    <<16#02, Val:32/big>>;
serialize_property('Content-Type', Val) ->
    <<16#03, (serialize_utf8_string(Val))/binary>>;
serialize_property('Response-Topic', Val) ->
    <<16#08, (serialize_utf8_string(Val))/binary>>;
serialize_property('Correlation-Data', Val) ->
    <<16#09, (byte_size(Val)):16, Val/binary>>;
serialize_property('Subscription-Identifier', Val) ->
    <<16#0B, (serialize_variable_byte_integer(Val))/binary>>;
serialize_property('Session-Expiry-Interval', Val) ->
    <<16#11, Val:32/big>>;
serialize_property('Assigned-Client-Identifier', Val) ->
    <<16#12, (serialize_utf8_string(Val))/binary>>;
serialize_property('Server-Keep-Alive', Val) ->
    <<16#13, Val:16/big>>;
serialize_property('Authentication-Method', Val) ->
    <<16#15, (serialize_utf8_string(Val))/binary>>;
serialize_property('Authentication-Data', Val) ->
    <<16#16, (iolist_size(Val)):16, Val/binary>>;
serialize_property('Request-Problem-Information', Val) ->
    <<16#17, Val>>;
serialize_property('Will-Delay-Interval', Val) ->
    <<16#18, Val:32/big>>;
serialize_property('Request-Response-Information', Val) ->
    <<16#19, Val>>;
serialize_property('Response-Information', Val) ->
    <<16#1A, (serialize_utf8_string(Val))/binary>>;
serialize_property('Server-Reference', Val) ->
    <<16#1C, (serialize_utf8_string(Val))/binary>>;
serialize_property('Reason-String', Val) ->
    <<16#1F, (serialize_utf8_string(Val))/binary>>;
serialize_property('Receive-Maximum', Val) ->
    <<16#21, Val:16/big>>;
serialize_property('Topic-Alias-Maximum', Val) ->
    <<16#22, Val:16/big>>;
serialize_property('Topic-Alias', Val) ->
    <<16#23, Val:16/big>>;
serialize_property('Maximum-QoS', Val) ->
    <<16#24, Val>>;
serialize_property('Retain-Available', Val) ->
    <<16#25, Val>>;
serialize_property('User-Property', {Key, Val}) ->
    <<16#26, (serialize_utf8_pair({Key, Val}))/binary>>;
serialize_property('User-Property', Props) when is_list(Props) ->
    << <<(serialize_property('User-Property', {Key, Val}))/binary>>
       || {Key, Val} <- Props >>;
serialize_property('Maximum-Packet-Size', Val) ->
    <<16#27, Val:32/big>>;
serialize_property('Wildcard-Subscription-Available', Val) ->
    <<16#28, Val>>;
serialize_property('Subscription-Identifier-Available', Val) ->
    <<16#29, Val>>;
serialize_property('Shared-Subscription-Available', Val) ->
    <<16#2A, Val>>.

serialize_topic_filters(subscribe, TopicFilters, ?MQTT_PROTO_V5) ->
    << <<(serialize_utf8_string(Topic))/binary,
         ?RESERVED:2, Rh:2, (flag(Rap)):1,(flag(Nl)):1, QoS:2 >>
       || {Topic, #{rh := Rh, rap := Rap, nl := Nl, qos := QoS}}
          <- TopicFilters >>;

serialize_topic_filters(subscribe, TopicFilters, _Ver) ->
    << <<(serialize_utf8_string(Topic))/binary, ?RESERVED:6, QoS:2>>
       || {Topic, #{qos := QoS}} <- TopicFilters >>;

serialize_topic_filters(unsubscribe, TopicFilters, _Ver) ->
    << <<(serialize_utf8_string(Topic))/binary>> || Topic <- TopicFilters >>.

serialize_reason_codes(undefined) ->
    <<>>;
serialize_reason_codes(ReasonCodes) when is_list(ReasonCodes) ->
     << <<Code>> || Code <- ReasonCodes >>.

serialize_utf8_pair({Name, Value}) ->
    << (serialize_utf8_string(Name))/binary, (serialize_utf8_string(Value))/binary >>.

serialize_binary_data(Bin) ->
    [<<(byte_size(Bin)):16/big-unsigned-integer>>, Bin].

serialize_utf8_string(undefined, false) ->
    error(utf8_string_undefined);
serialize_utf8_string(undefined, true) ->
    <<>>;
serialize_utf8_string(String, _AllowNull) ->
    serialize_utf8_string(String).

serialize_utf8_string(String) ->
    StringBin = unicode:characters_to_binary(String),
    Len = byte_size(StringBin),
    true = (Len =< 16#ffff),
    <<Len:16/big, StringBin/binary>>.

serialize_remaining_len(I) ->
    serialize_variable_byte_integer(I).

serialize_variable_byte_integer(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialize_variable_byte_integer(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialize_variable_byte_integer(N div ?HIGHBIT))/binary>>.

bool(0) -> false;
bool(1) -> true.

flag(undefined)            -> ?RESERVED;
flag(false)                -> 0;
flag(true)                 -> 1;
flag(X) when is_integer(X) -> X;
flag(B) when is_binary(B)  -> 1.

fixqos(?PUBREL, 0)      -> 1;
fixqos(?SUBSCRIBE, 0)   -> 1;
fixqos(?UNSUBSCRIBE, 0) -> 1;
fixqos(_Type, QoS)      -> QoS.
