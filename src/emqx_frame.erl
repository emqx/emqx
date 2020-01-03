%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , serialize_fun/0
        , serialize_fun/1
        , serialize/1
        , serialize/2
        ]).

-export_type([ options/0
             , parse_state/0
             , parse_result/0
             , serialize_fun/0
             ]).

-type(options() :: #{strict_mode => boolean(),
                     max_size => 1..?MAX_PACKET_SIZE,
                     version => emqx_types:version()
                    }).

-opaque(parse_state() :: {none, options()} | cont_fun()).

-opaque(parse_result() :: {more, cont_fun()}
                        | {ok, emqx_types:packet(), binary(), parse_state()}).

-type(cont_fun() :: fun((binary()) -> parse_result())).

-type(serialize_fun() :: fun((emqx_types:packet()) -> iodata())).

-define(none(Options), {none, Options}).

-define(DEFAULT_OPTIONS,
        #{strict_mode => false,
          max_size    => ?MAX_PACKET_SIZE,
          version     => ?MQTT_PROTO_V4
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
    {more, fun(Bin) -> parse(Bin, {none, Options}) end};
parse(<<Type:4, Dup:1, QoS:2, Retain:1, Rest/binary>>,
      {none, Options = #{strict_mode := StrictMode}}) ->
    %% Validate header if strict mode.
    StrictMode andalso validate_header(Type, Dup, QoS, Retain),
    Header = #mqtt_packet_header{type   = Type,
                                 dup    = bool(Dup),
                                 qos    = QoS,
                                 retain = bool(Retain)
                                },
    Header1 = case fixqos(Type, QoS) of
                  QoS      -> Header;
                  FixedQoS -> Header#mqtt_packet_header{qos = FixedQoS}
              end,
    parse_remaining_len(Rest, Header1, Options);
parse(Bin, Cont) when is_binary(Bin), is_function(Cont) ->
    Cont(Bin).

parse_remaining_len(<<>>, Header, Options) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Header, Options) end};
parse_remaining_len(Rest, Header, Options) ->
    parse_remaining_len(Rest, Header, 1, 0, Options).

parse_remaining_len(_Bin, _Header, _Multiplier, Length, #{max_size := MaxSize})
  when Length > MaxSize ->
    error(frame_too_large);
parse_remaining_len(<<>>, Header, Multiplier, Length, Options) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Header, Multiplier, Length, Options) end};
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
        FrameLen > MaxSize -> error(frame_too_large);
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
            {more, fun(BinMore) ->
                           parse_frame(<<TooShortBin/binary, BinMore/binary>>, Header, Length, Options)
                   end}
    end.

-compile({inline, [packet/1, packet/2, packet/3]}).
packet(Header) ->
    #mqtt_packet{header = Header}.
packet(Header, Variable) ->
    #mqtt_packet{header = Header, variable = Variable}.
packet(Header, Variable, Payload) ->
    #mqtt_packet{header = Header, variable = Variable, payload = Payload}.

parse_packet(#mqtt_packet_header{type = ?CONNECT}, FrameBin, _Options) ->
    {ProtoName, Rest} = parse_utf8_string(FrameBin),
    <<BridgeTag:4, ProtoVer:4, Rest1/binary>> = Rest,
    % Note: Crash when reserved flag doesn't equal to 0, there is no strict
    % compliance with the MQTT5.0.
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
                                      clientid    = ClientId
                                     },
    {ConnPacket1, Rest5} = parse_will_message(ConnPacket, Rest4),
    {Username, Rest6} = parse_utf8_string(Rest5, bool(UsernameFlag)),
    {Passsword, <<>>} = parse_utf8_string(Rest6, bool(PasswordFlag)),
    ConnPacket1#mqtt_packet_connect{username = Username, password = Passsword};

parse_packet(#mqtt_packet_header{type = ?CONNACK},
             <<AckFlags:8, ReasonCode:8, Rest/binary>>, #{version := Ver}) ->
    {Properties, <<>>} = parse_properties(Rest, Ver),
    #mqtt_packet_connack{ack_flags   = AckFlags,
                         reason_code = ReasonCode,
                         properties  = Properties
                        };

parse_packet(#mqtt_packet_header{type = ?PUBLISH, qos = QoS}, Bin,
             #{strict_mode := StrictMode, version := Ver}) ->
    {TopicName, Rest} = parse_utf8_string(Bin),
    {PacketId, Rest1} = case QoS of
                            ?QOS_0 -> {undefined, Rest};
                            _ -> parse_packet_id(Rest)
                        end,
    (PacketId =/= undefined) andalso
      StrictMode andalso validate_packet_id(PacketId),
    {Properties, Payload} = parse_properties(Rest1, Ver),
    Publish = #mqtt_packet_publish{topic_name = TopicName,
                                   packet_id  = PacketId,
                                   properties = Properties
                                  },
    {Publish, Payload};

parse_packet(#mqtt_packet_header{type = PubAck}, <<PacketId:16/big>>, #{strict_mode := StrictMode})
  when ?PUBACK =< PubAck, PubAck =< ?PUBCOMP ->
    StrictMode andalso validate_packet_id(PacketId),
    #mqtt_packet_puback{packet_id = PacketId, reason_code = 0};

parse_packet(#mqtt_packet_header{type = PubAck}, <<PacketId:16/big, ReasonCode, Rest/binary>>,
             #{strict_mode := StrictMode, version := Ver = ?MQTT_PROTO_V5})
  when ?PUBACK =< PubAck, PubAck =< ?PUBCOMP ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, <<>>} = parse_properties(Rest, Ver),
    #mqtt_packet_puback{packet_id   = PacketId,
                        reason_code = ReasonCode,
                        properties  = Properties
                       };

parse_packet(#mqtt_packet_header{type = ?SUBSCRIBE}, <<PacketId:16/big, Rest/binary>>,
             #{strict_mode := StrictMode, version := Ver}) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver),
    TopicFilters = parse_topic_filters(subscribe, Rest1),
    ok = validate_subqos([QoS || {_, #{qos := QoS}} <- TopicFilters]),
    #mqtt_packet_subscribe{packet_id     = PacketId,
                           properties    = Properties,
                           topic_filters = TopicFilters
                          };

parse_packet(#mqtt_packet_header{type = ?SUBACK}, <<PacketId:16/big, Rest/binary>>,
             #{strict_mode := StrictMode, version := Ver}) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver),
    ReasonCodes = parse_reason_codes(Rest1),
    #mqtt_packet_suback{packet_id    = PacketId,
                        properties   = Properties,
                        reason_codes = ReasonCodes
                       };

parse_packet(#mqtt_packet_header{type = ?UNSUBSCRIBE}, <<PacketId:16/big, Rest/binary>>,
             #{strict_mode := StrictMode, version := Ver}) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver),
    TopicFilters = parse_topic_filters(unsubscribe, Rest1),
    #mqtt_packet_unsubscribe{packet_id     = PacketId,
                             properties    = Properties,
                             topic_filters = TopicFilters
                            };

parse_packet(#mqtt_packet_header{type = ?UNSUBACK}, <<PacketId:16/big>>,
             #{strict_mode := StrictMode}) ->
    StrictMode andalso validate_packet_id(PacketId),
    #mqtt_packet_unsuback{packet_id = PacketId};

parse_packet(#mqtt_packet_header{type = ?UNSUBACK}, <<PacketId:16/big, Rest/binary>>,
             #{strict_mode := StrictMode, version := Ver}) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver),
    ReasonCodes = parse_reason_codes(Rest1),
    #mqtt_packet_unsuback{packet_id    = PacketId,
                          properties   = Properties,
                          reason_codes = ReasonCodes
                         };

parse_packet(#mqtt_packet_header{type = ?DISCONNECT}, <<ReasonCode, Rest/binary>>,
             #{version := ?MQTT_PROTO_V5}) ->
    {Properties, <<>>} = parse_properties(Rest, ?MQTT_PROTO_V5),
    #mqtt_packet_disconnect{reason_code = ReasonCode,
                            properties  = Properties
                           };

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
                                will_payload = Payload
                               }, Rest2};
parse_will_message(Packet, Bin) -> {Packet, Bin}.

-compile({inline, [parse_packet_id/1]}).
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
            UserProps1 = lists:append(UserProps, [Pair]),
            parse_property(Rest, Props#{'User-Property' := UserProps1});
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
    [{Topic, #{rh => Rh, rap => Rap, nl => Nl, qos => QoS}}
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

serialize_fun() -> serialize_fun(?DEFAULT_OPTIONS).

serialize_fun(#mqtt_packet_connect{proto_ver = ProtoVer, properties = ConnProps}) ->
    MaxSize = get_property('Maximum-Packet-Size', ConnProps, ?MAX_PACKET_SIZE),
    serialize_fun(#{version => ProtoVer, max_size => MaxSize});

serialize_fun(#{version := Ver, max_size := MaxSize}) ->
    fun(Packet) ->
        IoData = serialize(Packet, Ver),
        case is_too_large(IoData, MaxSize) of
            true  -> <<>>;
            false -> IoData
        end
    end.

-spec(serialize(emqx_types:packet()) -> iodata()).
serialize(Packet) -> serialize(Packet, ?MQTT_PROTO_V4).

-spec(serialize(emqx_types:packet(), emqx_types:version()) -> iodata()).
serialize(#mqtt_packet{header   = Header,
                       variable = Variable,
                       payload  = Payload}, Ver) ->
    serialize(Header, serialize_variable(Variable, Ver), serialize_payload(Payload)).

serialize(#mqtt_packet_header{type   = Type,
                              dup    = Dup,
                              qos    = QoS,
                              retain = Retain
                             }, VariableBin, PayloadBin)
    when ?CONNECT =< Type andalso Type =< ?AUTH ->
    Len = iolist_size(VariableBin) + iolist_size(PayloadBin),
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
                      clientid     = ClientId,
                      will_props   = WillProps,
                      will_topic   = WillTopic,
                      will_payload = WillPayload,
                      username     = Username,
                      password     = Password}, _Ver) ->
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
                                        properties  = Properties}, Ver) ->
    [AckFlags, ReasonCode, serialize_properties(Properties, Ver)];

serialize_variable(#mqtt_packet_publish{topic_name = TopicName,
                                        packet_id  = PacketId,
                                        properties = Properties}, Ver) ->
    [serialize_utf8_string(TopicName),
     if
         PacketId =:= undefined -> <<>>;
         true -> <<PacketId:16/big-unsigned-integer>>
     end,
     serialize_properties(Properties, Ver)];

serialize_variable(#mqtt_packet_puback{packet_id = PacketId}, Ver)
    when Ver == ?MQTT_PROTO_V3; Ver == ?MQTT_PROTO_V4 ->
    <<PacketId:16/big-unsigned-integer>>;
serialize_variable(#mqtt_packet_puback{packet_id   = PacketId,
                                       reason_code = ReasonCode,
                                       properties  = Properties
                                      },
                   Ver = ?MQTT_PROTO_V5) ->
    [<<PacketId:16/big-unsigned-integer>>, ReasonCode,
     serialize_properties(Properties, Ver)];

serialize_variable(#mqtt_packet_subscribe{packet_id     = PacketId,
                                          properties    = Properties,
                                          topic_filters = TopicFilters}, Ver) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     serialize_topic_filters(subscribe, TopicFilters, Ver)];

serialize_variable(#mqtt_packet_suback{packet_id    = PacketId,
                                       properties   = Properties,
                                       reason_codes = ReasonCodes}, Ver) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     serialize_reason_codes(ReasonCodes)];

serialize_variable(#mqtt_packet_unsubscribe{packet_id     = PacketId,
                                            properties    = Properties,
                                            topic_filters = TopicFilters}, Ver) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     serialize_topic_filters(unsubscribe, TopicFilters, Ver)];

serialize_variable(#mqtt_packet_unsuback{packet_id    = PacketId,
                                         properties   = Properties,
                                         reason_codes = ReasonCodes}, Ver) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     serialize_reason_codes(ReasonCodes)];

serialize_variable(#mqtt_packet_disconnect{}, Ver)
    when Ver == ?MQTT_PROTO_V3; Ver == ?MQTT_PROTO_V4 ->
    <<>>;

serialize_variable(#mqtt_packet_disconnect{reason_code = ReasonCode,
                                           properties  = Properties},
                   Ver = ?MQTT_PROTO_V5) ->
    [ReasonCode, serialize_properties(Properties, Ver)];
serialize_variable(#mqtt_packet_disconnect{}, _Ver) ->
    <<>>;

serialize_variable(#mqtt_packet_auth{reason_code = ReasonCode,
                                     properties  = Properties},
                   Ver = ?MQTT_PROTO_V5) ->
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
       || {Topic, #{rh := Rh, rap := Rap, nl := Nl, qos := QoS}} <- TopicFilters >>;

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

%% Is the frame too large?
-spec(is_too_large(iodata(), pos_integer()) -> boolean()).
is_too_large(IoData, MaxSize) ->
    iolist_size(IoData) >= MaxSize.

get_property(_Key, undefined, Default) ->
    Default;
get_property(Key, Props, Default) ->
    maps:get(Key, Props, Default).

%% Validate header if sctrict mode. See: mqtt-v5.0: 2.1.3 Flags
validate_header(?CONNECT, 0, 0, 0)      -> ok;
validate_header(?CONNACK, 0, 0, 0)      -> ok;
validate_header(?PUBLISH, 0, ?QOS_0, _) -> ok;
validate_header(?PUBLISH, _, ?QOS_1, _) -> ok;
validate_header(?PUBLISH, 0, ?QOS_2, _) -> ok;
validate_header(?PUBACK, 0, 0, 0)       -> ok;
validate_header(?PUBREC, 0, 0, 0)       -> ok;
validate_header(?PUBREL, 0, 1, 0)       -> ok;
validate_header(?PUBCOMP, 0, 0, 0)      -> ok;
validate_header(?SUBSCRIBE, 0, 1, 0)    -> ok;
validate_header(?SUBACK, 0, 0, 0)       -> ok;
validate_header(?UNSUBSCRIBE, 0, 1, 0)  -> ok;
validate_header(?UNSUBACK, 0, 0, 0)     -> ok;
validate_header(?PINGREQ, 0, 0, 0)      -> ok;
validate_header(?PINGRESP, 0, 0, 0)     -> ok;
validate_header(?DISCONNECT, 0, 0, 0)   -> ok;
validate_header(?AUTH, 0, 0, 0)         -> ok;
validate_header(_Type, _Dup, _QoS, _Rt) -> error(bad_frame_header).

-compile({inline, [validate_packet_id/1]}).
validate_packet_id(0) -> error(bad_packet_id);
validate_packet_id(_) -> ok.

validate_subqos([3|_]) -> error(bad_subqos);
validate_subqos([_|T]) -> validate_subqos(T);
validate_subqos([])    -> ok.

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

