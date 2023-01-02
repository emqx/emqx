%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , serialize_opts/0
        , serialize_opts/1
        , serialize_pkt/2
        , serialize/1
        , serialize/2
        ]).

-export_type([ options/0
             , parse_state/0
             , parse_result/0
             , serialize_opts/0
             ]).

-define(Q(BYTES, Q), {BYTES, Q}).

-type(options() :: #{strict_mode => boolean(),
                     max_size => 1..?MAX_PACKET_SIZE,
                     version => emqx_types:version()
                    }).

-type(parse_state() :: {none, options()} | {cont_state(), options()}).

-type(parse_result() :: {more, parse_state()}
                      | {ok, emqx_types:packet(), binary(), parse_state()}).

-type(cont_state() ::
      {Stage :: len | body,
       State ::  #{hdr := #mqtt_packet_header{},
                   len := {pos_integer(), non_neg_integer()} | non_neg_integer(),
                   rest => binary() | ?Q(non_neg_integer(), queue:queue(binary()))
                  }}).

-type(serialize_opts() :: options()).

-define(none(Options), {none, Options}).

-define(DEFAULT_OPTIONS,
        #{strict_mode => false,
          max_size    => ?MAX_PACKET_SIZE,
          version     => ?MQTT_PROTO_V4
         }).

-define(MULTIPLIER_MAX, 16#200000).

%% proxy_protocol v1 header human readable
-define(PPV1_PROXY, "PROXY ").
-define(PPV1_PROXY_UNKNOWN, "PROXY UNKNOWN").
%% proxy_protocol v2 header signature:
%% 16#0D,16#0A, 16#0D,16#0A,16#00,16#0D,16#0A,16#51,16#55,16#49,16#54,16#0A
-define(PPV2_HEADER_SIG, "\r\n\r\n\0\r\nQUIT\n").

-ifdef(TEST).
-export([parse_variable_byte_integer/1]).
-endif.

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
    {more, {none, Options}};
parse(<<?PPV1_PROXY, IPVer:5/binary, _Rest/binary>>, {none, _Options})
  when IPVer =:= <<"TCP4 ">> orelse IPVer =:= <<"TCP6 ">> ->
    error(proxy_protocol_config_disabled);
parse(<<?PPV1_PROXY_UNKNOWN, _Rest/binary>>, {none, _Options}) ->
    error(proxy_protocol_config_disabled);
parse(<<?PPV2_HEADER_SIG, _Rest/binary>>, {none, _Options}) ->
    error(proxy_protocol_config_disabled);
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

parse(Bin, {{len, #{hdr := Header,
                    len := {Multiplier, Length}}
             }, Options}) when is_binary(Bin) ->
    parse_remaining_len(Bin, Header, Multiplier, Length, Options);
parse(Bin, {{body, #{hdr := Header,
                     len := Length,
                     rest := Body}
             }, Options}) when is_binary(Bin) ->
    NewBody = append_body(Body, Bin),
    parse_frame(NewBody, Header, Length, Options).

parse_remaining_len(<<>>, Header, Options) ->
    {more, {{len, #{hdr => Header, len => {1, 0}}}, Options}};
parse_remaining_len(Rest, Header, Options) ->
    parse_remaining_len(Rest, Header, 1, 0, Options).

parse_remaining_len(_Bin, _Header, _Multiplier, Length, #{max_size := MaxSize})
  when Length > MaxSize ->
    error(frame_too_large);
parse_remaining_len(<<>>, Header, Multiplier, Length, Options) ->
    {more, {{len, #{hdr => Header, len => {Multiplier, Length}}}, Options}};
%% Match DISCONNECT without payload
parse_remaining_len(<<0:8, Rest/binary>>,
                    Header = #mqtt_packet_header{type = ?DISCONNECT}, 1, 0, Options) ->
    Packet = packet(Header, #mqtt_packet_disconnect{reason_code = ?RC_SUCCESS}),
    {ok, Packet, Rest, ?none(Options)};
%% Match PINGREQ.
parse_remaining_len(<<0:8, Rest/binary>>, Header, 1, 0, Options) ->
    parse_frame(Rest, Header, 0, Options);
%% Match PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK...
parse_remaining_len(<<0:1, 2:7, Rest/binary>>, Header, 1, 0, Options) ->
    parse_frame(Rest, Header, 2, Options);
parse_remaining_len(<<1:1, _Len:7, _Rest/binary>>, _Header, Multiplier, _Value, _Options)
  when Multiplier > ?MULTIPLIER_MAX ->
    error(malformed_variable_byte_integer);
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Header, Multiplier, Value, Options) ->
    parse_remaining_len(Rest, Header, Multiplier * ?HIGHBIT, Value + Len * Multiplier, Options);
parse_remaining_len(<<0:1, Len:7, Rest/binary>>, Header, Multiplier, Value,
                    Options = #{max_size := MaxSize}) ->
    FrameLen = Value + Len * Multiplier,
    case FrameLen > MaxSize of
        true -> error(frame_too_large);
        false -> parse_frame(Rest, Header, FrameLen, Options)
    end.

body_bytes(B) when is_binary(B) -> size(B);
body_bytes(?Q(Bytes, _)) -> Bytes.

append_body(H, T) when is_binary(H) andalso size(H) < 1024 ->
    <<H/binary, T/binary>>;
append_body(H, T) when is_binary(H) ->
    Bytes = size(H) + size(T),
    ?Q(Bytes, queue:from_list([H, T]));
append_body(?Q(Bytes, Q), T) ->
    ?Q(Bytes + iolist_size(T), queue:in(T, Q)).

flatten_body(Body) when is_binary(Body) -> Body;
flatten_body(?Q(_, Q)) -> iolist_to_binary(queue:to_list(Q)).

parse_frame(Body, Header, 0, Options) ->
    {ok, packet(Header), flatten_body(Body), ?none(Options)};
parse_frame(Body, Header, Length, Options) ->
    case body_bytes(Body) >= Length of
        true ->
            <<FrameBin:Length/binary, Rest/binary>> = flatten_body(Body),
            case parse_packet(Header, FrameBin, Options) of
                {Variable, Payload} ->
                    {ok, packet(Header, Variable, Payload), Rest, ?none(Options)};
                Variable = #mqtt_packet_connect{proto_ver = Ver} ->
                    {ok, packet(Header, Variable), Rest, ?none(Options#{version := Ver})};
                Variable ->
                    {ok, packet(Header, Variable), Rest, ?none(Options)}
            end;
        false ->
            {more, {{body, #{hdr => Header,
                             len => Length,
                             rest => Body
                            }}, Options}}
    end.

-compile({inline, [packet/1, packet/2, packet/3]}).
packet(Header) ->
    #mqtt_packet{header = Header}.
packet(Header, Variable) ->
    #mqtt_packet{header = Header, variable = Variable}.
packet(Header, Variable, Payload) ->
    #mqtt_packet{header = Header, variable = Variable, payload = Payload}.

parse_packet(#mqtt_packet_header{type = ?CONNECT}, FrameBin,
             #{strict_mode := StrictMode}) ->
    {ProtoName, Rest} = parse_utf8_string(FrameBin, StrictMode),
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

    {Properties, Rest3} = parse_properties(Rest2, ProtoVer, StrictMode),
    {ClientId, Rest4} = parse_utf8_string(Rest3, StrictMode),
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
    {ConnPacket1, Rest5} = parse_will_message(ConnPacket, Rest4, StrictMode),
    {Username, Rest6} = parse_utf8_string(Rest5, StrictMode, bool(UsernameFlag)),
    {Passsword, <<>>} = parse_utf8_string(Rest6, StrictMode, bool(PasswordFlag)),
    ConnPacket1#mqtt_packet_connect{username = Username, password = Passsword};

parse_packet(#mqtt_packet_header{type = ?CONNACK}, <<AckFlags:8, ReasonCode:8, Rest/binary>>,
             #{strict_mode := StrictMode, version := Ver}) ->
    {Properties, <<>>} = parse_properties(Rest, Ver, StrictMode),
    #mqtt_packet_connack{ack_flags   = AckFlags,
                         reason_code = ReasonCode,
                         properties  = Properties
                        };

parse_packet(#mqtt_packet_header{type = ?PUBLISH, qos = QoS}, Bin,
             #{strict_mode := StrictMode, version := Ver}) ->
    {TopicName, Rest} = parse_utf8_string(Bin, StrictMode),
    {PacketId, Rest1} = case QoS of
                            ?QOS_0 -> {undefined, Rest};
                            _ -> parse_packet_id(Rest)
                        end,
    (PacketId =/= undefined) andalso
      StrictMode andalso validate_packet_id(PacketId),
    {Properties, Payload} = parse_properties(Rest1, Ver, StrictMode),
    Publish = #mqtt_packet_publish{topic_name = TopicName,
                                   packet_id  = PacketId,
                                   properties = Properties
                                  },
    {Publish, Payload};

parse_packet(#mqtt_packet_header{type = PubAck}, <<PacketId:16/big>>,
             #{strict_mode := StrictMode})
  when ?PUBACK =< PubAck, PubAck =< ?PUBCOMP ->
    StrictMode andalso validate_packet_id(PacketId),
    #mqtt_packet_puback{packet_id = PacketId, reason_code = 0};

parse_packet(#mqtt_packet_header{type = PubAck}, <<PacketId:16/big, ReasonCode, Rest/binary>>,
             #{strict_mode := StrictMode, version := Ver = ?MQTT_PROTO_V5})
  when ?PUBACK =< PubAck, PubAck =< ?PUBCOMP ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, <<>>} = parse_properties(Rest, Ver, StrictMode),
    #mqtt_packet_puback{packet_id   = PacketId,
                        reason_code = ReasonCode,
                        properties  = Properties
                       };

parse_packet(#mqtt_packet_header{type = ?SUBSCRIBE}, <<PacketId:16/big, Rest/binary>>,
             #{strict_mode := StrictMode, version := Ver}) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    TopicFilters = parse_topic_filters(subscribe, Rest1),
    ok = validate_subqos([QoS || {_, #{qos := QoS}} <- TopicFilters]),
    #mqtt_packet_subscribe{packet_id     = PacketId,
                           properties    = Properties,
                           topic_filters = TopicFilters
                          };

parse_packet(#mqtt_packet_header{type = ?SUBACK}, <<PacketId:16/big, Rest/binary>>,
             #{strict_mode := StrictMode, version := Ver}) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    ReasonCodes = parse_reason_codes(Rest1),
    #mqtt_packet_suback{packet_id    = PacketId,
                        properties   = Properties,
                        reason_codes = ReasonCodes
                       };

parse_packet(#mqtt_packet_header{type = ?UNSUBSCRIBE}, <<PacketId:16/big, Rest/binary>>,
             #{strict_mode := StrictMode, version := Ver}) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
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
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    ReasonCodes = parse_reason_codes(Rest1),
    #mqtt_packet_unsuback{packet_id    = PacketId,
                          properties   = Properties,
                          reason_codes = ReasonCodes
                         };

parse_packet(#mqtt_packet_header{type = ?DISCONNECT}, <<ReasonCode, Rest/binary>>,
             #{strict_mode := StrictMode, version := ?MQTT_PROTO_V5}) ->
    {Properties, <<>>} = parse_properties(Rest, ?MQTT_PROTO_V5, StrictMode),
    #mqtt_packet_disconnect{reason_code = ReasonCode,
                            properties  = Properties
                           };

parse_packet(#mqtt_packet_header{type = ?AUTH}, <<ReasonCode, Rest/binary>>,
             #{strict_mode := StrictMode, version := ?MQTT_PROTO_V5}) ->
    {Properties, <<>>} = parse_properties(Rest, ?MQTT_PROTO_V5, StrictMode),
    #mqtt_packet_auth{reason_code = ReasonCode, properties = Properties}.

parse_will_message(Packet = #mqtt_packet_connect{will_flag = true,
                                                 proto_ver = Ver},
                   Bin, StrictMode) ->
    {Props, Rest} = parse_properties(Bin, Ver, StrictMode),
    {Topic, Rest1} = parse_utf8_string(Rest, StrictMode),
    {Payload, Rest2} = parse_binary_data(Rest1),
    {Packet#mqtt_packet_connect{will_props   = Props,
                                will_topic   = Topic,
                                will_payload = Payload
                               }, Rest2};
parse_will_message(Packet, Bin, _StrictMode) -> {Packet, Bin}.

-compile({inline, [parse_packet_id/1]}).
parse_packet_id(<<PacketId:16/big, Rest/binary>>) ->
    {PacketId, Rest}.

parse_properties(Bin, Ver, _StrictMode) when Ver =/= ?MQTT_PROTO_V5 ->
    {#{}, Bin};
%% TODO: version mess?
parse_properties(<<>>, ?MQTT_PROTO_V5, _StrictMode) ->
    {#{}, <<>>};
parse_properties(<<0, Rest/binary>>, ?MQTT_PROTO_V5, _StrictMode) ->
    {#{}, Rest};
parse_properties(Bin, ?MQTT_PROTO_V5, StrictMode) ->
    {Len, Rest} = parse_variable_byte_integer(Bin),
    <<PropsBin:Len/binary, Rest1/binary>> = Rest,
    {parse_property(PropsBin, #{}, StrictMode), Rest1}.

parse_property(<<>>, Props, _StrictMode) ->
    Props;
parse_property(<<16#01, Val, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Payload-Format-Indicator' => Val}, StrictMode);
parse_property(<<16#02, Val:32/big, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Message-Expiry-Interval' => Val}, StrictMode);
parse_property(<<16#03, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode),
    parse_property(Rest, Props#{'Content-Type' => Val}, StrictMode);
parse_property(<<16#08, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode),
    parse_property(Rest, Props#{'Response-Topic' => Val}, StrictMode);
parse_property(<<16#09, Len:16/big, Val:Len/binary, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Correlation-Data' => Val}, StrictMode);
parse_property(<<16#0B, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_variable_byte_integer(Bin),
    parse_property(Rest, Props#{'Subscription-Identifier' => Val}, StrictMode);
parse_property(<<16#11, Val:32/big, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Session-Expiry-Interval' => Val}, StrictMode);
parse_property(<<16#12, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode),
    parse_property(Rest, Props#{'Assigned-Client-Identifier' => Val}, StrictMode);
parse_property(<<16#13, Val:16, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Server-Keep-Alive' => Val}, StrictMode);
parse_property(<<16#15, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode),
    parse_property(Rest, Props#{'Authentication-Method' => Val}, StrictMode);
parse_property(<<16#16, Len:16/big, Val:Len/binary, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Authentication-Data' => Val}, StrictMode);
parse_property(<<16#17, Val, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Request-Problem-Information' => Val}, StrictMode);
parse_property(<<16#18, Val:32, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Will-Delay-Interval' => Val}, StrictMode);
parse_property(<<16#19, Val, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Request-Response-Information' => Val}, StrictMode);
parse_property(<<16#1A, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode),
    parse_property(Rest, Props#{'Response-Information' => Val}, StrictMode);
parse_property(<<16#1C, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode),
    parse_property(Rest, Props#{'Server-Reference' => Val}, StrictMode);
parse_property(<<16#1F, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode),
    parse_property(Rest, Props#{'Reason-String' => Val}, StrictMode);
parse_property(<<16#21, Val:16/big, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Receive-Maximum' => Val}, StrictMode);
parse_property(<<16#22, Val:16/big, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Topic-Alias-Maximum' => Val}, StrictMode);
parse_property(<<16#23, Val:16/big, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Topic-Alias' => Val}, StrictMode);
parse_property(<<16#24, Val, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Maximum-QoS' => Val}, StrictMode);
parse_property(<<16#25, Val, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Retain-Available' => Val}, StrictMode);
parse_property(<<16#26, Bin/binary>>, Props, StrictMode) ->
    {Pair, Rest} = parse_utf8_pair(Bin, StrictMode),
    case maps:find('User-Property', Props) of
        {ok, UserProps} ->
            UserProps1 = lists:append(UserProps, [Pair]),
            parse_property(Rest, Props#{'User-Property' := UserProps1}, StrictMode);
        error ->
            parse_property(Rest, Props#{'User-Property' => [Pair]}, StrictMode)
    end;
parse_property(<<16#27, Val:32, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Maximum-Packet-Size' => Val}, StrictMode);
parse_property(<<16#28, Val, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Wildcard-Subscription-Available' => Val}, StrictMode);
parse_property(<<16#29, Val, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Subscription-Identifier-Available' => Val}, StrictMode);
parse_property(<<16#2A, Val, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Shared-Subscription-Available' => Val}, StrictMode);
parse_property(<<Property:8, _Rest/binary>>, _Props, _StrictMode) ->
    error(#{invalid_property_code => Property}).
%% TODO: invalid property in specific packet.

parse_variable_byte_integer(Bin) ->
    parse_variable_byte_integer(Bin, 1, 0).
parse_variable_byte_integer(<<1:1, _Len:7, _Rest/binary>>, Multiplier, _Value)
  when Multiplier > ?MULTIPLIER_MAX ->
    error(malformed_variable_byte_integer);
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

%%--------------------
%% parse utf8 pair
parse_utf8_pair( <<Len1:16/big, Key:Len1/binary,
                   Len2:16/big, Val:Len2/binary, Rest/binary>>
               , true) ->
    {{validate_utf8(Key), validate_utf8(Val)}, Rest};
parse_utf8_pair( <<Len1:16/big, Key:Len1/binary,
                   Len2:16/big, Val:Len2/binary, Rest/binary>>
               , false) ->
    {{Key, Val}, Rest};
parse_utf8_pair(<<LenK:16/big, Rest/binary>>, _StrictMode)
  when LenK > byte_size(Rest) ->
    error(user_property_not_enough_bytes);
parse_utf8_pair(<<LenK:16/big, _Key:LenK/binary, %% key maybe malformed
                  LenV:16/big, Rest/binary>>, _StrictMode)
  when LenV > byte_size(Rest) ->
    error(malformed_user_property_value);
parse_utf8_pair(Bin, _StrictMode)
  when 4 > byte_size(Bin) ->
    error(user_property_not_enough_bytes).

%%--------------------
%% parse utf8 string
parse_utf8_string(Bin, _StrictMode, false) ->
    {undefined, Bin};
parse_utf8_string(Bin, StrictMode, true) ->
    parse_utf8_string(Bin, StrictMode).

parse_utf8_string(<<Len:16/big, Str:Len/binary, Rest/binary>>, true) ->
    {validate_utf8(Str), Rest};
parse_utf8_string(<<Len:16/big, Str:Len/binary, Rest/binary>>, false) ->
    {Str, Rest};
parse_utf8_string(<<Len:16/big, Rest/binary>>, _)
  when Len > byte_size(Rest) ->
    error(malformed_utf8_string);
parse_utf8_string(Bin, _)
  when 2 > byte_size(Bin) ->
    error(malformed_utf8_string_length).

parse_binary_data(<<Len:16/big, Data:Len/binary, Rest/binary>>) ->
    {Data, Rest};
parse_binary_data(<<Len:16/big, Rest/binary>>)
  when Len > byte_size(Rest) ->
    error(malformed_binary_data);
parse_binary_data(Bin)
  when 2 > byte_size(Bin) ->
    error(malformed_binary_data_length).

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

serialize_opts() ->
    ?DEFAULT_OPTIONS.

serialize_opts(#mqtt_packet_connect{proto_ver = ProtoVer, properties = ConnProps}) ->
    MaxSize = get_property('Maximum-Packet-Size', ConnProps, ?MAX_PACKET_SIZE),
    #{version => ProtoVer, max_size => MaxSize}.

serialize_pkt(Packet, #{version := Ver, max_size := MaxSize}) ->
    IoData = serialize(Packet, Ver),
    case is_too_large(IoData, MaxSize) of
        true -> <<>>;
        false -> IoData
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

validate_utf8(Bin) ->
    case unicode:characters_to_binary(Bin) of
        {error, _, _} ->
            error(utf8_string_invalid);
        {incomplete, _, _} ->
            error(utf8_string_invalid);
        Bin when is_binary(Bin) ->
            case validate_mqtt_utf8_char(Bin) of
                true -> Bin;
                false -> error(utf8_string_invalid)
            end
    end.

%% Is the utf8 string respecting UTF-8 characters defined by MQTT Spec?
%% i.e. contains invalid UTF-8 char or control char
validate_mqtt_utf8_char(<<>>) ->
    true;
%% ==== 1-Byte UTF-8 invalid: [[U+0000 .. U+001F] && [U+007F]]
validate_mqtt_utf8_char(<<B1, Bs/binary>>)
  when B1 >= 16#20, B1 =< 16#7E ->
    validate_mqtt_utf8_char(Bs);
validate_mqtt_utf8_char(<<B1, _Bs/binary>>)
  when B1 >=  16#00, B1 =< 16#1F;
       B1 =:= 16#7F ->
    %% [U+0000 .. U+001F] && [U+007F]
    false;
%% ==== 2-Bytes UTF-8 invalid: [U+0080 .. U+009F]
validate_mqtt_utf8_char(<<B1, B2, Bs/binary>>)
  when B1 =:= 16#C2;
       B2 >=  16#A0, B2 =< 16#BF;
       B1  >  16#C3, B1 =< 16#DE;
       B2 >=  16#80, B2 =< 16#BF ->
    validate_mqtt_utf8_char(Bs);
validate_mqtt_utf8_char(<<16#C2, B2, _Bs/binary>>)
  when B2 >= 16#80, B2 =< 16#9F ->
    %% [U+0080 .. U+009F]
    false;
%% ==== 3-Bytes UTF-8 invalid: [U+D800 .. U+DFFF]
validate_mqtt_utf8_char(<<B1, _B2, _B3, Bs/binary>>)
  when B1 >= 16#E0, B1 =< 16#EE;
       B1 =:= 16#EF ->
    validate_mqtt_utf8_char(Bs);
validate_mqtt_utf8_char(<<16#ED, _B2, _B3, _Bs/binary>>) ->
    false;
%% ==== 4-Bytes UTF-8
validate_mqtt_utf8_char(<<B1, _B2, _B3, _B4, Bs/binary>>)
  when B1 =:= 16#0F ->
    validate_mqtt_utf8_char(Bs).
