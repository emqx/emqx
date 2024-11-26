%%--------------------------------------------------------------------
%% Copyright (c) 2018-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_frame_parser).

-include("emqx_mqtt.hrl").

-export([
    init_options/1,
    update_options/2,
    parse/2
]).

-export_type([
    options/0
]).

-type options() :: #{
    strict_mode => boolean(),
    version => emqx_types:proto_ver()
}.

-define(DEFAULT_OPTIONS, #{
    strict_mode => false,
    version => ?MQTT_PROTO_V4
}).

-record(opts, {
    strict_mode :: boolean(),
    version :: emqx_types:proto_ver()
}).

-type stopts() :: #opts{}.

-define(PARSE_ERR(REASON), ?THROW_FRAME_ERROR(REASON)).
-define(PARSE_ERR(REASON, CTX), ?THROW_FRAME_ERROR(CTX#{cause => REASON})).

-define(PARSE_OPTIONAL(FLAG, EXPR, BYTES),
    case (FLAG) of
        true -> EXPR;
        false -> {undefined, BYTES}
    end
).

-define(PACKET(HEADER, VARIABLE), #mqtt_packet{
    header = HEADER,
    variable = VARIABLE
}).

-define(PACKET(HEADER, VARIABLE, PAYLOAD), #mqtt_packet{
    header = HEADER,
    variable = VARIABLE,
    payload = PAYLOAD
}).

%%--------------------------------------------------------------------
%% Parse MQTT Frame
%%--------------------------------------------------------------------

init_options(Overrides) ->
    EffectiveOptions = maps:merge(?DEFAULT_OPTIONS, Overrides),
    #opts{
        strict_mode = maps:get(strict_mode, EffectiveOptions),
        version = maps:get(version, EffectiveOptions)
    }.

update_options(#mqtt_packet{variable = #mqtt_packet_connect{proto_ver = Ver}}, Options) ->
    Options#opts{version = Ver};
update_options(_, Options) ->
    Options.

-spec parse(binary(), stopts()) -> emqx_types:packet().
parse(
    <<Type:4, Dup:1, QoS:2, Retain:1, Rest/binary>>,
    Options = #opts{strict_mode = StrictMode}
) ->
    %% Validate header if strict mode.
    StrictMode andalso validate_header(Type, Dup, QoS, Retain),
    Header = #mqtt_packet_header{
        type = Type,
        dup = bool(Dup),
        qos = fixqos(Type, QoS),
        retain = bool(Retain)
    },
    parse_remaining_len(Rest, Header, Options).

parse_remaining_len(Bytes, Header, Options) ->
    parse_variable(skip_variable_byte_integer(Bytes), Header, Options).

parse_variable(Bytes, Header, Options) when byte_size(Bytes) > 0 ->
    parse_packet(Bytes, Header, Options);
parse_variable(<<>>, Header = #mqtt_packet_header{type = Type}, _Options) ->
    case Type of
        %% Match DISCONNECT without payload
        ?DISCONNECT ->
            #mqtt_packet{
                header = Header,
                variable = #mqtt_packet_disconnect{reason_code = ?RC_SUCCESS}
            };
        %% Match PINGREQ.
        ?PINGREQ ->
            #mqtt_packet{header = Header};
        ?PINGRESP ->
            ?PARSE_ERR(#{cause => unexpected_packet, header_type => 'PINGRESP'});
        %% All other types of messages should not have a zero remaining length.
        _ ->
            ?PARSE_ERR(#{cause => zero_remaining_len, header_type => Header#mqtt_packet_header.type})
    end.

parse_connect(Frame, Header, Options = #opts{strict_mode = StrictMode}) ->
    {ProtoName, Rest0} = parse_utf8_string(Frame, StrictMode, invalid_proto_name),
    %% No need to parse and check proto_ver if proto_name is invalid, check it first
    %% And the matching check of `proto_name` and `proto_ver` fields will be done in `emqx_packet:check_proto_ver/2`
    _ = validate_proto_name(ProtoName),
    {IsBridge, ProtoVer, Rest2} = parse_connect_proto_ver(Rest0),
    NOptions = Options#opts{version = ProtoVer},
    parse_connect(Rest2, Header, ProtoName, ProtoVer, IsBridge, NOptions).

parse_connect(
    <<
        UsernameFlagB:1,
        PasswordFlagB:1,
        WillRetainB:1,
        WillQoS:2,
        WillFlagB:1,
        CleanStart:1,
        Reserved:1,
        KeepAlive:16/big,
        Rest/binary
    >>,
    Header,
    ProtoName,
    ProtoVer,
    IsBridge,
    #opts{strict_mode = StrictMode}
) ->
    Ctx = #{proto_name => ProtoName, proto_ver => ProtoVer},
    _ = validate_connect_reserved(Reserved, Ctx),
    _ = validate_connect_will(
        WillFlag = bool(WillFlagB),
        WillRetain = bool(WillRetainB),
        WillQoS,
        Ctx
    ),
    _ = validate_connect_password_flag(
        StrictMode,
        ProtoVer,
        UsernameFlag = bool(UsernameFlagB),
        PasswordFlag = bool(PasswordFlagB),
        Ctx
    ),
    {Properties, Rest3} = parse_properties(Rest, ProtoVer, StrictMode),
    {ClientId, Rest4} = parse_utf8_string(Rest3, StrictMode, invalid_clientid),
    ConnPacket = #mqtt_packet_connect{
        proto_name = ProtoName,
        proto_ver = ProtoVer,
        %% For bridge mode, non-standard implementation
        %% Invented by mosquitto, named 'try_private': https://mosquitto.org/man/mosquitto-conf-5.html
        is_bridge = IsBridge,
        clean_start = bool(CleanStart),
        will_flag = WillFlag,
        will_qos = WillQoS,
        will_retain = WillRetain,
        keepalive = KeepAlive,
        properties = Properties,
        clientid = ClientId
    },
    {ConnPacket1, Rest5} = parse_will_message(ConnPacket, Rest4, StrictMode),
    {Username, Rest6} = ?PARSE_OPTIONAL(
        UsernameFlag,
        parse_utf8_string(Rest5, StrictMode, invalid_username),
        Rest5
    ),
    {Password, Rest7} = ?PARSE_OPTIONAL(
        PasswordFlag,
        parse_utf8_string(Rest6, StrictMode, invalid_password),
        Rest6
    ),
    case Rest7 of
        <<>> ->
            ?PACKET(
                Header,
                ConnPacket1#mqtt_packet_connect{username = Username, password = Password}
            );
        _ ->
            ?PARSE_ERR(Ctx#{
                cause => malformed_connect,
                unexpected_trailing_bytes => size(Rest7)
            })
    end;
parse_connect(Frame, _Header, ProtoName, ProtoVer, _IsBridge, _Options) ->
    %% sent less than 24 bytes
    ?PARSE_ERR(#{
        cause => malformed_connect,
        header_bytes => Frame,
        proto_name => ProtoName,
        proto_ver => ProtoVer
    }).

parse_packet(
    Frame,
    #mqtt_packet_header{type = ?CONNECT} = Header,
    Options
) ->
    parse_connect(Frame, Header, Options);
parse_packet(
    <<AckFlags:8, ReasonCode:8, Rest/binary>>,
    #mqtt_packet_header{type = ?CONNACK} = Header,
    #opts{version = Ver, strict_mode = StrictMode}
) ->
    %% Not possible for broker to receive!
    case parse_properties(Rest, Ver, StrictMode) of
        {Properties, <<>>} ->
            ?PACKET(Header, #mqtt_packet_connack{
                ack_flags = AckFlags,
                reason_code = ReasonCode,
                properties = Properties
            });
        _ ->
            ?PARSE_ERR(malformed_properties)
    end;
parse_packet(
    Bin,
    #mqtt_packet_header{type = ?PUBLISH, qos = QoS} = Header,
    #opts{strict_mode = StrictMode, version = Ver}
) ->
    {TopicName, Rest} = parse_utf8_string(Bin, StrictMode, _Cause = invalid_topic),
    {PacketId, Rest1} =
        case QoS of
            ?QOS_0 -> {undefined, Rest};
            _ -> parse_packet_id(Rest)
        end,
    (PacketId =/= undefined) andalso
        StrictMode andalso validate_packet_id(PacketId),
    {Properties, Payload} = parse_properties(Rest1, Ver, StrictMode),
    ?PACKET(
        Header,
        #mqtt_packet_publish{
            topic_name = TopicName,
            packet_id = PacketId,
            properties = Properties
        },
        Payload
    );
parse_packet(
    <<PacketId:16/big>>,
    #mqtt_packet_header{type = PubAck} = Header,
    #opts{strict_mode = StrictMode}
) when ?PUBACK =< PubAck, PubAck =< ?PUBCOMP ->
    StrictMode andalso validate_packet_id(PacketId),
    ?PACKET(Header, #mqtt_packet_puback{packet_id = PacketId, reason_code = 0});
parse_packet(
    <<PacketId:16/big, ReasonCode, Rest/binary>>,
    #mqtt_packet_header{type = PubAck} = Header,
    #opts{strict_mode = StrictMode, version = Ver = ?MQTT_PROTO_V5}
) when ?PUBACK =< PubAck, PubAck =< ?PUBCOMP ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, <<>>} = parse_properties(Rest, Ver, StrictMode),
    ?PACKET(Header, #mqtt_packet_puback{
        packet_id = PacketId,
        reason_code = ReasonCode,
        properties = Properties
    });
parse_packet(
    <<PacketId:16/big, Rest/binary>>,
    #mqtt_packet_header{type = ?SUBSCRIBE} = Header,
    #opts{strict_mode = StrictMode, version = Ver}
) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    TopicFilters = parse_topic_filters(subscribe, Rest1),
    ok = validate_subqos([QoS || {_, #{qos := QoS}} <- TopicFilters]),
    ?PACKET(Header, #mqtt_packet_subscribe{
        packet_id = PacketId,
        properties = Properties,
        topic_filters = TopicFilters
    });
parse_packet(
    <<PacketId:16/big, Rest/binary>>,
    #mqtt_packet_header{type = ?SUBACK} = Header,
    #opts{strict_mode = StrictMode, version = Ver}
) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    ReasonCodes = parse_reason_codes(Rest1),
    ?PACKET(Header, #mqtt_packet_suback{
        packet_id = PacketId,
        properties = Properties,
        reason_codes = ReasonCodes
    });
parse_packet(
    <<PacketId:16/big, Rest/binary>>,
    #mqtt_packet_header{type = ?UNSUBSCRIBE} = Header,
    #opts{strict_mode = StrictMode, version = Ver}
) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    TopicFilters = parse_topic_filters(unsubscribe, Rest1),
    ?PACKET(Header, #mqtt_packet_unsubscribe{
        packet_id = PacketId,
        properties = Properties,
        topic_filters = TopicFilters
    });
parse_packet(
    <<PacketId:16/big>>,
    #mqtt_packet_header{type = ?UNSUBACK} = Header,
    #opts{strict_mode = StrictMode}
) ->
    StrictMode andalso validate_packet_id(PacketId),
    ?PACKET(Header, #mqtt_packet_unsuback{packet_id = PacketId});
parse_packet(
    <<PacketId:16/big, Rest/binary>>,
    #mqtt_packet_header{type = ?UNSUBACK} = Header,
    #opts{strict_mode = StrictMode, version = Ver}
) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    ReasonCodes = parse_reason_codes(Rest1),
    ?PACKET(Header, #mqtt_packet_unsuback{
        packet_id = PacketId,
        properties = Properties,
        reason_codes = ReasonCodes
    });
parse_packet(
    <<ReasonCode, Rest/binary>>,
    #mqtt_packet_header{type = ?DISCONNECT} = Header,
    #opts{strict_mode = StrictMode, version = ?MQTT_PROTO_V5}
) ->
    {Properties, <<>>} = parse_properties(Rest, ?MQTT_PROTO_V5, StrictMode),
    ?PACKET(Header, #mqtt_packet_disconnect{
        reason_code = ReasonCode,
        properties = Properties
    });
parse_packet(
    <<ReasonCode, Rest/binary>>,
    #mqtt_packet_header{type = ?AUTH} = Header,
    #opts{strict_mode = StrictMode, version = ?MQTT_PROTO_V5}
) ->
    {Properties, <<>>} = parse_properties(Rest, ?MQTT_PROTO_V5, StrictMode),
    ?PACKET(Header, #mqtt_packet_auth{reason_code = ReasonCode, properties = Properties});
parse_packet(_FrameBin, Header, _Options) ->
    ?PARSE_ERR(#{cause => malformed_packet, header_type => Header#mqtt_packet_header.type}).

parse_will_message(
    Packet = #mqtt_packet_connect{
        will_flag = true,
        proto_ver = Ver
    },
    Bin,
    StrictMode
) ->
    {Props, Rest} = parse_properties(Bin, Ver, StrictMode),
    {Topic, Rest1} = parse_utf8_string(Rest, StrictMode, _Cause = invalid_topic),
    {Payload, Rest2} = parse_will_payload(Rest1),
    {
        Packet#mqtt_packet_connect{
            will_props = Props,
            will_topic = Topic,
            will_payload = Payload
        },
        Rest2
    };
parse_will_message(Packet, Bin, _StrictMode) ->
    {Packet, Bin}.

-compile({inline, [parse_packet_id/1]}).
parse_packet_id(<<PacketId:16/big, Rest/binary>>) ->
    {PacketId, Rest};
parse_packet_id(_) ->
    ?PARSE_ERR(invalid_packet_id).

parse_connect_proto_ver(<<BridgeTag:4, ProtoVer:4, Rest/binary>>) ->
    {_IsBridge = (BridgeTag =:= 8), ProtoVer, Rest};
parse_connect_proto_ver(Bin) ->
    %% sent less than 1 bytes or empty
    ?PARSE_ERR(#{cause => malformed_connect, header_bytes => Bin}).

parse_properties(Bin, Ver, _StrictMode) when Ver =/= ?MQTT_PROTO_V5 ->
    {#{}, Bin};
%% TODO: version mess?
parse_properties(<<>>, ?MQTT_PROTO_V5, _StrictMode) ->
    {#{}, <<>>};
parse_properties(<<0, Rest/binary>>, ?MQTT_PROTO_V5, _StrictMode) ->
    {#{}, Rest};
parse_properties(Bin, ?MQTT_PROTO_V5, StrictMode) ->
    {Len, Rest} = parse_variable_byte_integer(Bin),
    case Rest of
        <<PropsBin:Len/binary, Rest1/binary>> ->
            {parse_property(PropsBin, #{}, StrictMode), Rest1};
        _ ->
            ?PARSE_ERR(#{
                cause => user_property_not_enough_bytes,
                parsed_key_length => Len,
                remaining_bytes_length => byte_size(Rest)
            })
    end.

parse_property(<<>>, Props, _StrictMode) ->
    Props;
parse_property(<<16#01, Val, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Payload-Format-Indicator' => Val}, StrictMode);
parse_property(<<16#02, Val:32/big, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Message-Expiry-Interval' => Val}, StrictMode);
parse_property(<<16#03, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode, _Cause = invalid_content_type),
    parse_property(Rest, Props#{'Content-Type' => Val}, StrictMode);
parse_property(<<16#08, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode, _Cause = invalid_response_topic),
    parse_property(Rest, Props#{'Response-Topic' => Val}, StrictMode);
parse_property(<<16#09, Len:16/big, Val:Len/binary, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Correlation-Data' => Val}, StrictMode);
parse_property(<<16#0B, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_variable_byte_integer(Bin),
    parse_property(Rest, Props#{'Subscription-Identifier' => Val}, StrictMode);
parse_property(<<16#11, Val:32/big, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Session-Expiry-Interval' => Val}, StrictMode);
parse_property(<<16#12, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode, _Cause = invalid_assigned_client_id),
    parse_property(Rest, Props#{'Assigned-Client-Identifier' => Val}, StrictMode);
parse_property(<<16#13, Val:16, Bin/binary>>, Props, StrictMode) ->
    parse_property(Bin, Props#{'Server-Keep-Alive' => Val}, StrictMode);
parse_property(<<16#15, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode, _Cause = invalid_authn_method),
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
    {Val, Rest} = parse_utf8_string(Bin, StrictMode, _Cause = invalid_response_info),
    parse_property(Rest, Props#{'Response-Information' => Val}, StrictMode);
parse_property(<<16#1C, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode, _Cause = invalid_server_reference),
    parse_property(Rest, Props#{'Server-Reference' => Val}, StrictMode);
parse_property(<<16#1F, Bin/binary>>, Props, StrictMode) ->
    {Val, Rest} = parse_utf8_string(Bin, StrictMode, _Cause = invalid_reason_string),
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
    ?PARSE_ERR(#{invalid_property_code => Property}).
%% TODO: invalid property in specific packet.

parse_variable_byte_integer(<<1:1, D1:7, 1:1, D2:7, 1:1, D3:7, 0:1, D4:7, Rest/binary>>) ->
    {((D4 bsl 7 + D3) bsl 7 + D2) bsl 7 + D1, Rest};
parse_variable_byte_integer(<<1:1, D1:7, 1:1, D2:7, 0:1, D3:7, Rest/binary>>) ->
    {(D3 bsl 7 + D2) bsl 7 + D1, Rest};
parse_variable_byte_integer(<<1:1, D1:7, 0:1, D2:7, Rest/binary>>) ->
    {D2 bsl 7 + D1, Rest};
parse_variable_byte_integer(<<0:1, D1:7, Rest/binary>>) ->
    {D1, Rest};
parse_variable_byte_integer(_) ->
    ?PARSE_ERR(malformed_variable_byte_integer).

skip_variable_byte_integer(<<1:1, _:7, 1:1, _:7, 1:1, _:7, 0:1, _:7, Rest/binary>>) ->
    Rest;
skip_variable_byte_integer(<<1:1, _:7, 1:1, _:7, 0:1, _:7, Rest/binary>>) ->
    Rest;
skip_variable_byte_integer(<<1:1, _:7, 0:1, _:7, Rest/binary>>) ->
    Rest;
skip_variable_byte_integer(<<0:1, _:7, Rest/binary>>) ->
    Rest;
skip_variable_byte_integer(_) ->
    ?PARSE_ERR(malformed_variable_byte_integer).

parse_topic_filters(subscribe, Bin) ->
    [
        {Topic, #{rh => Rh, rap => Rap, nl => Nl, qos => QoS}}
     || <<Len:16/big, Topic:Len/binary, _:2, Rh:2, Rap:1, Nl:1, QoS:2>> <= Bin
    ];
parse_topic_filters(unsubscribe, Bin) ->
    [Topic || <<Len:16/big, Topic:Len/binary>> <= Bin].

parse_reason_codes(Bin) ->
    [Code || <<Code>> <= Bin].

parse_utf8_pair(
    <<Len1:16/big, Key:Len1/binary, Len2:16/big, Val:Len2/binary, Rest/binary>>,
    true
) ->
    {{validate_utf8(Key), validate_utf8(Val)}, Rest};
parse_utf8_pair(
    <<Len1:16/big, Key:Len1/binary, Len2:16/big, Val:Len2/binary, Rest/binary>>,
    false
) ->
    {{Key, Val}, Rest};
parse_utf8_pair(<<LenK:16/big, Rest/binary>>, _StrictMode) when
    LenK > byte_size(Rest)
->
    ?PARSE_ERR(#{
        cause => user_property_not_enough_bytes,
        parsed_key_length => LenK,
        remaining_bytes_length => byte_size(Rest)
    });
%% key maybe malformed
parse_utf8_pair(<<LenK:16/big, _Key:LenK/binary, LenV:16/big, Rest/binary>>, _StrictMode) when
    LenV > byte_size(Rest)
->
    ?PARSE_ERR(#{
        cause => malformed_user_property_value,
        parsed_key_length => LenK,
        parsed_value_length => LenV,
        remaining_bytes_length => byte_size(Rest)
    });
parse_utf8_pair(Bin, _StrictMode) when
    4 > byte_size(Bin)
->
    ?PARSE_ERR(#{
        cause => user_property_not_enough_bytes,
        total_bytes => byte_size(Bin)
    }).

parse_utf8_string(<<Len:16/big, Str:Len/binary, Rest/binary>>, true, _) ->
    {validate_utf8(Str), Rest};
parse_utf8_string(<<Len:16/big, Str:Len/binary, Rest/binary>>, false, _) ->
    {Str, Rest};
parse_utf8_string(<<Len:16/big, Rest/binary>>, _, Cause) when Len > byte_size(Rest) ->
    ?PARSE_ERR(#{
        cause => Cause,
        parsed_length => Len,
        remaining_bytes_length => byte_size(Rest)
    });
parse_utf8_string(Bin, _, _) when 2 > byte_size(Bin) ->
    ?PARSE_ERR(#{reason => malformed_utf8_string_length}).

parse_will_payload(<<Len:16/big, Data:Len/binary, Rest/binary>>) ->
    {Data, Rest};
parse_will_payload(<<Len:16/big, Rest/binary>>) when
    Len > byte_size(Rest)
->
    ?PARSE_ERR(#{
        cause => malformed_will_payload,
        parsed_length => Len,
        remaining_bytes => byte_size(Rest)
    });
parse_will_payload(Bin) when
    2 > byte_size(Bin)
->
    ?PARSE_ERR(#{
        cause => malformed_will_payload,
        length_bytes => size(Bin),
        expected_bytes => 2
    }).

%% Validate header if sctrict mode. See: mqtt-v5.0: 2.1.3 Flags
validate_header(?CONNECT, 0, 0, 0) -> ok;
validate_header(?CONNACK, 0, 0, 0) -> ok;
validate_header(?PUBLISH, 0, ?QOS_0, _) -> ok;
validate_header(?PUBLISH, _, ?QOS_1, _) -> ok;
validate_header(?PUBLISH, 0, ?QOS_2, _) -> ok;
validate_header(?PUBACK, 0, 0, 0) -> ok;
validate_header(?PUBREC, 0, 0, 0) -> ok;
validate_header(?PUBREL, 0, 1, 0) -> ok;
validate_header(?PUBCOMP, 0, 0, 0) -> ok;
validate_header(?SUBSCRIBE, 0, 1, 0) -> ok;
validate_header(?SUBACK, 0, 0, 0) -> ok;
validate_header(?UNSUBSCRIBE, 0, 1, 0) -> ok;
validate_header(?UNSUBACK, 0, 0, 0) -> ok;
validate_header(?PINGREQ, 0, 0, 0) -> ok;
validate_header(?PINGRESP, 0, 0, 0) -> ok;
validate_header(?DISCONNECT, 0, 0, 0) -> ok;
validate_header(?AUTH, 0, 0, 0) -> ok;
validate_header(_Type, _Dup, _QoS, _Rt) -> ?PARSE_ERR(bad_frame_header).

-compile({inline, [validate_packet_id/1]}).
validate_packet_id(0) -> ?PARSE_ERR(bad_packet_id);
validate_packet_id(_) -> ok.

validate_subqos([3 | _]) -> ?PARSE_ERR(bad_subqos);
validate_subqos([_ | T]) -> validate_subqos(T);
validate_subqos([]) -> ok.

%% from spec: the server MAY send disconnect with reason code 0x84
%% we chose to close socket because the client is likely not talking MQTT anyway
validate_proto_name(<<"MQTT">>) ->
    ok;
validate_proto_name(<<"MQIsdp">>) ->
    ok;
validate_proto_name(ProtoName) ->
    ?PARSE_ERR(#{
        cause => invalid_proto_name,
        expected => <<"'MQTT' or 'MQIsdp'">>,
        received => ProtoName
    }).

%% MQTT-v3.1.1-[MQTT-3.1.2-3], MQTT-v5.0-[MQTT-3.1.2-3]
-compile({inline, [validate_connect_reserved/2]}).
validate_connect_reserved(0, _) -> ok;
validate_connect_reserved(1, Ctx) -> ?PARSE_ERR(reserved_connect_flag, Ctx).

-compile({inline, [validate_connect_will/4]}).
%% MQTT-v3.1.1-[MQTT-3.1.2-13], MQTT-v5.0-[MQTT-3.1.2-11]
validate_connect_will(false, _, WillQoS, Ctx) when WillQoS > 0 ->
    ?PARSE_ERR(invalid_will_qos, Ctx);
%% MQTT-v3.1.1-[MQTT-3.1.2-14], MQTT-v5.0-[MQTT-3.1.2-12]
validate_connect_will(true, _, WillQoS, Ctx) when WillQoS > 2 ->
    ?PARSE_ERR(invalid_will_qos, Ctx);
%% MQTT-v3.1.1-[MQTT-3.1.2-15], MQTT-v5.0-[MQTT-3.1.2-13]
validate_connect_will(false, WillRetain, _, Ctx) when WillRetain ->
    ?PARSE_ERR(invalid_will_retain, Ctx);
validate_connect_will(_, _, _, _) ->
    ok.

-compile({inline, [validate_connect_password_flag/5]}).
%% MQTT-v3.1
%% Username flag and password flag are not strongly related
%% https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#connect
validate_connect_password_flag(true, ?MQTT_PROTO_V3, _, _, _) ->
    ok;
%% MQTT-v3.1.1-[MQTT-3.1.2-22]
validate_connect_password_flag(true, ?MQTT_PROTO_V4, UsernameFlag, PasswordFlag, Ctx) ->
    %% BUG-FOR-BUG compatible, only check when `strict-mode`
    UsernameFlag orelse PasswordFlag andalso ?PARSE_ERR(invalid_password_flag, Ctx);
validate_connect_password_flag(true, ?MQTT_PROTO_V5, _, _, _) ->
    ok;
validate_connect_password_flag(_, _, _, _, _) ->
    ok.

-compile({inline, [bool/1, fixqos/2]}).
bool(0) -> false;
bool(1) -> true.

fixqos(?PUBREL, 0) -> 1;
fixqos(?SUBSCRIBE, 0) -> 1;
fixqos(?UNSUBSCRIBE, 0) -> 1;
fixqos(_Type, QoS) -> QoS.

validate_utf8(Bin) ->
    case validate_mqtt_utf8_char(Bin) of
        true -> Bin;
        false -> ?PARSE_ERR(utf8_string_invalid)
    end.

%% Is the utf8 string respecting UTF-8 characters defined by MQTT Spec?
%% i.e. does the string contains control characters?
validate_mqtt_utf8_char(<<>>) ->
    true;
validate_mqtt_utf8_char(<<H/utf8, _Rest/binary>>) when
    H >= 16#00, H =< 16#1F;
    H >= 16#7F, H =< 16#9F
->
    false;
validate_mqtt_utf8_char(<<_H/utf8, Rest/binary>>) ->
    validate_mqtt_utf8_char(Rest);
validate_mqtt_utf8_char(<<_Broken, _Rest/binary>>) ->
    false.
