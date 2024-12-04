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

-module(emqx_frame).

-include("emqx_mqtt.hrl").

-export([
    initial_parse_state/0,
    initial_parse_state/1,
    update_parse_state/2
]).

-export([
    parse/1,
    parse/2,
    parse_complete/2,
    serialize_fun/0,
    serialize_fun/1,
    initial_serialize_opts/1,
    serialize_opts/1,
    serialize_opts/2,
    serialize_pkt/2,
    serialize/1,
    serialize/2,
    serialize/3
]).

-export([describe_state/1]).

-export_type([
    options/0,
    parse_state/0,
    parse_state_initial/0,
    parse_result/0,
    serialize_opts/0
]).

-type options() :: #{
    strict_mode => boolean(),
    max_size => 1..?MAX_PACKET_SIZE,
    version => emqx_types:proto_ver()
}.

-record(options, {
    strict_mode :: boolean(),
    max_size :: 1..?MAX_PACKET_SIZE,
    version :: emqx_types:proto_ver()
}).

-record(remlen, {hdr, len, mult, opts :: #options{}}).
-record(body, {hdr, need, acc :: iodata(), opts :: #options{}}).

-type parse_state() :: #remlen{} | #body{} | parse_state_initial().
-type parse_state_initial() :: #options{}.

-type parse_result() ::
    {more, parse_state()}
    | {emqx_types:packet(), binary(), parse_state_initial()}.

-type serialize_opts() :: options().

-define(DEFAULT_OPTIONS, #{
    strict_mode => false,
    max_size => ?MAX_PACKET_SIZE,
    version => ?MQTT_PROTO_V4
}).

-define(PARSE_ERR(Reason), ?THROW_FRAME_ERROR(Reason)).
-define(SERIALIZE_ERR(Reason), ?THROW_SERIALIZE_ERROR(Reason)).

-define(MULTIPLIER_MAX, 16#200000).

-dialyzer({no_match, [serialize_utf8_string/3]}).

%% @doc Describe state for logging.
describe_state(Options = #options{}) ->
    #{
        state => clean,
        proto_ver => Options#options.version
    };
describe_state(#remlen{opts = Options}) ->
    #{
        state => parsing_varint_length,
        proto_ver => Options#options.version
    };
describe_state(#body{hdr = Hdr, need = Need, acc = Acc, opts = Options}) ->
    #{
        state => parsing_body,
        proto_ver => Options#options.version,
        parsed_header => Hdr,
        expected_bytes_remain => Need,
        received_bytes => iolist_size(Acc)
    }.

%%--------------------------------------------------------------------
%% Init Parse State
%%--------------------------------------------------------------------

-spec initial_parse_state() -> parse_state_initial().
initial_parse_state() ->
    initial_parse_state(#{}).

-spec initial_parse_state(options()) -> parse_state_initial().
initial_parse_state(Options) when is_map(Options) ->
    Effective = maps:merge(?DEFAULT_OPTIONS, Options),
    #options{
        strict_mode = maps:get(strict_mode, Effective),
        max_size = maps:get(max_size, Effective),
        version = maps:get(version, Effective)
    }.

%%--------------------------------------------------------------------
%% Parse MQTT Frame
%%--------------------------------------------------------------------

-spec parse(iodata()) -> parse_result().
parse(Bin) ->
    parse(Bin, initial_parse_state()).

-spec parse(iodata(), parse_state()) -> parse_result().
parse(
    <<Type:4, Dup:1, QoS:2, Retain:1, Rest/binary>>,
    Options = #options{strict_mode = StrictMode}
) ->
    %% Validate header if strict mode.
    StrictMode andalso validate_header(Type, Dup, QoS, Retain),
    Header = #mqtt_packet_header{
        type = Type,
        dup = bool(Dup),
        qos = fixqos(Type, QoS),
        retain = bool(Retain)
    },
    parse_remaining_len(Rest, Header, 1, 0, Options);
parse(
    Bin,
    #remlen{hdr = Header, len = Length, mult = Mult, opts = Options}
) ->
    parse_remaining_len(Bin, Header, Mult, Length, Options);
parse(
    Bin,
    #body{hdr = Header, need = Need, acc = Body, opts = Options}
) ->
    parse_body_frame(Bin, Header, Need, Body, Options);
parse(<<>>, State) ->
    {more, State}.

%% @doc Parses _complete_ binary frame into a single `#mqtt_packet{}`.
-spec parse_complete(iodata(), parse_state_initial()) ->
    emqx_types:packet() | [emqx_types:packet() | parse_state_initial()].
parse_complete(
    <<Type:4, Dup:1, QoS:2, Retain:1, Rest1/binary>>,
    Options = #options{strict_mode = StrictMode}
) ->
    %% Validate header if strict mode.
    StrictMode andalso validate_header(Type, Dup, QoS, Retain),
    Header = #mqtt_packet_header{
        type = Type,
        dup = bool(Dup),
        qos = fixqos(Type, QoS),
        retain = bool(Retain)
    },
    case Rest1 of
        <<0:8>> ->
            parse_bodyless_packet(Header);
        _ ->
            {_RemLen, Rest2} = parse_variable_byte_integer(Rest1),
            parse_packet_complete(Rest2, Header, Options)
    end.

parse_remaining_len(<<>>, Header, Mult, Length, Options) ->
    {more, #remlen{hdr = Header, len = Length, mult = Mult, opts = Options}};
parse_remaining_len(<<0:8, Rest/binary>>, Header, 1, 0, Options) ->
    Packet = parse_bodyless_packet(Header),
    {Packet, Rest, Options};
%% Match PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK...
parse_remaining_len(<<0:1, 2:7, Rest/binary>>, Header, 1, 0, Options) ->
    parse_body_frame(Rest, Header, 2, <<>>, Options);
parse_remaining_len(<<1:1, _Len:7, _Rest/binary>>, _Header, Multiplier, _Value, _Options) when
    Multiplier > ?MULTIPLIER_MAX
->
    ?PARSE_ERR(malformed_variable_byte_integer);
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Header, Multiplier, Value, Options) ->
    parse_remaining_len(Rest, Header, Multiplier * ?HIGHBIT, Value + Len * Multiplier, Options);
parse_remaining_len(
    <<0:1, Len:7, Rest/binary>>,
    Header,
    Multiplier,
    Value,
    Options = #options{max_size = MaxSize}
) ->
    FrameLen = Value + Len * Multiplier,
    case FrameLen > MaxSize of
        true -> ?PARSE_ERR(#{cause => frame_too_large, limit => MaxSize, received => FrameLen});
        false -> parse_body_frame(Rest, Header, FrameLen, <<>>, Options)
    end.

-compile({inline, [parse_bodyless_packet/1]}).

%% Match DISCONNECT without payload
parse_bodyless_packet(Header = #mqtt_packet_header{type = ?DISCONNECT}) ->
    packet(Header, #mqtt_packet_disconnect{reason_code = ?RC_SUCCESS});
%% Match PINGREQ.
parse_bodyless_packet(Header = #mqtt_packet_header{type = ?PINGREQ}) ->
    packet(Header);
parse_bodyless_packet(#mqtt_packet_header{type = ?PINGRESP}) ->
    ?PARSE_ERR(#{cause => unexpected_packet, header_type => 'PINGRESP'});
%% All other types of messages should not have a zero remaining length.
parse_bodyless_packet(#mqtt_packet_header{type = Type}) ->
    ?PARSE_ERR(#{cause => zero_remaining_len, header_type => Type}).

-compile({inline, [append_body/2]}).
-dialyzer({no_improper_lists, [append_body/2]}).
-spec append_body(iodata(), binary()) -> iodata().
append_body(Acc, <<>>) ->
    Acc;
append_body(Acc, Bytes) when is_binary(Acc) andalso byte_size(Acc) < 1024 ->
    <<Acc/binary, Bytes/binary>>;
append_body(Acc, Bytes) ->
    [Acc | Bytes].

parse_body_frame(Bin, Header, Need, Body, Options) ->
    case Need - byte_size(Bin) of
        More when More > 0 ->
            NewBody = append_body(Body, Bin),
            {more, #body{hdr = Header, need = More, acc = NewBody, opts = Options}};
        _ ->
            <<LastPart:Need/bytes, Rest/bytes>> = Bin,
            Frame = iolist_to_binary(append_body(Body, LastPart)),
            parse_packet(Frame, Header, Options, Rest)
    end.

-compile({inline, [packet/1, packet/2, packet/3]}).
packet(Header) ->
    #mqtt_packet{header = Header}.
packet(Header, Variable) ->
    #mqtt_packet{header = Header, variable = Variable}.
packet(Header, Variable, Payload) ->
    #mqtt_packet{header = Header, variable = Variable, payload = Payload}.

-compile({inline, [parse_packet_complete/3]}).
parse_packet_complete(Frame, Header = #mqtt_packet_header{type = ?CONNECT}, Options) ->
    Variable = parse_connect(Frame, Options),
    Packet = packet(Header, Variable),
    NOptions = update_parse_state(Variable#mqtt_packet_connect.proto_ver, Options),
    [Packet, NOptions];
parse_packet_complete(Frame, Header, Options) ->
    parse_packet(Frame, Header, Options).

-compile({inline, [parse_packet/4]}).
parse_packet(Frame, Header = #mqtt_packet_header{type = ?CONNECT}, Options, Rest) ->
    Variable = parse_connect(Frame, Options),
    Packet = packet(Header, Variable),
    {Packet, Rest, update_parse_state(Variable#mqtt_packet_connect.proto_ver, Options)};
parse_packet(Frame, Header, Options, Rest) ->
    Packet = parse_packet(Frame, Header, Options),
    {Packet, Rest, Options}.

parse_connect(Frame, Options = #options{strict_mode = StrictMode}) ->
    {ProtoName, Rest0} = parse_utf8_string(Frame, StrictMode, invalid_proto_name),
    %% No need to parse and check proto_ver if proto_name is invalid, check it first
    %% And the matching check of `proto_name` and `proto_ver` fields will be done in `emqx_packet:check_proto_ver/2`
    _ = validate_proto_name(ProtoName),
    {IsBridge, ProtoVer, Rest2} = parse_connect_proto_ver(Rest0),
    try
        do_parse_connect(ProtoName, IsBridge, ProtoVer, Rest2, StrictMode)
    catch
        throw:{?FRAME_PARSE_ERROR, ReasonM} when is_map(ReasonM) ->
            ?PARSE_ERR(
                ReasonM#{
                    proto_ver => ProtoVer,
                    proto_name => ProtoName,
                    parse_state => update_parse_state(ProtoVer, Options)
                }
            );
        throw:{?FRAME_PARSE_ERROR, Reason} ->
            ?PARSE_ERR(
                #{
                    cause => Reason,
                    proto_ver => ProtoVer,
                    proto_name => ProtoName,
                    parse_state => update_parse_state(ProtoVer, Options)
                }
            )
    end.

-spec update_parse_state(emqx_types:proto_ver(), parse_state_initial()) ->
    parse_state_initial().
update_parse_state(ProtoVer, Options) ->
    Options#options{version = ProtoVer}.

do_parse_connect(
    ProtoName,
    IsBridge,
    ProtoVer,
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
    StrictMode
) ->
    _ = validate_connect_reserved(Reserved),
    _ = validate_connect_will(
        WillFlag = bool(WillFlagB),
        WillRetain = bool(WillRetainB),
        WillQoS
    ),
    _ = validate_connect_password_flag(
        StrictMode,
        ProtoVer,
        UsernameFlag = bool(UsernameFlagB),
        PasswordFlag = bool(PasswordFlagB)
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
    {Username, Rest6} = parse_optional(
        Rest5,
        fun(Bin) ->
            parse_utf8_string(Bin, StrictMode, invalid_username)
        end,
        UsernameFlag
    ),
    {Password, Rest7} = parse_optional(
        Rest6,
        fun(Bin) ->
            parse_utf8_string(Bin, StrictMode, invalid_password)
        end,
        PasswordFlag
    ),
    case Rest7 of
        <<>> ->
            ConnPacket1#mqtt_packet_connect{username = Username, password = Password};
        _ ->
            ?PARSE_ERR(#{
                cause => malformed_connect,
                unexpected_trailing_bytes => size(Rest7)
            })
    end;
do_parse_connect(_ProtoName, _IsBridge, _ProtoVer, Bin, _StrictMode) ->
    %% sent less than 24 bytes
    ?PARSE_ERR(#{cause => malformed_connect, header_bytes => Bin}).

parse_packet(
    <<AckFlags:8, ReasonCode:8, Rest/binary>>,
    Header = #mqtt_packet_header{type = ?CONNACK},
    #options{version = Ver, strict_mode = StrictMode}
) ->
    %% Not possible for broker to receive!
    case parse_properties(Rest, Ver, StrictMode) of
        {Properties, <<>>} ->
            packet(Header, #mqtt_packet_connack{
                ack_flags = AckFlags,
                reason_code = ReasonCode,
                properties = Properties
            });
        _ ->
            ?PARSE_ERR(malformed_properties)
    end;
parse_packet(
    Bin,
    Header = #mqtt_packet_header{type = ?PUBLISH, qos = QoS},
    #options{strict_mode = StrictMode, version = Ver}
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
    packet(
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
    Header = #mqtt_packet_header{type = PubAck},
    #options{strict_mode = StrictMode}
) when ?PUBACK =< PubAck, PubAck =< ?PUBCOMP ->
    StrictMode andalso validate_packet_id(PacketId),
    packet(Header, #mqtt_packet_puback{packet_id = PacketId, reason_code = 0});
parse_packet(
    <<PacketId:16/big, ReasonCode, Rest/binary>>,
    Header = #mqtt_packet_header{type = PubAck},
    #options{strict_mode = StrictMode, version = Ver = ?MQTT_PROTO_V5}
) when ?PUBACK =< PubAck, PubAck =< ?PUBCOMP ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, <<>>} = parse_properties(Rest, Ver, StrictMode),
    packet(Header, #mqtt_packet_puback{
        packet_id = PacketId,
        reason_code = ReasonCode,
        properties = Properties
    });
parse_packet(
    <<PacketId:16/big, Rest/binary>>,
    Header = #mqtt_packet_header{type = ?SUBSCRIBE},
    #options{strict_mode = StrictMode, version = Ver}
) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    TopicFilters = parse_topic_filters(subscribe, Rest1),
    ok = validate_subqos([QoS || {_, #{qos := QoS}} <- TopicFilters]),
    packet(Header, #mqtt_packet_subscribe{
        packet_id = PacketId,
        properties = Properties,
        topic_filters = TopicFilters
    });
parse_packet(
    <<PacketId:16/big, Rest/binary>>,
    Header = #mqtt_packet_header{type = ?SUBACK},
    #options{strict_mode = StrictMode, version = Ver}
) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    ReasonCodes = parse_reason_codes(Rest1),
    packet(Header, #mqtt_packet_suback{
        packet_id = PacketId,
        properties = Properties,
        reason_codes = ReasonCodes
    });
parse_packet(
    <<PacketId:16/big, Rest/binary>>,
    Header = #mqtt_packet_header{type = ?UNSUBSCRIBE},
    #options{strict_mode = StrictMode, version = Ver}
) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    TopicFilters = parse_topic_filters(unsubscribe, Rest1),
    packet(Header, #mqtt_packet_unsubscribe{
        packet_id = PacketId,
        properties = Properties,
        topic_filters = TopicFilters
    });
parse_packet(
    <<PacketId:16/big>>,
    Header = #mqtt_packet_header{type = ?UNSUBACK},
    #options{strict_mode = StrictMode}
) ->
    StrictMode andalso validate_packet_id(PacketId),
    packet(Header, #mqtt_packet_unsuback{packet_id = PacketId});
parse_packet(
    <<PacketId:16/big, Rest/binary>>,
    Header = #mqtt_packet_header{type = ?UNSUBACK},
    #options{strict_mode = StrictMode, version = Ver}
) ->
    StrictMode andalso validate_packet_id(PacketId),
    {Properties, Rest1} = parse_properties(Rest, Ver, StrictMode),
    ReasonCodes = parse_reason_codes(Rest1),
    packet(Header, #mqtt_packet_unsuback{
        packet_id = PacketId,
        properties = Properties,
        reason_codes = ReasonCodes
    });
parse_packet(
    <<ReasonCode, Rest/binary>>,
    Header = #mqtt_packet_header{type = ?DISCONNECT},
    #options{strict_mode = StrictMode, version = ?MQTT_PROTO_V5}
) ->
    {Properties, <<>>} = parse_properties(Rest, ?MQTT_PROTO_V5, StrictMode),
    packet(Header, #mqtt_packet_disconnect{
        reason_code = ReasonCode,
        properties = Properties
    });
parse_packet(
    <<ReasonCode, Rest/binary>>,
    Header = #mqtt_packet_header{type = ?AUTH},
    #options{strict_mode = StrictMode, version = ?MQTT_PROTO_V5}
) ->
    {Properties, <<>>} = parse_properties(Rest, ?MQTT_PROTO_V5, StrictMode),
    packet(Header, #mqtt_packet_auth{
        reason_code = ReasonCode,
        properties = Properties
    });
parse_packet(_Frame, Header, _Options) ->
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
    ?PARSE_ERR(#{cause => invalid_property_code, property_code => Property}).
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

parse_optional(Bin, F, true) ->
    F(Bin);
parse_optional(Bin, _F, false) ->
    {undefined, Bin}.

parse_utf8_string(<<Len:16/big, Str:Len/binary, Rest/binary>>, true, _Cause) ->
    {validate_utf8(Str), Rest};
parse_utf8_string(<<Len:16/big, Str:Len/binary, Rest/binary>>, false, _Cause) ->
    {Str, Rest};
parse_utf8_string(<<Len:16/big, Rest/binary>>, _, Cause) when Len > byte_size(Rest) ->
    ?PARSE_ERR(#{
        cause => Cause,
        reason => malformed_utf8_string,
        parsed_length => Len,
        remaining_bytes_length => byte_size(Rest)
    });
parse_utf8_string(Bin, _, Cause) when 2 > byte_size(Bin) ->
    ?PARSE_ERR(#{
        cause => Cause,
        reason => malformed_utf8_string_length
    }).

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

%%--------------------------------------------------------------------
%% Serialize MQTT Packet
%%--------------------------------------------------------------------

serialize_fun() -> serialize_fun(?DEFAULT_OPTIONS).

serialize_fun(#mqtt_packet_connect{proto_ver = ProtoVer, properties = ConnProps}) ->
    MaxSize = get_property('Maximum-Packet-Size', ConnProps, ?MAX_PACKET_SIZE),
    serialize_fun(#{version => ProtoVer, max_size => MaxSize, strict_mode => false});
serialize_fun(#{version := Ver, max_size := MaxSize, strict_mode := StrictMode}) ->
    fun(Packet) ->
        IoData = serialize(Packet, Ver, StrictMode),
        case is_too_large(IoData, MaxSize) of
            true -> <<>>;
            false -> IoData
        end
    end.

initial_serialize_opts(Opts) ->
    maps:merge(?DEFAULT_OPTIONS, Opts).

serialize_opts(ProtoVer, MaxSize) ->
    #{version => ProtoVer, max_size => MaxSize, strict_mode => false}.

serialize_opts(#mqtt_packet_connect{proto_ver = ProtoVer, properties = ConnProps}) ->
    MaxSize = get_property('Maximum-Packet-Size', ConnProps, ?MAX_PACKET_SIZE),
    #{version => ProtoVer, max_size => MaxSize, strict_mode => false}.

serialize_pkt(Packet, #{version := Ver, max_size := MaxSize, strict_mode := StrictMode}) ->
    IoData = serialize(Packet, Ver, StrictMode),
    case is_too_large(IoData, MaxSize) of
        true -> <<>>;
        false -> IoData
    end.

-spec serialize(emqx_types:packet()) -> iodata().
serialize(Packet) -> serialize(Packet, ?MQTT_PROTO_V4, false).

serialize(Packet, Ver) -> serialize(Packet, Ver, false).

-spec serialize(emqx_types:packet(), emqx_types:proto_ver(), boolean()) -> iodata().
serialize(
    #mqtt_packet{
        header = Header,
        variable = Variable,
        payload = Payload
    },
    Ver,
    StrictMode
) ->
    serialize(
        Header,
        serialize_variable(Variable, Ver, StrictMode),
        serialize_payload(Payload),
        StrictMode
    ).

serialize(
    #mqtt_packet_header{
        type = Type,
        dup = Dup,
        qos = QoS,
        retain = Retain
    },
    VariableBin,
    PayloadBin,
    _StrictMode
) when
    ?CONNECT =< Type andalso Type =< ?AUTH
->
    Len = iolist_size(VariableBin) + iolist_size(PayloadBin),
    [
        <<Type:4, (flag(Dup)):1, (flag(QoS)):2, (flag(Retain)):1>>,
        serialize_remaining_len(Len),
        VariableBin,
        PayloadBin
    ].

serialize_variable(
    #mqtt_packet_connect{
        proto_name = ProtoName,
        proto_ver = ProtoVer,
        %% For bridge mode, non-standard implementation
        %% Invented by mosquitto, named 'try_private': https://mosquitto.org/man/mosquitto-conf-5.html
        is_bridge = IsBridge,
        clean_start = CleanStart,
        will_flag = WillFlag,
        will_qos = WillQoS,
        will_retain = WillRetain,
        keepalive = KeepAlive,
        properties = Properties,
        clientid = ClientId,
        will_props = WillProps,
        will_topic = WillTopic,
        will_payload = WillPayload,
        username = Username,
        password = Password
    },
    _Ver,
    StrictMode
) ->
    [
        serialize_binary_data(ProtoName),
        <<
            (case IsBridge of
                true -> 16#80 + ProtoVer;
                false -> ProtoVer
            end):8,
            (flag(Username)):1,
            (flag(Password)):1,
            (flag(WillRetain)):1,
            WillQoS:2,
            (flag(WillFlag)):1,
            (flag(CleanStart)):1,
            0:1,
            KeepAlive:16/big-unsigned-integer
        >>,
        serialize_properties(Properties, ProtoVer, StrictMode),
        serialize_utf8_string(ClientId, StrictMode),
        case WillFlag of
            true ->
                [
                    serialize_properties(WillProps, ProtoVer, StrictMode),
                    serialize_utf8_string(WillTopic, StrictMode),
                    serialize_binary_data(WillPayload)
                ];
            false ->
                <<>>
        end,
        serialize_utf8_string(Username, true, StrictMode),
        serialize_utf8_string(Password, true, StrictMode)
    ];
serialize_variable(
    #mqtt_packet_connack{
        ack_flags = AckFlags,
        reason_code = ReasonCode,
        properties = Properties
    },
    Ver,
    StrictMode
) ->
    [AckFlags, ReasonCode, serialize_properties(Properties, Ver, StrictMode)];
serialize_variable(
    #mqtt_packet_publish{
        topic_name = TopicName,
        packet_id = PacketId,
        properties = Properties
    },
    Ver,
    StrictMode
) ->
    [
        serialize_utf8_string(TopicName, StrictMode),
        case PacketId of
            undefined -> <<>>;
            _ -> <<PacketId:16/big-unsigned-integer>>
        end,
        serialize_properties(Properties, Ver, StrictMode)
    ];
serialize_variable(#mqtt_packet_puback{packet_id = PacketId}, Ver, _StrictMode) when
    Ver == ?MQTT_PROTO_V3; Ver == ?MQTT_PROTO_V4
->
    <<PacketId:16/big-unsigned-integer>>;
serialize_variable(
    #mqtt_packet_puback{
        packet_id = PacketId,
        reason_code = ReasonCode,
        properties = Properties
    },
    Ver = ?MQTT_PROTO_V5,
    StrictMode
) ->
    [
        <<PacketId:16/big-unsigned-integer>>,
        ReasonCode,
        serialize_properties(Properties, Ver, StrictMode)
    ];
serialize_variable(
    #mqtt_packet_subscribe{
        packet_id = PacketId,
        properties = Properties,
        topic_filters = TopicFilters
    },
    Ver,
    StrictMode
) ->
    [
        <<PacketId:16/big-unsigned-integer>>,
        serialize_properties(Properties, Ver, StrictMode),
        serialize_topic_filters(subscribe, TopicFilters, Ver, StrictMode)
    ];
serialize_variable(
    #mqtt_packet_suback{
        packet_id = PacketId,
        properties = Properties,
        reason_codes = ReasonCodes
    },
    Ver,
    StrictMode
) ->
    [
        <<PacketId:16/big-unsigned-integer>>,
        serialize_properties(Properties, Ver, StrictMode),
        serialize_reason_codes(ReasonCodes)
    ];
serialize_variable(
    #mqtt_packet_unsubscribe{
        packet_id = PacketId,
        properties = Properties,
        topic_filters = TopicFilters
    },
    Ver,
    StrictMode
) ->
    [
        <<PacketId:16/big-unsigned-integer>>,
        serialize_properties(Properties, Ver, StrictMode),
        serialize_topic_filters(unsubscribe, TopicFilters, Ver, StrictMode)
    ];
serialize_variable(
    #mqtt_packet_unsuback{
        packet_id = PacketId,
        properties = Properties,
        reason_codes = ReasonCodes
    },
    Ver,
    StrictMode
) ->
    [
        <<PacketId:16/big-unsigned-integer>>,
        serialize_properties(Properties, Ver, StrictMode),
        serialize_reason_codes(ReasonCodes)
    ];
serialize_variable(#mqtt_packet_disconnect{}, Ver, _StrictMode) when
    Ver == ?MQTT_PROTO_V3; Ver == ?MQTT_PROTO_V4
->
    <<>>;
serialize_variable(
    #mqtt_packet_disconnect{
        reason_code = ReasonCode,
        properties = Properties
    },
    Ver = ?MQTT_PROTO_V5,
    StrictMode
) ->
    [ReasonCode, serialize_properties(Properties, Ver, StrictMode)];
serialize_variable(#mqtt_packet_disconnect{}, _Ver, _StrictMode) ->
    <<>>;
serialize_variable(
    #mqtt_packet_auth{
        reason_code = ReasonCode,
        properties = Properties
    },
    Ver = ?MQTT_PROTO_V5,
    StrictMode
) ->
    [ReasonCode, serialize_properties(Properties, Ver, StrictMode)];
serialize_variable(PacketId, ?MQTT_PROTO_V3, _StrictMode) when is_integer(PacketId) ->
    <<PacketId:16/big-unsigned-integer>>;
serialize_variable(PacketId, ?MQTT_PROTO_V4, _StrictMode) when is_integer(PacketId) ->
    <<PacketId:16/big-unsigned-integer>>;
serialize_variable(undefined, _Ver, _StrictMode) ->
    <<>>.

serialize_payload(undefined) -> <<>>;
serialize_payload(Bin) -> Bin.

serialize_properties(_Props, Ver, _StrictMode) when Ver =/= ?MQTT_PROTO_V5 ->
    <<>>;
serialize_properties(Props, ?MQTT_PROTO_V5, StrictMode) ->
    serialize_properties(Props, StrictMode).

serialize_properties(undefined, _StrictMode) ->
    <<0>>;
serialize_properties(Props, _StrictMode) when map_size(Props) == 0 ->
    <<0>>;
serialize_properties(Props, StrictMode) when is_map(Props) ->
    Bin = <<
        <<(serialize_property(Prop, Val, StrictMode))/binary>>
     || {Prop, Val} <- maps:to_list(Props)
    >>,
    [serialize_variable_byte_integer(byte_size(Bin)), Bin].

serialize_property(_, Disabled, _StrictMode) when
    Disabled =:= disabled;
    Disabled =:= undefined
->
    <<>>;
serialize_property(?MQTT_INTERNAL_EXTRA, _, _StrictMode) ->
    <<>>;
serialize_property('Payload-Format-Indicator', Val, _StrictMode) ->
    <<16#01, Val>>;
serialize_property('Message-Expiry-Interval', Val, _StrictMode) ->
    <<16#02, Val:32/big>>;
serialize_property('Content-Type', Val, StrictMode) ->
    <<16#03, (serialize_utf8_string(Val, StrictMode))/binary>>;
serialize_property('Response-Topic', Val, StrictMode) ->
    <<16#08, (serialize_utf8_string(Val, StrictMode))/binary>>;
serialize_property('Correlation-Data', Val, _StrictMode) ->
    <<16#09, (byte_size(Val)):16, Val/binary>>;
serialize_property('Subscription-Identifier', Val, _StrictMode) ->
    <<16#0B, (serialize_variable_byte_integer(Val))/binary>>;
serialize_property('Session-Expiry-Interval', Val, _StrictMode) ->
    <<16#11, Val:32/big>>;
serialize_property('Assigned-Client-Identifier', Val, StrictMode) ->
    <<16#12, (serialize_utf8_string(Val, StrictMode))/binary>>;
serialize_property('Server-Keep-Alive', Val, _StrictMode) ->
    <<16#13, Val:16/big>>;
serialize_property('Authentication-Method', Val, StrictMode) ->
    <<16#15, (serialize_utf8_string(Val, StrictMode))/binary>>;
serialize_property('Authentication-Data', Val, _StrictMode) ->
    <<16#16, (iolist_size(Val)):16, Val/binary>>;
serialize_property('Request-Problem-Information', Val, _StrictMode) ->
    <<16#17, Val>>;
serialize_property('Will-Delay-Interval', Val, _StrictMode) ->
    <<16#18, Val:32/big>>;
serialize_property('Request-Response-Information', Val, _StrictMode) ->
    <<16#19, Val>>;
serialize_property('Response-Information', Val, StrictMode) ->
    <<16#1A, (serialize_utf8_string(Val, StrictMode))/binary>>;
serialize_property('Server-Reference', Val, StrictMode) ->
    <<16#1C, (serialize_utf8_string(Val, StrictMode))/binary>>;
serialize_property('Reason-String', Val, StrictMode) ->
    <<16#1F, (serialize_utf8_string(Val, StrictMode))/binary>>;
serialize_property('Receive-Maximum', Val, _StrictMode) ->
    <<16#21, Val:16/big>>;
serialize_property('Topic-Alias-Maximum', Val, _StrictMode) ->
    <<16#22, Val:16/big>>;
serialize_property('Topic-Alias', Val, _StrictMode) ->
    <<16#23, Val:16/big>>;
serialize_property('Maximum-QoS', Val, _StrictMode) ->
    <<16#24, Val>>;
serialize_property('Retain-Available', Val, _StrictMode) ->
    <<16#25, Val>>;
serialize_property('User-Property', {Key, Val}, StrictMode) ->
    <<16#26, (serialize_utf8_pair(Key, Val, StrictMode))/binary>>;
serialize_property('User-Property', Props, StrictMode) when is_list(Props) ->
    <<
        <<(serialize_property('User-Property', {Key, Val}, StrictMode))/binary>>
     || {Key, Val} <- Props
    >>;
serialize_property('Maximum-Packet-Size', Val, _StrictMode) ->
    <<16#27, Val:32/big>>;
serialize_property('Wildcard-Subscription-Available', Val, _StrictMode) ->
    <<16#28, Val>>;
serialize_property('Subscription-Identifier-Available', Val, _StrictMode) ->
    <<16#29, Val>>;
serialize_property('Shared-Subscription-Available', Val, _StrictMode) ->
    <<16#2A, Val>>.

serialize_topic_filters(subscribe, TopicFilters, ?MQTT_PROTO_V5, StrictMode) ->
    <<
        <<
            (serialize_utf8_string(Topic, StrictMode))/binary,
            ?RESERVED:2,
            Rh:2,
            (flag(Rap)):1,
            (flag(Nl)):1,
            QoS:2
        >>
     || {Topic, #{rh := Rh, rap := Rap, nl := Nl, qos := QoS}} <- TopicFilters
    >>;
serialize_topic_filters(subscribe, TopicFilters, _Ver, StrictMode) ->
    <<
        <<(serialize_utf8_string(Topic, StrictMode))/binary, ?RESERVED:6, QoS:2>>
     || {Topic, #{qos := QoS}} <- TopicFilters
    >>;
serialize_topic_filters(unsubscribe, TopicFilters, _Ver, StrictMode) ->
    <<<<(serialize_utf8_string(Topic, StrictMode))/binary>> || Topic <- TopicFilters>>.

serialize_reason_codes(undefined) ->
    <<>>;
serialize_reason_codes(ReasonCodes) when is_list(ReasonCodes) ->
    <<<<Code>> || Code <- ReasonCodes>>.

serialize_utf8_pair(Name, Value, StrictMode) ->
    <<
        (serialize_utf8_string(Name, StrictMode))/binary,
        (serialize_utf8_string(Value, StrictMode))/binary
    >>.

serialize_binary_data(Bin) ->
    [<<(byte_size(Bin)):16/big-unsigned-integer>>, Bin].

serialize_utf8_string(undefined, false, _StrictMode) ->
    ?SERIALIZE_ERR(utf8_string_undefined);
serialize_utf8_string(undefined, true, _StrictMode) ->
    <<>>;
serialize_utf8_string(String, _AllowNull, StrictMode) ->
    serialize_utf8_string(String, StrictMode).

serialize_utf8_string(String, true) ->
    StringBin = unicode:characters_to_binary(String),
    serialize_utf8_string(StringBin, false);
serialize_utf8_string(String, false) ->
    Len = byte_size(String),
    true = (Len =< 16#ffff),
    <<Len:16/big, String/binary>>.

serialize_remaining_len(I) ->
    serialize_variable_byte_integer(I).

serialize_variable_byte_integer(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialize_variable_byte_integer(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialize_variable_byte_integer(N div ?HIGHBIT))/binary>>.

%% Is the frame too large?
-spec is_too_large(iodata(), pos_integer()) -> boolean().
is_too_large(IoData, MaxSize) ->
    iolist_size(IoData) >= MaxSize.

get_property(_Key, undefined, Default) ->
    Default;
get_property(Key, Props, Default) ->
    maps:get(Key, Props, Default).

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
-compile({inline, [validate_connect_reserved/1]}).
validate_connect_reserved(0) -> ok;
validate_connect_reserved(1) -> ?PARSE_ERR(reserved_connect_flag).

-compile({inline, [validate_connect_will/3]}).
%% MQTT-v3.1.1-[MQTT-3.1.2-13], MQTT-v5.0-[MQTT-3.1.2-11]
validate_connect_will(false, _, WillQoS) when WillQoS > 0 -> ?PARSE_ERR(invalid_will_qos);
%% MQTT-v3.1.1-[MQTT-3.1.2-14], MQTT-v5.0-[MQTT-3.1.2-12]
validate_connect_will(true, _, WillQoS) when WillQoS > 2 -> ?PARSE_ERR(invalid_will_qos);
%% MQTT-v3.1.1-[MQTT-3.1.2-15], MQTT-v5.0-[MQTT-3.1.2-13]
validate_connect_will(false, WillRetain, _) when WillRetain -> ?PARSE_ERR(invalid_will_retain);
validate_connect_will(_, _, _) -> ok.

-compile({inline, [validate_connect_password_flag/4]}).
%% MQTT-v3.1
%% Username flag and password flag are not strongly related
%% https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#connect
validate_connect_password_flag(true, ?MQTT_PROTO_V3, _, _) ->
    ok;
%% MQTT-v3.1.1-[MQTT-3.1.2-22]
validate_connect_password_flag(true, ?MQTT_PROTO_V4, UsernameFlag, PasswordFlag) ->
    %% BUG-FOR-BUG compatible, only check when `strict-mode`
    UsernameFlag orelse PasswordFlag andalso ?PARSE_ERR(invalid_password_flag);
validate_connect_password_flag(true, ?MQTT_PROTO_V5, _, _) ->
    ok;
validate_connect_password_flag(_, _, _, _) ->
    ok.

-compile({inline, [bool/1]}).
bool(0) -> false;
bool(1) -> true.

flag(undefined) -> ?RESERVED;
flag(false) -> 0;
flag(true) -> 1;
flag(X) when is_integer(X) -> X;
flag(B) when is_binary(B) -> 1.

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
%% Note: this is under the assumption that the string is already validated by `unicode:characters_to_binary/1`
%% hence there is no need to validate utf8 byte sequence integrity
validate_mqtt_utf8_char(<<>>) ->
    true;
validate_mqtt_utf8_char(<<H/utf8, _Rest/binary>>) when
    H >= 16#00, H =< 16#1F;
    H >= 16#7F, H =< 16#9F
->
    false;
validate_mqtt_utf8_char(<<_H/utf8, Rest/binary>>) ->
    validate_mqtt_utf8_char(Rest);
validate_mqtt_utf8_char(<<_BadUtf8, _Rest/binary>>) ->
    false.
