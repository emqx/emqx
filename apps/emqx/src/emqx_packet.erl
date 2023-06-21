%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_packet).

-elvis([{elvis_style, no_spec_with_records, disable}]).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

%% Header APIs
-export([
    type/1,
    type_name/1,
    dup/1,
    qos/1,
    retain/1
]).

%% Field APIs
-export([
    proto_name/1,
    proto_ver/1,
    info/2,
    set_props/2
]).

%% Check API
-export([
    check/1,
    check/2
]).

-export([
    to_message/2,
    to_message/3,
    will_msg/1
]).

-export([
    format/1,
    format/2
]).

-export([encode_hex/1]).

-define(TYPE_NAMES,
    {'CONNECT', 'CONNACK', 'PUBLISH', 'PUBACK', 'PUBREC', 'PUBREL', 'PUBCOMP', 'SUBSCRIBE',
        'SUBACK', 'UNSUBSCRIBE', 'UNSUBACK', 'PINGREQ', 'PINGRESP', 'DISCONNECT', 'AUTH'}
).

-type connect() :: #mqtt_packet_connect{}.
-type publish() :: #mqtt_packet_publish{}.
-type subscribe() :: #mqtt_packet_subscribe{}.
-type unsubscribe() :: #mqtt_packet_unsubscribe{}.

%%--------------------------------------------------------------------
%% MQTT Packet Type and Flags.
%%--------------------------------------------------------------------

%% @doc MQTT packet type.
-spec type(emqx_types:packet()) -> emqx_types:packet_type().
type(#mqtt_packet{header = #mqtt_packet_header{type = Type}}) ->
    Type.

%% @doc Name of MQTT packet type.
-spec type_name(emqx_types:packet() | non_neg_integer()) -> atom() | string().
type_name(#mqtt_packet{} = Packet) ->
    type_name(type(Packet));
type_name(0) ->
    'FORBIDDEN';
type_name(Type) when Type > 0 andalso Type =< tuple_size(?TYPE_NAMES) ->
    element(Type, ?TYPE_NAMES);
type_name(Type) ->
    "UNKNOWN(" ++ integer_to_list(Type) ++ ")".

%% @doc Dup flag of MQTT packet.
-spec dup(emqx_types:packet()) -> boolean().
dup(#mqtt_packet{header = #mqtt_packet_header{dup = Dup}}) ->
    Dup.

%% @doc QoS of MQTT packet type.
-spec qos(emqx_types:packet()) -> emqx_types:qos().
qos(#mqtt_packet{header = #mqtt_packet_header{qos = QoS}}) ->
    QoS.

%% @doc Retain flag of MQTT packet.
-spec retain(emqx_types:packet()) -> boolean().
retain(#mqtt_packet{header = #mqtt_packet_header{retain = Retain}}) ->
    Retain.

%%--------------------------------------------------------------------
%% Protocol name and version of MQTT CONNECT Packet.
%%--------------------------------------------------------------------

%% @doc Protocol name of the CONNECT Packet.
-spec proto_name(emqx_types:packet() | connect()) -> binary().
proto_name(?CONNECT_PACKET(ConnPkt)) ->
    proto_name(ConnPkt);
proto_name(#mqtt_packet_connect{proto_name = Name}) ->
    Name.

%% @doc Protocol version of the CONNECT Packet.
-spec proto_ver(emqx_types:packet() | connect()) -> emqx_types:proto_ver().
proto_ver(?CONNECT_PACKET(ConnPkt)) ->
    proto_ver(ConnPkt);
proto_ver(#mqtt_packet_connect{proto_ver = Ver}) ->
    Ver.

%%--------------------------------------------------------------------
%% Field Info
%%--------------------------------------------------------------------

info(proto_name, #mqtt_packet_connect{proto_name = Name}) ->
    Name;
info(proto_ver, #mqtt_packet_connect{proto_ver = Ver}) ->
    Ver;
info(is_bridge, #mqtt_packet_connect{is_bridge = IsBridge}) ->
    IsBridge;
info(clean_start, #mqtt_packet_connect{clean_start = CleanStart}) ->
    CleanStart;
info(will_flag, #mqtt_packet_connect{will_flag = WillFlag}) ->
    WillFlag;
info(will_qos, #mqtt_packet_connect{will_qos = WillQoS}) ->
    WillQoS;
info(will_retain, #mqtt_packet_connect{will_retain = WillRetain}) ->
    WillRetain;
info(keepalive, #mqtt_packet_connect{keepalive = KeepAlive}) ->
    KeepAlive;
info(properties, #mqtt_packet_connect{properties = Props}) ->
    Props;
info(clientid, #mqtt_packet_connect{clientid = ClientId}) ->
    ClientId;
info(will_props, #mqtt_packet_connect{will_props = WillProps}) ->
    WillProps;
info(will_topic, #mqtt_packet_connect{will_topic = WillTopic}) ->
    WillTopic;
info(will_payload, #mqtt_packet_connect{will_payload = Payload}) ->
    Payload;
info(username, #mqtt_packet_connect{username = Username}) ->
    Username;
info(password, #mqtt_packet_connect{password = Password}) ->
    Password;
info(ack_flags, #mqtt_packet_connack{ack_flags = Flags}) ->
    Flags;
info(reason_code, #mqtt_packet_connack{reason_code = RC}) ->
    RC;
info(properties, #mqtt_packet_connack{properties = Props}) ->
    Props;
info(topic_name, #mqtt_packet_publish{topic_name = Topic}) ->
    Topic;
info(packet_id, #mqtt_packet_publish{packet_id = PacketId}) ->
    PacketId;
info(properties, #mqtt_packet_publish{properties = Props}) ->
    Props;
info(packet_id, #mqtt_packet_puback{packet_id = PacketId}) ->
    PacketId;
info(reason_code, #mqtt_packet_puback{reason_code = RC}) ->
    RC;
info(properties, #mqtt_packet_puback{properties = Props}) ->
    Props;
info(packet_id, #mqtt_packet_subscribe{packet_id = PacketId}) ->
    PacketId;
info(properties, #mqtt_packet_subscribe{properties = Props}) ->
    Props;
info(topic_filters, #mqtt_packet_subscribe{topic_filters = Topics}) ->
    Topics;
info(packet_id, #mqtt_packet_suback{packet_id = PacketId}) ->
    PacketId;
info(properties, #mqtt_packet_suback{properties = Props}) ->
    Props;
info(reason_codes, #mqtt_packet_suback{reason_codes = RCs}) ->
    RCs;
info(packet_id, #mqtt_packet_unsubscribe{packet_id = PacketId}) ->
    PacketId;
info(properties, #mqtt_packet_unsubscribe{properties = Props}) ->
    Props;
info(topic_filters, #mqtt_packet_unsubscribe{topic_filters = Topics}) ->
    Topics;
info(packet_id, #mqtt_packet_unsuback{packet_id = PacketId}) ->
    PacketId;
info(properties, #mqtt_packet_unsuback{properties = Props}) ->
    Props;
info(reason_codes, #mqtt_packet_unsuback{reason_codes = RCs}) ->
    RCs;
info(reason_code, #mqtt_packet_disconnect{reason_code = RC}) ->
    RC;
info(properties, #mqtt_packet_disconnect{properties = Props}) ->
    Props;
info(reason_code, #mqtt_packet_auth{reason_code = RC}) ->
    RC;
info(properties, #mqtt_packet_auth{properties = Props}) ->
    Props.

set_props(Props, #mqtt_packet_connect{} = Pkt) ->
    Pkt#mqtt_packet_connect{properties = Props};
set_props(Props, #mqtt_packet_connack{} = Pkt) ->
    Pkt#mqtt_packet_connack{properties = Props};
set_props(Props, #mqtt_packet_publish{} = Pkt) ->
    Pkt#mqtt_packet_publish{properties = Props};
set_props(Props, #mqtt_packet_puback{} = Pkt) ->
    Pkt#mqtt_packet_puback{properties = Props};
set_props(Props, #mqtt_packet_subscribe{} = Pkt) ->
    Pkt#mqtt_packet_subscribe{properties = Props};
set_props(Props, #mqtt_packet_suback{} = Pkt) ->
    Pkt#mqtt_packet_suback{properties = Props};
set_props(Props, #mqtt_packet_unsubscribe{} = Pkt) ->
    Pkt#mqtt_packet_unsubscribe{properties = Props};
set_props(Props, #mqtt_packet_unsuback{} = Pkt) ->
    Pkt#mqtt_packet_unsuback{properties = Props};
set_props(Props, #mqtt_packet_disconnect{} = Pkt) ->
    Pkt#mqtt_packet_disconnect{properties = Props};
set_props(Props, #mqtt_packet_auth{} = Pkt) ->
    Pkt#mqtt_packet_auth{properties = Props}.

%%--------------------------------------------------------------------
%% Check MQTT Packet
%%--------------------------------------------------------------------

%% @doc Check PubSub Packet.
-spec check(emqx_types:packet() | publish() | subscribe() | unsubscribe()) ->
    ok | {error, emqx_types:reason_code()}.
check(#mqtt_packet{
    header = #mqtt_packet_header{type = ?PUBLISH},
    variable = PubPkt
}) when not is_tuple(PubPkt) ->
    %% publish without any data
    %% disconnect instead of crash
    {error, ?RC_PROTOCOL_ERROR};
check(#mqtt_packet{variable = #mqtt_packet_publish{} = PubPkt}) ->
    check(PubPkt);
check(#mqtt_packet{variable = #mqtt_packet_subscribe{} = SubPkt}) ->
    check(SubPkt);
check(#mqtt_packet{variable = #mqtt_packet_unsubscribe{} = UnsubPkt}) ->
    check(UnsubPkt);
%% A Topic Alias of 0 is not permitted.
check(#mqtt_packet_publish{topic_name = <<>>, properties = #{'Topic-Alias' := 0}}) ->
    {error, ?RC_PROTOCOL_ERROR};
check(#mqtt_packet_publish{topic_name = <<>>, properties = #{'Topic-Alias' := _Alias}}) ->
    ok;
check(#mqtt_packet_publish{topic_name = <<>>, properties = #{}}) ->
    {error, ?RC_PROTOCOL_ERROR};
check(#mqtt_packet_publish{topic_name = TopicName, properties = Props}) ->
    try emqx_topic:validate(name, TopicName) of
        true -> check_pub_props(Props)
    catch
        error:_Error ->
            {error, ?RC_TOPIC_NAME_INVALID}
    end;
check(#mqtt_packet_subscribe{properties = #{'Subscription-Identifier' := I}}) when
    I =< 0; I >= 16#FFFFFFF
->
    {error, ?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED};
check(#mqtt_packet_subscribe{topic_filters = []}) ->
    {error, ?RC_TOPIC_FILTER_INVALID};
check(#mqtt_packet_subscribe{topic_filters = TopicFilters}) ->
    try
        validate_topic_filters(TopicFilters)
    catch
        %% Known Specificed Reason Code
        error:{error, RC} ->
            {error, RC};
        error:_Error ->
            {error, ?RC_TOPIC_FILTER_INVALID}
    end;
check(#mqtt_packet_unsubscribe{topic_filters = []}) ->
    {error, ?RC_TOPIC_FILTER_INVALID};
check(#mqtt_packet_unsubscribe{topic_filters = TopicFilters}) ->
    try
        validate_topic_filters(TopicFilters)
    catch
        error:_Error ->
            {error, ?RC_TOPIC_FILTER_INVALID}
    end.

check_pub_props(#{'Topic-Alias' := 0}) ->
    {error, ?RC_TOPIC_ALIAS_INVALID};
check_pub_props(#{'Subscription-Identifier' := 0}) ->
    {error, ?RC_PROTOCOL_ERROR};
check_pub_props(#{'Response-Topic' := ResponseTopic}) ->
    try emqx_topic:validate(name, ResponseTopic) of
        true -> ok
    catch
        error:_Error ->
            {error, ?RC_PROTOCOL_ERROR}
    end;
check_pub_props(_Props) ->
    ok.

%% @doc Check CONNECT Packet.
-spec check(emqx_types:packet() | connect(), Opts :: map()) ->
    ok | {error, emqx_types:reason_code()}.
check(?CONNECT_PACKET(ConnPkt), Opts) ->
    check(ConnPkt, Opts);
check(ConnPkt, Opts) when is_record(ConnPkt, mqtt_packet_connect) ->
    run_checks(
        [
            fun check_proto_ver/2,
            fun check_client_id/2,
            fun check_conn_props/2,
            fun check_will_msg/2
        ],
        ConnPkt,
        Opts
    ).

check_proto_ver(
    #mqtt_packet_connect{
        proto_ver = Ver,
        proto_name = Name
    },
    _Opts
) ->
    case proplists:get_value(Ver, ?PROTOCOL_NAMES) of
        Name -> ok;
        _Other -> {error, ?RC_UNSUPPORTED_PROTOCOL_VERSION}
    end.

%% MQTT3.1 does not allow null clientId
check_client_id(
    #mqtt_packet_connect{
        proto_ver = ?MQTT_PROTO_V3,
        clientid = <<>>
    },
    _Opts
) ->
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};
%% Issue#599: Null clientId and clean_start = false
check_client_id(
    #mqtt_packet_connect{
        clientid = <<>>,
        clean_start = false
    },
    _Opts
) ->
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};
check_client_id(
    #mqtt_packet_connect{
        clientid = <<>>,
        clean_start = true
    },
    _Opts
) ->
    ok;
check_client_id(
    #mqtt_packet_connect{clientid = ClientId},
    #{max_clientid_len := MaxLen} = _Opts
) ->
    case (1 =< (Len = byte_size(ClientId))) andalso (Len =< MaxLen) of
        true -> ok;
        false -> {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID}
    end.

check_conn_props(#mqtt_packet_connect{properties = undefined}, _Opts) ->
    ok;
check_conn_props(#mqtt_packet_connect{properties = #{'Receive-Maximum' := 0}}, _Opts) ->
    {error, ?RC_PROTOCOL_ERROR};
check_conn_props(
    #mqtt_packet_connect{properties = #{'Request-Response-Information' := ReqRespInfo}}, _Opts
) when
    ReqRespInfo =/= 0, ReqRespInfo =/= 1
->
    {error, ?RC_PROTOCOL_ERROR};
check_conn_props(
    #mqtt_packet_connect{properties = #{'Request-Problem-Information' := ReqProInfo}}, _Opts
) when
    ReqProInfo =/= 0, ReqProInfo =/= 1
->
    {error, ?RC_PROTOCOL_ERROR};
check_conn_props(_ConnPkt, _Opts) ->
    ok.

check_will_msg(#mqtt_packet_connect{will_flag = false}, _Caps) ->
    ok;
check_will_msg(
    #mqtt_packet_connect{will_retain = true},
    _Opts = #{mqtt_retain_available := false}
) ->
    {error, ?RC_RETAIN_NOT_SUPPORTED};
check_will_msg(
    #mqtt_packet_connect{will_qos = WillQoS},
    _Opts = #{max_qos_allowed := MaxQoS}
) when WillQoS > MaxQoS ->
    {error, ?RC_QOS_NOT_SUPPORTED};
check_will_msg(#mqtt_packet_connect{will_topic = WillTopic}, _Opts) ->
    try emqx_topic:validate(name, WillTopic) of
        true -> ok
    catch
        error:_Error ->
            {error, ?RC_TOPIC_NAME_INVALID}
    end.

run_checks([], _Packet, _Options) ->
    ok;
run_checks([Check | More], Packet, Options) ->
    case Check(Packet, Options) of
        ok -> run_checks(More, Packet, Options);
        Error = {error, _Reason} -> Error
    end.

%% @doc Validate MQTT Packet
%% @private
validate_topic_filters(TopicFilters) ->
    lists:foreach(
        fun
            %% Protocol Error and Should Disconnect
            %% MQTT-5.0 [MQTT-3.8.3-4] and [MQTT-4.13.1-1]
            ({<<?SHARE, "/", _Rest/binary>>, #{nl := 1}}) ->
                error({error, ?RC_PROTOCOL_ERROR});
            ({TopicFilter, _SubOpts}) ->
                emqx_topic:validate(TopicFilter);
            (TopicFilter) ->
                emqx_topic:validate(TopicFilter)
        end,
        TopicFilters
    ).

-spec to_message(emqx_types:packet(), emqx_types:clientid()) -> emqx_types:message().
to_message(Packet, ClientId) ->
    to_message(Packet, ClientId, #{}).

%% @doc Transform Publish Packet to Message.
-spec to_message(emqx_types:packet(), emqx_types:clientid(), map()) -> emqx_types:message().
to_message(
    #mqtt_packet{
        header = #mqtt_packet_header{
            type = ?PUBLISH,
            retain = Retain,
            qos = QoS,
            dup = Dup
        },
        variable = #mqtt_packet_publish{
            topic_name = Topic,
            properties = Props
        },
        payload = Payload
    },
    ClientId,
    Headers
) ->
    Msg = emqx_message:make(ClientId, QoS, Topic, Payload),
    Msg#message{
        flags = #{dup => Dup, retain => Retain},
        headers = Headers#{properties => Props}
    }.

-spec will_msg(#mqtt_packet_connect{}) -> emqx_types:message().
will_msg(#mqtt_packet_connect{will_flag = false}) ->
    undefined;
will_msg(#mqtt_packet_connect{
    clientid = ClientId,
    username = Username,
    will_retain = Retain,
    will_qos = QoS,
    will_topic = Topic,
    will_props = Props,
    will_payload = Payload
}) ->
    Msg = emqx_message:make(ClientId, QoS, Topic, Payload),
    Msg#message{
        flags = #{dup => false, retain => Retain},
        headers = #{username => Username, properties => Props}
    }.

%% @doc Format packet
-spec format(emqx_types:packet()) -> iolist().
format(Packet) -> format(Packet, emqx_trace_handler:payload_encode()).

%% @doc Format packet
-spec format(emqx_types:packet(), hex | text | hidden) -> iolist().
format(#mqtt_packet{header = Header, variable = Variable, payload = Payload}, PayloadEncode) ->
    HeaderIO = format_header(Header),
    case format_variable(Variable, Payload, PayloadEncode) of
        "" -> [HeaderIO, ")"];
        VarIO -> [HeaderIO, ", ", VarIO, ")"]
    end;
%% receive a frame error packet, such as {frame_error,frame_too_large} or
%% {frame_error,#{expected => <<"'MQTT' or 'MQIsdp'">>,hint => invalid_proto_name,received => <<"bad_name">>}}
format(FrameError, _PayloadEncode) ->
    lists:flatten(io_lib:format("~tp", [FrameError])).

format_header(#mqtt_packet_header{
    type = Type,
    dup = Dup,
    qos = QoS,
    retain = Retain
}) ->
    io_lib:format("~ts(Q~p, R~p, D~p", [type_name(Type), QoS, i(Retain), i(Dup)]).

format_variable(undefined, _, _) ->
    "";
format_variable(Variable, undefined, PayloadEncode) ->
    format_variable(Variable, PayloadEncode);
format_variable(Variable, Payload, PayloadEncode) ->
    [format_variable(Variable, PayloadEncode), ", ", format_payload(Payload, PayloadEncode)].

format_variable(
    #mqtt_packet_connect{
        proto_ver = ProtoVer,
        proto_name = ProtoName,
        will_retain = WillRetain,
        will_qos = WillQoS,
        will_flag = WillFlag,
        clean_start = CleanStart,
        keepalive = KeepAlive,
        clientid = ClientId,
        will_topic = WillTopic,
        will_payload = WillPayload,
        username = Username,
        password = Password
    },
    PayloadEncode
) ->
    Base = io_lib:format(
        "ClientId=~ts, ProtoName=~ts, ProtoVsn=~p, CleanStart=~ts, KeepAlive=~p, Username=~ts, Password=~ts",
        [ClientId, ProtoName, ProtoVer, CleanStart, KeepAlive, Username, format_password(Password)]
    ),
    case WillFlag of
        true ->
            [
                Base,
                io_lib:format(
                    ", Will(Q~p, R~p, Topic=~ts ",
                    [WillQoS, i(WillRetain), WillTopic]
                ),
                format_payload(WillPayload, PayloadEncode),
                ")"
            ];
        false ->
            Base
    end;
format_variable(
    #mqtt_packet_disconnect{
        reason_code = ReasonCode
    },
    _
) ->
    io_lib:format("ReasonCode=~p", [ReasonCode]);
format_variable(
    #mqtt_packet_connack{
        ack_flags = AckFlags,
        reason_code = ReasonCode
    },
    _
) ->
    io_lib:format("AckFlags=~p, ReasonCode=~p", [AckFlags, ReasonCode]);
format_variable(
    #mqtt_packet_publish{
        topic_name = TopicName,
        packet_id = PacketId
    },
    _
) ->
    io_lib:format("Topic=~ts, PacketId=~p", [TopicName, PacketId]);
format_variable(
    #mqtt_packet_puback{
        packet_id = PacketId,
        reason_code = ReasonCode
    },
    _
) ->
    io_lib:format("PacketId=~p, ReasonCode=~p", [PacketId, ReasonCode]);
format_variable(
    #mqtt_packet_subscribe{
        packet_id = PacketId,
        topic_filters = TopicFilters
    },
    _
) ->
    [
        io_lib:format("PacketId=~p ", [PacketId]),
        "TopicFilters=",
        format_topic_filters(TopicFilters)
    ];
format_variable(
    #mqtt_packet_unsubscribe{
        packet_id = PacketId,
        topic_filters = Topics
    },
    _
) ->
    [
        io_lib:format("PacketId=~p ", [PacketId]),
        "TopicFilters=",
        format_topic_filters(Topics)
    ];
format_variable(
    #mqtt_packet_suback{
        packet_id = PacketId,
        reason_codes = ReasonCodes
    },
    _
) ->
    io_lib:format("PacketId=~p, ReasonCodes=~p", [PacketId, ReasonCodes]);
format_variable(#mqtt_packet_unsuback{packet_id = PacketId}, _) ->
    io_lib:format("PacketId=~p", [PacketId]);
format_variable(#mqtt_packet_auth{reason_code = ReasonCode}, _) ->
    io_lib:format("ReasonCode=~p", [ReasonCode]);
format_variable(PacketId, _) when is_integer(PacketId) ->
    io_lib:format("PacketId=~p", [PacketId]).

format_password(undefined) -> "";
format_password(<<>>) -> "";
format_password(_Password) -> "******".

format_payload(Payload, text) -> ["Payload=", io_lib:format("~ts", [Payload])];
format_payload(Payload, hex) -> ["Payload(hex)=", encode_hex(Payload)];
format_payload(_, hidden) -> "Payload=******".

i(true) -> 1;
i(false) -> 0;
i(I) when is_integer(I) -> I.

format_topic_filters(Filters) ->
    [
        "[",
        lists:join(
            ",",
            lists:map(
                fun
                    ({TopicFilter, SubOpts}) ->
                        io_lib:format("~ts(~p)", [TopicFilter, SubOpts]);
                    (TopicFilter) ->
                        io_lib:format("~ts", [TopicFilter])
                end,
                Filters
            )
        ),
        "]"
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Hex encoding functions
%% Copy from binary:encode_hex/1 (was only introduced in OTP24).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(HEX(X), (hex(X)):16).
-compile({inline, [hex/1]}).
-spec encode_hex(Bin) -> Bin2 when
    Bin :: binary(),
    Bin2 :: <<_:_*16>>.
encode_hex(Data) when byte_size(Data) rem 8 =:= 0 ->
    <<
        <<?HEX(A), ?HEX(B), ?HEX(C), ?HEX(D), ?HEX(E), ?HEX(F), ?HEX(G), ?HEX(H)>>
     || <<A, B, C, D, E, F, G, H>> <= Data
    >>;
encode_hex(Data) when byte_size(Data) rem 7 =:= 0 ->
    <<
        <<?HEX(A), ?HEX(B), ?HEX(C), ?HEX(D), ?HEX(E), ?HEX(F), ?HEX(G)>>
     || <<A, B, C, D, E, F, G>> <= Data
    >>;
encode_hex(Data) when byte_size(Data) rem 6 =:= 0 ->
    <<<<?HEX(A), ?HEX(B), ?HEX(C), ?HEX(D), ?HEX(E), ?HEX(F)>> || <<A, B, C, D, E, F>> <= Data>>;
encode_hex(Data) when byte_size(Data) rem 5 =:= 0 ->
    <<<<?HEX(A), ?HEX(B), ?HEX(C), ?HEX(D), ?HEX(E)>> || <<A, B, C, D, E>> <= Data>>;
encode_hex(Data) when byte_size(Data) rem 4 =:= 0 ->
    <<<<?HEX(A), ?HEX(B), ?HEX(C), ?HEX(D)>> || <<A, B, C, D>> <= Data>>;
encode_hex(Data) when byte_size(Data) rem 3 =:= 0 ->
    <<<<?HEX(A), ?HEX(B), ?HEX(C)>> || <<A, B, C>> <= Data>>;
encode_hex(Data) when byte_size(Data) rem 2 =:= 0 ->
    <<<<?HEX(A), ?HEX(B)>> || <<A, B>> <= Data>>;
encode_hex(Data) when is_binary(Data) ->
    <<<<?HEX(N)>> || <<N>> <= Data>>;
encode_hex(Bin) ->
    erlang:error(badarg, [Bin]).

hex(X) ->
    element(
        X + 1,
        {16#3030, 16#3031, 16#3032, 16#3033, 16#3034, 16#3035, 16#3036, 16#3037, 16#3038, 16#3039,
            16#3041, 16#3042, 16#3043, 16#3044, 16#3045, 16#3046, 16#3130, 16#3131, 16#3132,
            16#3133, 16#3134, 16#3135, 16#3136, 16#3137, 16#3138, 16#3139, 16#3141, 16#3142,
            16#3143, 16#3144, 16#3145, 16#3146, 16#3230, 16#3231, 16#3232, 16#3233, 16#3234,
            16#3235, 16#3236, 16#3237, 16#3238, 16#3239, 16#3241, 16#3242, 16#3243, 16#3244,
            16#3245, 16#3246, 16#3330, 16#3331, 16#3332, 16#3333, 16#3334, 16#3335, 16#3336,
            16#3337, 16#3338, 16#3339, 16#3341, 16#3342, 16#3343, 16#3344, 16#3345, 16#3346,
            16#3430, 16#3431, 16#3432, 16#3433, 16#3434, 16#3435, 16#3436, 16#3437, 16#3438,
            16#3439, 16#3441, 16#3442, 16#3443, 16#3444, 16#3445, 16#3446, 16#3530, 16#3531,
            16#3532, 16#3533, 16#3534, 16#3535, 16#3536, 16#3537, 16#3538, 16#3539, 16#3541,
            16#3542, 16#3543, 16#3544, 16#3545, 16#3546, 16#3630, 16#3631, 16#3632, 16#3633,
            16#3634, 16#3635, 16#3636, 16#3637, 16#3638, 16#3639, 16#3641, 16#3642, 16#3643,
            16#3644, 16#3645, 16#3646, 16#3730, 16#3731, 16#3732, 16#3733, 16#3734, 16#3735,
            16#3736, 16#3737, 16#3738, 16#3739, 16#3741, 16#3742, 16#3743, 16#3744, 16#3745,
            16#3746, 16#3830, 16#3831, 16#3832, 16#3833, 16#3834, 16#3835, 16#3836, 16#3837,
            16#3838, 16#3839, 16#3841, 16#3842, 16#3843, 16#3844, 16#3845, 16#3846, 16#3930,
            16#3931, 16#3932, 16#3933, 16#3934, 16#3935, 16#3936, 16#3937, 16#3938, 16#3939,
            16#3941, 16#3942, 16#3943, 16#3944, 16#3945, 16#3946, 16#4130, 16#4131, 16#4132,
            16#4133, 16#4134, 16#4135, 16#4136, 16#4137, 16#4138, 16#4139, 16#4141, 16#4142,
            16#4143, 16#4144, 16#4145, 16#4146, 16#4230, 16#4231, 16#4232, 16#4233, 16#4234,
            16#4235, 16#4236, 16#4237, 16#4238, 16#4239, 16#4241, 16#4242, 16#4243, 16#4244,
            16#4245, 16#4246, 16#4330, 16#4331, 16#4332, 16#4333, 16#4334, 16#4335, 16#4336,
            16#4337, 16#4338, 16#4339, 16#4341, 16#4342, 16#4343, 16#4344, 16#4345, 16#4346,
            16#4430, 16#4431, 16#4432, 16#4433, 16#4434, 16#4435, 16#4436, 16#4437, 16#4438,
            16#4439, 16#4441, 16#4442, 16#4443, 16#4444, 16#4445, 16#4446, 16#4530, 16#4531,
            16#4532, 16#4533, 16#4534, 16#4535, 16#4536, 16#4537, 16#4538, 16#4539, 16#4541,
            16#4542, 16#4543, 16#4544, 16#4545, 16#4546, 16#4630, 16#4631, 16#4632, 16#4633,
            16#4634, 16#4635, 16#4636, 16#4637, 16#4638, 16#4639, 16#4641, 16#4642, 16#4643,
            16#4644, 16#4645, 16#4646}
    ).
