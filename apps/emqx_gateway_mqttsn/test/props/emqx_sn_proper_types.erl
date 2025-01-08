%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sn_proper_types).

-include("emqx_mqttsn.hrl").
-include_lib("proper/include/proper.hrl").

-compile({no_auto_import, [register/1]}).

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_proper_types, [topic/0]).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

'ADVERTISE'() ->
    ?LET(
        {GwId, Duration},
        {gateway_id(), duration()},
        ?SN_ADVERTISE_MSG(GwId, Duration)
    ).

'SEARCHGW'() ->
    ?LET(Radius, radius(), ?SN_SEARCHGW_MSG(Radius)).

'GWINFO'() ->
    ?LET(
        {GwId, GwAddr},
        {gateway_id(), gateway_addr()},
        ?SN_GWINFO_MSG(GwId, GwAddr)
    ).

'CONNECT'() ->
    ?LET(
        {Flags, ProtocolId, Duration, ClientId},
        {conn_flags(), protocol_id(), duration(), clientid()},
        ?SN_CONNECT_MSG(Flags, ProtocolId, Duration, ClientId)
    ).

'CONNACK'() ->
    ?LET(Rc, return_code(), ?SN_CONNACK_MSG(Rc)).

'WILLTOTPICREQ'() ->
    ?SN_WILLTOPICREQ_MSG().

'WILLTOPIC'() ->
    ?LET(
        {Flags, Topic},
        {will_topic_flags(), topic()},
        ?SN_WILLTOPIC_MSG(Flags, Topic)
    ).

'WILLTOPCI_EMPTY'() ->
    ?SN_WILLTOPIC_EMPTY_MSG.

'WILLMESSAGEREQ'() ->
    ?SN_WILLMSGREQ_MSG().

'WILLMESSAGE'() ->
    ?LET(Payload, binary(), ?SN_WILLMSG_MSG(Payload)).

'REGISTER'() ->
    ?LET(
        {MsgId, TopicName},
        {message_id(), topic()},
        ?SN_REGISTER_MSG(16#0000, MsgId, TopicName)
    ).

'REGACK'() ->
    ?LET(
        {TopicId, MsgId, Rc},
        {topic_id(), message_id(), return_code()},
        ?SN_REGACK_MSG(TopicId, MsgId, Rc)
    ).

'PUBLISH'() ->
    ?LET(
        {Flags, MsgId, Data},
        {publish_flags(), message_id(), binary()},
        ?SN_PUBLISH_MSG(Flags, pub_topic_by_type(Flags), MsgId, Data)
    ).

'PUBACK'() ->
    ?LET(
        {TopicId, MsgId, Rc},
        {topic_id(), message_id(), return_code()},
        ?SN_PUBACK_MSG(TopicId, MsgId, Rc)
    ).

'PUBCOMP_REC_REL'() ->
    ?LET(
        {Type, MsgId},
        {oneof([?SN_PUBREC, ?SN_PUBREL, ?SN_PUBCOMP]), message_id()},
        ?SN_PUBREC_MSG(Type, MsgId)
    ).

'SUBSCRIBE'() ->
    ?LET(
        {Flags, MsgId},
        {subscribe_flags(), message_id()},
        ?SN_SUBSCRIBE_MSG(Flags, MsgId, topic_by_type(Flags))
    ).

'SUBACK'() ->
    ?LET(
        {Flags, TopicId, MsgId, Rc},
        {suback_flags(), topic_id(), message_id(), return_code()},
        ?SN_SUBACK_MSG(Flags, TopicId, MsgId, Rc)
    ).

'UNSUBSCRIBE'() ->
    ?LET(
        {Flags, MsgId},
        {unsubscribe_flags(), message_id()},
        ?SN_UNSUBSCRIBE_MSG(Flags, MsgId, topic_by_type(Flags))
    ).

'UNSUBACK'() ->
    ?LET(
        MsgId,
        message_id(),
        ?SN_UNSUBACK_MSG(MsgId)
    ).

'PINGREQ'() ->
    ?LET(ClientId, clientid(), ?SN_PINGREQ_MSG(ClientId)).

'PINGRESP'() ->
    ?SN_PINGRESP_MSG().

'DISCONNECT'() ->
    ?LET(Duration, oneof([duration(), undefined]), ?SN_DISCONNECT_MSG(Duration)).

'WILLTOPICUPD'() ->
    ?LET(
        {Flags, Topic},
        {willtopic_upd_flags(), topic()},
        ?SN_WILLTOPICUPD_MSG(Flags, Topic)
    ).

'WILLTOPICRESP'() ->
    ?LET(Rc, return_code(), ?SN_WILLTOPICRESP_MSG(Rc)).

'WILLMSGUPD'() ->
    ?LET(Payload, binary(), ?SN_WILLMSGUPD_MSG(Payload)).

'WILLMSGRESP'() ->
    ?LET(Rc, return_code(), ?SN_WILLMSGRESP_MSG(Rc)).

%%--------------------------------------------------------------------
%% Micr types

%% The GwId field is 1-octet long and
gateway_id() ->
    range(0, 16#ff).

%% The Duration field is 2-octet long
duration() ->
    range(0, 16#ffff).

%% The Radius field is 1-octet long
radius() ->
    range(0, 16#ff).

gateway_addr() ->
    ?LET(
        L,
        oneof([0, range(2, 16#ff)]),
        begin
            Addr = list_to_binary([rand:uniform(256) - 1 || _ <- lists:seq(1, L)]),
            <<(byte_size(Addr)):8, Addr/binary>>
        end
    ).

mqtt_sn_flags() ->
    ?LET(
        {Dup, Qos, Retain, Will, CleanStart, IdType},
        {boolean(), mqtt_sn_qos(), boolean(), boolean(), boolean(), topic_id_type()},
        #mqtt_sn_flags{
            dup = Dup,
            qos = Qos,
            retain = Retain,
            will = Will,
            clean_start = CleanStart,
            topic_id_type = IdType
        }
    ).

conn_flags() ->
    ?LET(
        {Will, CleanStart},
        {boolean(), boolean()},
        #mqtt_sn_flags{will = Will, clean_start = CleanStart}
    ).

will_topic_flags() ->
    ?LET(
        {Qos, Retain},
        {mqtt_sn_qos(), boolean()},
        #mqtt_sn_flags{qos = Qos, retain = Retain}
    ).

publish_flags() ->
    ?LET(
        {Dup, Qos, Retain, IdType},
        {boolean(), publish_qos(), boolean(), pub_topic_id_type()},
        #mqtt_sn_flags{dup = Dup, qos = Qos, retain = Retain, topic_id_type = IdType}
    ).

subscribe_flags() ->
    ?LET(
        {Dup, Qos, IdType},
        {boolean(), publish_qos(), topic_id_type()},
        #mqtt_sn_flags{dup = Dup, qos = Qos, topic_id_type = IdType}
    ).

suback_flags() ->
    ?LET(Qos, mqtt_sn_qos(), #mqtt_sn_flags{qos = Qos}).

unsubscribe_flags() ->
    ?LET(IdType, topic_id_type(), #mqtt_sn_flags{topic_id_type = IdType}).

willtopic_upd_flags() ->
    ?LET(
        {Qos, Retain},
        {mqtt_sn_qos(), boolean()},
        #mqtt_sn_flags{qos = Qos, retain = Retain}
    ).

mqtt_sn_qos() ->
    oneof([0, 1, 2]).

publish_qos() ->
    oneof([0, 1, 2, 3]).

topic_id_type() ->
    oneof([0, 1, 2]).

pub_topic_id_type() ->
    %% Only topic id
    oneof([1, 2]).

%% The ProtocolId is 1-octet long.
%% It is coded 0x01
protocol_id() ->
    16#01.

%% The ClientId field has a variable length and contains
%% a 1-23 character long string
clientid() ->
    ?LET(
        L,
        range(1, 23),
        begin
            list_to_binary([rand_09_az_AZ() || _ <- lists:seq(1, L)])
        end
    ).

return_code() ->
    range(0, 16#ff).

rand_09_az_AZ() ->
    case rand:uniform(3) of
        1 -> $0 + rand:uniform(10) - 1;
        2 -> $a + rand:uniform(26) - 1;
        3 -> $A + rand:uniform(26) - 1
    end.

topic_id() ->
    range(0, 16#ffff).

message_id() ->
    range(0, 16#ffff).

topic_by_type(Flags) ->
    case Flags#mqtt_sn_flags.topic_id_type of
        0 -> topic();
        1 -> range(0, 16#ffff);
        2 -> list_to_binary([rand_09_az_AZ(), rand_09_az_AZ()])
    end.

pub_topic_by_type(Flags) ->
    case Flags#mqtt_sn_flags.topic_id_type of
        2 -> list_to_binary([rand_09_az_AZ(), rand_09_az_AZ()]);
        _ -> range(0, 16#ffff)
    end.
