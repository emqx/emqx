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

-module(emqx_sn_frame_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqttsn.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

parse(D) ->
    {ok, P, _Rest, _State} = emqx_mqttsn_frame:parse(D, #{}),
    P.

serialize_pkt(P) ->
    emqx_mqttsn_frame:serialize_pkt(P, #{}).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_advertise(_) ->
    Adv = ?SN_ADVERTISE_MSG(1, 100),
    ?assertEqual(Adv, parse(serialize_pkt(Adv))).

t_searchgw(_) ->
    Sgw = #mqtt_sn_message{type = ?SN_SEARCHGW, variable = 1},
    ?assertEqual(Sgw, parse(serialize_pkt(Sgw))).

t_gwinfo(_) ->
    GwInfo = #mqtt_sn_message{type = ?SN_GWINFO, variable = {2, <<"EMQGW">>}},
    ?assertEqual(GwInfo, parse(serialize_pkt(GwInfo))).

t_connect(_) ->
    Flags = #mqtt_sn_flags{will = true, clean_start = true},
    Conn = #mqtt_sn_message{type = ?SN_CONNECT, variable = {Flags, 4, 300, <<"ClientId">>}},
    ?assertEqual(Conn, parse(serialize_pkt(Conn))).

t_connack(_) ->
    ConnAck = #mqtt_sn_message{type = ?SN_CONNACK, variable = 2},
    ?assertEqual(ConnAck, parse(serialize_pkt(ConnAck))).

t_willtopicreq(_) ->
    WtReq = #mqtt_sn_message{type = ?SN_WILLTOPICREQ},
    ?assertEqual(WtReq, parse(serialize_pkt(WtReq))).

t_willtopic(_) ->
    Flags = #mqtt_sn_flags{qos = 1, retain = false},
    Wt = #mqtt_sn_message{type = ?SN_WILLTOPIC, variable = {Flags, <<"WillTopic">>}},
    ?assertEqual(Wt, parse(serialize_pkt(Wt))).

t_willmsgreq(_) ->
    WmReq = #mqtt_sn_message{type = ?SN_WILLMSGREQ},
    ?assertEqual(WmReq, parse(serialize_pkt(WmReq))).

t_willmsg(_) ->
    WlMsg = #mqtt_sn_message{type = ?SN_WILLMSG, variable = <<"WillMsg">>},
    ?assertEqual(WlMsg, parse(serialize_pkt(WlMsg))).

t_register(_) ->
    RegMsg = ?SN_REGISTER_MSG(1, 2, <<"Topic">>),
    ?assertEqual(RegMsg, parse(serialize_pkt(RegMsg))).

t_regack(_) ->
    RegAck = ?SN_REGACK_MSG(1, 2, 0),
    ?assertEqual(RegAck, parse(serialize_pkt(RegAck))).

t_publish(_) ->
    Flags = #mqtt_sn_flags{dup = false, qos = 1, retain = false, topic_id_type = 2#01},
    PubMsg = #mqtt_sn_message{type = ?SN_PUBLISH, variable = {Flags, 1, 2, <<"Payload">>}},
    ?assertEqual(PubMsg, parse(serialize_pkt(PubMsg))).

t_puback(_) ->
    PubAck = #mqtt_sn_message{type = ?SN_PUBACK, variable = {1, 2, 0}},
    ?assertEqual(PubAck, parse(serialize_pkt(PubAck))).

t_pubrec(_) ->
    PubRec = #mqtt_sn_message{type = ?SN_PUBREC, variable = 16#1234},
    ?assertEqual(PubRec, parse(serialize_pkt(PubRec))).

t_pubrel(_) ->
    PubRel = #mqtt_sn_message{type = ?SN_PUBREL, variable = 16#1234},
    ?assertEqual(PubRel, parse(serialize_pkt(PubRel))).

t_pubcomp(_) ->
    PubComp = #mqtt_sn_message{type = ?SN_PUBCOMP, variable = 16#1234},
    ?assertEqual(PubComp, parse(serialize_pkt(PubComp))).

t_subscribe(_) ->
    Flags = #mqtt_sn_flags{dup = false, qos = 1, topic_id_type = 16#01},
    SubMsg = #mqtt_sn_message{type = ?SN_SUBSCRIBE, variable = {Flags, 16#4321, 16}},
    ?assertEqual(SubMsg, parse(serialize_pkt(SubMsg))).

t_suback(_) ->
    Flags = #mqtt_sn_flags{qos = 1},
    SubAck = #mqtt_sn_message{type = ?SN_SUBACK, variable = {Flags, 98, 89, 0}},
    ?assertEqual(SubAck, parse(serialize_pkt(SubAck))).

t_unsubscribe(_) ->
    Flags = #mqtt_sn_flags{dup = false, qos = 1, topic_id_type = 16#01},
    UnSub = #mqtt_sn_message{type = ?SN_UNSUBSCRIBE, variable = {Flags, 16#4321, 16}},
    ?assertEqual(UnSub, parse(serialize_pkt(UnSub))).

t_unsuback(_) ->
    UnsubAck = #mqtt_sn_message{type = ?SN_UNSUBACK, variable = 72},
    ?assertEqual(UnsubAck, parse(serialize_pkt(UnsubAck))).

t_pingreq(_) ->
    Ping = #mqtt_sn_message{type = ?SN_PINGREQ, variable = <<>>},
    ?assertEqual(Ping, parse(serialize_pkt(Ping))),
    Ping1 = #mqtt_sn_message{type = ?SN_PINGREQ, variable = <<"ClientId">>},
    ?assertEqual(Ping1, parse(serialize_pkt(Ping1))).

t_pingresp(_) ->
    PingResp = #mqtt_sn_message{type = ?SN_PINGRESP},
    ?assertEqual(PingResp, parse(serialize_pkt(PingResp))).

t_disconnect(_) ->
    Disconn = #mqtt_sn_message{type = ?SN_DISCONNECT},
    ?assertEqual(Disconn, parse(serialize_pkt(Disconn))).

t_willtopicupd(_) ->
    Flags = #mqtt_sn_flags{qos = 1, retain = true},
    WtUpd = #mqtt_sn_message{type = ?SN_WILLTOPICUPD, variable = {Flags, <<"Topic">>}},
    ?assertEqual(WtUpd, parse(serialize_pkt(WtUpd))).

t_willmsgupd(_) ->
    WlMsgUpd = #mqtt_sn_message{type = ?SN_WILLMSGUPD, variable = <<"WillMsg">>},
    ?assertEqual(WlMsgUpd, parse(serialize_pkt(WlMsgUpd))).

t_willmsgresp(_) ->
    UpdResp = #mqtt_sn_message{type = ?SN_WILLMSGRESP, variable = 0},
    ?assertEqual(UpdResp, parse(serialize_pkt(UpdResp))).

t_random_test(_) ->
    random_test_body(),
    random_test_body(),
    random_test_body(),
    random_test_body(),
    random_test_body(),
    random_test_body().

random_test_body() ->
    Data = generate_random_binary(),
    case catch parse(Data) of
        Msg when is_record(Msg, mqtt_sn_message) -> ok;
        {'EXIT', {Err, _Stack}} when
            Err =:= unkown_message_type;
            Err =:= malformed_message_len;
            Err =:= malformed_message_flags
        ->
            ok
    end.

generate_random_binary() ->
    % The min packet length is 2
    Len = rand:uniform(299) + 1,
    gen_next(Len, <<>>).

gen_next(0, Acc) ->
    Acc;
gen_next(N, Acc) ->
    Byte = rand:uniform(256) - 1,
    gen_next(N - 1, <<Acc/binary, Byte:8>>).
