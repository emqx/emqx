%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx_sn/include/emqx_sn.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(SHOW(X), ??X).

-import(emqx_sn_frame, [ parse/1
                       , serialize/1
                       ]).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_advertise(_) ->
    Adv = ?SN_ADVERTISE_MSG(1, 100),
    ?assertEqual({ok, Adv}, parse(serialize(Adv))).

t_searchgw(_) ->
    Sgw = #mqtt_sn_message{type = ?SN_SEARCHGW, variable = 1},
    ?assertEqual({ok, Sgw}, parse(serialize(Sgw))).

t_gwinfo(_) ->
    GwInfo = #mqtt_sn_message{type = ?SN_GWINFO, variable = {2, <<"EMQGW">>}},
    ?assertEqual({ok, GwInfo}, parse(serialize(GwInfo))).

t_connect(_) ->
    Flags = #mqtt_sn_flags{will = true, clean_start = true},
    Conn = #mqtt_sn_message{type = ?SN_CONNECT, variable = {Flags, 4, 300, <<"ClientId">>}},
    ?assertEqual({ok, Conn}, parse(serialize(Conn))).

t_connack(_) ->
    ConnAck = #mqtt_sn_message{type = ?SN_CONNACK, variable = 2},
    ?assertEqual({ok, ConnAck}, parse(serialize(ConnAck))).

t_willtopicreq(_) ->
    WtReq = #mqtt_sn_message{type = ?SN_WILLTOPICREQ},
    ?assertEqual({ok, WtReq}, parse(serialize(WtReq))).

t_willtopic(_) ->
    Flags = #mqtt_sn_flags{qos = 1, retain = false},
    Wt = #mqtt_sn_message{type = ?SN_WILLTOPIC, variable = {Flags, <<"WillTopic">>}},
    ?assertEqual({ok, Wt}, parse(serialize(Wt))).

t_undefined_willtopic(_) ->
    Wt = #mqtt_sn_message{type = ?SN_WILLTOPIC},
    ?assertEqual({ok, Wt}, parse(serialize(Wt))).

t_willtopic_resp(_) ->
    Wt = #mqtt_sn_message{type = ?SN_WILLTOPICRESP, variable = 0},
    ?assertEqual({ok, Wt}, parse(serialize(Wt))).

t_willmsgreq(_) ->
    WmReq = #mqtt_sn_message{type = ?SN_WILLMSGREQ},
    ?assertEqual({ok, WmReq}, parse(serialize(WmReq))).

t_willmsg(_) ->
    WlMsg = #mqtt_sn_message{type = ?SN_WILLMSG, variable = <<"WillMsg">>},
    ?assertEqual({ok, WlMsg}, parse(serialize(WlMsg))).

t_register(_) ->
    RegMsg = ?SN_REGISTER_MSG(1, 2, <<"Topic">>),
    ?assertEqual({ok, RegMsg}, parse(serialize(RegMsg))).

t_regack(_) ->
    RegAck = ?SN_REGACK_MSG(1, 2, 0),
    ?assertEqual({ok, RegAck}, parse(serialize(RegAck))).

t_publish(_) ->
    Flags = #mqtt_sn_flags{dup = false, qos = 1, retain = false, topic_id_type = 2#01},
    PubMsg = #mqtt_sn_message{type = ?SN_PUBLISH, variable = {Flags, 1, 2, <<"Payload">>}},
    ?assertEqual({ok, PubMsg}, parse(serialize(PubMsg))).

t_publish_long_msg(_) ->
    Flags = #mqtt_sn_flags{dup = false, qos = 1, retain = false, topic_id_type = 2#01},
    Payload = generate_random_binary(256 + rand:uniform(256)),
    PubMsg = #mqtt_sn_message{type = ?SN_PUBLISH, variable = {Flags, 1, 2, Payload}},
    ?assertEqual({ok, PubMsg}, parse(serialize(PubMsg))).

t_puback(_) ->
    PubAck = #mqtt_sn_message{type = ?SN_PUBACK, variable = {1, 2, 0}},
    ?assertEqual({ok, PubAck}, parse(serialize(PubAck))).

t_pubrec(_) ->
    PubRec =  #mqtt_sn_message{type = ?SN_PUBREC, variable = 16#1234},
    ?assertEqual({ok, PubRec}, parse(serialize(PubRec))).

t_pubrel(_) ->
    PubRel =  #mqtt_sn_message{type = ?SN_PUBREL, variable = 16#1234},
    ?assertEqual({ok, PubRel}, parse(serialize(PubRel))).

t_pubcomp(_) ->
    PubComp =  #mqtt_sn_message{type = ?SN_PUBCOMP, variable = 16#1234},
    ?assertEqual({ok, PubComp}, parse(serialize(PubComp))).

t_subscribe(_) ->
    Flags = #mqtt_sn_flags{dup = false, qos = 1, topic_id_type = ?SN_PREDEFINED_TOPIC},
    SubMsg = #mqtt_sn_message{type = ?SN_SUBSCRIBE, variable = {Flags, 16#4321, 16}},
    ?assertEqual({ok, SubMsg}, parse(serialize(SubMsg))),

    Flags1 = #mqtt_sn_flags{dup = false, qos = 1, topic_id_type = ?SN_NORMAL_TOPIC},
    SubMsg1 = #mqtt_sn_message{type = ?SN_SUBSCRIBE, variable = {Flags1, 16#4321, <<"t/+">>}},
    ?assertEqual({ok, SubMsg1}, parse(serialize(SubMsg1))),

    Flags2 = #mqtt_sn_flags{dup = false, qos = 1, topic_id_type = ?SN_SHORT_TOPIC},
    SubMsg2 = #mqtt_sn_message{type = ?SN_SUBSCRIBE, variable = {Flags2, 16#4321, <<"t/+">>}},
    ?assertEqual({ok, SubMsg2}, parse(serialize(SubMsg2))),

    Flags3 = #mqtt_sn_flags{dup = false, qos = 1, topic_id_type = ?SN_RESERVED_TOPIC},
    SubMsg3 = #mqtt_sn_message{type = ?SN_SUBSCRIBE, variable = {Flags3, 16#4321, <<"t/+">>}},
    ?assertEqual({ok, SubMsg3}, parse(serialize(SubMsg3))).

t_suback(_) ->
    Flags = #mqtt_sn_flags{qos = 1},
    SubAck = #mqtt_sn_message{type = ?SN_SUBACK, variable = {Flags, 98, 89, 0}},
    ?assertEqual({ok, SubAck}, parse(serialize(SubAck))).

t_unsubscribe(_) ->
    Flags = #mqtt_sn_flags{dup = false, qos = 1, topic_id_type = 16#01},
    UnSub = #mqtt_sn_message{type = ?SN_UNSUBSCRIBE, variable = {Flags, 16#4321, 16}},
    ?assertEqual({ok, UnSub}, parse(serialize(UnSub))).

t_unsuback(_) ->
    UnsubAck = #mqtt_sn_message{type = ?SN_UNSUBACK, variable = 72},
    ?assertEqual({ok, UnsubAck}, parse(serialize(UnsubAck))).

t_pingreq(_) ->
    Ping = #mqtt_sn_message{type = ?SN_PINGREQ, variable = <<>>},
    ?assertEqual({ok, Ping}, parse(serialize(Ping))),
    Ping1 = #mqtt_sn_message{type = ?SN_PINGREQ, variable = <<"ClientId">>},
    ?assertEqual({ok, Ping1}, parse(serialize(Ping1))).

t_pingresp(_) ->
    PingResp = #mqtt_sn_message{type = ?SN_PINGRESP},
    ?assertEqual({ok, PingResp}, parse(serialize(PingResp))).

t_disconnect(_) ->
    Disconn = #mqtt_sn_message{type = ?SN_DISCONNECT},
    ?assertEqual({ok, Disconn}, parse(serialize(Disconn))).

t_disconnect_duration(_) ->
    Disconn = #mqtt_sn_message{type = ?SN_DISCONNECT, variable = 120},
    ?assertEqual({ok, Disconn}, parse(serialize(Disconn))).

t_willtopicupd(_) ->
    Flags = #mqtt_sn_flags{qos = 1, retain = true},
    WtUpd = #mqtt_sn_message{type = ?SN_WILLTOPICUPD, variable = {Flags, <<"Topic">>}},
    ?assertEqual({ok, WtUpd}, parse(serialize(WtUpd))).

t_willmsgupd(_) ->
    WlMsgUpd = #mqtt_sn_message{type = ?SN_WILLMSGUPD, variable = <<"WillMsg">>},
    ?assertEqual({ok, WlMsgUpd}, parse(serialize(WlMsgUpd))).

t_willmsgresp(_) ->
    UpdResp = #mqtt_sn_message{type = ?SN_WILLMSGRESP, variable = 0},
    ?assertEqual({ok, UpdResp}, parse(serialize(UpdResp))).

t_invalid_inpacket(_) ->
    Bin = <<2:8/big-integer, 16#F0:8/big-integer>>,
    ?assertMatch({'EXIT', {unkown_message_type, _Stack}}, catch parse(Bin)).

t_message_type(_) ->
    TypeNames = [ {?SN_ADVERTISE, ?SHOW(SN_ADVERTISE)}
                , {?SN_SEARCHGW, ?SHOW(SN_SEARCHGW)}
                , {?SN_GWINFO, ?SHOW(SN_GWINFO)}
                , {?SN_CONNECT, ?SHOW(SN_CONNECT)}
                , {?SN_CONNACK, ?SHOW(SN_CONNACK)}
                , {?SN_WILLTOPICREQ, ?SHOW(SN_WILLTOPICREQ)}
                , {?SN_WILLTOPIC, ?SHOW(SN_WILLTOPIC)}
                , {?SN_WILLMSGREQ, ?SHOW(SN_WILLMSGREQ)}
                , {?SN_WILLMSG, ?SHOW(SN_WILLMSG)}
                , {?SN_REGISTER, ?SHOW(SN_REGISTER)}
                , {?SN_REGACK, ?SHOW(SN_REGACK)}
                , {?SN_PUBLISH, ?SHOW(SN_PUBLISH)}
                , {?SN_PUBACK, ?SHOW(SN_PUBACK)}
                , {?SN_PUBCOMP, ?SHOW(SN_PUBCOMP)}
                , {?SN_PUBREC, ?SHOW(SN_PUBREC)}
                , {?SN_PUBREL, ?SHOW(SN_PUBREL)}
                , {?SN_SUBSCRIBE, ?SHOW(SN_SUBSCRIBE)}
                , {?SN_SUBACK, ?SHOW(SN_SUBACK)}
                , {?SN_UNSUBSCRIBE, ?SHOW(SN_UNSUBSCRIBE)}
                , {?SN_UNSUBACK, ?SHOW(SN_UNSUBACK)}
                , {?SN_PINGREQ, ?SHOW(SN_PINGREQ)}
                , {?SN_PINGRESP, ?SHOW(SN_PINGRESP)}
                , {?SN_DISCONNECT, ?SHOW(SN_DISCONNECT)}
                , {?SN_WILLTOPICUPD, ?SHOW(SN_WILLTOPICUPD)}
                , {?SN_WILLTOPICRESP, ?SHOW(SN_WILLTOPICRESP)}
                , {?SN_WILLMSGUPD, ?SHOW(SN_WILLMSGUPD)}
                , {?SN_WILLMSGRESP, ?SHOW(SN_WILLMSGRESP)}
                ],
    {Types, Names} = lists:unzip(TypeNames),
    ?assertEqual(Names, [emqx_sn_frame:message_type(Type) || Type <- Types]),
    ok.

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
        {ok, _Msg} -> ok;
        {'EXIT', {Err, _Stack}}
          when Err =:= unkown_message_type;
               Err =:= malformed_message_len;
               Err =:= malformed_message_flags -> ok
    end.

generate_random_binary() ->
    % The min packet length is 2
    Len = rand:uniform(299) + 1,
    generate_random_binary(Len).

generate_random_binary(Len) ->
    gen_next(Len, <<>>).

gen_next(0, Acc) ->
    Acc;
gen_next(N, Acc) ->
    Byte = rand:uniform(256) - 1,
    gen_next(N-1, <<Acc/binary, Byte:8>>).
