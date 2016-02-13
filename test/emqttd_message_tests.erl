%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_message_tests).

-ifdef(TEST).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(M, emqttd_message).

make_test() ->
    Msg = ?M:make(<<"clientid">>, <<"topic">>, <<"payload">>),
    ?assertEqual(0, Msg#mqtt_message.qos),
    ?assertEqual(undefined, Msg#mqtt_message.msgid),
    Msg1 = ?M:make(<<"clientid">>, qos2, <<"topic">>, <<"payload">>),
    ?assert(is_binary(Msg1#mqtt_message.msgid)),
    ?assertEqual(2, Msg1#mqtt_message.qos).

from_packet_test() ->
    Msg = ?M:from_packet(?PUBLISH_PACKET(1, <<"topic">>, 10, <<"payload">>)),
    ?assertEqual(1, Msg#mqtt_message.qos),
    ?assertEqual(10, Msg#mqtt_message.pktid),
    ?assertEqual(<<"topic">>, Msg#mqtt_message.topic),

    WillMsg = ?M:from_packet(#mqtt_packet_connect{will_flag  = true,
                                                  will_topic = <<"WillTopic">>,
                                                  will_msg   = <<"WillMsg">>}),
    ?assertEqual(<<"WillTopic">>, WillMsg#mqtt_message.topic),
    ?assertEqual(<<"WillMsg">>, WillMsg#mqtt_message.payload),

    Msg2 = ?M:from_packet(<<"username">>, <<"clientid">>,
                          ?PUBLISH_PACKET(1, <<"topic">>, 20, <<"payload">>)),
    ?assertEqual(<<"clientid">>, Msg2#mqtt_message.from),
    ?assertEqual(<<"username">>, Msg2#mqtt_message.sender),
    ?debugFmt("~s", [?M:format(Msg2)]).

flag_test() ->
    Pkt = ?PUBLISH_PACKET(1, <<"t">>, 2, <<"payload">>),
    Msg2 = ?M:from_packet(<<"clientid">>, Pkt),
    Msg3 = ?M:set_flag(retain, Msg2),
    Msg4 = ?M:set_flag(dup, Msg3),
    ?assert(Msg4#mqtt_message.dup),
    ?assert(Msg4#mqtt_message.retain),
    Msg5 = ?M:set_flag(Msg4),
    Msg6 = ?M:unset_flag(dup, Msg5),
    Msg7 = ?M:unset_flag(retain, Msg6),
    ?assertNot(Msg7#mqtt_message.dup),
    ?assertNot(Msg7#mqtt_message.retain),
    ?M:unset_flag(Msg7),
    ?M:to_packet(Msg7).

-endif.
