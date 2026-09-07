%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_tm_tests).

-include("../include/emqx_coap.hrl").
-include_lib("eunit/include/eunit.hrl").

late_empty_ack_does_not_match_new_same_token_transaction_test() ->
    Token = <<"observe-token">>,
    Notification = #coap_message{
        type = con,
        method = {ok, content},
        token = Token,
        options = #{observe => 1}
    },
    TM0 = emqx_coap_tm:new(),
    #{out := [First], tm := TM1} = emqx_coap_tm:handle_out(Notification, TM0),
    SeqId = maps:get({token, Token}, TM1),

    #{tm := TM2, observe_notification_done := 1} =
        emqx_coap_tm:timeout({SeqId, stop_timeout, stop}, TM1),

    #{out := [Second], tm := TM3} = emqx_coap_tm:handle_out(Notification, TM2),
    LateAck = #coap_message{
        type = ack,
        id = First#coap_message.id,
        token = Token
    },
    ?assertEqual(#{}, emqx_coap_tm:handle_response(LateAck, TM3)),

    CurrentAck = #coap_message{
        type = ack,
        id = Second#coap_message.id,
        token = Token
    },
    ?assertMatch(
        #{observe_notification_done := 1},
        emqx_coap_tm:handle_response(CurrentAck, TM3)
    ).
