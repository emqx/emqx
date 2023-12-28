%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_persistent_session_ds_inflight).

%% API:
-export([new/1, push/2, pop/1, n_buffered/1, n_inflight/1, inc_send_quota/1, receive_maximum/1]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([t/0]).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(inflight, {
    queue :: queue:queue(),
    receive_maximum :: pos_integer(),
    n_inflight = 0 :: non_neg_integer(),
    n_qos0 = 0 :: non_neg_integer(),
    n_qos1 = 0 :: non_neg_integer(),
    n_qos2 = 0 :: non_neg_integer()
}).

-type t() :: #inflight{}.

%%================================================================================
%% API funcions
%%================================================================================

-spec new(non_neg_integer()) -> t().
new(ReceiveMaximum) when ReceiveMaximum > 0 ->
    #inflight{queue = queue:new(), receive_maximum = ReceiveMaximum}.

-spec receive_maximum(t()) -> pos_integer().
receive_maximum(#inflight{receive_maximum = ReceiveMaximum}) ->
    ReceiveMaximum.

-spec push({emqx_types:packet_id() | undefined, emqx_types:message()}, t()) -> t().
push(Val = {_PacketId, Msg}, Rec) ->
    #inflight{queue = Q0, n_qos0 = NQos0, n_qos1 = NQos1, n_qos2 = NQos2} = Rec,
    Q = queue:in(Val, Q0),
    case Msg#message.qos of
        ?QOS_0 ->
            Rec#inflight{queue = Q, n_qos0 = NQos0 + 1};
        ?QOS_1 ->
            Rec#inflight{queue = Q, n_qos1 = NQos1 + 1};
        ?QOS_2 ->
            Rec#inflight{queue = Q, n_qos2 = NQos2 + 1}
    end.

-spec pop(t()) -> {[{emqx_types:packet_id() | undefined, emqx_types:message()}], t()}.
pop(Inflight = #inflight{receive_maximum = ReceiveMaximum}) ->
    do_pop(ReceiveMaximum, Inflight, []).

-spec n_buffered(t()) -> non_neg_integer().
n_buffered(#inflight{n_qos0 = NQos0, n_qos1 = NQos1, n_qos2 = NQos2}) ->
    NQos0 + NQos1 + NQos2.

-spec n_inflight(t()) -> non_neg_integer().
n_inflight(#inflight{n_inflight = NInflight}) ->
    NInflight.

%% https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Flow_Control
-spec inc_send_quota(t()) -> {non_neg_integer(), t()}.
inc_send_quota(Rec = #inflight{n_inflight = NInflight0}) ->
    NInflight = max(NInflight0 - 1, 0),
    {NInflight, Rec#inflight{n_inflight = NInflight}}.

%%================================================================================
%% Internal functions
%%================================================================================

do_pop(ReceiveMaximum, Rec0 = #inflight{n_inflight = NInflight, queue = Q0}, Acc) ->
    case NInflight < ReceiveMaximum andalso queue:out(Q0) of
        {{value, Val}, Q} ->
            #inflight{n_qos0 = NQos0, n_qos1 = NQos1, n_qos2 = NQos2} = Rec0,
            {_PacketId, #message{qos = Qos}} = Val,
            Rec =
                case Qos of
                    ?QOS_0 ->
                        Rec0#inflight{queue = Q, n_qos0 = NQos0 - 1};
                    ?QOS_1 ->
                        Rec0#inflight{queue = Q, n_qos1 = NQos1 - 1, n_inflight = NInflight + 1};
                    ?QOS_2 ->
                        Rec0#inflight{queue = Q, n_qos2 = NQos2 - 1, n_inflight = NInflight + 1}
                end,
            do_pop(ReceiveMaximum, Rec, [Val | Acc]);
        _ ->
            {lists:reverse(Acc), Rec0}
    end.
