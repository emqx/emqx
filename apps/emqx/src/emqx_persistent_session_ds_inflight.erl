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
-export([new/1, push/2, pop/1, n_buffered/2, n_inflight/1, inc_send_quota/1, receive_maximum/1]).

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

-type payload() ::
    {emqx_persistent_session_ds:seqno() | undefined, emqx_types:message()}
    | {pubrel, emqx_persistent_session_ds:seqno()}.

%%================================================================================
%% API funcions
%%================================================================================

-spec new(non_neg_integer()) -> t().
new(ReceiveMaximum) when ReceiveMaximum > 0 ->
    #inflight{queue = queue:new(), receive_maximum = ReceiveMaximum}.

-spec receive_maximum(t()) -> pos_integer().
receive_maximum(#inflight{receive_maximum = ReceiveMaximum}) ->
    ReceiveMaximum.

-spec push(payload(), t()) -> t().
push(Payload = {pubrel, _SeqNo}, Rec = #inflight{queue = Q}) ->
    Rec#inflight{queue = queue:in(Payload, Q)};
push(Payload = {_, Msg}, Rec) ->
    #inflight{queue = Q0, n_qos0 = NQos0, n_qos1 = NQos1, n_qos2 = NQos2} = Rec,
    Q = queue:in(Payload, Q0),
    case Msg#message.qos of
        ?QOS_0 ->
            Rec#inflight{queue = Q, n_qos0 = NQos0 + 1};
        ?QOS_1 ->
            Rec#inflight{queue = Q, n_qos1 = NQos1 + 1};
        ?QOS_2 ->
            Rec#inflight{queue = Q, n_qos2 = NQos2 + 1}
    end.

-spec pop(t()) -> {payload(), t()} | undefined.
pop(Rec0) ->
    #inflight{
        receive_maximum = ReceiveMaximum,
        n_inflight = NInflight,
        queue = Q0,
        n_qos0 = NQos0,
        n_qos1 = NQos1,
        n_qos2 = NQos2
    } = Rec0,
    case NInflight < ReceiveMaximum andalso queue:out(Q0) of
        {{value, Payload}, Q} ->
            Rec =
                case Payload of
                    {pubrel, _} ->
                        Rec0#inflight{queue = Q};
                    {_, #message{qos = Qos}} ->
                        case Qos of
                            ?QOS_0 ->
                                Rec0#inflight{queue = Q, n_qos0 = NQos0 - 1};
                            ?QOS_1 ->
                                Rec0#inflight{
                                    queue = Q, n_qos1 = NQos1 - 1, n_inflight = NInflight + 1
                                };
                            ?QOS_2 ->
                                Rec0#inflight{
                                    queue = Q, n_qos2 = NQos2 - 1, n_inflight = NInflight + 1
                                }
                        end
                end,
            {Payload, Rec};
        _ ->
            undefined
    end.

-spec n_buffered(0..2 | all, t()) -> non_neg_integer().
n_buffered(?QOS_0, #inflight{n_qos0 = NQos0}) ->
    NQos0;
n_buffered(?QOS_1, #inflight{n_qos1 = NQos1}) ->
    NQos1;
n_buffered(?QOS_2, #inflight{n_qos2 = NQos2}) ->
    NQos2;
n_buffered(all, #inflight{n_qos0 = NQos0, n_qos1 = NQos1, n_qos2 = NQos2}) ->
    NQos0 + NQos1 + NQos2.

-spec n_inflight(t()) -> non_neg_integer().
n_inflight(#inflight{n_inflight = NInflight}) ->
    NInflight.

%% https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Flow_Control
-spec inc_send_quota(t()) -> t().
inc_send_quota(Rec = #inflight{n_inflight = NInflight0}) ->
    NInflight = max(NInflight0 - 1, 0),
    Rec#inflight{n_inflight = NInflight}.

%%================================================================================
%% Internal functions
%%================================================================================
