%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_persistent_session_ds_buffer).

%% API:
-export([
    new/1,
    push/2,
    pop/1,
    n_buffered/2,
    n_inflight/1,
    puback/2,
    pubrec/2,
    pubcomp/2,
    receive_maximum/1
]).

%% internal exports:
-export([]).

-export_type([t/0]).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type payload() ::
    {emqx_persistent_session_ds:seqno() | undefined, emqx_types:message()}
    | {pubrel, emqx_persistent_session_ds:seqno()}.

-record(ds_buffer, {
    receive_maximum :: pos_integer(),
    %% Main queue:
    queue :: queue:queue(payload()),
    %% Queues that are used to track sequence numbers of ack tracks:
    puback_queue :: iqueue(),
    pubrec_queue :: iqueue(),
    pubcomp_queue :: iqueue(),
    %% Counters:
    n_inflight = 0 :: non_neg_integer(),
    n_qos0 = 0 :: non_neg_integer(),
    n_qos1 = 0 :: non_neg_integer(),
    n_qos2 = 0 :: non_neg_integer()
}).

-type t() :: #ds_buffer{}.

%%================================================================================
%% API functions
%%================================================================================

-spec new(non_neg_integer()) -> t().
new(ReceiveMaximum) when ReceiveMaximum > 0 ->
    #ds_buffer{
        receive_maximum = ReceiveMaximum,
        queue = queue:new(),
        puback_queue = iqueue_new(),
        pubrec_queue = iqueue_new(),
        pubcomp_queue = iqueue_new()
    }.

-spec receive_maximum(t()) -> pos_integer().
receive_maximum(#ds_buffer{receive_maximum = ReceiveMaximum}) ->
    ReceiveMaximum.

-spec push(payload(), t()) -> t().
push(Payload = {pubrel, _SeqNo}, Rec = #ds_buffer{queue = Q}) ->
    Rec#ds_buffer{queue = queue:in(Payload, Q)};
push(Payload = {_, Msg}, Rec) ->
    #ds_buffer{queue = Q0, n_qos0 = NQos0, n_qos1 = NQos1, n_qos2 = NQos2} = Rec,
    Q = queue:in(Payload, Q0),
    case Msg#message.qos of
        ?QOS_0 ->
            Rec#ds_buffer{queue = Q, n_qos0 = NQos0 + 1};
        ?QOS_1 ->
            Rec#ds_buffer{queue = Q, n_qos1 = NQos1 + 1};
        ?QOS_2 ->
            Rec#ds_buffer{queue = Q, n_qos2 = NQos2 + 1}
    end.

-spec pop(t()) -> {payload(), t()} | undefined.
pop(Rec0) ->
    #ds_buffer{
        receive_maximum = ReceiveMaximum,
        n_inflight = NInflight,
        queue = Q0,
        puback_queue = QAck,
        pubrec_queue = QRec,
        pubcomp_queue = QComp,
        n_qos0 = NQos0,
        n_qos1 = NQos1,
        n_qos2 = NQos2
    } = Rec0,
    case NInflight < ReceiveMaximum andalso queue:out(Q0) of
        {{value, Payload}, Q} ->
            Rec =
                case Payload of
                    {pubrel, _} ->
                        Rec0#ds_buffer{queue = Q};
                    {SeqNo, #message{qos = Qos}} ->
                        case Qos of
                            ?QOS_0 ->
                                Rec0#ds_buffer{queue = Q, n_qos0 = NQos0 - 1};
                            ?QOS_1 ->
                                Rec0#ds_buffer{
                                    queue = Q,
                                    n_qos1 = NQos1 - 1,
                                    n_inflight = NInflight + 1,
                                    puback_queue = ipush(SeqNo, QAck)
                                };
                            ?QOS_2 ->
                                Rec0#ds_buffer{
                                    queue = Q,
                                    n_qos2 = NQos2 - 1,
                                    n_inflight = NInflight + 1,
                                    pubrec_queue = ipush(SeqNo, QRec),
                                    pubcomp_queue = ipush(SeqNo, QComp)
                                }
                        end
                end,
            {Payload, Rec};
        _ ->
            undefined
    end.

-spec n_buffered(?QOS_0..?QOS_2 | all, t()) -> non_neg_integer().
n_buffered(?QOS_0, #ds_buffer{n_qos0 = NQos0}) ->
    NQos0;
n_buffered(?QOS_1, #ds_buffer{n_qos1 = NQos1}) ->
    NQos1;
n_buffered(?QOS_2, #ds_buffer{n_qos2 = NQos2}) ->
    NQos2;
n_buffered(all, #ds_buffer{n_qos0 = NQos0, n_qos1 = NQos1, n_qos2 = NQos2}) ->
    NQos0 + NQos1 + NQos2.

-spec n_inflight(t()) -> non_neg_integer().
n_inflight(#ds_buffer{n_inflight = NInflight}) ->
    NInflight.

-spec puback(emqx_persistent_session_ds:seqno(), t()) -> {ok, t()} | {error, Expected} when
    Expected :: emqx_persistent_session_ds:seqno() | undefined.
puback(SeqNo, Rec = #ds_buffer{puback_queue = Q0, n_inflight = N}) ->
    case ipop(Q0) of
        {{value, SeqNo}, Q} ->
            {ok, Rec#ds_buffer{
                puback_queue = Q,
                n_inflight = max(0, N - 1)
            }};
        {{value, Expected}, _} ->
            {error, Expected};
        _ ->
            {error, undefined}
    end.

-spec pubcomp(emqx_persistent_session_ds:seqno(), t()) -> {ok, t()} | {error, Expected} when
    Expected :: emqx_persistent_session_ds:seqno() | undefined.
pubcomp(SeqNo, Rec = #ds_buffer{pubcomp_queue = Q0, n_inflight = N}) ->
    case ipop(Q0) of
        {{value, SeqNo}, Q} ->
            {ok, Rec#ds_buffer{
                pubcomp_queue = Q,
                n_inflight = max(0, N - 1)
            }};
        {{value, Expected}, _} ->
            {error, Expected};
        _ ->
            {error, undefined}
    end.

%% PUBREC doesn't affect inflight window:
%% https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Flow_Control
-spec pubrec(emqx_persistent_session_ds:seqno(), t()) -> {ok, t()} | {error, Expected} when
    Expected :: emqx_persistent_session_ds:seqno() | undefined.
pubrec(SeqNo, Rec = #ds_buffer{pubrec_queue = Q0}) ->
    case ipop(Q0) of
        {{value, SeqNo}, Q} ->
            {ok, Rec#ds_buffer{
                pubrec_queue = Q
            }};
        {{value, Expected}, _} ->
            {error, Expected};
        _ ->
            {error, undefined}
    end.

%%================================================================================
%% Internal functions
%%================================================================================

%%%% Interval queue:

%% "Interval queue": a data structure that represents a queue of
%% monotonically increasing non-negative integers in a compact manner.
%% It is functionally equivalent to a `queue:queue(integer())'.
-record(iqueue, {
    %% Head interval:
    head = 0 :: integer(),
    head_end = 0 :: integer(),
    %% Intermediate ranges:
    queue :: queue:queue({integer(), integer()}),
    %% End interval:
    tail = 0 :: integer(),
    tail_end = 0 :: integer()
}).

-type iqueue() :: #iqueue{}.

iqueue_new() ->
    #iqueue{
        queue = queue:new()
    }.

%% @doc Push a value into the interval queue:
-spec ipush(integer(), iqueue()) -> iqueue().
ipush(Val, Q = #iqueue{tail_end = Val, head_end = Val}) ->
    %% Optimization: head and tail intervals overlap, and the newly
    %% inserted value extends both. Attach it to both intervals, to
    %% avoid `queue:out' in `ipop':
    Q#iqueue{
        tail_end = Val + 1,
        head_end = Val + 1
    };
ipush(Val, Q = #iqueue{tail_end = Val}) ->
    %% Extend tail interval:
    Q#iqueue{
        tail_end = Val + 1
    };
ipush(Val, Q = #iqueue{tail = Tl, tail_end = End, queue = IQ0}) when is_number(Val), Val > End ->
    IQ = queue:in({Tl, End}, IQ0),
    %% Begin a new interval:
    Q#iqueue{
        queue = IQ,
        tail = Val,
        tail_end = Val + 1
    }.

-spec ipop(iqueue()) -> {{value, integer()}, iqueue()} | {empty, iqueue()}.
ipop(Q = #iqueue{head = Hd, head_end = HdEnd}) when Hd < HdEnd ->
    %% Head interval is not empty. Consume a value from it:
    {{value, Hd}, Q#iqueue{head = Hd + 1}};
ipop(Q = #iqueue{head_end = End, tail_end = End}) ->
    %% Head interval is fully consumed, and it's overlaps with the
    %% tail interval. It means the queue is empty:
    {empty, Q};
ipop(Q = #iqueue{head = Hd0, tail = Tl, tail_end = TlEnd, queue = IQ0}) ->
    %% Head interval is fully consumed, and it doesn't overlap with
    %% the tail interval. Replace the head interval with the next
    %% interval from the queue or with the tail interval:
    case queue:out(IQ0) of
        {{value, {Hd, HdEnd}}, IQ} ->
            ipop(Q#iqueue{head = max(Hd0, Hd), head_end = HdEnd, queue = IQ});
        {empty, _} ->
            ipop(Q#iqueue{head = max(Hd0, Tl), head_end = TlEnd})
    end.

-ifdef(TEST).

%% Test that behavior of iqueue is identical to that of a regular queue of integers:
iqueue_compat_test_() ->
    Props = [iqueue_compat()],
    Opts = [{numtests, 1000}, {to_file, user}, {max_size, 100}],
    {timeout, 30, [?_assert(proper:quickcheck(Prop, Opts)) || Prop <- Props]}.

%% Generate a sequence of pops and pushes with monotonically
%% increasing arguments, and verify replaying produces equivalent
%% results for the optimized and the reference implementation:
iqueue_compat() ->
    ?FORALL(
        Cmds,
        iqueue_commands(),
        begin
            lists:foldl(
                fun
                    ({push, N}, {IQ, Q, Acc}) ->
                        {ipush(N, IQ), queue:in(N, Q), [N | Acc]};
                    (pop, {IQ0, Q0, Acc}) ->
                        {Ret, IQ} = ipop(IQ0),
                        {Expected, Q} = queue:out(Q0),
                        ?assertEqual(
                            Expected,
                            Ret,
                            #{
                                sequence => lists:reverse(Acc),
                                q => queue:to_list(Q0),
                                iq0 => iqueue_print(IQ0),
                                iq => iqueue_print(IQ)
                            }
                        ),
                        {IQ, Q, [pop | Acc]}
                end,
                {iqueue_new(), queue:new(), []},
                Cmds
            ),
            true
        end
    ).

iqueue_cmd() ->
    oneof([
        pop,
        {push, range(1, 3)}
    ]).

iqueue_commands() ->
    ?LET(
        Cmds,
        list(iqueue_cmd()),
        process_test_cmds(Cmds, 0)
    ).

process_test_cmds([], _) ->
    [];
process_test_cmds([pop | Tl], Cnt) ->
    [pop | process_test_cmds(Tl, Cnt)];
process_test_cmds([{push, N} | Tl], Cnt0) ->
    Cnt = Cnt0 + N,
    [{push, Cnt} | process_test_cmds(Tl, Cnt)].

iqueue_print(#iqueue{head = Hd, head_end = HdEnd, queue = Q, tail = Tl, tail_end = TlEnd}) ->
    #{
        hd => {Hd, HdEnd},
        tl => {Tl, TlEnd},
        q => queue:to_list(Q)
    }.

-endif.
