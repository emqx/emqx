%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_sessds_seqno_rel_q).
-doc """
This module implements a task queue where tasks are blocked by
un-acked sequence numbers. Each action can be blocked by sequence
numbers on two separate tracks.
""".

%% API:
-export([new/2, push/4, pop/1, pop/3]).

-export_type([elem/1, t/0, t/1]).

-include("emqx_mqtt.hrl").

-include_lib("stdlib/include/assert.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type elem(Val) :: {
    _SeqNoQos1 :: emqx_persistent_session_ds:seqno() | undefined,
    _SeqNoQoS2 :: emqx_persistent_session_ds:seqno() | undefined,
    Val
}.

-record(onrel_q, {
    %% Released seqnos:
    q1_rel = -1,
    q2_rel = -1,
    %% Seqnos of the last inserted actions:
    q1_last = 0,
    q2_last = 0,
    %% Queues of actions blocked by PUBACK and PUBCOMP respectively:
    q1 = queue:new(),
    q2 = queue:new(),
    %% "Queue" for immediate release:
    qnow = []
}).

-opaque t(Val) :: #onrel_q{
    q1_rel :: emqx_persistent_session_ds:seqno() | -1,
    q2_rel :: emqx_persistent_session_ds:seqno() | -1,
    q1_last :: emqx_persistent_session_ds:seqno(),
    q2_last :: emqx_persistent_session_ds:seqno(),
    q1 :: queue:queue(elem(Val)),
    q2 :: queue:queue(elem(Val)),
    qnow :: [Val]
}.

-type t() :: t(_).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Create a new queue.

Argements: released sequence numbers for QoS1 and QoS2 tracks
respectively.
""".
-spec new(emqx_persistent_session_ds:seqno(), emqx_persistent_session_ds:seqno()) -> t(_).
new(QoS1Released, QoS2Released) ->
    #onrel_q{
        q1_rel = QoS1Released,
        q2_rel = QoS2Released
    }.

-doc """
Add an action `Val` to the queue, blocked by `SN1` and `SN2` on QoS1
and QoS2 tracks respectively.

Note: both sequence numbers must be greater than or equal to the
sequence number of the previous action added by the track.
""".
-spec push(SeqNo, SeqNo, Val, t(Val)) -> t(Val) when
    SeqNo :: emqx_persistent_session_ds:seqno() | undefined.
push(
    SN1,
    SN2,
    Val,
    Rec0 = #onrel_q{
        q1_rel = First1,
        q1_last = Last1,
        q2_rel = First2,
        q2_last = Last2,
        q1 = Q1,
        q2 = Q2,
        qnow = QNow
    }
) ->
    IsBlocked1 = is_integer(SN1) andalso SN1 > First1,
    IsBlocked2 = is_integer(SN2) andalso SN2 > First2,
    if
        IsBlocked1 andalso IsBlocked2 ->
            ?assert(SN1 >= Last1, {SN1, '>=', Last1}),
            ?assert(SN2 >= Last2, {SN2, '>=', Last2}),
            Elem = {SN1, SN2, Val},
            Rec0#onrel_q{
                q1_last = SN1,
                q2_last = SN2,
                q1 = queue:in(Elem, Q1),
                q2 = queue:in(Elem, Q2)
            };
        IsBlocked1 ->
            ?assert(SN1 >= Last1, {SN1, '>=', Last1}),
            Elem = {SN1, SN2, Val},
            Rec0#onrel_q{
                q1_last = SN1,
                q1 = queue:in(Elem, Q1)
            };
        IsBlocked2 ->
            ?assert(SN2 >= Last2, {SN2, '>=', Last2}),
            Elem = {SN1, SN2, Val},
            Rec0#onrel_q{
                q2_last = SN2,
                q2 = queue:in(Elem, Q2)
            };
        true ->
            Rec0#onrel_q{
                qnow = [Val | QNow]
            }
    end.

-doc """
Pop out all elements that belong to seqnos that are already released
without updating the released sequence numbers.
""".
-spec pop(t(Val)) -> {[Val], t(Val)}.
pop(Rec0 = #onrel_q{q1_rel = S1, q2_rel = S2}) ->
    {Acc, Rec1} = pop(?QOS_1, S1, Rec0, []),
    pop(?QOS_2, S2, Rec1, Acc).

-doc """
Release a sequence number.
""".
-spec pop(
    ?QOS_1 | ?QOS_2,
    emqx_persistent_session_ds:seqno(),
    t(Val)
) -> {[Val], t(Val)}.
pop(QoS, SeqNo, Rec) ->
    pop(QoS, SeqNo, Rec, []).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

-spec pop(
    ?QOS_1 | ?QOS_2,
    emqx_persistent_session_ds:seqno(),
    t(Val),
    [Val]
) -> {[Val], t(Val)}.
pop(
    ?QOS_1, SeqNo, Rec = #onrel_q{q1_rel = First, q2_rel = Other, q1 = Q0, qnow = QNow}, Acc
) ->
    {Elems, Q} = do_pop(
        ?QOS_1,
        SeqNo,
        Other,
        Q0,
        QNow ++ Acc
    ),
    {Elems, Rec#onrel_q{q1_rel = max(First, SeqNo), q1 = Q, qnow = []}};
pop(
    ?QOS_2, SeqNo, Rec = #onrel_q{q2_rel = First, q1_rel = Other, q2 = Q0, qnow = QNow}, Acc
) ->
    {Elems, Q} = do_pop(
        ?QOS_2,
        SeqNo,
        Other,
        Q0,
        QNow ++ Acc
    ),
    {Elems, Rec#onrel_q{q2_rel = max(First, SeqNo), q2 = Q, qnow = []}}.

do_pop(QoS, ThisRel, OtherRel, Q0, Acc) ->
    case queue:out(Q0) of
        {empty, Q} ->
            %% End of queue:
            {lists:reverse(Acc), Q};
        {{value, {SN1, SN2, Val}}, Q} ->
            case QoS of
                ?QOS_1 ->
                    This = SN1,
                    Other = SN2;
                ?QOS_2 ->
                    This = SN2,
                    Other = SN1
            end,
            if
                This > ThisRel ->
                    %% Not released yet:
                    {lists:reverse(Acc), Q0};
                is_integer(Other) andalso Other > OtherRel ->
                    %% Not released by the other queue. Skip
                    %% (releasing seqno on the other track will return
                    %% this item):
                    do_pop(QoS, ThisRel, OtherRel, Q, Acc);
                true ->
                    do_pop(QoS, ThisRel, OtherRel, Q, [Val | Acc])
            end
    end.

-ifdef(TEST).

%% Delimit a scope
-define(sc(BODY),
    (fun() ->
        BODY
    end)()
).

simple_test() ->
    Q0 = lists:foldl(
        fun({SN1, SN2, Val}, Acc) ->
            push(SN1, SN2, Val, Acc)
        end,
        new(-1, -1),
        [
            {0, undefined, 1},
            {undefined, 0, 2},
            {2, 2, 3},
            {3, undefined, 4},
            {undefined, 3, 5}
        ]
    ),
    {[1], Q1} = pop(?QOS_1, 0, Q0),
    {[], Q2} = pop(?QOS_1, 0, Q1),
    {[4], Q3} = pop(?QOS_1, 10, Q2),
    {[2], Q4} = pop(?QOS_2, 0, Q3),
    {[3, 5], _} = pop(?QOS_2, 10, Q4).

symmetry_test() ->
    Q0 = push(0, 0, val, new(-1, -1)),
    ?sc(
        begin
            {[], Q1} = pop(?QOS_1, 0, Q0),
            {[val], Q2} = pop(?QOS_2, 0, Q1),
            {[], _} = pop(?QOS_1, 1, Q2),
            {[], _} = pop(?QOS_2, 1, Q2)
        end
    ),
    ?sc(
        begin
            {[], Q1} = pop(?QOS_2, 0, Q0),
            {[val], Q2} = pop(?QOS_1, 0, Q1),
            {[], _} = pop(?QOS_1, 1, Q2),
            {[], _} = pop(?QOS_2, 1, Q2)
        end
    ).

-endif.
