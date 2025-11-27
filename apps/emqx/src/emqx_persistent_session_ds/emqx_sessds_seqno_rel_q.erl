%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_sessds_seqno_rel_q).
-doc """
""".

%% API:
-export([new/0, push/2, pop/1, pop/3]).

-export_type([elem/1, t/0, t/1]).

-include("emqx_mqtt.hrl").
-include("session_internals.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type elem(Val) :: #on_release_action{
    qos1 :: emqx_persistent_session_ds:seqno() | undefined,
    qos2 :: emqx_persistent_session_ds:seqno() | undefined,
    val :: Val
}.

-record(onrel_q, {
    %% Released seqnos:
    q1_first = -1,
    q2_first = -1,
    %% Seqnos of last inserted actions:
    q1_last = 0,
    q2_last = 0,
    q1 = queue:new(),
    q2 = queue:new()
}).

-opaque t(Val) :: #onrel_q{
    q1_first :: emqx_persistent_session_ds:seqno() | -1,
    q2_first :: emqx_persistent_session_ds:seqno() | -1,
    q1_last :: emqx_persistent_session_ds:seqno(),
    q2_last :: emqx_persistent_session_ds:seqno(),
    q1 :: queue:queue(elem(Val)),
    q2 :: queue:queue(elem(Val))
}.

-type t() :: t(_).

%%================================================================================
%% API functions
%%================================================================================

-spec new() -> t(_).
new() ->
    #onrel_q{}.

-spec push(elem(Val), t(Val)) -> t(Val).
push(
    Elem = #on_release_action{qos1 = SN1, qos2 = SN2},
    Rec0 = #onrel_q{q1_last = Last1, q2_last = Last2, q1 = Q1, q2 = Q2}
) when
    is_integer(SN1) orelse is_integer(SN2),
    SN1 >= Last1,
    SN2 >= Last2
->
    Rec =
        case is_integer(SN1) of
            true ->
                Rec0#onrel_q{q1_last = SN1, q1 = queue:in(Elem, Q1)};
            false ->
                Rec0
        end,
    case is_integer(SN2) of
        true ->
            Rec#onrel_q{q2_last = SN2, q2 = queue:in(Elem, Q2)};
        false ->
            Rec
    end.

-doc """
Pop out all elements that belong to seqnos that are already released.
""".
-spec pop(t(Val)) -> {[Val], t(Val)}.
pop(Rec0 = #onrel_q{q1_first = S1, q2_first = S2}) ->
    {A1, Rec1} = pop(?QOS_1, S1, Rec0),
    {A2, Rec} = pop(?QOS_2, S2, Rec1),
    {A1 ++ A2, Rec}.

-doc """
Release a sequence number.
""".
-spec pop(
    ?QOS_1 | ?QOS_2,
    emqx_persistent_session_ds:seqno(),
    t(Val)
) -> {[Val], t(Val)}.
pop(?QOS_1, SeqNo, Rec = #onrel_q{q1_first = First, q2_first = Other, q1 = Q0}) when
    SeqNo >= First
->
    {Elems, Q} = do_pop(
        #on_release_action.qos1,
        #on_release_action.qos2,
        SeqNo,
        Other,
        Q0,
        []
    ),
    {Elems, Rec#onrel_q{q1_first = SeqNo, q1 = Q}};
pop(?QOS_2, SeqNo, Rec = #onrel_q{q2_first = First, q1_first = Other, q2 = Q0}) when
    SeqNo >= First
->
    {Elems, Q} = do_pop(
        #on_release_action.qos2,
        #on_release_action.qos1,
        SeqNo,
        Other,
        Q0,
        []
    ),
    {Elems, Rec#onrel_q{q2_first = SeqNo, q2 = Q}}.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

do_pop(ThisIdx, OtherIdx, This, Other, Q0, Acc) ->
    case queue:out(Q0) of
        {empty, Q} ->
            %% End of queue:
            {lists:reverse(Acc), Q};
        {{value, Rec = #on_release_action{val = Val}}, Q} ->
            T = element(ThisIdx, Rec),
            O = element(OtherIdx, Rec),
            if
                T > This ->
                    %% Not released yet:
                    {lists:reverse(Acc), Q0};
                is_integer(O) andalso O > Other ->
                    %% Not released by the other queue. Skip
                    %% (releasing seqno on the other track will return
                    %% this item):
                    do_pop(ThisIdx, OtherIdx, This, Other, Q, Acc);
                true ->
                    do_pop(ThisIdx, OtherIdx, This, Other, Q, [Val | Acc])
            end
    end.

-ifdef(TEST).

-define(sc(BODY),
    (fun() ->
        BODY
    end)()
).

simple_test() ->
    Q0 = lists:foldl(
        fun push/2,
        new(),
        [
            #on_release_action{qos1 = 0, val = 1},
            #on_release_action{qos2 = 0, val = 2},
            #on_release_action{qos1 = 2, qos2 = 2, val = 3},
            #on_release_action{qos1 = 3, val = 4},
            #on_release_action{qos2 = 3, val = 5}
        ]
    ),
    {[1], Q1} = pop(?QOS_1, 0, Q0),
    {[], Q2} = pop(?QOS_1, 0, Q1),
    {[4], Q3} = pop(?QOS_1, 10, Q2),
    {[2], Q4} = pop(?QOS_2, 0, Q3),
    {[3, 5], _} = pop(?QOS_2, 10, Q4).

symmetry_test() ->
    Q0 = push(#on_release_action{qos1 = 0, qos2 = 0, val = val}, new()),
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
