%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_persistent_session_ds_inflight).

%% API:
-export([
    new/1,
    push/2,
    pop/1,
    n_buffered/2,
    n_inflight/1,
    oldest_qos2/1,
    puback/2,
    pubrec/2,
    pubcomp/2,
    reject/2,
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
    | {pubrel, emqx_persistent_session_ds:seqno()}
    | {other, term()}.

-record(ds_inflight, {
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

-type t() :: #ds_inflight{}.

%%================================================================================
%% API functions
%%================================================================================

-spec new(non_neg_integer()) -> t().
new(ReceiveMaximum) when ReceiveMaximum > 0 ->
    #ds_inflight{
        receive_maximum = ReceiveMaximum,
        queue = queue:new(),
        puback_queue = iqueue_new(),
        pubrec_queue = iqueue_new(),
        pubcomp_queue = iqueue_new()
    }.

-spec receive_maximum(t()) -> pos_integer().
receive_maximum(#ds_inflight{receive_maximum = ReceiveMaximum}) ->
    ReceiveMaximum.

-spec push(payload(), t()) -> t().
push(Payload = {other, _}, Rec = #ds_inflight{queue = Q}) ->
    Rec#ds_inflight{queue = queue:in(Payload, Q)};
push(Payload = {pubrel, _SeqNo}, Rec = #ds_inflight{queue = Q}) ->
    Rec#ds_inflight{queue = queue:in(Payload, Q)};
push(Payload = {_, Msg}, Rec) ->
    #ds_inflight{queue = Q0, n_qos0 = NQos0, n_qos1 = NQos1, n_qos2 = NQos2} = Rec,
    Q = queue:in(Payload, Q0),
    case Msg#message.qos of
        ?QOS_0 ->
            Rec#ds_inflight{queue = Q, n_qos0 = NQos0 + 1};
        ?QOS_1 ->
            Rec#ds_inflight{queue = Q, n_qos1 = NQos1 + 1};
        ?QOS_2 ->
            Rec#ds_inflight{queue = Q, n_qos2 = NQos2 + 1}
    end.

-spec pop(t()) -> {payload(), t()} | undefined.
pop(Rec0) ->
    #ds_inflight{
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
                    {other, _} ->
                        Rec0#ds_inflight{
                            queue = Q
                        };
                    {pubrel, SeqNo} ->
                        Rec0#ds_inflight{
                            queue = Q,
                            pubcomp_queue = ipush(SeqNo, QComp)
                        };
                    {SeqNo, #message{qos = Qos}} ->
                        case Qos of
                            ?QOS_0 ->
                                Rec0#ds_inflight{queue = Q, n_qos0 = NQos0 - 1};
                            ?QOS_1 ->
                                Rec0#ds_inflight{
                                    queue = Q,
                                    n_qos1 = NQos1 - 1,
                                    n_inflight = NInflight + 1,
                                    puback_queue = ipush(SeqNo, QAck)
                                };
                            ?QOS_2 ->
                                Rec0#ds_inflight{
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
n_buffered(?QOS_0, #ds_inflight{n_qos0 = NQos0}) ->
    NQos0;
n_buffered(?QOS_1, #ds_inflight{n_qos1 = NQos1}) ->
    NQos1;
n_buffered(?QOS_2, #ds_inflight{n_qos2 = NQos2}) ->
    NQos2;
n_buffered(all, #ds_inflight{n_qos0 = NQos0, n_qos1 = NQos1, n_qos2 = NQos2}) ->
    NQos0 + NQos1 + NQos2.

-spec n_inflight(t()) -> non_neg_integer().
n_inflight(#ds_inflight{n_inflight = NInflight}) ->
    NInflight.

-doc "Return the oldest queued or sent QoS 2 exchange, including replayed PUBRELs.".
-spec oldest_qos2(t()) -> emqx_persistent_session_ds:seqno() | undefined.
oldest_qos2(#ds_inflight{queue = Q, pubcomp_queue = QComp}) ->
    Oldest =
        case ipop(QComp) of
            {{value, SeqNo}, _} -> SeqNo;
            {empty, _} -> undefined
        end,
    %% 'undefined' sorts after every sequence number.
    queue:fold(
        fun
            ({pubrel, SeqNo}, Acc) -> min(SeqNo, Acc);
            ({SeqNo, #message{qos = ?QOS_2}}, Acc) -> min(SeqNo, Acc);
            (_, Acc) -> Acc
        end,
        Oldest,
        Q
    ).

-spec puback(emqx_persistent_session_ds:seqno(), t()) -> {ok, t()} | {error, Expected} when
    Expected :: emqx_persistent_session_ds:seqno() | undefined.
puback(SeqNo, Rec = #ds_inflight{puback_queue = Q0, n_inflight = N}) ->
    case ipop(Q0) of
        {{value, SeqNo}, Q} ->
            {ok, Rec#ds_inflight{
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
pubcomp(SeqNo, Rec = #ds_inflight{pubcomp_queue = Q0, n_inflight = N}) ->
    case ipop(Q0) of
        {{value, SeqNo}, Q} ->
            {ok, Rec#ds_inflight{
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
pubrec(SeqNo, Rec = #ds_inflight{pubrec_queue = Q0}) ->
    case ipop(Q0) of
        {{value, SeqNo}, Q} ->
            {ok, Rec#ds_inflight{
                pubrec_queue = Q
            }};
        {{value, Expected}, _} ->
            {error, Expected};
        _ ->
            {error, undefined}
    end.

%% Called only after pubrec/2 accepted this sequence number. Unlike PUBCOMP,
%% a negative PUBREC can complete a message behind an earlier pending exchange.
-spec reject(emqx_persistent_session_ds:seqno(), t()) -> t().
reject(SeqNo, Rec = #ds_inflight{pubcomp_queue = Q0, n_inflight = N}) ->
    {ok, Q} = idelete(SeqNo, Q0),
    Rec#ds_inflight{pubcomp_queue = Q, n_inflight = max(0, N - 1)}.

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
    %% Head interval is fully consumed, and it overlaps with the tail
    %% interval. It means the queue is empty:
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

%% Delete from compressed intervals, without expanding them into individual
%% sequence numbers. The common case consists of a single interval.
idelete(Val, Q) ->
    case idelete_interval(Val, iintervals(Q)) of
        {ok, Intervals} -> {ok, ifrom_intervals(Intervals)};
        error -> error
    end.

iintervals(Q0) ->
    case ipop(Q0) of
        {{value, First}, Q = #iqueue{head_end = End}} ->
            [{First, End} | iintervals(Q#iqueue{head = End})];
        {empty, _} ->
            []
    end.

idelete_interval(Val, [{First, End} | Rest]) when First =< Val, Val < End ->
    Before = [{First, Val} || First < Val],
    After = [{Val + 1, End} || Val + 1 < End],
    {ok, Before ++ After ++ Rest};
idelete_interval(Val, [Interval | Rest]) ->
    case idelete_interval(Val, Rest) of
        {ok, Intervals} -> {ok, [Interval | Intervals]};
        error -> error
    end;
idelete_interval(_Val, []) ->
    error.

ifrom_intervals([]) ->
    iqueue_new();
ifrom_intervals([{First, End} | Rest]) ->
    {Tail, TailEnd} = lists:last([{First, End} | Rest]),
    Middle =
        case Rest of
            [] -> [];
            _ -> lists:droplast(Rest)
        end,
    #iqueue{
        head = First,
        head_end = End,
        queue = queue:from_list(Middle),
        tail = Tail,
        tail_end = TailEnd
    }.

-ifdef(TEST).

reject_replayed_pubrel_quota_test() ->
    Q0 = new(1),
    {{pubrel, 1}, Q1} = pop(push({pubrel, 1}, Q0)),
    {{pubrel, 2}, Q2} = pop(push({pubrel, 2}, Q1)),
    Msg = emqx_message:make(<<"publisher">>, ?QOS_2, <<"t">>, <<>>),
    {{3, Msg}, Q3} = pop(push({3, Msg}, Q2)),
    {ok, Q4} = pubcomp(1, Q3),
    ?assertEqual(0, n_inflight(Q4)),
    {ok, Q5} = pubrec(3, Q4),
    ?assertEqual({error, 2}, pubcomp(3, Q5)),
    Q6 = reject(3, Q5),
    ?assertEqual(0, n_inflight(Q6)),
    Q7 = push({5, Msg}, push({4, Msg}, Q6)),
    {{4, Msg}, Q8} = pop(Q7),
    ?assertEqual(undefined, pop(Q8)),
    {ok, Q9} = pubcomp(2, Q8),
    ?assertMatch({{5, Msg}, _}, pop(Q9)).

%% Test that behavior of iqueue is identical to that of a regular queue of integers:
iqueue_compat_test_() ->
    Props = [iqueue_compat()],
    Opts = [{numtests, 1000}, {to_file, user}, {max_size, 100}],
    {timeout, 30, [?_assert(proper:quickcheck(Prop, Opts)) || Prop <- Props]}.

%% Generate pushes with increasing arguments, pops, and arbitrary deletions.
%% Verify equivalent results for the optimized and reference implementations:
iqueue_compat() ->
    ?FORALL(
        Cmds,
        iqueue_commands(),
        begin
            lists:foldl(
                fun
                    ({push, N}, {IQ, Q, Acc}) ->
                        {ipush(N, IQ), queue:in(N, Q), [N | Acc]};
                    ({delete, N}, {IQ0, Q0, Acc}) ->
                        Items = queue:to_list(Q0),
                        case lists:member(N, Items) of
                            true ->
                                {ok, IQ} = idelete(N, IQ0),
                                Q = queue:from_list(lists:delete(N, Items)),
                                ?assertEqual(
                                    queue:to_list(Q),
                                    lists:append([lists:seq(A, B - 1) || {A, B} <- iintervals(IQ)])
                                ),
                                {IQ, Q, [{delete, N} | Acc]};
                            false ->
                                ?assertEqual(error, idelete(N, IQ0)),
                                {IQ0, Q0, [{delete, N} | Acc]}
                        end;
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
        {delete, range(0, 20)},
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
process_test_cmds([{delete, Offset} | Tl], Cnt) ->
    [{delete, max(0, Cnt - Offset)} | process_test_cmds(Tl, Cnt)];
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
