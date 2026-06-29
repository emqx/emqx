%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

%% Priority queues have essentially the same interface as ordinary
%% queues, except that a) there is an in/3 that takes a priority, and
%% b) we have only implemented the core API we need.
%%
%% Priorities should be integers - the higher the value the higher the
%% priority - but we don't actually check that.
%%
%% in/2 inserts items with priority 0.
%%
%% We optimise the case where a priority queue is being used just like
%% an ordinary queue. When that is the case we represent the priority
%% queue as an ordinary queue. We could just call into the 'queue'
%% module for that, but for efficiency we implement the relevant
%% functions directly in here, thus saving on inter-module calls and
%% eliminating a level of boxing.
%%
%% When the queue contains items with non-zero priorities, it is
%% represented as a sorted kv list with the inverted Priority as the
%% key and an ordinary queue as the value. Here again we use our own
%% ordinary queue implementation for efficiency, often making recursive
%% calls into the same function knowing that ordinary queues represent
%% a base case.

-module(emqx_pqueue).

-export([
    new/0,
    is_empty/1,
    len/1,
    plen/2,
    to_list/1,
    to_queues_list/1,
    from_list/1,
    in/2,
    in/4,
    out/1,
    out/2,
    drop/1,
    out_p/1,
    filter/2,
    fold/3,
    highest/1,
    shift/1
]).

%% Mostly for test purposes:
-export([
    cqueue_new/0,
    cqueue_in/3,
    cqueue_out/1,
    cqueue_drop/1
]).

-export_type([q/0]).

%%----------------------------------------------------------------------------

-type class() :: default | qos0.

%% Simple, single-class queue.
-type squeue() :: {queue, [any()], [any()], non_neg_integer()}.

%% Double-class queue.
%% Each class has its own in-list and out-list containing elements and special
%% "lane-switch" markers.
-type cqueue() ::
    {
        _Active :: class() | undefined,
        _Last :: class() | undefined,
        [any()],
        [any()],
        [any()],
        [any()],
        _Length :: non_neg_integer()
    }.

-type priority() :: integer() | 'infinity'.
-type pqueue() :: squeue() | cqueue() | {pqueue, [{priority(), squeue() | cqueue()}]}.
-type q() :: pqueue().

-define(switch, '$switch').

-define(cqueue(), ?cqueue(_, _, _)).
-define(cqueue(ACTIVE, LAST, LEN), {ACTIVE, LAST, _, _, _, _, LEN}).

%%----------------------------------------------------------------------------

-spec new() -> pqueue().
new() ->
    {queue, [], [], 0}.

-spec is_empty(pqueue()) -> boolean().
is_empty({queue, [], [], 0}) ->
    true;
is_empty(?cqueue(_Active, _Last, 0)) ->
    true;
is_empty(_) ->
    false.

-spec len(pqueue()) -> non_neg_integer().
len({queue, _R, _F, L}) ->
    L;
len(?cqueue(_Active, _Last, L)) ->
    L;
len({pqueue, Queues}) ->
    lists:sum([len(Q) || {_, Q} <- Queues]).

-spec plen(priority(), pqueue()) -> non_neg_integer().
plen(P, {pqueue, Queues}) ->
    case lists:keysearch(maybe_negate_priority(P), 1, Queues) of
        {value, {_, Q}} ->
            len(Q);
        false ->
            0
    end;
plen(0, Q) ->
    len(Q);
plen(_, _Q) ->
    0.

-spec to_list(pqueue()) -> [{priority(), any()}].
to_list(Q) ->
    %% NOTE: Suboptimal, so far used only in tests.
    case out_p(Q) of
        {{value, V, P}, NQ} ->
            [{P, V} | to_list(NQ)];
        {empty, _} ->
            []
    end.

-spec to_queues_list(pqueue()) -> [{priority(), squeue() | cqueue()}].
to_queues_list({queue, _, _, _} = Q) ->
    [{0, Q}];
to_queues_list(?cqueue() = CQ) ->
    [{0, CQ}];
to_queues_list({pqueue, Queues}) ->
    lists:sort(
        fun
            ({infinity = _P1, _}, {_P2, _}) -> true;
            ({P1, _}, {P2, _}) -> P1 >= P2
        end,
        [{maybe_negate_priority(P), Q} || {P, Q} <- Queues]
    ).

-spec from_list([{priority(), class(), any()}]) -> pqueue().
from_list(L) ->
    lists:foldl(fun({P, Class, E}, Q) -> in(E, P, Class, Q) end, new(), L).

-spec in(any(), pqueue()) -> pqueue().
in(Item, Q) ->
    in(Item, 0, default, Q).

-spec in(any(), priority(), class(), pqueue()) -> pqueue().
in(X, Priority, Class, {pqueue, Queues}) ->
    P = maybe_negate_priority(Priority),
    NQueues =
        case lists:keysearch(P, 1, Queues) of
            {value, {_, Q}} ->
                lists:keyreplace(P, 1, Queues, {P, in(X, Class, Q)});
            false when P == infinity ->
                Q = new(X, Class),
                [{P, Q} | Queues];
            false ->
                Q = new(X, Class),
                case Queues of
                    [{infinity, InfQueue} | Queues1] ->
                        [{infinity, InfQueue} | lists:keysort(1, [{P, Q} | Queues1])];
                    _ ->
                        lists:keysort(1, [{P, Q} | Queues])
                end
        end,
    {pqueue, NQueues};
in(X, 0, Class, Q) ->
    in(X, Class, Q);
in(X, Priority, Class, Q) ->
    case Q of
        {queue, [], [], 0} ->
            in(X, Priority, Class, {pqueue, []});
        %% Should be unreachable:
        {_Active, _Last, [], [], [], [], 0} ->
            in(X, Priority, Class, {pqueue, []});
        _NonEmpty ->
            in(X, Priority, Class, {pqueue, [{0, Q}]})
    end.

in(X, default, {queue, [_] = In, [], 1}) ->
    {queue, [X], In, 2};
in(X, default, {queue, In, Out, Len}) when is_list(In) ->
    {queue, [X | In], Out, Len + 1};
in(X, Class, ?cqueue() = CQ) ->
    cqueue_in(X, Class, CQ);
in(X, Class, {queue, In, Out, L}) ->
    cqueue_in(X, Class, cqueue_from_simple(In, Out, L)).

new(X, default) ->
    {queue, [X], [], 1};
new(X, Class) ->
    cqueue_new(X, Class).

-spec out(pqueue()) -> {empty | {value, any()}, pqueue()}.
out({queue, [], [], 0} = Q) ->
    {empty, Q};
out({queue, [V], [], 1}) ->
    {{value, V}, {queue, [], [], 0}};
out({queue, [Y | In], [], Len}) ->
    [V | Out] = lists:reverse(In, []),
    {{value, V}, {queue, [Y], Out, Len - 1}};
out({queue, In, [V], Len}) when is_list(In) ->
    {{value, V}, r2f(In, Len - 1)};
out({queue, In, [V | Out], Len}) when is_list(In) ->
    {{value, V}, {queue, In, Out, Len - 1}};
out({Active, Last, In, Out, Q0In, Q0Out, L}) ->
    cqueue_out(Active, Last, In, Out, Q0In, Q0Out, L);
out({pqueue, [{P, Q} | Queues]}) ->
    {R, Q1} = out(Q),
    case is_empty(Q1) of
        true ->
            {R, from_pqueue_list(Queues)};
        false ->
            NQ = {pqueue, [{P, Q1} | Queues]},
            {R, NQ}
    end.

-spec drop(pqueue()) -> {empty | {value, any()}, pqueue()}.
drop({queue, _, _, _} = Q) ->
    out(Q);
drop(?cqueue() = CQ) ->
    cqueue_drop(CQ);
drop({pqueue, [{P, Q} | Queues]}) ->
    {R, Q1} = drop(Q),
    case is_empty(Q1) of
        true ->
            {R, from_pqueue_list(Queues)};
        false ->
            NQ = {pqueue, [{P, Q1} | Queues]},
            {R, NQ}
    end.

-spec shift(pqueue()) -> pqueue().
shift({pqueue, []}) ->
    %% Shouldn't happen?
    {pqueue, []};
shift({pqueue, [Hd | Rest]}) ->
    %% Let's hope there are not many priorities.
    {pqueue, Rest ++ [Hd]};
shift(Q) ->
    Q.

-spec out_p(pqueue()) -> {empty | {value, any(), priority()}, pqueue()}.
out_p({queue, _, _, _} = Q) ->
    add_p(out(Q), 0);
out_p({Active, Last, In, Out, Q0In, Q0Out, L}) ->
    add_p(cqueue_out(Active, Last, In, Out, Q0In, Q0Out, L), 0);
out_p({pqueue, [{P, _} | _]} = Q) ->
    add_p(out(Q), maybe_negate_priority(P)).

out(0, {queue, _, _, _} = Q) ->
    out(Q);
out(0, ?cqueue() = CQ) ->
    cqueue_out(CQ);
out(Priority, {pqueue, Queues}) ->
    P = maybe_negate_priority(Priority),
    case lists:keysearch(P, 1, Queues) of
        {value, {_, Q}} ->
            {R, Q1} = out(Q),
            Queues1 =
                case is_empty(Q1) of
                    true -> lists:keydelete(P, 1, Queues);
                    false -> lists:keyreplace(P, 1, Queues, {P, Q1})
                end,
            {R, from_pqueue_list(Queues1)};
        false ->
            {empty, {pqueue, Queues}}
    end;
out(Priority, _) ->
    erlang:error(badarg, [Priority]).

add_p(R, P) ->
    case R of
        {empty, Q} -> {empty, Q};
        {{value, V}, Q} -> {{value, V, P}, Q}
    end.

-spec filter(fun((any()) -> boolean()), pqueue()) -> pqueue().
filter(Pred, {queue, In, Out, _}) ->
    {L1, FIn} = filter_list(Pred, In),
    {L2, FOut} = filter_list(Pred, Out),
    {queue, FIn, FOut, L1 + L2};
filter(Pred, {Active, Last, In, Out, Q0In, Q0Out, _}) ->
    cqueue_filter(Pred, Active, Last, In, Out, Q0In, Q0Out);
filter(Pred, {pqueue, Queues}) ->
    from_pqueue_list(
        lists:filtermap(
            fun({P, Q}) ->
                FQ = filter(Pred, Q),
                case is_empty(FQ) of
                    false -> {true, {P, FQ}};
                    true -> false
                end
            end,
            Queues
        )
    ).

filter_list(Pred, L) ->
    lists:foldr(
        fun(V, {Len, Acc}) ->
            case Pred(V) of
                true -> {Len + 1, [V | Acc]};
                false -> {Len, Acc}
            end
        end,
        {0, []},
        L
    ).

-spec fold(fun((any(), priority(), A) -> A), A, pqueue()) -> A.
fold(Fun, Init, Q) ->
    case out_p(Q) of
        {empty, _Q} -> Init;
        {{value, V, P}, Q1} -> fold(Fun, Fun(V, P, Init), Q1)
    end.

-spec highest(pqueue()) -> priority().
highest({pqueue, [{P, _} | _]}) ->
    maybe_negate_priority(P);
highest(_Q) ->
    0.

r2f([], 0) -> {queue, [], [], 0};
r2f([_] = R, 1) -> {queue, [], R, 1};
r2f([X, Y], 2) -> {queue, [X], [Y], 2};
r2f([X, Y | R], L) -> {queue, [X, Y], lists:reverse(R, []), L}.

maybe_negate_priority(infinity) -> infinity;
maybe_negate_priority(P) -> -P.

from_pqueue_list([]) ->
    new();
from_pqueue_list([{0, Q}]) ->
    Q;
from_pqueue_list(Queues) ->
    {pqueue, Queues}.

%% Classful queue

cqueue_new() ->
    {default, default, [], [], [], [], 0}.

cqueue_new(X, Class) ->
    case Class of
        default ->
            In = [X],
            Q0In = [];
        qos0 ->
            In = [],
            Q0In = [X]
    end,
    {Class, Class, In, [], Q0In, [], 1}.

cqueue_from_simple(In, Out, L) ->
    {default, default, In, Out, [], [], L}.

cqueue_in(X, Class, {_Active, _Last, [], [], [], [], 0}) ->
    cqueue_new(X, Class);
cqueue_in(X, Class, {Active, Last0, In0, Out, Q0In0, Q0Out, L}) ->
    case Class of
        Last0 when Class =:= default ->
            In = [X | In0],
            Q0In = Q0In0;
        Last0 when Class =:= qos0 ->
            In = In0,
            Q0In = [X | Q0In0];
        default ->
            In = [X | In0],
            Q0In = [?switch | Q0In0];
        qos0 ->
            In = [?switch | In0],
            Q0In = [X | Q0In0]
    end,
    {Active, Class, In, Out, Q0In, Q0Out, L + 1}.

cqueue_out({_Active, _Last, _, _, _, _, 0} = Q) ->
    {empty, Q};
cqueue_out({Active, Last, In, Out, Q0In, Q0Out, L}) ->
    cqueue_out(Active, Last, In, Out, Q0In, Q0Out, L).

cqueue_out(Active, Last, _, _, _, _, 0) ->
    {empty, {Active, Last, [], [], [], [], 0}};
cqueue_out(default, Last, In, Out, Q0In, Q0Out, L) ->
    case Out of
        [?switch | Rest] ->
            cqueue_out(qos0, Last, In, Rest, Q0In, Q0Out, L);
        [V | Rest] ->
            {{value, V}, {default, Last, In, Rest, Q0In, Q0Out, L - 1}};
        [] when In =:= [] ->
            cqueue_out(qos0, Last, In, Out, Q0In, Q0Out, L);
        [] ->
            NOut = lists:reverse(In, []),
            cqueue_out(default, Last, [], NOut, Q0In, Q0Out, L)
    end;
cqueue_out(qos0, Last, In, Out, Q0In, Q0Out, L) ->
    case Q0Out of
        [?switch | Rest] ->
            cqueue_out(default, Last, In, Out, Q0In, Rest, L);
        [V | Rest] ->
            {{value, V}, {qos0, Last, In, Out, Q0In, Rest, L - 1}};
        [] when Q0In =:= [] ->
            cqueue_out(default, Last, In, Out, Q0In, Q0Out, L);
        [] ->
            NOut = lists:reverse(Q0In, []),
            cqueue_out(qos0, Last, In, Out, [], NOut, L)
    end.

cqueue_filter(Pred, Active, Last, In, Out, Q0In, Q0Out) ->
    {L1, FIn} = cqueue_filter_list(Pred, In),
    {L2, FOut} = cqueue_filter_list(Pred, Out),
    {L3, FQ0In} = cqueue_filter_list(Pred, Q0In),
    {L4, FQ0Out} = cqueue_filter_list(Pred, Q0Out),
    {Active, Last, FIn, FOut, FQ0In, FQ0Out, L1 + L2 + L3 + L4}.

cqueue_filter_list(Pred, L) ->
    lists:foldr(
        fun
            (?switch, {Len, Acc}) ->
                {Len, [?switch | Acc]};
            (V, {Len, Acc}) ->
                case Pred(V) of
                    true -> {Len + 1, [V | Acc]};
                    false -> {Len, Acc}
                end
        end,
        {0, []},
        L
    ).

cqueue_drop({Active, Last, In, Out, Q0In, Q0Out, L}) ->
    cqueue_drop(Active, Last, In, Out, Q0In, Q0Out, L).

cqueue_drop(Active, Last, In, [?switch | Out], Q0In, [?switch | Q0Out], L) ->
    cqueue_drop(Active, Last, In, Out, Q0In, Q0Out, L);
cqueue_drop(Active, Last, In, Out, Q0In, [], L) ->
    case Q0In of
        [] ->
            cqueue_out(Active, Last, In, Out, [], [], L);
        _ ->
            cqueue_drop(Active, Last, In, Out, [], lists:reverse(Q0In), L)
    end;
cqueue_drop(Active, Last, In, Out, Q0In, Q0Out, L) ->
    case cqueue_scan_q0out(Q0Out, []) of
        {Q0Pre, V, Q0Rest} ->
            {{value, V}, {Active, Last, In, Out, Q0In, Q0Pre ++ Q0Rest, L - 1}};
        false when Q0In =:= [] ->
            cqueue_out(default, default, drop_switch(In), drop_switch(Out), [], [], L);
        false ->
            NQ0Out = lists:reverse(Q0In),
            case cqueue_scan_q0out(NQ0Out, []) of
                {Q0Pre, V, Q0Rest} ->
                    {{value, V}, {Active, Last, In, Out, [], Q0Out ++ Q0Pre ++ Q0Rest, L - 1}};
                false ->
                    cqueue_out(default, default, drop_switch(In), drop_switch(Out), [], [], L)
            end
    end.

cqueue_scan_q0out([?switch | Rest], Acc) ->
    cqueue_scan_q0out(Rest, [?switch | Acc]);
cqueue_scan_q0out([V | Rest], Acc) ->
    {Acc, V, Rest};
cqueue_scan_q0out([], _) ->
    false.

drop_switch(L) ->
    [X || X <- L, X =/= ?switch].
