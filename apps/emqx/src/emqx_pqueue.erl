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
    drop/2,
    out_p/1,
    filter/2,
    fold/3,
    highest/1,
    lowest/1,
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

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

%%----------------------------------------------------------------------------

-type class() :: default | qos0.

%% Simple, single-class queue.
%% Kept as better optimized, more GC-friendly implementation for situations
%% when no QoS0 messages are getting queued: no QoS0 messaging or `store_qos0`
%% is disabled.
-type squeue() :: {queue, [any()], [any()], non_neg_integer()}.

%% Double-class queue.
%% Each class has its own in-list and out-list containing elements and special
%% "lane-switch" markers.
-type cqueue() ::
    {
        %% Which lane we are currently pulling elements from?
        _HeadLane :: class(),
        %% Which lane we are currently queueing elements into?
        _TailLane :: class(),
        %% In-list and out-list of `default` class lane:
        [any()],
        [any()],
        %% In-list and out-list of `qos0` class lane:
        [any()],
        [any()],
        %% Total number of elements in the queue:
        _Length :: non_neg_integer()
    }.

-type priority() :: integer() | 'infinity'.
-type pqueue() :: squeue() | cqueue() | {pqueue, [{priority(), squeue() | cqueue()}]}.
-type q() :: pqueue().

-define(switch, '$switch').
-define(switch_(N), {'$switch', N}).

-define(cqueue(), ?cqueue(_, _, _)).
-define(cqueue(HEADLANE, TAILLANE, LEN), {HEADLANE, TAILLANE, _, _, _, _, LEN}).

%%----------------------------------------------------------------------------

-spec new() -> pqueue().
new() ->
    {queue, [], [], 0}.

-spec is_empty(pqueue()) -> boolean().
is_empty({queue, [], [], 0}) ->
    true;
is_empty(?cqueue(_, _, 0)) ->
    true;
is_empty(_) ->
    false.

-spec len(pqueue()) -> non_neg_integer().
len({queue, _R, _F, L}) ->
    L;
len(?cqueue(_, _, L)) ->
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
        ?cqueue(_, _, 0) ->
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
out({HeadLane, TailLane, In, Out, Q0In, Q0Out, L}) ->
    cqueue_out(HeadLane, TailLane, In, Out, Q0In, Q0Out, L);
out({pqueue, [{P, Q} | Queues]}) ->
    {R, Q1} = out(Q),
    case is_empty(Q1) of
        true ->
            {R, from_pqueue_list(Queues)};
        false ->
            NQ = {pqueue, [{P, Q1} | Queues]},
            {R, NQ}
    end.

-doc """
Drop oldest least important element, for a given priority.
* If there are `qos0`-class elements, oldest of them is dropped.
* If there are not, `drop(P, Q)` is equivalent to calling `out(P, Q)`.
""".
-spec drop(priority(), pqueue()) -> {empty | {value, any()}, pqueue()}.
drop(0, {queue, _, _, _} = Q) ->
    out(Q);
drop(0, {HeadLane, TailLane, In, Out, Q0In, Q0Out, L}) ->
    cqueue_drop(HeadLane, TailLane, In, Out, Q0In, Q0Out, L);
drop(Priority, {pqueue, Queues}) ->
    P = maybe_negate_priority(Priority),
    case lists:keysearch(P, 1, Queues) of
        {value, {_, Q}} ->
            {R, Q1} = drop(0, Q),
            Queues1 =
                case is_empty(Q1) of
                    true -> lists:keydelete(P, 1, Queues);
                    false -> lists:keyreplace(P, 1, Queues, {P, Q1})
                end,
            {R, from_pqueue_list(Queues1)};
        false ->
            {empty, {pqueue, Queues}}
    end;
drop(Priority, _) ->
    erlang:error(badarg, [Priority]).

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
out_p({HeadLane, TailLane, In, Out, Q0In, Q0Out, L}) ->
    add_p(cqueue_out(HeadLane, TailLane, In, Out, Q0In, Q0Out, L), 0);
out_p({pqueue, [{P, _} | _]} = Q) ->
    add_p(out(Q), maybe_negate_priority(P)).

out(0, {queue, _, _, _} = Q) ->
    out(Q);
out(0, {HeadLane, TailLane, In, Out, Q0In, Q0Out, L}) ->
    cqueue_out(HeadLane, TailLane, In, Out, Q0In, Q0Out, L);
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
filter(Pred, {HeadLane, TailLane, In, Out, Q0In, Q0Out, _}) ->
    cqueue_filter(Pred, HeadLane, TailLane, In, Out, Q0In, Q0Out);
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

%% Queues is sorted highest-priority-first (see `in/4`), and never holds
%% an emptied-out lane (lanes are pruned in `out/1`, `out/2` and `drop/2`),
%% so the last element is the lowest non-empty priority.
-spec lowest(pqueue()) -> priority().
lowest({pqueue, Queues}) ->
    {P, _} = lists:last(Queues),
    maybe_negate_priority(P);
lowest(_Q) ->
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
            Out = [X],
            Q0Out = [];
        qos0 ->
            Out = [],
            Q0Out = [X]
    end,
    {Class, Class, [], Out, [], Q0Out, 1}.

cqueue_from_simple(In, Out, L) ->
    {default, default, In, Out, [], [], L}.

cqueue_in(X, Class, {_HeadLane, _TailLane, [], [], [], [], 0}) ->
    cqueue_new(X, Class);
cqueue_in(X, Class, {HeadLane, TailLane, In0, Out, Q0In0, Q0Out, L}) ->
    case Class of
        %% If `Class` matches current tail lane, just push the element to in-list.
        TailLane when Class =:= default ->
            In = [X | In0],
            Q0In = Q0In0;
        TailLane when Class =:= qos0 ->
            In = In0,
            Q0In = [X | Q0In0];
        %% Otherwise:
        %% * Push the switch marker to tail lane in-list.
        %% * Push element to the other lane.
        default ->
            In = [X | In0],
            Q0In = cq_push_switch(Q0In0);
        qos0 ->
            In = cq_push_switch(In0),
            Q0In = [X | Q0In0]
    end,
    {HeadLane, Class, In, Out, Q0In, Q0Out, L + 1}.

cqueue_out({_HeadLane, _TailLane, _, _, _, _, 0} = Q) ->
    {empty, Q};
cqueue_out({HeadLane, TailLane, In, Out, Q0In, Q0Out, L}) ->
    cqueue_out(HeadLane, TailLane, In, Out, Q0In, Q0Out, L).

cqueue_out(_HeadLane, _TailLane, _, _, _, _, 0) ->
    {empty, cqueue_new()};
cqueue_out(default, TL, In, Out, Q0In, Q0Out, L) ->
    case Out of
        %% There's a switch marker on the head lane out-list: switch head lane and retry.
        [?switch | Rest] ->
            cqueue_out(qos0, TL, In, Rest, Q0In, Q0Out, L);
        [?switch_(N) | Rest] ->
            cqueue_out(qos0, TL, In, [cq_mk_switch(N - 1) | Rest], Q0In, Q0Out, L);
        %% There's an element, emit it.
        [V | Rest] ->
            {{value, V}, {default, TL, In, Rest, Q0In, Q0Out, L - 1}};
        %% Whole lane is empty, attempt to switch and retry.
        %% Should be unreachable.
        [] when In =:= [] ->
            cqueue_out(qos0, TL, In, Out, Q0In, Q0Out, L);
        %% Head lane out-list is empty, roll the in-list over and retry.
        [] ->
            NOut = lists:reverse(In, []),
            cqueue_out(default, TL, [], NOut, Q0In, Q0Out, L)
    end;
cqueue_out(qos0, TL, In, Out, Q0In, Q0Out, L) ->
    case Q0Out of
        %% There's a switch marker on the head lane out-list: switch head lane and retry.
        [?switch | Rest] ->
            cqueue_out(default, TL, In, Out, Q0In, Rest, L);
        [?switch_(N) | Rest] ->
            cqueue_out(default, TL, In, Out, Q0In, [cq_mk_switch(N - 1) | Rest], L);
        %% There's an element, emit it.
        [V | Rest] ->
            {{value, V}, {qos0, TL, In, Out, Q0In, Rest, L - 1}};
        %% Whole lane is empty, attempt to switch and retry.
        %% Should be unreachable.
        [] when Q0In =:= [] ->
            cqueue_out(default, TL, In, Out, Q0In, Q0Out, L);
        %% Head lane out-list is empty, roll the in-list over and retry.
        [] ->
            NOut = lists:reverse(Q0In, []),
            cqueue_out(qos0, TL, In, Out, [], NOut, L)
    end.

cqueue_filter(Pred, HeadLane, TailLane, In, Out, Q0In, Q0Out) ->
    {L1, FIn} = cqueue_filter_list(Pred, In),
    {L2, FOut} = cqueue_filter_list(Pred, Out),
    {L3, FQ0In} = cqueue_filter_list(Pred, Q0In),
    {L4, FQ0Out} = cqueue_filter_list(Pred, Q0Out),
    {HeadLane, TailLane, FIn, FOut, FQ0In, FQ0Out, L1 + L2 + L3 + L4}.

cqueue_filter_list(Pred, L) ->
    lists:foldr(
        fun
            (?switch, {Len, Acc}) ->
                {Len, cq_push_switch(Acc)};
            (?switch_(N), {Len, Acc}) ->
                {Len, cq_push_switches(N, Acc)};
            (V, {Len, Acc}) ->
                case Pred(V) of
                    true -> {Len + 1, [V | Acc]};
                    false -> {Len, Acc}
                end
        end,
        {0, []},
        L
    ).

cqueue_drop({HeadLane, TailLane, In, Out, Q0In, Q0Out, L}) ->
    cqueue_drop(HeadLane, TailLane, In, Out, Q0In, Q0Out, L).

cqueue_drop(HL, TL, In, [?switch | Out], Q0In, [?switch | Q0Out], L) ->
    %% Consume pair lane switches, must be leftovers from a previous drop.
    cqueue_drop(HL, TL, In, Out, Q0In, Q0Out, L);
cqueue_drop(HL, TL, In, [?switch | Out], Q0In, [?switch_(N) | Q0Out], L) ->
    %% Consume pair lane switches, must be leftovers from a previous drop.
    cqueue_drop(HL, TL, In, Out, Q0In, [cq_mk_switch(N - 1) | Q0Out], L);
cqueue_drop(HL, TL, In, Out, Q0In, [], L) ->
    case Q0In of
        [] ->
            %% Whole `qos0` lane is empty, fall back to `default` lane.
            cqueue_out(HL, TL, In, Out, [], [], L);
        _ ->
            %% `qos0` lane out-list is empty, roll the in-list over and retry.
            cqueue_drop(HL, TL, In, Out, [], lists:reverse(Q0In), L)
    end;
cqueue_drop(HL, TL, In, Out, Q0In, Q0Out, L) ->
    case cqueue_scan_q0out(Q0Out, 0) of
        {Q0Switches, V, Q0Rest} ->
            %% Found an element in `qos0` out-list: emit it and push combined switch marker back.
            NQ0Out = cq_push_switches(Q0Switches, Q0Rest),
            {{value, V}, {HL, TL, In, Out, Q0In, NQ0Out, L - 1}};
        false when Q0In =:= [] ->
            %% Found only switch markers and `qos0` in-list is empty: there's no `qos0` elements.
            %% Switch to `default`-only queue, clean out all switch markers as there's nothing to
            %% emit from `qos0` lane anymore.
            cqueue_out(default, default, cq_rm_switches(In), cq_rm_switches(Out), [], [], L);
        false ->
            %% Roll the `qos0` in-list over and try to find an element.
            NQ0Out = lists:reverse(Q0In),
            case cqueue_scan_q0out(NQ0Out, 0) of
                {Q0Switches, V, Q0Rest} ->
                    FQ0Out = Q0Out ++ cq_push_switches(Q0Switches, Q0Rest),
                    {{value, V}, {HL, TL, In, Out, [], FQ0Out, L - 1}};
                false ->
                    cqueue_out(default, default, cq_rm_switches(In), cq_rm_switches(Out), [], [], L)
            end
    end.

%% Find a next element on `qos0` out-list, skipping over switch markers.
%% Return `{<number of skipped lane switches>, <element>, <out-list tail>}` or `false` if
%% there are only switch markers left.
cqueue_scan_q0out([?switch | Rest], NSwitches) ->
    cqueue_scan_q0out(Rest, NSwitches + 1);
cqueue_scan_q0out([?switch_(N) | Rest], NSwitches) ->
    cqueue_scan_q0out(Rest, NSwitches + N);
cqueue_scan_q0out([V | Rest], NSwitches) ->
    {NSwitches, V, Rest};
cqueue_scan_q0out([], _) ->
    false.

cq_rm_switches(L) ->
    [X || X <- L, not cq_is_switch_marker(X)].

cq_mk_switch(1) ->
    ?switch;
cq_mk_switch(N) ->
    ?switch_(N).

cq_is_switch_marker(?switch) ->
    true;
cq_is_switch_marker(?switch_(_)) ->
    true;
cq_is_switch_marker(_) ->
    false.

cq_push_switch([?switch | Rest]) ->
    [?switch_(2) | Rest];
cq_push_switch([?switch_(N) | Rest]) ->
    [?switch_(N + 1) | Rest];
cq_push_switch(Rest) ->
    [?switch | Rest].

cq_push_switches(0, Rest) ->
    Rest;
cq_push_switches(N, [?switch | Rest]) ->
    [?switch_(N + 1) | Rest];
cq_push_switches(N, [?switch_(M) | Rest]) ->
    [?switch_(N + M) | Rest];
cq_push_switches(N, Rest) ->
    [cq_mk_switch(N) | Rest].
