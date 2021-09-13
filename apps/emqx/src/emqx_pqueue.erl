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
%% ordinary queue implemention for efficiency, often making recursive
%% calls into the same function knowing that ordinary queues represent
%% a base case.

-module(emqx_pqueue).

-export([ new/0
        , is_queue/1
        , is_empty/1
        , len/1
        , plen/2
        , to_list/1
        , from_list/1
        , in/2
        , in/3
        , out/1
        , out/2
        , out_p/1
        , join/2
        , filter/2
        , fold/3
        , highest/1
        , shift/1
        ]).

-export_type([q/0]).

%%----------------------------------------------------------------------------

-type(priority() :: integer() | 'infinity').
-type(squeue() :: {queue, [any()], [any()], non_neg_integer()}).
-type(pqueue() ::  squeue() | {pqueue, [{priority(), squeue()}]}).
-type(q() :: pqueue()).

%%----------------------------------------------------------------------------

-spec(new() -> pqueue()).
new() ->
    {queue, [], [], 0}.

-spec(is_queue(any()) -> boolean()).
is_queue({queue, R, F, L}) when is_list(R), is_list(F), is_integer(L) ->
    true;
is_queue({pqueue, Queues}) when is_list(Queues) ->
    lists:all(fun ({infinity, Q}) -> is_queue(Q);
                  ({P,        Q}) -> is_integer(P) andalso is_queue(Q)
              end, Queues);
is_queue(_) ->
    false.

-spec(is_empty(pqueue()) -> boolean()).
is_empty({queue, [], [], 0}) ->
    true;
is_empty(_) ->
    false.

-spec(len(pqueue()) -> non_neg_integer()).
len({queue, _R, _F, L}) ->
    L;
len({pqueue, Queues}) ->
    lists:sum([len(Q) || {_, Q} <- Queues]).

-spec(plen(priority(), pqueue()) -> non_neg_integer()).
plen(0, {queue, _R, _F, L}) ->
    L;
plen(_, {queue, _R, _F, _}) ->
    0;
plen(P, {pqueue, Queues}) ->
    case lists:keysearch(maybe_negate_priority(P), 1, Queues) of
        {value, {_, Q}} -> len(Q);
        false           -> 0
    end.

-spec(to_list(pqueue()) -> [{priority(), any()}]).
to_list({queue, In, Out, _Len}) when is_list(In), is_list(Out) ->
    [{0, V} || V <- Out ++ lists:reverse(In, [])];
to_list({pqueue, Queues}) ->
    [{maybe_negate_priority(P), V} || {P, Q} <- Queues,
                                      {0, V} <- to_list(Q)].

-spec(from_list([{priority(), any()}]) -> pqueue()).
from_list(L) ->
    lists:foldl(fun ({P, E}, Q) -> in(E, P, Q) end, new(), L).

-spec(in(any(), pqueue()) -> pqueue()).
in(Item, Q) ->
    in(Item, 0, Q).

-spec(in(any(), priority(), pqueue()) -> pqueue()).
in(X, 0, {queue, [_] = In, [], 1}) ->
    {queue, [X], In, 2};
in(X, 0, {queue, In, Out, Len}) when is_list(In), is_list(Out) ->
    {queue, [X|In], Out, Len + 1};
in(X, Priority, _Q = {queue, [], [], 0}) ->
    in(X, Priority, {pqueue, []});
in(X, Priority, Q = {queue, _, _, _}) ->
    in(X, Priority, {pqueue, [{0, Q}]});
in(X, Priority, {pqueue, Queues}) ->
    P = maybe_negate_priority(Priority),
    {pqueue, case lists:keysearch(P, 1, Queues) of
                 {value, {_, Q}} ->
                     lists:keyreplace(P, 1, Queues, {P, in(X, Q)});
                 false when P == infinity ->
                     [{P, {queue, [X], [], 1}} | Queues];
                 false ->
                     case Queues of
                         [{infinity, InfQueue} | Queues1] ->
                             [{infinity, InfQueue} |
                              lists:keysort(1, [{P, {queue, [X], [], 1}} | Queues1])];
                         _ ->
                             lists:keysort(1, [{P, {queue, [X], [], 1}} | Queues])
                     end
             end}.

-spec(out(pqueue()) -> {empty | {value, any()}, pqueue()}).
out({queue, [], [], 0} = Q) ->
    {empty, Q};
out({queue, [V], [], 1}) ->
    {{value, V}, {queue, [], [], 0}};
out({queue, [Y|In], [], Len}) ->
    [V|Out] = lists:reverse(In, []),
    {{value, V}, {queue, [Y], Out, Len - 1}};
out({queue, In, [V], Len}) when is_list(In) ->
    {{value,V}, r2f(In, Len - 1)};
out({queue, In,[V|Out], Len}) when is_list(In) ->
    {{value, V}, {queue, In, Out, Len - 1}};
out({pqueue, [{P, Q} | Queues]}) ->
    {R, Q1} = out(Q),
    NewQ = case is_empty(Q1) of
               true -> case Queues of
                           []           -> {queue, [], [], 0};
                           [{0, OnlyQ}] -> OnlyQ;
                           [_|_]        -> {pqueue, Queues}
                       end;
               false -> {pqueue, [{P, Q1} | Queues]}
           end,
    {R, NewQ}.

-spec(shift(pqueue()) -> pqueue()).
shift(Q = {queue, _, _, _}) ->
    Q;
shift({pqueue, []}) ->
    {pqueue, []}; %% Shouldn't happen?
shift({pqueue, [Hd|Rest]}) ->
    {pqueue, Rest ++ [Hd]}. %% Let's hope there are not many priorities.

-spec(out_p(pqueue()) -> {empty | {value, any(), priority()}, pqueue()}).
out_p({queue, _, _, _}       = Q) -> add_p(out(Q), 0);
out_p({pqueue, [{P, _} | _]} = Q) -> add_p(out(Q), maybe_negate_priority(P)).

out(0, {queue, _, _, _} = Q) ->
    out(Q);
out(Priority, {queue, _, _, _}) ->
    erlang:error(badarg, [Priority]);
out(Priority, {pqueue, Queues}) ->
    P = maybe_negate_priority(Priority),
    case lists:keysearch(P, 1, Queues) of
        {value, {_, Q}} ->
            {R, Q1} = out(Q),
            Queues1 = case is_empty(Q1) of
                true  -> lists:keydelete(P, 1, Queues);
                false -> lists:keyreplace(P, 1, Queues, {P, Q1})
            end,
            {R, case Queues1 of
                    []           -> {queue, [], [], 0};
                    [{0, OnlyQ}] -> OnlyQ;
                    [_|_]        -> {pqueue, Queues1}
                end};
        false ->
            {empty, {pqueue, Queues}}
    end.

add_p(R, P) -> case R of
                   {empty, Q}      -> {empty, Q};
                   {{value, V}, Q} -> {{value, V, P}, Q}
               end.

-spec(join(pqueue(), pqueue()) -> pqueue()).
join(A, {queue, [], [], 0}) ->
    A;
join({queue, [], [], 0}, B) ->
    B;
join({queue, AIn, AOut, ALen}, {queue, BIn, BOut, BLen}) ->
    {queue, BIn, AOut ++ lists:reverse(AIn, BOut), ALen + BLen};
join(A = {queue, _, _, _}, {pqueue, BPQ}) ->
    {Pre, Post} =
        lists:splitwith(fun ({P, _}) -> P < 0 orelse P == infinity end, BPQ),
    Post1 = case Post of
                []                        -> [ {0, A} ];
                [ {0, ZeroQueue} | Rest ] -> [ {0, join(A, ZeroQueue)} | Rest ];
                _                         -> [ {0, A} | Post ]
            end,
    {pqueue, Pre ++ Post1};
join({pqueue, APQ}, B = {queue, _, _, _}) ->
    {Pre, Post} =
        lists:splitwith(fun ({P, _}) -> P < 0 orelse P == infinity end, APQ),
    Post1 = case Post of
                []                        -> [ {0, B} ];
                [ {0, ZeroQueue} | Rest ] -> [ {0, join(ZeroQueue, B)} | Rest ];
                _                         -> [ {0, B} | Post ]
            end,
    {pqueue, Pre ++ Post1};
join({pqueue, APQ}, {pqueue, BPQ}) ->
    {pqueue, merge(APQ, BPQ, [])}.

merge([], BPQ, Acc) ->
    lists:reverse(Acc, BPQ);
merge(APQ, [], Acc) ->
    lists:reverse(Acc, APQ);
merge([{P, A}|As], [{P, B}|Bs], Acc) ->
    merge(As, Bs, [ {P, join(A, B)} | Acc ]);
merge([{PA, A}|As], Bs = [{PB, _}|_], Acc) when PA < PB orelse PA == infinity ->
    merge(As, Bs, [ {PA, A} | Acc ]);
merge(As = [{_, _}|_], [{PB, B}|Bs], Acc) ->
    merge(As, Bs, [ {PB, B} | Acc ]).

-spec(filter(fun ((any()) -> boolean()), pqueue()) -> pqueue()).
filter(Pred, Q) -> fold(fun(V, P, Acc) ->
                                case Pred(V) of
                                    true  -> in(V, P, Acc);
                                    false -> Acc
                                end
                        end, new(), Q).

-spec(fold(fun ((any(), priority(), A) -> A), A, pqueue()) -> A).
fold(Fun, Init, Q) -> case out_p(Q) of
                          {empty, _Q}         -> Init;
                          {{value, V, P}, Q1} -> fold(Fun, Fun(V, P, Init), Q1)
                      end.

-spec(highest(pqueue()) -> priority() | 'empty').
highest({queue, [], [], 0})     -> empty;
highest({queue, _, _, _})       -> 0;
highest({pqueue, [{P, _} | _]}) -> maybe_negate_priority(P).

r2f([],      0) -> {queue, [], [], 0};
r2f([_] = R, 1) -> {queue, [], R, 1};
r2f([X,Y],   2) -> {queue, [X], [Y], 2};
r2f([X,Y|R], L) -> {queue, [X,Y], lists:reverse(R, []), L}.

maybe_negate_priority(infinity) -> infinity;
maybe_negate_priority(P)        -> -P.
