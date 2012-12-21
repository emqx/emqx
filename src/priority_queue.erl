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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
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


-module(priority_queue).

-export([new/0, is_queue/1, is_empty/1, len/1, to_list/1, in/2, in/3,
         out/1, join/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([q/0]).

-type(q() :: pqueue()).
-type(priority() :: integer() | 'infinity').
-type(squeue() :: {queue, [any()], [any()]}).
-type(pqueue() ::  squeue() | {pqueue, [{priority(), squeue()}]}).

-spec(new/0 :: () -> pqueue()).
-spec(is_queue/1 :: (any()) -> boolean()).
-spec(is_empty/1 :: (pqueue()) -> boolean()).
-spec(len/1 :: (pqueue()) -> non_neg_integer()).
-spec(to_list/1 :: (pqueue()) -> [{priority(), any()}]).
-spec(in/2 :: (any(), pqueue()) -> pqueue()).
-spec(in/3 :: (any(), priority(), pqueue()) -> pqueue()).
-spec(out/1 :: (pqueue()) -> {empty | {value, any()}, pqueue()}).
-spec(join/2 :: (pqueue(), pqueue()) -> pqueue()).

-endif.

%%----------------------------------------------------------------------------

new() ->
    {queue, [], []}.

is_queue({queue, R, F}) when is_list(R), is_list(F) ->
    true;
is_queue({pqueue, Queues}) when is_list(Queues) ->
    lists:all(fun ({infinity, Q}) -> is_queue(Q);
                  ({P,        Q}) -> is_integer(P) andalso is_queue(Q)
              end, Queues);
is_queue(_) ->
    false.

is_empty({queue, [], []}) ->
    true;
is_empty(_) ->
    false.

len({queue, R, F}) when is_list(R), is_list(F) ->
    length(R) + length(F);
len({pqueue, Queues}) ->
    lists:sum([len(Q) || {_, Q} <- Queues]).

to_list({queue, In, Out}) when is_list(In), is_list(Out) ->
    [{0, V} || V <- Out ++ lists:reverse(In, [])];
to_list({pqueue, Queues}) ->
    [{maybe_negate_priority(P), V} || {P, Q} <- Queues,
                                      {0, V} <- to_list(Q)].

in(Item, Q) ->
    in(Item, 0, Q).

in(X, 0, {queue, [_] = In, []}) ->
    {queue, [X], In};
in(X, 0, {queue, In, Out}) when is_list(In), is_list(Out) ->
    {queue, [X|In], Out};
in(X, Priority, _Q = {queue, [], []}) ->
    in(X, Priority, {pqueue, []});
in(X, Priority, Q = {queue, _, _}) ->
    in(X, Priority, {pqueue, [{0, Q}]});
in(X, Priority, {pqueue, Queues}) ->
    P = maybe_negate_priority(Priority),
    {pqueue, case lists:keysearch(P, 1, Queues) of
                 {value, {_, Q}} ->
                     lists:keyreplace(P, 1, Queues, {P, in(X, Q)});
                 false when P == infinity ->
                     [{P, {queue, [X], []}} | Queues];
                 false ->
                     case Queues of
                         [{infinity, InfQueue} | Queues1] ->
                             [{infinity, InfQueue} |
                              lists:keysort(1, [{P, {queue, [X], []}} | Queues1])];
                         _ ->
                             lists:keysort(1, [{P, {queue, [X], []}} | Queues])
                     end
             end}.

out({queue, [], []} = Q) ->
    {empty, Q};
out({queue, [V], []}) ->
    {{value, V}, {queue, [], []}};
out({queue, [Y|In], []}) ->
    [V|Out] = lists:reverse(In, []),
    {{value, V}, {queue, [Y], Out}};
out({queue, In, [V]}) when is_list(In) ->
    {{value,V}, r2f(In)};
out({queue, In,[V|Out]}) when is_list(In) ->
    {{value, V}, {queue, In, Out}};
out({pqueue, [{P, Q} | Queues]}) ->
    {R, Q1} = out(Q),
    NewQ = case is_empty(Q1) of
               true -> case Queues of
                           []           -> {queue, [], []};
                           [{0, OnlyQ}] -> OnlyQ;
                           [_|_]        -> {pqueue, Queues}
                       end;
               false -> {pqueue, [{P, Q1} | Queues]}
           end,
    {R, NewQ}.

join(A, {queue, [], []}) ->
    A;
join({queue, [], []}, B) ->
    B;
join({queue, AIn, AOut}, {queue, BIn, BOut}) ->
    {queue, BIn, AOut ++ lists:reverse(AIn, BOut)};
join(A = {queue, _, _}, {pqueue, BPQ}) ->
    {Pre, Post} =
        lists:splitwith(fun ({P, _}) -> P < 0 orelse P == infinity end, BPQ),
    Post1 = case Post of
                []                        -> [ {0, A} ];
                [ {0, ZeroQueue} | Rest ] -> [ {0, join(A, ZeroQueue)} | Rest ];
                _                         -> [ {0, A} | Post ]
            end,
    {pqueue, Pre ++ Post1};
join({pqueue, APQ}, B = {queue, _, _}) ->
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

r2f([])      -> {queue, [], []};
r2f([_] = R) -> {queue, [], R};
r2f([X,Y])   -> {queue, [X], [Y]};
r2f([X,Y|R]) -> {queue, [X,Y], lists:reverse(R, [])}.

maybe_negate_priority(infinity) -> infinity;
maybe_negate_priority(P)        -> -P.
