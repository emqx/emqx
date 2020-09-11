%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_pqueue_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(PQ, emqx_pqueue).
-define(SUITE, ?MODULE).

all() -> emqx_ct:all(?SUITE).

t_is_queue(_) ->
    Q = ?PQ:new(),
    ?assertEqual(true, ?PQ:is_queue(Q)),
    Q1 = ?PQ:in(a, 1, Q),
    ?assertEqual(true, ?PQ:is_queue(Q1)),
    ?assertEqual(false, ?PQ:is_queue(bad_queue)).

t_is_empty(_) ->
    Q = ?PQ:new(),
    ?assertEqual(true, ?PQ:is_empty(Q)),
    ?assertEqual(false, ?PQ:is_empty(?PQ:in(a, Q))).

t_len(_) ->
    Q = ?PQ:new(),
    Q1 = ?PQ:in(a, Q),
    ?assertEqual(1, ?PQ:len(Q1)),
    Q2 = ?PQ:in(b, 1, Q1),
    ?assertEqual(2, ?PQ:len(Q2)).

t_plen(_) ->
    Q = ?PQ:new(),
    Q1 = ?PQ:in(a, Q),
    ?assertEqual(1, ?PQ:plen(0, Q1)),
    ?assertEqual(0, ?PQ:plen(1, Q1)),
    Q2 = ?PQ:in(b, 1, Q1),
    Q3 = ?PQ:in(c, 1, Q2),
    ?assertEqual(2, ?PQ:plen(1, Q3)),
    ?assertEqual(1, ?PQ:plen(0, Q3)),
    ?assertEqual(0, ?PQ:plen(0, {pqueue, []})).

t_to_list(_) ->
    Q = ?PQ:new(),
    ?assertEqual([], ?PQ:to_list(Q)),

    Q1 = ?PQ:in(a, Q),
    L1 = ?PQ:to_list(Q1),
    ?assertEqual([{0, a}], L1),

    Q2 = ?PQ:in(b, 1, Q1),
    L2 = ?PQ:to_list(Q2),
    ?assertEqual([{1, b}, {0, a}], L2).

t_from_list(_) ->
    Q = ?PQ:from_list([{1, c}, {1, d}, {0, a}, {0, b}]),
    ?assertEqual({pqueue, [{-1, {queue, [d], [c], 2}}, {0, {queue, [b], [a], 2}}]}, Q),
    ?assertEqual(true, ?PQ:is_queue(Q)),
    ?assertEqual(4, ?PQ:len(Q)).

t_in(_) ->
    Q = ?PQ:new(),
    Els = [a, b, {c, 1}, {d, 1}, {e, infinity}, {f, 2}],
    Q1 = lists:foldl(
            fun({El, P}, Acc) ->
                    ?PQ:in(El, P, Acc);
                (El, Acc) ->
                    ?PQ:in(El, Acc)
            end, Q, Els),
    ?assertEqual({pqueue, [{infinity, {queue, [e], [], 1}},
                           {-2, {queue, [f], [], 1}},
                           {-1, {queue, [d], [c], 2}},
                           {0, {queue, [b], [a], 2}}]}, Q1).

t_out(_) ->
    Q = ?PQ:new(),
    {empty, Q} = ?PQ:out(Q),
    {empty, Q} = ?PQ:out(0, Q),
    try ?PQ:out(1, Q) of
        _ -> ct:fail(should_throw_error)
    catch error:Reason ->
        ?assertEqual(Reason, badarg)
    end,
    {{value, a}, Q} = ?PQ:out(?PQ:from_list([{0, a}])),
    {{value, a}, {queue, [], [b], 1}} = ?PQ:out(?PQ:from_list([{0, a}, {0, b}])),
    {{value, a}, {queue, [], [], 0}} = ?PQ:out({queue, [], [a], 1}),
    {{value, a}, {queue, [c], [b], 2}} = ?PQ:out({queue, [c, b], [a], 3}),
    {{value, a}, {queue, [e, d], [b, c], 4}} = ?PQ:out({queue, [e, d, c, b], [a], 5}),
    {{value, a}, {queue, [c], [b], 2}} = ?PQ:out({queue, [c, b, a], [], 3}),
    {{value, a}, {queue, [d, c], [b], 3}} = ?PQ:out({queue, [d, c], [a, b], 4}),
    {{value, a}, {queue, [], [], 0}} = ?PQ:out(?PQ:from_list([{1, a}])),
    {{value, a}, {queue, [c], [b], 2}} = ?PQ:out(?PQ:from_list([{1, a}, {0, b}, {0, c}])),
    {{value, a}, {pqueue, [{-1, {queue, [b], [], 1}}]}} = ?PQ:out(?PQ:from_list([{1, b}, {2, a}])),
    {{value, a}, {pqueue, [{-1, {queue, [], [b], 1}}]}} = ?PQ:out(?PQ:from_list([{1, a}, {1, b}])).

t_out_2(_) ->
    {empty, {pqueue, [{-1, {queue, [a], [], 1}}]}} = ?PQ:out(0, ?PQ:from_list([{1, a}])),
    {{value, a}, {queue, [], [], 0}} = ?PQ:out(1, ?PQ:from_list([{1, a}])),
    {{value, a}, {pqueue, [{-1, {queue, [], [b], 1}}]}} = ?PQ:out(1, ?PQ:from_list([{1, a}, {1, b}])),
    {{value, a}, {queue, [b], [], 1}} = ?PQ:out(1, ?PQ:from_list([{1, a}, {0, b}])).

t_out_p(_) ->
    {empty, {queue, [], [], 0}} = ?PQ:out_p(?PQ:new()),
    {{value, a, 1}, {queue, [b], [], 1}} = ?PQ:out_p(?PQ:from_list([{1, a}, {0, b}])).

t_join(_) ->
    Q = ?PQ:in(a, ?PQ:new()),
    Q = ?PQ:join(Q, ?PQ:new()),
    Q = ?PQ:join(?PQ:new(), Q),

    Q1 = ?PQ:in(a, ?PQ:new()),
    Q2 = ?PQ:in(b, Q1),
    Q3 = ?PQ:in(c, Q2),
    {queue,[c,b],[a],3} = Q3,

    Q4 = ?PQ:in(x, ?PQ:new()),
    Q5 = ?PQ:in(y, Q4),
    Q6 = ?PQ:in(z, Q5),
    {queue,[z,y],[x],3} = Q6,

    {queue,[z,y],[a,b,c,x],6} = ?PQ:join(Q3, Q6),

    PQueue1 = ?PQ:from_list([{1, c}, {1, d}]),
    PQueue2 = ?PQ:from_list([{1, c}, {1, d}, {0, a}, {0, b}]),
    PQueue3 = ?PQ:from_list([{1, c}, {1, d}, {-1, a}, {-1, b}]),

    {pqueue,[{-1,{queue,[d],[c],2}},
             {0,{queue,[z,y],[x],3}}]} = ?PQ:join(PQueue1, Q6),
    {pqueue,[{-1,{queue,[d],[c],2}},
             {0,{queue,[z,y],[x],3}}]} = ?PQ:join(Q6, PQueue1),

    {pqueue,[{-1,{queue,[d],[c],2}},
             {0,{queue,[z,y],[a,b,x],5}}]} = ?PQ:join(PQueue2, Q6),
    {pqueue,[{-1,{queue,[d],[c],2}},
             {0,{queue,[b],[x,y,z,a],5}}]} = ?PQ:join(Q6, PQueue2),

    {pqueue,[{-1,{queue,[d],[c],2}},
             {0,{queue,[z,y],[x],3}},
             {1,{queue,[b],[a],2}}]} = ?PQ:join(PQueue3, Q6),
    {pqueue,[{-1,{queue,[d],[c],2}},
             {0,{queue,[z,y],[x],3}},
             {1,{queue,[b],[a],2}}]} = ?PQ:join(Q6, PQueue3),

    PQueue4 = ?PQ:from_list([{1, c}, {1, d}]),
    PQueue5 = ?PQ:from_list([{2, a}, {2, b}]),
    {pqueue,[{-2,{queue,[b],[a],2}},
             {-1,{queue,[d],[c],2}}]} = ?PQ:join(PQueue4, PQueue5).

t_filter(_) ->
    {pqueue, [{-2, {queue, [10], [4], 2}},
              {-1, {queue, [2], [], 1}}]} = 
        ?PQ:filter(fun(V) when V rem 2 =:= 0 ->
                       true;
                      (_) ->
                       false
                   end, ?PQ:from_list([{0, 1}, {0, 3}, {1, 2}, {2, 4}, {2, 10}])).

t_highest(_) ->
    empty = ?PQ:highest(?PQ:new()),
    0 = ?PQ:highest(?PQ:from_list([{0, a}, {0, b}])),
    2 = ?PQ:highest(?PQ:from_list([{0, a}, {0, b}, {1, c}, {2, d}, {2, e}])).