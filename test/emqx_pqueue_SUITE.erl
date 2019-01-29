%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_pqueue_SUITE).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(PQ, emqx_pqueue).

all() -> [t_priority_queue_plen, t_priority_queue_out2, t_priority_queues].

t_priority_queue_plen(_) ->
    Q = ?PQ:new(),
    0 = ?PQ:plen(0, Q),
    Q0 = ?PQ:in(z, Q),
    1 = ?PQ:plen(0, Q0),
    Q1 = ?PQ:in(x, 1, Q0),
    1 = ?PQ:plen(1, Q1),
    Q2 = ?PQ:in(y, 2, Q1),
    1 = ?PQ:plen(2, Q2),
    Q3 = ?PQ:in(z, 2, Q2),
    2 = ?PQ:plen(2, Q3),
    {_, Q4} = ?PQ:out(1, Q3),
    0 = ?PQ:plen(1, Q4),
    {_, Q5} = ?PQ:out(Q4),
    1 = ?PQ:plen(2, Q5),
    {_, Q6} = ?PQ:out(Q5),
    0 = ?PQ:plen(2, Q6),
    1 = ?PQ:len(Q6),
    {_, Q7} = ?PQ:out(Q6),
    0 = ?PQ:len(Q7).

t_priority_queue_out2(_) ->
    Els = [a, {b, 1}, {c, 1}, {d, 2}, {e, 2}, {f, 2}],
    Q  = ?PQ:new(),
    Q0 = lists:foldl(
            fun({El, P}, Acc) ->
                    ?PQ:in(El, P, Acc);
                (El, Acc) ->
                    ?PQ:in(El, Acc)
            end, Q, Els),
    {Val, Q1} = ?PQ:out(Q0),
    {value, d} = Val,
    {Val1, Q2} = ?PQ:out(2, Q1),
    {value, e} = Val1,
    {Val2, Q3} = ?PQ:out(1, Q2),
    {value, b} = Val2,
    {Val3, Q4} = ?PQ:out(Q3),
    {value, f} = Val3,
    {Val4, Q5} = ?PQ:out(Q4),
    {value, c} = Val4,
    {Val5, Q6} = ?PQ:out(Q5),
    {value, a} = Val5,
    {empty, _Q7} = ?PQ:out(Q6).

t_priority_queues(_) ->
    Q0 = ?PQ:new(),
    Q1 = ?PQ:new(),
    PQueue = {pqueue, [{0, Q0}, {1, Q1}]},
    ?assert(?PQ:is_queue(PQueue)),
    [] = ?PQ:to_list(PQueue),

    PQueue1 = ?PQ:in(a, 0, ?PQ:new()),
    PQueue2 = ?PQ:in(b, 0, PQueue1),

    PQueue3 = ?PQ:in(c, 1, PQueue2),
    PQueue4 = ?PQ:in(d, 1, PQueue3),

    4 = ?PQ:len(PQueue4),

    [{1, c}, {1, d}, {0, a}, {0, b}] = ?PQ:to_list(PQueue4),
    PQueue4 = ?PQ:from_list([{1, c}, {1, d}, {0, a}, {0, b}]),
    
    empty = ?PQ:highest(?PQ:new()),
    0 = ?PQ:highest(PQueue1),
    1 = ?PQ:highest(PQueue4),

    PQueue5 = ?PQ:in(e, infinity, PQueue4),
    PQueue6 = ?PQ:in(f, 1, PQueue5),

    {{value, e}, PQueue7} = ?PQ:out(PQueue6),
    {empty, _} = ?PQ:out(0, ?PQ:new()),

    {empty, Q0} = ?PQ:out_p(Q0),

    Q2 = ?PQ:in(a, Q0),
    Q3 = ?PQ:in(b, Q2),
    Q4 = ?PQ:in(c, Q3),

    {{value, a, 0}, _Q5} = ?PQ:out_p(Q4),

    {{value,c,1}, PQueue8} = ?PQ:out_p(PQueue7),

    Q4 = ?PQ:join(Q4, ?PQ:new()),
    Q4 = ?PQ:join(?PQ:new(), Q4),

    {queue, [a], [a], 2} = ?PQ:join(Q2, Q2),

    {pqueue,[{-1,{queue,[f],[d],2}},
             {0,{queue,[a],[a,b],3}}]} = ?PQ:join(PQueue8, Q2),

    {pqueue,[{-1,{queue,[f],[d],2}},
             {0,{queue,[b],[a,a],3}}]} = ?PQ:join(Q2, PQueue8),

    {pqueue,[{-1,{queue,[f],[d,f,d],4}},
             {0,{queue,[b],[a,b,a],4}}]} = ?PQ:join(PQueue8, PQueue8).


