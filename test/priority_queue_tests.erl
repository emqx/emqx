%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2016, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------

-module(priority_queue_tests).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(PQ, priority_queue).

plen_test() ->
    Q = ?PQ:new(),
    ?assertEqual(0, ?PQ:plen(0, Q)),
    Q0 = ?PQ:in(z, Q),
    ?assertEqual(1, ?PQ:plen(0, Q0)),
    Q1 = ?PQ:in(x, 1, Q0),
    ?assertEqual(1, ?PQ:plen(1, Q1)),
    Q2 = ?PQ:in(y, 2, Q1),
    ?assertEqual(1, ?PQ:plen(2, Q2)),
    Q3 = ?PQ:in(z, 2, Q2),
    ?assertEqual(2, ?PQ:plen(2, Q3)),
    {_, Q4} = ?PQ:out(1, Q3),
    ?assertEqual(0, ?PQ:plen(1, Q4)),
    {_, Q5} = ?PQ:out(Q4),
    ?assertEqual(1, ?PQ:plen(2, Q5)),
    {_, Q6} = ?PQ:out(Q5),
    ?assertEqual(0, ?PQ:plen(2, Q6)),
    ?assertEqual(1, ?PQ:len(Q6)),
    {_, Q7} = ?PQ:out(Q6),
    ?assertEqual(0, ?PQ:len(Q7)).

out2_test() ->
    Els = [a, {b, 1}, {c, 1}, {d, 2}, {e, 2}, {f, 2}],
    Q  = ?PQ:new(),
    Q0 = lists:foldl(
            fun({El, P}, Q) ->
                    ?PQ:in(El, P, Q);
                (El, Q) ->
                    ?PQ:in(El, Q)
            end, Q, Els),
    {Val, Q1} = ?PQ:out(Q0),
    ?assertEqual({value, d}, Val),
    {Val1, Q2} = ?PQ:out(2, Q1),
    ?assertEqual({value, e}, Val1),
    {Val2, Q3} = ?PQ:out(1, Q2),
    ?assertEqual({value, b}, Val2),
    {Val3, Q4} = ?PQ:out(Q3),
    ?assertEqual({value, f}, Val3),
    {Val4, Q5} = ?PQ:out(Q4),
    ?assertEqual({value, c}, Val4),
    {Val5, Q6} = ?PQ:out(Q5),
    ?assertEqual({value, a}, Val5),
    {empty, _Q7} = ?PQ:out(Q6).

-endif.

