%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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

-module(emqttd_mqueue_tests).

-include("emqttd.hrl").

-define(QM, emqttd_mqueue).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

in_test() ->
    Opts = [{max_length, 5},
            {queue_qos0, true}],
    Q = ?QM:new(<<"testQ">>, Opts, alarm_fun()),
    ?assertEqual(true, ?QM:is_empty(Q)),
    Q1 = ?QM:in(#mqtt_message{}, Q),
    ?assertEqual(1, ?QM:len(Q1)),
    Q2 = ?QM:in(#mqtt_message{qos = 1}, Q1),
    ?assertEqual(2, ?QM:len(Q2)),
    Q3 = ?QM:in(#mqtt_message{qos = 2}, Q2),
    Q4 = ?QM:in(#mqtt_message{}, Q3),
    Q5 = ?QM:in(#mqtt_message{}, Q4),
    ?assertEqual(true, ?QM:is_full(Q5)).
    
in_qos0_test() ->
    Opts = [{max_length, 5},
            {queue_qos0, false}],
    Q = ?QM:new(<<"testQ">>, Opts, alarm_fun()),
    Q1 = ?QM:in(#mqtt_message{}, Q),
    ?assertEqual(true, ?QM:is_empty(Q1)),
    Q2 = ?QM:in(#mqtt_message{qos = 0}, Q1),
    ?assertEqual(true, ?QM:is_empty(Q2)).

out_test() ->
    Opts = [{max_length, 5},
            {queue_qos0, true}],
    Q = ?QM:new(<<"testQ">>, Opts, alarm_fun()),
    ?assertMatch({empty, Q}, ?QM:out(Q)),
    Q1 = ?QM:in(#mqtt_message{}, Q),
    {Value, Q2} = ?QM:out(Q1),
    ?assertEqual(0, ?QM:len(Q2)),
    ?assertMatch({value, #mqtt_message{}}, Value).
 
alarm_fun() ->
   fun(_, _) -> alarm_fun() end.

-endif.


