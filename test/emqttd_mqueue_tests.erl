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

-module(emqttd_mqueue_tests).

-include("emqttd.hrl").

-define(Q, emqttd_mqueue).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

in_test() ->
    Opts = [{max_length, 5},
            {queue_qos0, true}],
    Q = ?Q:new(<<"testQ">>, Opts, alarm_fun()),
    ?assertEqual(true, ?Q:is_empty(Q)),
    Q1 = ?Q:in(#mqtt_message{}, Q),
    ?assertEqual(1, ?Q:len(Q1)),
    Q2 = ?Q:in(#mqtt_message{qos = 1}, Q1),
    ?assertEqual(2, ?Q:len(Q2)),
    Q3 = ?Q:in(#mqtt_message{qos = 2}, Q2),
    Q4 = ?Q:in(#mqtt_message{}, Q3),
    Q5 = ?Q:in(#mqtt_message{}, Q4),
    ?assertEqual(5, ?Q:len(Q5)).
    
in_qos0_test() ->
    Opts = [{max_length, 5},
            {queue_qos0, false}],
    Q = ?Q:new(<<"testQ">>, Opts, alarm_fun()),
    Q1 = ?Q:in(#mqtt_message{}, Q),
    ?assertEqual(true, ?Q:is_empty(Q1)),
    Q2 = ?Q:in(#mqtt_message{qos = 0}, Q1),
    ?assertEqual(true, ?Q:is_empty(Q2)).

out_test() ->
    Opts = [{max_length, 5},
            {queue_qos0, true}],
    Q = ?Q:new(<<"testQ">>, Opts, alarm_fun()),
    ?assertMatch({empty, Q}, ?Q:out(Q)),
    Q1 = ?Q:in(#mqtt_message{}, Q),
    {Value, Q2} = ?Q:out(Q1),
    ?assertEqual(0, ?Q:len(Q2)),
    ?assertMatch({value, #mqtt_message{}}, Value).

simple_mqueue_test() ->
    Opts = [{type, simple},
            {max_length, 3},
            {low_watermark, 0.2},
            {high_watermark, 0.6},
            {queue_qos0, false}],
    Q = ?Q:new("simple_queue", Opts, alarm_fun()),

    ?assert(?Q:is_empty(Q)),
    Q1 = ?Q:in(#mqtt_message{qos = 1, payload = <<"1">>}, Q),
    Q2 = ?Q:in(#mqtt_message{qos = 1, payload = <<"2">>}, Q1),
    Q3 = ?Q:in(#mqtt_message{qos = 1, payload = <<"3">>}, Q2),
    Q4 = ?Q:in(#mqtt_message{qos = 1, payload = <<"4">>}, Q3),
    ?assertEqual(3, ?Q:len(Q4)),
    {{value, Msg}, Q5} = ?Q:out(Q4),
    ?assertMatch(<<"2">>, Msg#mqtt_message.payload).

priority_mqueue_test() ->
    Opts = [{type, priority},
            {priority, [{<<"t">>, 10}]},
            {max_length, 3},
            {low_watermark, 0.2},
            {high_watermark, 0.6},
            {queue_qos0, false}],
    Q = ?Q:new("priority_queue", Opts, alarm_fun()),

    ?assert(?Q:is_empty(Q)),
    Q1 = ?Q:in(#mqtt_message{qos = 1, topic = <<"t1">>}, Q),
    Q2 = ?Q:in(#mqtt_message{qos = 1, topic = <<"t">>}, Q1),
    Q3 = ?Q:in(#mqtt_message{qos = 1, topic = <<"t2">>}, Q2),
    ?assertEqual(3, ?Q:len(Q3)),
    Q4 = ?Q:in(#mqtt_message{qos = 1, topic = <<"t1">>}, Q3),
    ?assertEqual(4, ?Q:len(Q4)),
    Q5 = ?Q:in(#mqtt_message{qos = 1, topic = <<"t1">>}, Q4),
    ?assertEqual(5, ?Q:len(Q5)),
    Q6 = ?Q:in(#mqtt_message{qos = 1, topic = <<"t1">>}, Q5),
    ?assertEqual(5, ?Q:len(Q6)),
    {{value, Msg}, Q7} = ?Q:out(Q6),
    ?assertMatch(<<"t">>, Msg#mqtt_message.topic).
 
alarm_fun() -> fun(_, _) -> alarm_fun() end.

-endif.

