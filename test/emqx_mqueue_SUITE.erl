%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mqueue_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(Q, emqx_mqueue).

all() -> [t_in, t_in_qos0, t_out, t_simple_mqueue, t_infinity_simple_mqueue, 
            t_priority_mqueue, t_infinity_priority_mqueue].

t_in(_) ->
    Opts = #{type => simple, max_len => 5, store_qos0 => true},
    Q = ?Q:new(<<"testQ">>, Opts),
    ?assert(?Q:is_empty(Q)),
    Q1 = ?Q:in(#message{}, Q),
    ?assertEqual(1, ?Q:len(Q1)),
    Q2 = ?Q:in(#message{qos = 1}, Q1),
    ?assertEqual(2, ?Q:len(Q2)),
    Q3 = ?Q:in(#message{qos = 2}, Q2),
    Q4 = ?Q:in(#message{}, Q3),
    Q5 = ?Q:in(#message{}, Q4),
    ?assertEqual(5, ?Q:len(Q5)).

t_in_qos0(_) ->
    Opts = #{type => simple, max_len => 5, store_qos0 => false},
    Q = ?Q:new(<<"testQ">>, Opts),
    Q1 = ?Q:in(#message{qos = 0}, Q),
    ?assert(?Q:is_empty(Q1)),
    Q2 = ?Q:in(#message{qos = 0}, Q1),
    ?assert(?Q:is_empty(Q2)).

t_out(_) ->
    Opts = #{type => simple, max_len => 5, store_qos0 => true},
    Q = ?Q:new(<<"testQ">>, Opts),
    {empty, Q} = ?Q:out(Q),
    Q1 = ?Q:in(#message{}, Q),
    {Value, Q2} = ?Q:out(Q1),
    ?assertEqual(0, ?Q:len(Q2)),
    ?assertEqual({value, #message{}}, Value).

t_simple_mqueue(_) ->
    Opts = #{type => simple, max_len => 3, store_qos0 => false},
    Q = ?Q:new("simple_queue", Opts),
    ?assertEqual(simple, ?Q:type(Q)),
    ?assertEqual(3, ?Q:max_len(Q)),
    ?assertEqual(<<"simple_queue">>, ?Q:name(Q)),
    ?assert(?Q:is_empty(Q)),
    Q1 = ?Q:in(#message{qos = 1, payload = <<"1">>}, Q),
    Q2 = ?Q:in(#message{qos = 1, payload = <<"2">>}, Q1),
    Q3 = ?Q:in(#message{qos = 1, payload = <<"3">>}, Q2),
    Q4 = ?Q:in(#message{qos = 1, payload = <<"4">>}, Q3),
    ?assertEqual(3, ?Q:len(Q4)),
    {{value, Msg}, Q5} = ?Q:out(Q4),
    ?assertEqual(<<"2">>, Msg#message.payload),
    ?assertEqual([{len, 2}, {max_len, 3}, {dropped, 1}], ?Q:stats(Q5)).

t_infinity_simple_mqueue(_) ->
    Opts = #{type => simple, max_len => 0, store_qos0 => false},
    Q = ?Q:new("infinity_simple_queue", Opts),
    ?assert(?Q:is_empty(Q)),
    ?assertEqual(0, ?Q:max_len(Q)),
    Qx = lists:foldl(fun(I, AccQ) ->
                    ?Q:in(#message{qos = 1, payload = iolist_to_binary([I])}, AccQ)
            end, Q, lists:seq(1, 255)),
    ?assertEqual(255, ?Q:len(Qx)),
    ?assertEqual([{len, 255}, {max_len, 0}, {dropped, 0}], ?Q:stats(Qx)),
    {{value, V}, _Qy} = ?Q:out(Qx),
    ?assertEqual(<<1>>, V#message.payload).

t_priority_mqueue(_) ->
    Opts = #{type => priority, max_len => 3, store_qos0 => false},
    Q = ?Q:new("priority_queue", Opts),
    ?assertEqual(priority, ?Q:type(Q)),
    ?assertEqual(3, ?Q:max_len(Q)),
    ?assertEqual(<<"priority_queue">>, ?Q:name(Q)),
    ?assert(?Q:is_empty(Q)),
    Q1 = ?Q:in(#message{qos = 1, topic = <<"t2">>}, Q),
    Q2 = ?Q:in(#message{qos = 1, topic = <<"t1">>}, Q1),
    Q3 = ?Q:in(#message{qos = 1, topic = <<"t3">>}, Q2),
    ?assertEqual(3, ?Q:len(Q3)),
    Q4 = ?Q:in(#message{qos = 1, topic = <<"t2">>}, Q3),
    ?assertEqual(4, ?Q:len(Q4)),
    Q5 = ?Q:in(#message{qos = 1, topic = <<"t2">>}, Q4),
    ?assertEqual(5, ?Q:len(Q5)),
    Q6 = ?Q:in(#message{qos = 1, topic = <<"t2">>}, Q5),
    ?assertEqual(5, ?Q:len(Q6)),
    {{value, Msg}, Q7} = ?Q:out(Q6),
    ?assertEqual(4, ?Q:len(Q7)),
    ?assertEqual(<<"t1">>, Msg#message.topic).

t_infinity_priority_mqueue(_) ->
    Opts = #{type => priority, max_len => 0, store_qos0 => false},
    Q = ?Q:new("infinity_priority_queue", Opts),
    ?assertEqual(0, ?Q:max_len(Q)),
    Qx = lists:foldl(fun(I, AccQ) ->
                    AccQ1 =
                    ?Q:in(#message{topic = <<"t1">>, qos = 1, payload = iolist_to_binary([I])}, AccQ),
                    ?Q:in(#message{topic = <<"t">>, qos = 1, payload = iolist_to_binary([I])}, AccQ1)
            end, Q, lists:seq(1, 255)),
    ?assertEqual(510, ?Q:len(Qx)),
    ?assertEqual([{len, 510}, {max_len, 0}, {dropped, 0}], ?Q:stats(Qx)).