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

-module(emqx_mqueue_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(Q, emqx_mqueue).

all() -> emqx_ct:all(?MODULE).

t_info(_) ->
    Q = ?Q:init(#{max_len => 5, store_qos0 => true}),
    true = ?Q:info(store_qos0, Q),
    5 = ?Q:info(max_len, Q),
    0 = ?Q:info(len, Q),
    0 = ?Q:info(dropped, Q),
    #{store_qos0 := true,
      max_len    := 5,
      len        := 0,
      dropped    := 0
     } = ?Q:info(Q).

t_in(_) ->
    Opts = #{max_len => 5, store_qos0 => true},
    Q = ?Q:init(Opts),
    ?assert(?Q:is_empty(Q)),
    {_, Q1} = ?Q:in(#message{}, Q),
    ?assertEqual(1, ?Q:len(Q1)),
    {_, Q2} = ?Q:in(#message{qos = 1}, Q1),
    ?assertEqual(2, ?Q:len(Q2)),
    {_, Q3} = ?Q:in(#message{qos = 2}, Q2),
    {_, Q4} = ?Q:in(#message{}, Q3),
    {_, Q5} = ?Q:in(#message{}, Q4),
    ?assertEqual(5, ?Q:len(Q5)).

t_in_qos0(_) ->
    Opts = #{max_len => 5, store_qos0 => false},
    Q = ?Q:init(Opts),
    {_, Q1} = ?Q:in(#message{qos = 0}, Q),
    ?assert(?Q:is_empty(Q1)),
    {_, Q2} = ?Q:in(#message{qos = 0}, Q1),
    ?assert(?Q:is_empty(Q2)).

t_out(_) ->
    Opts = #{max_len => 5, store_qos0 => true},
    Q = ?Q:init(Opts),
    {empty, Q} = ?Q:out(Q),
    {_, Q1} = ?Q:in(#message{}, Q),
    {Value, Q2} = ?Q:out(Q1),
    ?assertEqual(0, ?Q:len(Q2)),
    ?assertEqual({value, #message{}}, Value).

t_simple_mqueue(_) ->
    Opts = #{max_len => 3, store_qos0 => false},
    Q = ?Q:init(Opts),
    ?assertEqual(3, ?Q:max_len(Q)),
    ?assert(?Q:is_empty(Q)),
    {_, Q1} = ?Q:in(#message{qos = 1, payload = <<"1">>}, Q),
    {_, Q2} = ?Q:in(#message{qos = 1, payload = <<"2">>}, Q1),
    {_, Q3} = ?Q:in(#message{qos = 1, payload = <<"3">>}, Q2),
    {_, Q4} = ?Q:in(#message{qos = 1, payload = <<"4">>}, Q3),
    ?assertEqual(3, ?Q:len(Q4)),
    {{value, Msg}, Q5} = ?Q:out(Q4),
    ?assertEqual(<<"2">>, Msg#message.payload),
    ?assertEqual([{len, 2}, {max_len, 3}, {dropped, 1}], ?Q:stats(Q5)).

t_infinity_simple_mqueue(_) ->
    Opts = #{max_len => 0, store_qos0 => false},
    Q = ?Q:init(Opts),
    ?assert(?Q:is_empty(Q)),
    ?assertEqual(0, ?Q:max_len(Q)),
    Qx = lists:foldl(
           fun(I, AccQ) ->
                   {_, NewQ} = ?Q:in(#message{qos = 1, payload = iolist_to_binary([I])}, AccQ),
                   NewQ
            end, Q, lists:seq(1, 255)),
    ?assertEqual(255, ?Q:len(Qx)),
    ?assertEqual([{len, 255}, {max_len, 0}, {dropped, 0}], ?Q:stats(Qx)),
    {{value, V}, _Qy} = ?Q:out(Qx),
    ?assertEqual(<<1>>, V#message.payload).

t_priority_mqueue(_) ->
    Opts = #{max_len => 3,
             priorities =>
                #{<<"t1">> => 1,
                  <<"t2">> => 2,
                  <<"t3">> => 3
                 },
             store_qos0 => false},
    Q = ?Q:init(Opts),
    ?assertEqual(3, ?Q:max_len(Q)),
    ?assert(?Q:is_empty(Q)),
    {_, Q1} = ?Q:in(#message{qos = 1, topic = <<"t2">>}, Q),
    {_, Q2} = ?Q:in(#message{qos = 1, topic = <<"t1">>}, Q1),
    {_, Q3} = ?Q:in(#message{qos = 1, topic = <<"t3">>}, Q2),
    ?assertEqual(3, ?Q:len(Q3)),
    {_, Q4} = ?Q:in(#message{qos = 1, topic = <<"t2">>}, Q3),
    ?assertEqual(4, ?Q:len(Q4)),
    {_, Q5} = ?Q:in(#message{qos = 1, topic = <<"t2">>}, Q4),
    ?assertEqual(5, ?Q:len(Q5)),
    {_, Q6} = ?Q:in(#message{qos = 1, topic = <<"t2">>}, Q5),
    ?assertEqual(5, ?Q:len(Q6)),
    {{value, Msg}, Q7} = ?Q:out(Q6),
    ?assertEqual(4, ?Q:len(Q7)),
    ?assertEqual(<<"t3">>, Msg#message.topic).

t_infinity_priority_mqueue(_) ->
    Opts = #{max_len => 0,
             priorities =>
                #{<<"t">> => 1,
                  <<"t1">> => 2
                 },
                store_qos0 => false},
    Q = ?Q:init(Opts),
    ?assertEqual(0, ?Q:max_len(Q)),
    Qx = lists:foldl(fun(I, AccQ) ->
                             {undefined, AccQ1} = ?Q:in(#message{topic = <<"t1">>, qos = 1, payload = iolist_to_binary([I])}, AccQ),
                             {undefined, AccQ2} = ?Q:in(#message{topic = <<"t">>, qos = 1, payload = iolist_to_binary([I])}, AccQ1),
                             AccQ2
                     end, Q, lists:seq(1, 255)),
    ?assertEqual(510, ?Q:len(Qx)),
    ?assertEqual([{len, 510}, {max_len, 0}, {dropped, 0}], ?Q:stats(Qx)).

%%TODO: fixme later
t_length_priority_mqueue(_) ->
    Opts = #{max_len => 2,
             store_qos0 => false
            },
    Q = ?Q:init(Opts),
    2 = ?Q:max_len(Q),
    {_, Q1} = ?Q:in(#message{topic = <<"x">>, qos = 1, payload = <<1>>}, Q),
    {_, Q2} = ?Q:in(#message{topic = <<"x">>, qos = 1, payload = <<2>>}, Q1),
    {_, Q3} = ?Q:in(#message{topic = <<"y">>, qos = 1, payload = <<3>>}, Q2),
    {_, Q4} = ?Q:in(#message{topic = <<"y">>, qos = 1, payload = <<4>>}, Q3),
    ?assertEqual(2, ?Q:len(Q4)),
    {{value, _Val}, Q5} = ?Q:out(Q4),
    ?assertEqual(1, ?Q:len(Q5)).

t_dropped(_) ->
    Q = ?Q:init(#{max_len => 1, store_qos0 => true}),
    Msg = emqx_message:make(<<"t">>, <<"payload">>),
    {undefined, Q1} = ?Q:in(Msg, Q),
    {Msg, Q2} = ?Q:in(Msg, Q1),
    ?assertEqual(1, ?Q:dropped(Q2)).

