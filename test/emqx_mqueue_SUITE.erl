%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include_lib("proper/include/proper.hrl").
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
    {{value, _Msg}, Q7} = ?Q:out(Q6),
    ?assertEqual(4, ?Q:len(Q7)).

t_priority_mqueue_conservation(_) ->
    true = proper:quickcheck(conservation_prop()).

t_priority_order(_) ->
    Opts = #{max_len => 5,
             shift_multiplier => 1,
             priorities =>
                 #{<<"t1">> => 0,
                   <<"t2">> => 1,
                   <<"t3">> => 2
                  },
            store_qos0 => false
            },
    Messages = [{Topic, Message} ||
                   Topic <- [<<"t1">>, <<"t2">>, <<"t3">>],
                   Message <- lists:seq(1, 10)],
    Q = lists:foldl(fun({Topic, Message}, Q) ->
                            element(2, ?Q:in(#message{topic = Topic, qos = 1, payload = Message}, Q))
                    end,
                    ?Q:init(Opts),
                    Messages),
    ?assertMatch([{<<"t3">>, 6},
                  {<<"t3">>, 7},
                  {<<"t3">>, 8},

                  {<<"t2">>, 6},
                  {<<"t2">>, 7},

                  {<<"t1">>, 6},

                  {<<"t3">>, 9},
                  {<<"t3">>, 10},

                  {<<"t2">>, 8},

                  %% Note: for performance reasons we don't reset the
                  %% counter when we run out of messages with the
                  %% current prio, so next is t1:
                  {<<"t1">>, 7},

                  {<<"t2">>, 9},
                  {<<"t2">>, 10},

                  {<<"t1">>, 8},
                  {<<"t1">>, 9},
                  {<<"t1">>, 10}
                 ], drain(Q)).

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

t_live_upgrade(_) ->
    Q = {mqueue,true,1,0,0,none,0,
                   {queue,[],[],0}},
    ?assertMatch(#{}, ?Q:info(Q)),
    ?assertMatch(true, ?Q:is_empty(Q)),
    ?assertMatch(0, ?Q:len(Q)),
    ?assertMatch(1, ?Q:max_len(Q)),
    ?assertMatch({undefined, _}, ?Q:in(#message{qos = 0, topic = <<>>}, Q)),
    ?assertMatch({empty, _}, ?Q:out(Q)),
    ?assertMatch([_|_], ?Q:stats(Q)),
    ?assertMatch(0, ?Q:dropped(Q)).


t_live_upgrade2(_) ->
    Q = {mqueue,false,10,0,0,
                   #{<<"t">> => 1},
                   0,
                   {queue,[],[],0}},
    ?assertMatch(#{}, ?Q:info(Q)),
    ?assertMatch(true, ?Q:is_empty(Q)),
    ?assertMatch(0, ?Q:len(Q)),
    ?assertMatch(10, ?Q:max_len(Q)),
    ?assertMatch({_, _}, ?Q:in(#message{qos = 0, topic = <<>>}, Q)),
    ?assertMatch({empty, _}, ?Q:out(Q)),
    ?assertMatch([_|_], ?Q:stats(Q)),
    ?assertMatch(0, ?Q:dropped(Q)).

conservation_prop() ->
    ?FORALL({Priorities, Messages},
            ?LET(Priorities, topic_priorities(),
                 {Priorities, messages(Priorities)}),
            try
                Opts = #{max_len => 0,
                         priorities => maps:from_list(Priorities),
                         store_qos0 => false},
                %% Put messages in
                Q1 = lists:foldl(fun({Topic, Message}, Q) ->
                                         element(2, ?Q:in(#message{topic = Topic, qos = 1, payload = Message}, Q))
                                 end,
                                 ?Q:init(Opts),
                                 Messages),
                %% Collect messages
                Got = lists:sort(drain(Q1)),
                Expected = lists:sort(Messages),
                case Expected =:= Got of
                    true ->
                        true;
                    false ->
                        ct:pal("Mismatch: expected ~p~nGot ~p~n", [Expected, Got]),
                        false
                end
            catch
                EC:Err:Stack ->
                    ct:pal("Error: ~p", [{EC, Err, Stack}]),
                    false
            end).

%% Proper generators:

topic(Priorities) ->
    {Topics, _} = lists:unzip(Priorities),
    oneof(Topics).

topic_priorities() ->
    non_empty(list({binary(), priority()})).

priority() ->
    oneof([integer(), infinity]).

messages(Topics) ->
    list({topic(Topics), binary()}).

%% Internal functions:

drain(Q) ->
    case ?Q:out(Q) of
        {empty, _} ->
            [];
        {{value, #message{topic = T, payload = P}}, Q1} ->
            [{T, P}|drain(Q1)]
    end.
