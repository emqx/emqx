%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() -> emqx_common_test_helpers:all(?MODULE).

t_info(_) ->
    Q = ?Q:init(#{max_len => 5, store_qos0 => true}),
    true = ?Q:info(store_qos0, Q),
    5 = ?Q:info(max_len, Q),
    0 = ?Q:info(len, Q),
    0 = ?Q:info(dropped, Q),
    #{
        store_qos0 := true,
        max_len := 5,
        len := 0,
        dropped := 0
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
        end,
        Q,
        lists:seq(1, 255)
    ),
    ?assertEqual(255, ?Q:len(Qx)),
    ?assertEqual([{len, 255}, {max_len, 0}, {dropped, 0}], ?Q:stats(Qx)),
    {{value, V}, _Qy} = ?Q:out(Qx),
    ?assertEqual(<<1>>, V#message.payload).

t_priority_mqueue(_) ->
    Opts = #{
        max_len => 3,
        priorities =>
            #{
                <<"t1">> => 1,
                <<"t2">> => 2,
                <<"t3">> => 3
            },
        store_qos0 => false
    },
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
    Opts = #{
        max_len => 5,
        shift_multiplier => 1,
        priorities =>
            #{
                <<"t1">> => 0,
                <<"t2">> => 1,
                <<"t3">> => 2
            },
        store_qos0 => false
    },
    Messages = [
        {Topic, Message}
     || Topic <- [<<"t1">>, <<"t2">>, <<"t3">>],
        Message <- lists:seq(1, 10)
    ],
    Q = lists:foldl(
        fun({Topic, Message}, Q) ->
            element(2, ?Q:in(#message{topic = Topic, qos = 1, payload = Message}, Q))
        end,
        ?Q:init(Opts),
        Messages
    ),
    ?assertMatch(
        [
            {<<"t3">>, 6},
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
        ],
        drain(Q)
    ).

t_priority_order2(_) ->
    Opts = #{
        max_len => 5,
        shift_multiplier => 2,
        priorities =>
            #{
                <<"t1">> => 0,
                <<"t2">> => 1
            },
        store_qos0 => false
    },
    Messages = [
        {Topic, Message}
     || Topic <- [<<"t1">>, <<"t2">>],
        Message <- lists:seq(1, 10)
    ],
    Q = lists:foldl(
        fun({Topic, Message}, Q) ->
            element(2, ?Q:in(#message{topic = Topic, qos = 1, payload = Message}, Q))
        end,
        ?Q:init(Opts),
        Messages
    ),
    ?assertMatch(
        [
            {<<"t2">>, 6},
            {<<"t2">>, 7},
            {<<"t2">>, 8},
            {<<"t2">>, 9},

            {<<"t1">>, 6},
            {<<"t1">>, 7},

            {<<"t2">>, 10},

            {<<"t1">>, 8},
            {<<"t1">>, 9},
            {<<"t1">>, 10}
        ],
        drain(Q)
    ).

t_infinity_priority_mqueue(_) ->
    Opts = #{
        max_len => 0,
        priorities =>
            #{
                <<"t">> => 1,
                <<"t1">> => 2
            },
        store_qos0 => false
    },
    Q = ?Q:init(Opts),
    ?assertEqual(0, ?Q:max_len(Q)),
    Qx = lists:foldl(
        fun(I, AccQ) ->
            {undefined, AccQ1} = ?Q:in(
                #message{topic = <<"t1">>, qos = 1, payload = iolist_to_binary([I])}, AccQ
            ),
            {undefined, AccQ2} = ?Q:in(
                #message{topic = <<"t">>, qos = 1, payload = iolist_to_binary([I])}, AccQ1
            ),
            AccQ2
        end,
        Q,
        lists:seq(1, 255)
    ),
    ?assertEqual(510, ?Q:len(Qx)),
    ?assertEqual([{len, 510}, {max_len, 0}, {dropped, 0}], ?Q:stats(Qx)).

%%TODO: fixme later
t_length_priority_mqueue(_) ->
    Opts = #{
        max_len => 2,
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

t_query(_) ->
    EmptyQ = ?Q:init(#{max_len => 500, store_qos0 => true}),
    ?assertEqual({[], #{position => none, start => none}}, ?Q:query(EmptyQ, #{limit => 50})),
    RandPos = {erlang:system_time(nanosecond), 0},
    ?assertEqual(
        {[], #{position => RandPos, start => none}},
        ?Q:query(EmptyQ, #{position => RandPos, limit => 50})
    ),
    ?assertEqual(
        {[], #{position => none, start => none}},
        ?Q:query(EmptyQ, #{continuation => none, limit => 50})
    ),

    Q = lists:foldl(
        fun(Seq, QAcc) ->
            Msg = emqx_message:make(<<"t">>, integer_to_binary(Seq)),
            {_, QAcc1} = ?Q:in(Msg, QAcc),
            QAcc1
        end,
        EmptyQ,
        lists:seq(1, 114)
    ),

    {LastPos, LastStart} = lists:foldl(
        fun(PageSeq, {Pos, PrevStart}) ->
            Limit = 10,
            PagerParams = #{position => Pos, limit => Limit},
            {Page, #{position := NextPos, start := Start}} = ?Q:query(Q, PagerParams),
            ?assertEqual(10, length(Page)),
            ExpFirstPayload = integer_to_binary(PageSeq * Limit - Limit + 1),
            ExpLastPayload = integer_to_binary(PageSeq * Limit),
            FirstMsg = lists:nth(1, Page),
            LastMsg = lists:nth(10, Page),
            ?assertEqual(ExpFirstPayload, emqx_message:payload(FirstMsg)),
            ?assertEqual(ExpLastPayload, emqx_message:payload(LastMsg)),
            %% start value must not change as Mqueue is not modified during traversal
            NextStart =
                case PageSeq of
                    1 ->
                        ?assertEqual({mqueue_ts(FirstMsg), 0}, Start),
                        Start;
                    _ ->
                        ?assertEqual(PrevStart, Start),
                        PrevStart
                end,
            {NextPos, NextStart}
        end,
        {none, none},
        lists:seq(1, 11)
    ),

    {LastPartialPage, #{position := FinalPos} = LastMeta} = ?Q:query(Q, #{
        position => LastPos, limit => 10
    }),
    LastMsg = lists:nth(4, LastPartialPage),
    ?assertEqual(4, length(LastPartialPage)),
    ?assertEqual(<<"111">>, emqx_message:payload(lists:nth(1, LastPartialPage))),
    ?assertEqual(<<"114">>, emqx_message:payload(LastMsg)),
    ?assertEqual(#{position => {mqueue_ts(LastMsg), 0}, start => LastStart}, LastMeta),
    ?assertEqual(
        {[], #{start => LastStart, position => FinalPos}},
        ?Q:query(Q, #{position => FinalPos, limit => 10})
    ),

    {LargePage, LargeMeta} = ?Q:query(Q, #{position => none, limit => 1000}),
    ?assertEqual(114, length(LargePage)),
    ?assertEqual(<<"1">>, emqx_message:payload(hd(LargePage))),
    ?assertEqual(<<"114">>, emqx_message:payload(lists:last(LargePage))),
    ?assertEqual(#{start => LastStart, position => FinalPos}, LargeMeta),

    {FullPage, FullMeta} = ?Q:query(Q, #{position => none, limit => 114}),
    ?assertEqual(LargePage, FullPage),
    ?assertEqual(LargeMeta, FullMeta),

    {_, Q1} = emqx_mqueue:out(Q),
    {PageAfterRemove, #{start := StartAfterRemove}} = ?Q:query(Q1, #{position => none, limit => 10}),
    ?assertEqual(<<"2">>, emqx_message:payload(hd(PageAfterRemove))),
    ?assertEqual(StartAfterRemove, {mqueue_ts(hd(PageAfterRemove)), 0}).

t_query_with_priorities(_) ->
    Priorities = #{<<"t/infinity">> => infinity, <<"t/10">> => 10, <<"t/5">> => 5},
    EmptyQ = ?Q:init(#{max_len => 500, store_qos0 => true, priorities => Priorities}),

    ?assertEqual({[], #{position => none, start => none}}, ?Q:query(EmptyQ, #{limit => 50})),
    RandPos = {erlang:system_time(nanosecond), 0},
    ?assertEqual(
        {[], #{position => RandPos, start => none}},
        ?Q:query(EmptyQ, #{position => RandPos, limit => 50})
    ),
    ?assertEqual(
        {[], #{position => none, start => none}},
        ?Q:query(EmptyQ, #{continuation => none, limit => 50})
    ),

    {Q, ExpMsgsAcc} = lists:foldl(
        fun(Topic, {QAcc, MsgsAcc}) ->
            {TopicQ, TopicMsgs} =
                lists:foldl(
                    fun(Seq, {TopicQAcc, TopicMsgsAcc}) ->
                        Payload = <<Topic/binary, "_", (integer_to_binary(Seq))/binary>>,
                        Msg = emqx_message:make(Topic, Payload),
                        {_, TopicQAcc1} = ?Q:in(Msg, TopicQAcc),
                        {TopicQAcc1, [Msg | TopicMsgsAcc]}
                    end,
                    {QAcc, []},
                    lists:seq(1, 10)
                ),
            {TopicQ, [lists:reverse(TopicMsgs) | MsgsAcc]}
        end,
        {EmptyQ, []},
        [<<"t/test">>, <<"t/5">>, <<"t/infinity">>, <<"t/10">>]
    ),

    %% Manual resorting from the highest to the lowest priority
    [ExpMsgsPrio0, ExpMsgsPrio5, ExpMsgsPrioInf, ExpMsgsPrio10] = lists:reverse(ExpMsgsAcc),
    ExpMsgs = ExpMsgsPrioInf ++ ExpMsgsPrio10 ++ ExpMsgsPrio5 ++ ExpMsgsPrio0,
    {AllMsgs, #{start := StartPos, position := Pos}} = ?Q:query(Q, #{position => none, limit => 40}),
    ?assertEqual(40, length(AllMsgs)),
    ?assertEqual(ExpMsgs, with_empty_extra(AllMsgs)),
    FirstMsg = hd(AllMsgs),
    LastMsg = lists:last(AllMsgs),
    ?assertEqual(<<"t/infinity_1">>, emqx_message:payload(FirstMsg)),
    ?assertEqual(StartPos, {mqueue_ts(FirstMsg), infinity}),
    ?assertEqual(<<"t/test_10">>, emqx_message:payload(LastMsg)),
    ?assertMatch({_, 0}, Pos),
    ?assertEqual(Pos, {mqueue_ts(LastMsg), mqueue_prio(LastMsg)}),

    Pos5 = {mqueue_ts(lists:nth(5, AllMsgs)), mqueue_prio(lists:nth(5, AllMsgs))},
    LastInfPos = {mqueue_ts(lists:nth(10, AllMsgs)), mqueue_prio(lists:nth(5, AllMsgs))},

    {MsgsPrioInfTo10, #{start := StartPos, position := PosPrio10Msg5}} = ?Q:query(Q, #{
        position => Pos5, limit => 10
    }),
    ?assertEqual(10, length(MsgsPrioInfTo10)),
    ?assertEqual(<<"t/infinity_6">>, emqx_message:payload(hd(MsgsPrioInfTo10))),
    ?assertEqual(<<"t/10_5">>, emqx_message:payload(lists:last(MsgsPrioInfTo10))),
    ?assertEqual(PosPrio10Msg5, {
        mqueue_ts(lists:last(MsgsPrioInfTo10)), mqueue_prio(lists:last(MsgsPrioInfTo10))
    }),

    {MsgsPrioInfTo5, #{start := StartPos, position := PosPrio5Msg5}} = ?Q:query(Q, #{
        position => Pos5, limit => 20
    }),
    ?assertEqual(20, length(MsgsPrioInfTo5)),
    ?assertEqual(<<"t/infinity_6">>, emqx_message:payload(hd(MsgsPrioInfTo5))),
    ?assertEqual(<<"t/5_5">>, emqx_message:payload(lists:last(MsgsPrioInfTo5))),
    ?assertEqual(PosPrio5Msg5, {
        mqueue_ts(lists:last(MsgsPrioInfTo5)), mqueue_prio(lists:last(MsgsPrioInfTo5))
    }),

    {MsgsPrio10, #{start := StartPos, position := PosPrio10}} = ?Q:query(Q, #{
        position => LastInfPos, limit => 10
    }),
    ?assertEqual(ExpMsgsPrio10, with_empty_extra(MsgsPrio10)),
    ?assertEqual(10, length(MsgsPrio10)),
    ?assertEqual(<<"t/10_1">>, emqx_message:payload(hd(MsgsPrio10))),
    ?assertEqual(<<"t/10_10">>, emqx_message:payload(lists:last(MsgsPrio10))),
    ?assertEqual(PosPrio10, {mqueue_ts(lists:last(MsgsPrio10)), mqueue_prio(lists:last(MsgsPrio10))}),

    {MsgsPrio10To5, #{start := StartPos, position := _}} = ?Q:query(Q, #{
        position => LastInfPos, limit => 20
    }),
    ?assertEqual(ExpMsgsPrio10 ++ ExpMsgsPrio5, with_empty_extra(MsgsPrio10To5)).

conservation_prop() ->
    ?FORALL(
        {Priorities, Messages},
        ?LET(
            Priorities,
            topic_priorities(),
            {Priorities, messages(Priorities)}
        ),
        try
            Opts = #{
                max_len => 0,
                priorities => maps:from_list(Priorities),
                store_qos0 => false
            },
            %% Put messages in
            Q1 = lists:foldl(
                fun({Topic, Message}, Q) ->
                    element(2, ?Q:in(#message{topic = Topic, qos = 1, payload = Message}, Q))
                end,
                ?Q:init(Opts),
                Messages
            ),
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
        end
    ).

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
            [{T, P} | drain(Q1)]
    end.

mqueue_ts(#message{extra = #{mqueue_insert_ts := Ts}}) -> Ts.
mqueue_prio(#message{extra = #{mqueue_priority := Prio}}) -> Prio.

with_empty_extra(Msgs) ->
    [M#message{extra = #{}} || M <- Msgs].
