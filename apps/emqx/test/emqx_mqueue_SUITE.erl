%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    {_, Q1} = ?Q:in(#message{payload = <<>>}, Q),
    ?assertEqual(1, ?Q:len(Q1)),
    {_, Q2} = ?Q:in(#message{qos = 1, payload = <<>>}, Q1),
    ?assertEqual(2, ?Q:len(Q2)),
    {_, Q3} = ?Q:in(#message{qos = 2, payload = <<>>}, Q2),
    {_, Q4} = ?Q:in(#message{payload = <<>>}, Q3),
    {_, Q5} = ?Q:in(#message{payload = <<>>}, Q4),
    ?assertEqual(5, ?Q:len(Q5)).

t_in_qos0(_) ->
    Opts = #{max_len => 5, store_qos0 => false},
    Q = ?Q:init(Opts),
    false = ?Q:in(#message{qos = 0, payload = <<>>}, Q),
    ?assertEqual(0, ?Q:payload_bytes(Q)).

t_out(_) ->
    Opts = #{max_len => 5, store_qos0 => true},
    Q = ?Q:init(Opts),
    {empty, Q} = ?Q:out(Q),
    {_, Q1} = ?Q:in(#message{payload = <<"x">>}, Q),
    ?assertEqual(1, ?Q:payload_bytes(Q1)),
    {Value, Q2} = ?Q:out(Q1),
    ?assertEqual(0, ?Q:len(Q2)),
    ?assertEqual(0, ?Q:payload_bytes(Q2)),
    ?assertEqual({value, #message{payload = <<"x">>}}, Value).

t_payload_bytes_tracks_payload(_) ->
    Q0 = ?Q:init(#{max_len => 3, store_qos0 => true}),
    Msg1 = #message{qos = 1, payload = <<"one">>},
    Msg2 = #message{qos = 1, payload = <<"three">>},
    {_, Q1} = ?Q:in(Msg1, Q0),
    ?assertEqual(emqx_message:payload_size(Msg1), ?Q:payload_bytes(Q1)),
    {_, Q2} = ?Q:in(Msg2, Q1),
    ?assertEqual(
        emqx_message:payload_size(Msg1) + emqx_message:payload_size(Msg2),
        ?Q:payload_bytes(Q2)
    ),
    {{value, _}, Q3} = ?Q:out(Q2),
    ?assertEqual(emqx_message:payload_size(Msg2), ?Q:payload_bytes(Q3)).

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

-doc "max_len caps the total queue length, not each priority lane independently.".
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
    {_, Q1} = ?Q:in(#message{qos = 1, topic = <<"t2">>, payload = <<>>}, Q),
    {_, Q2} = ?Q:in(#message{qos = 1, topic = <<"t1">>, payload = <<>>}, Q1),
    {_, Q3} = ?Q:in(#message{qos = 1, topic = <<"t3">>, payload = <<>>}, Q2),
    ?assertEqual(3, ?Q:len(Q3)),
    %% Queue is at the global cap: enqueuing another t2 (prio 2) message must not
    %% grow the total past max_len. The dropped message comes from t1 (prio 1),
    %% the lowest non-empty lane, not from t2's own lane.
    {Dropped4, Q4} = ?Q:in(#message{qos = 1, topic = <<"t2">>, payload = <<>>}, Q3),
    ?assertEqual(<<"t1">>, Dropped4#message.topic),
    ?assertEqual(3, ?Q:len(Q4)),
    %% t1 is now empty; the lowest non-empty lane is t2, which holds the oldest
    %% surviving message.
    {Dropped5, Q5} = ?Q:in(#message{qos = 1, topic = <<"t2">>, payload = <<>>}, Q4),
    ?assertEqual(<<"t2">>, Dropped5#message.topic),
    ?assertEqual(3, ?Q:len(Q5)),
    {{value, Msg}, Q6} = ?Q:out(Q5),
    ?assertEqual(<<"t3">>, Msg#message.topic),
    ?assertEqual(2, ?Q:len(Q6)).

t_priority_mqueue_conservation(_) ->
    true = proper:quickcheck(conservation_prop()).

-doc """
Dequeue order across priority lanes (round-robin with priority-weighted
credits). max_len is unbounded here so the global cap from t_priority_mqueue
does not interact with the scheduling being tested.
""".
t_priority_order(_) ->
    Opts = #{
        max_len => 0,
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
            element(
                2,
                ?Q:in(
                    #message{topic = Topic, qos = 1, payload = integer_to_binary(Message)},
                    Q
                )
            )
        end,
        ?Q:init(Opts),
        Messages
    ),
    ?assertMatch(
        [
            {<<"t3">>, <<"1">>},
            {<<"t3">>, <<"2">>},
            {<<"t3">>, <<"3">>},

            {<<"t2">>, <<"1">>},
            {<<"t2">>, <<"2">>},

            {<<"t1">>, <<"1">>},

            {<<"t3">>, <<"4">>},
            {<<"t3">>, <<"5">>},
            {<<"t3">>, <<"6">>},

            {<<"t2">>, <<"3">>},
            {<<"t2">>, <<"4">>},

            {<<"t1">>, <<"2">>},

            {<<"t3">>, <<"7">>},
            {<<"t3">>, <<"8">>},
            {<<"t3">>, <<"9">>},

            {<<"t2">>, <<"5">>},
            {<<"t2">>, <<"6">>},

            {<<"t1">>, <<"3">>},

            {<<"t3">>, <<"10">>},

            {<<"t2">>, <<"7">>},
            {<<"t2">>, <<"8">>},

            {<<"t1">>, <<"4">>},

            {<<"t2">>, <<"9">>},
            {<<"t2">>, <<"10">>},

            %% t2 and t3 are exhausted; the round-robin drains the rest of t1.
            {<<"t1">>, <<"5">>},
            {<<"t1">>, <<"6">>},
            {<<"t1">>, <<"7">>},
            {<<"t1">>, <<"8">>},
            {<<"t1">>, <<"9">>},
            {<<"t1">>, <<"10">>}
        ],
        drain(Q)
    ).

-doc """
Same as t_priority_order/1, with a different shift_multiplier and negative
priorities. max_len is unbounded so the scheduling test is not affected by
the global cap.
""".
t_priority_order2(_) ->
    Opts = #{
        max_len => 0,
        shift_multiplier => 2,
        priorities =>
            #{
                <<"t1">> => -1,
                <<"t2">> => 0
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
            element(
                2,
                ?Q:in(
                    #message{topic = Topic, qos = 1, payload = integer_to_binary(Message)},
                    Q
                )
            )
        end,
        ?Q:init(Opts),
        Messages
    ),
    ?assertMatch(
        [
            {<<"t2">>, <<"1">>},
            {<<"t2">>, <<"2">>},
            {<<"t2">>, <<"3">>},
            {<<"t2">>, <<"4">>},

            {<<"t1">>, <<"1">>},
            {<<"t1">>, <<"2">>},

            {<<"t2">>, <<"5">>},
            {<<"t2">>, <<"6">>},
            {<<"t2">>, <<"7">>},
            {<<"t2">>, <<"8">>},

            {<<"t1">>, <<"3">>},
            {<<"t1">>, <<"4">>},

            {<<"t2">>, <<"9">>},
            {<<"t2">>, <<"10">>},

            %% t2 is exhausted; the round-robin drains the rest of t1.
            {<<"t1">>, <<"5">>},
            {<<"t1">>, <<"6">>},
            {<<"t1">>, <<"7">>},
            {<<"t1">>, <<"8">>},
            {<<"t1">>, <<"9">>},
            {<<"t1">>, <<"10">>}
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

-doc """
Regression test for https://github.com/emqx/emqx/issues/13409: with several
prioritised topics, max_len bounds the total number of queued messages, not
each priority lane independently.
""".
t_max_len_bounds_total_not_per_priority(_) ->
    MaxLen = 3,
    Topics = [<<"t1">>, <<"t2">>, <<"t3">>, <<"t4">>, <<"t5">>],
    Priorities = maps:from_list(lists:zip(Topics, lists:seq(1, length(Topics)))),
    Opts = #{max_len => MaxLen, priorities => Priorities, store_qos0 => false},
    Q = lists:foldl(
        fun(_Round, QAcc) ->
            lists:foldl(
                fun(Topic, QAcc1) ->
                    {_Dropped, QAcc2} = ?Q:in(
                        emqx_message:make(?MODULE, ?QOS_1, Topic, <<"p">>), QAcc1
                    ),
                    %% The regression: this must hold after every single enqueue,
                    %% not just at the end.
                    ?assert(?Q:len(QAcc2) =< MaxLen),
                    QAcc2
                end,
                QAcc,
                Topics
            )
        end,
        ?Q:init(Opts),
        lists:seq(1, 20)
    ),
    ?assertEqual(MaxLen, ?Q:len(Q)),
    %% t5 has the highest priority: a lower-priority message must never
    %% survive at the expense of a higher-priority one.
    ?assert(lists:any(fun(#message{topic = T}) -> T =:= <<"t5">> end, ?Q:to_list(Q))).

-doc """
The message dropped when the queue is full comes from the lowest-priority
non-empty lane, even when the incoming message's own lane was empty before
this enqueue (so a naive "drop from the incoming lane" rule would have
dropped the message just enqueued, or refused it, instead).
""".
t_drop_from_lowest_priority_lane(_) ->
    Opts = #{
        max_len => 4,
        priorities =>
            #{
                <<"low">> => 1,
                <<"mid">> => 2,
                <<"high">> => 3
            },
        store_qos0 => false
    },
    Q0 = ?Q:init(Opts),
    {undefined, Q1} = ?Q:in(emqx_message:make(?MODULE, ?QOS_1, <<"low">>, <<"low-1">>), Q0),
    {undefined, Q2} = ?Q:in(emqx_message:make(?MODULE, ?QOS_1, <<"low">>, <<"low-2">>), Q1),
    {undefined, Q3} = ?Q:in(emqx_message:make(?MODULE, ?QOS_1, <<"mid">>, <<"mid-1">>), Q2),
    {undefined, Q4} = ?Q:in(emqx_message:make(?MODULE, ?QOS_1, <<"mid">>, <<"mid-2">>), Q3),
    ?assertEqual(4, ?Q:len(Q4)),
    %% "high" is a brand-new, empty lane. The oldest message in "low", the
    %% lowest non-empty priority, is the one that must go.
    {Dropped, Q5} = ?Q:in(emqx_message:make(?MODULE, ?QOS_1, <<"high">>, <<"high-1">>), Q4),
    ?assertEqual(<<"low-1">>, emqx_message:payload(Dropped)),
    ?assertEqual(4, ?Q:len(Q5)),
    Remaining = [{T, P} || #message{topic = T, payload = P} <- ?Q:to_list(Q5)],
    ?assertEqual(4, length(Remaining)),
    ?assertNot(lists:member({<<"low">>, <<"low-1">>}, Remaining)),
    ?assert(lists:member({<<"low">>, <<"low-2">>}, Remaining)),
    ?assert(lists:member({<<"mid">>, <<"mid-1">>}, Remaining)),
    ?assert(lists:member({<<"mid">>, <<"mid-2">>}, Remaining)),
    ?assert(lists:member({<<"high">>, <<"high-1">>}, Remaining)).

t_dropped(_) ->
    Q = ?Q:init(#{max_len => 1, store_qos0 => true}),
    Msg1 = emqx_message:make(<<"t1">>, <<"payload">>),
    Msg2 = emqx_message:make(<<"t2">>, <<"payload">>),
    {undefined, Q1} = ?Q:in(Msg1, Q),
    {Dropped, Q2} = ?Q:in(Msg2, Q1),
    ?assertMatch(#message{topic = <<"t1">>}, Dropped),
    ?assertEqual(1, ?Q:dropped(Q2)).

t_dropped_qos0_first(_) ->
    Opts = #{max_len => 3, store_qos0 => true},
    Q0 = ?Q:init(Opts),
    Msg1 = emqx_message:make(?MODULE, ?QOS_1, ~"t", ~"qos1-1"),
    Msg2 = emqx_message:make(?MODULE, ?QOS_0, ~"t", ~"qos0-2"),
    Msg3 = emqx_message:make(?MODULE, ?QOS_1, ~"t", ~"qos1-3"),
    Msg4 = emqx_message:make(?MODULE, ?QOS_1, ~"t", ~"qos1-4"),
    {undefined, Q1} = ?Q:in(Msg1, Q0),
    {undefined, Q2} = ?Q:in(Msg2, Q1),
    {undefined, Q3} = ?Q:in(Msg3, Q2),
    {Dropped, Q4} = ?Q:in(Msg4, Q3),
    ?assertEqual(<<"qos0-2">>, emqx_message:payload(Dropped)),
    ?assertEqual(1, ?Q:dropped(Q4)),
    ?assertEqual(3, ?Q:len(Q4)),
    ?assertEqual(
        [{~"t", ~"qos1-1"}, {~"t", ~"qos1-3"}, {~"t", ~"qos1-4"}],
        drain(Q4)
    ).

t_dropped_incoming_qos0_first(_) ->
    Opts = #{max_len => 3, store_qos0 => true},
    Q0 = ?Q:init(Opts),
    Msg1 = emqx_message:make(?MODULE, ?QOS_1, ~"t", ~"qos1-1"),
    Msg2 = emqx_message:make(?MODULE, ?QOS_1, ~"t", ~"qos1-2"),
    Msg3 = emqx_message:make(?MODULE, ?QOS_2, ~"t", ~"qos2-3"),
    Msg4 = emqx_message:make(?MODULE, ?QOS_0, ~"t", ~"qos0-4"),
    {undefined, Q1} = ?Q:in(Msg1, Q0),
    {undefined, Q2} = ?Q:in(Msg2, Q1),
    {undefined, Q3} = ?Q:in(Msg3, Q2),
    {Dropped, Q4} = ?Q:in(Msg4, Q3),
    ?assertEqual(<<"qos0-4">>, emqx_message:payload(Dropped)),
    ?assertEqual(1, ?Q:dropped(Q4)),
    ?assertEqual(3, ?Q:len(Q4)),
    ?assertEqual(
        [{~"t", ~"qos1-1"}, {~"t", ~"qos1-2"}, {~"t", ~"qos2-3"}],
        drain(Q4)
    ).

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
