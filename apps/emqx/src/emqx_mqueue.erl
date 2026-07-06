%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc A Simple in-memory message queue.
%%
%% Notice that MQTT is not a (on-disk) persistent messaging queue.
%% It assumes that clients should be online in most of the time.
%%
%% This module implements a simple in-memory queue for MQTT persistent session.
%%
%% If the broker restarts or crashes, all queued messages will be lost.
%%
%% Concept of Message Queue and Inflight Window:
%%
%%       |<----------------- Max Len ----------------->|
%%       -----------------------------------------------
%% IN -> |      Messages Queue   |  Inflight Window    | -> Out
%%       -----------------------------------------------
%%                               |<---   Win Size  --->|
%%
%%
%% 1. Inflight Window is to store the messages
%%    that are delivered but still awaiting for puback.
%%
%% 2. Messages are enqueued to tail when the inflight window is full.
%%
%% 3. QoS=0 messages are only enqueued when `store_qos0' is given `true`
%%    in init options
%%
%% 4. If the queue is full, drop the oldest one
%%    unless `max_len' is set to `0' which implies (`infinity').
%%
%% @end
%%--------------------------------------------------------------------

-module(emqx_mqueue).

-include("emqx.hrl").
-include("types.hrl").
-include("emqx_mqtt.hrl").

-export([
    init/1,
    info/1,
    info/2
]).

-export([
    is_empty/1,
    len/1,
    max_len/1,
    in/2,
    out/1,
    stats/1,
    dropped/1,
    to_list/1,
    filter/2,
    query/2
]).

-define(NO_PRIORITY_TABLE, disabled).

-export_type([mqueue/0, options/0]).

-type priority() :: infinity | integer().
-type pq() :: emqx_pqueue:q().
-type count() :: non_neg_integer().
-type p_table() :: #{emqx_types:topic() := priority()}.
-type options() :: #{
    max_len := count(),
    priorities => p_table(),
    default_priority => highest | lowest,
    store_qos0 => boolean()
}.
-type message() :: emqx_types:message().

-type stat() ::
    {len, non_neg_integer()}
    | {max_len, non_neg_integer()}
    | {dropped, non_neg_integer()}.

-define(LOWEST_PRIORITY, 0).
-define(HIGHEST_PRIORITY, infinity).
-define(MAX_LEN_INFINITY, 0).
-define(INFO_KEYS, [store_qos0, max_len, len, dropped]).
-define(INSERT_TS, mqueue_insert_ts).

-record(prios, {
    t :: p_table(),
    default :: priority(),
    shift_mult :: non_neg_integer(),
    shift_base :: integer()
}).

-record(mqueue, {
    store_qos0 = false :: boolean(),
    max_len = ?MAX_LEN_INFINITY :: count(),
    dropped = 0 :: count(),
    q = emqx_pqueue:new() :: pq(),
    prios = ?NO_PRIORITY_TABLE :: #prios{} | ?NO_PRIORITY_TABLE,
    p_credit :: non_neg_integer() | undefined
}).

-type mqueue() :: #mqueue{}.

-spec init(options()) -> mqueue().
init(Opts = #{max_len := MaxLen0, store_qos0 := Qos0}) ->
    MaxLen =
        case (is_integer(MaxLen0) andalso MaxLen0 > ?MAX_LEN_INFINITY) of
            true -> MaxLen0;
            false -> ?MAX_LEN_INFINITY
        end,
    Prios =
        case get_opt(priorities, Opts, ?NO_PRIORITY_TABLE) of
            ?NO_PRIORITY_TABLE ->
                ?NO_PRIORITY_TABLE;
            PTab ->
                {Mult, Base} = get_shift_opt(PTab, Opts),
                #prios{
                    t = p_table(PTab),
                    default = get_priority_opt(Opts),
                    shift_mult = Mult,
                    shift_base = Base
                }
        end,
    #mqueue{
        max_len = MaxLen,
        store_qos0 = Qos0,
        prios = Prios
    }.

-spec info(mqueue()) -> emqx_types:infos().
info(MQ) ->
    maps:from_list([{Key, info(Key, MQ)} || Key <- ?INFO_KEYS]).

-spec info(atom(), mqueue()) -> term().
info(store_qos0, #mqueue{store_qos0 = True}) ->
    True;
info(max_len, #mqueue{max_len = MaxLen}) ->
    MaxLen;
info(len, #mqueue{q = Q}) ->
    emqx_pqueue:len(Q);
info(dropped, #mqueue{dropped = Dropped}) ->
    Dropped.

is_empty(#mqueue{q = Q}) ->
    emqx_pqueue:len(Q) =:= 0.

len(#mqueue{q = Q}) ->
    emqx_pqueue:len(Q).

max_len(#mqueue{max_len = MaxLen}) -> MaxLen.

%% @doc Return all queued items in a list.
-spec to_list(mqueue()) -> list().
to_list(MQ) ->
    to_list(MQ, []).

-spec filter(fun((any()) -> boolean()), mqueue()) -> mqueue().
filter(Pred, #mqueue{q = Q, dropped = Dropped} = MQ) ->
    L0 = emqx_pqueue:len(Q),
    Q1 = emqx_pqueue:filter(Pred, Q),
    case emqx_pqueue:len(Q1) of
        L0 ->
            MQ;
        L1 ->
            MQ#mqueue{q = Q1, dropped = Dropped + (L0 - L1)}
    end.

-spec query(mqueue(), #{position => Pos, limit := Limit}) ->
    {[message()], #{position := Pos, start := Pos}}
when
    Pos :: none | {integer(), priority()},
    Limit :: non_neg_integer().
query(MQ, #{limit := Limit} = PagerParams) ->
    Pos = maps:get(position, PagerParams, none),
    PQsList = emqx_pqueue:to_queues_list(MQ#mqueue.q),
    {Msgs, NxtPos} = sublist(skip_until(PQsList, Pos), Limit, [], Pos),
    {Msgs, #{position => NxtPos, start => first_msg_pos(PQsList)}}.

first_msg_pos([]) ->
    none;
first_msg_pos([{Prio, PQ} | T]) ->
    case emqx_pqueue:out(PQ) of
        {empty, _PQ} ->
            first_msg_pos(T);
        {{value, Msg}, _Q} ->
            {insert_ts(Msg), Prio}
    end.

skip_until(PQsList, none = _Pos) ->
    PQsList;
skip_until(PQsList, {MsgPos, PrioPos}) ->
    case skip_until_prio(PQsList, PrioPos) of
        [{Prio, PQ} | T] ->
            PQ1 = skip_until_msg(PQ, MsgPos),
            [{Prio, PQ1} | T];
        [] ->
            []
    end.

skip_until_prio(PQsList, PrioPos) ->
    lists:dropwhile(fun({Prio, _PQ}) -> Prio > PrioPos end, PQsList).

skip_until_msg(PQ, MsgPos) ->
    case emqx_pqueue:out(PQ) of
        {empty, PQ1} ->
            PQ1;
        {{value, Msg}, PQ1} ->
            case insert_ts(Msg) > MsgPos of
                true -> PQ;
                false -> skip_until_msg(PQ1, MsgPos)
            end
    end.

sublist(PQs, Len, Acc, LastPosPrio) when PQs =:= []; Len =:= 0 ->
    {Acc, LastPosPrio};
sublist([{Prio, PQ} | T], Len, Acc, LastPosPrio) ->
    {SingleQAcc, SingleQSize} = sublist_single_pq(Prio, PQ, Len, [], 0),
    Acc1 = Acc ++ lists:reverse(SingleQAcc),
    NxtPosPrio =
        case SingleQAcc of
            [H | _] -> {insert_ts(H), Prio};
            [] -> LastPosPrio
        end,
    case SingleQSize =:= Len of
        true ->
            {Acc1, NxtPosPrio};
        false ->
            sublist(T, Len - SingleQSize, Acc1, NxtPosPrio)
    end.

sublist_single_pq(_Prio, _PQ, 0, Acc, AccSize) ->
    {Acc, AccSize};
sublist_single_pq(Prio, PQ, Len, Acc, AccSize) ->
    case emqx_pqueue:out(0, PQ) of
        {empty, _PQ} ->
            {Acc, AccSize};
        {{value, Msg}, PQ1} ->
            Msg1 = with_prio(Msg, Prio),
            sublist_single_pq(Prio, PQ1, Len - 1, [Msg1 | Acc], AccSize + 1)
    end.

with_prio(#message{extra = Extra} = Msg, Prio) ->
    Msg#message{extra = Extra#{mqueue_priority => Prio}}.

to_list(MQ, Acc) ->
    case out(MQ) of
        {empty, _MQ} ->
            lists:reverse(Acc);
        {{value, Msg}, Q1} ->
            to_list(Q1, [Msg | Acc])
    end.

%% @doc Return number of dropped messages.
-spec dropped(mqueue()) -> count().
dropped(#mqueue{dropped = Dropped}) -> Dropped.

%% @doc Stats of the mqueue
-spec stats(mqueue()) -> [stat()].
stats(#mqueue{max_len = MaxLen, dropped = Dropped} = MQ) ->
    [{len, len(MQ)}, {max_len, MaxLen}, {dropped, Dropped}].

%% @doc Enqueue a message.
-spec in(message(), mqueue()) -> {option(message()), mqueue()} | false.
in(#message{qos = ?QOS_0}, #mqueue{store_qos0 = false}) ->
    false;
in(
    Msg = #message{topic = Topic, qos = QoS},
    MQ =
        #mqueue{
            prios = Prios,
            q = Q,
            max_len = MaxLen,
            dropped = Dropped
        } = MQ
) ->
    Class =
        case QoS of
            ?QOS_0 -> qos0;
            _ -> default
        end,
    Priority =
        case Prios of
            %% MICRO-OPTIMIZATION: When there is no priority table defined (from config),
            %% disregard default priority from config, always use lowest (?LOWEST_PRIORITY=0)
            %% because the lowest priority in emqx_pqueue is a fallback to queue:queue()
            %% while the highest 'infinity' is a [{infinity, queue:queue()}]
            ?NO_PRIORITY_TABLE ->
                ?LOWEST_PRIORITY;
            #prios{t = PTab, default = Dp} ->
                maps:get(Topic, PTab, Dp)
        end,
    Msg1 = with_ts(Msg),
    PLen = emqx_pqueue:plen(Priority, Q),
    case MaxLen =/= ?MAX_LEN_INFINITY andalso PLen =:= MaxLen of
        true ->
            %% reached max length, drop the oldest message
            {{value, DroppedMsg}, Q1} = emqx_pqueue:drop(Priority, Q),
            Q2 = emqx_pqueue:in(Msg1, Priority, Class, Q1),
            {DroppedMsg, MQ#mqueue{q = Q2, dropped = Dropped + 1}};
        false ->
            Q1 = emqx_pqueue:in(Msg1, Priority, Class, Q),
            {_DroppedMsg = undefined, MQ#mqueue{q = Q1}}
    end.

-spec out(mqueue()) -> {empty | {value, message()}, mqueue()}.
out(MQ = #mqueue{q = Q, prios = ?NO_PRIORITY_TABLE}) ->
    case emqx_pqueue:out(Q) of
        {{value, V}, Q1} ->
            {{value, without_ts(V)}, MQ#mqueue{q = Q1}};
        {empty, _} ->
            {empty, MQ}
    end;
out(MQ = #mqueue{q = Q, p_credit = undefined, prios = Prios}) ->
    case emqx_pqueue:out_p(Q) of
        {{value, V, Prio}, Q1} ->
            MQ1 = MQ#mqueue{
                q = Q1,
                p_credit = get_credits(Prio, Prios)
            },
            {{value, without_ts(V)}, MQ1};
        {empty, _} ->
            {empty, MQ}
    end;
out(MQ = #mqueue{q = Q, p_credit = 0}) ->
    out(MQ#mqueue{
        q = emqx_pqueue:shift(Q),
        p_credit = undefined
    });
out(MQ = #mqueue{q = Q, p_credit = C}) ->
    case emqx_pqueue:out(Q) of
        {{value, V}, Q1} ->
            {{value, without_ts(V)}, MQ#mqueue{q = Q1, p_credit = C - 1}};
        {empty, _} ->
            {empty, MQ}
    end.

get_opt(Key, Opts, Default) ->
    case maps:get(Key, Opts, Default) of
        undefined -> Default;
        X -> X
    end.

get_priority_opt(Opts) ->
    case get_opt(default_priority, Opts, ?LOWEST_PRIORITY) of
        lowest -> ?LOWEST_PRIORITY;
        highest -> ?HIGHEST_PRIORITY;
        N when is_integer(N) -> N
    end.

get_credits(?HIGHEST_PRIORITY, Prios) ->
    Infinity = 1000000,
    get_credits(Infinity, Prios);
get_credits(Prio, #prios{shift_mult = Mult, shift_base = Base}) ->
    (Prio + Base + 1) * Mult - 1.

get_shift_opt(PTab, Opts) ->
    %% Using 10 as a multiplier by default. This is needed to minimize
    %% overhead of emqx_pqueue:rotate
    Mult = maps:get(shift_multiplier, Opts, 10),
    true = is_integer(Mult) andalso Mult > 0,
    Min =
        case maps:size(PTab) of
            0 -> 0;
            _ -> lists:min(maps:values(PTab))
        end,
    %% `mqueue' module supports negative priorities, but we don't want
    %% the counter to be negative, so all priorities should be shifted
    %% by a constant, if negative priorities are used:
    Base =
        case Min < 0 of
            true -> -Min;
            false -> 0
        end,
    {Mult, Base}.

%% topic from mqtt.mqueue_priorities(map()) is atom.
p_table(PTab = #{}) ->
    maps:fold(
        fun
            (Topic, Priority, Acc) when is_atom(Topic) ->
                maps:put(atom_to_binary(Topic), Priority, Acc);
            (Topic, Priority, Acc) when is_binary(Topic) ->
                maps:put(Topic, Priority, Acc)
        end,
        #{},
        PTab
    ).

%% This is used to sort/traverse messages in query/2
with_ts(#message{extra = Extra} = Msg) ->
    TsNano = erlang:system_time(nanosecond),
    Extra1 =
        case is_map(Extra) of
            true -> Extra;
            %% extra field has not being used before EMQX 5.4.0
            %% and defaulted to an empty list,
            %% if it's not a map it's safe to overwrite it
            false -> #{}
        end,
    Msg#message{extra = Extra1#{?INSERT_TS => TsNano}}.

without_ts(#message{extra = Extra} = Msg) ->
    Msg#message{extra = maps:remove(?INSERT_TS, Extra)};
without_ts(Msg) ->
    Msg.

insert_ts(#message{extra = #{?INSERT_TS := Ts}}) -> Ts.
