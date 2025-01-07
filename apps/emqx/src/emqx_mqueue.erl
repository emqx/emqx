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
-type p_table() :: ?NO_PRIORITY_TABLE | #{emqx_types:topic() := priority()}.
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

-define(PQUEUE, emqx_pqueue).
-define(LOWEST_PRIORITY, 0).
-define(HIGHEST_PRIORITY, infinity).
-define(MAX_LEN_INFINITY, 0).
-define(INFO_KEYS, [store_qos0, max_len, len, dropped]).
-define(INSERT_TS, mqueue_insert_ts).

-record(shift_opts, {
    multiplier :: non_neg_integer(),
    base :: integer()
}).

-record(mqueue, {
    store_qos0 = false :: boolean(),
    max_len = ?MAX_LEN_INFINITY :: count(),
    len = 0 :: count(),
    dropped = 0 :: count(),
    p_table = ?NO_PRIORITY_TABLE :: p_table(),
    default_p = ?LOWEST_PRIORITY :: priority(),
    q = emqx_pqueue:new() :: pq(),
    shift_opts :: #shift_opts{},
    last_prio :: non_neg_integer() | undefined,
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
    #mqueue{
        max_len = MaxLen,
        store_qos0 = Qos0,
        p_table = p_table(get_opt(priorities, Opts, ?NO_PRIORITY_TABLE)),
        default_p = get_priority_opt(Opts),
        shift_opts = get_shift_opt(Opts)
    }.

-spec info(mqueue()) -> emqx_types:infos().
info(MQ) ->
    maps:from_list([{Key, info(Key, MQ)} || Key <- ?INFO_KEYS]).

-spec info(atom(), mqueue()) -> term().
info(store_qos0, #mqueue{store_qos0 = True}) ->
    True;
info(max_len, #mqueue{max_len = MaxLen}) ->
    MaxLen;
info(len, #mqueue{len = Len}) ->
    Len;
info(dropped, #mqueue{dropped = Dropped}) ->
    Dropped.

is_empty(#mqueue{len = Len}) -> Len =:= 0.

len(#mqueue{len = Len}) -> Len.

max_len(#mqueue{max_len = MaxLen}) -> MaxLen.

%% @doc Return all queued items in a list.
-spec to_list(mqueue()) -> list().
to_list(MQ) ->
    to_list(MQ, []).

-spec filter(fun((any()) -> boolean()), mqueue()) -> mqueue().
filter(_Pred, #mqueue{len = 0} = MQ) ->
    MQ;
filter(Pred, #mqueue{q = Q, len = Len, dropped = Droppend} = MQ) ->
    Q2 = ?PQUEUE:filter(Pred, Q),
    case ?PQUEUE:len(Q2) of
        Len ->
            MQ;
        Len2 ->
            Diff = Len - Len2,
            MQ#mqueue{q = Q2, len = Len2, dropped = Droppend + Diff}
    end.

-spec query(mqueue(), #{position => Pos, limit := Limit}) ->
    {[message()], #{position := Pos, start := Pos}}
when
    Pos :: none | {integer(), priority()},
    Limit :: non_neg_integer().
query(MQ, #{limit := Limit} = PagerParams) ->
    Pos = maps:get(position, PagerParams, none),
    PQsList = ?PQUEUE:to_queues_list(MQ#mqueue.q),
    {Msgs, NxtPos} = sublist(skip_until(PQsList, Pos), Limit, [], Pos),
    {Msgs, #{position => NxtPos, start => first_msg_pos(PQsList)}}.

first_msg_pos([]) ->
    none;
first_msg_pos([{Prio, PQ} | T]) ->
    case ?PQUEUE:out(PQ) of
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
    case ?PQUEUE:out(PQ) of
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
    case ?PQUEUE:out(0, PQ) of
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
-spec in(message(), mqueue()) -> {option(message()), mqueue()}.
in(Msg = #message{qos = ?QOS_0}, MQ = #mqueue{store_qos0 = false}) ->
    {_Dropped = Msg, MQ};
in(
    Msg = #message{topic = Topic},
    MQ =
        #mqueue{
            default_p = Dp,
            p_table = PTab,
            q = Q,
            len = Len,
            max_len = MaxLen,
            dropped = Dropped
        } = MQ
) ->
    Priority = get_priority(Topic, PTab, Dp),
    PLen = ?PQUEUE:plen(Priority, Q),
    Msg1 = with_ts(Msg),
    case MaxLen =/= ?MAX_LEN_INFINITY andalso PLen =:= MaxLen of
        true ->
            %% reached max length, drop the oldest message
            {{value, DroppedMsg}, Q1} = ?PQUEUE:out(Priority, Q),
            Q2 = ?PQUEUE:in(Msg1, Priority, Q1),
            {without_ts(DroppedMsg), MQ#mqueue{q = Q2, dropped = Dropped + 1}};
        false ->
            {_DroppedMsg = undefined, MQ#mqueue{len = Len + 1, q = ?PQUEUE:in(Msg1, Priority, Q)}}
    end.

-spec out(mqueue()) -> {empty | {value, message()}, mqueue()}.
out(MQ = #mqueue{len = 0, q = Q}) ->
    %% assert, in this case, ?PQUEUE:len should be very cheap
    0 = ?PQUEUE:len(Q),
    {empty, MQ};
out(MQ = #mqueue{q = Q, len = Len, last_prio = undefined, shift_opts = ShiftOpts}) ->
    %% Shouldn't fail, since we've checked the length
    {{value, Val, Prio}, Q1} = ?PQUEUE:out_p(Q),
    MQ1 = MQ#mqueue{
        q = Q1,
        len = Len - 1,
        last_prio = Prio,
        p_credit = get_credits(Prio, ShiftOpts)
    },
    {{value, without_ts(Val)}, MQ1};
out(MQ = #mqueue{q = Q, p_credit = 0}) ->
    MQ1 = MQ#mqueue{
        q = ?PQUEUE:shift(Q),
        last_prio = undefined
    },
    out(MQ1);
out(MQ = #mqueue{q = Q, len = Len, p_credit = Cnt}) ->
    {R, Q2} =
        case ?PQUEUE:out(Q) of
            {{value, Val}, Q1} -> {{value, without_ts(Val)}, Q1};
            Other -> Other
        end,
    {R, MQ#mqueue{q = Q2, len = Len - 1, p_credit = Cnt - 1}}.

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

%% MICRO-OPTIMIZATION: When there is no priority table defined (from config),
%% disregard default priority from config, always use lowest (?LOWEST_PRIORITY=0)
%% because the lowest priority in emqx_pqueue is a fallback to queue:queue()
%% while the highest 'infinity' is a [{infinity, queue:queue()}]
get_priority(_Topic, ?NO_PRIORITY_TABLE, _) -> ?LOWEST_PRIORITY;
get_priority(Topic, PTab, Dp) -> maps:get(Topic, PTab, Dp).

get_credits(?HIGHEST_PRIORITY, Opts) ->
    Infinity = 1000000,
    get_credits(Infinity, Opts);
get_credits(Prio, #shift_opts{multiplier = Mult, base = Base}) ->
    (Prio + Base + 1) * Mult - 1.

get_shift_opt(Opts) ->
    %% Using 10 as a multiplier by default. This is needed to minimize
    %% overhead of ?PQUEUE:rotate
    Mult = maps:get(shift_multiplier, Opts, 10),
    true = is_integer(Mult) andalso Mult > 0,
    Min =
        case Opts of
            #{p_table := PTab} ->
                case maps:size(PTab) of
                    0 -> 0;
                    _ -> lists:min(maps:values(PTab))
                end;
            _ ->
                ?LOWEST_PRIORITY
        end,
    %% `mqueue' module supports negative priorities, but we don't want
    %% the counter to be negative, so all priorities should be shifted
    %% by a constant, if negative priorities are used:
    Base =
        case Min < 0 of
            true -> -Min;
            false -> 0
        end,
    #shift_opts{
        multiplier = Mult,
        base = Base
    }.

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
    );
p_table(PTab) ->
    PTab.

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
