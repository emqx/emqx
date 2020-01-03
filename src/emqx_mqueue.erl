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

-export([ init/1
        , info/1
        , info/2
        ]).

-export([ is_empty/1
        , len/1
        , max_len/1
        , in/2
        , out/1
        , stats/1
        , dropped/1
        ]).

-export_type([mqueue/0, options/0]).

-type(topic() :: emqx_topic:topic()).
-type(priority() :: infinity | integer()).
-type(pq() :: emqx_pqueue:q()).
-type(count() :: non_neg_integer()).
-type(p_table() :: ?NO_PRIORITY_TABLE | #{topic() := priority()}).
-type(options() :: #{max_len := count(),
                     priorities => p_table(),
                     default_priority => highest | lowest,
                     store_qos0 => boolean()
                    }).
-type(message() :: emqx_types:message()).

-type(stat() :: {len, non_neg_integer()}
              | {max_len, non_neg_integer()}
              | {dropped, non_neg_integer()}).

-define(PQUEUE, emqx_pqueue).
-define(LOWEST_PRIORITY, 0).
-define(HIGHEST_PRIORITY, infinity).
-define(MAX_LEN_INFINITY, 0).
-define(INFO_KEYS, [store_qos0, max_len, len, dropped]).

-record(mqueue, {
          store_qos0 = false              :: boolean(),
          max_len    = ?MAX_LEN_INFINITY  :: count(),
          len        = 0                  :: count(),
          dropped    = 0                  :: count(),
          p_table    = ?NO_PRIORITY_TABLE :: p_table(),
          default_p  = ?LOWEST_PRIORITY   :: priority(),
          q          = ?PQUEUE:new()      :: pq()
         }).

-opaque(mqueue() :: #mqueue{}).

-spec(init(options()) -> mqueue()).
init(Opts = #{max_len := MaxLen0, store_qos0 := QoS_0}) ->
    MaxLen = case (is_integer(MaxLen0) andalso MaxLen0 > ?MAX_LEN_INFINITY) of
                 true -> MaxLen0;
                 false -> ?MAX_LEN_INFINITY
             end,
    #mqueue{max_len = MaxLen,
            store_qos0 = QoS_0,
            p_table = get_opt(priorities, Opts, ?NO_PRIORITY_TABLE),
            default_p = get_priority_opt(Opts)
           }.

-spec(info(mqueue()) -> emqx_types:infos()).
info(MQ) ->
    maps:from_list([{Key, info(Key, MQ)} || Key <- ?INFO_KEYS]).

-spec(info(atom(), mqueue()) -> term()).
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

%% @doc Return number of dropped messages.
-spec(dropped(mqueue()) -> count()).
dropped(#mqueue{dropped = Dropped}) -> Dropped.

%% @doc Stats of the mqueue
-spec(stats(mqueue()) -> [stat()]).
stats(#mqueue{max_len = MaxLen, dropped = Dropped} = MQ) ->
    [{len, len(MQ)}, {max_len, MaxLen}, {dropped, Dropped}].

%% @doc Enqueue a message.
-spec(in(message(), mqueue()) -> {maybe(message()), mqueue()}).
in(Msg = #message{qos = ?QOS_0}, MQ = #mqueue{store_qos0 = false}) ->
    {_Dropped = Msg, MQ};
in(Msg = #message{topic = Topic}, MQ = #mqueue{default_p = Dp,
                                               p_table = PTab,
                                               q = Q,
                                               len = Len,
                                               max_len = MaxLen,
                                               dropped = Dropped
                                              } = MQ) ->
    Priority = get_priority(Topic, PTab, Dp),
    PLen = ?PQUEUE:plen(Priority, Q),
    case MaxLen =/= ?MAX_LEN_INFINITY andalso PLen =:= MaxLen of
        true ->
            %% reached max length, drop the oldest message
            {{value, DroppedMsg}, Q1} = ?PQUEUE:out(Priority, Q),
            Q2 = ?PQUEUE:in(Msg, Priority, Q1),
            {DroppedMsg, MQ#mqueue{q = Q2, dropped = Dropped + 1}};
        false ->
            {_DroppedMsg = undefined, MQ#mqueue{len = Len + 1, q = ?PQUEUE:in(Msg, Priority, Q)}}
    end.

-spec(out(mqueue()) -> {empty | {value, message()}, mqueue()}).
out(MQ = #mqueue{len = 0, q = Q}) ->
    0 = ?PQUEUE:len(Q), %% assert, in this case, ?PQUEUE:len should be very cheap
    {empty, MQ};
out(MQ = #mqueue{q = Q, len = Len}) ->
    {R, Q1} = ?PQUEUE:out(Q),
    {R, MQ#mqueue{q = Q1, len = Len - 1}}.

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
