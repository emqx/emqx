%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_misc).

-export([ merge_opts/2
        , start_timer/2
        , start_timer/3
        , cancel_timer/1
        , proc_name/2
        , proc_stats/0
        , proc_stats/1
        ]).

-export([ init_proc_mng_policy/1
        , conn_proc_mng_policy/1
        ]).

-export([drain_down/1]).

%% @doc Merge options
-spec(merge_opts(list(), list()) -> list()).
merge_opts(Defaults, Options) ->
    lists:foldl(
      fun({Opt, Val}, Acc) ->
          lists:keystore(Opt, 1, Acc, {Opt, Val});
         (Opt, Acc) ->
          lists:usort([Opt | Acc])
      end, Defaults, Options).

-spec(start_timer(integer(), term()) -> reference()).
start_timer(Interval, Msg) ->
    start_timer(Interval, self(), Msg).

-spec(start_timer(integer(), pid() | atom(), term()) -> reference()).
start_timer(Interval, Dest, Msg) ->
    erlang:start_timer(Interval, Dest, Msg).

-spec(cancel_timer(undefined | reference()) -> ok).
cancel_timer(Timer) when is_reference(Timer) ->
    case erlang:cancel_timer(Timer) of
        false ->
            receive {timeout, Timer, _} -> ok after 0 -> ok end;
        _ -> ok
    end;
cancel_timer(_) -> ok.

-spec(proc_name(atom(), pos_integer()) -> atom()).
proc_name(Mod, Id) ->
    list_to_atom(lists:concat([Mod, "_", Id])).

-spec(proc_stats() -> list()).
proc_stats() ->
    proc_stats(self()).

-spec(proc_stats(pid()) -> list()).
proc_stats(Pid) ->
    Stats = process_info(Pid, [message_queue_len, heap_size, reductions]),
    {value, {_, V}, Stats1} = lists:keytake(message_queue_len, 1, Stats),
    [{mailbox_len, V} | Stats1].

-define(DISABLED, 0).

init_proc_mng_policy(undefined) -> ok;
init_proc_mng_policy(Zone) ->
    #{max_heap_size := MaxHeapSizeInBytes}
        = ShutdownPolicy
        = emqx_zone:get_env(Zone, force_shutdown_policy),
    MaxHeapSize = MaxHeapSizeInBytes div erlang:system_info(wordsize),
    _ = erlang:process_flag(max_heap_size, MaxHeapSize), % zero is discarded
    erlang:put(force_shutdown_policy, ShutdownPolicy),
    ok.

%% @doc Check self() process status against connection/session process management policy,
%% return `continue | hibernate | {shutdown, Reason}' accordingly.
%% `continue': There is nothing out of the ordinary.
%% `hibernate': Nothing to process in my mailbox, and since this check is triggered
%%              by a timer, we assume it is a fat chance to continue idel, hence hibernate.
%% `shutdown': Some numbers (message queue length hit the limit),
%%             hence shutdown for greater good (system stability).
-spec(conn_proc_mng_policy(#{message_queue_len => integer()} | false) ->
            continue | hibernate | {shutdown, _}).
conn_proc_mng_policy(#{message_queue_len := MaxMsgQueueLen}) ->
    Qlength = proc_info(message_queue_len),
    Checks =
        [{fun() -> is_message_queue_too_long(Qlength, MaxMsgQueueLen) end,
          {shutdown, message_queue_too_long}},
         {fun() -> Qlength > 0 end, continue},
         {fun() -> true end, hibernate}
        ],
    check(Checks);
conn_proc_mng_policy(_) ->
    %% disable by default
    conn_proc_mng_policy(#{message_queue_len => 0}).

check([{Pred, Result} | Rest]) ->
    case Pred() of
        true -> Result;
        false -> check(Rest)
    end.

is_message_queue_too_long(Qlength, Max) ->
    is_enabled(Max) andalso Qlength > Max.

is_enabled(Max) -> is_integer(Max) andalso Max > ?DISABLED.

proc_info(Key) ->
    {Key, Value} = erlang:process_info(self(), Key),
    Value.

-spec(drain_down(pos_integer()) -> list(pid())).
drain_down(Cnt) when Cnt > 0 ->
    drain_down(Cnt, []).

drain_down(0, Acc) ->
    lists:reverse(Acc);

drain_down(Cnt, Acc) ->
    receive
        {'DOWN', _MRef, process, Pid, _Reason} ->
            drain_down(Cnt - 1, [Pid|Acc])
    after 0 ->
          lists:reverse(Acc)
    end.

