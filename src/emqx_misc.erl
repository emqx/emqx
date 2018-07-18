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

-module(emqx_misc).

-export([merge_opts/2, start_timer/2, start_timer/3, cancel_timer/1,
         proc_name/2, proc_stats/0, proc_stats/1]).

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
cancel_timer(undefined) ->
    ok;
cancel_timer(Timer) ->
    case catch erlang:cancel_timer(Timer) of
        false ->
            receive {timeout, Timer, _} -> ok after 0 -> ok end;
        _ -> ok
    end.

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

