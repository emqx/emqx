%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_misc).

-include("types.hrl").

-export([ merge_opts/2
        , maybe_apply/2
        , run_fold/3
        , start_timer/2
        , start_timer/3
        , cancel_timer/1
        , proc_name/2
        , proc_stats/0
        , proc_stats/1
        ]).

-export([ drain_deliver/0
        , drain_deliver/1
        , drain_down/1
        ]).

-compile({inline,
          [ start_timer/2
          , start_timer/3
          ]}).

%% @doc Merge options
-spec(merge_opts(list(), list()) -> list()).
merge_opts(Defaults, Options) ->
    lists:foldl(
      fun({Opt, Val}, Acc) ->
          lists:keystore(Opt, 1, Acc, {Opt, Val});
         (Opt, Acc) ->
          lists:usort([Opt | Acc])
      end, Defaults, Options).

%% @doc Apply a function to a maybe argument.
-spec(maybe_apply(fun((maybe(A)) -> maybe(A)), maybe(A))
      -> maybe(A) when A :: any()).
maybe_apply(_Fun, undefined) ->
    undefined;
maybe_apply(Fun, Arg) when is_function(Fun) ->
    erlang:apply(Fun, [Arg]).

run_fold([], Acc, _State) ->
    Acc;
run_fold([Fun|More], Acc, State) ->
    run_fold(More, Fun(Acc, State), State).

-spec(start_timer(integer(), term()) -> reference()).
start_timer(Interval, Msg) ->
    start_timer(Interval, self(), Msg).

-spec(start_timer(integer(), pid() | atom(), term()) -> reference()).
start_timer(Interval, Dest, Msg) ->
    erlang:start_timer(Interval, Dest, Msg).

-spec(cancel_timer(maybe(reference())) -> ok).
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
    case process_info(Pid, [message_queue_len, heap_size,
                            total_heap_size, reductions, memory]) of
        undefined -> [];
        [{message_queue_len, Len}|Stats] ->
            [{mailbox_len, Len}|Stats]
    end.

%% @doc Drain delivers from the channel's mailbox.
drain_deliver() ->
    drain_deliver([]).

drain_deliver(Acc) ->
    receive
        Deliver = {deliver, _Topic, _Msg} ->
            drain_deliver([Deliver|Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

%% @doc Drain process down events.
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
        drain_down(0, Acc)
    end.

