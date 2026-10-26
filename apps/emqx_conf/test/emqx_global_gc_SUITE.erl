%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_global_gc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_cm.hrl").

%% Mirrors the ?CHUNK macro of emqx_global_gc. The distribution cases below
%% assert on chunk boundaries, so the two must agree.
-define(CHUNK, 100).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_config:erase_all(),
    ok.

t_run_gc(_) ->
    Conf0 = #{
        node => #{
            cookie => <<"cookie">>,
            data_dir => <<"data">>,
            global_gc_interval => <<"1s">>
        }
    },
    emqx_common_test_helpers:load_config(emqx_conf_schema, Conf0),
    ?assertEqual({ok, 1000}, application:get_env(emqx_machine, global_gc_interval)),
    {ok, _} = emqx_global_gc:start_link(),
    ok = timer:sleep(1500),
    {ok, MilliSecs} = emqx_global_gc:run(),
    ct:pal("Global GC: ~w(ms)~n", [MilliSecs]),
    emqx_global_gc:stop(),
    Conf1 = emqx_utils_maps:deep_put([node, global_gc_interval], Conf0, disabled),
    emqx_common_test_helpers:load_config(emqx_conf_schema, Conf1),
    {ok, Pid} = emqx_global_gc:start_link(),
    ?assertMatch(#{timer := undefined}, sys:get_state(Pid)),
    ?assertEqual({ok, disabled}, application:get_env(emqx_machine, global_gc_interval)),
    ok.

%% `run/0' collects every channel in the table in one pass.
t_run_collects_every_channel(_) ->
    ok = with_channel_table(fun() ->
        Pids = start_idle_channels(3),
        Before = [heap(P) || P <- Pids],
        ok = start_gc(<<"1h">>),
        {ok, _} = emqx_global_gc:run(),
        ok = stop_gc(),
        After = [heap(P) || P <- Pids],
        ?assert(
            lists:all(fun({B, A}) -> A < B end, lists:zip(Before, After)),
            #{before => Before, 'after' => After}
        )
    end).

%% A pass walks the table one chunk per tick instead of collecting everything
%% at once. This is what keeps the collection off the CPU profile.
t_pass_is_spread_over_chunks(_) ->
    ok = with_channel_table(fun() ->
        Total = ?CHUNK * 2 + 10,
        Pids = start_idle_channels(Total),
        Before = [heap(P) || P <- Pids],
        %% A long interval keeps the real timer out of the way; the case
        %% drives the ticks itself, so it needs no sleep.
        ok = start_gc(<<"1h">>),
        Server = whereis(emqx_global_gc),
        %% Three ticks cover 210 channels at 100 per tick, the third one
        %% reaching the end of the table.
        ?assertEqual(?CHUNK, collected_after_ticks(Server, 1, Pids, Before)),
        ?assertEqual(?CHUNK * 2, collected_after_ticks(Server, 1, Pids, Before)),
        ?assertEqual(Total, collected_after_ticks(Server, 1, Pids, Before)),
        %% The pass is over, so the cursor is cleared and the next tick starts
        %% a new one.
        ?assertMatch(#{cursor := undefined}, sys:get_state(Server)),
        ok = stop_gc()
    end).

%% A channel that closes while the cursor is behind it must not stop the pass.
%% `ets:next/2' on an ordered_set accepts a deleted key.
t_dead_channel_does_not_stop_the_pass(_) ->
    ok = with_channel_table(fun() ->
        [P1, P2, P3] = Pids = start_idle_channels(3),
        ok = stop_idle_channel(P2),
        Before = [heap(P) || P <- [P1, P3]],
        ok = start_gc(<<"1h">>),
        {ok, _} = emqx_global_gc:run(),
        ok = stop_gc(),
        After = [heap(P) || P <- [P1, P3]],
        ?assert(lists:all(fun({B, A}) -> A < B end, lists:zip(Before, After))),
        lists:foreach(fun stop_idle_channel/1, Pids -- [P2])
    end).

%% Loading this beam into a running node must be enough on its own. The
%% process keeps the state map and the armed timer it had under the previous
%% version, so the new code has to accept both: a state carrying only `timer',
%% and the `run' message the old timer was armed with.
t_beam_load_needs_no_upgrade_instruction(_) ->
    ok = start_gc(<<"1h">>),
    Server = whereis(emqx_global_gc),
    #{timer := TRef} = sys:get_state(Server),
    %% Shrink the state to the shape the previous version wrote.
    _ = sys:replace_state(Server, fun(_) -> #{timer => TRef} end),
    ?assertEqual(#{timer => TRef}, sys:get_state(Server)),
    %% The timer armed before the load delivers the old message.
    Server ! {timeout, TRef, run},
    State = sys:get_state(Server),
    ?assert(is_process_alive(Server)),
    ?assertMatch(
        #{timer := T, cursor := _, tick := _, started_at := _} when is_reference(T),
        State
    ),
    ok = stop_gc().

%% The collector starts before `emqx_cm' creates the table, so a missing table
%% is a normal state and not an error.
t_no_channel_table(_) ->
    ?assertEqual(undefined, ets:info(?CHAN_INFO_TAB, size)),
    ok = start_gc(<<"1h">>),
    ?assertMatch({ok, _}, emqx_global_gc:run()),
    ok = stop_gc().

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

start_gc(Interval) ->
    Conf = #{
        node => #{
            cookie => <<"cookie">>,
            data_dir => <<"data">>,
            global_gc_interval => Interval
        }
    },
    emqx_common_test_helpers:load_config(emqx_conf_schema, Conf),
    {ok, _} = emqx_global_gc:start_link(),
    ok.

stop_gc() ->
    emqx_global_gc:stop().

with_channel_table(Fun) ->
    _ = ets:new(?CHAN_INFO_TAB, [ordered_set, public, named_table]),
    try
        Fun()
    after
        lists:foreach(
            fun({{_ClientId, Pid}, _, _}) -> stop_idle_channel(Pid) end,
            ets:tab2list(?CHAN_INFO_TAB)
        ),
        ets:delete(?CHAN_INFO_TAB)
    end,
    ok.

%% A process parked in `receive' reports `status = waiting', which is what the
%% collector looks for. Each one first builds a heap it then drops, so a
%% collection is observable as the heap shrinking. A hibernating process would
%% not do: it is already collected down to a minimal heap, so there would be
%% nothing to observe.
start_idle_channels(N) ->
    Pids = [start_idle_channel(I) || I <- lists:seq(1, N)],
    ok = wait_until_grown(Pids),
    Pids.

start_idle_channel(I) ->
    Pid = erlang:spawn(fun() ->
        _Garbage = lists:seq(1, 20000),
        receive
            stop -> ok
        end
    end),
    ClientId = iolist_to_binary(io_lib:format("c~p", [I])),
    true = ets:insert(?CHAN_INFO_TAB, {{ClientId, Pid}, #{}, []}),
    Pid.

%% The processes above allocate before they park, so wait for the heap to be
%% there rather than assume it. `heap/1` answers 0 for a process that has not
%% been scheduled yet.
wait_until_grown(Pids) ->
    case [P || P <- Pids, heap(P) < 20000] of
        [] ->
            ok;
        _ ->
            receive
            after 10 -> wait_until_grown(Pids)
            end
    end.

stop_idle_channel(Pid) ->
    _ = erlang:exit(Pid, kill),
    ok.

%% Drive N ticks and report how many of the channels have been collected since
%% the baseline. Sending the timeout message the server is waiting for keeps
%% the case deterministic, with no sleep.
collected_after_ticks(Server, N, Pids, Before) ->
    lists:foreach(fun(_) -> tick(Server) end, lists:seq(1, N)),
    length([
        P
     || {P, B} <- lists:zip(Pids, Before), heap(P) < B
    ]).

tick(Server) ->
    #{timer := TRef} = sys:get_state(Server),
    Server ! {timeout, TRef, tick},
    %% sys:get_state/1 is a synchronous call, so it returns only after the
    %% message above has been handled.
    _ = sys:get_state(Server),
    ok.

%% `total_heap_size' is the observable for a forced collection. The garbage
%% collection counters are not: a forced collection is a full sweep, which
%% resets `minor_gcs' instead of raising it.
heap(Pid) ->
    case erlang:process_info(Pid, total_heap_size) of
        {total_heap_size, Size} -> Size;
        undefined -> 0
    end.
