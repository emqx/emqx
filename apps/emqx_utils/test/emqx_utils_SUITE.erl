%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include("../../emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(SOCKOPTS, [
    binary,
    {packet, raw},
    {reuseaddr, true},
    {backlog, 512},
    {nodelay, true}
]).

all() -> emqx_common_test_helpers:all(?MODULE).

t_merge_opts(_) ->
    Opts = emqx_utils:merge_opts(?SOCKOPTS, [
        raw,
        binary,
        {backlog, 1024},
        {nodelay, false},
        {max_clients, 1024},
        {acceptors, 16}
    ]),
    ?assertEqual(1024, proplists:get_value(backlog, Opts)),
    ?assertEqual(1024, proplists:get_value(max_clients, Opts)),
    ?assertEqual(
        [
            binary,
            raw,
            {acceptors, 16},
            {backlog, 1024},
            {max_clients, 1024},
            {nodelay, false},
            {packet, raw},
            {reuseaddr, true}
        ],
        lists:sort(Opts)
    ).

t_maybe_apply(_) ->
    ?assertEqual(undefined, emqx_utils:maybe_apply(fun(A) -> A end, undefined)),
    ?assertEqual(a, emqx_utils:maybe_apply(fun(A) -> A end, a)).

t_run_fold(_) ->
    ?assertEqual(1, emqx_utils:run_fold([], 1, state)),
    Add = fun(I, St) -> I + St end,
    Mul = fun(I, St) -> I * St end,
    ?assertEqual(6, emqx_utils:run_fold([Add, Mul], 1, 2)).

t_pipeline(_) ->
    ?assertEqual({ok, input, state}, emqx_utils:pipeline([], input, state)),
    Funs = [
        fun(_I, _St) -> ok end,
        fun(_I, St) -> {ok, St + 1} end,
        fun(I, St) -> {ok, I + 1, St + 1} end,
        fun(I, St) -> {ok, I * 2, St * 2} end
    ],
    ?assertEqual({ok, 4, 6}, emqx_utils:pipeline(Funs, 1, 1)),
    ?assertEqual(
        {error, undefined, 1}, emqx_utils:pipeline([fun(_I) -> {error, undefined} end], 1, 1)
    ),
    ?assertEqual(
        {error, undefined, 2},
        emqx_utils:pipeline([fun(_I, _St) -> {error, undefined, 2} end], 1, 1)
    ).

t_start_timer(_) ->
    TRef = emqx_utils:start_timer(1, tmsg),
    timer:sleep(2),
    ?assertEqual([{timeout, TRef, tmsg}], ?drainMailbox()),
    ok = emqx_utils:cancel_timer(TRef).

t_cancel_timer(_) ->
    Timer = emqx_utils:start_timer(0, foo),
    ok = emqx_utils:cancel_timer(Timer),
    ?assertEqual([], ?drainMailbox()),
    ok = emqx_utils:cancel_timer(undefined).

t_proc_name(_) ->
    ?assertEqual(emqx_pool_1, emqx_utils:proc_name(emqx_pool, 1)).

t_proc_stats(_) ->
    Pid1 = spawn(fun() -> exit(normal) end),
    timer:sleep(10),
    ?assertEqual([], emqx_utils:proc_stats(Pid1)),
    Pid2 = spawn(fun() ->
        ?assertMatch([{mailbox_len, 0} | _], emqx_utils:proc_stats()),
        timer:sleep(200)
    end),
    timer:sleep(10),
    Pid2 ! msg,
    timer:sleep(10),
    ?assertMatch([{mailbox_len, 1} | _], emqx_utils:proc_stats(Pid2)).

t_drain_deliver(_) ->
    self() ! {deliver, t1, m1},
    self() ! {deliver, t2, m2},
    ?assertEqual(
        [
            {deliver, t1, m1},
            {deliver, t2, m2}
        ],
        emqx_utils:drain_deliver(2)
    ).

t_drain_down(_) ->
    {Pid1, _Ref1} = erlang:spawn_monitor(fun() -> ok end),
    {Pid2, _Ref2} = erlang:spawn_monitor(fun() -> ok end),
    timer:sleep(100),
    ?assertEqual(lists:sort([Pid1, Pid2]), lists:sort(emqx_utils:drain_down(2))),
    ?assertEqual([], emqx_utils:drain_down(1)).

t_index_of(_) ->
    try emqx_utils:index_of(a, []) of
        _ -> ct:fail(should_throw_error)
    catch
        error:Reason ->
            ?assertEqual(badarg, Reason)
    end,
    ?assertEqual(3, emqx_utils:index_of(a, [b, c, a, e, f])).

t_check(_) ->
    Policy = #{
        max_mailbox_size => 10,
        max_heap_size => 1024 * 1024 * 8,
        enable => true
    },
    [self() ! {msg, I} || I <- lists:seq(1, 5)],
    ?assertEqual(ok, emqx_utils:check_oom(Policy)),
    [self() ! {msg, I} || I <- lists:seq(1, 6)],
    ?assertEqual(
        {shutdown, #{reason => mailbox_overflow, value => 11, max => 10}},
        emqx_utils:check_oom(Policy)
    ).

t_tune_heap_size(_Config) ->
    Policy = #{
        max_mailbox_size => 10,
        max_heap_size => 1024 * 1024 * 8,
        enable => true
    },
    ?assertEqual(ignore, emqx_utils:tune_heap_size(Policy#{enable := false})),
    %% Setting it to 0 disables the check.
    ?assertEqual(ignore, emqx_utils:tune_heap_size(Policy#{max_heap_size := 0})),
    {max_heap_size, PreviousHeapSize} = process_info(self(), max_heap_size),
    try
        ?assertMatch(PreviousHeapSize, emqx_utils:tune_heap_size(Policy))
    after
        process_flag(max_heap_size, PreviousHeapSize)
    end.

t_rand_seed(_) ->
    ?assert(is_tuple(emqx_utils:rand_seed())).

t_now_to_secs(_) ->
    ?assert(is_integer(emqx_utils:now_to_secs(os:timestamp()))).

t_now_to_ms(_) ->
    ?assert(is_integer(emqx_utils:now_to_ms(os:timestamp()))).

t_gen_id(_) ->
    ?assertEqual(10, length(emqx_utils:gen_id(10))),
    ?assertEqual(20, length(emqx_utils:gen_id(20))).

t_pmap_normal(_) ->
    ?assertEqual(
        [5, 7, 9],
        emqx_utils:pmap(
            fun({A, B}) -> A + B end,
            [{2, 3}, {3, 4}, {4, 5}]
        )
    ).

t_pmap_timeout(_) ->
    ?assertExit(
        timeout,
        emqx_utils:pmap(
            fun
                (timeout) -> ct:sleep(1000);
                ({A, B}) -> A + B
            end,
            [{2, 3}, {3, 4}, timeout],
            100
        )
    ).

t_pmap_exception(_) ->
    ?assertError(
        foobar,
        emqx_utils:pmap(
            fun
                (error) -> error(foobar);
                ({A, B}) -> A + B
            end,
            [{2, 3}, {3, 4}, error]
        )
    ).

t_pmap_late_reply(_) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := pmap_middleman_sent_response},
                #{?snk_kind := pmap_timeout}
            ),
            Timeout = 100,
            Res =
                catch emqx_utils:pmap(
                    fun(_) ->
                        process_flag(trap_exit, true),
                        timer:sleep(3 * Timeout),
                        done
                    end,
                    [1, 2, 3],
                    Timeout
                ),
            receive
                {Ref, LateReply} when is_reference(Ref) ->
                    ct:fail("should not receive late reply: ~p", [LateReply])
            after (5 * Timeout) ->
                ok
            end,
            ?assertMatch([done, done, done], Res),
            ok
        end,
        []
    ),
    ok.

t_flattermap(_) ->
    ?assertEqual(
        [42],
        emqx_utils:flattermap(fun identity/1, [42])
    ),
    ?assertEqual(
        [42, 42],
        emqx_utils:flattermap(fun duplicate/1, [42])
    ),
    ?assertEqual(
        [],
        emqx_utils:flattermap(fun nil/1, [42])
    ),
    ?assertEqual(
        [1, 1, 2, 2, 3, 3],
        emqx_utils:flattermap(fun duplicate/1, [1, 2, 3])
    ),
    ?assertEqual(
        [],
        emqx_utils:flattermap(fun nil/1, [1, 2, 3])
    ),
    ?assertEqual(
        [1, 2, 2, 4, 5, 5],
        emqx_utils:flattermap(
            fun(X) ->
                case X rem 3 of
                    0 -> [];
                    1 -> X;
                    2 -> [X, X]
                end
            end,
            [1, 2, 3, 4, 5]
        )
    ).

duplicate(X) ->
    [X, X].

nil(_) ->
    [].

identity(X) ->
    X.
