%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_misc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(SOCKOPTS, [
    binary,
    {packet, raw},
    {reuseaddr, true},
    {backlog, 512},
    {nodelay, true}
]).

all() -> emqx_common_test_helpers:all(?MODULE).

t_merge_opts(_) ->
    Opts = emqx_misc:merge_opts(?SOCKOPTS, [
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
    ?assertEqual(undefined, emqx_misc:maybe_apply(fun(A) -> A end, undefined)),
    ?assertEqual(a, emqx_misc:maybe_apply(fun(A) -> A end, a)).

t_run_fold(_) ->
    ?assertEqual(1, emqx_misc:run_fold([], 1, state)),
    Add = fun(I, St) -> I + St end,
    Mul = fun(I, St) -> I * St end,
    ?assertEqual(6, emqx_misc:run_fold([Add, Mul], 1, 2)).

t_pipeline(_) ->
    ?assertEqual({ok, input, state}, emqx_misc:pipeline([], input, state)),
    Funs = [
        fun(_I, _St) -> ok end,
        fun(_I, St) -> {ok, St + 1} end,
        fun(I, St) -> {ok, I + 1, St + 1} end,
        fun(I, St) -> {ok, I * 2, St * 2} end
    ],
    ?assertEqual({ok, 4, 6}, emqx_misc:pipeline(Funs, 1, 1)),
    ?assertEqual(
        {error, undefined, 1}, emqx_misc:pipeline([fun(_I) -> {error, undefined} end], 1, 1)
    ),
    ?assertEqual(
        {error, undefined, 2}, emqx_misc:pipeline([fun(_I, _St) -> {error, undefined, 2} end], 1, 1)
    ).

t_start_timer(_) ->
    TRef = emqx_misc:start_timer(1, tmsg),
    timer:sleep(2),
    ?assertEqual([{timeout, TRef, tmsg}], drain()),
    ok = emqx_misc:cancel_timer(TRef).

t_cancel_timer(_) ->
    Timer = emqx_misc:start_timer(0, foo),
    ok = emqx_misc:cancel_timer(Timer),
    ?assertEqual([], drain()),
    ok = emqx_misc:cancel_timer(undefined).

t_proc_name(_) ->
    ?assertEqual(emqx_pool_1, emqx_misc:proc_name(emqx_pool, 1)).

t_proc_stats(_) ->
    Pid1 = spawn(fun() -> exit(normal) end),
    timer:sleep(10),
    ?assertEqual([], emqx_misc:proc_stats(Pid1)),
    Pid2 = spawn(fun() ->
        ?assertMatch([{mailbox_len, 0} | _], emqx_misc:proc_stats()),
        timer:sleep(200)
    end),
    timer:sleep(10),
    Pid2 ! msg,
    timer:sleep(10),
    ?assertMatch([{mailbox_len, 1} | _], emqx_misc:proc_stats(Pid2)).

t_drain_deliver(_) ->
    self() ! {deliver, t1, m1},
    self() ! {deliver, t2, m2},
    ?assertEqual(
        [
            {deliver, t1, m1},
            {deliver, t2, m2}
        ],
        emqx_misc:drain_deliver(2)
    ).

t_drain_down(_) ->
    {Pid1, _Ref1} = erlang:spawn_monitor(fun() -> ok end),
    {Pid2, _Ref2} = erlang:spawn_monitor(fun() -> ok end),
    timer:sleep(100),
    ?assertEqual([Pid1, Pid2], lists:sort(emqx_misc:drain_down(2))),
    ?assertEqual([], emqx_misc:drain_down(1)).

t_index_of(_) ->
    try emqx_misc:index_of(a, []) of
        _ -> ct:fail(should_throw_error)
    catch
        error:Reason ->
            ?assertEqual(badarg, Reason)
    end,
    ?assertEqual(3, emqx_misc:index_of(a, [b, c, a, e, f])).

t_check(_) ->
    Policy = #{
        max_message_queue_len => 10,
        max_heap_size => 1024 * 1024 * 8,
        enable => true
    },
    [self() ! {msg, I} || I <- lists:seq(1, 5)],
    ?assertEqual(ok, emqx_misc:check_oom(Policy)),
    [self() ! {msg, I} || I <- lists:seq(1, 6)],
    ?assertEqual({shutdown, message_queue_too_long}, emqx_misc:check_oom(Policy)).

drain() ->
    drain([]).

drain(Acc) ->
    receive
        Msg -> drain([Msg | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

t_rand_seed(_) ->
    ?assert(is_tuple(emqx_misc:rand_seed())).

t_now_to_secs(_) ->
    ?assert(is_integer(emqx_misc:now_to_secs(os:timestamp()))).

t_now_to_ms(_) ->
    ?assert(is_integer(emqx_misc:now_to_ms(os:timestamp()))).

t_gen_id(_) ->
    ?assertEqual(10, length(emqx_misc:gen_id(10))),
    ?assertEqual(20, length(emqx_misc:gen_id(20))).

t_pmap_normal(_) ->
    ?assertEqual(
        [5, 7, 9],
        emqx_misc:pmap(
            fun({A, B}) -> A + B end,
            [{2, 3}, {3, 4}, {4, 5}]
        )
    ).

t_pmap_timeout(_) ->
    ?assertExit(
        timeout,
        emqx_misc:pmap(
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
        emqx_misc:pmap(
            fun
                (error) -> error(foobar);
                ({A, B}) -> A + B
            end,
            [{2, 3}, {3, 4}, error]
        )
    ).
