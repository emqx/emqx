%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_pool_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        {group, submit},
        {group, async_submit},
        t_unexpected,
        t_take_async_tasks
    ].

groups() ->
    [
        {submit, [sequence], [
            t_submit_mfa,
            t_submit_fa
        ]},
        {async_submit, [sequence], [
            t_async_submit_mfa,
            t_async_submit_crash
        ]}
    ].

init_per_suite(Config) ->
    _ = application:stop(grpoc),
    ok = emqx_logger:set_log_level(emergency),
    application:ensure_all_started(gproc),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_, Config) ->
    {ok, Sup} = emqx_pool_sup:start_link(),
    [{pool_sup, Sup} | Config].

end_per_testcase(_, Config) ->
    Sup = proplists:get_value(pool_sup, Config),
    %% ???
    exit(Sup, normal).

t_submit_mfa(_Config) ->
    Result = emqx_pool:submit({?MODULE, test_mfa, []}),
    ?assertEqual(15, Result).

t_submit_fa(_Config) ->
    Fun = fun(X) ->
        case X rem 2 of
            0 -> {true, X div 2};
            _ -> false
        end
    end,
    Result = emqx_pool:submit(Fun, [2]),
    ?assertEqual({true, 1}, Result).

t_async_submit_mfa(_Config) ->
    emqx_pool:async_submit({?MODULE, test_mfa, []}),
    emqx_pool:async_submit(fun ?MODULE:test_mfa/0, []).

t_async_submit_crash(_) ->
    emqx_pool:async_submit(fun() -> error(unexpected_error) end).

t_unexpected(_) ->
    Pid = emqx_pool:worker(),
    ?assertEqual(ignored, gen_server:call(Pid, bad_request)),
    ?assertEqual(ok, gen_server:cast(Pid, bad_msg)),
    Pid ! bad_info,
    ok = gen_server:stop(Pid).

-doc """
A running task takes the queued tasks of the same fun, in queue order, until
the limit. A task of another fun stays queued and runs later.
""".
t_take_async_tasks(_) ->
    Pool = take_async_tasks_pool,
    {ok, Sup} = emqx_pool_sup:start_link(Pool, random, 1, {emqx_pool, start_link, []}),
    Self = self(),
    Fun = fun ?MODULE:take_test_task/1,
    ok = emqx_pool:async_submit_to_pool(Pool, fun() -> take_and_report(Self, Fun) end, []),
    lists:foreach(
        fun(Args) -> ok = emqx_pool:async_submit_to_pool(Pool, Fun, Args) end,
        [[[1, 2]], [[3]], [[4, 5]], [[6]]]
    ),
    ok = emqx_pool:async_submit_to_pool(Pool, fun ?MODULE:send_to/2, [Self, other_task_ran]),
    ok = emqx_pool:async_submit_to_pool(Pool, Fun, [[7]]),
    gproc_pool:pick_worker(Pool) ! go,
    ?assertEqual(
        {taken, [], [1, 2, 3], [4, 5, 6, 7]},
        receive
            {taken, _, _, _} = Taken -> Taken
        after 5000 -> timeout
        end
    ),
    ?assertEqual(
        other_task_ran,
        receive
            other_task_ran -> other_task_ran
        after 5000 -> timeout
        end
    ),
    unlink(Sup),
    ok = gen_server:stop(Sup).

take_and_report(Pid, Fun) ->
    receive
        go -> ok
    end,
    None = emqx_pool:take_async_tasks(Fun, 0),
    First = emqx_pool:take_async_tasks(Fun, 3),
    Rest = emqx_pool:take_async_tasks(Fun, 100),
    Pid ! {taken, None, First, Rest}.

take_test_task(_List) ->
    error(should_be_taken).

send_to(Pid, Msg) ->
    Pid ! Msg.

test_mfa() ->
    lists:foldl(fun(X, Sum) -> X + Sum end, 0, [1, 2, 3, 4, 5]).

% t_async_submit(_) ->
%     error('TODO').
