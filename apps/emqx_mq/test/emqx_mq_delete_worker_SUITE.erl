%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mq_delete_worker_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).
init_per_suite(Config) -> emqx_mq_api_SUITE:init_per_suite(Config).
end_per_suite(Config) -> emqx_mq_api_SUITE:end_per_suite(Config).
init_per_testcase(Case, Config) -> emqx_mq_api_SUITE:init_per_testcase(Case, Config).
end_per_testcase(Case, Config) ->
    meck:unload(),
    emqx_mq_api_SUITE:end_per_testcase(Case, Config).

-doc "DELETE returns 204 before bulk removal finishes and the final pass removes racing writes".
t_background_delete(_Config) ->
    MQ0 = create(<<"background">>),
    #{id := Id0} = MQ0,
    Message0 = emqx_message:make(<<"test">>, 1, <<"background/1">>, <<"before">>),
    ok = emqx_mq_message_db:insert(MQ0, Message0),
    TestPid = self(),
    ok = meck:new(emqx_mq_message_db, [passthrough, no_link]),
    ok = meck:expect(emqx_mq_message_db, drop, fun(MQ) ->
        ok = meck:passthrough([MQ]),
        case get({bulk_done, Id0}) of
            undefined ->
                put({bulk_done, Id0}, true),
                TestPid ! {bulk_done, self()},
                receive
                    continue -> ok
                after 5000 -> error(release_timeout)
                end;
            true ->
                ok
        end
    end),
    spawn_link(fun() ->
        TestPid !
            {response, emqx_mq_api_helpers:api_delete([message_queues, queues, <<"background">>])}
    end),
    Worker0 =
        receive
            {bulk_done, Pid0} -> Pid0
        after 5000 -> error(bulk_timeout)
        end,
    try
        ?assertEqual(
            {ok, 204},
            receive
                {response, Response0} -> Response0
            after 1000 -> timeout
            end
        ),
        ?assertMatch({ok, #{id := Id0}}, emqx_mq_registry:find(<<"background">>)),
        ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ0)),
        %% A write after the bulk pass must be removed by delete_by_id.
        ok = emqx_mq_message_db:insert(MQ0, Message0)
    after
        Worker0 ! continue
    end,
    ?retry(50, 100, ?assertEqual(not_found, emqx_mq_registry:find(<<"background">>))),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ0)).

-doc "The pool runs at most five deletions and deduplicates repeated requests for one queue".
t_pool_limit(_Config) ->
    MQs0 = maps:values(queues_per_worker(1, #{})),
    MQ1 = create(<<"waiting">>),
    TestPid = self(),
    ok = meck:new(emqx_mq_message_db, [passthrough, no_link]),
    ok = meck:expect(emqx_mq_message_db, drop, fun(#{id := Id} = MQ) ->
        case get({bulk_done, Id}) of
            undefined ->
                put({bulk_done, Id}, true),
                TestPid ! {started, self()},
                receive
                    continue -> ok
                after 5000 -> error(release_timeout)
                end;
            true ->
                ok
        end,
        meck:passthrough([MQ])
    end),
    [ok = emqx_mq_delete_worker:enqueue(Id) || #{id := Id} <- MQs0 ++ [MQ1]],
    [#{id := Id0} | _] = MQs0,
    ok = emqx_mq_delete_worker:enqueue(Id0),
    Workers0 = [
        receive
            {started, Pid0} -> Pid0
        after 2000 -> error(start_timeout)
        end
     || _ <- lists:seq(1, 5)
    ],
    ?assertEqual(
        none,
        receive
            {started, _} -> extra_worker
        after 100 -> none
        end
    ),
    [Worker0 ! continue || Worker0 <- Workers0],
    receive
        {started, Pid1} -> Pid1 ! continue
    after 5000 -> error(waiting_queue_stalled)
    end,
    [
        ?retry(50, 100, ?assertEqual(not_found, emqx_mq_registry:find(Name)))
     || #{name := Name} <- MQs0 ++ [MQ1]
    ],
    ?assertEqual(
        none,
        receive
            {started, _} -> duplicate_worker
        after 100 -> none
        end
    ).

-doc "While a queue is being deleted, it is listed, GET and PUT return 400, and DELETE repeats".
t_api_while_deleting(_Config) ->
    Path = [message_queues, queues, <<"deleting">>],
    #{id := Id0} = create(<<"deleting">>),
    Worker0 = gproc_pool:pick_worker(emqx_mq_delete_worker, Id0),
    ok = sys:suspend(Worker0),
    try
        ?assertEqual({ok, 204}, emqx_mq_api_helpers:api_delete(Path)),
        ?assertMatch({ok, 400, _}, emqx_mq_api_helpers:api_get(Path)),
        ?assertMatch(
            {ok, 400, _}, emqx_mq_api_helpers:api_put(Path, #{<<"ping_interval">> => 10000})
        ),
        ?assertMatch(
            {ok, 200, #{<<"data">> := [#{<<"name">> := <<"deleting">>, <<"deleted_at">> := _}]}},
            emqx_mq_api_helpers:api_get([message_queues, queues])
        ),
        ?assertEqual({ok, 204}, emqx_mq_api_helpers:api_delete(Path))
    after
        ok = sys:resume(Worker0)
    end,
    ?retry(50, 100, ?assertMatch({ok, 404, _}, emqx_mq_api_helpers:api_get(Path))).

-doc "DELETE returns 503 without deletion workers, and a repeated DELETE removes the queue".
t_delete_without_workers(_Config) ->
    Path = [message_queues, queues, <<"no_workers">>],
    _ = create(<<"no_workers">>),
    ok = emqx_mq_sup:stop_services(),
    ?assertMatch({ok, 503, _}, emqx_mq_api_helpers:api_delete(Path)),
    ok = emqx_mq_sup:start_services(),
    ?assertMatch({ok, 400, _}, emqx_mq_api_helpers:api_get(Path)),
    ?assertEqual({ok, 204}, emqx_mq_api_helpers:api_delete(Path)),
    ?retry(50, 100, ?assertMatch({ok, 404, _}, emqx_mq_api_helpers:api_get(Path))).

queues_per_worker(_, Queues) when map_size(Queues) =:= 5 -> Queues;
queues_per_worker(N, Queues) ->
    #{id := Id} = MQ = create(integer_to_binary(N)),
    Worker = gproc_pool:pick_worker(emqx_mq_delete_worker, Id),
    queues_per_worker(N + 1, Queues#{Worker => MQ}).

create(Name) ->
    {ok, 200, _} = emqx_mq_api_helpers:api_post([message_queues, queues], #{
        <<"name">> => Name,
        <<"topic_filter">> => <<Name/binary, "/#">>,
        <<"key_expression">> => <<"message.topic">>
    }),
    {ok, MQ} = emqx_mq_registry:find(Name),
    MQ.
