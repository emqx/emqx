%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_streams_delete_worker_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).
init_per_suite(Config) -> emqx_streams_api_SUITE:init_per_suite(Config).
end_per_suite(Config) -> emqx_streams_api_SUITE:end_per_suite(Config).
init_per_testcase(Case, Config) -> emqx_streams_api_SUITE:init_per_testcase(Case, Config).
end_per_testcase(Case, Config) ->
    meck:unload(),
    emqx_streams_api_SUITE:end_per_testcase(Case, Config).

-doc "DELETE returns 204 before bulk removal finishes and the final pass removes racing writes".
t_background_delete(_Config) ->
    Stream0 = create(<<"background">>),
    #{id := Id0} = Stream0,
    Message0 = emqx_message:make(<<"test">>, 1, <<"background/1">>, <<"before">>),
    ok = emqx_streams_message_db:insert(Stream0, Message0),
    TestPid = self(),
    ok = meck:new(emqx_streams_message_db, [passthrough, no_link]),
    ok = meck:expect(emqx_streams_message_db, drop, fun(Stream) ->
        ok = meck:passthrough([Stream]),
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
        TestPid ! {response, emqx_mq_api_helpers:api_delete([stream, <<"background">>])}
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
        ?assertMatch({ok, #{id := Id0}}, emqx_streams_registry:find(<<"background">>)),
        ?assertEqual([], emqx_streams_message_db:dirty_read_all(Stream0)),
        ok = emqx_streams_message_db:insert(Stream0, Message0)
    after
        Worker0 ! continue
    end,
    ?retry(50, 100, ?assertEqual(not_found, emqx_streams_registry:find(<<"background">>))),
    ?assertEqual([], emqx_streams_message_db:dirty_read_all(Stream0)).

-doc "A queued deletion does not delete a replacement stream with the same name".
t_replacement(_Config) ->
    Name0 = <<"replacement">>,
    #{id := Id0} = create(Name0),
    Worker0 = gproc_pool:pick_worker(emqx_streams_delete_worker, Id0),
    ok = sys:suspend(Worker0),
    try
        ok = emqx_streams_delete_worker:enqueue(Name0, Id0),
        ok = emqx_streams_registry:delete(Name0),
        #{id := Id1} = create(Name0),
        ?assertNotEqual(Id0, Id1),
        ?assertEqual(not_found, emqx_streams_registry:delete(Name0, Id0))
    after
        ok = sys:resume(Worker0)
    end,
    %% A call waits until the worker has handled the earlier deletion cast.
    ?assertEqual({error, unknown_request}, gen_server:call(Worker0, sync)),
    ?assertMatch({ok, _}, emqx_streams_registry:find(Name0)).

-doc "While a stream is being deleted, it is listed, GET and PUT return 400, and DELETE repeats".
t_api_while_deleting(_Config) ->
    Path = [message_streams, streams, <<"deleting">>],
    #{id := Id0} = create(<<"deleting">>),
    Worker0 = gproc_pool:pick_worker(emqx_streams_delete_worker, Id0),
    ok = sys:suspend(Worker0),
    try
        ?assertEqual({ok, 204}, emqx_mq_api_helpers:api_delete(Path)),
        ?assertMatch({ok, 400, _}, emqx_mq_api_helpers:api_get(Path)),
        ?assertMatch(
            {ok, 400, _}, emqx_mq_api_helpers:api_put(Path, #{<<"read_max_unacked">> => 10000})
        ),
        ?assertMatch(
            {ok, 200, #{<<"data">> := [#{<<"name">> := <<"deleting">>, <<"deleted_at">> := _}]}},
            emqx_mq_api_helpers:api_get([message_streams, streams])
        ),
        ?assertEqual({ok, 204}, emqx_mq_api_helpers:api_delete(Path))
    after
        ok = sys:resume(Worker0)
    end,
    ?retry(50, 100, ?assertMatch({ok, 404, _}, emqx_mq_api_helpers:api_get(Path))).

-doc "DELETE returns 503 without deletion workers, and a repeated DELETE removes the stream".
t_delete_without_workers(_Config) ->
    Path = [message_streams, streams, <<"no_workers">>],
    _ = create(<<"no_workers">>),
    ok = emqx_streams_sup:stop_services(),
    ?assertMatch({ok, 503, _}, emqx_mq_api_helpers:api_delete(Path)),
    ok = emqx_streams_sup:start_services(),
    ?assertMatch({ok, 400, _}, emqx_mq_api_helpers:api_get(Path)),
    ?assertEqual({ok, 204}, emqx_mq_api_helpers:api_delete(Path)),
    ?retry(50, 100, ?assertMatch({ok, 404, _}, emqx_mq_api_helpers:api_get(Path))).

create(Name) ->
    {ok, 200, _} = emqx_mq_api_helpers:api_post([streams], #{
        <<"name">> => Name,
        <<"topic_filter">> => <<Name/binary, "/#">>,
        <<"key_expression">> => <<"message.topic">>
    }),
    {ok, Stream} = emqx_streams_registry:find(Name),
    Stream.
