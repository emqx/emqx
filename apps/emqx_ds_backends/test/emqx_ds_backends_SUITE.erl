%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_backends_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx/include/emqx.hrl").
-include("../../emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("../../emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(msg_fields, [topic, from, payload]).

%% A simple smoke test that verifies that opening/closing the DB
%% doesn't crash, and not much else
t_00_smoke_open_drop(Config) ->
    DB = 'DB',
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    %% Reopen the DB and make sure the operation is idempotent:
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    %% Close the DB:
    ?assertMatch(ok, emqx_ds:drop_db(DB)).

%% A simple smoke test that verifies that storing the messages doesn't
%% crash
t_01_smoke_store(Config) ->
    ?check_trace(
        #{timetrap => 10_000},
        begin
            DB = default,
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            Msg = message(<<"foo/bar">>, <<"foo">>, 0),
            ?assertMatch(ok, emqx_ds:store_batch(DB, [Msg]))
        end,
        []
    ).

%% A simple smoke test that verifies that getting the list of streams
%% doesn't crash, iterators can be opened, and that it's possible to iterate
%% over messages.
t_02_smoke_iterate(Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    StartTime = 0,
    TopicFilter = ['#'],
    Msgs = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo/bar">>, <<"2">>, 1),
        message(<<"foo/bar">>, <<"3">>, 2)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs, #{sync => true})),
    timer:sleep(1000),
    [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
    {ok, _Iter, Batch} = emqx_ds_test_helpers:consume_iter(DB, Iter0),
    emqx_ds_test_helpers:diff_messages(?msg_fields, Msgs, Batch).

%% Verify that iterators survive restart of the application. This is
%% an important property, since the lifetime of the iterators is tied
%% to the external resources, such as clients' sessions, and they
%% should always be able to continue replaying the topics from where
%% they are left off.
t_05_restart(Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    TopicFilter = ['#'],
    StartTime = 0,
    Msgs = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo/bar">>, <<"2">>, 1),
        message(<<"foo/bar">>, <<"3">>, 2)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs, #{sync => true})),
    timer:sleep(1_000),
    [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
    %% Restart the application:
    ?tp(warning, emqx_ds_SUITE_restart_app, #{}),
    ok = application:stop(emqx_durable_storage),
    {ok, _} = application:ensure_all_started(emqx_durable_storage),
    ok = emqx_ds_open_db(DB, opts(Config)),
    %% The old iterator should be still operational:
    {ok, _Iter, Batch} = emqx_ds_test_helpers:consume_iter(DB, Iter0),
    emqx_ds_test_helpers:diff_messages(?msg_fields, Msgs, Batch).

t_06_smoke_add_generation(Config) ->
    DB = ?FUNCTION_NAME,
    BeginTime = os:system_time(millisecond),

    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    [{Gen1, #{created_at := Created1, since := Since1, until := undefined}}] = maps:to_list(
        emqx_ds:list_generations_with_lifetimes(DB)
    ),

    ?assertMatch(ok, emqx_ds:add_generation(DB)),
    [
        {Gen1, #{created_at := Created1, since := Since1, until := Until1}},
        {_Gen2, #{created_at := Created2, since := Since2, until := undefined}}
    ] = maps:to_list(emqx_ds:list_generations_with_lifetimes(DB)),
    %% Check units of the return values (+/- 10s from test begin time):
    ?give_or_take(BeginTime, 10_000, Created1),
    ?give_or_take(BeginTime, 10_000, Created2),
    ?give_or_take(BeginTime, 10_000, Since2),
    ?give_or_take(BeginTime, 10_000, Until1).

t_07_smoke_update_config(Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    timer:sleep(1000),
    ?assertMatch(
        [{_, _}],
        maps:to_list(emqx_ds:list_generations_with_lifetimes(DB))
    ),
    ?assertMatch(ok, emqx_ds:update_db_config(DB, opts(Config))),
    ?assertMatch(
        [{_, _}, {_, _}],
        maps:to_list(emqx_ds:list_generations_with_lifetimes(DB))
    ).

%% Verifies the basic usage of `list_generations_with_lifetimes' and `drop_generation'...
%%   1) Cannot drop current generation.
%%   2) All existing generations are returned by `list_generation_with_lifetimes'.
%%   3) Dropping a generation removes it from the list.
%%   4) Dropped generations stay dropped even after restarting the application.
t_08_smoke_list_drop_generation(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            %% Exactly one generation at first.
            Generations0 = emqx_ds:list_generations_with_lifetimes(DB),
            ?assertMatch(
                [{_GenId, #{since := _, until := _}}],
                maps:to_list(Generations0),
                #{gens => Generations0}
            ),
            [{GenId0, _}] = maps:to_list(Generations0),
            %% Cannot delete current generation
            ?assertEqual({error, current_generation}, emqx_ds:drop_generation(DB, GenId0)),

            %% New gen
            ok = emqx_ds:add_generation(DB),
            Generations1 = emqx_ds:list_generations_with_lifetimes(DB),
            ?assertMatch(
                [
                    {GenId0, #{since := _, until := _}},
                    {_GenId1, #{since := _, until := _}}
                ],
                lists:sort(maps:to_list(Generations1)),
                #{gens => Generations1}
            ),
            [GenId0, GenId1] = lists:sort(maps:keys(Generations1)),

            %% Drop the older one
            ?assertEqual(ok, emqx_ds:drop_generation(DB, GenId0)),
            Generations2 = emqx_ds:list_generations_with_lifetimes(DB),
            ?assertMatch(
                [{GenId1, #{since := _, until := _}}],
                lists:sort(maps:to_list(Generations2)),
                #{gens => Generations2}
            ),

            %% Unknown gen_id, as it was already dropped
            ?assertEqual({error, not_found}, emqx_ds:drop_generation(DB, GenId0)),

            %% Should persist surviving generation list
            ok = application:stop(emqx_durable_storage),
            {ok, _} = application:ensure_all_started(emqx_durable_storage),
            ok = emqx_ds_open_db(DB, opts(Config)),

            Generations3 = emqx_ds:list_generations_with_lifetimes(DB),
            ?assertMatch(
                [{GenId1, #{since := _, until := _}}],
                lists:sort(maps:to_list(Generations3)),
                #{gens => Generations3}
            )
        end,
        []
    ),
    ok.

t_09_atomic_store_batch(Config) ->
    ct:pal("store batch ~p", [Config]),
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            DBOpts = (opts(Config))#{atomic_batches => true},
            ?assertMatch(ok, emqx_ds_open_db(DB, DBOpts)),
            Msgs = [
                message(<<"1">>, <<"1">>, 0),
                message(<<"2">>, <<"2">>, 1),
                message(<<"3">>, <<"3">>, 2)
            ],
            ?assertEqual(
                ok,
                emqx_ds:store_batch(DB, Msgs, #{sync => true})
            )
        end,
        []
    ),
    ok.

t_10_non_atomic_store_batch(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            application:set_env(emqx_durable_storage, egress_batch_size, 1),
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            Msgs = [
                message(<<"1">>, <<"1">>, 0),
                message(<<"2">>, <<"2">>, 1),
                message(<<"3">>, <<"3">>, 2)
            ],
            %% Non-atomic batches may be split.
            ?assertEqual(
                ok,
                emqx_ds:store_batch(DB, Msgs, #{
                    atomic => false,
                    sync => true
                })
            ),
            timer:sleep(1000)
        end,
        fun(Trace) ->
            %% Should contain one flush per message.
            Batches = ?projection(batch, ?of_kind(emqx_ds_buffer_flush, Trace)),
            ?assertMatch([_], Batches),
            ?assertMatch(
                [_, _, _],
                lists:append(Batches)
            ),
            ok
        end
    ),
    ok.

t_11_batch_preconditions(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            DBOpts = (opts(Config))#{
                atomic_batches => true,
                append_only => false
            },
            ?assertMatch(ok, emqx_ds_open_db(DB, DBOpts)),

            %% Conditional delete
            TS = 42,
            Batch1 = #dsbatch{
                preconditions = [{if_exists, matcher(<<"c1">>, <<"t/a">>, '_', TS)}],
                operations = [{delete, matcher(<<"c1">>, <<"t/a">>, '_', TS)}]
            },
            %% Conditional insert
            M1 = message(<<"c1">>, <<"t/a">>, <<"M1">>, TS),
            Batch2 = #dsbatch{
                preconditions = [{unless_exists, matcher(<<"c1">>, <<"t/a">>, '_', TS)}],
                operations = [M1]
            },

            %% No such message yet, precondition fails:
            ?assertEqual(
                {error, unrecoverable, {precondition_failed, not_found}},
                emqx_ds:store_batch(DB, Batch1)
            ),
            %% No such message yet, `unless` precondition holds:
            ?assertEqual(
                ok,
                emqx_ds:store_batch(DB, Batch2)
            ),
            %% Now there's such message, `unless` precondition now fails:
            ?assertMatch(
                {error, unrecoverable,
                    {precondition_failed, #message{topic = <<"t/a">>, payload = <<"M1">>}}},
                emqx_ds:store_batch(DB, Batch2)
            ),
            %% On the other hand, `if` precondition now holds:
            ?assertEqual(
                ok,
                emqx_ds:store_batch(DB, Batch1)
            ),

            %% Wait at least until current epoch ends.
            ct:sleep(1000),
            %% There's no messages in the DB.
            ?assertEqual(
                [],
                emqx_ds_test_helpers:consume(DB, emqx_topic:words(<<"t/#">>))
            )
        end,
        []
    ).

t_12_batch_precondition_conflicts(Config) ->
    DB = ?FUNCTION_NAME,
    NBatches = 50,
    NMessages = 10,
    ?check_trace(
        begin
            DBOpts = (opts(Config))#{
                atomic_batches => true,
                append_only => false
            },
            ?assertMatch(ok, emqx_ds_open_db(DB, DBOpts)),

            ConflictBatches = [
                #dsbatch{
                    %% If the slot is free...
                    preconditions = [{if_exists, matcher(<<"c1">>, <<"t/slot">>, _Free = <<>>, 0)}],
                    %% Take it and write NMessages extra messages, so that batches take longer to
                    %% process and have higher chances to conflict with each other.
                    operations =
                        [
                            message(<<"c1">>, <<"t/slot">>, integer_to_binary(I), _TS = 0)
                            | [
                                message(<<"c1">>, {"t/owner/~p/~p", [I, J]}, <<>>, I * 100 + J)
                             || J <- lists:seq(1, NMessages)
                            ]
                        ]
                }
             || I <- lists:seq(1, NBatches)
            ],

            %% Run those batches concurrently.
            ok = emqx_ds:store_batch(DB, [message(<<"c1">>, <<"t/slot">>, <<>>, 0)]),
            Results = emqx_utils:pmap(
                fun(B) -> emqx_ds:store_batch(DB, B) end,
                ConflictBatches,
                infinity
            ),

            %% Only one should have succeeded.
            ?assertEqual([ok], [Ok || Ok = ok <- Results]),

            %% While other failed with an identical `precondition_failed`.
            Failures = lists:usort([PreconditionFailed || {error, _, PreconditionFailed} <- Results]),
            ?assertMatch(
                [{precondition_failed, #message{topic = <<"t/slot">>, payload = <<_/bytes>>}}],
                Failures
            ),

            %% Wait at least until current epoch ends.
            ct:sleep(1000),
            %% Storage should contain single batch's messages.
            [{precondition_failed, #message{payload = IOwner}}] = Failures,
            WinnerBatch = lists:nth(binary_to_integer(IOwner), ConflictBatches),
            BatchMessages = lists:sort(WinnerBatch#dsbatch.operations),
            DBMessages = emqx_ds_test_helpers:consume(DB, emqx_topic:words(<<"t/#">>)),
            emqx_ds_test_helpers:diff_messages(
                ?msg_fields,
                lists:sort(BatchMessages),
                lists:sort(DBMessages)
            )
        end,
        []
    ).

t_smoke_delete_next(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            %% Preparation: this test verifies topic selector. To
            %% create a stream that contains multiple topics we need a
            %% learned wildcard:
            create_wildcard(DB, <<"foo">>),
            %% Actual test:
            StartTime = 0,
            TopicFilter = [<<"foo">>, '#'],
            %% Publish messages in different topics in two batches to distinguish between the streams:
            Msgs =
                [Msg1, _Msg2, Msg3] = [
                    message(<<"foo/bar">>, <<"1">>, 0),
                    message(<<"foo/foo">>, <<"2">>, 1),
                    message(<<"foo/baz">>, <<"3">>, 2)
                ],
            ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),

            [DStream] = emqx_ds:get_delete_streams(DB, TopicFilter, StartTime),
            {ok, DIter0} = emqx_ds:make_delete_iterator(DB, DStream, TopicFilter, StartTime),

            Selector = fun(#message{topic = Topic}) ->
                Topic == <<"foo/foo">>
            end,
            {ok, DIter1, NumDeleted1} = delete(DB, DIter0, Selector, 1),
            ?assertEqual(0, NumDeleted1),
            {ok, DIter2, NumDeleted2} = delete(DB, DIter1, Selector, 1),
            ?assertEqual(1, NumDeleted2),

            [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
            Batch = emqx_ds_test_helpers:consume_stream(DB, Stream, TopicFilter, StartTime),
            emqx_ds_test_helpers:diff_messages(?msg_fields, [Msg1, Msg3], Batch),

            ok = emqx_ds:add_generation(DB),

            ?assertMatch({ok, end_of_stream}, emqx_ds:delete_next(DB, DIter2, Selector, 1))
        end,
        []
    ),
    ok.

t_drop_generation_with_never_used_iterator(Config) ->
    %% This test checks how the iterator behaves when:
    %%   1) it's created at generation 1 and not consumed from.
    %%   2) generation 2 is created and 1 dropped.
    %%   3) iteration begins.
    %% In this case, the iterator won't see any messages and the stream will end.

    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    [GenId0] = maps:keys(emqx_ds:list_generations_with_lifetimes(DB)),

    TopicFilter = emqx_topic:words(<<"foo/+">>),
    StartTime = 0,
    Msgs0 = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo/bar">>, <<"2">>, 1)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs0, #{sync => true})),
    timer:sleep(1_000),

    [{_, Stream0}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream0, TopicFilter, StartTime),
    %% Rotate the generations:
    ok = emqx_ds:add_generation(DB),
    ok = emqx_ds:drop_generation(DB, GenId0),
    timer:sleep(1_000),
    [GenId1] = maps:keys(emqx_ds:list_generations_with_lifetimes(DB)),
    ?assertNotEqual(GenId1, GenId0),

    Now = emqx_message:timestamp_now(),
    Msgs1 = [
        message(<<"foo/bar">>, <<"3">>, Now + 100),
        message(<<"foo/bar">>, <<"4">>, Now + 101)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs1, #{sync => true})),
    timer:sleep(1_000),

    ?assertError(
        {error, unrecoverable, generation_not_found},
        emqx_ds_test_helpers:consume_iter(DB, Iter0)
    ),
    %% New iterator for the new stream will only see the later messages.
    [{_, Stream1}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    ?assertNotEqual(Stream0, Stream1),
    {ok, Iter1} = emqx_ds:make_iterator(DB, Stream1, TopicFilter, StartTime),

    {ok, _Iter, Batch} = emqx_ds_test_helpers:consume_iter(DB, Iter1, #{batch_size => 1}),
    emqx_ds_test_helpers:diff_messages(?msg_fields, Msgs1, Batch).

t_drop_generation_with_used_once_iterator(Config) ->
    %% This test checks how the iterator behaves when:
    %%   1) it's created at generation 1 and consumes at least 1 message.
    %%   2) generation 2 is created and 1 dropped.
    %%   3) iteration continues.
    %% In this case, the iterator should see no more messages and the stream will end.

    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    [GenId0] = maps:keys(emqx_ds:list_generations_with_lifetimes(DB)),

    TopicFilter = emqx_topic:words(<<"foo/+">>),
    StartTime = 0,
    Msgs0 =
        [Msg0 | _] = [
            message(<<"foo/bar">>, <<"1">>, 0),
            message(<<"foo/bar">>, <<"2">>, 1)
        ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs0)),
    timer:sleep(1_000),

    [{_, Stream0}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream0, TopicFilter, StartTime),
    {ok, Iter1, Batch1} = emqx_ds:next(DB, Iter0, 1),
    ?assertNotEqual(end_of_stream, Iter1),
    emqx_ds_test_helpers:diff_messages(?msg_fields, [Msg0], [Msg || {_Key, Msg} <- Batch1]),

    ok = emqx_ds:add_generation(DB),
    ok = emqx_ds:drop_generation(DB, GenId0),

    Now = emqx_message:timestamp_now(),
    Msgs1 = [
        message(<<"foo/bar">>, <<"3">>, Now + 100),
        message(<<"foo/bar">>, <<"4">>, Now + 101)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs1)),

    ?assertError(
        {error, unrecoverable, generation_not_found},
        emqx_ds_test_helpers:consume_iter(DB, Iter1)
    ).

t_make_iterator_stale_stream(Config) ->
    %% This checks the behavior of `emqx_ds:make_iterator' after the generation underlying
    %% the stream has been dropped.

    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    [GenId0] = maps:keys(emqx_ds:list_generations_with_lifetimes(DB)),

    TopicFilter = emqx_topic:words(<<"foo/+">>),
    StartTime = 0,
    Msgs0 = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo/bar">>, <<"2">>, 1)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs0)),
    timer:sleep(1_000),

    [{_, Stream0}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),

    ok = emqx_ds:add_generation(DB),
    ok = emqx_ds:drop_generation(DB, GenId0),
    timer:sleep(1_000),

    ?assertEqual(
        {error, unrecoverable, generation_not_found},
        emqx_ds:make_iterator(DB, Stream0, TopicFilter, StartTime)
    ).

%% @doc This is a smoke test for `subscribe' and `unsubscribe' APIs
t_sub_unsub(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            Stream = make_stream(Config),
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, Handle, _MRef} = emqx_ds:subscribe(DB, It, #{max_unacked => 100}),
            %% Subscription is registered:
            ?assertMatch(
                #{},
                emqx_ds:subscription_info(DB, Handle),
                #{handle => Handle}
            ),
            %% Unsubscribe and check that subscription has been
            %% unregistered:
            ?assertMatch(true, emqx_ds:unsubscribe(DB, Handle)),
            ?assertMatch(
                undefined,
                emqx_ds:subscription_info(DB, Handle),
                #{handle => Handle}
            ),
            %% Try to unsubscribe with invalid handle:
            ?assertMatch(false, emqx_ds:unsubscribe(DB, Handle)),
            ?assertMatch([], collect_down_msgs())
        end,
        []
    ).

%% @doc Verify the scenario where a subscriber terminates without
%% unsubscribing. We test this by creating a subscription from a
%% temporary process that exits normally. DS should automatically
%% remove this subscription.
t_sub_dead_subscriber_cleanup(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            Stream = make_stream(Config),
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            Parent = self(),
            Child = spawn_link(
                fun() ->
                    {ok, Handle, _MRef} = emqx_ds:subscribe(DB, It, #{
                        max_unacked => 100
                    }),
                    Parent ! {ready, Handle},
                    receive
                        exit -> ok
                    end
                end
            ),
            receive
                {ready, Handle} -> ok
            end,
            %% Currently the process is running. Verify that the
            %% subscription is present:
            ?assertMatch(
                #{},
                emqx_ds:subscription_info(DB, Handle),
                #{handle => Handle}
            ),
            %% Shutdown the child process and verify that the
            %% subscription has been automatically removed:
            Child ! exit,
            timer:sleep(100),
            ?assertMatch(false, is_process_alive(Child)),
            ?assertMatch(
                undefined,
                emqx_ds:subscription_info(DB, Handle),
                #{handle => Handle}
            )
        end,
        []
    ).

%% @doc Verify that a client receives `DOWN' message when the server
%% goes down:
t_sub_shard_down_notify(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            Stream = make_stream(Config),
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, _Handle, MRef} = emqx_ds:subscribe(DB, It, #{max_unacked => 100}),
            ?assertEqual(ok, emqx_ds:close_db(DB)),
            ?assertEqual([MRef], lists:usort(collect_down_msgs()))
        end,
        []
    ).

%% @doc Verify that a client is notified when the beamformer worker
%% currently owning the subscription dies:
t_sub_worker_down_notify(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{},
        try
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            Stream = make_stream(Config),
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, _Handle, MRef} = emqx_ds:subscribe(DB, It, #{max_unacked => 100}),
            %% Inject an error that should crash the worker:
            meck:new(emqx_ds_beamformer, [passthrough, no_history]),
            meck:expect(emqx_ds_beamformer, scan_stream, fun(
                _Mod, _Shard, _Stream, _TopicFilter, _StartKey, _BatchSize
            ) ->
                error(injected)
            end),
            %% Publish some messages to trigger stream scan leading up
            %% to the crash:
            publish_seq(DB, <<"t">>, 1, 1),
            %% Recieve notification:
            receive
                {'DOWN', MRef, process, Pid, Reason} ->
                    ?assertMatch(worker_crash, Reason),
                    ?assert(is_pid(Pid))
            after 5000 ->
                error(timeout_waiting_for_down)
            end
        after
            meck:unload()
        end,
        []
    ).

%% @doc Verify behavior of a subscription that replayes old messages.
%% This testcase focuses on the correctness of `catchup' beamformer
%% workers.
t_sub_catchup(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            Stream = make_stream(Config),
            %% Fill the storage and close the generation:
            publish_seq(DB, <<"t">>, 1, 9),
            emqx_ds:add_generation(DB),
            %% Subscribe:
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, Handle, SubRef} = emqx_ds:subscribe(DB, It, #{max_unacked => 3}),
            %% We receive one batch and stop waiting for ack. Note:
            %% batch may contain more messages than `max_unacked',
            %% because batch size is independent.
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef,
                        lagging = true,
                        seqno = 5,
                        size = 5,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"0">>}},
                                {_, #message{payload = <<"1">>}},
                                {_, #message{payload = <<"2">>}},
                                {_, #message{payload = <<"3">>}},
                                {_, #message{payload = <<"4">>}}
                            ]}
                    }
                ],
                recv(SubRef, 5)
            ),
            %% Ack and receive the rest of the messages:
            ?assertMatch(ok, emqx_ds:suback(DB, Handle, 5)),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef,
                        lagging = true,
                        seqno = 10,
                        size = 5,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"5">>}},
                                {_, #message{payload = <<"6">>}},
                                {_, #message{payload = <<"7">>}},
                                {_, #message{payload = <<"8">>}},
                                {_, #message{payload = <<"9">>}}
                            ]}
                    }
                ],
                recv(SubRef, 5)
            ),
            %% Ack and receive `end_of_stream':
            ?assertMatch(ok, emqx_ds:suback(DB, Handle, 10)),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef,
                        seqno = 11,
                        size = 1,
                        payload = {ok, end_of_stream}
                    }
                ],
                recv(SubRef, 1)
            )
        end,
        []
    ).

%% @doc Verify behavior of a subscription that always stays at the top
%% of the stream. This testcase focuses on the correctness of
%% `rt' beamformer workers.
t_sub_realtime(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            Stream = make_stream(Config),
            %% Subscribe:
            {ok, It} = emqx_ds:make_iterator(
                DB, Stream, [<<"t">>], erlang:system_time(millisecond)
            ),
            {ok, Handle, SubRef} = emqx_ds:subscribe(DB, It, #{max_unacked => 100}),
            timer:sleep(1_000),
            %% Publish/consume/ack loop:
            ?tp(notice, test_publish_first_batch, #{}),
            publish_seq(DB, <<"t">>, 1, 2),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef,
                        lagging = false,
                        stuck = false,
                        seqno = 2,
                        size = 2,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"1">>}},
                                {_, #message{payload = <<"2">>}}
                            ]}
                    }
                ],
                recv(SubRef, 2),
                #{sub_info => emqx_ds:subscription_info(DB, Handle)}
            ),
            ?tp(notice, test_publish_second_batch, #{}),
            publish_seq(DB, <<"t">>, 3, 4),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef,
                        lagging = false,
                        stuck = false,
                        seqno = 4,
                        size = 2,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"3">>}},
                                {_, #message{payload = <<"4">>}}
                            ]}
                    }
                ],
                recv(SubRef, 2),
                #{sub_info => emqx_ds:subscription_info(DB, Handle)}
            ),
            ?assertMatch(ok, emqx_ds:suback(DB, Handle, 4)),
            %% Close the generation. The subscriber should be promptly
            %% notified:
            ?tp(notice, test_rotate_generations, #{}),
            ?assertMatch(ok, emqx_ds:add_generation(DB)),
            ?assertMatch(
                [#ds_sub_reply{ref = SubRef, seqno = 5, payload = {ok, end_of_stream}}],
                recv(SubRef, 1),
                #{sub_info => emqx_ds:subscription_info(DB, Handle)}
            )
        end,
        []
    ).

%% @doc This testcase verifies that multiple clients can consume
%% messages from a topics with injected wildcards in parallel:
t_sub_wildcard(Config) ->
    DB = ?FUNCTION_NAME,
    Topics = [<<"t/1">>, <<"t/2">>, <<"t/3">>],
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            create_wildcard(DB, <<"t">>),
            %% Insert some data before starting the clients to test catchup:
            Expect1 = [publish_seq(DB, Topic, 0, 9) || Topic <- Topics],
            %% Start a mix of clients subscribing to both wildcard and concrete topics:
            [{_, Stream}] = emqx_ds:get_streams(DB, [<<"t">>, '#'], 0),
            Clients = [
                begin
                    {ok, It} = emqx_ds:make_iterator(DB, Stream, emqx_topic:words(Topic), 0),
                    {ok, _Handle, SubRef} = emqx_ds:subscribe(DB, It, #{max_unacked => 100}),
                    #{topic => Topic, sub_ref => SubRef, it => It}
                end
             || Topic <- [<<"t/#">> | Topics],
                _Duplicate <- [1, 2]
            ],
            %% They should get the old messages immediately:
            ?assertMatch(N when N > 0, verify_receive(lists:append(Expect1), Clients)),
            %% Verify realtime delivery flow:
            Expect2 = [publish_seq(DB, Topic, 10, 20) || Topic <- Topics],
            ?assertMatch(N when N > 0, verify_receive(lists:append(Expect2), Clients))
        end,
        []
    ).

verify_receive(_Messages, []) ->
    0;
verify_receive(Messages, [#{topic := TF, sub_ref := SubRef} | Rest]) ->
    Expected = [Msg || #message{topic = T} = Msg <- Messages, emqx_topic:match(T, TF)],
    Got = [
        Msg
     || #ds_sub_reply{payload = {ok, _It, Msgs}} <- recv(SubRef, length(Expected)),
        Msg <- Msgs
    ],
    emqx_ds_test_helpers:diff_messages(?msg_fields, Expected, Got),
    length(Expected) + verify_receive(Messages, Rest).

%% @doc This testcase emulates a slow subscriber.
t_sub_slow(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            Stream = make_stream(Config),
            %% Subscribe:
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, Handle, SubRef} = emqx_ds:subscribe(DB, It, #{max_unacked => 3}),
            %% Check receiving of messages published at the beginning:
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef,
                        lagging = true,
                        stuck = false,
                        seqno = 1,
                        size = 1,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"0">>}}
                            ]}
                    }
                ],
                recv(SubRef, 1),
                #{sub_info => emqx_ds:subscription_info(DB, Handle)}
            ),
            %% Publish more data, it should result in an event:
            publish_seq(DB, <<"t">>, 1, 2),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef,
                        lagging = false,
                        stuck = true,
                        seqno = 3,
                        size = 2,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"1">>}},
                                {_, #message{payload = <<"2">>}}
                            ]}
                    }
                ],
                recv(SubRef, 2),
                #{sub_info => emqx_ds:subscription_info(DB, Handle)}
            ),
            %% Fill more data:
            publish_seq(DB, <<"t">>, 3, 4),
            %% This data should NOT be delivered to the subscriber
            %% until it acks enough messages:
            ?assertMatch([], recv(SubRef, 1)),
            %% Ack sequence number:
            ?assertMatch(ok, emqx_ds:suback(DB, Handle, 3)),
            %% Now we get the messages:
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef,
                        lagging = true,
                        stuck = false,
                        seqno = 5,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"3">>}},
                                {_, #message{payload = <<"4">>}}
                            ]}
                    }
                ],
                recv(SubRef, 2),
                #{sub_info => emqx_ds:subscription_info(DB, Handle)}
            )
        end,
        []
    ).

%% @doc Remove generation during catchup. The client should receive an
%% unrecoverable error.
t_sub_catchup_unrecoverable(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            Stream = make_stream(Config),
            %% Fill the storage and close the generation:
            publish_seq(DB, <<"t">>, 1, 9),
            emqx_ds:add_generation(DB),
            %% Subscribe:
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, Handle, SubRef} = emqx_ds:subscribe(DB, It, #{max_unacked => 3}),
            %% Receive a batch and pause for the ack:
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef,
                        seqno = 5,
                        size = 5,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"0">>}},
                                {_, #message{payload = <<"1">>}},
                                {_, #message{payload = <<"2">>}},
                                {_, #message{payload = <<"3">>}},
                                {_, #message{payload = <<"4">>}}
                            ]}
                    }
                ],
                recv(SubRef, 5)
            ),
            %% Drop generation:
            ?assertMatch(
                #{{<<"0">>, 1} := _, {<<"0">>, 2} := _},
                emqx_ds:list_generations_with_lifetimes(DB)
            ),
            ?assertMatch(ok, emqx_ds:drop_generation(DB, {<<"0">>, 1})),
            %% Ack and receive unrecoverable error:
            emqx_ds:suback(DB, Handle, 5),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef,
                        size = 1,
                        seqno = 6,
                        payload = {error, unrecoverable, generation_not_found}
                    }
                ],
                recv(SubRef, 1)
            )
        end,
        []
    ).

%% Verify functionality of the low-level transaction API.
t_13_smoke_kv_tx(Config) ->
    DB = ?FUNCTION_NAME,
    Owner = <<"test_clientid">>,
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_kv => true,
                storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% 1. Start a write-only transaction to create some data:
            {ok, Tx1} = emqx_ds:new_kv_tx(DB, #{
                generation => 1,
                shard => emqx_ds:shard_of(DB, Owner),
                timeout => infinity
            }),
            Ops1 = #{
                ?ds_tx_write => [
                    {[<<"foo">>], 0, <<"payload0">>},
                    {[<<"t">>, <<"1">>], 0, <<"payload1">>},
                    {[<<"t">>, <<"2">>], 0, <<"payload2">>}
                ]
            },
            %% Commit:
            ?assertMatch(
                {ok, _Serial},
                do_commit_tx(DB, Tx1, Ops1)
            ),

            %% Verify side effects of the transaction:
            ?assertEqual(
                [{[<<"t">>, <<"1">>], 0, <<"payload1">>}],
                emqx_ds:dirty_read(DB, [<<"t">>, <<"1">>])
            ),
            ?assertEqual(
                [{[<<"t">>, <<"2">>], 0, <<"payload2">>}],
                emqx_ds:dirty_read(DB, [<<"t">>, <<"2">>])
            ),
            %%   All topics together:
            ?assertEqual(
                [
                    {[<<"foo">>], 0, <<"payload0">>},
                    {[<<"t">>, <<"1">>], 0, <<"payload1">>},
                    {[<<"t">>, <<"2">>], 0, <<"payload2">>}
                ],
                lists:sort(emqx_ds:dirty_read(DB, ['#']))
            ),

            %% 2. Create a transaction that deletes one of the topics:
            {ok, Tx2} = emqx_ds:new_kv_tx(DB, #{
                generation => 1,
                shard => {auto, Owner},
                timeout => infinity
            }),
            Ops2 = #{
                ?ds_tx_delete_topic => [[<<"t">>, <<"2">>]]
            },
            ?assertMatch({ok, _Serial}, do_commit_tx(DB, Tx2, Ops2)),
            %% Verify that data in t/2 is gone:
            ?assertEqual(
                [
                    {[<<"foo">>], 0, <<"payload0">>},
                    {[<<"t">>, <<"1">>], 0, <<"payload1">>}
                ],
                lists:sort(emqx_ds:dirty_read(DB, ['#']))
            )
        end,
        []
    ).

%% Verify correctness of wildcard topic deletions in the transactional
%% API
t_14_kv_wildcard_deletes(Config) ->
    DB = ?FUNCTION_NAME,
    TXOpts = #{shard => {auto, <<"me">>}, timeout => infinity},
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_kv => true,
                storage =>
                    {emqx_ds_storage_skipstream_lts_v2, #{
                        timestamp_bytes => 0
                    }}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% 1. Insert test data:
            {ok, Tx1} = emqx_ds:new_kv_tx(DB, TXOpts),
            Msgs0 =
                [{[<<"foo">>], 0, <<"payload">>}] ++
                    [
                        {[<<"t">>, <<I:16>>, <<J:16>>], 0, <<I:16, J:16>>}
                     || I <- lists:seq(1, 10),
                        J <- lists:seq(1, 10)
                    ],
            Ops1 = #{?ds_tx_write => Msgs0},
            ?assertMatch({ok, _}, do_commit_tx(DB, Tx1, Ops1)),
            %% Verify that all data has been inserted:
            ?assertEqual(
                100,
                emqx_ds:fold_topic(
                    fun(_, _, _, _, Acc) -> Acc + 1 end,
                    0,
                    [<<"t">>, '+', '+'],
                    #{db => DB}
                )
            ),
            %% 2. Issue a new transaction that deletes t/10/+:
            {ok, Tx2} = emqx_ds:new_kv_tx(DB, TXOpts),
            Ops2 = #{
                ?ds_tx_delete_topic => [[<<"t">>, <<10:16>>, '+']]
            },
            ?assertMatch({ok, _}, do_commit_tx(DB, Tx2, Ops2)),
            Msgs1 = lists:filter(
                fun
                    ({[<<"t">>, <<10:16>>, _], _, _}) ->
                        false;
                    (_) ->
                        true
                end,
                Msgs0
            ),
            %% Verify side effects, t/100/+ is gone:
            ?assertEqual(
                lists:sort(Msgs1),
                lists:sort(emqx_ds:dirty_read(DB, ['#']))
            )
        end,
        []
    ).

%% Verify that `?ds_tx_serial' is properly substituted with the
%% transaction serial.
t_15_kv_write_serial(Config) ->
    DB = ?FUNCTION_NAME,
    TXOpts = #{shard => {auto, <<"me">>}, timeout => infinity},
    Topic = [<<"foo">>],
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_kv => true,
                storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% 1. Insert test data:
            {ok, Tx1} = emqx_ds:new_kv_tx(DB, TXOpts),
            Ops = #{
                ?ds_tx_write => [{Topic, 0, ?ds_tx_serial}]
            },
            {ok, Serial1} = do_commit_tx(DB, Tx1, Ops),
            %% Verify that foo is set to Serial:
            ?assertEqual(
                [{Topic, 0, Serial1}],
                emqx_ds:dirty_read(DB, Topic)
            ),
            %% Repeat the procedure and make sure serial increases
            {ok, Tx2} = emqx_ds:new_kv_tx(DB, TXOpts),
            {ok, Serial2} = do_commit_tx(DB, Tx2, Ops),
            ?assertEqual(
                [{Topic, 0, Serial2}],
                emqx_ds:dirty_read(DB, Topic)
            ),
            ?assert(Serial2 > Serial1, [Serial2, Serial1])
        end,
        []
    ).

t_16_kv_preconditions(Config) ->
    DB = ?FUNCTION_NAME,
    TXOpts = #{shard => {auto, <<"me">>}, timeout => infinity},
    Topic1 = [<<"foo">>],
    Topic2 = [<<"bar">>],
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_kv => true,
                storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% 1. Insert test data:
            {ok, Tx1} = emqx_ds:new_kv_tx(DB, TXOpts),
            Ops1 = #{
                ?ds_tx_write => [{Topic1, 0, <<"payload0">>}]
            },
            ?assertMatch({ok, _}, do_commit_tx(DB, Tx1, Ops1)),

            %% 2. Preconditions, successful.
            {ok, Tx2} = emqx_ds:new_kv_tx(DB, TXOpts),
            Ops2 = #{
                ?ds_tx_expected => [
                    {Topic1, 0, '_'},
                    {Topic1, 0, <<"payload0">>}
                ],
                ?ds_tx_unexpected => [{Topic2, 0}],
                ?ds_tx_write => [{Topic1, 0, <<"payload1">>}]
            },
            ?assertMatch(
                {ok, _},
                do_commit_tx(DB, Tx2, Ops2)
            ),
            ?assertEqual(
                [{Topic1, 0, <<"payload1">>}],
                emqx_ds:dirty_read(DB, ['#'])
            ),

            %% 3. Precondiction fail, unexpected message:
            {ok, Tx3} = emqx_ds:new_kv_tx(DB, TXOpts),
            Ops3 = #{
                ?ds_tx_unexpected => [{Topic1, 0}],
                ?ds_tx_write => [{Topic1, 0, <<"fail">>}]
            },
            ?assertMatch(
                ?err_unrec({precondition_failed, _}),
                do_commit_tx(DB, Tx3, Ops3)
            ),

            %% 4 Preconditions, fail, expected message not found:
            {ok, Tx4} = emqx_ds:new_kv_tx(DB, TXOpts),
            Ops4 = #{
                ?ds_tx_expected => [{Topic2, 0, '_'}],
                ?ds_tx_write => [{Topic2, 0, <<"fail">>}]
            },
            ?assertMatch(
                ?err_unrec({precondition_failed, _}),
                do_commit_tx(DB, Tx4, Ops4)
            ),

            %% 5 Preconditions, fail, value is different:
            {ok, Tx5} = emqx_ds:new_kv_tx(DB, TXOpts),
            Ops5 = #{
                ?ds_tx_expected => [{Topic1, 0, <<"payload0">>}],
                ?ds_tx_write => [{Topic1, 0, <<"fail">>}]
            },
            ?assertMatch(
                ?err_unrec(
                    {precondition_failed, [
                        #{expected := <<"payload0">>, got := <<"payload1">>, topic := Topic1}
                    ]}
                ),
                do_commit_tx(DB, Tx5, Ops5)
            ),

            %% Verify that side effects of failed transactions weren't
            %% applied:
            ?assertEqual(
                [{Topic1, 0, <<"payload1">>}],
                emqx_ds:dirty_read(DB, ['#'])
            )
        end,
        []
    ).

%% Verify basic functions of the high-level transaction wrapper:
t_17_tx_wrapper(Config) ->
    DB = ?FUNCTION_NAME,
    TXOpts = #{db => DB, shard => {auto, <<"me">>}, generation => 1},
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_kv => true,
                storage =>
                    {emqx_ds_storage_skipstream_lts_v2, #{
                        timestamp_bytes => 0
                    }}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% Transaction wrapper uses process dictionary to store
            %% pending ops. Make the snapshot of the PD to make sure
            %% no garbage is left behind:
            PD = lists:sort(get()),
            %% 1. Empty transaction:
            ?assertEqual(
                {nop, hello},
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        hello
                    end
                )
            ),
            ?assertEqual(PD, lists:sort(get())),
            %% 2. Write transaction:
            ?assertMatch(
                {atomic, Serial, ok} when is_binary(Serial),
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        emqx_ds:tx_ttv_write([<<"foo">>], 0, <<"1">>),
                        emqx_ds:tx_ttv_write([<<"t">>, <<"1">>], 0, <<"2">>)
                    end
                )
            ),
            ?assertEqual(PD, lists:sort(get())),
            ?assertEqual(
                [
                    {[<<"foo">>], 0, <<"1">>},
                    {[<<"t">>, <<"1">>], 0, <<"2">>}
                ],
                lists:sort(emqx_ds:dirty_read(DB, ['#']))
            ),
            %% 3. Delete transaction:
            ?assertMatch(
                {atomic, _Serial, ok},
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        emqx_ds:tx_del_topic([<<"foo">>])
                    end
                )
            ),
            ?assertEqual(PD, lists:sort(get())),
            ?assertEqual(
                [
                    {[<<"t">>, <<"1">>], 0, <<"2">>}
                ],
                lists:sort(emqx_ds:dirty_read(DB, ['#']))
            )
        end,
        []
    ).

%% Verify properties of asynchronous transaction commit
t_18_async_trans(Config) ->
    DB = ?FUNCTION_NAME,
    Timeout = 1_000,
    TXOpts = #{
        db => DB, shard => {auto, <<"me">>}, sync => false, timeout => Timeout, generation => 1
    },
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_kv => true,
                storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% 1. Successful async write transaction:
            {async, Ref1, hello} =
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        emqx_ds:tx_ttv_write([<<"foo">>], 0, ?ds_tx_serial),
                        hello
                    end
                ),
            %% Wait for commit and verify side effects:
            receive
                ?ds_tx_commit_reply(Ref1, Reply1) ->
                    {ok, Serial1} = emqx_ds:tx_commit_outcome(DB, Ref1, Reply1)
            end,
            ?assertEqual(
                [
                    {[<<"foo">>], 0, Serial1}
                ],
                lists:sort(emqx_ds:dirty_read(DB, ['#']))
            ),
            %% 2. Verify error handling (preconditions)
            {async, Ref2, there} =
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        emqx_ds:tx_ttv_write([<<"foo">>], 0, ?ds_tx_serial),
                        emqx_ds:tx_ttv_assert_present([<<"bar">>], 0, '_'),
                        there
                    end
                ),
            %% Wait for the commit outcome:
            receive
                ?ds_tx_commit_reply(Ref2, Reply2) ->
                    ?assertMatch(
                        {error, unrecoverable,
                            {precondition_failed, [
                                #{
                                    expected := '_',
                                    got := undefined,
                                    topic := [<<"bar">>]
                                }
                            ]}},
                        emqx_ds:tx_commit_outcome(DB, Ref2, Reply2)
                    )
            end,
            %% 3. Verify absence of stray unexpected messages, such as
            %% timeouts.
            timer:sleep(Timeout * 2),
            ?assertMatch([], mailbox()),

            %% 4. Timeout (there's a miniscule chance of committing
            %% before the timeout of 0 which might make this case
            %% unstable)
            {async, Ref3, ok} =
                emqx_ds:trans(
                    TXOpts#{timeout => 0},
                    fun() ->
                        emqx_ds:tx_ttv_write([<<"foo">>], 0, ?ds_tx_serial)
                    end
                ),
            %% Wait for timeout
            receive
                ?ds_tx_commit_reply(Ref3, Reply3) ->
                    ?assertMatch(
                        {error, unrecoverable, timeout},
                        emqx_ds:tx_commit_outcome(DB, Ref3, Reply3)
                    )
            end
        end,
        []
    ).

message(ClientId, Topic, Payload, PublishedAt) ->
    Msg = message(Topic, Payload, PublishedAt),
    Msg#message{from = ClientId}.

message(Topic, Payload, PublishedAt) ->
    #message{
        topic = try_format(Topic),
        payload = try_format(Payload),
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

matcher(ClientID, Topic, Payload, Timestamp) ->
    #message_matcher{
        from = ClientID,
        topic = try_format(Topic),
        timestamp = Timestamp,
        payload = Payload
    }.

try_format({Fmt, Args}) ->
    emqx_utils:format(Fmt, Args);
try_format(String) ->
    String.

delete(DB, It, Selector, BatchSize) ->
    delete(DB, It, Selector, BatchSize, 0).

delete(DB, It0, Selector, BatchSize, Acc) ->
    case emqx_ds:delete_next(DB, It0, Selector, BatchSize) of
        {ok, It, 0} ->
            {ok, It, Acc};
        {ok, It, NumDeleted} ->
            delete(DB, It, Selector, BatchSize, Acc + NumDeleted);
        {ok, end_of_stream} ->
            {ok, end_of_stream, Acc};
        Ret ->
            Ret
    end.

%% CT callbacks

all() ->
    [{group, Backend} || Backend <- backends()].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    lists:map(
        fun(Backend) ->
            TCs =
                case Backend of
                    emqx_ds_builtin_local ->
                        AllTCs;
                    _ ->
                        AllTCs --
                            [
                                t_13_smoke_kv_tx,
                                t_14_kv_wildcard_deletes,
                                t_15_kv_write_serial,
                                t_16_kv_preconditions,
                                t_17_tx_wrapper,
                                t_18_async_trans
                            ]
                end,
            {Backend, TCs}
        end,
        backends()
    ).

init_per_group(emqx_ds_builtin_raft, Config) ->
    %% Raft backend is an odd one, as its main module is named
    %% `emqx_ds_replication_layer' for historical reasons:
    [
        {backend, emqx_ds_replication_layer},
        {ds_conf, emqx_ds_replication_layer:test_db_config(Config)}
        | Config
    ];
init_per_group(Backend, Config) ->
    [
        {backend, Backend},
        {ds_conf, Backend:test_db_config(Config)}
        | Config
    ].

end_per_group(_Group, Config) ->
    Config.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

suite() ->
    [{timetrap, 50_000}].

init_per_testcase(TC, Config) ->
    Backend = proplists:get_value(backend, Config),
    %% TODO: add a nicer way to deal with transient configuration. It
    %% should be possible to pass options like this to `open_db':
    AppConfig =
        case TC of
            _ when TC =:= t_sub_catchup; TC =:= t_sub_catchup_unrecoverable ->
                #{emqx_durable_storage => #{override_env => [{poll_batch_size, 5}]}};
            _ ->
                #{}
        end,
    Apps = emqx_cth_suite:start(Backend:test_applications(AppConfig), #{
        work_dir => emqx_cth_suite:work_dir(TC, Config)
    }),
    ct:pal("Started apps: ~p", [Apps]),
    timer:sleep(1000),
    [{apps, Apps}, {tc, TC} | Config].

end_per_testcase(TC, Config) ->
    catch ok = emqx_ds:drop_db(TC),
    catch ok = emqx_cth_suite:stop(?config(apps, Config)),
    catch mnesia:delete_schema([node()]),
    snabbkaffe:stop(),
    ok.

emqx_ds_open_db(DB, Opts) ->
    ct:pal("Opening DB ~p with options ~p", [DB, Opts]),
    case emqx_ds:open_db(DB, Opts) of
        ok -> timer:sleep(1000);
        Other -> Other
    end.

backends() ->
    application:load(emqx_ds_backends),
    {ok, L} = application:get_env(emqx_ds_backends, available_backends),
    L.

opts(Config) ->
    proplists:get_value(ds_conf, Config).

%% Subscription-related helper functions:

%% @doc Recieve poll replies with given SubRef:
recv(SubRef, N) ->
    recv(SubRef, N, 5000).

recv(_SubRef, 0, _Timeout) ->
    [];
recv(SubRef, N, Timeout) ->
    T0 = erlang:monotonic_time(millisecond),
    receive
        #ds_sub_reply{ref = SubRef, size = Size} = Msg ->
            T1 = erlang:monotonic_time(millisecond),
            NextTimeout = max(0, Timeout - (T1 - T0)),
            [Msg | recv(SubRef, N - Size, NextTimeout)];
        {'DOWN', SubRef, _, _, Reason} ->
            error({unexpected_beamformer_termination, Reason})
    after Timeout ->
        []
    end.

collect_down_msgs() ->
    receive
        {'DOWN', MRef, _, _, _} ->
            [MRef | collect_down_msgs()]
    after 100 ->
        []
    end.

%% Fill topic "t" with some data and return the corresponding stream:
make_stream(Config) ->
    DB = proplists:get_value(tc, Config),
    publish_seq(DB, <<"t">>, 0, 0),
    timer:sleep(100),
    [{_, Stream}] = emqx_ds:get_streams(DB, [<<"t">>], 0),
    Stream.

%% @doc Publish sequence of integers from `Start' to `End' to a topic:
publish_seq(DB, Topic, Start, End) ->
    Batch = [
        emqx_message:make(<<"pub">>, Topic, integer_to_binary(I))
     || I <- lists:seq(Start, End)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Batch)),
    Batch.

%% @doc Create a learned wildcard for a given topic prefix:
create_wildcard(DB, Prefix) ->
    %% Introduce enough topics to learn the wildcard:
    ?assertMatch(
        ok,
        emqx_ds:store_batch(
            DB,
            [message({"~s/~p", [Prefix, I]}, <<"">>, 0) || I <- lists:seq(1, 100)]
        )
    ),
    %% Rotate generations; new one should inherit learned wildcards:
    ?assertMatch(ok, emqx_ds:add_generation(DB)),
    [GenToDel] = [
        GenId
     || {GenId, #{until := Until}} <- maps:to_list(
            emqx_ds:list_generations_with_lifetimes(DB)
        ),
        is_integer(Until)
    ],
    ?assertMatch(ok, emqx_ds:drop_generation(DB, GenToDel)).

%% Manual sync wrapper for the low-level DS transaction API.
do_commit_tx(DB, Ctx, Ops) ->
    Ref = emqx_ds:commit_tx(DB, Ctx, Ops),
    ?assert(is_reference(Ref)),
    receive
        ?ds_tx_commit_reply(Ref, Reply) ->
            emqx_ds:tx_commit_outcome(DB, Ref, Reply)
    after 5_000 ->
        error({timeout, mailbox()})
    end.

mailbox() ->
    receive
        A -> [A | mailbox()]
    after 0 ->
        []
    end.
