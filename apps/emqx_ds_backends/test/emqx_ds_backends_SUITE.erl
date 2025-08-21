%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_backends_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
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
    {[{_, Stream}], []} = emqx_ds:get_streams(DB, TopicFilter, StartTime, #{}),
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
    {[{_, Stream}], []} = emqx_ds:get_streams(DB, TopicFilter, StartTime, #{}),
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
        emqx_ds:list_slabs(DB)
    ),

    ?assertMatch(ok, emqx_ds:add_generation(DB)),
    [
        {Gen1, #{created_at := Created1, since := Since1, until := Until1}},
        {_Gen2, #{created_at := Created2, since := Since2, until := undefined}}
    ] = maps:to_list(emqx_ds:list_slabs(DB)),
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
        maps:to_list(emqx_ds:list_slabs(DB))
    ),
    ?assertMatch(ok, emqx_ds:update_db_config(DB, opts(Config))),
    ?assertMatch(
        [{_, _}, {_, _}],
        maps:to_list(emqx_ds:list_slabs(DB))
    ).

%% Verifies the basic usage of `list_slabs' and `drop_generation'...
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
            Generations0 = emqx_ds:list_slabs(DB),
            ?assertMatch(
                [{_GenId, #{since := _, until := _}}],
                maps:to_list(Generations0),
                #{gens => Generations0}
            ),
            [{GenId0, _}] = maps:to_list(Generations0),
            %% Cannot delete current generation
            ?assertEqual({error, current_generation}, emqx_ds:drop_slab(DB, GenId0)),

            %% New gen
            ok = emqx_ds:add_generation(DB),
            Generations1 = emqx_ds:list_slabs(DB),
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
            ?assertEqual(ok, emqx_ds:drop_slab(DB, GenId0)),
            Generations2 = emqx_ds:list_slabs(DB),
            ?assertMatch(
                [{GenId1, #{since := _, until := _}}],
                lists:sort(maps:to_list(Generations2)),
                #{gens => Generations2}
            ),

            %% Unknown gen_id, as it was already dropped
            ?assertEqual({error, not_found}, emqx_ds:drop_slab(DB, GenId0)),

            %% Should persist surviving generation list
            ok = application:stop(emqx_durable_storage),
            {ok, _} = application:ensure_all_started(emqx_durable_storage),
            ok = emqx_ds_open_db(DB, opts(Config)),

            Generations3 = emqx_ds:list_slabs(DB),
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
                emqx_ds:dirty_read(DB, emqx_topic:words(<<"t/#">>))
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
            DBMessages = emqx_ds:dirty_read(DB, emqx_topic:words(<<"t/#">>)),
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
    [GenId0] = maps:keys(emqx_ds:list_slabs(DB)),

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
    ok = emqx_ds:drop_slab(DB, GenId0),
    timer:sleep(1_000),
    [GenId1] = maps:keys(emqx_ds:list_slabs(DB)),
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
    [GenId0] = maps:keys(emqx_ds:list_slabs(DB)),

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
    emqx_ds_test_helpers:diff_messages(?msg_fields, [Msg0], Batch1),

    ok = emqx_ds:add_generation(DB),
    ok = emqx_ds:drop_slab(DB, GenId0),

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
    [GenId0] = maps:keys(emqx_ds:list_slabs(DB)),

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
    ok = emqx_ds:drop_slab(DB, GenId0),
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
                                #message{payload = <<"0">>},
                                #message{payload = <<"1">>},
                                #message{payload = <<"2">>},
                                #message{payload = <<"3">>},
                                #message{payload = <<"4">>}
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
                                #message{payload = <<"5">>},
                                #message{payload = <<"6">>},
                                #message{payload = <<"7">>},
                                #message{payload = <<"8">>},
                                #message{payload = <<"9">>}
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
                                #message{payload = <<"1">>},
                                #message{payload = <<"2">>}
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
                                #message{payload = <<"3">>},
                                #message{payload = <<"4">>}
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

%% @doc This testcase verifies a corner case in the beamformer logic
%% when data is added while a subscription is being handed over from
%% catchup to RT worker. Such "intermediate" data should be send to
%% the subscriber if its volume is under size of the batch.
%%
%% Expected properties: subscription can be successfully handed over
%% to RT worker when intermediate data is present. The subscriber
%% receives intermediate data. If subscription's iterator cannot be
%% fast-forwarded, then the subscription is dropped with a recoverable
%% error. No duplication of messages occur.
t_sub_unclean_handover(Config) ->
    DB = ?FUNCTION_NAME,
    DefaultBatchSize = emqx_ds_beamformer:cfg_batch_size(),
    ?check_trace(
        #{timetrap => 30_000},
        try
            application:set_env(emqx_durable_storage, poll_batch_size, 3),
            %% Delay handover to RT until data is published:
            ?force_ordering(
                #{?snk_kind := test_added_data},
                #{?snk_kind := beamformer_move_to_rt}
            ),
            %% Prepare SUT:
            Opts = #{
                store_ttv => true,
                storage =>
                    {emqx_ds_storage_skipstream_lts_v2, #{
                        timestamp_bytes => 8,
                        %% Create a single stream:
                        lts_threshold_spec => {simple, {0}}
                    }}
            },
            ?assertMatch(ok, emqx_ds_open_db(DB, maps:merge(opts(Config), Opts))),
            Shard = emqx_ds:shard_of(DB, <<>>),
            TXOpts = #{db => DB, shard => Shard, generation => 1},
            PublishData = fun(Value) ->
                ?assertMatch(
                    {atomic, _, _},
                    emqx_ds:trans(
                        TXOpts,
                        fun() ->
                            [
                                emqx_ds:tx_write({
                                    [<<"t">>, <<I>>, <<J>>], ?ds_tx_ts_monotonic, Value
                                })
                             || I <- lists:seq(1, 2),
                                J <- lists:seq(1, 2)
                            ]
                        end
                    )
                )
            end,
            %% Create a stream that will be used for all iterators:
            ?assertMatch(
                {atomic, _, _},
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        emqx_ds:tx_write({[<<"ignore">>, <<"1">>, <<"2">>], 0, <<>>})
                    end
                )
            ),
            %% Create iterators and subscribe:
            {[{_Slab, Stream}], []} = emqx_ds:get_streams(DB, ['#'], 0, #{shard => Shard}),
            {ok, It0} = emqx_ds:make_iterator(DB, Stream, [<<"t">>, '+', '+'], 0),
            {ok, It1} = emqx_ds:make_iterator(DB, Stream, [<<"t">>, <<1>>, '+'], 0),
            {ok, It2} = emqx_ds:make_iterator(DB, Stream, [<<"t">>, <<1>>, <<1>>], 0),
            SubOpts = #{max_unacked => 1000},
            {ok, _, Sub0} = emqx_ds:subscribe(DB, It0, SubOpts),
            {ok, _, Sub1} = emqx_ds:subscribe(DB, It1, SubOpts),
            {ok, _, Sub2} = emqx_ds:subscribe(DB, It2, SubOpts),
            %% Publish initial batch of data that should be fulfilled
            %% via `emqx_ds_beamformer_rt:send_intermediate_beam':
            PublishData(<<1>>),
            %% No messages should be received while handover is
            %% blocked by `force_ordering`:
            receive
                A -> error({unexpected_message, A})
            after 1000 ->
                ok
            end,
            %% Unblock subscriptions:
            ?tp(notice, test_added_data, #{}),
            %%
            %% Verify first batch of data
            %%
            %% 1. Sub0 should fail because its topic received 4
            %% messages, while poll_batch_size is only 3:
            receive
                #ds_sub_reply{ref = Sub0, payload = Payload0} ->
                    ?assertMatch(?err_rec(cannot_fast_forward), Payload0)
            after 1000 ->
                error({subscription_not_received, Sub0, mailbox()})
            end,
            %% 2. Sub1 and 2 should receive the data:
            receive
                #ds_sub_reply{ref = Sub1, payload = Payload1} ->
                    ?assertMatch(
                        {ok, _, [
                            {[<<"t">>, <<1>>, <<1>>], _, <<1>>},
                            {[<<"t">>, <<1>>, <<2>>], _, <<1>>}
                        ]},
                        Payload1
                    )
            after 1000 ->
                error({subscription_not_received, Sub1, mailbox()})
            end,
            receive
                #ds_sub_reply{ref = Sub2, payload = Payload2} ->
                    ?assertMatch(
                        {ok, _, [{[<<"t">>, <<1>>, <<1>>], _, <<1>>}]},
                        Payload2
                    )
            after 1000 ->
                error({subscription_not_received, Sub2, mailbox()})
            end,
            %% Now send more data to verify that it's received by the
            %% remaining subscribers, and there's no duplication:
            PublishData(<<2>>),
            receive
                #ds_sub_reply{ref = Sub1, payload = Payload12} ->
                    ?assertMatch(
                        {ok, _, [
                            {[<<"t">>, <<1>>, <<1>>], _, <<2>>},
                            {[<<"t">>, <<1>>, <<2>>], _, <<2>>}
                        ]},
                        Payload12
                    )
            after 1000 ->
                error({subscription_not_received, Sub1, mailbox()})
            end,
            receive
                #ds_sub_reply{ref = Sub2, payload = Payload22} ->
                    ?assertMatch(
                        {ok, _, [{[<<"t">>, <<1>>, <<1>>], _, <<2>>}]},
                        Payload22
                    )
            after 1000 ->
                error({subscription_not_received, Sub2, mailbox()})
            end,
            ok
        after
            application:set_env(emqx_durable_storage, poll_batch_size, DefaultBatchSize)
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
                                #message{payload = <<"0">>}
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
                                #message{payload = <<"1">>},
                                #message{payload = <<"2">>}
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
                                #message{payload = <<"3">>},
                                #message{payload = <<"4">>}
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
                                #message{payload = <<"0">>},
                                #message{payload = <<"1">>},
                                #message{payload = <<"2">>},
                                #message{payload = <<"3">>},
                                #message{payload = <<"4">>}
                            ]}
                    }
                ],
                recv(SubRef, 5)
            ),
            %% Drop generation:
            ?assertMatch(
                #{{<<"0">>, 1} := _, {<<"0">>, 2} := _},
                emqx_ds:list_slabs(DB)
            ),
            ?assertMatch(ok, emqx_ds:drop_slab(DB, {<<"0">>, 1})),
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
t_13_smoke_ttv_tx(Config) ->
    DB = ?FUNCTION_NAME,
    Owner = <<"test_clientid">>,
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_ttv => true,
                storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% 1. Start a write-only transaction to create some data:
            {ok, Tx1} = emqx_ds:new_tx(DB, #{
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
            {ok, Tx2} = emqx_ds:new_tx(DB, #{
                generation => 1,
                shard => {auto, Owner},
                timeout => infinity
            }),
            Ops2 = #{
                ?ds_tx_delete_topic => [{[<<"t">>, <<"2">>], 0, infinity}]
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
t_14_ttv_wildcard_deletes(Config) ->
    DB = ?FUNCTION_NAME,
    TXOpts = #{shard => {auto, <<"me">>}, timeout => infinity, generation => 1},
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_ttv => true,
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
            {ok, Tx1} = emqx_ds:new_tx(DB, TXOpts),
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
                    fun(_Slab, _Stream, _Obj, Acc) -> Acc + 1 end,
                    0,
                    [<<"t">>, '+', '+'],
                    #{db => DB}
                )
            ),
            %% 2. Issue a new transaction that deletes t/10/+:
            {ok, Tx2} = emqx_ds:new_tx(DB, TXOpts),
            Ops2 = #{
                ?ds_tx_delete_topic => [{[<<"t">>, <<10:16>>, '+'], 0, infinity}]
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
t_15_ttv_write_serial(Config) ->
    DB = ?FUNCTION_NAME,
    TXOpts = #{shard => {auto, <<"me">>}, timeout => infinity, generation => 1},
    Topic = [<<"foo">>],
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_ttv => true,
                storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% 1. Insert test data:
            {ok, Tx1} = emqx_ds:new_tx(DB, TXOpts),
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
            {ok, Tx2} = emqx_ds:new_tx(DB, TXOpts),
            {ok, Serial2} = do_commit_tx(DB, Tx2, Ops),
            ?assertEqual(
                [{Topic, 0, Serial2}],
                emqx_ds:dirty_read(DB, Topic)
            ),
            ?assert(Serial2 > Serial1, [Serial2, Serial1])
        end,
        []
    ).

t_16_ttv_preconditions(Config) ->
    DB = ?FUNCTION_NAME,
    TXOpts = #{shard => {auto, <<"me">>}, timeout => infinity, generation => 1},
    Topic1 = [<<"foo">>],
    Topic2 = [<<"bar">>],
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_ttv => true,
                storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% 1. Insert test data:
            {ok, Tx1} = emqx_ds:new_tx(DB, TXOpts),
            Ops1 = #{
                ?ds_tx_write => [{Topic1, 0, <<"payload0">>}]
            },
            ?assertMatch({ok, _}, do_commit_tx(DB, Tx1, Ops1)),

            %% 2. Preconditions, successful.
            {ok, Tx2} = emqx_ds:new_tx(DB, TXOpts),
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
            {ok, Tx3} = emqx_ds:new_tx(DB, TXOpts),
            Ops3 = #{
                ?ds_tx_unexpected => [{Topic1, 0}],
                ?ds_tx_write => [{Topic1, 0, <<"fail">>}]
            },
            ?assertMatch(
                ?err_unrec({precondition_failed, _}),
                do_commit_tx(DB, Tx3, Ops3)
            ),

            %% 4 Preconditions, fail, expected message not found:
            {ok, Tx4} = emqx_ds:new_tx(DB, TXOpts),
            Ops4 = #{
                ?ds_tx_expected => [{Topic2, 0, '_'}],
                ?ds_tx_write => [{Topic2, 0, <<"fail">>}]
            },
            ?assertMatch(
                ?err_unrec({precondition_failed, _}),
                do_commit_tx(DB, Tx4, Ops4)
            ),

            %% 5 Preconditions, fail, value is different:
            {ok, Tx5} = emqx_ds:new_tx(DB, TXOpts),
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
                store_ttv => true,
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
                        emqx_ds:tx_write({[<<"foo">>], 0, <<"1">>}),
                        emqx_ds:tx_write({[<<"t">>, <<"1">>], 0, <<"2">>})
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
            ),
            %% 4. Transaction in the latest generation:
            ?assertMatch(
                {atomic, _Serial, [{_, _, <<"2">>}]},
                emqx_ds:trans(
                    TXOpts#{generation => latest},
                    fun() ->
                        emqx_ds:tx_read(['#'])
                    end
                )
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
                store_ttv => true,
                storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            timer:sleep(100),
            %% 1. Successful async write transaction:
            {async, Ref1, hello} =
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        emqx_ds:tx_write({[<<"foo">>], 0, ?ds_tx_serial}),
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
                        emqx_ds:tx_write({[<<"foo">>], 0, ?ds_tx_serial}),
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
                        emqx_ds:tx_write({[<<"foo">>], 0, ?ds_tx_serial})
                    end
                ),
            %% Wait for timeout
            receive
                ?ds_tx_commit_reply(Ref3, Reply3) ->
                    ?assertMatch(
                        {error, unrecoverable, commit_timeout},
                        emqx_ds:tx_commit_outcome(DB, Ref3, Reply3)
                    )
            end
        end,
        []
    ).

%% Verify timestamp monotonicity:
t_20_tx_monotonic_ts(Config) ->
    DB = ?FUNCTION_NAME,
    TXOpts = #{db => DB, shard => {auto, <<"me">>}, generation => 1, retries => 10},
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_ttv => true,
                storage =>
                    {emqx_ds_storage_skipstream_lts_v2, #{
                        timestamp_bytes => 8
                    }}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            {atomic, _, TransTS} =
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        emqx_ds:tx_write({[], ?ds_tx_ts_monotonic, <<0>>}),
                        emqx_ds:tx_write({[], ?ds_tx_ts_monotonic, <<1>>}),
                        emqx_ds:tx_write({[], ?ds_tx_ts_monotonic, <<2>>}),
                        erlang:system_time(microsecond)
                    end
                ),
            timer:sleep(100),
            %% Verify that the data has been written:
            Data = lists:sort(emqx_ds:dirty_read(DB, [])),
            ?assertMatch(
                [
                    {[], _, <<0>>},
                    {[], _, <<1>>},
                    {[], _, <<2>>}
                ],
                Data
            ),
            %% Verify that the assigned timestamp are within the expected range:
            lists:foreach(
                fun({_, TS, _}) ->
                    ?assert(
                        abs(TS - TransTS) < 1_000_000,
                        #{ts => TS, now => TransTS}
                    )
                end,
                Data
            ),
            %% Reopen DB and append messages:
            ?assertMatch(
                ok,
                emqx_ds:close_db(DB)
            ),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            {atomic, _, _} =
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        emqx_ds:tx_write({[<<>>], ?ds_tx_ts_monotonic, <<4>>})
                    end
                ),
            timer:sleep(100),
            %% Verify that the timestamp is still increasing (note:
            %% this condition is unlikely to fail...):
            TSBefore = lists:max([TS || {_, TS, _} <- Data]),
            [{_, TSAfterReopen, _}] = emqx_ds:dirty_read(DB, [<<>>]),
            ?assert(
                TSAfterReopen > TSBefore,
                #{before => TSBefore, after_ => TSAfterReopen}
            )
        end,
        []
    ).

%% This testcase verifies that subscriptions work for TTV style of databases.
t_21_ttv_subscription(Config) ->
    DB = ?FUNCTION_NAME,
    TXOpts = #{db => DB, shard => {auto, <<"me">>}, generation => 1, retries => 10},
    Topic0 = [<<>>],
    Topic1 = [<<>>, <<1>>],
    Topic2 = [<<>>, <<2>>],
    ?check_trace(
        begin
            %% Open the database
            Opts = maps:merge(opts(Config), #{
                store_ttv => true,
                storage =>
                    {emqx_ds_storage_skipstream_lts_v2, #{
                        timestamp_bytes => 8,
                        lts_threshold_spec => {simple, {infinity, 0}}
                    }}
            }),
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% Insert some data:
            {atomic, _, _} =
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        emqx_ds:tx_write({Topic0, ?ds_tx_ts_monotonic, <<0>>}),
                        emqx_ds:tx_write({Topic0, ?ds_tx_ts_monotonic, <<1>>}),
                        emqx_ds:tx_write({Topic0, ?ds_tx_ts_monotonic, <<2>>}),
                        emqx_ds:tx_write({Topic1, ?ds_tx_ts_monotonic, <<3>>}),
                        emqx_ds:tx_write({Topic1, ?ds_tx_ts_monotonic, <<4>>}),
                        emqx_ds:tx_write({Topic2, ?ds_tx_ts_monotonic, <<5>>}),
                        emqx_ds:tx_write({Topic2, ?ds_tx_ts_monotonic, <<6>>})
                    end
                ),
            %% Create the subscriptions:
            %%   Topic0:
            [{_, Stream0}] = emqx_ds:get_streams(DB, Topic0, 0),
            {ok, It0} = emqx_ds:make_iterator(DB, Stream0, Topic0, 0),
            {ok, SubHandle0, SubRef0} = emqx_ds:subscribe(DB, It0, #{max_unacked => 100}),
            %%   Topic1-2:
            [{_, Stream1}] = emqx_ds:get_streams(DB, Topic1, 0),
            {ok, It1} = emqx_ds:make_iterator(DB, Stream1, Topic1, 0),
            {ok, It2} = emqx_ds:make_iterator(DB, Stream1, Topic2, 0),
            {ok, It3} = emqx_ds:make_iterator(DB, Stream1, [<<>>, '+'], 0),
            {ok, SubHandle1, SubRef1} = emqx_ds:subscribe(DB, It1, #{max_unacked => 100}),
            {ok, SubHandle2, SubRef2} = emqx_ds:subscribe(DB, It2, #{max_unacked => 100}),
            {ok, SubHandle3, SubRef3} = emqx_ds:subscribe(DB, It3, #{max_unacked => 100}),
            %% Receive historical data:
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef0,
                        lagging = true,
                        seqno = 3,
                        size = 3,
                        payload =
                            {ok, _, [
                                {Topic0, _, <<0>>},
                                {Topic0, _, <<1>>},
                                {Topic0, _, <<2>>}
                            ]}
                    }
                ],
                recv(SubRef0, 1)
            ),
            ok = emqx_ds:suback(DB, SubHandle0, 3),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef1,
                        lagging = true,
                        seqno = 2,
                        size = 2,
                        payload =
                            {ok, _, [
                                {Topic1, _, <<3>>},
                                {Topic1, _, <<4>>}
                            ]}
                    }
                ],
                recv(SubRef1, 1)
            ),
            ok = emqx_ds:suback(DB, SubHandle1, 2),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef2,
                        lagging = true,
                        seqno = 2,
                        size = 2,
                        payload =
                            {ok, _, [
                                {Topic2, _, <<5>>},
                                {Topic2, _, <<6>>}
                            ]}
                    }
                ],
                recv(SubRef2, 1)
            ),
            ok = emqx_ds:suback(DB, SubHandle2, 2),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef3,
                        lagging = true,
                        seqno = 4,
                        size = 4,
                        payload =
                            {ok, _, [
                                {Topic1, _, <<3>>},
                                {Topic1, _, <<4>>},
                                {Topic2, _, <<5>>},
                                {Topic2, _, <<6>>}
                            ]}
                    }
                ],
                recv(SubRef3, 1)
            ),
            ok = emqx_ds:suback(DB, SubHandle3, 4),
            %% Publish and receive new data:
            ?tp(info, "Test: publish new data", #{}),
            {atomic, _, _} =
                emqx_ds:trans(
                    TXOpts,
                    fun() ->
                        emqx_ds:tx_write({Topic0, ?ds_tx_ts_monotonic, <<7>>}),
                        emqx_ds:tx_write({Topic0, ?ds_tx_ts_monotonic, <<8>>}),
                        emqx_ds:tx_write({Topic1, ?ds_tx_ts_monotonic, <<9>>}),
                        emqx_ds:tx_write({Topic2, ?ds_tx_ts_monotonic, <<10>>})
                    end
                ),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef0,
                        lagging = false,
                        seqno = 5,
                        size = 2,
                        payload =
                            {ok, _, [
                                {Topic0, _, <<7>>},
                                {Topic0, _, <<8>>}
                            ]}
                    }
                ],
                recv(SubRef0, 1)
            ),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef1,
                        lagging = false,
                        seqno = 3,
                        size = 1,
                        payload =
                            {ok, _, [
                                {Topic1, _, <<9>>}
                            ]}
                    }
                ],
                recv(SubRef1, 1)
            ),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef2,
                        lagging = false,
                        seqno = 3,
                        size = 1,
                        payload =
                            {ok, _, [
                                {Topic2, _, <<10>>}
                            ]}
                    }
                ],
                recv(SubRef2, 1)
            ),
            ?assertMatch(
                [
                    #ds_sub_reply{
                        ref = SubRef3,
                        lagging = false,
                        seqno = 6,
                        size = 2,
                        payload =
                            {ok, _, [
                                {Topic1, _, <<9>>},
                                {Topic2, _, <<10>>}
                            ]}
                    }
                ],
                recv(SubRef3, 1)
            )
        end,
        []
    ).

%% This testcase verifies metadata serialization and deserialization
%% for databases with store_ttv => false.
t_22_metadata_serialization(Config) ->
    DB = ?FUNCTION_NAME,
    Opts = opts(Config),
    Topics =
        [<<"foo">>, <<>>] ++
            [emqx_topic:join([<<"foo">>, integer_to_binary(N)]) || N <- lists:seq(1, 100)] ++
            [
                emqx_topic:join([<<"$foo">>, <<"bar">>, integer_to_binary(N)])
             || N <- lists:seq(1, 100)
            ],
    TopicFilters =
        [
            <<>>,
            <<"foo/#">>,
            <<"#">>,
            <<"+/+">>,
            <<"$foo/#">>,
            <<"foo/1">>,
            <<"foo/99">>,
            <<"$foo/bar/#">>,
            <<"$foo/bar/99">>
        ],
    Batch = [message(Topic, <<>>, 0) || Topic <- Topics],
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, Opts)),
            %% 1. Create generations using different layouts and
            %% insert data there to create a variety of streams and
            %% iterators.
            %%
            %%   1.1 Reference:
            ok = emqx_ds:update_db_config(DB, Opts#{storage => {emqx_ds_storage_reference, #{}}}),
            ok = emqx_ds:add_generation(DB),
            ok = emqx_ds:store_batch(DB, Batch),
            %%   1.2 Bitfield:
            ok = emqx_ds:update_db_config(DB, Opts#{storage => {emqx_ds_storage_bitfield_lts, #{}}}),
            ok = emqx_ds:add_generation(DB),
            ok = emqx_ds:store_batch(DB, Batch),
            %%   1.3 Skipstream:
            ok = emqx_ds:update_db_config(DB, Opts#{
                storage => {emqx_ds_storage_skipstream_lts, #{}}
            }),
            ok = emqx_ds:add_generation(DB),
            ok = emqx_ds:store_batch(DB, Batch),
            %%
            %% 2. Get streams and create iterators:
            timer:sleep(1000),
            {_, Streams = [_ | _]} = lists:unzip(emqx_ds:get_streams(DB, ['#'], 0)),
            Iterators = [
                It
             || Stream <- Streams,
                TopicFilter <- TopicFilters,
                {ok, It} <- [emqx_ds:make_iterator(DB, Stream, TopicFilter, 0)]
            ],
            ReplayPositions = [end_of_stream | Iterators],
            %% 3. Check transcoding of streams:
            [
                begin
                    {ok, Bin} = emqx_ds:stream_to_binary(DB, Stream),
                    ?defer_assert(
                        ?assertEqual({ok, Stream}, emqx_ds:binary_to_stream(DB, Bin))
                    )
                end
             || Stream <- Streams
            ],
            %% 4. Check transcoding of replay positions:
            [
                begin
                    {ok, Bin} = emqx_ds:iterator_to_binary(DB, Pos),
                    ?defer_assert(
                        ?assertEqual({ok, Pos}, emqx_ds:binary_to_iterator(DB, Bin))
                    )
                end
             || Pos <- ReplayPositions
            ]
        end,
        []
    ).

t_23_ttv_metadata_serialization(Config) ->
    DB = ?FUNCTION_NAME,
    Opts = maps:merge(opts(Config), #{
        store_ttv => true,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
    }),
    Topics =
        [[], [<<"foo">>]] ++
            [[<<"foo">>, <<I:32>>] || I <- lists:seq(1, 100)] ++
            [[<<"$foo">>, <<>>, <<I:32>>] || I <- lists:seq(1, 100)],
    TopicFilters =
        [
            [],
            [<<"foo">>],
            [<<"foo">>, '#'],
            [<<"$foo">>, <<>>, <<0:32>>]
        ],
    TXOpts = #{db => DB, shard => {auto, <<"me">>}, generation => 1, retries => 10},
    Trans = fun() ->
        [emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, <<>>}) || Topic <- Topics],
        ok
    end,
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, Opts)),
            %% 1. Create generations using different layouts and
            %% insert data there to create a variety of streams and
            %% iterators.
            %%
            %%   1.1 Skipstream:
            {atomic, _, _} = emqx_ds:trans(TXOpts, Trans),
            %%
            %% 2. Get streams and create iterators:
            timer:sleep(1000),
            {_, Streams = [_ | _]} = lists:unzip(emqx_ds:get_streams(DB, ['#'], 0)),
            Iterators = [
                It
             || Stream <- Streams,
                TopicFilter <- TopicFilters,
                {ok, It} <- [emqx_ds:make_iterator(DB, Stream, TopicFilter, 0)]
            ],
            ReplayPositions = [end_of_stream | Iterators],
            %% 3. Check transcoding of streams:
            [
                begin
                    {ok, Bin} = emqx_ds:stream_to_binary(DB, Stream),
                    ?defer_assert(
                        ?assertEqual({ok, Stream}, emqx_ds:binary_to_stream(DB, Bin))
                    )
                end
             || Stream <- Streams
            ],
            %% 4. Check transcoding of replay positions:
            [
                begin
                    {ok, Bin} = emqx_ds:iterator_to_binary(DB, Pos),
                    ?defer_assert(
                        ?assertEqual({ok, Pos}, emqx_ds:binary_to_iterator(DB, Bin))
                    )
                end
             || Pos <- ReplayPositions
            ]
        end,
        []
    ).

%% This testcase verifies `emqx_ds:tx_on_success' API.
t_24_tx_side_effects(Config) ->
    DB = ?FUNCTION_NAME,
    Opts = maps:merge(opts(Config), #{
        store_ttv => true,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
    }),
    TxOpts = #{db => DB, shard => {auto, <<>>}, generation => 1, retries => 10},
    %% Emit a trace as a side effect:
    SideEffect = fun(Id, Expected) ->
        ?ds_tx_on_success(?tp(test_side_effect, #{id => Id, expected => Expected}))
    end,
    ?check_trace(
        begin
            ?assertMatch(
                ok,
                emqx_ds_open_db(DB, Opts)
            ),
            %% This function verifies that transaction wrapper didn't
            %% leave any garbage in the process dictionary:
            PD = get(),
            CheckPD = fun() ->
                ?assertMatch([], get() -- PD)
            end,
            %% 1. Verify that side effects of an empty transaction are
            %% executed:
            ?assertMatch(
                {nop, ok},
                emqx_ds:trans(
                    TxOpts,
                    fun() ->
                        SideEffect(1, true)
                    end
                )
            ),
            CheckPD(),
            %% 2. Verify that side effects not executed on exception:
            ?assertError(
                _,
                emqx_ds:trans(
                    TxOpts,
                    fun() ->
                        SideEffect(2, false),
                        error(crash)
                    end
                )
            ),
            CheckPD(),
            %% 3. Verify that side effect is applied only once when
            %% transaction is successful, but is restarted before
            %% completion:
            put(restart_count, 5),
            ?assertMatch(
                {atomic, _, ok},
                emqx_ds:trans(
                    TxOpts,
                    fun() ->
                        SideEffect(3, true),
                        emqx_ds:tx_write({[], 0, <<>>}),
                        case get(restart_count) of
                            0 ->
                                erase(restart_count),
                                ok;
                            N ->
                                put(restart_count, N - 1),
                                emqx_ds:reset_trans(test)
                        end
                    end
                )
            ),
            CheckPD(),
            %% 4. Async transaction, successful:
            {async, Ref1, _} = emqx_ds:trans(
                TxOpts#{sync => false},
                fun() ->
                    %% Add multiple side effects to verify that they are executed in order:
                    SideEffect(4, true),
                    SideEffect(5, true),
                    emqx_ds:tx_write({[], 0, <<>>}),
                    SideEffect(6, true)
                end
            ),
            receive
                ?ds_tx_commit_reply(Ref1, Reply1) ->
                    {ok, _} = emqx_ds:tx_commit_outcome(DB, Ref1, Reply1)
            end,
            CheckPD(),
            %% 4. Async transaction, aborted:
            {async, Ref2, _} = emqx_ds:trans(
                TxOpts#{sync => false},
                fun() ->
                    SideEffect(7, false),
                    emqx_ds:tx_ttv_assert_absent([], 0)
                end
            ),
            receive
                ?ds_tx_commit_reply(Ref2, Reply2) ->
                    {error, unrecoverable, _} = emqx_ds:tx_commit_outcome(DB, Ref2, Reply2)
            end,
            CheckPD()
        end,
        fun(_, Trace) ->
            ?assertMatch(
                [1, 3, 4, 5, 6],
                ?projection(id, ?of_kind(test_side_effect, Trace)),
                "Sequence of IDs of the expected side effects"
            )
        end
    ).

t_25_get_streams_generation_min(Config) ->
    %% Get generations from the list of streams:
    Generations = fun(L) ->
        lists:usort([G || {{_Shard, G}, _Stream} <- L])
    end,
    %%
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    %% Make streams in the 1st generation:
    _ = publish_seq(DB, <<>>, 0, 100),
    ct:sleep(100),
    {Streams1, []} = emqx_ds:get_streams(DB, ['#'], 0, #{}),
    ?assertMatch([1], Generations(Streams1)),
    %% Add a generation and write more data:
    ?assertMatch(ok, emqx_ds:add_generation(DB)),
    _ = publish_seq(DB, <<>>, 0, 100),
    ct:sleep(100),
    %% Verify that now we have streams in both generations:
    {Streams2, []} = emqx_ds:get_streams(DB, ['#'], 0, #{}),
    ?assertMatch([1, 2], Generations(Streams2)),
    %% But the first generation is ignored when we apply the filter:
    {Streams3, []} = emqx_ds:get_streams(DB, ['#'], 0, #{generation_min => 2}),
    ?assertMatch([2], Generations(Streams3)).

t_26_ttv_next_with_upper_time_limit(Config) ->
    DB = ?FUNCTION_NAME,
    Opts = maps:merge(opts(Config), #{
        store_ttv => true,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{timestamp_bytes => 8}}
    }),
    ?check_trace(
        begin
            Topic = [<<1>>, <<1>>],
            ?assertMatch(ok, emqx_ds_open_db(DB, Opts)),
            TMax = 1 bsl 64 - 1,
            %% 1. Insert data:
            ?assertMatch(
                {atomic, _, _},
                emqx_ds:trans(
                    #{db => DB, shard => {auto, <<>>}, generation => 1},
                    fun() ->
                        emqx_ds:tx_write({Topic, 0, <<0>>}),
                        emqx_ds:tx_write({Topic, 1, <<1>>}),
                        emqx_ds:tx_write({Topic, 2, <<2>>}),
                        emqx_ds:tx_write({Topic, 3, <<3>>}),
                        emqx_ds:tx_write({Topic, 4, <<4>>}),
                        emqx_ds:tx_write({Topic, TMax, <<5>>})
                    end
                )
            ),
            ct:sleep(100),
            %% 2. Create iterators:
            {[{_, Stream}], []} = emqx_ds:get_streams(DB, Topic, 0, #{}),
            {ok, It1} = emqx_ds:make_iterator(DB, Stream, Topic, 0),
            %% 3. Read data without time limit. Note: TTV DBs don't
            %% limit the scans to the local timestamp by default.
            ?defer_assert(
                ?assertMatch(
                    {ok, _, [
                        {Topic, 0, <<0>>},
                        {Topic, 1, <<1>>},
                        {Topic, 2, <<2>>},
                        {Topic, 3, <<3>>},
                        {Topic, 4, <<4>>},
                        {Topic, TMax, <<5>>}
                    ]},
                    emqx_ds:next(DB, It1, 100)
                )
            ),
            %% 4. Read data with limit:
            ?defer_assert(
                ?assertMatch(
                    {ok, _, []},
                    emqx_ds:next(DB, It1, {time, 0, 100})
                )
            ),
            ?defer_assert(
                ?assertMatch(
                    {ok, _, [{Topic, 0, <<0>>}]},
                    emqx_ds:next(DB, It1, {time, 1, 100})
                )
            ),
            ?defer_assert(
                ?assertMatch(
                    {ok, _, [{Topic, 0, <<0>>}, {Topic, 1, <<1>>}]},
                    emqx_ds:next(DB, It1, {time, 2, 100})
                )
            ),
            ?defer_assert(
                ?assertMatch(
                    {ok, _, [{Topic, 0, <<0>>}, {Topic, 1, <<1>>}, {Topic, 2, <<2>>}]},
                    emqx_ds:next(DB, It1, {time, 3, 100})
                )
            ),
            ?defer_assert(
                ?assertMatch(
                    {ok, _, [
                        {Topic, 0, <<0>>}, {Topic, 1, <<1>>}, {Topic, 2, <<2>>}, {Topic, 3, <<3>>}
                    ]},
                    emqx_ds:next(DB, It1, {time, 4, 100})
                )
            ),
            ?defer_assert(
                ?assertMatch(
                    {ok, _, [
                        {Topic, 0, <<0>>},
                        {Topic, 1, <<1>>},
                        {Topic, 2, <<2>>},
                        {Topic, 3, <<3>>},
                        {Topic, 4, <<4>>}
                    ]},
                    emqx_ds:next(DB, It1, {time, 5, 100})
                )
            )
        end,
        []
    ).

t_27_tx_read_conflicts(Config) ->
    DB = ?FUNCTION_NAME,
    Opts = maps:merge(opts(Config), #{
        store_ttv => true,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
    }),
    TXOpts = #{shard => {auto, <<"me">>}, generation => 1, timeout => infinity},
    %% Record that should not be present in the DB:
    Canary = [{[<<"canary">>], 0, <<>>}],
    CheckCanary = fun() ->
        ?assertMatch([], emqx_ds:dirty_read(DB, [<<"canary">>]))
    end,
    %% Helper function that create a pair of transactions
    %% (simultaneously), commits the first one, and then tries to
    %% commit the second one.
    Par = fun(Ops1, Ops2) ->
        %% 1. Create context for two transactions:
        {ok, Tx1} = emqx_ds:new_tx(DB, TXOpts),
        {ok, Tx2} = emqx_ds:new_tx(DB, TXOpts),
        %% 2. Commit the first one:
        ?assertMatch(
            {ok, _},
            do_commit_tx(DB, Tx1, Ops1)
        ),
        %% 3. Try to commit the second one and return the result:
        do_commit_tx(DB, Tx2, Ops2)
    end,
    %% Wrappers:
    Ok = fun(Ops1, Ops2) ->
        ?assertMatch({ok, _}, Par(Ops1, Ops2))
    end,
    Conflict = fun(Ops1, Ops2) ->
        ?assertMatch(
            ?err_rec({read_conflict, _}),
            Par(
                Ops1,
                Ops2#{?ds_tx_write => Canary}
            )
        ),
        CheckCanary()
    end,
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, Opts)),
            %% Create data:
            Ok(
                #{
                    ?ds_tx_write => [
                        {Topic, Time, <<Time:32>>}
                     || Topic <- [[<<1>>], [<<1>>, <<2>>]],
                        Time <- lists:seq(0, 100)
                    ]
                },
                #{?ds_tx_read => [{[<<>>], 0, infinity}]}
            ),
            %% Writes:
            %%   Read the same topic as was updated:
            Conflict(
                #{?ds_tx_write => [{[<<>>], 0, <<>>}]},
                #{?ds_tx_read => [{[<<>>], 0, infinity}]}
            ),
            %%   Read the topic with '+' wildcard:
            Conflict(
                #{?ds_tx_write => [{[<<1>>, <<2>>], 0, <<>>}]},
                #{?ds_tx_read => [{[<<1>>, '+'], 0, infinity}]}
            ),
            Ok(
                #{?ds_tx_write => [{[<<1>>, <<2>>], 0, <<>>}]},
                #{?ds_tx_read => [{[<<2>>, '+'], 0, infinity}]}
            ),
            Conflict(
                #{?ds_tx_write => [{[<<2>>, <<2>>], 0, <<>>}]},
                #{?ds_tx_read => [{['+', <<2>>], 0, infinity}]}
            ),
            %%   Read the topic with '#' wildcard:
            Conflict(
                #{?ds_tx_write => [{[<<1>>, <<2>>], 0, <<>>}]},
                #{?ds_tx_read => [{['#'], 0, 0}]}
            ),
            Ok(
                #{?ds_tx_write => [{[<<2>>, <<2>>], 0, <<>>}]},
                #{?ds_tx_read => [{[<<1>>, '#'], 0, infinity}]}
            ),
            %% Topic deletions:
            Conflict(
                #{?ds_tx_delete_topic => [{[<<"foo">>], 0, infinity}]},
                #{?ds_tx_read => [{[<<"foo">>], 0, 1}]}
            ),
            Ok(
                #{?ds_tx_delete_topic => [{[<<"foo">>, <<1>>], 0, infinity}]},
                #{?ds_tx_read => [{[<<"foo">>], 0, 1}]}
            ),
            Conflict(
                #{?ds_tx_delete_topic => [{[<<"foo">>, <<1>>], 0, infinity}]},
                #{?ds_tx_read => [{[<<"foo">>, '+'], 0, infinity}]}
            ),
            Conflict(
                #{?ds_tx_delete_topic => [{[<<"foo">>, <<1>>], 0, infinity}]},
                #{?ds_tx_read => [{['#'], 0, infinity}]}
            ),
            %% No conflict because transactions operate on different
            %% time ranges:
            Ok(
                #{?ds_tx_write => [{[<<>>], 0, <<>>}]},
                #{?ds_tx_read => [{['#'], 100, infinity}]}
            ),
            Ok(
                #{?ds_tx_delete_topic => [{[<<>>, '#'], 0, 10}]},
                #{?ds_tx_read => [{['#'], 100, infinity}]}
            )
        end,
        []
    ).

%% This testcase verifies time limiting functionality of reads and topic deletions.
t_28_ttv_time_limited(Config) ->
    DB = ?FUNCTION_NAME,
    Opts = maps:merge(opts(Config), #{
        store_ttv => true,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
    }),
    Trans = fun(Fun) ->
        ?assertMatch(
            {atomic, _, _},
            emqx_ds:trans(
                #{db => DB, shard => {auto, <<"me">>}, generation => 1, timeout => infinity}, Fun
            )
        )
    end,
    Filter = fun(From, To, L) ->
        [{Topic, T, Val} || {Topic, T, Val} <- L, T >= From, T < To]
    end,
    FilterOut = fun(From, To, L) ->
        [{Topic, T, Val} || {Topic, T, Val} <- L, not (T >= From andalso T < To)]
    end,
    Compare = fun(From, To, Expect, Got) ->
        snabbkaffe_diff:assert_lists_eq(
            lists:sort(Filter(From, To, Expect)),
            lists:sort(Got)
        )
    end,
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, Opts)),
            %% Create data:
            Msgs0 = [
                {[<<>>, <<Topic:32>>], Time, <<>>}
             || Topic <- lists:seq(1, 20),
                Time <- lists:seq(1, 100)
            ],
            Trans(
                fun() ->
                    [emqx_ds:tx_write(I) || I <- Msgs0],
                    ok
                end
            ),
            %% Test reading various time ranges:
            Trans(
                fun() ->
                    Got = emqx_ds:tx_read(['#']),
                    Compare(0, infinity, Msgs0, Got)
                end
            ),
            Trans(
                fun() ->
                    Got = emqx_ds:tx_read(#{start_time => 10, end_time => 20}, ['#']),
                    Compare(10, 20, Msgs0, Got)
                end
            ),
            Trans(
                fun() ->
                    Got = emqx_ds:tx_read(#{start_time => 50}, ['#']),
                    Compare(50, infinity, Msgs0, Got)
                end
            ),
            %% Test deletions:
            Msgs1 = FilterOut(20, 50, Msgs0),
            Trans(
                fun() ->
                    emqx_ds:tx_del_topic(['#'], 20, 50)
                end
            ),
            Trans(
                fun() ->
                    Got = emqx_ds:tx_read(['#']),
                    Compare(0, infinity, Msgs1, Got)
                end
            ),
            Msgs2 = FilterOut(0, 10, Msgs1),
            Trans(
                fun() ->
                    emqx_ds:tx_del_topic(['#'], 0, 10)
                end
            ),
            Trans(
                fun() ->
                    Got = emqx_ds:tx_read(#{end_time => 70}, ['#']),
                    Compare(0, 70, Msgs2, Got)
                end
            ),
            %% Test deletion with monotonic ts limit:
            Trans(
                fun() ->
                    emqx_ds:tx_write({[<<"foo">>], ?ds_tx_ts_monotonic, <<1>>})
                end
            ),
            ?assertMatch(
                [{_, _, <<1>>}],
                emqx_ds:dirty_read(DB, [<<"foo">>])
            ),
            Trans(
                fun() ->
                    emqx_ds:tx_write({[<<"foo">>], ?ds_tx_ts_monotonic, <<2>>}),
                    emqx_ds:tx_del_topic([<<"foo">>], 0, ?ds_tx_ts_monotonic)
                end
            ),
            ?assertMatch(
                [{_, _, <<2>>}],
                emqx_ds:dirty_read(DB, [<<"foo">>])
            )
        end,
        []
    ).

%% This testcase verifies a corner case where a new generation is
%% added while a transaction created with `generation => latest` is
%% running. It is expected that such transaction will be aborted with
%% a recoverable error.
t_29_tx_latest_generation_race_condition(Config) ->
    DB = ?FUNCTION_NAME,
    Opts = maps:merge(opts(Config), #{
        store_ttv => true,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
    }),
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, Opts)),
            {ok, Tx} = emqx_ds:new_tx(DB, #{
                generation => latest,
                shard => emqx_ds:shard_of(DB, <<>>),
                timeout => infinity
            }),
            ?assertMatch(ok, emqx_ds:add_generation(DB)),
            Ops = #{
                ?ds_tx_write => [
                    {[<<"foo">>], 0, <<"payload0">>}
                ]
            },
            ?assertMatch(?err_rec(_), do_commit_tx(DB, Tx, Ops))
        end,
        []
    ).

%% This testcase veriries functionality of `emqx_ds:multi_iterator_next' function.
%%
%% Properties checked:
%%
%% - Multi-iterator covers all data  multiple streams
%% - Size of the batch is respected
t_multi_iterator(Config) ->
    DB = ?FUNCTION_NAME,
    Opts = maps:merge(opts(Config), #{
        n_shards => 5,
        store_ttv => true,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
    }),
    ?assertMatch(ok, emqx_ds_open_db(DB, Opts)),
    Slabs = maps:keys(emqx_ds:list_slabs(DB)),
    lists:foreach(
        fun({Shard, Gen}) ->
            emqx_ds:trans(
                #{db => DB, shard => Shard, generation => Gen},
                fun() ->
                    [emqx_ds:tx_write({[Shard], T, <<T>>}) || T <- lists:seq(1, 3)]
                end
            )
        end,
        Slabs
    ),
    %% Get all data from all streams in one batchs:
    (fun() ->
        MIt = emqx_ds:make_multi_iterator(#{db => DB}, ['#']),
        {L, '$end_of_table'} = emqx_ds:multi_iterator_next(#{db => DB}, ['#'], MIt, 1000),
        ?assertEqual(length(L), length(Slabs) * 3)
    end)(),
    %% Limit itertion to shard 0, split batch in two:
    (fun() ->
        ItOpts = #{db => DB, shard => <<"0">>},
        MIt0 = emqx_ds:make_multi_iterator(ItOpts, ['#']),
        {[{_, 1, <<1>>}, {_, 2, <<2>>}], MIt1} =
            emqx_ds:multi_iterator_next(ItOpts, ['#'], MIt0, 2),
        {[{_, 3, <<3>>}], '$end_of_table'} =
            emqx_ds:multi_iterator_next(ItOpts, ['#'], MIt1, 2)
    end)(),
    %% Interrupt iteration in the middle of the stream. Expect that
    %% iteration will continue where it left off:
    (fun() ->
        ItOpts = #{db => DB},
        MIt0 = emqx_ds:make_multi_iterator(ItOpts, ['#']),
        {[{_, 1, <<1>>}, {_, 2, <<2>>}, {_, 3, <<3>>}, {_, 1, <<1>>}], MIt1} =
            emqx_ds:multi_iterator_next(ItOpts, ['#'], MIt0, 4),
        {[{_, 2, <<2>>}, {_, 3, <<3>>}, {_, 1, <<1>>}, {_, 2, <<2>>}], MIt2} =
            emqx_ds:multi_iterator_next(ItOpts, ['#'], MIt1, 4),
        {[{_, 3, <<3>>}, {_, 1, <<1>>}, {_, 2, <<2>>}, {_, 3, <<3>>}], MIt3} =
            emqx_ds:multi_iterator_next(ItOpts, ['#'], MIt2, 4),
        {[{_, 1, <<1>>}, {_, 2, <<2>>}, {_, 3, <<3>>}], '$end_of_table'} =
            emqx_ds:multi_iterator_next(ItOpts, ['#'], MIt3, 4)
    end)().

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
    TCs = emqx_common_test_helpers:all(?MODULE),
    lists:map(
        fun(Backend) ->
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
        ok ->
            emqx_ds:wait_db(DB, all, infinity),
            ct:sleep(1000);
        Other ->
            Other
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
    {[{_, Stream}], []} = emqx_ds:get_streams(DB, [<<"t">>], 0, #{}),
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
            emqx_ds:list_slabs(DB)
        ),
        is_integer(Until)
    ],
    ?assertMatch(ok, emqx_ds:drop_slab(DB, GenToDel)).

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
