%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_backends_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx/include/emqx.hrl").
-include("../../emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("../../emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

opts(Config) ->
    proplists:get_value(ds_conf, Config).

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
    emqx_ds_test_helpers:diff_messages(Msgs, Batch).

%% A simple smoke test that verifies that poll request is fulfilled
%% immediately when the new data is present at the time of poll
%% request.
t_03_smoke_poll_immediate(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            %% Store one message to create a stream:
            Msgs1 = [message(<<"foo/bar">>, <<"0">>, 0)],
            ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs1, #{sync => true})),
            timer:sleep(1_000),
            %% Create the iterator:
            StartTime = 0,
            TopicFilter = ['#'],
            [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
            {ok, Iter1, Batch1} = emqx_ds_test_helpers:consume_iter(DB, Iter0),
            emqx_ds_test_helpers:diff_messages(Msgs1, Batch1),
            %% Publish some messages:
            Msgs2 = [
                message(<<"foo/bar">>, <<"1">>, 0),
                message(<<"foo/bar">>, <<"2">>, 1),
                message(<<"foo/bar">>, <<"3">>, 2)
            ],
            ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs2, #{sync => true})),
            timer:sleep(1_000),
            %% Now poll the iterator:
            UserData = ?FUNCTION_NAME,
            {ok, Ref} = emqx_ds:poll(DB, [{UserData, Iter1}], #{timeout => 5_000}),
            receive
                #poll_reply{ref = Ref, userdata = UserData, payload = Payload} ->
                    {ok, Iter, Batch2} = Payload,
                    emqx_ds_test_helpers:diff_messages(Msgs2, Batch2),
                    %% Now verify that the received iterator is valid:
                    ?assertMatch({ok, _, []}, emqx_ds:next(DB, Iter, 10))
            after 1_000 ->
                error(no_poll_reply)
            end
        end,
        []
    ).

%% A simple test that verifies that poll request is fulfilled after
%% new data is added to the stream
t_04_smoke_poll_new_data(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            %% Store one message to create a stream:
            Msgs1 = [message(<<"foo/bar">>, <<"0">>, 0)],
            ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs1, #{sync => true})),
            timer:sleep(1_000),
            %% Create the iterator:
            StartTime = 0,
            TopicFilter = ['#'],
            [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
            {ok, Iter1, Batch1} = emqx_ds_test_helpers:consume_iter(DB, Iter0),
            emqx_ds_test_helpers:diff_messages(Msgs1, Batch1),
            %% Now poll the iterator:
            UserData = ?FUNCTION_NAME,
            {ok, Ref} = emqx_ds:poll(DB, [{UserData, Iter1}], #{timeout => 5_000}),
            %% Publish some messages:
            Msgs2 = [
                message(<<"foo/bar">>, <<"1">>, 0),
                message(<<"foo/bar">>, <<"2">>, 1),
                message(<<"foo/bar">>, <<"3">>, 2)
            ],
            ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs2, #{sync => true})),
            receive
                #poll_reply{ref = Ref, userdata = UserData, payload = Payload} ->
                    {ok, Iter, Batch2} = Payload,
                    emqx_ds_test_helpers:diff_messages(Msgs2, Batch2),
                    %% Now verify that the received iterator is valid:
                    ?assertMatch({ok, _, []}, emqx_ds:next(DB, Iter, 10))
            after 5_000 ->
                error(no_poll_reply)
            end
        end,
        []
    ).

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
    emqx_ds_test_helpers:diff_messages(Msgs, Batch).

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
            ?assertEqual(
                {error, unrecoverable, {precondition_failed, M1}},
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
            ?assertEqual(
                BatchMessages,
                DBMessages
            )
        end,
        []
    ).

t_smoke_delete_next(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
            StartTime = 0,
            TopicFilter = [<<"foo">>, '#'],
            Msgs =
                [Msg1, _Msg2, Msg3] = [
                    message(<<"foo/bar">>, <<"1">>, 0),
                    message(<<"foo">>, <<"2">>, 1),
                    message(<<"bar/bar">>, <<"3">>, 2)
                ],
            ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),

            [DStream] = emqx_ds:get_delete_streams(DB, TopicFilter, StartTime),
            {ok, DIter0} = emqx_ds:make_delete_iterator(DB, DStream, TopicFilter, StartTime),

            Selector = fun(#message{topic = Topic}) ->
                Topic == <<"foo">>
            end,
            {ok, DIter1, NumDeleted1} = delete(DB, DIter0, Selector, 1),
            ?assertEqual(0, NumDeleted1),
            {ok, DIter2, NumDeleted2} = delete(DB, DIter1, Selector, 1),
            ?assertEqual(1, NumDeleted2),

            TopicFilterHash = ['#'],
            [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilterHash, StartTime),
            Batch = emqx_ds_test_helpers:consume_stream(DB, Stream, TopicFilterHash, StartTime),
            ?assertEqual([Msg1, Msg3], Batch),

            ok = emqx_ds:add_generation(DB),

            ?assertMatch({ok, end_of_stream}, emqx_ds:delete_next(DB, DIter2, Selector, 1)),

            ok
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
    emqx_ds_test_helpers:diff_messages(Msgs1, Batch).

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
    emqx_ds_test_helpers:diff_messages([Msg0], [Msg || {_Key, Msg} <- Batch1]),

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

%% Validate that polling the iterator from the deleted generation is
%% handled gracefully:
t_poll_missing_generation(Config) ->
    %% Open the DB and push some messages to create a stream:
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    Msgs = [message(<<"foo/bar">>, <<"1">>, 0)],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
    timer:sleep(1_000),
    %% Create an iterator:
    TopicFilter = [<<"foo">>, '+'],
    [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, 0),
    {ok, It} = emqx_ds:make_iterator(DB, Stream, TopicFilter, 0),
    %% Rotate generations:
    [GenId0] = maps:keys(emqx_ds:list_generations_with_lifetimes(DB)),
    emqx_ds:add_generation(DB),
    emqx_ds:drop_generation(DB, GenId0),
    timer:sleep(1_000),
    [GenId1] = maps:keys(emqx_ds:list_generations_with_lifetimes(DB)),
    ?assertNotEqual(GenId0, GenId1),
    %% Poll iterator:
    Tag = ?FUNCTION_NAME,
    {ok, Ref} = emqx_ds:poll(DB, [{Tag, It}], #{timeout => 1_000}),
    ?assertReceive(#poll_reply{ref = Ref, userdata = Tag, payload = {error, unrecoverable, _}}).

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

update_data_set() ->
    [
        [
            {<<"foo/bar">>, <<"1">>}
        ],

        [
            {<<"foo">>, <<"2">>}
        ],

        [
            {<<"bar/bar">>, <<"3">>}
        ]
    ].

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
            delete(DB, It, BatchSize, Selector, Acc + NumDeleted);
        {ok, end_of_stream} ->
            {ok, end_of_stream, Acc};
        Ret ->
            Ret
    end.

%% CT callbacks

all() ->
    [{group, Backend} || Backend <- backends()].

exclude(emqx_ds_fdb_backend) ->
    [
        %% Atomic operations and preconditions are not supported:
        t_09_atomic_store_batch,
        t_11_batch_preconditions,
        t_12_batch_precondition_conflicts,
        %% Deletions are not supported at this moment:
        t_smoke_delete_next,
        %% FIXME: this test shouldn't pass for ANY backend (as
        %% `update_config' call must not create a new generation by
        %% itself), investigate why it does for builtins:
        t_07_smoke_update_config
    ];
exclude(_) ->
    [].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [{Backend, TCs -- exclude(Backend)} || Backend <- backends()].

init_per_group(emqx_fdb_ds, _Config) ->
    {skip, fixme};
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
    Apps = emqx_cth_suite:start(Backend:test_applications(Config), #{
        work_dir => emqx_cth_suite:work_dir(TC, Config)
    }),
    ct:pal("Started apps: ~p", [Apps]),
    timer:sleep(1000),
    Config ++ [{apps, Apps}].

end_per_testcase(TC, Config) ->
    catch ok = emqx_ds:drop_db(TC),
    catch ok = emqx_cth_suite:stop(?config(apps, Config)),
    catch mnesia:delete_schema([node()]),
    snabbkaffe:stop(),
    ok.

emqx_ds_open_db(X1, X2) ->
    case emqx_ds:open_db(X1, X2) of
        ok -> timer:sleep(1000);
        Other -> Other
    end.

backends() ->
    application:load(emqx_ds_backends),
    {ok, L} = application:get_env(emqx_ds_backends, available_backends),
    L.
