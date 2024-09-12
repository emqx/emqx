%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(N_SHARDS, 1).

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
%% doesn't crash and that iterators can be opened.
t_02_smoke_get_streams_start_iter(Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    StartTime = 0,
    TopicFilter = ['#'],
    [{Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    ?assertMatch({_, _}, Rank),
    ?assertMatch({ok, _Iter}, emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime)).

%% A simple smoke test that verifies that it's possible to iterate
%% over messages.
t_03_smoke_iterate(Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    StartTime = 0,
    TopicFilter = ['#'],
    Msgs = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo">>, <<"2">>, 1),
        message(<<"bar/bar">>, <<"3">>, 2)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs, #{sync => true})),
    [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
    {ok, Iter, Batch} = emqx_ds_test_helpers:consume_iter(DB, Iter0),
    ?assertEqual(Msgs, Batch, {Iter0, Iter}).

%% Verify that iterators survive restart of the application. This is
%% an important property, since the lifetime of the iterators is tied
%% to the external resources, such as clients' sessions, and they
%% should always be able to continue replaying the topics from where
%% they are left off.
t_04_restart(Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
    TopicFilter = ['#'],
    StartTime = 0,
    Msgs = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo">>, <<"2">>, 1),
        message(<<"bar/bar">>, <<"3">>, 2)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs, #{sync => true})),
    [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
    %% Restart the application:
    ?tp(warning, emqx_ds_SUITE_restart_app, #{}),
    ok = application:stop(emqx_durable_storage),
    {ok, _} = application:ensure_all_started(emqx_durable_storage),
    ok = emqx_ds_open_db(DB, opts(Config)),
    %% The old iterator should be still operational:
    {ok, Iter, Batch} = emqx_ds_test_helpers:consume_iter(DB, Iter0),
    ?assertEqual(Msgs, Batch, {Iter0, Iter}).

%% Check that we can create iterators directly from DS keys.
%% t_05_update_iterator(Config) ->
%%     DB = ?FUNCTION_NAME,
%%     ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
%%     TopicFilter = ['#'],
%%     StartTime = 0,
%%     Msgs = [
%%         message(<<"foo/bar">>, <<"1">>, 0),
%%         message(<<"foo">>, <<"2">>, 1),
%%         message(<<"bar/bar">>, <<"3">>, 2)
%%     ],
%%     ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
%%     [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
%%     {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
%%     Res0 = emqx_ds:next(DB, Iter0, 1),
%%     ?assertMatch({ok, _OldIter, [{_Key0, _Msg0}]}, Res0),
%%     {ok, OldIter, [{Key0, Msg0}]} = Res0,
%%     Res1 = emqx_ds:update_iterator(DB, OldIter, Key0),
%%     ?assertMatch({ok, _Iter1}, Res1),
%%     {ok, Iter1} = Res1,
%%     {ok, Iter, Batch} = emqx_ds_test_helpers:consume_iter(DB, Iter1, #{batch_size => 1}),
%%     ?assertEqual(Msgs, [Msg0 | Batch], #{from_key => Iter1, final_iter => Iter}),
%%     ok.

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
            ),

            ok
        end,
        []
    ),
    ok.

t_09_atomic_store_batch(Config) ->
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
                force_monotonic_timestamps => false
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
                force_monotonic_timestamps => false
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
        message(<<"foo/baz">>, <<"2">>, 1)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs0)),

    [{_, Stream0}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream0, TopicFilter, StartTime),

    ok = emqx_ds:add_generation(DB),
    ok = emqx_ds:drop_generation(DB, GenId0),

    Now = emqx_message:timestamp_now(),
    Msgs1 = [
        message(<<"foo/bar">>, <<"3">>, Now + 100),
        message(<<"foo/baz">>, <<"4">>, Now + 101)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs1)),

    ?assertError(
        {error, unrecoverable, generation_not_found},
        emqx_ds_test_helpers:consume_iter(DB, Iter0)
    ),

    %% New iterator for the new stream will only see the later messages.
    [{_, Stream1}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    ?assertNotEqual(Stream0, Stream1),
    {ok, Iter1} = emqx_ds:make_iterator(DB, Stream1, TopicFilter, StartTime),

    {ok, Iter, Batch} = emqx_ds_test_helpers:consume_iter(DB, Iter1, #{batch_size => 1}),
    ?assertNotEqual(end_of_stream, Iter),
    ?assertEqual(Msgs1, Batch),

    ok.

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
            message(<<"foo/baz">>, <<"2">>, 1)
        ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs0)),

    [{_, Stream0}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream0, TopicFilter, StartTime),
    {ok, Iter1, Batch1} = emqx_ds:next(DB, Iter0, 1),
    ?assertNotEqual(end_of_stream, Iter1),
    ?assertEqual([Msg0], [Msg || {_Key, Msg} <- Batch1]),

    ok = emqx_ds:add_generation(DB),
    ok = emqx_ds:drop_generation(DB, GenId0),

    Now = emqx_message:timestamp_now(),
    Msgs1 = [
        message(<<"foo/bar">>, <<"3">>, Now + 100),
        message(<<"foo/baz">>, <<"4">>, Now + 101)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs1)),

    ?assertError(
        {error, unrecoverable, generation_not_found},
        emqx_ds_test_helpers:consume_iter(DB, Iter1)
    ).

%% t_drop_generation_update_iterator(Config) ->
%%     %% This checks the behavior of `emqx_ds:update_iterator' after the generation
%%     %% underlying the iterator has been dropped.

%%     DB = ?FUNCTION_NAME,
%%     ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),
%%     [GenId0] = maps:keys(emqx_ds:list_generations_with_lifetimes(DB)),

%%     TopicFilter = emqx_topic:words(<<"foo/+">>),
%%     StartTime = 0,
%%     Msgs0 = [
%%         message(<<"foo/bar">>, <<"1">>, 0),
%%         message(<<"foo/baz">>, <<"2">>, 1)
%%     ],
%%     ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs0)),

%%     [{_, Stream0}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
%%     {ok, Iter0} = emqx_ds:make_iterator(DB, Stream0, TopicFilter, StartTime),
%%     {ok, Iter1, _Batch1} = emqx_ds:next(DB, Iter0, 1),
%%     {ok, _Iter2, [{Key2, _Msg}]} = emqx_ds:next(DB, Iter1, 1),

%%     ok = emqx_ds:add_generation(DB),
%%     ok = emqx_ds:drop_generation(DB, GenId0),

%%     ?assertEqual(
%%         {error, unrecoverable, generation_not_found},
%%         emqx_ds:update_iterator(DB, Iter1, Key2)
%%     ).

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
        message(<<"foo/baz">>, <<"2">>, 1)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs0)),

    [{_, Stream0}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),

    ok = emqx_ds:add_generation(DB),
    ok = emqx_ds:drop_generation(DB, GenId0),

    ?assertEqual(
        {error, unrecoverable, generation_not_found},
        emqx_ds:make_iterator(DB, Stream0, TopicFilter, StartTime)
    ),

    ok.

t_get_streams_concurrently_with_drop_generation(Config) ->
    %% This checks that we can get all streams while a generation is dropped
    %% mid-iteration.

    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 5_000},
        begin
            ?assertMatch(ok, emqx_ds_open_db(DB, opts(Config))),

            [GenId0] = maps:keys(emqx_ds:list_generations_with_lifetimes(DB)),
            ok = emqx_ds:add_generation(DB),
            ok = emqx_ds:add_generation(DB),

            %% All streams
            TopicFilter = emqx_topic:words(<<"foo/+">>),
            StartTime = 0,
            ?assertMatch([_, _, _], emqx_ds:get_streams(DB, TopicFilter, StartTime)),

            ?force_ordering(
                #{?snk_kind := dropped_gen},
                #{?snk_kind := get_streams_get_gen}
            ),

            spawn_link(fun() ->
                {ok, _} = ?block_until(#{?snk_kind := get_streams_all_gens}),
                ok = emqx_ds:drop_generation(DB, GenId0),
                ?tp(dropped_gen, #{})
            end),

            ?assertMatch([_, _], emqx_ds:get_streams(DB, TopicFilter, StartTime)),

            ok
        end,
        []
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
    [{group, builtin_local}, {group, builtin_raft}].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {builtin_local, TCs},
        {builtin_raft, TCs}
    ].

init_per_group(builtin_local, Config) ->
    Conf = #{
        backend => builtin_local,
        storage => {emqx_ds_storage_reference, #{}},
        n_shards => ?N_SHARDS
    },
    [{ds_conf, Conf} | Config];
init_per_group(builtin_raft, Config) ->
    case emqx_ds_test_helpers:skip_if_norepl() of
        false ->
            Conf = #{
                backend => builtin_raft,
                storage => {emqx_ds_storage_reference, #{}},
                n_shards => ?N_SHARDS,
                n_sites => 1,
                replication_factor => 3,
                replication_options => #{}
            },
            [{ds_conf, Conf} | Config];
        Yes ->
            Yes
    end.

end_per_group(_Group, Config) ->
    Config.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

suite() ->
    [{timetrap, 50_000}].

init_per_testcase(TC, Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_durable_storage, emqx_ds_backends],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    ct:pal("Apps: ~p", [Apps]),
    [{apps, Apps} | Config].

end_per_testcase(TC, Config) ->
    ok = emqx_ds:drop_db(TC),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    _ = mnesia:delete_schema([node()]),
    snabbkaffe:stop(),
    ok.

emqx_ds_open_db(X1, X2) ->
    case emqx_ds:open_db(X1, X2) of
        ok -> timer:sleep(1000);
        Other -> Other
    end.
