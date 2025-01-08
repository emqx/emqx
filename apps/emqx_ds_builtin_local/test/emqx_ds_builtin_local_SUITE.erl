%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_builtin_local_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx_utils/include/emqx_message.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("../../emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(N_SHARDS, 1).

opts(_Config) ->
    #{
        backend => builtin_local,
        storage => {emqx_ds_storage_reference, #{}},
        n_shards => ?N_SHARDS
    }.

t_drop_generation_with_never_used_iterator(Config) ->
    %% This test checks how the iterator behaves when:
    %%   1) it's created at generation 1 and not consumed from.
    %%   2) generation 2 is created and 1 dropped.
    %%   3) iteration begins.
    %% In this case, the iterator won't see any messages and the stream will end.

    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, opts(Config))),
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
    ?assertMatch(ok, emqx_ds:open_db(DB, opts(Config))),
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

t_make_iterator_stale_stream(Config) ->
    %% This checks the behavior of `emqx_ds:make_iterator' after the generation underlying
    %% the stream has been dropped.

    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, opts(Config))),
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
            ?assertMatch(ok, emqx_ds:open_db(DB, opts(Config))),

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

%% This testcase verifies the behavior of `store_batch' operation
%% when the underlying code experiences recoverable or unrecoverable
%% problems.
t_store_batch_fail(Config) ->
    ?check_trace(
        #{timetrap => 15_000},
        try
            meck:new(emqx_ds_storage_layer, [passthrough, no_history]),
            DB = ?FUNCTION_NAME,
            ?assertMatch(ok, emqx_ds:open_db(DB, (opts(Config))#{n_shards => 2})),
            %% Success:
            Batch1 = [
                message(<<"C1">>, <<"foo/bar">>, <<"1">>, 1),
                message(<<"C1">>, <<"foo/bar">>, <<"2">>, 1)
            ],
            ?assertMatch(ok, emqx_ds:store_batch(DB, Batch1, #{sync => true})),
            %% Inject unrecoverable error:
            meck:expect(emqx_ds_storage_layer, store_batch, fun(_DB, _Shard, _Messages, _DispatchF) ->
                {error, unrecoverable, mock}
            end),
            Batch2 = [
                message(<<"C1">>, <<"foo/bar">>, <<"3">>, 1),
                message(<<"C1">>, <<"foo/bar">>, <<"4">>, 1)
            ],
            ?assertMatch(
                {error, unrecoverable, mock}, emqx_ds:store_batch(DB, Batch2, #{sync => true})
            ),
            %% Inject a recoveralbe error:
            meck:expect(emqx_ds_storage_layer, store_batch, fun(_DB, _Shard, _Messages, _DispatchF) ->
                {error, recoverable, mock}
            end),
            Batch3 = [
                message(<<"C1">>, <<"foo/bar">>, <<"5">>, 2),
                message(<<"C2">>, <<"foo/bar">>, <<"6">>, 2),
                message(<<"C1">>, <<"foo/bar">>, <<"7">>, 3),
                message(<<"C2">>, <<"foo/bar">>, <<"8">>, 3)
            ],
            %% Note: due to idempotency issues the number of retries
            %% is currently set to 0:
            ?assertMatch(
                {error, recoverable, mock},
                emqx_ds:store_batch(DB, Batch3, #{sync => true})
            ),
            meck:unload(emqx_ds_storage_layer),
            ?assertMatch(ok, emqx_ds:store_batch(DB, Batch3, #{sync => true})),
            lists:sort(emqx_ds_test_helpers:consume_per_stream(DB, ['#'], 1))
        after
            meck:unload()
        end,
        [
            {"message ordering", fun(StoredMessages, _Trace) ->
                [{_, MessagesFromStream1}, {_, MessagesFromStream2}] = StoredMessages,
                emqx_ds_test_helpers:diff_messages(
                    [payload],
                    [
                        #message{payload = <<"1">>},
                        #message{payload = <<"2">>},
                        #message{payload = <<"5">>},
                        #message{payload = <<"7">>}
                    ],
                    MessagesFromStream1
                ),
                emqx_ds_test_helpers:diff_messages(
                    [payload],
                    [
                        #message{payload = <<"6">>},
                        #message{payload = <<"8">>}
                    ],
                    MessagesFromStream2
                )
            end}
        ]
    ).

message(ClientId, Topic, Payload, PublishedAt) ->
    Msg = message(Topic, Payload, PublishedAt),
    Msg#message{from = ClientId}.

message(Topic, Payload, PublishedAt) ->
    #message{
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

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
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_screen(),
    Apps = emqx_cth_suite:start(
        [mria, emqx_ds_builtin_local],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_TC, Config) ->
    application:ensure_all_started(emqx_ds_builtin_local),
    Config.

end_per_testcase(_TC, _Config) ->
    snabbkaffe:stop(),
    ok = application:stop(emqx_ds_builtin_local),
    mria:stop(),
    _ = mnesia:delete_schema([node()]),
    ok.
