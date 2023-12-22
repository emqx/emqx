%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(N_SHARDS, 1).

opts() ->
    #{
        backend => builtin,
        storage => {emqx_ds_storage_reference, #{}},
        n_shards => ?N_SHARDS,
        replication_factor => 3
    }.

%% A simple smoke test that verifies that opening/closing the DB
%% doesn't crash, and not much else
t_00_smoke_open_drop(_Config) ->
    DB = 'DB',
    ?assertMatch(ok, emqx_ds:open_db(DB, opts())),
    %% Check metadata:
    %%    We have only one site:
    [Site] = emqx_ds_replication_layer_meta:sites(),
    %%    Check all shards:
    Shards = emqx_ds_replication_layer_meta:shards(DB),
    %%    Since there is only one site all shards should be allocated
    %%    to this site:
    MyShards = emqx_ds_replication_layer_meta:my_shards(DB),
    ?assertEqual(?N_SHARDS, length(Shards)),
    lists:foreach(
        fun(Shard) ->
            ?assertEqual(
                {ok, []}, emqx_ds_replication_layer_meta:replica_set(DB, Shard)
            ),
            ?assertEqual(
                [Site], emqx_ds_replication_layer_meta:in_sync_replicas(DB, Shard)
            ),
            %%  Check that the leader is eleected;
            ?assertEqual({ok, node()}, emqx_ds_replication_layer_meta:shard_leader(DB, Shard))
        end,
        Shards
    ),
    ?assertEqual(lists:sort(Shards), lists:sort(MyShards)),
    %% Reopen the DB and make sure the operation is idempotent:
    ?assertMatch(ok, emqx_ds:open_db(DB, opts())),
    %% Close the DB:
    ?assertMatch(ok, emqx_ds:drop_db(DB)).

%% A simple smoke test that verifies that storing the messages doesn't
%% crash
t_01_smoke_store(_Config) ->
    DB = default,
    ?assertMatch(ok, emqx_ds:open_db(DB, opts())),
    Msg = message(<<"foo/bar">>, <<"foo">>, 0),
    ?assertMatch(ok, emqx_ds:store_batch(DB, [Msg])).

%% A simple smoke test that verifies that getting the list of streams
%% doesn't crash and that iterators can be opened.
t_02_smoke_get_streams_start_iter(_Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, opts())),
    StartTime = 0,
    TopicFilter = ['#'],
    [{Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    ?assertMatch({_, _}, Rank),
    ?assertMatch({ok, _Iter}, emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime)).

%% A simple smoke test that verifies that it's possible to iterate
%% over messages.
t_03_smoke_iterate(_Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, opts())),
    StartTime = 0,
    TopicFilter = ['#'],
    Msgs = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo">>, <<"2">>, 1),
        message(<<"bar/bar">>, <<"3">>, 2)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
    [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
    {ok, Iter, Batch} = iterate(DB, Iter0, 1),
    ?assertEqual(Msgs, [Msg || {_Key, Msg} <- Batch], {Iter0, Iter}).

%% Verify that iterators survive restart of the application. This is
%% an important property, since the lifetime of the iterators is tied
%% to the external resources, such as clients' sessions, and they
%% should always be able to continue replaying the topics from where
%% they are left off.
t_04_restart(_Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, opts())),
    TopicFilter = ['#'],
    StartTime = 0,
    Msgs = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo">>, <<"2">>, 1),
        message(<<"bar/bar">>, <<"3">>, 2)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
    [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
    %% Restart the application:
    ?tp(warning, emqx_ds_SUITE_restart_app, #{}),
    ok = application:stop(emqx_durable_storage),
    {ok, _} = application:ensure_all_started(emqx_durable_storage),
    ok = emqx_ds:open_db(DB, opts()),
    %% The old iterator should be still operational:
    {ok, Iter, Batch} = iterate(DB, Iter0, 1),
    ?assertEqual(Msgs, [Msg || {_Key, Msg} <- Batch], {Iter0, Iter}).

%% Check that we can create iterators directly from DS keys.
t_05_update_iterator(_Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, opts())),
    TopicFilter = ['#'],
    StartTime = 0,
    Msgs = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo">>, <<"2">>, 1),
        message(<<"bar/bar">>, <<"3">>, 2)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
    [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
    Res0 = emqx_ds:next(DB, Iter0, 1),
    ?assertMatch({ok, _OldIter, [{_Key0, _Msg0}]}, Res0),
    {ok, OldIter, [{Key0, Msg0}]} = Res0,
    Res1 = emqx_ds:update_iterator(DB, OldIter, Key0),
    ?assertMatch({ok, _Iter1}, Res1),
    {ok, Iter1} = Res1,
    {ok, FinalIter, Batch} = iterate(DB, Iter1, 1),
    AllMsgs = [Msg0 | [Msg || {_Key, Msg} <- Batch]],
    ?assertEqual(Msgs, AllMsgs, #{from_key => Iter1, final_iter => FinalIter}),
    ok.

t_05_update_config(_Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, opts())),
    TopicFilter = ['#'],

    DataSet = update_data_set(),

    ToMsgs = fun(Datas) ->
        lists:map(
            fun({Topic, Payload}) ->
                message(Topic, Payload, emqx_message:timestamp_now())
            end,
            Datas
        )
    end,

    {_, StartTimes, MsgsList} =
        lists:foldl(
            fun
                (Datas, {true, TimeAcc, MsgAcc}) ->
                    Msgs = ToMsgs(Datas),
                    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
                    {false, TimeAcc, [Msgs | MsgAcc]};
                (Datas, {Any, TimeAcc, MsgAcc}) ->
                    timer:sleep(500),
                    ?assertMatch(ok, emqx_ds:add_generation(DB, opts())),
                    timer:sleep(500),
                    StartTime = emqx_message:timestamp_now(),
                    Msgs = ToMsgs(Datas),
                    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
                    {Any, [StartTime | TimeAcc], [Msgs | MsgAcc]}
            end,
            {true, [emqx_message:timestamp_now()], []},
            DataSet
        ),

    Checker = fun({StartTime, Msgs0}, Acc) ->
        Msgs = Msgs0 ++ Acc,
        Batch = fetch_all(DB, TopicFilter, StartTime),
        ?assertEqual(Msgs, Batch, {StartTime}),
        Msgs
    end,
    lists:foldl(Checker, [], lists:zip(StartTimes, MsgsList)).

t_06_add_generation(_Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, opts())),
    TopicFilter = ['#'],

    DataSet = update_data_set(),

    ToMsgs = fun(Datas) ->
        lists:map(
            fun({Topic, Payload}) ->
                message(Topic, Payload, emqx_message:timestamp_now())
            end,
            Datas
        )
    end,

    {_, StartTimes, MsgsList} =
        lists:foldl(
            fun
                (Datas, {true, TimeAcc, MsgAcc}) ->
                    Msgs = ToMsgs(Datas),
                    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
                    {false, TimeAcc, [Msgs | MsgAcc]};
                (Datas, {Any, TimeAcc, MsgAcc}) ->
                    timer:sleep(500),
                    ?assertMatch(ok, emqx_ds:add_generation(DB)),
                    timer:sleep(500),
                    StartTime = emqx_message:timestamp_now(),
                    Msgs = ToMsgs(Datas),
                    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
                    {Any, [StartTime | TimeAcc], [Msgs | MsgAcc]}
            end,
            {true, [emqx_message:timestamp_now()], []},
            DataSet
        ),

    Checker = fun({StartTime, Msgs0}, Acc) ->
        Msgs = Msgs0 ++ Acc,
        Batch = fetch_all(DB, TopicFilter, StartTime),
        ?assertEqual(Msgs, Batch, {StartTime}),
        Msgs
    end,
    lists:foldl(Checker, [], lists:zip(StartTimes, MsgsList)).

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

fetch_all(DB, TopicFilter, StartTime) ->
    Streams0 = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    Streams = lists:sort(
        fun({{_, A}, _}, {{_, B}, _}) ->
            A < B
        end,
        Streams0
    ),
    lists:foldl(
        fun({_, Stream}, Acc) ->
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
            {ok, _, Msgs0} = iterate(DB, Iter0, StartTime),
            Msgs = lists:map(fun({_, Msg}) -> Msg end, Msgs0),
            Acc ++ Msgs
        end,
        [],
        Streams
    ).

message(Topic, Payload, PublishedAt) ->
    #message{
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

iterate(DB, It, BatchSize) ->
    iterate(DB, It, BatchSize, []).

iterate(DB, It0, BatchSize, Acc) ->
    case emqx_ds:next(DB, It0, BatchSize) of
        {ok, It, []} ->
            {ok, It, Acc};
        {ok, It, Msgs} ->
            iterate(DB, It, BatchSize, Acc ++ Msgs);
        {ok, end_of_stream} ->
            {ok, It0, Acc};
        Ret ->
            Ret
    end.

%% CT callbacks

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [mria, emqx_durable_storage],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_TC, Config) ->
    application:ensure_all_started(emqx_durable_storage),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = application:stop(emqx_durable_storage),
    mria:stop(),
    _ = mnesia:delete_schema([node()]),
    ok.
