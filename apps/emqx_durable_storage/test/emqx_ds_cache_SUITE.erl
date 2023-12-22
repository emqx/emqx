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
-module(emqx_ds_cache_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/include/emqx.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_durable_storage],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

default_db_opts() ->
    #{
        backend => builtin,
        storage => {emqx_ds_storage_reference, #{}},
        n_shards => 1,
        replication_factor => 1
    }.

open_db(DB) ->
    open_db(DB, _Overrides = #{}).

open_db(DB, Overrides) ->
    ok = emqx_ds:open_db(DB, default_db_opts()),
    Opts0 = #{
        db => DB,
        topic_filters => [],
        batch_size => 1
    },
    Opts = maps:merge(Opts0, Overrides),
    {ok, Pid} = emqx_ds_cache:start_link(Opts),
    on_exit(fun() -> emqx_ds_cache:stop(Pid) end),
    on_exit(fun() -> emqx_ds:drop_db(DB) end),
    ok.

message(Topic, Payload, PublishedAt) ->
    #message{
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

now_ms() ->
    erlang:system_time(millisecond).

iterate(DB, It, BatchSize, Opts) ->
    iterate(DB, It, BatchSize, Opts, []).

iterate(DB, It0, BatchSize, Opts, Acc) ->
    case emqx_ds:next(DB, It0, BatchSize, Opts) of
        {ok, It, []} ->
            {ok, It, Acc};
        {ok, It, Msgs} ->
            iterate(DB, It, BatchSize, Opts, Acc ++ Msgs);
        Ret ->
            Ret
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_empty_cache(_Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            ok = open_db(DB),
            NowMS = now_ms(),
            Msgs = [
                message(<<"some/topic">>, <<"1">>, NowMS + 60_000),
                message(<<"some/topic">>, <<"2">>, NowMS + 60_001)
            ],
            ok = emqx_ds:store_batch(DB, Msgs),
            TopicFilterBin = <<"some/+">>,
            TopicFilter = emqx_topic:words(TopicFilterBin),
            [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, NowMS),
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, NowMS),
            {ok, _Iter1, Batch0} = iterate(DB, Iter0, 1, #{use_cache => true}),
            Batch = [Msg || {_DSKey, Msg} <- Batch0],
            ?assertEqual(Msgs, Batch),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_, _, _ | _], ?of_kind(ds_cache_miss, Trace)),
            ok
        end
    ),
    ok.

t_full_cache(_Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 10_000},
        begin
            TopicFilterBin = <<"some/+">>,
            TopicFilter = emqx_topic:words(TopicFilterBin),
            ok = open_db(DB, #{batch_size => 5, topic_filters => [TopicFilter]}),
            NowMS = now_ms(),
            Msgs = [
                message(<<"some/topic">>, <<"1">>, NowMS + 60_000),
                message(<<"some/topic">>, <<"2">>, NowMS + 60_001),
                message(<<"some/topic">>, <<"3">>, NowMS + 60_002)
            ],
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:store_batch(DB, Msgs),
                    #{?snk_kind := ds_cache_stored_batch},
                    2_000
                ),
            [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, NowMS),
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, NowMS),
            {ok, Iter1, Batch1} = iterate(DB, Iter0, 1, #{use_cache => true}),
            %% The next fetches should use the cache.
            {ok, _Iter2, Batch2} = iterate(DB, Iter1, 2, #{use_cache => true}),
            Fetched = [Msg || {_DSKey, Msg} <- Batch1 ++ Batch2],
            ?assertEqual(Msgs, Fetched),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [_, _, _],
                lists:flatten(?projection(batch, ?of_kind(ds_cache_stored_batch, Trace)))
            ),
            %% First fetch is a miss because the initial iterator doesn't have a last
            %% seen key
            ?assertMatch([#{last_seen_key := undefined}], ?of_kind(ds_cache_miss, Trace)),
            ?assertMatch([_, _], ?of_kind(ds_cache_hit, Trace)),
            ok
        end
    ),
    ok.

t_key_deleted_while_iterating(_Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := key_deleted},
                #{?snk_kind := ds_cache_lookup_enter}
            ),

            spawn(fun() ->
                {ok, #{key := Key}} =
                    ?block_until(#{?snk_kind := ds_cache_will_fetch}),
                ?assertNotEqual('$end_of_table', Key),
                emqx_ds_cache:delete(DB, Key),
                ?tp(key_deleted, #{}),
                ok
            end),

            TopicFilterBin = <<"some/+">>,
            TopicFilter = emqx_topic:words(TopicFilterBin),
            ok = open_db(DB, #{batch_size => 5, topic_filters => [TopicFilter]}),
            NowMS = now_ms(),
            Msgs =
                [Msg1 | _] = [
                    message(<<"some/topic">>, <<"1">>, NowMS + 60_000),
                    message(<<"some/topic">>, <<"2">>, NowMS + 60_001),
                    message(<<"some/topic">>, <<"2">>, NowMS + 60_002)
                ],
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:store_batch(DB, Msgs),
                    #{?snk_kind := ds_cache_stored_batch},
                    2_000
                ),
            [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, NowMS),
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, NowMS),
            {ok, Iter1, Batch1} = emqx_ds:next(DB, Iter0, 1, #{use_cache => false}),
            {ok, _Iter2, Batch2} = emqx_ds:next(DB, Iter1, 2, #{use_cache => true}),
            %% We don't report a cache miss in this case, as this case is equivalent to
            %% when the client is up to date with the stream and it's the cache that needs
            %% to fetch more.
            %% TODO: what to do if the client started from an old key and it got GC'ed?
            Fetched = [Msg || {_DSKey, Msg} <- Batch1 ++ Batch2],
            ?assertEqual([Msg1], Fetched),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(ds_cache_empty_result, Trace)),
            ?assertEqual([], ?of_kind(ds_cache_hit, Trace)),
            ok
        end
    ),
    ok.

t_last_seen_key_not_contained(_Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            TopicFilterBin = <<"some/+">>,
            TopicFilter = emqx_topic:words(TopicFilterBin),
            ok = open_db(DB, #{batch_size => 5, topic_filters => [TopicFilter]}),
            NowMS = now_ms(),
            Msgs = [
                message(<<"some/topic">>, <<"1">>, NowMS + 60_000),
                message(<<"some/topic">>, <<"2">>, NowMS + 60_001),
                message(<<"some/topic">>, <<"2">>, NowMS + 60_002)
            ],
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:store_batch(DB, Msgs),
                    #{?snk_kind := ds_cache_stored_batch},
                    2_000
                ),
            [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, NowMS),
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, NowMS),
            {ok, Iter1, [{DSKey1, _Msg1}] = Batch1} = emqx_ds:next(DB, Iter0, 1, #{
                use_cache => false
            }),
            emqx_ds_cache:delete(DB, Stream, DSKey1),
            {ok, _Iter2, Batch2} = emqx_ds:next(DB, Iter1, 2, #{use_cache => true}),
            Fetched = [Msg || {_DSKey, Msg} <- Batch1 ++ Batch2],
            ?assertEqual(Msgs, Fetched),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(ds_cache_miss, Trace)),
            ?assertEqual([], ?of_kind(ds_cache_hit, Trace)),
            ok
        end
    ),
    ok.

t_only_last_seen_key_contained(_Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            TopicFilterBin = <<"some/+">>,
            TopicFilter = emqx_topic:words(TopicFilterBin),
            ok = open_db(DB, #{batch_size => 5, topic_filters => [TopicFilter]}),
            NowMS = now_ms(),
            Msgs = [
                message(<<"some/topic">>, <<"1">>, NowMS + 60_000),
                message(<<"some/topic">>, <<"2">>, NowMS + 60_001),
                message(<<"some/topic">>, <<"2">>, NowMS + 60_002)
            ],
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:store_batch(DB, Msgs),
                    #{?snk_kind := ds_cache_stored_batch},
                    2_000
                ),
            [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, NowMS),
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, NowMS),
            {ok, Iter1, _Batch1} = emqx_ds:next(DB, Iter0, 3, #{use_cache => false}),
            {ok, _Iter2, []} = emqx_ds:next(DB, Iter1, 3, #{use_cache => true}),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(ds_cache_will_fetch, Trace)),
            ?assertMatch([_], ?of_kind(ds_cache_empty_result, Trace)),
            ok
        end
    ),
    ok.

t_gc(_Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            TopicFilterBin = <<"some/+">>,
            TopicFilter = emqx_topic:words(TopicFilterBin),
            GCInterval = 1_000,
            ok = open_db(DB, #{
                batch_size => 5,
                topic_filters => [TopicFilter],
                gc_interval => GCInterval
            }),
            NowMS = now_ms(),
            Msgs = [
                message(<<"some/topic">>, <<"1">>, NowMS + 60_000),
                message(<<"some/topic">>, <<"2">>, NowMS + 60_001),
                message(<<"some/topic">>, <<"2">>, NowMS + 60_002)
            ],
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:store_batch(DB, Msgs),
                    #{?snk_kind := ds_cache_stored_batch},
                    2_000
                ),
            [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, NowMS),
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, NowMS),
            {ok, Iter1, Batch1} = emqx_ds:next(DB, Iter0, 1, #{use_cache => false}),
            {ok, _} = ?block_until(
                #{?snk_kind := ds_cache_gc_ran, num_deleted := N} when N > 0,
                GCInterval * 3
            ),
            {ok, _Iter2, Batch2} = emqx_ds:next(DB, Iter1, 3, #{use_cache => true}),
            Fetched = [Msg || {_DSKey, Msg} <- Batch1 ++ Batch2],
            ?assertEqual(Msgs, Fetched),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind(ds_cache_will_fetch, Trace)),
            ?assertMatch([], ?of_kind(ds_cache_empty_result, Trace)),
            ?assertMatch([_], ?of_kind(ds_cache_miss, Trace)),
            ok
        end
    ),
    ok.

t_end_of_stream(_Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            TopicFilterBin = <<"some/+">>,
            TopicFilter = emqx_topic:words(TopicFilterBin),
            ok = open_db(DB, #{batch_size => 5, topic_filters => [TopicFilter]}),
            NowMS = now_ms(),
            Msgs = [
                message(<<"some/topic">>, <<"1">>, NowMS + 60_000),
                message(<<"some/topic">>, <<"2">>, NowMS + 60_001),
                message(<<"some/topic">>, <<"2">>, NowMS + 60_002)
            ],
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:store_batch(DB, Msgs),
                    #{?snk_kind := ds_cache_stored_batch},
                    2_000
                ),
            [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, NowMS),
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, NowMS),
            {ok, Iter1, _Batch1} = iterate(DB, Iter0, 3, #{use_cache => false}),
            %% todo: currently, there's no way (?) to cause a new generation to be created
            %% so that a new stream is created.
            emqx_common_test_helpers:with_mock(
                emqx_ds,
                next,
                fun
                    (_DB, _Iter, _BatchSize, #{use_cache := false}) ->
                        %% cache trying to pull new messages
                        {ok, end_of_stream};
                    (DBIn, Iter, BatchSize, #{use_cache := true} = Opts) ->
                        meck:passthrough([DBIn, Iter, BatchSize, Opts])
                end,
                fun() ->
                    {ok, _} = ?block_until(#{?snk_kind := ds_cache_eos_inserted}, 1_000),
                    ?assertMatch(
                        {ok, end_of_stream},
                        emqx_ds:next(DB, Iter1, 3, #{use_cache => true})
                    ),
                    ok
                end
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(ds_cache_eos_found, Trace)),
            ok
        end
    ),
    ok.
