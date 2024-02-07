%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx_durable_storage, #{override_env => [{cache_enabled, true}]}}],
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
%% Helper functions
%%------------------------------------------------------------------------------

default_db_opts() ->
    #{
        backend => builtin,
        storage => {emqx_ds_storage_bitfield_lts, #{}},
        n_shards => 1,
        replication_factor => 1
    }.

default_reference_db_opts() ->
    #{
        backend => builtin,
        storage => {emqx_ds_storage_reference, #{}},
        n_shards => 1,
        replication_factor => 1
    }.

open_db(DB) ->
    open_db(DB, _Overrides = #{}).

open_db(DB, Overrides) ->
    Opts = maps:merge(default_db_opts(), Overrides),
    ok = emqx_ds:open_db(DB, Opts),
    on_exit(fun() -> emqx_ds:drop_db(DB) end),
    ok.

make_message(PublishedAt, Topic, Payload) ->
    ID = emqx_guid:gen(),
    #message{
        id = ID,
        topic = Topic,
        timestamp = PublishedAt,
        payload = Payload
    }.

now_ms() ->
    erlang:system_time(millisecond).

store_and_cache(DB, Messages) ->
    LastMsg = lists:last(Messages),
    {ok, {ok, _}} =
        snabbkaffe:wait_async_action(
            fun() -> emqx_ds:store_batch(DB, Messages) end,
            fun
                (#{?snk_kind := ds_cache_stored_batch, batch := Batch}) ->
                    lists:member(LastMsg, [Msg || {_DSKey, Msg} <- Batch]);
                (_) ->
                    false
            end,
            5_000
        ),
    ok.

iterate_1_by_1(DB, Iter0) ->
    iterate_1_by_1(DB, Iter0, _Acc = []).

iterate_1_by_1(DB, Iter0, Acc) ->
    case emqx_ds:next(DB, Iter0, 1) of
        {ok, Iter1, []} ->
            {ok, Iter1, Acc};
        {ok, Iter1, Batch} ->
            iterate_1_by_1(DB, Iter1, Acc ++ Batch)
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_new_streams(_Config) ->
    %% Checks that the coordinator periodically refreshes the streams for tracked topic
    %% streams and starts new worker processes for each.
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 10_000},
        begin
            StartTime = 0,
            TopicFilter = [<<"t">>, '#'],

            ok = open_db(DB, #{cache_prefetch_topic_filters => [TopicFilter]}),

            ?assertEqual([], emqx_ds:get_streams(DB, TopicFilter, StartTime)),
            ?assertEqual([], emqx_ds_builtin_db_sup:get_cache_workers(DB)),

            %% Store some messages to create streams.
            Msg1 = make_message(now_ms(), <<"t/1">>, <<"1">>),
            ok = emqx_ds:store_batch(DB, [Msg1]),
            ok = emqx_ds_cache_coordinator:renew_streams(DB),

            ?assertMatch([_], emqx_ds:get_streams(DB, TopicFilter, StartTime)),
            ?assertMatch([_], emqx_ds_builtin_db_sup:get_cache_workers(DB)),

            ok
        end,
        []
    ),
    ok.

t_deleted_streams(_Config) ->
    %% Checks that the coordinator periodically refreshes the streams for tracked topic
    %% streams and stops worker processes when streams disappear.
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 10_000},
        begin
            StartTime = 0,
            TopicFilter = [<<"t">>, '#'],

            ok = open_db(DB, #{cache_prefetch_topic_filters => [TopicFilter]}),

            %% Store some messages to create streams.
            Msg1 = make_message(now_ms(), <<"t/1">>, <<"1">>),
            ok = emqx_ds:store_batch(DB, [Msg1]),
            ok = emqx_ds_cache_coordinator:renew_streams(DB),

            ?assertMatch([_], emqx_ds:get_streams(DB, TopicFilter, StartTime)),
            ?assertMatch([_], emqx_ds_builtin_db_sup:get_cache_workers(DB)),

            %% Create a new generation and drop the old one.
            [GenId0] = maps:keys(emqx_ds:list_generations_with_lifetimes(DB)),
            ok = emqx_ds:add_generation(DB),
            ok = emqx_ds:drop_generation(DB, GenId0),
            ok = emqx_ds_cache_coordinator:renew_streams(DB),

            ?assertMatch([], emqx_ds:get_streams(DB, TopicFilter, StartTime)),
            ?assertMatch([], emqx_ds_builtin_db_sup:get_cache_workers(DB)),

            ok
        end,
        []
    ),
    ok.

t_cache_hit(_Config) ->
    %% Checks the basic happy path for a cache hit: if a client asks for a batch that
    %% _may_ be contained in the cache, the cache serves it.
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 10_000},
        begin
            StartTime = 0,
            TopicFilter = [<<"t">>, '#'],

            ok = open_db(DB, #{cache_prefetch_topic_filters => [TopicFilter]}),

            %% Store a message to create a stream.  This message won't make it to the
            %% cache.
            %% Using the same topic to reuitilize the now tracked stream.
            Topic = <<"t/a">>,
            Msg0 = make_message(_PublishedAt = 0, Topic, <<"0">>),
            ok = emqx_ds:store_batch(DB, [Msg0], #{sync => true}),
            ok = emqx_ds_cache_coordinator:renew_streams(DB),

            %% Now we publish some messages to be cached.
            NowMS0 = now_ms(),
            Msg1 = make_message(NowMS0 + 100, Topic, <<"1">>),
            Msg2 = make_message(NowMS0 + 101, Topic, <<"2">>),
            ok = store_and_cache(DB, [Msg1, Msg2]),

            [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, NowMS0),
            {ok, Iter0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),

            %% The first message was published "before" the cache started to track the
            %% stream.
            ?tp(notice, "fetching: should miss cache", #{}),
            {Res1, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:next(DB, Iter0, 1, #{use_cache => true}),
                    #{?snk_kind := ds_cache_miss},
                    2_000
                ),
            ?assertMatch({ok, _Iter1, [{_DSKey0, Msg0}]}, Res1),
            {ok, Iter1, _Batch1} = Res1,

            ?tp(notice, "fetching: should miss cache (first cached key)", #{}),
            {Res2, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:next(DB, Iter1, 1, #{use_cache => true}),
                    #{?snk_kind := ds_cache_miss},
                    2_000
                ),
            ?assertMatch({ok, _Iter2, [{_DSKey1, Msg1}]}, Res2),
            {ok, Iter2, _Batch2} = Res2,

            ?tp(notice, "fetching: should hit cache (contains last seen key)", #{}),
            {Res3, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:next(DB, Iter2, 1, #{use_cache => true}),
                    #{?snk_kind := ds_cache_hit},
                    2_000
                ),
            ?assertMatch({ok, _Iter3, [{_DSKey2, Msg2}]}, Res3),
            {ok, Iter3, _Batch3} = Res3,

            ?tp(notice, "fetching: should \"hit\" and return empty", #{}),
            {Res4, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:next(DB, Iter3, 1, #{use_cache => true}),
                    #{?snk_kind := ds_cache_empty_result},
                    2_000
                ),
            ?assertMatch({ok, _Iter4, []}, Res4),
            {ok, Iter4, []} = Res4,

            Msg3 = make_message(NowMS0 + 102, Topic, <<"3">>),
            ok = store_and_cache(DB, [Msg3]),

            ?tp(notice, "fetching: should hit again", #{}),
            {Res5, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:next(DB, Iter4, 1, #{use_cache => true}),
                    #{?snk_kind := ds_cache_hit},
                    2_000
                ),
            ?assertMatch({ok, _Iter5, [{_DSKey3, Msg3}]}, Res5),

            ok
        end,
        []
    ),
    ok.

t_end_of_stream(_Config) ->
    %% Checks that the cache returns `end_of_stream' after it's occurrence is cached.
    DB = ?FUNCTION_NAME,
    ?check_trace(
        begin
            StartTime = 0,
            TopicFilter = [<<"t">>, '#'],

            ok = open_db(DB, #{cache_prefetch_topic_filters => [TopicFilter]}),

            %% Store a message to create a stream.  This message won't make it to the
            %% cache.
            %% Using the same topic to reuitilize the now tracked stream.
            Topic = <<"t/a">>,
            Msg0 = make_message(_PublishedAt = 0, Topic, <<"0">>),
            ok = emqx_ds:store_batch(DB, [Msg0], #{sync => true}),
            ok = emqx_ds_cache_coordinator:renew_streams(DB),

            [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
            {ok, Iter1} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
            {ok, Iter2, [_]} = emqx_ds:next(DB, Iter1, 1),

            %% Now we start a new generation, so that the existing stream reaches an end.
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ds:add_generation(DB),
                    #{?snk_kind := ds_cache_end_of_stream},
                    2_000
                ),

            ?assertEqual({ok, end_of_stream}, emqx_ds:next(DB, Iter2, 1)),

            ok
        end,
        []
    ),
    ok.

t_multiple_topics_same_stream(_Config) ->
    %% Checks the basic happy path for a cache hit: if a client asks for a batch that
    %% _may_ be contained in the cache, the cache serves it.
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 10_000},
        begin
            TopicFilter = [<<"t">>, '#'],

            Opts = maps:merge(
                default_reference_db_opts(),
                #{cache_prefetch_topic_filters => [TopicFilter]}
            ),
            ok = open_db(DB, Opts),
            ok = emqx_ds_cache_coordinator:renew_streams(DB),

            %% Now we publish some messages to be cached.
            NowMS0 = now_ms(),
            Msgs = [
                make_message(NowMS0 + I, Topic, integer_to_binary(I))
             || Topic <- [<<"t/a">>, <<"t/b">>, <<"t/c">>],
                I <- lists:seq(1, 4)
            ],
            ok = store_and_cache(DB, Msgs),

            TopicFilter1 = [<<"t">>, <<"a">>],
            TopicFilter2 = [<<"t">>, <<"c">>],
            [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter1, NowMS0),
            %% Assert same stream for all filters for this particular storage
            [{_Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter2, NowMS0),
            {ok, Iter1} = emqx_ds:make_iterator(DB, Stream, TopicFilter1, NowMS0),
            {ok, Iter2} = emqx_ds:make_iterator(DB, Stream, TopicFilter2, NowMS0),

            TopicFilter1Msgs = [Msg || Msg = #message{topic = <<"t/a">>} <- Msgs],
            {ok, _Iter12, Batch1} = iterate_1_by_1(DB, Iter1),
            ?assertEqual(TopicFilter1Msgs, [Msg || {_DSKey, Msg} <- Batch1]),

            TopicFilter2Msgs = [Msg || Msg = #message{topic = <<"t/c">>} <- Msgs],
            {ok, _Iter22, Batch2} = iterate_1_by_1(DB, Iter2),
            ?assertEqual(TopicFilter2Msgs, [Msg || {_DSKey, Msg} <- Batch2]),

            ok
        end,
        []
    ),
    ok.
