%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_storage_bitfield_lts_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(SHARD, shard(?FUNCTION_NAME)).

-define(DEFAULT_CONFIG, #{
    backend => builtin,
    storage => {emqx_ds_storage_bitfield_lts, #{}},
    n_shards => 1,
    replication_factor => 1
}).

-define(COMPACT_CONFIG, #{
    backend => builtin,
    storage =>
        {emqx_ds_storage_bitfield_lts, #{
            bits_per_wildcard_level => 8
        }},
    n_shards => 1,
    replication_factor => 1
}).

%% Smoke test of store function
t_store(_Config) ->
    MessageID = emqx_guid:gen(),
    PublishedAt = 1000,
    Topic = <<"foo/bar">>,
    Payload = <<"message">>,
    Msg = #message{
        id = MessageID,
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt
    },
    ?assertMatch(ok, emqx_ds_storage_layer:store_batch(?SHARD, [Msg], #{})).

%% Smoke test for iteration through a concrete topic
t_iterate(_Config) ->
    %% Prepare data:
    Topics = [<<"foo/bar">>, <<"foo/bar/baz">>, <<"a">>],
    Timestamps = lists:seq(1, 10),
    Batch = [
        make_message(PublishedAt, Topic, integer_to_binary(PublishedAt))
     || Topic <- Topics, PublishedAt <- Timestamps
    ],
    ok = emqx_ds_storage_layer:store_batch(?SHARD, Batch, []),
    %% Iterate through individual topics:
    [
        begin
            [{_Rank, Stream}] = emqx_ds_storage_layer:get_streams(?SHARD, parse_topic(Topic), 0),
            {ok, It} = emqx_ds_storage_layer:make_iterator(?SHARD, Stream, parse_topic(Topic), 0),
            {ok, NextIt, MessagesAndKeys} = emqx_ds_storage_layer:next(?SHARD, It, 100),
            Messages = [Msg || {_DSKey, Msg} <- MessagesAndKeys],
            ?assertEqual(
                lists:map(fun integer_to_binary/1, Timestamps),
                payloads(Messages)
            ),
            {ok, _, []} = emqx_ds_storage_layer:next(?SHARD, NextIt, 100)
        end
     || Topic <- Topics
    ],
    ok.

-define(assertSameSet(A, B), ?assertEqual(lists:sort(A), lists:sort(B))).

%% Smoke test that verifies that concrete topics are mapped to
%% individual streams, unless there's too many of them.
t_get_streams(_Config) ->
    %% Prepare data (without wildcards):
    Topics = [<<"foo/bar">>, <<"foo/bar/baz">>, <<"a">>],
    Timestamps = lists:seq(1, 10),
    Batch = [
        make_message(PublishedAt, Topic, integer_to_binary(PublishedAt))
     || Topic <- Topics, PublishedAt <- Timestamps
    ],
    ok = emqx_ds_storage_layer:store_batch(?SHARD, Batch, []),
    GetStream = fun(Topic) ->
        StartTime = 0,
        emqx_ds_storage_layer:get_streams(?SHARD, parse_topic(Topic), StartTime)
    end,
    %% Get streams for individual topics to use as a reference for later:
    [FooBar = {_, _}] = GetStream(<<"foo/bar">>),
    [FooBarBaz] = GetStream(<<"foo/bar/baz">>),
    [A] = GetStream(<<"a">>),
    %% Restart shard to make sure trie is persisted and restored:
    ok = emqx_ds_builtin_sup:stop_db(?FUNCTION_NAME),
    {ok, _} = emqx_ds_builtin_sup:start_db(?FUNCTION_NAME, #{}),
    %% Verify that there are no "ghost streams" for topics that don't
    %% have any messages:
    [] = GetStream(<<"bar/foo">>),
    %% Test some wildcard patterns:
    ?assertEqual([FooBar], GetStream("+/+")),
    ?assertSameSet([FooBar, FooBarBaz], GetStream(<<"foo/#">>)),
    ?assertSameSet([FooBar, FooBarBaz, A], GetStream(<<"#">>)),
    %% Now insert a bunch of messages with different topics to create wildcards:
    NewBatch = [
        begin
            B = integer_to_binary(I),
            make_message(100, <<"foo/bar/", B/binary>>, <<"filler", B/binary>>)
        end
     || I <- lists:seq(1, 200)
    ],
    ok = emqx_ds_storage_layer:store_batch(?SHARD, NewBatch, []),
    %% Check that "foo/bar/baz" topic now appears in two streams:
    %% "foo/bar/baz" and "foo/bar/+":
    NewStreams = lists:sort(GetStream("foo/bar/baz")),
    ?assertMatch([_, _], NewStreams),
    ?assert(lists:member(FooBarBaz, NewStreams)),
    %% Verify that size of the trie is still relatively small, even
    %% after processing 200+ topics:
    AllStreams = GetStream("#"),
    NTotal = length(AllStreams),
    ?assert(NTotal < 30, {NTotal, '<', 30}),
    ?assert(lists:member(FooBar, AllStreams)),
    ?assert(lists:member(FooBarBaz, AllStreams)),
    ?assert(lists:member(A, AllStreams)),
    ok.

t_new_generation_inherit_trie(_Config) ->
    %% This test checks that we inherit the previous generation's LTS when creating a new
    %% generation.
    ?check_trace(
        begin
            %% Create a bunch of topics to be learned in the first generation
            Timestamps = lists:seq(1, 10_000, 100),
            Batch = [
                begin
                    B = integer_to_binary(I),
                    make_message(
                        TS,
                        <<"wildcard/", B/binary, "/suffix/", Suffix/binary>>,
                        integer_to_binary(TS)
                    )
                end
             || I <- lists:seq(1, 200),
                TS <- Timestamps,
                Suffix <- [<<"foo">>, <<"bar">>]
            ],
            ok = emqx_ds_storage_layer:store_batch(?SHARD, Batch, []),
            %% Now we create a new generation with the same LTS module.  It should inherit the
            %% learned trie.
            ok = emqx_ds_storage_layer:add_generation(?SHARD),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(bitfield_lts_inherited_trie, Trace)),
            ok
        end
    ),
    ok.

t_replay(_Config) ->
    %% Create concrete topics:
    Topics = [<<"foo/bar">>, <<"foo/bar/baz">>],
    Timestamps = lists:seq(1, 10_000, 100),
    Batch1 = [
        make_message(PublishedAt, Topic, integer_to_binary(PublishedAt))
     || Topic <- Topics, PublishedAt <- Timestamps
    ],
    ok = emqx_ds_storage_layer:store_batch(?SHARD, Batch1, []),
    %% Create wildcard topics `wildcard/+/suffix/foo' and `wildcard/+/suffix/bar':
    Batch2 = [
        begin
            B = integer_to_binary(I),
            make_message(
                TS, <<"wildcard/", B/binary, "/suffix/", Suffix/binary>>, integer_to_binary(TS)
            )
        end
     || I <- lists:seq(1, 200), TS <- Timestamps, Suffix <- [<<"foo">>, <<"bar">>]
    ],
    ok = emqx_ds_storage_layer:store_batch(?SHARD, Batch2, []),
    %% Check various topic filters:
    Messages = Batch1 ++ Batch2,
    %% Missing topics (no ghost messages):
    ?assertNot(check(?SHARD, <<"missing/foo/bar">>, 0, Messages)),
    %% Regular topics:
    ?assert(check(?SHARD, <<"foo/bar">>, 0, Messages)),
    ?assert(check(?SHARD, <<"foo/bar/baz">>, 0, Messages)),
    ?assert(check(?SHARD, <<"foo/#">>, 0, Messages)),
    ?assert(check(?SHARD, <<"foo/+">>, 0, Messages)),
    ?assert(check(?SHARD, <<"foo/+/+">>, 0, Messages)),
    ?assert(check(?SHARD, <<"+/+/+">>, 0, Messages)),
    ?assert(check(?SHARD, <<"+/+/baz">>, 0, Messages)),
    %% Restart the DB to make sure trie is persisted and restored:
    ok = emqx_ds_builtin_sup:stop_db(?FUNCTION_NAME),
    {ok, _} = emqx_ds_builtin_sup:start_db(?FUNCTION_NAME, #{}),
    %% Learned wildcard topics:
    ?assertNot(check(?SHARD, <<"wildcard/1000/suffix/foo">>, 0, [])),
    ?assert(check(?SHARD, <<"wildcard/1/suffix/foo">>, 0, Messages)),
    ?assert(check(?SHARD, <<"wildcard/100/suffix/foo">>, 0, Messages)),
    ?assert(check(?SHARD, <<"wildcard/+/suffix/foo">>, 0, Messages)),
    ?assert(check(?SHARD, <<"wildcard/1/suffix/+">>, 0, Messages)),
    ?assert(check(?SHARD, <<"wildcard/100/suffix/+">>, 0, Messages)),
    ?assert(check(?SHARD, <<"wildcard/#">>, 0, Messages)),
    ?assert(check(?SHARD, <<"wildcard/1/#">>, 0, Messages)),
    ?assert(check(?SHARD, <<"wildcard/100/#">>, 0, Messages)),
    ?assert(check(?SHARD, <<"#">>, 0, Messages)),
    ok.

check(Shard, TopicFilter, StartTime, ExpectedMessages) ->
    ExpectedFiltered = lists:filter(
        fun(#message{topic = Topic, timestamp = TS}) ->
            emqx_topic:match(Topic, TopicFilter) andalso TS >= StartTime
        end,
        ExpectedMessages
    ),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            Dump = dump_messages(Shard, TopicFilter, StartTime),
            verify_dump(TopicFilter, StartTime, Dump),
            Missing = ExpectedFiltered -- Dump,
            Extras = Dump -- ExpectedFiltered,
            ?assertMatch(
                #{missing := [], unexpected := []},
                #{
                    missing => Missing,
                    unexpected => Extras,
                    topic_filter => TopicFilter,
                    start_time => StartTime
                }
            )
        end,
        []
    ),
    length(ExpectedFiltered) > 0.

verify_dump(TopicFilter, StartTime, Dump) ->
    lists:foldl(
        fun(#message{topic = Topic, timestamp = TS}, Acc) ->
            %% Verify that the topic of the message returned by the
            %% iterator matches the expected topic filter:
            ?assert(emqx_topic:match(Topic, TopicFilter), {unexpected_topic, Topic, TopicFilter}),
            %% Verify that timestamp of the message is greater than
            %% the StartTime of the iterator:
            ?assert(TS >= StartTime, {start_time, TopicFilter, TS, StartTime}),
            %% Verify that iterator didn't reorder messages
            %% (timestamps for each topic are growing):
            LastTopicTs = maps:get(Topic, Acc, -1),
            ?assert(TS >= LastTopicTs, {topic_ts_reordering, Topic, TS, LastTopicTs}),
            Acc#{Topic => TS}
        end,
        #{},
        Dump
    ).

dump_messages(Shard, TopicFilter, StartTime) ->
    Streams = emqx_ds_storage_layer:get_streams(Shard, parse_topic(TopicFilter), StartTime),
    lists:flatmap(
        fun({_Rank, Stream}) ->
            dump_stream(Shard, Stream, TopicFilter, StartTime)
        end,
        Streams
    ).

dump_stream(Shard, Stream, TopicFilter, StartTime) ->
    BatchSize = 100,
    {ok, Iterator} = emqx_ds_storage_layer:make_iterator(
        Shard, Stream, parse_topic(TopicFilter), StartTime
    ),
    Loop = fun
        F(It, 0) ->
            error({too_many_iterations, It});
        F(It, N) ->
            case emqx_ds_storage_layer:next(Shard, It, BatchSize) of
                end_of_stream ->
                    [];
                {ok, _NextIt, []} ->
                    [];
                {ok, NextIt, Batch} ->
                    [Msg || {_DSKey, Msg} <- Batch] ++ F(NextIt, N - 1)
            end
    end,
    MaxIterations = 1000000,
    Loop(Iterator, MaxIterations).

%% t_create_gen(_Config) ->
%%     {ok, 1} = emqx_ds_storage_layer:create_generation(?SHARD, 5, ?DEFAULT_CONFIG),
%%     ?assertEqual(
%%         {error, nonmonotonic},
%%         emqx_ds_storage_layer:create_generation(?SHARD, 1, ?DEFAULT_CONFIG)
%%     ),
%%     ?assertEqual(
%%         {error, nonmonotonic},
%%         emqx_ds_storage_layer:create_generation(?SHARD, 5, ?DEFAULT_CONFIG)
%%     ),
%%     {ok, 2} = emqx_ds_storage_layer:create_generation(?SHARD, 10, ?COMPACT_CONFIG),
%%     Topics = ["foo/bar", "foo/bar/baz"],
%%     Timestamps = lists:seq(1, 100),
%%     [
%%         ?assertMatch({ok, [_]}, store(?SHARD, PublishedAt, Topic, <<>>))
%%      || Topic <- Topics, PublishedAt <- Timestamps
%%     ].

%% t_iterate_multigen(_Config) ->
%%     {ok, 1} = emqx_ds_storage_layer:create_generation(?SHARD, 10, ?COMPACT_CONFIG),
%%     {ok, 2} = emqx_ds_storage_layer:create_generation(?SHARD, 50, ?DEFAULT_CONFIG),
%%     {ok, 3} = emqx_ds_storage_layer:create_generation(?SHARD, 1000, ?DEFAULT_CONFIG),
%%     Topics = ["foo/bar", "foo/bar/baz", "a", "a/bar"],
%%     Timestamps = lists:seq(1, 100),
%%     _ = [
%%         store(?SHARD, PublishedAt, Topic, term_to_binary({Topic, PublishedAt}))
%%      || Topic <- Topics, PublishedAt <- Timestamps
%%     ],
%%     ?assertEqual(
%%         lists:sort([
%%             {Topic, PublishedAt}
%%          || Topic <- ["foo/bar", "foo/bar/baz"], PublishedAt <- Timestamps
%%         ]),
%%         lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "foo/#", 0)])
%%     ),
%%     ?assertEqual(
%%         lists:sort([
%%             {Topic, PublishedAt}
%%          || Topic <- ["a", "a/bar"], PublishedAt <- lists:seq(60, 100)
%%         ]),
%%         lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "a/#", 60)])
%%     ).

%% t_iterate_multigen_preserve_restore(_Config) ->
%%     ReplayID = atom_to_binary(?FUNCTION_NAME),
%%     {ok, 1} = emqx_ds_storage_layer:create_generation(?SHARD, 10, ?COMPACT_CONFIG),
%%     {ok, 2} = emqx_ds_storage_layer:create_generation(?SHARD, 50, ?DEFAULT_CONFIG),
%%     {ok, 3} = emqx_ds_storage_layer:create_generation(?SHARD, 100, ?DEFAULT_CONFIG),
%%     Topics = ["foo/bar", "foo/bar/baz", "a/bar"],
%%     Timestamps = lists:seq(1, 100),
%%     TopicFilter = "foo/#",
%%     TopicsMatching = ["foo/bar", "foo/bar/baz"],
%%     _ = [
%%         store(?SHARD, TS, Topic, term_to_binary({Topic, TS}))
%%      || Topic <- Topics, TS <- Timestamps
%%     ],
%%     It0 = iterator(?SHARD, TopicFilter, 0),
%%     {It1, Res10} = iterate(It0, 10),
%%     % preserve mid-generation
%%     ok = emqx_ds_storage_layer:preserve_iterator(It1, ReplayID),
%%     {ok, It2} = emqx_ds_storage_layer:restore_iterator(?SHARD, ReplayID),
%%     {It3, Res100} = iterate(It2, 88),
%%     % preserve on the generation boundary
%%     ok = emqx_ds_storage_layer:preserve_iterator(It3, ReplayID),
%%     {ok, It4} = emqx_ds_storage_layer:restore_iterator(?SHARD, ReplayID),
%%     {It5, Res200} = iterate(It4, 1000),
%%     ?assertEqual({end_of_stream, []}, iterate(It5, 1)),
%%     ?assertEqual(
%%         lists:sort([{Topic, TS} || Topic <- TopicsMatching, TS <- Timestamps]),
%%         lists:sort([binary_to_term(Payload) || Payload <- Res10 ++ Res100 ++ Res200])
%%     ),
%%     ?assertEqual(
%%         ok,
%%         emqx_ds_storage_layer:discard_iterator(?SHARD, ReplayID)
%%     ),
%%     ?assertEqual(
%%         {error, not_found},
%%         emqx_ds_storage_layer:restore_iterator(?SHARD, ReplayID)
%%     ).

make_message(PublishedAt, Topic, Payload) when is_list(Topic) ->
    make_message(PublishedAt, list_to_binary(Topic), Payload);
make_message(PublishedAt, Topic, Payload) when is_binary(Topic) ->
    ID = emqx_guid:gen(),
    #message{
        id = ID,
        topic = Topic,
        timestamp = PublishedAt,
        payload = Payload
    }.

store(Shard, PublishedAt, TopicL, Payload) when is_list(TopicL) ->
    store(Shard, PublishedAt, list_to_binary(TopicL), Payload);
store(Shard, PublishedAt, Topic, Payload) ->
    ID = emqx_guid:gen(),
    Msg = #message{
        id = ID,
        topic = Topic,
        timestamp = PublishedAt,
        payload = Payload
    },
    emqx_ds_storage_layer:message_store(Shard, [Msg], #{}).

payloads(Messages) ->
    lists:map(
        fun(#message{payload = P}) ->
            P
        end,
        Messages
    ).

parse_topic(Topic = [L | _]) when is_binary(L); is_atom(L) ->
    Topic;
parse_topic(Topic) ->
    emqx_topic:words(iolist_to_binary(Topic)).

%% CT callbacks

all() -> emqx_common_test_helpers:all(?MODULE).
suite() -> [{timetrap, {seconds, 20}}].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_durable_storage],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TC, Config) ->
    ok = emqx_ds:open_db(TC, ?DEFAULT_CONFIG),
    Config.

end_per_testcase(TC, _Config) ->
    emqx_ds:drop_db(TC),
    ok.

shard(TC) ->
    {TC, <<"0">>}.

keyspace(TC) ->
    TC.

set_keyspace_config(Keyspace, Config) ->
    ok = application:set_env(emqx_ds, keyspace_config, #{Keyspace => Config}).
