%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_replay_local_store_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(SHARD, shard(?FUNCTION_NAME)).

-define(DEFAULT_CONFIG,
    {emqx_replay_message_storage, #{
        timestamp_bits => 64,
        topic_bits_per_level => [8, 8, 32, 16],
        epoch => 5,
        iteration => #{
            iterator_refresh => {every, 5}
        }
    }}
).

-define(COMPACT_CONFIG,
    {emqx_replay_message_storage, #{
        timestamp_bits => 16,
        topic_bits_per_level => [16, 16],
        epoch => 10
    }}
).

%% Smoke test for opening and reopening the database
t_open(_Config) ->
    ok = emqx_replay_local_store_sup:stop_shard(?SHARD),
    {ok, _} = emqx_replay_local_store_sup:start_shard(?SHARD).

%% Smoke test of store function
t_store(_Config) ->
    MessageID = emqx_guid:gen(),
    PublishedAt = 1000,
    Topic = [<<"foo">>, <<"bar">>],
    Payload = <<"message">>,
    ?assertMatch(ok, emqx_replay_local_store:store(?SHARD, MessageID, PublishedAt, Topic, Payload)).

%% Smoke test for iteration through a concrete topic
t_iterate(_Config) ->
    %% Prepare data:
    Topics = [[<<"foo">>, <<"bar">>], [<<"foo">>, <<"bar">>, <<"baz">>], [<<"a">>]],
    Timestamps = lists:seq(1, 10),
    [
        emqx_replay_local_store:store(
            ?SHARD,
            emqx_guid:gen(),
            PublishedAt,
            Topic,
            integer_to_binary(PublishedAt)
        )
     || Topic <- Topics, PublishedAt <- Timestamps
    ],
    %% Iterate through individual topics:
    [
        begin
            {ok, It} = emqx_replay_local_store:make_iterator(?SHARD, {Topic, 0}),
            Values = iterate(It),
            ?assertEqual(lists:map(fun integer_to_binary/1, Timestamps), Values)
        end
     || Topic <- Topics
    ],
    ok.

%% Smoke test for iteration with wildcard topic filter
t_iterate_wildcard(_Config) ->
    %% Prepare data:
    Topics = ["foo/bar", "foo/bar/baz", "a", "a/bar"],
    Timestamps = lists:seq(1, 10),
    _ = [
        store(?SHARD, PublishedAt, Topic, term_to_binary({Topic, PublishedAt}))
     || Topic <- Topics, PublishedAt <- Timestamps
    ],
    ?assertEqual(
        lists:sort([{Topic, PublishedAt} || Topic <- Topics, PublishedAt <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "#", 0)])
    ),
    ?assertEqual(
        [],
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "#", 10 + 1)])
    ),
    ?assertEqual(
        lists:sort([{Topic, PublishedAt} || Topic <- Topics, PublishedAt <- lists:seq(5, 10)]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "#", 5)])
    ),
    ?assertEqual(
        lists:sort([
            {Topic, PublishedAt}
         || Topic <- ["foo/bar", "foo/bar/baz"], PublishedAt <- Timestamps
        ]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "foo/#", 0)])
    ),
    ?assertEqual(
        lists:sort([{"foo/bar", PublishedAt} || PublishedAt <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "foo/+", 0)])
    ),
    ?assertEqual(
        [],
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "foo/+/bar", 0)])
    ),
    ?assertEqual(
        lists:sort([
            {Topic, PublishedAt}
         || Topic <- ["foo/bar", "foo/bar/baz", "a/bar"], PublishedAt <- Timestamps
        ]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "+/bar/#", 0)])
    ),
    ?assertEqual(
        lists:sort([{Topic, PublishedAt} || Topic <- ["a", "a/bar"], PublishedAt <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "a/#", 0)])
    ),
    ?assertEqual(
        [],
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "a/+/+", 0)])
    ),
    ok.

t_iterate_long_tail_wildcard(_Config) ->
    Topic = "b/c/d/e/f/g",
    TopicFilter = "b/c/d/e/+/+",
    Timestamps = lists:seq(1, 100),
    _ = [
        store(?SHARD, PublishedAt, Topic, term_to_binary({Topic, PublishedAt}))
     || PublishedAt <- Timestamps
    ],
    ?assertEqual(
        lists:sort([{"b/c/d/e/f/g", PublishedAt} || PublishedAt <- lists:seq(50, 100)]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, TopicFilter, 50)])
    ).

t_create_gen(_Config) ->
    {ok, 1} = emqx_replay_local_store:create_generation(?SHARD, 5, ?DEFAULT_CONFIG),
    ?assertEqual(
        {error, nonmonotonic},
        emqx_replay_local_store:create_generation(?SHARD, 1, ?DEFAULT_CONFIG)
    ),
    ?assertEqual(
        {error, nonmonotonic},
        emqx_replay_local_store:create_generation(?SHARD, 5, ?DEFAULT_CONFIG)
    ),
    {ok, 2} = emqx_replay_local_store:create_generation(?SHARD, 10, ?COMPACT_CONFIG),
    Topics = ["foo/bar", "foo/bar/baz"],
    Timestamps = lists:seq(1, 100),
    [
        ?assertEqual(ok, store(?SHARD, PublishedAt, Topic, <<>>))
     || Topic <- Topics, PublishedAt <- Timestamps
    ].

t_iterate_multigen(_Config) ->
    {ok, 1} = emqx_replay_local_store:create_generation(?SHARD, 10, ?COMPACT_CONFIG),
    {ok, 2} = emqx_replay_local_store:create_generation(?SHARD, 50, ?DEFAULT_CONFIG),
    {ok, 3} = emqx_replay_local_store:create_generation(?SHARD, 1000, ?DEFAULT_CONFIG),
    Topics = ["foo/bar", "foo/bar/baz", "a", "a/bar"],
    Timestamps = lists:seq(1, 100),
    _ = [
        store(?SHARD, PublishedAt, Topic, term_to_binary({Topic, PublishedAt}))
     || Topic <- Topics, PublishedAt <- Timestamps
    ],
    ?assertEqual(
        lists:sort([
            {Topic, PublishedAt}
         || Topic <- ["foo/bar", "foo/bar/baz"], PublishedAt <- Timestamps
        ]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "foo/#", 0)])
    ),
    ?assertEqual(
        lists:sort([
            {Topic, PublishedAt}
         || Topic <- ["a", "a/bar"], PublishedAt <- lists:seq(60, 100)
        ]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?SHARD, "a/#", 60)])
    ).

t_iterate_multigen_preserve_restore(_Config) ->
    ReplayID = atom_to_binary(?FUNCTION_NAME),
    {ok, 1} = emqx_replay_local_store:create_generation(?SHARD, 10, ?COMPACT_CONFIG),
    {ok, 2} = emqx_replay_local_store:create_generation(?SHARD, 50, ?DEFAULT_CONFIG),
    {ok, 3} = emqx_replay_local_store:create_generation(?SHARD, 100, ?DEFAULT_CONFIG),
    Topics = ["foo/bar", "foo/bar/baz", "a/bar"],
    Timestamps = lists:seq(1, 100),
    TopicFilter = "foo/#",
    TopicsMatching = ["foo/bar", "foo/bar/baz"],
    _ = [
        store(?SHARD, TS, Topic, term_to_binary({Topic, TS}))
     || Topic <- Topics, TS <- Timestamps
    ],
    It0 = iterator(?SHARD, TopicFilter, 0),
    {It1, Res10} = iterate(It0, 10),
    % preserve mid-generation
    ok = emqx_replay_local_store:preserve_iterator(It1, ReplayID),
    {ok, It2} = emqx_replay_local_store:restore_iterator(?SHARD, ReplayID),
    {It3, Res100} = iterate(It2, 88),
    % preserve on the generation boundary
    ok = emqx_replay_local_store:preserve_iterator(It3, ReplayID),
    {ok, It4} = emqx_replay_local_store:restore_iterator(?SHARD, ReplayID),
    {It5, Res200} = iterate(It4, 1000),
    ?assertEqual(none, It5),
    ?assertEqual(
        lists:sort([{Topic, TS} || Topic <- TopicsMatching, TS <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- Res10 ++ Res100 ++ Res200])
    ),
    ?assertEqual(
        ok,
        emqx_replay_local_store:discard_iterator(?SHARD, ReplayID)
    ),
    ?assertEqual(
        {error, not_found},
        emqx_replay_local_store:restore_iterator(?SHARD, ReplayID)
    ).

store(Shard, PublishedAt, Topic, Payload) ->
    ID = emqx_guid:gen(),
    emqx_replay_local_store:store(Shard, ID, PublishedAt, parse_topic(Topic), Payload).

iterate(DB, TopicFilter, StartTime) ->
    iterate(iterator(DB, TopicFilter, StartTime)).

iterate(It) ->
    case emqx_replay_local_store:next(It) of
        {value, Payload, ItNext} ->
            [Payload | iterate(ItNext)];
        none ->
            []
    end.

iterate(It, 0) ->
    {It, []};
iterate(It, N) ->
    case emqx_replay_local_store:next(It) of
        {value, Payload, ItNext} ->
            {ItFinal, Ps} = iterate(ItNext, N - 1),
            {ItFinal, [Payload | Ps]};
        none ->
            {none, []}
    end.

iterator(DB, TopicFilter, StartTime) ->
    {ok, It} = emqx_replay_local_store:make_iterator(DB, {parse_topic(TopicFilter), StartTime}),
    It.

parse_topic(Topic = [L | _]) when is_binary(L); is_atom(L) ->
    Topic;
parse_topic(Topic) ->
    emqx_topic:words(iolist_to_binary(Topic)).

%% CT callbacks

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(emqx_replay),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(emqx_replay).

init_per_testcase(TC, Config) ->
    ok = set_shard_config(shard(TC), ?DEFAULT_CONFIG),
    {ok, _} = emqx_replay_local_store_sup:start_shard(shard(TC)),
    Config.

end_per_testcase(TC, _Config) ->
    ok = emqx_replay_local_store_sup:stop_shard(shard(TC)).

shard(TC) ->
    list_to_binary(lists:concat([?MODULE, "_", TC])).

set_shard_config(Shard, Config) ->
    ok = application:set_env(emqx_replay, shard_config, #{Shard => Config}).
