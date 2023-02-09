%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_replay_local_store_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ZONE, zone(?FUNCTION_NAME)).

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
    ok = emqx_replay_local_store_sup:stop_zone(?ZONE),
    {ok, _} = emqx_replay_local_store_sup:start_zone(?ZONE).

%% Smoke test of store function
t_store(_Config) ->
    MessageID = emqx_guid:gen(),
    PublishedAt = 1000,
    Topic = [<<"foo">>, <<"bar">>],
    Payload = <<"message">>,
    ?assertMatch(ok, emqx_replay_local_store:store(?ZONE, MessageID, PublishedAt, Topic, Payload)).

%% Smoke test for iteration through a concrete topic
t_iterate(_Config) ->
    %% Prepare data:
    Topics = [[<<"foo">>, <<"bar">>], [<<"foo">>, <<"bar">>, <<"baz">>], [<<"a">>]],
    Timestamps = lists:seq(1, 10),
    [
        emqx_replay_local_store:store(
            ?ZONE,
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
            {ok, It} = emqx_replay_local_store:make_iterator(?ZONE, {Topic, 0}),
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
        store(?ZONE, PublishedAt, Topic, term_to_binary({Topic, PublishedAt}))
     || Topic <- Topics, PublishedAt <- Timestamps
    ],
    ?assertEqual(
        lists:sort([{Topic, PublishedAt} || Topic <- Topics, PublishedAt <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "#", 0)])
    ),
    ?assertEqual(
        [],
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "#", 10 + 1)])
    ),
    ?assertEqual(
        lists:sort([{Topic, PublishedAt} || Topic <- Topics, PublishedAt <- lists:seq(5, 10)]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "#", 5)])
    ),
    ?assertEqual(
        lists:sort([
            {Topic, PublishedAt}
         || Topic <- ["foo/bar", "foo/bar/baz"], PublishedAt <- Timestamps
        ]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "foo/#", 0)])
    ),
    ?assertEqual(
        lists:sort([{"foo/bar", PublishedAt} || PublishedAt <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "foo/+", 0)])
    ),
    ?assertEqual(
        [],
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "foo/+/bar", 0)])
    ),
    ?assertEqual(
        lists:sort([
            {Topic, PublishedAt}
         || Topic <- ["foo/bar", "foo/bar/baz", "a/bar"], PublishedAt <- Timestamps
        ]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "+/bar/#", 0)])
    ),
    ?assertEqual(
        lists:sort([{Topic, PublishedAt} || Topic <- ["a", "a/bar"], PublishedAt <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "a/#", 0)])
    ),
    ?assertEqual(
        [],
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "a/+/+", 0)])
    ),
    ok.

t_iterate_long_tail_wildcard(_Config) ->
    Topic = "b/c/d/e/f/g",
    TopicFilter = "b/c/d/e/+/+",
    Timestamps = lists:seq(1, 100),
    _ = [
        store(?ZONE, PublishedAt, Topic, term_to_binary({Topic, PublishedAt}))
     || PublishedAt <- Timestamps
    ],
    ?assertEqual(
        lists:sort([{"b/c/d/e/f/g", PublishedAt} || PublishedAt <- lists:seq(50, 100)]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, TopicFilter, 50)])
    ).

t_create_gen(_Config) ->
    {ok, 1} = emqx_replay_local_store:create_generation(?ZONE, 5, ?DEFAULT_CONFIG),
    ?assertEqual(
        {error, nonmonotonic},
        emqx_replay_local_store:create_generation(?ZONE, 1, ?DEFAULT_CONFIG)
    ),
    ?assertEqual(
        {error, nonmonotonic},
        emqx_replay_local_store:create_generation(?ZONE, 5, ?DEFAULT_CONFIG)
    ),
    {ok, 2} = emqx_replay_local_store:create_generation(?ZONE, 10, ?COMPACT_CONFIG),
    Topics = ["foo/bar", "foo/bar/baz"],
    Timestamps = lists:seq(1, 100),
    [
        ?assertEqual(ok, store(?ZONE, PublishedAt, Topic, <<>>))
     || Topic <- Topics, PublishedAt <- Timestamps
    ].

t_iterate_multigen(_Config) ->
    {ok, 1} = emqx_replay_local_store:create_generation(?ZONE, 10, ?COMPACT_CONFIG),
    {ok, 2} = emqx_replay_local_store:create_generation(?ZONE, 50, ?DEFAULT_CONFIG),
    {ok, 3} = emqx_replay_local_store:create_generation(?ZONE, 1000, ?DEFAULT_CONFIG),
    Topics = ["foo/bar", "foo/bar/baz", "a", "a/bar"],
    Timestamps = lists:seq(1, 100),
    _ = [
        store(?ZONE, PublishedAt, Topic, term_to_binary({Topic, PublishedAt}))
     || Topic <- Topics, PublishedAt <- Timestamps
    ],
    ?assertEqual(
        lists:sort([
            {Topic, PublishedAt}
         || Topic <- ["foo/bar", "foo/bar/baz"], PublishedAt <- Timestamps
        ]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "foo/#", 0)])
    ),
    ?assertEqual(
        lists:sort([
            {Topic, PublishedAt}
         || Topic <- ["a", "a/bar"], PublishedAt <- lists:seq(60, 100)
        ]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(?ZONE, "a/#", 60)])
    ).

t_iterate_multigen_preserve_restore(_Config) ->
    ReplayID = atom_to_binary(?FUNCTION_NAME),
    {ok, 1} = emqx_replay_local_store:create_generation(?ZONE, 10, ?COMPACT_CONFIG),
    {ok, 2} = emqx_replay_local_store:create_generation(?ZONE, 50, ?DEFAULT_CONFIG),
    {ok, 3} = emqx_replay_local_store:create_generation(?ZONE, 100, ?DEFAULT_CONFIG),
    Topics = ["foo/bar", "foo/bar/baz", "a/bar"],
    Timestamps = lists:seq(1, 100),
    TopicFilter = "foo/#",
    TopicsMatching = ["foo/bar", "foo/bar/baz"],
    _ = [
        store(?ZONE, TS, Topic, term_to_binary({Topic, TS}))
     || Topic <- Topics, TS <- Timestamps
    ],
    It0 = iterator(?ZONE, TopicFilter, 0),
    {It1, Res10} = iterate(It0, 10),
    % preserve mid-generation
    ok = emqx_replay_local_store:preserve_iterator(It1, ReplayID),
    {ok, It2} = emqx_replay_local_store:restore_iterator(?ZONE, ReplayID),
    {It3, Res100} = iterate(It2, 88),
    % preserve on the generation boundary
    ok = emqx_replay_local_store:preserve_iterator(It3, ReplayID),
    {ok, It4} = emqx_replay_local_store:restore_iterator(?ZONE, ReplayID),
    {It5, Res200} = iterate(It4, 1000),
    ?assertEqual(none, It5),
    ?assertEqual(
        lists:sort([{Topic, TS} || Topic <- TopicsMatching, TS <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- Res10 ++ Res100 ++ Res200])
    ),
    ?assertEqual(
        ok,
        emqx_replay_local_store:discard_iterator(?ZONE, ReplayID)
    ),
    ?assertEqual(
        {error, not_found},
        emqx_replay_local_store:restore_iterator(?ZONE, ReplayID)
    ).

store(Zone, PublishedAt, Topic, Payload) ->
    ID = emqx_guid:gen(),
    emqx_replay_local_store:store(Zone, ID, PublishedAt, parse_topic(Topic), Payload).

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
    ok = set_zone_config(zone(TC), ?DEFAULT_CONFIG),
    {ok, _} = emqx_replay_local_store_sup:start_zone(zone(TC)),
    Config.

end_per_testcase(TC, _Config) ->
    ok = emqx_replay_local_store_sup:stop_zone(zone(TC)).

zone(TC) ->
    list_to_atom(lists:concat([?MODULE, "_", TC])).

set_zone_config(Zone, Config) ->
    ok = application:set_env(emqx_replay, zone_config, #{Zone => Config}).
