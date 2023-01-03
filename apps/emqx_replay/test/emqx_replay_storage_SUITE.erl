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
-module(emqx_replay_storage_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").

-define(ZONE, zone(?FUNCTION_NAME)).

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
            {ok, It} = emqx_replay_local_store:make_iterator(?ZONE, Topic, 0),
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

store(Zone, PublishedAt, Topic, Payload) ->
    ID = emqx_guid:gen(),
    emqx_replay_local_store:store(Zone, ID, PublishedAt, parse_topic(Topic), Payload).

iterate(DB, TopicFilter, StartTime) ->
    {ok, It} = emqx_replay_local_store:make_iterator(DB, parse_topic(TopicFilter), StartTime),
    iterate(It).

iterate(It) ->
    case emqx_replay_local_store:next(It) of
        {value, Payload, ItNext} ->
            [Payload | iterate(ItNext)];
        none ->
            []
    end.

parse_topic(Topic = [L | _]) when is_binary(L); is_atom(L) ->
    Topic;
parse_topic(Topic) ->
    emqx_topic:words(iolist_to_binary(Topic)).

%%

t_prop_topic_hash_computes(_) ->
    Keymapper = emqx_replay_message_storage:make_keymapper(#{
        timestamp_bits => 32,
        topic_bits_per_level => [8, 12, 16, 24],
        epoch => 10000
    }),
    ?assert(
        proper:quickcheck(
            ?FORALL({Topic, Timestamp}, {topic(), integer()}, begin
                BS = emqx_replay_message_storage:compute_bitstring(Topic, Timestamp, Keymapper),
                is_integer(BS) andalso (BS < (1 bsl 92))
            end)
        )
    ).

t_prop_topic_bitmask_computes(_) ->
    Keymapper = emqx_replay_message_storage:make_keymapper(#{
        timestamp_bits => 16,
        topic_bits_per_level => [8, 12, 16],
        epoch => 100
    }),
    ?assert(
        proper:quickcheck(
            ?FORALL(TopicFilter, topic_filter(), begin
                Mask = emqx_replay_message_storage:compute_topic_bitmask(TopicFilter, Keymapper),
                is_integer(Mask) andalso (Mask < (1 bsl (36 + 6)))
            end)
        )
    ).

t_prop_iterate_stored_messages(_) ->
    ?assertEqual(
        true,
        proper:quickcheck(
            ?FORALL(
                Streams,
                messages(),
                begin
                    Stream = payload_gen:interleave_streams(Streams),
                    ok = store_message_stream(?ZONE, Stream),
                    % TODO actually verify some property
                    true
                end
            )
        )
    ).

store_message_stream(Zone, [{Topic, {Payload, ChunkNum, _ChunkCount}} | Rest]) ->
    MessageID = <<ChunkNum:32>>,
    PublishedAt = rand:uniform(ChunkNum),
    ok = emqx_replay_local_store:store(Zone, MessageID, PublishedAt, Topic, Payload),
    store_message_stream(Zone, payload_gen:next(Rest));
store_message_stream(_Zone, []) ->
    ok.

messages() ->
    ?LET(Topics, list(topic()), begin
        [{Topic, payload_gen:binary_stream_gen(64)} || Topic <- Topics]
    end).

topic() ->
    % TODO
    % Somehow generate topic levels with variance according to the entropy distribution?
    non_empty(list(topic_level())).

topic(EntropyWeights) ->
    ?LET(
        L,
        list(1),
        ?SIZED(S, [topic_level(S * EW) || EW <- lists:sublist(EntropyWeights ++ L, length(L))])
    ).

topic_filter() ->
    ?SUCHTHAT(
        L,
        non_empty(
            list(
                frequency([
                    {5, topic_level()},
                    {2, '+'},
                    {1, '#'}
                ])
            )
        ),
        not lists:member('#', L) orelse lists:last(L) == '#'
    ).

% topic() ->
%     ?LAZY(?SIZED(S, frequency([
%         {S, [topic_level() | topic()]},
%         {1, []}
%     ]))).

% topic_filter() ->
%     ?LAZY(?SIZED(S, frequency([
%         {round(S / 3 * 2), [topic_level() | topic_filter()]},
%         {round(S / 3 * 1), ['+' | topic_filter()]},
%         {1, []},
%         {1, ['#']}
%     ]))).

topic_level() ->
    ?LET(L, list(oneof([range($a, $z), range($0, $9)])), iolist_to_binary(L)).

topic_level(Entropy) ->
    S = floor(1 + math:log2(Entropy) / 4),
    ?LET(I, range(1, Entropy), iolist_to_binary(io_lib:format("~*.16.0B", [S, I]))).

%% CT callbacks

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TC, Config) ->
    {ok, _} = application:ensure_all_started(emqx_replay),
    {ok, _} = emqx_replay_local_store_sup:start_zone(zone(TC)),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = application:stop(emqx_replay).

zone(TC) ->
    list_to_atom(?MODULE_STRING ++ atom_to_list(TC)).
