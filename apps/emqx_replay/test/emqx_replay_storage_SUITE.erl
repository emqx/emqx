%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Smoke test of store function
t_store(Config) ->
    DB = ?config(handle, Config),
    MessageID = emqx_guid:gen(),
    PublishedAt = 1000,
    Topic = [<<"foo">>, <<"bar">>],
    Payload = <<"message">>,
    ?assertMatch(ok, emqx_replay_message_storage:store(DB, MessageID, PublishedAt, Topic, Payload)).

%% Smoke test for iteration through a concrete topic
t_iterate(Config) ->
    DB = ?config(handle, Config),
    %% Prepare data:
    Topics = [[<<"foo">>, <<"bar">>], [<<"foo">>, <<"bar">>, <<"baz">>], [<<"a">>]],
    Timestamps = lists:seq(1, 10),
    [
        emqx_replay_message_storage:store(
            DB,
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
            {ok, It} = emqx_replay_message_storage:make_iterator(DB, Topic, 0),
            Values = iterate(It),
            ?assertEqual(Values, lists:map(fun integer_to_binary/1, Timestamps))
        end
     || Topic <- Topics
    ],
    ok.

%% Smoke test for iteration with wildcard topic filter
t_iterate_wildcard(Config) ->
    DB = ?config(handle, Config),
    %% Prepare data:
    Topics = ["foo/bar", "foo/bar/baz", "a", "a/bar"],
    Timestamps = lists:seq(1, 10),
    _ = [
        store(DB, PublishedAt, Topic, term_to_binary({Topic, PublishedAt}))
     || Topic <- Topics, PublishedAt <- Timestamps
    ],
    ?assertEqual(
        lists:sort([{Topic, PublishedAt} || Topic <- Topics, PublishedAt <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(DB, "#", 0)])
    ),
    ?assertEqual(
        [],
        lists:sort([binary_to_term(Payload) || Payload <- iterate(DB, "#", 10 + 1)])
    ),
    ?assertEqual(
        lists:sort([{Topic, PublishedAt} || Topic <- Topics, PublishedAt <- lists:seq(5, 10)]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(DB, "#", 5)])
    ),
    ?assertEqual(
        lists:sort([
            {Topic, PublishedAt}
         || Topic <- ["foo/bar", "foo/bar/baz"], PublishedAt <- Timestamps
        ]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(DB, "foo/#", 0)])
    ),
    ?assertEqual(
        lists:sort([{"foo/bar", PublishedAt} || PublishedAt <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(DB, "foo/+", 0)])
    ),
    ?assertEqual(
        [],
        lists:sort([binary_to_term(Payload) || Payload <- iterate(DB, "foo/+/bar", 0)])
    ),
    ?assertEqual(
        lists:sort([
            {Topic, PublishedAt}
         || Topic <- ["foo/bar", "foo/bar/baz", "a/bar"], PublishedAt <- Timestamps
        ]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(DB, "+/bar/#", 0)])
    ),
    ?assertEqual(
        lists:sort([{Topic, PublishedAt} || Topic <- ["a", "a/bar"], PublishedAt <- Timestamps]),
        lists:sort([binary_to_term(Payload) || Payload <- iterate(DB, "a/#", 0)])
    ),
    ?assertEqual(
        [],
        lists:sort([binary_to_term(Payload) || Payload <- iterate(DB, "a/+/+", 0)])
    ),
    ok.

store(DB, PublishedAt, Topic, Payload) ->
    ID = emqx_guid:gen(),
    emqx_replay_message_storage:store(DB, ID, PublishedAt, parse_topic(Topic), Payload).

iterate(DB, TopicFilter, StartTime) ->
    {ok, It} = emqx_replay_message_storage:make_iterator(DB, parse_topic(TopicFilter), StartTime),
    iterate(It).

iterate(It) ->
    case emqx_replay_message_storage:next(It) of
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
        topic_bits_per_level => [8, 12, 16, 24],
        timestamp_bits => 0
    }),
    ?assert(
        proper:quickcheck(
            ?FORALL(Topic, topic(), begin
                Hash = emqx_replay_message_storage:compute_topic_hash(Topic, Keymapper),
                is_integer(Hash) andalso (byte_size(binary:encode_unsigned(Hash)) =< 8)
            end)
        )
    ).

t_prop_hash_bitmask_computes(_) ->
    Keymapper = emqx_replay_message_storage:make_keymapper(#{
        topic_bits_per_level => [8, 12, 16, 24],
        timestamp_bits => 0
    }),
    ?assert(
        proper:quickcheck(
            ?FORALL(TopicFilter, topic_filter(), begin
                Hash = emqx_replay_message_storage:compute_hash_bitmask(TopicFilter, Keymapper),
                is_integer(Hash) andalso (byte_size(binary:encode_unsigned(Hash)) =< 8)
            end)
        )
    ).

t_prop_iterate_stored_messages(Config) ->
    DB = ?config(handle, Config),
    ?assertEqual(
        true,
        proper:quickcheck(
            ?FORALL(
                Streams,
                messages(),
                begin
                    Stream = payload_gen:interleave_streams(Streams),
                    ok = store_message_stream(DB, Stream),
                    % TODO actually verify some property
                    true
                end
            )
        )
    ).

store_message_stream(DB, [{Topic, {Payload, ChunkNum, _ChunkCount}} | Rest]) ->
    MessageID = <<ChunkNum:32>>,
    PublishedAt = rand:uniform(ChunkNum),
    ok = emqx_replay_message_storage:store(DB, MessageID, PublishedAt, Topic, Payload),
    store_message_stream(DB, payload_gen:next(Rest));
store_message_stream(_DB, []) ->
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
    Filename = filename:join(?MODULE_STRING, atom_to_list(TC)),
    ok = filelib:ensure_dir(Filename),
    {ok, DB} = emqx_replay_message_storage:open(Filename, #{
        column_family => {atom_to_list(TC), []},
        keymapper => emqx_replay_message_storage:make_keymapper(#{
            topic_bits_per_level => [8, 8, 32, 16],
            timestamp_bits => 64
        })
    }),
    [{handle, DB} | Config].

end_per_testcase(_TC, Config) ->
    DB = ?config(handle, Config),
    catch emqx_replay_message_storage:close(DB).
