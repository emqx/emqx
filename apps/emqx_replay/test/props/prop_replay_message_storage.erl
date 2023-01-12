%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(prop_replay_message_storage).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(WORK_DIR, ["_build", "test"]).
-define(RUN_ID, {?MODULE, testrun_id}).
-define(GEN_ID, 42).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_bitstring_computes() ->
    ?FORALL(
        Keymapper,
        keymapper(),
        ?FORALL({Topic, Timestamp}, {topic(), integer()}, begin
            BS = emqx_replay_message_storage:compute_bitstring(Topic, Timestamp, Keymapper),
            is_integer(BS) andalso (BS < (1 bsl get_keymapper_bitsize(Keymapper)))
        end)
    ).

prop_topic_bitmask_computes() ->
    Keymapper = make_keymapper(16, [8, 12, 16], 100),
    ?FORALL(TopicFilter, topic_filter(), begin
        Mask = emqx_replay_message_storage:compute_topic_bitmask(TopicFilter, Keymapper),
        % topic bits + timestamp LSBs
        is_integer(Mask) andalso (Mask < (1 bsl (36 + 6)))
    end).

prop_next_seek_monotonic() ->
    ?FORALL(
        {TopicFilter, StartTime, Keymapper},
        {topic_filter(), pos_integer(), keymapper()},
        begin
            Filter = emqx_replay_message_storage:make_keyspace_filter(
                TopicFilter,
                StartTime,
                Keymapper
            ),
            ?FORALL(
                Bitstring,
                bitstr(get_keymapper_bitsize(Keymapper)),
                emqx_replay_message_storage:compute_next_seek(Bitstring, Filter) >= Bitstring
            )
        end
    ).

prop_next_seek_eq_initial_seek() ->
    ?FORALL(
        Filter,
        keyspace_filter(),
        emqx_replay_message_storage:compute_initial_seek(Filter) =:=
            emqx_replay_message_storage:compute_next_seek(0, Filter)
    ).

prop_iterate_messages() ->
    TBPL = [4, 8, 12],
    Options = #{
        timestamp_bits => 32,
        topic_bits_per_level => TBPL,
        epoch => 200
    },
    % TODO
    % Shrinking is too unpredictable and leaves a LOT of garbage in the scratch dit.
    ?FORALL(Stream, noshrink(non_empty(messages(topic(TBPL)))), begin
        Filepath = make_filepath(?FUNCTION_NAME, erlang:system_time(microsecond)),
        {DB, Handle} = open_db(Filepath, Options),
        Shim = emqx_replay_message_storage_shim:open(),
        ok = store_db(DB, Stream),
        ok = store_shim(Shim, Stream),
        ?FORALL(
            {
                {Topic, _},
                Pattern,
                StartTime
            },
            {
                nth(Stream),
                topic_filter_pattern(),
                start_time()
            },
            begin
                TopicFilter = make_topic_filter(Pattern, Topic),
                Messages = iterate_db(DB, TopicFilter, StartTime),
                Reference = iterate_shim(Shim, TopicFilter, StartTime),
                ok = close_db(Handle),
                ok = emqx_replay_message_storage_shim:close(Shim),
                ?WHENFAIL(
                    begin
                        io:format(user, " *** Filepath = ~s~n", [Filepath]),
                        io:format(user, " *** TopicFilter = ~p~n", [TopicFilter]),
                        io:format(user, " *** StartTime = ~p~n", [StartTime])
                    end,
                    is_list(Messages) andalso equals(Messages -- Reference, Reference -- Messages)
                )
            end
        )
    end).

prop_iterate_eq_iterate_with_preserve_restore() ->
    TBPL = [4, 8, 16, 12],
    Options = #{
        timestamp_bits => 32,
        topic_bits_per_level => TBPL,
        epoch => 500
    },
    {DB, _Handle} = open_db(make_filepath(?FUNCTION_NAME), Options),
    ?FORALL(Stream, non_empty(messages(topic(TBPL))), begin
        % TODO
        % This proptest is impure because messages from testruns assumed to be
        % independent of each other are accumulated in the same storage. This
        % would probably confuse shrinker in the event a testrun fails.
        ok = store_db(DB, Stream),
        ?FORALL(
            {
                {Topic, _},
                Pat,
                StartTime,
                Commands
            },
            {
                nth(Stream),
                topic_filter_pattern(),
                start_time(),
                shuffled(flat([non_empty(list({preserve, restore})), list(iterate)]))
            },
            begin
                TopicFilter = make_topic_filter(Pat, Topic),
                Iterator = make_iterator(DB, TopicFilter, StartTime),
                Messages = run_iterator_commands(Commands, Iterator, DB),
                equals(Messages, iterate_db(DB, TopicFilter, StartTime))
            end
        )
    end).

prop_iterate_eq_iterate_with_refresh() ->
    TBPL = [4, 8, 16, 12],
    Options = #{
        timestamp_bits => 32,
        topic_bits_per_level => TBPL,
        epoch => 500
    },
    {DB, _Handle} = open_db(make_filepath(?FUNCTION_NAME), Options),
    ?FORALL(Stream, non_empty(messages(topic(TBPL))), begin
        % TODO
        % This proptest is also impure, see above.
        ok = store_db(DB, Stream),
        ?FORALL(
            {
                {Topic, _},
                Pat,
                StartTime,
                RefreshEvery
            },
            {
                nth(Stream),
                topic_filter_pattern(),
                start_time(),
                pos_integer()
            },
            ?TIMEOUT(5000, begin
                TopicFilter = make_topic_filter(Pat, Topic),
                IterationOptions = #{iterator_refresh => {every, RefreshEvery}},
                Iterator = make_iterator(DB, TopicFilter, StartTime, IterationOptions),
                Messages = iterate_db(Iterator),
                equals(Messages, iterate_db(DB, TopicFilter, StartTime))
            end)
        )
    end).

% store_message_stream(DB, [{Topic, {Payload, ChunkNum, _ChunkCount}} | Rest]) ->
%     MessageID = emqx_guid:gen(),
%     PublishedAt = ChunkNum,
%         MessageID, PublishedAt, Topic
%     ]),
%     ok = emqx_replay_message_storage:store(DB, MessageID, PublishedAt, Topic, Payload),
%     store_message_stream(DB, payload_gen:next(Rest));
% store_message_stream(_Zone, []) ->
%     ok.

store_db(DB, Messages) ->
    lists:foreach(
        fun({Topic, Payload = {MessageID, Timestamp, _}}) ->
            Bin = term_to_binary(Payload),
            emqx_replay_message_storage:store(DB, MessageID, Timestamp, Topic, Bin)
        end,
        Messages
    ).

iterate_db(DB, TopicFilter, StartTime) ->
    iterate_db(make_iterator(DB, TopicFilter, StartTime)).

iterate_db(It) ->
    case emqx_replay_message_storage:next(It) of
        {value, Payload, ItNext} ->
            [binary_to_term(Payload) | iterate_db(ItNext)];
        none ->
            []
    end.

make_iterator(DB, TopicFilter, StartTime) ->
    {ok, It} = emqx_replay_message_storage:make_iterator(DB, TopicFilter, StartTime),
    It.

make_iterator(DB, TopicFilter, StartTime, Options) ->
    {ok, It} = emqx_replay_message_storage:make_iterator(DB, TopicFilter, StartTime, Options),
    It.

run_iterator_commands([iterate | Rest], It, DB) ->
    case emqx_replay_message_storage:next(It) of
        {value, Payload, ItNext} ->
            [binary_to_term(Payload) | run_iterator_commands(Rest, ItNext, DB)];
        none ->
            []
    end;
run_iterator_commands([{preserve, restore} | Rest], It, DB) ->
    Serial = emqx_replay_message_storage:preserve_iterator(It),
    {ok, ItNext} = emqx_replay_message_storage:restore_iterator(DB, Serial),
    run_iterator_commands(Rest, ItNext, DB);
run_iterator_commands([], It, _DB) ->
    iterate_db(It).

store_shim(Shim, Messages) ->
    lists:foreach(
        fun({Topic, Payload = {MessageID, Timestamp, _}}) ->
            Bin = term_to_binary(Payload),
            emqx_replay_message_storage_shim:store(Shim, MessageID, Timestamp, Topic, Bin)
        end,
        Messages
    ).

iterate_shim(Shim, TopicFilter, StartTime) ->
    lists:map(
        fun binary_to_term/1,
        emqx_replay_message_storage_shim:iterate(Shim, TopicFilter, StartTime)
    ).

%%--------------------------------------------------------------------
%% Setup / teardown
%%--------------------------------------------------------------------

open_db(Filepath, Options) ->
    {ok, Handle} = rocksdb:open(Filepath, [{create_if_missing, true}]),
    {Schema, CFRefs} = emqx_replay_message_storage:create_new(Handle, ?GEN_ID, Options),
    DB = emqx_replay_message_storage:open(Handle, ?GEN_ID, CFRefs, Schema),
    {DB, Handle}.

close_db(Handle) ->
    rocksdb:close(Handle).

make_filepath(TC) ->
    make_filepath(TC, 0).

make_filepath(TC, InstID) ->
    Name = io_lib:format("~0p.~0p", [TC, InstID]),
    Path = filename:join(?WORK_DIR ++ ["proper", "runs", get_run_id(), ?MODULE_STRING, Name]),
    ok = filelib:ensure_dir(Path),
    Path.

get_run_id() ->
    case persistent_term:get(?RUN_ID, undefined) of
        RunID when RunID /= undefined ->
            RunID;
        undefined ->
            RunID = make_run_id(),
            ok = persistent_term:put(?RUN_ID, RunID),
            RunID
    end.

make_run_id() ->
    calendar:system_time_to_rfc3339(erlang:system_time(second), [{offset, "Z"}]).

%%--------------------------------------------------------------------
%% Type generators
%%--------------------------------------------------------------------

topic() ->
    non_empty(list(topic_level())).

topic(EntropyWeights) ->
    ?LET(L, scaled(1 / 4, list(1)), begin
        EWs = lists:sublist(EntropyWeights ++ L, length(L)),
        ?SIZED(S, [oneof([topic_level(S * EW), topic_level_fixed()]) || EW <- EWs])
    end).

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

topic_level_pattern() ->
    frequency([
        {5, level},
        {2, '+'},
        {1, '#'}
    ]).

topic_filter_pattern() ->
    list(topic_level_pattern()).

topic_filter(Topic) ->
    ?LET({T, Pat}, {Topic, topic_filter_pattern()}, make_topic_filter(Pat, T)).

make_topic_filter([], _) ->
    [];
make_topic_filter(_, []) ->
    [];
make_topic_filter(['#' | _], _) ->
    ['#'];
make_topic_filter(['+' | Rest], [_ | Levels]) ->
    ['+' | make_topic_filter(Rest, Levels)];
make_topic_filter([level | Rest], [L | Levels]) ->
    [L | make_topic_filter(Rest, Levels)].

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

topic_level_fixed() ->
    oneof([
        <<"foo">>,
        <<"bar">>,
        <<"baz">>,
        <<"xyzzy">>
    ]).

keymapper() ->
    ?LET(
        {TimestampBits, TopicBits, Epoch},
        {
            range(0, 128),
            non_empty(list(range(1, 32))),
            pos_integer()
        },
        make_keymapper(TimestampBits, TopicBits, Epoch * 100)
    ).

keyspace_filter() ->
    ?LET(
        {TopicFilter, StartTime, Keymapper},
        {topic_filter(), pos_integer(), keymapper()},
        emqx_replay_message_storage:make_keyspace_filter(TopicFilter, StartTime, Keymapper)
    ).

messages(Topic) ->
    ?LET(
        Ts,
        list(Topic),
        interleaved(
            ?LET(Messages, vector(length(Ts), scaled(4, list(message()))), lists:zip(Ts, Messages))
        )
    ).

message() ->
    ?LET({Timestamp, Payload}, {timestamp(), binary()}, {emqx_guid:gen(), Timestamp, Payload}).

message_streams(Topic) ->
    ?LET(Topics, list(Topic), [{T, payload_gen:binary_stream_gen(64)} || T <- Topics]).

timestamp() ->
    scaled(20, pos_integer()).

start_time() ->
    scaled(10, pos_integer()).

bitstr(Size) ->
    ?LET(B, binary(1 + (Size div 8)), binary:decode_unsigned(B) band (1 bsl Size - 1)).

nth(L) ->
    ?LET(I, range(1, length(L)), lists:nth(I, L)).

scaled(Factor, T) ->
    ?SIZED(S, resize(ceil(S * Factor), T)).

interleaved(T) ->
    ?LET({L, Seed}, {T, integer()}, interleave(L, rand:seed_s(exsss, Seed))).

shuffled(T) ->
    ?LET({L, Seed}, {T, integer()}, shuffle(L, rand:seed_s(exsss, Seed))).

flat(T) ->
    ?LET(L, T, lists:flatten(L)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

make_keymapper(TimestampBits, TopicBits, MaxEpoch) ->
    emqx_replay_message_storage:make_keymapper(#{
        timestamp_bits => TimestampBits,
        topic_bits_per_level => TopicBits,
        epoch => MaxEpoch
    }).

get_keymapper_bitsize(Keymapper) ->
    maps:get(bitsize, emqx_replay_message_storage:keymapper_info(Keymapper)).

-spec interleave(list({Tag, list(E)}), rand:state()) -> list({Tag, E}).
interleave(Seqs, Rng) ->
    interleave(Seqs, length(Seqs), Rng).

interleave(Seqs, L, Rng) when L > 0 ->
    {N, RngNext} = rand:uniform_s(L, Rng),
    {SeqHead, SeqTail} = lists:split(N - 1, Seqs),
    case SeqTail of
        [{Tag, [M | Rest]} | SeqRest] ->
            [{Tag, M} | interleave(SeqHead ++ [{Tag, Rest} | SeqRest], L, RngNext)];
        [{_, []} | SeqRest] ->
            interleave(SeqHead ++ SeqRest, L - 1, RngNext)
    end;
interleave([], 0, _) ->
    [].

-spec shuffle(list(E), rand:state()) -> list(E).
shuffle(L, Rng) ->
    {Rands, _} = randoms(length(L), Rng),
    [E || {_, E} <- lists:sort(lists:zip(Rands, L))].

randoms(N, Rng) when N > 0 ->
    {Rand, RngNext} = rand:uniform_s(Rng),
    {Tail, RngFinal} = randoms(N - 1, RngNext),
    {[Rand | Tail], RngFinal};
randoms(_, Rng) ->
    {[], Rng}.
