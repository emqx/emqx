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

-module(prop_replay_storage).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ZONE, mk_zone_name(?FUNCTION_NAME)).
-define(SETUP(Test), ?SETUP(fun() -> setup(?ZONE) end, Test)).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_bitstring_computes() ->
    ?FORALL(Keymapper, keymapper(), begin
        Bitsize = emqx_replay_message_storage:bitsize(Keymapper),
        ?FORALL({Topic, Timestamp}, {topic(), integer()}, begin
            BS = emqx_replay_message_storage:compute_bitstring(Topic, Timestamp, Keymapper),
            is_integer(BS) andalso (BS < (1 bsl Bitsize))
        end)
    end).

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
                TopicFilter, StartTime, Keymapper
            ),
            ?FORALL(
                Bitstring,
                bitstr(emqx_replay_message_storage:bitsize(Keymapper)),
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

prop_iterate_stored_messages() ->
    ?SETUP(
        ?FORALL(Streams, message_streams(), begin
            Stream = payload_gen:interleave_streams(Streams),
            ok = store_message_stream(?ZONE, Stream),
            % TODO actually verify some property
            true
        end)
    ).

store_message_stream(Zone, [{Topic, {Payload, ChunkNum, _ChunkCount}} | Rest]) ->
    MessageID = <<ChunkNum:32>>,
    PublishedAt = rand:uniform(ChunkNum),
    ok = emqx_replay_local_store:store(Zone, MessageID, PublishedAt, Topic, Payload),
    store_message_stream(Zone, payload_gen:next(Rest));
store_message_stream(_Zone, []) ->
    ok.

%%--------------------------------------------------------------------
%% Setup / teardown
%%--------------------------------------------------------------------

setup(Zone) ->
    {ok, _} = application:ensure_all_started(emqx_replay),
    {ok, _} = emqx_replay_local_store_sup:start_zone(Zone),
    fun() ->
        application:stop(emqx_replay)
    end.

%%--------------------------------------------------------------------
%% Type generators
%%--------------------------------------------------------------------

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

% entropy_weights() ->

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

bitstr(Size) ->
    ?LET(B, binary(1 + (Size div 8)), binary:decode_unsigned(B) band (1 bsl Size - 1)).

message_streams() ->
    ?LET(Topics, list(topic()), begin
        [{Topic, payload_gen:binary_stream_gen(64)} || Topic <- Topics]
    end).

%%

make_keymapper(TimestampBits, TopicBits, MaxEpoch) ->
    emqx_replay_message_storage:make_keymapper(#{
        timestamp_bits => TimestampBits,
        topic_bits_per_level => TopicBits,
        epoch => MaxEpoch
    }).

mk_zone_name(TC) ->
    list_to_atom(?MODULE_STRING ++ "_" ++ atom_to_list(TC)).
