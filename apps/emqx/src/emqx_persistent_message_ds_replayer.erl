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

%% @doc This module implements the routines for replaying streams of
%% messages.
-module(emqx_persistent_message_ds_replayer).

%% API:
-export([new/0, open/1, next_packet_id/1, replay/1, commit_offset/3, poll/3, n_inflight/1]).

%% internal exports:
-export([]).

-export_type([inflight/0, seqno/0]).

-include_lib("emqx/include/logger.hrl").
-include("emqx_persistent_session_ds.hrl").

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

%% Note: sequence numbers are monotonic; they don't wrap around:
-type seqno() :: non_neg_integer().

-record(inflight, {
    next_seqno = 1 :: seqno(),
    acked_until = 1 :: seqno(),
    %% Ranges are sorted in ascending order of their sequence numbers.
    offset_ranges = [] :: [ds_pubrange()]
}).

-opaque inflight() :: #inflight{}.

%%================================================================================
%% API funcions
%%================================================================================

-spec new() -> inflight().
new() ->
    #inflight{}.

-spec open(emqx_persistent_session_ds:id()) -> inflight().
open(SessionId) ->
    Ranges = ro_transaction(fun() -> get_ranges(SessionId) end),
    {AckedUntil, NextSeqno} = compute_inflight_range(Ranges),
    #inflight{
        acked_until = AckedUntil,
        next_seqno = NextSeqno,
        offset_ranges = Ranges
    }.

-spec next_packet_id(inflight()) -> {emqx_types:packet_id(), inflight()}.
next_packet_id(Inflight0 = #inflight{next_seqno = LastSeqno}) ->
    Inflight = Inflight0#inflight{next_seqno = next_seqno(LastSeqno)},
    {seqno_to_packet_id(LastSeqno), Inflight}.

-spec n_inflight(inflight()) -> non_neg_integer().
n_inflight(#inflight{next_seqno = NextSeqno, acked_until = AckedUntil}) ->
    range_size(AckedUntil, NextSeqno).

-spec replay(inflight()) ->
    {emqx_session:replies(), inflight()}.
replay(Inflight0 = #inflight{acked_until = AckedUntil, offset_ranges = Ranges0}) ->
    {Ranges, Replies} = lists:mapfoldr(
        fun(Range, Acc) ->
            replay_range(Range, AckedUntil, Acc)
        end,
        [],
        Ranges0
    ),
    Inflight = Inflight0#inflight{offset_ranges = Ranges},
    {Replies, Inflight}.

-spec commit_offset(emqx_persistent_session_ds:id(), emqx_types:packet_id(), inflight()) ->
    {_IsValidOffset :: boolean(), inflight()}.
commit_offset(
    SessionId,
    PacketId,
    Inflight0 = #inflight{
        acked_until = AckedUntil, next_seqno = NextSeqno
    }
) ->
    case packet_id_to_seqno(NextSeqno, PacketId) of
        Seqno when Seqno >= AckedUntil andalso Seqno < NextSeqno ->
            %% TODO
            %% We do not preserve `acked_until` in the database. Instead, we discard
            %% fully acked ranges from the database. In effect, this means that the
            %% most recent `acked_until` the client has sent may be lost in case of a
            %% crash or client loss.
            Inflight1 = Inflight0#inflight{acked_until = next_seqno(Seqno)},
            Inflight = discard_acked(SessionId, Inflight1),
            {true, Inflight};
        OutOfRange ->
            ?SLOG(warning, #{
                msg => "out-of-order_ack",
                acked_until => AckedUntil,
                acked_seqno => OutOfRange,
                next_seqno => NextSeqno,
                packet_id => PacketId
            }),
            {false, Inflight0}
    end.

-spec poll(emqx_persistent_session_ds:id(), inflight(), pos_integer()) ->
    {emqx_session:replies(), inflight()}.
poll(SessionId, Inflight0, WindowSize) when WindowSize > 0, WindowSize < 16#7fff ->
    #inflight{next_seqno = NextSeqNo0, acked_until = AckedSeqno} =
        Inflight0,
    FetchThreshold = max(1, WindowSize div 2),
    FreeSpace = AckedSeqno + WindowSize - NextSeqNo0,
    case FreeSpace >= FetchThreshold of
        false ->
            %% TODO: this branch is meant to avoid fetching data from
            %% the DB in chunks that are too small. However, this
            %% logic is not exactly good for the latency. Can the
            %% client get stuck even?
            {[], Inflight0};
        true ->
            %% TODO: Wrap this in `mria:async_dirty/2`?
            Streams = shuffle(get_streams(SessionId)),
            fetch(SessionId, Inflight0, Streams, FreeSpace, [])
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

compute_inflight_range([]) ->
    {1, 1};
compute_inflight_range(Ranges) ->
    _RangeLast = #ds_pubrange{until = LastSeqno} = lists:last(Ranges),
    RangesUnacked = lists:dropwhile(
        fun(#ds_pubrange{type = T}) -> T == checkpoint end,
        Ranges
    ),
    case RangesUnacked of
        [#ds_pubrange{id = {_, AckedUntil}} | _] ->
            {AckedUntil, LastSeqno};
        [] ->
            {LastSeqno, LastSeqno}
    end.

-spec get_ranges(emqx_persistent_session_ds:id()) -> [ds_pubrange()].
get_ranges(SessionId) ->
    Pat = erlang:make_tuple(
        record_info(size, ds_pubrange),
        '_',
        [{1, ds_pubrange}, {#ds_pubrange.id, {SessionId, '_'}}]
    ),
    mnesia:match_object(?SESSION_PUBRANGE_TAB, Pat, read).

fetch(SessionId, Inflight0, [DSStream | Streams], N, Acc) when N > 0 ->
    #inflight{next_seqno = FirstSeqno, offset_ranges = Ranges} = Inflight0,
    ItBegin = get_last_iterator(DSStream, Ranges),
    {ok, ItEnd, Messages} = emqx_ds:next(?PERSISTENT_MESSAGE_DB, ItBegin, N),
    {Publishes, UntilSeqno} = publish(FirstSeqno, Messages),
    case range_size(FirstSeqno, UntilSeqno) of
        Size when Size > 0 ->
            %% We need to preserve the iterator pointing to the beginning of the
            %% range, so that we can replay it if needed.
            Range0 = #ds_pubrange{
                id = {SessionId, FirstSeqno},
                type = inflight,
                until = UntilSeqno,
                stream = DSStream#ds_stream.ref,
                iterator = ItBegin
            },
            ok = preserve_range(Range0),
            %% ...Yet we need to keep the iterator pointing past the end of the
            %% range, so that we can pick up where we left off: it will become
            %% `ItBegin` of the next range for this stream.
            Range = Range0#ds_pubrange{iterator = ItEnd},
            Inflight = Inflight0#inflight{
                next_seqno = UntilSeqno,
                offset_ranges = Ranges ++ [Range]
            },
            fetch(SessionId, Inflight, Streams, N - Size, [Publishes | Acc]);
        0 ->
            fetch(SessionId, Inflight0, Streams, N, Acc)
    end;
fetch(_SessionId, Inflight, _Streams, _N, Acc) ->
    Publishes = lists:append(lists:reverse(Acc)),
    {Publishes, Inflight}.

discard_acked(
    SessionId,
    Inflight0 = #inflight{acked_until = AckedUntil, offset_ranges = Ranges0}
) ->
    %% TODO: This could be kept and incrementally updated in the inflight state.
    Checkpoints = find_checkpoints(Ranges0),
    %% TODO: Wrap this in `mria:async_dirty/2`?
    Ranges = discard_acked_ranges(SessionId, AckedUntil, Checkpoints, Ranges0),
    Inflight0#inflight{offset_ranges = Ranges}.

find_checkpoints(Ranges) ->
    lists:foldl(
        fun(#ds_pubrange{stream = StreamRef, until = Until}, Acc) ->
            %% For each stream, remember the last range over this stream.
            Acc#{StreamRef => Until}
        end,
        #{},
        Ranges
    ).

discard_acked_ranges(
    SessionId,
    AckedUntil,
    Checkpoints,
    [Range = #ds_pubrange{until = Until, stream = StreamRef} | Rest]
) when Until =< AckedUntil ->
    %% This range has been fully acked.
    %% Either discard it completely, or preserve the iterator for the next range
    %% over this stream (i.e. a checkpoint).
    RangeKept =
        case maps:get(StreamRef, Checkpoints) of
            CP when CP > Until ->
                discard_range(Range),
                [];
            Until ->
                [checkpoint_range(Range)]
        end,
    %% Since we're (intentionally) not using transactions here, it's important to
    %% issue database writes in the same order in which ranges are stored: from
    %% the oldest to the newest. This is also why we need to compute which ranges
    %% should become checkpoints before we start writing anything.
    RangeKept ++ discard_acked_ranges(SessionId, AckedUntil, Checkpoints, Rest);
discard_acked_ranges(_SessionId, _AckedUntil, _Checkpoints, Ranges) ->
    %% The rest of ranges (if any) still have unacked messages.
    Ranges.

replay_range(
    Range0 = #ds_pubrange{type = inflight, id = {_, First}, until = Until, iterator = It},
    AckedUntil,
    Acc
) ->
    Size = range_size(First, Until),
    FirstUnacked = max(First, AckedUntil),
    {ok, ItNext, Messages} = emqx_ds:next(?PERSISTENT_MESSAGE_DB, It, Size),
    MessagesUnacked =
        case FirstUnacked of
            First ->
                Messages;
            _ ->
                lists:nthtail(range_size(First, FirstUnacked), Messages)
        end,
    %% Asserting that range is consistent with the message storage state.
    {Replies, Until} = publish(FirstUnacked, MessagesUnacked),
    %% Again, we need to keep the iterator pointing past the end of the
    %% range, so that we can pick up where we left off.
    Range = Range0#ds_pubrange{iterator = ItNext},
    {Range, Replies ++ Acc};
replay_range(Range0 = #ds_pubrange{type = checkpoint}, _AckedUntil, Acc) ->
    {Range0, Acc}.

publish(FirstSeqno, Messages) ->
    lists:mapfoldl(
        fun(Message, Seqno) ->
            PacketId = seqno_to_packet_id(Seqno),
            {{PacketId, Message}, next_seqno(Seqno)}
        end,
        FirstSeqno,
        Messages
    ).

-spec preserve_range(ds_pubrange()) -> ok.
preserve_range(Range = #ds_pubrange{type = inflight}) ->
    mria:dirty_write(?SESSION_PUBRANGE_TAB, Range).

-spec discard_range(ds_pubrange()) -> ok.
discard_range(#ds_pubrange{id = RangeId}) ->
    mria:dirty_delete(?SESSION_PUBRANGE_TAB, RangeId).

-spec checkpoint_range(ds_pubrange()) -> ds_pubrange().
checkpoint_range(Range0 = #ds_pubrange{type = inflight}) ->
    Range = Range0#ds_pubrange{type = checkpoint},
    ok = mria:dirty_write(?SESSION_PUBRANGE_TAB, Range),
    Range;
checkpoint_range(Range = #ds_pubrange{type = checkpoint}) ->
    %% This range should have been checkpointed already.
    Range.

get_last_iterator(DSStream = #ds_stream{ref = StreamRef}, Ranges) ->
    case lists:keyfind(StreamRef, #ds_pubrange.stream, lists:reverse(Ranges)) of
        false ->
            DSStream#ds_stream.beginning;
        #ds_pubrange{iterator = ItNext} ->
            ItNext
    end.

-spec get_streams(emqx_persistent_session_ds:id()) -> [ds_stream()].
get_streams(SessionId) ->
    mnesia:dirty_read(?SESSION_STREAM_TAB, SessionId).

next_seqno(Seqno) ->
    NextSeqno = Seqno + 1,
    case seqno_to_packet_id(NextSeqno) of
        0 ->
            %% We skip sequence numbers that lead to PacketId = 0 to
            %% simplify math. Note: it leads to occasional gaps in the
            %% sequence numbers.
            NextSeqno + 1;
        _ ->
            NextSeqno
    end.

%% Reconstruct session counter by adding most significant bits from
%% the current counter to the packet id.
-spec packet_id_to_seqno(_Next :: seqno(), emqx_types:packet_id()) -> seqno().
packet_id_to_seqno(NextSeqNo, PacketId) ->
    Epoch = NextSeqNo bsr 16,
    case packet_id_to_seqno_(Epoch, PacketId) of
        N when N =< NextSeqNo ->
            N;
        _ ->
            packet_id_to_seqno_(Epoch - 1, PacketId)
    end.

-spec packet_id_to_seqno_(non_neg_integer(), emqx_types:packet_id()) -> seqno().
packet_id_to_seqno_(Epoch, PacketId) ->
    (Epoch bsl 16) + PacketId.

-spec seqno_to_packet_id(seqno()) -> emqx_types:packet_id() | 0.
seqno_to_packet_id(Seqno) ->
    Seqno rem 16#10000.

range_size(FirstSeqno, UntilSeqno) ->
    %% This function assumes that gaps in the sequence ID occur _only_ when the
    %% packet ID wraps.
    Size = UntilSeqno - FirstSeqno,
    Size + (FirstSeqno bsr 16) - (UntilSeqno bsr 16).

-spec shuffle([A]) -> [A].
shuffle(L0) ->
    L1 = lists:map(
        fun(A) ->
            {rand:uniform(), A}
        end,
        L0
    ),
    L2 = lists:sort(L1),
    {_, L} = lists:unzip(L2),
    L.

ro_transaction(Fun) ->
    {atomic, Res} = mria:ro_transaction(?DS_MRIA_SHARD, Fun),
    Res.

-ifdef(TEST).

%% This test only tests boundary conditions (to make sure property-based test didn't skip them):
packet_id_to_seqno_test() ->
    %% Packet ID = 1; first epoch:
    ?assertEqual(1, packet_id_to_seqno(1, 1)),
    ?assertEqual(1, packet_id_to_seqno(10, 1)),
    ?assertEqual(1, packet_id_to_seqno(1 bsl 16 - 1, 1)),
    ?assertEqual(1, packet_id_to_seqno(1 bsl 16, 1)),
    %% Packet ID = 1; second and 3rd epochs:
    ?assertEqual(1 bsl 16 + 1, packet_id_to_seqno(1 bsl 16 + 1, 1)),
    ?assertEqual(1 bsl 16 + 1, packet_id_to_seqno(2 bsl 16, 1)),
    ?assertEqual(2 bsl 16 + 1, packet_id_to_seqno(2 bsl 16 + 1, 1)),
    %% Packet ID = 16#ffff:
    PID = 1 bsl 16 - 1,
    ?assertEqual(PID, packet_id_to_seqno(PID, PID)),
    ?assertEqual(PID, packet_id_to_seqno(1 bsl 16, PID)),
    ?assertEqual(1 bsl 16 + PID, packet_id_to_seqno(2 bsl 16, PID)),
    ok.

packet_id_to_seqno_test_() ->
    Opts = [{numtests, 1000}, {to_file, user}],
    {timeout, 30, fun() -> ?assert(proper:quickcheck(packet_id_to_seqno_prop(), Opts)) end}.

packet_id_to_seqno_prop() ->
    ?FORALL(
        NextSeqNo,
        next_seqno_gen(),
        ?FORALL(
            SeqNo,
            seqno_gen(NextSeqNo),
            begin
                PacketId = SeqNo rem 16#10000,
                ?assertEqual(SeqNo, packet_id_to_seqno(NextSeqNo, PacketId)),
                true
            end
        )
    ).

next_seqno_gen() ->
    ?LET(
        {Epoch, Offset},
        {non_neg_integer(), non_neg_integer()},
        Epoch bsl 16 + Offset
    ).

seqno_gen(NextSeqNo) ->
    WindowSize = 1 bsl 16 - 1,
    Min = max(0, NextSeqNo - WindowSize),
    Max = max(0, NextSeqNo - 1),
    range(Min, Max).

range_size_test_() ->
    [
        ?_assertEqual(0, range_size(42, 42)),
        ?_assertEqual(1, range_size(42, 43)),
        ?_assertEqual(1, range_size(16#ffff, 16#10001)),
        ?_assertEqual(16#ffff - 456 + 123, range_size(16#1f0000 + 456, 16#200000 + 123))
    ].

compute_inflight_range_test_() ->
    [
        ?_assertEqual(
            {1, 1},
            compute_inflight_range([])
        ),
        ?_assertEqual(
            {12, 42},
            compute_inflight_range([
                #ds_pubrange{id = {<<>>, 1}, until = 2, type = checkpoint},
                #ds_pubrange{id = {<<>>, 4}, until = 8, type = checkpoint},
                #ds_pubrange{id = {<<>>, 11}, until = 12, type = checkpoint},
                #ds_pubrange{id = {<<>>, 12}, until = 13, type = inflight},
                #ds_pubrange{id = {<<>>, 13}, until = 20, type = inflight},
                #ds_pubrange{id = {<<>>, 20}, until = 42, type = inflight}
            ])
        ),
        ?_assertEqual(
            {13, 13},
            compute_inflight_range([
                #ds_pubrange{id = {<<>>, 1}, until = 2, type = checkpoint},
                #ds_pubrange{id = {<<>>, 4}, until = 8, type = checkpoint},
                #ds_pubrange{id = {<<>>, 11}, until = 12, type = checkpoint},
                #ds_pubrange{id = {<<>>, 12}, until = 13, type = checkpoint}
            ])
        )
    ].

-endif.
