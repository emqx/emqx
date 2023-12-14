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
-export([new/0, open/1, next_packet_id/1, n_inflight/1]).

-export([poll/4, replay/2, commit_offset/4]).

-export([seqno_to_packet_id/1, packet_id_to_seqno/2]).

-export([committed_until/2]).

%% internal exports:
-export([]).

-export_type([inflight/0, seqno/0]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include("emqx_persistent_session_ds.hrl").

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(EPOCH_SIZE, 16#10000).

-define(ACK, 0).
-define(COMP, 1).

-define(TRACK_FLAG(WHICH), (1 bsl WHICH)).
-define(TRACK_FLAGS_ALL, ?TRACK_FLAG(?ACK) bor ?TRACK_FLAG(?COMP)).
-define(TRACK_FLAGS_NONE, 0).

%%================================================================================
%% Type declarations
%%================================================================================

%% Note: sequence numbers are monotonic; they don't wrap around:
-type seqno() :: non_neg_integer().

-type track() :: ack | comp.
-type commit_type() :: rec.

-record(inflight, {
    next_seqno = 1 :: seqno(),
    commits = #{ack => 1, comp => 1, rec => 1} :: #{track() | commit_type() => seqno()},
    %% Ranges are sorted in ascending order of their sequence numbers.
    offset_ranges = [] :: [ds_pubrange()]
}).

-opaque inflight() :: #inflight{}.

-type message() :: emqx_types:message().
-type replies() :: [emqx_session:reply()].

-type preproc_fun() :: fun((message()) -> message() | [message()]).

%%================================================================================
%% API funcions
%%================================================================================

-spec new() -> inflight().
new() ->
    #inflight{}.

-spec open(emqx_persistent_session_ds:id()) -> inflight().
open(SessionId) ->
    {Ranges, RecUntil} = ro_transaction(
        fun() -> {get_ranges(SessionId), get_committed_offset(SessionId, rec)} end
    ),
    {Commits, NextSeqno} = compute_inflight_range(Ranges),
    #inflight{
        commits = Commits#{rec => RecUntil},
        next_seqno = NextSeqno,
        offset_ranges = Ranges
    }.

-spec next_packet_id(inflight()) -> {emqx_types:packet_id(), inflight()}.
next_packet_id(Inflight0 = #inflight{next_seqno = LastSeqno}) ->
    Inflight = Inflight0#inflight{next_seqno = next_seqno(LastSeqno)},
    {seqno_to_packet_id(LastSeqno), Inflight}.

-spec n_inflight(inflight()) -> non_neg_integer().
n_inflight(#inflight{offset_ranges = Ranges}) ->
    %% TODO
    %% This is not very efficient. Instead, we can take the maximum of
    %% `range_size(AckedUntil, NextSeqno)` and `range_size(CompUntil, NextSeqno)`.
    %% This won't be exact number but a pessimistic estimate, but this way we
    %% will penalize clients that PUBACK QoS 1 messages but don't PUBCOMP QoS 2
    %% messages for some reason. For that to work, we need to additionally track
    %% actual `AckedUntil` / `CompUntil` during `commit_offset/4`.
    lists:foldl(
        fun
            (#ds_pubrange{type = ?T_CHECKPOINT}, N) ->
                N;
            (#ds_pubrange{type = ?T_INFLIGHT} = Range, N) ->
                N + range_size(Range)
        end,
        0,
        Ranges
    ).

-spec replay(preproc_fun(), inflight()) -> {emqx_session:replies(), inflight()}.
replay(PreprocFunFun, Inflight0 = #inflight{offset_ranges = Ranges0, commits = Commits}) ->
    {Ranges, Replies} = lists:mapfoldr(
        fun(Range, Acc) ->
            replay_range(PreprocFunFun, Commits, Range, Acc)
        end,
        [],
        Ranges0
    ),
    Inflight = Inflight0#inflight{offset_ranges = Ranges},
    {Replies, Inflight}.

-spec commit_offset(emqx_persistent_session_ds:id(), Offset, emqx_types:packet_id(), inflight()) ->
    {_IsValidOffset :: boolean(), inflight()}
when
    Offset :: track() | commit_type().
commit_offset(
    SessionId,
    Track,
    PacketId,
    Inflight0 = #inflight{commits = Commits}
) when Track == ack orelse Track == comp ->
    case validate_commit(Track, PacketId, Inflight0) of
        CommitUntil when is_integer(CommitUntil) ->
            %% TODO
            %% We do not preserve `CommitUntil` in the database. Instead, we discard
            %% fully acked ranges from the database. In effect, this means that the
            %% most recent `CommitUntil` the client has sent may be lost in case of a
            %% crash or client loss.
            Inflight1 = Inflight0#inflight{commits = Commits#{Track := CommitUntil}},
            Inflight = discard_committed(SessionId, Inflight1),
            {true, Inflight};
        false ->
            {false, Inflight0}
    end;
commit_offset(
    SessionId,
    CommitType = rec,
    PacketId,
    Inflight0 = #inflight{commits = Commits}
) ->
    case validate_commit(CommitType, PacketId, Inflight0) of
        CommitUntil when is_integer(CommitUntil) ->
            update_committed_offset(SessionId, CommitType, CommitUntil),
            Inflight = Inflight0#inflight{commits = Commits#{CommitType := CommitUntil}},
            {true, Inflight};
        false ->
            {false, Inflight0}
    end.

-spec poll(preproc_fun(), emqx_persistent_session_ds:id(), inflight(), pos_integer()) ->
    {emqx_session:replies(), inflight()}.
poll(PreprocFun, SessionId, Inflight0, WindowSize) when WindowSize > 0, WindowSize < ?EPOCH_SIZE ->
    MinBatchSize = emqx_config:get([session_persistence, min_batch_size]),
    FetchThreshold = min(MinBatchSize, ceil(WindowSize / 2)),
    FreeSpace = WindowSize - n_inflight(Inflight0),
    case FreeSpace >= FetchThreshold of
        false ->
            %% TODO: this branch is meant to avoid fetching data from
            %% the DB in chunks that are too small. However, this
            %% logic is not exactly good for the latency. Can the
            %% client get stuck even?
            {[], Inflight0};
        true ->
            %% TODO: Wrap this in `mria:async_dirty/2`?
            Checkpoints = find_checkpoints(Inflight0#inflight.offset_ranges),
            StreamGroups = group_streams(get_streams(SessionId)),
            {Publihes, Inflight} =
                fetch(PreprocFun, SessionId, Inflight0, Checkpoints, StreamGroups, FreeSpace, []),
            %% Discard now irrelevant QoS0-only ranges, if any.
            {Publihes, discard_committed(SessionId, Inflight)}
    end.

%% Which seqno this track is committed until.
%% "Until" means this is first seqno that is _not yet committed_ for this track.
-spec committed_until(track() | commit_type(), inflight()) -> seqno().
committed_until(Track, #inflight{commits = Commits}) ->
    maps:get(Track, Commits).

-spec seqno_to_packet_id(seqno()) -> emqx_types:packet_id() | 0.
seqno_to_packet_id(Seqno) ->
    Seqno rem ?EPOCH_SIZE.

%% Reconstruct session counter by adding most significant bits from
%% the current counter to the packet id.
-spec packet_id_to_seqno(emqx_types:packet_id(), inflight()) -> seqno().
packet_id_to_seqno(PacketId, #inflight{next_seqno = NextSeqno}) ->
    packet_id_to_seqno_(NextSeqno, PacketId).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

compute_inflight_range([]) ->
    {#{ack => 1, comp => 1}, 1};
compute_inflight_range(Ranges) ->
    _RangeLast = #ds_pubrange{until = LastSeqno} = lists:last(Ranges),
    AckedUntil = find_committed_until(ack, Ranges),
    CompUntil = find_committed_until(comp, Ranges),
    Commits = #{
        ack => emqx_maybe:define(AckedUntil, LastSeqno),
        comp => emqx_maybe:define(CompUntil, LastSeqno)
    },
    {Commits, LastSeqno}.

find_committed_until(Track, Ranges) ->
    RangesUncommitted = lists:dropwhile(
        fun(Range) ->
            case Range of
                #ds_pubrange{type = ?T_CHECKPOINT} ->
                    true;
                #ds_pubrange{type = ?T_INFLIGHT, tracks = Tracks} ->
                    not has_track(Track, Tracks)
            end
        end,
        Ranges
    ),
    case RangesUncommitted of
        [#ds_pubrange{id = {_, CommittedUntil, _StreamRef}} | _] ->
            CommittedUntil;
        [] ->
            undefined
    end.

-spec get_ranges(emqx_persistent_session_ds:id()) -> [ds_pubrange()].
get_ranges(SessionId) ->
    Pat = erlang:make_tuple(
        record_info(size, ds_pubrange),
        '_',
        [{1, ds_pubrange}, {#ds_pubrange.id, {SessionId, '_', '_'}}]
    ),
    mnesia:match_object(?SESSION_PUBRANGE_TAB, Pat, read).

fetch(PreprocFun, SessionId, Inflight0, CPs, Groups, N, Acc) when N > 0, Groups =/= [] ->
    #inflight{next_seqno = FirstSeqno, offset_ranges = Ranges} = Inflight0,
    {Stream, Groups2} = get_the_first_stream(Groups),
    case get_next_n_messages_from_stream(Stream, CPs, N) of
        [] ->
            fetch(PreprocFun, SessionId, Inflight0, CPs, Groups2, N, Acc);
        {ItBegin, ItEnd, Messages} ->
            %% We need to preserve the iterator pointing to the beginning of the
            %% range, so that we can replay it if needed.
            {Publishes, UntilSeqno} = publish_fetch(PreprocFun, FirstSeqno, Messages),
            Size = range_size(FirstSeqno, UntilSeqno),
            Range0 = #ds_pubrange{
                id = {SessionId, FirstSeqno, Stream#ds_stream.ref},
                type = ?T_INFLIGHT,
                tracks = compute_pub_tracks(Publishes),
                until = UntilSeqno,
                iterator = ItBegin
            },
            ok = preserve_range(Range0),
            %% ...Yet we need to keep the iterator pointing past the end of the
            %% range, so that we can pick up where we left off: it will become
            %% `ItBegin` of the next range for this stream.
            Range = keep_next_iterator(ItEnd, Range0),
            Inflight = Inflight0#inflight{
                next_seqno = UntilSeqno,
                offset_ranges = Ranges ++ [Range]
            },
            fetch(PreprocFun, SessionId, Inflight, CPs, Groups2, N - Size, [Publishes | Acc])
    end;
fetch(_ReplyFun, _SessionId, Inflight, _CPs, _Groups, _N, Acc) ->
    Publishes = lists:append(lists:reverse(Acc)),
    {Publishes, Inflight}.

discard_committed(
    SessionId,
    Inflight0 = #inflight{commits = Commits, offset_ranges = Ranges0}
) ->
    %% TODO: This could be kept and incrementally updated in the inflight state.
    Checkpoints = find_checkpoints(Ranges0),
    %% TODO: Wrap this in `mria:async_dirty/2`?
    Ranges = discard_committed_ranges(SessionId, Commits, Checkpoints, Ranges0),
    Inflight0#inflight{offset_ranges = Ranges}.

find_checkpoints(Ranges) ->
    lists:foldl(
        fun(#ds_pubrange{id = {_SessionId, _, StreamRef}} = Range, Acc) ->
            %% For each stream, remember the last range over this stream.
            Acc#{StreamRef => Range}
        end,
        #{},
        Ranges
    ).

discard_committed_ranges(
    SessionId,
    Commits,
    Checkpoints,
    Ranges = [Range = #ds_pubrange{id = {_SessionId, _, StreamRef}} | Rest]
) ->
    case discard_committed_range(Commits, Range) of
        discard ->
            %% This range has been fully committed.
            %% Either discard it completely, or preserve the iterator for the next range
            %% over this stream (i.e. a checkpoint).
            RangeKept =
                case maps:get(StreamRef, Checkpoints) of
                    Range ->
                        [checkpoint_range(Range)];
                    _Previous ->
                        discard_range(Range),
                        []
                end,
            %% Since we're (intentionally) not using transactions here, it's important to
            %% issue database writes in the same order in which ranges are stored: from
            %% the oldest to the newest. This is also why we need to compute which ranges
            %% should become checkpoints before we start writing anything.
            RangeKept ++ discard_committed_ranges(SessionId, Commits, Checkpoints, Rest);
        keep ->
            %% This range has not been fully committed.
            [Range | discard_committed_ranges(SessionId, Commits, Checkpoints, Rest)];
        keep_all ->
            %% The rest of ranges (if any) still have uncommitted messages.
            Ranges;
        TracksLeft ->
            %% Only some track has been committed.
            %% Preserve the uncommitted tracks in the database.
            RangeKept = Range#ds_pubrange{tracks = TracksLeft},
            preserve_range(restore_first_iterator(RangeKept)),
            [RangeKept | discard_committed_ranges(SessionId, Commits, Checkpoints, Rest)]
    end;
discard_committed_ranges(_SessionId, _Commits, _Checkpoints, []) ->
    [].

discard_committed_range(_Commits, #ds_pubrange{type = ?T_CHECKPOINT}) ->
    discard;
discard_committed_range(
    #{ack := AckedUntil, comp := CompUntil},
    #ds_pubrange{until = Until}
) when Until > AckedUntil andalso Until > CompUntil ->
    keep_all;
discard_committed_range(Commits, #ds_pubrange{until = Until, tracks = Tracks}) ->
    case discard_tracks(Commits, Until, Tracks) of
        0 ->
            discard;
        Tracks ->
            keep;
        TracksLeft ->
            TracksLeft
    end.

discard_tracks(#{ack := AckedUntil, comp := CompUntil}, Until, Tracks) ->
    TAck =
        case Until > AckedUntil of
            true -> ?TRACK_FLAG(?ACK) band Tracks;
            false -> 0
        end,
    TComp =
        case Until > CompUntil of
            true -> ?TRACK_FLAG(?COMP) band Tracks;
            false -> 0
        end,
    TAck bor TComp.

replay_range(
    PreprocFun,
    Commits,
    Range0 = #ds_pubrange{
        type = ?T_INFLIGHT, id = {_, First, _StreamRef}, until = Until, iterator = It
    },
    Acc
) ->
    Size = range_size(First, Until),
    {ok, ItNext, MessagesUnacked} = emqx_ds:next(?PERSISTENT_MESSAGE_DB, It, Size),
    %% Asserting that range is consistent with the message storage state.
    {Replies, Until} = publish_replay(PreprocFun, Commits, First, MessagesUnacked),
    %% Again, we need to keep the iterator pointing past the end of the
    %% range, so that we can pick up where we left off.
    Range = keep_next_iterator(ItNext, Range0),
    {Range, Replies ++ Acc};
replay_range(_PreprocFun, _Commits, Range0 = #ds_pubrange{type = ?T_CHECKPOINT}, Acc) ->
    {Range0, Acc}.

validate_commit(
    Track,
    PacketId,
    Inflight = #inflight{commits = Commits, next_seqno = NextSeqno}
) ->
    Seqno = packet_id_to_seqno_(NextSeqno, PacketId),
    CommittedUntil = maps:get(Track, Commits),
    CommitNext = get_commit_next(Track, Inflight),
    case Seqno >= CommittedUntil andalso Seqno < CommitNext of
        true ->
            next_seqno(Seqno);
        false ->
            ?SLOG(warning, #{
                msg => "out-of-order_commit",
                track => Track,
                packet_id => PacketId,
                commit_seqno => Seqno,
                committed_until => CommittedUntil,
                commit_next => CommitNext
            }),
            false
    end.

get_commit_next(ack, #inflight{next_seqno = NextSeqno}) ->
    NextSeqno;
get_commit_next(rec, #inflight{next_seqno = NextSeqno}) ->
    NextSeqno;
get_commit_next(comp, #inflight{commits = Commits}) ->
    maps:get(rec, Commits).

publish_fetch(PreprocFun, FirstSeqno, Messages) ->
    flatmapfoldl(
        fun(MessageIn, Acc) ->
            Message = PreprocFun(MessageIn),
            publish_fetch(Message, Acc)
        end,
        FirstSeqno,
        Messages
    ).

publish_fetch(#message{qos = ?QOS_0} = Message, Seqno) ->
    {{undefined, Message}, Seqno};
publish_fetch(#message{} = Message, Seqno) ->
    PacketId = seqno_to_packet_id(Seqno),
    {{PacketId, Message}, next_seqno(Seqno)};
publish_fetch(Messages, Seqno) ->
    flatmapfoldl(fun publish_fetch/2, Seqno, Messages).

publish_replay(PreprocFun, Commits, FirstSeqno, Messages) ->
    #{ack := AckedUntil, comp := CompUntil, rec := RecUntil} = Commits,
    flatmapfoldl(
        fun(MessageIn, Acc) ->
            Message = PreprocFun(MessageIn),
            publish_replay(Message, AckedUntil, CompUntil, RecUntil, Acc)
        end,
        FirstSeqno,
        Messages
    ).

publish_replay(#message{qos = ?QOS_0}, _, _, _, Seqno) ->
    %% QoS 0 (at most once) messages should not be replayed.
    {[], Seqno};
publish_replay(#message{qos = Qos} = Message, AckedUntil, CompUntil, RecUntil, Seqno) ->
    case Qos of
        ?QOS_1 when Seqno < AckedUntil ->
            %% This message has already been acked, so we can skip it.
            %% We still need to advance seqno, because previously we assigned this message
            %% a unique Packet Id.
            {[], next_seqno(Seqno)};
        ?QOS_2 when Seqno < CompUntil ->
            %% This message's flow has already been fully completed, so we can skip it.
            %% We still need to advance seqno, because previously we assigned this message
            %% a unique Packet Id.
            {[], next_seqno(Seqno)};
        ?QOS_2 when Seqno < RecUntil ->
            %% This message's flow has been partially completed, we need to resend a PUBREL.
            PacketId = seqno_to_packet_id(Seqno),
            Pub = {pubrel, PacketId},
            {Pub, next_seqno(Seqno)};
        _ ->
            %% This message flow hasn't been acked and/or received, we need to resend it.
            PacketId = seqno_to_packet_id(Seqno),
            Pub = {PacketId, emqx_message:set_flag(dup, true, Message)},
            {Pub, next_seqno(Seqno)}
    end;
publish_replay([], _, _, _, Seqno) ->
    {[], Seqno};
publish_replay(Messages, AckedUntil, CompUntil, RecUntil, Seqno) ->
    flatmapfoldl(
        fun(Message, Acc) ->
            publish_replay(Message, AckedUntil, CompUntil, RecUntil, Acc)
        end,
        Seqno,
        Messages
    ).

-spec compute_pub_tracks(replies()) -> non_neg_integer().
compute_pub_tracks(Pubs) ->
    compute_pub_tracks(Pubs, ?TRACK_FLAGS_NONE).

compute_pub_tracks(_Pubs, Tracks = ?TRACK_FLAGS_ALL) ->
    Tracks;
compute_pub_tracks([Pub | Rest], Tracks) ->
    Track =
        case Pub of
            {_PacketId, #message{qos = ?QOS_1}} -> ?TRACK_FLAG(?ACK);
            {_PacketId, #message{qos = ?QOS_2}} -> ?TRACK_FLAG(?COMP);
            {pubrel, _PacketId} -> ?TRACK_FLAG(?COMP);
            _ -> ?TRACK_FLAGS_NONE
        end,
    compute_pub_tracks(Rest, Track bor Tracks);
compute_pub_tracks([], Tracks) ->
    Tracks.

keep_next_iterator(ItNext, Range = #ds_pubrange{iterator = ItFirst, misc = Misc}) ->
    Range#ds_pubrange{
        iterator = ItNext,
        %% We need to keep the first iterator around, in case we need to preserve
        %% this range again, updating still uncommitted tracks it's part of.
        misc = Misc#{iterator_first => ItFirst}
    }.

restore_first_iterator(Range = #ds_pubrange{misc = Misc = #{iterator_first := ItFirst}}) ->
    Range#ds_pubrange{
        iterator = ItFirst,
        misc = maps:remove(iterator_first, Misc)
    }.

-spec preserve_range(ds_pubrange()) -> ok.
preserve_range(Range = #ds_pubrange{type = ?T_INFLIGHT}) ->
    mria:dirty_write(?SESSION_PUBRANGE_TAB, Range).

has_track(ack, Tracks) ->
    (?TRACK_FLAG(?ACK) band Tracks) > 0;
has_track(comp, Tracks) ->
    (?TRACK_FLAG(?COMP) band Tracks) > 0.

-spec discard_range(ds_pubrange()) -> ok.
discard_range(#ds_pubrange{id = RangeId}) ->
    mria:dirty_delete(?SESSION_PUBRANGE_TAB, RangeId).

-spec checkpoint_range(ds_pubrange()) -> ds_pubrange().
checkpoint_range(Range0 = #ds_pubrange{type = ?T_INFLIGHT}) ->
    Range = Range0#ds_pubrange{type = ?T_CHECKPOINT, misc = #{}},
    ok = mria:dirty_write(?SESSION_PUBRANGE_TAB, Range),
    Range;
checkpoint_range(Range = #ds_pubrange{type = ?T_CHECKPOINT}) ->
    %% This range should have been checkpointed already.
    Range.

get_last_iterator(Stream = #ds_stream{ref = StreamRef}, Checkpoints) ->
    case maps:get(StreamRef, Checkpoints, none) of
        none ->
            Stream#ds_stream.beginning;
        #ds_pubrange{iterator = ItNext} ->
            ItNext
    end.

-spec get_streams(emqx_persistent_session_ds:id()) -> [ds_stream()].
get_streams(SessionId) ->
    mnesia:dirty_read(?SESSION_STREAM_TAB, SessionId).

-spec get_committed_offset(emqx_persistent_session_ds:id(), _Name) -> seqno().
get_committed_offset(SessionId, Name) ->
    case mnesia:read(?SESSION_COMMITTED_OFFSET_TAB, {SessionId, Name}) of
        [] ->
            1;
        [#ds_committed_offset{until = Seqno}] ->
            Seqno
    end.

-spec update_committed_offset(emqx_persistent_session_ds:id(), _Name, seqno()) -> ok.
update_committed_offset(SessionId, Name, Until) ->
    mria:dirty_write(?SESSION_COMMITTED_OFFSET_TAB, #ds_committed_offset{
        id = {SessionId, Name}, until = Until
    }).

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

packet_id_to_seqno_(NextSeqno, PacketId) ->
    Epoch = NextSeqno bsr 16,
    case (Epoch bsl 16) + PacketId of
        N when N =< NextSeqno ->
            N;
        N ->
            N - ?EPOCH_SIZE
    end.

range_size(#ds_pubrange{id = {_, First, _StreamRef}, until = Until}) ->
    range_size(First, Until).

range_size(FirstSeqno, UntilSeqno) ->
    %% This function assumes that gaps in the sequence ID occur _only_ when the
    %% packet ID wraps.
    Size = UntilSeqno - FirstSeqno,
    Size + (FirstSeqno bsr 16) - (UntilSeqno bsr 16).

%%================================================================================
%% stream scheduler

%% group streams by the first position in the rank
-spec group_streams(list(ds_stream())) -> list(list(ds_stream())).
group_streams(Streams) ->
    Groups = maps:groups_from_list(
        fun(#ds_stream{rank = {RankX, _}}) -> RankX end,
        Streams
    ),
    shuffle(maps:values(Groups)).

-spec shuffle([A]) -> [A].
shuffle(L0) ->
    L1 = lists:map(
        fun(A) ->
            %% maybe topic/stream prioritization could be introduced here?
            {rand:uniform(), A}
        end,
        L0
    ),
    L2 = lists:sort(L1),
    {_, L} = lists:unzip(L2),
    L.

get_the_first_stream([Group | Groups]) ->
    case get_next_stream_from_group(Group) of
        {Stream, {sorted, []}} ->
            {Stream, Groups};
        {Stream, Group2} ->
            {Stream, [Group2 | Groups]};
        undefined ->
            get_the_first_stream(Groups)
    end;
get_the_first_stream([]) ->
    %% how this possible ?
    throw(#{reason => no_valid_stream}).

%% the scheduler is simple, try to get messages from the same shard, but it's okay to take turns
get_next_stream_from_group({sorted, [H | T]}) ->
    {H, {sorted, T}};
get_next_stream_from_group({sorted, []}) ->
    undefined;
get_next_stream_from_group(Streams) ->
    [Stream | T] = lists:sort(
        fun(#ds_stream{rank = {_, RankA}}, #ds_stream{rank = {_, RankB}}) ->
            RankA < RankB
        end,
        Streams
    ),
    {Stream, {sorted, T}}.

get_next_n_messages_from_stream(Stream, CPs, N) ->
    ItBegin = get_last_iterator(Stream, CPs),
    case emqx_ds:next(?PERSISTENT_MESSAGE_DB, ItBegin, N) of
        {ok, _ItEnd, []} ->
            [];
        {ok, ItEnd, Messages} ->
            {ItBegin, ItEnd, Messages};
        {ok, end_of_stream} ->
            %% TODO: how to skip this closed stream or it should be taken over by lower level layer
            []
    end.

%%================================================================================

-spec flatmapfoldl(fun((X, Acc) -> {Y | [Y], Acc}), Acc, [X]) -> {[Y], Acc}.
flatmapfoldl(_Fun, Acc, []) ->
    {[], Acc};
flatmapfoldl(Fun, Acc, [X | Xs]) ->
    {Ys, NAcc} = Fun(X, Acc),
    {Zs, FAcc} = flatmapfoldl(Fun, NAcc, Xs),
    case is_list(Ys) of
        true ->
            {Ys ++ Zs, FAcc};
        _ ->
            {[Ys | Zs], FAcc}
    end.

ro_transaction(Fun) ->
    {atomic, Res} = mria:ro_transaction(?DS_MRIA_SHARD, Fun),
    Res.

-ifdef(TEST).

%% This test only tests boundary conditions (to make sure property-based test didn't skip them):
packet_id_to_seqno_test() ->
    %% Packet ID = 1; first epoch:
    ?assertEqual(1, packet_id_to_seqno_(1, 1)),
    ?assertEqual(1, packet_id_to_seqno_(10, 1)),
    ?assertEqual(1, packet_id_to_seqno_(1 bsl 16 - 1, 1)),
    ?assertEqual(1, packet_id_to_seqno_(1 bsl 16, 1)),
    %% Packet ID = 1; second and 3rd epochs:
    ?assertEqual(1 bsl 16 + 1, packet_id_to_seqno_(1 bsl 16 + 1, 1)),
    ?assertEqual(1 bsl 16 + 1, packet_id_to_seqno_(2 bsl 16, 1)),
    ?assertEqual(2 bsl 16 + 1, packet_id_to_seqno_(2 bsl 16 + 1, 1)),
    %% Packet ID = 16#ffff:
    PID = 1 bsl 16 - 1,
    ?assertEqual(PID, packet_id_to_seqno_(PID, PID)),
    ?assertEqual(PID, packet_id_to_seqno_(1 bsl 16, PID)),
    ?assertEqual(1 bsl 16 + PID, packet_id_to_seqno_(2 bsl 16, PID)),
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
                PacketId = seqno_to_packet_id(SeqNo),
                ?assertEqual(SeqNo, packet_id_to_seqno_(NextSeqNo, PacketId)),
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
            {#{ack => 1, comp => 1}, 1},
            compute_inflight_range([])
        ),
        ?_assertEqual(
            {#{ack => 12, comp => 13}, 42},
            compute_inflight_range([
                #ds_pubrange{id = {<<>>, 1, 0}, until = 2, type = ?T_CHECKPOINT},
                #ds_pubrange{id = {<<>>, 4, 0}, until = 8, type = ?T_CHECKPOINT},
                #ds_pubrange{id = {<<>>, 11, 0}, until = 12, type = ?T_CHECKPOINT},
                #ds_pubrange{
                    id = {<<>>, 12, 0},
                    until = 13,
                    type = ?T_INFLIGHT,
                    tracks = ?TRACK_FLAG(?ACK)
                },
                #ds_pubrange{
                    id = {<<>>, 13, 0},
                    until = 20,
                    type = ?T_INFLIGHT,
                    tracks = ?TRACK_FLAG(?COMP)
                },
                #ds_pubrange{
                    id = {<<>>, 20, 0},
                    until = 42,
                    type = ?T_INFLIGHT,
                    tracks = ?TRACK_FLAG(?ACK) bor ?TRACK_FLAG(?COMP)
                }
            ])
        ),
        ?_assertEqual(
            {#{ack => 13, comp => 13}, 13},
            compute_inflight_range([
                #ds_pubrange{id = {<<>>, 1, 0}, until = 2, type = ?T_CHECKPOINT},
                #ds_pubrange{id = {<<>>, 4, 0}, until = 8, type = ?T_CHECKPOINT},
                #ds_pubrange{id = {<<>>, 11, 0}, until = 12, type = ?T_CHECKPOINT},
                #ds_pubrange{id = {<<>>, 12, 0}, until = 13, type = ?T_CHECKPOINT}
            ])
        )
    ].

-endif.
