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
-export([new/0, next_packet_id/1, replay/2, commit_offset/3, poll/3]).

%% internal exports:
-export([]).

-export_type([inflight/0]).

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

-record(range, {
    stream :: emqx_ds:stream(),
    first :: seqno(),
    last :: seqno(),
    iterator_next :: emqx_ds:iterator() | undefined
}).

-type range() :: #range{}.

-record(inflight, {
    next_seqno = 0 :: seqno(),
    acked_seqno = 0 :: seqno(),
    offset_ranges = [] :: [range()]
}).

-opaque inflight() :: #inflight{}.

%%================================================================================
%% API funcions
%%================================================================================

-spec new() -> inflight().
new() ->
    #inflight{}.

-spec next_packet_id(inflight()) -> {emqx_types:packet_id(), inflight()}.
next_packet_id(Inflight0 = #inflight{next_seqno = LastSeqNo}) ->
    Inflight = Inflight0#inflight{next_seqno = LastSeqNo + 1},
    case LastSeqNo rem 16#10000 of
        0 ->
            %% We skip sequence numbers that lead to PacketId = 0 to
            %% simplify math. Note: it leads to occasional gaps in the
            %% sequence numbers.
            next_packet_id(Inflight);
        PacketId ->
            {PacketId, Inflight}
    end.

-spec replay(emqx_persistent_session_ds:id(), inflight()) ->
    emqx_session:replies().
replay(_SessionId, _Inflight = #inflight{offset_ranges = _Ranges}) ->
    [].

-spec commit_offset(emqx_persistent_session_ds:id(), emqx_types:packet_id(), inflight()) ->
    {_IsValidOffset :: boolean(), inflight()}.
commit_offset(
    SessionId,
    PacketId,
    Inflight0 = #inflight{
        acked_seqno = AckedSeqno0, next_seqno = NextSeqNo, offset_ranges = Ranges0
    }
) ->
    AckedSeqno =
        case packet_id_to_seqno(NextSeqNo, PacketId) of
            N when N > AckedSeqno0; AckedSeqno0 =:= 0 ->
                N;
            OutOfRange ->
                ?SLOG(warning, #{
                    msg => "out-of-order_ack",
                    prev_seqno => AckedSeqno0,
                    acked_seqno => OutOfRange,
                    next_seqno => NextSeqNo,
                    packet_id => PacketId
                }),
                AckedSeqno0
        end,
    Ranges = lists:filter(
        fun(#range{stream = Stream, last = LastSeqno, iterator_next = ItNext}) ->
            case LastSeqno =< AckedSeqno of
                true ->
                    %% This range has been fully
                    %% acked. Remove it and replace saved
                    %% iterator with the trailing iterator.
                    update_iterator(SessionId, Stream, ItNext),
                    false;
                false ->
                    %% This range still has unacked
                    %% messages:
                    true
            end
        end,
        Ranges0
    ),
    Inflight = Inflight0#inflight{acked_seqno = AckedSeqno, offset_ranges = Ranges},
    {true, Inflight}.

-spec poll(emqx_persistent_session_ds:id(), inflight(), pos_integer()) ->
    {emqx_session:replies(), inflight()}.
poll(SessionId, Inflight0, WindowSize) when WindowSize > 0, WindowSize < 16#7fff ->
    #inflight{next_seqno = NextSeqNo0, acked_seqno = AckedSeqno} =
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
            Streams = shuffle(get_streams(SessionId)),
            fetch(SessionId, Inflight0, Streams, FreeSpace, [])
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

fetch(_SessionId, Inflight, _Streams = [], _N, Acc) ->
    {lists:reverse(Acc), Inflight};
fetch(_SessionId, Inflight, _Streams, 0, Acc) ->
    {lists:reverse(Acc), Inflight};
fetch(SessionId, Inflight0, [#ds_stream{stream = Stream} | Streams], N, Publishes0) ->
    #inflight{next_seqno = FirstSeqNo, offset_ranges = Ranges0} = Inflight0,
    ItBegin = get_last_iterator(SessionId, Stream, Ranges0),
    {ok, ItEnd, Messages} = emqx_ds:next(ItBegin, N),
    {NMessages, Publishes, Inflight1} =
        lists:foldl(
            fun(Msg, {N0, PubAcc0, InflightAcc0}) ->
                {PacketId, InflightAcc} = next_packet_id(InflightAcc0),
                PubAcc = [{PacketId, Msg} | PubAcc0],
                {N0 + 1, PubAcc, InflightAcc}
            end,
            {0, Publishes0, Inflight0},
            Messages
        ),
    #inflight{next_seqno = LastSeqNo} = Inflight1,
    case NMessages > 0 of
        true ->
            Range = #range{
                first = FirstSeqNo,
                last = LastSeqNo - 1,
                stream = Stream,
                iterator_next = ItEnd
            },
            Inflight = Inflight1#inflight{offset_ranges = Ranges0 ++ [Range]},
            fetch(SessionId, Inflight, Streams, N - NMessages, Publishes);
        false ->
            fetch(SessionId, Inflight1, Streams, N, Publishes)
    end.

update_iterator(SessionId, Stream, Iterator) ->
    mria:dirty_write(?SESSION_ITER_TAB, #ds_iter{id = {SessionId, Stream}, iter = Iterator}).

get_last_iterator(SessionId, Stream, Ranges) ->
    case lists:keyfind(Stream, #range.stream, lists:reverse(Ranges)) of
        false ->
            get_iterator(SessionId, Stream);
        #range{iterator_next = Next} ->
            Next
    end.

get_iterator(SessionId, Stream) ->
    Id = {SessionId, Stream},
    [#ds_iter{iter = It}] = mnesia:dirty_read(?SESSION_ITER_TAB, Id),
    It.

get_streams(SessionId) ->
    mnesia:dirty_read(?SESSION_STREAM_TAB, SessionId).

%% Reconstruct session counter by adding most significant bits from
%% the current counter to the packet id.
-spec packet_id_to_seqno(non_neg_integer(), emqx_types:packet_id()) -> non_neg_integer().
packet_id_to_seqno(NextSeqNo, PacketId) ->
    Epoch = NextSeqNo bsr 16,
    case packet_id_to_seqno_(Epoch, PacketId) of
        N when N =< NextSeqNo ->
            N;
        _ ->
            packet_id_to_seqno_(Epoch - 1, PacketId)
    end.

-spec packet_id_to_seqno_(non_neg_integer(), emqx_types:packet_id()) -> non_neg_integer().
packet_id_to_seqno_(Epoch, PacketId) ->
    (Epoch bsl 16) + PacketId.

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

-ifdef(TEST).

%% This test only tests boundary conditions (to make sure property-based test didn't skip them):
packet_id_to_seqno_test() ->
    %% Packet ID = 1; first epoch:
    ?assertEqual(1, packet_id_to_seqno(1, 1)),
    ?assertEqual(1, packet_id_to_seqno(10, 1)),
    ?assertEqual(1, packet_id_to_seqno(1 bsl 16 - 1, 1)),
    %% Packet ID = 1; second and 3rd epochs:
    ?assertEqual(1 bsl 16 + 1, packet_id_to_seqno(1 bsl 16 + 1, 1)),
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
    WindowSize = 1 bsl 16 - 2,
    Min = max(0, NextSeqNo - WindowSize),
    Max = max(0, NextSeqNo - 1),
    range(Min, Max).

-endif.
