%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_session_ds_streams).
-moduledoc """
This helper module contains various functions used by durable sessions
to track streams.

** Blocked streams

For performance reasons we keep only one record of in-flight
messages per stream, and we don't want to overwrite these records
prematurely.

** Stream state machine

During normal operation, state of each stream can be informally
described as a FSM with the following stream states:

- *(R)eady*: stream is not blocked. Messages from such streams can
be enqueued directly to inflight.

- *BQ1*, *BQ2* and *BQ12*: stream is blocked by un-acked QoS1, QoS2
or QoS1&2 messages respectively. Such streams are stored in
`#s.bq1' or `#s.bq2' buckets (or both).

- *(U)ndead*: zombie streams that are kept for historical reasons
only. For example, streams for unsubcribed topics can linger in the
session state for a while until all queued messages are acked, as
well as streams that reached `end_of_stream'. This state is
implicit: scheduler doesn't actively track such streams. Undead
streams are ignored by the scheduler until the moment they can be
garbage-collected. So this is a terminal state. Even if the client
resubscribes, it will produce a new, totally separate set of SRS.

*** State transitions

New streams start in *Ready* state, from which they follow one of
these paths:

Upon receiving a `ds_sub_reply' for a *ready* stream, the session
assigns QoS and sequence numbers to the messages according to its
own logic, then enqueues the batch into the inflight. It
subsequently passes the updated SRS back to the scheduler, where it
can determine its block status:

      ,--(`on_unsubscribe')--> *U*
     /
    /--(only QoS0 messages in the batch)--> *R* --> ...
   /
*R* --([QoS0] & QoS1 messages)--> *BQ1* --> ...
   \
    \--([QoS0] & QoS2 messages)--> *BQ2* --> ...
     \
      \--([QoS0] & QoS1 & QoS2)--> *BQ12* --> ...
       \
        `--([QoS0] & `end_of_stream')--> *U*


*BQ1* and *BQ2* are handled similarly. They transition to *Ready*
once session calls `?MODULE:on_seqno_release' for the corresponding
QoS track and sequence number equal to the SRS's last sequence
number for the track:

                                                         .-> *R* --> ...
                                                        /
*BQX* --(`?MODULE:on_seqno_release(?QOS_X, LastSeqNoX)')
                                                        \
                                                         `--> *U*

*BQ12* is handled like this:

       .--(`on_seqno_release(?QOS_1, LastSeqNo1)')--> *BQ2* --> ...
      /
*BQ12*
      \
       `--(`on_seqno_release(?QOS_2, LastSeqNo2)')--> *BQ1* --> ...


""".

%% API:
-export([
    new/0,
    restore/1,
    on_unsubscribe/2,
    on_shared_stream_revoke/2,
    on_enqueue/5,
    on_seqno_release/4,
    find_replay_streams/1,
    is_fully_acked/2,
    is_active_unblocked/2,
    gc/1
]).

%% internal exports:
-export([]).

-export_type([t/0, stream_key/0, srs/0, ret/0, block_status/0]).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("emqx_mqtt.hrl").
-include("session_internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type stream_key() :: {emqx_persistent_session_ds:subscription_id(), emqx_ds:stream()}.

-doc """
Common return type for functions dealing with stream block states,
tuple containing three elements:

1. List of streams that entered *Ready* state.
2. List of streams that entered *Undead* state.
3. Stream manager state
""".
-type ret() :: {_R :: [stream_key()], _U :: [stream_key()], t()}.

-type srs() :: #srs{}.

-record(block, {
    id :: stream_key(),
    secondary_seqno :: emqx_persistent_session_ds:seqno() | undefined
}).

-type block() :: #block{}.

-type block_queue() :: gb_trees:tree(emqx_persistent_session_ds:seqno(), block()).

-record(s, {
    %% Block queues:
    bq1 :: block_queue(),
    bq2 :: block_queue()
}).

-opaque t() :: #s{}.

-type block_status() :: r | bq1 | bq2 | bq12 | u.

%%================================================================================
%% API functions
%%================================================================================

-spec new() -> t().
new() ->
    #s{
        bq1 = gb_trees:empty(),
        bq2 = gb_trees:empty()
    }.

-spec restore(emqx_persistent_session_ds_state:t()) -> t().
restore(S) ->
    %% Restore stream states.
    %%
    %% Note: these states are NOT used during replay.
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    emqx_persistent_session_ds_state:fold_streams(
        fun(Key, Srs, Acc) ->
            ?tp(?sessds_stream_state_trans, #{key => Key, to => '$restore'}),
            case derive_state(Comm1, Comm2, Srs) of
                r ->
                    ?tp(?sessds_stream_state_trans, #{key => Key, to => r, from => '$restore'}),
                    Acc;
                u ->
                    ?tp(?sessds_stream_state_trans, #{key => Key, to => u, from => '$restore'}),
                    Acc;
                bq1 ->
                    to_BQ1(Key, Srs, Acc);
                bq2 ->
                    to_BQ2(Key, Srs, Acc);
                bq12 ->
                    to_BQ12(Key, Srs, Acc)
            end
        end,
        new(),
        S
    ).

%% @doc Find the streams that have unacked (in-flight) messages.
%% Return them in the order they were previously replayed.
-spec find_replay_streams(emqx_persistent_session_ds_state:t()) ->
    [{stream_key(), srs()}].
find_replay_streams(S) ->
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    %% 1. Find the streams that aren't fully acked
    Streams = emqx_persistent_session_ds_state:fold_streams(
        fun(Key, Stream, Acc) ->
            case is_fully_acked(Comm1, Comm2, Stream) of
                false ->
                    [{Key, Stream} | Acc];
                true ->
                    Acc
            end
        end,
        [],
        S
    ),
    lists:sort(fun compare_streams/2, Streams).

-doc """
Marks stream as unsubscribed.
""".
-spec on_shared_stream_revoke(stream_key(), emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds_state:t().
on_shared_stream_revoke(StreamKey, S) ->
    %% TODO: what if stream is immediately returned back to the session?
    case emqx_persistent_session_ds_state:get_stream(StreamKey, S) of
        undefined ->
            S;
        SRS ->
            Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
            Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
            mark_as_unsubscribed(Comm1, Comm2, StreamKey, SRS, S)
    end.

-doc """
Update block status of streams added to the queue.

Note: this function may return *completed* streams.
This happens when the enqueued batch is the last one for the stream,
and it contains only QoS0 messages.
""".
-spec on_enqueue(
    _IsReplay :: boolean(),
    stream_key(),
    srs(),
    emqx_persistent_session_ds_state:t(),
    emqx_persistent_session_ds:session()
) ->
    ret().
on_enqueue(true, _Key, _SRS, _S, Blocks) ->
    {[], [], Blocks};
on_enqueue(false, Key, SRS, S0, Blocks) ->
    %% Is the stream blocked?
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S0),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S0),
    case derive_state(Comm1, Comm2, SRS) of
        r ->
            ?tp(?sessds_stream_state_trans, #{key => Key, to => r, from => r}),
            {[Key], [], Blocks};
        bq1 ->
            {[], [], to_BQ1(Key, SRS, Blocks)};
        bq2 ->
            {[], [], to_BQ2(Key, SRS, Blocks)};
        bq12 ->
            {[], [], to_BQ12(Key, SRS, Blocks)};
        u ->
            %% Handle a special case: session just enqueued the last
            %% batch at the end of a stream, that contained only QoS0
            %% messages. Since no acks are expected for this batch, we
            %% should attempt to advance generation now:
            on_stream_unblock(Key, r, SRS, Blocks)
    end.

-spec on_seqno_release(
    ?QOS_1 | ?QOS_2, emqx_persistent_session_ds:seqno(), emqx_persistent_session_ds_state:t(), t()
) ->
    ret().
on_seqno_release(?QOS_1, ReleasedSeqNo, S, Blocks = #s{bq1 = BQ10}) ->
    CommQoS2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    case bq_pop(ReleasedSeqNo, CommQoS2, BQ10) of
        true ->
            %% Still blocked:
            {[], [], Blocks};
        {true, Key, BQ1} ->
            %% It was bq12:
            ?tp(?sessds_stream_state_trans, #{
                key => Key,
                from => bq12,
                to => bq2
            }),
            {[], [], Blocks#s{bq1 = BQ1}};
        {false, Key, BQ1} ->
            %% It was bq1:
            ?tp(?sessds_stream_state_trans, #{
                key => Key,
                from => bq1,
                to => r
            }),
            SRS = emqx_persistent_session_ds_state:get_stream(Key, S),
            on_stream_unblock(Key, bq1, SRS, Blocks#s{bq1 = BQ1})
    end;
on_seqno_release(?QOS_2, ReleasedSeqNo, S, Blocks = #s{bq2 = BQ20}) ->
    CommQoS1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    case bq_pop(ReleasedSeqNo, CommQoS1, BQ20) of
        true ->
            %% Still blocked:
            {[], Blocks};
        {true, Key, BQ2} ->
            %% It was bq12:
            ?tp(?sessds_stream_state_trans, #{
                key => Key,
                from => bq12,
                to => bq1
            }),
            {[], Blocks#s{bq2 = BQ2}};
        {false, Key, BQ2} ->
            %% It was bq2:
            ?tp(?sessds_stream_state_trans, #{
                key => Key,
                from => bq2,
                to => r
            }),
            SRS = emqx_persistent_session_ds_state:get_stream(Key, S),
            on_stream_unblock(Key, bq2, SRS, Blocks#s{bq2 = BQ2})
    end.

-spec on_stream_unblock(stream_key(), block_status(), srs(), t()) ->
    ret().
on_stream_unblock(Key, PrevState, #srs{unsubscribed = true}, Blocks) ->
    ?tp(?sessds_stream_state_trans, #{key => Key, to => u, from => PrevState, unsubcribed => true}),
    {[], [Key], Blocks};
on_stream_unblock(
    Key,
    PrevState,
    #srs{it_end = end_of_stream},
    Blocks
) ->
    ?tp(?sessds_stream_state_trans, #{key => Key, to => u, from => PrevState, eos => true}),
    {[], [Key], Blocks};
on_stream_unblock(Key, PrevState, _SRS, Blocks) ->
    ?tp(?sessds_stream_state_trans, #{key => Key, to => r, from => PrevState}),
    {[Key], [], Blocks}.

-doc """
Batch operation that cleans up historical streams.

1. Remove fully replayed streams for past generations.
2. Remove unblocked streams for deleted subscriptions.
""".
-spec gc(emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds_state:t().
gc(S) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    emqx_persistent_session_ds_state:fold_streams(
        fun(Key, SRS, Acc) ->
            maybe_drop_stream(CommQos1, CommQos2, Key, SRS, Acc)
        end,
        S,
        S
    ).

-doc """
Makes stream manager forget about a subscription. This
operation is idempotent.

Side effects:

1. Set `unsubscribed' flag on SRS records that belong to the subscription
""".
-spec on_unsubscribe(
    emqx_persistent_session_ds:subscription_id(),
    emqx_persistent_session_ds_state:t()
) ->
    emqx_persistent_session_ds_state:t().
on_unsubscribe(SubId, S) ->
    %% NOTE: this function only marks streams for deletion, but
    %% it doesn't outright delete them.
    %%
    %% It's done for two reasons:
    %%
    %% - MQTT standard states that the broker MUST process acks for
    %% all sent messages, and it MAY keep on sending buffered
    %% messages:
    %% https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901186
    %%
    %% - Deleting the streams may lead to gaps in the sequence number
    %% series, and lead to problems with acknowledgement tracking, we
    %% avoid that by delaying the deletion.
    %%
    %% When the stream is marked for deletion, the session won't fetch
    %% _new_ batches from it. Actual deletion is done by `gc', when it
    %% detects that all in-flight messages from the stream have been
    %% acked by the client.
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    emqx_persistent_session_ds_state:fold_streams(
        SubId,
        fun(Stream, SRS, SAcc) ->
            mark_as_unsubscribed(Comm1, Comm2, {SubId, Stream}, SRS, SAcc)
        end,
        S,
        S
    ).

-spec is_fully_acked(
    srs(), emqx_persistent_session_ds_state:t()
) -> boolean().
is_fully_acked(Srs, S) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    is_fully_acked(CommQos1, CommQos2, Srs).

-spec is_active_unblocked(srs() | undefined, emqx_persistent_session_ds_state:t()) -> boolean().
is_active_unblocked(undefined, _S) ->
    false;
is_active_unblocked(#srs{unsubscribed = true}, _S) ->
    false;
is_active_unblocked(SRS, S) ->
    is_fully_acked(SRS, S).

%%================================================================================
%% Internal functions
%%================================================================================

%% @doc Set `unsubscribed' flag to SRS and remove DS subscription
mark_as_unsubscribed(Comm1, Comm2, Key, SRS0 = #srs{}, S) ->
    SRS = SRS0#srs{unsubscribed = true},
    is_fully_acked(Comm1, Comm2, SRS) andalso
        ?tp(?sessds_stream_state_trans, #{key => Key, to => u, from => x}),
    emqx_persistent_session_ds_state:put_stream(Key, SRS, S).

maybe_drop_stream(CommQos1, CommQos2, Key, SRS = #srs{unsubscribed = true}, S) ->
    %% This stream belongs to an unsubscribed topic:
    case is_fully_acked(CommQos1, CommQos2, SRS) of
        true ->
            ?SLOG(debug, #{
                msg => sessds_del_stream,
                reason => unsubscribed,
                key => Key
            }),
            emqx_persistent_session_ds_state:del_stream(Key, S);
        false ->
            S
    end;
maybe_drop_stream(
    _CommQos1,
    _CommQos2,
    Key = {SubId, _Stream},
    #srs{rank_x = RankX, rank_y = RankY, it_end = end_of_stream},
    S
) ->
    %% This is a fully replayed stream:
    case emqx_persistent_session_ds_state:get_rank({SubId, RankX}, S) of
        undefined ->
            S;
        MinRankY when RankY =< MinRankY ->
            %% 1. Remove fully replayed streams:
            ?SLOG(debug, #{
                msg => sessds_del_stream,
                reason => fully_replayed,
                key => Key,
                rank => {RankX, RankY},
                min => MinRankY
            }),
            emqx_persistent_session_ds_state:del_stream(Key, S);
        _ ->
            S
    end;
maybe_drop_stream(_CommQos1, _CommQos2, _Key, _SRS, S) ->
    S.

%%--------------------------------------------------------------------------------
%% SRS FSM
%%--------------------------------------------------------------------------------

-spec derive_state(
    emqx_persistent_session_ds:seqno(), emqx_persistent_session_ds:seqno(), srs()
) -> block_status().
derive_state(Comm1, Comm2, SRS) ->
    case is_fully_replayed(Comm1, Comm2, SRS) of
        true ->
            u;
        false ->
            case {is_track_acked(?QOS_1, Comm1, SRS), is_track_acked(?QOS_2, Comm2, SRS)} of
                {true, true} -> r;
                {false, true} -> bq1;
                {true, false} -> bq2;
                {false, false} -> bq12
            end
    end.

-spec to_BQ1(stream_key(), srs(), t()) -> t().
to_BQ1(Key, SRS, S = #s{bq1 = BQ1}) ->
    ?tp(?sessds_stream_state_trans, #{
        key => Key,
        to => bq1
    }),
    Block = #block{id = Key},
    S#s{bq1 = bq_push(SRS#srs.last_seqno_qos1, Block, BQ1)}.

-spec to_BQ2(stream_key(), srs(), t()) -> t().
to_BQ2(Key, SRS, S = #s{bq2 = BQ2}) ->
    ?tp(?sessds_stream_state_trans, #{
        key => Key,
        to => bq2
    }),
    Block = #block{id = Key},
    S#s{bq2 = bq_push(SRS#srs.last_seqno_qos2, Block, BQ2)}.

-spec to_BQ12(stream_key(), srs(), t()) -> t().
to_BQ12(Key, SRS, S = #s{bq1 = BQ1, bq2 = BQ2}) ->
    ?tp(?sessds_stream_state_trans, #{
        key => Key,
        to => bq12
    }),
    #srs{last_seqno_qos1 = SN1, last_seqno_qos2 = SN2} = SRS,
    Block1 = #block{id = Key, secondary_seqno = SN2},
    Block2 = #block{id = Key, secondary_seqno = SN1},
    S#s{bq1 = bq_push(SN1, Block1, BQ1), bq2 = bq_push(SN2, Block2, BQ2)}.

%%--------------------------------------------------------------------------------
%% Misc.
%%--------------------------------------------------------------------------------

%% @doc Compare the streams by the order in which they were replayed.
compare_streams(
    {_KeyA, #srs{first_seqno_qos1 = A1, first_seqno_qos2 = A2}},
    {_KeyB, #srs{first_seqno_qos1 = B1, first_seqno_qos2 = B2}}
) ->
    case A1 =:= B1 of
        true ->
            A2 =< B2;
        false ->
            A1 < B1
    end.

is_fully_replayed(Comm1, Comm2, S = #srs{it_end = It, unsubscribed = Unsubscribed}) ->
    ((It =:= end_of_stream) or Unsubscribed) andalso is_fully_acked(Comm1, Comm2, S).

is_fully_acked(Comm1, Comm2, SRS) ->
    is_track_acked(?QOS_1, Comm1, SRS) andalso
        is_track_acked(?QOS_2, Comm2, SRS).

is_track_acked(?QOS_1, Committed, #srs{first_seqno_qos1 = First, last_seqno_qos1 = Last}) ->
    First =:= Last orelse Committed >= Last;
is_track_acked(?QOS_2, Committed, #srs{first_seqno_qos2 = First, last_seqno_qos2 = Last}) ->
    First =:= Last orelse Committed >= Last.

%%--------------------------------------------------------------------------------
%% Block queue
%%--------------------------------------------------------------------------------

bq_pop(ReleasedSeqNo, SecondaryCommittedSeqNo, BQ0) ->
    case gb_trees:take_any(ReleasedSeqNo, BQ0) of
        error ->
            %% Nothing was unblocked:
            true;
        {#block{id = Key, secondary_seqno = Secondary}, BQ} when
            Secondary =:= undefined; Secondary =< SecondaryCommittedSeqNo
        ->
            %% Unblocked:
            {false, Key, BQ};
        {#block{id = Key}, BQ} ->
            %% Still blocked by other QoS track:
            {true, Key, BQ}
    end.

bq_push(Primary, B = #block{}, BQ) ->
    gb_trees:insert(Primary, B, BQ).

%%--------------------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------------------
