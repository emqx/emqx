%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Stream scheduler is a helper module used by durable sessions
%% to track states of the DS streams (Stream Replay States, or SRS for
%% short). It has two main duties:
%%
%% - It manages DS subscriptions.
%%
%% - It keeps track of stream block states.
%%
%% - It accumulates new stream notifications, tracks stream ranks, and
%% advances generations.
%%
%% - During session reconnect, it returns the list of SRS that must be
%% replayed in order.
%%
%% ** Blocked streams
%%
%% For performance reasons we keep only one record of in-flight
%% messages per stream, and we don't want to overwrite these records
%% prematurely.
%%
%% ** Stream state machine
%%
%% During normal operation, state of each stream can be informally
%% described as a FSM with the following stream states:
%%
%% - *(P)ending*: stream belongs to a generation that is newer than
%% the one currently being processed.
%%
%% - *(R)eady*: stream is not blocked. Messages from such streams can
%% be enqueued directly to inflight.
%%
%% - *BQ1*, *BQ2* and *BQ12*: stream is blocked by un-acked QoS1, QoS2
%% or QoS1&2 messages respectively. Such streams are stored in
%% `#s.bq1' or `#s.bq2' buckets (or both).
%%
%% - *(U)ndead*: zombie streams that are kept for historical reasons
%% only. For example, streams for unsubcribed topics can linger in the
%% session state for a while until all queued messages are acked, as
%% well as streams that reached `end_of_stream'. This state is
%% implicit: scheduler doesn't actively track such streams. Undead
%% streams are ignored by the scheduler until the moment they can be
%% garbage-collected. So this is a terminal state. Even if the client
%% resubscribes, it will produce a new, totally separate set of SRS.
%%
%% *** State transitions
%%
%% New streams start in *Ready* or *Pending* state, depending on their
%% rank Y, from which they follow one of these paths:
%%
%%     ,--(`renew_streams') --> *P* -> ...
%%    /
%% *P* --(`renew_streams', advance generation) --> *R* -> ...
%%    \
%%     `--(`on_unsubscribe') --> *U*
%%
%% Upon receiving a `ds_sub_reply' for a *ready* stream, the session
%% assigns QoS and sequence numbers to the messages according to its
%% own logic, then enqueues the batch into the inflight. It
%% subsequently passes the updated SRS back to the scheduler, where it
%% can determine its block status:
%%
%%        ,--(`on_unsubscribe')--> *U*
%%       /
%%      /--(only QoS0 messages in the batch)--> *R* --> ...
%%     /
%% *R* --([QoS0] & QoS1 messages)--> *BQ1* --> ...
%%    \
%%     \--([QoS0] & QoS2 messages)--> *BQ2* --> ...
%%      \
%%       \--([QoS0] & QoS1 & QoS2)--> *BQ12* --> ...
%%        \
%%         `--([QoS0] & `end_of_stream')--> *U*
%%
%%
%% *BQ1* and *BQ2* are handled similarly. They transition to *Ready*
%% once session calls `?MODULE:on_seqno_release' for the corresponding
%% QoS track and sequence number equal to the SRS's last sequence
%% number for the track:
%%
%%                                                          .-> *R* --> ...
%%                                                         /
%% *BQX* --(`?MODULE:on_seqno_release(?QOS_X, LastSeqNoX)')
%%                                                         \
%%                                                          `-->*U*
%%
%% *BQ12* is handled like this:
%%
%%        .--(`on_seqno_release(?QOS_1, LastSeqNo1)')--> *BQ2* --> ...
%%       /
%% *BQ12*
%%       \
%%        `--(`on_seqno_release(?QOS_2, LastSeqNo2)')--> *BQ1* --> ...
%%
%%
-module(emqx_persistent_session_ds_stream_scheduler).

%% API:
-export([
    new/0,
    on_session_replay/2,
    on_unsubscribe/4,
    on_shared_stream_revoke/3,
    on_enqueue/4,
    on_seqno_release/3,
    find_replay_streams/1,
    is_fully_acked/2,
    is_active_unblocked/2,
    gc/1
]).

%% internal exports:
-export([]).

-ifdef(TEST).
-export([
    runtime_state_invariants/2,
    offline_state_invariants/2,
    assert_runtime_durable_subscription/3
]).
-endif.

-export_type([t/0, stream_key/0, srs/0, ret/0]).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("emqx_mqtt.hrl").
-include("session_internals.hrl").

-ifdef(TEST).
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type stream_key() :: {emqx_persistent_session_ds:subscription_id(), emqx_ds:stream()}.

-type ret() :: {[stream_key()], emqx_persistent_session_ds:session()}.

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

-type state() :: p | r | bq1 | bq2 | bq12 | u.

%%================================================================================
%% API functions
%%================================================================================

-spec new() -> t().
new() ->
    #s{
        bq1 = gb_trees:empty(),
        bq2 = gb_trees:empty()
    }.

-spec on_session_replay(emqx_persistent_session_ds_state:t(), t()) ->
    t().
on_session_replay(S, SchedS0) ->
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
        SchedS0,
        S
    ).

-spec find_active_streams(emqx_persistent_session_ds_state:t()) -> [stream_key()].
find_active_streams(S) ->
    emqx_persistent_session_ds_state:fold_streams(
        fun(Key, SRS, Acc) ->
            case SRS of
                #srs{unsubscribed = true} -> Acc;
                #srs{it_end = end_of_stream} -> Acc;
                #srs{} -> [Key | Acc]
            end
        end,
        [],
        S
    ).

%% @doc Find the streams that have unacked (in-flight) messages.
%% Return them in the order they were previously replayed.
-spec find_replay_streams(emqx_persistent_session_ds_state:t()) ->
    [{stream_key(), emqx_persistent_session_ds:stream_state()}].
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

-spec on_shared_stream_revoke(stream_key(), emqx_persistent_session_ds_state:t(), t()) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_shared_stream_revoke(StreamKey, S, SchedS) ->
    case emqx_persistent_session_ds_state:get_stream(StreamKey, S) of
        undefined ->
            {S, SchedS};
        SRS ->
            Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
            Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
            {unsubscribe_stream(Comm1, Comm2, StreamKey, SRS, S), SchedS}
    end.

-spec on_enqueue(
    _IsReplay :: boolean(), stream_key(), srs(), emqx_persistent_session_ds:session()
) ->
    ret().
on_enqueue(true, _Key, _SRS, Sess) ->
    {[], Sess};
on_enqueue(false, Key, SRS, Sess = #{s := S0, stream_scheduler_s := SchedS}) ->
    %% Is the stream blocked?
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S0),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S0),
    case derive_state(Comm1, Comm2, SRS) of
        r ->
            ?tp(?sessds_stream_state_trans, #{key => Key, to => r, from => r}),
            {[Key], Sess};
        bq1 ->
            {[], Sess#{stream_scheduler_s := to_BQ1(Key, SRS, SchedS)}};
        bq2 ->
            {[], Sess#{stream_scheduler_s := to_BQ2(Key, SRS, SchedS)}};
        bq12 ->
            {[], Sess#{stream_scheduler_s := to_BQ12(Key, SRS, SchedS)}};
        u ->
            %% Handle a special case: session just enqueued the last
            %% batch at the end of a stream, that contained only QoS0
            %% messages. Since no acks are expected for this batch, we
            %% should attempt to advance generation now:
            on_stream_unblock(Key, r, SRS, Sess)
    end.

-spec on_seqno_release(
    ?QOS_1 | ?QOS_2, emqx_persistent_session_ds:seqno(), emqx_persistent_session_ds:session()
) -> ret().
on_seqno_release(?QOS_1, ReleasedSeqNo, Sess) ->
    #{s := S, stream_scheduler_s := SchedS = #s{bq1 = BQ10}} = Sess,
    CommQoS2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    case bq_pop(ReleasedSeqNo, CommQoS2, BQ10) of
        true ->
            %% Still blocked:
            {[], Sess};
        {true, Key, BQ1} ->
            %% It was bq12:
            ?tp(?sessds_stream_state_trans, #{
                key => Key,
                to => bq2,
                from => bq12
            }),
            {[], Sess#{stream_scheduler_s := SchedS#s{bq1 = BQ1}}};
        {false, Key, BQ1} ->
            %% It was bq1:
            ?tp(?sessds_stream_state_trans, #{
                key => Key,
                to => r,
                from => bq1
            }),
            SRS = emqx_persistent_session_ds_state:get_stream(Key, S),
            on_stream_unblock(Key, bq1, SRS, Sess#{stream_scheduler_s := SchedS#s{bq1 = BQ1}})
    end;
on_seqno_release(?QOS_2, ReleasedSeqNo, Sess) ->
    #{s := S, stream_scheduler_s := SchedS = #s{bq2 = BQ20}} = Sess,
    CommQoS1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    case bq_pop(ReleasedSeqNo, CommQoS1, BQ20) of
        true ->
            %% Still blocked:
            {[], Sess};
        {true, Key, BQ2} ->
            %% It was bq12:
            ?tp(?sessds_stream_state_trans, #{
                key => Key,
                to => bq1,
                from => bq12
            }),
            {[], Sess#{stream_scheduler_s := SchedS#s{bq2 = BQ2}}};
        {false, Key, BQ2} ->
            %% It was bq2:
            ?tp(?sessds_stream_state_trans, #{
                key => Key,
                to => r,
                from => bq2
            }),
            SRS = emqx_persistent_session_ds_state:get_stream(Key, S),
            on_stream_unblock(Key, bq1, SRS, Sess#{stream_scheduler_s := SchedS#s{bq2 = BQ2}})
    end.

-spec on_stream_unblock(stream_key(), state(), srs(), emqx_persistent_session_ds:session()) ->
    {[stream_key(), ...], emqx_persistent_session_ds:session()}.
on_stream_unblock(Key, PrevState, #srs{unsubscribed = true}, Sess) ->
    ?tp(?sessds_stream_state_trans, #{key => Key, to => u, from => PrevState, unsubcribed => true}),
    {[Key], Sess};
on_stream_unblock(
    Key = {SubId, Stream},
    PrevState,
    #srs{it_end = end_of_stream, rank_x = Shard, rank_y = Generation},
    Sess0
) ->
    %% We've reached end of the stream. We might advance generation
    %% now:
    #{s := S0, stream_scheduler_s := SchedS0, dscli := CS0} = Sess0,
    ?tp(?sessds_stream_state_trans, #{key => Key, to => u, from => PrevState, eos => true}),
    %% FIXME: currently DS subscription will leak when this function
    %% is called. Fix it in ds_client
    {CS, Sess} = emqx_ds_client:complete_stream(CS0, SubId, {Shard, Generation}, Stream, Sess0),
    %% TODO: Reporting this key as unblocked for compatibility with
    %% shared_sub. This should not be needed.
    {[Key], Sess#{dscli := CS}};
on_stream_unblock(Key, PrevState, _SRS, Sess) ->
    ?tp(?sessds_stream_state_trans, #{key => Key, to => r, from => PrevState}),
    {[Key], Sess}.

%% @doc Batch operation to clean up historical streams.
%%
%% 1. Remove fully replayed streams for past generations.
%%
%% 2. Remove unblocked streams for deleted subscriptions.
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

%% @doc Makes stream scheduler forget about a subscription.
%% This operation is idempotent.
%%
%% Side effects:
%%
%% 1. Unsubscribe from the new stream events for the topic-filter.
%%
%% 2. Set `unsubscribed' flag on SRS records that belong to the subscription
%%
%% 3. Remove streams from the buckets
-spec on_unsubscribe(
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds:subscription_id(),
    emqx_persistent_session_ds_state:t(),
    t()
) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_unsubscribe(_Topic, SubId, S0, SchedS) ->
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
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S0),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S0),
    S = emqx_persistent_session_ds_state:fold_streams(
        SubId,
        fun(Stream, SRS, SAcc) ->
            unsubscribe_stream(Comm1, Comm2, {SubId, Stream}, SRS, SAcc)
        end,
        S0,
        S0
    ),
    {S, SchedS}.

-spec is_fully_acked(
    emqx_persistent_session_ds:stream_state(), emqx_persistent_session_ds_state:t()
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
unsubscribe_stream(Comm1, Comm2, Key, SRS0 = #srs{}, S) ->
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
) -> state().
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

-ifdef(TEST).

-spec runtime_state_invariants(
    emqx_persistent_session_ds_fuzzer:model_state(), #{s := map(), scheduler_state_s := t()}
) ->
    boolean().
runtime_state_invariants(_ModelState, #{s := S, stream_scheduler_s := SchedS}) ->
    invariant_active_streams_y_ranks(S) and
        invariant_ds_subscriptions(S, SchedS).

-spec offline_state_invariants(
    emqx_persistent_session_ds_fuzzer:model_state(), #{s := map()}
) ->
    boolean().
offline_state_invariants(_ModelState, #{s := S}) ->
    invariant_active_streams_y_ranks(S).

%% Assert that durable topic subscription is represented in the scheduler's state:
assert_runtime_durable_subscription(Topic, _SubOpts, #{stream_scheduler_s := SchedS}) ->
    #s{new_stream_subs = Watches, sub_metadata = SubStates} = SchedS,
    case SubStates of
        #{Topic := _} ->
            Acc1 = [];
        _ ->
            Acc1 = [
                "There's a 1:1 relationship between durable subscriptions and "
                "the scheduler's new stream watches"
            ]
    end,
    case [T || {_, T} <- maps:to_list(Watches), T =:= Topic] of
        [Topic] ->
            Acc2 = [];
        [] ->
            Acc2 = [
                "There's a 1:1 relationship between durable subscriptions and "
                "the values of scheduler's watch reference => topic lookup table"
            ]
    end,
    Acc1 ++ Acc2.

%% Verify that each active stream has a DS subscription:
invariant_ds_subscriptions(#{streams := Streams}, #s{ds_subs = DSSubs}) ->
    ActiveStreams = maps:fold(
        fun(StreamId, #srs{it_end = ItEnd, unsubscribed = IsUnsub}, Acc) ->
            case (ItEnd =/= end_of_stream) and (not IsUnsub) of
                true ->
                    [StreamId | Acc];
                false ->
                    Acc
            end
        end,
        [],
        Streams
    ),
    ?defer_assert(
        ?assertEqual(
            lists:sort(ActiveStreams),
            lists:sort([K || #ds_sub{stream_key = K} <- maps:values(DSSubs)]),
            "There's a 1:1 relationship between active streams and DS subscriptions"
        )
    ),
    %% maps:foreach(
    %%     fun(StreamId, #ds_sub{handle = Handle}) ->
    %%         ?defer_assert(
    %%             ?assertMatch(
    %%                 #{},
    %%                 emqx_ds:subscription_info(?PERSISTENT_MESSAGE_DB, Handle),
    %%                 #{msg => "Every DS subscription is active", id => StreamId}
    %%             )
    %%         )
    %%     end,
    %%     DSSubs
    %% ),
    true.

%% Verify that all active streams belong to the same generation:
invariant_active_streams_y_ranks(#{streams := StreamMap}) ->
    %% Derive active ranks:
    ActiveRanks = maps:fold(
        fun({SubId, _Stream}, #srs{rank_x = X, rank_y = Y, unsubscribed = U, it_end = ItEnd}, Acc) ->
            case (not U) and (ItEnd =/= end_of_stream) of
                true ->
                    %% This is an active stream, add its rank to the list:
                    map_cons({SubId, X}, Y, Acc);
                false ->
                    Acc
            end
        end,
        #{},
        StreamMap
    ),
    %% Verify that only one generation is active at a time:
    maps:foreach(
        fun({SubId, X}, Ys) ->
            Comment = #{
                msg => "For each subscription and x-rank there should be only one active y-rank",
                subid => SubId,
                x => X
            },
            ?defer_assert(?assertMatch([_], Ys, Comment))
        end,
        ActiveRanks
    ),
    true.

map_cons(Key, NewElem, Map) ->
    maps:update_with(Key, fun(L) -> [NewElem | L] end, [NewElem], Map).

-endif.
