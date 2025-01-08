%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Stream scheduler is a helper module used by durable sessions
%% to track states of the DS streams (Stream Replay States, or SRS for
%% short). It has two main duties:
%%
%% - During normal operation, it polls DS iterators that are eligible
%% for poll.
%%
%% - During session reconnect, it returns the list of SRS that must be
%% replayed in order.
%%
%% ** Blocked streams
%%
%% For performance reasons we keep only one record of in-flight
%% messages per stream, and we don't want to overwrite these records
%% prematurely. So scheduler makes sure that streams that have
%% un-acked QoS1 or QoS2 messages are not polled.
%%
%% ** Stream state machine
%%
%% During normal operation, state of each iterator can be described as
%% a FSM. Implementation detail: unconventially, iterators' states are
%% tracked implicitly, by moving SRS ID between different buckets.
%% This facilitates faster processing of iterators that have a certain
%% state.
%%
%% There are the following stream replay states:
%%
%% - *(R)eady*: stream iterator can be polled. Ready SRS are stored in
%% `#s.ready' bucket.
%%
%% - *(P)ending*: poll request for the iterator has been sent to DS,
%% and we're awaiting the response. Such iterators are stored in
%% `#s.pending' bucket.
%%
%% - *(S)erved*: poll reply has been received, and ownership over SRS
%% has been handed over to the parent session. This state is implicit:
%% *served* streams are not tracked by the scheduler. It's assumed
%% that the session will process the batch and immediately hand SRS
%% back via `on_enqueue' call.
%%
%% - *BQ1*, *BQ2* and *BQ12*: these three states correspond to the
%% situations when stream cannot be polled, because it is blocked by
%% un-acked QoS1, QoS2 or QoS1&2 messages respectively. Such streams
%% are stored in `#s.bq1' or `#s.bq2' buckets (or both).
%%
%% - *(U)ndead*: zombie streams that are kept for historical reasons
%% only. For example, streams for unsubcribed topics can linger in the
%% session state for a while until all queued messages are acked, as
%% well as streams that reached `end_of_stream'. This state is
%% implicit: undead streams are simply removed from all buckets.
%% Undead streams are ignored by the scheduler until the moment they
%% can be garbage-collected. So this is a terminal state. Even if the
%% client resubscribes, it will produce a new, totally separate set of
%% SRS.
%%
%% *** State transitions
%%
%% New streams start in the *Ready* state, from which they follow one
%% of these paths:
%%
%%      .--(`?MODULE:poll')--> *P* --(Poll reply)--> *S* --> ...
%%     /
%% *R* --(`?MODULE:on_unsubscribe')--> *U*
%%  ^  \
%%  |   `--(`?MODULE:poll')--->---.
%%  |                              \
%%  \        Idle longpoll loop    *P*
%%   \                             ,
%%    `---<--(Poll timeout)---<---'
%%
%% *Served* streams are returned to the parent session, which assigns
%% QoS and sequence numbers to the batch messages according to its own
%% logic, and enqueues batch to the buffer. Then it returns the
%% updated SRS back to the scheduler, where it can undergo the
%% following transitions:
%%
%%          .--(buffer is full)--> *R* --> ...
%%         /
%%        /--(`on_unsubscribe')--> *U*
%%       /
%%      /--(only QoS0 messages in the batch)--> *R* --> ...
%%     /
%% *S* --([QoS0] & QoS1 messages)--> *BQ1* --> ...
%%    \
%%     \--([QoS0] & QoS2 messages)--> *BQ2* --> ...
%%      \
%%       \--([QoS0] & QoS1 & QoS2)--> *BQ12* --> ...
%%        \
%%         `--(`end_of_stream')--> *U*
%%
%%
%% *BQ1* and *BQ2* are handled similarly. They transition to *Ready*
%% once session calls `?MODULE:on_seqno_release' for the corresponding
%% QoS track and sequence number equal to the SRS's last sequence
%% number for the track:
%%
%% *BQX* --(`?MODULE:on_seqno_release(?QOS_X, LastSeqNoX)')--> *R* --> ...
%%      \
%%       `--(`on_unsubscribe')--> *U*
%%
%% *BQ12* is handled like this:
%%
%%        .--(`on_seqno_release(?QOS_1, LastSeqNo1)')--> *BQ2* --> ...
%%       /
%% *BQ12*--(`on_unsubscribe')--> *U*
%%       \
%%        `--(`on_seqno_release(?QOS_2, LastSeqNo2)')--> *BQ1* --> ...
%%
%%
-module(emqx_persistent_session_ds_stream_scheduler).

%% API:
-export([
    init/1,
    poll/3,
    on_ds_reply/3,
    on_enqueue/5,
    on_seqno_release/4,
    find_replay_streams/1,
    is_fully_acked/2
]).

-export([
    renew_streams/2,
    renew_streams/4,
    on_unsubscribe/3,
    on_unsubscribe/4
]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([t/0, stream_key/0, srs/0]).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("emqx_mqtt.hrl").
-include("session_internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type stream_key() :: {emqx_persistent_session_ds:subscription_id(), _StreamId}.

-type srs() :: #srs{}.

%%%%%% Pending poll for the iterator:
-record(pending_poll, {
    %% Poll reference:
    ref :: reference(),
    %% Iterator at the beginning of poll:
    it_begin :: emqx_ds:iterator()
}).

-type pending() :: #pending_poll{}.

-record(block, {
    id :: stream_key(),
    last_seqno_qos1 :: emqx_persistent_session_ds:seqno(),
    last_seqno_qos2 :: emqx_persistent_session_ds:seqno()
}).

-type block() :: #block{}.

-type blocklist() :: gb_trees:tree(emqx_persistent_session_ds:seqno(), block()).

-type ready() :: [stream_key()].

-record(s, {
    %% Buckets:
    ready :: ready(),
    pending = #{} :: #{stream_key() => #pending_poll{}},
    bq1 :: blocklist(),
    bq2 :: blocklist()
}).

-opaque t() :: #s{}.

-type state() :: r | p | s | bq1 | bq2 | bq12 | u.

%%================================================================================
%% API functions
%%================================================================================

-spec init(emqx_persistent_session_ds_state:t()) -> t().
init(S) ->
    SchedS0 = #s{
        ready = empty_ready(),
        bq1 = gb_trees:empty(),
        bq2 = gb_trees:empty()
    },
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    %% Restore stream states.
    %%
    %% Note: these states are NOT used during replay.
    emqx_persistent_session_ds_state:fold_streams(
        fun(Key, Srs, Acc) ->
            ?tp(sessds_stream_state_trans, #{key => Key, to => '$restore'}),
            case derive_state(Comm1, Comm2, Srs) of
                r -> to_RU(Key, Srs, Acc);
                u -> to_U(Key, Srs, Acc);
                bq1 -> to_BQ1(Key, Srs, Acc);
                bq2 -> to_BQ2(Key, Srs, Acc);
                bq12 -> to_BQ12(Key, Srs, Acc)
            end
        end,
        SchedS0,
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

%% @doc Send poll request to DS for all iterators that are currently
%% in ready state.
-spec poll(emqx_ds:poll_opts(), t(), emqx_persistent_session_ds_state:t()) -> t().
poll(PollOpts0, SchedS0 = #s{ready = Ready}, S) ->
    %% Create an alias for replies:
    Ref = alias([explicit_unalias]),
    %% Scan ready streams and create poll requests:
    {Iterators, SchedS} = fold_ready(
        fun(StreamKey, {AccIt, SchedS1}) ->
            SRS = emqx_persistent_session_ds_state:get_stream(StreamKey, S),
            It = {StreamKey, SRS#srs.it_end},
            Pending = #pending_poll{ref = Ref, it_begin = SRS#srs.it_begin},
            {
                [It | AccIt],
                to_P(StreamKey, Pending, SchedS1)
            }
        end,
        {[], SchedS0},
        Ready
    ),
    case Iterators of
        [] ->
            %% Nothing to poll:
            unalias(Ref),
            ok;
        _ ->
            %% Send poll request:
            PollOpts = PollOpts0#{reply_to => Ref},
            {ok, Ref} = emqx_ds:poll(?PERSISTENT_MESSAGE_DB, Iterators, PollOpts),
            ok
    end,
    %% Clean ready bucket at once, since we poll all ready streams at once:
    SchedS#s{ready = empty_ready()}.

on_ds_reply(#poll_reply{ref = Ref, payload = poll_timeout}, S, SchedS0 = #s{pending = P0}) ->
    %% Poll request has timed out. All pending streams that match poll
    %% reference can be moved to R state:
    ?SLOG(debug, #{msg => sess_poll_timeout, ref => Ref}),
    unalias(Ref),
    SchedS = maps:fold(
        fun(Key, #pending_poll{ref = R}, SchedS1 = #s{pending = P}) ->
            case R =:= Ref of
                true ->
                    SchedS2 = SchedS1#s{pending = maps:remove(Key, P)},
                    case emqx_persistent_session_ds_state:get_stream(Key, S) of
                        undefined ->
                            SchedS2;
                        Srs ->
                            to_RU(Key, Srs, SchedS2)
                    end;
                false ->
                    SchedS1
            end
        end,
        SchedS0,
        P0
    ),
    {undefined, SchedS};
on_ds_reply(
    #poll_reply{ref = Ref, userdata = StreamKey, payload = Payload},
    _S,
    SchedS0 = #s{pending = Pending0}
) ->
    case maps:take(StreamKey, Pending0) of
        {#pending_poll{ref = Ref, it_begin = ItBegin}, Pending} ->
            ?tp(debug, sess_poll_reply, #{ref => Ref, stream_key => StreamKey}),
            SchedS = SchedS0#s{pending = Pending},
            {{StreamKey, ItBegin, Payload}, to_S(StreamKey, SchedS)};
        _ ->
            ?SLOG(
                info,
                #{
                    msg => "sessds_unexpected_msg",
                    userdata => StreamKey,
                    ref => Ref
                }
            ),
            {undefined, SchedS0}
    end.

on_enqueue(true, _Key, _Srs, _S, SchedS) ->
    {[], SchedS};
on_enqueue(false, Key, Srs, S, SchedS) ->
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    case derive_state(Comm1, Comm2, Srs) of
        r ->
            {[Key], to_RU(Key, Srs, SchedS)};
        u ->
            {[Key], to_U(Key, Srs, SchedS)};
        bq1 ->
            {[], to_BQ1(Key, Srs, SchedS)};
        bq2 ->
            {[], to_BQ2(Key, Srs, SchedS)};
        bq12 ->
            {[], to_BQ12(Key, Srs, SchedS)}
    end.

on_seqno_release(?QOS_1, SnQ1, S, SchedS0 = #s{bq1 = PrimaryTab0, bq2 = SecondaryTab}) ->
    case check_block_status(PrimaryTab0, SecondaryTab, SnQ1, #block.last_seqno_qos2) of
        false ->
            %% This seqno doesn't unlock anything:
            {[], SchedS0};
        {false, Key, PrimaryTab} ->
            %% It was BQ1:
            Srs = emqx_persistent_session_ds_state:get_stream(Key, S),
            {[Key], to_RU(Key, Srs, SchedS0#s{bq1 = PrimaryTab})};
        {true, Key, PrimaryTab} ->
            %% It was BQ12:
            ?tp(sessds_stream_state_trans, #{
                key => Key,
                to => bq2
            }),
            {[], SchedS0#s{bq1 = PrimaryTab}}
    end;
on_seqno_release(?QOS_2, SnQ2, S, SchedS0 = #s{bq2 = PrimaryTab0, bq1 = SecondaryTab}) ->
    case check_block_status(PrimaryTab0, SecondaryTab, SnQ2, #block.last_seqno_qos1) of
        false ->
            %% This seqno doesn't unlock anything:
            {[], SchedS0};
        {false, Key, PrimaryTab} ->
            %% It was BQ2:
            Srs = emqx_persistent_session_ds_state:get_stream(Key, S),
            {[Key], to_RU(Key, Srs, SchedS0#s{bq2 = PrimaryTab})};
        {true, Key, PrimaryTab} ->
            %% It was BQ12:
            ?tp(sessds_stream_state_trans, #{
                key => Key,
                to => bq1
            }),
            {[], SchedS0#s{bq2 = PrimaryTab}}
    end.

check_block_status(PrimaryTab0, SecondaryTab, PrimaryKey, SecondaryIdx) ->
    case gb_trees:take_any(PrimaryKey, PrimaryTab0) of
        error ->
            false;
        {Block = #block{id = StreamKey}, PrimaryTab} ->
            StillBlocked = gb_trees:is_defined(element(SecondaryIdx, Block), SecondaryTab),
            {StillBlocked, StreamKey, PrimaryTab}
    end.

%% @doc This function makes the session aware of the new streams.
%%
%% It has the following properties:
%%
%% 1. For each RankX, it keeps only the streams with the same RankY.
%%
%% 2. For each RankX, it never advances RankY until _all_ streams with
%% the same RankX are replayed.
%%
%% 3. Once all streams with the given rank are replayed, it advances
%% the RankY to the smallest known RankY that is greater than replayed
%% RankY.
%%
%% 4. If the RankX has never been replayed, it selects the streams
%% with the smallest RankY.
%%
%% This way, messages from the same topic/shard are never reordered.
-spec renew_streams(
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds_subs:subscription(),
    emqx_persistent_session_ds_state:t(),
    t()
) ->
    {emqx_persistent_session_ds_state:t(), t()}.
renew_streams(
    TopicFilterBin, #{start_time := StartTime, id := SubId, current_state := SStateId}, S0, SchedS0
) ->
    TopicFilter = emqx_topic:words(TopicFilterBin),
    S1 = remove_unsubscribed_streams(S0),
    S2 = remove_fully_replayed_streams(S1),
    S3 = update_stream_subscription_state_ids(S2),
    Streams = select_streams(
        SubId,
        emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, StartTime),
        S3
    ),
    lists:foldl(
        fun(StreamWithRank, Acc) ->
            ensure_iterator(TopicFilter, StartTime, SubId, SStateId, StreamWithRank, Acc)
        end,
        {S3, SchedS0},
        Streams
    ).

-spec renew_streams(emqx_persistent_session_ds_state:t(), t()) ->
    {emqx_persistent_session_ds_state:t(), t()}.
renew_streams(S0, SchedS0) ->
    %% For shared subscriptions, the streams are populated by
    %% `emqx_persistent_session_ds_shared_subs`.
    emqx_persistent_session_ds_subs:fold_private_subscriptions(
        fun(Key, Sub, {S1, SchedS1}) ->
            renew_streams(Key, Sub, S1, SchedS1)
        end,
        {S0, SchedS0},
        S0
    ).

-spec on_unsubscribe(
    emqx_persistent_session_ds:subscription_id(), emqx_persistent_session_ds_state:t(), t()
) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_unsubscribe(SubId, S0, SchedS0) ->
    %% NOTE: this function only marks the streams for deletion,
    %% instead of outright deleting them.
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
    %% _new_ batches from it. Actual deletion is done by
    %% `renew_streams', when it detects that all in-flight messages
    %% from the stream have been acked by the client.
    emqx_persistent_session_ds_state:fold_streams(
        fun(Key, Srs0, {S1, SchedS1}) ->
            case Key of
                {SubId, _Stream} ->
                    %% This stream belongs to a deleted subscription.
                    %% Mark for deletion:
                    unsubscribe_stream(Key, Srs0, S1, SchedS1);
                _ ->
                    {S1, SchedS1}
            end
        end,
        {S0, SchedS0},
        S0
    ).

on_unsubscribe(SubId, Stream, S, SchedS) ->
    Key = {SubId, Stream},
    Srs = emqx_persistent_session_ds_state:get_stream(Key, S),
    unsubscribe_stream(Key, Srs, S, SchedS).

unsubscribe_stream(Key, Srs0 = #srs{}, S, SchedS) ->
    Srs = Srs0#srs{unsubscribed = true},
    {
        emqx_persistent_session_ds_state:put_stream(Key, Srs, S),
        to_U(Key, Srs, SchedS)
    };
unsubscribe_stream(_Key, undefined, S, SchedS) ->
    {S, SchedS}.

-spec is_fully_acked(
    emqx_persistent_session_ds:stream_state(), emqx_persistent_session_ds_state:t()
) -> boolean().
is_fully_acked(Srs, S) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    is_fully_acked(CommQos1, CommQos2, Srs).

%%================================================================================
%% Internal functions
%%================================================================================

%%--------------------------------------------------------------------------------
%% SRS FSM
%%--------------------------------------------------------------------------------

-spec derive_state(
    emqx_persistent_session_ds:seqno(), emqx_persistent_session_ds:seqno(), srs()
) -> state().
derive_state(_, _, #srs{unsubscribed = true}) ->
    u;
derive_state(Comm1, Comm2, SRS) ->
    case {is_track_acked(?QOS_1, Comm1, SRS), is_track_acked(?QOS_2, Comm2, SRS)} of
        {true, true} -> r;
        {false, true} -> bq1;
        {true, false} -> bq2;
        {false, false} -> bq12
    end.

%% Note: `to_State' functions must be called from a correct state.
%% They are NOT idempotent, and they don't do full cleanup.

%% Transfer the stream either to R state if it's pollable or to U
%% state if it's not.
-spec to_RU(stream_key(), srs(), t()) -> t().
to_RU(_Key, #srs{it_end = end_of_stream}, S) ->
    %% Just don't add to ready
    S;
to_RU(Key, Srs = #srs{unsubscribed = true}, S) ->
    to_U(Key, Srs, S);
to_RU(Key, _Srs, S = #s{ready = R}) ->
    ?tp(sessds_stream_state_trans, #{
        key => Key,
        to => r
    }),
    S#s{ready = push_to_ready(Key, R)}.

-spec to_P(stream_key(), pending(), t()) -> t().
to_P(Key, Pending, S = #s{pending = P}) ->
    ?tp(sessds_stream_state_trans, #{
        key => Key,
        to => p
    }),
    S#s{pending = P#{Key => Pending}}.

-spec to_BQ1(stream_key(), srs(), t()) -> t().
to_BQ1(Key, SRS, S = #s{bq1 = BQ1}) ->
    ?tp(sessds_stream_state_trans, #{
        key => Key,
        to => bq1
    }),
    Block = #block{last_seqno_qos1 = SN1} = block_of_srs(Key, SRS),
    S#s{bq1 = gb_trees:insert(SN1, Block, BQ1)}.

-spec to_BQ2(stream_key(), srs(), t()) -> t().
to_BQ2(Key, SRS, S = #s{bq2 = BQ2}) ->
    ?tp(sessds_stream_state_trans, #{
        key => Key,
        to => bq1
    }),
    Block = #block{last_seqno_qos2 = SN2} = block_of_srs(Key, SRS),
    S#s{bq2 = gb_trees:insert(SN2, Block, BQ2)}.

-spec to_BQ12(stream_key(), srs(), t()) -> t().
to_BQ12(Key, SRS, S = #s{bq1 = BQ1, bq2 = BQ2}) ->
    ?tp(sessds_stream_state_trans, #{
        key => Key,
        to => bq12
    }),
    Block = #block{last_seqno_qos1 = SN1, last_seqno_qos2 = SN2} = block_of_srs(Key, SRS),
    S#s{bq1 = gb_trees:insert(SN1, Block, BQ1), bq2 = gb_trees:insert(SN2, Block, BQ2)}.

-spec to_U(stream_key(), srs(), t()) -> t().
to_U(
    Key,
    #srs{},
    S = #s{ready = R, pending = P}
) ->
    ?tp(sessds_stream_state_trans, #{
        key => Key,
        to => u
    }),
    S#s{
        ready = del_ready(Key, R),
        pending = maps:remove(Key, P)
        %% We do not remove the stream from the blocked queues.
        %% It will be removed on seqno release.
    }.

-spec to_S(stream_key(), t()) -> t().
to_S(Key, S) ->
    ?tp(sessds_stream_state_trans, #{
        key => Key,
        to => s
    }),
    S.

-spec block_of_srs(stream_key(), srs()) -> block().
block_of_srs(Key, #srs{last_seqno_qos1 = SN1, last_seqno_qos2 = SN2}) ->
    #block{id = Key, last_seqno_qos1 = SN1, last_seqno_qos2 = SN2}.

%%--------------------------------------------------------------------------------
%% Misc.
%%--------------------------------------------------------------------------------

fold_ready(Fun, Acc, Ready) ->
    lists:foldl(Fun, Acc, Ready).

empty_ready() ->
    [].

push_to_ready(K, Ready) ->
    [K | Ready].

del_ready(K, Ready) ->
    Ready -- [K].

ensure_iterator(TopicFilter, StartTime, SubId, SStateId, {{RankX, RankY}, Stream}, {S, SchedS}) ->
    Key = {SubId, Stream},
    case emqx_persistent_session_ds_state:get_stream(Key, S) of
        undefined ->
            %% This is a newly discovered stream. Create an iterator
            %% for it, and mark it as ready:
            case emqx_ds:make_iterator(?PERSISTENT_MESSAGE_DB, Stream, TopicFilter, StartTime) of
                {ok, Iterator} ->
                    NewStreamState = #srs{
                        rank_x = RankX,
                        rank_y = RankY,
                        it_begin = Iterator,
                        it_end = Iterator,
                        sub_state_id = SStateId
                    },
                    {
                        emqx_persistent_session_ds_state:put_stream(Key, NewStreamState, S),
                        to_RU(Key, NewStreamState, SchedS)
                    };
                {error, Class, Reason} ->
                    ?SLOG(info, #{
                        msg => "failed_to_initialize_stream_iterator",
                        stream => Stream,
                        class => Class,
                        reason => Reason
                    }),
                    {S, SchedS}
            end;
        #srs{} ->
            {S, SchedS}
    end.

select_streams(SubId, Streams0, S) ->
    TopicStreamGroups = maps:groups_from_list(fun({{X, _}, _}) -> X end, Streams0),
    maps:fold(
        fun(RankX, Streams, Acc) ->
            select_streams(SubId, RankX, Streams, S) ++ Acc
        end,
        [],
        TopicStreamGroups
    ).

select_streams(SubId, RankX, Streams0, S) ->
    %% 1. Find the streams with the rank Y greater than the recorded one:
    Streams1 =
        case emqx_persistent_session_ds_state:get_rank({SubId, RankX}, S) of
            undefined ->
                Streams0;
            ReplayedY ->
                [I || I = {{_, Y}, _} <- Streams0, Y > ReplayedY]
        end,
    %% 2. Sort streams by rank Y:
    Streams = lists:sort(
        fun({{_, Y1}, _}, {{_, Y2}, _}) ->
            Y1 =< Y2
        end,
        Streams1
    ),
    %% 3. Select streams with the least rank Y:
    case Streams of
        [] ->
            [];
        [{{_, MinRankY}, _} | _] ->
            lists:takewhile(fun({{_, Y}, _}) -> Y =:= MinRankY end, Streams)
    end.

%% @doc Remove fully acked streams for the deleted subscriptions.
-spec remove_unsubscribed_streams(emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds_state:t().
remove_unsubscribed_streams(S0) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S0),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S0),
    emqx_persistent_session_ds_state:fold_streams(
        fun(Key, ReplayState, S1) ->
            case
                ReplayState#srs.unsubscribed andalso is_fully_acked(CommQos1, CommQos2, ReplayState)
            of
                true ->
                    emqx_persistent_session_ds_state:del_stream(Key, S1);
                false ->
                    S1
            end
        end,
        S0,
        S0
    ).

%% @doc Advance RankY for each RankX that doesn't have any unreplayed
%% streams.
%%
%% Drop streams with the fully replayed rank. This function relies on
%% the fact that all streams with the same RankX have also the same
%% RankY.
-spec remove_fully_replayed_streams(emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds_state:t().
remove_fully_replayed_streams(S0) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S0),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S0),
    %% 1. For each subscription, find the X ranks that were fully replayed:
    Groups = emqx_persistent_session_ds_state:fold_streams(
        fun({SubId, _Stream}, StreamState = #srs{rank_x = RankX, rank_y = RankY}, Acc) ->
            Key = {SubId, RankX},
            case {is_fully_replayed(CommQos1, CommQos2, StreamState), Acc} of
                {_, #{Key := false}} ->
                    Acc;
                {true, #{Key := {true, RankY}}} ->
                    Acc;
                {true, #{Key := {true, _RankYOther}}} ->
                    %% assert, should never happen
                    error(multiple_rank_y_for_rank_x);
                {true, #{}} ->
                    Acc#{Key => {true, RankY}};
                {false, #{}} ->
                    Acc#{Key => false}
            end
        end,
        #{},
        S0
    ),
    %% 2. Advance rank y for each fully replayed set of streams:
    S1 = maps:fold(
        fun
            (Key, {true, RankY}, Acc) ->
                emqx_persistent_session_ds_state:put_rank(Key, RankY, Acc);
            (_, _, Acc) ->
                Acc
        end,
        S0,
        Groups
    ),
    %% 3. Remove the fully replayed streams:
    emqx_persistent_session_ds_state:fold_streams(
        fun(Key = {SubId, _Stream}, #srs{rank_x = RankX, rank_y = RankY}, Acc) ->
            case emqx_persistent_session_ds_state:get_rank({SubId, RankX}, Acc) of
                undefined ->
                    Acc;
                MinRankY when RankY =< MinRankY ->
                    ?SLOG(debug, #{
                        msg => del_fully_replayed_stream,
                        key => Key,
                        rank => {RankX, RankY},
                        min => MinRankY
                    }),
                    emqx_persistent_session_ds_state:del_stream(Key, Acc);
                _ ->
                    Acc
            end
        end,
        S1,
        S1
    ).

%% @doc Update subscription state IDs for all streams that don't have unacked messages
-spec update_stream_subscription_state_ids(emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds_state:t().
update_stream_subscription_state_ids(S0) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S0),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S0),
    %% Find the latest state IDs for each subscription:
    LastSStateIds = emqx_persistent_session_ds_state:fold_subscriptions(
        fun(_, #{id := SubId, current_state := SStateId}, Acc) ->
            Acc#{SubId => SStateId}
        end,
        #{},
        S0
    ),
    %% Update subscription state IDs for fully acked streams:
    emqx_persistent_session_ds_state:fold_streams(
        fun
            (_, #srs{unsubscribed = true}, S) ->
                S;
            (Key = {SubId, _Stream}, SRS0, S) ->
                case is_fully_acked(CommQos1, CommQos2, SRS0) of
                    true ->
                        SRS = SRS0#srs{sub_state_id = maps:get(SubId, LastSStateIds)},
                        emqx_persistent_session_ds_state:put_stream(Key, SRS, S);
                    false ->
                        S
                end
        end,
        S0,
        S0
    ).

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

is_fully_replayed(Comm1, Comm2, S = #srs{it_end = It}) ->
    It =:= end_of_stream andalso is_fully_acked(Comm1, Comm2, S).

is_fully_acked(Comm1, Comm2, SRS) ->
    is_track_acked(?QOS_1, Comm1, SRS) andalso
        is_track_acked(?QOS_2, Comm2, SRS).

is_track_acked(?QOS_1, Committed, #srs{first_seqno_qos1 = First, last_seqno_qos1 = Last}) ->
    First =:= Last orelse Committed >= Last;
is_track_acked(?QOS_2, Committed, #srs{first_seqno_qos2 = First, last_seqno_qos2 = Last}) ->
    First =:= Last orelse Committed >= Last.
