%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_leader).

-include("emqx_ds_shared_sub_proto.hrl").
-include("emqx_ds_shared_sub_config.hrl").
-include("emqx_ds_shared_sub_format.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_persistent_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    become/2
]).

-behaviour(gen_server).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type share_topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type group_id() :: share_topic_filter().
-type borrower_id() :: emqx_ds_shared_sub_proto:borrower_id().

-type options() :: #{
    share_topic_filter := share_topic_filter()
}.

%% Stream statuses. Stream with states other than `stream_granted` are
%% considered "transient".

%% Stream is being assigned to a borrower, a borrower should confirm the assignment
%% by sending progress or reject the assignment.
-define(stream_granting, stream_granting).
%% Stream is assigned to a borrower, the borrower confirmed the assignment.
-define(stream_granted, stream_granted).
%% Stream is being revoked from a borrower, a borrower should send us final progress
%% for the stream.
-define(stream_revoking, stream_revoking).
%% Stream is revoked from a borrower, the borrower sent us final progress.
%% We wait till the borrower confirms that it has cleaned the data.
-define(stream_revoked, stream_revoked).

-type stream_status() :: ?stream_granting | ?stream_granted | ?stream_revoking | ?stream_revoked.

-type stream_ownership() :: #{
    status := stream_status(),
    borrower_id := borrower_id()
}.

-type borrower_state() :: #{
    validity_deadline := integer()
}.

%% Some data should be persisted
-type st() :: #{
    %%
    %% Persistent data
    %%
    group_id := group_id(),
    topic := emqx_types:topic(),
    %% Implement some stats to assign evenly?
    store := emqx_ds_shared_sub_store:t(),

    %%
    %% Ephemeral data, should not be persisted
    %%
    borrowers := #{
        emqx_ds_shared_sub_proto:borrower_id() => borrower_state()
    },
    stream_owners := #{
        emqx_ds:stream() => stream_ownership()
    }
}.

-export_type([
    options/0,
    st/0
]).

%% Events

-record(renew_streams, {}).
-record(periodical_actions_timeout, {}).
-record(renew_leader_claim, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

become(ShareTopicFilter, Claim) ->
    St0 = init_state(ShareTopicFilter),
    St1 = attach_claim(Claim, St0),
    ok = init_claim_renewal(St1),
    gen_server:enter_loop(?MODULE, [], St1).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(_Args) ->
    %% NOTE: Currently, the only entrypoint is `become/2` that calls `enter_loop/5`.
    {error, noimpl}.

init_state(#share{topic = Topic} = ShareTopicFilter) ->
    StoreID = emqx_ds_shared_sub_store:mk_id(ShareTopicFilter),
    case emqx_ds_shared_sub_store:open(StoreID) of
        {ok, Store} ->
            ?tp(debug, dssub_store_open, #{topic => ShareTopicFilter, store => Store}),
            ok = send_after(0, #renew_streams{}),
            ok = send_after(leader_periodical_actions_interval, #periodical_actions_timeout{}),
            #{
                group_id => ShareTopicFilter,
                topic => Topic,
                store => Store,
                stream_owners => #{},
                borrowers => #{}
            };
        false ->
            %% NOTE: No leader store -> no subscription
            ?tp(warning, dssub_store_notfound, #{topic => ShareTopicFilter}),
            exit(shared_subscription_not_declared)
    end.

attach_claim(Claim, St) ->
    St#{leader_claim => Claim}.

init_claim_renewal(_St = #{leader_claim := Claim}) ->
    Interval = emqx_ds_shared_sub_store:heartbeat_interval(Claim),
    ok = send_after(Interval, #renew_leader_claim{}).

%%--------------------------------------------------------------------
%% timers
%% renew_streams timer
handle_info(#renew_streams{}, St0) ->
    ?tp(debug, ds_shared_sub_leader_timeout, #{timeout => renew_streams}),
    St1 = renew_streams(St0),
    ok = send_after(leader_renew_streams_interval, #renew_streams{}),
    {noreply, St1};
%% periodical_actions_timeout timer
handle_info(#periodical_actions_timeout{}, St0) ->
    St1 = do_timeout_actions(St0),
    ok = send_after(leader_periodical_actions_interval, #periodical_actions_timeout{}),
    {noreply, St1};
handle_info(#renew_leader_claim{}, St0) ->
    case renew_leader_claim(St0) of
        St1 = #{} ->
            ok = init_claim_renewal(St1),
            {noreply, St1};
        {stop, Reason} ->
            {stop, {shutdown, Reason}, St0};
        Error ->
            {stop, Error, St0}
    end;
%%--------------------------------------------------------------------
%% borrower messages
handle_info(?borrower_connect_match(BorrowerId, _ShareTopicFilter), St0) ->
    St1 = handle_borrower_connect(St0, BorrowerId),
    {noreply, St1};
handle_info(?borrower_ping_match(BorrowerId), St0) ->
    St1 = with_valid_borrower(St0, BorrowerId, fun() ->
        handle_borrower_ping(St0, BorrowerId)
    end),
    {noreply, St1};
handle_info(?borrower_update_progress_match(BorrowerId, StreamProgress), St0) ->
    St1 = with_valid_borrower(St0, BorrowerId, fun() ->
        handle_update_stream_progress(St0, BorrowerId, StreamProgress)
    end),
    {noreply, St1};
handle_info(?borrower_revoke_finished_match(BorrowerId, Stream), St0) ->
    St1 = with_valid_borrower(St0, BorrowerId, fun() ->
        handle_revoke_finished(St0, BorrowerId, Stream)
    end),
    {noreply, St1};
handle_info(?borrower_disconnect_match(BorrowerId, StreamProgresses), St0) ->
    %% We allow this event to be processed even if the borrower is unknown
    St1 = handle_disconnect_borrower(St0, BorrowerId, StreamProgresses),
    {noreply, St1};
%%--------------------------------------------------------------------
%% fallback
handle_info(Info, #{group_id := GroupId} = St) ->
    ?tp(warning, ds_shared_sub_leader_unknown_info, #{info => Info, group_id => GroupId}),
    {noreply, St}.

handle_call(_Request, _From, St) ->
    {reply, {error, noimpl}, St}.

handle_cast(_Event, St) ->
    {noreply, St}.

terminate(
    _Reason,
    #{group_id := ShareTopicFilter, leader_claim := Claim, store := Store}
) ->
    %% NOTE
    %% Call to `commit_dirty/2` will currently block.
    %% On the other hand, call to `disown_leadership/1` should be non-blocking.
    StoreID = emqx_ds_shared_sub_store:mk_id(ShareTopicFilter),
    Result = emqx_ds_shared_sub_store:commit_dirty(Claim, Store),
    ok = emqx_ds_shared_sub_store:disown_leadership(StoreID, Claim),
    ?tp(debug, ds_shared_sub_leader_store_committed_dirty, #{
        id => ShareTopicFilter,
        store => StoreID,
        claim => Claim,
        result => Result
    }).

%%--------------------------------------------------------------------
%% Event handlers
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------

renew_leader_claim(St = #{group_id := ShareTopicFilter, store := Store0, leader_claim := Claim}) ->
    TS = emqx_message:timestamp_now(),
    case emqx_ds_shared_sub_store:commit_renew(Claim, TS, Store0) of
        {ok, RenewedClaim, CommittedStore} ->
            ?tp(shared_sub_leader_store_committed, #{
                id => ShareTopicFilter,
                store => emqx_ds_shared_sub_store:id(CommittedStore),
                claim => Claim,
                renewed => RenewedClaim
            }),
            attach_claim(RenewedClaim, St#{store := CommittedStore});
        destroyed ->
            %% NOTE
            %% Not doing anything under the assumption that destroys happen long after
            %% clients are gone and leaders are dead.
            ?tp(warning, ds_shared_sub_leader_store_destroyed, #{
                id => ShareTopicFilter,
                store => emqx_ds_shared_sub_store:id(Store0)
            }),
            {stop, shared_subscription_destroyed};
        {error, Class, Reason} = Error ->
            ?tp(warning, ds_shared_sub_leader_store_commit_failed, #{
                id => ShareTopicFilter,
                store => emqx_ds_shared_sub_store:id(Store0),
                claim => Claim,
                reason => Reason
            }),
            case Class of
                %% Will retry.
                recoverable -> St;
                %% Will have to crash.
                unrecoverable -> Error
            end
    end.

%%--------------------------------------------------------------------
%% Renew streams

%% * Find new streams in DS
%% * Revoke streams from borrowers having too many streams
%% * Assign streams to borrowers having too few streams

renew_streams(#{topic := Topic} = St0) ->
    TopicFilter = emqx_topic:words(Topic),
    StartTime = store_get_start_time(St0),
    StreamsWRanks = get_streams(TopicFilter, StartTime),

    %% Discard streams that are already replayed and init new
    {NewStreamsWRanks, RankProgress} = emqx_ds_shared_sub_leader_rank_progress:add_streams(
        StreamsWRanks,
        store_get_rank_progress(St0)
    ),
    {St1, VanishedStreams} = update_progresses(St0, NewStreamsWRanks, TopicFilter, StartTime),
    St2 = store_put_rank_progress(St1, RankProgress),
    St3 = remove_vanished_streams(St2, VanishedStreams),
    DesiredCounts = desired_stream_count_for_borrowers(St3),
    St4 = revoke_streams(St3, DesiredCounts),
    St5 = assign_streams(St4, DesiredCounts),
    ?tp(info, ds_shared_sub_leader_renew_streams, #{
        topic_filter => TopicFilter,
        new_streams => length(NewStreamsWRanks)
    }),
    St5.

update_progresses(St0, NewStreamsWRanks, TopicFilter, StartTime) ->
    ExistingStreams = store_setof_streams(St0),
    St = lists:foldl(
        fun({Rank, Stream}, DataAcc) ->
            case sets:is_element(Stream, ExistingStreams) of
                true ->
                    DataAcc;
                false ->
                    {ok, It} = make_iterator(Stream, TopicFilter, StartTime),
                    StreamData = #{progress => #{iterator => It}, rank => Rank},
                    store_put_stream(DataAcc, Stream, StreamData)
            end
        end,
        St0,
        NewStreamsWRanks
    ),
    VanishedStreams = lists:foldl(
        fun({_Rank, Stream}, Acc) -> sets:del_element(Stream, Acc) end,
        ExistingStreams,
        NewStreamsWRanks
    ),
    {St, sets:to_list(VanishedStreams)}.

%% We mark disappeared streams as revoked.
%%
%% We do not receive any progress on vanished streams;
%% the borrowers will also delete them from their state
%% because of revoked status.
remove_vanished_streams(St, VanishedStreams) ->
    finalize_streams(St, VanishedStreams).

finalize_streams(St, []) ->
    St;
finalize_streams(#{stream_owners := StreamOwners0} = St, [Stream | RestStreams]) ->
    case StreamOwners0 of
        #{Stream := #{status := ?stream_revoked}} ->
            finalize_streams(St, RestStreams);
        #{
            Stream := #{status := _OtherStatus, borrower_id := BorrowerId} =
                Ownership0
        } ->
            emqx_ds_shared_sub_proto:send_to_borrower(
                BorrowerId,
                ?leader_revoked(this_leader(St), Stream)
            ),
            Ownership1 = Ownership0#{status := ?stream_revoked},
            StreamOwners1 = StreamOwners0#{Stream => Ownership1},
            finalize_streams(
                St#{stream_owners => StreamOwners1},
                RestStreams
            );
        _ ->
            finalize_streams(St, RestStreams)
    end.

%% We revoke streams from borrowers that have too many streams (> desired_stream_count_per_borrower).
%% We revoke only from stable subscribers — those not having streams in transient states.
%% After revoking, no unassigned streams appear. Streams will become unassigned
%% only after borrower reports them back as revoked.
revoke_streams(St0, DesiredCounts) ->
    StableBorrowers = stable_borrowers(St0),
    maps:fold(
        fun(BorrowerId, GrantedStreams, DataAcc) ->
            DesiredCount = maps:get(BorrowerId, DesiredCounts),
            revoke_excess_streams_from_borrower(
                DataAcc, BorrowerId, GrantedStreams, DesiredCount
            )
        end,
        St0,
        StableBorrowers
    ).

revoke_excess_streams_from_borrower(St, BorrowerId, GrantedStreams, DesiredCount) ->
    CurrentCount = length(GrantedStreams),
    RevokeCount = CurrentCount - DesiredCount,
    case RevokeCount > 0 of
        false ->
            St;
        true ->
            ?tp(debug, ds_shared_sub_leader_revoke_streams, #{
                borrower_id => ?format_borrower_id(BorrowerId),
                current_count => CurrentCount,
                revoke_count => RevokeCount,
                desired_count => DesiredCount
            }),
            StreamsToRevoke = select_streams_for_revoke(St, GrantedStreams, RevokeCount),
            revoke_streams_from_borrower(St, BorrowerId, StreamsToRevoke)
    end.

select_streams_for_revoke(_St, GrantedStreams, RevokeCount) ->
    %% TODO
    %% Some intellectual logic should be used regarding:
    %% * shard ids (better do not mix shards in the same borrower);
    %% * stream stats (how much data was replayed from stream),
    %%   heavy streams should be distributed across different borrowers);
    %% * data locality (borrowers better preserve streams with data available on the borrower's node)
    lists:sublist(shuffle(GrantedStreams), RevokeCount).

%% We assign streams to borrowers that have too few streams (< desired_stream_count_per_borrower).
%% We assign only to stable subscribers — those not having streams in transient states.
assign_streams(St0, DesiredCounts) ->
    StableBorrowers = stable_borrowers(St0),
    maps:fold(
        fun(BorrowerId, GrantedStreams, DataAcc) ->
            DesiredCount = maps:get(BorrowerId, DesiredCounts),
            assign_lacking_streams(DataAcc, BorrowerId, GrantedStreams, DesiredCount)
        end,
        St0,
        StableBorrowers
    ).

assign_lacking_streams(St0, BorrowerId, GrantedStreams, DesiredCount) ->
    CurrentCount = length(GrantedStreams),
    AssignCount = DesiredCount - CurrentCount,
    case AssignCount > 0 of
        false ->
            St0;
        true ->
            StreamsToAssign = select_streams_for_assign(St0, BorrowerId, AssignCount),
            ?tp(debug, ds_shared_sub_leader_assign_streams, #{
                borrower_id => ?format_borrower_id(BorrowerId),
                current_count => CurrentCount,
                assign_count => AssignCount,
                desired_count => DesiredCount,
                streams_to_assign => ?format_streams(StreamsToAssign)
            }),
            assign_streams_to_borrower(St0, BorrowerId, StreamsToAssign)
    end.

select_streams_for_assign(St0, _BorrowerId, AssignCount) ->
    %% TODO
    %% Some intellectual logic should be used. See `select_streams_for_revoke/3`.
    UnassignedStreams = unassigned_streams(St0),
    lists:sublist(shuffle(UnassignedStreams), AssignCount).

revoke_streams_from_borrower(St, BorrowerId, StreamsToRevoke) ->
    lists:foldl(
        fun(Stream, DataAcc0) ->
            ok = emqx_ds_shared_sub_proto:send_to_borrower(
                BorrowerId,
                ?leader_revoke(this_leader(St), Stream)
            ),
            set_stream_status(DataAcc0, Stream, ?stream_revoking)
        end,
        St,
        StreamsToRevoke
    ).

assign_streams_to_borrower(St, BorrowerId, StreamsToAssign) ->
    lists:foldl(
        fun(Stream, DataAcc) ->
            StreamProgress = stream_progress(DataAcc, Stream),
            emqx_ds_shared_sub_proto:send_to_borrower(
                BorrowerId,
                ?leader_grant(this_leader(DataAcc), StreamProgress)
            ),
            set_stream_granting(DataAcc, Stream, BorrowerId)
        end,
        St,
        StreamsToAssign
    ).

%%--------------------------------------------------------------------
%% Timeout actions

do_timeout_actions(St0) ->
    St1 = drop_timeout_borrowers(St0),
    St2 = renew_transient_streams(St1),
    St2.

drop_timeout_borrowers(#{borrowers := Borrowers} = St) ->
    Now = now_ms_monotonic(),
    maps:fold(
        fun(BorrowerId, #{validity_deadline := Deadline}, DataAcc) ->
            case Deadline < Now of
                true ->
                    ?tp(warning, ds_shared_sub_leader_drop_timeout_borrower, #{
                        borrower_id => ?format_borrower_id(BorrowerId),
                        deadline => Deadline,
                        now => Now
                    }),
                    drop_invalidate_borrower(DataAcc, BorrowerId);
                false ->
                    DataAcc
            end
        end,
        St,
        Borrowers
    ).

renew_transient_streams(#{stream_owners := StreamOwners} = St) ->
    Leader = this_leader(St),
    ok = maps:foreach(
        fun
            (Stream, #{status := ?stream_granting, borrower_id := BorrowerId}) ->
                StreamProgress = stream_progress(St, Stream),
                emqx_ds_shared_sub_proto:send_to_borrower(
                    BorrowerId,
                    ?leader_grant(Leader, StreamProgress)
                );
            (Stream, #{status := ?stream_revoking, borrower_id := BorrowerId}) ->
                emqx_ds_shared_sub_proto:send_to_borrower(
                    BorrowerId,
                    ?leader_revoke(Leader, Stream)
                );
            (Stream, #{status := ?stream_revoked, borrower_id := BorrowerId}) ->
                emqx_ds_shared_sub_proto:send_to_borrower(
                    BorrowerId,
                    ?leader_revoked(Leader, Stream)
                );
            (_Stream, #{}) ->
                ok
        end,
        StreamOwners
    ),
    St.

%%--------------------------------------------------------------------
%% Handle a newly connected subscriber

handle_borrower_connect(
    #{group_id := GroupId, borrowers := Borrowers0} = St0,
    BorrowerId
) ->
    ?tp(debug, ds_shared_sub_leader_borrower_connect, #{
        borrower_id => ?format_borrower_id(BorrowerId),
        group_id => GroupId,
        borrower_ids => ?format_borrower_ids(maps:keys(Borrowers0))
    }),
    Borrowers1 =
        case Borrowers0 of
            #{BorrowerId := _} ->
                Borrowers0;
            _ ->
                Borrowers0#{BorrowerId => new_borrower_data()}
        end,
    St1 = St0#{borrowers => Borrowers1},
    DesiredCounts = desired_stream_count_for_borrowers(St1, maps:keys(Borrowers1)),
    DesiredCount = maps:get(BorrowerId, DesiredCounts),
    assign_initial_streams_to_borrower(St1, BorrowerId, DesiredCount).

assign_initial_streams_to_borrower(St, BorrowerId, AssignCount) ->
    case select_streams_for_assign(St, BorrowerId, AssignCount) of
        [] ->
            ok = emqx_ds_shared_sub_proto:send_to_borrower(
                BorrowerId,
                ?leader_connect_response(this_leader(St))
            ),
            St;
        InitialStreamsToAssign ->
            assign_streams_to_borrower(St, BorrowerId, InitialStreamsToAssign)
    end.

%%--------------------------------------------------------------------
%% Handle borrower ping

handle_borrower_ping(St0, BorrowerId) ->
    ?tp(debug, ds_shared_sub_leader_borrower_ping, #{
        borrower_id => ?format_borrower_id(BorrowerId)
    }),
    #{borrowers := #{BorrowerId := BorrowerState0} = Borrowers0} = St0,
    NewDeadline = now_ms_monotonic() + ?dq_config(leader_borrower_timeout),
    BorrowerState1 = BorrowerState0#{validity_deadline => NewDeadline},
    ok = emqx_ds_shared_sub_proto:send_to_borrower(
        BorrowerId,
        ?leader_ping_response(this_leader(St0))
    ),
    St0#{
        borrowers => Borrowers0#{BorrowerId => BorrowerState1}
    }.

%%--------------------------------------------------------------------
%% Handle stream progress

handle_update_stream_progress(St0, BorrowerId, #{stream := Stream} = StreamProgress) ->
    case stream_ownership(St0, Stream) of
        #{status := Status, borrower_id := BorrowerId} ->
            handle_update_stream_progress(St0, BorrowerId, Status, StreamProgress);
        undefined ->
            St0;
        _ ->
            drop_invalidate_borrower(St0, BorrowerId)
    end.

handle_update_stream_progress(
    St0, _BorrowerId, Status, #{stream := Stream} = StreamProgress
) when
    Status =:= ?stream_granting
->
    St1 = update_stream_progress(St0, StreamProgress),
    St2 = set_stream_status(St1, Stream, ?stream_granted),
    finalize_stream_if_replayed(St2, StreamProgress);
handle_update_stream_progress(St0, _BorrowerId, Status, StreamProgress) when
    Status =:= ?stream_granted
->
    St1 = update_stream_progress(St0, StreamProgress),
    finalize_stream_if_replayed(St1, StreamProgress);
handle_update_stream_progress(St0, _BorrowerId, ?stream_revoking, StreamProgress) ->
    St1 = update_stream_progress(St0, StreamProgress),
    finalize_stream_if_revoke_finished_or_replayed(St1, StreamProgress);
handle_update_stream_progress(St, _BorrowerId, ?stream_revoked, _StreamProgress) ->
    St.

update_stream_progress(St0, StreamProgress) ->
    St1 = update_store_progress(St0, StreamProgress),
    update_rank_progress(St1, StreamProgress).

update_store_progress(St, #{stream := Stream, progress := #{iterator := end_of_stream}}) ->
    store_delete_stream(St, Stream);
update_store_progress(St, #{stream := Stream, progress := Progress}) ->
    StreamData0 = store_get_stream(St, Stream),
    StreamData = StreamData0#{progress => Progress},
    store_put_stream(St, Stream, StreamData).

finalize_stream_if_replayed(St, #{stream := Stream, progress := #{iterator := end_of_stream}}) ->
    finalize_streams(St, [Stream]);
finalize_stream_if_replayed(St, _Progress) ->
    St.

finalize_stream_if_revoke_finished_or_replayed(St, #{stream := Stream, use_finished := true}) ->
    finalize_streams(St, [Stream]);
finalize_stream_if_revoke_finished_or_replayed(St, #{
    stream := Stream, progress := #{iterator := end_of_stream}
}) ->
    finalize_streams(St, [Stream]);
finalize_stream_if_revoke_finished_or_replayed(St, _StreamProgress) ->
    St.

update_rank_progress(St, #{stream := Stream, progress := #{iterator := end_of_stream}}) ->
    RankProgress0 = store_get_rank_progress(St),
    #{rank := Rank} = store_get_stream(St, Stream),
    RankProgress = emqx_ds_shared_sub_leader_rank_progress:set_replayed(
        {Rank, Stream}, RankProgress0
    ),
    store_put_rank_progress(St, RankProgress);
update_rank_progress(St, _StreamProgress) ->
    St.

set_stream_status(#{stream_owners := StreamOwners} = St, Stream, Status) ->
    ?tp(debug, ds_shared_sub_leader_set_stream_status, #{
        stream => ?format_stream(Stream), status => Status
    }),
    #{Stream := Ownership0} = StreamOwners,
    Ownership1 = Ownership0#{status := Status},
    StreamOwners1 = StreamOwners#{Stream => Ownership1},
    St#{
        stream_owners => StreamOwners1
    }.

set_stream_granting(#{stream_owners := StreamOwners0} = St, Stream, BorrowerId) ->
    ?tp(debug, ds_shared_sub_leader_set_stream_granting, #{
        stream => ?format_stream(Stream), borrower_id => ?format_borrower_id(BorrowerId)
    }),
    undefined = stream_ownership(St, Stream),
    StreamOwners1 = StreamOwners0#{
        Stream => #{
            status => ?stream_granting,
            borrower_id => BorrowerId
        }
    },
    St#{
        stream_owners => StreamOwners1
    }.

set_stream_free(#{stream_owners := StreamOwners} = St, Stream) ->
    ?tp(debug, ds_shared_sub_leader_set_stream_free, #{
        stream => ?format_stream(Stream)
    }),
    StreamOwners1 = maps:remove(Stream, StreamOwners),
    St#{
        stream_owners => StreamOwners1
    }.

%%--------------------------------------------------------------------
%% Disconnect borrower gracefully

handle_disconnect_borrower(St0, BorrowerId, StreamProgresses) ->
    ?tp(debug, ds_shared_sub_leader_disconnect_borrower, #{
        borrower_id => ?format_borrower_id(BorrowerId),
        stream_progresses => ?format_deep(StreamProgresses)
    }),
    St1 = lists:foldl(
        fun(#{stream := Stream} = StreamProgress, DataAcc0) ->
            case stream_ownership(DataAcc0, Stream) of
                #{borrower_id := BorrowerId} ->
                    DataAcc1 = update_stream_progress(DataAcc0, StreamProgress),
                    ok = send_after(0, #renew_streams{}),
                    set_stream_free(DataAcc1, Stream);
                _ ->
                    DataAcc0
            end
        end,
        St0,
        StreamProgresses
    ),
    drop_borrower(St1, BorrowerId).

%%--------------------------------------------------------------------
%% Finalize revoked stream

handle_revoke_finished(St0, BorrowerId, Stream) ->
    case stream_ownership(St0, Stream) of
        #{status := ?stream_revoked, borrower_id := BorrowerId} ->
            set_stream_free(St0, Stream);
        _ ->
            drop_invalidate_borrower(St0, BorrowerId)
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

send_after(Timeout, Event) when is_integer(Timeout) ->
    _ = erlang:send_after(Timeout, self(), Event),
    ok;
send_after(Timeout, Event) when is_atom(Timeout) ->
    _ = erlang:send_after(?dq_config(Timeout), self(), Event),
    ok.

new_borrower_data() ->
    Deadline = now_ms_monotonic() + ?dq_config(leader_borrower_timeout),
    #{validity_deadline => Deadline}.

unassigned_streams(#{stream_owners := StreamOwners} = St) ->
    Streams = store_setof_streams(St),
    sets:to_list(sets:subtract(Streams, StreamOwners)).

desired_stream_count_for_borrowers(#{borrowers := Borrowers} = St) ->
    desired_stream_count_for_borrowers(St, maps:keys(Borrowers)).

desired_stream_count_for_borrowers(_St, []) ->
    0;
desired_stream_count_for_borrowers(St, BorrowerIds) ->
    StreamCount = store_num_streams(St),
    BorrowerCount = length(BorrowerIds),
    maps:from_list(
        lists:map(
            fun({I, BorrowerId}) ->
                {BorrowerId, desired_stream_count_for_borrower(StreamCount, BorrowerCount, I)}
            end,
            enumerate(lists:sort(BorrowerIds))
        )
    ).

enumerate(List) ->
    enumerate(0, List).

enumerate(_, []) ->
    [];
enumerate(I, [H | T]) ->
    [{I, H} | enumerate(I + 1, T)].

desired_stream_count_for_borrower(StreamCount, BorrowerCount, I) ->
    (StreamCount div BorrowerCount) +
        extra_stream_count_for_borrower(StreamCount, BorrowerCount, I).

extra_stream_count_for_borrower(StreamCount, BorrowerCount, I) when
    I < (StreamCount rem BorrowerCount)
->
    1;
extra_stream_count_for_borrower(_StreamCount, _BorrowerCount, _I) ->
    0.

stream_progress(St, Stream) ->
    StreamData = store_get_stream(St, Stream),
    #{
        stream => Stream,
        progress => maps:get(progress, StreamData)
    }.

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

this_leader(_St) ->
    self().

drop_borrower(
    #{borrowers := Borrowers0, stream_owners := StreamOwners0} = St, BorrowerIdDrop
) ->
    ?tp(debug, ds_shared_sub_leader_drop_borrower, #{
        borrower_id => ?format_borrower_id(BorrowerIdDrop)
    }),
    Subscribers1 = maps:remove(BorrowerIdDrop, Borrowers0),
    StreamOwners1 = maps:filter(
        fun(_Stream, #{borrower_id := BorrowerId}) ->
            BorrowerId =/= BorrowerIdDrop
        end,
        StreamOwners0
    ),
    St#{
        borrowers => Subscribers1,
        stream_owners => StreamOwners1
    }.

invalidate_subscriber(St, BorrowerId) ->
    ok = emqx_ds_shared_sub_proto:send_to_borrower(
        BorrowerId,
        ?leader_invalidate(this_leader(St))
    ).

drop_invalidate_borrower(St0, BorrowerId) ->
    St1 = drop_borrower(St0, BorrowerId),
    ok = invalidate_subscriber(St1, BorrowerId),
    St1.

stream_ownership(St, Stream) ->
    case St of
        #{stream_owners := #{Stream := #{} = Ownership}} ->
            Ownership;
        _ ->
            undefined
    end.

with_valid_borrower(#{borrowers := Borrowers} = St, BorrowerId, Fun) ->
    Now = now_ms_monotonic(),
    case Borrowers of
        #{BorrowerId := #{validity_deadline := Deadline}} when Deadline > Now ->
            Fun();
        _ ->
            drop_invalidate_borrower(St, BorrowerId)
    end.

%% Borrowers that do not have streams in transient states.
%% The result is a map from borrower_id to a list of granted streams.
stable_borrowers(#{borrowers := Borrowers, stream_owners := StreamOwners} = _St) ->
    ?tp(debug, ds_shared_sub_leader_stable_borrowers, #{
        borrower_ids => ?format_borrower_ids(maps:keys(Borrowers)),
        stream_owners => ?format_stream_map(StreamOwners)
    }),
    maps:fold(
        fun
            (
                Stream,
                #{status := ?stream_granted, borrower_id := BorrowerId},
                BorrowersAcc
            ) ->
                emqx_utils_maps:update_if_present(
                    BorrowerId,
                    fun(Streams) -> [Stream | Streams] end,
                    BorrowersAcc
                );
            (
                _Stream,
                #{status := _OtherStatus, borrower_id := BorrowerId},
                IdsAcc
            ) ->
                maps:remove(BorrowerId, IdsAcc)
        end,
        maps:from_keys(maps:keys(Borrowers), []),
        StreamOwners
    ).

%% DS helpers

get_streams(TopicFilter, StartTime) ->
    emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, StartTime).

make_iterator(Stream, TopicFilter, StartTime) ->
    emqx_ds:make_iterator(?PERSISTENT_MESSAGE_DB, Stream, TopicFilter, StartTime).

%% Leader store

store_get_stream(#{store := Store}, ID) ->
    emqx_ds_shared_sub_store:get(stream, ID, Store).

store_put_stream(St = #{store := Store0}, ID, StreamData) ->
    Store = emqx_ds_shared_sub_store:put(stream, ID, StreamData, Store0),
    St#{store := Store}.

store_delete_stream(St = #{store := Store0}, ID) ->
    Store = emqx_ds_shared_sub_store:delete(stream, ID, Store0),
    St#{store := Store}.

store_get_rank_progress(#{store := Store}) ->
    emqx_ds_shared_sub_store:get(rank_progress, Store).

store_put_rank_progress(St = #{store := Store0}, RankProgress) ->
    Store = emqx_ds_shared_sub_store:set(rank_progress, RankProgress, Store0),
    St#{store := Store}.

store_get_start_time(#{store := Store}) ->
    Props = emqx_ds_shared_sub_store:get(properties, Store),
    maps:get(start_time, Props).

store_num_streams(#{store := Store}) ->
    emqx_ds_shared_sub_store:size(stream, Store).

store_setof_streams(#{store := Store}) ->
    Acc0 = sets:new([{version, 2}]),
    FoldFun = fun(Stream, _StreamData, Acc) -> sets:add_element(Stream, Acc) end,
    emqx_ds_shared_sub_store:fold(stream, FoldFun, Acc0, Store).
