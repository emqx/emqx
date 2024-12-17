%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-type ssubscriber_id() :: emqx_ds_shared_sub_proto:ssubscriber_id().

-type options() :: #{
    share_topic_filter := share_topic_filter()
}.

%% Stream statuses. Stream with states other than `stream_granted` are
%% considered "transient".

%% Stream is being assigned to a ssubscriber, a ssubscriber should confirm the assignment
%% by sending progress or reject the assignment.
-define(stream_granting, stream_granting).
%% Stream is assigned to a ssubscriber, the ssubscriber confirmed the assignment.
-define(stream_granted, stream_granted).
%% Stream is being revoked from a ssubscriber, a ssubscriber should send us final progress
%% for the stream.
-define(stream_revoking, stream_revoking).
%% Stream is revoked from a ssubscriber, the ssubscriber sent us final progress.
%% We wait till the ssubscriber confirms that it has cleaned the data.
-define(stream_revoked, stream_revoked).

-type stream_status() :: ?stream_granting | ?stream_granted | ?stream_revoking | ?stream_revoked.

-type stream_ownership() :: #{
    status := stream_status(),
    ssubscriber_id := ssubscriber_id()
}.

-type ssubscriber_state() :: #{
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
    ssubscribers := #{
        emqx_ds_shared_sub_proto:ssubscriber_id() => ssubscriber_state()
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
-record(ssubscriber_timeout, {}).
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
            ok = send_after(leader_drop_timeout_interval_ms, #ssubscriber_timeout{}),
            #{
                group_id => ShareTopicFilter,
                topic => Topic,
                store => Store,
                stream_owners => #{},
                ssubscribers => #{}
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
    ok = send_after(leader_renew_streams_interval_ms, #renew_streams{}),
    {noreply, St1};
%% ssubscriber_timeout timer
handle_info(#ssubscriber_timeout{}, St0) ->
    St1 = do_timeout_actions(St0),
    ok = send_after(leader_drop_timeout_interval_ms, #ssubscriber_timeout{}),
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
%% ssubscriber messages
handle_info(?ssubscriber_connect_match(SSubscriberId, _ShareTopicFilter), St0) ->
    St1 = handle_ssubscriber_connect(St0, SSubscriberId),
    {noreply, St1};
handle_info(?ssubscriber_ping_match(SSubscriberId), St0) ->
    St1 = with_valid_ssubscriber(St0, SSubscriberId, fun() ->
        handle_ssubscriber_ping(St0, SSubscriberId)
    end),
    {noreply, St1};
handle_info(?ssubscriber_update_progress_match(SSubscriberId, StreamProgress), St0) ->
    St1 = with_valid_ssubscriber(St0, SSubscriberId, fun() ->
        handle_update_stream_progress(St0, SSubscriberId, StreamProgress)
    end),
    {noreply, St1};
handle_info(?ssubscriber_revoke_finished_match(SSubscriberId, Stream), St0) ->
    St1 = with_valid_ssubscriber(St0, SSubscriberId, fun() ->
        handle_revoke_finished(St0, SSubscriberId, Stream)
    end),
    {noreply, St1};
handle_info(?ssubscriber_disconnect_match(SSubscriberId, StreamProgresses), St0) ->
    %% We allow this event to be processed even if the ssubscriber is unknown
    St1 = handle_disconnect_ssubscriber(St0, SSubscriberId, StreamProgresses),
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
%% * Revoke streams from ssubscribers having too many streams
%% * Assign streams to ssubscribers having too few streams

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
    DesiredCounts = desired_stream_count_for_ssubscribers(St3),
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
%% the ssubscribers will also delete them from their state
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
            Stream := #{status := _OtherStatus, ssubscriber_id := SSubscriberId} =
                Ownership0
        } ->
            emqx_ds_shared_sub_proto:send_to_ssubscriber(
                SSubscriberId,
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

%% We revoke streams from ssubscribers that have too many streams (> desired_stream_count_per_ssubscriber).
%% We revoke only from stable subscribers — those not having streams in transient states.
%% After revoking, no unassigned streams appear. Streams will become unassigned
%% only after ssubscriber reports them back as revoked.
revoke_streams(St0, DesiredCounts) ->
    StableSSubscribers = stable_ssubscribers(St0),
    maps:fold(
        fun(SSubscriberId, GrantedStreams, DataAcc) ->
            DesiredCount = maps:get(SSubscriberId, DesiredCounts),
            revoke_excess_streams_from_ssubscriber(
                DataAcc, SSubscriberId, GrantedStreams, DesiredCount
            )
        end,
        St0,
        StableSSubscribers
    ).

revoke_excess_streams_from_ssubscriber(St, SSubscriberId, GrantedStreams, DesiredCount) ->
    CurrentCount = length(GrantedStreams),
    RevokeCount = CurrentCount - DesiredCount,
    case RevokeCount > 0 of
        false ->
            St;
        true ->
            ?tp(debug, ds_shared_sub_leader_revoke_streams, #{
                ssubscriber_id => ?format_ssubscriber_id(SSubscriberId),
                current_count => CurrentCount,
                revoke_count => RevokeCount,
                desired_count => DesiredCount
            }),
            StreamsToRevoke = select_streams_for_revoke(St, GrantedStreams, RevokeCount),
            revoke_streams_from_ssubscriber(St, SSubscriberId, StreamsToRevoke)
    end.

select_streams_for_revoke(_St, GrantedStreams, RevokeCount) ->
    %% TODO
    %% Some intellectual logic should be used regarding:
    %% * shard ids (better do not mix shards in the same ssubscriber);
    %% * stream stats (how much data was replayed from stream),
    %%   heavy streams should be distributed across different ssubscribers);
    %% * data locality (ssubscribers better preserve streams with data available on the ssubscriber's node)
    lists:sublist(shuffle(GrantedStreams), RevokeCount).

%% We assign streams to ssubscribers that have too few streams (< desired_stream_count_per_ssubscriber).
%% We assign only to stable subscribers — those not having streams in transient states.
assign_streams(St0, DesiredCounts) ->
    StableSSubscribers = stable_ssubscribers(St0),
    maps:fold(
        fun(SSubscriberId, GrantedStreams, DataAcc) ->
            DesiredCount = maps:get(SSubscriberId, DesiredCounts),
            assign_lacking_streams(DataAcc, SSubscriberId, GrantedStreams, DesiredCount)
        end,
        St0,
        StableSSubscribers
    ).

assign_lacking_streams(St0, SSubscriberId, GrantedStreams, DesiredCount) ->
    CurrentCount = length(GrantedStreams),
    AssignCount = DesiredCount - CurrentCount,
    case AssignCount > 0 of
        false ->
            St0;
        true ->
            StreamsToAssign = select_streams_for_assign(St0, SSubscriberId, AssignCount),
            ?tp(debug, ds_shared_sub_leader_assign_streams, #{
                ssubscriber_id => ?format_ssubscriber_id(SSubscriberId),
                current_count => CurrentCount,
                assign_count => AssignCount,
                desired_count => DesiredCount,
                streams_to_assign => ?format_streams(StreamsToAssign)
            }),
            assign_streams_to_ssubscriber(St0, SSubscriberId, StreamsToAssign)
    end.

select_streams_for_assign(St0, _SSubscriberId, AssignCount) ->
    %% TODO
    %% Some intellectual logic should be used. See `select_streams_for_revoke/3`.
    UnassignedStreams = unassigned_streams(St0),
    lists:sublist(shuffle(UnassignedStreams), AssignCount).

revoke_streams_from_ssubscriber(St, SSubscriberId, StreamsToRevoke) ->
    lists:foldl(
        fun(Stream, DataAcc0) ->
            ok = emqx_ds_shared_sub_proto:send_to_ssubscriber(
                SSubscriberId,
                ?leader_revoke(this_leader(St), Stream)
            ),
            set_stream_status(DataAcc0, Stream, ?stream_revoking)
        end,
        St,
        StreamsToRevoke
    ).

assign_streams_to_ssubscriber(St, SSubscriberId, StreamsToAssign) ->
    lists:foldl(
        fun(Stream, DataAcc) ->
            StreamProgress = stream_progress(DataAcc, Stream),
            emqx_ds_shared_sub_proto:send_to_ssubscriber(
                SSubscriberId,
                ?leader_grant(this_leader(DataAcc), StreamProgress)
            ),
            set_stream_granting(DataAcc, Stream, SSubscriberId)
        end,
        St,
        StreamsToAssign
    ).

%%--------------------------------------------------------------------
%% Timeout actions

do_timeout_actions(St0) ->
    St1 = drop_timeout_ssubscribers(St0),
    St2 = renew_transient_streams(St1),
    St2.

drop_timeout_ssubscribers(#{ssubscribers := SSubscribers} = St) ->
    Now = now_ms_monotonic(),
    maps:fold(
        fun(SSubscriberId, #{validity_deadline := Deadline}, DataAcc) ->
            case Deadline < Now of
                true ->
                    ?tp(warning, ds_shared_sub_leader_drop_timeout_ssubscriber, #{
                        ssubscriber_id => ?format_ssubscriber_id(SSubscriberId),
                        deadline => Deadline,
                        now => Now
                    }),
                    drop_invalidate_ssubscriber(DataAcc, SSubscriberId);
                false ->
                    DataAcc
            end
        end,
        St,
        SSubscribers
    ).

renew_transient_streams(#{stream_owners := StreamOwners} = St) ->
    Leader = this_leader(St),
    ok = maps:foreach(
        fun
            (Stream, #{status := ?stream_granting, ssubscriber_id := SSubscriberId}) ->
                StreamProgress = stream_progress(St, Stream),
                emqx_ds_shared_sub_proto:send_to_ssubscriber(
                    SSubscriberId,
                    ?leader_grant(Leader, StreamProgress)
                );
            (Stream, #{status := ?stream_revoking, ssubscriber_id := SSubscriberId}) ->
                emqx_ds_shared_sub_proto:send_to_ssubscriber(
                    SSubscriberId,
                    ?leader_revoke(Leader, Stream)
                );
            (Stream, #{status := ?stream_revoked, ssubscriber_id := SSubscriberId}) ->
                emqx_ds_shared_sub_proto:send_to_ssubscriber(
                    SSubscriberId,
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

handle_ssubscriber_connect(
    #{group_id := GroupId, ssubscribers := SSubscribers0} = St0,
    SSubscriberId
) ->
    ?tp(debug, ds_shared_sub_leader_ssubscriber_connect, #{
        ssubscriber_id => ?format_ssubscriber_id(SSubscriberId),
        group_id => GroupId,
        ssubscriber_ids => ?format_ssubscriber_ids(maps:keys(SSubscribers0))
    }),
    SSubscribers1 =
        case SSubscribers0 of
            #{SSubscriberId := _} ->
                SSubscribers0;
            _ ->
                SSubscribers0#{SSubscriberId => new_ssubscriber_data()}
        end,
    St1 = St0#{ssubscribers => SSubscribers1},
    DesiredCounts = desired_stream_count_for_ssubscribers(St1, maps:keys(SSubscribers1)),
    DesiredCount = maps:get(SSubscriberId, DesiredCounts),
    assign_initial_streams_to_ssubscriber(St1, SSubscriberId, DesiredCount).

assign_initial_streams_to_ssubscriber(St, SSubscriberId, AssignCount) ->
    case select_streams_for_assign(St, SSubscriberId, AssignCount) of
        [] ->
            ok = emqx_ds_shared_sub_proto:send_to_ssubscriber(
                SSubscriberId,
                ?leader_connect_response(this_leader(St))
            ),
            St;
        InitialStreamsToAssign ->
            assign_streams_to_ssubscriber(St, SSubscriberId, InitialStreamsToAssign)
    end.

%%--------------------------------------------------------------------
%% Handle ssubscriber ping

handle_ssubscriber_ping(St0, SSubscriberId) ->
    ?tp(debug, ds_shared_sub_leader_ssubscriber_ping, #{
        ssubscriber_id => ?format_ssubscriber_id(SSubscriberId)
    }),
    #{ssubscribers := #{SSubscriberId := SSubscriberState0} = SSubscribers0} = St0,
    NewDeadline = now_ms_monotonic() + ?dq_config(leader_ssubscriber_timeout_interval_ms),
    SSubscriberState1 = SSubscriberState0#{validity_deadline => NewDeadline},
    ok = emqx_ds_shared_sub_proto:send_to_ssubscriber(
        SSubscriberId,
        ?leader_ping_response(this_leader(St0))
    ),
    St0#{
        ssubscribers => SSubscribers0#{SSubscriberId => SSubscriberState1}
    }.

%%--------------------------------------------------------------------
%% Handle stream progress

handle_update_stream_progress(St0, SSubscriberId, #{stream := Stream} = StreamProgress) ->
    case stream_ownership(St0, Stream) of
        #{status := Status, ssubscriber_id := SSubscriberId} ->
            handle_update_stream_progress(St0, SSubscriberId, Status, StreamProgress);
        undefined ->
            St0;
        _ ->
            drop_invalidate_ssubscriber(St0, SSubscriberId)
    end.

handle_update_stream_progress(
    St0, _SSubscriberId, Status, #{stream := Stream} = StreamProgress
) when
    Status =:= ?stream_granting
->
    St1 = update_stream_progress(St0, StreamProgress),
    St2 = set_stream_status(St1, Stream, ?stream_granted),
    finalize_stream_if_replayed(St2, StreamProgress);
handle_update_stream_progress(St0, _SSubscriberId, Status, StreamProgress) when
    Status =:= ?stream_granted
->
    St1 = update_stream_progress(St0, StreamProgress),
    finalize_stream_if_replayed(St1, StreamProgress);
handle_update_stream_progress(St0, _SSubscriberId, ?stream_revoking, StreamProgress) ->
    St1 = update_stream_progress(St0, StreamProgress),
    finalize_stream_if_revoke_finished_or_replayed(St1, StreamProgress);
handle_update_stream_progress(St, _SSubscriberId, ?stream_revoked, _StreamProgress) ->
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

set_stream_granting(#{stream_owners := StreamOwners0} = St, Stream, SSubscriberId) ->
    ?tp(debug, ds_shared_sub_leader_set_stream_granting, #{
        stream => ?format_stream(Stream), ssubscriber_id => ?format_ssubscriber_id(SSubscriberId)
    }),
    undefined = stream_ownership(St, Stream),
    StreamOwners1 = StreamOwners0#{
        Stream => #{
            status => ?stream_granting,
            ssubscriber_id => SSubscriberId
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
%% Disconnect ssubscriber gracefully

handle_disconnect_ssubscriber(St0, SSubscriberId, StreamProgresses) ->
    ?tp(debug, ds_shared_sub_leader_disconnect_ssubscriber, #{
        ssubscriber_id => ?format_ssubscriber_id(SSubscriberId),
        stream_progresses => ?format_deep(StreamProgresses)
    }),
    St1 = lists:foldl(
        fun(#{stream := Stream} = StreamProgress, DataAcc0) ->
            case stream_ownership(DataAcc0, Stream) of
                #{ssubscriber_id := SSubscriberId} ->
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
    drop_ssubscriber(St1, SSubscriberId).

%%--------------------------------------------------------------------
%% Finalize revoked stream

handle_revoke_finished(St0, SSubscriberId, Stream) ->
    case stream_ownership(St0, Stream) of
        #{status := ?stream_revoked, ssubscriber_id := SSubscriberId} ->
            set_stream_free(St0, Stream);
        _ ->
            drop_invalidate_ssubscriber(St0, SSubscriberId)
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

new_ssubscriber_data() ->
    Deadline = now_ms_monotonic() + ?dq_config(leader_ssubscriber_timeout_interval_ms),
    #{validity_deadline => Deadline}.

unassigned_streams(#{stream_owners := StreamOwners} = St) ->
    Streams = store_setof_streams(St),
    sets:to_list(sets:subtract(Streams, StreamOwners)).

desired_stream_count_for_ssubscribers(#{ssubscribers := SSubscribers} = St) ->
    desired_stream_count_for_ssubscribers(St, maps:keys(SSubscribers)).

desired_stream_count_for_ssubscribers(_St, []) ->
    0;
desired_stream_count_for_ssubscribers(St, SSubscriberIds) ->
    StreamCount = store_num_streams(St),
    SSubscriberCount = length(SSubscriberIds),
    maps:from_list(
        lists:map(
            fun({I, SSubscriberId}) ->
                {SSubscriberId,
                    desired_stream_count_for_ssubscriber(StreamCount, SSubscriberCount, I)}
            end,
            enumerate(lists:sort(SSubscriberIds))
        )
    ).

enumerate(List) ->
    enumerate(0, List).

enumerate(_, []) ->
    [];
enumerate(I, [H | T]) ->
    [{I, H} | enumerate(I + 1, T)].

desired_stream_count_for_ssubscriber(StreamCount, SSubscriberCount, I) ->
    (StreamCount div SSubscriberCount) +
        extra_stream_count_for_ssubscriber(StreamCount, SSubscriberCount, I).

extra_stream_count_for_ssubscriber(StreamCount, SSubscriberCount, I) when
    I < (StreamCount rem SSubscriberCount)
->
    1;
extra_stream_count_for_ssubscriber(_StreamCount, _SSubscriberCount, _I) ->
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

drop_ssubscriber(
    #{ssubscribers := SSubscribers0, stream_owners := StreamOwners0} = St, SSubscriberIdDrop
) ->
    ?tp(debug, ds_shared_sub_leader_drop_ssubscriber, #{
        ssubscriber_id => ?format_ssubscriber_id(SSubscriberIdDrop)
    }),
    Subscribers1 = maps:remove(SSubscriberIdDrop, SSubscribers0),
    StreamOwners1 = maps:filter(
        fun(_Stream, #{ssubscriber_id := SSubscriberId}) ->
            SSubscriberId =/= SSubscriberIdDrop
        end,
        StreamOwners0
    ),
    St#{
        ssubscribers => Subscribers1,
        stream_owners => StreamOwners1
    }.

invalidate_subscriber(St, SSubscriberId) ->
    ok = emqx_ds_shared_sub_proto:send_to_ssubscriber(
        SSubscriberId,
        ?leader_invalidate(this_leader(St))
    ).

drop_invalidate_ssubscriber(St0, SSubscriberId) ->
    St1 = drop_ssubscriber(St0, SSubscriberId),
    ok = invalidate_subscriber(St1, SSubscriberId),
    St1.

stream_ownership(St, Stream) ->
    case St of
        #{stream_owners := #{Stream := #{} = Ownership}} ->
            Ownership;
        _ ->
            undefined
    end.

with_valid_ssubscriber(#{ssubscribers := SSubscribers} = St, SSubscriberId, Fun) ->
    Now = now_ms_monotonic(),
    case SSubscribers of
        #{SSubscriberId := #{validity_deadline := Deadline}} when Deadline > Now ->
            Fun();
        _ ->
            drop_invalidate_ssubscriber(St, SSubscriberId)
    end.

%% SSubscribers that do not have streams in transient states.
%% The result is a map from ssubscriber_id to a list of granted streams.
stable_ssubscribers(#{ssubscribers := SSubscribers, stream_owners := StreamOwners} = _St) ->
    ?tp(debug, ds_shared_sub_leader_stable_ssubscribers, #{
        ssubscriber_ids => ?format_ssubscriber_ids(maps:keys(SSubscribers)),
        stream_owners => ?format_stream_map(StreamOwners)
    }),
    maps:fold(
        fun
            (
                Stream,
                #{status := ?stream_granted, ssubscriber_id := SSubscriberId},
                SSubscribersAcc
            ) ->
                case SSubscribersAcc of
                    #{SSubscriberId := Streams} ->
                        SSubscribersAcc#{SSubscriberId => [Stream | Streams]};
                    _ ->
                        SSubscribersAcc
                end;
            (
                _Stream,
                #{status := _OtherStatus, ssubscriber_id := SSubscriberId},
                IdsAcc
            ) ->
                maps:remove(SSubscriberId, IdsAcc)
        end,
        maps:from_keys(maps:keys(SSubscribers), []),
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
