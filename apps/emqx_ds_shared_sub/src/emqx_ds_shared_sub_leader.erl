%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_leader).

-include("emqx_ds_shared_sub_proto.hrl").
-include("emqx_ds_shared_sub_config.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_persistent_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    become/2
]).

-behaviour(gen_statem).
-export([
    callback_mode/0,
    init/1,
    handle_event/4,
    terminate/3
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
%% by sending progress or reject the assignment
-define(stream_granting, stream_granting).
%% Stream is assigned to a ssubscriber, the ssubscriber confirmed the assignment
-define(stream_granted, stream_granted).
%% Stream is being revoked from a ssubscriber, a ssubscriber should send us final progress
%% for the stream
-define(stream_revoking, stream_revoking).
%% Stream is revoked from a ssubscriber, the ssubscriber sent us final progress.
%% We wait till the ssubscriber cleans up the data.
-define(stream_revoked, stream_revoked).

-type stream_status() :: ?stream_granting | ?stream_granted | ?stream_revoking | ?stream_revoked.

-record(stream_ownership, {
    status :: stream_status(),
    ssubscriber_id :: ssubscriber_id()
}).

-type stream_ownership() :: #stream_ownership{}.

-type ssubscriber_state() :: #{
    validity_deadline := integer()
}.

%% Some data should be persisted
-type data() :: #{
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
    data/0
]).

%% Leader states

-define(leader_active, leader_active).

%% Events

-record(renew_streams, {}).
-record(ssubscriber_timeout, {}).
-record(renew_leader_claim, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

become(ShareTopicFilter, Claim) ->
    Data0 = init_data(ShareTopicFilter),
    Data1 = attach_claim(Claim, Data0),
    gen_statem:enter_loop(?MODULE, [], ?leader_active, Data1, init_claim_renewal(Data1)).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> [handle_event_function, state_enter].

init(_Args) ->
    %% NOTE: Currently, the only entrypoint is `become/2` that calls `enter_loop/5`.
    {error, noimpl}.

init_data(#share{topic = Topic} = ShareTopicFilter) ->
    StoreID = emqx_ds_shared_sub_store:mk_id(ShareTopicFilter),
    case emqx_ds_shared_sub_store:open(StoreID) of
        {ok, Store} ->
            ?tp(debug, dssub_store_open, #{topic => ShareTopicFilter, store => Store}),
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

attach_claim(Claim, Data) ->
    Data#{leader_claim => Claim}.

init_claim_renewal(_Data = #{leader_claim := Claim}) ->
    Interval = emqx_ds_shared_sub_store:heartbeat_interval(Claim),
    [{{timeout, #renew_leader_claim{}}, Interval, #renew_leader_claim{}}].

%%--------------------------------------------------------------------
%% repalying state
handle_event(enter, _OldState, ?leader_active, #{topic := Topic} = _Data) ->
    ?tp(debug, shared_sub_leader_enter_actve, #{topic => Topic}),
    {keep_state_and_data, [
        {{timeout, #renew_streams{}}, 0, #renew_streams{}},
        {
            {timeout, #ssubscriber_timeout{}},
            ?dq_config(leader_drop_timeout_interval_ms),
            #ssubscriber_timeout{}
        }
    ]};
%%--------------------------------------------------------------------
%% timers
%% renew_streams timer
handle_event({timeout, #renew_streams{}}, #renew_streams{}, ?leader_active, Data0) ->
    ?tp(debug, shared_sub_leader_timeout, #{timeout => renew_streams}),
    Data1 = renew_streams(Data0),
    {keep_state, Data1,
        {
            {timeout, #renew_streams{}},
            ?dq_config(leader_renew_streams_interval_ms),
            #renew_streams{}
        }};
%% ssubscriber_timeout timer
handle_event({timeout, #ssubscriber_timeout{}}, #ssubscriber_timeout{}, ?leader_active, Data0) ->
    Data1 = do_timeout_actions(Data0),
    {keep_state, Data1,
        {
            {timeout, #ssubscriber_timeout{}},
            ?dq_config(leader_drop_timeout_interval_ms),
            #ssubscriber_timeout{}
        }};
handle_event({timeout, #renew_leader_claim{}}, #renew_leader_claim{}, ?leader_active, Data0) ->
    case renew_leader_claim(Data0) of
        Data1 = #{} ->
            Actions = init_claim_renewal(Data1),
            {keep_state, Data1, Actions};
        {stop, Reason} ->
            {stop, {shutdown, Reason}};
        Error ->
            {stop, Error}
    end;
%%--------------------------------------------------------------------
%% agent events
handle_event(
    info, ?ssubscriber_connect_match(SSubscriberId, _ShareTopicFilter), ?leader_active, Data0
) ->
    Data1 = handle_ssubscriber_connect(Data0, SSubscriberId),
    {keep_state, Data1};
handle_event(
    info, ?ssubscriber_ping_match(SSubscriberId), ?leader_active, Data0
) ->
    Data1 = with_valid_ssubscriber(Data0, SSubscriberId, fun() ->
        handle_ssubscriber_ping(Data0, SSubscriberId)
    end),
    {keep_state, Data1};
handle_event(
    info,
    ?ssubscriber_update_progress_match(SSubscriberId, Stream, StreamProgress),
    ?leader_active,
    Data0
) ->
    Data1 = with_valid_ssubscriber(Data0, SSubscriberId, fun() ->
        handle_update_stream_progress(Data0, SSubscriberId, Stream, StreamProgress)
    end),
    {keep_state, Data1};
handle_event(
    info,
    ?ssubscriber_revoke_finished_match(SSubscriberId, Stream),
    ?leader_active,
    Data0
) ->
    Data1 = with_valid_ssubscriber(Data0, SSubscriberId, fun() ->
        handle_revoke_finished(Data0, SSubscriberId, Stream)
    end),
    {keep_state, Data1};
handle_event(
    info,
    ?ssubscriber_disconnect_match(SSubscriberId, StreamProgresses),
    ?leader_active,
    Data0
) ->
    %% We allow this event to be processed even if the ssubscriber is unknown
    Data1 = disconnect_ssubscriber(Data0, SSubscriberId, StreamProgresses),
    {keep_state, Data1};
%%--------------------------------------------------------------------
%% fallback
handle_event(enter, _OldState, _State, _Data) ->
    keep_state_and_data;
handle_event(Event, Content, State, _Data) ->
    ?SLOG(warning, #{
        msg => unexpected_event,
        event => Event,
        content => Content,
        state => State
    }),
    keep_state_and_data.

terminate(
    _Reason,
    _State,
    #{group_id := ShareTopicFilter, leader_claim := Claim, store := Store}
) ->
    %% NOTE
    %% Call to `commit_dirty/2` will currently block.
    %% On the other hand, call to `disown_leadership/1` should be non-blocking.
    StoreID = emqx_ds_shared_sub_store:mk_id(ShareTopicFilter),
    Result = emqx_ds_shared_sub_store:commit_dirty(Claim, Store),
    ok = emqx_ds_shared_sub_store:disown_leadership(StoreID, Claim),
    ?tp(shared_sub_leader_store_committed_dirty, #{
        id => ShareTopicFilter,
        store => StoreID,
        claim => Claim,
        result => Result
    }).

%%--------------------------------------------------------------------
%% Event handlers
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------

renew_leader_claim(Data = #{group_id := ShareTopicFilter, store := Store0, leader_claim := Claim}) ->
    TS = emqx_message:timestamp_now(),
    case emqx_ds_shared_sub_store:commit_renew(Claim, TS, Store0) of
        {ok, RenewedClaim, CommittedStore} ->
            ?tp(shared_sub_leader_store_committed, #{
                id => ShareTopicFilter,
                store => emqx_ds_shared_sub_store:id(CommittedStore),
                claim => Claim,
                renewed => RenewedClaim
            }),
            attach_claim(RenewedClaim, Data#{store := CommittedStore});
        destroyed ->
            %% NOTE
            %% Not doing anything under the assumption that destroys happen long after
            %% clients are gone and leaders are dead.
            ?tp(warning, "Shared subscription leader store destroyed", #{
                id => ShareTopicFilter,
                store => emqx_ds_shared_sub_store:id(Store0)
            }),
            {stop, shared_subscription_destroyed};
        {error, Class, Reason} = Error ->
            ?tp(warning, "Shared subscription leader store commit failed", #{
                id => ShareTopicFilter,
                store => emqx_ds_shared_sub_store:id(Store0),
                claim => Claim,
                reason => Reason
            }),
            case Class of
                %% Will retry.
                recoverable -> Data;
                %% Will have to crash.
                unrecoverable -> Error
            end
    end.

%%--------------------------------------------------------------------
%% Renew streams

%% * Find new streams in DS
%% * Revoke streams from ssubscribers having too many streams
%% * Assign streams to ssubscribers having too few streams

renew_streams(#{topic := Topic} = Data0) ->
    TopicFilter = emqx_topic:words(Topic),
    StartTime = store_get_start_time(Data0),
    StreamsWRanks = get_streams(TopicFilter, StartTime),

    %% Discard streams that are already replayed and init new
    {NewStreamsWRanks, RankProgress} = emqx_ds_shared_sub_leader_rank_progress:add_streams(
        StreamsWRanks,
        store_get_rank_progress(Data0)
    ),
    {Data1, VanishedStreams} = update_progresses(Data0, NewStreamsWRanks, TopicFilter, StartTime),
    Data2 = store_put_rank_progress(Data1, RankProgress),
    Data3 = remove_vanished_streams(Data2, VanishedStreams),
    DesiredCounts = desired_stream_count_for_ssubscribers(Data3),
    Data4 = revoke_streams(Data3, DesiredCounts),
    Data5 = assign_streams(Data4, DesiredCounts),
    ?SLOG(info, #{
        msg => leader_renew_streams,
        topic_filter => TopicFilter,
        new_streams => length(NewStreamsWRanks)
    }),
    Data5.

update_progresses(Data0, NewStreamsWRanks, TopicFilter, StartTime) ->
    ExistingStreams = store_setof_streams(Data0),
    Data = lists:foldl(
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
        Data0,
        NewStreamsWRanks
    ),
    VanishedStreams = lists:foldl(
        fun({_Rank, Stream}, Acc) -> sets:del_element(Stream, Acc) end,
        ExistingStreams,
        NewStreamsWRanks
    ),
    {Data, sets:to_list(VanishedStreams)}.

%% We just remove disappeared streams from anywhere.
%%
%% If streams disappear from DS during leader being in replaying state
%% this is an abnormal situation (we should receive `end_of_stream` first),
%% but clients are unlikely to report any progress on them.
%%
%% If streams disappear after long leader sleep, it is a normal situation.
%% This removal will be a part of initialization before any agents connect.
remove_vanished_streams(Data, VanishedStreams) ->
    finalize_streams(Data, VanishedStreams).

finalize_streams(#{stream_owners := StreamOwners0} = Data, [Stream | RestStreams]) ->
    case StreamOwners0 of
        #{Stream := #stream_ownership{status = ?stream_revoked}} ->
            finalize_streams(Data, RestStreams);
        #{
            Stream := #stream_ownership{status = _OtherStatus, ssubscriber_id = SSubscriberId} =
                Ownership0
        } ->
            emqx_ds_shared_sub_proto:send_to_ssubscriber(
                SSubscriberId,
                ?leader_revoked(this_leader(Data), Stream)
            ),
            Ownership1 = Ownership0#{status => ?stream_revoked},
            StreamOwners1 = StreamOwners0#{Stream => Ownership1},
            finalize_streams(
                Data#{
                    stream_owners => StreamOwners1
                },
                RestStreams
            );
        _ ->
            finalize_streams(Data, RestStreams)
    end.

%% We revoke streams from ssubscribers that have too many streams (> desired_stream_count_per_agent).
%% We revoke only from stable subscribers — those not having streams in transient states.
%% After revoking, no unassigned streams appear. Streams will become unassigned
%% only after ssubscriber reports them back as revoked.
revoke_streams(Data0, DesiredCounts) ->
    StableSSubscribers = stable_ssubscribers(Data0),
    maps:fold(
        fun(SSubscriberId, GrantedStreams, DataAcc) ->
            DesiredCount = maps:get(SSubscriberId, DesiredCounts),
            revoke_excess_streams_from_ssubscriber(
                DataAcc, SSubscriberId, GrantedStreams, DesiredCount
            )
        end,
        Data0,
        StableSSubscribers
    ).

revoke_excess_streams_from_ssubscriber(Data, SSubscriberId, GrantedStreams, DesiredCount) ->
    CurrentCount = length(GrantedStreams),
    RevokeCount = CurrentCount - DesiredCount,
    case RevokeCount > 0 of
        false ->
            Data;
        true ->
            ?tp(debug, shared_sub_leader_revoke_streams, #{
                ssubscriber_id => SSubscriberId,
                current_count => CurrentCount,
                revoke_count => RevokeCount,
                desired_count => DesiredCount
            }),
            StreamsToRevoke = select_streams_for_revoke(Data, GrantedStreams, RevokeCount),
            revoke_streams_from_ssubscriber(Data, SSubscriberId, StreamsToRevoke)
    end.

select_streams_for_revoke(_Data, GrantedStreams, RevokeCount) ->
    %% TODO
    %% Some intellectual logic should be used regarding:
    %% * shard ids (better do not mix shards in the same agent);
    %% * stream stats (how much data was replayed from stream),
    %%   heavy streams should be distributed across different agents);
    %% * data locality (agents better preserve streams with data available on the agent's node)
    lists:sublist(shuffle(GrantedStreams), RevokeCount).

%% We assign streams to agents that have too few streams (< desired_stream_count_per_agent).
%% We assign only to stable subscribers — those not having streams in transient states.
assign_streams(Data0, DesiredCounts) ->
    StableSSubscribers = stable_ssubscribers(Data0),
    maps:fold(
        fun(SSubscriberId, GrantedStreams, DataAcc) ->
            DesiredCount = maps:get(SSubscriberId, DesiredCounts),
            assign_lacking_streams(DataAcc, SSubscriberId, GrantedStreams, DesiredCount)
        end,
        Data0,
        StableSSubscribers
    ).

assign_lacking_streams(Data0, SSubscriberId, GrantedStreams, DesiredCount) ->
    CurrentCount = length(GrantedStreams),
    AssignCount = DesiredCount - CurrentCount,
    case AssignCount > 0 of
        false ->
            Data0;
        true ->
            ?tp(debug, shared_sub_leader_assign_streams, #{
                ssubscriber_id => SSubscriberId,
                current_count => CurrentCount,
                assign_count => AssignCount,
                desired_count => DesiredCount
            }),
            StreamsToAssign = select_streams_for_assign(Data0, SSubscriberId, AssignCount),
            assign_streams_to_ssubscriber(Data0, SSubscriberId, StreamsToAssign)
    end.

select_streams_for_assign(Data0, _SSubscriberId, AssignCount) ->
    %% TODO
    %% Some intellectual logic should be used. See `select_streams_for_revoke/3`.
    UnassignedStreams = unassigned_streams(Data0),
    lists:sublist(shuffle(UnassignedStreams), AssignCount).

revoke_streams_from_ssubscriber(Data, SSubscriberId, StreamsToRevoke) ->
    lists:foldl(
        fun(Stream, DataAcc0) ->
            ok = emqx_ds_shared_sub_proto:send_to_ssubscriber(
                SSubscriberId,
                ?leader_revoke(this_leader(Data), Stream)
            ),
            set_stream_status(DataAcc0, Stream, ?stream_revoking)
        end,
        Data,
        StreamsToRevoke
    ).

assign_streams_to_ssubscriber(Data, SSubscriberId, StreamsToAssign) ->
    lists:foldl(
        fun(Stream, DataAcc) ->
            Progress = stream_progress(DataAcc, Stream),
            emqx_ds_shared_sub_proto:send_to_ssubscriber(
                SSubscriberId,
                ?leader_grant(this_leader(DataAcc), Stream, Progress)
            ),
            set_stream_status(DataAcc, Stream, ?stream_granting)
        end,
        Data,
        StreamsToAssign
    ).

%%--------------------------------------------------------------------
%% Timeout actions

do_timeout_actions(Data0) ->
    Data1 = drop_timeout_ssubscribers(Data0),
    Data2 = renew_transient_streams(Data1),
    Data2.

drop_timeout_ssubscribers(#{ssubscribers := SSubscribers} = Data) ->
    Now = now_ms_monotonic(),
    map:foldl(
        fun(SSubscriberId, #{validity_deadline := Deadline}, DataAcc) ->
            case Deadline < Now of
                true ->
                    ?tp(debug, shared_sub_leader_drop_timeout_ssubscriber, #{
                        ssubscriber_id => SSubscriberId,
                        deadline => Deadline,
                        now => Now
                    }),
                    drop_invalidate_ssubscriber(DataAcc, SSubscriberId);
                false ->
                    DataAcc
            end
        end,
        Data,
        SSubscribers
    ).

renew_transient_streams(#{stream_owners := StreamOwners} = Data) ->
    Leader = this_leader(Data),
    maps:foreach(
        fun
            (Stream, #stream_ownership{status = ?stream_granting, ssubscriber_id = SSubscriberId}) ->
                Progress = stream_progress(Data, Stream),
                emqx_ds_shared_sub_proto:send_to_ssubscriber(
                    SSubscriberId,
                    ?leader_grant(Leader, Stream, Progress)
                );
            (Stream, #stream_ownership{status = ?stream_revoking, ssubscriber_id = SSubscriberId}) ->
                emqx_ds_shared_sub_proto:send_to_ssubscriber(
                    SSubscriberId,
                    ?leader_revoke(Leader, Stream)
                );
            (Stream, #stream_ownership{status = ?stream_revoked, ssubscriber_id = SSubscriberId}) ->
                emqx_ds_shared_sub_proto:send_to_ssubscriber(
                    SSubscriberId,
                    ?leader_revoked(Leader, Stream)
                )
        end,
        StreamOwners
    ).

%%--------------------------------------------------------------------
%% Handle a newly connected subscriber

handle_ssubscriber_connect(
    #{group_id := GroupId, ssubscribers := SSubscribers} = Data,
    SSubscriberId
) ->
    ?SLOG(debug, #{
        msg => leader_ssubscriber_connect,
        ssubscriber => SSubscriberId,
        group_id => GroupId
    }),
    case SSubscribers of
        #{SSubscriberId := _} ->
            ?tp(debug, shared_sub_leader_ssubscriber_already_connected, #{
                ssubscriber_id => SSubscriberId
            });
        _ ->
            DesiredCounts = desired_stream_count_for_ssubscribers(Data, [
                SSubscriberId | maps:keys(SSubscribers)
            ]),
            DesiredCount = maps:get(SSubscriberId, DesiredCounts),
            assign_initial_streams_to_ssubscriber(Data, SSubscriberId, DesiredCount)
    end.

assign_initial_streams_to_ssubscriber(Data, SSubscriberId, AssignCount) ->
    InitialStreamsToAssign = select_streams_for_assign(Data, SSubscriberId, AssignCount),
    assign_streams_to_ssubscriber(Data, SSubscriberId, InitialStreamsToAssign).

%%--------------------------------------------------------------------
%% Handle ssubscriber ping

handle_ssubscriber_ping(Data0, SSubscriberId) ->
    ?SLOG(debug, #{
        msg => leader_ssubscriber_ping,
        ssubscriber => SSubscriberId
    }),
    #{ssubscribers := #{SSubscriberId := SSubscriberState0} = SSubscribers0} = Data0,
    NewDeadline = now_ms_monotonic() + ?dq_config(leader_ssubscriber_timeout_interval_ms),
    SSubscriberState1 = SSubscriberState0#{validity_deadline => NewDeadline},
    ok = emqx_ds_shared_sub_proto:send_to_ssubscriber(
        SSubscriberId,
        ?leader_ping_response(this_leader(Data0))
    ),
    Data0#{
        ssubscribers => SSubscribers0#{SSubscriberId => SSubscriberState1}
    }.

%%--------------------------------------------------------------------
%% Handle stream progress

handle_update_stream_progress(Data0, SSubscriberId, Stream, Progress) ->
    case stream_ownership(Data0, Stream) of
        #stream_ownership{status = Status, ssubscriber_id = SSubscriberId} ->
            handle_update_stream_progress(Data0, SSubscriberId, Stream, Status, Progress);
        _ ->
            drop_invalidate_ssubscriber(Data0, SSubscriberId)
    end.

handle_update_stream_progress(Data0, _SSubscriberId, Stream, Status, Progress) when
    Status =:= ?stream_granted orelse Status =:= ?stream_granting
->
    Data1 = update_stream_progress(Data0, Stream, Progress),
    Data2 = set_stream_status(Data1, Stream, ?stream_granted),
    finalize_stream_if_replayed(Data2, Stream, Progress);
handle_update_stream_progress(Data0, _SSubscriberId, Stream, ?stream_revoking, Progress) ->
    Data1 = update_stream_progress(Data0, Stream, Progress),
    finalize_stream_if_revoke_finished_or_replayed(Data1, Stream, Progress);
handle_update_stream_progress(Data, _SSubscriberId, _Stream, ?stream_revoked, _Progress) ->
    Data.

update_stream_progress(Data0, Stream, Progress) ->
    Data1 = update_store_progress(Data0, Stream, Progress),
    update_rank_progress(Data1, Stream, Progress).

update_store_progress(Data, Stream, #{iterator := end_of_stream}) ->
    store_delete_stream(Data, Stream);
update_store_progress(Data, Stream, Progress) ->
    StreamData0 = store_get_stream(Data, Stream),
    StreamData = StreamData0#{progress => Progress},
    store_put_stream(Data, Stream, StreamData).

finalize_stream_if_replayed(Data, Stream, #{iterator := end_of_stream}) ->
    finalize_streams(Data, [Stream]);
finalize_stream_if_replayed(Data, _Stream, _Progress) ->
    Data.

finalize_stream_if_revoke_finished_or_replayed(Data, Stream, #{use_finished := true}) ->
    finalize_streams(Data, [Stream]);
finalize_stream_if_revoke_finished_or_replayed(Data, Stream, #{iterator := end_of_stream}) ->
    finalize_streams(Data, [Stream]);
finalize_stream_if_revoke_finished_or_replayed(Data, _Stream, _Progress) ->
    Data.

update_rank_progress(Data, Stream, #{iterator := end_of_stream}) ->
    RankProgress0 = store_get_rank_progress(Data),
    #{rank := Rank} = store_get_stream(Data, Stream),
    RankProgress = emqx_ds_shared_sub_leader_rank_progress:set_replayed(
        {Rank, Stream}, RankProgress0
    ),
    store_put_rank_progress(Data, RankProgress);
update_rank_progress(Data, _Stream, _Progress) ->
    Data.

set_stream_status(#{stream_owners := StreamOwners} = Data, Stream, Status) ->
    #{Stream := Ownership0} = StreamOwners,
    Ownership1 = Ownership0#stream_ownership{status = Status},
    StreamOwners1 = StreamOwners#{Stream => Ownership1},
    Data#{
        stream_owners => StreamOwners1
    }.

set_stream_free(#{stream_owners := StreamOwners} = Data, Stream) ->
    StreamOwners1 = maps:remove(Stream, StreamOwners),
    Data#{
        stream_owners => StreamOwners1
    }.

%%--------------------------------------------------------------------
%% Disconnect agent gracefully

disconnect_ssubscriber(Data0, SSubscriberId, StreamProgresses) ->
    Data1 = maps:fold(
        fun({Stream, Progress}, DataAcc0) ->
            case stream_ownership(DataAcc0, Stream) of
                SSubscriberId ->
                    DataAcc1 = update_stream_progress(DataAcc0, Stream, Progress),
                    set_stream_free(DataAcc1, Stream);
                _ ->
                    DataAcc0
            end
        end,
        Data0,
        StreamProgresses
    ),
    ok = emqx_ds_shared_sub_proto:send_to_ssubscriber(
        SSubscriberId,
        ?leader_disconnect_response(this_leader(Data1))
    ),
    drop_ssubscriber(Data1, SSubscriberId).

%%--------------------------------------------------------------------
%% Finalize revoked stream

handle_revoke_finished(Data0, SSubscriberId, Stream) ->
    case stream_ownership(Data0, Stream) of
        #stream_ownership{status = ?stream_revoked, ssubscriber_id = SSubscriberId} ->
            set_stream_free(Data0, Stream);
        _ ->
            drop_invalidate_ssubscriber(Data0, SSubscriberId)
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

unassigned_streams(#{stream_owners := StreamOwners} = Data) ->
    Streams = store_setof_streams(Data),
    sets:to_list(sets:subtract(Streams, StreamOwners)).

desired_stream_count_for_ssubscribers(#{ssubscribers := SSubscribers} = Data) ->
    desired_stream_count_for_ssubscribers(Data, maps:keys(SSubscribers)).

desired_stream_count_for_ssubscribers(_Data, []) ->
    0;
desired_stream_count_for_ssubscribers(Data, SSubscriberIds) ->
    StreamCount = store_num_streams(Data),
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

stream_progress(Data, Stream) ->
    StreamData = store_get_stream(Data, Stream),
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

this_leader(_Data) ->
    self().

drop_ssubscriber(
    #{ssubscribers := SSubscribers0, stream_owners := StreamOwners0} = Data, SSubscriberIdDrop
) ->
    ?tp(debug, shared_sub_leader_drop_ssubscriber, #{agent => SSubscriberIdDrop}),
    Subscribers1 = maps:remove(SSubscriberIdDrop, SSubscribers0),
    StreamOwners1 = maps:filter(
        fun(_Stream, SSubscriberId) -> SSubscriberId =/= SSubscriberIdDrop end,
        StreamOwners0
    ),
    Data#{
        ssubscribers => Subscribers1,
        stream_owners => StreamOwners1
    }.

invalidate_subscriber(Data, SSubscriberId) ->
    ok = emqx_ds_shared_sub_proto:send_to_ssubscriber(
        SSubscriberId,
        ?leader_invalidate(this_leader(Data))
    ).

drop_invalidate_ssubscriber(Data0, SSubscriberId) ->
    Data1 = drop_ssubscriber(Data0, SSubscriberId),
    ok = invalidate_subscriber(Data1, SSubscriberId),
    Data1.

stream_ownership(Data, Stream) ->
    case Data of
        #{stream_owners := #{Stream := #stream_ownership{} = Ownership}} ->
            Ownership;
        _ ->
            undefined
    end.

with_valid_ssubscriber(#{ssubscribers := SSubscribers} = Data, SSubscriberId, Fun) ->
    Now = now_ms_monotonic(),
    case SSubscribers of
        #{SSubscriberId := #{validity_deadline := Deadline}} when Deadline > Now ->
            Fun();
        _ ->
            drop_invalidate_ssubscriber(Data, SSubscriberId)
    end.

%% SSubscribers that do not have streams in transient states.
%% The result is a map from ssubscriber_id to a list of granted streams.
stable_ssubscribers(#{ssubscribers := SSubscribers, stream_owners := StreamOwners} = _Data) ->
    maps:fold(
        fun
            (
                Stream,
                #stream_ownership{status = ?stream_granted, ssubscriber_id = SSubscriberId},
                SSubscribersAcc
            ) ->
                maps:update_with(
                    SSubscriberId,
                    fun(_SSubscriberId, Streams) -> [Stream | Streams] end,
                    SSubscribersAcc
                );
            (
                _Stream,
                #stream_ownership{status = _OtherStatus, ssubscriber_id = SSubscriberId},
                IdsAcc
            ) ->
                maps:remove(SSubscriberId, IdsAcc)
        end,
        maps:from_list(maps:keys(SSubscribers), []),
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

store_put_stream(Data = #{store := Store0}, ID, StreamData) ->
    Store = emqx_ds_shared_sub_store:put(stream, ID, StreamData, Store0),
    Data#{store := Store}.

store_delete_stream(Data = #{store := Store0}, ID) ->
    Store = emqx_ds_shared_sub_store:delete(stream, ID, Store0),
    Data#{store := Store}.

store_get_rank_progress(#{store := Store}) ->
    emqx_ds_shared_sub_store:get(rank_progress, Store).

store_put_rank_progress(Data = #{store := Store0}, RankProgress) ->
    Store = emqx_ds_shared_sub_store:set(rank_progress, RankProgress, Store0),
    Data#{store := Store}.

store_get_start_time(#{store := Store}) ->
    Props = emqx_ds_shared_sub_store:get(properties, Store),
    maps:get(start_time, Props).

store_num_streams(#{store := Store}) ->
    emqx_ds_shared_sub_store:size(stream, Store).

store_setof_streams(#{store := Store}) ->
    Acc0 = sets:new([{version, 2}]),
    FoldFun = fun(Stream, _StreamData, Acc) -> sets:add_element(Stream, Acc) end,
    emqx_ds_shared_sub_store:fold(stream, FoldFun, Acc0, Store).
