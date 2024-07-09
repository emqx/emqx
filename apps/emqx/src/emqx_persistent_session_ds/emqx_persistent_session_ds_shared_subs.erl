%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module
%% * handles creation and management of _shared_ subscriptions for the session;
%% * provides streams to the session;
%% * handles progress of stream replay.
%%
%% The logic is quite straightforward; most of the parts resemble the logic of the
%% `emqx_persistent_session_ds_subs` (subscribe/unsubscribe) and
%% `emqx_persistent_session_ds_scheduler` (providing new streams),
%% but some data is sent or received from the `emqx_persistent_session_ds_shared_subs_agent`
%% which communicates with remote shared subscription leaders.
%%
%% A tricky part is the concept of "scheduled actions". When we unsubscribe from a topic
%% we may have some streams that have unacked messages. So we do not have a reliable
%% progress for them. Sending the current progress to the leader and disconnecting
%% will lead to the duplication of messages. So after unsubscription, we need to wait
%% some time until all streams are acked, and only then we disconnect from the leader.
%%
%% For this purpose we have the `scheduled_actions` map in the state of the module.
%% We preserve there the streams that we need to wait for and collect their progress.
%% We also use `scheduled_actions` for resubscriptions. If a client quickly resubscribes
%% after unsubscription, we may still have the mentioned streams unacked. If we abandon
%% them, just connect to the leader, then it may lease us the same streams again, but with
%% the previous progress. So messages may duplicate.

-module(emqx_persistent_session_ds_shared_subs).

-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("session_internals.hrl").

-include_lib("emqx/include/emqx_persistent_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    new/1,
    open/2,

    on_subscribe/3,
    on_unsubscribe/4,
    on_disconnect/2,

    on_streams_replay/2,
    on_info/3,

    renew_streams/2,
    to_map/2
]).

-define(EPOCH_BITS, 15).

-define(schedule_subscribe, schedule_subscribe).
-define(schedule_unsubscribe, schedule_unsubscribe).

-type stream_key() :: {emqx_persistent_session_ds:id(), emqx_ds:stream()}.

-type scheduled_action_type() ::
    {?schedule_subscribe, emqx_types:subopts()} | ?schedule_unsubscribe.

-type agent_stream_progress() :: #{
    stream := emqx_ds:stream(),
    progress := progress(),
    use_finished := boolean()
}.

-type progress() ::
    #{
        iterator := emqx_ds:iterator()
    }.

-type scheduled_action() :: #{
    type := scheduled_action_type(),
    stream_keys_to_wait := [stream_key()],
    progresses := [agent_stream_progress()]
}.

-type t() :: #{
    agent := emqx_persistent_session_ds_shared_subs_agent:t(),
    scheduled_actions := #{
        share_topic_filter() => scheduled_action()
    }
}.
-type share_topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type opts() :: #{
    session_id := emqx_persistent_session_ds:id()
}.

-define(rank_x, rank_shared).
-define(rank_y, 0).

-export_type([
    progress/0,
    agent_stream_progress/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% new

-spec new(opts()) -> t().
new(Opts) ->
    #{
        agent => emqx_persistent_session_ds_shared_subs_agent:new(
            agent_opts(Opts)
        ),
        scheduled_actions => #{}
    }.

%%--------------------------------------------------------------------
%% open

-spec open(emqx_persistent_session_ds_state:t(), opts()) ->
    {ok, emqx_persistent_session_ds_state:t(), t()}.
open(S, Opts) ->
    SharedSubscriptions = fold_shared_subs(
        fun(#share{} = TopicFilter, Sub, Acc) ->
            [{TopicFilter, to_agent_subscription(S, Sub)} | Acc]
        end,
        [],
        S
    ),
    Agent = emqx_persistent_session_ds_shared_subs_agent:open(
        SharedSubscriptions, agent_opts(Opts)
    ),
    SharedSubS = #{agent => Agent, scheduled_actions => #{}},
    {ok, S, SharedSubS}.

%%--------------------------------------------------------------------
%% on_subscribe

-spec on_subscribe(
    share_topic_filter(),
    emqx_types:subopts(),
    emqx_persistent_session_ds:session()
) -> {ok, emqx_persistent_session_ds_state:t(), t()} | {error, emqx_types:reason_code()}.
on_subscribe(TopicFilter, SubOpts, #{s := S} = Session) ->
    Subscription = emqx_persistent_session_ds_state:get_subscription(TopicFilter, S),
    on_subscribe(Subscription, TopicFilter, SubOpts, Session).

%%--------------------------------------------------------------------
%% on_subscribe internal functions

on_subscribe(undefined, TopicFilter, SubOpts, #{props := Props, s := S} = Session) ->
    #{max_subscriptions := MaxSubscriptions} = Props,
    case emqx_persistent_session_ds_state:n_subscriptions(S) < MaxSubscriptions of
        true ->
            create_new_subscription(TopicFilter, SubOpts, Session);
        false ->
            {error, ?RC_QUOTA_EXCEEDED}
    end;
on_subscribe(Subscription, TopicFilter, SubOpts, Session) ->
    update_subscription(Subscription, TopicFilter, SubOpts, Session).

-dialyzer({nowarn_function, create_new_subscription/3}).
create_new_subscription(TopicFilter, SubOpts, #{
    s := S0,
    shared_sub_s := #{agent := Agent} = SharedSubS0,
    props := Props
}) ->
    case
        emqx_persistent_session_ds_shared_subs_agent:can_subscribe(
            Agent, TopicFilter, SubOpts
        )
    of
        ok ->
            #{upgrade_qos := UpgradeQoS} = Props,
            {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
            {SStateId, S2} = emqx_persistent_session_ds_state:new_id(S1),
            SState = #{
                parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts
            },
            S3 = emqx_persistent_session_ds_state:put_subscription_state(
                SStateId, SState, S2
            ),
            Subscription = #{
                id => SubId,
                current_state => SStateId,
                start_time => now_ms()
            },
            S = emqx_persistent_session_ds_state:put_subscription(
                TopicFilter, Subscription, S3
            ),
            SharedSubS = schedule_subscribe(SharedSubS0, TopicFilter, SubOpts),
            {ok, S, SharedSubS};
        {error, _} = Error ->
            Error
    end.

update_subscription(#{current_state := SStateId0, id := SubId} = Sub0, TopicFilter, SubOpts, #{
    s := S0, shared_sub_s := SharedSubS, props := Props
}) ->
    #{upgrade_qos := UpgradeQoS} = Props,
    SState = #{parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts},
    case emqx_persistent_session_ds_state:get_subscription_state(SStateId0, S0) of
        SState ->
            %% Client resubscribed with the same parameters:
            {ok, S0, SharedSubS};
        _ ->
            %% Subsription parameters changed:
            {SStateId, S1} = emqx_persistent_session_ds_state:new_id(S0),
            S2 = emqx_persistent_session_ds_state:put_subscription_state(
                SStateId, SState, S1
            ),
            Sub = Sub0#{current_state => SStateId},
            S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, Sub, S2),
            {ok, S, SharedSubS}
    end.

-dialyzer({nowarn_function, schedule_subscribe/3}).
schedule_subscribe(
    #{agent := Agent0, scheduled_actions := ScheduledActions0} = SharedSubS0, TopicFilter, SubOpts
) ->
    case ScheduledActions0 of
        #{TopicFilter := ScheduledAction} ->
            ScheduledActions1 = ScheduledActions0#{
                TopicFilter => ScheduledAction#{type => {?schedule_subscribe, SubOpts}}
            },
            ?tp(warning, shared_subs_schedule_subscribe_override, #{
                topic_filter => TopicFilter,
                new_type => {?schedule_subscribe, SubOpts},
                old_action => format_schedule_action(ScheduledAction)
            }),
            SharedSubS0#{scheduled_actions := ScheduledActions1};
        _ ->
            ?tp(warning, shared_subs_schedule_subscribe_new, #{
                topic_filter => TopicFilter, subopts => SubOpts
            }),
            Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_subscribe(
                Agent0, TopicFilter, SubOpts
            ),
            SharedSubS0#{agent => Agent1}
    end.

%%--------------------------------------------------------------------
%% on_unsubscribe

-spec on_unsubscribe(
    emqx_persistent_session_ds:id(),
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds_state:t(),
    t()
) ->
    {ok, emqx_persistent_session_ds_state:t(), t(), emqx_persistent_session_ds:subscription()}
    | {error, emqx_types:reason_code()}.
on_unsubscribe(SessionId, TopicFilter, S0, SharedSubS0) ->
    case lookup(TopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        #{id := SubId} = Subscription ->
            ?tp(persistent_session_ds_subscription_delete, #{
                session_id => SessionId, topic_filter => TopicFilter
            }),
            S = emqx_persistent_session_ds_state:del_subscription(TopicFilter, S0),
            SharedSubS = schedule_unsubscribe(S, SharedSubS0, SubId, TopicFilter),
            {ok, S, SharedSubS, Subscription}
    end.

%%--------------------------------------------------------------------
%% on_unsubscribe internal functions

schedule_unsubscribe(
    S, #{scheduled_actions := ScheduledActions0} = SharedSubS0, UnsubscridedSubId, TopicFilter
) ->
    case ScheduledActions0 of
        #{TopicFilter := ScheduledAction0} ->
            ScheduledAction1 = ScheduledAction0#{type => ?schedule_unsubscribe},
            ScheduledActions1 = ScheduledActions0#{
                TopicFilter => ScheduledAction1
            },
            ?tp(warning, shared_subs_schedule_unsubscribe_override, #{
                topic_filter => TopicFilter,
                new_type => ?schedule_unsubscribe,
                old_action => format_schedule_action(ScheduledAction0)
            }),
            SharedSubS0#{scheduled_actions := ScheduledActions1};
        _ ->
            StreamKeys = stream_keys_by_sub_id(S, UnsubscridedSubId),
            ScheduledActions1 = ScheduledActions0#{
                TopicFilter => #{
                    type => ?schedule_unsubscribe,
                    stream_keys_to_wait => StreamKeys,
                    progresses => []
                }
            },
            ?tp(warning, shared_subs_schedule_unsubscribe_new, #{
                topic_filter => TopicFilter,
                stream_keys => format_stream_keys(StreamKeys)
            }),
            SharedSubS0#{scheduled_actions := ScheduledActions1}
    end.

%%--------------------------------------------------------------------
%% renew_streams

-spec renew_streams(emqx_persistent_session_ds_state:t(), t()) ->
    {emqx_persistent_session_ds_state:t(), t()}.
renew_streams(S0, #{agent := Agent0, scheduled_actions := ScheduledActions} = SharedSubS0) ->
    {StreamLeaseEvents, Agent1} = emqx_persistent_session_ds_shared_subs_agent:renew_streams(
        Agent0
    ),
    StreamLeaseEvents =/= [] andalso
        ?tp(warning, shared_subs_new_stream_lease_events, #{
            stream_lease_events => format_lease_events(StreamLeaseEvents)
        }),
    S1 = lists:foldl(
        fun
            (#{type := lease} = Event, S) -> accept_stream(Event, S, ScheduledActions);
            (#{type := revoke} = Event, S) -> revoke_stream(Event, S)
        end,
        S0,
        StreamLeaseEvents
    ),
    SharedSubS1 = SharedSubS0#{agent => Agent1},
    {S1, SharedSubS1}.

%%--------------------------------------------------------------------
%% renew_streams internal functions

accept_stream(#{topic_filter := TopicFilter} = Event, S, ScheduledActions) ->
    %% If we have a pending action (subscribe or unsubscribe) for this topic filter,
    %% we should not accept a stream and start replaying it. We won't use it anyway:
    %% * if subscribe is pending, we will reset agent obtain a new lease
    %% * if unsubscribe is pending, we will drop connection
    case ScheduledActions of
        #{TopicFilter := _Action} ->
            S;
        _ ->
            accept_stream(Event, S)
    end.

accept_stream(
    #{
        topic_filter := TopicFilter,
        stream := Stream,
        progress := #{iterator := Iterator} = _Progress
    } = _Event,
    S0
) ->
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S0) of
        undefined ->
            %% We unsubscribed
            S0;
        #{id := SubId, current_state := SStateId} ->
            Key = {SubId, Stream},
            NeedCreateStream =
                case emqx_persistent_session_ds_state:get_stream(Key, S0) of
                    undefined ->
                        true;
                    #srs{unsubscribed = true} ->
                        true;
                    _SRS ->
                        false
                end,
            case NeedCreateStream of
                true ->
                    NewSRS =
                        #srs{
                            rank_x = ?rank_x,
                            rank_y = ?rank_y,
                            it_begin = Iterator,
                            it_end = Iterator,
                            sub_state_id = SStateId
                        },
                    S1 = emqx_persistent_session_ds_state:put_stream(Key, NewSRS, S0),
                    S1;
                false ->
                    S0
            end
    end.

revoke_stream(
    #{topic_filter := TopicFilter, stream := Stream}, S0
) ->
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S0) of
        undefined ->
            %% This should not happen.
            %% Agent should have received unsubscribe callback
            %% and should not have revoked this stream
            S0;
        #{id := SubId} ->
            Key = {SubId, Stream},
            case emqx_persistent_session_ds_state:get_stream(Key, S0) of
                undefined ->
                    S0;
                SRS0 ->
                    SRS1 = SRS0#srs{unsubscribed = true},
                    S1 = emqx_persistent_session_ds_state:put_stream(Key, SRS1, S0),
                    S1
            end
    end.

%%--------------------------------------------------------------------
%% on_streams_replay

-spec on_streams_replay(
    emqx_persistent_session_ds_state:t(),
    t()
) -> {emqx_persistent_session_ds_state:t(), t()}.
on_streams_replay(S0, SharedSubS0) ->
    {S1, #{agent := Agent0, scheduled_actions := ScheduledActions0} = SharedSubS1} =
        renew_streams(S0, SharedSubS0),

    Progresses = all_stream_progresses(S1, Agent0),
    Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_stream_progress(
        Agent0, Progresses
    ),
    {Agent2, ScheduledActions1} = run_scheduled_actions(S1, Agent1, ScheduledActions0),
    SharedSubS2 = SharedSubS1#{
        agent => Agent2,
        scheduled_actions => ScheduledActions1
    },
    {S1, SharedSubS2}.

%%--------------------------------------------------------------------
%% on_streams_replay internal functions

all_stream_progresses(S, Agent) ->
    all_stream_progresses(S, Agent, _NeedUnacked = false).

all_stream_progresses(S, _Agent, NeedUnacked) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    fold_shared_stream_states(
        fun(
            #share{group = Group},
            Stream,
            SRS,
            ProgressesAcc0
        ) ->
            case
                is_stream_started(CommQos1, CommQos2, SRS) and
                    (NeedUnacked or is_stream_fully_acked(CommQos1, CommQos2, SRS))
            of
                true ->
                    StreamProgress = stream_progress(CommQos1, CommQos2, Stream, SRS),
                    maps:update_with(
                        Group,
                        fun(Progresses) -> [StreamProgress | Progresses] end,
                        [StreamProgress],
                        ProgressesAcc0
                    );
                false ->
                    ProgressesAcc0
            end
        end,
        #{},
        S
    ).

run_scheduled_actions(S, Agent, ScheduledActions) ->
    maps:fold(
        fun(TopicFilter, Action0, {AgentAcc0, ScheduledActionsAcc}) ->
            case run_scheduled_action(S, AgentAcc0, TopicFilter, Action0) of
                {ok, AgentAcc1} ->
                    {AgentAcc1, maps:remove(TopicFilter, ScheduledActionsAcc)};
                {continue, Action1} ->
                    {AgentAcc0, ScheduledActionsAcc#{TopicFilter => Action1}}
            end
        end,
        {Agent, ScheduledActions},
        ScheduledActions
    ).

run_scheduled_action(
    S,
    Agent0,
    #share{group = Group} = TopicFilter,
    #{type := Type, stream_keys_to_wait := StreamKeysToWait0, progresses := Progresses0} = Action
) ->
    StreamKeysToWait1 = filter_unfinished_streams(S, StreamKeysToWait0),
    Progresses1 = stream_progresses(S, StreamKeysToWait0 -- StreamKeysToWait1) ++ Progresses0,
    case StreamKeysToWait1 of
        [] ->
            ?tp(warning, shared_subs_schedule_action_complete, #{
                topic_filter => TopicFilter,
                progresses => format_stream_progresses(Progresses1),
                type => Type
            }),
            %% Regular progress won't se unsubscribed streams, so we need to
            %% send the progress explicitly.
            Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_stream_progress(
                Agent0, #{Group => Progresses1}
            ),
            case Type of
                {?schedule_subscribe, SubOpts} ->
                    {ok,
                        emqx_persistent_session_ds_shared_subs_agent:on_subscribe(
                            Agent1, TopicFilter, SubOpts
                        )};
                ?schedule_unsubscribe ->
                    {ok,
                        emqx_persistent_session_ds_shared_subs_agent:on_unsubscribe(
                            Agent1, TopicFilter, Progresses1
                        )}
            end;
        _ ->
            Action1 = Action#{stream_keys_to_wait => StreamKeysToWait1, progresses => Progresses1},
            ?tp(warning, shared_subs_schedule_action_continue, #{
                topic_filter => TopicFilter,
                new_action => format_schedule_action(Action1)
            }),
            {continue, Action1}
    end.

filter_unfinished_streams(S, StreamKeysToWait) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    lists:filter(
        fun(Key) ->
            case emqx_persistent_session_ds_state:get_stream(Key, S) of
                undefined ->
                    %% This should not happen: we should see any stream
                    %% in completed state before deletion
                    true;
                SRS ->
                    not is_stream_fully_acked(CommQos1, CommQos2, SRS)
            end
        end,
        StreamKeysToWait
    ).

stream_progresses(S, StreamKeys) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    lists:map(
        fun({_SubId, Stream} = Key) ->
            SRS = emqx_persistent_session_ds_state:get_stream(Key, S),
            stream_progress(CommQos1, CommQos2, Stream, SRS)
        end,
        StreamKeys
    ).

%%--------------------------------------------------------------------
%% on_disconnect

on_disconnect(S0, #{agent := Agent0} = SharedSubS0) ->
    S1 = revoke_all_streams(S0),
    Progresses = all_stream_progresses(S1, Agent0, _NeedUnacked = true),
    Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_disconnect(Agent0, Progresses),
    SharedSubS1 = SharedSubS0#{agent => Agent1, scheduled_actions => #{}},
    {S1, SharedSubS1}.

%%--------------------------------------------------------------------
%% on_disconnect helpers

revoke_all_streams(S0) ->
    fold_shared_stream_states(
        fun(TopicFilter, Stream, _SRS, S) ->
            revoke_stream(#{topic_filter => TopicFilter, stream => Stream}, S)
        end,
        S0,
        S0
    ).

%%--------------------------------------------------------------------
%% on_info

-spec on_info(emqx_persistent_session_ds_state:t(), t(), term()) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_info(S, #{agent := Agent0} = SharedSubS0, Info) ->
    Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_info(Agent0, Info),
    SharedSubS1 = SharedSubS0#{agent => Agent1},
    {S, SharedSubS1}.

%%--------------------------------------------------------------------
%% to_map

-spec to_map(emqx_persistent_session_ds_state:t(), t()) -> map().
to_map(_S, _SharedSubS) ->
    %% TODO
    #{}.

%%--------------------------------------------------------------------
%% Generic helpers
%%--------------------------------------------------------------------

lookup(TopicFilter, S) ->
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S) of
        Sub = #{current_state := SStateId} ->
            case emqx_persistent_session_ds_state:get_subscription_state(SStateId, S) of
                #{subopts := SubOpts} ->
                    Sub#{subopts => SubOpts};
                undefined ->
                    undefined
            end;
        undefined ->
            undefined
    end.

stream_keys_by_sub_id(S, MatchSubId) ->
    emqx_persistent_session_ds_state:fold_streams(
        fun({SubId, _Stream} = StreamKey, _SRS, StreamKeys) ->
            case SubId of
                MatchSubId ->
                    [StreamKey | StreamKeys];
                _ ->
                    StreamKeys
            end
        end,
        [],
        S
    ).

stream_progress(
    CommQos1,
    CommQos2,
    Stream,
    #srs{
        it_end = EndIt,
        it_begin = BeginIt,
        first_seqno_qos1 = StartQos1,
        first_seqno_qos2 = StartQos2
    } = SRS
) ->
    Qos1Acked = n_acked(?QOS_1, CommQos1, StartQos1),
    Qos2Acked = n_acked(?QOS_2, CommQos2, StartQos2),
    Iterator =
        case is_stream_fully_acked(CommQos1, CommQos2, SRS) of
            true ->
                EndIt;
            false ->
                emqx_ds_skipping_iterator:update_or_new(
                    BeginIt, Qos1Acked, Qos2Acked
                )
        end,
    #{
        stream => Stream,
        progress => #{
            iterator => Iterator
        },
        use_finished => is_use_finished(SRS)
    }.

fold_shared_subs(Fun, Acc, S) ->
    emqx_persistent_session_ds_state:fold_subscriptions(
        fun
            (#share{} = TopicFilter, Sub, Acc0) -> Fun(TopicFilter, Sub, Acc0);
            (_, _Sub, Acc0) -> Acc0
        end,
        Acc,
        S
    ).

fold_shared_stream_states(Fun, Acc, S) ->
    %% TODO
    %% Optimize or cache
    TopicFilters = fold_shared_subs(
        fun
            (#share{} = TopicFilter, #{id := Id} = _Sub, Acc0) ->
                Acc0#{Id => TopicFilter};
            (_, _, Acc0) ->
                Acc0
        end,
        #{},
        S
    ),
    emqx_persistent_session_ds_state:fold_streams(
        fun({SubId, Stream}, SRS, Acc0) ->
            case TopicFilters of
                #{SubId := TopicFilter} ->
                    Fun(TopicFilter, Stream, SRS, Acc0);
                _ ->
                    Acc0
            end
        end,
        Acc,
        S
    ).

to_agent_subscription(_S, Subscription) ->
    maps:with([start_time], Subscription).

agent_opts(#{session_id := SessionId}) ->
    #{session_id => SessionId}.

-dialyzer({nowarn_function, now_ms/0}).
now_ms() ->
    erlang:system_time(millisecond).

is_use_finished(#srs{unsubscribed = Unsubscribed}) ->
    Unsubscribed.

is_stream_started(CommQos1, CommQos2, #srs{first_seqno_qos1 = Q1, last_seqno_qos1 = Q2}) ->
    (CommQos1 >= Q1) or (CommQos2 >= Q2).

is_stream_fully_acked(_, _, #srs{
    first_seqno_qos1 = Q1, last_seqno_qos1 = Q1, first_seqno_qos2 = Q2, last_seqno_qos2 = Q2
}) ->
    %% Streams where the last chunk doesn't contain any QoS1 and 2
    %% messages are considered fully acked:
    true;
is_stream_fully_acked(Comm1, Comm2, #srs{last_seqno_qos1 = S1, last_seqno_qos2 = S2}) ->
    (Comm1 >= S1) andalso (Comm2 >= S2).

n_acked(Qos, A, B) ->
    max(seqno_diff(Qos, A, B), 0).

-dialyzer({nowarn_function, seqno_diff/3}).
seqno_diff(?QOS_1, A, B) ->
    %% For QoS1 messages we skip a seqno every time the epoch changes,
    %% we need to substract that from the diff:
    EpochA = A bsr ?EPOCH_BITS,
    EpochB = B bsr ?EPOCH_BITS,
    A - B - (EpochA - EpochB);
seqno_diff(?QOS_2, A, B) ->
    A - B.

%%--------------------------------------------------------------------
%% Formatters
%%--------------------------------------------------------------------

format_schedule_action(#{
    type := Type, progresses := Progresses, stream_keys_to_wait := StreamKeysToWait
}) ->
    #{
        type => Type,
        progresses => format_stream_progresses(Progresses),
        stream_keys_to_wait => format_stream_keys(StreamKeysToWait)
    }.

format_stream_progresses(Streams) ->
    lists:map(
        fun format_stream_progress/1,
        Streams
    ).

format_stream_progress(#{stream := Stream, progress := Progress} = Value) ->
    Value#{stream => format_opaque(Stream), progress => format_progress(Progress)}.

format_progress(#{iterator := Iterator} = Progress) ->
    Progress#{iterator => format_opaque(Iterator)}.

format_stream_key(beginning) -> beginning;
format_stream_key({SubId, Stream}) -> {SubId, format_opaque(Stream)}.

format_stream_keys(StreamKeys) ->
    lists:map(
        fun format_stream_key/1,
        StreamKeys
    ).

format_lease_events(Events) ->
    lists:map(
        fun format_lease_event/1,
        Events
    ).

format_lease_event(#{stream := Stream, progress := Progress} = Event) ->
    Event#{stream => format_opaque(Stream), progress => format_progress(Progress)};
format_lease_event(#{stream := Stream} = Event) ->
    Event#{stream => format_opaque(Stream)}.

format_opaque(Opaque) ->
    erlang:phash2(Opaque).
