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
%% The most complex part is stream revoking and unsubscribing. When a stream is revoked,
%% or a shared subscription is unsubscribed, a stream may be in the replaying state. So we
%% keep track of such streams a bit further till we see them in fully acked state. Then we
%% can safely report their progress to the shared subscription leader.

-module(emqx_persistent_session_ds_shared_subs).

-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("session_internals.hrl").

-include_lib("snabbkaffe/include/trace.hrl").

-export([
    new/1,
    open/2,

    on_subscribe/3,
    on_unsubscribe/5,
    on_disconnect/2,

    on_streams_replay/3,
    on_streams_gc/2,
    on_info/4,

    to_map/2
]).

%% Management API:
-export([
    cold_get_subscription/2
]).

-type subscription_id() :: emqx_persistent_session_ds:subscription_id().

-define(subscription_active, subscription_active).
-define(subscription_unsubscribed, subscription_unsubscribed).

-type subscription_status() :: ?subscription_active | ?subscription_unsubscribed.

-type agent_stream_progress() :: #{
    stream := emqx_ds:stream(),
    progress := progress(),
    use_finished := boolean()
}.

-type progress() ::
    #{
        iterator := emqx_ds:iterator()
    }.

-type t() :: #{
    agent := emqx_persistent_session_ds_shared_subs_agent:t(),
    subscriptions := #{
        subscription_id() => #{
            subscription := emqx_persistent_session_ds:subscription(),
            share_topic_filter := share_topic_filter(),
            streams_active := sets:set(emqx_ds:stream()),
            streams_revoked := sets:set(emqx_ds:stream()),
            %% The lifecycle is as follows:
            %% subscription_active -> subscription_unsubscribed -> [deleted]
            status := subscription_status()
        }
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
        subscriptions => #{}
    }.

%%--------------------------------------------------------------------
%% open

-spec open(emqx_persistent_session_ds_state:t(), opts()) ->
    {ok, emqx_persistent_session_ds_state:t(), t()}.
open(S0, Opts) ->
    {ToOpen, Subscriptions} = fold_shared_subs(
        fun(
            #share{} = ShareTopicFilter,
            #{id := SubscriptionId} = Subscription,
            {ToOpenAcc, SubscriptionsAcc}
        ) ->
            {[{ShareTopicFilter, Subscription} | ToOpenAcc], SubscriptionsAcc#{
                SubscriptionId => new_subscription(Subscription, ShareTopicFilter)
            }}
        end,
        {[], #{}},
        S0
    ),
    Agent = emqx_persistent_session_ds_shared_subs_agent:open(
        ToOpen, agent_opts(Opts)
    ),
    SharedSubS = #{agent => Agent, subscriptions => Subscriptions},
    S1 = terminate_streams(S0),
    {ok, S1, SharedSubS}.

new_subscription(Subscription, ShareTopicFilter) ->
    #{
        subscription => Subscription,
        share_topic_filter => ShareTopicFilter,
        streams_active => sets:new([{version, 2}]),
        streams_revoked => sets:new([{version, 2}]),
        status => ?subscription_active
    }.

%%--------------------------------------------------------------------
%% on_subscribe

-spec on_subscribe(
    share_topic_filter(),
    emqx_types:subopts(),
    emqx_persistent_session_ds:session()
) -> {ok, emqx_persistent_session_ds_state:t(), t()} | {error, emqx_types:reason_code()}.
on_subscribe(#share{} = ShareTopicFilter, SubOpts, #{s := S} = Session) ->
    Subscription = emqx_persistent_session_ds_state:get_subscription(ShareTopicFilter, S),
    on_subscribe(Subscription, ShareTopicFilter, SubOpts, Session).

%%--------------------------------------------------------------------
%% on_subscribe internal functions

on_subscribe(undefined, ShareTopicFilter, SubOpts, #{props := Props, s := S} = Session) ->
    #{max_subscriptions := MaxSubscriptions} = Props,
    case emqx_persistent_session_ds_state:n_subscriptions(S) < MaxSubscriptions of
        true ->
            create_new_subscription(ShareTopicFilter, SubOpts, Session);
        false ->
            {error, ?RC_QUOTA_EXCEEDED}
    end;
on_subscribe(Subscription, ShareTopicFilter, SubOpts, Session) ->
    update_subscription(Subscription, ShareTopicFilter, SubOpts, Session).

-dialyzer({nowarn_function, create_new_subscription/3}).
create_new_subscription(ShareTopicFilter, SubOpts, #{
    s := S0,
    shared_sub_s := #{agent := Agent0, subscriptions := Subscriptions0} = SharedSubS0,
    props := #{upgrade_qos := UpgradeQoS}
}) ->
    case
        emqx_persistent_session_ds_shared_subs_agent:can_subscribe(
            Agent0, ShareTopicFilter, SubOpts
        )
    of
        ok ->
            %% NOTE
            %% Module that manages durable shared subscription / durable queue state is
            %% also responsible for propagating corresponding routing updates.
            {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
            {SStateId, S2} = emqx_persistent_session_ds_state:new_id(S1),
            SState = #{
                parent_subscription => SubId,
                upgrade_qos => UpgradeQoS,
                subopts => SubOpts,
                share_topic_filter => ShareTopicFilter
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
                ShareTopicFilter, Subscription, S3
            ),
            Agent = emqx_persistent_session_ds_shared_subs_agent:on_subscribe(
                Agent0, SubId, ShareTopicFilter, SubOpts
            ),
            Subscriptions = Subscriptions0#{
                SubId => new_subscription(Subscription, ShareTopicFilter)
            },
            ?tp(debug, ds_shared_subs_on_subscribe_new, #{
                share_topic_filter => ShareTopicFilter,
                subscription_id => SubId,
                subscriptions => Subscriptions
            }),
            SharedSubS = SharedSubS0#{agent => Agent, subscriptions => Subscriptions},
            {ok, S, SharedSubS};
        {error, _} = Error ->
            Error
    end.

update_subscription(
    #{current_state := SStateId0, id := SubId} = Sub0, ShareTopicFilter, SubOpts, #{
        s := S0, shared_sub_s := SharedSubS, props := Props
    }
) ->
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
            S = emqx_persistent_session_ds_state:put_subscription(ShareTopicFilter, Sub, S2),
            {ok, S, SharedSubS}
    end.

%%--------------------------------------------------------------------
%% on_unsubscribe

-spec on_unsubscribe(
    emqx_persistent_session_ds:id(),
    share_topic_filter(),
    emqx_persistent_session_ds_state:t(),
    emqx_persistent_session_ds_stream_scheduler:t(),
    t()
) ->
    {ok, emqx_persistent_session_ds_state:t(), t(), emqx_persistent_session_ds:subscription()}
    | {error, emqx_types:reason_code()}.
on_unsubscribe(
    SessionId, ShareTopicFilter, S0, SchedS0, #{subscriptions := Subscriptions0} = SharedSubS0
) ->
    case lookup(ShareTopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        #{id := SubId} = Sub ->
            ?tp(debug, ds_shared_subs_on_unsubscribe, #{
                subscriptions => Subscriptions0,
                subscription_id => SubId,
                session_id => SessionId,
                share_topic_filter => ShareTopicFilter
            }),
            SubscriptionData0 = maps:get(SubId, Subscriptions0),
            #{streams_active := ActiveStreams, streams_revoked := StreamsRevoked} =
                SubscriptionData0,
            SubscriptionData1 = SubscriptionData0#{
                status => ?subscription_unsubscribed,
                streams_revoked => sets:union(ActiveStreams, StreamsRevoked),
                streams_active => sets:new([{version, 2}])
            },
            S1 = emqx_persistent_session_ds_state:del_subscription(ShareTopicFilter, S0),
            Subscriptions1 = Subscriptions0#{SubId => SubscriptionData1},
            ?tp(debug, ds_shared_subs_on_unsubscribed, #{
                subscriptions => Subscriptions1,
                subscription_id => SubId,
                session_id => SessionId,
                share_topic_filter => ShareTopicFilter
            }),
            SharedSubS1 = SharedSubS0#{subscriptions => Subscriptions1},
            {S2, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_unsubscribe(
                SubId, S1, SchedS0
            ),
            {S, SharedSubS} = on_streams_gc(S2, SharedSubS1),
            {ok, S, SchedS, SharedSubS, Sub}
    end.

%%--------------------------------------------------------------------
%% on_streams_replay

-spec on_streams_replay(emqx_persistent_session_ds_state:t(), t(), [emqx_ds:stream()]) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_streams_replay(S, SharedS, []) ->
    {S, SharedS};
%% No-op if there are no shared subscriptions
on_streams_replay(S, #{subscriptions := Subscriptions} = SharedS, _StreamKeys) when
    map_size(Subscriptions) == 0
->
    {S, SharedS};
on_streams_replay(S, SharedS, StreamKeys) ->
    report_progress(S, SharedS, StreamKeys, _NeedUnacked = false).

%%--------------------------------------------------------------------
%% on_streams_replay gc

-spec on_streams_gc(emqx_persistent_session_ds_state:t(), t()) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_streams_gc(S, SharedS) ->
    report_progress(S, SharedS, all, _NeedUnacked = false).

%%--------------------------------------------------------------------
%% on_streams_replay/on_streams_gc internal functions

report_progress(
    S, #{agent := Agent0, subscriptions := Subscriptions0} = SharedS, StreamKeySelector, NeedUnacked
) ->
    Progresses = stream_progresses(S, SharedS, StreamKeySelector, NeedUnacked),
    Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_stream_progress(
        Agent0, Progresses
    ),
    Subscriptions1 = cleanup_revoked_streams(Subscriptions0, Progresses),
    {Subscriptions, Agent} = cleanup_unsubscribed(Subscriptions1, Agent1),
    {S, SharedS#{agent => Agent, subscriptions => Subscriptions}}.

stream_progresses(S, SharedS, StreamKeySelector, NeedUnacked) ->
    StreamStates = select_stream_states(S, SharedS, StreamKeySelector),
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    lists:foldl(
        fun({{SubId, Stream}, SRS}, ProgressesAcc) ->
            ?tp(debug, ds_shared_subs_stream_progress, #{
                stream => Stream,
                srs => SRS,
                fully_acked => is_stream_fully_acked(CommQos1, CommQos2, SRS),
                comm_qos1 => CommQos1,
                comm_qos2 => CommQos2
            }),
            case
                is_stream_started(CommQos1, CommQos2, SRS) and
                    (NeedUnacked or is_stream_fully_acked(CommQos1, CommQos2, SRS))
            of
                true ->
                    StreamProgress = stream_progress(CommQos1, CommQos2, Stream, SRS),
                    maps:update_with(
                        SubId,
                        fun(Progresses) -> [StreamProgress | Progresses] end,
                        [StreamProgress],
                        ProgressesAcc
                    );
                false ->
                    ProgressesAcc
            end
        end,
        #{},
        StreamStates
    ).

cleanup_revoked_streams(Subscriptions0, Progresses) ->
    maps:fold(
        fun(SubId, SubIdProgresses, SubscriptionsAcc) ->
            #{streams_revoked := RevokedStreams0, share_topic_filter := ShareTopicFilter} =
                SubscriptionData0 = maps:get(SubId, SubscriptionsAcc),
            RevokedStreams = lists:foldl(
                fun
                    (
                        #{stream := Stream, use_finished := true, fully_acked := true},
                        RevokedStreamsAcc0
                    ) ->
                        ?tp(debug, ds_shared_subs_stream_revoked, #{
                            subscription_id => SubId,
                            share_topic_filter => ShareTopicFilter,
                            stream => Stream
                        }),
                        sets:del_element(Stream, RevokedStreamsAcc0);
                    (_, RevokedStreamsAcc0) ->
                        RevokedStreamsAcc0
                end,
                RevokedStreams0,
                SubIdProgresses
            ),
            SubscriptionData1 = SubscriptionData0#{streams_revoked => RevokedStreams},
            SubscriptionsAcc#{SubId => SubscriptionData1}
        end,
        Subscriptions0,
        Progresses
    ).

cleanup_unsubscribed(Subscriptions0, Agent0) ->
    {Subscriptions1, Agent1} = maps:fold(
        fun(
            SubId,
            #{
                status := Status,
                streams_revoked := RevokedStreams,
                share_topic_filter := ShareTopicFilter
            } = SubscriptionData,
            {SubscriptionsAcc0, AgentAcc0}
        ) ->
            RevokedStreamCount = sets:size(RevokedStreams),
            case {Status, RevokedStreamCount} of
                {?subscription_unsubscribed, 0} ->
                    ?tp(debug, ds_shared_subs_cleanup_unsubscribed, #{
                        subscription_id => SubId,
                        share_topic_filter => ShareTopicFilter
                    }),
                    SubscriptionsAcc1 = maps:remove(SubId, SubscriptionsAcc0),
                    AgentAcc1 = emqx_persistent_session_ds_shared_subs_agent:on_unsubscribe(
                        AgentAcc0, SubId, []
                    ),
                    {SubscriptionsAcc1, AgentAcc1};
                _ ->
                    {SubscriptionsAcc0#{SubId => SubscriptionData}, AgentAcc0}
            end
        end,
        {#{}, Agent0},
        Subscriptions0
    ),
    {Subscriptions1, Agent1}.

stream_progress(
    CommQos1,
    CommQos2,
    Stream,
    #srs{
        it_end = EndIt,
        it_begin = BeginIt
    } = SRS
) ->
    {FullyAcked, Iterator} =
        case is_stream_fully_acked(CommQos1, CommQos2, SRS) of
            true -> {true, EndIt};
            false -> {false, BeginIt}
        end,
    #{
        stream => Stream,
        progress => #{
            iterator => Iterator
        },
        use_finished => is_use_finished(SRS),
        fully_acked => FullyAcked
    }.

select_stream_states(S, #{subscriptions := Subscriptions} = _SharedS, all) ->
    emqx_persistent_session_ds_state:fold_streams(
        fun({SubId, _Stream} = Key, SRS, Acc0) ->
            case Subscriptions of
                #{SubId := _} ->
                    [{Key, SRS} | Acc0];
                _ ->
                    Acc0
            end
        end,
        [],
        S
    );
select_stream_states(S, #{subscriptions := Subscriptions} = _SharedS, StreamKeys) ->
    lists:filtermap(
        fun({SubId, _} = Key) ->
            case Subscriptions of
                #{SubId := _} ->
                    case emqx_persistent_session_ds_state:get_stream(Key, S) of
                        undefined -> false;
                        SRS -> {true, {Key, SRS}}
                    end;
                _ ->
                    false
            end
        end,
        StreamKeys
    ).

%%--------------------------------------------------------------------
%% on_disconnect

on_disconnect(S0, #{agent := Agent0} = SharedSubS0) ->
    S1 = terminate_streams(S0),
    Progresses = stream_progresses(S1, SharedSubS0, all, _NeedUnacked = true),
    Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_disconnect(Agent0, Progresses),
    SharedSubS1 = SharedSubS0#{agent => Agent1},
    {S1, SharedSubS1}.

%%--------------------------------------------------------------------
%% on_disconnect helpers

terminate_streams(S0) ->
    fold_shared_stream_states(
        fun(_ShareTopicFilter, SubId, Stream, Srs0, S) ->
            Srs = Srs0#srs{unsubscribed = true},
            emqx_persistent_session_ds_state:put_stream({SubId, Stream}, Srs, S)
        end,
        S0,
        S0
    ).

%%--------------------------------------------------------------------
%% on_info

-spec on_info(
    emqx_persistent_session_ds_state:t(),
    emqx_persistent_session_ds_stream_scheduler:t(),
    t(),
    term()
) ->
    {
        _NeedPush :: boolean(),
        emqx_persistent_session_ds_state:t(),
        emqx_persistent_session_ds_stream_scheduler:t(),
        t()
    }.
on_info(S0, SchedS0, #{agent := Agent0} = SharedSubS0, ?shared_sub_message(SubscriptionId, Msg)) ->
    {StreamLeaseEvents, Agent1} = emqx_persistent_session_ds_shared_subs_agent:on_info(
        Agent0, SubscriptionId, Msg
    ),
    SharedSubS1 = SharedSubS0#{agent => Agent1},
    handle_events(S0, SchedS0, SharedSubS1, StreamLeaseEvents).

-dialyzer({nowarn_function, handle_events/4}).
handle_events(S0, SchedS0, SharedS0, []) ->
    {false, S0, SchedS0, SharedS0};
handle_events(S0, SchedS0, SharedS0, StreamLeaseEvents) ->
    ?tp(debug, ds_shared_subs_new_stream_lease_events, #{
        stream_lease_events => StreamLeaseEvents
    }),
    {S1, SchedS, SharedS1} = lists:foldl(
        fun
            (#{type := lease} = Event, {S, SchedS, SharedS}) ->
                handle_lease_stream(Event, S, SchedS, SharedS);
            (#{type := revoke} = Event, {S, SchedS, SharedS}) ->
                handle_revoke_stream(Event, S, SchedS, SharedS)
        end,
        {S0, SchedS0, SharedS0},
        StreamLeaseEvents
    ),
    {S, SharedS} = on_streams_gc(S1, SharedS1),
    {true, S, SchedS, SharedS}.

handle_lease_stream(
    #{subscription_id := SubscriptionId} = Event,
    S0,
    SchedS0,
    #{subscriptions := Subscriptions0} = SharedS0
) ->
    case Subscriptions0 of
        #{
            SubscriptionId := #{
                status := ?subscription_active,
                share_topic_filter := ShareTopicFilter
            }
        } ->
            %% We need fresh subscription state
            case emqx_persistent_session_ds_state:get_subscription(ShareTopicFilter, S0) of
                undefined ->
                    %% This should not happen
                    {S0, SchedS0, SharedS0};
                Sub ->
                    {S, Sched} = add_stream_to_session(Event, Sub, S0, SchedS0),
                    SharedS = add_stream_to_subscription_data(Event, SubscriptionId, SharedS0),
                    {S, Sched, SharedS}
            end;
        _ ->
            %% We unsubscribed
            {S0, SchedS0, SharedS0}
    end.

add_stream_to_session(
    #{stream := Stream, progress := #{iterator := Iterator}} = _Event,
    #{id := SubId, current_state := SStateId} = _Sub,
    S0,
    SchedS0
) ->
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
            ?tp(debug, ds_shared_subs_add_stream_to_session, #{
                stream => Stream,
                sub_id => SubId
            }),
            S = emqx_persistent_session_ds_state:put_stream(Key, NewSRS, S0),
            {_, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_enqueue(
                _IsReplay = false, Key, NewSRS, S, SchedS0
            ),
            {S, SchedS};
        false ->
            {S0, SchedS0}
    end.

add_stream_to_subscription_data(
    #{stream := Stream}, SubscriptionId, #{subscriptions := Subscriptions0} = SharedS0
) ->
    #{streams_active := ActiveStreams0} =
        SubscriptionData0 = maps:get(SubscriptionId, Subscriptions0),
    ActiveStreams1 = sets:add_element(Stream, ActiveStreams0),
    SubscriptionData1 = SubscriptionData0#{streams_active => ActiveStreams1},
    Subscriptions1 = Subscriptions0#{SubscriptionId => SubscriptionData1},
    SharedS0#{subscriptions => Subscriptions1}.

handle_revoke_stream(
    #{subscription_id := SubscriptionId, stream := Stream} = _Event,
    S0,
    SchedS0,
    SharedS0
) ->
    {S, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_unsubscribe(
        SubscriptionId, Stream, S0, SchedS0
    ),
    SharedS = move_to_revoked_in_subscription_data(SubscriptionId, Stream, SharedS0),
    {S, SchedS, SharedS}.

move_to_revoked_in_subscription_data(
    SubscriptionId, Stream, #{subscriptions := Subscriptions0} = SharedS0
) ->
    #{streams_active := ActiveStreams0, streams_revoked := RevokedStreams0} =
        SubscriptionData0 = maps:get(SubscriptionId, Subscriptions0),
    case sets:is_element(Stream, ActiveStreams0) of
        true ->
            ActiveStreams1 = sets:del_element(Stream, ActiveStreams0),
            RevokedStreams1 = sets:add_element(Stream, RevokedStreams0),
            SubscriptionData1 = SubscriptionData0#{
                streams_active => ActiveStreams1,
                streams_revoked => RevokedStreams1
            },
            Subscriptions1 = Subscriptions0#{SubscriptionId => SubscriptionData1},
            SharedS0#{subscriptions => Subscriptions1};
        false ->
            SharedS0
    end.

%%--------------------------------------------------------------------
%% to_map

-spec to_map(emqx_persistent_session_ds_state:t(), t()) -> map().
to_map(S, _SharedSubS) ->
    fold_shared_subs(
        fun(ShareTopicFilter, _, Acc) -> Acc#{ShareTopicFilter => lookup(ShareTopicFilter, S)} end,
        #{},
        S
    ).

%%--------------------------------------------------------------------
%% cold_get_subscription

-spec cold_get_subscription(emqx_persistent_session_ds:id(), share_topic_filter()) ->
    emqx_persistent_session_ds:subscription() | undefined.
cold_get_subscription(SessionId, ShareTopicFilter) ->
    case emqx_persistent_session_ds_state:cold_get_subscription(SessionId, ShareTopicFilter) of
        [Sub = #{current_state := SStateId}] ->
            case
                emqx_persistent_session_ds_state:cold_get_subscription_state(SessionId, SStateId)
            of
                [#{subopts := Subopts}] ->
                    Sub#{subopts => Subopts};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.

%%--------------------------------------------------------------------
%% Generic helpers
%%--------------------------------------------------------------------

lookup(ShareTopicFilter, S) ->
    case emqx_persistent_session_ds_state:get_subscription(ShareTopicFilter, S) of
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

fold_shared_subs(Fun, Acc, S) ->
    emqx_persistent_session_ds_state:fold_subscriptions(
        fun
            (#share{} = ShareTopicFilter, Sub, Acc0) -> Fun(ShareTopicFilter, Sub, Acc0);
            (_, _Sub, Acc0) -> Acc0
        end,
        Acc,
        S
    ).

fold_shared_stream_states(Fun, Acc, S) ->
    ShareTopicFilters = fold_shared_subs(
        fun
            (#share{} = ShareTopicFilter, #{id := Id} = _Sub, Acc0) ->
                Acc0#{Id => ShareTopicFilter};
            (_, _, Acc0) ->
                Acc0
        end,
        #{},
        S
    ),
    emqx_persistent_session_ds_state:fold_streams(
        fun({SubId, Stream}, SRS, Acc0) ->
            case ShareTopicFilters of
                #{SubId := ShareTopicFilter} ->
                    Fun(ShareTopicFilter, SubId, Stream, SRS, Acc0);
                _ ->
                    Acc0
            end
        end,
        Acc,
        S
    ).

agent_opts(#{session_id := SessionId}) ->
    #{session_id => SessionId}.

-dialyzer({nowarn_function, now_ms/0}).
now_ms() ->
    erlang:system_time(millisecond).

is_use_finished(#srs{unsubscribed = Unsubscribed}) ->
    Unsubscribed.

is_stream_started(CommQos1, CommQos2, #srs{first_seqno_qos1 = Q1, first_seqno_qos2 = Q2}) ->
    (CommQos1 >= Q1) or (CommQos2 >= Q2).

is_stream_fully_acked(_, _, #srs{
    first_seqno_qos1 = Q1, last_seqno_qos1 = Q1, first_seqno_qos2 = Q2, last_seqno_qos2 = Q2
}) ->
    %% Streams where the last chunk doesn't contain any QoS1 and 2
    %% messages are considered fully acked:
    true;
is_stream_fully_acked(Comm1, Comm2, #srs{last_seqno_qos1 = S1, last_seqno_qos2 = S2}) ->
    (Comm1 >= S1) andalso (Comm2 >= S2).
