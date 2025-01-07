%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    agent := emqx_persistent_session_ds_shared_subs_agent:t()
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
        )
    }.

%%--------------------------------------------------------------------
%% open

-spec open(emqx_persistent_session_ds_state:t(), opts()) ->
    {ok, emqx_persistent_session_ds_state:t(), t()}.
open(S0, Opts) ->
    ToOpen = fold_shared_subs(
        fun(#share{} = ShareTopicFilter, Subscription, ToOpenAcc) ->
            [{ShareTopicFilter, Subscription} | ToOpenAcc]
        end,
        [],
        S0
    ),
    Agent = emqx_persistent_session_ds_shared_subs_agent:open(
        ToOpen, agent_opts(Opts)
    ),
    SharedSubS = #{agent => Agent},
    S1 = terminate_streams(S0),
    {ok, S1, SharedSubS}.

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
    shared_sub_s := #{agent := Agent0} = SharedSubS0,
    props := #{upgrade_qos := UpgradeQoS}
}) ->
    case
        emqx_persistent_session_ds_shared_subs_agent:pre_subscribe(
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
            ?tp(debug, ds_shared_subs_on_subscribe_new, #{
                share_topic_filter => ShareTopicFilter,
                subscription_id => SubId
            }),
            SharedSubS = SharedSubS0#{agent => Agent},
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
    SessionId, ShareTopicFilter, S0, SchedS0, #{agent := Agent0} = SharedSubS0
) ->
    case lookup(ShareTopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        #{id := SubId} = Sub ->
            ?tp(debug, ds_shared_subs_on_unsubscribe, #{
                subscription_id => SubId,
                session_id => SessionId,
                share_topic_filter => ShareTopicFilter
            }),
            S1 = emqx_persistent_session_ds_state:del_subscription(ShareTopicFilter, S0),
            ?tp(debug, ds_shared_subs_on_unsubscribed, #{
                subscription_id => SubId,
                session_id => SessionId,
                share_topic_filter => ShareTopicFilter
            }),
            {S2, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_unsubscribe(
                SubId, S1, SchedS0
            ),
            Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_unsubscribe(
                Agent0, SubId
            ),
            SharedSubS1 = SharedSubS0#{agent => Agent1},
            {S, SharedSubS} = on_streams_gc(S2, SharedSubS1),
            {ok, S, SchedS, SharedSubS, Sub}
    end.

%%--------------------------------------------------------------------
%% on_streams_replay

-dialyzer({nowarn_function, on_streams_replay/3}).
-spec on_streams_replay(emqx_persistent_session_ds_state:t(), t(), [emqx_ds:stream()]) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_streams_replay(S, SharedS, []) ->
    {S, SharedS};
on_streams_replay(S, #{agent := Agent} = SharedS, StreamKeys) ->
    %% No-op if there are no shared subscriptions
    case emqx_persistent_session_ds_shared_subs_agent:has_subscriptions(Agent) of
        false -> {S, SharedS};
        true -> report_progress(S, SharedS, StreamKeys, _NeedUnacked = false)
    end.

%%--------------------------------------------------------------------
%% on_streams_replay gc

-spec on_streams_gc(emqx_persistent_session_ds_state:t(), t()) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_streams_gc(S, SharedS) ->
    report_progress(S, SharedS, all, _NeedUnacked = false).

%%--------------------------------------------------------------------
%% on_streams_replay/on_streams_gc internal functions

report_progress(
    S, #{agent := Agent0} = SharedS, StreamKeySelector, NeedUnacked
) ->
    Progresses = stream_progresses(S, SharedS, StreamKeySelector, NeedUnacked),
    Agent = emqx_persistent_session_ds_shared_subs_agent:on_stream_progress(
        Agent0, Progresses
    ),
    {S, SharedS#{agent => Agent}}.

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

-dialyzer({nowarn_function, select_stream_states/3}).
select_stream_states(S, #{agent := Agent} = _SharedS, all) ->
    emqx_persistent_session_ds_state:fold_streams(
        fun({SubId, _Stream} = Key, SRS, Acc0) ->
            case emqx_persistent_session_ds_shared_subs_agent:has_subscription(Agent, SubId) of
                true ->
                    [{Key, SRS} | Acc0];
                false ->
                    Acc0
            end
        end,
        [],
        S
    );
select_stream_states(S, #{agent := Agent} = _SharedS, StreamKeys) ->
    lists:filtermap(
        fun({SubId, _} = Key) ->
            case emqx_persistent_session_ds_shared_subs_agent:has_subscription(Agent, SubId) of
                true ->
                    case emqx_persistent_session_ds_state:get_stream(Key, S) of
                        undefined -> false;
                        SRS -> {true, {Key, SRS}}
                    end;
                false ->
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
    #{share_topic_filter := ShareTopicFilter} = Event,
    S0,
    SchedS0,
    SharedS
) ->
    case emqx_persistent_session_ds_state:get_subscription(ShareTopicFilter, S0) of
        undefined ->
            %% This should not happen
            {S0, SchedS0, SharedS};
        Sub ->
            {S, Sched} = add_stream_to_session(Event, Sub, S0, SchedS0),
            {S, Sched, SharedS}
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

handle_revoke_stream(
    #{subscription_id := SubscriptionId, stream := Stream} = _Event,
    S0,
    SchedS0,
    SharedS
) ->
    {S, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_unsubscribe(
        SubscriptionId, Stream, S0, SchedS0
    ),
    {S, SchedS, SharedS}.

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
