%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_agent).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_ds_shared_sub_proto.hrl").
-include("emqx_ds_shared_sub_config.hrl").

-export([
    new/1,
    open/2,
    can_subscribe/3,

    on_subscribe/4,
    on_unsubscribe/3,
    on_stream_progress/2,
    on_info/3,
    on_disconnect/2
]).

-behaviour(emqx_persistent_session_ds_shared_subs_agent).

-type subscription() :: emqx_persistent_session_ds_shared_subs_agent:subscription().
-type share_topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type subscription_id() :: emqx_persistent_session_ds_shared_subs_agent:subscription_id().
-type group_id() :: subscription_id().

-type options() :: #{
    session_id := emqx_persistent_session_ds:id()
}.

-type t() :: #{
    groups := #{
        group_id() => emqx_ds_shared_sub_group_sm:t()
    },
    session_id := emqx_persistent_session_ds:id()
}.

-define(group_id(SubscriptionId), SubscriptionId).
-define(share_topic_filter(GroupId), GroupId).

-record(message_to_group_sm, {
    group_id :: group_id(),
    message :: term()
}).

-export_type([
    t/0,
    group_id/0,
    options/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(options()) -> t().
new(Opts) ->
    init_state(Opts).

-spec open([{share_topic_filter(), subscription()}], options()) -> t().
open(TopicSubscriptions, Opts) ->
    State0 = init_state(Opts),
    State1 = lists:foldl(
        fun({ShareTopicFilter, #{id := SubscriptionId}}, State) ->
            ?tp(debug, ds_agent_open_subscription, #{
                subscription_id => SubscriptionId,
                topic_filter => ShareTopicFilter
            }),
            add_shared_subscription(State, SubscriptionId, ShareTopicFilter)
        end,
        State0,
        TopicSubscriptions
    ),
    State1.

-spec can_subscribe(t(), share_topic_filter(), emqx_types:subopts()) ->
    ok | {error, emqx_types:reason_code()}.
can_subscribe(_State, #share{group = Group, topic = Topic}, _SubOpts) ->
    case ?dq_config(enable) of
        true ->
            %% TODO: Weird to have side effects in function with this name.
            TS = emqx_message:timestamp_now(),
            case emqx_ds_shared_sub_queue:declare(Group, Topic, TS, _StartTime = TS) of
                {ok, _} ->
                    ok;
                exists ->
                    ok;
                {error, Class, Reason} ->
                    ?tp(warning, "Shared queue declare failed", #{
                        group => Group,
                        topic => Topic,
                        class => Class,
                        reason => Reason
                    }),
                    {error, ?RC_UNSPECIFIED_ERROR}
            end;
        false ->
            {error, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED}
    end.

-spec on_subscribe(t(), subscription_id(), share_topic_filter(), emqx_types:subopts()) -> t().
on_subscribe(State0, SubscriptionId, ShareTopicFilter, _SubOpts) ->
    ?tp(debug, ds_agent_on_subscribe, #{
        share_topic_filter => ShareTopicFilter
    }),
    add_shared_subscription(State0, SubscriptionId, ShareTopicFilter).

-spec on_unsubscribe(t(), subscription_id(), [
    emqx_persistent_session_ds_shared_subs:agent_stream_progress()
]) -> t().
on_unsubscribe(State, SubscriptionId, GroupProgress) ->
    delete_shared_subscription(State, SubscriptionId, GroupProgress).

-spec on_stream_progress(t(), #{
    subscription_id() => [emqx_persistent_session_ds_shared_subs:agent_stream_progress()]
}) -> t().
on_stream_progress(State, StreamProgresses) when map_size(StreamProgresses) == 0 ->
    State;
on_stream_progress(State, StreamProgresses) ->
    maps:fold(
        fun(SubscriptionId, GroupProgresses, StateAcc) ->
            with_group_sm(StateAcc, ?group_id(SubscriptionId), fun(GSM) ->
                emqx_ds_shared_sub_group_sm:handle_stream_progress(GSM, GroupProgresses)
            end)
        end,
        State,
        StreamProgresses
    ).

-spec on_disconnect(t(), #{
    subscription_id() => [emqx_persistent_session_ds_shared_subs:agent_stream_progress()]
}) -> t().
on_disconnect(#{groups := Groups0} = State, StreamProgresses) ->
    ok = maps:foreach(
        fun(GroupId, GroupSM0) ->
            GroupProgresses = maps:get(?share_topic_filter(GroupId), StreamProgresses, []),
            emqx_ds_shared_sub_group_sm:handle_disconnect(GroupSM0, GroupProgresses)
        end,
        Groups0
    ),
    State#{groups => #{}}.

-spec on_info(t(), subscription_id(), term()) ->
    {[emqx_persistent_session_ds_shared_subs_agent:event()], t()}.
on_info(
    State, SubscriptionId, ?leader_lease_streams_match(_GroupId, Leader, StreamProgresses, Version)
) ->
    GroupId = ?group_id(SubscriptionId),
    ?tp(debug, ds_shared_sub_agent_leader_lease_streams, #{
        group_id => GroupId,
        streams => StreamProgresses,
        version => Version,
        leader => Leader
    }),
    with_group_sm_events(State, GroupId, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_lease_streams(
            GSM, Leader, StreamProgresses, Version
        )
    end);
on_info(State, SubscriptionId, ?leader_renew_stream_lease_match(_GroupId, Version)) ->
    GroupId = ?group_id(SubscriptionId),
    ?tp(debug, ds_shared_sub_agent_leader_renew_stream_lease, #{
        group_id => GroupId,
        version => Version
    }),
    with_group_sm_events(State, GroupId, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_renew_stream_lease(GSM, Version)
    end);
on_info(State, SubscriptionId, ?leader_renew_stream_lease_match(_GroupId, VersionOld, VersionNew)) ->
    GroupId = ?group_id(SubscriptionId),
    ?tp(debug, ds_shared_sub_agent_leader_renew_stream_lease, #{
        group_id => GroupId,
        version_old => VersionOld,
        version_new => VersionNew
    }),
    with_group_sm_events(State, GroupId, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_renew_stream_lease(GSM, VersionOld, VersionNew)
    end);
on_info(
    State,
    SubscriptionId,
    ?leader_update_streams_match(_GroupId, VersionOld, VersionNew, StreamsNew)
) ->
    GroupId = ?group_id(SubscriptionId),
    ?tp(debug, ds_shared_sub_agent_leader_update_streams, #{
        group_id => GroupId,
        version_old => VersionOld,
        version_new => VersionNew,
        streams_new => StreamsNew
    }),
    with_group_sm_events(State, GroupId, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_update_streams(
            GSM, VersionOld, VersionNew, StreamsNew
        )
    end);
on_info(State, SubscriptionId, ?leader_invalidate_match(_GroupId)) ->
    GroupId = ?group_id(SubscriptionId),
    ?tp(debug, ds_shared_sub_agent_leader_invalidate, #{
        group_id => GroupId
    }),
    with_group_sm_events(State, GroupId, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_invalidate(GSM)
    end);
%% Generic messages sent by group_sm's to themselves (timeouts).
on_info(State, SubscriptionId, #message_to_group_sm{group_id = _GroupId, message = Message}) ->
    GroupId = ?group_id(SubscriptionId),
    with_group_sm_events(State, GroupId, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_info(GSM, Message)
    end).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

init_state(Opts) ->
    SessionId = maps:get(session_id, Opts),
    #{
        session_id => SessionId,
        groups => #{}
    }.

delete_shared_subscription(State, SubscriptionId, GroupProgress) ->
    GroupId = ?group_id(SubscriptionId),
    case State of
        #{groups := #{GroupId := GSM} = Groups} ->
            _ = emqx_ds_shared_sub_group_sm:handle_disconnect(GSM, GroupProgress),
            State#{groups => maps:remove(GroupId, Groups)};
        _ ->
            State
    end.

add_shared_subscription(
    #{session_id := SessionId, groups := Groups0} = State0, SubscriptionId, ShareTopicFilter
) ->
    GroupId = ?group_id(SubscriptionId),
    ?SLOG(debug, #{
        msg => agent_add_shared_subscription,
        share_topic_filter => ShareTopicFilter
    }),
    Groups1 = Groups0#{
        GroupId => emqx_ds_shared_sub_group_sm:new(#{
            session_id => SessionId,
            share_topic_filter => ShareTopicFilter,
            agent => this_agent(SessionId, GroupId),
            send_after => send_to_subscription_after(SubscriptionId)
        })
    },
    State1 = State0#{groups => Groups1},
    State1.

this_agent(Id, GroupId) ->
    emqx_ds_shared_sub_proto:agent(Id, GroupId, self()).

send_to_subscription_after(SubscriptionId) ->
    GroupId = ?group_id(SubscriptionId),
    fun(Time, Msg) ->
        emqx_persistent_session_ds_shared_subs_agent:send_after(
            Time,
            SubscriptionId,
            self(),
            #message_to_group_sm{group_id = GroupId, message = Msg}
        )
    end.

with_group_sm(State, GroupId, Fun) ->
    case State of
        #{groups := #{GroupId := GSM0} = Groups} ->
            GSM1 = Fun(GSM0),
            State#{groups => Groups#{GroupId => GSM1}};
        _ ->
            ?tp(warning, ds_shared_sub_agent_group_not_found, #{
                group_id => GroupId
            }),
            State
    end.

with_group_sm_events(State, GroupId, Fun) ->
    case State of
        #{groups := #{GroupId := GSM0} = Groups} ->
            case Fun(GSM0) of
                {Events0, GSM1} ->
                    Events = add_subscription_id(Events0, GroupId),
                    {Events, State#{
                        groups => Groups#{GroupId => GSM1}
                    }};
                GSM1 ->
                    {[], State#{groups => Groups#{GroupId => GSM1}}}
            end;
        _ ->
            ?tp(warning, ds_shared_sub_agent_group_not_found, #{
                group_id => GroupId
            }),
            {[], State}
    end.

add_subscription_id(Events, GroupId) ->
    [Event#{subscription_id => GroupId} || Event <- Events].
