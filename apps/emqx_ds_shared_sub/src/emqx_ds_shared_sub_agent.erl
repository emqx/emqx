%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

    on_subscribe/3,
    on_unsubscribe/3,
    on_stream_progress/2,
    on_info/2,
    on_disconnect/2
]).

-behaviour(emqx_persistent_session_ds_shared_subs_agent).

-type share_topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type group_id() :: share_topic_filter().

-type progress() :: emqx_persistent_session_ds_shared_subs:progress().
-type external_lease_event() ::
    #{
        type => lease,
        stream => emqx_ds:stream(),
        progress => progress(),
        share_topic_filter => emqx_persistent_session_ds:share_topic_filter()
    }
    | #{
        type => revoke,
        stream => emqx_ds:stream(),
        share_topic_filter => emqx_persistent_session_ds:share_topic_filter()
    }.

-type options() :: #{
    session_id := emqx_persistent_session_ds:id()
}.

-type t() :: #{
    groups := #{
        group_id() => emqx_ds_shared_sub_group_sm:t()
    },
    session_id := emqx_persistent_session_ds:id()
}.

%% We speak in the terms of share_topic_filter in the module API
%% which is consumed by persistent session.
%%
%% We speak in the terms of group_id internally:
%% * to identfy shared subscription's group_sm in the state;
%% * to addres agent's group_sm while communicating with leader.
%% * to identify the leader itself.
%%
%% share_topic_filter should be uniquely determined by group_id. See MQTT 5.0 spec:
%%
%% > Note that "$share/consumer1//finance" and "$share/consumer1/sport/tennis/+"
%% > are distinct shared subscriptions, even though they have the same ShareName.
%% > While they might be related in some way, no specific relationship between them
%% > is implied by them having the same ShareName.
%%
%% So we just use the full share_topic_filter record as group_id.

-define(group_id(ShareTopicFilter), ShareTopicFilter).
-define(share_topic_filter(GroupId), GroupId).

-record(message_to_group_sm, {
    group_id :: group_id(),
    message :: term()
}).

-export_type([
    t/0,
    group_id/0,
    options/0,
    external_lease_event/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(options()) -> t().
new(Opts) ->
    init_state(Opts).

-spec open([{share_topic_filter(), emqx_types:subopts()}], options()) -> t().
open(TopicSubscriptions, Opts) ->
    State0 = init_state(Opts),
    State1 = lists:foldl(
        fun({ShareTopicFilter, #{}}, State) ->
            ?tp(debug, ds_agent_open_subscription, #{
                topic_filter => ShareTopicFilter
            }),
            add_shared_subscription(State, ShareTopicFilter)
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

-spec on_subscribe(t(), share_topic_filter(), emqx_types:subopts()) -> t().
on_subscribe(State0, ShareTopicFilter, _SubOpts) ->
    ?tp(debug, ds_agent_on_subscribe, #{
        share_topic_filter => ShareTopicFilter
    }),
    add_shared_subscription(State0, ShareTopicFilter).

-spec on_unsubscribe(t(), share_topic_filter(), [
    emqx_persistent_session_ds_shared_subs:agent_stream_progress()
]) -> t().
on_unsubscribe(State, ShareTopicFilter, GroupProgress) ->
    delete_shared_subscription(State, ShareTopicFilter, GroupProgress).

-spec on_stream_progress(t(), #{
    share_topic_filter() => [emqx_persistent_session_ds_shared_subs:agent_stream_progress()]
}) -> t().
on_stream_progress(State, StreamProgresses) when map_size(StreamProgresses) == 0 ->
    State;
on_stream_progress(State, StreamProgresses) ->
    maps:fold(
        fun(ShareTopicFilter, GroupProgresses, StateAcc) ->
            with_group_sm(StateAcc, ?group_id(ShareTopicFilter), fun(GSM) ->
                emqx_ds_shared_sub_group_sm:handle_stream_progress(GSM, GroupProgresses)
            end)
        end,
        State,
        StreamProgresses
    ).

-spec on_disconnect(t(), [emqx_persistent_session_ds_shared_subs:agent_stream_progress()]) -> t().
on_disconnect(#{groups := Groups0} = State, StreamProgresses) ->
    ok = maps:foreach(
        fun(GroupId, GroupSM0) ->
            GroupProgresses = maps:get(?share_topic_filter(GroupId), StreamProgresses, []),
            emqx_ds_shared_sub_group_sm:handle_disconnect(GroupSM0, GroupProgresses)
        end,
        Groups0
    ),
    State#{groups => #{}}.

-spec on_info(t(), term()) -> t().
on_info(State, ?leader_lease_streams_match(GroupId, Leader, StreamProgresses, Version)) ->
    ?tp(warning, ds_shared_sub_agent_leader_lease_streams, #{
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
on_info(State, ?leader_renew_stream_lease_match(GroupId, Version)) ->
    ?tp(debug, ds_shared_sub_agent_leader_renew_stream_lease, #{
        group_id => GroupId,
        version => Version
    }),
    with_group_sm_events(State, GroupId, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_renew_stream_lease(GSM, Version)
    end);
on_info(State, ?leader_renew_stream_lease_match(GroupId, VersionOld, VersionNew)) ->
    ?tp(debug, ds_shared_sub_agent_leader_renew_stream_lease, #{
        group_id => GroupId,
        version_old => VersionOld,
        version_new => VersionNew
    }),
    with_group_sm_events(State, GroupId, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_renew_stream_lease(GSM, VersionOld, VersionNew)
    end);
on_info(State, ?leader_update_streams_match(GroupId, VersionOld, VersionNew, StreamsNew)) ->
    ?tp(warning, ds_shared_sub_agent_leader_update_streams, #{
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
on_info(State, ?leader_invalidate_match(GroupId)) ->
    ?tp(warning, ds_shared_sub_agent_leader_invalidate, #{
        group_id => GroupId
    }),
    with_group_sm_events(State, GroupId, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_invalidate(GSM)
    end);
%% Generic messages sent by group_sm's to themselves (timeouts).
on_info(State, #message_to_group_sm{group_id = GroupId, message = Message}) ->
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

delete_shared_subscription(State, ShareTopicFilter, GroupProgress) ->
    GroupId = ?group_id(ShareTopicFilter),
    case State of
        #{groups := #{GroupId := GSM} = Groups} ->
            _ = emqx_ds_shared_sub_group_sm:handle_disconnect(GSM, GroupProgress),
            State#{groups => maps:remove(GroupId, Groups)};
        _ ->
            State
    end.

add_shared_subscription(
    #{session_id := SessionId, groups := Groups0} = State0, ShareTopicFilter
) ->
    ?SLOG(debug, #{
        msg => agent_add_shared_subscription,
        share_topic_filter => ShareTopicFilter
    }),
    GroupId = ?group_id(ShareTopicFilter),
    Groups1 = Groups0#{
        GroupId => emqx_ds_shared_sub_group_sm:new(#{
            session_id => SessionId,
            share_topic_filter => ShareTopicFilter,
            agent => this_agent(SessionId),
            send_after => send_to_subscription_after(GroupId)
        })
    },
    State1 = State0#{groups => Groups1},
    State1.

this_agent(Id) ->
    emqx_ds_shared_sub_proto:agent(Id, self()).

send_to_subscription_after(GroupId) ->
    fun(Time, Msg) ->
        emqx_persistent_session_ds_shared_subs_agent:send_after(
            Time,
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
                {Events, GSM1} ->
                    {add_group_id(Events, GroupId), State#{groups => Groups#{GroupId => GSM1}}};
                GSM1 ->
                    {[], State#{groups => Groups#{GroupId => GSM1}}}
            end;
        _ ->
            ?tp(warning, ds_shared_sub_agent_group_not_found, #{
                group_id => GroupId
            }),
            {[], State}
    end.

add_group_id(Events, GroupId) ->
    [Event#{share_topic_filter => GroupId} || Event <- Events].



