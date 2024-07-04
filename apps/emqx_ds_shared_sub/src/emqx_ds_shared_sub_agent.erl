%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_agent).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_ds_shared_sub_proto.hrl").

-export([
    new/1,
    open/2,
    can_subscribe/3,

    on_subscribe/3,
    on_unsubscribe/3,
    on_stream_progress/2,
    on_info/2,
    on_disconnect/2,

    renew_streams/1
]).

-behaviour(emqx_persistent_session_ds_shared_subs_agent).

-record(message_to_group_sm, {
    group :: emqx_types:group(),
    message :: term()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

new(Opts) ->
    init_state(Opts).

open(TopicSubscriptions, Opts) ->
    State0 = init_state(Opts),
    State1 = lists:foldl(
        fun({ShareTopicFilter, #{}}, State) ->
            ?tp(warning, ds_agent_open_subscription, #{
                topic_filter => ShareTopicFilter
            }),
            add_group_subscription(State, ShareTopicFilter)
        end,
        State0,
        TopicSubscriptions
    ),
    State1.

can_subscribe(_State, _TopicFilter, _SubOpts) ->
    ok.

on_subscribe(State0, TopicFilter, _SubOpts) ->
    ?tp(warning, ds_agent_on_subscribe, #{
        topic_filter => TopicFilter
    }),
    add_group_subscription(State0, TopicFilter).

on_unsubscribe(State, TopicFilter, GroupProgress) ->
    delete_group_subscription(State, TopicFilter, GroupProgress).

renew_streams(#{} = State) ->
    fetch_stream_events(State).

on_stream_progress(State, StreamProgresses) ->
    maps:fold(
        fun(Group, GroupProgresses, StateAcc) ->
            with_group_sm(StateAcc, Group, fun(GSM) ->
                emqx_ds_shared_sub_group_sm:handle_stream_progress(GSM, GroupProgresses)
            end)
        end,
        State,
        StreamProgresses
    ).

on_disconnect(#{groups := Groups0} = State, StreamProgresses) ->
    ok = maps:foreach(
        fun(Group, GroupSM0) ->
            GroupProgresses = maps:get(Group, StreamProgresses, []),
            emqx_ds_shared_sub_group_sm:handle_disconnect(GroupSM0, GroupProgresses)
        end,
        Groups0
    ),
    State#{groups => #{}}.

on_info(State, ?leader_lease_streams_match(Group, Leader, StreamProgresses, Version)) ->
    ?SLOG(info, #{
        msg => leader_lease_streams,
        group => Group,
        streams => StreamProgresses,
        version => Version,
        leader => Leader
    }),
    with_group_sm(State, Group, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_lease_streams(
            GSM, Leader, StreamProgresses, Version
        )
    end);
on_info(State, ?leader_renew_stream_lease_match(Group, Version)) ->
    ?SLOG(info, #{
        msg => leader_renew_stream_lease,
        group => Group,
        version => Version
    }),
    with_group_sm(State, Group, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_renew_stream_lease(GSM, Version)
    end);
on_info(State, ?leader_renew_stream_lease_match(Group, VersionOld, VersionNew)) ->
    ?SLOG(info, #{
        msg => leader_renew_stream_lease,
        group => Group,
        version_old => VersionOld,
        version_new => VersionNew
    }),
    with_group_sm(State, Group, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_renew_stream_lease(GSM, VersionOld, VersionNew)
    end);
on_info(State, ?leader_update_streams_match(Group, VersionOld, VersionNew, StreamsNew)) ->
    ?SLOG(info, #{
        msg => leader_update_streams,
        group => Group,
        version_old => VersionOld,
        version_new => VersionNew,
        streams_new => StreamsNew
    }),
    with_group_sm(State, Group, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_update_streams(
            GSM, VersionOld, VersionNew, StreamsNew
        )
    end);
on_info(State, ?leader_invalidate_match(Group)) ->
    ?SLOG(info, #{
        msg => leader_invalidate,
        group => Group
    }),
    with_group_sm(State, Group, fun(GSM) ->
        emqx_ds_shared_sub_group_sm:handle_leader_invalidate(GSM)
    end);
%% Generic messages sent by group_sm's to themselves (timeouts).
on_info(State, #message_to_group_sm{group = Group, message = Message}) ->
    with_group_sm(State, Group, fun(GSM) ->
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

delete_group_subscription(State, #share{group = Group}, GroupProgress) ->
    case State of
        #{groups := #{Group := GSM} = Groups} ->
            _ = emqx_ds_shared_sub_group_sm:handle_disconnect(GSM, GroupProgress),
            State#{groups => maps:remove(Group, Groups)};
        _ ->
            State
    end.

add_group_subscription(
    #{session_id := SessionId, groups := Groups0} = State0, ShareTopicFilter
) ->
    ?SLOG(info, #{
        msg => agent_add_group_subscription,
        topic_filter => ShareTopicFilter
    }),
    #share{group = Group} = ShareTopicFilter,
    Groups1 = Groups0#{
        Group => emqx_ds_shared_sub_group_sm:new(#{
            session_id => SessionId,
            topic_filter => ShareTopicFilter,
            agent => this_agent(SessionId),
            send_after => send_to_subscription_after(Group)
        })
    },
    State1 = State0#{groups => Groups1},
    State1.

fetch_stream_events(#{groups := Groups0} = State0) ->
    {Groups1, Events} = maps:fold(
        fun(Group, GroupSM0, {GroupsAcc, EventsAcc}) ->
            {GroupSM1, Events} = emqx_ds_shared_sub_group_sm:fetch_stream_events(GroupSM0),
            {GroupsAcc#{Group => GroupSM1}, [Events | EventsAcc]}
        end,
        {#{}, []},
        Groups0
    ),
    State1 = State0#{groups => Groups1},
    {lists:concat(Events), State1}.

this_agent(Id) ->
    emqx_ds_shared_sub_proto:agent(Id, self()).

send_to_subscription_after(Group) ->
    fun(Time, Msg) ->
        emqx_persistent_session_ds_shared_subs_agent:send_after(
            Time,
            self(),
            #message_to_group_sm{group = Group, message = Msg}
        )
    end.

with_group_sm(State, Group, Fun) ->
    case State of
        #{groups := #{Group := GSM0} = Groups} ->
            #{} = GSM1 = Fun(GSM0),
            State#{groups => Groups#{Group => GSM1}};
        _ ->
            %% TODO
            %% Error?
            State
    end.
