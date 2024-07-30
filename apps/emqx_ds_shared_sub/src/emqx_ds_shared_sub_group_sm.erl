%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc State machine for a single subscription of a shared subscription agent.
%% Implements GSFSM described in
%% https://github.com/emqx/eip/blob/main/active/0028-durable-shared-subscriptions.md

%% `group_sm` stands for "group state machine".
-module(emqx_ds_shared_sub_group_sm).

-include_lib("emqx/include/logger.hrl").
-include("emqx_ds_shared_sub_config.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/1,

    %% Leader messages
    handle_leader_lease_streams/4,
    handle_leader_renew_stream_lease/2,
    handle_leader_renew_stream_lease/3,
    handle_leader_update_streams/4,
    handle_leader_invalidate/1,

    %% Self-initiated messages
    handle_info/2,

    %% API
    fetch_stream_events/1,
    handle_stream_progress/2,
    handle_disconnect/2
]).

-export_type([
    t/0,
    options/0,
    state/0
]).

-type options() :: #{
    session_id := emqx_persistent_session_ds:id(),
    agent := emqx_ds_shared_sub_proto:agent(),
    share_topic_filter := emqx_persistent_session_ds:share_topic_filter(),
    send_after := fun((non_neg_integer(), term()) -> reference())
}.

-type progress() :: emqx_persistent_session_ds_shared_subs:progress().

-type stream_lease_event() ::
    #{
        type => lease,
        stream => emqx_ds:stream(),
        progress => progress()
    }
    | #{
        type => revoke,
        stream => emqx_ds:stream()
    }.

%% GroupSM States

-define(connecting, connecting).
-define(replaying, replaying).
-define(updating, updating).
-define(disconnected, disconnected).

-type state() :: ?connecting | ?replaying | ?updating | ?disconnected.

-type connecting_data() :: #{}.
-type replaying_data() :: #{
    leader => emqx_ds_shared_sub_proto:leader(),
    streams => #{emqx_ds:stream() => progress()},
    version => emqx_ds_shared_sub_proto:version(),
    prev_version => undefined
}.
-type updating_data() :: #{
    leader => emqx_ds_shared_sub_proto:leader(),
    streams => #{emqx_ds:stream() => progress()},
    version => emqx_ds_shared_sub_proto:version(),
    prev_version => emqx_ds_shared_sub_proto:version()
}.

-type state_data() :: connecting_data() | replaying_data() | updating_data().

-record(state_timeout, {
    id :: reference(),
    name :: atom(),
    message :: term()
}).

-record(timer, {
    ref :: reference(),
    id :: reference()
}).

-type timer_name() :: atom().
-type timer() :: #timer{}.

-type t() :: #{
    share_topic_filter => emqx_persistent_session_ds:share_topic_filter(),
    agent => emqx_ds_shared_sub_proto:agent(),
    send_after => fun((non_neg_integer(), term()) -> reference()),
    stream_lease_events => list(stream_lease_event()),

    state => state(),
    state_data => state_data(),
    state_timers => #{timer_name() => timer()}
}.

%%-----------------------------------------------------------------------
%% API
%%-----------------------------------------------------------------------

-spec new(options()) -> t().
new(#{
    session_id := SessionId,
    agent := Agent,
    share_topic_filter := ShareTopicFilter,
    send_after := SendAfter
}) ->
    ?SLOG(
        info,
        #{
            msg => group_sm_new,
            agent => Agent,
            share_topic_filter => ShareTopicFilter
        }
    ),
    GSM0 = #{
        id => SessionId,
        share_topic_filter => ShareTopicFilter,
        agent => Agent,
        send_after => SendAfter
    },
    ?tp(warning, group_sm_new, #{
        agent => Agent,
        share_topic_filter => ShareTopicFilter
    }),
    transition(GSM0, ?connecting, #{}).

-spec fetch_stream_events(t()) ->
    {t(), [emqx_ds_shared_sub_agent:external_lease_event()]}.
fetch_stream_events(
    #{
        state := _State,
        share_topic_filter := ShareTopicFilter,
        stream_lease_events := Events0
    } = GSM
) ->
    Events1 = lists:map(
        fun(Event) ->
            Event#{share_topic_filter => ShareTopicFilter}
        end,
        Events0
    ),
    {GSM#{stream_lease_events => []}, Events1}.

-spec handle_disconnect(t(), emqx_ds_shared_sub_proto:agent_stream_progress()) -> t().
handle_disconnect(#{state := ?connecting} = GSM, _StreamProgresses) ->
    transition(GSM, ?disconnected, #{});
handle_disconnect(
    #{agent := Agent, state_data := #{leader := Leader, version := Version} = StateData} = GSM,
    StreamProgresses
) ->
    ok = emqx_ds_shared_sub_proto:agent_disconnect(
        Leader, Agent, StreamProgresses, Version
    ),
    transition(GSM, ?disconnected, StateData).

%%-----------------------------------------------------------------------
%% Event Handlers
%%-----------------------------------------------------------------------

%%-----------------------------------------------------------------------
%% Connecting state

handle_connecting(#{agent := Agent, share_topic_filter := ShareTopicFilter} = GSM) ->
    ?tp(warning, group_sm_enter_connecting, #{
        agent => Agent,
        share_topic_filter => ShareTopicFilter
    }),
    ok = emqx_ds_shared_sub_registry:lookup_leader(Agent, agent_metadata(GSM), ShareTopicFilter),
    ensure_state_timeout(GSM, find_leader_timeout, ?dq_config(session_find_leader_timeout_ms)).

handle_leader_lease_streams(
    #{state := ?connecting, share_topic_filter := ShareTopicFilter} = GSM0,
    Leader,
    StreamProgresses,
    Version
) ->
    ?tp(debug, leader_lease_streams, #{share_topic_filter => ShareTopicFilter}),
    Streams = progresses_to_map(StreamProgresses),
    StreamLeaseEvents = progresses_to_lease_events(StreamProgresses),
    transition(
        GSM0,
        ?replaying,
        #{
            leader => Leader,
            streams => Streams,
            prev_version => undefined,
            version => Version
        },
        StreamLeaseEvents
    );
handle_leader_lease_streams(GSM, _Leader, _StreamProgresses, _Version) ->
    GSM.

handle_find_leader_timeout(#{agent := Agent, share_topic_filter := ShareTopicFilter} = GSM0) ->
    ?tp(warning, group_sm_find_leader_timeout, #{
        agent => Agent,
        share_topic_filter => ShareTopicFilter
    }),
    ok = emqx_ds_shared_sub_registry:lookup_leader(Agent, agent_metadata(GSM0), ShareTopicFilter),
    GSM1 = ensure_state_timeout(
        GSM0, find_leader_timeout, ?dq_config(session_find_leader_timeout_ms)
    ),
    GSM1.

%%-----------------------------------------------------------------------
%% Replaying state

handle_replaying(GSM0) ->
    GSM1 = ensure_state_timeout(
        GSM0, renew_lease_timeout, ?dq_config(session_renew_lease_timeout_ms)
    ),
    GSM2 = ensure_state_timeout(
        GSM1, update_stream_state_timeout, ?dq_config(session_min_update_stream_state_interval_ms)
    ),
    GSM2.

handle_renew_lease_timeout(#{agent := Agent, share_topic_filter := ShareTopicFilter} = GSM) ->
    ?tp(warning, renew_lease_timeout, #{agent => Agent, share_topic_filter => ShareTopicFilter}),
    transition(GSM, ?connecting, #{}).

%%-----------------------------------------------------------------------
%% Updating state

handle_updating(GSM0) ->
    GSM1 = ensure_state_timeout(
        GSM0, renew_lease_timeout, ?dq_config(session_renew_lease_timeout_ms)
    ),
    GSM2 = ensure_state_timeout(
        GSM1, update_stream_state_timeout, ?dq_config(session_min_update_stream_state_interval_ms)
    ),
    GSM2.

%%-----------------------------------------------------------------------
%% Disconnected state

handle_disconnected(GSM) ->
    GSM.

%%-----------------------------------------------------------------------
%% Common handlers

handle_leader_update_streams(
    #{
        id := Id,
        state := ?replaying,
        state_data := #{streams := Streams0, version := VersionOld} = StateData
    } = GSM,
    VersionOld,
    VersionNew,
    StreamProgresses
) ->
    ?tp(warning, shared_sub_group_sm_leader_update_streams, #{
        id => Id,
        version_old => VersionOld,
        version_new => VersionNew,
        stream_progresses => emqx_ds_shared_sub_proto:format_stream_progresses(StreamProgresses)
    }),
    {AddEvents, Streams1} = lists:foldl(
        fun(#{stream := Stream, progress := Progress}, {AddEventAcc, StreamsAcc}) ->
            case maps:is_key(Stream, StreamsAcc) of
                true ->
                    %% We prefer our own progress
                    {AddEventAcc, StreamsAcc};
                false ->
                    {
                        [#{type => lease, stream => Stream, progress => Progress} | AddEventAcc],
                        StreamsAcc#{Stream => Progress}
                    }
            end
        end,
        {[], Streams0},
        StreamProgresses
    ),
    NewStreamMap = progresses_to_map(StreamProgresses),
    {RevokeEvents, Streams2} = lists:foldl(
        fun(Stream, {RevokeEventAcc, StreamsAcc}) ->
            case maps:is_key(Stream, NewStreamMap) of
                true ->
                    {RevokeEventAcc, StreamsAcc};
                false ->
                    {
                        [#{type => revoke, stream => Stream} | RevokeEventAcc],
                        maps:remove(Stream, StreamsAcc)
                    }
            end
        end,
        {[], Streams1},
        maps:keys(Streams1)
    ),
    StreamLeaseEvents = AddEvents ++ RevokeEvents,
    ?tp(warning, shared_sub_group_sm_leader_update_streams, #{
        id => Id,
        stream_lease_events => emqx_ds_shared_sub_proto:format_lease_events(StreamLeaseEvents)
    }),
    transition(
        GSM,
        ?updating,
        StateData#{
            streams => Streams2,
            prev_version => VersionOld,
            version => VersionNew
        },
        StreamLeaseEvents
    );
handle_leader_update_streams(
    #{
        state := ?updating,
        state_data := #{version := VersionNew} = _StreamData
    } = GSM,
    _VersionOld,
    VersionNew,
    _StreamProgresses
) ->
    ensure_state_timeout(GSM, renew_lease_timeout, ?dq_config(session_renew_lease_timeout_ms));
handle_leader_update_streams(
    #{state := ?disconnected} = GSM, _VersionOld, _VersionNew, _StreamProgresses
) ->
    GSM;
handle_leader_update_streams(GSM, VersionOld, VersionNew, _StreamProgresses) ->
    %% Unexpected versions or state
    ?tp(warning, shared_sub_group_sm_unexpected_leader_update_streams, #{
        gsm => GSM,
        version_old => VersionOld,
        version_new => VersionNew
    }),
    transition(GSM, ?connecting, #{}).

handle_leader_renew_stream_lease(
    #{state := ?replaying, state_data := #{version := Version}} = GSM, Version
) ->
    ensure_state_timeout(GSM, renew_lease_timeout, ?dq_config(session_renew_lease_timeout_ms));
handle_leader_renew_stream_lease(
    #{state := ?updating, state_data := #{version := Version} = StateData} = GSM, Version
) ->
    transition(
        GSM,
        ?replaying,
        StateData#{prev_version => undefined}
    );
handle_leader_renew_stream_lease(GSM, _Version) ->
    GSM.

handle_leader_renew_stream_lease(
    #{state := ?replaying, state_data := #{version := Version}} = GSM, VersionOld, VersionNew
) when VersionOld =:= Version orelse VersionNew =:= Version ->
    ensure_state_timeout(GSM, renew_lease_timeout, ?dq_config(session_renew_lease_timeout_ms));
handle_leader_renew_stream_lease(
    #{state := ?updating, state_data := #{version := VersionNew, prev_version := VersionOld}} = GSM,
    VersionOld,
    VersionNew
) ->
    ensure_state_timeout(GSM, renew_lease_timeout, ?dq_config(session_renew_lease_timeout_ms));
handle_leader_renew_stream_lease(
    #{state := ?disconnected} = GSM, _VersionOld, _VersionNew
) ->
    GSM;
handle_leader_renew_stream_lease(GSM, VersionOld, VersionNew) ->
    %% Unexpected versions or state
    ?tp(warning, shared_sub_group_sm_unexpected_leader_renew_stream_lease, #{
        gsm => GSM,
        version_old => VersionOld,
        version_new => VersionNew
    }),
    transition(GSM, ?connecting, #{}).

-spec handle_stream_progress(t(), list(emqx_ds_shared_sub_proto:agent_stream_progress())) ->
    t().
handle_stream_progress(#{state := ?connecting} = GSM, _StreamProgresses) ->
    GSM;
handle_stream_progress(
    #{
        state := ?replaying,
        agent := Agent,
        state_data := #{
            leader := Leader,
            version := Version
        }
    } = GSM,
    StreamProgresses
) ->
    ok = emqx_ds_shared_sub_proto:agent_update_stream_states(
        Leader, Agent, StreamProgresses, Version
    ),
    ensure_state_timeout(
        GSM, update_stream_state_timeout, ?dq_config(session_min_update_stream_state_interval_ms)
    );
handle_stream_progress(
    #{
        state := ?updating,
        agent := Agent,
        state_data := #{
            leader := Leader,
            version := Version,
            prev_version := PrevVersion
        }
    } = GSM,
    StreamProgresses
) ->
    ok = emqx_ds_shared_sub_proto:agent_update_stream_states(
        Leader, Agent, StreamProgresses, PrevVersion, Version
    ),
    ensure_state_timeout(
        GSM, update_stream_state_timeout, ?dq_config(session_min_update_stream_state_interval_ms)
    );
handle_stream_progress(#{state := ?disconnected} = GSM, _StreamProgresses) ->
    GSM.

handle_leader_invalidate(#{agent := Agent, share_topic_filter := ShareTopicFilter} = GSM) ->
    ?tp(warning, shared_sub_group_sm_leader_invalidate, #{
        agent => Agent,
        share_topic_filter => ShareTopicFilter
    }),
    transition(GSM, ?connecting, #{}).

%%-----------------------------------------------------------------------
%% Internal API
%%-----------------------------------------------------------------------

handle_state_timeout(
    #{state := ?connecting, share_topic_filter := ShareTopicFilter} = GSM,
    find_leader_timeout,
    _Message
) ->
    ?tp(debug, find_leader_timeout, #{share_topic_filter => ShareTopicFilter}),
    handle_find_leader_timeout(GSM);
handle_state_timeout(
    #{state := ?replaying} = GSM,
    renew_lease_timeout,
    _Message
) ->
    handle_renew_lease_timeout(GSM);
handle_state_timeout(
    GSM,
    update_stream_state_timeout,
    _Message
) ->
    ?tp(debug, update_stream_state_timeout, #{}),
    handle_stream_progress(GSM, []).

handle_info(
    #{state_timers := Timers} = GSM, #state_timeout{message = Message, name = Name, id = Id} = _Info
) ->
    case Timers of
        #{Name := #timer{id = Id}} ->
            handle_state_timeout(GSM, Name, Message);
        _ ->
            %% Stale timer
            GSM
    end;
handle_info(GSM, _Info) ->
    GSM.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

transition(GSM0, NewState, NewStateData) ->
    transition(GSM0, NewState, NewStateData, []).

transition(GSM0, NewState, NewStateData, LeaseEvents) ->
    Timers = maps:get(state_timers, GSM0, #{}),
    TimerNames = maps:keys(Timers),
    GSM1 = lists:foldl(
        fun(Name, Acc) ->
            cancel_timer(Acc, Name)
        end,
        GSM0,
        TimerNames
    ),
    GSM2 = GSM1#{
        state => NewState,
        state_data => NewStateData,
        state_timers => #{},
        stream_lease_events => LeaseEvents
    },
    run_enter_callback(GSM2).

agent_metadata(#{id := Id} = _GSM) ->
    #{id => Id}.

ensure_state_timeout(GSM0, Name, Delay) ->
    ensure_state_timeout(GSM0, Name, Delay, Name).

ensure_state_timeout(GSM0, Name, Delay, Message) ->
    Id = make_ref(),
    GSM1 = cancel_timer(GSM0, Name),
    Timers = maps:get(state_timers, GSM1),
    TimerMessage = #state_timeout{
        id = Id,
        name = Name,
        message = Message
    },
    TimerRef = send_after(GSM1, Delay, TimerMessage),
    GSM2 = GSM1#{
        state_timers := Timers#{Name => #timer{ref = TimerRef, id = Id}}
    },
    GSM2.

send_after(#{send_after := SendAfter} = _GSM, Delay, Message) ->
    SendAfter(Delay, Message).

cancel_timer(GSM, Name) ->
    Timers = maps:get(state_timers, GSM, #{}),
    case Timers of
        #{Name := #timer{ref = TimerRef}} ->
            _ = erlang:cancel_timer(TimerRef),
            GSM#{
                state_timers := maps:remove(Name, Timers)
            };
        _ ->
            GSM
    end.

run_enter_callback(#{state := ?connecting} = GSM) ->
    handle_connecting(GSM);
run_enter_callback(#{state := ?replaying} = GSM) ->
    handle_replaying(GSM);
run_enter_callback(#{state := ?updating} = GSM) ->
    handle_updating(GSM);
run_enter_callback(#{state := ?disconnected} = GSM) ->
    handle_disconnected(GSM).

progresses_to_lease_events(StreamProgresses) ->
    lists:map(
        fun(#{stream := Stream, progress := Progress}) ->
            #{
                type => lease,
                stream => Stream,
                progress => Progress
            }
        end,
        StreamProgresses
    ).

progresses_to_map(StreamProgresses) ->
    lists:foldl(
        fun(#{stream := Stream, progress := Progress}, Acc) ->
            Acc#{Stream => Progress}
        end,
        #{},
        StreamProgresses
    ).
