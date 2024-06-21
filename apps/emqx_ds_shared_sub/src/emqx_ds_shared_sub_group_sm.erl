%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc State machine for a single subscription of a shared subscription agent.
%% Implements GSFSM described in
%% https://github.com/emqx/eip/blob/main/active/0028-durable-shared-subscriptions.md

%% `group_sm` stands for "group state machine".
-module(emqx_ds_shared_sub_group_sm).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/1,

    %% Leader messages
    handle_leader_lease_streams/3,
    handle_leader_renew_stream_lease/2,

    %% Self-initiated messages
    handle_info/2,

    %% API
    fetch_stream_events/1
]).

-type options() :: #{
    agent := emqx_ds_shared_sub_proto:agent(),
    topic_filter := emqx_persistent_session_ds:share_topic_filter(),
    send_after := fun((non_neg_integer(), term()) -> reference())
}.

%% Subscription states

-define(connecting, connecting).
-define(replaying, replaying).
-define(updating, updating).

-type state() :: ?connecting | ?replaying | ?updating.

-type group_sm() :: #{
    topic_filter => emqx_persistent_session_ds:share_topic_filter(),
    agent => emqx_ds_shared_sub_proto:agent(),
    send_after => fun((non_neg_integer(), term()) -> reference()),

    state => state(),
    state_data => map(),
    state_timers => map()
}.

-record(state_timeout, {
    id :: reference(),
    name :: atom(),
    message :: term()
}).
-record(timer, {
    ref :: reference(),
    id :: reference()
}).

%%-----------------------------------------------------------------------
%% Constants
%%-----------------------------------------------------------------------

%% TODO https://emqx.atlassian.net/browse/EMQX-12574
%% Move to settings
-define(FIND_LEADER_TIMEOUT, 1000).

%%-----------------------------------------------------------------------
%% API
%%-----------------------------------------------------------------------

-spec new(options()) -> group_sm().
new(#{
    agent := Agent,
    topic_filter := ShareTopicFilter,
    send_after := SendAfter
}) ->
    ?SLOG(
        info,
        #{
            msg => group_sm_new,
            agent => Agent,
            topic_filter => ShareTopicFilter
        }
    ),
    GSM0 = #{
        topic_filter => ShareTopicFilter,
        agent => Agent,
        send_after => SendAfter
    },
    GSM1 = transition(GSM0, ?connecting, #{}),
    ok = emqx_ds_shared_sub_registry:lookup_leader(Agent, ShareTopicFilter),
    GSM2 = ensure_state_timeout(GSM1, find_leader_timeout, ?FIND_LEADER_TIMEOUT, find_leader),
    GSM2.

fetch_stream_events(
    #{
        state := ?replaying,
        topic_filter := TopicFilter,
        state_data := #{stream_lease_events := Events0} = Data
    } = GSM
) ->
    Events1 = lists:map(
        fun(Event) ->
            Event#{topic_filter => TopicFilter}
        end,
        Events0
    ),
    {
        GSM#{
            state_data => Data#{stream_lease_events => []}
        },
        Events1
    };
fetch_stream_events(GSM) ->
    {GSM, []}.

%%-----------------------------------------------------------------------
%% Event Handlers
%%-----------------------------------------------------------------------

%%-----------------------------------------------------------------------
%% Connecting state

handle_leader_lease_streams(
    #{state := ?connecting, topic_filter := TopicFilter} = GSM, StreamProgresses, Version
) ->
    ?tp(debug, leader_lease_streams, #{topic_filter => TopicFilter}),
    Streams = lists:foldl(
        fun(#{stream := Stream, iterator := It}, Acc) ->
            Acc#{Stream => It}
        end,
        #{},
        StreamProgresses
    ),
    StreamLeaseEvents = lists:map(
        fun(#{stream := Stream, iterator := It}) ->
            #{
                type => lease,
                stream => Stream,
                iterator => It
            }
        end,
        StreamProgresses
    ),
    transition(
        GSM,
        ?replaying,
        #{
            streams => Streams,
            stream_lease_events => StreamLeaseEvents,
            prev_version => undefined,
            version => Version,
            last_update_time => erlang:monotonic_time(millisecond)
        }
    );
handle_leader_lease_streams(GSM, _StreamProgresses, _Version) ->
    GSM.

handle_find_leader_timeout(#{agent := Agent, topic_filter := TopicFilter} = GSM0) ->
    ok = emqx_ds_shared_sub_registry:lookup_leader(Agent, TopicFilter),
    GSM1 = ensure_state_timeout(GSM0, find_leader_timeout, ?FIND_LEADER_TIMEOUT, find_leader),
    GSM1.

%%-----------------------------------------------------------------------
%% Replaying state

handle_leader_renew_stream_lease(
    #{state := ?replaying, state_data := #{version := Version} = Data} = GSM, Version
) ->
    GSM#{
        state_data => Data#{last_update_time => erlang:monotonic_time(millisecond)}
    };
handle_leader_renew_stream_lease(GSM, _Version) ->
    GSM.

%%-----------------------------------------------------------------------
%% Updating state

%%-----------------------------------------------------------------------
%% Internal API
%%-----------------------------------------------------------------------

handle_state_timeout(
    #{state := ?connecting, topic_filter := TopicFilter} = GSM,
    find_leader_timeout,
    find_leader
) ->
    ?tp(debug, find_leader_timeout, #{topic_filter => TopicFilter}),
    handle_find_leader_timeout(GSM).

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
    Timers = maps:get(state_timers, GSM0, #{}),
    TimerNames = maps:keys(Timers),
    GSM1 = lists:foldl(
        fun(Name, Acc) ->
            cancel_timer(Acc, Name)
        end,
        GSM0,
        TimerNames
    ),
    GSM1#{
        state => NewState,
        state_data => NewStateData,
        state_timers => #{}
    }.

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
