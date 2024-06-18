%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_agent).

-include_lib("emqx/include/emqx_persistent_message.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-include("emqx_ds_shared_sub_proto.hrl").

-export([
    new/1,
    open/2,

    on_subscribe/3,
    on_unsubscribe/2,
    on_stream_progress/2,
    on_info/2,

    renew_streams/1
]).

%% Individual subscription state

-define(connecting, connecting).
-define(replaying, replaying).
% -define(updating, updating).

-behaviour(emqx_persistent_session_ds_shared_subs_agent).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

new(Opts) ->
    init_state(Opts).

open(TopicSubscriptions, Opts) ->
    State0 = init_state(Opts),
    State1 = lists:foldl(
        fun({ShareTopicFilter, #{}}, State) ->
            add_subscription(State, ShareTopicFilter)
        end,
        State0,
        TopicSubscriptions
    ),
    State1.

on_subscribe(State0, TopicFilter, _SubOpts) ->
    State1 = add_subscription(State0, TopicFilter),
    {ok, State1}.

on_unsubscribe(State, TopicFilter) ->
    delete_subscription(State, TopicFilter).

renew_streams(#{} = State) ->
    fetch_stream_events(State).

on_stream_progress(State, _StreamProgress) ->
    State.

on_info(State, ?leader_lease_streams_match(Group, StreamProgresses, Version)) ->
    case State of
        #{subscriptions := #{Group := Sub0} = Subs} ->
            Sub1 = handle_leader_lease_streams(Sub0, StreamProgresses, Version),
            State#{subscriptions => Subs#{Group => Sub1}};
        _ ->
            %% TODO
            %% Handle unknown group?
            State
    end;
on_info(State, ?leader_renew_stream_lease_match(Group, Version)) ->
    case State of
        #{subscriptions := #{Group := Sub0} = Subs} ->
            Sub1 = handle_leader_renew_stream_lease(Sub0, Version),
            State#{subscriptions => Subs#{Group => Sub1}};
        _ ->
            %% TODO
            %% Handle unknown group?
            State
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

init_state(Opts) ->
    SessionId = maps:get(session_id, Opts),
    #{
        session_id => SessionId,
        subscriptions => #{}
    }.

delete_subscription(State, _ShareTopicFilter) ->
    %% TODO
    State.

add_subscription(
    #{subscriptions := Subs0} = State0, ShareTopicFilter
) ->
    #share{topic = TopicFilter, group = Group} = ShareTopicFilter,
    ok = emqx_ds_shared_sub_registry:lookup_leader(this_agent(), TopicFilter),
    Subs1 = Subs0#{
        %% TODO
        %% State machine is complex, so better move it to a separate module
        Group => #{
            state => ?connecting,
            topic_filter => ShareTopicFilter,
            streams => #{},
            version => undefined,
            prev_version => undefined,
            stream_lease_events => []
        }
    },
    State1 = State0#{subscriptions => Subs1},
    State1.

fetch_stream_events(#{subscriptions := Subs0} = State0) ->
    {Subs1, Events} = lists:foldl(
        fun(
            {_Group, #{stream_lease_events := Events0, topic_filter := TopicFilter} = Sub},
            {SubsAcc, EventsAcc}
        ) ->
            Events1 = lists:map(
                fun(Event) ->
                    Event#{topic_filter => TopicFilter}
                end,
                Events0
            ),
            {SubsAcc#{TopicFilter => Sub#{stream_lease_events => []}}, [Events1 | EventsAcc]}
        end,
        {Subs0, []},
        maps:to_list(Subs0)
    ),
    State1 = State0#{subscriptions => Subs1},
    {lists:concat(Events), State1}.

%%--------------------------------------------------------------------
%% Handler of leader messages
%%--------------------------------------------------------------------

handle_leader_lease_streams(#{state := ?connecting} = Sub, StreamProgresses, Version) ->
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
    Sub#{
        state => ?replaying,
        streams => Streams,
        stream_lease_events => StreamLeaseEvents,
        version => Version,
        last_update_time => erlang:monotonic_time(millisecond)
    }.

handle_leader_renew_stream_lease(#{state := ?replaying, version := Version} = Sub, Version) ->
    Sub#{
        last_update_time => erlang:monotonic_time(millisecond)
    };
handle_leader_renew_stream_lease(Sub, _Version) ->
    Sub.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

this_agent() -> self().
