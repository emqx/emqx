%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc State machine for a single subscription of a shared subscription agent.
%% Implements GSFSM described in
%% https://github.com/emqx/eip/blob/main/active/0028-durable-shared-subscriptions.md

%% `group_sm` stands for "group state machine".
-module(emqx_ds_shared_sub_group_sm).

-include_lib("emqx/include/logger.hrl").

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

-type group_sm() :: #{
    topic_filter => emqx_persistent_session_ds:share_topic_filter(),
    agent => emqx_ds_shared_sub_proto:agent(),
    send_after => fun((non_neg_integer(), term()) -> reference()),

    state => ?connecting | ?replaying | ?updating,
    state_data => map()
}.

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
    ok = emqx_ds_shared_sub_registry:lookup_leader(Agent, ShareTopicFilter),
    #{
        topic_filter => ShareTopicFilter,
        agent => Agent,
        send_after => SendAfter,

        state => ?connecting,
        state_data => #{}
    }.

handle_leader_lease_streams(#{state := ?connecting} = GSM, StreamProgresses, Version) ->
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
    GSM#{
        state => ?replaying,
        state_data => #{
            streams => Streams,
            stream_lease_events => StreamLeaseEvents,
            prev_version => undefined,
            version => Version,
            last_update_time => erlang:monotonic_time(millisecond)
        }
    };
handle_leader_lease_streams(GSM, _StreamProgresses, _Version) ->
    GSM.

handle_leader_renew_stream_lease(
    #{state := ?replaying, state_data := #{version := Version} = Data} = GSM, Version
) ->
    GSM#{
        state_data => Data#{last_update_time => erlang:monotonic_time(millisecond)}
    };
handle_leader_renew_stream_lease(GSM, _Version) ->
    GSM.

handle_info(GSM, _Info) ->
    GSM.

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

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

% send_after(#{send_after := SendAfter} = _GSM, Delay, Message) ->
%     SendAfter(Delay, Message).
