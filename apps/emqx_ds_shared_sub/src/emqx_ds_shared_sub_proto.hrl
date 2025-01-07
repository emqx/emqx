%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Asynchronous messages between shared sub agent and shared sub leader
%% These messages are instantiated on the receiver's side, so they do not
%% travel over the network.

%% NOTE
%% We do not need any kind of request/response identification,
%% because the protocol is fully event-based.

%% agent messages, sent from agent side to the leader

-define(agent_connect_leader_msg, 1).
-define(agent_update_stream_states_msg, 2).
-define(agent_connect_leader_timeout_msg, 3).
-define(agent_renew_stream_lease_timeout_msg, 4).
-define(agent_disconnect_msg, 5).

%% message keys (used used not to send atoms over the network)
-define(agent_msg_type, 1).
-define(agent_msg_agent, 2).
-define(agent_msg_share_topic_filter, 3).
-define(agent_msg_agent_metadata, 4).
-define(agent_msg_stream_states, 5).
-define(agent_msg_version, 6).
-define(agent_msg_version_old, 7).
-define(agent_msg_version_new, 8).

%% Agent messages sent to the leader.
%% Leader talks to many agents, `agent` field is used to identify the sender.

-define(agent_connect_leader(Agent, AgentMetadata, ShareTopicFilter), #{
    ?agent_msg_type => ?agent_connect_leader_msg,
    ?agent_msg_share_topic_filter => ShareTopicFilter,
    ?agent_msg_agent_metadata => AgentMetadata,
    ?agent_msg_agent => Agent
}).

-define(agent_connect_leader_match(Agent, AgentMetadata, ShareTopicFilter), #{
    ?agent_msg_type := ?agent_connect_leader_msg,
    ?agent_msg_share_topic_filter := ShareTopicFilter,
    ?agent_msg_agent_metadata := AgentMetadata,
    ?agent_msg_agent := Agent
}).

-define(agent_update_stream_states(Agent, StreamStates, Version), #{
    ?agent_msg_type => ?agent_update_stream_states_msg,
    ?agent_msg_stream_states => StreamStates,
    ?agent_msg_version => Version,
    ?agent_msg_agent => Agent
}).

-define(agent_update_stream_states_match(Agent, StreamStates, Version), #{
    ?agent_msg_type := ?agent_update_stream_states_msg,
    ?agent_msg_stream_states := StreamStates,
    ?agent_msg_version := Version,
    ?agent_msg_agent := Agent
}).

-define(agent_update_stream_states(Agent, StreamStates, VersionOld, VersionNew), #{
    ?agent_msg_type => ?agent_update_stream_states_msg,
    ?agent_msg_stream_states => StreamStates,
    ?agent_msg_version_old => VersionOld,
    ?agent_msg_version_new => VersionNew,
    ?agent_msg_agent => Agent
}).

-define(agent_update_stream_states_match(Agent, StreamStates, VersionOld, VersionNew), #{
    ?agent_msg_type := ?agent_update_stream_states_msg,
    ?agent_msg_stream_states := StreamStates,
    ?agent_msg_version_old := VersionOld,
    ?agent_msg_version_new := VersionNew,
    ?agent_msg_agent := Agent
}).

-define(agent_disconnect(Agent, StreamStates, Version), #{
    ?agent_msg_type => ?agent_disconnect_msg,
    ?agent_msg_stream_states => StreamStates,
    ?agent_msg_version => Version,
    ?agent_msg_agent => Agent
}).

-define(agent_disconnect_match(Agent, StreamStates, Version), #{
    ?agent_msg_type := ?agent_disconnect_msg,
    ?agent_msg_stream_states := StreamStates,
    ?agent_msg_version := Version,
    ?agent_msg_agent := Agent
}).

%% leader messages, sent from the leader to the agent
%% Agent may have several shared subscriptions, so may talk to several leaders
%% `group_id` field is used to identify the leader.

-define(leader_lease_streams_msg, 101).
-define(leader_renew_stream_lease_msg, 102).
-define(leader_update_streams, 103).
-define(leader_invalidate, 104).

-define(leader_msg_type, 101).
-define(leader_msg_streams, 102).
-define(leader_msg_version, 103).
-define(leader_msg_version_old, 104).
-define(leader_msg_version_new, 105).
-define(leader_msg_streams_new, 106).
-define(leader_msg_leader, 107).
-define(leader_msg_group_id, 108).

-define(leader_lease_streams(GrouId, Leader, Streams, Version), #{
    ?leader_msg_type => ?leader_lease_streams_msg,
    ?leader_msg_streams => Streams,
    ?leader_msg_version => Version,
    ?leader_msg_leader => Leader,
    ?leader_msg_group_id => GrouId
}).

-define(leader_lease_streams_match(GroupId, Leader, Streams, Version), #{
    ?leader_msg_type := ?leader_lease_streams_msg,
    ?leader_msg_streams := Streams,
    ?leader_msg_version := Version,
    ?leader_msg_leader := Leader,
    ?leader_msg_group_id := GroupId
}).

-define(leader_renew_stream_lease(GroupId, Version), #{
    ?leader_msg_type => ?leader_renew_stream_lease_msg,
    ?leader_msg_version => Version,
    ?leader_msg_group_id => GroupId
}).

-define(leader_renew_stream_lease_match(GroupId, Version), #{
    ?leader_msg_type := ?leader_renew_stream_lease_msg,
    ?leader_msg_version := Version,
    ?leader_msg_group_id := GroupId
}).

-define(leader_renew_stream_lease(GroupId, VersionOld, VersionNew), #{
    ?leader_msg_type => ?leader_renew_stream_lease_msg,
    ?leader_msg_version_old => VersionOld,
    ?leader_msg_version_new => VersionNew,
    ?leader_msg_group_id => GroupId
}).

-define(leader_renew_stream_lease_match(GroupId, VersionOld, VersionNew), #{
    ?leader_msg_type := ?leader_renew_stream_lease_msg,
    ?leader_msg_version_old := VersionOld,
    ?leader_msg_version_new := VersionNew,
    ?leader_msg_group_id := GroupId
}).

-define(leader_update_streams(GroupId, VersionOld, VersionNew, StreamsNew), #{
    ?leader_msg_type => ?leader_update_streams,
    ?leader_msg_version_old => VersionOld,
    ?leader_msg_version_new => VersionNew,
    ?leader_msg_streams_new => StreamsNew,
    ?leader_msg_group_id => GroupId
}).

-define(leader_update_streams_match(GroupId, VersionOld, VersionNew, StreamsNew), #{
    ?leader_msg_type := ?leader_update_streams,
    ?leader_msg_version_old := VersionOld,
    ?leader_msg_version_new := VersionNew,
    ?leader_msg_streams_new := StreamsNew,
    ?leader_msg_group_id := GroupId
}).

-define(leader_invalidate(GroupId), #{
    ?leader_msg_type => ?leader_invalidate,
    ?leader_msg_group_id => GroupId
}).

-define(leader_invalidate_match(GroupId), #{
    ?leader_msg_type := ?leader_invalidate,
    ?leader_msg_group_id := GroupId
}).

%% Helpers
%% In test mode we extend agents with (session) Id to have more
%% readable traces.

-ifdef(TEST).

-define(agent(SessionId, SubscriptionId, Pid), {Pid, SubscriptionId, SessionId}).

-define(format_agent_msg(Msg), emqx_ds_shared_sub_proto_format:format_agent_msg(Msg)).

-define(format_leader_msg(Msg), emqx_ds_shared_sub_proto_format:format_leader_msg(Msg)).

%% -ifdef(TEST).
-else.

-define(agent(_SessionId, SubscriptionId, Pid), {Pid, SubscriptionId}).

-define(format_agent_msg(Msg), Msg).

-define(format_leader_msg(Msg), Msg).

%% -ifdef(TEST).
-endif.

-define(agent_pid(Agent), element(1, Agent)).

-define(agent_subscription_id(Agent), element(2, Agent)).

-define(agent_node(Agent), node(element(1, Agent))).

-define(is_local_agent(Agent), (?agent_node(Agent) =:= node())).

-define(leader_node(Leader), node(Leader)).

-define(is_local_leader(Leader), (?leader_node(Leader) =:= node())).
