%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Asynchronous messages between shared sub agent and shared sub leader
%% These messages are instantiated on the receiver's side, so they do not
%% travel over the network.

%% NOTE
%% We do not need any kind of request/response identification,
%% because the protocol is fully event-based.

%% agent messages, sent from agent side to the leader

-define(agent_connect_leader_msg, agent_connect_leader).
-define(agent_update_stream_states_msg, agent_update_stream_states).
-define(agent_connect_leader_timeout_msg, agent_connect_leader_timeout).
-define(agent_renew_stream_lease_timeout_msg, agent_renew_stream_lease_timeout).
-define(agent_disconnect_msg, agent_disconnect).

%% Agent messages sent to the leader.
%% Leader talks to many agents, `agent` field is used to identify the sender.

-define(agent_connect_leader(Agent, AgentMetadata, ShareTopicFilter), #{
    type => ?agent_connect_leader_msg,
    share_topic_filter => ShareTopicFilter,
    agent_metadata => AgentMetadata,
    agent => Agent
}).

-define(agent_connect_leader_match(Agent, AgentMetadata, ShareTopicFilter), #{
    type := ?agent_connect_leader_msg,
    share_topic_filter := ShareTopicFilter,
    agent_metadata := AgentMetadata,
    agent := Agent
}).

-define(agent_update_stream_states(Agent, StreamStates, Version), #{
    type => ?agent_update_stream_states_msg,
    stream_states => StreamStates,
    version => Version,
    agent => Agent
}).

-define(agent_update_stream_states_match(Agent, StreamStates, Version), #{
    type := ?agent_update_stream_states_msg,
    stream_states := StreamStates,
    version := Version,
    agent := Agent
}).

-define(agent_update_stream_states(Agent, StreamStates, VersionOld, VersionNew), #{
    type => ?agent_update_stream_states_msg,
    stream_states => StreamStates,
    version_old => VersionOld,
    version_new => VersionNew,
    agent => Agent
}).

-define(agent_update_stream_states_match(Agent, StreamStates, VersionOld, VersionNew), #{
    type := ?agent_update_stream_states_msg,
    stream_states := StreamStates,
    version_old := VersionOld,
    version_new := VersionNew,
    agent := Agent
}).

-define(agent_disconnect(Agent, StreamStates, Version), #{
    type => ?agent_disconnect_msg,
    stream_states => StreamStates,
    version => Version,
    agent => Agent
}).

-define(agent_disconnect_match(Agent, StreamStates, Version), #{
    type := ?agent_disconnect_msg,
    stream_states := StreamStates,
    version := Version,
    agent := Agent
}).

%% leader messages, sent from the leader to the agent
%% Agent may have several shared subscriptions, so may talk to several leaders
%% `group_id` field is used to identify the leader.

-define(leader_lease_streams_msg, leader_lease_streams).
-define(leader_renew_stream_lease_msg, leader_renew_stream_lease).

-define(leader_lease_streams(GrouId, Leader, Streams, Version), #{
    type => ?leader_lease_streams_msg,
    streams => Streams,
    version => Version,
    leader => Leader,
    group_id => GrouId
}).

-define(leader_lease_streams_match(GroupId, Leader, Streams, Version), #{
    type := ?leader_lease_streams_msg,
    streams := Streams,
    version := Version,
    leader := Leader,
    group_id := GroupId
}).

-define(leader_renew_stream_lease(GroupId, Version), #{
    type => ?leader_renew_stream_lease_msg,
    version => Version,
    group_id => GroupId
}).

-define(leader_renew_stream_lease_match(GroupId, Version), #{
    type := ?leader_renew_stream_lease_msg,
    version := Version,
    group_id := GroupId
}).

-define(leader_renew_stream_lease(GroupId, VersionOld, VersionNew), #{
    type => ?leader_renew_stream_lease_msg,
    version_old => VersionOld,
    version_new => VersionNew,
    group_id => GroupId
}).

-define(leader_renew_stream_lease_match(GroupId, VersionOld, VersionNew), #{
    type := ?leader_renew_stream_lease_msg,
    version_old := VersionOld,
    version_new := VersionNew,
    group_id := GroupId
}).

-define(leader_update_streams(GroupId, VersionOld, VersionNew, StreamsNew), #{
    type => leader_update_streams,
    version_old => VersionOld,
    version_new => VersionNew,
    streams_new => StreamsNew,
    group_id => GroupId
}).

-define(leader_update_streams_match(GroupId, VersionOld, VersionNew, StreamsNew), #{
    type := leader_update_streams,
    version_old := VersionOld,
    version_new := VersionNew,
    streams_new := StreamsNew,
    group_id := GroupId
}).

-define(leader_invalidate(GroupId), #{
    type => leader_invalidate,
    group_id => GroupId
}).

-define(leader_invalidate_match(GroupId), #{
    type := leader_invalidate,
    group_id := GroupId
}).

%% Helpers
%% In test mode we extend agents with (session) Id to have more
%% readable traces.

-ifdef(TEST).

-define(agent(Id, Pid), {Id, Pid}).

-define(agent_pid(Agent), element(2, Agent)).

-define(agent_node(Agent), node(element(2, Agent))).

%% -ifdef(TEST).
-else.

-define(agent(Id, Pid), Pid).

-define(agent_pid(Agent), Agent).

-define(agent_node(Agent), node(Agent)).

%% -ifdef(TEST).
-endif.

-define(is_local_agent(Agent), (?agent_node(Agent) =:= node())).

-define(leader_node(Leader), node(Leader)).

-define(is_local_leader(Leader), (?leader_node(Leader) =:= node())).
