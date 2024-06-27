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

%% Agent messages sent to the leader.
%% Leader talks to many agents, `agent` field is used to identify the sender.

-define(agent_connect_leader(Agent, TopicFilter), #{
    type => ?agent_connect_leader_msg,
    topic_filter => TopicFilter,
    agent => Agent
}).

-define(agent_connect_leader_match(Agent, TopicFilter), #{
    type := ?agent_connect_leader_msg,
    topic_filter := TopicFilter,
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

%% leader messages, sent from the leader to the agent
%% Agent may have several shared subscriptions, so may talk to several leaders
%% `group` field is used to identify the leader.

-define(leader_lease_streams_msg, leader_lease_streams).
-define(leader_renew_stream_lease_msg, leader_renew_stream_lease).

-define(leader_lease_streams(Group, Leader, Streams, Version), #{
    type => ?leader_lease_streams_msg,
    streams => Streams,
    version => Version,
    leader => Leader,
    group => Group
}).

-define(leader_lease_streams_match(Group, Leader, Streams, Version), #{
    type := ?leader_lease_streams_msg,
    streams := Streams,
    version := Version,
    leader := Leader,
    group := Group
}).

-define(leader_renew_stream_lease(Group, Version), #{
    type => ?leader_renew_stream_lease_msg,
    version => Version,
    group => Group
}).

-define(leader_renew_stream_lease_match(Group, Version), #{
    type := ?leader_renew_stream_lease_msg,
    version := Version,
    group := Group
}).

-define(leader_renew_stream_lease(Group, VersionOld, VersionNew), #{
    type => ?leader_renew_stream_lease_msg,
    version_old => VersionOld,
    version_new => VersionNew,
    group => Group
}).

-define(leader_renew_stream_lease_match(Group, VersionOld, VersionNew), #{
    type := ?leader_renew_stream_lease_msg,
    version_old := VersionOld,
    version_new := VersionNew,
    group := Group
}).

-define(leader_update_streams(Group, VersionOld, VersionNew, StreamsNew), #{
    type => leader_update_streams,
    version_old => VersionOld,
    version_new => VersionNew,
    streams_new => StreamsNew,
    group => Group
}).

-define(leader_update_streams_match(Group, VersionOld, VersionNew, StreamsNew), #{
    type := leader_update_streams,
    version_old := VersionOld,
    version_new := VersionNew,
    streams_new := StreamsNew,
    group := Group
}).

-define(leader_invalidate(Group), #{
    type => leader_invalidate,
    group => Group
}).

-define(leader_invalidate_match(Group), #{
    type := leader_invalidate,
    group := Group
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
