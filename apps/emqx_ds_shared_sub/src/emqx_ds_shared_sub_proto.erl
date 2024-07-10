%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_proto).

-include("emqx_ds_shared_sub_proto.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    agent_connect_leader/4,
    agent_update_stream_states/4,
    agent_update_stream_states/5,
    agent_disconnect/4,

    leader_lease_streams/5,
    leader_renew_stream_lease/3,
    leader_renew_stream_lease/4,
    leader_update_streams/5,
    leader_invalidate/2
]).

-export([
    format_stream_progresses/1,
    format_stream_progress/1,
    format_stream_key/1,
    format_stream_keys/1,
    format_lease_event/1,
    format_lease_events/1,
    agent/2
]).

-type agent() :: ?agent(emqx_persistent_session_ds:id(), pid()).
-type leader() :: pid().
-type share_topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type group() :: emqx_types:group().
-type version() :: non_neg_integer().
-type agent_metadata() :: #{
    id := emqx_persistent_session_ds:id()
}.

-type leader_stream_progress() :: #{
    stream := emqx_ds:stream(),
    progress := emqx_persistent_session_ds_shared_subs:progress()
}.

-type agent_stream_progress() :: emqx_persistent_session_ds_shared_subs:agent_stream_progress().

-export_type([
    agent/0,
    leader/0,
    group/0,
    version/0,
    leader_stream_progress/0,
    agent_stream_progress/0,
    agent_metadata/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% agent -> leader messages

-spec agent_connect_leader(leader(), agent(), agent_metadata(), share_topic_filter()) -> ok.
agent_connect_leader(ToLeader, FromAgent, AgentMetadata, ShareTopicFilter) when
    ?is_local_leader(ToLeader)
->
    ?tp(warning, shared_sub_proto_msg, #{
        type => agent_connect_leader,
        to_leader => ToLeader,
        from_agent => FromAgent,
        agent_metadata => AgentMetadata,
        share_topic_filter => ShareTopicFilter
    }),
    _ = erlang:send(ToLeader, ?agent_connect_leader(FromAgent, AgentMetadata, ShareTopicFilter)),
    ok;
agent_connect_leader(ToLeader, FromAgent, AgentMetadata, ShareTopicFilter) ->
    emqx_ds_shared_sub_proto_v1:agent_connect_leader(
        ?leader_node(ToLeader), ToLeader, FromAgent, AgentMetadata, ShareTopicFilter
    ).

-spec agent_update_stream_states(leader(), agent(), list(agent_stream_progress()), version()) -> ok.
agent_update_stream_states(ToLeader, FromAgent, StreamProgresses, Version) when
    ?is_local_leader(ToLeader)
->
    ?tp(warning, shared_sub_proto_msg, #{
        type => agent_update_stream_states,
        to_leader => ToLeader,
        from_agent => FromAgent,
        stream_progresses => format_stream_progresses(StreamProgresses),
        version => Version
    }),
    _ = erlang:send(ToLeader, ?agent_update_stream_states(FromAgent, StreamProgresses, Version)),
    ok;
agent_update_stream_states(ToLeader, FromAgent, StreamProgresses, Version) ->
    emqx_ds_shared_sub_proto_v1:agent_update_stream_states(
        ?leader_node(ToLeader), ToLeader, FromAgent, StreamProgresses, Version
    ).

-spec agent_update_stream_states(
    leader(), agent(), list(agent_stream_progress()), version(), version()
) -> ok.
agent_update_stream_states(ToLeader, FromAgent, StreamProgresses, VersionOld, VersionNew) when
    ?is_local_leader(ToLeader)
->
    ?tp(warning, shared_sub_proto_msg, #{
        type => agent_update_stream_states,
        to_leader => ToLeader,
        from_agent => FromAgent,
        stream_progresses => format_stream_progresses(StreamProgresses),
        version_old => VersionOld,
        version_new => VersionNew
    }),
    _ = erlang:send(
        ToLeader, ?agent_update_stream_states(FromAgent, StreamProgresses, VersionOld, VersionNew)
    ),
    ok;
agent_update_stream_states(ToLeader, FromAgent, StreamProgresses, VersionOld, VersionNew) ->
    emqx_ds_shared_sub_proto_v1:agent_update_stream_states(
        ?leader_node(ToLeader), ToLeader, FromAgent, StreamProgresses, VersionOld, VersionNew
    ).

agent_disconnect(ToLeader, FromAgent, StreamProgresses, Version) when
    ?is_local_leader(ToLeader)
->
    ?tp(warning, shared_sub_proto_msg, #{
        type => agent_disconnect,
        to_leader => ToLeader,
        from_agent => FromAgent,
        stream_progresses => format_stream_progresses(StreamProgresses),
        version => Version
    }),
    _ = erlang:send(ToLeader, ?agent_disconnect(FromAgent, StreamProgresses, Version)),
    ok;
agent_disconnect(ToLeader, FromAgent, StreamProgresses, Version) ->
    emqx_ds_shared_sub_proto_v1:agent_disconnect(
        ?leader_node(ToLeader), ToLeader, FromAgent, StreamProgresses, Version
    ).

%% leader -> agent messages

-spec leader_lease_streams(agent(), group(), leader(), list(leader_stream_progress()), version()) ->
    ok.
leader_lease_streams(ToAgent, OfGroup, Leader, Streams, Version) when ?is_local_agent(ToAgent) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => leader_lease_streams,
        to_agent => ToAgent,
        of_group => OfGroup,
        leader => Leader,
        streams => format_stream_progresses(Streams),
        version => Version
    }),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        ?agent_pid(ToAgent),
        ?leader_lease_streams(OfGroup, Leader, Streams, Version)
    ),
    ok;
leader_lease_streams(ToAgent, OfGroup, Leader, Streams, Version) ->
    emqx_ds_shared_sub_proto_v1:leader_lease_streams(
        ?agent_node(ToAgent), ToAgent, OfGroup, Leader, Streams, Version
    ).

-spec leader_renew_stream_lease(agent(), group(), version()) -> ok.
leader_renew_stream_lease(ToAgent, OfGroup, Version) when ?is_local_agent(ToAgent) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => leader_renew_stream_lease,
        to_agent => ToAgent,
        of_group => OfGroup,
        version => Version
    }),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        ?agent_pid(ToAgent),
        ?leader_renew_stream_lease(OfGroup, Version)
    ),
    ok;
leader_renew_stream_lease(ToAgent, OfGroup, Version) ->
    emqx_ds_shared_sub_proto_v1:leader_renew_stream_lease(
        ?agent_node(ToAgent), ToAgent, OfGroup, Version
    ).

-spec leader_renew_stream_lease(agent(), group(), version(), version()) -> ok.
leader_renew_stream_lease(ToAgent, OfGroup, VersionOld, VersionNew) when ?is_local_agent(ToAgent) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => leader_renew_stream_lease,
        to_agent => ToAgent,
        of_group => OfGroup,
        version_old => VersionOld,
        version_new => VersionNew
    }),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        ?agent_pid(ToAgent),
        ?leader_renew_stream_lease(OfGroup, VersionOld, VersionNew)
    ),
    ok;
leader_renew_stream_lease(ToAgent, OfGroup, VersionOld, VersionNew) ->
    emqx_ds_shared_sub_proto_v1:leader_renew_stream_lease(
        ?agent_node(ToAgent), ToAgent, OfGroup, VersionOld, VersionNew
    ).

-spec leader_update_streams(agent(), group(), version(), version(), list(leader_stream_progress())) ->
    ok.
leader_update_streams(ToAgent, OfGroup, VersionOld, VersionNew, StreamsNew) when
    ?is_local_agent(ToAgent)
->
    ?tp(warning, shared_sub_proto_msg, #{
        type => leader_update_streams,
        to_agent => ToAgent,
        of_group => OfGroup,
        version_old => VersionOld,
        version_new => VersionNew,
        streams_new => format_stream_progresses(StreamsNew)
    }),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        ?agent_pid(ToAgent),
        ?leader_update_streams(OfGroup, VersionOld, VersionNew, StreamsNew)
    ),
    ok;
leader_update_streams(ToAgent, OfGroup, VersionOld, VersionNew, StreamsNew) ->
    emqx_ds_shared_sub_proto_v1:leader_update_streams(
        ?agent_node(ToAgent), ToAgent, OfGroup, VersionOld, VersionNew, StreamsNew
    ).

-spec leader_invalidate(agent(), group()) -> ok.
leader_invalidate(ToAgent, OfGroup) when ?is_local_agent(ToAgent) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => leader_invalidate,
        to_agent => ToAgent,
        of_group => OfGroup
    }),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        ?agent_pid(ToAgent),
        ?leader_invalidate(OfGroup)
    ),
    ok;
leader_invalidate(ToAgent, OfGroup) ->
    emqx_ds_shared_sub_proto_v1:leader_invalidate(
        ?agent_node(ToAgent), ToAgent, OfGroup
    ).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

agent(Id, Pid) ->
    _ = Id,
    ?agent(Id, Pid).

format_stream_progresses(Streams) ->
    lists:map(
        fun format_stream_progress/1,
        Streams
    ).

format_stream_progress(#{stream := Stream, progress := Progress} = Value) ->
    Value#{stream => format_opaque(Stream), progress => format_progress(Progress)}.

format_progress(#{iterator := Iterator} = Progress) ->
    Progress#{iterator => format_opaque(Iterator)}.

format_stream_key({SubId, Stream}) ->
    {SubId, format_opaque(Stream)}.

format_stream_keys(StreamKeys) ->
    lists:map(
        fun format_stream_key/1,
        StreamKeys
    ).

format_lease_events(Events) ->
    lists:map(
        fun format_lease_event/1,
        Events
    ).

format_lease_event(#{stream := Stream, progress := Progress} = Event) ->
    Event#{stream => format_opaque(Stream), progress => format_progress(Progress)};
format_lease_event(#{stream := Stream} = Event) ->
    Event#{stream => format_opaque(Stream)}.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

format_opaque(Opaque) ->
    erlang:phash2(Opaque).
