%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,

    agent_connect_leader/4,
    agent_update_stream_states/5,
    agent_update_stream_states/6,

    leader_lease_streams/6,
    leader_renew_stream_lease/4,
    leader_renew_stream_lease/5,
    leader_update_streams/6,
    leader_invalidate/3
]).

introduced_in() ->
    "5.8.0".

-spec agent_connect_leader(
    node(),
    emqx_ds_shared_sub_proto:leader(),
    emqx_ds_shared_sub_proto:agent(),
    emqx_ds_shared_sub_proto:topic_filter()
) -> ok.
agent_connect_leader(Node, ToLeader, FromAgent, TopicFilter) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, agent_connect_leader, [
        ToLeader, FromAgent, TopicFilter
    ]).

-spec agent_update_stream_states(
    node(),
    emqx_ds_shared_sub_proto:leader(),
    emqx_ds_shared_sub_proto:agent(),
    list(emqx_ds_shared_sub_proto:agent_stream_progress()),
    emqx_ds_shared_sub_proto:version()
) -> ok.
agent_update_stream_states(Node, ToLeader, FromAgent, StreamProgresses, Version) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, agent_update_stream_states, [
        ToLeader, FromAgent, StreamProgresses, Version
    ]).

-spec agent_update_stream_states(
    node(),
    emqx_ds_shared_sub_proto:leader(),
    emqx_ds_shared_sub_proto:agent(),
    list(emqx_ds_shared_sub_proto:agent_stream_progress()),
    emqx_ds_shared_sub_proto:version(),
    emqx_ds_shared_sub_proto:version()
) -> ok.
agent_update_stream_states(Node, ToLeader, FromAgent, StreamProgresses, VersionOld, VersionNew) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, agent_update_stream_states, [
        ToLeader, FromAgent, StreamProgresses, VersionOld, VersionNew
    ]).

%% leader -> agent messages

-spec leader_lease_streams(
    node(),
    emqx_ds_shared_sub_proto:agent(),
    emqx_ds_shared_sub_proto:group(),
    emqx_ds_shared_sub_proto:leader(),
    list(emqx_ds_shared_sub_proto:stream_progress()),
    emqx_ds_shared_sub_proto:version()
) -> ok.
leader_lease_streams(Node, ToAgent, OfGroup, Leader, Streams, Version) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_lease_streams, [
        ToAgent, OfGroup, Leader, Streams, Version
    ]).

-spec leader_renew_stream_lease(
    node(),
    emqx_ds_shared_sub_proto:agent(),
    emqx_ds_shared_sub_proto:group(),
    emqx_ds_shared_sub_proto:version()
) -> ok.
leader_renew_stream_lease(Node, ToAgent, OfGroup, Version) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_renew_stream_lease, [ToAgent, OfGroup, Version]).

-spec leader_renew_stream_lease(
    node(),
    emqx_ds_shared_sub_proto:agent(),
    emqx_ds_shared_sub_proto:group(),
    emqx_ds_shared_sub_proto:version(),
    emqx_ds_shared_sub_proto:version()
) -> ok.
leader_renew_stream_lease(Node, ToAgent, OfGroup, VersionOld, VersionNew) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_renew_stream_lease, [
        ToAgent, OfGroup, VersionOld, VersionNew
    ]).

-spec leader_update_streams(
    node(),
    emqx_ds_shared_sub_proto:agent(),
    emqx_ds_shared_sub_proto:group(),
    emqx_ds_shared_sub_proto:version(),
    emqx_ds_shared_sub_proto:version(),
    list(emqx_ds_shared_sub_proto:stream_progress())
) -> ok.
leader_update_streams(Node, ToAgent, OfGroup, VersionOld, VersionNew, StreamsNew) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_update_streams, [
        ToAgent, OfGroup, VersionOld, VersionNew, StreamsNew
    ]).

-spec leader_invalidate(node(), emqx_ds_shared_sub_proto:agent(), emqx_ds_shared_sub_proto:group()) ->
    ok.
leader_invalidate(Node, ToAgent, OfGroup) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_invalidate, [ToAgent, OfGroup]).
