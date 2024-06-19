%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% TODO
%% This should be wrapped with a proto_v1 module.
%% For simplicity, send as simple OTP messages for now.

-module(emqx_ds_shared_sub_proto).

-include("emqx_ds_shared_sub_proto.hrl").

-export([
    agent_connect_leader/3,
    agent_update_stream_states/4,

    leader_lease_streams/4,
    leader_renew_stream_lease/3
]).

-type agent() :: pid().
-type leader() :: pid().
-type topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type group() :: emqx_types:group().
-type version() :: non_neg_integer().

-type stream_progress() :: #{
    stream := emqx_ds:stream(),
    iterator := emqx_ds:iterator()
}.

-export_type([
    agent/0,
    leader/0,
    group/0,
    version/0,
    stream_progress/0
]).

%% agent -> leader messages

-spec agent_connect_leader(leader(), agent(), topic_filter()) -> ok.
agent_connect_leader(ToLeader, FromAgent, TopicFilter) ->
    _ = erlang:send(ToLeader, ?agent_connect_leader(FromAgent, TopicFilter)),
    ok.

-spec agent_update_stream_states(leader(), agent(), list(stream_progress()), version()) -> ok.
agent_update_stream_states(ToLeader, FromAgent, StreamProgresses, Version) ->
    _ = erlang:send(ToLeader, ?agent_update_stream_states(FromAgent, StreamProgresses, Version)),
    ok.

%% ...

%% leader -> agent messages

-spec leader_lease_streams(agent(), group(), list(stream_progress()), version()) -> ok.
leader_lease_streams(ToAgent, OfGroup, Streams, Version) ->
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        ToAgent,
        ?leader_lease_streams(OfGroup, Streams, Version)
    ),
    ok.

-spec leader_renew_stream_lease(agent(), group(), version()) -> ok.
leader_renew_stream_lease(ToAgent, OfGroup, Version) ->
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        ToAgent,
        ?leader_renew_stream_lease(OfGroup, Version)
    ),
    ok.

%% ...
