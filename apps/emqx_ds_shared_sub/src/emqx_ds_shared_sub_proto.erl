%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_proto).

-include("emqx_ds_shared_sub_proto.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-type ssubscriber_id() :: ?ssubscriber_id(
    emqx_persistent_session_ds_shared_subs_agent:subscription_id(),
    emqx_persistent_session_ds:id(),
    reference()
).
-type leader() :: pid().
-type share_topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type leader_stream_progress() :: #{
    stream := emqx_ds:stream(),
    progress := emqx_persistent_session_ds_shared_subs:progress()
}.
-type agent_stream_progress() :: emqx_persistent_session_ds_shared_subs:agent_stream_progress().

-type to_leader_msg() ::
    ?ssubscriber_ping_match(ssubscriber_id())
    | ?ssubscriber_connect_match(ssubscriber_id(), share_topic_filter()).

-type to_ssubscriber_msg() ::
    ?leader_connect_response_match(leader())
    | ?leader_ping_response_match(leader()).

-export_type([
    leader/0,
    ssubscriber_id/0,
    leader_stream_progress/0,
    agent_stream_progress/0,
    to_leader_msg/0,
    to_ssubscriber_msg/0
]).

-export([
    send_to_leader/2,
    send_to_ssubscriber/2
]).

-export([
    ssubscriber_id/3,
    ssubscriber_pidref/1
]).

%% Legacy types
-type agent() :: term().
-type group() :: emqx_types:group().
-type version() :: non_neg_integer().
-type agent_metadata() :: #{
    id := emqx_persistent_session_ds:id()
}.

-export_type([
    agent/0,
    group/0,
    version/0,
    agent_metadata/0
]).

%% Legacy exports
-export([
    agent_connect_leader/4,
    agent_update_stream_states/4,
    agent_update_stream_states/5,
    agent_disconnect/4,

    leader_lease_streams/5,
    leader_renew_stream_lease/3,
    leader_renew_stream_lease/4,
    leader_update_streams/5,
    leader_invalidate/2,

    leader_lease_streams_v2/5,
    leader_renew_stream_lease_v2/3,
    leader_renew_stream_lease_v2/4,
    leader_update_streams_v2/5,
    leader_invalidate_v2/2
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec send_to_leader(leader(), to_leader_msg()) -> ok.
send_to_leader(ToLeader, Msg) when ?is_local_leader(ToLeader) ->
    ?log_ssubscriber_msg(ToLeader, Msg),
    _ = erlang:send(ToLeader, Msg),
    ok;
send_to_leader(ToLeader, Msg) ->
    emqx_ds_shared_sub_proto_v3:send_to_leader(node(ToLeader), ToLeader, Msg).

-spec send_to_ssubscriber(ssubscriber_id(), to_ssubscriber_msg()) -> ok.
send_to_ssubscriber(ToSSubscriberId, Msg) when ?is_local_ssubscriber(ToSSubscriberId) ->
    ?log_leader_msg(ToSSubscriberId, Msg),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        ?ssubscriber_pidref(ToSSubscriberId), ?ssubscriber_subscription_id(ToSSubscriberId), {
            ?ssubscriber_pidref(ToSSubscriberId), Msg
        }
    ),
    ok;
send_to_ssubscriber(ToSSubscriberId, Msg) ->
    emqx_ds_shared_sub_proto_v3:send_to_ssubscriber(
        ?ssubscriber_node(ToSSubscriberId), ToSSubscriberId, Msg
    ).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

ssubscriber_id(SessionId, SubscriptionId, PidRef) ->
    ?ssubscriber_id(SessionId, SubscriptionId, PidRef).

ssubscriber_pidref(SSubscriberId) ->
    ?ssubscriber_pidref(SSubscriberId).

%%--------------------------------------------------------------------
%% Legacy API
%%--------------------------------------------------------------------

%% agent -> leader messages

-spec agent_connect_leader(leader(), agent(), agent_metadata(), share_topic_filter()) -> ok.
agent_connect_leader(_ToLeader, _FromAgent, _AgentMetadata, _ShareTopicFilter) -> ok.

-spec agent_update_stream_states(leader(), agent(), list(agent_stream_progress()), version()) -> ok.
agent_update_stream_states(_ToLeader, _FromAgent, _StreamProgresses, _Version) -> ok.

-spec agent_update_stream_states(
    leader(), agent(), list(agent_stream_progress()), version(), version()
) -> ok.
agent_update_stream_states(_ToLeader, _FromAgent, _StreamProgresses, _VersionOld, _VersionNew) ->
    ok.

agent_disconnect(_ToLeader, _FromAgent, _StreamProgresses, _Version) -> ok.

%% leader -> agent messages

-spec leader_lease_streams_v2(
    agent(), group(), leader(), list(leader_stream_progress()), version()
) ->
    ok.
leader_lease_streams_v2(_ToAgent, _OfGroup, _Leader, _Streams, _Version) -> ok.

-spec leader_renew_stream_lease_v2(agent(), group(), version()) -> ok.
leader_renew_stream_lease_v2(_ToAgent, _OfGroup, _Version) -> ok.

-spec leader_renew_stream_lease_v2(agent(), group(), version(), version()) -> ok.
leader_renew_stream_lease_v2(_ToAgent, _OfGroup, _VersionOld, _VersionNew) -> ok.

-spec leader_update_streams_v2(
    agent(), group(), version(), version(), list(leader_stream_progress())
) ->
    ok.
leader_update_streams_v2(_ToAgent, _OfGroup, _VersionOld, _VersionNew, _StreamsNew) -> ok.

-spec leader_invalidate_v2(agent(), group()) -> ok.
leader_invalidate_v2(_ToAgent, _OfGroup) -> ok.

%% v1,v2 targets for leader <-> agent messages. Ignore them.

-spec leader_lease_streams(term(), group(), leader(), list(leader_stream_progress()), version()) ->
    ok.
leader_lease_streams(_ToAgent, _OfGroup, _Leader, _Streams, _Version) -> ok.

-spec leader_renew_stream_lease(term(), group(), version()) -> ok.
leader_renew_stream_lease(_ToAgent, _OfGroup, _Version) -> ok.

-spec leader_renew_stream_lease(term(), group(), version(), version()) -> ok.
leader_renew_stream_lease(_ToAgent, _OfGroup, _VersionOld, _VersionNew) -> ok.

-spec leader_update_streams(term(), group(), version(), version(), list(leader_stream_progress())) ->
    ok.
leader_update_streams(_ToAgent, _OfGroup, _VersionOld, _VersionNew, _StreamsNew) -> ok.

-spec leader_invalidate(term(), group()) -> ok.
leader_invalidate(_ToAgent, _OfGroup) -> ok.
