%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_proto).

-include("emqx_ds_shared_sub_proto.hrl").
-include("emqx_ds_shared_sub_format.hrl").

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
-type agent_stream_progresses() :: list(agent_stream_progress()).
-type stream() :: emqx_ds:stream().

-type to_leader_msg() ::
    ?ssubscriber_ping_match(ssubscriber_id())
    | ?ssubscriber_connect_match(ssubscriber_id(), share_topic_filter())
    | ?ssubscriber_disconnect_match(ssubscriber_id(), agent_stream_progresses())
    | ?ssubscriber_update_progress_match(ssubscriber_id(), agent_stream_progress())
    | ?ssubscriber_revoke_finished_match(ssubscriber_id(), emqx_ds:stream()).

-type to_ssubscriber_msg() ::
    ?leader_connect_response_match(leader())
    | ?leader_ping_response_match(leader())
    | ?leader_grant_match(leader(), leader_stream_progress())
    | ?leader_revoke_match(leader(), emqx_ds:stream())
    | ?leader_revoked_match(leader(), emqx_ds:stream()).

-export_type([
    leader/0,
    stream/0,
    share_topic_filter/0,
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
    ssubscriber_connect_v3/3,
    ssubscriber_ping_v3/2,
    ssubscriber_disconnect_v3/3,
    ssubscriber_update_progress_v3/3,
    ssubscriber_revoke_finished_v3/3,

    leader_connect_response_v3/2,
    leader_ping_response_v3/2,
    leader_grant_v3/3,
    leader_revoke_v3/3,
    leader_revoked_v3/3
]).

-export([
    ssubscriber_id/3,
    ssubscriber_pidref/1,
    ssubscriber_subscription_id/1
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
send_to_leader(ToLeader, ?ssubscriber_connect_match(FromSSubscriberId, ShareTopicFilter)) ->
    emqx_ds_shared_sub_proto_v3:ssubscriber_connect(
        ?ssubscriber_node(FromSSubscriberId), ToLeader, FromSSubscriberId, ShareTopicFilter
    );
send_to_leader(ToLeader, ?ssubscriber_ping_match(FromSSubscriberId)) ->
    emqx_ds_shared_sub_proto_v3:ssubscriber_ping(
        ?ssubscriber_node(FromSSubscriberId), ToLeader, FromSSubscriberId
    );
send_to_leader(ToLeader, ?ssubscriber_disconnect_match(FromSSubscriberId, StreamProgresses)) ->
    emqx_ds_shared_sub_proto_v3:ssubscriber_disconnect(
        ?ssubscriber_node(FromSSubscriberId), ToLeader, FromSSubscriberId, StreamProgresses
    );
send_to_leader(ToLeader, ?ssubscriber_update_progress_match(FromSSubscriberId, StreamProgress)) ->
    emqx_ds_shared_sub_proto_v3:ssubscriber_update_progress(
        ?ssubscriber_node(FromSSubscriberId), ToLeader, FromSSubscriberId, StreamProgress
    );
send_to_leader(ToLeader, ?ssubscriber_revoke_finished_match(FromSSubscriberId, Stream)) ->
    emqx_ds_shared_sub_proto_v3:ssubscriber_revoke_finished(
        ?ssubscriber_node(FromSSubscriberId), ToLeader, FromSSubscriberId, Stream
    ).

-spec send_to_ssubscriber(ssubscriber_id(), to_ssubscriber_msg()) -> ok.
send_to_ssubscriber(ToSSubscriberId, Msg) when ?is_local_ssubscriber(ToSSubscriberId) ->
    ?log_leader_msg(ToSSubscriberId, Msg),
    _ = emqx_ds_shared_sub_agent:send_to_ssubscriber(ToSSubscriberId, Msg),
    ok;
send_to_ssubscriber(ToSSubscriberId, ?leader_connect_response_match(FromLeader)) ->
    emqx_ds_shared_sub_proto_v3:leader_connect_response(
        ?ssubscriber_node(ToSSubscriberId), ToSSubscriberId, FromLeader
    );
send_to_ssubscriber(ToSSubscriberId, ?leader_ping_response_match(FromLeader)) ->
    emqx_ds_shared_sub_proto_v3:leader_ping_response(
        ?ssubscriber_node(ToSSubscriberId), ToSSubscriberId, FromLeader
    );
send_to_ssubscriber(ToSSubscriberId, ?leader_grant_match(FromLeader, StreamProgress)) ->
    emqx_ds_shared_sub_proto_v3:leader_grant(
        ?ssubscriber_node(ToSSubscriberId), ToSSubscriberId, FromLeader, StreamProgress
    );
send_to_ssubscriber(ToSSubscriberId, ?leader_revoke_match(FromLeader, Stream)) ->
    emqx_ds_shared_sub_proto_v3:leader_revoke(
        ?ssubscriber_node(ToSSubscriberId), ToSSubscriberId, FromLeader, Stream
    );
send_to_ssubscriber(ToSSubscriberId, ?leader_revoked_match(FromLeader, Stream)) ->
    emqx_ds_shared_sub_proto_v3:leader_revoked(
        ?ssubscriber_node(ToSSubscriberId), ToSSubscriberId, FromLeader, Stream
    ).

%%--------------------------------------------------------------------
%% RPC Targets
%%--------------------------------------------------------------------

-spec ssubscriber_connect_v3(leader(), ssubscriber_id(), share_topic_filter()) -> ok.
ssubscriber_connect_v3(ToLeader, FromSSubscriberId, ShareTopicFilter) ->
    send_to_leader(ToLeader, ?ssubscriber_connect(FromSSubscriberId, ShareTopicFilter)).

-spec ssubscriber_ping_v3(leader(), ssubscriber_id()) -> ok.
ssubscriber_ping_v3(ToLeader, FromSSubscriberId) ->
    send_to_leader(ToLeader, ?ssubscriber_ping(FromSSubscriberId)).

-spec ssubscriber_disconnect_v3(leader(), ssubscriber_id(), agent_stream_progresses()) -> ok.
ssubscriber_disconnect_v3(ToLeader, FromSSubscriberId, StreamProgresses) ->
    send_to_leader(ToLeader, ?ssubscriber_disconnect(FromSSubscriberId, StreamProgresses)).

-spec ssubscriber_update_progress_v3(leader(), ssubscriber_id(), agent_stream_progress()) -> ok.
ssubscriber_update_progress_v3(ToLeader, FromSSubscriberId, StreamProgress) ->
    send_to_leader(ToLeader, ?ssubscriber_update_progress(FromSSubscriberId, StreamProgress)).

-spec ssubscriber_revoke_finished_v3(leader(), ssubscriber_id(), stream()) -> ok.
ssubscriber_revoke_finished_v3(ToLeader, FromSSubscriberId, Stream) ->
    send_to_leader(ToLeader, ?ssubscriber_revoke_finished(FromSSubscriberId, Stream)).

-spec leader_connect_response_v3(leader(), ssubscriber_id()) -> ok.
leader_connect_response_v3(ToLeader, FromSSubscriberId) ->
    send_to_ssubscriber(ToLeader, ?leader_connect_response(FromSSubscriberId)).

-spec leader_ping_response_v3(leader(), ssubscriber_id()) -> ok.
leader_ping_response_v3(ToLeader, FromSSubscriberId) ->
    send_to_ssubscriber(ToLeader, ?leader_ping_response(FromSSubscriberId)).

-spec leader_grant_v3(leader(), ssubscriber_id(), leader_stream_progress()) -> ok.
leader_grant_v3(ToLeader, FromSSubscriberId, StreamProgress) ->
    send_to_ssubscriber(ToLeader, ?leader_grant(FromSSubscriberId, StreamProgress)).

-spec leader_revoke_v3(leader(), ssubscriber_id(), stream()) -> ok.
leader_revoke_v3(ToLeader, FromSSubscriberId, Stream) ->
    send_to_ssubscriber(ToLeader, ?leader_revoke(FromSSubscriberId, Stream)).

-spec leader_revoked_v3(leader(), ssubscriber_id(), stream()) -> ok.
leader_revoked_v3(ToLeader, FromSSubscriberId, Stream) ->
    send_to_ssubscriber(ToLeader, ?leader_revoked(FromSSubscriberId, Stream)).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

-spec ssubscriber_id(
    emqx_persistent_session_ds:id(),
    emqx_persistent_session_ds_shared_subs_agent:subscription_id(),
    reference()
) ->
    ssubscriber_id().
ssubscriber_id(SessionId, SubscriptionId, PidRef) ->
    ?ssubscriber_id(SessionId, SubscriptionId, PidRef).

-spec ssubscriber_pidref(ssubscriber_id()) -> reference().
ssubscriber_pidref(SSubscriberId) ->
    ?ssubscriber_pidref(SSubscriberId).

-spec ssubscriber_subscription_id(ssubscriber_id()) ->
    emqx_persistent_session_ds_shared_subs_agent:subscription_id().
ssubscriber_subscription_id(SSubscriberId) ->
    ?ssubscriber_subscription_id(SSubscriberId).

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
