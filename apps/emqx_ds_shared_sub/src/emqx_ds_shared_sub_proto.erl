%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_proto).

-include("emqx_ds_shared_sub_proto.hrl").
-include("emqx_ds_shared_sub_format.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-type borrower_id() :: ?borrower_id(
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
    ?borrower_ping_match(borrower_id())
    | ?borrower_connect_match(borrower_id(), share_topic_filter())
    | ?borrower_disconnect_match(borrower_id(), agent_stream_progresses())
    | ?borrower_update_progress_match(borrower_id(), agent_stream_progress())
    | ?borrower_revoke_finished_match(borrower_id(), emqx_ds:stream()).

-type to_borrower_msg() ::
    ?leader_connect_response_match(leader())
    | ?leader_ping_response_match(leader())
    | ?leader_grant_match(leader(), leader_stream_progress())
    | ?leader_revoke_match(leader(), emqx_ds:stream())
    | ?leader_revoked_match(leader(), emqx_ds:stream())
    | ?leader_invalidate_match(leader()).

-export_type([
    leader/0,
    stream/0,
    share_topic_filter/0,
    borrower_id/0,
    leader_stream_progress/0,
    agent_stream_progress/0,
    agent_stream_progresses/0,
    to_leader_msg/0,
    to_borrower_msg/0
]).

-export([
    send_to_leader/2,
    send_to_borrower/2
]).

-export([
    borrower_connect_v3/3,
    borrower_ping_v3/2,
    borrower_disconnect_v3/3,
    borrower_update_progress_v3/3,
    borrower_revoke_finished_v3/3,

    leader_connect_response_v3/2,
    leader_ping_response_v3/2,
    leader_grant_v3/3,
    leader_revoke_v3/3,
    leader_revoked_v3/3,
    leader_invalidate_v3/2
]).

-export([
    borrower_id/3,
    borrower_pidref/1,
    borrower_subscription_id/1
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
    ?log_borrower_msg(ToLeader, Msg),
    _ = erlang:send(ToLeader, Msg),
    ok;
send_to_leader(ToLeader, ?borrower_connect_match(FromBorrowerId, ShareTopicFilter)) ->
    emqx_ds_shared_sub_proto_v3:borrower_connect(
        ?borrower_node(FromBorrowerId), ToLeader, FromBorrowerId, ShareTopicFilter
    );
send_to_leader(ToLeader, ?borrower_ping_match(FromBorrowerId)) ->
    emqx_ds_shared_sub_proto_v3:borrower_ping(
        ?borrower_node(FromBorrowerId), ToLeader, FromBorrowerId
    );
send_to_leader(ToLeader, ?borrower_disconnect_match(FromBorrowerId, StreamProgresses)) ->
    emqx_ds_shared_sub_proto_v3:borrower_disconnect(
        ?borrower_node(FromBorrowerId), ToLeader, FromBorrowerId, StreamProgresses
    );
send_to_leader(ToLeader, ?borrower_update_progress_match(FromBorrowerId, StreamProgress)) ->
    emqx_ds_shared_sub_proto_v3:borrower_update_progress(
        ?borrower_node(FromBorrowerId), ToLeader, FromBorrowerId, StreamProgress
    );
send_to_leader(ToLeader, ?borrower_revoke_finished_match(FromBorrowerId, Stream)) ->
    emqx_ds_shared_sub_proto_v3:borrower_revoke_finished(
        ?borrower_node(FromBorrowerId), ToLeader, FromBorrowerId, Stream
    ).

-spec send_to_borrower(borrower_id(), to_borrower_msg()) -> ok.
send_to_borrower(ToBorrowerId, Msg) when ?is_local_borrower(ToBorrowerId) ->
    ?log_leader_msg(ToBorrowerId, Msg),
    _ = emqx_ds_shared_sub_agent:send_to_borrower(ToBorrowerId, Msg),
    ok;
send_to_borrower(ToBorrowerId, ?leader_connect_response_match(FromLeader)) ->
    emqx_ds_shared_sub_proto_v3:leader_connect_response(
        ?borrower_node(ToBorrowerId), ToBorrowerId, FromLeader
    );
send_to_borrower(ToBorrowerId, ?leader_ping_response_match(FromLeader)) ->
    emqx_ds_shared_sub_proto_v3:leader_ping_response(
        ?borrower_node(ToBorrowerId), ToBorrowerId, FromLeader
    );
send_to_borrower(ToBorrowerId, ?leader_grant_match(FromLeader, StreamProgress)) ->
    emqx_ds_shared_sub_proto_v3:leader_grant(
        ?borrower_node(ToBorrowerId), ToBorrowerId, FromLeader, StreamProgress
    );
send_to_borrower(ToBorrowerId, ?leader_revoke_match(FromLeader, Stream)) ->
    emqx_ds_shared_sub_proto_v3:leader_revoke(
        ?borrower_node(ToBorrowerId), ToBorrowerId, FromLeader, Stream
    );
send_to_borrower(ToBorrowerId, ?leader_revoked_match(FromLeader, Stream)) ->
    emqx_ds_shared_sub_proto_v3:leader_revoked(
        ?borrower_node(ToBorrowerId), ToBorrowerId, FromLeader, Stream
    );
send_to_borrower(ToBorrowerId, ?leader_invalidate_match(FromLeader)) ->
    emqx_ds_shared_sub_proto_v3:leader_invalidate(
        ?borrower_node(ToBorrowerId), ToBorrowerId, FromLeader
    ).

%%--------------------------------------------------------------------
%% RPC Targets
%%--------------------------------------------------------------------

%% borrower -> leader messages

-spec borrower_connect_v3(leader(), borrower_id(), share_topic_filter()) -> ok.
borrower_connect_v3(ToLeader, FromBorrowerId, ShareTopicFilter) ->
    send_to_leader(ToLeader, ?borrower_connect(FromBorrowerId, ShareTopicFilter)).

-spec borrower_ping_v3(leader(), borrower_id()) -> ok.
borrower_ping_v3(ToLeader, FromBorrowerId) ->
    send_to_leader(ToLeader, ?borrower_ping(FromBorrowerId)).

-spec borrower_disconnect_v3(leader(), borrower_id(), agent_stream_progresses()) -> ok.
borrower_disconnect_v3(ToLeader, FromBorrowerId, StreamProgresses) ->
    send_to_leader(ToLeader, ?borrower_disconnect(FromBorrowerId, StreamProgresses)).

-spec borrower_update_progress_v3(leader(), borrower_id(), agent_stream_progress()) -> ok.
borrower_update_progress_v3(ToLeader, FromBorrowerId, StreamProgress) ->
    send_to_leader(ToLeader, ?borrower_update_progress(FromBorrowerId, StreamProgress)).

-spec borrower_revoke_finished_v3(leader(), borrower_id(), stream()) -> ok.
borrower_revoke_finished_v3(ToLeader, FromBorrowerId, Stream) ->
    send_to_leader(ToLeader, ?borrower_revoke_finished(FromBorrowerId, Stream)).

%% leader -> borrower messages

-spec leader_connect_response_v3(borrower_id(), leader()) -> ok.
leader_connect_response_v3(ToBorrowerId, FromLeader) ->
    send_to_borrower(ToBorrowerId, ?leader_connect_response(FromLeader)).

-spec leader_ping_response_v3(borrower_id(), leader()) -> ok.
leader_ping_response_v3(ToBorrowerId, FromLeader) ->
    send_to_borrower(ToBorrowerId, ?leader_ping_response(FromLeader)).

-spec leader_grant_v3(borrower_id(), leader(), leader_stream_progress()) -> ok.
leader_grant_v3(ToBorrowerId, FromLeader, StreamProgress) ->
    send_to_borrower(ToBorrowerId, ?leader_grant(FromLeader, StreamProgress)).

-spec leader_revoke_v3(borrower_id(), leader(), stream()) -> ok.
leader_revoke_v3(ToBorrowerId, FromLeader, Stream) ->
    send_to_borrower(ToBorrowerId, ?leader_revoke(FromLeader, Stream)).

-spec leader_revoked_v3(borrower_id(), leader(), stream()) -> ok.
leader_revoked_v3(ToBorrowerId, FromLeader, Stream) ->
    send_to_borrower(ToBorrowerId, ?leader_revoked(FromLeader, Stream)).

-spec leader_invalidate_v3(borrower_id(), leader()) -> ok.
leader_invalidate_v3(ToBorrowerId, FromLeader) ->
    send_to_borrower(ToBorrowerId, ?leader_invalidate(FromLeader)).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

-spec borrower_id(
    emqx_persistent_session_ds:id(),
    emqx_persistent_session_ds_shared_subs_agent:subscription_id(),
    reference()
) ->
    borrower_id().
borrower_id(SessionId, SubscriptionId, PidRef) ->
    ?borrower_id(SessionId, SubscriptionId, PidRef).

-spec borrower_pidref(borrower_id()) -> reference().
borrower_pidref(BorrowerId) ->
    ?borrower_pidref(BorrowerId).

-spec borrower_subscription_id(borrower_id()) ->
    emqx_persistent_session_ds_shared_subs_agent:subscription_id().
borrower_subscription_id(BorrowerId) ->
    ?borrower_subscription_id(BorrowerId).

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
