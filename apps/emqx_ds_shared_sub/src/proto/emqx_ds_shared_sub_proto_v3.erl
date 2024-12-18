%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_proto_v3).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,

    ssubscriber_connect/4,
    ssubscriber_ping/3,
    ssubscriber_disconnect/4,
    ssubscriber_update_progress/4,
    ssubscriber_revoke_finished/4,

    leader_connect_response/3,
    leader_ping_response/3,
    leader_grant/4,
    leader_revoke/4,
    leader_revoked/4,
    leader_invalidate/3
]).

introduced_in() ->
    "5.8.5".

-type share_topic_filter() :: emqx_ds_shared_sub_proto:share_topic_filter().
-type stream() :: emqx_ds_shared_sub_proto:stream().
-type ssubscriber_id() :: emqx_ds_shared_sub_proto:ssubscriber_id().
-type leader() :: emqx_ds_shared_sub_proto:leader().
-type leader_stream_progress() :: emqx_ds_shared_sub_proto:leader_stream_progress().
-type agent_stream_progress() :: emqx_ds_shared_sub_proto:agent_stream_progress().
-type agent_stream_progresses() :: emqx_ds_shared_sub_proto:agent_stream_progresses().

%%--------------------------------------------------------------------
%% SSubscriber -> Leader messages
%%--------------------------------------------------------------------

-spec ssubscriber_connect(node(), leader(), ssubscriber_id(), share_topic_filter()) -> ok.
ssubscriber_connect(Node, ToLeader, FromSSubscriberId, ShareTopicFilter) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, ssubscriber_connect_v3, [
        ToLeader, FromSSubscriberId, ShareTopicFilter
    ]).

-spec ssubscriber_ping(node(), leader(), ssubscriber_id()) -> ok.
ssubscriber_ping(Node, ToLeader, FromSSubscriberId) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, ssubscriber_ping_v3, [ToLeader, FromSSubscriberId]).

-spec ssubscriber_disconnect(node(), leader(), ssubscriber_id(), agent_stream_progresses()) -> ok.
ssubscriber_disconnect(Node, ToLeader, FromSSubscriberId, StreamProgresses) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, ssubscriber_disconnect_v3, [
        ToLeader, FromSSubscriberId, StreamProgresses
    ]).

-spec ssubscriber_update_progress(node(), leader(), ssubscriber_id(), agent_stream_progress()) ->
    ok.
ssubscriber_update_progress(Node, ToLeader, FromSSubscriberId, StreamProgress) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, ssubscriber_update_progress_v3, [
        ToLeader, FromSSubscriberId, StreamProgress
    ]).

-spec ssubscriber_revoke_finished(node(), leader(), ssubscriber_id(), stream()) -> ok.
ssubscriber_revoke_finished(Node, ToLeader, FromSSubscriberId, Stream) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, ssubscriber_revoke_finished_v3, [
        ToLeader, FromSSubscriberId, Stream
    ]).

%%--------------------------------------------------------------------
%% Leader -> SSubscriber messages
%%--------------------------------------------------------------------

-spec leader_connect_response(node(), ssubscriber_id(), leader()) -> ok.
leader_connect_response(Node, ToSSubscriberId, FromLeader) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_connect_response_v3, [
        ToSSubscriberId, FromLeader
    ]).

-spec leader_ping_response(node(), ssubscriber_id(), leader()) -> ok.
leader_ping_response(Node, ToSSubscriberId, FromLeader) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_ping_response_v3, [ToSSubscriberId, FromLeader]).

-spec leader_grant(node(), ssubscriber_id(), leader(), leader_stream_progress()) -> ok.
leader_grant(Node, ToSSubscriberId, FromLeader, StreamProgress) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_grant_v3, [
        ToSSubscriberId, FromLeader, StreamProgress
    ]).

-spec leader_revoke(node(), ssubscriber_id(), leader(), stream()) -> ok.
leader_revoke(Node, ToSSubscriberId, FromLeader, Stream) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_revoke_v3, [
        ToSSubscriberId, FromLeader, Stream
    ]).

-spec leader_revoked(node(), ssubscriber_id(), leader(), stream()) -> ok.
leader_revoked(Node, ToSSubscriberId, FromLeader, Stream) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_revoked_v3, [
        ToSSubscriberId, FromLeader, Stream
    ]).

-spec leader_invalidate(node(), ssubscriber_id(), leader()) -> ok.
leader_invalidate(Node, ToSSubscriberId, FromLeader) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_invalidate_v3, [ToSSubscriberId, FromLeader]).
