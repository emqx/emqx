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
    leader_revoked/4
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
    erpc:cast(Node, emqx_ds_shared_sub_proto, ssubscriber_connect, [
        ToLeader, FromSSubscriberId, ShareTopicFilter
    ]).

-spec ssubscriber_ping(node(), leader(), ssubscriber_id()) -> ok.
ssubscriber_ping(Node, ToLeader, FromSSubscriberId) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, ssubscriber_ping, [ToLeader, FromSSubscriberId]).

-spec ssubscriber_disconnect(node(), leader(), ssubscriber_id(), agent_stream_progresses()) -> ok.
ssubscriber_disconnect(Node, ToLeader, FromSSubscriberId, StreamProgresses) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, ssubscriber_disconnect, [
        ToLeader, FromSSubscriberId, StreamProgresses
    ]).

-spec ssubscriber_update_progress(node(), leader(), ssubscriber_id(), agent_stream_progress()) ->
    ok.
ssubscriber_update_progress(Node, ToLeader, FromSSubscriberId, StreamProgress) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, ssubscriber_update_progress, [
        ToLeader, FromSSubscriberId, StreamProgress
    ]).

-spec ssubscriber_revoke_finished(node(), leader(), ssubscriber_id(), stream()) -> ok.
ssubscriber_revoke_finished(Node, ToLeader, FromSSubscriberId, Stream) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, ssubscriber_revoke_finished, [
        ToLeader, FromSSubscriberId, Stream
    ]).

%%--------------------------------------------------------------------
%% Leader -> SSubscriber messages
%%--------------------------------------------------------------------

-spec leader_connect_response(node(), leader(), ssubscriber_id()) -> ok.
leader_connect_response(Node, ToLeader, FromSSubscriberId) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_connect_response, [ToLeader, FromSSubscriberId]).

-spec leader_ping_response(node(), leader(), ssubscriber_id()) -> ok.
leader_ping_response(Node, ToLeader, FromSSubscriberId) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_ping_response, [ToLeader, FromSSubscriberId]).

-spec leader_grant(node(), leader(), ssubscriber_id(), leader_stream_progress()) -> ok.
leader_grant(Node, ToLeader, FromSSubscriberId, StreamProgress) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_grant, [
        ToLeader, FromSSubscriberId, StreamProgress
    ]).

-spec leader_revoke(node(), leader(), ssubscriber_id(), stream()) -> ok.
leader_revoke(Node, ToLeader, FromSSubscriberId, Stream) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_revoke, [ToLeader, FromSSubscriberId, Stream]).

-spec leader_revoked(node(), leader(), ssubscriber_id(), stream()) -> ok.
leader_revoked(Node, ToLeader, FromSSubscriberId, Stream) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_revoked, [ToLeader, FromSSubscriberId, Stream]).
