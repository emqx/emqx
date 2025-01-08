%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_proto_v3).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,

    borrower_connect/4,
    borrower_ping/3,
    borrower_disconnect/4,
    borrower_update_progress/4,
    borrower_revoke_finished/4,

    leader_connect_response/3,
    leader_ping_response/3,
    leader_grant/4,
    leader_revoke/4,
    leader_revoked/4,
    leader_invalidate/3
]).

introduced_in() ->
    "5.9.0".

-type share_topic_filter() :: emqx_ds_shared_sub_proto:share_topic_filter().
-type stream() :: emqx_ds_shared_sub_proto:stream().
-type borrower_id() :: emqx_ds_shared_sub_proto:borrower_id().
-type leader() :: emqx_ds_shared_sub_proto:leader().
-type leader_stream_progress() :: emqx_ds_shared_sub_proto:leader_stream_progress().
-type agent_stream_progress() :: emqx_ds_shared_sub_proto:agent_stream_progress().
-type agent_stream_progresses() :: emqx_ds_shared_sub_proto:agent_stream_progresses().

%%--------------------------------------------------------------------
%% Borrower -> Leader messages
%%--------------------------------------------------------------------

-spec borrower_connect(node(), leader(), borrower_id(), share_topic_filter()) -> ok.
borrower_connect(Node, ToLeader, FromBorrowerId, ShareTopicFilter) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, borrower_connect_v3, [
        ToLeader, FromBorrowerId, ShareTopicFilter
    ]).

-spec borrower_ping(node(), leader(), borrower_id()) -> ok.
borrower_ping(Node, ToLeader, FromBorrowerId) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, borrower_ping_v3, [ToLeader, FromBorrowerId]).

-spec borrower_disconnect(node(), leader(), borrower_id(), agent_stream_progresses()) -> ok.
borrower_disconnect(Node, ToLeader, FromBorrowerId, StreamProgresses) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, borrower_disconnect_v3, [
        ToLeader, FromBorrowerId, StreamProgresses
    ]).

-spec borrower_update_progress(node(), leader(), borrower_id(), agent_stream_progress()) ->
    ok.
borrower_update_progress(Node, ToLeader, FromBorrowerId, StreamProgress) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, borrower_update_progress_v3, [
        ToLeader, FromBorrowerId, StreamProgress
    ]).

-spec borrower_revoke_finished(node(), leader(), borrower_id(), stream()) -> ok.
borrower_revoke_finished(Node, ToLeader, FromBorrowerId, Stream) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, borrower_revoke_finished_v3, [
        ToLeader, FromBorrowerId, Stream
    ]).

%%--------------------------------------------------------------------
%% Leader -> Borrower messages
%%--------------------------------------------------------------------

-spec leader_connect_response(node(), borrower_id(), leader()) -> ok.
leader_connect_response(Node, ToBorrowerId, FromLeader) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_connect_response_v3, [
        ToBorrowerId, FromLeader
    ]).

-spec leader_ping_response(node(), borrower_id(), leader()) -> ok.
leader_ping_response(Node, ToBorrowerId, FromLeader) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_ping_response_v3, [ToBorrowerId, FromLeader]).

-spec leader_grant(node(), borrower_id(), leader(), leader_stream_progress()) -> ok.
leader_grant(Node, ToBorrowerId, FromLeader, StreamProgress) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_grant_v3, [
        ToBorrowerId, FromLeader, StreamProgress
    ]).

-spec leader_revoke(node(), borrower_id(), leader(), stream()) -> ok.
leader_revoke(Node, ToBorrowerId, FromLeader, Stream) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_revoke_v3, [
        ToBorrowerId, FromLeader, Stream
    ]).

-spec leader_revoked(node(), borrower_id(), leader(), stream()) -> ok.
leader_revoked(Node, ToBorrowerId, FromLeader, Stream) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_revoked_v3, [
        ToBorrowerId, FromLeader, Stream
    ]).

-spec leader_invalidate(node(), borrower_id(), leader()) -> ok.
leader_invalidate(Node, ToBorrowerId, FromLeader) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, leader_invalidate_v3, [ToBorrowerId, FromLeader]).
