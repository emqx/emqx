%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_queue).

-export([
    lookup/1,
    lookup/2,
    exists/2,
    declare/4,
    destroy/1,
    destroy/2
]).

-export([
    id/1,
    properties/1
]).

%%

lookup(Group, Topic) ->
    lookup(emqx_ds_shared_sub_store:mk_id(Group, Topic)).

lookup(ID) ->
    emqx_ds_shared_sub_store:open(ID).

exists(Group, Topic) ->
    ID = emqx_ds_shared_sub_store:mk_id(Group, Topic),
    emqx_ds_shared_sub_store:exists(ID).

declare(Group, Topic, CreatedAt, StartTime) ->
    ID = emqx_ds_shared_sub_store:mk_id(Group, Topic),
    Props = #{
        group => Group,
        topic => Topic,
        start_time => StartTime,
        created_at => CreatedAt
    },
    RankProgress = emqx_ds_shared_sub_leader_rank_progress:init(),
    Store0 = emqx_ds_shared_sub_store:init(ID),
    Store1 = emqx_ds_shared_sub_store:set(properties, Props, Store0),
    Store = emqx_ds_shared_sub_store:set(rank_progress, RankProgress, Store1),
    %% TODO: There's an obvious lack of transactionality.
    case emqx_ds_shared_sub_store:create(Store) of
        {ok, _} = Ok ->
            _ = ensure_route(Topic, ID),
            Ok;
        exists ->
            _ = ensure_route(Topic, ID),
            exists;
        Error ->
            Error
    end.

destroy(Group, Topic) ->
    destroy(emqx_ds_shared_sub_store:mk_id(Group, Topic)).

destroy(ID) ->
    %% TODO: There's an obvious lack of transactionality.
    case lookup(ID) of
        {ok, Queue} ->
            #{topic := Topic} = properties(Queue),
            case emqx_ds_shared_sub_store:destroy(Queue) of
                ok ->
                    _ = ensure_delete_route(Topic, ID),
                    ok;
                Error ->
                    Error
            end;
        false ->
            not_found
    end.

ensure_route(Topic, QueueID) ->
    _ = emqx_persistent_session_ds_router:do_add_route(Topic, QueueID),
    _ = emqx_external_broker:add_persistent_route(Topic, QueueID),
    ok.

ensure_delete_route(Topic, QueueID) ->
    %% TODO
    %% Potentially broken ordering assumptions? This delete op is not serialized with
    %% respective add op, it's possible (yet extremely unlikely) that they will arrive
    %% to the external broker out-of-order.
    _ = emqx_external_broker:delete_persistent_route(Topic, QueueID),
    _ = emqx_persistent_session_ds_router:do_delete_route(Topic, QueueID),
    ok.

%%

id(Queue) ->
    emqx_ds_shared_sub_store:id(Queue).

properties(Queue) ->
    emqx_ds_shared_sub_store:get(properties, Queue).
