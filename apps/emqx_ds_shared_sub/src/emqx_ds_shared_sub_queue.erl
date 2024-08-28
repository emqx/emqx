%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_queue).

-include_lib("emqx/include/emqx_mqtt.hrl").

-export([
    lookup/1,
    lookup/2,
    exists/2,
    declare/4,
    destroy/2
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
    %% FIXME: Routing.
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
    emqx_ds_shared_sub_store:create(Store).

destroy(Group, Topic) ->
    destroy(emqx_ds_shared_sub_store:mk_id(Group, Topic)).

destroy(ID) ->
    %% FIXME: Sync on leader.
    case lookup(ID) of
        false ->
            notfound;
        Queue ->
            emqx_ds_shared_sub_store:destroy(Queue)
    end.
