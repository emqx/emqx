%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_PMAP_HRL).
-define(EMQX_PMAP_HRL, true).

-include_lib("emqx_durable_storage/include/emqx_ds_pmap.hrl").

%% State structure keys:
-define(id, id).
-define(checkpoint_ref, checkpoint_ref).

%%   Pmap names:
-define(metadata, metadata).
-define(subscriptions, subscriptions).
-define(subscription_states, subscription_states).
-define(seqnos, seqnos).
-define(streams, streams).
-define(ranks, ranks).
-define(awaiting_rel, awaiting_rel).

-endif.
