%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_RETAINER_HRL).
-define(EMQX_RETAINER_HRL, true).

-include_lib("emqx/include/emqx.hrl").

-define(APP, emqx_retainer).
-define(TAB_MESSAGE, emqx_retainer_message).
-define(TAB_INDEX, emqx_retainer_index).
-define(TAB_INDEX_META, emqx_retainer_index_meta).
-define(RETAINER_SHARD, emqx_retainer_shard).

-define(RETAINER_LIMITER_GROUP, emqx_retainer).
-define(DISPATCHER_LIMITER_NAME, dispatcher).
-define(PUBLISHER_LIMITER_NAME, publisher).

-define(DISPATCHER_POOL, emqx_retainer_dispatcher).

%% Reply of a dispatcher worker, or of a read in the channel, with one batch of a retained read
-record(retained_batch, {
    topic_filter :: emqx_types:topic(),
    request_ref :: reference(),
    messages :: [emqx_types:message()],
    %% `done' when no more messages follow
    cursor :: term()
}).

-endif.
