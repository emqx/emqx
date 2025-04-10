%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(TOPK_TAB, emqx_slow_subs_topk).
-define(INDEX_TAB, emqx_slow_subs_index).

-define(ID(ClientId, Topic), {ClientId, Topic}).
-define(INDEX(TimeSpan, Id), {Id, TimeSpan}).
-define(TOPK_INDEX(TimeSpan, Id), {TimeSpan, Id}).

-define(MAX_SIZE, 1000).

-record(top_k, {
    index :: topk_index(),
    last_update_time :: pos_integer(),
    extra = []
}).

-record(index_tab, {index :: index()}).

-type top_k() :: #top_k{}.
-type index_tab() :: #index_tab{}.

-type id() :: {emqx_types:clientid(), emqx_types:topic()}.
-type index() :: ?INDEX(non_neg_integer(), id()).
-type topk_index() :: ?TOPK_INDEX(non_neg_integer(), id()).
