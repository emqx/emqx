%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_PS_DS_HRL).
-define(EMQX_PS_DS_HRL, true).

-define(PS_ROUTER_TAB, emqx_ds_ps_router).
-define(PS_FILTERS_TAB, emqx_ds_ps_filters).

-record(ps_route, {
    topic :: binary(),
    dest :: emqx_persistent_session_ds_router:dest() | '_'
}).

-record(ps_routeidx, {
    entry :: '$1' | emqx_topic_index:key(emqx_persistent_session_ds_router:dest()),
    unused = [] :: nil() | '_'
}).

-endif.
