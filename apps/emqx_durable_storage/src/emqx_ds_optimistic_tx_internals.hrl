%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_OPTIMISTIC_TX_INTERNALS_HRL).
-define(EMQX_DS_OPTIMISTIC_TX_INTERNALS_HRL, true).

-define(name(DB, SHARD), {n, l, {emqx_ds_optimistic_tx, DB, SHARD}}).
-define(via(DB, SHARD), {via, gproc, ?name(DB, SHARD)}).

-endif.
