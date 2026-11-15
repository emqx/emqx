%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_BUILTIN_TX_HRL).
-define(EMQX_DS_BUILTIN_TX_HRL, true).

-record(kv_tx_ctx, {
    shard :: emqx_ds:shard(),
    %% Leader at the time of transaction start. It is used for
    %% verification rather than message routing:
    leader :: pid(),
    serial :: term(),
    generation :: emqx_ds:generation(),
    latest_generation :: boolean(),
    opts :: emqx_ds:transaction_opts()
}).

-record(ds_tx, {
    ctx :: emqx_ds_optimistic_tx:ctx(),
    ops :: emqx_ds:tx_ops() | emqx_ds:tx_ops(),
    from :: pid() | reference() | undefined,
    ref :: reference(),
    meta :: term()
}).

-record(ds_del_batch, {
    db :: emqx_ds:db(),
    stream :: emqx_ds:ds_specific_stream(),
    range :: emqx_ds:topic_range(),
    batch_size :: pos_integer(),
    alias :: reference(),
    dirty :: boolean(),
    reserved
}).

-endif.
