%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% The registry maps table name to its current data-table generation:
%% `{Name, Tid, Meta}'. Owned by emqx_maptabs_server, read directly
%% (no gen_server call) by the emqx_maptabs lookup hot path.
-define(MAPTABS_REGISTRY, emqx_maptabs_registry).

%% Durable, cluster-replicated storage: one record per mapping table,
%% holding the validated JSON source. A single-record write is atomic,
%% which preserves the whole-table generation-swap guarantee when the
%% cache is rebuilt from it.
-define(MAPTABS_TAB, emqx_maptabs).
-define(MAPTABS_SHARD, emqx_maptabs_shard).

-record(maptab, {
    name :: binary(),
    json :: binary(),
    version :: binary(),
    updated_at :: integer()
}).
