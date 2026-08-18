%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% The registry maps table name to its current data-table generation:
%% `{Name, Tid, Meta}'. Owned by emqx_maptabs_server, read directly
%% (no gen_server call) by the emqx_maptabs lookup hot path.
-define(MAPTABS_REGISTRY, emqx_maptabs_registry).

%% Durable, cluster-replicated storage, split over two tables written
%% in one transaction:
%%   - the index (disc_copies, tiny) is the authority on which tables
%%     exist and at which content version; it also serves the cheap
%%     queries: the table count (mnesia:table_info(size) is unreliable
%%     on rocksdb tables) and the per-table versions used by reconcile;
%%   - the blob table (rocksdb) holds the validated JSON source on
%%     disk, so the payloads do not reside in RAM.
-define(MAPTABS_INDEX_TAB, emqx_maptabs_index).
-define(MAPTABS_TAB, emqx_maptabs).
-define(MAPTABS_SHARD, emqx_maptabs_shard).

-record(maptab_index, {
    name :: binary(),
    version :: binary(),
    updated_at :: integer(),
    %% reserved for future use
    extra = #{} :: map()
}).

%% version and updated_at live in the index only
-record(maptab, {
    name :: binary(),
    json :: binary(),
    %% reserved for future use
    extra = #{} :: map()
}).
