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
%%     exist and at which content version; mnesia recovers it exactly
%%     on node restart, deletes included;
%%   - the blob table (rocksdb) holds the validated JSON source on
%%     disk, so the payloads do not reside in RAM. Restart recovery of
%%     the rocksdb backend does not remove records deleted while the
%%     node was down; such stale blobs have no index entry, are never
%%     served, and are purged on reconcile.
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

-record(maptab, {
    name :: binary(),
    json :: binary(),
    version :: binary(),
    updated_at :: integer(),
    %% reserved for future use
    extra = #{} :: map()
}).
