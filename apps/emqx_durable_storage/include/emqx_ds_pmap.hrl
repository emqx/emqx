%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_PMAP_HRL).
-define(EMQX_DS_PMAP_HRL, true).

%% Persistent map.
%%
%% Pmap accumulates the updates in a term stored in the heap of a
%% process, so they can be committed all at once in a single
%% transaction.
%%
%% It should be possible to make frequent changes to the pmap without
%% stressing DS.
%%
%% It's implemented as two maps: `cache', and `dirty'. `cache' stores
%% the data, and `dirty' contains information about dirty and deleted
%% keys. When `commit/1' is called, dirty keys are dumped to the
%% tables, and deleted keys are removed from the tables.
%%
%% Transaction counter is stepped each time there's an attempt to
%% commit pmap to the storage.
-record(pmap, {
    name,
    module,
    cache = #{},
    dirty = #{},
    tx_counter = 0
}).

%% A "diff"-style persistent map that tries to avoid unnecessary
%% writes and deletions to the store that lead to the identical
%% persistent state:
-record(pdiffmap, {
    name,
    module,
    %% Simulated persistent state:
    pstate = #{},
    %% Data being flushed to the storage by an ongoing async commit.
    %% If the application updates a key while it's already in-flight
    %% to the storage, it's unconditionally added to the diff. This is
    %% because the commit may timeout with system ending in unknown
    %% state: with changes applied or not. We resolve this ambiguity
    %% in the simulated state by overwriting such keys, at the cost of
    %% potentially duplicate writes.
    pending = #{},
    %% The diff between the persistent state and the working copy of
    %% the data:
    diff = #{}
}).

-define(top_data, <<"d">>).
-define(top_guard, <<"g">>).

-define(pmap_topic(PMAP, COLLECTION, KEY), [?top_data, COLLECTION, PMAP, KEY]).
-define(guard_topic(COLLECTION), [?top_guard, COLLECTION]).

%% Collection

-define(collection_guard, collection_guard).
-define(collection_dirty, collection_dirty).

%% Enable this flag if you suspect some code breaks the sequence:
-ifndef(CHECK_SEQNO).
-define(set_dirty, ?collection_dirty := true).
-define(unset_dirty, ?collection_dirty := false).
-else.
-define(set_dirty, ?collection_dirty := true, '_' => emqx_ds_pmap:do_seqno()).
-define(unset_dirty, ?collection_dirty := false, '_' => emqx_ds_pmap:do_seqno()).
-endif.

-endif.
