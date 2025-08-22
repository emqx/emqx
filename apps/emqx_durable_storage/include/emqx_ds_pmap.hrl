%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-record(pmap, {
    name,
    module,
    cache = #{},
    dirty = #{}
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
