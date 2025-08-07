%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_PMAP_HRL).
-define(EMQX_PMAP_HRL, true).

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
    name :: atom(),
    cache = #{},
    dirty = #{}
}).

%% State structure keys:
-define(id, id).
-define(dirty, dirty).
-define(guard, guard).
-define(checkpoint_ref, checkpoint_ref).

%%   Pmap names:
-define(metadata, metadata).
-define(subscriptions, subscriptions).
-define(subscription_states, subscription_states).
-define(seqnos, seqnos).
-define(streams, streams).
-define(ranks, ranks).
-define(awaiting_rel, awaiting_rel).

%% Enable this flag if you suspect some code breaks the sequence:
-ifndef(CHECK_SEQNO).
-define(set_dirty, dirty := true).
-define(unset_dirty, dirty := false).
-else.
-define(set_dirty, dirty := true, '_' => do_seqno()).
-define(unset_dirty, dirty := false, '_' => do_seqno()).
-endif.

-endif.
