%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
%% stressing Mria.
%%
%% It's implemented as two maps: `cache', and `dirty'. `cache' stores
%% the data, and `dirty' contains information about dirty and deleted
%% keys. When `commit/1' is called, dirty keys are dumped to the
%% tables, and deleted keys are removed from the tables.
%%
%% `key_mapping' is used only when the feature flag to store session data in DS is
%% enabled.  It maps external keys to unique integers, which are internally used in the
%% topic level structure to avoid costly encodings of arbitrary key terms.
-record(pmap, {
    name :: atom(),
    cache = #{},
    dirty = #{}
}).

%% State structure keys:
-define(id, id).
-define(dirty, dirty).
-define(guard, guard).
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
