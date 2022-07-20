%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(TOPK_TAB, emqx_slow_subs_topk).
-define(INDEX_TAB, emqx_slow_subs_index).

-define(ID(ClientId, Topic), {ClientId, Topic}).
-define(INDEX(TimeSpan, Id), {Id, TimeSpan}).
-define(TOPK_INDEX(TimeSpan, Id), {TimeSpan, Id}).

-define(MAX_SIZE, 1000).

-record(top_k, { index :: topk_index()
               , last_update_time :: pos_integer()
               , extra = []
               }).

-record(index_tab, { index :: index()}).

-type top_k() :: #top_k{}.
-type index_tab() :: #index_tab{}.

-type id() :: {emqx_types:clientid(), emqx_types:topic()}.
-type index() :: ?INDEX(non_neg_integer(), id()).
-type topk_index() :: ?TOPK_INDEX(non_neg_integer(), id()).
