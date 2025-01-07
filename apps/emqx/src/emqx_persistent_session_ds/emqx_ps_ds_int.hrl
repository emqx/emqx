%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-ifndef(EMQX_PS_DS_HRL).
-define(EMQX_PS_DS_HRL, true).

-define(PS_ROUTER_TAB, emqx_ds_ps_router).
-define(PS_FILTERS_TAB, emqx_ds_ps_filters).

-record(ps_route, {
    topic :: binary(),
    dest :: emqx_persistent_session_ds_router:dest() | '_'
}).

-record(ps_routeidx, {
    entry :: '$1' | emqx_topic_index:key(emqx_persistent_session_ds_router:dest()),
    unused = [] :: nil() | '_'
}).

-endif.
