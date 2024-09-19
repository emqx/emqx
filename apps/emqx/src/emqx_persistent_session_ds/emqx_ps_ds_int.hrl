%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(PS_ROUTER_EXT_TAB, emqx_ds_ps_router_ext).
-define(PS_FILTERS_EXT_TAB, emqx_ds_ps_filters_ext).

-define(PS_ROUTER_TABS, [
    ?PS_ROUTER_TAB,
    ?PS_FILTERS_TAB,
    ?PS_ROUTER_EXT_TAB,
    ?PS_FILTERS_EXT_TAB
]).

-record(ps_route, {
    topic :: binary() | emqx_utils_ets:pat(),
    dest :: emqx_persistent_session_ds_router:dest() | emqx_utils_ets:pat()
}).

-record(ps_route_ext, {
    entry :: {
        _Topic :: binary() | emqx_utils_ets:pat(),
        _Scope :: pos_integer() | emqx_utils_ets:pat(),
        emqx_persistent_session_ds_router:dest() | emqx_utils_ets:pat()
    },
    extra = [] :: nil() | '_'
}).

-record(ps_routeidx, {
    entry ::
        emqx_topic_index:key(emqx_persistent_session_ds_router:dest())
        | emqx_utils_ets:pat(),
    extra = [] :: nil() | '_'
}).

-record(ps_routeidx_ext, {
    entry ::
        {
            _Scope :: pos_integer() | emqx_utils_ets:pat(),
            emqx_topic_index:key(emqx_persistent_session_ds_router:dest()) | emqx_utils_ets:pat()
        },
    extra = [] :: nil() | '_'
}).

-endif.
