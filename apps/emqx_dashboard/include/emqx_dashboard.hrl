%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-define(ADMIN, emqx_admin).

-record(?ADMIN, {
    username         :: binary(),
    pwdhash          :: binary(),
    description      :: binary(),
    role = undefined :: atom(),
    extra = []       :: term() %% not used so far, for future extension
    }).


-define(ADMIN_JWT, emqx_admin_jwt).

-record(?ADMIN_JWT, {
    token      :: binary(),
    username   :: binary(),
    exptime    :: integer(),
    extra = [] :: term() %% not used so far, fur future extension
    }).

-define(TAB_COLLECT, emqx_collect).

-define(EMPTY_KEY(Key), ((Key == undefined) orelse (Key == <<>>))).

-define(DASHBOARD_SHARD, emqx_dashboard_shard).

-record(mqtt_collect, {
    timestamp :: integer(),
    collect
    }).
