%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("emqx_dashboard_rbac.hrl").

-define(ADMIN, emqx_admin).

-define(BACKEND_LOCAL, local).
-define(SSO_USERNAME(Backend, Name), {Backend, Name}).

-type dashboard_sso_backend() :: atom().
-type dashboard_sso_username() :: {dashboard_sso_backend(), binary()}.
-type dashboard_username() :: binary() | dashboard_sso_username().
-type dashboard_user_role() :: binary().

-record(?ADMIN, {
    username :: dashboard_username(),
    pwdhash :: binary(),
    description :: binary(),
    role = ?ROLE_DEFAULT :: dashboard_user_role(),
    extra = #{} :: map()
}).

-type dashboard_user() :: #?ADMIN{}.

-define(ADMIN_JWT, emqx_admin_jwt).

-record(?ADMIN_JWT, {
    token :: binary(),
    username :: binary(),
    exptime :: integer(),
    extra = #{} :: map()
}).

-define(TAB_COLLECT, emqx_collect).

-define(EMPTY_KEY(Key), ((Key == undefined) orelse (Key == <<>>))).

-define(DASHBOARD_SHARD, emqx_dashboard_shard).

-ifdef(TEST).
%% for test
-define(DEFAULT_SAMPLE_INTERVAL, 1).
-define(RPC_TIMEOUT, 50).
-else.
%% dashboard monitor do sample interval, default 10s
-define(DEFAULT_SAMPLE_INTERVAL, 10).
-define(RPC_TIMEOUT, 5000).
-endif.

-define(DELTA_SAMPLER_LIST, [
    received,
    sent,
    validation_succeeded,
    validation_failed,
    transformation_succeeded,
    transformation_failed,
    dropped,
    persisted
]).

-define(GAUGE_SAMPLER_LIST, [
    disconnected_durable_sessions,
    subscriptions_durable,
    subscriptions,
    topics,
    connections,
    live_connections
]).

-define(SAMPLER_LIST, (?GAUGE_SAMPLER_LIST ++ ?DELTA_SAMPLER_LIST)).

-define(DELTA_SAMPLER_RATE_MAP, #{
    received => received_msg_rate,
    sent => sent_msg_rate,
    validation_succeeded => validation_succeeded_rate,
    validation_failed => validation_failed_rate,
    transformation_succeeded => transformation_succeeded_rate,
    transformation_failed => transformation_failed_rate,
    dropped => dropped_msg_rate,
    persisted => persisted_rate
}).

-define(CURRENT_SAMPLE_NON_RATE,
    [
        node_uptime,
        retained_msg_count,
        shared_subscriptions
    ] ++ ?LICENSE_QUOTA
).

-define(CLUSTERONLY_SAMPLER_LIST, [
    subscriptions_durable,
    disconnected_durable_sessions
]).

-if(?EMQX_RELEASE_EDITION == ee).
-define(LICENSE_QUOTA, [license_quota]).
-else.
-define(LICENSE_QUOTA, []).
-endif.
