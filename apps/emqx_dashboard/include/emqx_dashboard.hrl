%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% TODO:
%% The predefined roles of the preliminary RBAC implementation,
%% these may be removed when developing the full RBAC feature.
%% In full RBAC feature, the role may be customised created and deleted,
%% a predefined configuration would replace these macros.
-define(ROLE_VIEWER, <<"viewer">>).
-define(ROLE_SUPERUSER, <<"superuser">>).

-if(?EMQX_RELEASE_EDITION == ee).
-define(ROLE_DEFAULT, ?ROLE_VIEWER).
-else.
-define(ROLE_DEFAULT, ?ROLE_SUPERUSER).
-endif.

-record(?ADMIN, {
    username :: binary(),
    pwdhash :: binary(),
    description :: binary(),
    role = ?ROLE_DEFAULT :: binary(),
    extra = #{} :: map()
}).

-type dashboard_user_role() :: binary().
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
    %, received_bytes
    sent,
    %, sent_bytes
    dropped
]).

-define(GAUGE_SAMPLER_LIST, [
    subscriptions,
    topics,
    connections,
    live_connections
]).

-define(SAMPLER_LIST, ?GAUGE_SAMPLER_LIST ++ ?DELTA_SAMPLER_LIST).

-define(DELTA_SAMPLER_RATE_MAP, #{
    received => received_msg_rate,
    %% In 5.0.0, temporarily comment it to suppress bytes rate
    %received_bytes  => received_bytes_rate,
    %sent_bytes      => sent_bytes_rate,
    sent => sent_msg_rate,
    dropped => dropped_msg_rate
}).
