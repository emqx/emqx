%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    username :: binary(),
    pwdhash :: binary(),
    description :: binary(),
    role = undefined :: atom(),
    %% not used so far, for future extension
    extra = [] :: term()
}).

-define(ADMIN_JWT, emqx_admin_jwt).

-record(?ADMIN_JWT, {
    token :: binary(),
    username :: binary(),
    exptime :: integer(),
    %% not used so far, fur future extension
    extra = [] :: term()
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
    connections
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
