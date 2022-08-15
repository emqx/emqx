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
-type resource_type() :: module().
-type resource_id() :: binary().
-type manager_id() :: binary().
-type raw_resource_config() :: binary() | raw_term_resource_config().
-type raw_term_resource_config() :: #{binary() => term()} | [raw_term_resource_config()].
-type resource_config() :: term().
-type resource_spec() :: map().
-type resource_state() :: term().
-type resource_status() :: connected | disconnected | connecting | stopped.
-type callback_mode() :: always_sync | async_if_possible.
-type result() :: term().
-type reply_fun() :: {fun((result(), Args :: term()) -> any()), Args :: term()} | undefined.
-type query_opts() :: #{
    %% The key used for picking a resource worker
    pick_key => term(),
    async_reply_fun => reply_fun()
}.
-type resource_data() :: #{
    id := resource_id(),
    mod := module(),
    callback_mode := callback_mode(),
    config := resource_config(),
    state := resource_state(),
    status := resource_status(),
    metrics := emqx_metrics_worker:metrics()
}.
-type resource_group() :: binary().
-type creation_opts() :: #{
    %%======================================= Deprecated Opts:
    %% use health_check_interval instead
    health_check_timeout => integer(),
    %% use start_timeout instead
    wait_for_resource_ready => integer(),
    %% use auto_restart_interval instead
    auto_retry_interval => integer(),
    %%======================================= Deprecated Opts End
    health_check_interval => integer(),
    %% We can choose to block the return of emqx_resource:start until
    %% the resource connected, wait max to `start_timeout` ms.
    start_timeout => integer(),
    %% If `start_after_created` is set to true, the resource is started right
    %% after it is created. But note that a `started` resource is not guaranteed
    %% to be `connected`.
    start_after_created => boolean(),
    %% If the resource disconnected, we can set to retry starting the resource
    %% periodically.
    auto_restart_interval => integer(),
    enable_batch => boolean(),
    batch_size => integer(),
    batch_time => integer(),
    enable_queue => boolean(),
    queue_max_bytes => integer(),
    query_mode => async | sync | dynamic,
    async_inflight_window => integer()
}.
-type query_result() ::
    ok
    | {ok, term()}
    | {error, term()}
    | {resource_down, term()}.

-define(DEFAULT_QUEUE_SIZE, 1024 * 1024 * 1024).
-define(DEFAULT_QUEUE_SIZE_RAW, <<"1GB">>).

%% count
-define(DEFAULT_BATCH_SIZE, 100).

%% milliseconds
-define(DEFAULT_BATCH_TIME, 10).
-define(DEFAULT_BATCH_TIME_RAW, <<"10ms">>).

%% count
-define(DEFAULT_INFLIGHT, 100).

%% milliseconds
-define(HEALTHCHECK_INTERVAL, 15000).
-define(HEALTHCHECK_INTERVAL_RAW, <<"15s">>).

-define(START_AFTER_CREATED, true).

%% milliseconds
-define(START_TIMEOUT, 5000).
-define(START_TIMEOUT_RAW, <<"5s">>).

%% milliseconds
-define(AUTO_RESTART_INTERVAL, 60000).
-define(AUTO_RESTART_INTERVAL_RAW, <<"60s">>).

-define(TEST_ID_PREFIX, "_test_:").
-define(RES_METRICS, resource_metrics).
