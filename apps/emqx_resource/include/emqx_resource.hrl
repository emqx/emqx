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
-type create_opts() :: #{
    health_check_interval => integer(),
    health_check_timeout => integer(),
    %% We can choose to block the return of emqx_resource:start until
    %% the resource connected, wait max to `wait_for_resource_ready` ms.
    wait_for_resource_ready => integer(),
    %% If `start_after_created` is set to true, the resource is started right
    %% after it is created. But note that a `started` resource is not guaranteed
    %% to be `connected`.
    start_after_created => boolean(),
    %% If the resource disconnected, we can set to retry starting the resource
    %% periodically.
    auto_retry_interval => integer()
}.
-type query_result() ::
    ok
    | {ok, term()}
    | {error, term()}
    | {resource_down, term()}.

-define(TEST_ID_PREFIX, "_test_:").
-define(RES_METRICS, resource_metrics).
