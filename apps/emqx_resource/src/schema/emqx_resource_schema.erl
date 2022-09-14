%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_resource_schema).

-include("emqx_resource.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([namespace/0, roots/0, fields/1, desc/1]).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions

namespace() -> "resource_schema".

roots() -> [].

fields("resource_opts") ->
    [
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(<<"resource_opts">>)
                }
            )}
    ];
fields("creation_opts") ->
    [
        {worker_pool_size, fun worker_pool_size/1},
        {health_check_interval, fun health_check_interval/1},
        {auto_restart_interval, fun auto_restart_interval/1},
        {query_mode, fun query_mode/1},
        {async_inflight_window, fun async_inflight_window/1},
        {enable_batch, fun enable_batch/1},
        {batch_size, fun batch_size/1},
        {batch_time, fun batch_time/1},
        {enable_queue, fun enable_queue/1},
        {max_queue_bytes, fun max_queue_bytes/1}
    ].

worker_pool_size(type) -> pos_integer();
worker_pool_size(desc) -> ?DESC("worker_pool_size");
worker_pool_size(default) -> ?WORKER_POOL_SIZE;
worker_pool_size(required) -> false;
worker_pool_size(_) -> undefined.

health_check_interval(type) -> emqx_schema:duration_ms();
health_check_interval(desc) -> ?DESC("health_check_interval");
health_check_interval(default) -> ?HEALTHCHECK_INTERVAL_RAW;
health_check_interval(required) -> false;
health_check_interval(_) -> undefined.

auto_restart_interval(type) -> hoconsc:union([infinity, emqx_schema:duration_ms()]);
auto_restart_interval(desc) -> ?DESC("auto_restart_interval");
auto_restart_interval(default) -> ?AUTO_RESTART_INTERVAL_RAW;
auto_restart_interval(required) -> false;
auto_restart_interval(_) -> undefined.

query_mode(type) -> enum([sync, async]);
query_mode(desc) -> ?DESC("query_mode");
query_mode(default) -> async;
query_mode(required) -> false;
query_mode(_) -> undefined.

enable_batch(type) -> boolean();
enable_batch(required) -> false;
enable_batch(default) -> true;
enable_batch(desc) -> ?DESC("enable_batch");
enable_batch(_) -> undefined.

enable_queue(type) -> boolean();
enable_queue(required) -> false;
enable_queue(default) -> false;
enable_queue(desc) -> ?DESC("enable_queue");
enable_queue(_) -> undefined.

async_inflight_window(type) -> pos_integer();
async_inflight_window(desc) -> ?DESC("async_inflight_window");
async_inflight_window(default) -> ?DEFAULT_INFLIGHT;
async_inflight_window(required) -> false;
async_inflight_window(_) -> undefined.

batch_size(type) -> pos_integer();
batch_size(desc) -> ?DESC("batch_size");
batch_size(default) -> ?DEFAULT_BATCH_SIZE;
batch_size(required) -> false;
batch_size(_) -> undefined.

batch_time(type) -> emqx_schema:duration_ms();
batch_time(desc) -> ?DESC("batch_time");
batch_time(default) -> ?DEFAULT_BATCH_TIME_RAW;
batch_time(required) -> false;
batch_time(_) -> undefined.

max_queue_bytes(type) -> emqx_schema:bytesize();
max_queue_bytes(desc) -> ?DESC("max_queue_bytes");
max_queue_bytes(default) -> ?DEFAULT_QUEUE_SIZE_RAW;
max_queue_bytes(required) -> false;
max_queue_bytes(_) -> undefined.

desc("creation_opts") ->
    ?DESC("creation_opts").
