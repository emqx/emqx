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

-ifndef(EMQX_RESOURCE_HRL).
-define(EMQX_RESOURCE_HRL, true).
%% bridge/connector/action status
-define(status_connected, connected).
-define(status_connecting, connecting).
-define(status_disconnected, disconnected).
%% Note: the `stopped' status can only be emitted by `emqx_resource_manager'...  Modules
%% implementing `emqx_resource' behavior should not return it.  The `rm_' prefix is to
%% remind us of that.
-define(rm_status_stopped, stopped).

-type resource_type() :: atom().
-type resource_module() :: module().
-type resource_id() :: binary().
-type channel_id() :: action_resource_id() | source_resource_id().
-type raw_resource_config() :: binary() | raw_term_resource_config().
-type raw_term_resource_config() :: #{binary() => term()} | [raw_term_resource_config()].
-type resource_config() :: term().
-type resource_state() :: term().
%% Note: the `stopped' status can only be emitted by `emqx_resource_manager'...  Modules
%% implementing `emqx_resource' behavior should not return it.
-type resource_status() ::
    ?status_connected | ?status_disconnected | ?status_connecting | ?rm_status_stopped.
-type health_check_status() :: ?status_connected | ?status_disconnected | ?status_connecting.
-type channel_status() :: ?status_connected | ?status_connecting | ?status_disconnected.
-type callback_mode() :: always_sync | async_if_possible.
-type query_mode() :: resource_query_mode().
-type resource_query_mode() ::
    simple_sync
    | simple_async
    | simple_sync_internal_buffer
    | simple_async_internal_buffer
    | sync
    | async
    | no_queries.
-type query_kind() :: sync | async.
-type result() :: term().
-type reply_fun() ::
    {fun((...) -> any()), Args :: [term()]}
    | {fun((...) -> any()), Args :: [term()], reply_context()}
    | undefined.
-type reply_context() :: #{reply_dropped => boolean()}.
-type query_opts() :: #{
    %% The key used for picking a resource worker
    pick_key => term(),
    timeout => timeout(),
    expire_at => infinity | integer(),
    async_reply_fun => reply_fun(),
    simple_query => boolean(),
    reply_to => reply_fun(),
    %% Called `query_mode' due to legacy reasons...
    query_mode => query_kind(),
    connector_resource_id => resource_id(),
    is_fallback => boolean()
}.
-type resource_data() :: #{
    id := resource_id(),
    mod := module(),
    callback_mode := callback_mode(),
    query_mode := resource_query_mode(),
    config := resource_config(),
    error := term(),
    status := resource_status(),
    added_channels := term(),
    state := resource_state()
}.
-type resource_group() :: binary().
-type creation_opts() :: #{
    %%======================================= Deprecated Opts BEGIN
    %% use health_check_interval instead
    health_check_timeout => integer(),
    %% use start_timeout instead
    wait_for_resource_ready => integer(),
    %% use health_check_interval instead
    auto_retry_interval => integer(),
    %% use health_check_interval instead
    auto_restart_interval => pos_integer() | infinity,
    %%======================================= Deprecated Opts END
    worker_pool_size => non_neg_integer(),
    %% use `integer()` compatibility to release 5.0.0 bpapi
    health_check_interval => integer(),
    %% We can choose to block the return of emqx_resource:start until
    %% the resource connected, wait max to `start_timeout` ms.
    start_timeout => pos_integer(),
    %% If `start_after_created` is set to true, the resource is started right
    %% after it is created. But note that a `started` resource is not guaranteed
    %% to be `connected`.
    start_after_created => boolean(),
    batch_size => pos_integer(),
    batch_time => pos_integer(),
    max_buffer_bytes => pos_integer(),
    query_mode => resource_query_mode(),
    resume_interval => pos_integer(),
    inflight_window => pos_integer(),
    %% Only for `emqx_resource_manager' usage.  If false, prevents spawning buffer
    %% workers, regardless of resource query mode.
    spawn_buffer_workers => boolean()
}.
-type query_result() ::
    ok
    | {ok, term()}
    | {ok, term(), term()}
    | {ok, term(), term(), term()}
    | {error, {recoverable_error, term()}}
    | {error, term()}.

-type batch_query_result() :: query_result() | [query_result()].

-type action_resource_id() :: resource_id().
-type source_resource_id() :: resource_id().
-type connector_resource_id() :: resource_id().
-type message_tag() :: action_resource_id().

-define(WORKER_POOL_SIZE, 16).

-define(DEFAULT_BUFFER_BYTES, 256 * 1024 * 1024).
-define(DEFAULT_BUFFER_BYTES_RAW, <<"256MB">>).

-define(DEFAULT_REQUEST_TTL, timer:seconds(45)).
-define(DEFAULT_REQUEST_TTL_RAW, <<"45s">>).

%% count
-define(DEFAULT_BATCH_SIZE, 1).

%% milliseconds
-define(DEFAULT_BATCH_TIME, 0).
-define(DEFAULT_BATCH_TIME_RAW, <<"0ms">>).

%% count
-define(DEFAULT_INFLIGHT, 100).

%% milliseconds
-define(HEALTHCHECK_INTERVAL, 15000).
-define(HEALTHCHECK_INTERVAL_RAW, <<"15s">>).

%% milliseconds
-define(DEFAULT_METRICS_FLUSH_INTERVAL, 5_000).
-define(DEFAULT_METRICS_FLUSH_INTERVAL_RAW, <<"5s">>).

%% milliseconds
-define(START_TIMEOUT, 5000).
-define(START_TIMEOUT_RAW, <<"5s">>).

%% boolean
-define(START_AFTER_CREATED, true).

%% Keep this test_id_prefix is match "^[A-Za-z0-9]+[A-Za-z0-9-_]*$".
%% See `hocon_tconf`
-define(PROBE_ID_PREFIX, "PROBE_").
-define(PROBE_ID_RAND_BYTES, 8).
-define(PROBE_ID_NEW(),
    iolist_to_binary([?PROBE_ID_PREFIX, emqx_utils:rand_id(?PROBE_ID_RAND_BYTES)])
).
-define(PROBE_ID_MATCH(Suffix), <<?PROBE_ID_PREFIX, _:?PROBE_ID_RAND_BYTES/binary, Suffix/binary>>).
-define(RES_METRICS, resource_metrics).
-define(LOG_LEVEL(_L_),
    case _L_ of
        true -> info;
        false -> warning
    end
).
-define(TAG, "RESOURCE").

-define(RESOURCE_ALLOCATION_TAB, emqx_resource_allocations).
-define(RESOURCE_CACHE, emqx_resource_cache).

-endif.
