%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_resource_manager).

-feature(maybe_expr, enable).

-behaviour(gen_statem).

-include("emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

% API
-export([
    ensure_resource/5,
    recreate/4,
    remove/1,
    create_dry_run/2,
    create_dry_run/3,
    create_dry_run/4,
    restart/2,
    start/2,
    stop/1,
    health_check/1,
    channel_health_check/2,
    add_channel/3,
    add_channel/4,
    remove_channel/2,
    get_channels/1
]).

-export([
    lookup/1,
    list_all/0,
    list_group/1,
    lookup_cached/1,
    is_exist/1,
    get_metrics/1,
    reset_metrics/1,
    get_query_mode_and_last_error/2
]).

-export([
    set_resource_status_connecting/1,
    external_error/1
]).

% Server
-export([start_link/5, where/1]).

% Behaviour
-export([init/1, callback_mode/0, handle_event/4, terminate/3]).

%% Internal exports.
-export([worker_resource_health_check/1, worker_channel_health_check/2]).

-ifdef(TEST).
-export([stop/2]).
-endif.

%%------------------------------------------------------------------------------
%% Type definitions
%%------------------------------------------------------------------------------

-define(not_added_yet, {?MODULE, not_added_yet}).
-define(add_channel_failed(REASON), {?MODULE, add_channel_failed, REASON}).

% State record
-record(data, {
    id,
    group,
    type,
    mod,
    callback_mode,
    query_mode,
    config,
    opts,
    status,
    state,
    error,
    pid,
    added_channels = #{},
    %% Reference to process performing resource health check.
    hc_workers = #{
        resource => #{},
        channel => #{
            ongoing => #{}
        }
    } :: #{
        resource := #{pid() => true},
        channel := #{
            pid() => channel_id(),
            ongoing := #{channel_id() => channel_status_map()}
        }
    },
    %% Callers waiting on health check
    hc_pending_callers = #{resource => [], channel => #{}} :: #{
        resource := [gen_statem:from()],
        channel := #{channel_id() => [gen_statem:from()]}
    },
    extra
}).

-type data() :: #data{}.
-type channel_status_map() :: #{status := channel_status(), error := term()}.

-define(NAME(ResId), {n, l, {?MODULE, ResId}}).
-define(REF(ResId), {via, gproc, ?NAME(ResId)}).

-define(WAIT_FOR_RESOURCE_DELAY, 100).
-define(T_OPERATION, 5000).
-define(T_LOOKUP, 1000).

%% `gen_statem' states
%% Note: most of them coincide with resource _status_.  We use a different set of macros
%% to avoid mixing those concepts up.
%% Also note: the `stopped' _status_ can only be emitted by `emqx_resource_manager'...
%% Modules implementing `emqx_resource' behavior should not return it.
-define(state_connected, connected).
-define(state_connecting, connecting).
-define(state_disconnected, disconnected).
-define(state_stopped, stopped).

-type state() ::
    ?state_stopped
    | ?state_disconnected
    | ?state_connecting
    | ?state_connected.

-define(IS_STATUS(ST),
    ST =:= ?status_connecting; ST =:= ?status_connected; ST =:= ?status_disconnected
).

-type add_channel_opts() :: #{
    %% Whether to immediately perform a health check after adding the channel.
    %% Default: `true'
    perform_health_check => boolean()
}.

%% calls/casts/generic timeouts
-record(add_channel, {channel_id :: channel_id(), config :: map()}).
-record(start_channel_health_check, {channel_id :: channel_id()}).
-record(retry_add_channel, {channel_id :: channel_id()}).

-type generic_timeout(Id, Content) :: {{timeout, Id}, timeout(), Content}.
-type start_channel_health_check_action() :: generic_timeout(
    #start_channel_health_check{}, #start_channel_health_check{}
).
-type retry_add_channel_action() :: generic_timeout(
    #retry_add_channel{}, #retry_add_channel{}
).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

where(ResId) ->
    gproc:where(?NAME(ResId)).

%% @doc Called from emqx_resource when starting a resource instance.
%%
%% Triggers the emqx_resource_manager_sup supervisor to actually create
%% and link the process itself if not already started.
-spec ensure_resource(
    resource_id(),
    resource_group(),
    resource_module(),
    resource_config(),
    creation_opts()
) -> {ok, resource_data()}.
ensure_resource(ResId, Group, ResourceType, Config, Opts) ->
    case lookup(ResId) of
        {ok, _Group, Data} ->
            {ok, Data};
        {error, not_found} ->
            create_and_return_data(ResId, Group, ResourceType, Config, Opts)
    end.

%% @doc Called from emqx_resource when recreating a resource which may or may not exist
-spec recreate(
    resource_id(), resource_module(), resource_config(), creation_opts()
) ->
    {ok, resource_data()} | {error, not_found} | {error, updating_to_incorrect_resource_type}.
recreate(ResId, ResourceType, NewConfig, Opts) ->
    case lookup(ResId) of
        {ok, Group, #{mod := ResourceType, status := _} = _Data} ->
            _ = remove(ResId, false),
            create_and_return_data(ResId, Group, ResourceType, NewConfig, Opts);
        {ok, _, #{mod := Mod}} when Mod =/= ResourceType ->
            {error, updating_to_incorrect_resource_type};
        {error, not_found} ->
            {error, not_found}
    end.

create_and_return_data(ResId, Group, ResourceType, Config, Opts) ->
    _ = create(ResId, Group, ResourceType, Config, Opts),
    {ok, _Group, Data} = lookup(ResId),
    {ok, Data}.

%% @doc Create a resource_manager and wait until it is running
create(ResId, Group, ResourceType, Config, Opts) ->
    % The state machine will make the actual call to the callback/resource module after init
    ok = emqx_resource_manager_sup:ensure_child(ResId, Group, ResourceType, Config, Opts),
    % Create metrics for the resource
    ok = emqx_resource:create_metrics(ResId),
    QueryMode = emqx_resource:query_mode(ResourceType, Config, Opts),
    SpawnBufferWorkers = maps:get(spawn_buffer_workers, Opts, true),
    case SpawnBufferWorkers andalso lists:member(QueryMode, [sync, async]) of
        true ->
            %% start resource workers as the query type requires them
            ok = emqx_resource_buffer_worker_sup:start_workers(ResId, Opts);
        false ->
            ok
    end,
    case maps:get(start_after_created, Opts, ?START_AFTER_CREATED) of
        true ->
            wait_for_ready(ResId, maps:get(start_timeout, Opts, ?START_TIMEOUT));
        false ->
            ok
    end.

%% @doc Called from `emqx_resource` when doing a dry run for creating a resource instance.
%%
%% Triggers the `emqx_resource_manager_sup` supervisor to actually create
%% and link the process itself if not already started, and then immediately stops.
-spec create_dry_run(resource_module(), resource_config()) ->
    ok | {error, Reason :: term()}.
create_dry_run(ResourceType, Config) ->
    ResId = ?PROBE_ID_NEW(),
    create_dry_run(ResId, ResourceType, Config).

create_dry_run(ResId, ResourceType, Config) ->
    create_dry_run(ResId, ResourceType, Config, fun do_nothing_on_ready/1).

do_nothing_on_ready(_ResId) ->
    ok.

-spec create_dry_run(
    resource_id(), resource_module(), resource_config(), OnReadyCallback
) ->
    ok | {error, Reason :: term()}
when
    OnReadyCallback :: fun((resource_id()) -> ok | {error, Reason :: term()}).
create_dry_run(ResId, ResourceType, Config, OnReadyCallback) ->
    Opts =
        case is_map(Config) of
            true -> maps:get(resource_opts, Config, #{});
            false -> #{}
        end,
    %% Ensure that the dry run resource is terminated, even if this process is forcefully
    %% killed (e.g.: cowboy / HTTP API request times out).
    emqx_resource_cache_cleaner:add_dry_run(ResId, self()),
    ok = emqx_resource_manager_sup:ensure_child(
        ResId, <<"dry_run">>, ResourceType, Config, Opts
    ),
    HealthCheckInterval = maps:get(health_check_interval, Opts, ?HEALTHCHECK_INTERVAL),
    Timeout = emqx_utils:clamp(HealthCheckInterval, 5_000, 60_000),
    case wait_for_ready(ResId, Timeout) of
        ok ->
            CallbackResult =
                try
                    OnReadyCallback(ResId)
                catch
                    _:CallbackReason ->
                        {error, CallbackReason}
                end,
            case remove(ResId) of
                ok ->
                    CallbackResult;
                {error, _} = Error ->
                    Error
            end;
        {error, Reason} ->
            %% Removal is done asynchronously.  See comment below.
            {error, Reason};
        timeout ->
            %% Removal is done asynchronously by the cache cleaner.  If the resource
            %% process is stuck and not responding to calls, doing the removal
            %% synchronously here would take more time than the defined timeout, possibly
            %% timing out HTTP API requests.
            {error, timeout}
    end.

%% @doc Stops a running resource_manager and clears the metrics for the resource
-spec remove(resource_id()) -> ok | {error, Reason :: term()}.
remove(ResId) when is_binary(ResId) ->
    remove(ResId, true).

%% @doc Stops a running resource_manager and optionally clears the metrics for the resource
-spec remove(resource_id(), boolean()) -> ok | {error, Reason :: term()}.
remove(ResId, ClearMetrics) when is_binary(ResId) ->
    try
        do_remove(ResId, ClearMetrics)
    after
        %% Ensure the supervisor has it removed, otherwise the immediate re-add will see a stale process
        %% If the 'remove' call above had succeeded, this is mostly a no-op but still needed to avoid race condition.
        %% Otherwise this is a 'infinity' shutdown, so it may take arbitrary long.
        emqx_resource_manager_sup:delete_child(ResId)
    end.

do_remove(ResId, ClearMetrics) ->
    case gproc:whereis_name(?NAME(ResId)) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            MRef = monitor(process, Pid),
            case safe_call(ResId, {remove, ClearMetrics}, ?T_OPERATION) of
                {error, timeout} ->
                    ?tp(error, "forcefully_stopping_resource_due_to_timeout", #{
                        action => remove,
                        resource_id => ResId
                    }),
                    force_kill(ResId, MRef),
                    ok;
                ok ->
                    receive
                        {'DOWN', MRef, process, Pid, _} ->
                            ok
                    end,
                    ok;
                Res ->
                    Res
            end
    end.

%% @doc Stops and then starts an instance that was already running
-spec restart(resource_id(), creation_opts()) -> ok | {error, Reason :: term()}.
restart(ResId, Opts) when is_binary(ResId) ->
    case safe_call(ResId, restart, ?T_OPERATION) of
        ok ->
            _ = wait_for_ready(ResId, maps:get(start_timeout, Opts, 5000)),
            ok;
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Start the resource
-spec start(resource_id(), creation_opts()) -> ok | timeout | {error, Reason :: term()}.
start(ResId, Opts) ->
    StartTimeout = maps:get(start_timeout, Opts, ?T_OPERATION),
    case safe_call(ResId, start, StartTimeout) of
        ok ->
            wait_for_ready(ResId, StartTimeout);
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Stop the resource
-spec stop(resource_id()) -> ok | {error, Reason :: term()}.
stop(ResId) ->
    stop(ResId, ?T_OPERATION).

-spec stop(resource_id(), timeout()) -> ok | {error, Reason :: term()}.
stop(ResId, Timeout) ->
    case safe_call(ResId, stop, Timeout) of
        ok ->
            ok;
        {error, timeout} ->
            ?tp(error, "forcefully_stopping_resource_due_to_timeout", #{
                action => stop,
                resource_id => ResId
            }),
            force_kill(ResId, _MRef = undefined),
            ok;
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Test helper
-spec set_resource_status_connecting(resource_id()) -> ok.
set_resource_status_connecting(ResId) ->
    safe_call(ResId, set_resource_status_connecting, infinity).

%% @doc Lookup the group and data of a resource
-spec lookup(resource_id()) -> {ok, resource_group(), resource_data()} | {error, not_found}.
lookup(ResId) ->
    case safe_call(ResId, lookup, ?T_LOOKUP) of
        {error, timeout} -> lookup_cached(ResId);
        Result -> Result
    end.

%% @doc Lookup the group and data of a resource from the cache
-spec lookup_cached(resource_id()) -> {ok, resource_group(), resource_data()} | {error, not_found}.
lookup_cached(ResId) ->
    case read_cache(ResId) of
        [{Group, Data}] ->
            {ok, Group, Data};
        [] ->
            {error, not_found}
    end.

%% @doc Check if the resource is cached.
is_exist(ResId) ->
    emqx_resource_cache:is_exist(ResId).

%% @doc Get the metrics for the specified resource
get_metrics(ResId) ->
    emqx_metrics_worker:get_metrics(?RES_METRICS, ResId).

%% @doc Reset the metrics for the specified resource
-spec reset_metrics(resource_id()) -> ok.
reset_metrics(ResId) ->
    ok = ensure_metrics(ResId),
    emqx_metrics_worker:reset_metrics(?RES_METRICS, ResId).

%% @doc Returns the data for all resources
-spec list_all() -> [resource_data()].
list_all() ->
    emqx_resource_cache:list_all().

%% @doc Returns a list of ids for all the resources in a group
-spec list_group(resource_group()) -> [resource_id()].
list_group(Group) ->
    emqx_resource_cache:group_ids(Group).

-spec health_check(resource_id()) -> {ok, resource_status()} | {error, term()}.
health_check(ResId) ->
    safe_call(ResId, health_check, ?T_OPERATION).

-spec channel_health_check(resource_id(), channel_id()) ->
    #{status := resource_status(), error := term()}.
channel_health_check(ResId, ChannelId) ->
    %% Do normal health check first to trigger health checks for channels
    %% and update the cached health status for the channels
    _ = health_check(ResId),
    safe_call(ResId, {channel_health_check, ChannelId}, ?T_OPERATION).

-spec add_channel(
    connector_resource_id(),
    action_resource_id() | source_resource_id(),
    _Config
) ->
    ok | {error, term()}.
add_channel(ResId, ChannelId, Config) ->
    add_channel(ResId, ChannelId, Config, _Opts = #{}).

-spec add_channel(
    connector_resource_id(),
    action_resource_id() | source_resource_id(),
    _Config,
    add_channel_opts()
) ->
    ok | {error, term()}.
add_channel(ResId, ChannelId, Config, Opts) ->
    Result = safe_call(ResId, #add_channel{channel_id = ChannelId, config = Config}, ?T_OPERATION),
    maybe
        true ?= maps:get(perform_health_check, Opts, true),
        %% Wait for health_check to finish
        _ = channel_health_check(ResId, ChannelId),
        ok
    end,
    Result.

remove_channel(ResId, ChannelId) ->
    safe_call(ResId, {remove_channel, ChannelId}, ?T_OPERATION).

get_channels(ResId) ->
    safe_call(ResId, get_channels, ?T_OPERATION).

-spec get_query_mode_and_last_error(resource_id(), query_opts()) ->
    {ok, {query_mode(), LastError}} | {error, not_found}
when
    LastError ::
        unhealthy_target
        | {unhealthy_target, binary()}
        | channel_status_map()
        | term().
get_query_mode_and_last_error(RequestResId, Opts = #{connector_resource_id := ResId}) ->
    do_get_query_mode_error(ResId, RequestResId, Opts);
get_query_mode_and_last_error(RequestResId, Opts) ->
    do_get_query_mode_error(RequestResId, RequestResId, Opts).

do_get_query_mode_error(ResId, RequestResId, Opts) ->
    case emqx_resource_manager:lookup_cached(ResId) of
        {ok, _Group, ResourceData} ->
            QM = get_query_mode(ResourceData, Opts),
            Error = get_error(RequestResId, ResourceData),
            {ok, {QM, Error}};
        {error, not_found} ->
            {error, not_found}
    end.

get_query_mode(_ResourceData, #{query_mode := QM}) ->
    QM;
get_query_mode(#{query_mode := QM}, _Opts) ->
    QM.

get_error(ResId, #{added_channels := #{} = Channels} = ResourceData) when
    is_map_key(ResId, Channels)
->
    case maps:get(ResId, Channels) of
        #{error := Error} ->
            Error;
        _ ->
            maps:get(error, ResourceData, undefined)
    end;
get_error(_ResId, #{error := Error}) ->
    Error.

force_kill(ResId, MRef0) ->
    case gproc:whereis_name(?NAME(ResId)) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            MRef =
                case MRef0 of
                    undefined -> monitor(process, Pid);
                    _ -> MRef0
                end,
            exit(Pid, kill),
            receive
                {'DOWN', MRef, process, Pid, _} ->
                    ok
            end,
            try_clean_allocated_resources(ResId),
            ok
    end.

try_clean_allocated_resources(ResId) ->
    case emqx_resource_cache:read_mod(ResId) of
        {ok, Mod} ->
            catch emqx_resource:clean_allocated_resources(ResId, Mod),
            ok;
        not_found ->
            ok
    end.

%% Server start/stop callbacks

%% @doc Function called from the supervisor to actually start the server
start_link(ResId, Group, ResourceType, Config, Opts) ->
    QueryMode = emqx_resource:query_mode(
        ResourceType,
        Config,
        Opts
    ),
    Data = #data{
        id = ResId,
        type = emqx_resource:get_resource_type(ResourceType),
        group = Group,
        mod = ResourceType,
        callback_mode = emqx_resource:get_callback_mode(ResourceType),
        query_mode = QueryMode,
        config = Config,
        opts = Opts,
        state = undefined,
        error = undefined,
        added_channels = #{}
    },
    gen_statem:start_link(?REF(ResId), ?MODULE, {Data, Opts}, []).

init({DataIn, Opts}) ->
    process_flag(trap_exit, true),
    Data = DataIn#data{pid = self()},
    emqx_resource_cache_cleaner:add_cache(Data#data.id, self()),
    case maps:get(start_after_created, Opts, ?START_AFTER_CREATED) of
        true ->
            %% init the cache so that lookup/1 will always return something
            UpdatedData = update_state(Data#data{status = ?status_connecting}),
            {ok, ?state_connecting, UpdatedData, {next_event, internal, start_resource}};
        false ->
            %% init the cache so that lookup/1 will always return something
            UpdatedData = update_state(Data#data{status = ?rm_status_stopped}),
            {ok, ?state_stopped, UpdatedData}
    end.

terminate({shutdown, removed}, _State, _Data) ->
    ok;
terminate(_Reason, _State, Data) ->
    ok = terminate_health_check_workers(Data),
    _ = maybe_stop_resource(Data),
    _ = erase_cache(Data),
    ok.

%% Behavior callback

callback_mode() -> [handle_event_function, state_enter].

%% Common event Function

% Called during testing to force a specific state
handle_event({call, From}, set_resource_status_connecting, _State, Data) ->
    UpdatedData = update_state(Data#data{status = ?status_connecting}),
    {next_state, ?state_connecting, UpdatedData, [{reply, From, ok}]};
% Called when the resource is to be restarted
handle_event({call, From}, restart, _State, Data) ->
    DataNext = stop_resource(Data),
    start_resource(DataNext, From);
% Called when the resource is to be started (also used for manual reconnect)
handle_event({call, From}, start, State, Data) when
    State =:= ?state_stopped orelse
        State =:= ?state_disconnected
->
    start_resource(Data, From);
handle_event({call, From}, start, _State, _Data) ->
    {keep_state_and_data, [{reply, From, ok}]};
% Called when the resource is to be stopped
handle_event({call, From}, stop, ?state_stopped, _Data) ->
    {keep_state_and_data, [{reply, From, ok}]};
handle_event({call, From}, stop, _State, Data) ->
    UpdatedData = stop_resource(Data),
    {next_state, ?state_stopped, update_state(UpdatedData), [{reply, From, ok}]};
% Called when a resource is to be stopped and removed.
handle_event({call, From}, {remove, ClearMetrics}, _State, Data) ->
    handle_remove_event(From, ClearMetrics, Data);
% Called when the state-data of the resource is being looked up.
handle_event({call, From}, lookup, _State, #data{group = Group} = Data) ->
    Reply = {ok, Group, data_record_to_external_map(Data)},
    {keep_state_and_data, [{reply, From, Reply}]};
% Called when doing a manual health check.
handle_event({call, From}, health_check, ?state_stopped, _Data) ->
    Actions = [{reply, From, {error, resource_is_stopped}}],
    {keep_state_and_data, Actions};
handle_event({call, From}, {channel_health_check, _}, ?state_stopped, _Data) ->
    Actions = [{reply, From, {error, resource_is_stopped}}],
    {keep_state_and_data, Actions};
handle_event({call, From}, health_check, _State, Data) ->
    handle_manual_resource_health_check(From, Data);
handle_event({call, From}, {channel_health_check, ChannelId}, _State, Data) ->
    handle_manual_channel_health_check(From, Data, ChannelId);
%%--------------------------
%% State: CONNECTING
%%--------------------------
handle_event(enter, _OldState, ?state_connecting = State, Data0) ->
    Data = abort_all_channel_health_checks(Data0),
    ok = log_status_consistency(State, Data),
    {keep_state, Data, [{state_timeout, 0, health_check}]};
handle_event(internal, start_resource, ?state_connecting, Data) ->
    start_resource(Data, undefined);
handle_event(state_timeout, health_check, ?state_connecting, Data) ->
    start_resource_health_check(Data);
handle_event(
    {call, From}, {remove_channel, ChannelId}, ?state_connecting = _State, Data
) ->
    handle_remove_channel(From, ChannelId, Data);
%%--------------------------
%% State: CONNECTED
%% The connected state is entered after a successful on_start/2 of the callback mod
%% and successful health_checks
%%--------------------------
handle_event(enter, _OldState, ?state_connected = State, Data) ->
    ok = log_status_consistency(State, Data),
    _ = emqx_alarm:safe_deactivate(Data#data.id),
    ?tp(resource_connected_enter, #{}),
    {keep_state, Data, resource_health_check_actions(Data)};
handle_event(state_timeout, health_check, ?state_connected, Data) ->
    start_resource_health_check(Data);
handle_event(
    {call, From},
    #add_channel{channel_id = ChannelId, config = Config},
    ?state_connected = _State,
    Data
) ->
    handle_add_channel(From, Data, ChannelId, Config);
handle_event(
    {call, From}, {remove_channel, ChannelId}, ?state_connected = _State, Data
) ->
    handle_remove_channel(From, ChannelId, Data);
handle_event(
    {timeout, #start_channel_health_check{channel_id = ChannelId}},
    _,
    ?state_connected = _State,
    Data
) ->
    handle_start_channel_health_check(Data, ChannelId);
handle_event(
    {timeout, #retry_add_channel{channel_id = ChannelId}}, _, ?state_connected = _State, Data
) ->
    handle_retry_add_channel(Data, ChannelId);
%%--------------------------
%% State: DISCONNECTED
%%--------------------------
handle_event(enter, _OldState, ?state_disconnected = State, Data0) ->
    ok = log_status_consistency(State, Data0),
    ?tp(resource_disconnected_enter, #{}),
    Data = abort_all_channel_health_checks(Data0),
    {keep_state, Data, retry_actions(Data)};
handle_event(state_timeout, auto_retry, ?state_disconnected, Data) ->
    ?tp(resource_auto_reconnect, #{}),
    start_resource(Data, undefined);
%%--------------------------
%% State: STOPPED
%% The stopped state is entered after the resource has been explicitly stopped
%%--------------------------
handle_event(enter, _OldState, ?state_stopped = State, Data0) ->
    Data = abort_all_channel_health_checks(Data0),
    ok = log_status_consistency(State, Data),
    {keep_state, Data};
%%--------------------------
%% The following events can be handled in any other state
%%--------------------------
handle_event(
    {call, From}, #add_channel{channel_id = ChannelId, config = Config}, State, Data
) ->
    handle_not_connected_add_channel(From, ChannelId, Config, State, Data);
handle_event(
    {call, From}, {remove_channel, ChannelId}, _State, Data
) ->
    handle_not_connected_and_not_connecting_remove_channel(From, ChannelId, Data);
handle_event(
    {call, From}, get_channels, _State, Data
) ->
    Channels = emqx_resource:call_get_channels(Data#data.id, Data#data.mod),
    {keep_state_and_data, {reply, From, {ok, Channels}}};
handle_event(
    info,
    {'EXIT', Pid, Res},
    State0,
    Data0 = #data{hc_workers = #{resource := RHCWorkers}}
) when
    is_map_key(Pid, RHCWorkers)
->
    handle_resource_health_check_worker_down(State0, Data0, Pid, Res);
handle_event(
    info,
    {'EXIT', Pid, Res},
    _State,
    Data0 = #data{hc_workers = #{channel := CHCWorkers}}
) when
    is_map_key(Pid, CHCWorkers)
->
    handle_channel_health_check_worker_down(Data0, Pid, Res);
handle_event({timeout, #retry_add_channel{channel_id = _}}, _, _State, _Data) ->
    %% We only add channels to the resource state in the connected state.
    {keep_state_and_data, [postpone]};
handle_event({timeout, #start_channel_health_check{channel_id = _}}, _, _State, _Data) ->
    %% Stale health check action; currently, we only probe channel health when connected.
    keep_state_and_data;
% Ignore all other events
handle_event(EventType, EventData, State, Data) ->
    ?SLOG(
        error,
        #{
            msg => "ignore_all_other_events",
            resource_id => Data#data.id,
            event_type => EventType,
            event_data => EventData,
            state => State,
            data => emqx_utils:redact(Data)
        },
        #{tag => tag(Data#data.group, Data#data.type)}
    ),
    keep_state_and_data.

log_status_consistency(Status, #data{status = Status} = Data0) ->
    [{_Group, Cached}] = read_cache(Data0#data.id),
    Data = data_record_to_external_map(Data0),
    log_cache_consistency(Cached, Data);
log_status_consistency(Status, Data) ->
    ?tp(warning, "inconsistent_status", #{
        status => Status,
        data => emqx_utils:redact(Data)
    }).

log_cache_consistency(Data, Data) ->
    ok;
log_cache_consistency(DataCached, Data) ->
    ?tp(warning, "inconsistent_cache", #{
        cache => emqx_utils:redact(DataCached),
        data => emqx_utils:redact(Data)
    }).

%%------------------------------------------------------------------------------
%% internal functions
%%------------------------------------------------------------------------------
insert_cache(Group, Data) ->
    emqx_resource_cache:write(self(), Group, Data).

read_cache(ResId) ->
    emqx_resource_cache:read(ResId).

erase_cache(#data{id = ResId}) ->
    emqx_resource_cache:erase(ResId).

retry_actions(Data) ->
    case maps:get(health_check_interval, Data#data.opts, ?HEALTHCHECK_INTERVAL) of
        undefined ->
            [];
        RetryInterval ->
            [{state_timeout, RetryInterval, auto_retry}]
    end.

resource_health_check_actions(Data) ->
    [{state_timeout, health_check_interval(Data#data.opts), health_check}].

handle_remove_event(From, ClearMetrics, Data) ->
    %% stop the buffer workers first, brutal_kill, so it should be fast
    ok = emqx_resource_buffer_worker_sup:stop_workers(Data#data.id, Data#data.opts),
    ok = terminate_health_check_workers(Data),
    %% now stop the resource, this can be slow
    _ = stop_resource(Data),
    case ClearMetrics of
        true -> ok = emqx_metrics_worker:clear_metrics(?RES_METRICS, Data#data.id);
        false -> ok
    end,
    _ = erase_cache(Data),
    {stop_and_reply, {shutdown, removed}, [{reply, From, ok}]}.

start_resource(Data, From) ->
    %% in case the emqx_resource:call_start/2 hangs, the lookup/1 can read status from the cache
    #data{id = ResId, mod = Mod, config = Config, group = Group, type = Type} = Data,
    ok = ensure_metrics(ResId),
    case emqx_resource:call_start(ResId, Mod, Config) of
        {ok, ResourceState} ->
            UpdatedData1 = Data#data{status = ?status_connecting, state = ResourceState},
            %% Perform an initial health_check immediately before transitioning into a connected state
            UpdatedData2 = add_channels(UpdatedData1),
            UpdatedData3 = maybe_update_callback_mode(UpdatedData2),
            Actions = maybe_reply([{state_timeout, 0, health_check}], From, ok),
            {next_state, ?state_connecting, update_state(UpdatedData3), Actions};
        {error, Reason} = Err ->
            IsDryRun = emqx_resource:is_dry_run(ResId),
            ?SLOG(
                log_level(IsDryRun),
                #{
                    msg => "start_resource_failed",
                    resource_id => ResId,
                    reason => Reason
                },
                #{tag => tag(Group, Type)}
            ),
            _ = maybe_alarm(?status_disconnected, IsDryRun, ResId, Err, Data#data.error),
            %% Add channels and raise alarms
            {Actions0, NewData1} = channels_health_check(?status_disconnected, add_channels(Data)),
            %% Keep track of the error reason why the connection did not work
            %% so that the Reason can be returned when the verification call is made.
            NewData2 = NewData1#data{status = ?status_disconnected, error = Err},
            Actions1 = maybe_reply(retry_actions(NewData2), From, Err),
            Actions = Actions1 ++ Actions0,
            {next_state, ?state_disconnected, update_state(NewData2), Actions}
    end.

add_channels(Data) ->
    %% Add channels to the Channels map but not to the resource state
    %% Channels will be added to the resource state after the initial health_check
    %% if that succeeds.
    ChannelIDConfigTuples = emqx_resource:call_get_channels(Data#data.id, Data#data.mod),
    Channels = Data#data.added_channels,
    NewChannels = lists:foldl(
        fun
            ({ChannelID, #{enable := true} = Config}, Acc) ->
                maps:put(ChannelID, channel_status_not_added(Config), Acc);
            ({_, #{enable := false}}, Acc) ->
                Acc
        end,
        Channels,
        ChannelIDConfigTuples
    ),
    Data#data{added_channels = NewChannels}.

maybe_update_callback_mode(Data = #data{mod = ResourceType, state = ResourceState}) ->
    case emqx_resource:get_callback_mode(ResourceType, ResourceState) of
        undefined ->
            Data;
        CallMode ->
            Data#data{callback_mode = CallMode}
    end.

add_channels_in_list(ChannelsWithConfigs, Data) ->
    add_channels_in_list(ChannelsWithConfigs, Data, _Actions = []).

add_channels_in_list([], Data, Actions) ->
    {Actions, Data};
add_channels_in_list([{ChannelID, ChannelConfig} | Rest], Data, Actions) ->
    #data{
        id = ResId,
        mod = Mod,
        state = State,
        added_channels = AddedChannelsMap,
        group = Group,
        type = Type
    } = Data,
    case
        emqx_resource:call_add_channel(
            ResId, Mod, State, ChannelID, ChannelConfig
        )
    of
        {ok, NewState} ->
            %% Set the channel status to connecting to indicate that
            %% we have not yet performed the initial health_check
            NewAddedChannelsMap = maps:put(
                ChannelID,
                channel_status_new_waiting_for_health_check(ChannelConfig),
                AddedChannelsMap
            ),
            NewActions = Actions,
            NewData = Data#data{
                state = NewState,
                added_channels = NewAddedChannelsMap
            };
        {error, Reason} = Error ->
            IsDryRun = emqx_resource:is_dry_run(ResId),
            ?SLOG(
                log_level(IsDryRun),
                #{
                    msg => "add_channel_failed",
                    resource_id => ResId,
                    channel_id => ChannelID,
                    reason => Reason
                },
                #{tag => tag(Group, Type)}
            ),
            NewAddedChannelsMap = maps:put(
                ChannelID,
                channel_status(?add_channel_failed(Reason), ChannelConfig),
                AddedChannelsMap
            ),
            NewActions = [retry_add_channel_action(ChannelID, ChannelConfig, Data) | Actions],
            NewData = Data#data{
                added_channels = NewAddedChannelsMap
            },
            %% Raise an alarm since the channel could not be added
            _ = maybe_alarm(?status_disconnected, IsDryRun, ChannelID, Error, no_prev_error)
    end,
    add_channels_in_list(Rest, NewData, NewActions).

maybe_stop_resource(#data{status = Status} = Data) when Status =/= ?rm_status_stopped ->
    stop_resource(Data);
maybe_stop_resource(#data{status = ?rm_status_stopped} = Data) ->
    Data.

stop_resource(#data{id = ResId} = Data) ->
    %% We don't care about the return value of `Mod:on_stop/2'.
    %% The callback mod should make sure the resource is stopped after on_stop/2
    %% is returned.
    HasAllocatedResources = emqx_resource:has_allocated_resources(ResId),
    %% Before stop is called we remove all the channels from the resource
    NewData = remove_channels(Data),
    NewResState = NewData#data.state,
    case NewResState =/= undefined orelse HasAllocatedResources of
        true ->
            %% we clear the allocated resources after stop is successful
            emqx_resource:call_stop(NewData#data.id, NewData#data.mod, NewResState);
        false ->
            ok
    end,
    IsDryRun = emqx_resource:is_dry_run(ResId),
    _ = maybe_clear_alarm(IsDryRun, ResId),
    ok = emqx_metrics_worker:reset_metrics(?RES_METRICS, ResId),
    NewData#data{status = ?rm_status_stopped}.

remove_channels(Data) ->
    Channels = maps:keys(Data#data.added_channels),
    remove_channels_in_list(Channels, Data).

remove_channels_in_list([], Data) ->
    Data;
remove_channels_in_list([ChannelID | Rest], Data) ->
    #data{
        id = ResId,
        added_channels = AddedChannelsMap,
        mod = Mod,
        state = State,
        group = Group,
        type = Type
    } = Data,
    IsDryRun = emqx_resource:is_dry_run(ResId),
    _ = maybe_clear_alarm(IsDryRun, ChannelID),
    NewAddedChannelsMap = maps:remove(ChannelID, AddedChannelsMap),
    case safe_call_remove_channel(ResId, Mod, State, ChannelID) of
        {ok, NewState} ->
            NewData = Data#data{
                state = NewState,
                added_channels = NewAddedChannelsMap
            };
        {error, Reason} ->
            ?tp("remove_channel_failed", #{resource_id => ResId, reason => Reason}),
            ?SLOG(
                log_level(IsDryRun),
                #{
                    msg => "remove_channel_failed",
                    resource_id => ResId,
                    group => Group,
                    type => Type,
                    channel_id => ChannelID,
                    reason => Reason
                },
                #{tag => tag(Group, Type)}
            ),
            NewData = Data#data{
                added_channels = NewAddedChannelsMap
            }
    end,
    remove_channels_in_list(Rest, NewData).

safe_call_remove_channel(_ResId, _Mod, undefined = State, _ChannelID) ->
    {ok, State};
safe_call_remove_channel(ResId, Mod, State, ChannelID) ->
    emqx_resource:call_remove_channel(ResId, Mod, State, ChannelID).

%% For cases where we need to terminate and there are running health checks.
terminate_health_check_workers(Data) ->
    #data{
        hc_workers = #{resource := RHCWorkers, channel := CHCWorkers},
        hc_pending_callers = #{resource := RPending, channel := CPending}
    } = Data,
    maps:foreach(
        fun(Pid, _) ->
            exit(Pid, kill)
        end,
        RHCWorkers
    ),
    maps:foreach(
        fun
            (Pid, _) when is_pid(Pid) ->
                exit(Pid, kill);
            (_, _) ->
                ok
        end,
        CHCWorkers
    ),
    Pending = lists:flatten([RPending, maps:values(CPending)]),
    lists:foreach(
        fun(From) ->
            gen_statem:reply(From, {error, resource_shutting_down})
        end,
        Pending
    ).

handle_add_channel(From, Data, ChannelId, Config) ->
    Channels = Data#data.added_channels,
    case
        channel_status_is_channel_added(
            maps:get(
                ChannelId,
                Channels,
                channel_status_not_added(Config)
            )
        )
    of
        false ->
            %% The channel is not installed in the connector state
            %% We insert it into the channels map and let the health check
            %% take care of the rest
            NewChannels = maps:put(ChannelId, channel_status_not_added(Config), Channels),
            NewData = Data#data{added_channels = NewChannels},
            {keep_state, update_state(NewData), [
                {reply, From, ok}
            ]};
        true ->
            %% The channel is already installed in the connector state
            %% We don't need to install it again
            {keep_state_and_data, [{reply, From, ok}]}
    end.

handle_not_connected_add_channel(From, ChannelId, ChannelConfig, State, Data) ->
    %% When state is not connected the channel will be added to the channels
    %% map but nothing else will happen.
    NewData = add_or_update_channel_status(Data, ChannelId, ChannelConfig, State),
    {keep_state, update_state(NewData), [{reply, From, ok}]}.

handle_remove_channel(From, ChannelId, Data0) ->
    Data = abort_health_checks_for_channel(Data0, ChannelId),
    Channels = Data#data.added_channels,
    IsDryRun = emqx_resource:is_dry_run(Data#data.id),
    _ = maybe_clear_alarm(IsDryRun, ChannelId),
    case
        channel_status_is_channel_added(
            maps:get(ChannelId, Channels, channel_status_not_added(undefined))
        )
    of
        false ->
            %% The channel is already not installed in the connector state.
            %% We still need to remove it from the added_channels map
            AddedChannels = Data#data.added_channels,
            NewAddedChannels = maps:remove(ChannelId, AddedChannels),
            NewData = Data#data{
                added_channels = NewAddedChannels
            },
            {keep_state, NewData, [{reply, From, ok}]};
        true ->
            %% The channel is installed in the connector state
            handle_remove_channel_exists(From, ChannelId, Data)
    end.

handle_remove_channel_exists(From, ChannelId, Data) ->
    #data{
        id = Id,
        group = Group,
        type = Type,
        added_channels = AddedChannelsMap
    } = Data,
    case
        emqx_resource:call_remove_channel(
            Id, Data#data.mod, Data#data.state, ChannelId
        )
    of
        {ok, NewState} ->
            NewAddedChannelsMap = maps:remove(ChannelId, AddedChannelsMap),
            UpdatedData = Data#data{
                state = NewState,
                added_channels = NewAddedChannelsMap
            },
            {keep_state, update_state(UpdatedData), [{reply, From, ok}]};
        {error, Reason} = Error ->
            IsDryRun = emqx_resource:is_dry_run(Id),
            ?tp("remove_channel_failed", #{resource_id => Id, reason => Reason}),
            ?SLOG(
                log_level(IsDryRun),
                #{
                    msg => "remove_channel_failed",
                    resource_id => Id,
                    channel_id => ChannelId,
                    reason => Reason
                },
                #{tag => tag(Group, Type)}
            ),
            {keep_state_and_data, [{reply, From, Error}]}
    end.

handle_not_connected_and_not_connecting_remove_channel(From, ChannelId, Data) ->
    %% When state is not connected and not connecting the channel will be removed
    %% from the channels map but nothing else will happen since the channel
    %% is not added/installed in the resource state.
    Channels = Data#data.added_channels,
    NewChannels = maps:remove(ChannelId, Channels),
    NewData = Data#data{added_channels = NewChannels},
    IsDryRun = emqx_resource:is_dry_run(Data#data.id),
    _ = maybe_clear_alarm(IsDryRun, ChannelId),
    {keep_state, update_state(NewData), [{reply, From, ok}]}.

handle_manual_resource_health_check(From, Data0 = #data{hc_workers = #{resource := HCWorkers}}) when
    map_size(HCWorkers) > 0
->
    %% ongoing health check
    #data{hc_pending_callers = Pending0 = #{resource := RPending0}} = Data0,
    Pending = Pending0#{resource := [From | RPending0]},
    Data = Data0#data{hc_pending_callers = Pending},
    {keep_state, Data};
handle_manual_resource_health_check(From, Data0) ->
    #data{hc_pending_callers = Pending0 = #{resource := RPending0}} = Data0,
    Pending = Pending0#{resource := [From | RPending0]},
    Data = Data0#data{hc_pending_callers = Pending},
    start_resource_health_check(Data).

reply_pending_resource_health_check_callers(Status, Data0 = #data{hc_pending_callers = Pending0}) ->
    #{resource := RPending} = Pending0,
    Actions = [{reply, From, {ok, Status}} || From <- RPending],
    Data = Data0#data{hc_pending_callers = Pending0#{resource := []}},
    {Actions, Data}.

start_resource_health_check(#data{state = undefined} = Data) ->
    %% No resource running, thus disconnected.
    %% A health check spawn when state is undefined can only happen when someone manually
    %% asks for a health check and the resource could not initialize or has not had enough
    %% time to do so.  Let's assume the continuation is as if we were `?status_connecting'.
    continue_resource_health_check_not_connected(?status_disconnected, Data);
start_resource_health_check(#data{hc_workers = #{resource := HCWorkers}}) when
    map_size(HCWorkers) > 0
->
    %% Already ongoing
    keep_state_and_data;
start_resource_health_check(#data{} = Data0) ->
    #data{hc_workers = HCWorkers0 = #{resource := RHCWorkers0}} = Data0,
    WorkerPid = spawn_resource_health_check_worker(Data0),
    HCWorkers = HCWorkers0#{resource := RHCWorkers0#{WorkerPid => true}},
    Data = Data0#data{hc_workers = HCWorkers},
    {keep_state, Data}.

-spec spawn_resource_health_check_worker(data()) -> pid().
spawn_resource_health_check_worker(#data{} = Data) ->
    spawn_link(?MODULE, worker_resource_health_check, [Data]).

%% separated so it can be spec'ed and placate dialyzer tantrums...
-spec worker_resource_health_check(data()) -> no_return().
worker_resource_health_check(Data) ->
    HCRes = emqx_resource:call_health_check(Data#data.id, Data#data.mod, Data#data.state),
    exit({ok, HCRes}).

handle_resource_health_check_worker_down(CurrentState, Data0, WorkerRef, ExitResult) ->
    #data{hc_workers = HCWorkers0 = #{resource := RHCWorkers0}} = Data0,
    HCWorkers = HCWorkers0#{resource := maps:remove(WorkerRef, RHCWorkers0)},
    Data1 = Data0#data{hc_workers = HCWorkers},
    case ExitResult of
        {ok, HCRes} ->
            continue_with_health_check(Data1, CurrentState, HCRes);
        _ ->
            %% Unexpected: `emqx_resource:call_health_check' catches all exceptions.
            continue_with_health_check(Data1, CurrentState, {error, ExitResult})
    end.

continue_with_health_check(#data{} = Data0, CurrentState, HCRes) ->
    #data{
        id = ResId,
        error = PrevError
    } = Data0,
    {NewStatus, Err} = parse_health_check_result(HCRes, Data0),
    IsDryRun = emqx_resource:is_dry_run(ResId),
    _ = maybe_alarm(NewStatus, IsDryRun, ResId, Err, PrevError),
    ok = maybe_resume_resource_workers(ResId, NewStatus),
    Data1 = Data0#data{
        status = NewStatus, error = Err
    },
    Data = update_state(Data1),
    case CurrentState of
        ?state_connected ->
            continue_resource_health_check_connected(NewStatus, Data);
        _ ->
            %% `?state_connecting' | `?state_disconnected' | `?state_stopped'
            continue_resource_health_check_not_connected(NewStatus, Data)
    end.

%% Continuation to be used when the current resource state is `?state_connected'.
continue_resource_health_check_connected(NewStatus, Data0) ->
    case NewStatus of
        ?status_connected ->
            {Replies, Data1} = reply_pending_resource_health_check_callers(NewStatus, Data0),
            {Actions0, Data2} = channels_health_check(?status_connected, Data1),
            Data = update_state(Data2),
            Actions = Replies ++ Actions0 ++ resource_health_check_actions(Data),
            {keep_state, Data, Actions};
        _ ->
            #data{id = ResId, group = Group, type = Type} = Data0,
            IsDryRun = emqx_resource:is_dry_run(ResId),
            ?SLOG(
                log_level(IsDryRun),
                #{
                    msg => "health_check_failed",
                    resource_id => ResId,
                    status => NewStatus
                },
                #{tag => tag(Group, Type)}
            ),
            %% Note: works because, coincidentally, channel/resource status is a
            %% subset of resource manager state...  But there should be a conversion
            %% between the two here, as resource manager also has `stopped', which is
            %% not a valid status at the time of writing.
            {Replies, Data1} = reply_pending_resource_health_check_callers(NewStatus, Data0),
            {Actions0, Data} = channels_health_check(NewStatus, Data1),
            Actions = Replies ++ Actions0,
            {next_state, NewStatus, Data, Actions}
    end.

%% Continuation to be used when the current resource state is not `?state_connected'.
continue_resource_health_check_not_connected(NewStatus, Data0) ->
    {Replies, Data1} = reply_pending_resource_health_check_callers(NewStatus, Data0),
    case NewStatus of
        ?status_connected ->
            {Actions0, Data} = channels_health_check(?status_connected, Data1),
            Actions = Replies ++ Actions0,
            {next_state, ?state_connected, Data, Actions};
        ?status_connecting ->
            {Actions0, Data} = channels_health_check(?status_connecting, Data1),
            Actions = Replies ++ Actions0 ++ resource_health_check_actions(Data),
            {next_state, ?status_connecting, Data, Actions};
        ?status_disconnected ->
            {Actions0, Data} = channels_health_check(?status_disconnected, Data1),
            Actions = Replies ++ Actions0,
            {next_state, ?state_disconnected, Data, Actions}
    end.

handle_manual_channel_health_check(From, #data{state = undefined}, _ChannelId) ->
    {keep_state_and_data, [
        {reply, From, channel_error_status(resource_disconnected)}
    ]};
handle_manual_channel_health_check(
    From,
    #data{
        added_channels = Channels,
        hc_pending_callers = #{channel := CPending0} = Pending0,
        hc_workers = #{channel := #{ongoing := Ongoing}}
    } = Data0,
    ChannelId
) when
    is_map_key(ChannelId, Channels),
    is_map_key(ChannelId, Ongoing)
->
    %% Ongoing health check.
    CPending = maps:update_with(
        ChannelId,
        fun(OtherCallers) ->
            [From | OtherCallers]
        end,
        [From],
        CPending0
    ),
    Pending = Pending0#{channel := CPending},
    Data = Data0#data{hc_pending_callers = Pending},
    {keep_state, Data};
handle_manual_channel_health_check(
    From,
    #data{added_channels = Channels} = _Data,
    ChannelId
) when
    is_map_key(ChannelId, Channels)
->
    %% No ongoing health check: reply with current status.
    {keep_state_and_data, [
        {reply, From, to_external_channel_status(maps:get(ChannelId, Channels))}
    ]};
handle_manual_channel_health_check(
    From,
    _Data,
    _ChannelId
) ->
    {keep_state_and_data, [
        {reply, From, channel_error_status(channel_not_found)}
    ]}.

-spec channels_health_check(resource_status(), data()) -> {[gen_statem:action()], data()}.
channels_health_check(?status_connected = _ConnectorStatus, Data0) ->
    Channels = maps:to_list(Data0#data.added_channels),
    ChannelsNotAdded = [
        ChannelId
     || {ChannelId, Status} <- Channels,
        not channel_status_is_channel_added(Status)
    ],
    %% Attempt to add channels to resource state that are not added yet
    ChannelsNotAddedWithConfigs = get_config_for_channels(Data0, ChannelsNotAdded),
    {Actions, Data1} = add_channels_in_list(ChannelsNotAddedWithConfigs, Data0),
    %% Now that we have done the adding, we can get the status of all channels (execept
    %% unhealthy ones)
    Data2 = trigger_health_check_for_added_channels(Data1),
    {Actions, update_state(Data2)};
channels_health_check(?status_connecting = _ConnectorStatus, Data0) ->
    %% Whenever the resource is connecting:
    %% 1. Change the status of all added channels to connecting
    %% 2. Raise alarms
    Channels = Data0#data.added_channels,
    ChannelsToChangeStatusFor = [
        {ChannelId, Config}
     || {ChannelId, #{config := Config} = Status} <- maps:to_list(Channels),
        channel_status_is_channel_added(Status)
    ],
    ChannelsWithNewStatuses =
        [
            {ChannelId, channel_status({?status_connecting, resource_is_connecting}, Config)}
         || {ChannelId, Config} <- ChannelsToChangeStatusFor
        ],
    %% Update the channels map
    NewChannels = lists:foldl(
        fun({ChannelId, NewStatus}, Acc) ->
            maps:update(ChannelId, NewStatus, Acc)
        end,
        Channels,
        ChannelsWithNewStatuses
    ),
    ChannelsWithNewAndPrevErrorStatuses =
        [
            {ChannelId, NewStatus, maps:get(ChannelId, Channels)}
         || {ChannelId, NewStatus} <- maps:to_list(NewChannels)
        ],
    %% Raise alarms for all channels
    IsDryRun = emqx_resource:is_dry_run(Data0#data.id),
    lists:foreach(
        fun({ChannelId, Status, PrevStatus}) ->
            maybe_alarm(?status_connecting, IsDryRun, ChannelId, Status, PrevStatus)
        end,
        ChannelsWithNewAndPrevErrorStatuses
    ),
    Data1 = Data0#data{added_channels = NewChannels},
    {_Actions = [], update_state(Data1)};
channels_health_check(?status_disconnected = ConnectorStatus, Data1) ->
    %% Whenever the resource is disconnected:
    %% 1. Change the status of channels to an error status
    %%    - Except for channels yet to be added to the resource state.  Those need to keep
    %%    those special errors so they are added or retried.
    %% 2. Raise alarms
    Channels = Data1#data.added_channels,
    ChannelsWithNewAndOldStatuses =
        lists:map(
            fun
                ({ChannelId, #{error := ?not_added_yet} = OldStatus}) ->
                    {ChannelId, OldStatus, OldStatus};
                ({ChannelId, #{error := ?add_channel_failed(_)} = OldStatus}) ->
                    {ChannelId, OldStatus, OldStatus};
                ({ChannelId, #{config := Config} = OldStatus}) ->
                    {ChannelId, OldStatus,
                        channel_status(
                            {error,
                                resource_not_connected_channel_error_msg(
                                    ConnectorStatus,
                                    ChannelId,
                                    Data1
                                )},
                            Config
                        )}
            end,
            maps:to_list(Data1#data.added_channels)
        ),
    %% Raise alarms
    IsDryRun = emqx_resource:is_dry_run(Data1#data.id),
    _ = lists:foreach(
        fun({ChannelId, OldStatus, NewStatus}) ->
            _ = maybe_alarm(NewStatus, IsDryRun, ChannelId, NewStatus, OldStatus)
        end,
        ChannelsWithNewAndOldStatuses
    ),
    %% Update the channels map
    NewChannels = lists:foldl(
        fun({ChannelId, _, NewStatus}, Acc) ->
            maps:put(ChannelId, NewStatus, Acc)
        end,
        Channels,
        ChannelsWithNewAndOldStatuses
    ),
    Data2 = Data1#data{added_channels = NewChannels},
    {_Actions = [], update_state(Data2)}.

resource_not_connected_channel_error_msg(ResourceStatus, ChannelId, Data1) ->
    ResourceId = Data1#data.id,
    iolist_to_binary(
        io_lib:format(
            "Resource ~s for channel ~s is not connected. "
            "Resource status: ~p",
            [
                ResourceId,
                ChannelId,
                ResourceStatus
            ]
        )
    ).

-spec generic_timeout_action(Id, timeout(), Content) -> generic_timeout(Id, Content).
generic_timeout_action(Id, Timeout, Content) ->
    {{timeout, Id}, Timeout, Content}.

-spec start_channel_health_check_action(channel_id(), map(), map(), data()) ->
    [start_channel_health_check_action()].
start_channel_health_check_action(ChannelId, NewChanStatus, PreviousChanStatus, Data = #data{}) ->
    ConfigSources =
        lists:map(
            fun
                (#{config := Config}) ->
                    Config;
                (_) ->
                    #{}
            end,
            [NewChanStatus, PreviousChanStatus]
        ),
    Timeout = get_channel_health_check_interval(ChannelId, ConfigSources, Data),
    Event = #start_channel_health_check{channel_id = ChannelId},
    [generic_timeout_action(Event, Timeout, Event)].

-spec retry_add_channel_action(channel_id(), map(), data()) -> retry_add_channel_action().
retry_add_channel_action(ChannelId, ChannelConfig, Data) ->
    Timeout = get_channel_health_check_interval(ChannelId, [ChannelConfig], Data),
    Event = #retry_add_channel{channel_id = ChannelId},
    generic_timeout_action(Event, Timeout, Event).

get_channel_health_check_interval(ChannelId, ConfigSources, Data) ->
    emqx_utils:foldl_while(
        fun
            (#{resource_opts := #{health_check_interval := HCInterval}}, _Acc) ->
                {halt, HCInterval};
            (_, Acc) ->
                {cont, Acc}
        end,
        ?HEALTHCHECK_INTERVAL,
        ConfigSources ++
            [emqx_utils_maps:deep_get([ChannelId, config], Data#data.added_channels, #{})]
    ).

%% Currently, we only call resource channel health checks when the underlying resource is
%% `?status_connected'.
-spec trigger_health_check_for_added_channels(data()) -> data().
trigger_health_check_for_added_channels(Data0 = #data{hc_workers = HCWorkers0}) ->
    #{
        channel := #{ongoing := Ongoing0}
    } = HCWorkers0,
    NewOngoing = maps:filter(
        fun(ChannelId, OldStatus) ->
            (not is_map_key(ChannelId, Ongoing0)) andalso
                is_channel_apt_for_health_check(OldStatus)
        end,
        Data0#data.added_channels
    ),
    ChannelsToCheck = maps:keys(NewOngoing),
    lists:foldl(
        fun(ChannelId, Acc) ->
            start_channel_health_check(Acc, ChannelId)
        end,
        Data0,
        ChannelsToCheck
    ).

-spec continue_channel_health_check_connected(
    channel_id(), channel_status_map(), channel_status_map(), data()
) -> data().
continue_channel_health_check_connected(ChannelId, OldStatus, CurrentStatus, Data0) ->
    #data{hc_workers = HCWorkers0} = Data0,
    #{channel := CHCWorkers0} = HCWorkers0,
    CHCWorkers = emqx_utils_maps:deep_remove([ongoing, ChannelId], CHCWorkers0),
    Data1 = Data0#data{hc_workers = HCWorkers0#{channel := CHCWorkers}},
    case OldStatus =:= CurrentStatus of
        true ->
            continue_channel_health_check_connected_no_update_during_check(
                ChannelId, OldStatus, Data1
            );
        false ->
            %% Channel has been updated while the health check process was working so
            %% we should not clear any alarm or remove the channel from the
            %% connector
            Data1
    end.

continue_channel_health_check_connected_no_update_during_check(ChannelId, OldStatus, Data) ->
    %% Remove the added channels with a status different from connected or connecting
    NewStatus = maps:get(ChannelId, Data#data.added_channels),
    IsDryRun = emqx_resource:is_dry_run(Data#data.id),
    %% Raise/clear alarms
    case NewStatus of
        #{status := ?status_connected} ->
            _ = maybe_clear_alarm(IsDryRun, ChannelId),
            ok;
        _ ->
            _ = maybe_alarm(NewStatus, IsDryRun, ChannelId, NewStatus, OldStatus),
            ok
    end,
    Data.

-spec handle_start_channel_health_check(data(), channel_id()) ->
    gen_statem:event_handler_result(state(), data()).
handle_start_channel_health_check(Data0, ChannelId) ->
    Data = start_channel_health_check(Data0, ChannelId),
    {keep_state, Data}.

-spec start_channel_health_check(data(), channel_id()) -> data().
start_channel_health_check(
    #data{added_channels = AddedChannels, hc_workers = #{channel := #{ongoing := CHCOngoing0}}} =
        Data0,
    ChannelId
) when
    is_map_key(ChannelId, AddedChannels) andalso (not is_map_key(ChannelId, CHCOngoing0))
->
    #data{hc_workers = HCWorkers0 = #{channel := CHCWorkers0}} = Data0,
    WorkerPid = spawn_channel_health_check_worker(Data0, ChannelId),
    ChannelStatus = maps:get(ChannelId, AddedChannels),
    CHCOngoing = CHCOngoing0#{ChannelId => ChannelStatus},
    CHCWorkers = CHCWorkers0#{WorkerPid => ChannelId, ongoing := CHCOngoing},
    HCWorkers = HCWorkers0#{channel := CHCWorkers},
    Data0#data{hc_workers = HCWorkers};
start_channel_health_check(Data, _ChannelId) ->
    Data.

-spec spawn_channel_health_check_worker(data(), channel_id()) -> pid().
spawn_channel_health_check_worker(#data{} = Data, ChannelId) ->
    spawn_link(?MODULE, worker_channel_health_check, [Data, ChannelId]).

%% separated so it can be spec'ed and placate dialyzer tantrums...
-spec worker_channel_health_check(data(), channel_id()) -> no_return().
worker_channel_health_check(Data, ChannelId) ->
    #data{id = ResId, mod = Mod, state = State, added_channels = Channels} = Data,
    ChannelStatus = maps:get(ChannelId, Channels, #{}),
    ChannelConfig = maps:get(config, ChannelStatus, undefined),
    RawStatus = emqx_resource:call_channel_health_check(ResId, ChannelId, Mod, State),
    exit({ok, channel_status(RawStatus, ChannelConfig)}).

-spec handle_channel_health_check_worker_down(
    data(), pid(), {ok, channel_status_map()}
) ->
    gen_statem:event_handler_result(state(), data()).
handle_channel_health_check_worker_down(Data0, Pid, ExitResult) ->
    #data{
        hc_workers = HCWorkers0 = #{channel := CHCWorkers0},
        added_channels = AddedChannels0
    } = Data0,
    {ChannelId, CHCWorkers1} = maps:take(Pid, CHCWorkers0),
    %% The channel might have got removed while the health check was going on
    CurrentStatus = maps:get(ChannelId, AddedChannels0, channel_not_added),
    {AddedChannels, NewStatus} =
        handle_channel_health_check_worker_down_new_channels_and_status(
            ChannelId,
            ExitResult,
            CurrentStatus,
            AddedChannels0
        ),
    #{ongoing := Ongoing0} = CHCWorkers1,
    {PreviousChanStatus, Ongoing1} = maps:take(ChannelId, Ongoing0),
    CHCWorkers2 = CHCWorkers1#{ongoing := Ongoing1},
    Data1 = Data0#data{added_channels = AddedChannels},
    {Replies, Data2} = reply_pending_channel_health_check_callers(ChannelId, NewStatus, Data1),
    HCWorkers = HCWorkers0#{channel := CHCWorkers2},
    Data3 = Data2#data{hc_workers = HCWorkers},
    Data = continue_channel_health_check_connected(
        ChannelId,
        PreviousChanStatus,
        CurrentStatus,
        Data3
    ),
    CHCActions = start_channel_health_check_action(ChannelId, NewStatus, PreviousChanStatus, Data),
    Actions = Replies ++ CHCActions,
    {keep_state, update_state(Data), Actions}.

handle_channel_health_check_worker_down_new_channels_and_status(
    ChannelId,
    {ok, #{config := CheckedConfig} = NewStatus} = _ExitResult,
    #{config := CurrentConfig} = _CurrentStatus,
    AddedChannels
) when CheckedConfig =:= CurrentConfig ->
    %% Checked config is the same as the current config so we can update the
    %% status in AddedChannels
    {maps:put(ChannelId, NewStatus, AddedChannels), NewStatus};
handle_channel_health_check_worker_down_new_channels_and_status(
    _ChannelId,
    {ok, NewStatus} = _ExitResult,
    _CurrentStatus,
    AddedChannels
) ->
    %% The checked config is different from the current config which means we
    %% should not update AddedChannels because the channel has been removed or
    %% updated while the health check was in progress. We can still reply with
    %% NewStatus because the health check must have been issued before the
    %% configuration changed or the channel got removed.
    {AddedChannels, NewStatus}.

reply_pending_channel_health_check_callers(
    ChannelId, Status0, Data0 = #data{hc_pending_callers = Pending0}
) ->
    Status = to_external_channel_status(Status0),
    #{channel := CPending0} = Pending0,
    Pending = maps:get(ChannelId, CPending0, []),
    Actions = [{reply, From, Status} || From <- Pending],
    CPending = maps:remove(ChannelId, CPending0),
    Data = Data0#data{hc_pending_callers = Pending0#{channel := CPending}},
    {Actions, Data}.

handle_retry_add_channel(Data0, ChannelId) ->
    ?tp(retry_add_channel, #{channel_id => ChannelId}),
    maybe
        {ok, StatusMap} ?= maps:find(ChannelId, Data0#data.added_channels),
        %% Must contain config map if in data.
        #{config := #{} = ChannelConfig} = StatusMap,
        {Actions, Data1} = add_channels_in_list([{ChannelId, ChannelConfig}], Data0),
        Data = trigger_health_check_for_added_channels(Data1),
        {keep_state, Data, Actions}
    else
        error ->
            %% Channel has been removed since timer was set?
            keep_state_and_data
    end.

get_config_for_channels(Data0, ChannelsWithoutConfig) ->
    ResId = Data0#data.id,
    Mod = Data0#data.mod,
    Channels = emqx_resource:call_get_channels(ResId, Mod),
    ChannelIdToConfig = maps:from_list(Channels),
    ChannelStatusMap = Data0#data.added_channels,
    ChannelsWithConfig = [
        {Id, get_config_from_map_or_channel_status(Id, ChannelIdToConfig, ChannelStatusMap)}
     || Id <- ChannelsWithoutConfig
    ],
    %% Filter out channels without config
    [
        ChConf
     || {_Id, Conf} = ChConf <- ChannelsWithConfig,
        Conf =/= no_config
    ].

get_config_from_map_or_channel_status(ChannelId, ChannelIdToConfig, ChannelStatusMap) ->
    ChannelStatus = maps:get(ChannelId, ChannelStatusMap, #{}),
    case maps:get(config, ChannelStatus, undefined) of
        undefined ->
            %% Channel config
            maps:get(ChannelId, ChannelIdToConfig, no_config);
        Config ->
            Config
    end.

-spec update_state(data()) -> data().
update_state(#data{group = Group} = Data) ->
    ToCache = data_record_to_external_map(Data),
    ok = insert_cache(Group, ToCache),
    Data.

health_check_interval(Opts) ->
    maps:get(health_check_interval, Opts, ?HEALTHCHECK_INTERVAL).

-spec maybe_alarm(
    resource_status(),
    boolean(),
    resource_id(),
    _Error :: term(),
    _PrevError :: term()
) -> ok.
maybe_alarm(?status_connected, _IsDryRun, _ResId, _Error, _PrevError) ->
    ok;
maybe_alarm(_Status, true, _ResId, _Error, _PrevError) ->
    ok;
%% Assume that alarm is already active
maybe_alarm(_Status, _IsDryRun, _ResId, Error, Error) ->
    ok;
maybe_alarm(_Status, false, ResId, Error, _PrevError) ->
    HrError =
        case Error of
            {error, undefined} ->
                <<"Unknown reason">>;
            {error, Reason} ->
                emqx_utils:readable_error_msg(Reason);
            _ ->
                Error1 = to_external_channel_status(Error),
                emqx_utils:readable_error_msg(Error1)
        end,
    emqx_alarm:safe_activate(
        ResId,
        #{resource_id => ResId, reason => resource_down},
        <<"resource down: ", HrError/binary>>
    ),
    ?tp(resource_activate_alarm, #{resource_id => ResId, error => HrError}).

without_channel_config(Map) ->
    maps:without([config], Map).

to_external_channel_status(StatusMap0) ->
    StatusMap = without_channel_config(StatusMap0),
    maps:update_with(error, fun external_error/1, StatusMap).

-spec maybe_resume_resource_workers(resource_id(), resource_status()) -> ok.
maybe_resume_resource_workers(ResId, ?status_connected) ->
    lists:foreach(
        fun emqx_resource_buffer_worker:resume/1,
        emqx_resource_buffer_worker_sup:worker_pids(ResId)
    );
maybe_resume_resource_workers(_, _) ->
    ok.

-spec maybe_clear_alarm(boolean(), resource_id()) -> ok | {error, not_found}.
maybe_clear_alarm(true, _ResId) ->
    ok;
maybe_clear_alarm(false, ResId) ->
    emqx_alarm:safe_deactivate(ResId).

parse_health_check_result(Status, _Data) when ?IS_STATUS(Status) ->
    {Status, status_to_error(Status)};
parse_health_check_result({Status, Error}, _Data) when ?IS_STATUS(Status) ->
    {Status, {error, Error}};
parse_health_check_result({error, Error}, Data) ->
    ?tp("health_check_exception", #{resource_id => Data#data.id, reason => Error}),
    ?SLOG(
        error,
        #{
            msg => "health_check_exception",
            resource_id => Data#data.id,
            reason => Error
        },
        #{tag => tag(Data#data.group, Data#data.type)}
    ),
    {?status_disconnected, {error, Error}}.

status_to_error(?status_connected) ->
    undefined;
status_to_error(_) ->
    {error, undefined}.

%% Compatibility
external_error(?not_added_yet) -> not_added_yet;
external_error(?add_channel_failed(Reason)) -> external_error(Reason);
external_error({error, Reason}) -> Reason;
external_error(Other) -> Other.

maybe_reply(Actions, undefined, _Reply) ->
    Actions;
maybe_reply(Actions, From, Reply) ->
    [{reply, From, Reply} | Actions].

-spec data_record_to_external_map(data()) -> resource_data().
data_record_to_external_map(Data) ->
    AddedChannelsWithoutConfigs =
        maps:map(
            fun(_ChanID, Status) ->
                to_external_channel_status(Status)
            end,
            Data#data.added_channels
        ),
    #{
        id => Data#data.id,
        error => external_error(Data#data.error),
        mod => Data#data.mod,
        callback_mode => Data#data.callback_mode,
        query_mode => Data#data.query_mode,
        config => Data#data.config,
        status => Data#data.status,
        state => Data#data.state,
        added_channels => AddedChannelsWithoutConfigs
    }.

-spec wait_for_ready(resource_id(), integer()) -> ok | timeout | {error, term()}.
wait_for_ready(ResId, WaitTime) ->
    do_wait_for_ready(ResId, WaitTime div ?WAIT_FOR_RESOURCE_DELAY).

do_wait_for_ready(_ResId, 0) ->
    timeout;
do_wait_for_ready(ResId, Retry) ->
    case emqx_resource_cache:read_status(ResId) of
        #{status := ?status_connected} ->
            ok;
        #{status := ?status_disconnected, error := Err} ->
            {error, Err};
        _ ->
            %% connecting, or not_found
            timer:sleep(?WAIT_FOR_RESOURCE_DELAY),
            do_wait_for_ready(ResId, Retry - 1)
    end.

safe_call(ResId, Message, Timeout) ->
    try
        gen_statem:call(?REF(ResId), Message, {clean_timeout, Timeout})
    catch
        error:badarg ->
            {error, not_found};
        exit:{R, _} when R == noproc; R == normal; R == shutdown ->
            {error, not_found};
        exit:{timeout, _} ->
            {error, timeout};
        exit:{{shutdown, removed}, _} ->
            {error, not_found}
    end.

%% Helper functions for chanel status data

channel_status_not_added(ChannelConfig) ->
    #{
        %% The status of the channel. Can be one of the following:
        %% - disconnected: the channel is not added to the resource (error may contain the reason))
        %% - connecting:   the channel has been added to the resource state but
        %%                 either the resource status is connecting or the
        %%                 on_channel_get_status callback has returned connecting
        %% - connected:    the channel is added to the resource, the resource is
        %%                 connected and the on_channel_get_status callback has returned
        %%                 connected. The error field should be undefined.
        status => ?status_disconnected,
        error => ?not_added_yet,
        config => ChannelConfig
    }.

channel_status_new_waiting_for_health_check(ChannelConfig) ->
    #{
        status => ?status_connecting,
        error => no_health_check_yet,
        config => ChannelConfig
    }.

channel_status({?status_connecting, Error}, ChannelConfig) ->
    #{
        status => ?status_connecting,
        error => Error,
        config => ChannelConfig
    };
channel_status({?status_disconnected, Error}, ChannelConfig) ->
    #{
        status => ?status_disconnected,
        error => Error,
        config => ChannelConfig
    };
channel_status(?status_disconnected, ChannelConfig) ->
    #{
        status => ?status_disconnected,
        error => <<"Disconnected for unknown reason">>,
        config => ChannelConfig
    };
channel_status(?status_connecting, ChannelConfig) ->
    #{
        status => ?status_connecting,
        error => <<"Not connected for unknown reason">>,
        config => ChannelConfig
    };
channel_status(?status_connected, ChannelConfig) ->
    #{
        status => ?status_connected,
        error => undefined,
        config => ChannelConfig
    };
%% Probably not so useful but it is permitted to set an error even when the
%% status is connected
channel_status({?status_connected, Error}, ChannelConfig) ->
    #{
        status => ?status_connected,
        error => Error,
        config => ChannelConfig
    };
channel_status(?add_channel_failed(_Reason) = Error, ChannelConfig) ->
    #{
        status => ?status_disconnected,
        error => Error,
        config => ChannelConfig
    };
channel_status({error, Reason}, ChannelConfig) ->
    S = channel_error_status(Reason),
    S#{config => ChannelConfig}.

channel_error_status(Reason) ->
    #{
        status => ?status_disconnected,
        error => Reason
    }.

is_channel_apt_for_health_check(#{error := {unhealthy_target, _}}) ->
    false;
is_channel_apt_for_health_check(#{error := unhealthy_target}) ->
    false;
is_channel_apt_for_health_check(StatusMap) ->
    channel_status_is_channel_added(StatusMap).

channel_status_is_channel_added(#{error := ?not_added_yet}) ->
    false;
channel_status_is_channel_added(#{error := ?add_channel_failed(_)}) ->
    false;
channel_status_is_channel_added(_StatusMap) ->
    true.

-spec add_or_update_channel_status(data(), channel_id(), map(), resource_state()) -> data().
add_or_update_channel_status(Data, ChannelId, ChannelConfig, State) ->
    Channels = Data#data.added_channels,
    ChannelStatus = channel_status_not_added(ChannelConfig),
    NewChannels = maps:put(ChannelId, ChannelStatus, Channels),
    ResStatus = state_to_status(State),
    IsDryRun = emqx_resource:is_dry_run(ChannelId),
    maybe_alarm(ResStatus, IsDryRun, ChannelId, ChannelStatus, no_prev),
    Data#data{added_channels = NewChannels}.

state_to_status(?state_stopped) -> ?rm_status_stopped;
state_to_status(?state_connected) -> ?status_connected;
state_to_status(?state_connecting) -> ?status_connecting;
state_to_status(?state_disconnected) -> ?status_disconnected.

log_level(true) -> info;
log_level(false) -> warning.

tag(Group, Type) ->
    Str = emqx_utils_conv:str(Group) ++ "/" ++ emqx_utils_conv:str(Type),
    string:uppercase(Str).

%% For still unknown reasons (e.g.: `emqx_metrics_worker' process might die?), metrics
%% might be lost for a running resource, and future attempts to bump them result in
%% errors.  As mitigation, we ensure such metrics are created here so that restarting
%% the resource or resetting its metrics can recreate them.
ensure_metrics(ResId) ->
    {ok, _} = emqx_resource:ensure_metrics(ResId),
    ok.

%% When a resource enters a `?status_disconnected' state, late channel health check
%% replies are useless and could corrup state.
-spec abort_all_channel_health_checks(data()) -> data().
abort_all_channel_health_checks(Data0) ->
    #data{
        hc_workers = #{channel := CHCWorkers} = HCWorkers0,
        hc_pending_callers = #{channel := CPending} = Pending0
    } = Data0,
    lists:foreach(
        fun(From) ->
            gen_statem:reply(From, {error, resource_disconnected})
        end,
        lists:flatten(maps:values(CPending))
    ),
    maps:foreach(
        fun
            (Pid, _ChannelId) when is_pid(Pid) ->
                abort_channel_health_check(Pid);
            (_, _) ->
                ok
        end,
        CHCWorkers
    ),
    HCWorkers = HCWorkers0#{channel := #{ongoing => #{}}},
    Pending = Pending0#{channel := #{}},
    Data0#data{
        hc_workers = HCWorkers,
        hc_pending_callers = Pending
    }.

abort_channel_health_check(Pid) ->
    %% We're already linked to the worker pids due to `spawn_link'.
    MRef = monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', MRef, process, Pid, _} ->
            ok
    end,
    %% Clean the exit signal so it doesn't contaminate state handling.
    receive
        {'EXIT', Pid, _} ->
            ok
    after 0 -> ok
    end.

map_take_or(Map, Key, Default) ->
    maybe
        error ?= maps:take(Key, Map),
        {Default, Map}
    end.

abort_health_checks_for_channel(Data0, ChannelId) ->
    #data{
        hc_workers = #{channel := #{ongoing := Ongoing0} = CHCWorkers0} = HCWorkers0,
        hc_pending_callers = #{channel := CPending0} = Pending0
    } = Data0,
    Ongoing = maps:remove(ChannelId, Ongoing0),
    {Callers, CPending} = map_take_or(CPending0, ChannelId, []),
    lists:foreach(
        fun(From) ->
            gen_statem:reply(From, {error, resource_disconnected})
        end,
        Callers
    ),
    CHCWorkers = maps:fold(
        fun
            (Pid, ChannelId0, Acc) when is_pid(Pid), ChannelId0 == ChannelId ->
                ?tp(warning, "aborting_channel_hc", #{channel_id => ChannelId, pid => Pid}),
                abort_channel_health_check(Pid),
                maps:remove(Pid, Acc);
            (ChannelId0, _Config, Acc) when ChannelId0 == ChannelId ->
                maps:remove(ChannelId0, Acc);
            (_, _, Acc) ->
                Acc
        end,
        CHCWorkers0,
        CHCWorkers0
    ),
    HCWorkers = HCWorkers0#{channel := CHCWorkers#{ongoing := Ongoing}},
    Pending = Pending0#{channel := CPending},
    Data0#data{
        hc_workers = HCWorkers,
        hc_pending_callers = Pending
    }.
