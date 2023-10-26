%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    remove_channel/2,
    get_channels/1
]).

-export([
    lookup/1,
    list_all/0,
    list_group/1,
    lookup_cached/1,
    get_metrics/1,
    reset_metrics/1
]).

-export([
    set_resource_status_connecting/1
]).

% Server
-export([start_link/5]).

% Behaviour
-export([init/1, callback_mode/0, handle_event/4, terminate/3]).

% State record
-record(data, {
    id,
    group,
    mod,
    callback_mode,
    query_mode,
    config,
    opts,
    status,
    state,
    error,
    pid,
    added_channels,
    extra
}).
-type data() :: #data{}.

-define(NAME(ResId), {n, l, {?MODULE, ResId}}).
-define(REF(ResId), {via, gproc, ?NAME(ResId)}).

-define(WAIT_FOR_RESOURCE_DELAY, 100).
-define(T_OPERATION, 5000).
-define(T_LOOKUP, 1000).

-define(IS_STATUS(ST), ST =:= connecting; ST =:= connected; ST =:= disconnected).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Called from emqx_resource when starting a resource instance.
%%
%% Triggers the emqx_resource_manager_sup supervisor to actually create
%% and link the process itself if not already started.
-spec ensure_resource(
    resource_id(),
    resource_group(),
    resource_type(),
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
-spec recreate(resource_id(), resource_type(), resource_config(), creation_opts()) ->
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
    case QueryMode of
        %% the resource has built-in buffer, so there is no need for resource workers
        simple_sync_internal_buffer ->
            ok;
        simple_async_internal_buffer ->
            ok;
        %% The resource is a consumer resource, so there is no need for resource workers
        no_queries ->
            ok;
        _ ->
            %% start resource workers as the query type requires them
            ok = emqx_resource_buffer_worker_sup:start_workers(ResId, Opts),
            case maps:get(start_after_created, Opts, ?START_AFTER_CREATED) of
                true ->
                    wait_for_ready(ResId, maps:get(start_timeout, Opts, ?START_TIMEOUT));
                false ->
                    ok
            end
    end.

%% @doc Called from `emqx_resource` when doing a dry run for creating a resource instance.
%%
%% Triggers the `emqx_resource_manager_sup` supervisor to actually create
%% and link the process itself if not already started, and then immedately stops.
-spec create_dry_run(resource_type(), resource_config()) ->
    ok | {error, Reason :: term()}.
create_dry_run(ResourceType, Config) ->
    ResId = make_test_id(),
    create_dry_run(ResId, ResourceType, Config).

create_dry_run(ResId, ResourceType, Config) ->
    create_dry_run(ResId, ResourceType, Config, fun do_nothing_on_ready/1).

do_nothing_on_ready(_ResId) ->
    ok.

-spec create_dry_run(resource_id(), resource_type(), resource_config(), OnReadyCallback) ->
    ok | {error, Reason :: term()}
when
    OnReadyCallback :: fun((resource_id()) -> ok | {error, Reason :: term()}).
create_dry_run(ResId, ResourceType, Config, OnReadyCallback) ->
    Opts =
        case is_map(Config) of
            true -> maps:get(resource_opts, Config, #{});
            false -> #{}
        end,
    ok = emqx_resource_manager_sup:ensure_child(ResId, <<"dry_run">>, ResourceType, Config, Opts),
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
            _ = remove(ResId),
            {error, Reason};
        timeout ->
            _ = remove(ResId),
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
        safe_call(ResId, {remove, ClearMetrics}, ?T_OPERATION)
    after
        %% Ensure the supervisor has it removed, otherwise the immediate re-add will see a stale process
        %% If the 'remove' call above had succeeded, this is mostly a no-op but still needed to avoid race condition.
        %% Otherwise this is a 'infinity' shutdown, so it may take arbitrary long.
        emqx_resource_manager_sup:delete_child(ResId)
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
-spec start(resource_id(), creation_opts()) -> ok | {error, Reason :: term()}.
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
    case safe_call(ResId, stop, ?T_OPERATION) of
        ok ->
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
    try read_cache(ResId) of
        Data = #data{group = Group} ->
            {ok, Group, data_record_to_external_map(Data)}
    catch
        error:badarg ->
            {error, not_found}
    end.

%% @doc Get the metrics for the specified resource
get_metrics(ResId) ->
    emqx_metrics_worker:get_metrics(?RES_METRICS, ResId).

%% @doc Reset the metrics for the specified resource
-spec reset_metrics(resource_id()) -> ok.
reset_metrics(ResId) ->
    emqx_metrics_worker:reset_metrics(?RES_METRICS, ResId).

%% @doc Returns the data for all resources
-spec list_all() -> [resource_data()].
list_all() ->
    lists:map(
        fun data_record_to_external_map/1,
        gproc:select({local, names}, [{{?NAME('_'), '_', '$1'}, [], ['$1']}])
    ).

%% @doc Returns a list of ids for all the resources in a group
-spec list_group(resource_group()) -> [resource_id()].
list_group(Group) ->
    Guard = {'==', {element, #data.group, '$1'}, Group},
    gproc:select({local, names}, [{{?NAME('$2'), '_', '$1'}, [Guard], ['$2']}]).

-spec health_check(resource_id()) -> {ok, resource_status()} | {error, term()}.
health_check(ResId) ->
    safe_call(ResId, health_check, ?T_OPERATION).

-spec channel_health_check(resource_id(), channel_id()) ->
    {ok, resource_status()} | {error, term()}.
channel_health_check(ResId, ChannelId) ->
    %% Do normal health check first to trigger health checks for channels
    %% and update the cached health status for the channels
    _ = health_check(ResId),
    safe_call(ResId, {channel_health_check, ChannelId}, ?T_OPERATION).

add_channel(ResId, ChannelId, Config) ->
    safe_call(ResId, {add_channel, ChannelId, Config}, ?T_OPERATION).

remove_channel(ResId, ChannelId) ->
    safe_call(ResId, {remove_channel, ChannelId}, ?T_OPERATION).

get_channels(ResId) ->
    safe_call(ResId, get_channels, ?T_OPERATION).

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
    case maps:get(start_after_created, Opts, ?START_AFTER_CREATED) of
        true ->
            %% init the cache so that lookup/1 will always return something
            UpdatedData = update_state(Data#data{status = connecting}),
            {ok, connecting, UpdatedData, {next_event, internal, start_resource}};
        false ->
            %% init the cache so that lookup/1 will always return something
            UpdatedData = update_state(Data#data{status = stopped}),
            {ok, stopped, UpdatedData}
    end.

terminate({shutdown, removed}, _State, _Data) ->
    ok;
terminate(_Reason, _State, Data) ->
    _ = maybe_stop_resource(Data),
    _ = erase_cache(Data),
    ok.

%% Behavior callback

callback_mode() -> [handle_event_function, state_enter].

%% Common event Function

% Called during testing to force a specific state
handle_event({call, From}, set_resource_status_connecting, _State, Data) ->
    UpdatedData = update_state(Data#data{status = connecting}, Data),
    {next_state, connecting, UpdatedData, [{reply, From, ok}]};
% Called when the resource is to be restarted
handle_event({call, From}, restart, _State, Data) ->
    DataNext = stop_resource(Data),
    start_resource(DataNext, From);
% Called when the resource is to be started (also used for manual reconnect)
handle_event({call, From}, start, State, Data) when
    State =:= stopped orelse
        State =:= disconnected
->
    start_resource(Data, From);
handle_event({call, From}, start, _State, _Data) ->
    {keep_state_and_data, [{reply, From, ok}]};
% Called when the resource is to be stopped
handle_event({call, From}, stop, stopped, _Data) ->
    {keep_state_and_data, [{reply, From, ok}]};
handle_event({call, From}, stop, _State, Data) ->
    UpdatedData = stop_resource(Data),
    {next_state, stopped, update_state(UpdatedData, Data), [{reply, From, ok}]};
% Called when a resource is to be stopped and removed.
handle_event({call, From}, {remove, ClearMetrics}, _State, Data) ->
    handle_remove_event(From, ClearMetrics, Data);
% Called when the state-data of the resource is being looked up.
handle_event({call, From}, lookup, _State, #data{group = Group} = Data) ->
    Reply = {ok, Group, data_record_to_external_map(Data)},
    {keep_state_and_data, [{reply, From, Reply}]};
% Called when doing a manually health check.
handle_event({call, From}, health_check, stopped, _Data) ->
    Actions = [{reply, From, {error, resource_is_stopped}}],
    {keep_state_and_data, Actions};
handle_event({call, From}, {channel_health_check, _}, stopped, _Data) ->
    Actions = [{reply, From, {error, resource_is_stopped}}],
    {keep_state_and_data, Actions};
handle_event({call, From}, health_check, _State, Data) ->
    handle_manually_health_check(From, Data);
handle_event({call, From}, {channel_health_check, ChannelId}, _State, Data) ->
    handle_manually_channel_health_check(From, Data, ChannelId);
% State: CONNECTING
handle_event(enter, _OldState, connecting = State, Data) ->
    ok = log_state_consistency(State, Data),
    {keep_state_and_data, [{state_timeout, 0, health_check}]};
handle_event(internal, start_resource, connecting, Data) ->
    start_resource(Data, undefined);
handle_event(state_timeout, health_check, connecting, Data) ->
    handle_connecting_health_check(Data);
%% State: CONNECTED
%% The connected state is entered after a successful on_start/2 of the callback mod
%% and successful health_checks
handle_event(enter, _OldState, connected = State, Data) ->
    ok = log_state_consistency(State, Data),
    _ = emqx_alarm:safe_deactivate(Data#data.id),
    ?tp(resource_connected_enter, #{}),
    {keep_state_and_data, health_check_actions(Data)};
handle_event(state_timeout, health_check, connected, Data) ->
    handle_connected_health_check(Data);
handle_event(
    {call, From}, {add_channel, ChannelId, Config}, connected = _State, Data
) ->
    handle_add_channel(From, Data, ChannelId, Config);
handle_event(
    {call, From}, {remove_channel, ChannelId}, connected = _State, Data
) ->
    handle_remove_channel(From, ChannelId, Data);
%% State: DISCONNECTED
handle_event(enter, _OldState, disconnected = State, Data) ->
    ok = log_state_consistency(State, Data),
    ?tp(resource_disconnected_enter, #{}),
    {keep_state_and_data, retry_actions(Data)};
handle_event(state_timeout, auto_retry, disconnected, Data) ->
    ?tp(resource_auto_reconnect, #{}),
    start_resource(Data, undefined);
%% State: STOPPED
%% The stopped state is entered after the resource has been explicitly stopped
handle_event(enter, _OldState, stopped = State, Data) ->
    ok = log_state_consistency(State, Data),
    {keep_state_and_data, []};
%% The following events can be handled in any other state
handle_event(
    {call, From}, {add_channel, ChannelId, _Config}, State, Data
) ->
    handle_not_connected_add_channel(From, ChannelId, State, Data);
handle_event(
    {call, From}, {remove_channel, ChannelId}, _State, Data
) ->
    handle_not_connected_remove_channel(From, ChannelId, Data);
handle_event(
    {call, From}, get_channels, _State, Data
) ->
    Channels = emqx_resource:call_get_channels(Data#data.id, Data#data.mod),
    {keep_state_and_data, {reply, From, {ok, Channels}}};
% Ignore all other events
handle_event(EventType, EventData, State, Data) ->
    ?SLOG(
        error,
        #{
            msg => "ignore_all_other_events",
            event_type => EventType,
            event_data => EventData,
            state => State,
            data => emqx_utils:redact(Data)
        }
    ),
    keep_state_and_data.

log_state_consistency(State, #data{status = State} = Data) ->
    log_cache_consistency(read_cache(Data#data.id), Data);
log_state_consistency(State, Data) ->
    ?tp(warning, "inconsistent_state", #{
        state => State,
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
insert_cache(ResId, Data = #data{}) ->
    gproc:set_value(?NAME(ResId), Data).

read_cache(ResId) ->
    gproc:lookup_value(?NAME(ResId)).

erase_cache(_Data = #data{id = ResId}) ->
    gproc:unreg(?NAME(ResId)).

try_read_cache(ResId) ->
    try
        read_cache(ResId)
    catch
        error:badarg -> not_found
    end.

retry_actions(Data) ->
    case maps:get(health_check_interval, Data#data.opts, ?HEALTHCHECK_INTERVAL) of
        undefined ->
            [];
        RetryInterval ->
            [{state_timeout, RetryInterval, auto_retry}]
    end.

health_check_actions(Data) ->
    [{state_timeout, health_check_interval(Data#data.opts), health_check}].

handle_remove_event(From, ClearMetrics, Data) ->
    %% stop the buffer workers first, brutal_kill, so it should be fast
    ok = emqx_resource_buffer_worker_sup:stop_workers(Data#data.id, Data#data.opts),
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
    case emqx_resource:call_start(Data#data.id, Data#data.mod, Data#data.config) of
        {ok, ResourceState} ->
            UpdatedData1 = Data#data{status = connecting, state = ResourceState},
            %% Perform an initial health_check immediately before transitioning into a connected state
            UpdatedData2 = add_channels(UpdatedData1),
            Actions = maybe_reply([{state_timeout, 0, health_check}], From, ok),
            {next_state, connecting, update_state(UpdatedData2, Data), Actions};
        {error, Reason} = Err ->
            ?SLOG(warning, #{
                msg => "start_resource_failed",
                id => Data#data.id,
                reason => Reason
            }),
            _ = maybe_alarm(disconnected, Data#data.id, Err, Data#data.error),
            %% Add channels and raise alarms
            NewData1 = channels_health_check(disconnected, add_channels(Data)),
            %% Keep track of the error reason why the connection did not work
            %% so that the Reason can be returned when the verification call is made.
            NewData2 = NewData1#data{status = disconnected, error = Err},
            Actions = maybe_reply(retry_actions(NewData2), From, Err),
            {next_state, disconnected, update_state(NewData2, Data), Actions}
    end.

add_channels(Data) ->
    %% Add channels to the Channels map but not to the resource state
    %% Channels will be added to the resouce state after the initial health_check
    %% if that succeeds.
    ChannelIDConfigTuples = emqx_resource:call_get_channels(Data#data.id, Data#data.mod),
    Channels = Data#data.added_channels,
    NewChannels = lists:foldl(
        fun({ChannelID, _Conf}, Acc) ->
            maps:put(ChannelID, {error, connecting}, Acc)
        end,
        Channels,
        ChannelIDConfigTuples
    ),
    Data#data{added_channels = NewChannels}.

add_channels_in_list([], Data) ->
    Data;
add_channels_in_list([{ChannelID, ChannelConfig} | Rest], Data) ->
    case
        emqx_resource:call_add_channel(
            Data#data.id, Data#data.mod, Data#data.state, ChannelID, ChannelConfig
        )
    of
        {ok, NewState} ->
            AddedChannelsMap = Data#data.added_channels,
            %% Set the channel status to connecting to indicate that
            %% we have not yet performed the initial health_check
            NewAddedChannelsMap = maps:put(ChannelID, connecting, AddedChannelsMap),
            NewData = Data#data{
                state = NewState,
                added_channels = NewAddedChannelsMap
            },
            add_channels_in_list(Rest, NewData);
        {error, Reason} = Error ->
            ?SLOG(warning, #{
                msg => add_channel_failed,
                id => Data#data.id,
                channel_id => ChannelID,
                reason => Reason
            }),
            AddedChannelsMap = Data#data.added_channels,
            NewAddedChannelsMap = maps:put(ChannelID, Error, AddedChannelsMap),
            NewData = Data#data{
                added_channels = NewAddedChannelsMap
            },
            %% Raise an alarm since the channel could not be added
            _ = maybe_alarm(disconnected, ChannelID, Error, no_prev_error),
            add_channels_in_list(Rest, NewData)
    end.

maybe_stop_resource(#data{status = Status} = Data) when Status /= stopped ->
    stop_resource(Data);
maybe_stop_resource(#data{status = stopped} = Data) ->
    Data.

stop_resource(#data{state = ResState, id = ResId} = Data) ->
    %% We don't care the return value of the Mod:on_stop/2.
    %% The callback mod should make sure the resource is stopped after on_stop/2
    %% is returned.
    HasAllocatedResources = emqx_resource:has_allocated_resources(ResId),
    %% Before stop is called we remove all the channels from the resource
    NewData = remove_channels(Data),
    case ResState =/= undefined orelse HasAllocatedResources of
        true ->
            %% we clear the allocated resources after stop is successful
            emqx_resource:call_stop(NewData#data.id, NewData#data.mod, ResState);
        false ->
            ok
    end,
    _ = maybe_clear_alarm(ResId),
    ok = emqx_metrics_worker:reset_metrics(?RES_METRICS, ResId),
    NewData#data{status = stopped}.

remove_channels(Data) ->
    Channels = maps:keys(Data#data.added_channels),
    remove_channels_in_list(Channels, Data, false).

remove_channels_in_list([], Data, _KeepInChannelMap) ->
    Data;
remove_channels_in_list([ChannelID | Rest], Data, KeepInChannelMap) ->
    AddedChannelsMap = Data#data.added_channels,
    NewAddedChannelsMap =
        case KeepInChannelMap of
            true ->
                AddedChannelsMap;
            false ->
                maybe_clear_alarm(ChannelID),
                maps:remove(ChannelID, AddedChannelsMap)
        end,
    case safe_call_remove_channel(Data#data.id, Data#data.mod, Data#data.state, ChannelID) of
        {ok, NewState} ->
            NewData = Data#data{
                state = NewState,
                added_channels = NewAddedChannelsMap
            },
            remove_channels_in_list(Rest, NewData, KeepInChannelMap);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => remove_channel_failed,
                id => Data#data.id,
                channel_id => ChannelID,
                reason => Reason
            }),
            NewData = Data#data{
                added_channels = NewAddedChannelsMap
            },
            remove_channels_in_list(Rest, NewData, KeepInChannelMap)
    end.

safe_call_remove_channel(_ResId, _Mod, undefined = State, _ChannelID) ->
    {ok, State};
safe_call_remove_channel(ResId, Mod, State, ChannelID) ->
    emqx_resource:call_remove_channel(ResId, Mod, State, ChannelID).

make_test_id() ->
    RandId = iolist_to_binary(emqx_utils:gen_id(16)),
    <<?TEST_ID_PREFIX, RandId/binary>>.

handle_add_channel(From, Data, ChannelId, ChannelConfig) ->
    Channels = Data#data.added_channels,
    case maps:get(ChannelId, Channels, {error, not_added}) of
        {error, _Reason} ->
            %% The channel is not installed in the connector state
            %% We need to install it
            handle_add_channel_need_insert(From, Data, ChannelId, Data, ChannelConfig);
        _ ->
            %% The channel is already installed in the connector state
            %% We don't need to install it again
            {keep_state_and_data, [{reply, From, ok}]}
    end.

handle_add_channel_need_insert(From, Data, ChannelId, Data, ChannelConfig) ->
    NewData = add_channel_need_insert_update_data(Data, ChannelId, ChannelConfig),
    %% Trigger a health check to raise alarm if channel is not healthy
    {keep_state, NewData, [{reply, From, ok}, {state_timeout, 0, health_check}]}.

add_channel_need_insert_update_data(Data, ChannelId, ChannelConfig) ->
    case
        emqx_resource:call_add_channel(
            Data#data.id, Data#data.mod, Data#data.state, ChannelId, ChannelConfig
        )
    of
        {ok, NewState} ->
            AddedChannelsMap = Data#data.added_channels,
            %% Setting channel status to connecting to indicate that an health check
            %% has not been performed yet
            NewAddedChannelsMap = maps:put(ChannelId, connecting, AddedChannelsMap),
            UpdatedData = Data#data{
                state = NewState,
                added_channels = NewAddedChannelsMap
            },
            update_state(UpdatedData, Data);
        {error, _Reason} = Error ->
            ChannelsMap = Data#data.added_channels,
            NewChannelsMap = maps:put(ChannelId, Error, ChannelsMap),
            UpdatedData = Data#data{
                added_channels = NewChannelsMap
            },
            update_state(UpdatedData, Data)
    end.

handle_not_connected_add_channel(From, ChannelId, State, Data) ->
    %% When state is not connected the channel will be added to the channels
    %% map but nothing else will happen.
    Channels = Data#data.added_channels,
    NewChannels = maps:put(ChannelId, {error, resource_not_operational}, Channels),
    NewData1 = Data#data{added_channels = NewChannels},
    %% Do channel health check to trigger alarm
    NewData2 = channels_health_check(State, NewData1),
    {keep_state, update_state(NewData2, Data), [{reply, From, ok}]}.

handle_remove_channel(From, ChannelId, Data) ->
    Channels = Data#data.added_channels,
    %% Deactivate alarm
    _ = maybe_clear_alarm(ChannelId),
    case maps:get(ChannelId, Channels, {error, not_added}) of
        {error, _} ->
            %% The channel is already not installed in the connector state.
            %% We still need to remove it from the added_channels map
            AddedChannels = Data#data.added_channels,
            NewAddedChannels = maps:remove(ChannelId, AddedChannels),
            NewData = Data#data{
                added_channels = NewAddedChannels
            },
            {keep_state, NewData, [{reply, From, ok}]};
        _ ->
            %% The channel is installed in the connector state
            handle_remove_channel_exists(From, ChannelId, Data)
    end.

handle_remove_channel_exists(From, ChannelId, Data) ->
    case
        emqx_resource:call_remove_channel(
            Data#data.id, Data#data.mod, Data#data.state, ChannelId
        )
    of
        {ok, NewState} ->
            AddedChannelsMap = Data#data.added_channels,
            NewAddedChannelsMap = maps:remove(ChannelId, AddedChannelsMap),
            UpdatedData = Data#data{
                state = NewState,
                added_channels = NewAddedChannelsMap
            },
            {keep_state, update_state(UpdatedData, Data), [{reply, From, ok}]};
        {error, Reason} = Error ->
            %% Log the error as a warning
            ?SLOG(warning, #{
                msg => remove_channel_failed,
                id => Data#data.id,
                channel_id => ChannelId,
                reason => Reason
            }),
            {keep_state_and_data, [{reply, From, Error}]}
    end.

handle_not_connected_remove_channel(From, ChannelId, Data) ->
    %% When state is not connected the channel will be removed from the channels
    %% map but nothing else will happen.
    Channels = Data#data.added_channels,
    NewChannels = maps:remove(ChannelId, Channels),
    NewData = Data#data{added_channels = NewChannels},
    _ = maybe_clear_alarm(ChannelId),
    {keep_state, update_state(NewData, Data), [{reply, From, ok}]}.

handle_manually_health_check(From, Data) ->
    with_health_check(
        Data,
        fun(Status, UpdatedData) ->
            Actions = [{reply, From, {ok, Status}}],
            {next_state, Status, channels_health_check(Status, UpdatedData), Actions}
        end
    ).

handle_manually_channel_health_check(From, #data{state = undefined}, _ChannelId) ->
    {keep_state_and_data, [{reply, From, {ok, disconnected}}]};
handle_manually_channel_health_check(
    From,
    #data{added_channels = Channels} = _Data,
    ChannelId
) when
    is_map_key(ChannelId, Channels)
->
    {keep_state_and_data, [{reply, From, maps:get(ChannelId, Channels)}]};
handle_manually_channel_health_check(
    From,
    _Data,
    _ChannelId
) ->
    {keep_state_and_data, [{reply, From, {error, channel_not_found}}]}.

get_channel_status_channel_added(#data{id = ResId, mod = Mod, state = State}, ChannelId) ->
    emqx_resource:call_channel_health_check(ResId, ChannelId, Mod, State).

handle_connecting_health_check(Data) ->
    with_health_check(
        Data,
        fun
            (connected, UpdatedData) ->
                {next_state, connected, channels_health_check(connected, UpdatedData)};
            (connecting, UpdatedData) ->
                {keep_state, channels_health_check(connecting, UpdatedData),
                    health_check_actions(UpdatedData)};
            (disconnected, UpdatedData) ->
                {next_state, disconnected, channels_health_check(disconnected, UpdatedData)}
        end
    ).

handle_connected_health_check(Data) ->
    with_health_check(
        Data,
        fun
            (connected, UpdatedData) ->
                {keep_state, channels_health_check(connected, UpdatedData),
                    health_check_actions(UpdatedData)};
            (Status, UpdatedData) ->
                ?SLOG(warning, #{
                    msg => "health_check_failed",
                    id => Data#data.id,
                    status => Status
                }),
                {next_state, Status, channels_health_check(Status, UpdatedData)}
        end
    ).

with_health_check(#data{state = undefined} = Data, Func) ->
    Func(disconnected, Data);
with_health_check(#data{error = PrevError} = Data, Func) ->
    ResId = Data#data.id,
    HCRes = emqx_resource:call_health_check(Data#data.id, Data#data.mod, Data#data.state),
    {Status, NewState, Err} = parse_health_check_result(HCRes, Data),
    _ = maybe_alarm(Status, ResId, Err, PrevError),
    ok = maybe_resume_resource_workers(ResId, Status),
    UpdatedData = Data#data{
        state = NewState, status = Status, error = Err
    },
    Func(Status, update_state(UpdatedData, Data)).

channels_health_check(connected = _ResourceStatus, Data0) ->
    Channels = maps:to_list(Data0#data.added_channels),
    %% All channels with an error status are considered not added
    ChannelsNotAdded = [
        ChannelId
     || {ChannelId, Status} <- Channels,
        not is_channel_added(Status)
    ],
    %% Attempt to add channels that are not added
    ChannelsNotAddedWithConfigs = get_config_for_channels(Data0, ChannelsNotAdded),
    Data1 = add_channels_in_list(ChannelsNotAddedWithConfigs, Data0),
    %% Now that we have done the adding, we can get the status of all channels
    Data2 = channel_status_for_all_channels(Data1),
    update_state(Data2, Data0);
channels_health_check(ResourceStatus, Data0) ->
    %% Whenever the resource is not connected:
    %% 1. Remove all added channels
    %% 2. Change the status to an error status
    %% 3. Raise alarms
    Channels = Data0#data.added_channels,
    ChannelsToRemove = [
        ChannelId
     || {ChannelId, Status} <- maps:to_list(Channels),
        is_channel_added(Status)
    ],
    Data1 = remove_channels_in_list(ChannelsToRemove, Data0, true),
    ChannelsWithNewAndOldStatuses =
        [
            {ChannelId, OldStatus,
                {error, resource_not_connected_channel_error_msg(ResourceStatus, ChannelId, Data1)}}
         || {ChannelId, OldStatus} <- maps:to_list(Data1#data.added_channels)
        ],
    %% Raise alarms
    _ = lists:foreach(
        fun({ChannelId, OldStatus, NewStatus}) ->
            _ = maybe_alarm(NewStatus, ChannelId, NewStatus, OldStatus)
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
    update_state(Data2, Data0).

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

channel_status_for_all_channels(Data) ->
    Channels = maps:to_list(Data#data.added_channels),
    AddedChannelsWithOldAndNewStatus = [
        {ChannelId, OldStatus, get_channel_status_channel_added(Data, ChannelId)}
     || {ChannelId, OldStatus} <- Channels,
        is_channel_added(OldStatus)
    ],
    %% Remove the added channels with a new error statuses
    ChannelsToRemove = [
        ChannelId
     || {ChannelId, _, {error, _}} <- AddedChannelsWithOldAndNewStatus
    ],
    Data1 = remove_channels_in_list(ChannelsToRemove, Data, true),
    %% Raise/clear alarms
    lists:foreach(
        fun
            ({ID, _OldStatus, connected}) ->
                _ = maybe_clear_alarm(ID);
            ({ID, OldStatus, NewStatus}) ->
                _ = maybe_alarm(NewStatus, ID, NewStatus, OldStatus)
        end,
        AddedChannelsWithOldAndNewStatus
    ),
    %% Update the ChannelsMap
    ChannelsMap = Data1#data.added_channels,
    NewChannelsMap =
        lists:foldl(
            fun({ChannelId, _, NewStatus}, Acc) ->
                maps:put(ChannelId, NewStatus, Acc)
            end,
            ChannelsMap,
            AddedChannelsWithOldAndNewStatus
        ),
    Data1#data{added_channels = NewChannelsMap}.

is_channel_added({error, _}) ->
    false;
is_channel_added(_) ->
    true.

get_config_for_channels(Data0, ChannelsWithoutConfig) ->
    ResId = Data0#data.id,
    Mod = Data0#data.mod,
    Channels = emqx_resource:call_get_channels(ResId, Mod),
    ChannelIdToConfig = maps:from_list(Channels),
    ChannelsWithConfig = [
        {Id, maps:get(Id, ChannelIdToConfig, no_config)}
     || Id <- ChannelsWithoutConfig
    ],
    %% Filter out channels without config
    [
        ChConf
     || {_Id, Conf} = ChConf <- ChannelsWithConfig,
        Conf =/= no_config
    ].

update_state(Data) ->
    update_state(Data, undefined).

update_state(DataWas, DataWas) ->
    DataWas;
update_state(Data, _DataWas) ->
    _ = insert_cache(Data#data.id, Data),
    Data.

health_check_interval(Opts) ->
    maps:get(health_check_interval, Opts, ?HEALTHCHECK_INTERVAL).

maybe_alarm(connected, _ResId, _Error, _PrevError) ->
    ok;
maybe_alarm(_Status, <<?TEST_ID_PREFIX, _/binary>>, _Error, _PrevError) ->
    ok;
%% Assume that alarm is already active
maybe_alarm(_Status, _ResId, Error, Error) ->
    ok;
maybe_alarm(_Status, ResId, Error, _PrevError) ->
    HrError =
        case Error of
            {error, undefined} -> <<"Unknown reason">>;
            {error, Reason} -> emqx_utils:readable_error_msg(Reason);
            Error -> emqx_utils:readable_error_msg(Error)
        end,
    emqx_alarm:safe_activate(
        ResId,
        #{resource_id => ResId, reason => resource_down},
        <<"resource down: ", HrError/binary>>
    ),
    ?tp(resource_activate_alarm, #{resource_id => ResId}).

maybe_resume_resource_workers(ResId, connected) ->
    lists:foreach(
        fun emqx_resource_buffer_worker:resume/1,
        emqx_resource_buffer_worker_sup:worker_pids(ResId)
    );
maybe_resume_resource_workers(_, _) ->
    ok.

maybe_clear_alarm(<<?TEST_ID_PREFIX, _/binary>>) ->
    ok;
maybe_clear_alarm(ResId) ->
    emqx_alarm:safe_deactivate(ResId).

parse_health_check_result(Status, Data) when ?IS_STATUS(Status) ->
    {Status, Data#data.state, status_to_error(Status)};
parse_health_check_result({Status, NewState}, _Data) when ?IS_STATUS(Status) ->
    {Status, NewState, status_to_error(Status)};
parse_health_check_result({Status, NewState, Error}, _Data) when ?IS_STATUS(Status) ->
    {Status, NewState, {error, Error}};
parse_health_check_result({error, Error}, Data) ->
    ?SLOG(
        error,
        #{
            msg => "health_check_exception",
            resource_id => Data#data.id,
            reason => Error
        }
    ),
    {disconnected, Data#data.state, {error, Error}}.

status_to_error(connected) ->
    undefined;
status_to_error(_) ->
    {error, undefined}.

%% Compatibility
external_error({error, Reason}) -> Reason;
external_error(Other) -> Other.

maybe_reply(Actions, undefined, _Reply) ->
    Actions;
maybe_reply(Actions, From, Reply) ->
    [{reply, From, Reply} | Actions].

-spec data_record_to_external_map(data()) -> resource_data().
data_record_to_external_map(Data) ->
    #{
        id => Data#data.id,
        error => external_error(Data#data.error),
        mod => Data#data.mod,
        callback_mode => Data#data.callback_mode,
        query_mode => Data#data.query_mode,
        config => Data#data.config,
        status => Data#data.status,
        state => Data#data.state,
        added_channels => Data#data.added_channels
    }.

-spec wait_for_ready(resource_id(), integer()) -> ok | timeout | {error, term()}.
wait_for_ready(ResId, WaitTime) ->
    do_wait_for_ready(ResId, WaitTime div ?WAIT_FOR_RESOURCE_DELAY).

do_wait_for_ready(_ResId, 0) ->
    timeout;
do_wait_for_ready(ResId, Retry) ->
    case try_read_cache(ResId) of
        #data{status = connected} ->
            ok;
        #data{status = disconnected, error = Err} ->
            {error, external_error(Err)};
        _ ->
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
            {error, timeout}
    end.
