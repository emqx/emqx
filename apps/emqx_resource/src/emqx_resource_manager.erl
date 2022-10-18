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
-module(emqx_resource_manager).
-behaviour(gen_statem).

-include("emqx_resource.hrl").
-include("emqx_resource_utils.hrl").
-include_lib("emqx/include/logger.hrl").

% API
-export([
    ensure_resource/5,
    recreate/4,
    remove/1,
    create_dry_run/2,
    restart/2,
    start/2,
    stop/1,
    health_check/1
]).

-export([
    lookup/1,
    list_all/0,
    list_group/1,
    ets_lookup/1,
    get_metrics/1,
    reset_metrics/1
]).

-export([
    set_resource_status_connecting/1,
    manager_id_to_resource_id/1
]).

% Server
-export([start_link/6]).

% Behaviour
-export([init/1, callback_mode/0, handle_event/4, terminate/3]).

% State record
-record(data, {
    id, manager_id, group, mod, callback_mode, query_mode, config, opts, status, state, error
}).
-type data() :: #data{}.

-define(ETS_TABLE, ?MODULE).
-define(WAIT_FOR_RESOURCE_DELAY, 100).
-define(T_OPERATION, 5000).
-define(T_LOOKUP, 1000).

-define(IS_STATUS(ST), ST =:= connecting; ST =:= connected; ST =:= disconnected).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

make_manager_id(ResId) ->
    emqx_resource:generate_id(ResId).

manager_id_to_resource_id(MgrId) ->
    [ResId, _Index] = string:split(MgrId, ":", trailing),
    ResId.

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
            MgrId = set_new_owner(ResId),
            create_and_return_data(MgrId, ResId, Group, ResourceType, Config, Opts)
    end.

%% @doc Called from emqx_resource when recreating a resource which may or may not exist
-spec recreate(resource_id(), resource_type(), resource_config(), creation_opts()) ->
    {ok, resource_data()} | {error, not_found} | {error, updating_to_incorrect_resource_type}.
recreate(ResId, ResourceType, NewConfig, Opts) ->
    case lookup(ResId) of
        {ok, Group, #{mod := ResourceType, status := _} = _Data} ->
            _ = remove(ResId, false),
            MgrId = set_new_owner(ResId),
            create_and_return_data(MgrId, ResId, Group, ResourceType, NewConfig, Opts);
        {ok, _, #{mod := Mod}} when Mod =/= ResourceType ->
            {error, updating_to_incorrect_resource_type};
        {error, not_found} ->
            {error, not_found}
    end.

create_and_return_data(MgrId, ResId, Group, ResourceType, Config, Opts) ->
    create(MgrId, ResId, Group, ResourceType, Config, Opts),
    {ok, _Group, Data} = lookup(ResId),
    {ok, Data}.

%% internal configs
-define(START_AFTER_CREATED, true).
%% in milliseconds
-define(START_TIMEOUT, 5000).

%% @doc Create a resource_manager and wait until it is running
create(MgrId, ResId, Group, ResourceType, Config, Opts) ->
    % The state machine will make the actual call to the callback/resource module after init
    ok = emqx_resource_manager_sup:ensure_child(MgrId, ResId, Group, ResourceType, Config, Opts),
    ok = emqx_metrics_worker:create_metrics(
        ?RES_METRICS,
        ResId,
        [
            'matched',
            'retried',
            'retried.success',
            'retried.failed',
            'success',
            'failed',
            'dropped',
            'dropped.queue_not_enabled',
            'dropped.queue_full',
            'dropped.resource_not_found',
            'dropped.resource_stopped',
            'dropped.other',
            'queuing',
            'batching',
            'inflight',
            'received'
        ],
        [matched]
    ),
    case emqx_resource:is_buffer_supported(ResourceType) of
        true ->
            %% the resource it self supports
            %% buffer, so there is no need for resource workers
            ok;
        false ->
            ok = emqx_resource_worker_sup:start_workers(ResId, Opts),
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
    MgrId = set_new_owner(ResId),
    ok = emqx_resource_manager_sup:ensure_child(
        MgrId, ResId, <<"dry_run">>, ResourceType, Config, #{}
    ),
    case wait_for_ready(ResId, 15000) of
        ok ->
            remove(ResId);
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
    safe_call(ResId, {remove, ClearMetrics}, ?T_OPERATION).

%% @doc Stops and then starts an instance that was already running
-spec restart(resource_id(), creation_opts()) -> ok | {error, Reason :: term()}.
restart(ResId, Opts) when is_binary(ResId) ->
    case safe_call(ResId, restart, ?T_OPERATION) of
        ok ->
            wait_for_ready(ResId, maps:get(start_timeout, Opts, 5000)),
            ok;
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Start the resource
-spec start(resource_id(), creation_opts()) -> ok | {error, Reason :: term()}.
start(ResId, Opts) ->
    case safe_call(ResId, start, ?T_OPERATION) of
        ok ->
            wait_for_ready(ResId, maps:get(start_timeout, Opts, 5000)),
            ok;
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
        {error, timeout} -> ets_lookup(ResId);
        Result -> Result
    end.

%% @doc Lookup the group and data of a resource
-spec ets_lookup(resource_id()) -> {ok, resource_group(), resource_data()} | {error, not_found}.
ets_lookup(ResId) ->
    case read_cache(ResId) of
        {Group, Data} ->
            {ok, Group, data_record_to_external_map_with_metrics(Data)};
        not_found ->
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
-spec list_all() -> [resource_data()] | [].
list_all() ->
    try
        [
            data_record_to_external_map_with_metrics(Data)
         || {_Id, _Group, Data} <- ets:tab2list(?ETS_TABLE)
        ]
    catch
        error:badarg -> []
    end.

%% @doc Returns a list of ids for all the resources in a group
-spec list_group(resource_group()) -> [resource_id()].
list_group(Group) ->
    List = ets:match(?ETS_TABLE, {'$1', Group, '_'}),
    lists:flatten(List).

-spec health_check(resource_id()) -> {ok, resource_status()} | {error, term()}.
health_check(ResId) ->
    safe_call(ResId, health_check, ?T_OPERATION).

%% Server start/stop callbacks

%% @doc Function called from the supervisor to actually start the server
start_link(MgrId, ResId, Group, ResourceType, Config, Opts) ->
    Data = #data{
        id = ResId,
        manager_id = MgrId,
        group = Group,
        mod = ResourceType,
        callback_mode = emqx_resource:get_callback_mode(ResourceType),
        %% query_mode = dynamic | sync | async
        %% TODO:
        %%  dynamic mode is async mode when things are going well, but becomes sync mode
        %%  if the resource worker is overloaded
        query_mode = maps:get(query_mode, Opts, sync),
        config = Config,
        opts = Opts,
        status = connecting,
        state = undefined,
        error = undefined
    },
    Module = atom_to_binary(?MODULE),
    ProcName = binary_to_atom(<<Module/binary, "_", MgrId/binary>>, utf8),
    gen_statem:start_link({local, ProcName}, ?MODULE, {Data, Opts}, []).

init({Data, Opts}) ->
    process_flag(trap_exit, true),
    %% init the cache so that lookup/1 will always return something
    insert_cache(Data#data.id, Data#data.group, Data),
    case maps:get(start_after_created, Opts, true) of
        true -> {ok, connecting, Data, {next_event, internal, start_resource}};
        false -> {ok, stopped, Data}
    end.

terminate(_Reason, _State, Data) ->
    _ = maybe_clear_alarm(Data#data.id),
    delete_cache(Data#data.id, Data#data.manager_id),
    ok.

%% Behavior callback

callback_mode() -> [handle_event_function, state_enter].

%% Common event Function

% Called during testing to force a specific state
handle_event({call, From}, set_resource_status_connecting, _State, Data) ->
    {next_state, connecting, Data#data{status = connecting}, [{reply, From, ok}]};
% Called when the resource is to be restarted
handle_event({call, From}, restart, _State, Data) ->
    _ = stop_resource(Data),
    start_resource(Data, From);
% Called when the resource is to be started
handle_event({call, From}, start, stopped, Data) ->
    start_resource(Data, From);
handle_event({call, From}, start, _State, _Data) ->
    {keep_state_and_data, [{reply, From, ok}]};
% Called when the resource received a `quit` message
handle_event(info, quit, stopped, _Data) ->
    {stop, {shutdown, quit}};
handle_event(info, quit, _State, Data) ->
    _ = stop_resource(Data),
    {stop, {shutdown, quit}};
% Called when the resource is to be stopped
handle_event({call, From}, stop, stopped, _Data) ->
    {keep_state_and_data, [{reply, From, ok}]};
handle_event({call, From}, stop, _State, Data) ->
    Result = stop_resource(Data),
    {next_state, stopped, Data, [{reply, From, Result}]};
% Called when a resource is to be stopped and removed.
handle_event({call, From}, {remove, ClearMetrics}, _State, Data) ->
    handle_remove_event(From, ClearMetrics, Data);
% Called when the state-data of the resource is being looked up.
handle_event({call, From}, lookup, _State, #data{group = Group} = Data) ->
    Reply = {ok, Group, data_record_to_external_map_with_metrics(Data)},
    {keep_state_and_data, [{reply, From, Reply}]};
% Called when doing a manually health check.
handle_event({call, From}, health_check, stopped, _Data) ->
    Actions = [{reply, From, {error, resource_is_stopped}}],
    {keep_state_and_data, Actions};
handle_event({call, From}, health_check, _State, Data) ->
    handle_manually_health_check(From, Data);
% State: CONNECTING
handle_event(enter, _OldState, connecting, Data) ->
    UpdatedData = Data#data{status = connecting},
    insert_cache(Data#data.id, Data#data.group, Data),
    Actions = [{state_timeout, 0, health_check}],
    {keep_state, UpdatedData, Actions};
handle_event(internal, start_resource, connecting, Data) ->
    start_resource(Data, undefined);
handle_event(state_timeout, health_check, connecting, Data) ->
    handle_connecting_health_check(Data);
%% State: CONNECTED
%% The connected state is entered after a successful on_start/2 of the callback mod
%% and successful health_checks
handle_event(enter, _OldState, connected, Data) ->
    UpdatedData = Data#data{status = connected},
    insert_cache(Data#data.id, Data#data.group, UpdatedData),
    _ = emqx_alarm:deactivate(Data#data.id),
    Actions = [{state_timeout, health_check_interval(Data#data.opts), health_check}],
    {next_state, connected, UpdatedData, Actions};
handle_event(state_timeout, health_check, connected, Data) ->
    handle_connected_health_check(Data);
%% State: DISCONNECTED
handle_event(enter, _OldState, disconnected, Data) ->
    UpdatedData = Data#data{status = disconnected},
    insert_cache(Data#data.id, Data#data.group, UpdatedData),
    handle_disconnected_state_enter(UpdatedData);
handle_event(state_timeout, auto_retry, disconnected, Data) ->
    start_resource(Data, undefined);
%% State: STOPPED
%% The stopped state is entered after the resource has been explicitly stopped
handle_event(enter, _OldState, stopped, Data) ->
    UpdatedData = Data#data{status = stopped},
    insert_cache(Data#data.id, Data#data.group, UpdatedData),
    {next_state, stopped, UpdatedData};
% Ignore all other events
handle_event(EventType, EventData, State, Data) ->
    ?SLOG(
        error,
        #{
            msg => ignore_all_other_events,
            event_type => EventType,
            event_data => EventData,
            state => State,
            data => Data
        }
    ),
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% internal functions
%%------------------------------------------------------------------------------
insert_cache(ResId, Group, Data = #data{manager_id = MgrId}) ->
    case get_owner(ResId) of
        not_found ->
            ets:insert(?ETS_TABLE, {ResId, Group, Data});
        MgrId ->
            ets:insert(?ETS_TABLE, {ResId, Group, Data});
        _ ->
            ?SLOG(error, #{
                msg => get_resource_owner_failed,
                resource_id => ResId,
                action => quit_resource
            }),
            self() ! quit
    end.

read_cache(ResId) ->
    case ets:lookup(?ETS_TABLE, ResId) of
        [{_Id, Group, Data}] -> {Group, Data};
        [] -> not_found
    end.

delete_cache(ResId, MgrId) ->
    case get_owner(ResId) of
        MgrIdNow when MgrIdNow == not_found; MgrIdNow == MgrId ->
            do_delete_cache(ResId);
        _ ->
            ok
    end.

do_delete_cache(<<?TEST_ID_PREFIX, _/binary>> = ResId) ->
    ets:delete(?ETS_TABLE, {owner, ResId}),
    ets:delete(?ETS_TABLE, ResId);
do_delete_cache(ResId) ->
    ets:delete(?ETS_TABLE, ResId).

set_new_owner(ResId) ->
    MgrId = make_manager_id(ResId),
    ok = set_owner(ResId, MgrId),
    MgrId.

set_owner(ResId, MgrId) ->
    ets:insert(?ETS_TABLE, {{owner, ResId}, MgrId}),
    ok.

get_owner(ResId) ->
    case ets:lookup(?ETS_TABLE, {owner, ResId}) of
        [{_, MgrId}] -> MgrId;
        [] -> not_found
    end.

handle_disconnected_state_enter(Data) ->
    {next_state, disconnected, Data, retry_actions(Data)}.

retry_actions(Data) ->
    case maps:get(auto_restart_interval, Data#data.opts, ?AUTO_RESTART_INTERVAL) of
        undefined ->
            [];
        RetryInterval ->
            [{state_timeout, RetryInterval, auto_retry}]
    end.

handle_remove_event(From, ClearMetrics, Data) ->
    stop_resource(Data),
    ok = emqx_resource_worker_sup:stop_workers(Data#data.id, Data#data.opts),
    case ClearMetrics of
        true -> ok = emqx_metrics_worker:clear_metrics(?RES_METRICS, Data#data.id);
        false -> ok
    end,
    {stop_and_reply, normal, [{reply, From, ok}]}.

start_resource(Data, From) ->
    %% in case the emqx_resource:call_start/2 hangs, the lookup/1 can read status from the cache
    insert_cache(Data#data.id, Data#data.group, Data),
    case emqx_resource:call_start(Data#data.manager_id, Data#data.mod, Data#data.config) of
        {ok, ResourceState} ->
            UpdatedData = Data#data{state = ResourceState, status = connecting},
            %% Perform an initial health_check immediately before transitioning into a connected state
            Actions = maybe_reply([{state_timeout, 0, health_check}], From, ok),
            {next_state, connecting, UpdatedData, Actions};
        {error, Reason} = Err ->
            ?SLOG(error, #{
                msg => start_resource_failed,
                id => Data#data.id,
                reason => Reason
            }),
            _ = maybe_alarm(disconnected, Data#data.id),
            %% Keep track of the error reason why the connection did not work
            %% so that the Reason can be returned when the verification call is made.
            UpdatedData = Data#data{error = Reason},
            Actions = maybe_reply(retry_actions(UpdatedData), From, Err),
            {next_state, disconnected, UpdatedData, Actions}
    end.

stop_resource(#data{state = undefined, id = ResId} = _Data) ->
    _ = maybe_clear_alarm(ResId),
    ok = emqx_metrics_worker:reset_metrics(?RES_METRICS, ResId),
    ok;
stop_resource(Data) ->
    %% We don't care the return value of the Mod:on_stop/2.
    %% The callback mod should make sure the resource is stopped after on_stop/2
    %% is returned.
    ResId = Data#data.id,
    _ = emqx_resource:call_stop(Data#data.manager_id, Data#data.mod, Data#data.state),
    _ = maybe_clear_alarm(ResId),
    ok = emqx_metrics_worker:reset_metrics(?RES_METRICS, ResId),
    ok.

make_test_id() ->
    RandId = iolist_to_binary(emqx_misc:gen_id(16)),
    <<?TEST_ID_PREFIX, RandId/binary>>.

handle_manually_health_check(From, Data) ->
    with_health_check(Data, fun(Status, UpdatedData) ->
        Actions = [{reply, From, {ok, Status}}],
        {next_state, Status, UpdatedData, Actions}
    end).

handle_connecting_health_check(Data) ->
    with_health_check(
        Data,
        fun
            (connected, UpdatedData) ->
                {next_state, connected, UpdatedData};
            (connecting, UpdatedData) ->
                Actions = [{state_timeout, health_check_interval(Data#data.opts), health_check}],
                {keep_state, UpdatedData, Actions};
            (disconnected, UpdatedData) ->
                {next_state, disconnected, UpdatedData}
        end
    ).

handle_connected_health_check(Data) ->
    with_health_check(
        Data,
        fun
            (connected, UpdatedData) ->
                Actions = [{state_timeout, health_check_interval(Data#data.opts), health_check}],
                {keep_state, UpdatedData, Actions};
            (Status, UpdatedData) ->
                ?SLOG(error, #{
                    msg => health_check_failed,
                    id => Data#data.id,
                    status => Status
                }),
                {next_state, Status, UpdatedData}
        end
    ).

with_health_check(Data, Func) ->
    ResId = Data#data.id,
    HCRes = emqx_resource:call_health_check(Data#data.manager_id, Data#data.mod, Data#data.state),
    {Status, NewState, Err} = parse_health_check_result(HCRes, Data),
    _ = maybe_alarm(Status, ResId),
    ok = maybe_resume_resource_workers(Status),
    UpdatedData = Data#data{
        state = NewState, status = Status, error = Err
    },
    insert_cache(ResId, UpdatedData#data.group, UpdatedData),
    Func(Status, UpdatedData).

health_check_interval(Opts) ->
    maps:get(health_check_interval, Opts, ?HEALTHCHECK_INTERVAL).

maybe_alarm(connected, _ResId) ->
    ok;
maybe_alarm(_Status, <<?TEST_ID_PREFIX, _/binary>>) ->
    ok;
maybe_alarm(_Status, ResId) ->
    emqx_alarm:activate(
        ResId,
        #{resource_id => ResId, reason => resource_down},
        <<"resource down: ", ResId/binary>>
    ).

maybe_resume_resource_workers(connected) ->
    lists:foreach(
        fun({_, Pid, _, _}) ->
            emqx_resource_worker:resume(Pid)
        end,
        supervisor:which_children(emqx_resource_worker_sup)
    );
maybe_resume_resource_workers(_) ->
    ok.

maybe_clear_alarm(<<?TEST_ID_PREFIX, _/binary>>) ->
    ok;
maybe_clear_alarm(ResId) ->
    emqx_alarm:deactivate(ResId).

parse_health_check_result(Status, Data) when ?IS_STATUS(Status) ->
    {Status, Data#data.state, undefined};
parse_health_check_result({Status, NewState}, _Data) when ?IS_STATUS(Status) ->
    {Status, NewState, undefined};
parse_health_check_result({Status, NewState, Error}, _Data) when ?IS_STATUS(Status) ->
    {Status, NewState, Error};
parse_health_check_result({error, Error}, Data) ->
    ?SLOG(
        error,
        #{
            msg => health_check_exception,
            resource_id => Data#data.id,
            reason => Error
        }
    ),
    {disconnected, Data#data.state, Error}.

maybe_reply(Actions, undefined, _Reply) ->
    Actions;
maybe_reply(Actions, From, Reply) ->
    [{reply, From, Reply} | Actions].

-spec data_record_to_external_map_with_metrics(data()) -> resource_data().
data_record_to_external_map_with_metrics(Data) ->
    #{
        id => Data#data.id,
        mod => Data#data.mod,
        callback_mode => Data#data.callback_mode,
        query_mode => Data#data.query_mode,
        config => Data#data.config,
        status => Data#data.status,
        state => Data#data.state,
        metrics => get_metrics(Data#data.id)
    }.

-spec wait_for_ready(resource_id(), integer()) -> ok | timeout.
wait_for_ready(ResId, WaitTime) ->
    do_wait_for_ready(ResId, WaitTime div ?WAIT_FOR_RESOURCE_DELAY).

do_wait_for_ready(_ResId, 0) ->
    timeout;
do_wait_for_ready(ResId, Retry) ->
    case ets_lookup(ResId) of
        {ok, _Group, #{status := connected}} ->
            ok;
        _ ->
            timer:sleep(?WAIT_FOR_RESOURCE_DELAY),
            do_wait_for_ready(ResId, Retry - 1)
    end.

safe_call(ResId, Message, Timeout) ->
    try
        Module = atom_to_binary(?MODULE),
        MgrId = get_owner(ResId),
        ProcName = binary_to_existing_atom(<<Module/binary, "_", MgrId/binary>>, utf8),
        gen_statem:call(ProcName, Message, {clean_timeout, Timeout})
    catch
        error:badarg ->
            {error, not_found};
        exit:{R, _} when R == noproc; R == normal; R == shutdown ->
            {error, not_found};
        exit:{timeout, _} ->
            {error, timeout}
    end.
