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
-module(emqx_resource_instance).

-behaviour(gen_server).

-include("emqx_resource.hrl").
-include("emqx_resource_utils.hrl").
-include_lib("emqx/include/logger.hrl").

-export([start_link/2]).

%% load resource instances from *.conf files
-export([
    lookup/1,
    get_metrics/1,
    reset_metrics/1,
    list_all/0,
    list_group/1,
    set_resource_status/2
]).

-export([
    hash_call/2,
    hash_call/3
]).

%% gen_server Callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {worker_pool, worker_id}).

-type state() :: #state{}.

%%------------------------------------------------------------------------------
%% Start the registry
%%------------------------------------------------------------------------------

start_link(Pool, Id) ->
    gen_server:start_link(
        {local, proc_name(?MODULE, Id)},
        ?MODULE,
        {Pool, Id},
        []
    ).

%% call the worker by the hash of resource-instance-id, to make sure we always handle
%% operations on the same instance in the same worker.
hash_call(InstId, Request) ->
    hash_call(InstId, Request, infinity).

hash_call(InstId, Request, Timeout) ->
    gen_server:call(pick(InstId), Request, Timeout).

-spec lookup(instance_id()) -> {ok, resource_group(), resource_data()} | {error, not_found}.
lookup(InstId) ->
    case ets:lookup(emqx_resource_instance, InstId) of
        [] -> {error, not_found};
        [{_, Group, Data}] -> {ok, Group, Data#{id => InstId, metrics => get_metrics(InstId)}}
    end.

make_test_id() ->
    RandId = iolist_to_binary(emqx_misc:gen_id(16)),
    <<?TEST_ID_PREFIX, RandId/binary>>.

get_metrics(InstId) ->
    emqx_metrics_worker:get_metrics(resource_metrics, InstId).

reset_metrics(InstId) ->
    emqx_metrics_worker:reset_metrics(resource_metrics, InstId).

force_lookup(InstId) ->
    {ok, _Group, Data} = lookup(InstId),
    Data.

-spec list_all() -> [resource_data()].
list_all() ->
    try
        [Data#{id => Id} || {Id, _Group, Data} <- ets:tab2list(emqx_resource_instance)]
    catch
        error:badarg -> []
    end.

-spec list_group(resource_group()) -> [instance_id()].
list_group(Group) ->
    List = ets:match(emqx_resource_instance, {'$1', Group, '_'}),
    lists:map(fun([A | _]) -> A end, List).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

-spec init({atom(), integer()}) ->
    {ok, State :: state()}
    | {ok, State :: state(), timeout() | hibernate | {continue, term()}}
    | {stop, Reason :: term()}
    | ignore.
init({Pool, Id}) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #state{worker_pool = Pool, worker_id = Id}}.

handle_call({create, InstId, Group, ResourceType, Config, Opts}, _From, State) ->
    {reply, do_create(InstId, Group, ResourceType, Config, Opts), State};
handle_call({create_dry_run, ResourceType, Config}, _From, State) ->
    {reply, do_create_dry_run(ResourceType, Config), State};
handle_call({recreate, InstId, ResourceType, Config, Opts}, _From, State) ->
    {reply, do_recreate(InstId, ResourceType, Config, Opts), State};
handle_call({reset_metrics, InstId}, _From, State) ->
    {reply, do_reset_metrics(InstId), State};
handle_call({remove, InstId}, _From, State) ->
    {reply, do_remove(InstId), State};
handle_call({restart, InstId, Opts}, _From, State) ->
    {reply, do_restart(InstId, Opts), State};
handle_call({stop, InstId}, _From, State) ->
    {reply, do_stop(InstId), State};
handle_call({health_check, InstId}, _From, State) ->
    {reply, do_health_check(InstId), State};
handle_call({set_resource_status_connecting, InstId}, _From, State) ->
    {reply, do_set_resource_status_connecting(InstId), State};
handle_call(Req, _From, State) ->
    logger:error("Received unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{worker_pool = Pool, worker_id = Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------

%% suppress the race condition check, as these functions are protected in gproc workers
-dialyzer(
    {nowarn_function, [
        do_recreate/4,
        do_create/5,
        do_restart/2,
        do_start/5,
        do_stop/1,
        do_health_check/1,
        start_and_check/6
    ]}
).

do_recreate(InstId, ResourceType, NewConfig, Opts) ->
    case lookup(InstId) of
        %% We recreate the resource no matter if it is connected and in use!
        %% As we can not know if the resource is "really disconnected" or we mark the status
        %% to "disconnected" because the emqx_resource_instance process is not responding.
        {ok, Group, #{mod := ResourceType, status := _} = Data} ->
            do_remove(Group, Data, false),
            do_create(InstId, Group, ResourceType, NewConfig, Opts);
        {ok, _, #{mod := Mod}} when Mod =/= ResourceType ->
            {error, updating_to_incorrect_resource_type};
        {error, not_found} ->
            {error, not_found}
    end.

wait_for_resource_ready(InstId, WaitTime) ->
    do_wait_for_resource_ready(InstId, WaitTime div 100).

do_wait_for_resource_ready(_InstId, 0) ->
    timeout;
do_wait_for_resource_ready(InstId, Retry) ->
    case force_lookup(InstId) of
        #{status := connected} ->
            ok;
        _ ->
            timer:sleep(100),
            do_wait_for_resource_ready(InstId, Retry - 1)
    end.

do_create(InstId, Group, ResourceType, Config, Opts) ->
    case lookup(InstId) of
        {ok, _, _} ->
            {ok, already_created};
        {error, not_found} ->
            ok = do_start(InstId, Group, ResourceType, Config, Opts),
            ok = emqx_metrics_worker:create_metrics(
                resource_metrics,
                InstId,
                [matched, success, failed, exception],
                [matched]
            ),
            {ok, force_lookup(InstId)}
    end.

do_create_dry_run(ResourceType, Config) ->
    InstId = make_test_id(),
    case emqx_resource:call_start(InstId, ResourceType, Config) of
        {ok, ResourceState} ->
            Health =
                case emqx_resource:call_health_check(InstId, ResourceType, ResourceState) of
                    connected ->
                        ok;
                    {connected, _N} ->
                        ok;
                    ConnectStatus ->
                        {error, ConnectStatus}
                end,
            case emqx_resource:call_stop(InstId, ResourceType, ResourceState) of
                {error, _} = Error -> Error;
                _ -> Health
            end;
        {error, Reason} ->
            {error, Reason}
    end.

do_reset_metrics(Instance) ->
    reset_metrics(Instance).

do_remove(Instance) ->
    do_remove(Instance, true).

do_remove(InstId, ClearMetrics) when is_binary(InstId) ->
    do_with_group_and_instance_data(InstId, fun do_remove/3, [ClearMetrics]).

do_remove(Group, #{id := InstId} = Data, ClearMetrics) ->
    _ = do_stop(Group, Data),
    ets:delete(emqx_resource_instance, InstId),
    case ClearMetrics of
        true -> ok = emqx_metrics_worker:clear_metrics(resource_metrics, InstId);
        false -> ok
    end,
    ok.

do_restart(InstId, Opts) ->
    case lookup(InstId) of
        {ok, Group, #{mod := ResourceType, config := Config} = Data} ->
            ok = do_stop(Group, Data),
            do_start(InstId, Group, ResourceType, Config, Opts);
        Error ->
            Error
    end.

do_start(InstId, Group, ResourceType, Config, Opts) when is_binary(InstId) ->
    InitData = #{
        id => InstId,
        mod => ResourceType,
        config => Config,
        status => connecting,
        state => undefined
    },
    %% The `emqx_resource:call_start/3` need the instance exist beforehand
    update_resource(InstId, Group, InitData),
    spawn(fun() ->
        start_and_check(InstId, Group, ResourceType, Config, Opts, InitData)
    end),
    _ = wait_for_resource_ready(InstId, maps:get(wait_for_resource_ready, Opts, 5000)),
    ok.

start_and_check(InstId, Group, ResourceType, Config, Opts, Data) ->
    case emqx_resource:call_start(InstId, ResourceType, Config) of
        {ok, ResourceState} ->
            Data2 = Data#{state => ResourceState, status => connected},
            update_resource(InstId, Group, Data2),
            create_default_checker(InstId, Opts);
        {error, Reason} ->
            update_resource(InstId, Group, Data#{status => disconnected}),
            {error, Reason}
    end.

create_default_checker(InstId, Opts) ->
    emqx_resource_health_check:create_checker(
        InstId,
        maps:get(health_check_interval, Opts, 15000),
        maps:get(health_check_timeout, Opts, 10000)
    ).

do_stop(InstId) when is_binary(InstId) ->
    do_with_group_and_instance_data(InstId, fun do_stop/2, []).

do_stop(_Group, #{state := undefined}) ->
    ok;
do_stop(Group, #{id := InstId, mod := Mod, state := ResourceState} = Data) ->
    _ = emqx_resource:call_stop(InstId, Mod, ResourceState),
    _ = emqx_resource_health_check:delete_checker(InstId),
    update_resource(InstId, Group, Data#{status => disconnected}),
    ok.

do_health_check(InstId) when is_binary(InstId) ->
    do_with_group_and_instance_data(InstId, fun do_health_check/2, []).

do_health_check(_Group, #{state := undefined}) ->
    {error, resource_not_initialized};
do_health_check(
    Group,
    #{id := InstId, mod := Mod, state := ResourceState, status := OldStatus} = Data
) ->
    case emqx_resource:call_health_check(InstId, Mod, ResourceState) of
        {NewConnStatus, NewResourceState} ->
            NData = Data#{status => NewConnStatus, state => NewResourceState},
            update_resource(InstId, Group, NData),
            maybe_log_health_check_result(InstId, NewConnStatus);
        NewConnStatus ->
            NData = Data#{status => NewConnStatus},
            NewConnStatus /= OldStatus andalso update_resource(InstId, Group, NData),
            maybe_log_health_check_result(InstId, NewConnStatus)
    end.

maybe_log_health_check_result(InstId, Result) ->
    case Result of
        connected ->
            ok;
        ConnectStatus ->
            logger:error("health check for ~p failed: ~p", [InstId, ConnectStatus])
    end.

do_set_resource_status_connecting(InstId) ->
    case emqx_resource_instance:lookup(InstId) of
        {ok, Group, #{id := InstId} = Data} ->
            logger:error("health check for ~p failed: timeout", [InstId]),
            update_resource(InstId, Group, Data#{status => connecting});
        Error ->
            {error, Error}
    end.

-spec set_resource_status(instance_id(), resource_connection_status()) -> ok | {error, term()}.
set_resource_status(InstId, Status) ->
    case lookup(InstId) of
        {ok, Group, #{id := _} = Data} ->
            update_resource(InstId, Group, Data#{status => Status});
        Error ->
            ?SLOG(
                error,
                #{
                    msg => "set resource status field",
                    resource_id => InstId,
                    reason => Error
                }
            ),
            Error
    end.

%%------------------------------------------------------------------------------
%% internal functions
%%------------------------------------------------------------------------------

update_resource(InstId, Group, Data) ->
    ets:insert(emqx_resource_instance, {InstId, Group, Data}).

do_with_group_and_instance_data(InstId, Do, Args) ->
    case lookup(InstId) of
        {ok, Group, Data} -> erlang:apply(Do, [Group, Data | Args]);
        Error -> Error
    end.

proc_name(Mod, Id) ->
    list_to_atom(lists:concat([Mod, "_", Id])).

pick(InstId) ->
    Pid = gproc_pool:pick_worker(emqx_resource_instance, InstId),
    case is_pid(Pid) of
        true -> Pid;
        false -> error({failed_to_pick_worker, emqx_resource_instance, InstId})
    end.
