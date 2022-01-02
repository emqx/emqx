%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start_link/2]).

%% load resource instances from *.conf files
-export([ lookup/1
        , get_metrics/1
        , list_all/0
        , make_test_id/0
        ]).

-export([ hash_call/2
        , hash_call/3
        ]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {worker_pool, worker_id}).

-type state() :: #state{}.

%%------------------------------------------------------------------------------
%% Start the registry
%%------------------------------------------------------------------------------

start_link(Pool, Id) ->
    gen_server:start_link({local, proc_name(?MODULE, Id)},
                          ?MODULE, {Pool, Id}, []).

%% call the worker by the hash of resource-instance-id, to make sure we always handle
%% operations on the same instance in the same worker.
hash_call(InstId, Request) ->
    hash_call(InstId, Request, infinity).

hash_call(InstId, Request, Timeout) ->
    gen_server:call(pick(InstId), Request, Timeout).

-spec lookup(instance_id()) -> {ok, resource_data()} | {error, not_found}.
lookup(InstId) ->
    case ets:lookup(emqx_resource_instance, InstId) of
        [] -> {error, not_found};
        [{_, Data}] ->
            {ok, Data#{id => InstId, metrics => get_metrics(InstId)}}
    end.

make_test_id() ->
    RandId = iolist_to_binary(emqx_misc:gen_id(16)),
    <<?TEST_ID_PREFIX, RandId/binary>>.

get_metrics(InstId) ->
    emqx_plugin_libs_metrics:get_metrics(resource_metrics, InstId).

force_lookup(InstId) ->
    {ok, Data} = lookup(InstId),
    Data.

-spec list_all() -> [resource_data()].
list_all() ->
    try
        [Data#{id => Id} || {Id, Data} <- ets:tab2list(emqx_resource_instance)]
    catch
        error:badarg -> []
    end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

-spec init({atom(), integer()}) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate | {continue, term()}} |
    {stop, Reason :: term()} | ignore.
init({Pool, Id}) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #state{worker_pool = Pool, worker_id = Id}}.

handle_call({create, InstId, ResourceType, Config, Opts}, _From, State) ->
    {reply, do_create(InstId, ResourceType, Config, Opts), State};

handle_call({create_dry_run, ResourceType, Config}, _From, State) ->
    {reply, do_create_dry_run(ResourceType, Config), State};

handle_call({recreate, InstId, ResourceType, Config, Opts}, _From, State) ->
    {reply, do_recreate(InstId, ResourceType, Config, Opts), State};

handle_call({remove, InstId}, _From, State) ->
    {reply, do_remove(InstId), State};

handle_call({restart, InstId, Opts}, _From, State) ->
    {reply, do_restart(InstId, Opts), State};

handle_call({stop, InstId}, _From, State) ->
    {reply, do_stop(InstId), State};

handle_call({health_check, InstId}, _From, State) ->
    {reply, do_health_check(InstId), State};

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
-dialyzer({nowarn_function, [do_recreate/4,
                             do_create/4,
                             do_restart/2,
                             do_stop/1,
                             do_health_check/1]}).

do_recreate(InstId, ResourceType, NewConfig, Opts) ->
    case lookup(InstId) of
        {ok, #{mod := ResourceType, status := started} = Data} ->
            %% If this resource is in use (status='started'), we should make sure
            %% the new config is OK before removing the old one.
            case do_create_dry_run(ResourceType, NewConfig) of
                ok ->
                    do_remove(Data, false),
                    do_create(InstId, ResourceType, NewConfig, Opts);
                Error ->
                    Error
            end;
        {ok, #{mod := ResourceType, status := _} = Data} ->
            do_remove(Data, false),
            do_create(InstId, ResourceType, NewConfig, Opts);
        {ok, #{mod := Mod}} when Mod =/= ResourceType ->
            {error, updating_to_incorrect_resource_type};
        {error, not_found} ->
            {error, not_found}
    end.

do_create(InstId, ResourceType, Config, Opts) ->
    case lookup(InstId) of
        {ok, _} ->
            {ok, already_created};
        {error, not_found} ->
            case do_start(InstId, ResourceType, Config, Opts) of
                ok ->
                    ok = emqx_plugin_libs_metrics:clear_metrics(resource_metrics, InstId),
                    {ok, force_lookup(InstId)};
                Error ->
                    Error
            end
    end.

do_create_dry_run(ResourceType, Config) ->
    InstId = make_test_id(),
    Opts = #{async_create => false},
    case do_create(InstId, ResourceType, Config, Opts) of
        {ok, Data} ->
            Return = do_health_check(Data),
            _ = do_remove(Data),
            Return;
        {error, Reason} ->
            {error, Reason}
    end.

do_remove(Instance) ->
    do_remove(Instance, true).

do_remove(InstId, ClearMetrics) when is_binary(InstId) ->
    do_with_instance_data(InstId, fun do_remove/2, [ClearMetrics]);
do_remove(#{id := InstId} = Data, ClearMetrics) ->
    _ = do_stop(Data),
    ets:delete(emqx_resource_instance, InstId),
    case ClearMetrics of
        true -> ok = emqx_plugin_libs_metrics:clear_metrics(resource_metrics, InstId);
        false -> ok
    end,
    ok.

do_restart(InstId, Opts) ->
    case lookup(InstId) of
        {ok, #{mod := ResourceType, config := Config} = Data} ->
            ok = do_stop(Data),
            do_start(InstId, ResourceType, Config, Opts);
        Error ->
            Error
    end.

do_start(InstId, ResourceType, Config, Opts) when is_binary(InstId) ->
    InitData = #{id => InstId, mod => ResourceType, config => Config,
                 status => starting, state => undefined},
    %% The `emqx_resource:call_start/3` need the instance exist beforehand
    ets:insert(emqx_resource_instance, {InstId, InitData}),
    case maps:get(async_create, Opts, false) of
        false ->
            start_and_check(InstId, ResourceType, Config, Opts, InitData);
        true ->
            spawn(fun() ->
                    start_and_check(InstId, ResourceType, Config, Opts, InitData)
                end),
            ok
    end.

start_and_check(InstId, ResourceType, Config, Opts, Data) ->
    case emqx_resource:call_start(InstId, ResourceType, Config) of
        {ok, ResourceState} ->
            Data2 = Data#{state => ResourceState},
            ets:insert(emqx_resource_instance, {InstId, Data2}),
            case maps:get(async_create, Opts, false) of
                false -> do_health_check(Data2);
                true -> emqx_resource_health_check_sup:create_checker(InstId,
                            maps:get(health_check_interval, Opts, 15000))
            end;
        {error, Reason} ->
            ets:insert(emqx_resource_instance, {InstId, Data#{status => stopped}}),
            {error, Reason}
    end.

do_stop(InstId) when is_binary(InstId) ->
    do_with_instance_data(InstId, fun do_stop/1, []);
do_stop(#{state := undefined}) ->
    ok;
do_stop(#{id := InstId, mod := Mod, state := ResourceState} = Data) ->
    _ = emqx_resource:call_stop(InstId, Mod, ResourceState),
    ok = emqx_resource_health_check_sup:delete_checker(InstId),
    ets:insert(emqx_resource_instance, {InstId, Data#{status => stopped}}),
    ok.

do_health_check(InstId) when is_binary(InstId) ->
    do_with_instance_data(InstId, fun do_health_check/1, []);
do_health_check(#{state := undefined}) ->
    {error, resource_not_initialized};
do_health_check(#{id := InstId, mod := Mod, state := ResourceState0} = Data) ->
    case emqx_resource:call_health_check(InstId, Mod, ResourceState0) of
        {ok, ResourceState1} ->
            ets:insert(emqx_resource_instance,
                {InstId, Data#{status => started, state => ResourceState1}}),
            ok;
        {error, Reason, ResourceState1} ->
            logger:error("health check for ~p failed: ~p", [InstId, Reason]),
            ets:insert(emqx_resource_instance,
                {InstId, Data#{status => stopped, state => ResourceState1}}),
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% internal functions
%%------------------------------------------------------------------------------

do_with_instance_data(InstId, Do, Args) ->
    case lookup(InstId) of
        {ok, Data} -> erlang:apply(Do, [Data | Args]);
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
