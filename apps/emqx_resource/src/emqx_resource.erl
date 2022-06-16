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

-module(emqx_resource).

-include("emqx_resource.hrl").
-include("emqx_resource_utils.hrl").

%% APIs for resource types

-export([list_types/0]).

%% APIs for behaviour implementations

-export([
    query_success/1,
    query_failed/1
]).

%% APIs for instances

-export([
    check_config/2,
    check_and_create/4,
    check_and_create/5,
    check_and_create_local/4,
    check_and_create_local/5,
    check_and_recreate/4,
    check_and_recreate_local/4
]).

%% Sync resource instances and files
%% provisional solution: rpc:multicall to all the nodes for creating/updating/removing
%% todo: replicate operations

%% store the config and start the instance
-export([
    create/4,
    create/5,
    create_local/4,
    create_local/5,
    %% run start/2, health_check/2 and stop/1 sequentially
    create_dry_run/2,
    create_dry_run_local/2,
    %% this will do create_dry_run, stop the old instance and start a new one
    recreate/3,
    recreate/4,
    recreate_local/3,
    recreate_local/4,
    %% remove the config and stop the instance
    remove/1,
    remove_local/1,
    reset_metrics/1,
    reset_metrics_local/1
]).

%% Calls to the callback module with current resource state
%% They also save the state after the call finished (except query/2,3).

-export([
    start/1,
    start/2,
    restart/1,
    restart/2,
    %% verify if the resource is working normally
    health_check/1,
    %% set resource status to disconnected
    set_resource_status_connecting/1,
    %% stop the instance
    stop/1,
    %% query the instance
    query/2,
    %% query the instance with after_query()
    query/3
]).

%% Direct calls to the callback module

%% start the instance
-export([
    call_start/3,
    %% verify if the resource is working normally
    call_health_check/3,
    %% stop the instance
    call_stop/3
]).

%% list all the instances, id only.
-export([
    list_instances/0,
    %% list all the instances
    list_instances_verbose/0,
    %% return the data of the instance
    get_instance/1,
    %% return all the instances of the same resource type
    list_instances_by_type/1,
    generate_id/1,
    list_group_instances/1
]).

-optional_callbacks([
    on_query/4,
    on_get_status/2
]).

%% when calling emqx_resource:start/1
-callback on_start(resource_id(), resource_config()) ->
    {ok, resource_state()} | {error, Reason :: term()}.

%% when calling emqx_resource:stop/1
-callback on_stop(resource_id(), resource_state()) -> term().

%% when calling emqx_resource:query/3
-callback on_query(resource_id(), Request :: term(), after_query(), resource_state()) -> term().

%% when calling emqx_resource:health_check/2
-callback on_get_status(resource_id(), resource_state()) ->
    resource_status()
    | {resource_status(), resource_state()}
    | {resource_status(), resource_state(), term()}.

-spec list_types() -> [module()].
list_types() ->
    discover_resource_mods().

-spec discover_resource_mods() -> [module()].
discover_resource_mods() ->
    [Mod || {Mod, _} <- code:all_loaded(), is_resource_mod(Mod)].

-spec is_resource_mod(module()) -> boolean().
is_resource_mod(Module) ->
    Info = Module:module_info(attributes),
    Behaviour =
        proplists:get_value(behavior, Info, []) ++
            proplists:get_value(behaviour, Info, []),
    lists:member(?MODULE, Behaviour).

-spec query_success(after_query()) -> ok.
query_success(undefined) -> ok;
query_success({OnSucc, _}) -> apply_query_after_calls(OnSucc).

-spec query_failed(after_query()) -> ok.
query_failed(undefined) -> ok;
query_failed({_, OnFailed}) -> apply_query_after_calls(OnFailed).

apply_query_after_calls(Funcs) ->
    lists:foreach(
        fun({Fun, Args}) ->
            safe_apply(Fun, Args)
        end,
        Funcs
    ).

%% =================================================================================
%% APIs for resource instances
%% =================================================================================
-spec create(resource_id(), resource_group(), resource_type(), resource_config()) ->
    {ok, resource_data() | 'already_created'} | {error, Reason :: term()}.
create(ResId, Group, ResourceType, Config) ->
    create(ResId, Group, ResourceType, Config, #{}).

-spec create(resource_id(), resource_group(), resource_type(), resource_config(), create_opts()) ->
    {ok, resource_data() | 'already_created'} | {error, Reason :: term()}.
create(ResId, Group, ResourceType, Config, Opts) ->
    emqx_resource_proto_v1:create(ResId, Group, ResourceType, Config, Opts).
% --------------------------------------------

-spec create_local(resource_id(), resource_group(), resource_type(), resource_config()) ->
    {ok, resource_data() | 'already_created'} | {error, Reason :: term()}.
create_local(ResId, Group, ResourceType, Config) ->
    create_local(ResId, Group, ResourceType, Config, #{}).

-spec create_local(
    resource_id(),
    resource_group(),
    resource_type(),
    resource_config(),
    create_opts()
) ->
    {ok, resource_data()}.
create_local(ResId, Group, ResourceType, Config, Opts) ->
    emqx_resource_manager:ensure_resource(ResId, Group, ResourceType, Config, Opts).

-spec create_dry_run(resource_type(), resource_config()) ->
    ok | {error, Reason :: term()}.
create_dry_run(ResourceType, Config) ->
    emqx_resource_proto_v1:create_dry_run(ResourceType, Config).

-spec create_dry_run_local(resource_type(), resource_config()) ->
    ok | {error, Reason :: term()}.
create_dry_run_local(ResourceType, Config) ->
    emqx_resource_manager:create_dry_run(ResourceType, Config).

-spec recreate(resource_id(), resource_type(), resource_config()) ->
    {ok, resource_data()} | {error, Reason :: term()}.
recreate(ResId, ResourceType, Config) ->
    recreate(ResId, ResourceType, Config, #{}).

-spec recreate(resource_id(), resource_type(), resource_config(), create_opts()) ->
    {ok, resource_data()} | {error, Reason :: term()}.
recreate(ResId, ResourceType, Config, Opts) ->
    emqx_resource_proto_v1:recreate(ResId, ResourceType, Config, Opts).

-spec recreate_local(resource_id(), resource_type(), resource_config()) ->
    {ok, resource_data()} | {error, Reason :: term()}.
recreate_local(ResId, ResourceType, Config) ->
    recreate_local(ResId, ResourceType, Config, #{}).

-spec recreate_local(resource_id(), resource_type(), resource_config(), create_opts()) ->
    {ok, resource_data()} | {error, Reason :: term()}.
recreate_local(ResId, ResourceType, Config, Opts) ->
    emqx_resource_manager:recreate(ResId, ResourceType, Config, Opts).

-spec remove(resource_id()) -> ok | {error, Reason :: term()}.
remove(ResId) ->
    emqx_resource_proto_v1:remove(ResId).

-spec remove_local(resource_id()) -> ok | {error, Reason :: term()}.
remove_local(ResId) ->
    emqx_resource_manager:remove(ResId).

-spec reset_metrics_local(resource_id()) -> ok.
reset_metrics_local(ResId) ->
    emqx_resource_manager:reset_metrics(ResId).

-spec reset_metrics(resource_id()) -> ok | {error, Reason :: term()}.
reset_metrics(ResId) ->
    emqx_resource_proto_v1:reset_metrics(ResId).

%% =================================================================================
-spec query(resource_id(), Request :: term()) -> Result :: term().
query(ResId, Request) ->
    query(ResId, Request, inc_metrics_funcs(ResId)).

%% same to above, also defines what to do when the Module:on_query success or failed
%% it is the duty of the Module to apply the `after_query()` functions.
-spec query(resource_id(), Request :: term(), after_query()) -> Result :: term().
query(ResId, Request, AfterQuery) ->
    case emqx_resource_manager:ets_lookup(ResId) of
        {ok, _Group, #{mod := Mod, state := ResourceState, status := connected}} ->
            %% the resource state is readonly to Module:on_query/4
            %% and the `after_query()` functions should be thread safe
            ok = emqx_metrics_worker:inc(resource_metrics, ResId, matched),
            try
                Mod:on_query(ResId, Request, AfterQuery, ResourceState)
            catch
                Err:Reason:ST ->
                    emqx_metrics_worker:inc(resource_metrics, ResId, exception),
                    erlang:raise(Err, Reason, ST)
            end;
        {ok, _Group, _Data} ->
            query_error(not_connected, <<"resource not connected">>);
        {error, not_found} ->
            query_error(not_found, <<"resource not found">>)
    end.

-spec start(resource_id()) -> ok | {error, Reason :: term()}.
start(ResId) ->
    start(ResId, #{}).

-spec start(resource_id(), create_opts()) -> ok | {error, Reason :: term()}.
start(ResId, Opts) ->
    emqx_resource_manager:start(ResId, Opts).

-spec restart(resource_id()) -> ok | {error, Reason :: term()}.
restart(ResId) ->
    restart(ResId, #{}).

-spec restart(resource_id(), create_opts()) -> ok | {error, Reason :: term()}.
restart(ResId, Opts) ->
    emqx_resource_manager:restart(ResId, Opts).

-spec stop(resource_id()) -> ok | {error, Reason :: term()}.
stop(ResId) ->
    emqx_resource_manager:stop(ResId).

-spec health_check(resource_id()) -> {ok, resource_status()} | {error, term()}.
health_check(ResId) ->
    emqx_resource_manager:health_check(ResId).

set_resource_status_connecting(ResId) ->
    emqx_resource_manager:set_resource_status_connecting(ResId).

-spec get_instance(resource_id()) ->
    {ok, resource_group(), resource_data()} | {error, Reason :: term()}.
get_instance(ResId) ->
    emqx_resource_manager:lookup(ResId).

-spec list_instances() -> [resource_id()].
list_instances() ->
    [Id || #{id := Id} <- list_instances_verbose()].

-spec list_instances_verbose() -> [resource_data()].
list_instances_verbose() ->
    emqx_resource_manager:list_all().

-spec list_instances_by_type(module()) -> [resource_id()].
list_instances_by_type(ResourceType) ->
    filter_instances(fun
        (_, RT) when RT =:= ResourceType -> true;
        (_, _) -> false
    end).

-spec generate_id(term()) -> resource_id().
generate_id(Name) when is_binary(Name) ->
    Id = integer_to_binary(erlang:unique_integer([monotonic, positive])),
    <<Name/binary, ":", Id/binary>>.

-spec list_group_instances(resource_group()) -> [resource_id()].
list_group_instances(Group) -> emqx_resource_manager:list_group(Group).

-spec call_start(manager_id(), module(), resource_config()) ->
    {ok, resource_state()} | {error, Reason :: term()}.
call_start(MgrId, Mod, Config) ->
    ?SAFE_CALL(Mod:on_start(MgrId, Config)).

-spec call_health_check(manager_id(), module(), resource_state()) ->
    resource_status()
    | {resource_status(), resource_state()}
    | {resource_status(), resource_state(), term()}
    | {error, term()}.
call_health_check(MgrId, Mod, ResourceState) ->
    ?SAFE_CALL(Mod:on_get_status(MgrId, ResourceState)).

-spec call_stop(manager_id(), module(), resource_state()) -> term().
call_stop(MgrId, Mod, ResourceState) ->
    ?SAFE_CALL(Mod:on_stop(MgrId, ResourceState)).

-spec check_config(resource_type(), raw_resource_config()) ->
    {ok, resource_config()} | {error, term()}.
check_config(ResourceType, Conf) ->
    emqx_hocon:check(ResourceType, Conf).

-spec check_and_create(
    resource_id(),
    resource_group(),
    resource_type(),
    raw_resource_config()
) ->
    {ok, resource_data() | 'already_created'} | {error, term()}.
check_and_create(ResId, Group, ResourceType, RawConfig) ->
    check_and_create(ResId, Group, ResourceType, RawConfig, #{}).

-spec check_and_create(
    resource_id(),
    resource_group(),
    resource_type(),
    raw_resource_config(),
    create_opts()
) ->
    {ok, resource_data() | 'already_created'} | {error, term()}.
check_and_create(ResId, Group, ResourceType, RawConfig, Opts) ->
    check_and_do(
        ResourceType,
        RawConfig,
        fun(ResConf) -> create(ResId, Group, ResourceType, ResConf, Opts) end
    ).

-spec check_and_create_local(
    resource_id(),
    resource_group(),
    resource_type(),
    raw_resource_config()
) ->
    {ok, resource_data()} | {error, term()}.
check_and_create_local(ResId, Group, ResourceType, RawConfig) ->
    check_and_create_local(ResId, Group, ResourceType, RawConfig, #{}).

-spec check_and_create_local(
    resource_id(),
    resource_group(),
    resource_type(),
    raw_resource_config(),
    create_opts()
) -> {ok, resource_data()} | {error, term()}.
check_and_create_local(ResId, Group, ResourceType, RawConfig, Opts) ->
    check_and_do(
        ResourceType,
        RawConfig,
        fun(ResConf) -> create_local(ResId, Group, ResourceType, ResConf, Opts) end
    ).

-spec check_and_recreate(
    resource_id(),
    resource_type(),
    raw_resource_config(),
    create_opts()
) ->
    {ok, resource_data()} | {error, term()}.
check_and_recreate(ResId, ResourceType, RawConfig, Opts) ->
    check_and_do(
        ResourceType,
        RawConfig,
        fun(ResConf) -> recreate(ResId, ResourceType, ResConf, Opts) end
    ).

-spec check_and_recreate_local(
    resource_id(),
    resource_type(),
    raw_resource_config(),
    create_opts()
) ->
    {ok, resource_data()} | {error, term()}.
check_and_recreate_local(ResId, ResourceType, RawConfig, Opts) ->
    check_and_do(
        ResourceType,
        RawConfig,
        fun(ResConf) -> recreate_local(ResId, ResourceType, ResConf, Opts) end
    ).

check_and_do(ResourceType, RawConfig, Do) when is_function(Do) ->
    case check_config(ResourceType, RawConfig) of
        {ok, ResConf} -> Do(ResConf);
        Error -> Error
    end.

%% =================================================================================

filter_instances(Filter) ->
    [Id || #{id := Id, mod := Mod} <- list_instances_verbose(), Filter(Id, Mod)].

inc_metrics_funcs(ResId) ->
    OnFailed = [{fun emqx_metrics_worker:inc/3, [resource_metrics, ResId, failed]}],
    OnSucc = [{fun emqx_metrics_worker:inc/3, [resource_metrics, ResId, success]}],
    {OnSucc, OnFailed}.

safe_apply(Func, Args) ->
    ?SAFE_CALL(erlang:apply(Func, Args)).

query_error(Reason, Msg) ->
    {error, {?MODULE, #{reason => Reason, msg => Msg}}}.
