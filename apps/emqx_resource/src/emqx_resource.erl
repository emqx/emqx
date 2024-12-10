%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("emqx_resource_errors.hrl").
-include_lib("emqx/include/logger.hrl").

%% APIs for resource types

-export([list_types/0]).

%% APIs for instances

-export([
    check_config/2,
    check_and_create_local/4,
    check_and_create_local/5,
    check_and_recreate_local/4
]).

%% Sync resource instances and files
%% provisional solution: rpc:multicall to all the nodes for creating/updating/removing
%% todo: replicate operations

-export([
    %% store the config and start the instance
    create_local/5,
    create_dry_run_local/2,
    create_dry_run_local/3,
    create_dry_run_local/4,
    recreate_local/4,
    %% remove the config and stop the instance
    remove_local/1,
    reset_metrics/1,
    reset_metrics_local/1,
    reset_metrics_local/2,
    %% Create metrics for a resource ID
    create_metrics/1,
    ensure_metrics/1,
    %% Delete metrics for a resource ID
    clear_metrics/1
]).

%% Calls to the callback module with current resource state
%% They also save the state after the call finished (except call_get_channel_config/3).

-export([
    start/1,
    start/2,
    restart/1,
    restart/2,
    %% verify if the resource is working normally
    health_check/1,
    channel_health_check/2,
    get_channels/1,
    %% set resource status to disconnected
    set_resource_status_connecting/1,
    %% stop the instance
    stop/1,
    %% query the instance
    query/2,
    query/3,
    %% query the instance without batching and queuing messages.
    simple_sync_query/2,
    %% functions used by connectors to register resources that must be
    %% freed when stopping or even when a resource manager crashes.
    allocate_resource/3,
    has_allocated_resources/1,
    get_allocated_resources/1,
    get_allocated_resources_list/1,
    forget_allocated_resources/1,
    deallocate_resource/2,
    clean_allocated_resources/2,
    %% Get channel config from resource
    call_get_channel_config/3,
    % Call the format query result function
    call_format_query_result/2
]).

%% Direct calls to the callback module

-export([
    %% get the callback mode of a specific module
    get_callback_mode/1,
    get_resource_type/1,
    get_callback_mode/2,
    get_query_opts/2,
    %% start the instance
    call_start/3,
    %% verify if the resource is working normally
    call_health_check/3,
    %% verify if the resource channel is working normally
    call_channel_health_check/4,
    %% stop the instance
    call_stop/3,
    %% get the query mode of the resource
    query_mode/3,
    %% Add channel to resource
    call_add_channel/5,
    %% Remove channel from resource
    call_remove_channel/4,
    %% Get channels from resource
    call_get_channels/2
]).

%% list all the instances, id only.
-export([
    list_instances/0,
    %% list all the instances
    list_instances_verbose/0,
    %% return the data of the instance
    get_instance/1,
    is_exist/1,
    get_metrics/1,
    fetch_creation_opts/1,
    %% return all the instances of the same resource type
    list_instances_by_type/1,
    generate_id/1,
    list_group_instances/1
]).

-export([apply_reply_fun/2]).

%% common validations
-export([
    parse_resource_id/2,
    validate_type/1,
    validate_name/1
]).

-export([is_dry_run/1]).

-export_type([
    query_mode/0,
    resource_id/0,
    channel_id/0,
    resource_data/0,
    resource_status/0
]).

-optional_callbacks([
    on_query/3,
    on_batch_query/3,
    on_query_async/4,
    on_batch_query_async/4,
    on_get_status/2,
    on_get_channel_status/3,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    query_mode/1,
    on_format_query_result/1,
    callback_mode/1,
    query_opts/1
]).

%% when calling emqx_resource:start/1
-callback on_start(resource_id(), resource_config()) ->
    {ok, resource_state()} | {error, Reason :: term()}.

%% when calling emqx_resource:stop/1
-callback on_stop(resource_id(), resource_state()) -> term().

%% when calling emqx_resource:get_callback_mode/1
-callback callback_mode() -> callback_mode() | undefined.

%% when calling emqx_resource:get_callback_mode/1
-callback callback_mode(resource_state()) -> callback_mode().

%% when calling emqx_resource:query/3
-callback on_query(resource_id(), Request :: term(), resource_state()) -> query_result().

%% when calling emqx_resource:on_batch_query/3
-callback on_batch_query(resource_id(), Request :: term(), resource_state()) ->
    batch_query_result().

%% when calling emqx_resource:on_query_async/4
-callback on_query_async(
    resource_id(),
    Request :: term(),
    {ReplyFun :: function(), Args :: list()},
    resource_state()
) -> query_result().

%% when calling emqx_resource:on_batch_query_async/4
-callback on_batch_query_async(
    resource_id(),
    Request :: term(),
    {ReplyFun :: function(), Args :: list()},
    resource_state()
) -> query_result().

%% when calling emqx_resource:health_check/2
-callback on_get_status(resource_id(), resource_state()) ->
    health_check_status()
    | {health_check_status(), Reason :: term()}.

-callback on_get_channel_status(resource_id(), channel_id(), resource_state()) ->
    channel_status()
    | {channel_status(), Reason :: term()}
    | {error, term()}.

-callback query_mode(Config :: term()) -> query_mode().

-callback query_opts(Config :: term()) -> #{timeout => timeout()}.

%% This callback handles the installation of a specified channel.
%%
%% If the channel cannot be successfully installed, the callback shall
%% throw an exception or return an error tuple.
-callback on_add_channel(
    ResId :: term(), ResourceState :: term(), ChannelId :: binary(), ChannelConfig :: map()
) -> {ok, term()} | {error, term()}.

%% This callback handles the removal of a specified channel resource.
%%
%% It's guaranteed that the provided channel is installed when this
%% function is invoked. Upon successful deinstallation, the function should return
%% a new state
%%
%% If the channel cannot be successfully deinstalled, the callback should
%% log an error.
%%
-callback on_remove_channel(
    ResId :: term(), ResourceState :: term(), ChannelId :: binary()
) -> {ok, NewState :: term()}.

%% This callback shall return a list of channel configs that are currently active
%% for the resource with the given id.
-callback on_get_channels(
    ResId :: term()
) -> [term()].

%% When given the result of a on_*query call this function should return a
%% version of the result that is suitable for JSON trace logging. This
%% typically means converting Erlang tuples to maps with appropriate names for
%% the values in the tuple.
-callback on_format_query_result(
    QueryResult :: term()
) -> term().

%% Used for tagging log entries.
-callback resource_type() -> atom().

-define(SAFE_CALL(EXPR),
    (fun() ->
        try
            EXPR
        catch
            throw:Reason ->
                {error, Reason};
            C:E:S ->
                {error, #{
                    exception => C,
                    reason => emqx_utils:redact(E),
                    stacktrace => emqx_utils:redact(S)
                }}
        end
    end)()
).

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

%% =================================================================================
%% APIs for resource instances
%% =================================================================================
-spec create_local(
    resource_id(),
    resource_group(),
    resource_module(),
    resource_config(),
    creation_opts()
) ->
    {ok, resource_data()}.
create_local(ResId, Group, ResourceType, Config, Opts) ->
    emqx_resource_manager:ensure_resource(ResId, Group, ResourceType, Config, Opts).

-spec create_dry_run_local(resource_module(), resource_config()) ->
    ok | {error, Reason :: term()}.
create_dry_run_local(ResourceType, Config) ->
    emqx_resource_manager:create_dry_run(ResourceType, Config).

create_dry_run_local(ResId, ResourceType, Config) ->
    emqx_resource_manager:create_dry_run(ResId, ResourceType, Config).

-spec create_dry_run_local(
    resource_id(),
    resource_module(),
    resource_config(),
    OnReadyCallback
) ->
    ok | {error, Reason :: term()}
when
    OnReadyCallback :: fun((resource_id()) -> ok | {error, Reason :: term()}).
create_dry_run_local(ResId, ResourceType, Config, OnReadyCallback) ->
    emqx_resource_manager:create_dry_run(ResId, ResourceType, Config, OnReadyCallback).

-spec recreate_local(
    resource_id(), resource_module(), resource_config(), creation_opts()
) ->
    {ok, resource_data()} | {error, Reason :: term()}.
recreate_local(ResId, ResourceType, Config, Opts) ->
    emqx_resource_manager:recreate(ResId, ResourceType, Config, Opts).

-spec remove_local(resource_id()) -> ok.
remove_local(ResId) ->
    case emqx_resource_manager:remove(ResId) of
        ok ->
            ok;
        {error, not_found} ->
            ok;
        Error ->
            %% Only log, the ResId worker is always removed in manager's remove action.
            ?SLOG(
                warning,
                #{
                    msg => "remove_resource_failed",
                    error => Error,
                    resource_id => ResId
                },
                #{tag => ?TAG}
            ),
            ok
    end.

%% Tip: Don't delete reset_metrics_local/1, use before v572 rpc
-spec reset_metrics_local(resource_id()) -> ok.
reset_metrics_local(ResId) ->
    reset_metrics_local(ResId, #{}).

-spec reset_metrics_local(resource_id(), map()) -> ok.
reset_metrics_local(ResId, _ClusterOpts) ->
    emqx_resource_manager:reset_metrics(ResId).

-spec reset_metrics(resource_id()) -> ok | {error, Reason :: term()}.
reset_metrics(ResId) ->
    emqx_resource_proto_v2:reset_metrics(ResId).

%% =================================================================================
-spec query(resource_id(), Request :: term()) -> Result :: term().
query(ResId, Request) ->
    query(ResId, Request, #{}).

-spec query(resource_id(), Request :: term(), query_opts()) ->
    Result :: term().
query(ResId, Request, Opts) ->
    case emqx_resource_manager:get_query_mode_and_last_error(ResId, Opts) of
        {error, _} = ErrorTuple ->
            ErrorTuple;
        {ok, {_, unhealthy_target}} ->
            emqx_resource_metrics:matched_inc(ResId),
            emqx_resource_metrics:dropped_resource_stopped_inc(ResId),
            ?RESOURCE_ERROR(unhealthy_target, "unhealthy target");
        {ok, {_, {unhealthy_target, Message}}} ->
            emqx_resource_metrics:matched_inc(ResId),
            emqx_resource_metrics:dropped_resource_stopped_inc(ResId),
            ?RESOURCE_ERROR(unhealthy_target, Message);
        {ok, {simple_async, _}} ->
            %% TODO(5.1.1): pass Resource instead of ResId to simple APIs
            %% so the buffer worker does not need to lookup the cache again
            emqx_resource_buffer_worker:simple_async_query(ResId, Request, Opts);
        {ok, {simple_sync, _}} ->
            %% TODO(5.1.1): pass Resource instead of ResId to simple APIs
            %% so the buffer worker does not need to lookup the cache again
            emqx_resource_buffer_worker:simple_sync_query(ResId, Request, Opts);
        {ok, {simple_async_internal_buffer, _}} ->
            %% This is for bridges/connectors that have internal buffering, such
            %% as Kafka and Pulsar producers.
            %% TODO(5.1.1): pass Resource instead of ResId to simple APIs
            %% so the buffer worker does not need to lookup the cache again
            emqx_resource_buffer_worker:simple_async_query(ResId, Request, Opts);
        {ok, {simple_sync_internal_buffer, _}} ->
            %% This is for bridges/connectors that have internal buffering, such
            %% as Kafka and Pulsar producers.
            %% TODO(5.1.1): pass Resource instead of ResId to simple APIs
            %% so the buffer worker does not need to lookup the cache again
            emqx_resource_buffer_worker:simple_sync_internal_buffer_query(
                ResId, Request, Opts
            );
        {ok, {sync, _}} ->
            emqx_resource_buffer_worker:sync_query(ResId, Request, Opts);
        {ok, {async, _}} ->
            emqx_resource_buffer_worker:async_query(ResId, Request, Opts)
    end.

-spec simple_sync_query(resource_id(), Request :: term()) -> Result :: term().
simple_sync_query(ResId, Request) ->
    emqx_resource_buffer_worker:simple_sync_query(ResId, Request).

-spec start(resource_id()) -> ok | {error, Reason :: term()}.
start(ResId) ->
    start(ResId, #{}).

-spec start(resource_id(), creation_opts()) -> ok | {error, Reason :: term()}.
start(ResId, Opts) ->
    emqx_resource_manager:start(ResId, Opts).

-spec restart(resource_id()) -> ok | {error, Reason :: term()}.
restart(ResId) ->
    restart(ResId, #{}).

-spec restart(resource_id(), creation_opts()) -> ok | {error, Reason :: term()}.
restart(ResId, Opts) ->
    emqx_resource_manager:restart(ResId, Opts).

-spec stop(resource_id()) -> ok | {error, Reason :: term()}.
stop(ResId) ->
    emqx_resource_manager:stop(ResId).

-spec health_check(resource_id()) -> {ok, resource_status()} | {error, term()}.
health_check(ResId) ->
    emqx_resource_manager:health_check(ResId).

-spec channel_health_check(resource_id(), channel_id()) ->
    #{status := resource_status(), error := term()}.
channel_health_check(ResId, ChannelId) ->
    emqx_resource_manager:channel_health_check(ResId, ChannelId).

-spec get_channels(resource_id()) -> {ok, [{binary(), map()}]} | {error, term()}.
get_channels(ResId) ->
    case emqx_resource_manager:lookup_cached(ResId) of
        {error, not_found} ->
            {error, not_found};
        {ok, _Group, _ResourceData = #{mod := Mod}} ->
            {ok, emqx_resource:call_get_channels(ResId, Mod)}
    end.

set_resource_status_connecting(ResId) ->
    emqx_resource_manager:set_resource_status_connecting(ResId).

-spec get_instance(resource_id()) ->
    {ok, resource_group(), resource_data()} | {error, Reason :: term()}.
get_instance(ResId) ->
    emqx_resource_manager:lookup_cached(ResId).

-spec is_exist(resource_id()) -> boolean().
is_exist(ResId) ->
    emqx_resource_manager:is_exist(ResId).

-spec get_metrics(resource_id()) ->
    emqx_metrics_worker:metrics().
get_metrics(ResId) ->
    emqx_resource_manager:get_metrics(ResId).

-spec fetch_creation_opts(map()) -> creation_opts().
fetch_creation_opts(Opts) ->
    maps:get(resource_opts, Opts, #{}).

-spec list_instances() -> [resource_id()].
list_instances() ->
    emqx_resource_cache:all_ids().

-spec list_instances_verbose() -> [_ResourceDataWithMetrics :: map()].
list_instances_verbose() ->
    lists:map(
        fun(#{id := ResId} = Res) ->
            Res#{metrics => get_metrics(ResId)}
        end,
        emqx_resource_manager:list_all()
    ).

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

-spec get_callback_mode(module()) -> callback_mode().
get_callback_mode(Mod) ->
    Mod:callback_mode().

-spec get_resource_type(module()) -> resource_type().
get_resource_type(Mod) ->
    Mod:resource_type().

-spec get_callback_mode(module(), resource_state()) -> callback_mode() | undefined.
get_callback_mode(Mod, State) ->
    case erlang:function_exported(Mod, callback_mode, 1) of
        true ->
            Mod:callback_mode(State);
        _ ->
            undefined
    end.

-spec get_query_opts(module(), map()) -> #{timeout => timeout()}.
get_query_opts(Mod, ActionOrSourceConfig) ->
    case erlang:function_exported(Mod, query_opts, 1) of
        true ->
            Mod:query_opts(ActionOrSourceConfig);
        false ->
            emqx_bridge:query_opts(ActionOrSourceConfig)
    end.

-spec call_start(resource_id(), module(), resource_config()) ->
    {ok, resource_state()} | {error, Reason :: term()}.
call_start(ResId, Mod, Config) ->
    ?SAFE_CALL(
        begin
            %% If the previous manager process crashed without cleaning up
            %% allocated resources, clean them up.
            clean_allocated_resources(ResId, Mod),
            Mod:on_start(ResId, Config)
        end
    ).

-spec call_health_check(resource_id(), module(), resource_state()) ->
    resource_status()
    | {resource_status(), resource_state()}
    | {resource_status(), resource_state(), term()}
    | {error, term()}.
call_health_check(ResId, Mod, ResourceState) ->
    ?SAFE_CALL(Mod:on_get_status(ResId, ResourceState)).

-spec call_channel_health_check(resource_id(), channel_id(), module(), resource_state()) ->
    channel_status()
    | {channel_status(), Reason :: term()}
    | {error, term()}.
call_channel_health_check(ResId, ChannelId, Mod, ResourceState) ->
    ?SAFE_CALL(Mod:on_get_channel_status(ResId, ChannelId, ResourceState)).

call_add_channel(ResId, Mod, ResourceState, ChannelId, ChannelConfig) ->
    %% Check if on_add_channel is exported
    case erlang:function_exported(Mod, on_add_channel, 4) of
        true ->
            ?SAFE_CALL(
                Mod:on_add_channel(
                    ResId, ResourceState, ChannelId, ChannelConfig
                )
            );
        false ->
            {error,
                <<<<"on_add_channel callback function not available for connector with resource id ">>/binary,
                    ResId/binary>>}
    end.

call_remove_channel(ResId, Mod, ResourceState, ChannelId) ->
    %% Check if maybe_install_insert_template is exported
    case erlang:function_exported(Mod, on_remove_channel, 3) of
        true ->
            ?SAFE_CALL(
                Mod:on_remove_channel(
                    ResId, ResourceState, ChannelId
                )
            );
        false ->
            {error,
                <<<<"on_remove_channel callback function not available for connector with resource id ">>/binary,
                    ResId/binary>>}
    end.

call_get_channels(ResId, Mod) ->
    case erlang:function_exported(Mod, on_get_channels, 1) of
        true ->
            Mod:on_get_channels(ResId);
        false ->
            []
    end.

call_get_channel_config(ResId, ChannelId, Mod) ->
    case erlang:function_exported(Mod, on_get_channels, 1) of
        true ->
            ChConfigs = Mod:on_get_channels(ResId),
            case [Conf || {ChId, Conf} <- ChConfigs, ChId =:= ChannelId] of
                [ChannelConf] ->
                    ChannelConf;
                _ ->
                    {error,
                        <<"Channel ", ChannelId/binary,
                            "not found. There seems to be a broken reference">>}
            end;
        false ->
            {error,
                <<"on_get_channels callback function not available for resource id", ResId/binary>>}
    end.

call_format_query_result(Mod, Result) ->
    case erlang:function_exported(Mod, on_format_query_result, 1) of
        true ->
            Mod:on_format_query_result(Result);
        false ->
            Result
    end.

-spec call_stop(resource_id(), module(), resource_state()) -> term().
call_stop(ResId, Mod, ResourceState) ->
    ?SAFE_CALL(begin
        Res = Mod:on_stop(ResId, ResourceState),
        case Res of
            ok ->
                emqx_resource:forget_allocated_resources(ResId);
            _ ->
                ok
        end,
        Res
    end).

-spec query_mode(module(), term(), creation_opts()) -> query_mode().
query_mode(Mod, Config, Opts) ->
    case erlang:function_exported(Mod, query_mode, 1) of
        true ->
            Mod:query_mode(Config);
        false ->
            maps:get(query_mode, Opts, sync)
    end.

-spec check_config(resource_module(), raw_resource_config()) ->
    {ok, resource_config()} | {error, term()}.
check_config(ResourceType, Conf) ->
    emqx_hocon:check(ResourceType, Conf).

-spec check_and_create_local(
    resource_id(),
    resource_group(),
    resource_module(),
    raw_resource_config()
) ->
    {ok, resource_data()} | {error, term()}.
check_and_create_local(ResId, Group, ResourceType, RawConfig) ->
    check_and_create_local(ResId, Group, ResourceType, RawConfig, #{}).

-spec check_and_create_local(
    resource_id(),
    resource_group(),
    resource_module(),
    raw_resource_config(),
    creation_opts()
) -> {ok, resource_data()} | {error, term()}.
check_and_create_local(ResId, Group, ResourceType, RawConfig, Opts) ->
    check_and_do(
        ResourceType,
        RawConfig,
        fun(ResConf) -> create_local(ResId, Group, ResourceType, ResConf, Opts) end
    ).

-spec check_and_recreate_local(
    resource_id(),
    resource_module(),
    raw_resource_config(),
    creation_opts()
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

apply_reply_fun({F, A}, Result) when is_function(F) ->
    _ = erlang:apply(F, A ++ [Result]),
    ok;
apply_reply_fun(From, Result) ->
    gen_server:reply(From, Result).

-spec allocate_resource(resource_id(), any(), term()) -> ok.
allocate_resource(InstanceId, Key, Value) ->
    true = ets:insert(?RESOURCE_ALLOCATION_TAB, {InstanceId, Key, Value}),
    ok.

-spec has_allocated_resources(resource_id()) -> boolean().
has_allocated_resources(InstanceId) ->
    ets:member(?RESOURCE_ALLOCATION_TAB, InstanceId).

-spec get_allocated_resources(resource_id()) -> map().
get_allocated_resources(InstanceId) ->
    Objects = ets:lookup(?RESOURCE_ALLOCATION_TAB, InstanceId),
    maps:from_list([{K, V} || {_InstanceId, K, V} <- Objects]).

-spec get_allocated_resources_list(resource_id()) -> list(tuple()).
get_allocated_resources_list(InstanceId) ->
    ets:lookup(?RESOURCE_ALLOCATION_TAB, InstanceId).

-spec forget_allocated_resources(resource_id()) -> ok.
forget_allocated_resources(InstanceId) ->
    true = ets:delete(?RESOURCE_ALLOCATION_TAB, InstanceId),
    ok.

deallocate_resource(InstanceId, Key) ->
    true = ets:match_delete(?RESOURCE_ALLOCATION_TAB, {InstanceId, Key, '_'}),
    ok.

-spec create_metrics(resource_id()) -> ok.
create_metrics(ResId) ->
    emqx_metrics_worker:create_metrics(?RES_METRICS, ResId, metrics(), rate_metrics()).

-spec ensure_metrics(resource_id()) -> {ok, created | already_created}.
ensure_metrics(ResId) ->
    emqx_metrics_worker:ensure_metrics(?RES_METRICS, ResId, metrics(), rate_metrics()).

-spec clear_metrics(resource_id()) -> ok.
clear_metrics(ResId) ->
    emqx_metrics_worker:clear_metrics(?RES_METRICS, ResId).
%% =================================================================================

metrics() ->
    [
        'matched',
        'retried',
        'retried.success',
        'retried.failed',
        'success',
        'late_reply',
        'failed',
        'dropped',
        'dropped.expired',
        'dropped.queue_full',
        'dropped.resource_not_found',
        'dropped.resource_stopped',
        'dropped.other',
        'received'
    ].

rate_metrics() ->
    ['matched'].

filter_instances(Filter) ->
    [Id || #{id := Id, mod := Mod} <- list_instances_verbose(), Filter(Id, Mod)].

clean_allocated_resources(ResourceId, ResourceMod) ->
    case emqx_resource:has_allocated_resources(ResourceId) of
        true ->
            %% The resource entries in the ETS table are erased inside
            %% `call_stop' if the call is successful.
            ok = call_stop(ResourceId, ResourceMod, _ResourceState = undefined),
            ok;
        false ->
            ok
    end.

%% @doc Split : separated resource id into type and name.
%% Type must be an existing atom.
%% Name is converted to atom if `atom_name` option is true.
-spec parse_resource_id(list() | binary(), #{atom_name => boolean()}) ->
    {atom(), atom() | binary()}.
parse_resource_id(Id0, Opts) ->
    Id = bin(Id0),
    case string:split(bin(Id), ":", all) of
        [Type, Name] ->
            {to_type_atom(Type), validate_name(Name, Opts)};
        _ ->
            invalid_data(
                <<"should be of pattern {type}:{name}, but got: ", Id/binary>>
            )
    end.

to_type_atom(Type) when is_binary(Type) ->
    try
        erlang:binary_to_existing_atom(Type, utf8)
    catch
        _:_ ->
            throw(#{
                kind => validation_error,
                reason => <<"unknown resource type: ", Type/binary>>
            })
    end.

%% @doc Validate if type is valid.
%% Throws and JSON-map error if invalid.
-spec validate_type(binary()) -> ok.
validate_type(Type) ->
    _ = to_type_atom(Type),
    ok.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

%% @doc Validate if name is valid for bridge.
%% Throws and JSON-map error if invalid.
-spec validate_name(binary()) -> ok.
validate_name(Name) ->
    _ = validate_name(Name, #{atom_name => false}),
    ok.

-spec is_dry_run(resource_id()) -> boolean().
is_dry_run(?PROBE_ID_MATCH(_)) ->
    %% A probe connector
    true;
is_dry_run(ID) ->
    %% A probe action/source
    RE = ":" ++ ?PROBE_ID_PREFIX ++ "[a-zA-Z0-9]{8}:",
    match =:= re:run(ID, RE, [{capture, none}]).

validate_name(<<>>, _Opts) ->
    invalid_data("Name cannot be empty string");
validate_name(Name, _Opts) when size(Name) >= 255 ->
    invalid_data("Name length must be less than 255");
validate_name(Name, Opts) ->
    case re:run(Name, <<"^[0-9a-zA-Z][-0-9a-zA-Z_]*$">>, [{capture, none}]) of
        match ->
            case maps:get(atom_name, Opts, true) of
                %% NOTE
                %% Rule may be created before bridge, thus not `list_to_existing_atom/1`,
                %% also it is infrequent user input anyway.
                true -> binary_to_atom(Name, utf8);
                false -> Name
            end;
        nomatch ->
            invalid_data(
                <<
                    "Invalid name format. The name must begin with a letter or number "
                    "(0-9, a-z, A-Z) and can only include underscores and hyphens as "
                    "non-initial characters. Got: ",
                    Name/binary
                >>
            )
    end.

-spec invalid_data(binary()) -> no_return().
invalid_data(Reason) -> throw(#{kind => validation_error, reason => Reason}).
