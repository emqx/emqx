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
-module(emqx_bridge_v2).

-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Note: this is strange right now, because it lives in `emqx_bridge_v2', but it shall be
%% refactored into a new module/application with appropriate name.
-define(ROOT_KEY_ACTIONS, actions).
-define(ROOT_KEY_ACTIONS_BIN, <<"actions">>).
-define(ROOT_KEY_SOURCES, sources).
-define(ROOT_KEY_SOURCES_BIN, <<"sources">>).

%% Loading and unloading config when EMQX starts and stops
-export([
    load/0,
    unload/0
]).

%% CRUD API

-export([
    list/0,
    list/1,
    lookup/2,
    lookup/3,
    lookup_raw_conf/3,
    is_exist/3,
    create/3,
    create/4,
    %% The remove/2 function is only for internal use as it may create
    %% rules with broken dependencies
    remove/2,
    remove/3,
    %% The following is the remove function that is called by the HTTP API
    %% It also checks for rule action dependencies and optionally removes
    %% them
    check_deps_and_remove/3,
    check_deps_and_remove/4
]).
-export([is_action_exist/2, is_source_exist/2]).

%% Operations

-export([
    disable_enable/3,
    disable_enable/4,
    health_check/2,
    send_message/4,
    query/4,
    start/2,
    start/3,
    reset_metrics/2,
    reset_metrics/3,
    create_dry_run/2,
    create_dry_run/3,
    get_metrics/2,
    get_metrics/3
]).

%% On message publish hook (for local_topics)

-export([on_message_publish/1]).

%% Convenience functions for connector implementations

-export([
    parse_id/1,
    get_channels_for_connector/1
]).

-export([diff_confs/2]).

%% Exported for tests
-export([
    id/2,
    id/3,
    source_id/3,
    source_hookpoint/1,
    bridge_v1_is_valid/2,
    bridge_v1_is_valid/3,
    extract_connector_id_from_bridge_v2_id/1
]).

%% Config Update Handler API

-export([
    post_config_update/5,
    pre_config_update/3
]).

%% Data backup
-export([
    import_config/1
]).

%% Bridge V2 Types and Conversions

-export([
    bridge_v2_type_to_connector_type/1,
    is_bridge_v2_type/1,
    connector_type/1
]).

%% Compatibility Layer API
%% All public functions for the compatibility layer should be prefixed with
%% bridge_v1_

-export([
    bridge_v1_lookup_and_transform/2,
    bridge_v1_list_and_transform/0,
    bridge_v1_check_deps_and_remove/3,
    bridge_v1_split_config_and_create/3,
    bridge_v1_create_dry_run/2,
    bridge_v1_type_to_bridge_v2_type/1,
    %% Exception from the naming convention:
    bridge_v2_type_to_bridge_v1_type/2,
    bridge_v1_id_to_connector_resource_id/1,
    bridge_v1_id_to_connector_resource_id/2,
    bridge_v1_enable_disable/3,
    bridge_v1_restart/2,
    bridge_v1_stop/2,
    bridge_v1_start/2,
    bridge_v1_reset_metrics/2,
    %% For test cases only
    bridge_v1_remove/2,
    get_conf_root_key_if_only_one/2
]).

%%====================================================================
%% Types
%%====================================================================

-type bridge_v2_info() :: #{
    type := binary(),
    name := binary(),
    raw_config := map(),
    resource_data := map(),
    status := emqx_resource:resource_status(),
    %% Explanation of the status if the status is not connected
    error := term()
}.

-type bridge_v2_type() :: binary() | atom() | [byte()].
-type bridge_v2_name() :: binary() | atom() | [byte()].

-type root_cfg_key() :: ?ROOT_KEY_ACTIONS | ?ROOT_KEY_SOURCES.

-export_type([root_cfg_key/0, bridge_v2_type/0, bridge_v2_name/0]).

%%====================================================================

%%====================================================================

%%====================================================================
%% Loading and unloading config when EMQX starts and stops
%%====================================================================

load() ->
    load_bridges(?ROOT_KEY_ACTIONS),
    load_bridges(?ROOT_KEY_SOURCES),
    load_message_publish_hook(),
    ok = emqx_config_handler:add_handler(config_key_path_leaf(), emqx_bridge_v2),
    ok = emqx_config_handler:add_handler(config_key_path(), emqx_bridge_v2),
    ok = emqx_config_handler:add_handler(config_key_path_leaf_sources(), emqx_bridge_v2),
    ok = emqx_config_handler:add_handler(config_key_path_sources(), emqx_bridge_v2),
    ok.

load_bridges(RootName) ->
    Bridges = emqx:get_config([RootName], #{}),
    _ = emqx_utils:pmap(
        fun({Type, Bridge}) ->
            emqx_utils:pmap(
                fun({Name, BridgeConf}) ->
                    install_bridge_v2(RootName, Type, Name, BridgeConf)
                end,
                maps:to_list(Bridge),
                infinity
            )
        end,
        maps:to_list(Bridges),
        infinity
    ),
    ok.

unload() ->
    unload_bridges(?ROOT_KEY_ACTIONS),
    unload_bridges(?ROOT_KEY_SOURCES),
    unload_message_publish_hook(),
    emqx_conf:remove_handler(config_key_path()),
    emqx_conf:remove_handler(config_key_path_leaf()),
    ok.

unload_bridges(ConfRootKey) ->
    Bridges = emqx:get_config([ConfRootKey], #{}),
    _ = emqx_utils:pmap(
        fun({Type, Bridge}) ->
            emqx_utils:pmap(
                fun({Name, BridgeConf}) ->
                    uninstall_bridge_v2(ConfRootKey, Type, Name, BridgeConf)
                end,
                maps:to_list(Bridge),
                infinity
            )
        end,
        maps:to_list(Bridges),
        infinity
    ),
    ok.

%%====================================================================
%% CRUD API
%%====================================================================

-spec lookup(bridge_v2_type(), bridge_v2_name()) -> {ok, bridge_v2_info()} | {error, not_found}.
lookup(Type, Name) ->
    lookup(?ROOT_KEY_ACTIONS, Type, Name).

is_action_exist(Type, Name) ->
    is_exist(?ROOT_KEY_ACTIONS, Type, Name).

is_source_exist(Type, Name) ->
    is_exist(?ROOT_KEY_SOURCES, Type, Name).

is_exist(ConfRootName, Type, Name) ->
    {error, not_found} =/= lookup_raw_conf(ConfRootName, Type, Name).

lookup_raw_conf(ConfRootName, Type, Name) ->
    case emqx:get_raw_config([ConfRootName, Type, Name], not_found) of
        not_found ->
            {error, not_found};
        #{<<"connector">> := _} = RawConf ->
            {ok, RawConf}
    end.

-spec lookup(root_cfg_key(), bridge_v2_type(), bridge_v2_name()) ->
    {ok, bridge_v2_info()} | {error, not_found}.
lookup(ConfRootName, Type, Name) ->
    case emqx:get_raw_config([ConfRootName, Type, Name], not_found) of
        not_found ->
            {error, not_found};
        #{<<"connector">> := BridgeConnector} = RawConf ->
            ConnectorId = emqx_connector_resource:resource_id(
                connector_type(Type), BridgeConnector
            ),
            %% The connector should always exist
            %% ... but, in theory, there might be no channels associated to it when we try
            %% to delete the connector, and then this reference will become dangling...
            ConnectorData =
                case emqx_resource:get_instance(ConnectorId) of
                    {ok, _, Data} ->
                        Data;
                    {error, not_found} ->
                        #{}
                end,
            %% Find the Bridge V2 status from the ConnectorData
            ConnectorStatus = maps:get(status, ConnectorData, undefined),
            Channels = maps:get(added_channels, ConnectorData, #{}),
            BridgeV2Id = id_with_root_name(ConfRootName, Type, Name, BridgeConnector),
            ChannelStatus = maps:get(BridgeV2Id, Channels, undefined),
            {DisplayBridgeV2Status, ErrorMsg} =
                case {ChannelStatus, ConnectorStatus} of
                    {_, ?status_disconnected} ->
                        {?status_disconnected, <<"Resource not operational">>};
                    {#{status := ?status_connected}, _} ->
                        {?status_connected, <<"">>};
                    {#{error := resource_not_operational}, ?status_connecting} ->
                        {?status_connecting, <<"Not installed">>};
                    {#{error := not_added_yet}, _} ->
                        {?status_connecting, <<"Not installed">>};
                    {#{status := Status, error := undefined}, _} ->
                        {Status, <<"Unknown reason">>};
                    {#{status := Status, error := Error}, _} ->
                        {Status, emqx_utils:readable_error_msg(Error)};
                    {undefined, _} ->
                        {?status_disconnected, <<"Not installed">>}
                end,
            {ok, #{
                type => bin(Type),
                name => bin(Name),
                raw_config => RawConf,
                resource_data => ConnectorData,
                status => DisplayBridgeV2Status,
                error => ErrorMsg
            }}
    end.

-spec list() -> [bridge_v2_info()] | {error, term()}.
list() ->
    list_with_lookup_fun(?ROOT_KEY_ACTIONS, fun lookup/2).

list(ConfRootKey) ->
    LookupFun = fun(Type, Name) ->
        lookup(ConfRootKey, Type, Name)
    end,
    list_with_lookup_fun(ConfRootKey, LookupFun).

-spec create(bridge_v2_type(), bridge_v2_name(), map()) ->
    {ok, emqx_config:update_result()} | {error, any()}.
create(BridgeType, BridgeName, RawConf) ->
    create(?ROOT_KEY_ACTIONS, BridgeType, BridgeName, RawConf).

create(ConfRootKey, BridgeType, BridgeName, RawConf0) ->
    ?SLOG(debug, #{
        bridge_action => create,
        bridge_version => 2,
        bridge_type => BridgeType,
        bridge_name => BridgeName,
        bridge_raw_config => emqx_utils:redact(RawConf0),
        root_key_path => ConfRootKey
    }),
    RawConf =
        emqx_utils_maps:put_if(
            RawConf0,
            <<"last_modified_at">>,
            now_ms(),
            not is_map_key(<<"last_modified_at">>, RawConf0)
        ),
    emqx_conf:update(
        [ConfRootKey, BridgeType, BridgeName],
        RawConf,
        #{override_to => cluster}
    ).

-spec remove(bridge_v2_type(), bridge_v2_name()) -> ok | {error, any()}.
remove(BridgeType, BridgeName) ->
    %% NOTE: This function can cause broken references from rules but it is only
    %% called directly from test cases.
    remove(?ROOT_KEY_ACTIONS, BridgeType, BridgeName).

remove(ConfRootKey, BridgeType, BridgeName) ->
    ?SLOG(debug, #{
        bridge_action => remove,
        bridge_version => 2,
        bridge_type => BridgeType,
        bridge_name => BridgeName
    }),
    case
        emqx_conf:remove(
            [ConfRootKey, BridgeType, BridgeName],
            #{override_to => cluster}
        )
    of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec check_deps_and_remove(bridge_v2_type(), bridge_v2_name(), boolean()) -> ok | {error, any()}.
check_deps_and_remove(BridgeType, BridgeName, AlsoDeleteActions) ->
    check_deps_and_remove(?ROOT_KEY_ACTIONS, BridgeType, BridgeName, AlsoDeleteActions).

-spec check_deps_and_remove(root_cfg_key(), bridge_v2_type(), bridge_v2_name(), boolean()) ->
    ok | {error, any()}.
check_deps_and_remove(ConfRootKey, BridgeType, BridgeName, AlsoDeleteActions) ->
    AlsoDelete =
        case AlsoDeleteActions of
            true -> [rule_actions];
            false -> []
        end,
    case
        emqx_bridge_lib:maybe_withdraw_rule_action(
            ConfRootKey,
            BridgeType,
            BridgeName,
            AlsoDelete
        )
    of
        ok ->
            remove(ConfRootKey, BridgeType, BridgeName);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Helpers for CRUD API
%%--------------------------------------------------------------------

list_with_lookup_fun(ConfRootName, LookupFun) ->
    maps:fold(
        fun(Type, NameAndConf, Bridges) ->
            maps:fold(
                fun(Name, _RawConf, Acc) ->
                    [
                        begin
                            case LookupFun(Type, Name) of
                                {ok, BridgeInfo} ->
                                    BridgeInfo;
                                {error, not_bridge_v1_compatible} = Err ->
                                    %% Filtered out by the caller
                                    Err
                            end
                        end
                        | Acc
                    ]
                end,
                Bridges,
                NameAndConf
            )
        end,
        [],
        emqx:get_raw_config([ConfRootName], #{})
    ).

install_bridge_v2(
    _RootName,
    _BridgeType,
    _BridgeName,
    #{enable := false}
) ->
    ok;
install_bridge_v2(
    RootName,
    BridgeV2Type,
    BridgeName,
    Config
) ->
    install_bridge_v2_helper(
        RootName,
        BridgeV2Type,
        BridgeName,
        combine_connector_and_bridge_v2_config(
            BridgeV2Type,
            BridgeName,
            Config
        )
    ).

install_bridge_v2_helper(
    _RootName,
    _BridgeV2Type,
    _BridgeName,
    {error, Reason} = Error
) ->
    ?SLOG(warning, Reason),
    Error;
install_bridge_v2_helper(
    RootName,
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = Config
) ->
    BridgeV2Id = id_with_root_name(RootName, BridgeV2Type, BridgeName, ConnectorName),
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    %% Create metrics for Bridge V2
    ok = emqx_resource:create_metrics(BridgeV2Id),
    %% We might need to create buffer workers for Bridge V2
    case get_resource_query_mode(BridgeV2Type, Config) of
        %% the Bridge V2 has built-in buffer, so there is no need for resource workers
        simple_sync_internal_buffer ->
            ok;
        simple_async_internal_buffer ->
            ok;
        %% The Bridge V2 is a consumer Bridge V2, so there is no need for resource workers
        no_queries ->
            ok;
        _ ->
            %% start resource workers as the query type requires them
            ok = emqx_resource_buffer_worker_sup:start_workers(BridgeV2Id, CreationOpts)
    end,
    %% If there is a running connector, we need to install the Bridge V2 in it
    ConnectorId = emqx_connector_resource:resource_id(
        connector_type(BridgeV2Type), ConnectorName
    ),
    _ = emqx_resource_manager:add_channel(
        ConnectorId,
        BridgeV2Id,
        augment_channel_config(
            RootName,
            BridgeV2Type,
            BridgeName,
            Config
        )
    ),
    ok.

augment_channel_config(
    ConfigRoot,
    BridgeV2Type,
    BridgeName,
    Config
) ->
    AugmentedConf = Config#{
        config_root => ConfigRoot,
        bridge_type => bin(BridgeV2Type),
        bridge_name => bin(BridgeName)
    },
    case emqx_action_info:is_source(BridgeV2Type) andalso ConfigRoot =:= ?ROOT_KEY_SOURCES of
        true ->
            BId = emqx_bridge_resource:bridge_id(BridgeV2Type, BridgeName),
            BridgeHookpoint = emqx_bridge_resource:bridge_hookpoint(BId),
            SourceHookpoint = source_hookpoint(BId),
            HookPoints = [BridgeHookpoint, SourceHookpoint],
            AugmentedConf#{hookpoints => HookPoints};
        false ->
            AugmentedConf
    end.

source_hookpoint(BridgeId) ->
    <<"$sources/", (bin(BridgeId))/binary>>.

uninstall_bridge_v2(
    _ConfRootKey,
    _BridgeType,
    _BridgeName,
    #{enable := false}
) ->
    %% Already not installed
    ok;
uninstall_bridge_v2(
    ConfRootKey,
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = Config
) ->
    BridgeV2Id = id_with_root_name(ConfRootKey, BridgeV2Type, BridgeName, ConnectorName),
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    ok = emqx_resource_buffer_worker_sup:stop_workers(BridgeV2Id, CreationOpts),
    case referenced_connectors_exist(BridgeV2Type, ConnectorName, BridgeName) of
        {error, _} ->
            ok;
        ok ->
            %% uninstall from connector
            ConnectorId = emqx_connector_resource:resource_id(
                connector_type(BridgeV2Type), ConnectorName
            ),
            Res = emqx_resource_manager:remove_channel(ConnectorId, BridgeV2Id),
            case Res of
                ok ->
                    ok = emqx_resource:clear_metrics(BridgeV2Id);
                _ ->
                    ok
            end,
            Res
    end.

combine_connector_and_bridge_v2_config(
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = BridgeV2Config
) ->
    ConnectorType = connector_type(BridgeV2Type),
    try emqx_config:get([connectors, ConnectorType, to_existing_atom(ConnectorName)]) of
        ConnectorConfig ->
            ConnectorCreationOpts = emqx_resource:fetch_creation_opts(ConnectorConfig),
            BridgeV2CreationOpts = emqx_resource:fetch_creation_opts(BridgeV2Config),
            CombinedCreationOpts = emqx_utils_maps:deep_merge(
                ConnectorCreationOpts,
                BridgeV2CreationOpts
            ),
            BridgeV2Config#{resource_opts => CombinedCreationOpts}
    catch
        _:_ ->
            alarm_connector_not_found(BridgeV2Type, BridgeName, ConnectorName),
            {error, #{
                reason => <<"connector_not_found_or_wrong_type">>,
                bridge_type => BridgeV2Type,
                bridge_name => BridgeName,
                connector_name => ConnectorName
            }}
    end.

%%====================================================================
%% Operations
%%====================================================================
-define(ENABLE_OR_DISABLE(A), (A =:= disable orelse A =:= enable)).

-spec disable_enable(disable | enable, bridge_v2_type(), bridge_v2_name()) ->
    {ok, any()} | {error, any()}.
disable_enable(Action, BridgeType, BridgeName) when ?ENABLE_OR_DISABLE(Action) ->
    disable_enable(?ROOT_KEY_ACTIONS, Action, BridgeType, BridgeName).

disable_enable(ConfRootKey, Action, BridgeType, BridgeName) when ?ENABLE_OR_DISABLE(Action) ->
    emqx_conf:update(
        [ConfRootKey, BridgeType, BridgeName],
        Action,
        #{override_to => cluster}
    ).

%% Manually start connector. This function can speed up reconnection when
%% waiting for auto reconnection. The function forwards the start request to
%% its connector. Returns ok if the status of the bridge is connected after
%% starting the connector. Returns {error, Reason} if the status of the bridge
%% is something else than connected after starting the connector or if an
%% error occurred when the connector was started.
-spec start(term(), term()) -> ok | {error, Reason :: term()}.
start(ActionOrSourceType, Name) ->
    start(?ROOT_KEY_ACTIONS, ActionOrSourceType, Name).

-spec start(root_cfg_key(), term(), term()) -> ok | {error, Reason :: term()}.
start(ConfRootKey, BridgeV2Type, Name) ->
    ConnectorOpFun = fun(ConnectorType, ConnectorName) ->
        emqx_connector_resource:start(ConnectorType, ConnectorName)
    end,
    connector_operation_helper(ConfRootKey, BridgeV2Type, Name, ConnectorOpFun, true).

connector_operation_helper(ConfRootKey, BridgeV2Type, Name, ConnectorOpFun, DoHealthCheck) ->
    connector_operation_helper_with_conf(
        ConfRootKey,
        BridgeV2Type,
        Name,
        lookup_conf(ConfRootKey, BridgeV2Type, Name),
        ConnectorOpFun,
        DoHealthCheck
    ).

connector_operation_helper_with_conf(
    _ConfRootKey,
    _BridgeV2Type,
    _Name,
    {error, _} = Error,
    _ConnectorOpFun,
    _DoHealthCheck
) ->
    Error;
connector_operation_helper_with_conf(
    _ConfRootKey,
    _BridgeV2Type,
    _Name,
    #{enable := false},
    _ConnectorOpFun,
    _DoHealthCheck
) ->
    ok;
connector_operation_helper_with_conf(
    ConfRootKey,
    BridgeV2Type,
    Name,
    #{connector := ConnectorName},
    ConnectorOpFun,
    DoHealthCheck
) ->
    ConnectorType = connector_type(BridgeV2Type),
    ConnectorOpFunResult = ConnectorOpFun(ConnectorType, ConnectorName),
    case {DoHealthCheck, ConnectorOpFunResult} of
        {false, _} ->
            ConnectorOpFunResult;
        {true, {error, Reason}} ->
            {error, Reason};
        {true, ok} ->
            case health_check(ConfRootKey, BridgeV2Type, Name) of
                #{status := connected} ->
                    ok;
                {error, Reason} ->
                    {error, Reason};
                #{status := Status, error := Reason} ->
                    Msg = io_lib:format(
                        "Connector started but bridge (~s:~s) is not connected. "
                        "Bridge Status: ~p, Error: ~p",
                        [bin(BridgeV2Type), bin(Name), Status, Reason]
                    ),
                    {error, iolist_to_binary(Msg)}
            end
    end.

-spec reset_metrics(bridge_v2_type(), bridge_v2_name()) -> ok | {error, not_found}.
reset_metrics(Type, Name) ->
    reset_metrics(?ROOT_KEY_ACTIONS, Type, Name).

reset_metrics(ConfRootKey, Type, Name) ->
    reset_metrics_helper(ConfRootKey, Type, Name, lookup_conf(ConfRootKey, Type, Name)).

reset_metrics_helper(_ConfRootKey, _Type, _Name, #{enable := false}) ->
    ok;
reset_metrics_helper(ConfRootKey, BridgeV2Type, BridgeName, #{connector := ConnectorName}) ->
    ResourceId = id_with_root_name(ConfRootKey, BridgeV2Type, BridgeName, ConnectorName),
    emqx_resource:reset_metrics(ResourceId);
reset_metrics_helper(_, _, _, _) ->
    {error, not_found}.

get_resource_query_mode(ActionType, Config) ->
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    ConnectorType = connector_type(ActionType),
    ResourceMod = emqx_connector_resource:connector_to_resource_type(ConnectorType),
    emqx_resource:query_mode(ResourceMod, Config, CreationOpts).

-spec query(bridge_v2_type(), bridge_v2_name(), Message :: term(), QueryOpts :: map()) ->
    term() | {error, term()}.
query(BridgeType, BridgeName, Message, QueryOpts0) ->
    case lookup_conf(BridgeType, BridgeName) of
        #{enable := true} = Config0 ->
            Config = combine_connector_and_bridge_v2_config(BridgeType, BridgeName, Config0),
            do_query_with_enabled_config(BridgeType, BridgeName, Message, QueryOpts0, Config);
        #{enable := false} ->
            {error, bridge_disabled};
        {error, bridge_not_found} ->
            %% race
            {error, bridge_not_found}
    end.

do_query_with_enabled_config(
    _BridgeType, _BridgeName, _Message, _QueryOpts0, {error, Reason} = Error
) ->
    ?SLOG(warning, Reason),
    Error;
do_query_with_enabled_config(
    BridgeType, BridgeName, Message, QueryOpts0, Config
) ->
    ConnectorName = maps:get(connector, Config),
    ConnectorType = emqx_action_info:action_type_to_connector_type(BridgeType),
    ConnectorResId = emqx_connector_resource:resource_id(ConnectorType, ConnectorName),
    QueryOpts = maps:merge(
        query_opts(BridgeType, Config),
        QueryOpts0#{connector_resource_id => ConnectorResId}
    ),
    BridgeV2Id = id(BridgeType, BridgeName),
    case Message of
        {send_message, Msg} ->
            emqx_resource:query(BridgeV2Id, {BridgeV2Id, Msg}, QueryOpts);
        Msg ->
            emqx_resource:query(BridgeV2Id, Msg, QueryOpts)
    end.

-spec send_message(bridge_v2_type(), bridge_v2_name(), Message :: term(), QueryOpts :: map()) ->
    term() | {error, term()}.
send_message(BridgeType, BridgeName, Message, QueryOpts0) ->
    query(BridgeType, BridgeName, {send_message, Message}, QueryOpts0).

query_opts(ActionOrSourceType, Config) ->
    ConnectorType = connector_type(ActionOrSourceType),
    Mod = emqx_connector_resource:connector_to_resource_type(ConnectorType),
    emqx_resource:get_query_opts(Mod, Config).

-spec health_check(BridgeType :: term(), BridgeName :: term()) ->
    #{status := emqx_resource:resource_status(), error := term()} | {error, Reason :: term()}.
health_check(BridgeType, BridgeName) ->
    health_check(?ROOT_KEY_ACTIONS, BridgeType, BridgeName).

health_check(ConfRootKey, BridgeType, BridgeName) ->
    case lookup_conf(ConfRootKey, BridgeType, BridgeName) of
        #{
            enable := true,
            connector := ConnectorName
        } ->
            ConnectorId = emqx_connector_resource:resource_id(
                connector_type(BridgeType), ConnectorName
            ),
            emqx_resource_manager:channel_health_check(
                ConnectorId, id_with_root_name(ConfRootKey, BridgeType, BridgeName, ConnectorName)
            );
        #{enable := false} ->
            {error, bridge_disabled};
        {error, bridge_not_found} ->
            %% race
            {error, bridge_not_found}
    end.

-spec create_dry_run(bridge_v2_type(), Config :: map()) -> ok | {error, term()}.
create_dry_run(Type, Conf) ->
    create_dry_run(?ROOT_KEY_ACTIONS, Type, Conf).

-spec create_dry_run(root_cfg_key(), bridge_v2_type(), Config :: map()) -> ok | {error, term()}.
create_dry_run(ConfRootKey, Type, Conf0) ->
    Conf1 = maps:without([<<"name">>], Conf0),
    TypeBin = bin(Type),
    ConfRootKeyBin = bin(ConfRootKey),
    RawConf = #{ConfRootKeyBin => #{TypeBin => #{<<"temp_name">> => Conf1}}},
    %% Check config
    try
        _ =
            hocon_tconf:check_plain(
                emqx_bridge_v2_schema,
                RawConf,
                #{atom_key => true, required => false}
            ),
        #{<<"connector">> := ConnectorName} = Conf1,
        %% Check that the connector exists and do the dry run if it exists
        ConnectorType = connector_type(Type),
        case emqx:get_raw_config([connectors, ConnectorType, ConnectorName], not_found) of
            not_found ->
                {error, iolist_to_binary(io_lib:format("Connector ~p not found", [ConnectorName]))};
            ConnectorRawConf ->
                create_dry_run_helper(
                    ensure_atom_root_key(ConfRootKey), Type, ConnectorRawConf, Conf1
                )
        end
    catch
        %% validation errors
        throw:Reason1 ->
            {error, Reason1}
    end.

create_dry_run_helper(ConfRootKey, BridgeV2Type, ConnectorRawConf, BridgeV2RawConf) ->
    BridgeName = ?PROBE_ID_NEW(),
    ConnectorType = connector_type(BridgeV2Type),
    OnReadyCallback =
        fun(ConnectorId) ->
            {_, ConnectorName} = emqx_connector_resource:parse_connector_id(ConnectorId),
            ChannelTestId = id(BridgeV2Type, BridgeName, ConnectorName),
            BridgeV2Conf0 = fill_defaults(
                BridgeV2Type,
                BridgeV2RawConf,
                bin(ConfRootKey),
                emqx_bridge_v2_schema,
                #{make_serializable => false}
            ),
            BridgeV2Conf = emqx_utils_maps:unsafe_atom_key_map(BridgeV2Conf0),
            AugmentedConf = augment_channel_config(
                ConfRootKey,
                BridgeV2Type,
                BridgeName,
                BridgeV2Conf
            ),
            %% We'll perform it ourselves to get the resulting status afterwards.
            Opts = #{perform_health_check => false},
            case
                emqx_resource_manager:add_channel(ConnectorId, ChannelTestId, AugmentedConf, Opts)
            of
                {error, Reason} ->
                    {error, Reason};
                ok ->
                    HealthCheckResult = emqx_resource_manager:channel_health_check(
                        ConnectorId, ChannelTestId
                    ),
                    case HealthCheckResult of
                        #{status := connected} ->
                            ok;
                        #{status := Status, error := Error} ->
                            {error, {Status, Error}}
                    end
            end
        end,
    emqx_connector_resource:create_dry_run(ConnectorType, ConnectorRawConf, OnReadyCallback).

-spec get_metrics(bridge_v2_type(), bridge_v2_name()) -> emqx_metrics_worker:metrics().
get_metrics(Type, Name) ->
    get_metrics(?ROOT_KEY_ACTIONS, Type, Name).

-spec get_metrics(root_cfg_key(), bridge_v2_type(), bridge_v2_name()) ->
    emqx_metrics_worker:metrics().
get_metrics(ConfRootKey, Type, Name) ->
    emqx_resource:get_metrics(id_with_root_name(ConfRootKey, Type, Name)).

%%====================================================================
%% On message publish hook (for local topics)
%%====================================================================

%% The following functions are more or less copied from emqx_bridge.erl

reload_message_publish_hook(Bridges) ->
    ok = unload_message_publish_hook(),
    ok = load_message_publish_hook(Bridges).

load_message_publish_hook() ->
    Bridges = emqx:get_config([?ROOT_KEY_ACTIONS], #{}),
    load_message_publish_hook(Bridges).

load_message_publish_hook(Bridges) ->
    lists:foreach(
        fun({Type, Bridge}) ->
            lists:foreach(
                fun({_Name, BridgeConf}) ->
                    do_load_message_publish_hook(Type, BridgeConf)
                end,
                maps:to_list(Bridge)
            )
        end,
        maps:to_list(Bridges)
    ).

do_load_message_publish_hook(_Type, #{local_topic := LocalTopic}) when is_binary(LocalTopic) ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_BRIDGE);
do_load_message_publish_hook(_Type, _Conf) ->
    ok.

unload_message_publish_hook() ->
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}).

on_message_publish(Message = #message{topic = Topic, flags = Flags}) ->
    case maps:get(sys, Flags, false) of
        false ->
            {Msg, _} = emqx_rule_events:eventmsg_publish(Message),
            send_to_matched_egress_bridges(Topic, Msg);
        true ->
            ok
    end,
    {ok, Message}.

send_to_matched_egress_bridges(Topic, Msg) ->
    MatchedBridgeIds = get_matched_egress_bridges(Topic),
    lists:foreach(
        fun({Type, Name}) ->
            try send_message(Type, Name, Msg, #{}) of
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "send_message_to_bridge_failed",
                        bridge_type => Type,
                        bridge_name => Name,
                        error => Reason
                    });
                _ ->
                    ok
            catch
                Err:Reason:ST ->
                    ?SLOG(error, #{
                        msg => "send_message_to_bridge_exception",
                        bridge_type => Type,
                        bridge_name => Name,
                        error => Err,
                        reason => Reason,
                        stacktrace => ST
                    })
            end
        end,
        MatchedBridgeIds
    ).

get_matched_egress_bridges(Topic) ->
    Bridges = emqx:get_config([?ROOT_KEY_ACTIONS], #{}),
    maps:fold(
        fun(BType, Conf, Acc0) ->
            maps:fold(
                fun(BName, BConf, Acc1) ->
                    get_matched_bridge_id(BType, BConf, Topic, BName, Acc1)
                end,
                Acc0,
                Conf
            )
        end,
        [],
        Bridges
    ).

get_matched_bridge_id(_BType, #{enable := false}, _Topic, _BName, Acc) ->
    Acc;
get_matched_bridge_id(BType, Conf, Topic, BName, Acc) ->
    case maps:get(local_topic, Conf, undefined) of
        undefined ->
            Acc;
        Filter ->
            do_get_matched_bridge_id(Topic, Filter, BType, BName, Acc)
    end.

do_get_matched_bridge_id(Topic, Filter, BType, BName, Acc) ->
    case emqx_topic:match(Topic, Filter) of
        true -> [{BType, BName} | Acc];
        false -> Acc
    end.

%%====================================================================
%% Convenience functions for connector implementations
%%====================================================================

parse_id(Id) ->
    case binary:split(Id, <<":">>, [global]) of
        [Type, Name] ->
            #{kind => undefined, type => Type, name => Name};
        [<<"action">>, Type, Name | _] ->
            #{kind => action, type => Type, name => Name};
        [<<"source">>, Type, Name | _] ->
            #{kind => source, type => Type, name => Name};
        _ ->
            error({error, iolist_to_binary(io_lib:format("Invalid id: ~p", [Id]))})
    end.

get_channels_for_connector(ConnectorId) ->
    Actions = get_channels_for_connector(?ROOT_KEY_ACTIONS, ConnectorId),
    Sources = get_channels_for_connector(?ROOT_KEY_SOURCES, ConnectorId),
    Actions ++ Sources.

get_channels_for_connector(SourcesOrActions, ConnectorId) ->
    try emqx_connector_resource:parse_connector_id(ConnectorId) of
        {ConnectorType, ConnectorName} ->
            RootConf = maps:keys(emqx:get_config([SourcesOrActions], #{})),
            RelevantBridgeV2Types = [
                Type
             || Type <- RootConf,
                connector_type(Type) =:= ConnectorType
            ],
            lists:flatten([
                get_channels_for_connector(SourcesOrActions, ConnectorName, BridgeV2Type)
             || BridgeV2Type <- RelevantBridgeV2Types
            ])
    catch
        _:_ ->
            %% ConnectorId is not a valid connector id so we assume the connector
            %% has no channels (e.g. it is a a connector for authn or authz)
            []
    end.

get_channels_for_connector(SourcesOrActions, ConnectorName, BridgeV2Type) ->
    BridgeV2s = emqx:get_config([SourcesOrActions, BridgeV2Type], #{}),
    [
        {
            id_with_root_name(SourcesOrActions, BridgeV2Type, Name, ConnectorName),
            augment_channel_config(SourcesOrActions, BridgeV2Type, Name, Conf)
        }
     || {Name, Conf} <- maps:to_list(BridgeV2s),
        bin(ConnectorName) =:= maps:get(connector, Conf, no_name)
    ].

%%====================================================================
%% ID related functions
%%====================================================================

id(BridgeType, BridgeName) ->
    id_with_root_name(?ROOT_KEY_ACTIONS, BridgeType, BridgeName).

id(BridgeType, BridgeName, ConnectorName) ->
    id_with_root_name(?ROOT_KEY_ACTIONS, BridgeType, BridgeName, ConnectorName).

source_id(BridgeType, BridgeName, ConnectorName) ->
    id_with_root_name(?ROOT_KEY_SOURCES, BridgeType, BridgeName, ConnectorName).

id_with_root_name(RootName, BridgeType, BridgeName) ->
    case lookup_conf(RootName, BridgeType, BridgeName) of
        #{connector := ConnectorName} ->
            id_with_root_name(RootName, BridgeType, BridgeName, ConnectorName);
        {error, Reason} ->
            throw(
                {action_source_not_found, #{
                    reason => Reason,
                    root_name => RootName,
                    type => BridgeType,
                    name => BridgeName
                }}
            )
    end.

id_with_root_name(RootName0, BridgeType, BridgeName, ConnectorName) ->
    RootName =
        case bin(RootName0) of
            <<"actions">> -> <<"action">>;
            <<"sources">> -> <<"source">>
        end,
    ConnectorType = bin(connector_type(BridgeType)),
    <<
        (bin(RootName))/binary,
        ":",
        (bin(BridgeType))/binary,
        ":",
        (bin(BridgeName))/binary,
        ":connector:",
        (bin(ConnectorType))/binary,
        ":",
        (bin(ConnectorName))/binary
    >>.

connector_type(Type) ->
    %% remote call so it can be mocked
    ?MODULE:bridge_v2_type_to_connector_type(Type).

bridge_v2_type_to_connector_type(Type) ->
    emqx_action_info:action_type_to_connector_type(Type).

%%====================================================================
%% Data backup API
%%====================================================================

import_config(RawConf) ->
    %% actions structure
    ActionRes = emqx_bridge:import_config(
        RawConf, <<"actions">>, ?ROOT_KEY_ACTIONS, config_key_path()
    ),
    SourceRes = emqx_bridge:import_config(
        RawConf, <<"sources">>, ?ROOT_KEY_SOURCES, config_key_path_sources()
    ),
    group_import_results([ActionRes, SourceRes]).

group_import_results(Results0) ->
    Results = lists:foldr(
        fun
            ({ok, OkRes}, {OkAcc, ErrAcc}) ->
                {[OkRes | OkAcc], ErrAcc};
            ({error, ErrRes}, {OkAcc, ErrAcc}) ->
                {OkAcc, [ErrRes | ErrAcc]}
        end,
        {[], []},
        Results0
    ),
    {results, Results}.

%%====================================================================
%% Config Update Handler API
%%====================================================================

config_key_path() ->
    [?ROOT_KEY_ACTIONS].

config_key_path_leaf() ->
    [?ROOT_KEY_ACTIONS, '?', '?'].

config_key_path_sources() ->
    [?ROOT_KEY_SOURCES].

config_key_path_leaf_sources() ->
    [?ROOT_KEY_SOURCES, '?', '?'].

%% enable or disable action
pre_config_update([ConfRootKey, _Type, _Name], Oper, undefined) when
    ?ENABLE_OR_DISABLE(Oper) andalso
        (ConfRootKey =:= ?ROOT_KEY_ACTIONS orelse ConfRootKey =:= ?ROOT_KEY_SOURCES)
->
    {error, bridge_not_found};
pre_config_update([ConfRootKey, _Type, _Name], Oper, OldAction) when
    ?ENABLE_OR_DISABLE(Oper) andalso
        (ConfRootKey =:= ?ROOT_KEY_ACTIONS orelse ConfRootKey =:= ?ROOT_KEY_SOURCES)
->
    {ok, OldAction#{<<"enable">> => operation_to_enable(Oper)}};
%% Updates a single action from a specific HTTP API.
%% If the connector is not found, the update operation fails.
pre_config_update([ConfRootKey, Type, Name], Conf = #{}, _OldConf) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS orelse ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    convert_from_connector(ConfRootKey, Type, Name, Conf);
%% Batch updates actions when importing a configuration or executing a CLI command.
%% Update succeeded even if the connector is not found, alarm in post_config_update
pre_config_update([ConfRootKey], Conf = #{}, _OldConfs) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS orelse ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    {ok, convert_from_connectors(ConfRootKey, Conf)}.

%% This top level handler will be triggered when the actions path is updated
%% with calls to emqx_conf:update([actions], BridgesConf, #{}).
post_config_update([ConfRootKey], _Req, NewConf, OldConf, _AppEnv) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS; ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    #{added := Added, removed := Removed, changed := Updated} =
        diff_confs(NewConf, OldConf),
    RemoveFun = fun(Type, Name, Conf) ->
        uninstall_bridge_v2(ConfRootKey, Type, Name, Conf)
    end,
    CreateFun = fun(Type, Name, Conf) ->
        install_bridge_v2(ConfRootKey, Type, Name, Conf)
    end,
    UpdateFun = fun(Type, Name, {OldBridgeConf, Conf}) ->
        uninstall_bridge_v2(ConfRootKey, Type, Name, OldBridgeConf),
        install_bridge_v2(ConfRootKey, Type, Name, Conf)
    end,
    Result = perform_bridge_changes([
        #{action => RemoveFun, action_name => remove, data => Removed},
        #{
            action => CreateFun,
            action_name => create,
            data => Added,
            on_exception_fn => fun emqx_bridge_resource:remove/4
        },
        #{action => UpdateFun, action_name => update, data => Updated}
    ]),
    reload_message_publish_hook(NewConf),
    ?tp(bridge_post_config_update_done, #{}),
    Result;
%% Don't crash even when the bridge is not found
post_config_update([ConfRootKey, Type, Name], '$remove', _, _OldConf, _AppEnvs) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS; ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    AllBridges = emqx:get_config([ConfRootKey]),
    case emqx_utils_maps:deep_get([Type, Name], AllBridges, undefined) of
        undefined ->
            ok;
        Action ->
            ok = uninstall_bridge_v2(ConfRootKey, Type, Name, Action),
            Bridges = emqx_utils_maps:deep_remove([Type, Name], AllBridges),
            reload_message_publish_hook(Bridges)
    end,
    ?tp(bridge_post_config_update_done, #{}),
    ok;
%% Create a single bridge fails if the connector is not found (already checked in pre_config_update)
post_config_update([ConfRootKey, BridgeType, BridgeName], _Req, NewConf, undefined, _AppEnvs) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS; ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    ok = install_bridge_v2(ConfRootKey, BridgeType, BridgeName, NewConf),
    Bridges = emqx_utils_maps:deep_put(
        [BridgeType, BridgeName], emqx:get_config([ConfRootKey]), NewConf
    ),
    reload_message_publish_hook(Bridges),
    ?tp(bridge_post_config_update_done, #{}),
    ok;
%% update bridges fails if the connector is not found (already checked in pre_config_update)
post_config_update([ConfRootKey, BridgeType, BridgeName], _Req, NewConf, OldConf, _AppEnvs) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS; ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    case uninstall_bridge_v2(ConfRootKey, BridgeType, BridgeName, OldConf) of
        ok ->
            ok;
        {error, timeout} ->
            ErrorContext = #{
                error => uninstall_timeout,
                bridge_kind => ConfRootKey,
                type => BridgeType,
                name => BridgeName,
                reason => <<
                    "Timed out trying to remove action or source.  Please try again and,"
                    " if the error persists, try disabling the connector before retrying."
                >>
            },
            throw(ErrorContext);
        {error, not_found} ->
            %% Should not happen, unless config is inconsistent.
            throw(<<"Referenced connector not found">>)
    end,
    ok = install_bridge_v2(ConfRootKey, BridgeType, BridgeName, NewConf),
    Bridges = emqx_utils_maps:deep_put(
        [BridgeType, BridgeName], emqx:get_config([ConfRootKey]), NewConf
    ),
    reload_message_publish_hook(Bridges),
    ?tp(bridge_post_config_update_done, #{}),
    ok.

diff_confs(NewConfs, OldConfs) ->
    emqx_utils_maps:diff_maps(
        flatten_confs(NewConfs),
        flatten_confs(OldConfs)
    ).

flatten_confs(Conf0) ->
    maps:from_list(
        lists:flatmap(
            fun({Type, Conf}) ->
                do_flatten_confs(Type, Conf)
            end,
            maps:to_list(Conf0)
        )
    ).

do_flatten_confs(Type, Conf0) ->
    [{{Type, Name}, Conf} || {Name, Conf} <- maps:to_list(Conf0)].

perform_bridge_changes(Tasks) ->
    perform_bridge_changes(Tasks, []).

perform_bridge_changes([], Errors) ->
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end;
perform_bridge_changes([#{action := Action, data := MapConfs} = Task | Tasks], Errors0) ->
    OnException = maps:get(on_exception_fn, Task, fun(_Type, _Name, _Conf, _Opts) -> ok end),
    Results = emqx_utils:pmap(
        fun({{Type, Name}, Conf}) ->
            Res =
                try
                    Action(Type, Name, Conf)
                catch
                    Kind:Error:Stacktrace ->
                        ?SLOG(error, #{
                            msg => "bridge_config_update_exception",
                            kind => Kind,
                            error => Error,
                            type => Type,
                            name => Name,
                            stacktrace => Stacktrace
                        }),
                        OnException(Type, Name, Conf),
                        {error, Error}
                end,
            {{Type, Name}, Res}
        end,
        maps:to_list(MapConfs),
        infinity
    ),
    Errs = lists:filter(
        fun
            ({_TypeName, {error, _}}) -> true;
            (_) -> false
        end,
        Results
    ),
    Errors =
        case Errs of
            [] ->
                Errors0;
            _ ->
                #{action_name := ActionName} = Task,
                [#{action => ActionName, errors => Errs} | Errors0]
        end,
    perform_bridge_changes(Tasks, Errors).

fill_defaults(Type, RawConf, TopLevelConf, SchemaModule) ->
    fill_defaults(Type, RawConf, TopLevelConf, SchemaModule, _Opts = #{}).

fill_defaults(Type, RawConf, TopLevelConf, SchemaModule, Opts) ->
    PackedConf = pack_bridge_conf(Type, RawConf, TopLevelConf),
    FullConf = emqx_config:fill_defaults(SchemaModule, PackedConf, Opts),
    unpack_bridge_conf(Type, FullConf, TopLevelConf).

pack_bridge_conf(Type, RawConf, TopLevelConf) ->
    #{TopLevelConf => #{bin(Type) => #{<<"foo">> => RawConf}}}.

unpack_bridge_conf(Type, PackedConf, TopLevelConf) ->
    TypeBin = bin(Type),
    #{TopLevelConf := Bridges} = PackedConf,
    #{<<"foo">> := RawConf} = maps:get(TypeBin, Bridges),
    RawConf.

%%====================================================================
%% Compatibility API
%%====================================================================

%% Check if the bridge can be converted to a valid bridge v1
%%
%% * The corresponding bridge v2 should exist
%% * The connector for the bridge v2 should have exactly one channel
bridge_v1_is_valid(BridgeV1Type, BridgeName) ->
    bridge_v1_is_valid(?ROOT_KEY_ACTIONS, BridgeV1Type, BridgeName).

%% rabbitmq's source don't have v1 version. but action has v1 version...
%% There's no good way to distinguish it, so it has to be hardcoded here.
bridge_v1_is_valid(?ROOT_KEY_SOURCES, rabbitmq, _BridgeName) ->
    false;
bridge_v1_is_valid(ConfRootKey, BridgeV1Type, BridgeName) ->
    BridgeV2Type = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    case lookup_conf(ConfRootKey, BridgeV2Type, BridgeName) of
        {error, _} ->
            %% If the bridge v2 does not exist, it is a valid bridge v1
            true;
        #{connector := ConnectorName} ->
            ConnectorType = connector_type(BridgeV2Type),
            ConnectorResourceId = emqx_connector_resource:resource_id(ConnectorType, ConnectorName),
            case emqx_resource:get_channels(ConnectorResourceId) of
                {ok, [_Channel]} -> true;
                %% not_found, [], [_|_]
                _ -> false
            end
    end.

bridge_v1_type_to_bridge_v2_type(Type) ->
    emqx_action_info:bridge_v1_type_to_action_type(Type).

bridge_v2_type_to_bridge_v1_type(ActionType, ActionConf) ->
    emqx_action_info:action_type_to_bridge_v1_type(ActionType, ActionConf).

is_bridge_v2_type(Type) ->
    emqx_action_info:is_action_type(Type).

bridge_v1_list_and_transform() ->
    BridgesFromActions0 = list_with_lookup_fun(
        ?ROOT_KEY_ACTIONS,
        fun bridge_v1_lookup_and_transform/2
    ),
    BridgesFromActions1 = [
        B
     || B <- BridgesFromActions0,
        B =/= not_bridge_v1_compatible_error()
    ],
    FromActionsNames = maps:from_keys([Name || #{name := Name} <- BridgesFromActions1], true),
    BridgesFromSources0 = list_with_lookup_fun(
        ?ROOT_KEY_SOURCES,
        fun bridge_v1_lookup_and_transform/2
    ),
    BridgesFromSources1 = [
        B
     || #{name := SourceBridgeName} = B <- BridgesFromSources0,
        B =/= not_bridge_v1_compatible_error(),
        %% Action is only shown in case of name conflict
        not maps:is_key(SourceBridgeName, FromActionsNames)
    ],
    BridgesFromActions1 ++ BridgesFromSources1.

bridge_v1_lookup_and_transform(ActionType, Name) ->
    case lookup_in_actions_or_sources(ActionType, Name) of
        {ok, ConfRootKey,
            #{raw_config := #{<<"connector">> := ConnectorName} = RawConfig} = ActionConfig} ->
            BridgeV1Type = ?MODULE:bridge_v2_type_to_bridge_v1_type(ActionType, RawConfig),
            HasBridgeV1Equivalent = has_bridge_v1_equivalent(ActionType),
            case
                HasBridgeV1Equivalent andalso
                    ?MODULE:bridge_v1_is_valid(ConfRootKey, BridgeV1Type, Name)
            of
                true ->
                    ConnectorType = connector_type(ActionType),
                    case emqx_connector:lookup(ConnectorType, ConnectorName) of
                        {ok, Connector} ->
                            bridge_v1_lookup_and_transform_helper(
                                ConfRootKey,
                                BridgeV1Type,
                                Name,
                                ActionType,
                                ActionConfig,
                                ConnectorType,
                                Connector
                            );
                        Error ->
                            Error
                    end;
                false ->
                    not_bridge_v1_compatible_error()
            end;
        Error ->
            Error
    end.

lookup_in_actions_or_sources(ActionType, Name) ->
    case lookup(?ROOT_KEY_ACTIONS, ActionType, Name) of
        {error, not_found} ->
            case lookup(?ROOT_KEY_SOURCES, ActionType, Name) of
                {ok, SourceInfo} ->
                    {ok, ?ROOT_KEY_SOURCES, SourceInfo};
                Error ->
                    Error
            end;
        {ok, ActionInfo} ->
            {ok, ?ROOT_KEY_ACTIONS, ActionInfo}
    end.

not_bridge_v1_compatible_error() ->
    {error, not_bridge_v1_compatible}.

has_bridge_v1_equivalent(ActionType) ->
    case emqx_action_info:bridge_v1_type_name(ActionType) of
        {ok, _} -> true;
        {error, no_v1_equivalent} -> false
    end.

connector_raw_config(Connector, ConnectorType) ->
    get_raw_with_defaults(Connector, ConnectorType, <<"connectors">>, emqx_connector_schema).

action_raw_config(ConfRootName, Action, ActionType) ->
    get_raw_with_defaults(Action, ActionType, bin(ConfRootName), emqx_bridge_v2_schema).

get_raw_with_defaults(Config, Type, TopLevelConf, SchemaModule) ->
    RawConfig = maps:get(raw_config, Config),
    fill_defaults(Type, RawConfig, TopLevelConf, SchemaModule).

bridge_v1_lookup_and_transform_helper(
    ConfRootName, BridgeV1Type, BridgeName, ActionType, Action, ConnectorType, Connector
) ->
    ConnectorRawConfig = connector_raw_config(Connector, ConnectorType),
    ActionRawConfig = action_raw_config(ConfRootName, Action, ActionType),
    BridgeV1Config = emqx_action_info:connector_action_config_to_bridge_v1_config(
        BridgeV1Type, ConnectorRawConfig, ActionRawConfig
    ),
    BridgeV1Tmp = maps:put(raw_config, BridgeV1Config, Action),
    BridgeV1 = maps:remove(status, BridgeV1Tmp),
    BridgeV2Status = maps:get(status, Action, undefined),
    BridgeV2Error = maps:get(error, Action, undefined),
    ResourceData1 = maps:get(resource_data, BridgeV1, #{}),
    %% Replace id in resource data
    BridgeV1Id = <<"bridge:", (bin(BridgeV1Type))/binary, ":", (bin(BridgeName))/binary>>,
    ResourceData2 = maps:put(id, BridgeV1Id, ResourceData1),
    ConnectorStatus = maps:get(status, ResourceData2, undefined),
    case ConnectorStatus of
        connected ->
            case BridgeV2Status of
                connected ->
                    %% No need to modify the status
                    {ok, BridgeV1#{resource_data => ResourceData2}};
                NotConnected ->
                    ResourceData3 = maps:put(status, NotConnected, ResourceData2),
                    ResourceData4 = maps:put(error, BridgeV2Error, ResourceData3),
                    BridgeV1Final = maps:put(resource_data, ResourceData4, BridgeV1),
                    {ok, BridgeV1Final}
            end;
        _ ->
            %% No need to modify the status
            {ok, BridgeV1#{resource_data => ResourceData2}}
    end.

lookup_conf(Type, Name) ->
    lookup_conf(?ROOT_KEY_ACTIONS, Type, Name).

lookup_conf_if_exists_in_exactly_one_of_sources_and_actions(Type, Name) ->
    LookUpConfActions = lookup_conf(?ROOT_KEY_ACTIONS, Type, Name),
    LookUpConfSources = lookup_conf(?ROOT_KEY_SOURCES, Type, Name),
    case {LookUpConfActions, LookUpConfSources} of
        {{error, bridge_not_found}, {error, bridge_not_found}} ->
            {error, bridge_not_found};
        {{error, bridge_not_found}, Conf} ->
            Conf;
        {Conf, {error, bridge_not_found}} ->
            Conf;
        {_Conf1, _Conf2} ->
            {error, name_conflict_sources_actions}
    end.

is_only_source(BridgeType, BridgeName) ->
    LookUpConfActions = lookup_conf(?ROOT_KEY_ACTIONS, BridgeType, BridgeName),
    LookUpConfSources = lookup_conf(?ROOT_KEY_SOURCES, BridgeType, BridgeName),
    case {LookUpConfActions, LookUpConfSources} of
        {{error, bridge_not_found}, {error, bridge_not_found}} ->
            false;
        {{error, bridge_not_found}, _Conf} ->
            true;
        {_Conf, {error, bridge_not_found}} ->
            false;
        {_Conf1, _Conf2} ->
            false
    end.

get_conf_root_key_if_only_one(BridgeType, BridgeName) ->
    LookUpConfActions = lookup_conf(?ROOT_KEY_ACTIONS, BridgeType, BridgeName),
    LookUpConfSources = lookup_conf(?ROOT_KEY_SOURCES, BridgeType, BridgeName),
    case {LookUpConfActions, LookUpConfSources} of
        {{error, bridge_not_found}, {error, bridge_not_found}} ->
            error({action_or_source_not_found, BridgeType, BridgeName});
        {{error, bridge_not_found}, _Conf} ->
            ?ROOT_KEY_SOURCES;
        {_Conf, {error, bridge_not_found}} ->
            ?ROOT_KEY_ACTIONS;
        {_Conf1, _Conf2} ->
            error({name_clash_action_source, BridgeType, BridgeName})
    end.

lookup_conf(RootName, Type, Name) ->
    case emqx:get_config([RootName, Type, Name], not_found) of
        not_found ->
            {error, bridge_not_found};
        Config ->
            Config
    end.

bridge_v1_split_config_and_create(BridgeV1Type, BridgeName, RawConf) ->
    BridgeV2Type = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    %% Check if the bridge v2 exists
    case lookup_conf_if_exists_in_exactly_one_of_sources_and_actions(BridgeV2Type, BridgeName) of
        {error, _} ->
            %% If the bridge v2 does not exist, it is a valid bridge v1
            PreviousRawConf = undefined,
            split_bridge_v1_config_and_create_helper(
                BridgeV1Type, BridgeName, RawConf, PreviousRawConf, fun() -> ok end
            );
        _Conf ->
            case ?MODULE:bridge_v1_is_valid(BridgeV1Type, BridgeName) of
                true ->
                    %% Using remove + create as update, hence do not delete deps.
                    RemoveDeps = [],
                    ConfRootKey = get_conf_root_key_if_only_one(BridgeV2Type, BridgeName),
                    PreviousRawConf = emqx:get_raw_config(
                        [ConfRootKey, BridgeV2Type, BridgeName], undefined
                    ),
                    %% To avoid losing configurations. We have to make sure that no crash occurs
                    %% during deletion and creation of configurations.
                    PreCreateFun = fun() ->
                        bridge_v1_check_deps_and_remove(BridgeV1Type, BridgeName, RemoveDeps)
                    end,
                    split_bridge_v1_config_and_create_helper(
                        BridgeV1Type, BridgeName, RawConf, PreviousRawConf, PreCreateFun
                    );
                false ->
                    %% If the bridge v2 exists, it is not a valid bridge v1
                    {error, non_compatible_bridge_v2_exists}
            end
    end.

split_bridge_v1_config_and_create_helper(
    BridgeV1Type, BridgeName, RawConf, PreviousRawConf, PreCreateFun
) ->
    try
        #{
            connector_type := ConnectorType,
            connector_name := NewConnectorName,
            connector_conf := NewConnectorRawConf,
            bridge_v2_type := BridgeType,
            bridge_v2_name := BridgeName,
            bridge_v2_conf := NewBridgeV2RawConf,
            conf_root_key := ConfRootName
        } = split_and_validate_bridge_v1_config(
            BridgeV1Type,
            BridgeName,
            RawConf,
            PreviousRawConf
        ),
        _ = PreCreateFun(),

        do_connector_and_bridge_create(
            ConfRootName,
            ConnectorType,
            NewConnectorName,
            NewConnectorRawConf,
            BridgeType,
            BridgeName,
            NewBridgeV2RawConf,
            RawConf
        )
    catch
        throw:Reason ->
            {error, Reason}
    end.

do_connector_and_bridge_create(
    ConfRootName,
    ConnectorType,
    NewConnectorName,
    NewConnectorRawConf,
    BridgeType,
    BridgeName,
    NewBridgeV2RawConf,
    RawConf
) ->
    case emqx_connector:create(ConnectorType, NewConnectorName, NewConnectorRawConf) of
        {ok, _} ->
            case create(ConfRootName, BridgeType, BridgeName, NewBridgeV2RawConf) of
                {ok, _} = Result ->
                    Result;
                {error, Reason1} ->
                    case emqx_connector:remove(ConnectorType, NewConnectorName) of
                        ok ->
                            {error, Reason1};
                        {error, Reason2} ->
                            ?SLOG(warning, #{
                                message => failed_to_remove_connector,
                                bridge_version => 2,
                                bridge_type => BridgeType,
                                bridge_name => BridgeName,
                                bridge_raw_config => emqx_utils:redact(RawConf)
                            }),
                            {error, Reason2}
                    end
            end;
        Error ->
            Error
    end.

split_and_validate_bridge_v1_config(BridgeV1Type, BridgeName, RawConf, PreviousRawConf) ->
    %% Create fake global config for the transformation and then call
    %% `emqx_connector_schema:transform_bridges_v1_to_connectors_and_bridges_v2/1'
    BridgeV2Type = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    ConnectorType = connector_type(BridgeV2Type),
    %% Needed to avoid name conflicts
    CurrentConnectorsConfig = emqx:get_raw_config([connectors], #{}),
    FakeGlobalConfig0 = #{
        <<"connectors">> => CurrentConnectorsConfig,
        <<"bridges">> => #{
            bin(BridgeV1Type) => #{
                bin(BridgeName) => RawConf
            }
        }
    },
    ConfRootKeyPrevRawConf =
        case PreviousRawConf =/= undefined of
            true -> get_conf_root_key_if_only_one(BridgeV2Type, BridgeName);
            false -> not_used
        end,
    FakeGlobalConfig =
        emqx_utils_maps:put_if(
            FakeGlobalConfig0,
            bin(ConfRootKeyPrevRawConf),
            #{bin(BridgeV2Type) => #{bin(BridgeName) => PreviousRawConf}},
            PreviousRawConf =/= undefined
        ),
    %% [FIXME] this will loop through all connector types, instead pass the
    %% connector type and just do it for that one
    Output = emqx_connector_schema:transform_bridges_v1_to_connectors_and_bridges_v2(
        FakeGlobalConfig
    ),
    ConfRootKey = get_conf_root_key(Output),
    NewBridgeV2RawConf =
        emqx_utils_maps:deep_get(
            [
                ConfRootKey,
                bin(BridgeV2Type),
                bin(BridgeName)
            ],
            Output
        ),
    ConnectorName = emqx_utils_maps:deep_get(
        [
            ConfRootKey,
            bin(BridgeV2Type),
            bin(BridgeName),
            <<"connector">>
        ],
        Output
    ),
    NewConnectorRawConf =
        emqx_utils_maps:deep_get(
            [
                <<"connectors">>,
                bin(ConnectorType),
                bin(ConnectorName)
            ],
            Output
        ),
    %% Validate the connector config and the bridge_v2 config
    NewFakeConnectorConfig = #{
        <<"connectors">> => #{
            bin(ConnectorType) => #{
                bin(ConnectorName) => NewConnectorRawConf
            }
        }
    },
    NewFakeBridgeV2Config = #{
        ConfRootKey => #{
            bin(BridgeV2Type) => #{
                bin(BridgeName) => NewBridgeV2RawConf
            }
        }
    },
    try
        _ = hocon_tconf:check_plain(
            emqx_connector_schema,
            NewFakeConnectorConfig,
            #{atom_key => false, required => false}
        ),
        _ = hocon_tconf:check_plain(
            emqx_bridge_v2_schema,
            NewFakeBridgeV2Config,
            #{atom_key => false, required => false}
        )
    of
        _ ->
            #{
                connector_type => ConnectorType,
                connector_name => ConnectorName,
                connector_conf => NewConnectorRawConf,
                bridge_v2_type => BridgeV2Type,
                bridge_v2_name => BridgeName,
                bridge_v2_conf => NewBridgeV2RawConf,
                conf_root_key => ConfRootKey
            }
    catch
        %% validation errors
        throw:{_Module, [Reason1 | _]} ->
            throw(Reason1);
        throw:Reason1 ->
            throw(Reason1)
    end.

get_conf_root_key(#{<<"actions">> := _}) ->
    <<"actions">>;
get_conf_root_key(#{<<"sources">> := _}) ->
    <<"sources">>;
get_conf_root_key(_NoMatch) ->
    error({incompatible_bridge_v1, no_action_or_source}).

bridge_v1_create_dry_run(BridgeType, RawConfig0) ->
    RawConf = maps:without([<<"name">>], RawConfig0),
    TmpName = ?PROBE_ID_NEW(),
    PreviousRawConf = undefined,
    try
        #{
            conf_root_key := ConfRootKey,
            connector_type := _ConnectorType,
            connector_name := _NewConnectorName,
            connector_conf := ConnectorRawConf,
            bridge_v2_type := BridgeV2Type,
            bridge_v2_name := _BridgeName,
            bridge_v2_conf := BridgeV2RawConf0
        } = split_and_validate_bridge_v1_config(BridgeType, TmpName, RawConf, PreviousRawConf),
        BridgeV2RawConf = emqx_action_info:action_convert_from_connector(
            BridgeType, ConnectorRawConf, BridgeV2RawConf0
        ),
        create_dry_run_helper(
            ensure_atom_root_key(ConfRootKey), BridgeV2Type, ConnectorRawConf, BridgeV2RawConf
        )
    catch
        throw:Reason ->
            {error, Reason}
    end.

%% Only called by test cases (may create broken references)
bridge_v1_remove(BridgeV1Type, BridgeName) ->
    ActionType = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    bridge_v1_remove(
        ActionType,
        BridgeName,
        lookup_conf_if_exists_in_exactly_one_of_sources_and_actions(ActionType, BridgeName)
    ).

bridge_v1_remove(
    ActionType,
    Name,
    #{connector := ConnectorName}
) ->
    ConfRootKey = get_conf_root_key_if_only_one(ActionType, Name),
    case remove(ConfRootKey, ActionType, Name) of
        ok ->
            ConnectorType = connector_type(ActionType),
            emqx_connector:remove(ConnectorType, ConnectorName);
        Error ->
            Error
    end;
bridge_v1_remove(
    _ActionType,
    _Name,
    Error
) ->
    Error.

bridge_v1_check_deps_and_remove(BridgeV1Type, BridgeName, RemoveDeps) ->
    BridgeV2Type = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    bridge_v1_check_deps_and_remove(
        BridgeV2Type,
        BridgeName,
        RemoveDeps,
        lookup_conf_if_exists_in_exactly_one_of_sources_and_actions(BridgeV2Type, BridgeName)
    ).

%% Bridge v1 delegated-removal in 3 steps:
%% 1. Delete rule actions if RemoveDeps has 'rule_actions'
%% 2. Delete self (the bridge v2), also delete its channel in the connector
%% 3. Delete the connector if the connector has no more channel left and if 'connector' is in RemoveDeps
bridge_v1_check_deps_and_remove(
    BridgeType,
    BridgeName,
    RemoveDeps,
    #{connector := ConnectorName}
) ->
    RemoveConnector = lists:member(connector, RemoveDeps),
    case maybe_withdraw_rule_action(BridgeType, BridgeName, RemoveDeps) of
        ok ->
            ConfRootKey = get_conf_root_key_if_only_one(BridgeType, BridgeName),
            case remove(ConfRootKey, BridgeType, BridgeName) of
                ok when RemoveConnector ->
                    maybe_delete_channels(BridgeType, BridgeName, ConnectorName);
                ok ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
bridge_v1_check_deps_and_remove(_BridgeType, _BridgeName, _RemoveDeps, Error) ->
    %% TODO: the connector is gone, for whatever reason, maybe call remove/2 anyway?
    Error.

maybe_withdraw_rule_action(BridgeType, BridgeName, RemoveDeps) ->
    case is_only_source(BridgeType, BridgeName) of
        true ->
            ok;
        false ->
            emqx_bridge_lib:maybe_withdraw_rule_action(BridgeType, BridgeName, RemoveDeps)
    end.

maybe_delete_channels(BridgeType, BridgeName, ConnectorName) ->
    case connector_has_channels(BridgeType, ConnectorName) of
        true ->
            ok;
        false ->
            ConnectorType = connector_type(BridgeType),
            case emqx_connector:remove(ConnectorType, ConnectorName) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => failed_to_delete_connector,
                        bridge_type => BridgeType,
                        bridge_name => BridgeName,
                        connector_name => ConnectorName,
                        reason => Reason
                    }),
                    {error, Reason}
            end
    end.

connector_has_channels(BridgeV2Type, ConnectorName) ->
    ConnectorType = connector_type(BridgeV2Type),
    case emqx_connector_resource:get_channels(ConnectorType, ConnectorName) of
        {ok, []} ->
            false;
        _ ->
            true
    end.

bridge_v1_id_to_connector_resource_id(BridgeId) ->
    bridge_v1_id_to_connector_resource_id(?ROOT_KEY_ACTIONS, BridgeId).

bridge_v1_id_to_connector_resource_id(ConfRootKey, BridgeId) ->
    case binary:split(BridgeId, <<":">>) of
        [Type, Name] ->
            BridgeV2Type = bin(bridge_v1_type_to_bridge_v2_type(Type)),
            ConnectorName =
                case lookup_conf(ConfRootKey, BridgeV2Type, Name) of
                    #{connector := Con} ->
                        Con;
                    {error, Reason} ->
                        throw(Reason)
                end,
            ConnectorType = bin(connector_type(BridgeV2Type)),
            <<"connector:", ConnectorType/binary, ":", ConnectorName/binary>>
    end.

bridge_v1_enable_disable(Action, BridgeType, BridgeName) ->
    case emqx_bridge_v2:bridge_v1_is_valid(BridgeType, BridgeName) of
        true ->
            bridge_v1_enable_disable_helper(
                Action,
                BridgeType,
                BridgeName,
                lookup_conf_if_exists_in_exactly_one_of_sources_and_actions(BridgeType, BridgeName)
            );
        false ->
            {error, not_bridge_v1_compatible}
    end.

bridge_v1_enable_disable_helper(_Op, _BridgeType, _BridgeName, {error, Reason}) ->
    {error, Reason};
bridge_v1_enable_disable_helper(enable, BridgeType, BridgeName, #{connector := ConnectorName}) ->
    BridgeV2Type = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeType),
    ConnectorType = connector_type(BridgeV2Type),
    {ok, _} = emqx_connector:disable_enable(enable, ConnectorType, ConnectorName),
    ConfRootKey = get_conf_root_key_if_only_one(BridgeType, BridgeName),
    emqx_bridge_v2:disable_enable(ConfRootKey, enable, BridgeV2Type, BridgeName);
bridge_v1_enable_disable_helper(disable, BridgeType, BridgeName, #{connector := ConnectorName}) ->
    BridgeV2Type = emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(BridgeType),
    ConnectorType = connector_type(BridgeV2Type),
    ConfRootKey = get_conf_root_key_if_only_one(BridgeType, BridgeName),
    {ok, _} = emqx_bridge_v2:disable_enable(ConfRootKey, disable, BridgeV2Type, BridgeName),
    emqx_connector:disable_enable(disable, ConnectorType, ConnectorName).

bridge_v1_restart(BridgeV1Type, Name) ->
    ConnectorOpFun = fun(ConnectorType, ConnectorName) ->
        emqx_connector_resource:restart(ConnectorType, ConnectorName)
    end,
    bridge_v1_operation_helper(BridgeV1Type, Name, ConnectorOpFun, true).

bridge_v1_stop(BridgeV1Type, Name) ->
    ConnectorOpFun = fun(ConnectorType, ConnectorName) ->
        emqx_connector_resource:stop(ConnectorType, ConnectorName)
    end,
    bridge_v1_operation_helper(BridgeV1Type, Name, ConnectorOpFun, false).

bridge_v1_start(BridgeV1Type, Name) ->
    ConnectorOpFun = fun(ConnectorType, ConnectorName) ->
        emqx_connector_resource:start(ConnectorType, ConnectorName)
    end,
    bridge_v1_operation_helper(BridgeV1Type, Name, ConnectorOpFun, true).

bridge_v1_operation_helper(BridgeV1Type, Name, ConnectorOpFun, DoHealthCheck) ->
    BridgeV2Type = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    case emqx_bridge_v2:bridge_v1_is_valid(BridgeV1Type, Name) of
        true ->
            ConfRootKey = get_conf_root_key_if_only_one(BridgeV2Type, Name),
            connector_operation_helper_with_conf(
                ConfRootKey,
                BridgeV2Type,
                Name,
                lookup_conf_if_exists_in_exactly_one_of_sources_and_actions(BridgeV2Type, Name),
                ConnectorOpFun,
                DoHealthCheck
            );
        false ->
            {error, not_bridge_v1_compatible}
    end.

bridge_v1_reset_metrics(BridgeV1Type, BridgeName) ->
    BridgeV2Type = bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    ConfRootKey = get_conf_root_key_if_only_one(
        BridgeV2Type, BridgeName
    ),
    ok = reset_metrics(ConfRootKey, BridgeV2Type, BridgeName).

%%====================================================================
%% Misc helper functions
%%====================================================================

operation_to_enable(disable) -> false;
operation_to_enable(enable) -> true.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

extract_connector_id_from_bridge_v2_id(Id) ->
    case binary:split(Id, <<":">>, [global]) of
        [<<"action">>, _Type, _Name, <<"connector">>, ConnectorType, ConnecorName] ->
            <<"connector:", ConnectorType/binary, ":", ConnecorName/binary>>;
        _X ->
            error({error, iolist_to_binary(io_lib:format("Invalid action ID: ~p", [Id]))})
    end.

ensure_atom_root_key(ConfRootKey) when is_atom(ConfRootKey) ->
    ConfRootKey;
ensure_atom_root_key(?ROOT_KEY_ACTIONS_BIN) ->
    ?ROOT_KEY_ACTIONS;
ensure_atom_root_key(?ROOT_KEY_SOURCES_BIN) ->
    ?ROOT_KEY_SOURCES.

to_existing_atom(X) ->
    case emqx_utils:safe_to_existing_atom(X, utf8) of
        {ok, A} -> A;
        {error, _} -> throw(bad_atom)
    end.

referenced_connectors_exist(BridgeType, ConnectorNameBin, BridgeName) ->
    %% N.B.: assumes that, for all bridgeV2 types, the name of the bridge type is
    %% identical to its matching connector type name.
    case get_connector_info(ConnectorNameBin, BridgeType) of
        {error, not_found} ->
            {error, #{
                reason => "connector_not_found_or_wrong_type",
                connector_name => ConnectorNameBin,
                bridge_name => BridgeName,
                bridge_type => BridgeType
            }};
        {ok, _Connector} ->
            ok
    end.

convert_from_connectors(ConfRootKey, Conf) ->
    maps:map(
        fun(ActionType, Actions) ->
            maps:map(
                fun(ActionName, Action) ->
                    case convert_from_connector(ConfRootKey, ActionType, ActionName, Action) of
                        {ok, NewAction} -> NewAction;
                        {error, _} -> Action
                    end
                end,
                Actions
            )
        end,
        Conf
    ).

convert_from_connector(ConfRootKey, Type, Name, Action = #{<<"connector">> := ConnectorName}) ->
    case get_connector_info(ConnectorName, Type) of
        {ok, Connector} ->
            TypeAtom = to_existing_atom(Type),
            Action1 = emqx_action_info:action_convert_from_connector(TypeAtom, Connector, Action),
            {ok, Action1};
        {error, not_found} ->
            {error, #{
                bridge_name => Name,
                reason => <<"connector_not_found_or_wrong_type">>,
                bridge_type => Type,
                connector_name => ConnectorName,
                conf_root_key => ConfRootKey
            }}
    end.

get_connector_info(ConnectorNameBin, BridgeType) ->
    case to_connector(ConnectorNameBin, BridgeType) of
        {error, not_found} ->
            {error, not_found};
        {ConnectorName, ConnectorType} ->
            case emqx_config:get_raw([connectors, ConnectorType, ConnectorName], undefined) of
                undefined -> {error, not_found};
                Connector -> {ok, Connector}
            end
    end.

to_connector(ConnectorNameBin, BridgeType) ->
    try
        ConnectorName = to_existing_atom(ConnectorNameBin),
        BridgeType1 = to_existing_atom(BridgeType),
        ConnectorType = ?MODULE:bridge_v2_type_to_connector_type(BridgeType1),
        {ConnectorName, ConnectorType}
    catch
        _:_ ->
            {error, not_found}
    end.

alarm_connector_not_found(ActionType, ActionName, ConnectorName) ->
    ConnectorType = connector_type(to_existing_atom(ActionType)),
    ResId = emqx_connector_resource:resource_id(
        ConnectorType, ConnectorName
    ),
    _ = emqx_alarm:safe_activate(
        ResId,
        #{
            connector_name => ConnectorName,
            connector_type => ConnectorType,
            action_type => ActionType,
            action_name => ActionName
        },
        <<"connector not found">>
    ).

now_ms() ->
    erlang:system_time(millisecond).
