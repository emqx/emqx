%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_v2).

-feature(maybe_expr, enable).

-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_resource/include/emqx_resource_id.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hocon.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%% Note: this is strange right now, because it lives in `emqx_bridge_v2', but it shall be
%% refactored into a new module/application with appropriate name.
-define(ROOT_KEY_ACTIONS, actions).
-define(ROOT_KEY_ACTIONS_BIN, <<"actions">>).
-define(ROOT_KEY_SOURCES, sources).
-define(ROOT_KEY_SOURCES_BIN, <<"sources">>).

-define(ENABLE_OR_DISABLE(A), (A =:= disable orelse A =:= enable)).

%% Loading and unloading config when EMQX starts and stops
-export([
    load/0,
    unload/0
]).

%% CRUD API

-export([
    list/2,
    lookup/4,
    lookup_raw_conf/4,
    is_exist/4,
    create/5,
    %% The remove/4 function is only for internal use as it may create
    %% rules with broken dependencies
    remove/4,
    %% The following is the remove function that is called by the HTTP API
    %% It also checks for rule action dependencies and optionally removes
    %% them
    check_deps_and_remove/5
]).
-export([is_action_exist/3, is_source_exist/3]).

%% Operations

-export([
    disable_enable/5,
    send_message/5,
    query/5,
    start/4,
    reset_metrics/4,
    create_dry_run/4,
    get_metrics/4
]).

%% Convenience functions for connector implementations

-export([
    parse_id/1,
    get_resource_ids/4,
    get_channels_for_connector/1
]).

-export([diff_confs/2]).

%% Internal export for buffer worker (and tests)
-export([lookup_chan_id_in_conf/4]).

%% Exported for tests
-export([
    id_with_root_and_connector_names/5,
    health_check/4,
    source_id/3,
    source_hookpoint/1,
    extract_connector_id_from_bridge_v2_id/1
]).

%% Config Update Handler API

-export([
    pre_config_update/4,
    post_config_update/6
]).

%% `emqx_config_backup` API
-export([
    import_config/2
]).

%% Bridge V2 Types and Conversions

-export([
    bridge_v2_type_to_connector_type/1,
    connector_type/1
]).

%% `emqx_telemetry` callback
-export([get_basic_usage_info/0]).

%%====================================================================
%% Types
%%====================================================================

-type namespace() :: binary().

-type bridge_v2_info() :: #{
    namespace := maybe_namespace(),
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

-type maybe_namespace() :: emqx_config:maybe_namespace().

-export_type([root_cfg_key/0, bridge_v2_type/0, bridge_v2_name/0, maybe_namespace/0]).

%%====================================================================
%% Loading and unloading config when EMQX starts and stops
%%====================================================================

load() ->
    load_bridges(?ROOT_KEY_ACTIONS),
    load_bridges(?ROOT_KEY_SOURCES),
    ok = emqx_config_handler:add_handler(config_key_path_leaf(), emqx_bridge_v2),
    ok = emqx_config_handler:add_handler(config_key_path(), emqx_bridge_v2),
    ok = emqx_config_handler:add_handler(config_key_path_leaf_sources(), emqx_bridge_v2),
    ok = emqx_config_handler:add_handler(config_key_path_sources(), emqx_bridge_v2),
    ok.

load_bridges(RootName) ->
    NamespaceToRoots = get_root_config_from_all_namespaces(RootName, #{}),
    emqx_utils:pforeach(
        fun({Namespace, RootConfig}) ->
            do_load_bridges(Namespace, RootName, RootConfig)
        end,
        maps:to_list(NamespaceToRoots),
        infinity
    ),
    ok.

do_load_bridges(Namespace, RootName, RootConfig) ->
    emqx_utils:pforeach(
        fun
            ({?COMPUTED, _}) ->
                ok;
            ({Type, Bridge}) ->
                emqx_utils:pmap(
                    fun({Name, BridgeConf}) ->
                        install_bridge_v2(Namespace, RootName, Type, Name, BridgeConf)
                    end,
                    maps:to_list(Bridge),
                    infinity
                )
        end,
        maps:to_list(RootConfig),
        infinity
    ).

unload() ->
    unload_bridges(?ROOT_KEY_ACTIONS),
    unload_bridges(?ROOT_KEY_SOURCES),
    emqx_conf:remove_handler(config_key_path()),
    emqx_conf:remove_handler(config_key_path_leaf()),
    ok.

unload_bridges(RootName) ->
    NamespaceToRoots = get_root_config_from_all_namespaces(RootName, #{}),
    emqx_utils:pforeach(
        fun({Namespace, RootConfig}) ->
            do_unload_bridges(Namespace, RootName, RootConfig)
        end,
        maps:to_list(NamespaceToRoots),
        infinity
    ),
    ok.

do_unload_bridges(Namespace, RootName, RootConfig) ->
    emqx_utils:pforeach(
        fun
            ({?COMPUTED, _}) ->
                ok;
            ({Type, Bridge}) ->
                emqx_utils:pmap(
                    fun({Name, BridgeConf}) ->
                        uninstall_bridge_v2(Namespace, RootName, Type, Name, BridgeConf)
                    end,
                    maps:to_list(Bridge),
                    infinity
                )
        end,
        maps:to_list(RootConfig),
        infinity
    ),
    ok.

%%====================================================================
%% CRUD API
%%====================================================================

is_action_exist(Namespace, Type, Name) ->
    is_exist(Namespace, ?ROOT_KEY_ACTIONS, Type, Name).

is_source_exist(Namespace, Type, Name) ->
    is_exist(Namespace, ?ROOT_KEY_SOURCES, Type, Name).

is_exist(Namespace, ConfRootName, Type, Name) ->
    {error, not_found} =/= lookup_raw_conf(Namespace, ConfRootName, Type, Name).

lookup_raw_conf(Namespace, ConfRootName, Type, Name) ->
    case get_raw_config(Namespace, [ConfRootName, Type, Name], not_found) of
        not_found ->
            {error, not_found};
        #{<<"connector">> := _} = RawConf ->
            {ok, RawConf}
    end.

-spec lookup(maybe_namespace(), root_cfg_key(), bridge_v2_type(), bridge_v2_name()) ->
    {ok, bridge_v2_info()} | {error, not_found}.
lookup(Namespace, ConfRootName, Type, Name) ->
    case get_raw_config(Namespace, [ConfRootName, Type, Name], not_found) of
        not_found ->
            {error, not_found};
        #{<<"connector">> := ConnectorName} = RawConf ->
            ConnectorId = emqx_connector_resource:resource_id(
                Namespace, connector_type(Type), ConnectorName
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
            BridgeV2Id = id_with_root_and_connector_names(
                Namespace, ConfRootName, Type, Name, ConnectorName
            ),
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
                namespace => Namespace,
                type => bin(Type),
                name => bin(Name),
                raw_config => RawConf,
                resource_data => ConnectorData,
                status => DisplayBridgeV2Status,
                error => ErrorMsg
            }}
    end.

-spec list(all | maybe_namespace(), root_cfg_key()) -> [bridge_v2_info()] | {error, term()}.
list(all, ConfRootKey) ->
    Namespaces = [?global_ns | emqx_config:get_all_namespaces_containing(ConfRootKey)],
    lists:flatmap(fun(Ns) -> list(Ns, ConfRootKey) end, Namespaces);
list(Namespace, ConfRootKey) ->
    LookupFun = fun(Type, Name) ->
        lookup(Namespace, ConfRootKey, Type, Name)
    end,
    list_with_lookup_fun(Namespace, ConfRootKey, LookupFun).

-spec create(maybe_namespace(), root_cfg_key(), bridge_v2_type(), bridge_v2_name(), map()) ->
    {ok, emqx_config:update_result()} | {error, any()}.
create(Namespace, ConfRootKey, BridgeType, BridgeName, RawConf0) ->
    ?SLOG(debug, #{
        namespace => Namespace,
        bridge_action => create,
        bridge_version => 2,
        bridge_type => BridgeType,
        bridge_name => BridgeName,
        bridge_raw_config => emqx_utils:redact(RawConf0),
        root_key_path => ConfRootKey
    }),
    RawConf1 = ensure_created_at(RawConf0),
    RawConf = ensure_last_modified_at(RawConf1),
    emqx_conf:update(
        [ConfRootKey, BridgeType, BridgeName],
        RawConf,
        with_namespace(#{override_to => cluster}, Namespace)
    ).

-spec remove(maybe_namespace(), root_cfg_key(), bridge_v2_type(), bridge_v2_name()) ->
    ok | {error, any()}.
remove(Namespace, ConfRootKey, BridgeType, BridgeName) ->
    ?SLOG(debug, #{
        bridge_action => remove,
        bridge_version => 2,
        namespace => Namespace,
        bridge_type => BridgeType,
        bridge_name => BridgeName
    }),
    Res = emqx_conf:remove(
        [ConfRootKey, BridgeType, BridgeName],
        with_namespace(#{override_to => cluster}, Namespace)
    ),
    case Res of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec check_deps_and_remove(
    maybe_namespace(), root_cfg_key(), bridge_v2_type(), bridge_v2_name(), boolean()
) ->
    ok | {error, any()}.
check_deps_and_remove(Namespace, ConfRootKey, BridgeType, BridgeName, AlsoDeleteActions) ->
    AlsoDelete =
        case AlsoDeleteActions of
            true -> [rule_actions];
            false -> []
        end,
    case
        maybe_withdraw_rule_action(
            Namespace,
            ConfRootKey,
            BridgeType,
            BridgeName,
            AlsoDelete
        )
    of
        ok ->
            remove(Namespace, ConfRootKey, BridgeType, BridgeName);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Helpers for CRUD API
%%--------------------------------------------------------------------

maybe_withdraw_rule_action(Namespace, ConfRootKey, BridgeType, BridgeName, DeleteDeps) ->
    BridgeIds = [emqx_bridge_resource:bridge_id(BridgeType, BridgeName)],
    DeleteActions = lists:member(rule_actions, DeleteDeps),
    GetFn =
        case ConfRootKey of
            ?ROOT_KEY_ACTIONS ->
                fun(Id) ->
                    emqx_rule_engine:get_rule_ids_by_bridge_action(Namespace, Id)
                end;
            ?ROOT_KEY_SOURCES ->
                fun(Id) ->
                    emqx_rule_engine:get_rule_ids_by_bridge_source(Namespace, Id)
                end
        end,
    maybe_withdraw_rule_action_loop(BridgeIds, DeleteActions, Namespace, GetFn).

maybe_withdraw_rule_action_loop([], _DeleteActions, _Namespace, _GetFn) ->
    ok;
maybe_withdraw_rule_action_loop([BridgeId | More], DeleteActions, Namespace, GetFn) ->
    case GetFn(BridgeId) of
        [] ->
            maybe_withdraw_rule_action_loop(More, DeleteActions, Namespace, GetFn);
        RuleIds when DeleteActions ->
            lists:foreach(
                fun(R) ->
                    emqx_rule_engine:ensure_action_removed(Namespace, R, BridgeId)
                end,
                RuleIds
            ),
            maybe_withdraw_rule_action_loop(More, DeleteActions, Namespace, GetFn);
        RuleIds ->
            {error, #{
                reason => rules_depending_on_this_bridge,
                bridge_id => BridgeId,
                rule_ids => RuleIds
            }}
    end.

list_with_lookup_fun(Namespace, ConfRootName, LookupFun) ->
    maps:fold(
        fun(Type, NameAndConf, Bridges) ->
            maps:fold(
                fun(Name, _RawConf, Acc) ->
                    [
                        begin
                            case LookupFun(Type, Name) of
                                {ok, BridgeInfo} ->
                                    BridgeInfo;
                                Err ->
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
        get_raw_config(Namespace, [ConfRootName], #{})
    ).

install_bridge_v2(
    _Namespace,
    _RootName,
    _BridgeType,
    _BridgeName,
    #{enable := false}
) ->
    ok;
install_bridge_v2(
    Namespace,
    RootName,
    BridgeV2Type,
    BridgeName,
    Config
) ->
    CombinedConfig = combine_connector_and_bridge_v2_config(
        Namespace,
        BridgeV2Type,
        BridgeName,
        Config
    ),
    case CombinedConfig of
        {error, Reason} = Error ->
            ?SLOG(warning, Reason),
            Error;
        #{} ->
            install_bridge_v2_helper(
                Namespace,
                RootName,
                BridgeV2Type,
                BridgeName,
                CombinedConfig
            )
    end.

install_bridge_v2_helper(
    Namespace,
    RootName,
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = Config
) ->
    BridgeV2Id = id_with_root_and_connector_names(
        Namespace, RootName, BridgeV2Type, BridgeName, ConnectorName
    ),
    CreationOpts0 = emqx_resource:fetch_creation_opts(Config),
    CreationOpts = CreationOpts0#{namespace => Namespace},
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
        Namespace, connector_type(BridgeV2Type), ConnectorName
    ),
    _ = emqx_resource_manager:add_channel_async(
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
    _Namespace,
    _ConfRootKey,
    _BridgeType,
    _BridgeName,
    #{enable := false}
) ->
    %% Already not installed
    ok;
uninstall_bridge_v2(
    Namespace,
    ConfRootKey,
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = Config
) ->
    BridgeV2Id = id_with_root_and_connector_names(
        Namespace, ConfRootKey, BridgeV2Type, BridgeName, ConnectorName
    ),
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    ok = emqx_resource_buffer_worker_sup:stop_workers(BridgeV2Id, CreationOpts),
    case referenced_connectors_exist(Namespace, BridgeV2Type, ConnectorName, BridgeName) of
        {error, _} ->
            ok;
        ok ->
            %% uninstall from connector
            ConnectorId = emqx_connector_resource:resource_id(
                Namespace, connector_type(BridgeV2Type), ConnectorName
            ),
            emqx_resource_manager:remove_channel_async(ConnectorId, BridgeV2Id)
    end.

combine_connector_and_bridge_v2_config(
    Namespace,
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = BridgeV2Config
) ->
    ConnectorType = connector_type(BridgeV2Type),
    ConnectorConfig =
        try
            get_config(
                Namespace,
                [connectors, ConnectorType, to_existing_atom(ConnectorName)],
                undefined
            )
        catch
            _:_ ->
                %% inexistent atom = unknown name
                undefined
        end,
    case ConnectorConfig of
        undefined ->
            alarm_connector_not_found(Namespace, BridgeV2Type, BridgeName, ConnectorName),
            {error, #{
                reason => <<"connector_not_found_or_wrong_type">>,
                namespace => Namespace,
                bridge_type => BridgeV2Type,
                bridge_name => BridgeName,
                connector_name => ConnectorName
            }};
        #{} ->
            ConnectorCreationOpts = emqx_resource:fetch_creation_opts(ConnectorConfig),
            BridgeV2CreationOpts = emqx_resource:fetch_creation_opts(BridgeV2Config),
            CombinedCreationOpts0 = emqx_utils_maps:deep_merge(
                ConnectorCreationOpts,
                BridgeV2CreationOpts
            ),
            CombinedCreationOpts = remove_connector_only_resource_opts(CombinedCreationOpts0),
            BridgeV2Config#{resource_opts => CombinedCreationOpts}
    end.

remove_connector_only_resource_opts(ResourceOpts) ->
    maps:without([start_after_created, start_timeout], ResourceOpts).

%%====================================================================
%% Operations
%%====================================================================

-spec disable_enable(
    maybe_namespace(), root_cfg_key(), disable | enable, bridge_v2_type(), bridge_v2_name()
) ->
    {ok, any()} | {error, any()}.
disable_enable(Namespace, ConfRootKey, EnableOrDisable, BridgeType, BridgeName) when
    ?ENABLE_OR_DISABLE(EnableOrDisable)
->
    emqx_conf:update(
        [ConfRootKey, BridgeType, BridgeName],
        {EnableOrDisable, #{now => now_ms()}},
        with_namespace(#{override_to => cluster}, Namespace)
    ).

%% Manually start connector. This function can speed up reconnection when
%% waiting for auto reconnection. The function forwards the start request to
%% its connector. Returns ok if the status of the bridge is connected after
%% starting the connector. Returns {error, Reason} if the status of the bridge
%% is something else than connected after starting the connector or if an
%% error occurred when the connector was started.
-spec start(maybe_namespace(), root_cfg_key(), term(), term()) -> ok | {error, Reason :: term()}.
start(Namespace, ConfRootKey, BridgeV2Type, Name) ->
    ConnectorOpFun = fun(ConnectorType, ConnectorName) ->
        emqx_connector_resource:start(Namespace, ConnectorType, ConnectorName)
    end,
    connector_operation_helper(Namespace, ConfRootKey, BridgeV2Type, Name, ConnectorOpFun).

connector_operation_helper(
    Namespace, ConfRootKey, BridgeV2Type, Name, ConnectorOpFun
) ->
    maybe
        #{} = PreviousConf ?= lookup_conf(Namespace, ConfRootKey, BridgeV2Type, Name),
        connector_operation_helper_with_conf(
            Namespace,
            ConfRootKey,
            BridgeV2Type,
            Name,
            PreviousConf,
            ConnectorOpFun
        )
    end.

connector_operation_helper_with_conf(
    _Namespace,
    _ConfRootKey,
    _BridgeV2Type,
    _Name,
    #{enable := false},
    _ConnectorOpFun
) ->
    ok;
connector_operation_helper_with_conf(
    Namespace,
    ConfRootKey,
    BridgeV2Type,
    Name,
    #{connector := ConnectorName},
    ConnectorOpFun
) ->
    ConnectorType = connector_type(BridgeV2Type),
    ConnectorOpFunResult = ConnectorOpFun(ConnectorType, ConnectorName),
    case ConnectorOpFunResult of
        {error, Reason} ->
            {error, Reason};
        ok ->
            case health_check(Namespace, ConfRootKey, BridgeV2Type, Name) of
                #{status := ?status_connected} ->
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

reset_metrics(Namespace, ConfRootKey, Type, Name) ->
    case lookup_conf(Namespace, ConfRootKey, Type, Name) of
        #{} = PreviousConf ->
            reset_metrics_helper(Namespace, ConfRootKey, Type, Name, PreviousConf);
        {error, bridge_not_found} ->
            {error, not_found}
    end.

reset_metrics_helper(_Namespace, _ConfRootKey, _Type, _Name, #{enable := false}) ->
    ok;
reset_metrics_helper(Namespace, ConfRootKey, BridgeV2Type, BridgeName, #{connector := ConnectorName}) ->
    ResourceId = id_with_root_and_connector_names(
        Namespace, ConfRootKey, BridgeV2Type, BridgeName, ConnectorName
    ),
    emqx_resource:reset_metrics(ResourceId).

get_resource_query_mode(ActionType, Config) ->
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    ConnectorType = connector_type(ActionType),
    ResourceMod = emqx_connector_resource:connector_to_resource_type(ConnectorType),
    emqx_resource:query_mode(ResourceMod, Config, CreationOpts).

-spec query(
    maybe_namespace(), bridge_v2_type(), bridge_v2_name(), Message :: term(), QueryOpts :: map()
) ->
    term() | {error, term()}.
query(Namespace, Type, Name, Message, QueryOpts0) ->
    case lookup_conf(Namespace, ?ROOT_KEY_ACTIONS, Type, Name) of
        #{enable := true} = Config0 ->
            Config = combine_connector_and_bridge_v2_config(
                Namespace, Type, Name, Config0
            ),
            case Config of
                {error, Reason} ->
                    ?SLOG(warning, Reason),
                    {error, Reason};
                #{} ->
                    do_query_with_enabled_config(Namespace, Type, Name, Message, QueryOpts0, Config)
            end;
        #{enable := false} ->
            {error, bridge_disabled};
        {error, bridge_not_found} ->
            %% race or bad configuration (e.g.: referenced fallback action does not exist)
            {error, bridge_not_found}
    end.

do_query_with_enabled_config(
    Namespace, Type, Name, Message, QueryOpts0, Config
) ->
    ConnectorName = maps:get(connector, Config),
    FallbackActions = maps:get(fallback_actions, Config, []),
    ConnectorType = emqx_action_info:action_type_to_connector_type(Type),
    ConnectorResId = emqx_connector_resource:resource_id(Namespace, ConnectorType, ConnectorName),
    QueryOpts = maps:merge(
        query_opts(Type, Config),
        QueryOpts0#{
            connector_resource_id => ConnectorResId,
            fallback_actions => FallbackActions
        }
    ),
    BridgeV2Id = id_with_root_and_connector_names(
        Namespace, ?ROOT_KEY_ACTIONS, Type, Name, ConnectorName
    ),
    case Message of
        {send_message, Msg} ->
            emqx_resource:query(BridgeV2Id, {BridgeV2Id, Msg}, QueryOpts);
        Msg ->
            emqx_resource:query(BridgeV2Id, Msg, QueryOpts)
    end.

-spec send_message(
    maybe_namespace(), bridge_v2_type(), bridge_v2_name(), Message :: term(), QueryOpts :: map()
) ->
    term() | {error, term()}.
send_message(Namespace, Type, Name, Message, QueryOpts0) ->
    query(Namespace, Type, Name, {send_message, Message}, QueryOpts0).

query_opts(ActionOrSourceType, Config) ->
    ConnectorType = connector_type(ActionOrSourceType),
    Mod = emqx_connector_resource:connector_to_resource_type(ConnectorType),
    emqx_resource:get_query_opts(Mod, Config).

%% N.B.: This ONLY for tests; actual health checks should be triggered by timers in the
%% process.  Avoid doing manual health checks outside tests.
health_check(Namespace, ConfRootKey, BridgeType, BridgeName) ->
    case lookup_conf(Namespace, ConfRootKey, BridgeType, BridgeName) of
        #{
            enable := true,
            connector := ConnectorName
        } ->
            ConnectorId = emqx_connector_resource:resource_id(
                Namespace, connector_type(BridgeType), ConnectorName
            ),
            ChannelId = id_with_root_and_connector_names(
                Namespace, ConfRootKey, BridgeType, BridgeName, ConnectorName
            ),
            emqx_resource_manager:channel_health_check(ConnectorId, ChannelId);
        #{enable := false} ->
            {error, bridge_disabled};
        {error, bridge_not_found} ->
            %% race
            {error, bridge_not_found}
    end.

-spec create_dry_run(maybe_namespace(), root_cfg_key(), bridge_v2_type(), Config :: map()) ->
    ok | {error, term()}.
create_dry_run(Namespace, ConfRootKey, Type, Conf0) ->
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
        case get_raw_config(Namespace, [connectors, ConnectorType, ConnectorName], not_found) of
            not_found ->
                {error, iolist_to_binary(io_lib:format("Connector ~p not found", [ConnectorName]))};
            ConnectorRawConf ->
                create_dry_run_helper(
                    Namespace, ensure_atom_root_key(ConfRootKey), Type, ConnectorRawConf, Conf1
                )
        end
    catch
        %% validation errors
        throw:Reason1 ->
            {error, Reason1}
    end.

create_dry_run_helper(Namespace, ConfRootKey, BridgeV2Type, ConnectorRawConf, BridgeV2RawConf) ->
    BridgeName = ?PROBE_ID_NEW(),
    ConnectorType = connector_type(BridgeV2Type),
    OnReadyCallback =
        fun(ConnectorId) ->
            #{name := ConnectorName} = emqx_connector_resource:parse_connector_id(ConnectorId),
            ChannelTestId = id_with_root_and_connector_names(
                Namespace, ConfRootKey, BridgeV2Type, BridgeName, ConnectorName
            ),
            BridgeV2Conf0 = fill_defaults(
                BridgeV2Type,
                BridgeV2RawConf,
                bin(ConfRootKey),
                emqx_bridge_v2_schema,
                #{make_serializable => false}
            ),
            BridgeV2Conf1 = maps:remove(?COMPUTED, BridgeV2Conf0),
            BridgeV2Conf = emqx_utils_maps:unsafe_atom_key_map(BridgeV2Conf1),
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
                        #{status := ?status_connected} ->
                            ok;
                        #{status := Status, error := Error} ->
                            {error, {Status, Error}}
                    end
            end
        end,
    emqx_connector_resource:create_dry_run(ConnectorType, ConnectorRawConf, OnReadyCallback).

get_metrics(Namespace, ConfRootKey, Type, Name) ->
    ChanResId = lookup_chan_id_in_conf(Namespace, ConfRootKey, Type, Name),
    emqx_resource:get_metrics(ChanResId).

%%====================================================================
%% Convenience functions for connector implementations
%%====================================================================

-doc """
Parses a binary action or source resource id into a structured map.

Throws `{invalid_id, Id}` if the input `Id` has an invalid format.

See [`emqx_resource:parse_channel_id/1`].
""".
-doc #{equiv => "emqx_resource:parse_channel_id/1"}.
-spec parse_id(binary()) ->
    #{
        namespace := ?global_ns | namespace(),
        kind := action | source,
        type := binary(),
        name := binary(),
        connector_type := binary(),
        connector_name := binary()
    }.
parse_id(Id) ->
    emqx_resource:parse_channel_id(Id).

get_channels_for_connector(ConnectorId) ->
    Actions = get_channels_for_connector(?ROOT_KEY_ACTIONS, ConnectorId),
    Sources = get_channels_for_connector(?ROOT_KEY_SOURCES, ConnectorId),
    Actions ++ Sources.

get_channels_for_connector(SourcesOrActions, ConnectorId) ->
    try emqx_connector_resource:parse_connector_id(ConnectorId) of
        #{type := ConnectorType, name := ConnectorName, namespace := Namespace} ->
            RootConf = get_config(Namespace, [SourcesOrActions], #{}),
            Types = maps:keys(RootConf),
            RelevantBridgeV2Types = [
                Type
             || Type <- Types,
                connector_type(Type) =:= ConnectorType
            ],
            lists:flatten([
                get_channels_for_connector(Namespace, SourcesOrActions, ConnectorName, BridgeV2Type)
             || BridgeV2Type <- RelevantBridgeV2Types
            ])
    catch
        _:_ ->
            %% ConnectorId is not a valid connector id so we assume the connector
            %% has no channels (e.g. it is a a connector for authn or authz)
            []
    end.

get_channels_for_connector(Namespace, SourcesOrActions, ConnectorName, BridgeV2Type) ->
    BridgeV2s = get_config(Namespace, [SourcesOrActions, BridgeV2Type], #{}),
    [
        {
            id_with_root_and_connector_names(
                Namespace, SourcesOrActions, BridgeV2Type, Name, ConnectorName
            ),
            augment_channel_config(SourcesOrActions, BridgeV2Type, Name, Conf)
        }
     || {Name, Conf} <- maps:to_list(BridgeV2s),
        bin(ConnectorName) =:= maps:get(connector, Conf, no_name)
    ].

%%====================================================================
%% ID related functions
%%====================================================================

source_id(BridgeType, BridgeName, ConnectorName) ->
    id_with_root_and_connector_names(
        ?global_ns, ?ROOT_KEY_SOURCES, BridgeType, BridgeName, ConnectorName
    ).

get_resource_ids(Namespace, ConfRootKey, Type, Name) ->
    try
        maybe
            ChannelResId = lookup_chan_id_in_conf(Namespace, ConfRootKey, Type, Name),
            {ok, ConnResId} ?= extract_connector_id_from_bridge_v2_id(ChannelResId),
            {ok, {ConnResId, ChannelResId}}
        end
    catch
        throw:Reason ->
            {error, Reason}
    end.

lookup_chan_id_in_conf(Namespace, RootName, BridgeType, BridgeName) ->
    case lookup_conf(Namespace, RootName, BridgeType, BridgeName) of
        #{connector := ConnectorName} ->
            id_with_root_and_connector_names(
                Namespace, RootName, BridgeType, BridgeName, ConnectorName
            );
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

id_with_root_and_connector_names(Namespace, RootName0, BridgeType, BridgeName, ConnectorName) ->
    RootName =
        case bin(RootName0) of
            <<"actions">> -> ?ACTION_SEG;
            <<"sources">> -> ?SOURCE_SEG
        end,
    ConnectorType = bin(connector_type(BridgeType)),
    NSTag =
        case is_binary(Namespace) of
            true -> iolist_to_binary([?NS_SEG, ?RES_SEP, Namespace, ?RES_SEP]);
            false -> <<"">>
        end,
    iolist_to_binary([
        NSTag,
        bin(RootName),
        ?RES_SEP,
        bin(BridgeType),
        ?RES_SEP,
        bin(BridgeName),
        ?RES_SEP,
        ?CONN_SEG,
        ?RES_SEP,
        bin(ConnectorType),
        ?RES_SEP,
        bin(ConnectorName)
    ]).

connector_type(Type) ->
    %% remote call so it can be mocked
    ?MODULE:bridge_v2_type_to_connector_type(Type).

bridge_v2_type_to_connector_type(Type) ->
    emqx_action_info:action_type_to_connector_type(Type).

%%====================================================================
%% `emqx_config_backup` API
%%====================================================================

import_config(Namespace, RawConf) ->
    %% actions structure
    ActionRes = do_import_config(
        Namespace, RawConf, <<"actions">>, ?ROOT_KEY_ACTIONS, config_key_path()
    ),
    SourceRes = do_import_config(
        Namespace, RawConf, <<"sources">>, ?ROOT_KEY_SOURCES, config_key_path_sources()
    ),
    group_import_results([ActionRes, SourceRes]).

do_import_config(Namespace, RawConf, RawConfKey, RootKey, RootKeyPath) ->
    BridgesConf = maps:get(RawConfKey, RawConf, #{}),
    OldBridgesConf = get_raw_config(Namespace, RootKeyPath, #{}),
    MergedConf = merge_confs(OldBridgesConf, BridgesConf),
    UpdateRes = emqx_conf:update(
        RootKeyPath,
        MergedConf,
        with_namespace(#{override_to => cluster}, Namespace)
    ),
    case UpdateRes of
        {ok, #{raw_config := NewRawConf}} ->
            {ok, #{
                root_key => RootKey,
                changed => changed_paths(RootKey, OldBridgesConf, NewRawConf)
            }};
        Error ->
            {error, #{root_key => RootKey, reason => Error}}
    end.

merge_confs(OldConf, NewConf) ->
    AllTypes = maps:keys(maps:merge(OldConf, NewConf)),
    lists:foldr(
        fun(Type, Acc) ->
            NewBridges = maps:get(Type, NewConf, #{}),
            OldBridges = maps:get(Type, OldConf, #{}),
            Acc#{Type => maps:merge(OldBridges, NewBridges)}
        end,
        #{},
        AllTypes
    ).

changed_paths(RootKey, OldRawConf, NewRawConf) ->
    maps:fold(
        fun(Type, Bridges, ChangedAcc) ->
            OldBridges = maps:get(Type, OldRawConf, #{}),
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(Bridges, OldBridges)),
            [[RootKey, Type, K] || K <- maps:keys(Changed)] ++ ChangedAcc
        end,
        [],
        NewRawConf
    ).

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
pre_config_update([ConfRootKey, _Type, _Name], Oper, undefined, _ExtraContext) when
    ?ENABLE_OR_DISABLE(Oper) andalso
        (ConfRootKey =:= ?ROOT_KEY_ACTIONS orelse ConfRootKey =:= ?ROOT_KEY_SOURCES)
->
    {error, bridge_not_found};
pre_config_update([ConfRootKey, _Type, _Name], {Oper, #{}}, undefined, _ExtraContext) when
    ?ENABLE_OR_DISABLE(Oper) andalso
        (ConfRootKey =:= ?ROOT_KEY_ACTIONS orelse ConfRootKey =:= ?ROOT_KEY_SOURCES)
->
    {error, bridge_not_found};
pre_config_update([ConfRootKey, _Type, _Name], Oper, OldAction, _ExtraContext) when
    ?ENABLE_OR_DISABLE(Oper) andalso
        (ConfRootKey =:= ?ROOT_KEY_ACTIONS orelse ConfRootKey =:= ?ROOT_KEY_SOURCES)
->
    {ok, OldAction#{<<"enable">> => operation_to_enable(Oper)}};
pre_config_update(
    [ConfRootKey, _Type, _Name], {Oper, #{now := NowMS}}, OldAction, _ExtraContext
) when
    ?ENABLE_OR_DISABLE(Oper) andalso
        (ConfRootKey =:= ?ROOT_KEY_ACTIONS orelse ConfRootKey =:= ?ROOT_KEY_SOURCES)
->
    Action = OldAction#{
        <<"enable">> => operation_to_enable(Oper),
        <<"last_modified_at">> => NowMS
    },
    {ok, Action};
%% Updates a single action from a specific HTTP API.
%% If the connector is not found, the update operation fails.
pre_config_update([ConfRootKey, Type, Name], Conf = #{}, _OldConf, ExtraContext) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS orelse ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    convert_from_connector(Namespace, ConfRootKey, Type, Name, Conf);
%% Batch updates actions when importing a configuration or executing a CLI command.
%% Update succeeded even if the connector is not found, alarm in post_config_update
pre_config_update([ConfRootKey], Conf = #{}, OldConfs, ExtraContext) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS orelse ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    {ok, convert_from_connectors(Namespace, ConfRootKey, Conf, OldConfs)}.

%% This top level handler will be triggered when the actions path is updated
%% with calls to emqx_conf:update([actions], BridgesConf, #{}).
post_config_update([ConfRootKey], _Req, NewConf0, OldConf0, _AppEnv, ExtraContext) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS; ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    NewConf = remove_computed_fields(NewConf0),
    OldConf = remove_computed_fields(OldConf0),
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    #{added := Added, removed := Removed, changed := Updated} =
        diff_confs(NewConf, OldConf),
    RemoveFun = fun(Type, Name, Conf) ->
        uninstall_bridge_v2(Namespace, ConfRootKey, Type, Name, Conf)
    end,
    CreateFun = fun(Type, Name, Conf) ->
        install_bridge_v2(Namespace, ConfRootKey, Type, Name, Conf)
    end,
    UpdateFun = fun(Type, Name, {OldBridgeConf, Conf}) ->
        _ = uninstall_bridge_v2(Namespace, ConfRootKey, Type, Name, OldBridgeConf),
        install_bridge_v2(Namespace, ConfRootKey, Type, Name, Conf)
    end,
    Result = perform_bridge_changes([
        #{action => RemoveFun, action_name => remove, data => Removed},
        #{
            action => CreateFun,
            action_name => create,
            data => Added,
            on_exception_fn => fun(Type, Name, Conf, _Opts) ->
                RemoveFun(Type, Name, Conf)
            end
        },
        #{action => UpdateFun, action_name => update, data => Updated}
    ]),
    ?tp(bridge_post_config_update_done, #{}),
    Result;
%% Don't crash even when the bridge is not found
post_config_update([ConfRootKey, Type, Name], '$remove', _, _OldConf, _AppEnvs, ExtraContext) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS; ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    AllBridges = get_config(Namespace, [ConfRootKey], #{}),
    case emqx_utils_maps:deep_get([Type, Name], AllBridges, undefined) of
        undefined ->
            ok;
        Action ->
            ok = uninstall_bridge_v2(Namespace, ConfRootKey, Type, Name, Action)
    end,
    ?tp(bridge_post_config_update_done, #{}),
    ok;
%% Create a single bridge fails if the connector is not found (already checked in pre_config_update)
post_config_update(
    [ConfRootKey, BridgeType, BridgeName], _Req, NewConf, undefined, _AppEnvs, ExtraContext
) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS; ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    ok = install_bridge_v2(Namespace, ConfRootKey, BridgeType, BridgeName, NewConf),
    ?tp(bridge_post_config_update_done, #{}),
    ok;
%% update bridges fails if the connector is not found (already checked in pre_config_update)
post_config_update(
    [ConfRootKey, BridgeType, BridgeName], _Req, NewConf, OldConf, _AppEnvs, ExtraContext
) when
    ConfRootKey =:= ?ROOT_KEY_ACTIONS; ConfRootKey =:= ?ROOT_KEY_SOURCES
->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    case uninstall_bridge_v2(Namespace, ConfRootKey, BridgeType, BridgeName, OldConf) of
        ok ->
            ok;
        {error, not_found} ->
            %% Should not happen, unless config is inconsistent.
            throw(<<"Referenced connector not found">>)
    end,
    ok = install_bridge_v2(Namespace, ConfRootKey, BridgeType, BridgeName, NewConf),
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
%% `emqx_telemetry` callback
%%====================================================================

-spec get_basic_usage_info() ->
    #{
        num_bridges => non_neg_integer(),
        count_by_type =>
            #{BridgeType => non_neg_integer()}
    }
when
    BridgeType :: atom().
get_basic_usage_info() ->
    InitialAcc = #{num_bridges => 0, count_by_type => #{}},
    try
        lists:foldl(
            fun
                (#{raw_config := #{<<"enable">> := false}}, Acc) ->
                    Acc;
                (#{type := BridgeType}, Acc) ->
                    NumBridges = maps:get(num_bridges, Acc),
                    CountByType0 = maps:get(count_by_type, Acc),
                    CountByType = maps:update_with(
                        binary_to_atom(BridgeType, utf8),
                        fun(X) -> X + 1 end,
                        1,
                        CountByType0
                    ),
                    Acc#{
                        num_bridges => NumBridges + 1,
                        count_by_type => CountByType
                    }
            end,
            InitialAcc,
            emqx_bridge_v2:list(?global_ns, actions) ++
                emqx_bridge_v2:list(?global_ns, sources)
        )
    catch
        %% for instance, when the bridge app is not ready yet.
        _:_ ->
            InitialAcc
    end.

%%====================================================================
%% Compatibility API
%%====================================================================

lookup_conf(Namespace, RootName, Type, Name) ->
    case get_config(Namespace, [RootName, Type, Name], not_found) of
        not_found ->
            {error, bridge_not_found};
        Config ->
            Config
    end.

%%====================================================================
%% Misc helper functions
%%====================================================================

operation_to_enable(disable) -> false;
operation_to_enable(enable) -> true.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

extract_connector_id_from_bridge_v2_id(Id) ->
    emqx_resource:parse_connector_id_from_channel_id(Id).

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

referenced_connectors_exist(Namespace, BridgeType, ConnectorNameBin, BridgeName) ->
    %% N.B.: assumes that, for all bridgeV2 types, the name of the bridge type is
    %% identical to its matching connector type name.
    case get_connector_info(Namespace, ConnectorNameBin, BridgeType) of
        {error, not_found} ->
            {error, #{
                reason => "connector_not_found_or_wrong_type",
                namespace => Namespace,
                connector_name => ConnectorNameBin,
                bridge_name => BridgeName,
                bridge_type => BridgeType
            }};
        {ok, _Connector} ->
            ok
    end.

convert_from_connectors(Namespace, ConfRootKey, Conf, OldRootConfig) ->
    maps:map(
        fun(ActionType, Actions) ->
            maps:map(
                fun(ActionName, Action0) ->
                    Action1 = ensure_created_at(Action0),
                    Action = ensure_last_modified_at(
                        Action1, ActionType, ActionName, OldRootConfig
                    ),
                    case
                        convert_from_connector(
                            Namespace, ConfRootKey, ActionType, ActionName, Action
                        )
                    of
                        {ok, NewAction} -> NewAction;
                        {error, _} -> Action
                    end
                end,
                Actions
            )
        end,
        Conf
    ).

convert_from_connector(Namespace, ConfRootKey, Type, Name, Action) ->
    #{<<"connector">> := ConnectorName} = Action,
    case get_connector_info(Namespace, ConnectorName, Type) of
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

ensure_last_modified_at(RawConfig) ->
    RawConfig#{<<"last_modified_at">> => now_ms()}.

ensure_last_modified_at(ChannelRawConfig, Type, Name, OldRootRawConfig) ->
    case emqx_utils_maps:deep_get([Type, Name], OldRootRawConfig, undefined) of
        #{<<"last_modified_at">> := _} = ChannelRawConfig ->
            %% No changes
            ChannelRawConfig;
        _ ->
            %% New config contains changes, or config lacks modification date
            ensure_last_modified_at(ChannelRawConfig)
    end.

ensure_created_at(RawConfig) when is_map_key(<<"created_at">>, RawConfig) ->
    RawConfig;
ensure_created_at(RawConfig) ->
    RawConfig#{<<"created_at">> => now_ms()}.

get_connector_info(Namespace, ConnectorNameBin, BridgeType) ->
    case to_connector(ConnectorNameBin, BridgeType) of
        {error, not_found} ->
            {error, not_found};
        {ConnectorName, ConnectorType} ->
            case get_raw_config(Namespace, [connectors, ConnectorType, ConnectorName], undefined) of
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

alarm_connector_not_found(Namespace, ActionType, ActionName, ConnectorName) ->
    ConnectorType = connector_type(to_existing_atom(ActionType)),
    ResId = emqx_connector_resource:resource_id(
        Namespace, ConnectorType, ConnectorName
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

get_config(Namespace, KeyPath, Default) when is_binary(Namespace) ->
    emqx:get_namespaced_config(Namespace, KeyPath, Default);
get_config(?global_ns, KeyPath, Default) ->
    emqx:get_config(KeyPath, Default).

get_raw_config(Namespace, KeyPath, Default) when is_binary(Namespace) ->
    emqx:get_raw_namespaced_config(Namespace, KeyPath, Default);
get_raw_config(?global_ns, KeyPath, Default) ->
    emqx:get_raw_config(KeyPath, Default).

with_namespace(UpdateOpts, ?global_ns) ->
    UpdateOpts;
with_namespace(UpdateOpts, Namespace) when is_binary(Namespace) ->
    UpdateOpts#{namespace => Namespace}.

get_root_config_from_all_namespaces(RootKey, Default) ->
    NamespacedConfigs = emqx_config:get_root_from_all_namespaces(RootKey, Default),
    NamespacedConfigs#{?global_ns => emqx:get_config([RootKey], Default)}.

remove_computed_fields(#{} = Map) ->
    maps:map(
        fun(_K, V) -> remove_computed_fields(V) end,
        maps:remove(?COMPUTED, Map)
    );
remove_computed_fields(X) ->
    X.
