%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-define(ROOT_KEY, actions).

%% Loading and unloading config when EMQX starts and stops
-export([
    load/0,
    unload/0
]).

%% CRUD API

-export([
    list/0,
    lookup/2,
    create/3,
    %% The remove/2 function is only for internal use as it may create
    %% rules with broken dependencies
    remove/2,
    %% The following is the remove function that is called by the HTTP API
    %% It also checks for rule action dependencies and optionally removes
    %% them
    check_deps_and_remove/3
]).

%% Operations

-export([
    disable_enable/3,
    health_check/2,
    send_message/4,
    query/4,
    start/2,
    reset_metrics/2,
    create_dry_run/2,
    get_metrics/2
]).

%% On message publish hook (for local_topics)

-export([on_message_publish/1]).

%% Convenience functions for connector implementations

-export([
    parse_id/1,
    get_channels_for_connector/1
]).

%% Exported for tests
-export([
    id/2,
    id/3,
    bridge_v1_is_valid/2,
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
    is_bridge_v2_type/1
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
    bridge_v1_enable_disable/3,
    bridge_v1_restart/2,
    bridge_v1_stop/2,
    bridge_v1_start/2,
    %% For test cases only
    bridge_v1_remove/2
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

%%====================================================================

%%====================================================================

%%====================================================================
%% Loading and unloading config when EMQX starts and stops
%%====================================================================

load() ->
    load_bridges(),
    load_message_publish_hook(),
    ok = emqx_config_handler:add_handler(config_key_path_leaf(), emqx_bridge_v2),
    ok = emqx_config_handler:add_handler(config_key_path(), emqx_bridge_v2),
    ok.

load_bridges() ->
    Bridges = emqx:get_config([?ROOT_KEY], #{}),
    lists:foreach(
        fun({Type, Bridge}) ->
            lists:foreach(
                fun({Name, BridgeConf}) ->
                    install_bridge_v2(Type, Name, BridgeConf)
                end,
                maps:to_list(Bridge)
            )
        end,
        maps:to_list(Bridges)
    ).

unload() ->
    unload_bridges(),
    unload_message_publish_hook(),
    emqx_conf:remove_handler(config_key_path()),
    emqx_conf:remove_handler(config_key_path_leaf()),
    ok.

unload_bridges() ->
    Bridges = emqx:get_config([?ROOT_KEY], #{}),
    lists:foreach(
        fun({Type, Bridge}) ->
            lists:foreach(
                fun({Name, BridgeConf}) ->
                    uninstall_bridge_v2(Type, Name, BridgeConf)
                end,
                maps:to_list(Bridge)
            )
        end,
        maps:to_list(Bridges)
    ).

%%====================================================================
%% CRUD API
%%====================================================================

-spec lookup(bridge_v2_type(), bridge_v2_name()) -> {ok, bridge_v2_info()} | {error, not_found}.
lookup(Type, Name) ->
    case emqx:get_raw_config([?ROOT_KEY, Type, Name], not_found) of
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
            BridgeV2Id = id(Type, Name, BridgeConnector),
            ChannelStatus = maps:get(BridgeV2Id, Channels, undefined),
            {DisplayBridgeV2Status, ErrorMsg} =
                case {ChannelStatus, ConnectorStatus} of
                    {#{status := ?status_connected}, _} ->
                        {?status_connected, <<"">>};
                    {#{error := resource_not_operational}, ?status_connecting} ->
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
    list_with_lookup_fun(fun lookup/2).

-spec create(bridge_v2_type(), bridge_v2_name(), map()) ->
    {ok, emqx_config:update_result()} | {error, any()}.
create(BridgeType, BridgeName, RawConf) ->
    ?SLOG(debug, #{
        brige_action => create,
        bridge_version => 2,
        bridge_type => BridgeType,
        bridge_name => BridgeName,
        bridge_raw_config => emqx_utils:redact(RawConf)
    }),
    emqx_conf:update(
        config_key_path() ++ [BridgeType, BridgeName],
        RawConf,
        #{override_to => cluster}
    ).

%% NOTE: This function can cause broken references from rules but it is only
%% called directly from test cases.

-spec remove(bridge_v2_type(), bridge_v2_name()) -> ok | {error, any()}.
remove(BridgeType, BridgeName) ->
    ?SLOG(debug, #{
        brige_action => remove,
        bridge_version => 2,
        bridge_type => BridgeType,
        bridge_name => BridgeName
    }),
    case
        emqx_conf:remove(
            config_key_path() ++ [BridgeType, BridgeName],
            #{override_to => cluster}
        )
    of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec check_deps_and_remove(bridge_v2_type(), bridge_v2_name(), boolean()) -> ok | {error, any()}.
check_deps_and_remove(BridgeType, BridgeName, AlsoDeleteActions) ->
    AlsoDelete =
        case AlsoDeleteActions of
            true -> [rule_actions];
            false -> []
        end,
    case
        emqx_bridge_lib:maybe_withdraw_rule_action(
            BridgeType,
            BridgeName,
            AlsoDelete
        )
    of
        ok ->
            remove(BridgeType, BridgeName);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Helpers for CRUD API
%%--------------------------------------------------------------------

list_with_lookup_fun(LookupFun) ->
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
        emqx:get_raw_config([?ROOT_KEY], #{})
    ).

install_bridge_v2(
    _BridgeType,
    _BridgeName,
    #{enable := false}
) ->
    ok;
install_bridge_v2(
    BridgeV2Type,
    BridgeName,
    Config
) ->
    install_bridge_v2_helper(
        BridgeV2Type,
        BridgeName,
        combine_connector_and_bridge_v2_config(
            BridgeV2Type,
            BridgeName,
            Config
        )
    ).

install_bridge_v2_helper(
    _BridgeV2Type,
    _BridgeName,
    {error, Reason} = Error
) ->
    ?SLOG(error, Reason),
    Error;
install_bridge_v2_helper(
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = Config
) ->
    BridgeV2Id = id(BridgeV2Type, BridgeName, ConnectorName),
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    %% Create metrics for Bridge V2
    ok = emqx_resource:create_metrics(BridgeV2Id),
    %% We might need to create buffer workers for Bridge V2
    case get_query_mode(BridgeV2Type, Config) of
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
    ConfigWithTypeAndName = Config#{
        bridge_type => bin(BridgeV2Type),
        bridge_name => bin(BridgeName)
    },
    emqx_resource_manager:add_channel(
        ConnectorId,
        BridgeV2Id,
        ConfigWithTypeAndName
    ),
    ok.

uninstall_bridge_v2(
    _BridgeType,
    _BridgeName,
    #{enable := false}
) ->
    %% Already not installed
    ok;
uninstall_bridge_v2(
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = Config
) ->
    BridgeV2Id = id(BridgeV2Type, BridgeName, ConnectorName),
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    ok = emqx_resource_buffer_worker_sup:stop_workers(BridgeV2Id, CreationOpts),
    ok = emqx_resource:clear_metrics(BridgeV2Id),
    case validate_referenced_connectors(BridgeV2Type, ConnectorName, BridgeName) of
        {error, _} ->
            ok;
        ok ->
            %% Deinstall from connector
            ConnectorId = emqx_connector_resource:resource_id(
                connector_type(BridgeV2Type), ConnectorName
            ),
            emqx_resource_manager:remove_channel(ConnectorId, BridgeV2Id)
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
            {error, #{
                reason => "connector_not_found",
                type => BridgeV2Type,
                bridge_name => BridgeName,
                connector_name => ConnectorName
            }}
    end.

%%====================================================================
%% Operations
%%====================================================================

-spec disable_enable(disable | enable, bridge_v2_type(), bridge_v2_name()) ->
    {ok, any()} | {error, any()}.
disable_enable(Action, BridgeType, BridgeName) when
    Action =:= disable; Action =:= enable
->
    emqx_conf:update(
        config_key_path() ++ [BridgeType, BridgeName],
        {Action, BridgeType, BridgeName},
        #{override_to => cluster}
    ).

%% Manually start connector. This function can speed up reconnection when
%% waiting for auto reconnection. The function forwards the start request to
%% its connector. Returns ok if the status of the bridge is connected after
%% starting the connector. Returns {error, Reason} if the status of the bridge
%% is something else than connected after starting the connector or if an
%% error occurred when the connector was started.
-spec start(term(), term()) -> ok | {error, Reason :: term()}.
start(BridgeV2Type, Name) ->
    ConnectorOpFun = fun(ConnectorType, ConnectorName) ->
        emqx_connector_resource:start(ConnectorType, ConnectorName)
    end,
    connector_operation_helper(BridgeV2Type, Name, ConnectorOpFun, true).

connector_operation_helper(BridgeV2Type, Name, ConnectorOpFun, DoHealthCheck) ->
    connector_operation_helper_with_conf(
        BridgeV2Type,
        Name,
        lookup_conf(BridgeV2Type, Name),
        ConnectorOpFun,
        DoHealthCheck
    ).

connector_operation_helper_with_conf(
    _BridgeV2Type,
    _Name,
    {error, bridge_not_found} = Error,
    _ConnectorOpFun,
    _DoHealthCheck
) ->
    Error;
connector_operation_helper_with_conf(
    _BridgeV2Type,
    _Name,
    #{enable := false},
    _ConnectorOpFun,
    _DoHealthCheck
) ->
    ok;
connector_operation_helper_with_conf(
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
            case health_check(BridgeV2Type, Name) of
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
    reset_metrics_helper(Type, Name, lookup_conf(Type, Name)).

reset_metrics_helper(_Type, _Name, #{enable := false}) ->
    ok;
reset_metrics_helper(BridgeV2Type, BridgeName, #{connector := ConnectorName}) ->
    BridgeV2Id = id(BridgeV2Type, BridgeName, ConnectorName),
    ok = emqx_metrics_worker:reset_metrics(?RES_METRICS, BridgeV2Id);
reset_metrics_helper(_, _, _) ->
    {error, not_found}.

get_query_mode(BridgeV2Type, Config) ->
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    ConnectorType = connector_type(BridgeV2Type),
    ResourceType = emqx_connector_resource:connector_to_resource_type(ConnectorType),
    emqx_resource:query_mode(ResourceType, Config, CreationOpts).

-spec query(bridge_v2_type(), bridge_v2_name(), Message :: term(), QueryOpts :: map()) ->
    term() | {error, term()}.
query(BridgeType, BridgeName, Message, QueryOpts0) ->
    case lookup_conf(BridgeType, BridgeName) of
        #{enable := true} = Config0 ->
            Config = combine_connector_and_bridge_v2_config(BridgeType, BridgeName, Config0),
            do_query_with_enabled_config(BridgeType, BridgeName, Message, QueryOpts0, Config);
        #{enable := false} ->
            {error, bridge_stopped};
        _Error ->
            {error, bridge_not_found}
    end.

do_query_with_enabled_config(
    _BridgeType, _BridgeName, _Message, _QueryOpts0, {error, Reason} = Error
) ->
    ?SLOG(error, Reason),
    Error;
do_query_with_enabled_config(
    BridgeType, BridgeName, Message, QueryOpts0, Config
) ->
    QueryMode = get_query_mode(BridgeType, Config),
    ConnectorName = maps:get(connector, Config),
    ConnectorResId = emqx_connector_resource:resource_id(BridgeType, ConnectorName),
    QueryOpts = maps:merge(
        emqx_bridge:query_opts(Config),
        QueryOpts0#{
            connector_resource_id => ConnectorResId,
            query_mode => QueryMode
        }
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

-spec health_check(BridgeType :: term(), BridgeName :: term()) ->
    #{status := emqx_resource:resource_status(), error := term()} | {error, Reason :: term()}.
health_check(BridgeType, BridgeName) ->
    case lookup_conf(BridgeType, BridgeName) of
        #{
            enable := true,
            connector := ConnectorName
        } ->
            ConnectorId = emqx_connector_resource:resource_id(
                connector_type(BridgeType), ConnectorName
            ),
            emqx_resource_manager:channel_health_check(
                ConnectorId, id(BridgeType, BridgeName, ConnectorName)
            );
        #{enable := false} ->
            {error, bridge_stopped};
        Error ->
            Error
    end.

-spec create_dry_run(bridge_v2_type(), Config :: map()) -> ok | {error, term()}.
create_dry_run(Type, Conf0) ->
    Conf1 = maps:without([<<"name">>], Conf0),
    TypeBin = bin(Type),
    RawConf = #{<<"actions">> => #{TypeBin => #{<<"temp_name">> => Conf1}}},
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
                create_dry_run_helper(Type, ConnectorRawConf, Conf1)
        end
    catch
        %% validation errors
        throw:Reason1 ->
            {error, Reason1}
    end.

create_dry_run_helper(BridgeType, ConnectorRawConf, BridgeV2RawConf) ->
    BridgeName = iolist_to_binary([?TEST_ID_PREFIX, emqx_utils:gen_id(8)]),
    ConnectorType = connector_type(BridgeType),
    OnReadyCallback =
        fun(ConnectorId) ->
            {_, ConnectorName} = emqx_connector_resource:parse_connector_id(ConnectorId),
            ChannelTestId = id(BridgeType, BridgeName, ConnectorName),
            Conf = emqx_utils_maps:unsafe_atom_key_map(BridgeV2RawConf),
            ConfWithTypeAndName = Conf#{
                bridge_type => bin(BridgeType),
                bridge_name => bin(BridgeName)
            },
            case
                emqx_resource_manager:add_channel(ConnectorId, ChannelTestId, ConfWithTypeAndName)
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
    emqx_resource:get_metrics(id(Type, Name)).

%%====================================================================
%% On message publish hook (for local topics)
%%====================================================================

%% The following functions are more or less copied from emqx_bridge.erl

reload_message_publish_hook(Bridges) ->
    ok = unload_message_publish_hook(),
    ok = load_message_publish_hook(Bridges).

load_message_publish_hook() ->
    Bridges = emqx:get_config([?ROOT_KEY], #{}),
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
    Bridges = emqx:get_config([?ROOT_KEY], #{}),
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
            {Type, Name};
        [<<"action">>, Type, Name | _] ->
            {Type, Name};
        _X ->
            error({error, iolist_to_binary(io_lib:format("Invalid id: ~p", [Id]))})
    end.

get_channels_for_connector(ConnectorId) ->
    try emqx_connector_resource:parse_connector_id(ConnectorId) of
        {ConnectorType, ConnectorName} ->
            RootConf = maps:keys(emqx:get_config([?ROOT_KEY], #{})),
            RelevantBridgeV2Types = [
                Type
             || Type <- RootConf,
                connector_type(Type) =:= ConnectorType
            ],
            lists:flatten([
                get_channels_for_connector(ConnectorName, BridgeV2Type)
             || BridgeV2Type <- RelevantBridgeV2Types
            ])
    catch
        _:_ ->
            %% ConnectorId is not a valid connector id so we assume the connector
            %% has no channels (e.g. it is a a connector for authn or authz)
            []
    end.

get_channels_for_connector(ConnectorName, BridgeV2Type) ->
    BridgeV2s = emqx:get_config([?ROOT_KEY, BridgeV2Type], #{}),
    [
        {id(BridgeV2Type, Name, ConnectorName), Conf#{
            bridge_name => bin(Name),
            bridge_type => bin(BridgeV2Type)
        }}
     || {Name, Conf} <- maps:to_list(BridgeV2s),
        bin(ConnectorName) =:= maps:get(connector, Conf, no_name)
    ].

%%====================================================================
%% Exported for tests
%%====================================================================

id(BridgeType, BridgeName) ->
    case lookup_conf(BridgeType, BridgeName) of
        #{connector := ConnectorName} ->
            id(BridgeType, BridgeName, ConnectorName);
        {error, Reason} ->
            throw(Reason)
    end.

id(BridgeType, BridgeName, ConnectorName) ->
    ConnectorType = bin(connector_type(BridgeType)),
    <<"action:", (bin(BridgeType))/binary, ":", (bin(BridgeName))/binary, ":connector:",
        (bin(ConnectorType))/binary, ":", (bin(ConnectorName))/binary>>.

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
    emqx_bridge:import_config(RawConf, <<"actions">>, ?ROOT_KEY, config_key_path()).

%%====================================================================
%% Config Update Handler API
%%====================================================================

config_key_path() ->
    [?ROOT_KEY].

config_key_path_leaf() ->
    [?ROOT_KEY, '?', '?'].

%% NOTE: We depend on the `emqx_bridge:pre_config_update/3` to restart/stop the
%%       underlying resources.
pre_config_update(_, {_Oper, _, _}, undefined) ->
    {error, bridge_not_found};
pre_config_update(_, {Oper, _Type, _Name}, OldConfig) ->
    %% to save the 'enable' to the config files
    {ok, OldConfig#{<<"enable">> => operation_to_enable(Oper)}};
pre_config_update(_Path, Conf, _OldConfig) when is_map(Conf) ->
    {ok, Conf}.

operation_to_enable(disable) -> false;
operation_to_enable(enable) -> true.

%% This top level handler will be triggered when the actions path is updated
%% with calls to emqx_conf:update([actions], BridgesConf, #{}).
%%
%% A public API that can trigger this is:
%% bin/emqx ctl conf load data/configs/cluster.hocon
post_config_update([?ROOT_KEY], _Req, NewConf, OldConf, _AppEnv) ->
    #{added := Added, removed := Removed, changed := Updated} =
        diff_confs(NewConf, OldConf),
    %% new and updated bridges must have their connector references validated
    UpdatedConfigs =
        lists:map(
            fun({{Type, BridgeName}, {_Old, New}}) ->
                {Type, BridgeName, New}
            end,
            maps:to_list(Updated)
        ),
    AddedConfigs =
        lists:map(
            fun({{Type, BridgeName}, AddedConf}) ->
                {Type, BridgeName, AddedConf}
            end,
            maps:to_list(Added)
        ),
    ToValidate = UpdatedConfigs ++ AddedConfigs,
    case multi_validate_referenced_connectors(ToValidate) of
        ok ->
            %% The config update will be failed if any task in `perform_bridge_changes` failed.
            RemoveFun = fun uninstall_bridge_v2/3,
            CreateFun = fun install_bridge_v2/3,
            UpdateFun = fun(Type, Name, {OldBridgeConf, Conf}) ->
                uninstall_bridge_v2(Type, Name, OldBridgeConf),
                install_bridge_v2(Type, Name, Conf)
            end,
            Result = perform_bridge_changes([
                #{action => RemoveFun, data => Removed},
                #{
                    action => CreateFun,
                    data => Added,
                    on_exception_fn => fun emqx_bridge_resource:remove/4
                },
                #{action => UpdateFun, data => Updated}
            ]),
            ok = unload_message_publish_hook(),
            ok = load_message_publish_hook(NewConf),
            ?tp(bridge_post_config_update_done, #{}),
            Result;
        {error, Error} ->
            {error, Error}
    end;
post_config_update([?ROOT_KEY, BridgeType, BridgeName], '$remove', _, _OldConf, _AppEnvs) ->
    Conf = emqx:get_config([?ROOT_KEY, BridgeType, BridgeName]),
    ok = uninstall_bridge_v2(BridgeType, BridgeName, Conf),
    Bridges = emqx_utils_maps:deep_remove([BridgeType, BridgeName], emqx:get_config([?ROOT_KEY])),
    reload_message_publish_hook(Bridges),
    ?tp(bridge_post_config_update_done, #{}),
    ok;
post_config_update([?ROOT_KEY, BridgeType, BridgeName], _Req, NewConf, undefined, _AppEnvs) ->
    %% N.B.: all bridges must use the same field name (`connector`) to define the
    %% connector name.
    ConnectorName = maps:get(connector, NewConf),
    case validate_referenced_connectors(BridgeType, ConnectorName, BridgeName) of
        ok ->
            ok = install_bridge_v2(BridgeType, BridgeName, NewConf),
            Bridges = emqx_utils_maps:deep_put(
                [BridgeType, BridgeName], emqx:get_config([?ROOT_KEY]), NewConf
            ),
            reload_message_publish_hook(Bridges),
            ?tp(bridge_post_config_update_done, #{}),
            ok;
        {error, Error} ->
            {error, Error}
    end;
post_config_update([?ROOT_KEY, BridgeType, BridgeName], _Req, NewConf, OldConf, _AppEnvs) ->
    ConnectorName = maps:get(connector, NewConf),
    case validate_referenced_connectors(BridgeType, ConnectorName, BridgeName) of
        ok ->
            ok = uninstall_bridge_v2(BridgeType, BridgeName, OldConf),
            ok = install_bridge_v2(BridgeType, BridgeName, NewConf),
            Bridges = emqx_utils_maps:deep_put(
                [BridgeType, BridgeName], emqx:get_config([?ROOT_KEY]), NewConf
            ),
            reload_message_publish_hook(Bridges),
            ?tp(bridge_post_config_update_done, #{}),
            ok;
        {error, Error} ->
            {error, Error}
    end.

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
    perform_bridge_changes(Tasks, ok).

perform_bridge_changes([], Result) ->
    Result;
perform_bridge_changes([#{action := Action, data := MapConfs} = Task | Tasks], Result0) ->
    OnException = maps:get(on_exception_fn, Task, fun(_Type, _Name, _Conf, _Opts) -> ok end),
    Result = maps:fold(
        fun
            ({_Type, _Name}, _Conf, {error, Reason}) ->
                {error, Reason};
            %% for update
            ({Type, Name}, {OldConf, Conf}, _) ->
                case Action(Type, Name, {OldConf, Conf}) of
                    {error, Reason} -> {error, Reason};
                    Return -> Return
                end;
            ({Type, Name}, Conf, _) ->
                try Action(Type, Name, Conf) of
                    {error, Reason} -> {error, Reason};
                    Return -> Return
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
                        erlang:raise(Kind, Error, Stacktrace)
                end
        end,
        Result0,
        MapConfs
    ),
    perform_bridge_changes(Tasks, Result).

fill_defaults(Type, RawConf, TopLevelConf, SchemaModule) ->
    PackedConf = pack_bridge_conf(Type, RawConf, TopLevelConf),
    FullConf = emqx_config:fill_defaults(SchemaModule, PackedConf, #{}),
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
    BridgeV2Type = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    case lookup_conf(BridgeV2Type, BridgeName) of
        {error, _} ->
            %% If the bridge v2 does not exist, it is a valid bridge v1
            true;
        #{connector := ConnectorName} ->
            ConnectorType = connector_type(BridgeV2Type),
            ConnectorResourceId = emqx_connector_resource:resource_id(ConnectorType, ConnectorName),
            {ok, Channels} = emqx_resource:get_channels(ConnectorResourceId),
            case Channels of
                [_Channel] ->
                    true;
                _ ->
                    false
            end
    end.

bridge_v1_type_to_bridge_v2_type(Type) ->
    emqx_action_info:bridge_v1_type_to_action_type(Type).

bridge_v2_type_to_bridge_v1_type(ActionType, ActionConf) ->
    emqx_action_info:action_type_to_bridge_v1_type(ActionType, ActionConf).

is_bridge_v2_type(Type) ->
    emqx_action_info:is_action_type(Type).

bridge_v1_list_and_transform() ->
    Bridges = list_with_lookup_fun(fun bridge_v1_lookup_and_transform/2),
    [B || B <- Bridges, B =/= not_bridge_v1_compatible_error()].

bridge_v1_lookup_and_transform(ActionType, Name) ->
    case lookup(ActionType, Name) of
        {ok, #{raw_config := #{<<"connector">> := ConnectorName} = RawConfig} = ActionConfig} ->
            BridgeV1Type = ?MODULE:bridge_v2_type_to_bridge_v1_type(ActionType, RawConfig),
            case ?MODULE:bridge_v1_is_valid(BridgeV1Type, Name) of
                true ->
                    ConnectorType = connector_type(ActionType),
                    case emqx_connector:lookup(ConnectorType, ConnectorName) of
                        {ok, Connector} ->
                            bridge_v1_lookup_and_transform_helper(
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

not_bridge_v1_compatible_error() ->
    {error, not_bridge_v1_compatible}.

bridge_v1_lookup_and_transform_helper(
    BridgeV1Type, BridgeName, ActionType, Action, ConnectorType, Connector
) ->
    ConnectorRawConfig1 = maps:get(raw_config, Connector),
    ConnectorRawConfig2 = fill_defaults(
        ConnectorType,
        ConnectorRawConfig1,
        <<"connectors">>,
        emqx_connector_schema
    ),
    ActionRawConfig1 = maps:get(raw_config, Action),
    ActionRawConfig2 = fill_defaults(
        ActionType,
        ActionRawConfig1,
        <<"actions">>,
        emqx_bridge_v2_schema
    ),
    BridgeV1ConfigFinal =
        case
            emqx_action_info:has_custom_connector_action_config_to_bridge_v1_config(BridgeV1Type)
        of
            false ->
                BridgeV1Config1 = maps:remove(<<"connector">>, ActionRawConfig2),
                %% Move parameters to the top level
                ParametersMap = maps:get(<<"parameters">>, BridgeV1Config1, #{}),
                BridgeV1Config2 = maps:remove(<<"parameters">>, BridgeV1Config1),
                BridgeV1Config3 = emqx_utils_maps:deep_merge(BridgeV1Config2, ParametersMap),
                emqx_utils_maps:deep_merge(ConnectorRawConfig2, BridgeV1Config3);
            true ->
                emqx_action_info:connector_action_config_to_bridge_v1_config(
                    BridgeV1Type, ConnectorRawConfig2, ActionRawConfig2
                )
        end,
    BridgeV1Tmp = maps:put(raw_config, BridgeV1ConfigFinal, Action),
    BridgeV1 = maps:remove(status, BridgeV1Tmp),
    BridgeV2Status = maps:get(status, Action, undefined),
    BridgeV2Error = maps:get(error, Action, undefined),
    ResourceData1 = maps:get(resource_data, BridgeV1, #{}),
    %% Replace id in resouce data
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
    case emqx:get_config([?ROOT_KEY, Type, Name], not_found) of
        not_found ->
            {error, bridge_not_found};
        Config ->
            Config
    end.

bridge_v1_split_config_and_create(BridgeV1Type, BridgeName, RawConf) ->
    BridgeV2Type = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    %% Check if the bridge v2 exists
    case lookup_conf(BridgeV2Type, BridgeName) of
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
                    PreviousRawConf = emqx:get_raw_config(
                        [?ROOT_KEY, BridgeV2Type, BridgeName], undefined
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
            bridge_v2_conf := NewBridgeV2RawConf
        } = split_and_validate_bridge_v1_config(
            BridgeV1Type,
            BridgeName,
            RawConf,
            PreviousRawConf
        ),

        _ = PreCreateFun(),

        do_connector_and_bridge_create(
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
            case create(BridgeType, BridgeName, NewBridgeV2RawConf) of
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
    FakeGlobalConfig =
        emqx_utils_maps:put_if(
            FakeGlobalConfig0,
            bin(?ROOT_KEY),
            #{bin(BridgeV2Type) => #{bin(BridgeName) => PreviousRawConf}},
            PreviousRawConf =/= undefined
        ),
    %% [FIXME] this will loop through all connector types, instead pass the
    %% connector type and just do it for that one
    Output = emqx_connector_schema:transform_bridges_v1_to_connectors_and_bridges_v2(
        FakeGlobalConfig
    ),
    NewBridgeV2RawConf =
        emqx_utils_maps:deep_get(
            [
                bin(?ROOT_KEY),
                bin(BridgeV2Type),
                bin(BridgeName)
            ],
            Output
        ),
    ConnectorName = emqx_utils_maps:deep_get(
        [
            bin(?ROOT_KEY),
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
    NewFakeGlobalConfig = #{
        <<"connectors">> => #{
            bin(ConnectorType) => #{
                bin(ConnectorName) => NewConnectorRawConf
            }
        },
        <<"actions">> => #{
            bin(BridgeV2Type) => #{
                bin(BridgeName) => NewBridgeV2RawConf
            }
        }
    },
    try
        hocon_tconf:check_plain(
            emqx_schema,
            NewFakeGlobalConfig,
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
                bridge_v2_conf => NewBridgeV2RawConf
            }
    catch
        %% validation errors
        throw:Reason1 ->
            {error, Reason1}
    end.

bridge_v1_create_dry_run(BridgeType, RawConfig0) ->
    RawConf = maps:without([<<"name">>], RawConfig0),
    TmpName = iolist_to_binary([?TEST_ID_PREFIX, emqx_utils:gen_id(8)]),
    PreviousRawConf = undefined,
    try
        #{
            connector_type := _ConnectorType,
            connector_name := _NewConnectorName,
            connector_conf := ConnectorRawConf,
            bridge_v2_type := BridgeV2Type,
            bridge_v2_name := _BridgeName,
            bridge_v2_conf := BridgeV2RawConf
        } = split_and_validate_bridge_v1_config(BridgeType, TmpName, RawConf, PreviousRawConf),
        create_dry_run_helper(BridgeV2Type, ConnectorRawConf, BridgeV2RawConf)
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
        lookup_conf(ActionType, BridgeName)
    ).

bridge_v1_remove(
    ActionType,
    Name,
    #{connector := ConnectorName}
) ->
    case remove(ActionType, Name) of
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
        lookup_conf(BridgeV2Type, BridgeName)
    ).

%% Bridge v1 delegated-removal in 3 steps:
%% 1. Delete rule actions if RmoveDeps has 'rule_actions'
%% 2. Delete self (the bridge v2), also delete its channel in the connector
%% 3. Delete the connector if the connector has no more channel left and if 'connector' is in RemoveDeps
bridge_v1_check_deps_and_remove(
    BridgeType,
    BridgeName,
    RemoveDeps,
    #{connector := ConnectorName}
) ->
    RemoveConnector = lists:member(connector, RemoveDeps),
    case emqx_bridge_lib:maybe_withdraw_rule_action(BridgeType, BridgeName, RemoveDeps) of
        ok ->
            case remove(BridgeType, BridgeName) of
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
    case binary:split(BridgeId, <<":">>) of
        [Type, Name] ->
            BridgeV2Type = bin(bridge_v1_type_to_bridge_v2_type(Type)),
            ConnectorName =
                case lookup_conf(BridgeV2Type, Name) of
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
                lookup_conf(BridgeType, BridgeName)
            );
        false ->
            {error, not_bridge_v1_compatible}
    end.

bridge_v1_enable_disable_helper(_Op, _BridgeType, _BridgeName, {error, bridge_not_found}) ->
    {error, bridge_not_found};
bridge_v1_enable_disable_helper(enable, BridgeType, BridgeName, #{connector := ConnectorName}) ->
    BridgeV2Type = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeType),
    ConnectorType = connector_type(BridgeV2Type),
    {ok, _} = emqx_connector:disable_enable(enable, ConnectorType, ConnectorName),
    emqx_bridge_v2:disable_enable(enable, BridgeV2Type, BridgeName);
bridge_v1_enable_disable_helper(disable, BridgeType, BridgeName, #{connector := ConnectorName}) ->
    BridgeV2Type = emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(BridgeType),
    ConnectorType = connector_type(BridgeV2Type),
    {ok, _} = emqx_bridge_v2:disable_enable(disable, BridgeV2Type, BridgeName),
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
            connector_operation_helper_with_conf(
                BridgeV2Type,
                Name,
                lookup_conf(BridgeV2Type, Name),
                ConnectorOpFun,
                DoHealthCheck
            );
        false ->
            {error, not_bridge_v1_compatible}
    end.

%%====================================================================
%% Misc helper functions
%%====================================================================

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

to_existing_atom(X) ->
    case emqx_utils:safe_to_existing_atom(X, utf8) of
        {ok, A} -> A;
        {error, _} -> throw(bad_atom)
    end.

validate_referenced_connectors(BridgeType, ConnectorNameBin, BridgeName) ->
    %% N.B.: assumes that, for all bridgeV2 types, the name of the bridge type is
    %% identical to its matching connector type name.
    try
        {ConnectorName, ConnectorType} = to_connector(ConnectorNameBin, BridgeType),
        case emqx_config:get([connectors, ConnectorType, ConnectorName], undefined) of
            undefined ->
                throw(not_found);
            _ ->
                ok
        end
    catch
        throw:not_found ->
            {error, #{
                reason => "connector_not_found_or_wrong_type",
                connector_name => ConnectorNameBin,
                bridge_name => BridgeName,
                bridge_type => BridgeType
            }}
    end.

to_connector(ConnectorNameBin, BridgeType) ->
    try
        ConnectorType = ?MODULE:bridge_v2_type_to_connector_type(to_existing_atom(BridgeType)),
        ConnectorName = to_existing_atom(ConnectorNameBin),
        {ConnectorName, ConnectorType}
    catch
        _:_ ->
            throw(not_found)
    end.

multi_validate_referenced_connectors(Configs) ->
    Pipeline =
        lists:map(
            fun({Type, BridgeName, #{connector := ConnectorName}}) ->
                fun(_) -> validate_referenced_connectors(Type, ConnectorName, BridgeName) end
            end,
            Configs
        ),
    case emqx_utils:pipeline(Pipeline, unused, unused) of
        {ok, _, _} ->
            ok;
        {error, Reason, _State} ->
            {error, Reason}
    end.
