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
% -behaviour(emqx_config_backup).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ROOT_KEY, bridges_v2).

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
    remove/2,
    check_deps_and_remove/3
]).

%% Operations
-export([
    disable_enable/3,
    health_check/2,
    send_message/4,
    start/2,
    stop/2,
    restart/2,
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
    bridge_v2_type_to_connector_type/1,
    id/2,
    id/3,
    is_valid_bridge_v1/2
]).

%% Config Update Handler API

-export([
    post_config_update/5,
    pre_config_update/3
]).

%% Compatibility API

-export([
    is_bridge_v2_type/1,
    lookup_and_transform_to_bridge_v1/2,
    list_and_transform_to_bridge_v1/0,
    bridge_v1_check_deps_and_remove/3,
    split_bridge_v1_config_and_create/3,
    bridge_v1_create_dry_run/2,
    extract_connector_id_from_bridge_v2_id/1,
    bridge_v1_type_to_bridge_v2_type/1,
    bridge_v1_id_to_connector_resource_id/1
]).

%%====================================================================
%% Loading and unloading config when EMQX starts and stops
%%====================================================================

load() ->
    load_message_publish_hook(),
    ok = emqx_config_handler:add_handler(config_key_path_leaf(), emqx_bridge_v2),
    ok = emqx_config_handler:add_handler(config_key_path(), emqx_bridge_v2),
    ok.

unload() ->
    unload_message_publish_hook(),
    emqx_conf:remove_handler(config_key_path()),
    emqx_conf:remove_handler(config_key_path_leaf()),
    ok.

%%====================================================================
%% CRUD API
%%====================================================================

lookup(Type, Name) ->
    case emqx:get_raw_config([?ROOT_KEY, Type, Name], not_found) of
        not_found ->
            {error, not_found};
        #{<<"connector">> := BridgeConnector} = RawConf ->
            ConnectorId = emqx_connector_resource:resource_id(
                ?MODULE:bridge_v2_type_to_connector_type(Type), BridgeConnector
            ),
            %% The connector should always exist
            InstanceData =
                case emqx_resource:get_instance(ConnectorId) of
                    {ok, _, Data} ->
                        Data
                end,
            {ok, #{
                type => Type,
                name => Name,
                raw_config => RawConf,
                resource_data => InstanceData
            }}
    end.

list() ->
    list_with_lookup_fun(fun lookup/2).

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

%% NOTE: This function can cause broken references but it is only called from
%% test cases.
remove(BridgeType, BridgeName) ->
    ?SLOG(debug, #{
        brige_action => remove,
        bridge_version => 2,
        bridge_type => BridgeType,
        bridge_name => BridgeName
    }),
    emqx_conf:remove(
        config_key_path() ++ [BridgeType, BridgeName],
        #{override_to => cluster}
    ).

check_deps_and_remove(BridgeType, BridgeName, RemoveDeps) ->
    BridgeId = external_id(BridgeType, BridgeName),
    %% NOTE: This violates the design: Rule depends on data-bridge but not vice versa.
    case emqx_rule_engine:get_rule_ids_by_action(BridgeId) of
        [] ->
            remove(BridgeType, BridgeName);
        RuleIds when RemoveDeps =:= false ->
            {error, {rules_deps_on_this_bridge, RuleIds}};
        RuleIds when RemoveDeps =:= true ->
            lists:foreach(
                fun(R) ->
                    emqx_rule_engine:ensure_action_removed(R, BridgeId)
                end,
                RuleIds
            ),
            remove(BridgeType, BridgeName)
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
                            {ok, BridgeInfo} =
                                LookupFun(Type, Name),
                            BridgeInfo
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
    #{connector := ConnectorName} = Config
) ->
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    BridgeV2Id = id(BridgeV2Type, BridgeName, ConnectorName),
    %% Create metrics for Bridge V2
    ok = emqx_resource:create_metrics(BridgeV2Id),
    %% We might need to create buffer workers for Bridge V2
    case get_query_mode(BridgeV2Type, Config) of
        %% the Bridge V2 has built-in buffer, so there is no need for resource workers
        simple_sync ->
            ok;
        simple_async ->
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
        ?MODULE:bridge_v2_type_to_connector_type(BridgeV2Type), ConnectorName
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
    %% Deinstall from connector
    ConnectorId = emqx_connector_resource:resource_id(
        ?MODULE:bridge_v2_type_to_connector_type(BridgeV2Type), ConnectorName
    ),
    emqx_resource_manager:remove_channel(ConnectorId, BridgeV2Id).

%% Creates the external id for the bridge_v2 that is used by the rule actions
%% to refer to the bridge_v2
external_id(BridgeType, BridgeName) ->
    Name = bin(BridgeName),
    Type = bin(BridgeType),
    <<Type/binary, ":", Name/binary>>.

%%====================================================================
%% Operations
%%====================================================================

disable_enable(Action, BridgeType, BridgeName) when
    Action =:= disable; Action =:= enable
->
    emqx_conf:update(
        config_key_path() ++ [BridgeType, BridgeName],
        {Action, BridgeType, BridgeName},
        #{override_to => cluster}
    ).

restart(Type, Name) ->
    stop(Type, Name),
    start(Type, Name).

%% TODO: it is not clear what these operations should do

stop(Type, Name) ->
    %% Stop means that we should remove the channel from the connector and reset the metrics
    %% The emqx_resource_buffer_worker is not stopped
    stop_helper(Type, Name, lookup_raw_conf(Type, Name)).

stop_helper(_Type, _Name, #{enable := false}) ->
    ok;
stop_helper(BridgeV2Type, BridgeName, #{connector := ConnectorName}) ->
    BridgeV2Id = id(BridgeV2Type, BridgeName, ConnectorName),
    ok = emqx_metrics_worker:reset_metrics(?RES_METRICS, BridgeV2Id),
    ConnectorId = emqx_connector_resource:resource_id(
        ?MODULE:bridge_v2_type_to_connector_type(BridgeV2Type), ConnectorName
    ),
    emqx_resource_manager:remove_channel(ConnectorId, BridgeV2Id).

start(Type, Name) ->
    %% Start means that we should add the channel to the connector (if it is not already there)
    start_helper(Type, Name, lookup_raw_conf(Type, Name)).

start_helper(_Type, _Name, #{enable := false}) ->
    ok;
start_helper(BridgeV2Type, BridgeName, #{connector := ConnectorName} = Config) ->
    BridgeV2Id = id(BridgeV2Type, BridgeName, ConnectorName),
    %% Deinstall from connector
    ConnectorId = emqx_connector_resource:resource_id(
        ?MODULE:bridge_v2_type_to_connector_type(BridgeV2Type), ConnectorName
    ),
    ConfigWithTypeAndName = Config#{
        bridge_type => bin(BridgeV2Type),
        bridge_name => bin(BridgeName)
    },
    emqx_resource_manager:add_channel(
        ConnectorId,
        BridgeV2Id,
        ConfigWithTypeAndName
    ).

reset_metrics(Type, Name) ->
    reset_metrics_helper(Type, Name, lookup_raw_conf(Type, Name)).

reset_metrics_helper(_Type, _Name, #{enable := false}) ->
    ok;
reset_metrics_helper(BridgeV2Type, BridgeName, #{connector := ConnectorName}) ->
    BridgeV2Id = id(BridgeV2Type, BridgeName, ConnectorName),
    ok = emqx_metrics_worker:reset_metrics(?RES_METRICS, BridgeV2Id).

get_query_mode(BridgeV2Type, Config) ->
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    ConnectorType = ?MODULE:bridge_v2_type_to_connector_type(BridgeV2Type),
    ResourceType = emqx_connector_resource:connector_to_resource_type(ConnectorType),
    emqx_resource:query_mode(ResourceType, Config, CreationOpts).

send_message(BridgeType, BridgeName, Message, QueryOpts0) ->
    case lookup_raw_conf(BridgeType, BridgeName) of
        #{enable := true} = Config ->
            do_send_msg_with_enabled_config(BridgeType, BridgeName, Message, QueryOpts0, Config);
        #{enable := false} ->
            {error, bridge_stopped};
        _Error ->
            {error, bridge_not_found}
    end.

do_send_msg_with_enabled_config(
    BridgeType, BridgeName, Message, QueryOpts0, Config
) ->
    QueryMode = get_query_mode(BridgeType, Config),
    QueryOpts = maps:merge(
        emqx_bridge:query_opts(Config),
        QueryOpts0#{
            query_mode => QueryMode,
            query_mode_cache_override => false
        }
    ),
    BridgeV2Id = emqx_bridge_v2:id(BridgeType, BridgeName),
    emqx_resource:query(BridgeV2Id, {BridgeV2Id, Message}, QueryOpts).

health_check(BridgeType, BridgeName) ->
    case lookup_raw_conf(BridgeType, BridgeName) of
        #{
            enable := true,
            connector := ConnectorName
        } ->
            ConnectorId = emqx_connector_resource:resource_id(
                ?MODULE:bridge_v2_type_to_connector_type(BridgeType), ConnectorName
            ),
            emqx_resource_manager:channel_health_check(
                ConnectorId, id(BridgeType, BridgeName, ConnectorName)
            );
        #{enable := false} ->
            {error, bridge_stopped};
        Error ->
            Error
    end.

create_dry_run_helper(BridgeType, ConnectorRawConf, BridgeV2RawConf) ->
    BridgeName = iolist_to_binary([?TEST_ID_PREFIX, emqx_utils:gen_id(8)]),
    ConnectorType = ?MODULE:bridge_v2_type_to_connector_type(BridgeType),
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
                        {error, Reason} ->
                            {error, Reason};
                        _ ->
                            ok
                    end
            end
        end,
    emqx_connector_resource:create_dry_run(ConnectorType, ConnectorRawConf, OnReadyCallback).

create_dry_run(Type, Conf0) ->
    Conf1 = maps:without([<<"name">>], Conf0),
    TypeBin = bin(Type),
    RawConf = #{<<"bridges_v2">> => #{TypeBin => #{<<"temp_name">> => Conf1}}},
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
        ConnectorType = ?MODULE:bridge_v2_type_to_connector_type(Type),
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
        [<<"bridge_v2">>, Type, Name | _] ->
            {Type, Name};
        _X ->
            error({error, iolist_to_binary(io_lib:format("Invalid id: ~p", [Id]))})
    end.

get_channels_for_connector(ConnectorId) ->
    {ConnectorType, ConnectorName} = emqx_connector_resource:parse_connector_id(ConnectorId),
    RootConf = maps:keys(emqx:get_config([?ROOT_KEY], #{})),
    RelevantBridgeV2Types = [
        Type
     || Type <- RootConf,
        ?MODULE:bridge_v2_type_to_connector_type(Type) =:= ConnectorType
    ],
    lists:flatten([
        get_channels_for_connector(ConnectorName, BridgeV2Type)
     || BridgeV2Type <- RelevantBridgeV2Types
    ]).

get_channels_for_connector(ConnectorName, BridgeV2Type) ->
    BridgeV2s = emqx:get_config([?ROOT_KEY, BridgeV2Type], #{}),
    [
        {id(BridgeV2Type, Name, ConnectorName), Conf}
     || {Name, Conf} <- maps:to_list(BridgeV2s),
        bin(ConnectorName) =:= maps:get(connector, Conf, no_name)
    ].

%%====================================================================
%% Exported for tests
%%====================================================================

id(BridgeType, BridgeName) ->
    case lookup_raw_conf(BridgeType, BridgeName) of
        #{connector := ConnectorName} ->
            id(BridgeType, BridgeName, ConnectorName);
        Error ->
            error(Error)
    end.

id(BridgeType, BridgeName, ConnectorName) ->
    ConnectorType = bin(?MODULE:bridge_v2_type_to_connector_type(BridgeType)),
    <<"bridge_v2:", (bin(BridgeType))/binary, ":", (bin(BridgeName))/binary, ":connector:",
        (bin(ConnectorType))/binary, ":", (bin(ConnectorName))/binary>>.

bridge_v2_type_to_connector_type(Bin) when is_binary(Bin) ->
    ?MODULE:bridge_v2_type_to_connector_type(binary_to_existing_atom(Bin));
bridge_v2_type_to_connector_type(kafka) ->
    kafka.

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

%% This top level handler will be triggered when the bridges_v2 path is updated
%% with calls to emqx_conf:update([bridges_v2], BridgesConf, #{}).
%%
%% A public API that can trigger this is:
%% bin/emqx ctl conf load data/configs/cluster.hocon
post_config_update([?ROOT_KEY], _Req, NewConf, OldConf, _AppEnv) ->
    #{added := Added, removed := Removed, changed := Updated} =
        diff_confs(NewConf, OldConf),
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
post_config_update([?ROOT_KEY, BridgeType, BridgeName], '$remove', _, _OldConf, _AppEnvs) ->
    Conf = emqx:get_config([?ROOT_KEY, BridgeType, BridgeName]),
    ok = uninstall_bridge_v2(BridgeType, BridgeName, Conf),
    Bridges = emqx_utils_maps:deep_remove([BridgeType, BridgeName], emqx:get_config([?ROOT_KEY])),
    reload_message_publish_hook(Bridges),
    ?tp(bridge_post_config_update_done, #{}),
    ok;
post_config_update([?ROOT_KEY, BridgeType, BridgeName], _Req, NewConf, undefined, _AppEnvs) ->
    ok = install_bridge_v2(BridgeType, BridgeName, NewConf),
    Bridges = emqx_utils_maps:deep_put(
        [BridgeType, BridgeName], emqx:get_config([?ROOT_KEY]), NewConf
    ),
    reload_message_publish_hook(Bridges),
    ?tp(bridge_post_config_update_done, #{}),
    ok;
post_config_update([?ROOT_KEY, BridgeType, BridgeName], _Req, NewConf, OldConf, _AppEnvs) ->
    ok = uninstall_bridge_v2(BridgeType, BridgeName, OldConf),
    ok = install_bridge_v2(BridgeType, BridgeName, NewConf),
    Bridges = emqx_utils_maps:deep_put(
        [BridgeType, BridgeName], emqx:get_config([?ROOT_KEY]), NewConf
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
%% * The connector for the bridge v2 should have exactly on channel
is_valid_bridge_v1(BridgeV1Type, BridgeName) ->
    BridgeV2Type = ?MODULE:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    case lookup_raw_conf(BridgeV2Type, BridgeName) of
        {error, _} ->
            false;
        #{connector := ConnectorName} ->
            ConnectorType = ?MODULE:bridge_v2_type_to_connector_type(BridgeV2Type),
            ConnectorResourceId = emqx_connector_resource:resource_id(ConnectorType, ConnectorName),
            {ok, Channels} = emqx_resource:get_channels(ConnectorResourceId),
            case Channels of
                [_Channel] ->
                    true;
                _ ->
                    false
            end
    end.

bridge_v1_type_to_bridge_v2_type(Bin) when is_binary(Bin) ->
    ?MODULE:bridge_v1_type_to_bridge_v2_type(binary_to_existing_atom(Bin));
bridge_v1_type_to_bridge_v2_type(kafka) ->
    kafka.

is_bridge_v2_type(Atom) when is_atom(Atom) ->
    is_bridge_v2_type(atom_to_binary(Atom, utf8));
is_bridge_v2_type(<<"kafka">>) ->
    true;
is_bridge_v2_type(_) ->
    false.

list_and_transform_to_bridge_v1() ->
    list_with_lookup_fun(fun lookup_and_transform_to_bridge_v1/2).

lookup_and_transform_to_bridge_v1(Type, Name) ->
    case lookup(Type, Name) of
        {ok, #{raw_config := #{<<"connector">> := ConnectorName}} = BridgeV2} ->
            ConnectorType = ?MODULE:bridge_v2_type_to_connector_type(Type),
            case emqx_connector:lookup(ConnectorType, ConnectorName) of
                {ok, Connector} ->
                    lookup_and_transform_to_bridge_v1_helper(
                        Type, BridgeV2, ConnectorType, Connector
                    );
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

lookup_and_transform_to_bridge_v1_helper(BridgeV2Type, BridgeV2, ConnectorType, Connector) ->
    ConnectorRawConfig1 = maps:get(raw_config, Connector),
    ConnectorRawConfig2 = fill_defaults(
        ConnectorType,
        ConnectorRawConfig1,
        <<"connectors">>,
        emqx_connector_schema
    ),
    BridgeV2RawConfig1 = maps:get(raw_config, BridgeV2),
    BridgeV2RawConfig2 = fill_defaults(
        BridgeV2Type,
        BridgeV2RawConfig1,
        <<"bridges_v2">>,
        emqx_bridge_v2_schema
    ),
    BridgeV1Config1 = maps:remove(<<"connector">>, BridgeV2RawConfig2),
    BridgeV1Config2 = maps:merge(BridgeV1Config1, ConnectorRawConfig2),
    BridgeV1 = maps:put(raw_config, BridgeV1Config2, BridgeV2),
    {ok, BridgeV1}.

lookup_raw_conf(Type, Name) ->
    case emqx:get_config([?ROOT_KEY, Type, Name], not_found) of
        not_found ->
            {error, bridge_not_found};
        Config ->
            Config
    end.

split_bridge_v1_config_and_create(BridgeV1Type, BridgeName, RawConf) ->
    #{
        connector_type := ConnectorType,
        connector_name := NewConnectorName,
        connector_conf := NewConnectorRawConf,
        bridge_v2_type := BridgeType,
        bridge_v2_name := BridgeName,
        bridge_v2_conf := NewBridgeV2RawConf
    } =
        split_and_validate_bridge_v1_config(BridgeV1Type, BridgeName, RawConf),
    %% TODO should we really create an atom here?
    ConnectorNameAtom = binary_to_atom(NewConnectorName),
    case emqx_connector:create(ConnectorType, ConnectorNameAtom, NewConnectorRawConf) of
        {ok, _} ->
            case create(BridgeType, BridgeName, NewBridgeV2RawConf) of
                {ok, _} = Result ->
                    Result;
                Error ->
                    case emqx_connector:remove(ConnectorType, ConnectorNameAtom) of
                        {ok, _} ->
                            Error;
                        Error ->
                            %% TODO log error
                            ?SLOG(warning, #{
                                message =>
                                    <<"Failed to remove connector after bridge creation failed">>,
                                bridge_version => 2,
                                bridge_type => BridgeType,
                                bridge_name => BridgeName,
                                bridge_raw_config => emqx_utils:redact(RawConf)
                            }),
                            Error
                    end
            end;
        Error ->
            Error
    end.

split_and_validate_bridge_v1_config(BridgeType, BridgeName, RawConf) ->
    %% Create fake global config for the transformation and then call
    %% emqx_connector_schema:transform_bridges_v1_to_connectors_and_bridges_v2/1

    ConnectorType = ?MODULE:bridge_v2_type_to_connector_type(BridgeType),
    %% Needed so name confligts will ba avoided
    CurrentConnectorsConfig = emqx:get_raw_config([connectors], #{}),
    FakeGlobalConfig = #{
        <<"connectors">> => CurrentConnectorsConfig,
        <<"bridges">> => #{
            bin(BridgeType) => #{
                bin(BridgeName) => RawConf
            }
        }
    },
    Output = emqx_connector_schema:transform_bridges_v1_to_connectors_and_bridges_v2(
        FakeGlobalConfig
    ),
    NewBridgeV2RawConf =
        emqx_utils_maps:deep_get(
            [
                bin(?ROOT_KEY),
                bin(BridgeType),
                bin(BridgeName)
            ],
            Output
        ),
    ConnectorsBefore =
        maps:keys(
            emqx_utils_maps:deep_get(
                [
                    <<"connectors">>,
                    bin(ConnectorType)
                ],
                FakeGlobalConfig,
                #{}
            )
        ),
    ConnectorsAfter =
        maps:keys(
            emqx_utils_maps:deep_get(
                [
                    <<"connectors">>,
                    bin(ConnectorType)
                ],
                Output
            )
        ),
    [NewConnectorName] = ConnectorsAfter -- ConnectorsBefore,
    NewConnectorRawConf =
        emqx_utils_maps:deep_get(
            [
                <<"connectors">>,
                bin(ConnectorType),
                bin(NewConnectorName)
            ],
            Output
        ),
    %% Validate the connector config and the bridge_v2 config
    NewFakeGlobalConfig = #{
        <<"connectors">> => #{
            bin(ConnectorType) => #{
                bin(NewConnectorName) => NewConnectorRawConf
            }
        },
        <<"bridges_v2">> => #{
            bin(BridgeType) => #{
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
                connector_name => NewConnectorName,
                connector_conf => NewConnectorRawConf,
                bridge_v2_type => BridgeType,
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
    #{
        connector_type := _ConnectorType,
        connector_name := _NewConnectorName,
        connector_conf := ConnectorRawConf,
        bridge_v2_type := BridgeType,
        bridge_v2_name := _BridgeName,
        bridge_v2_conf := BridgeV2RawConf
    } = split_and_validate_bridge_v1_config(BridgeType, TmpName, RawConf),
    create_dry_run_helper(BridgeType, ConnectorRawConf, BridgeV2RawConf).

bridge_v1_check_deps_and_remove(BridgeType, BridgeName, RemoveDeps) ->
    bridge_v1_check_deps_and_remove(
        BridgeType,
        BridgeName,
        RemoveDeps,
        lookup_raw_conf(BridgeType, BridgeName)
    ).

bridge_v1_check_deps_and_remove(
    BridgeType,
    BridgeName,
    RemoveDeps,
    #{connector := ConnectorName} = Conf
) ->
    case check_deps_and_remove(BridgeType, BridgeName, RemoveDeps) of
        {error, _} = Error ->
            Error;
        Result ->
            %% Check if there are other channels that depends on the same connector
            case connector_has_channels(BridgeType, ConnectorName) of
                false ->
                    ConnectorType = ?MODULE:bridge_v2_type_to_connector_type(BridgeType),
                    case emqx_connector:remove(ConnectorType, ConnectorName) of
                        {ok, _} ->
                            ok;
                        Error ->
                            ?SLOG(warning, #{
                                message => <<"Failed to remove connector after bridge removal">>,
                                bridge_version => 2,
                                bridge_type => BridgeType,
                                bridge_name => BridgeName,
                                error => Error,
                                bridge_raw_config => emqx_utils:redact(Conf)
                            }),
                            ok
                    end;
                true ->
                    ok
            end,
            Result
    end;
bridge_v1_check_deps_and_remove(_BridgeType, _BridgeName, _RemoveDeps, Error) ->
    Error.

connector_has_channels(BridgeV2Type, ConnectorName) ->
    ConnectorType = ?MODULE:bridge_v2_type_to_connector_type(BridgeV2Type),
    case emqx_connector_resource:get_channels(ConnectorType, ConnectorName) of
        {ok, []} ->
            false;
        _ ->
            true
    end.

bridge_v1_id_to_connector_resource_id(BridgeId) ->
    case binary:split(BridgeId, <<":">>) of
        [Type, Name] ->
            BridgeV2Type = bin(?MODULE:bridge_v1_type_to_bridge_v2_type(Type)),
            ConnectorName =
                case lookup_raw_conf(BridgeV2Type, Name) of
                    #{connector := Con} ->
                        Con;
                    Error ->
                        throw(Error)
                end,
            ConnectorType = bin(?MODULE:bridge_v2_type_to_connector_type(BridgeV2Type)),
            <<"connector:", ConnectorType/binary, ":", ConnectorName/binary>>
    end.

%%====================================================================
%% Misc helper functions
%%====================================================================

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

extract_connector_id_from_bridge_v2_id(Id) ->
    case binary:split(Id, <<":">>, [global]) of
        [<<"bridge_v2">>, _Type, _Name, <<"connector">>, ConnectorType, ConnecorName] ->
            <<"connector:", ConnectorType/binary, ":", ConnecorName/binary>>;
        _X ->
            error({error, iolist_to_binary(io_lib:format("Invalid bridge V2 ID: ~p", [Id]))})
    end.
