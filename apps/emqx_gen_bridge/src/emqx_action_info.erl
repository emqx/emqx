%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The module which knows everything about actions.

-module(emqx_action_info).

-export([
    action_type_to_connector_type/1,
    is_source/1,
    is_action/1,
    registered_schema_modules_actions/0,
    registered_schema_modules_sources/0,
    action_convert_from_connector/3
]).
-export([clean_cache/0]).

%% For tests
-export([hard_coded_test_action_info_modules/0]).

-ifdef(TEST).
-export([get_action_info_module/1]).
-endif.

-callback action_type_name() -> atom().
-callback connector_type_name() -> atom().
-callback schema_module() -> atom().
-callback is_source() ->
    boolean().
-callback is_action() ->
    boolean().

-optional_callbacks([
    is_source/0,
    is_action/0
]).

%% ====================================================================
%% HardCoded list of info modules for actions
%% TODO: Remove this list once we have made sure that all relevants
%% apps are loaded before this module is called.
%% ====================================================================

hard_coded_action_info_modules_ee() ->
    [
        emqx_bridge_alloydb_action_info,
        emqx_bridge_aws_timestream_action_info,
        emqx_bridge_azure_blob_storage_action_info,
        emqx_bridge_azure_event_hub_action_info,
        emqx_bridge_bigquery_action_info,
        emqx_bridge_cassandra_action_info,
        emqx_bridge_clickhouse_action_info,
        emqx_bridge_cockroachdb_action_info,
        emqx_bridge_confluent_producer_action_info,
        emqx_bridge_couchbase_action_info,
        emqx_bridge_disk_log_action_info,
        emqx_bridge_doris_action_info,
        emqx_bridge_dynamo_action_info,
        emqx_bridge_emqx_tables_action_info,
        emqx_bridge_es_action_info,
        emqx_bridge_gcp_pubsub_consumer_action_info,
        emqx_bridge_gcp_pubsub_producer_action_info,
        emqx_bridge_greptimedb_action_info,
        emqx_bridge_influxdb_action_info,
        emqx_bridge_iotdb_action_info,
        emqx_bridge_kafka_consumer_action_info,
        emqx_bridge_kafka_producer_action_info,
        emqx_bridge_kinesis_action_info,
        emqx_bridge_matrix_action_info,
        emqx_bridge_mongodb_action_info,
        emqx_bridge_mysql_action_info,
        emqx_bridge_opents_action_info,
        emqx_bridge_oracle_action_info,
        emqx_bridge_pgsql_action_info,
        emqx_bridge_pulsar_action_info,
        emqx_bridge_rabbitmq_action_info,
        emqx_bridge_redis_action_info,
        emqx_bridge_redshift_action_info,
        emqx_bridge_rocketmq_action_info,
        emqx_bridge_s3_upload_action_info,
        emqx_bridge_s3tables_action_info,
        emqx_bridge_snowflake_aggregated_action_info,
        emqx_bridge_snowflake_streaming_action_info,
        emqx_bridge_sqlserver_action_info,
        emqx_bridge_syskeeper_action_info,
        emqx_bridge_tdengine_action_info,
        emqx_bridge_timescale_action_info
    ].

hard_coded_action_info_modules_common() ->
    [
        emqx_bridge_http_action_info,
        emqx_bridge_mqtt_pubsub_action_info
    ].

%% This exists so that it can be mocked for test cases
hard_coded_test_action_info_modules() -> [].

hard_coded_action_info_modules() ->
    hard_coded_action_info_modules_common() ++
        hard_coded_action_info_modules_ee() ++
        ?MODULE:hard_coded_test_action_info_modules().

%% ====================================================================
%% API
%% ====================================================================

action_type_to_connector_type(Type) when not is_atom(Type) ->
    action_type_to_connector_type(binary_to_existing_atom(iolist_to_binary(Type)));
action_type_to_connector_type(Type) ->
    ActionInfoMap = info_map(),
    ActionTypeToConnectorTypeMap = maps:get(action_type_to_connector_type, ActionInfoMap),
    case maps:get(Type, ActionTypeToConnectorTypeMap, undefined) of
        undefined -> Type;
        ConnectorType -> ConnectorType
    end.

%% Returns true if the action is an ingress action, false otherwise.
is_source(Bin) when is_binary(Bin) ->
    is_source(binary_to_existing_atom(Bin));
is_source(Type) ->
    ActionInfoMap = info_map(),
    IsSourceMap = maps:get(is_source, ActionInfoMap),
    maps:get(Type, IsSourceMap, false).

%% Returns true if the action is an egress action, false otherwise.
is_action(Bin) when is_binary(Bin) ->
    is_action(binary_to_existing_atom(Bin));
is_action(Type) ->
    ActionInfoMap = info_map(),
    IsActionMap = maps:get(is_action, ActionInfoMap),
    maps:get(Type, IsActionMap, true).

registered_schema_modules_actions() ->
    InfoMap = info_map(),
    Schemas = maps:get(action_type_to_schema_module, InfoMap),
    All = maps:to_list(Schemas),
    [{Type, SchemaMod} || {Type, SchemaMod} <- All, is_action(Type)].

registered_schema_modules_sources() ->
    InfoMap = info_map(),
    Schemas = maps:get(action_type_to_schema_module, InfoMap),
    All = maps:to_list(Schemas),
    [{Type, SchemaMod} || {Type, SchemaMod} <- All, is_source(Type)].

action_convert_from_connector(ActionOrBridgeType, ConnectorConfig, ActionConfig) ->
    Module = get_action_info_module(ActionOrBridgeType),
    case erlang:function_exported(Module, action_convert_from_connector, 2) of
        true ->
            Module:action_convert_from_connector(ConnectorConfig, ActionConfig);
        false ->
            ActionConfig
    end.

%% ====================================================================
%% Internal functions for building the info map and accessing it
%% ====================================================================

get_action_info_module(ActionOrBridgeType) ->
    InfoMap = info_map(),
    ActionInfoModuleMap = maps:get(action_type_to_info_module, InfoMap),
    maps:get(ActionOrBridgeType, ActionInfoModuleMap, undefined).

internal_emqx_action_persistent_term_info_key() ->
    ?FUNCTION_NAME.

info_map() ->
    case persistent_term:get(internal_emqx_action_persistent_term_info_key(), not_found) of
        not_found ->
            build_cache();
        ActionInfoMap ->
            ActionInfoMap
    end.

build_cache() ->
    ActionInfoModules = action_info_modules(),
    ActionInfoMap =
        lists:foldl(
            fun(Module, InfoMapSoFar) ->
                ModuleInfoMap = get_info_map(Module),
                emqx_utils_maps:deep_merge(InfoMapSoFar, ModuleInfoMap)
            end,
            initial_info_map(),
            ActionInfoModules
        ),
    %% Update the persistent term with the new info map
    persistent_term:put(internal_emqx_action_persistent_term_info_key(), ActionInfoMap),
    ActionInfoMap.

clean_cache() ->
    persistent_term:erase(internal_emqx_action_persistent_term_info_key()).

action_info_modules() ->
    ActionInfoModules = [
        action_info_modules(App)
     || {App, _, _} <- application:loaded_applications()
    ],
    lists:usort(lists:flatten(ActionInfoModules) ++ hard_coded_action_info_modules()).

action_info_modules(App) ->
    case application:get_env(App, emqx_action_info_modules) of
        {ok, Modules} ->
            Modules;
        _ ->
            []
    end.

initial_info_map() ->
    #{
        action_type_names => #{},
        action_type_to_connector_type => #{},
        action_type_to_schema_module => #{},
        action_type_to_info_module => #{},
        is_source => #{},
        is_action => #{}
    }.

get_info_map(Module) ->
    %% Force the module to get loaded
    _ = code:ensure_loaded(Module),
    ActionType = Module:action_type_name(),
    IsIngress =
        case erlang:function_exported(Module, is_source, 0) of
            true ->
                Module:is_source();
            false ->
                false
        end,
    IsEgress =
        case erlang:function_exported(Module, is_action, 0) of
            true ->
                Module:is_action();
            false ->
                true
        end,
    #{
        action_type_names => #{ActionType => true},
        action_type_to_connector_type => #{ActionType => Module:connector_type_name()},
        action_type_to_schema_module => #{
            ActionType => Module:schema_module()
        },
        action_type_to_info_module => #{ActionType => Module},
        is_source => #{
            ActionType => IsIngress
        },
        is_action => #{
            ActionType => IsEgress
        }
    }.
