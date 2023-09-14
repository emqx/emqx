%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_conf).

%% TODO: make a proper HOCON schema and all...

%% API:
-export([keyspace_config/1, db_options/1]).

-export([iteration_options/1]).
-export([default_iteration_options/0]).

-type backend_config() ::
    {emqx_ds_message_storage_bitmask, emqx_ds_message_storage_bitmask:options()}
    | {module(), _Options}.

-export_type([backend_config/0]).

%%================================================================================
%% API funcions
%%================================================================================

-define(APP, emqx_ds).

-spec keyspace_config(emqx_ds:keyspace()) -> backend_config().
keyspace_config(Keyspace) ->
    DefaultKeyspaceConfig = application:get_env(
        ?APP,
        default_keyspace_config,
        default_keyspace_config()
    ),
    Keyspaces = application:get_env(?APP, keyspace_config, #{}),
    maps:get(Keyspace, Keyspaces, DefaultKeyspaceConfig).

-spec iteration_options(emqx_ds:keyspace()) ->
    emqx_ds_message_storage_bitmask:iteration_options().
iteration_options(Keyspace) ->
    case keyspace_config(Keyspace) of
        {emqx_ds_message_storage_bitmask, Config} ->
            maps:get(iteration, Config, default_iteration_options());
        {_Module, _} ->
            default_iteration_options()
    end.

-spec default_iteration_options() -> emqx_ds_message_storage_bitmask:iteration_options().
default_iteration_options() ->
    {emqx_ds_message_storage_bitmask, Config} = default_keyspace_config(),
    maps:get(iteration, Config).

-spec default_keyspace_config() -> backend_config().
default_keyspace_config() ->
    {emqx_ds_message_storage_bitmask, #{
        db_options => [],
        timestamp_bits => 64,
        topic_bits_per_level => [8, 8, 8, 32, 16],
        epoch => 5,
        iteration => #{
            iterator_refresh => {every, 100}
        }
    }}.

-spec db_options(emqx_ds:keyspace()) -> emqx_ds_storage_layer:db_options().
db_options(Keyspace) ->
    DefaultDBOptions = application:get_env(?APP, default_db_options, []),
    Keyspaces = application:get_env(?APP, keyspace_config, #{}),
    emqx_utils_maps:deep_get([Keyspace, db_options], Keyspaces, DefaultDBOptions).
