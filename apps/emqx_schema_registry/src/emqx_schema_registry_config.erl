%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_config).

-feature(maybe_expr, enable).

-include("emqx_schema_registry.hrl").

%% API
-export([
    add_handlers/0,
    remove_handlers/0,

    list_external_registries/0,
    list_external_registries_raw/0,
    lookup_external_registry_raw/1,

    upsert_external_registry/2,
    delete_external_registry/1
]).

%% `emqx_config_handler' API
-export([post_config_update/5]).

%% `emqx_config_backup' API
-behaviour(emqx_config_backup).
-export([import_config/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(SCHEMA_CONF_PATH(NAME), [?CONF_KEY_ROOT, schemas, NAME]).
-define(EXTERNAL_REGISTRY_CONF_PATH(NAME), [?CONF_KEY_ROOT, external, NAME]).

-define(EXTERNAL_REGISTRIES_CONF_PATH, [?CONF_KEY_ROOT, external]).
-define(EXTERNAL_REGISTRIES_CONF_PATH_BIN, [?CONF_KEY_ROOT, <<"external">>]).
-define(EXTERNAL_REGISTRIES_CONF_PATH_BIN(NAME), [?CONF_KEY_ROOT, <<"external">>, NAME]).

-type external_registry_name() :: binary().
-type external_registry_raw() :: #{binary() => term()}.

-type external_registry() :: external_registry_confluent().

-type external_registry_confluent() :: #{type := confluent}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec add_handlers() -> ok.
add_handlers() ->
    %% HTTP API handlers
    ok = emqx_conf:add_handler([?CONF_KEY_ROOT, schemas, '?'], ?MODULE),
    ok = emqx_conf:add_handler([?CONF_KEY_ROOT, external, '?'], ?MODULE),
    %% Conf load / data import handler
    ok = emqx_conf:add_handler(?CONF_KEY_PATH, ?MODULE),
    ok.

-spec remove_handlers() -> ok.
remove_handlers() ->
    ok = emqx_conf:remove_handler([?CONF_KEY_ROOT, external, '?']),
    ok = emqx_conf:remove_handler([?CONF_KEY_ROOT, schemas, '?']),
    ok = emqx_conf:remove_handler(?CONF_KEY_PATH),
    ok.

-spec list_external_registries() ->
    #{atom() => external_registry()}.
list_external_registries() ->
    emqx:get_config(?EXTERNAL_REGISTRIES_CONF_PATH, #{}).

-spec list_external_registries_raw() ->
    #{external_registry_name() => external_registry_raw()}.
list_external_registries_raw() ->
    emqx:get_raw_config(?EXTERNAL_REGISTRIES_CONF_PATH_BIN, #{}).

-spec lookup_external_registry_raw(external_registry_name()) ->
    {ok, external_registry_raw()} | {error, not_found}.
lookup_external_registry_raw(Name) ->
    case emqx:get_raw_config(?EXTERNAL_REGISTRIES_CONF_PATH_BIN(Name), undefined) of
        undefined ->
            {error, not_found};
        Registry ->
            {ok, Registry}
    end.

-spec upsert_external_registry(external_registry_name(), external_registry_raw()) ->
    {ok, external_registry_raw()} | {error, term()}.
upsert_external_registry(Name, RegistryRaw) ->
    case
        emqx_conf:update(
            ?EXTERNAL_REGISTRY_CONF_PATH(Name),
            RegistryRaw,
            #{override_to => cluster}
        )
    of
        {ok, #{raw_config := NewRegistryRaw}} ->
            {ok, NewRegistryRaw};
        {error, _} = Error ->
            Error
    end.

-spec delete_external_registry(external_registry_name()) ->
    ok | {error, term()}.
delete_external_registry(Name) ->
    case emqx_conf:remove(?EXTERNAL_REGISTRY_CONF_PATH(Name), #{override_to => cluster}) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% `emqx_config_handler' API
%%------------------------------------------------------------------------------

%% remove schema
post_config_update(
    ?SCHEMA_CONF_PATH(Name),
    '$remove',
    _NewSchemas,
    _OldSchemas,
    _AppEnvs
) ->
    emqx_schema_registry:async_delete_serdes([Name]),
    ok;
%% add or update schema
post_config_update(
    ?SCHEMA_CONF_PATH(NewName),
    _Cmd,
    NewSchema,
    OldSchema,
    _AppEnvs
) ->
    case OldSchema of
        undefined ->
            ok;
        _ ->
            emqx_schema_registry:ensure_serde_absent(NewName)
    end,
    case emqx_schema_registry:build_serdes([{NewName, NewSchema}]) of
        ok ->
            {ok, #{NewName => NewSchema}};
        {error, Reason, SerdesToRollback} ->
            lists:foreach(fun emqx_schema_registry:ensure_serde_absent/1, SerdesToRollback),
            {error, Reason}
    end;
%% remove external registry
post_config_update(
    ?EXTERNAL_REGISTRY_CONF_PATH(Name),
    '$remove',
    _New,
    _Old,
    _AppEnvs
) ->
    remove_external_registry(Name),
    ok;
%% add or update external registry
post_config_update(
    ?EXTERNAL_REGISTRY_CONF_PATH(Name),
    _Cmd,
    NewConfig,
    _Old,
    _AppEnvs
) ->
    do_upsert_external_registry(Name, NewConfig),
    ok;
post_config_update([?CONF_KEY_ROOT], _Cmd, NewConf, OldConf, _AppEnvs) ->
    Context0 = #{},
    maybe
        {ok, Context1} ?= handle_import_schemas(Context0, NewConf, OldConf),
        {ok, _Context2} ?= handle_import_external_registries(Context1, NewConf, OldConf)
    end;
post_config_update(_Path, _Cmd, NewConf, _OldConf, _AppEnvs) ->
    {ok, NewConf}.

%%------------------------------------------------------------------------------
%% `emqx_config_backup' API
%%------------------------------------------------------------------------------

import_config(#{?CONF_KEY_ROOT_BIN := RawConf0}) ->
    Result = emqx_conf:update(
        [?CONF_KEY_ROOT],
        RawConf0,
        #{override_to => cluster, rawconf_with_defaults => true}
    ),
    case Result of
        {error, Reason} ->
            {error, #{root_key => ?CONF_KEY_ROOT, reason => Reason}};
        {ok, Res} ->
            ChangedPaths = emqx_utils_maps:deep_get(
                [post_config_update, ?MODULE, changed_paths],
                Res,
                []
            ),
            {ok, #{root_key => ?CONF_KEY_ROOT, changed => ChangedPaths}}
    end;
import_config(_RawConf) ->
    {ok, #{root_key => ?CONF_KEY_ROOT, changed => []}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

add_external_registry(Name, Config) ->
    ok = emqx_schema_registry_external:add(Name, Config),
    ok.

remove_external_registry(Name) ->
    ok = emqx_schema_registry_external:remove(Name),
    ok.

%% receives parsed config with atom keys
do_upsert_external_registry(Name, NewConfig) ->
    remove_external_registry(Name),
    add_external_registry(Name, NewConfig),
    ok.

handle_import_schemas(Context0, #{schemas := NewSchemas}, OldConf) ->
    OldSchemas = maps:get(schemas, OldConf, #{}),
    #{
        added := Added,
        changed := Changed0,
        removed := Removed
    } = emqx_utils_maps:diff_maps(NewSchemas, OldSchemas),
    Changed = maps:map(fun(_N, {_Old, New}) -> New end, Changed0),
    RemovedNames = maps:keys(Removed),
    case RemovedNames of
        [] ->
            ok;
        _ ->
            emqx_schema_registry:async_delete_serdes(RemovedNames)
    end,
    SchemasToBuild = maps:to_list(maps:merge(Changed, Added)),
    ok = lists:foreach(fun emqx_schema_registry:ensure_serde_absent/1, [
        N
     || {N, _} <- SchemasToBuild
    ]),
    case emqx_schema_registry:build_serdes(SchemasToBuild) of
        ok ->
            ChangedPaths = [
                [?CONF_KEY_ROOT, schemas, N]
             || {N, _} <- SchemasToBuild
            ],
            Context = maps:update_with(
                changed_paths,
                fun(Ps) -> ChangedPaths ++ Ps end,
                ChangedPaths,
                Context0
            ),
            {ok, Context};
        {error, Reason, SerdesToRollback} ->
            lists:foreach(fun emqx_schema_registry:ensure_serde_absent/1, SerdesToRollback),
            {error, Reason}
    end.

handle_import_external_registries(Context0, NewConf, OldConf) ->
    New = maps:get(external, NewConf, #{}),
    Old = maps:get(external, OldConf, #{}),
    #{
        added := Added,
        changed := Changed0,
        removed := Removed
    } = emqx_utils_maps:diff_maps(New, Old),
    Changed = maps:map(fun(_N, {_Old, New0}) -> New0 end, Changed0),
    RemovedNames = maps:keys(Removed),
    RegistriesToUpsert = maps:to_list(maps:merge(Changed, Added)),
    lists:foreach(fun remove_external_registry/1, RemovedNames),
    lists:foreach(
        fun({Name, Cfg}) -> do_upsert_external_registry(Name, Cfg) end,
        RegistriesToUpsert
    ),
    ChangedPaths = [
        [?CONF_KEY_ROOT, external, N]
     || {N, _} <- RegistriesToUpsert
    ],
    Context = maps:update_with(
        changed_paths,
        fun(Ps) -> ChangedPaths ++ Ps end,
        ChangedPaths,
        Context0
    ),
    {ok, Context}.
