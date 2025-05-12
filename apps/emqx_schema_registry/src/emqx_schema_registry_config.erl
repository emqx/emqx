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
-export([pre_config_update/3, post_config_update/5]).

%% `emqx_config_backup' API
-behaviour(emqx_config_backup).
-export([import_config/1]).

-ifdef(TEST).
-export([protobuf_bundle_data_dir/1]).
-endif.

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

pre_config_update(?SCHEMA_CONF_PATH(Name), RawConf0, _OldConf) ->
    maybe
        ok ?= validate_name(Name),
        {ok, RawConf1} ?= convert_certs(Name, RawConf0),
        {ok, RawConf2} ?= convert_protobuf_bundle(Name, RawConf1),
        validate_protobuf_imports_already_converted(RawConf2)
    end;
pre_config_update(_Path, NewRawConf, _OldConf) ->
    {ok, NewRawConf}.

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
    OldRawConf = emqx:get_raw_config([?CONF_KEY_ROOT_BIN], #{}),
    RawConf = emqx_utils_maps:deep_merge(OldRawConf, RawConf0),
    Result = emqx_conf:update(
        [?CONF_KEY_ROOT],
        RawConf,
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

validate_name(Name) ->
    try
        _ = emqx_resource:validate_name(bin(Name)),
        ok
    catch
        throw:Error ->
            {error, Error}
    end.

bin(X) -> emqx_utils_conv:bin(X).

convert_certs(Name, #{<<"parameters">> := #{<<"ssl">> := SSL} = Params0} = RawConf0) when
    ?IS_TYPE_WITH_RESOURCE(RawConf0)
->
    CertsDir = filename:join(["schema_registry", "schemas", Name]),
    maybe
        {ok, NewSSL} ?= emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(CertsDir, SSL),
        {ok, RawConf0#{<<"parameters">> := Params0#{<<"ssl">> := NewSSL}}}
    end;
convert_certs(_Name, RawConf) ->
    {ok, RawConf}.

convert_protobuf_bundle(
    Name,
    #{
        <<"type">> := <<"protobuf">>,
        <<"source">> := #{
            <<"type">> := <<"bundle">>,
            <<"files">> := _
        }
    } = RawConf0
) ->
    do_convert_protobuf_bundle(Name, RawConf0);
convert_protobuf_bundle(_Name, RawConf) ->
    {ok, RawConf}.

do_convert_protobuf_bundle(Name, RawConf0) ->
    #{
        <<"type">> := <<"protobuf">>,
        <<"source">> := #{
            <<"type">> := <<"bundle">>,
            <<"files">> := Files
        }
    } = RawConf0,
    maybe
        {ok, RootFile, OtherFiles} ?= find_protobuf_bundle_root_file(Files),
        ok ?= validate_protobuf_path(RootFile),
        {ok, RootPath} ?= copy_protobuf_bundle_to_data_dir(Name, RootFile, OtherFiles),
        ok ?= validate_protobuf_imports({path, RootPath}),
        NewSource = #{
            <<"type">> => <<"bundle">>,
            <<"root_proto_path">> => RootPath
        },
        RawConf = RawConf0#{<<"source">> := NewSource},
        {ok, RawConf}
    else
        {error, {invalid_or_missing_imports, _} = Reason} ->
            delete_protobuf_bundle_data_dir(Name),
            {error, Reason};
        Error ->
            Error
    end.

find_protobuf_bundle_root_file(Files) ->
    {RootFiles, OtherFiles} =
        lists:partition(
            fun
                (#{<<"root">> := true}) -> true;
                (_) -> false
            end,
            Files
        ),
    case RootFiles of
        [] ->
            {error, <<"must have exactly one root file">>};
        [_, _ | _] ->
            {error, <<"must have exactly one root file">>};
        [RootFile] ->
            {ok, RootFile, OtherFiles}
    end.

protobuf_bundle_data_dir(Name) ->
    DataDir = emqx:data_dir(),
    filename:join([DataDir, "schemas", "proto", Name]).

validate_protobuf_path(#{<<"path">> := Path}) ->
    case filelib:safe_relative_path(Path, "") of
        unsafe ->
            {error, {bad_path, Path}};
        _ ->
            ok
    end;
validate_protobuf_path(BadFile) ->
    {error, {bad_file, BadFile}}.

%% If the config update request already points to existings files, we need to check them
%% too.
validate_protobuf_imports_already_converted(
    #{
        <<"type">> := <<"protobuf">>,
        <<"source">> := #{
            <<"type">> := <<"bundle">>,
            <<"root_proto_path">> := RootPath
        }
    } = RawConfig
) ->
    maybe
        ok ?= validate_protobuf_imports({path, RootPath}),
        {ok, RawConfig}
    end;
validate_protobuf_imports_already_converted(
    #{
        <<"type">> := <<"protobuf">>,
        <<"source">> := Source
    } = RawConfig
) when is_binary(Source) ->
    maybe
        ok ?= validate_protobuf_imports({raw, Source}),
        {ok, RawConfig}
    end;
validate_protobuf_imports_already_converted(RawConfig) ->
    {ok, RawConfig}.

validate_protobuf_imports(SourceOrRootPath) ->
    #{
        missing := Missing,
        invalid := Invalid
    } = emqx_schema_registry_serde:protobuf_resolve_imports(SourceOrRootPath),
    case {Missing, Invalid} of
        {[], []} ->
            ok;
        _ ->
            {error,
                {invalid_or_missing_imports, #{
                    missing => Missing,
                    invalid => Invalid
                }}}
    end.

delete_protobuf_bundle_data_dir(Name) ->
    DataDir = protobuf_bundle_data_dir(Name),
    _ = file:del_dir_r(DataDir),
    ok.

copy_protobuf_bundle_to_data_dir(Name, RootFile, OtherFiles) ->
    DataDir = protobuf_bundle_data_dir(Name),
    ok = filelib:ensure_path(DataDir),
    %% Root file is validated earlier in the pipeline.
    #{<<"path">> := RootPath0} = RootFile,
    RootPath = filename:join([DataDir, RootPath0]),
    ok = filelib:ensure_dir(RootPath),
    SuccessRes = {ok, RootPath},
    emqx_utils:foldl_while(
        fun
            (#{<<"path">> := Path0, <<"contents">> := Contents} = File, Acc) ->
                case validate_protobuf_path(File) of
                    {error, Reason} ->
                        {halt, {error, Reason}};
                    ok ->
                        Path = filename:join([DataDir, Path0]),
                        ok = filelib:ensure_dir(Path),
                        ok = file:write_file(Path, Contents),
                        {cont, Acc}
                end;
            (BadFile, _Acc) ->
                {halt, {error, {bad_file, BadFile}}}
        end,
        SuccessRes,
        [RootFile | OtherFiles]
    ).
