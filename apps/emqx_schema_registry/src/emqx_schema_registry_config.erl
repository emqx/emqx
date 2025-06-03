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

%% Internal exports for application
-export([protobuf_bundle_data_dir/1]).

%% `emqx_config_handler' API
-export([pre_config_update/3, post_config_update/5]).

%% `emqx_config_backup' API
-behaviour(emqx_config_backup).
-export([import_config/1]).
-export([prepare_protobuf_files_for_export/1]).

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
    BundleDir = protobuf_bundle_data_dir(Name),
    TmpDir = mk_temp_dir(BundleDir),
    try
        maybe
            {ok, RawConf, SideEffect} ?=
                handle_one_schema_pre_config_update(Name, RawConf0, TmpDir),
            SideEffect(),
            {ok, RawConf}
        end
    after
        file:del_dir_r(TmpDir)
    end;
pre_config_update([?CONF_KEY_ROOT], NewRawConf, _OldConf) ->
    handle_import_root_pre_config_update(NewRawConf);
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
    delete_protobuf_bundle_data_dir(Name),
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

prepare_protobuf_files_for_export(
    #{?CONF_KEY_ROOT_BIN := #{<<"schemas">> := Schemas0} = SR0} = RawConf0
) ->
    Schemas = prepare_protobuf_files_for_export1(Schemas0),
    RawConf0#{<<"schema_registry">> := SR0#{<<"schemas">> := Schemas}};
prepare_protobuf_files_for_export(RawConf) ->
    RawConf.

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
    } = RawConf0,
    TmpDir
) ->
    do_convert_protobuf_bundle(Name, RawConf0, TmpDir);
convert_protobuf_bundle(_Name, RawConf, _TmpDir) ->
    SideEffect = fun() -> ok end,
    {ok, RawConf, _TmpRootPath = undefined, SideEffect}.

do_convert_protobuf_bundle(Name, RawConf0, TmpDir) ->
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
        do_convert_protobuf_bundle1(Name, RawConf0, RootFile, OtherFiles, TmpDir)
    end.

do_convert_protobuf_bundle1(Name, RawConf0, RootFile, OtherFiles, TmpDir) ->
    maybe
        {ok, RootPath, TmpRootPath, SideEffect} ?=
            copy_protobuf_bundle_to_data_dir(Name, RootFile, OtherFiles, TmpDir),
        ok ?= validate_protobuf_imports({path, TmpRootPath}),
        NewSource = #{
            <<"type">> => <<"bundle">>,
            <<"root_proto_path">> => RootPath
        },
        RawConf = RawConf0#{<<"source">> := NewSource},
        {ok, RawConf, TmpRootPath, SideEffect}
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
            <<"root_proto_path">> := RootPath0
        }
    } = RawConfig,
    TmpRootPath
) ->
    RootPath =
        case TmpRootPath of
            undefined ->
                %% We didn't just convert input files.
                RootPath0;
            _ ->
                %% Just converted files that are in temporary path awaiting to be
                %% moved to final destination.
                TmpRootPath
        end,
    maybe
        ok ?= validate_protobuf_imports({path, RootPath}),
        {ok, RawConfig}
    end;
validate_protobuf_imports_already_converted(
    #{
        <<"type">> := <<"protobuf">>,
        <<"source">> := Source
    } = RawConfig,
    _TmpRootPath
) when is_binary(Source) ->
    maybe
        ok ?= validate_protobuf_imports({raw, Source}),
        {ok, RawConfig}
    end;
validate_protobuf_imports_already_converted(RawConfig, _TmpRootPath) ->
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

copy_protobuf_bundle_to_data_dir(Name, RootFile, OtherFiles, TmpDir) ->
    DataDir = protobuf_bundle_data_dir(Name),
    ok = filelib:ensure_path(DataDir),
    %% Root file is validated earlier in the pipeline.
    #{<<"path">> := RootPath0} = RootFile,
    RootPath = filename:join([DataDir, RootPath0]),
    TmpRootPath = filename:join([TmpDir, RootPath0]),
    ok = filelib:ensure_dir(RootPath),
    SideEffect = fun() -> replace_directory(TmpDir, DataDir) end,
    SuccessRes = {ok, RootPath, TmpRootPath, SideEffect},
    emqx_utils:foldl_while(
        fun
            (#{<<"path">> := Path0, <<"contents">> := Contents} = File, Acc) ->
                case validate_protobuf_path(File) of
                    {error, Reason} ->
                        {halt, {error, Reason}};
                    ok ->
                        Path = filename:join([TmpDir, Path0]),
                        %% Note: `Path0` may contain nested directories.
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

-doc """
Creates a new temporary directory with a random name given a desired final directory.
""".
mk_temp_dir(BaseDir) ->
    RandomSuffix = emqx_utils:rand_id(10),
    Name = filename:basename(BaseDir),
    DirName = filename:dirname(BaseDir),
    TmpName0 = iolist_to_binary([Name, "-", RandomSuffix]),
    TmpName = filename:join([DirName, TmpName0]),
    ok = filelib:ensure_path(TmpName),
    TmpName.

%% If the destination directory exists, rename it to something random, move the temporary
%% dir to its place, delete original dir.  Otherwise, just move temporary dir to final
%% destination.
replace_directory(NewTmpDir, DestinationDir) ->
    case filelib:is_dir(DestinationDir) of
        true ->
            DestinationDirSegs0 = filename:split(DestinationDir),
            [DestinationDirBaseName0 | RevSegs] = lists:reverse(DestinationDirSegs0),
            RandId = emqx_utils:rand_id(10),
            DestinationDirBaseName = iolist_to_binary([DestinationDirBaseName0, "-", RandId]),
            DestinationDirSegs = lists:reverse([DestinationDirBaseName | RevSegs]),
            DestinationDirTmp = filename:join(DestinationDirSegs),
            ok = file:rename(DestinationDir, DestinationDirTmp),
            ok = file:rename(NewTmpDir, DestinationDir),
            ok = file:del_dir_r(DestinationDirTmp);
        false ->
            ok = file:rename(NewTmpDir, DestinationDir)
    end.

prepare_protobuf_files_for_export1(Schemas0) ->
    maps:map(
        fun
            (Name, #{<<"type">> := <<"protobuf">>, <<"source">> := Source0} = Sc0) ->
                Source = prepare_protobuf_files_for_export2(Source0, Name),
                Sc0#{<<"source">> := Source};
            (_Name, Schema) ->
                Schema
        end,
        Schemas0
    ).

str(X) -> emqx_utils_conv:str(X).

strip_leading_dir(Path0, Dir0) ->
    Path = str(Path0),
    Dir = str(Dir0),
    lists:flatten(string:replace(Path, Dir ++ "/", "", leading)).

prepare_protobuf_files_for_export2(#{<<"type">> := <<"bundle">>} = Source0, Name) ->
    #{<<"root_proto_path">> := RootPath} = Source0,
    #{valid := Valid} = emqx_schema_registry_serde:protobuf_resolve_imports({path, RootPath}),
    RootName = str(filename:basename(RootPath)),
    %% todo: warn about missing/invalid files?
    SchemaDir = protobuf_bundle_data_dir(Name),
    Files =
        lists:map(
            fun(Path0) ->
                {ok, Contents} = file:read_file(Path0),
                Path = strip_leading_dir(Path0, SchemaDir),
                #{
                    <<"path">> => Path,
                    <<"root">> => Path == RootName,
                    <<"contents">> => Contents
                }
            end,
            Valid
        ),
    #{
        <<"type">> => <<"bundle">>,
        <<"files">> => Files
    };
prepare_protobuf_files_for_export2(Source, _Name) ->
    Source.

handle_one_schema_pre_config_update(Name, RawConf0, TmpDir) ->
    maybe
        ok ?= validate_name(Name),
        {ok, RawConf1} ?= convert_certs(Name, RawConf0),
        {ok, RawConf2, TmpRootPath, SideEffect} ?= convert_protobuf_bundle(Name, RawConf1, TmpDir),
        {ok, RawConf3} ?= validate_protobuf_imports_already_converted(RawConf2, TmpRootPath),
        {ok, RawConf3, SideEffect}
    end.

handle_import_root_pre_config_update(#{<<"schemas">> := Schemas0} = RawConf0) ->
    Res =
        emqx_utils:foldl_while(
            fun({Name, Sc0}, {ok, ScAcc0, SideEffectAcc0, CleanupAcc0}) ->
                BundleDir = protobuf_bundle_data_dir(Name),
                TmpDir = mk_temp_dir(BundleDir),
                case handle_one_schema_pre_config_update(Name, Sc0, TmpDir) of
                    {ok, Sc, SideEffect} ->
                        ScAcc = ScAcc0#{Name => Sc},
                        SideEffectAcc = fun() ->
                            SideEffectAcc0(),
                            SideEffect()
                        end,
                        CleanupAcc = fun() ->
                            CleanupAcc0(),
                            file:del_dir_r(TmpDir)
                        end,
                        {cont, {ok, ScAcc, SideEffectAcc, CleanupAcc}};
                    {error, Reason} ->
                        CleanupAcc = fun() ->
                            CleanupAcc0(),
                            file:del_dir_r(TmpDir)
                        end,
                        {halt, {error, Reason, CleanupAcc}}
                end
            end,
            {ok, #{}, fun() -> ok end, fun() -> ok end},
            maps:to_list(Schemas0)
        ),
    case Res of
        {ok, Schemas, SideEffect, Cleanup} ->
            RawConf = RawConf0#{<<"schemas">> := Schemas},
            SideEffect(),
            Cleanup(),
            {ok, RawConf};
        {error, Reason, Cleanup} ->
            Cleanup(),
            {error, Reason}
    end;
handle_import_root_pre_config_update(RawConf) ->
    {ok, RawConf}.
