%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_data_backup).

-feature(maybe_expr, enable).

-export([
    export/0,
    all_table_set_names/0,
    compile_mnesia_table_filter/1,
    export/1,
    import/1,
    import/2,
    import_local/1,
    import_local/2,
    format_error/1
]).

%% HTTP API
-export([
    upload/2,
    %% RPC target (v1)
    maybe_copy_and_import/2,
    %% RPC target (v2)
    maybe_copy_and_import/3,
    read_file/1,
    delete_file/1,
    list_files/0,
    format_conf_errors/1,
    format_db_errors/1
]).

-export([default_validate_mnesia_backup/1]).

-export([
    validate_export_root_keys/1,
    parse_export_request/1
]).

-export_type([import_res/0]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-elvis([
    {elvis_style, invalid_dynamic_call, disable},
    {elvis_style, no_catch_expressions, disable}
]).

-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(ROOT_BACKUP_DIR, "backup").
-define(BACKUP_MNESIA_DIR, "mnesia").
-define(TAR_SUFFIX, ".tar.gz").
-define(META_FILENAME, "META.hocon").
-define(CLUSTER_HOCON_FILENAME, "cluster.hocon").
-define(CONF_KEYS, [
    [<<"delayed">>],
    [<<"rewrite">>],
    [<<"retainer">>],
    [<<"mqtt">>],
    [<<"alarm">>],
    [<<"sysmon">>],
    [<<"sys_topics">>],
    [<<"limiter">>],
    [<<"log">>],
    [<<"persistent_session_store">>],
    [<<"durable_sessions">>],
    [<<"prometheus">>],
    [<<"crl_cache">>],
    [<<"conn_congestion">>],
    [<<"force_shutdown">>],
    [<<"flapping_detect">>],
    [<<"broker">>],
    [<<"force_gc">>],
    [<<"zones">>],
    [<<"slow_subs">>],
    [<<"cluster">>, <<"links">>]
]).

-define(DEFAULT_OPTS, #{}).

-define(HOCON_NS_INTERNAL_TAR_PATH(BASE_NAME, NAMESPACE), [
    BASE_NAME, "ns", NAMESPACE, ?CLUSTER_HOCON_FILENAME
]).
-define(HOCON_NS_INTERNAL_TAR_PATH_REV_PAT(NAMESPACE), [
    ?CLUSTER_HOCON_FILENAME, NAMESPACE, "ns" | _
]).

-type backup_file_info() :: #{
    filename := binary(),
    size := non_neg_integer(),
    created_at := binary(),
    created_at_sec := integer(),
    node := node(),
    atom() => _
}.

-type db_error_details() :: #{mria:table() => {error, _}}.
-type config_error_details() :: #{emqx_utils_maps:config_key_path() => {error, _}}.
-type import_res() ::
    {ok, #{db_errors => db_error_details(), config_errors => config_error_details()}} | {error, _}.

-type export_opts() :: #{
    mnesia_table_filter => mnesia_table_filter(),
    print_fun => fun((io:format(), [term()]) -> ok),
    raw_conf_transform => fun((raw_config()) -> raw_config()),
    namespace => emqx_config:maybe_namespace()
}.
-type raw_config() :: #{binary() => any()}.
-type mnesia_table_filter() :: fun((atom()) -> boolean()).
-type maybe_namespace() :: ?global_ns | binary().

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec export() -> {ok, backup_file_info()} | {error, _}.
export() ->
    export(?DEFAULT_OPTS).

-spec compile_mnesia_table_filter([binary()]) -> {ok, mnesia_table_filter()} | {error, any()}.
compile_mnesia_table_filter(TableSets) ->
    Mapping = table_set_to_tables_mapping(),
    {TableNames, Errors} =
        lists:foldl(
            fun(TableSetName, {TableAcc, ErrorAcc}) ->
                case maps:find(TableSetName, Mapping) of
                    {ok, Tables} ->
                        {lists:usort(Tables ++ TableAcc), ErrorAcc};
                    error ->
                        {TableAcc, [TableSetName | ErrorAcc]}
                end
            end,
            {[], []},
            TableSets
        ),
    case Errors of
        [] ->
            Filter = fun(Table) -> lists:member(Table, TableNames) end,
            {ok, Filter};
        _ ->
            {error, Errors}
    end.

-spec export(export_opts()) -> {ok, backup_file_info()} | {error, _}.
export(Opts) ->
    {BackupName, TarDescriptor} = prepare_new_backup(Opts),
    try
        do_export(BackupName, TarDescriptor, Opts)
    catch
        Class:Reason:Stack ->
            ?SLOG(error, #{
                msg => "emqx_data_export_failed",
                exception => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, Reason}
    after
        %% erl_tar:close/1 raises error if called on an already closed tar
        catch erl_tar:close(TarDescriptor),
        file:del_dir_r(BackupName)
    end.

-spec all_table_set_names() -> [binary()].
all_table_set_names() ->
    Key = {?MODULE, all_table_set_names},
    case persistent_term:get(Key, undefined) of
        undefined ->
            Names = build_all_table_set_names(),
            persistent_term:put(Key, Names),
            Names;
        Names ->
            Names
    end.

build_all_table_set_names() ->
    Mods = modules_with_mnesia_tabs_to_backup(),
    lists:usort(
        lists:map(
            fun(Mod) ->
                {TableSetName, _Tabs} = emqx_db_backup:backup_tables(Mod),
                TableSetName
            end,
            Mods
        )
    ).

validate_export_root_keys(Params) ->
    RootKeys0 = maps:get(<<"root_keys">>, Params, []),
    RootKeys = lists:usort(RootKeys0),
    RootNames = emqx_config:get_root_names(),
    UnknownKeys = RootKeys -- RootNames,
    case UnknownKeys of
        [] ->
            ok;
        [_ | _] ->
            {error, {unknown_root_keys, UnknownKeys}}
    end.

parse_export_request(Params) ->
    Namespace = maps:get(<<"namespace">>, Params, ?global_ns),
    OutDir = maps:get(<<"out_dir">>, Params, undefined),
    Opts0 = emqx_utils_maps:put_if(#{}, out_dir, OutDir, OutDir /= undefined),
    Opts1 =
        maybe
            {ok, Keys0} ?= maps:find(<<"root_keys">>, Params),
            Keys = lists:usort(Keys0),
            Transform = fun(RawConf) ->
                maps:with(Keys, RawConf)
            end,
            Opts0#{raw_conf_transform => Transform}
        else
            error -> Opts0
        end,
    CheckNamespaceValid =
        case Namespace of
            ?global_ns -> ok;
            _ when is_binary(Namespace) -> ok;
            _ -> {error, {invalid_namespace, Namespace}}
        end,
    Opts2 = Opts1#{namespace => Namespace},
    maybe
        ok ?= CheckNamespaceValid,
        {ok, TableSetNames0} ?= maps:find(<<"table_sets">>, Params),
        TableSetNames = lists:usort(TableSetNames0),
        {ok, Filter} ?= emqx_mgmt_data_backup:compile_mnesia_table_filter(TableSetNames),
        {ok, Opts2#{mnesia_table_filter => Filter}}
    else
        error ->
            {ok, Opts2};
        {error, {invalid_namespace, _}} = Error ->
            Error;
        {error, InvalidSetNames0} ->
            InvalidSetNames = lists:sort(InvalidSetNames0),
            {error, {bad_table_sets, InvalidSetNames}}
    end.

-doc """
This function is called from CLI.
Tries to interpret the backup file path as a full local backup file path,
and on fail tries to install from backup data directory.
""".
-spec import_local(file:filename_all()) -> import_res().
import_local(BackupFilePath) ->
    import_local(BackupFilePath, ?DEFAULT_OPTS).

-spec import_local(file:filename_all(), map()) -> import_res().
import_local(BackupFilePath, Opts) ->
    case import_from_path(BackupFilePath, Opts) of
        {error, not_found} ->
            case import(BackupFilePath, Opts) of
                {error, bad_backup_name} ->
                    {error, not_found};
                Res ->
                    Res
            end;
        Res ->
            Res
    end.

-doc """
This function is called from CLI.
Backup file is manually placed somewhere in the local file system.
""".
-spec import_from_path(file:filename_all(), map()) -> import_res().
import_from_path(BackupFilePath, Opts) ->
    maybe
        ok ?= validate_import_allowed(),
        ok ?= validate_file_exists(BackupFilePath),
        do_import(str(BackupFilePath), Opts)
    end.

-doc """
This function is called from API.
It receives file name of the backup file in the backup data folder.
Backup file should be previously created by `upload/2` function.
""".
-spec import(file:filename_all()) -> import_res().
import(BackupFilename) ->
    import(BackupFilename, ?DEFAULT_OPTS).

-spec import(file:filename_all(), map()) -> import_res().
import(BackupFilename, Opts) ->
    maybe
        ok ?= validate_import_allowed(),
        {ok, BackupFilePath} ?= backup_path(BackupFilename),
        ok ?= validate_file_exists(BackupFilePath),
        do_import(BackupFilePath, Opts)
    end.

-doc """
This function is an RPC target (v1) and is called from the API handler.
It runs on a core node.

`FileNode` is the node to take backup file from.
""".
-spec maybe_copy_and_import(node(), file:filename_all()) -> import_res().
maybe_copy_and_import(FileNode, BackupFilename) ->
    maybe_copy_and_import(FileNode, BackupFilename, _Opts = #{}).

-doc """
This function is an RPC target (v2) and is called from the API handler.
It runs on a core node.

`FileNode` is the node to take backup file from.
""".
-spec maybe_copy_and_import(node(), file:filename_all(), map()) -> import_res().
maybe_copy_and_import(FileNode, BackupFilename, Opts) ->
    maybe
        ok ?= maybe_copy(FileNode, BackupFilename),
        import(BackupFilename, Opts)
    end.

-spec read_file(file:filename_all()) ->
    {ok, #{filename => file:filename_all(), file => binary()}} | {error, _}.
read_file(BackupFilename) ->
    maybe
        {ok, FilePath} ?= backup_path(BackupFilename),
        maybe_not_found(file:read_file(FilePath))
    end.

-spec delete_file(file:filename_all()) -> ok | {error, _}.
delete_file(BackupFilename) ->
    maybe
        {ok, FilePath} ?= backup_path(BackupFilename),
        maybe_not_found(file:delete(FilePath))
    end.

-spec upload(file:filename_all(), binary()) -> ok | {error, _}.
upload(BackupFilename, BackupFileContent) ->
    maybe
        {ok, FilePath} ?= backup_path(BackupFilename),
        case filelib:is_file(FilePath) of
            true -> {error, {already_exists, BackupFilename}};
            false -> do_upload(FilePath, BackupFileContent)
        end
    end.

-spec list_files() -> [backup_file_info()].
list_files() ->
    Filter =
        fun(File) ->
            case file:read_file_info(File, [{time, posix}]) of
                {ok, #file_info{size = Size, ctime = CTimeSec}} ->
                    BaseFilename = bin(filename:basename(File)),
                    Info = #{
                        filename => BaseFilename,
                        size => Size,
                        created_at => emqx_utils_calendar:epoch_to_rfc3339(CTimeSec, second),
                        created_at_sec => CTimeSec,
                        node => node()
                    },
                    {true, Info};
                _ ->
                    false
            end
        end,
    lists:filtermap(Filter, backup_files()).

-spec backup_files() -> [file:filename()].
backup_files() ->
    {ok, Pattern} = backup_path("*" ++ ?TAR_SUFFIX),
    filelib:wildcard(Pattern).

-spec format_error(atom()) -> string() | term().
format_error(not_core_node) ->
    str(
        io_lib:format(
            "backup data import is only allowed on core EMQX nodes, but requested node ~p is not core",
            [node()]
        )
    );
format_error(ee_to_ce_backup) ->
    "importing EMQX Enterprise data backup to EMQX is not allowed";
format_error(missing_backup_meta) ->
    "invalid backup archive file: missing " ?META_FILENAME;
format_error(invalid_edition) ->
    "invalid backup archive content: wrong EMQX edition value in " ?META_FILENAME;
format_error(invalid_version) ->
    "invalid backup archive content: wrong EMQX version value in " ?META_FILENAME;
format_error(bad_archive_dir) ->
    "invalid backup archive content: all files in the archive must be under <backup name> directory";
format_error(not_found) ->
    "backup file not found";
format_error(bad_backup_name) ->
    "invalid backup name: file name must have " ?TAR_SUFFIX " extension";
format_error(unsafe_backup_name) ->
    "unsafe backup name";
format_error({unsupported_version, ImportVersion}) ->
    str(
        io_lib:format(
            "[warning] Backup version ~p is incompatible with EMQX version ~p, import is not allowed.~n",
            [str(ImportVersion), str(emqx_release:version())]
        )
    );
format_error({already_exists, BackupFilename}) ->
    str(io_lib:format("Backup file \"~s\" already exists", [BackupFilename]));
format_error(Reason) ->
    Reason.

-spec format_conf_errors(#{maybe_namespace() => map()}) -> #{maybe_namespace() => iodata()}.
format_conf_errors(ErrorsPerNamespace) ->
    Opts = #{print_fun => fun io_lib:format/2},
    maps:map(
        fun(_Namespace, Errors) ->
            Formatted = maps:map(conf_error_formatter(Opts), Errors),
            maps:values(Formatted)
        end,
        ErrorsPerNamespace
    ).

-spec format_db_errors(map()) -> list().
format_db_errors(Errors) ->
    Opts = #{print_fun => fun io_lib:format/2},
    maps:values(
        maps:map(
            fun(Tab, Err) -> maybe_print_mnesia_import_err(Tab, Err, Opts) end,
            Errors
        )
    ).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

maybe_copy(FileNode, _BackupFilename) when FileNode =:= node() ->
    ok;
maybe_copy(FileNode, BackupFilename) ->
    maybe
        {ok, FilePath} ?= backup_path(BackupFilename),
        %% The file can be already present locally.
        case filelib:is_regular(FilePath) of
            true ->
                ok;
            false ->
                copy(FileNode, BackupFilename)
        end
    end.

copy(FileNode, BackupFilename) ->
    maybe
        {ok, BackupFileContent} ?=
            emqx_mgmt_data_backup_proto_v1:read_file(FileNode, BackupFilename, infinity),
        ok ?= upload(BackupFilename, BackupFileContent)
    end.

maybe_not_found({error, enoent}) ->
    {error, not_found};
maybe_not_found(Other) ->
    Other.

do_upload(BackupFilePath, BackupFileContent) ->
    try
        maybe
            {ok, BackupDir} ?= backup_dir(BackupFilePath),
            ok ?= file:write_file(BackupFilePath, BackupFileContent),
            ok ?= extract_backup(BackupFilePath),
            {ok, _} ?= validate_backup(BackupDir),
            HoconFilename = filename:join(BackupDir, ?CLUSTER_HOCON_FILENAME),
            ok ?=
                case filelib:is_regular(HoconFilename) of
                    true ->
                        maybe
                            {ok, RawConf} ?= hocon:files([HoconFilename]),
                            RawConf1 = upgrade_raw_conf(emqx_conf:schema_module(), RawConf),
                            {ok, _} ?= validate_cluster_hocon(RawConf1),
                            ok
                        end;
                    false ->
                        %% cluster.hocon can be missing in the backup
                        ok
                end,
            ?SLOG(info, #{msg => "emqx_data_upload_success"})
        else
            {error, Error} ->
                ?SLOG(error, #{
                    msg => "emqx_data_upload_failed",
                    reason => Error,
                    backup_file_path => BackupFilePath
                }),
                _ = file:delete(BackupFilePath),
                {error, Error}
        end
    catch
        Class:Reason:Stack ->
            _ = file:delete(BackupFilePath),
            ?SLOG(error, #{
                msg => "emqx_data_upload_failed",
                backup_file_path => BackupFilePath,
                exception => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, Reason}
    after
        cleanup_backup_dir(BackupFilePath)
    end.

prepare_new_backup(Opts) ->
    Ts = erlang:system_time(millisecond),
    {{Y, M, D}, {HH, MM, SS}} = local_datetime(Ts),
    BackupBaseName = str(
        io_lib:format(
            "emqx-export-~0p-~2..0b-~2..0b-~2..0b-~2..0b-~2..0b.~3..0b",
            [Y, M, D, HH, MM, SS, Ts rem 1000]
        )
    ),
    BackupDir = maps:get(out_dir, Opts, root_backup_dir()),
    BackupName = filename:join(BackupDir, BackupBaseName),
    BackupTarName = tar(BackupName),
    maybe_print("Exporting data to ~p...~n", [BackupTarName], Opts),
    {ok, TarDescriptor} = format_tar_error(erl_tar:open(BackupTarName, [write, compressed])),
    {BackupName, TarDescriptor}.

do_export(BackupName, TarDescriptor, Opts) ->
    BackupBaseName = filename:basename(BackupName),
    BackupTarName = tar(BackupName),
    Meta = #{
        version => emqx_release:version(),
        edition => emqx_release:edition()
    },
    MetaBin = bin(hocon_pp:do(Meta, #{})),
    MetaFilename = filename:join(BackupBaseName, ?META_FILENAME),
    Namespace = maps:get(namespace, Opts, ?global_ns),

    ok = format_tar_error(erl_tar:add(TarDescriptor, MetaBin, MetaFilename, [])),
    case Namespace of
        ?global_ns ->
            ok = export_cluster_hocon(TarDescriptor, BackupBaseName, Opts),
            ok = export_mnesia_tabs(TarDescriptor, BackupName, BackupBaseName, Opts);
        _ when is_binary(Namespace) ->
            ok = export_namespaced_cluster_hocon(TarDescriptor, BackupBaseName, Namespace, Opts)
    end,
    ok = format_tar_error(erl_tar:close(TarDescriptor)),
    {ok, #file_info{
        size = Size,
        ctime = CTime
    }} = file:read_file_info(BackupTarName, [{time, posix}]),
    {ok, #{
        filename => bin(BackupTarName),
        size => Size,
        created_at => emqx_utils_calendar:epoch_to_rfc3339(CTime, second),
        created_at_sec => CTime,
        node => node()
    }}.

export_cluster_hocon(TarDescriptor, BackupBaseName, Opts) ->
    maybe_print("Exporting cluster configuration...~n", [], Opts),
    RawConf1 = emqx_config:read_override_conf(#{override_to => cluster}),
    maybe_print(
        "Exporting additional files from EMQX data_dir: ~p...~n", [str(emqx:data_dir())], Opts
    ),
    RawConf2 = read_data_files(RawConf1),
    TransformFn = maps:get(raw_conf_transform, Opts, fun(Raw) -> Raw end),
    RawConf = TransformFn(RawConf2),
    RawConfBin = bin(hocon_pp:do(RawConf, #{})),
    NameInArchive = filename:join(BackupBaseName, ?CLUSTER_HOCON_FILENAME),
    ok = format_tar_error(erl_tar:add(TarDescriptor, RawConfBin, NameInArchive, [])).

export_namespaced_cluster_hocon(TarDescriptor, BackupBaseName, Namespace, Opts) ->
    maybe_print("Exporting cluster configuration for namespace ~s...~n", [Namespace], Opts),
    TransformFn = maps:get(raw_conf_transform, Opts, fun(Raw) -> Raw end),
    RootConfig0 = emqx_config:get_all_roots_from_namespace(Namespace),
    RootConfig = TransformFn(RootConfig0),
    RawConfBin = bin(hocon_pp:do(RootConfig, #{})),
    NameInArchive = str(
        filename:join(?HOCON_NS_INTERNAL_TAR_PATH(BackupBaseName, Namespace))
    ),
    ok = format_tar_error(erl_tar:add(TarDescriptor, RawConfBin, NameInArchive, [])),
    ok.

export_mnesia_tabs(TarDescriptor, BackupName, BackupBaseName, Opts) ->
    maybe_print("Exporting built-in database...~n", [], Opts),
    FilterFn = maps:get(mnesia_table_filter, Opts, fun(_TableName) -> true end),
    lists:foreach(
        fun(Mod) ->
            {_Name, Tabs} = emqx_db_backup:backup_tables(Mod),
            lists:foreach(
                fun(Tab) ->
                    export_mnesia_tab(TarDescriptor, Tab, BackupName, BackupBaseName, Opts)
                end,
                lists:filter(FilterFn, Tabs)
            )
        end,
        tabs_to_backup()
    ).

export_mnesia_tab(TarDescriptor, TabName, BackupName, BackupBaseName, Opts) ->
    maybe_print("Exporting ~p database table...~n", [TabName], Opts),
    {ok, MnesiaBackupName} = do_export_mnesia_tab(TabName, BackupName),
    NameInArchive = mnesia_backup_name(BackupBaseName, TabName),
    ok = format_tar_error(erl_tar:add(TarDescriptor, MnesiaBackupName, NameInArchive, [])),
    _ = file:delete(MnesiaBackupName),
    ok.

do_export_mnesia_tab(TabName, BackupName) ->
    Node = node(),
    try
        Opts0 = [{name, TabName}, {min, [TabName]}, {allow_remote, false}],
        Opts =
            case mnesia:table_info(TabName, storage_type) of
                ram_copies -> [{ram_overrides_dump, true} | Opts0];
                _ -> Opts0
            end,
        {ok, TabName, [Node]} = mnesia:activate_checkpoint(Opts),
        MnesiaBackupName = mnesia_backup_name(BackupName, TabName),
        ok = filelib:ensure_dir(MnesiaBackupName),
        ok = mnesia:backup_checkpoint(TabName, MnesiaBackupName),
        {ok, MnesiaBackupName}
    after
        mnesia:deactivate_checkpoint(TabName)
    end.

-ifdef(TEST).
tabs_to_backup() ->
    %% Allow mocking in tests
    ?MODULE:modules_with_mnesia_tabs_to_backup().
-else.
tabs_to_backup() ->
    modules_with_mnesia_tabs_to_backup().
-endif.

modules_with_mnesia_tabs_to_backup() ->
    lists:flatten([M || M <- find_behaviours(emqx_db_backup)]).

mnesia_backup_name(Path, TabName) ->
    filename:join([Path, ?BACKUP_MNESIA_DIR, atom_to_list(TabName)]).

validate_import_allowed() ->
    case mria_rlog:role() of
        core -> ok;
        _ -> {error, not_core_node}
    end.

validate_file_exists(FilePath) ->
    case filelib:is_regular(FilePath) of
        true -> ok;
        false -> {error, not_found}
    end.

validate_backup(BackupDir) ->
    case hocon:files([filename:join(BackupDir, ?META_FILENAME)]) of
        {ok, #{
            <<"edition">> := Edition,
            <<"version">> := Version
        }} = Meta ->
            validate(
                [
                    fun() -> check_edition(Edition) end,
                    fun() -> check_version(Version) end
                ],
                Meta
            );
        _ ->
            ?SLOG(error, #{msg => "missing_backup_meta", backup => BackupDir}),
            {error, missing_backup_meta}
    end.

validate([ValidatorFun | T], OkRes) ->
    case ValidatorFun() of
        ok -> validate(T, OkRes);
        Err -> Err
    end;
validate([], OkRes) ->
    OkRes.

check_edition(BackupEdition) when BackupEdition =:= <<"ce">>; BackupEdition =:= <<"ee">> ->
    Edition = bin(emqx_release:edition()),
    case {BackupEdition, Edition} of
        {<<"ee">>, <<"ce">>} ->
            {error, ee_to_ce_backup};
        _ ->
            ok
    end;
check_edition(BackupEdition) ->
    ?SLOG(error, #{msg => "invalid_backup_edition", edition => BackupEdition}),
    {error, invalid_edition}.

check_version(ImportVersion) ->
    case parse_version_no_patch(ImportVersion) of
        {ok, {ImportMajorInt, ImportMinorInt}} ->
            Version = emqx_release:version(),
            {ok, {MajorInt, MinorInt}} = parse_version_no_patch(bin(Version)),
            case
                ImportMajorInt =< 4 orelse ImportMajorInt > MajorInt orelse
                    (ImportMajorInt =:= MajorInt andalso ImportMinorInt > MinorInt)
            of
                true ->
                    %% - do not allow import of backups from version 4 and older
                    %% - do not allow import of backups from newer versions to older versions
                    {error, {unsupported_version, str(ImportVersion)}};
                false ->
                    ok
            end;
        Err ->
            Err
    end.

parse_version_no_patch(VersionBin) ->
    case string:split(VersionBin, ".", all) of
        [Major, Minor | _] ->
            {MajorInt, _} = emqx_utils_binary:bin_to_int(Major),
            {MinorInt, _} = emqx_utils_binary:bin_to_int(Minor),
            {ok, {MajorInt, MinorInt}};
        _ ->
            ?SLOG(error, #{msg => "failed_to_parse_backup_version", version => VersionBin}),
            {error, invalid_version}
    end.

do_import(BackupFilePath, Opts) ->
    maybe_print("Importing data from ~p...~n", [BackupFilePath], Opts),
    Namespace = maps:get(namespace, Opts, ?global_ns),
    try
        maybe
            {ok, BackupDir} ?= backup_dir(BackupFilePath),
            ok ?= validate_backup_basename(BackupFilePath),
            ok ?= extract_backup(BackupFilePath),
            {ok, _} ?= validate_backup(BackupDir),
            {ok, #{conf_errors := ConfErrors, mnesia_errors := MnesiaErrors}} ?=
                do_import_for_namespace(Namespace, BackupDir, Opts),
            ?SLOG(info, #{msg => "emqx_data_import_success"}),
            {ok, #{db_errors => MnesiaErrors, config_errors => ConfErrors}}
        else
            {error, Error} ->
                ?SLOG(error, #{
                    msg => "emqx_data_import_failed",
                    reason => Error,
                    backup_file_path => BackupFilePath
                }),
                {error, Error}
        end
    catch
        Class:Reason:Stack ->
            ?SLOG(error, #{
                msg => "emqx_data_import_failed",
                backup_file_path => BackupFilePath,
                exception => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, Reason}
    after
        cleanup_backup_dir(BackupFilePath)
    end.

do_import_for_namespace(?global_ns, BackupDir, Opts) ->
    maybe
        {ok, GlobalConfErrors} ?= import_cluster_hocon(BackupDir, Opts),
        MnesiaErrors = import_mnesia_tabs(BackupDir, Opts),
        ConfErrors =
            case map_size(GlobalConfErrors) of
                0 -> #{};
                _ -> #{?global_ns => GlobalConfErrors}
            end,
        {ok, #{
            mnesia_errors => MnesiaErrors,
            conf_errors => ConfErrors
        }}
    end;
do_import_for_namespace(Namespace, BackupDir, Opts) when is_binary(Namespace) ->
    maybe
        {ok, NSConfErrors} ?= import_namespaced_cluster_hocon(Namespace, BackupDir, Opts),
        {ok, #{conf_errors => NSConfErrors, mnesia_errors => #{}}}
    end.

import_mnesia_tabs(BackupDir, Opts) ->
    maybe_print("Importing built-in database...~n", [], Opts),
    filter_errors(
        lists:foldr(
            fun(Mod, Acc) ->
                {_Name, Tabs} = emqx_db_backup:backup_tables(Mod),
                lists:foldr(
                    fun(Tab, InAcc) ->
                        InAcc#{Tab => import_mnesia_tab(BackupDir, Mod, Tab, Opts)}
                    end,
                    Acc,
                    Tabs
                )
            end,
            #{},
            tabs_to_backup()
        )
    ).

-spec import_mnesia_tab(file:filename_all(), module(), mria:table(), map()) ->
    ok | {ok, no_backup_file} | {error, term()} | no_return().
import_mnesia_tab(BackupDir, Mod, TabName, Opts) ->
    MnesiaBackupFilename = mnesia_backup_name(BackupDir, TabName),
    case filelib:is_regular(MnesiaBackupFilename) of
        true ->
            maybe_print("Importing ~p database table...~n", [TabName], Opts),
            restore_mnesia_tab(BackupDir, MnesiaBackupFilename, Mod, TabName, Opts);
        false ->
            maybe_print("No backup file for ~p database table...~n", [TabName], Opts),
            ?SLOG(info, #{msg => "missing_mnesia_backup", table => TabName, backup => BackupDir}),
            ok
    end.

restore_mnesia_tab(BackupDir, MnesiaBackupFilename, Mod, TabName, Opts) ->
    Validated = validate_mnesia_backup(MnesiaBackupFilename, Mod),
    try
        case Validated of
            {ok, #{backup_file := BackupFile}} ->
                %% As we use keep_tables option, we don't need to modify 'copies' (nodes)
                %% in a backup file before restoring it,  as `mnsia:restore/2` will ignore
                %% backed-up schema and keep the current table schema unchanged
                Restored = mnesia:restore(BackupFile, [{default_op, keep_tables}]),
                case Restored of
                    {atomic, [TabName]} ->
                        on_table_imported(Mod, TabName, Opts);
                    RestoreErr ->
                        ?SLOG(error, #{
                            msg => "failed_to_restore_mnesia_backup",
                            table => TabName,
                            backup => BackupDir,
                            reason => RestoreErr
                        }),
                        maybe_print_mnesia_import_err(TabName, RestoreErr, Opts),
                        {error, RestoreErr}
                end;
            PrepareErr ->
                ?SLOG(error, #{
                    msg => "failed_to_prepare_mnesia_backup_for_restoring",
                    table => TabName,
                    backup => BackupDir,
                    reason => PrepareErr
                }),
                maybe_print_mnesia_import_err(TabName, PrepareErr, Opts),
                PrepareErr
        end
    after
        %% Cleanup files as soon as they are not needed any more for more efficient disk usage
        _ = file:delete(MnesiaBackupFilename)
    end.

on_table_imported(Mod, Tab, Opts) ->
    case erlang:function_exported(Mod, on_backup_table_imported, 2) of
        true ->
            try
                Mod:on_backup_table_imported(Tab, Opts)
            catch
                Class:Reason:Stack ->
                    ?SLOG(error, #{
                        msg => "post_database_import_callback_failed",
                        table => Tab,
                        module => Mod,
                        exception => Class,
                        reason => Reason,
                        stacktrace => Stack
                    }),
                    {error, Reason}
            end;
        false ->
            ok
    end.

%% NOTE: if backup file is valid, we keep traversing it, though we only need to validate schema.
%% Looks like there is no clean way to abort traversal without triggering any error reporting,
%% `mnesia_bup:read_schema/2` is an option but its direct usage should also be avoided...
validate_mnesia_backup(MnesiaBackupFilename, Mod) ->
    Init = #{backup_file => MnesiaBackupFilename},
    Validated =
        catch mnesia:traverse_backup(
            MnesiaBackupFilename,
            mnesia_backup,
            dummy,
            read_only,
            mnesia_backup_validator(Mod),
            Init
        ),
    case Validated of
        ok ->
            {ok, Init};
        {error, {_, over}} ->
            {ok, Init};
        {error, {_, migrate}} ->
            migrate_mnesia_backup(MnesiaBackupFilename, Mod, Init);
        Error ->
            Error
    end.

%% if the module has validator callback, use it else use the default
mnesia_backup_validator(Mod) ->
    Validator =
        case erlang:function_exported(Mod, validate_mnesia_backup, 1) of
            true ->
                fun Mod:validate_mnesia_backup/1;
            _ ->
                fun default_validate_mnesia_backup/1
        end,
    fun(Schema, Acc) ->
        case Validator(Schema) of
            ok ->
                {[Schema], Acc};
            {ok, Break} ->
                throw({error, Break});
            Error ->
                throw(Error)
        end
    end.

default_validate_mnesia_backup({schema, Tab, CreateList}) ->
    ImportAttributes = proplists:get_value(attributes, CreateList),
    Attributes = mnesia:table_info(Tab, attributes),
    case ImportAttributes == Attributes of
        true ->
            ok;
        false ->
            {error, different_table_schema}
    end;
default_validate_mnesia_backup(_Other) ->
    ok.

migrate_mnesia_backup(MnesiaBackupFilename, Mod, Acc) ->
    case erlang:function_exported(Mod, migrate_mnesia_backup, 1) of
        true ->
            MigrateFile = MnesiaBackupFilename ++ ".migrate",
            Migrator = fun(Schema, InAcc) ->
                case Mod:migrate_mnesia_backup(Schema) of
                    {ok, NewSchema} ->
                        {[NewSchema], InAcc};
                    Error ->
                        throw(Error)
                end
            end,
            catch mnesia:traverse_backup(
                MnesiaBackupFilename,
                MigrateFile,
                Migrator,
                Acc#{backup_file := MigrateFile}
            );
        _ ->
            {error, no_migrator}
    end.

extract_backup(BackupFilePath) ->
    ?SLOG(debug, #{msg => "extract_backup", backup_file_path => BackupFilePath}),
    RootBackupDir = root_backup_dir(),
    maybe
        ok ?= validate_filenames(BackupFilePath),
        ?SLOG(debug, #{
            msg => "extracting_backup", backup => BackupFilePath, root_backup_dir => RootBackupDir
        }),
        format_tar_error(erl_tar:extract(BackupFilePath, [{cwd, RootBackupDir}, compressed]))
    end.

validate_filenames(BackupFilePath) ->
    maybe
        {ok, Filenames} ?= format_tar_error(erl_tar:table(BackupFilePath, [compressed])),
        BackupName = filename:basename(BackupFilePath, ?TAR_SUFFIX),
        IsValid = lists:all(
            fun(Filename) ->
                [Root | _] = filename:split(Filename),
                Root =:= BackupName
            end,
            Filenames
        ),
        case IsValid of
            true -> ok;
            false -> {error, bad_archive_dir}
        end
    end.

import_cluster_hocon(BackupDir, Opts) ->
    HoconFilename = filename:join(BackupDir, ?CLUSTER_HOCON_FILENAME),
    do_import_cluster_hocon(?global_ns, BackupDir, HoconFilename, Opts).

import_namespaced_cluster_hocon(Namespace, BackupDir, Opts) when is_binary(Namespace) ->
    NSHoconPath = filename:join(?HOCON_NS_INTERNAL_TAR_PATH(BackupDir, Namespace)),
    case do_import_cluster_hocon(Namespace, BackupDir, NSHoconPath, Opts) of
        {ok, Res} when map_size(Res) == 0 ->
            {ok, #{}};
        {ok, Res} ->
            {ok, #{Namespace => Res}};
        {error, Reason} ->
            {error, #{Namespace => Reason}}
    end.

do_import_cluster_hocon(Namespace, BackupDir, Filename, Opts) ->
    case filelib:is_regular(Filename) of
        true ->
            maybe
                {ok, RawConf} ?= hocon:files([Filename]),
                RawConf1 = upgrade_raw_conf(emqx_conf:schema_module(), RawConf),
                {ok, _} ?= validate_cluster_hocon(RawConf1),
                maybe_print(
                    "Importing cluster configuration for namespace ~s...~n", [Namespace], Opts
                ),
                %% At this point, when all validations have been passed, we want to log errors (if any)
                %% but proceed with the next items, instead of aborting the whole import operation
                {ok, do_import_conf(Namespace, RawConf1, Opts)}
            end;
        false ->
            maybe_print("No cluster configuration to be imported.~n", [], Opts),
            ?SLOG(info, #{
                msg => "no_backup_hocon_config_to_import",
                namespace => Namespace,
                backup => BackupDir
            }),
            {ok, #{}}
    end.

upgrade_raw_conf(SchemaMod, RawConf) ->
    ok = emqx_utils:interactive_load(SchemaMod),
    case erlang:function_exported(SchemaMod, upgrade_raw_conf, 1) of
        true ->
            %% TODO make it a schema module behaviour in hocon_schema
            apply(SchemaMod, upgrade_raw_conf, [RawConf]);
        false ->
            RawConf
    end.

read_data_files(RawConf0) ->
    DataDir = bin(emqx:data_dir()),
    {ok, Cwd} = file:get_cwd(),
    AbsDataDir = bin(filename:join(Cwd, DataDir)),
    RawConf1 = emqx_authz:maybe_read_files(RawConf0),
    RawConf2 = emqx_schema_registry_config:prepare_protobuf_files_for_export(RawConf1),
    emqx_utils_maps:deep_convert(RawConf2, fun read_data_file/4, [DataDir, AbsDataDir]).

-define(dir_pattern(_Dir_), <<_Dir_:(byte_size(_Dir_))/binary, _/binary>>).

read_data_file(Key, Val, DataDir, AbsDataDir) ->
    Val1 =
        case Val of
            ?dir_pattern(DataDir) = Filename ->
                do_read_file(Filename);
            ?dir_pattern(AbsDataDir) = Filename ->
                do_read_file(Filename);
            V ->
                V
        end,
    {Key, Val1}.

do_read_file(Filename) ->
    case file:read_file(Filename) of
        {ok, Content} ->
            Content;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_read_data_file",
                filename => Filename,
                reason => Reason
            }),
            Filename
    end.

validate_cluster_hocon(RawConf) ->
    %% write ACL file to comply with the schema...
    RawConf1 = emqx_authz:maybe_write_files(RawConf),
    emqx_hocon:check(
        emqx_conf:schema_module(),
        maps:merge(emqx:get_raw_config([]), RawConf1),
        #{atom_key => false, required => false}
    ).

do_import_conf(Namespace, RawConf, Opts) ->
    GenConfErrs = filter_errors(maps:from_list(import_generic_conf(Namespace, RawConf))),
    maybe_print_conf_errors(GenConfErrs, Opts),
    Modules = emqx_config_dep_registry:sorted_importer_modules(),
    Errors = lists:foldl(
        print_ok_results_collect_errors(Namespace, RawConf, Opts),
        GenConfErrs,
        Modules
    ),
    maybe_print_conf_errors(Errors, Opts),
    Errors.

print_ok_results_collect_errors(Namespace, RawConf, Opts) ->
    fun(Module, Errors) ->
        case Module:import_config(Namespace, RawConf) of
            {results, {OkResults, ErrResults}} ->
                print_ok_results(OkResults, Opts),
                collect_errors(ErrResults, Errors);
            {ok, OkResult} ->
                print_ok_results([OkResult], Opts),
                Errors;
            {error, ErrResult} ->
                collect_errors([ErrResult], Errors)
        end
    end.

print_ok_results(Results, Opts) ->
    lists:foreach(
        fun(#{changed := Changed}) ->
            maybe_print_changed(Changed, Opts)
        end,
        Results
    ).

collect_errors(Results, Errors) ->
    lists:foldr(
        fun(#{root_key := RootKey, reason := Reason}, Acc) ->
            Acc#{[RootKey] => Reason}
        end,
        Errors,
        Results
    ).

import_generic_conf(Namespace, Data) ->
    lists:map(
        fun(KeyPath) ->
            case emqx_utils_maps:deep_get(KeyPath, Data, undefined) of
                undefined ->
                    {[KeyPath], ok};
                Conf ->
                    UpdateRes = emqx_conf:update(
                        KeyPath,
                        Conf,
                        with_namespace(#{override_to => cluster}, Namespace)
                    ),
                    {[KeyPath], UpdateRes}
            end
        end,
        ?CONF_KEYS
    ).

maybe_print_changed(Changed, Opts) ->
    lists:foreach(
        fun(ChangedPath) ->
            maybe_print(
                "Config key path ~p was present before import and "
                "has been overwritten.~n",
                [pretty_path(ChangedPath)],
                Opts
            )
        end,
        Changed
    ).

maybe_print_conf_errors(Errors, Opts) ->
    maps:foreach(conf_error_formatter(Opts), Errors).

conf_error_formatter(Opts) ->
    fun(Path, Err) ->
        maybe_print(
            "Failed to import the following config path: ~p, reason: ~p~n",
            [pretty_path(Path), Err],
            Opts
        )
    end.

filter_errors(Results) ->
    maps:filter(
        fun
            (_Path, {error, _}) -> true;
            (_, _) -> false
        end,
        Results
    ).

pretty_path(Path) ->
    str(lists:join(".", [str(Part) || Part <- Path])).

str(Data) when is_atom(Data) ->
    atom_to_list(Data);
str(Data) ->
    unicode:characters_to_list(Data).

bin(Data) when is_atom(Data) ->
    atom_to_binary(Data, utf8);
bin(Data) ->
    unicode:characters_to_binary(Data).

root_backup_dir() ->
    Dir = filename:join(emqx:data_dir(), ?ROOT_BACKUP_DIR),
    ok = ensure_path(Dir),
    Dir.

ensure_path(Path) ->
    filelib:ensure_path(Path).

local_datetime(MillisecondTs) ->
    calendar:system_time_to_local_time(MillisecondTs, millisecond).

maybe_print(Format, Args, #{print_fun := PrintFun}) ->
    PrintFun(Format, Args);
maybe_print(_Format, _Args, _Opts) ->
    ok.

maybe_print_mnesia_import_err(TabName, Error, Opts) ->
    maybe_print(
        "[error] Failed to import built-in database table: ~p, reason: ~p~n",
        [TabName, Error],
        Opts
    ).

find_behaviours(Behaviour) ->
    emqx_behaviour_utils:find_behaviours(Behaviour).

-spec table_set_to_tables_mapping() -> #{binary() => module()}.
table_set_to_tables_mapping() ->
    Key = {?MODULE, table_set_to_tables_mapping},
    case persistent_term:get(Key, undefined) of
        undefined ->
            Mapping = build_table_set_to_tables_mapping(),
            persistent_term:put(Key, Mapping),
            Mapping;
        Mapping ->
            Mapping
    end.

build_table_set_to_tables_mapping() ->
    Mods = modules_with_mnesia_tabs_to_backup(),
    lists:foldl(
        fun(Mod, Acc) ->
            {Name, Tabs} = emqx_db_backup:backup_tables(Mod),
            maps:update_with(
                Name,
                fun(PrevTabs) -> Tabs ++ PrevTabs end,
                Tabs,
                Acc
            )
        end,
        #{},
        Mods
    ).

tar(Filename) ->
    Filename ++ ?TAR_SUFFIX.

format_tar_error(Expr) ->
    case Expr of
        {error, Reason} -> {error, erl_tar:format_error(Reason)};
        Other -> Other
    end.

validate_backup_basename(BackupFilePath) ->
    BaseNameNoExt = filename:basename(BackupFilePath, ?TAR_SUFFIX),
    BaseName = filename:basename(BackupFilePath),
    case BaseName =:= BaseNameNoExt ++ ?TAR_SUFFIX of
        true -> ok;
        false -> {error, bad_backup_name}
    end.

backup_dir(BackupFilePath) ->
    BaseName = filename:basename(BackupFilePath, ?TAR_SUFFIX),
    RootBackupDir = root_backup_dir(),
    case filelib:safe_relative_path(BaseName, RootBackupDir) of
        unsafe -> {error, unsafe_backup_name};
        _ -> {ok, filename:join(RootBackupDir, BaseName)}
    end.

backup_path(Filename) when is_binary(Filename) ->
    backup_path(str(Filename));
backup_path(Filename) when is_list(Filename) ->
    maybe
        [Filename] ?= filename:split(Filename),
        ok ?= validate_backup_basename(Filename),
        {ok, filename:join(root_backup_dir(), Filename)}
    else
        _ -> {error, bad_backup_name}
    end.

cleanup_backup_dir(BackupFilePath) ->
    maybe
        {ok, BackupDir} ?= backup_dir(BackupFilePath),
        file:del_dir_r(BackupDir)
    end.

with_namespace(UpdateOpts, ?global_ns) ->
    UpdateOpts;
with_namespace(UpdateOpts, Namespace) when is_binary(Namespace) ->
    UpdateOpts#{namespace => Namespace}.
