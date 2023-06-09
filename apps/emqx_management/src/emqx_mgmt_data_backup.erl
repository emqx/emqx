%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_data_backup).

-export([
    export/0,
    export/1,
    import/1,
    import/2,
    format_error/1
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/logger.hrl").

-define(ROOT_BACKUP_DIR, "backup").
-define(BACKUP_MNESIA_DIR, "mnesia").
-define(TAR_SUFFIX, ".tar.gz").
-define(META_FILENAME, "META.hocon").
-define(CLUSTER_HOCON_FILENAME, "cluster.hocon").
-define(CONF_KEYS, [
    <<"delayed">>,
    <<"rewrite">>,
    <<"retainer">>,
    <<"mqtt">>,
    <<"alarm">>,
    <<"sysmon">>,
    <<"sys_topics">>,
    <<"limiter">>,
    <<"log">>,
    <<"persistent_session_store">>,
    <<"prometheus">>,
    <<"crl_cache">>,
    <<"conn_congestion">>,
    <<"force_shutdown">>,
    <<"flapping_detect">>,
    <<"broker">>,
    <<"force_gc">>,
    <<"zones">>
]).

-define(DEFAULT_OPTS, #{}).
-define(tar(_FileName_), _FileName_ ++ ?TAR_SUFFIX).
-define(fmt_tar_err(_Expr_),
    fun() ->
        case _Expr_ of
            {error, _Reason_} -> {error, erl_tar:format_error(_Reason_)};
            _Other_ -> _Other_
        end
    end()
).

-type backup_file_info() :: #{
    filename => binary(),
    size => non_neg_integer(),
    created_at => binary(),
    node => node(),
    atom() => _
}.

-type db_error_details() :: #{mria:table() => {error, _}}.
-type config_error_details() :: #{emqx_utils_maps:config_path() => {error, _}}.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec export() -> {ok, backup_file_info()} | {error, _}.
export() ->
    export(?DEFAULT_OPTS).

-spec export(map()) -> {ok, backup_file_info()} | {error, _}.
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

-spec import(file:filename_all()) ->
    {ok, #{db_errors => db_error_details(), config_errors => config_error_details()}}
    | {error, _}.
import(BackupFileName) ->
    import(BackupFileName, ?DEFAULT_OPTS).

-spec import(file:filename_all(), map()) ->
    {ok, #{db_errors => db_error_details(), config_errors => config_error_details()}}
    | {error, _}.
import(BackupFileName, Opts) ->
    case is_import_allowed() of
        true ->
            case lookup_file(str(BackupFileName)) of
                {ok, FilePath} ->
                    do_import(FilePath, Opts);
                Err ->
                    Err
            end;
        false ->
            {error, not_core_node}
    end.

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
format_error({unsupported_version, ImportVersion}) ->
    str(
        io_lib:format(
            "[warning] Backup version ~p is newer than EMQX version ~p, import is not allowed.~n",
            [str(ImportVersion), str(emqx_release:version())]
        )
    );
format_error(Reason) ->
    Reason.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

prepare_new_backup(Opts) ->
    Ts = erlang:system_time(millisecond),
    {{Y, M, D}, {HH, MM, SS}} = local_datetime(Ts),
    BackupBaseName = str(
        io_lib:format(
            "emqx-export-~0p-~2..0b-~2..0b-~2..0b-~2..0b-~2..0b.~3..0b",
            [Y, M, D, HH, MM, SS, Ts rem 1000]
        )
    ),
    BackupName = filename:join(root_backup_dir(), BackupBaseName),
    BackupTarName = ?tar(BackupName),
    maybe_print("Exporting data to ~p...~n", [BackupTarName], Opts),
    {ok, TarDescriptor} = ?fmt_tar_err(erl_tar:open(BackupTarName, [write, compressed])),
    {BackupName, TarDescriptor}.

do_export(BackupName, TarDescriptor, Opts) ->
    BackupBaseName = filename:basename(BackupName),
    BackupTarName = ?tar(BackupName),
    Meta = #{
        version => emqx_release:version(),
        edition => emqx_release:edition()
    },
    MetaBin = bin(hocon_pp:do(Meta, #{})),
    MetaFileName = filename:join(BackupBaseName, ?META_FILENAME),

    ok = ?fmt_tar_err(erl_tar:add(TarDescriptor, MetaBin, MetaFileName, [])),
    ok = export_cluster_hocon(TarDescriptor, BackupBaseName, Opts),
    ok = export_mnesia_tabs(TarDescriptor, BackupName, BackupBaseName, Opts),
    ok = ?fmt_tar_err(erl_tar:close(TarDescriptor)),
    {ok, #file_info{
        size = Size,
        ctime = {{Y1, M1, D1}, {H1, MM1, S1}}
    }} = file:read_file_info(BackupTarName),
    CreatedAt = io_lib:format("~p-~p-~p ~p:~p:~p", [Y1, M1, D1, H1, MM1, S1]),
    {ok, #{
        filename => bin(BackupTarName),
        size => Size,
        created_at => bin(CreatedAt),
        node => node()
    }}.

export_cluster_hocon(TarDescriptor, BackupBaseName, Opts) ->
    maybe_print("Exporting cluster configuration...~n", [], Opts),
    RawConf = emqx_config:read_override_conf(#{override_to => cluster}),
    maybe_print(
        "Exporting additional files from EMQX data_dir: ~p...~n", [str(emqx:data_dir())], Opts
    ),
    RawConf1 = read_data_files(RawConf),
    RawConfBin = bin(hocon_pp:do(RawConf1, #{})),
    NameInArchive = filename:join(BackupBaseName, ?CLUSTER_HOCON_FILENAME),
    ok = ?fmt_tar_err(erl_tar:add(TarDescriptor, RawConfBin, NameInArchive, [])).

export_mnesia_tabs(TarDescriptor, BackupName, BackupBaseName, Opts) ->
    maybe_print("Exporting built-in database...~n", [], Opts),
    lists:foreach(
        fun(Tab) -> export_mnesia_tab(TarDescriptor, Tab, BackupName, BackupBaseName, Opts) end,
        tabs_to_backup()
    ).

export_mnesia_tab(TarDescriptor, TabName, BackupName, BackupBaseName, Opts) ->
    maybe_print("Exporting ~p database table...~n", [TabName], Opts),
    {ok, MnesiaBackupName} = do_export_mnesia_tab(TabName, BackupName),
    NameInArchive = mnesia_backup_name(BackupBaseName, TabName),
    ok = ?fmt_tar_err(erl_tar:add(TarDescriptor, MnesiaBackupName, NameInArchive, [])),
    _ = file:delete(MnesiaBackupName),
    ok.

do_export_mnesia_tab(TabName, BackupName) ->
    Node = node(),
    try
        {ok, TabName, [Node]} = mnesia:activate_checkpoint(
            [{name, TabName}, {min, [TabName]}, {allow_remote, false}]
        ),
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
    ?MODULE:mnesia_tabs_to_backup().
-else.
tabs_to_backup() ->
    mnesia_tabs_to_backup().
-endif.

mnesia_tabs_to_backup() ->
    lists:flatten([M:backup_tables() || M <- find_behaviours(emqx_db_backup)]).

mnesia_backup_name(Path, TabName) ->
    filename:join([Path, ?BACKUP_MNESIA_DIR, atom_to_list(TabName)]).

is_import_allowed() ->
    mria_rlog:role() =:= core.

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
            case ImportMajorInt > MajorInt orelse ImportMinorInt > MinorInt of
                true ->
                    %% 4.x backup files are anyway not compatible and will be treated as invalid,
                    %% before this step,
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

do_import(BackupFileName, Opts) ->
    BackupDir = filename:join(root_backup_dir(), filename:basename(BackupFileName, ?TAR_SUFFIX)),
    maybe_print("Importing data from ~p...~n", [BackupFileName], Opts),
    try
        ok = validate_backup_name(BackupFileName),
        ok = extract_backup(BackupFileName),
        {ok, _} = validate_backup(BackupDir),
        ConfErrors = import_cluster_hocon(BackupDir, Opts),
        MnesiaErrors = import_mnesia_tabs(BackupDir, Opts),
        ?SLOG(info, #{msg => "emqx_data_import_success"}),
        {ok, #{db_errors => MnesiaErrors, config_errors => ConfErrors}}
    catch
        error:{badmatch, {error, Reason}}:Stack ->
            ?SLOG(error, #{msg => "emqx_data_import_failed", reason => Reason, stacktrace => Stack}),
            {error, Reason};
        Class:Reason:Stack ->
            ?SLOG(error, #{
                msg => "emqx_data_import_failed",
                exception => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, Reason}
    after
        file:del_dir_r(BackupDir)
    end.

import_mnesia_tabs(BackupDir, Opts) ->
    maybe_print("Importing built-in database...~n", [], Opts),
    filter_errors(
        lists:foldr(
            fun(Tab, Acc) -> Acc#{Tab => import_mnesia_tab(BackupDir, Tab, Opts)} end,
            #{},
            tabs_to_backup()
        )
    ).

import_mnesia_tab(BackupDir, TabName, Opts) ->
    MnesiaBackupFileName = mnesia_backup_name(BackupDir, TabName),
    case filelib:is_regular(MnesiaBackupFileName) of
        true ->
            maybe_print("Importing ~p database table...~n", [TabName], Opts),
            restore_mnesia_tab(BackupDir, MnesiaBackupFileName, TabName, Opts);
        false ->
            maybe_print("No backup file for ~p database table...~n", [TabName], Opts),
            ?SLOG(info, #{msg => "missing_mnesia_backup", table => TabName, backup => BackupDir}),
            ok
    end.

restore_mnesia_tab(BackupDir, MnesiaBackupFileName, TabName, Opts) ->
    Validated =
        catch mnesia:traverse_backup(
            MnesiaBackupFileName, mnesia_backup, dummy, read_only, fun validate_mnesia_backup/2, 0
        ),
    try
        case Validated of
            {ok, _} ->
                %% As we use keep_tables option, we don't need to modify 'copies' (nodes)
                %% in a backup file before restoring it,  as `mnsia:restore/2` will ignore
                %% backed-up schema and keep the current table schema unchanged
                Restored = mnesia:restore(MnesiaBackupFileName, [{default_op, keep_tables}]),
                case Restored of
                    {atomic, [TabName]} ->
                        ok;
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
        _ = file:delete(MnesiaBackupFileName)
    end.

%% NOTE: if backup file is valid, we keep traversing it, though we only need to validate schema.
%% Looks like there is no clean way to abort traversal without triggering any error reporting,
%% `mnesia_bup:read_schema/2` is an option but its direct usage should also be avoided...
validate_mnesia_backup({schema, Tab, CreateList} = Schema, Acc) ->
    ImportAttributes = proplists:get_value(attributes, CreateList),
    Attributes = mnesia:table_info(Tab, attributes),
    case ImportAttributes =/= Attributes of
        true ->
            throw({error, different_table_schema});
        false ->
            {[Schema], Acc}
    end;
validate_mnesia_backup(Other, Acc) ->
    {[Other], Acc}.

extract_backup(BackupFileName) ->
    BackupDir = root_backup_dir(),
    ok = validate_filenames(BackupFileName),
    ?fmt_tar_err(erl_tar:extract(BackupFileName, [{cwd, BackupDir}, compressed])).

validate_filenames(BackupFileName) ->
    {ok, FileNames} = ?fmt_tar_err(erl_tar:table(BackupFileName, [compressed])),
    BackupName = filename:basename(BackupFileName, ?TAR_SUFFIX),
    IsValid = lists:all(
        fun(FileName) ->
            [Root | _] = filename:split(FileName),
            Root =:= BackupName
        end,
        FileNames
    ),
    case IsValid of
        true -> ok;
        false -> {error, bad_archive_dir}
    end.

import_cluster_hocon(BackupDir, Opts) ->
    HoconFileName = filename:join(BackupDir, ?CLUSTER_HOCON_FILENAME),
    case filelib:is_regular(HoconFileName) of
        true ->
            {ok, RawConf} = hocon:files([HoconFileName]),
            {ok, _} = validate_cluster_hocon(RawConf),
            maybe_print("Importing cluster configuration...~n", [], Opts),
            %% At this point, when all validations have been passed, we want to log errors (if any)
            %% but proceed with the next items, instead of aborting the whole import operation
            do_import_conf(RawConf, Opts);
        false ->
            maybe_print("No cluster configuration to be imported.~n", [], Opts),
            ?SLOG(info, #{
                msg => "no_backup_hocon_config_to_import",
                backup => BackupDir
            }),
            #{}
    end.

read_data_files(RawConf) ->
    DataDir = bin(emqx:data_dir()),
    {ok, Cwd} = file:get_cwd(),
    AbsDataDir = bin(filename:join(Cwd, DataDir)),
    RawConf1 = emqx_authz:maybe_read_acl_file(RawConf),
    emqx_utils_maps:deep_convert(RawConf1, fun read_data_file/4, [DataDir, AbsDataDir]).

-define(dir_pattern(_Dir_), <<_Dir_:(byte_size(_Dir_))/binary, _/binary>>).

read_data_file(Key, Val, DataDir, AbsDataDir) ->
    Val1 =
        case Val of
            ?dir_pattern(DataDir) = FileName ->
                do_read_file(FileName);
            ?dir_pattern(AbsDataDir) = FileName ->
                do_read_file(FileName);
            V ->
                V
        end,
    {Key, Val1}.

do_read_file(FileName) ->
    case file:read_file(FileName) of
        {ok, Content} ->
            Content;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_read_data_file",
                filename => FileName,
                reason => Reason
            }),
            FileName
    end.

validate_cluster_hocon(RawConf) ->
    %% write ACL file to comply with the schema...
    RawConf1 = emqx_authz:maybe_write_acl_file(RawConf),
    emqx_hocon:check(
        emqx_conf:schema_module(),
        maps:merge(emqx:get_raw_config([]), RawConf1),
        #{atom_key => false, required => false}
    ).

do_import_conf(RawConf, Opts) ->
    GenConfErrs = filter_errors(maps:from_list(import_generic_conf(RawConf))),
    maybe_print_errors(GenConfErrs, Opts),
    Errors =
        lists:foldr(
            fun(Module, ErrorsAcc) ->
                Module:import_config(RawConf),
                case Module:import_config(RawConf) of
                    {ok, #{changed := Changed}} ->
                        maybe_print_changed(Changed, Opts),
                        ErrorsAcc;
                    {error, #{root_key := RootKey, reason := Reason}} ->
                        ErrorsAcc#{[RootKey] => Reason}
                end
            end,
            GenConfErrs,
            find_behaviours(emqx_config_backup)
        ),
    maybe_print_errors(Errors, Opts),
    Errors.

import_generic_conf(Data) ->
    lists:map(
        fun(Key) ->
            case maps:get(Key, Data, undefined) of
                undefined -> {[Key], ok};
                Conf -> {[Key], emqx_conf:update([Key], Conf, #{override_to => cluster})}
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

maybe_print_errors(Errors, Opts) ->
    maps:foreach(
        fun(Path, Err) ->
            maybe_print(
                "Failed to import the following config path: ~p, reason: ~p~n",
                [pretty_path(Path), Err],
                Opts
            )
        end,
        Errors
    ).

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

validate_backup_name(FileName) ->
    BaseName = filename:basename(FileName, ?TAR_SUFFIX),
    ValidName = BaseName ++ ?TAR_SUFFIX,
    case filename:basename(FileName) of
        ValidName -> ok;
        _ -> {error, bad_backup_name}
    end.

lookup_file(FileName) ->
    case filelib:is_regular(FileName) of
        true ->
            {ok, FileName};
        false ->
            %% Only lookup by basename, don't allow to lookup by file path
            case FileName =:= filename:basename(FileName) of
                true ->
                    FilePath = filename:join(root_backup_dir(), FileName),
                    case filelib:is_file(FilePath) of
                        true -> {ok, FilePath};
                        false -> {error, not_found}
                    end;
                false ->
                    {error, not_found}
            end
    end.

root_backup_dir() ->
    Dir = filename:join(emqx:data_dir(), ?ROOT_BACKUP_DIR),
    ok = ensure_path(Dir),
    Dir.

-if(?OTP_RELEASE < 25).
ensure_path(Path) -> filelib:ensure_dir(filename:join([Path, "dummy"])).
-else.
ensure_path(Path) -> filelib:ensure_path(Path).
-endif.

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
    find_behaviours(Behaviour, apps(), []).

%% Based on minirest_api:find_api_modules/1
find_behaviours(_Behaviour, [] = _Apps, Acc) ->
    Acc;
find_behaviours(Behaviour, [App | Apps], Acc) ->
    case application:get_key(App, modules) of
        undefined ->
            Acc;
        {ok, Modules} ->
            NewAcc = lists:filter(
                fun(Module) ->
                    Info = Module:module_info(attributes),
                    Bhvrs = lists:flatten(
                        proplists:get_all_values(behavior, Info) ++
                            proplists:get_all_values(behaviour, Info)
                    ),
                    lists:member(Behaviour, Bhvrs)
                end,
                Modules
            ),
            find_behaviours(Behaviour, Apps, NewAcc ++ Acc)
    end.

apps() ->
    [
        App
     || {App, _, _} <- application:loaded_applications(),
        case re:run(atom_to_list(App), "^emqx") of
            {match, [{0, 4}]} -> true;
            _ -> false
        end
    ].
