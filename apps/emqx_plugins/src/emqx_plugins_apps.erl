%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_apps).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Plugin's app lifecycle
-export([
    start/1,
    validate/2,
    load/2,
    unload/1,
    stop/1,
    stop_and_unload_loaded/1,
    running_status/1,
    loaded_apps_from/1,
    running_apps_from/1
]).

%% Triggering app's callbacks
-export([
    on_config_changed/3,
    on_health_check/2,
    on_handle_api_call/2
]).

-ifdef(TEST).
-export([do_load/2, rollback/1]).
-endif.

-type health_check_options() :: #{}.

%% `code:which/1' returns `loaded_filename() | non_existing' (see code.erl), but
%% OTP does not export that type, so it is spelled out here.
-type which_ret() :: non_existing | preloaded | cover_compiled | file:filename().

%% One beam file of a plugin package, parsed but not loaded.
-type preflight_entry() :: #{
    module := module(),
    beam := file:filename(),
    bin := binary()
}.
%% The preflight result for one application, in `rel_apps' order: the name-vsn,
%% the application resource file it validated, and its beams.
-type preflight_app() :: {name_vsn(), tuple(), [preflight_entry()]}.
-type preflight() :: [preflight_app()].

%% What a published module replaced, to be able to put it back on rollback: the
%% module, where its previous code came from, and whether that is the very file
%% this package published (then its bytes are not the previous code any more).
-type loaded_module_ref() :: {module(), which_ret(), boolean()}.

-type load_plan_entry() :: #{
    app := module(),
    ebin := file:filename(),
    spec := tuple(),
    entries := [preflight_entry()]
}.
-type load_plan() :: [load_plan_entry()].

%% Everything the publishing phase changed on the code server, so that a failure
%% can be rolled back.
-type tx() :: #{
    prev_path := [file:filename()],
    added_paths := [file:filename()],
    loaded_mods := [loaded_module_ref()],
    loaded_apps := [atom()]
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec running_status(name_vsn() | emqx_plugins_info:t()) -> running | loaded | stopped.
running_status(#{name := PluginName, rel_apps := Apps}) ->
    {AppName, AppVsn} = primary_app_name_vsn(PluginName, Apps),
    RunningApps = running_apps(),
    LoadedApps = loaded_apps(),
    app_running_status(AppName, AppVsn, RunningApps, LoadedApps);
running_status(NameVsn) ->
    {AppName, AppVsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    RunningApps = running_apps(),
    LoadedApps = loaded_apps(),
    app_running_status(AppName, AppVsn, RunningApps, LoadedApps).

%% @doc The applications loaded on this node that run code from `Dir'.
%%
%% This tells apart the leftovers of an interrupted installation from an
%% installation whose code is in use: it only looks at the loaded applications,
%% so it keeps working when the plugin's `release.json' is missing or
%% unreadable.  Unlike `running_status/1', it does not assume that the name of
%% the plugin is the name of one of its applications.
-spec loaded_apps_from(file:filename()) -> [module()].
loaded_apps_from(Dir) ->
    apps_in_dir(application:loaded_applications(), Dir).

%% @doc The applications running on this node that run code from `Dir'.
-spec running_apps_from(file:filename()) -> [module()].
running_apps_from(Dir) ->
    apps_in_dir(application:which_applications(), Dir).

apps_in_dir(Applications, Dir) ->
    DirParts = filename:split(filename:join([Dir])),
    [
        AppName
     || {AppName, _Description, _Vsn} <- Applications,
        app_loaded_from(AppName, DirParts)
    ].

%% @doc Stop and unload the applications that run code from `Dir'.
%%
%% This is the way out for a plugin whose `release.json' can not be read: its
%% applications are found on disk instead of in its metadata, so that the
%% installation can still be stopped and replaced.
-spec stop_and_unload_loaded(file:filename()) -> {ok, [module()]} | {error, term()}.
stop_and_unload_loaded(Dir) ->
    case loaded_apps_from(Dir) of
        [] ->
            {ok, []};
        AppNames ->
            case stop_apps_by_name(AppNames) of
                ok ->
                    ok = unload_apps_by_name(AppNames),
                    {ok, AppNames};
                {error, _} = Error ->
                    Error
            end
    end.

app_loaded_from(AppName, DirParts) ->
    case code:lib_dir(AppName) of
        {error, _} ->
            false;
        AppDir ->
            %% Compare path components: a plugin directory must not match a
            %% sibling whose name merely starts with the same characters.
            lists:prefix(DirParts, filename:split(AppDir))
    end.

-spec start(emqx_plugins_info:t()) -> ok | {error, term()}.
start(#{rel_apps := Apps}) ->
    AppNames =
        lists:map(
            fun(AppNameVsn) ->
                {AppName, _AppVsn} = emqx_plugins_utils:parse_name_vsn(AppNameVsn),
                AppName
            end,
            Apps
        ),
    try
        lists:foreach(
            fun(AppName) ->
                case start_app(AppName) of
                    ok -> ok;
                    {error, Reason} -> throw(Reason)
                end
            end,
            AppNames
        )
    catch
        throw:Reason ->
            {error, Reason}
    end.

%% Stop all apps installed by the plugin package,
%% but not the ones shared with others.
-spec stop(emqx_plugins_info:t()) -> ok | {error, term()}.
stop(#{rel_apps := Apps}) ->
    %% load plugin apps and beam code
    stop_apps_by_name(lists:filtermap(fun parse_name_vsn_for_stopping/1, Apps)).

-spec load(emqx_plugins_info:t(), file:filename()) -> ok | {error, term()}.
load(#{rel_apps := Apps}, LibDir) ->
    %% Everything which can be checked without touching the code server is
    %% checked first: a package rejected here never runs any of its code.
    case preflight_beams(Apps, LibDir) of
        {error, _} = Error ->
            Error;
        {ok, Preflight} ->
            LoadedApps = loaded_apps(),
            case plan_load(Apps, LibDir, Preflight, LoadedApps) of
                {error, _} = Error ->
                    Error;
                {ok, Plan0} ->
                    load_plan(Plan0)
            end
    end.

load_plan(Plan0) ->
    case order_plan_by_included_apps(Plan0) of
        {error, _} = Error ->
            Error;
        {ok, Plan} ->
            publish_plan(Plan)
    end.

publish_plan(Plan) ->
    case do_load(Plan, new_tx()) of
        {ok, _Tx} ->
            ok;
        {error, Reason, Tx} ->
            RollbackErrors = rollback(Tx),
            ?SLOG(error, #{
                msg => "plugin_load_failed_and_rolled_back",
                reason => Reason,
                rollback_errors => RollbackErrors
            }),
            {error, add_rollback_errors(Reason, RollbackErrors)}
    end.

-spec validate(emqx_plugins_info:t(), file:filename()) -> ok | {error, map()}.
validate(#{rel_apps := Apps}, LibDir) ->
    maybe
        ok ?= lists:foldl(fun(App, Acc) -> validate_plugin_app(App, LibDir, Acc) end, ok, Apps),
        {ok, _} ?= preflight_beams(Apps, LibDir),
        ok
    end.

-spec unload(emqx_plugins_info:t()) -> ok | {error, term()}.
unload(#{rel_apps := Apps}) ->
    unload_apps_by_name(lists:filtermap(fun parse_name_vsn_for_stopping/1, Apps)).

stop_apps_by_name(AppNames) ->
    case stop_apps(AppNames) of
        {ok, []} ->
            %% all apps stopped
            ok;
        {ok, Left} ->
            ?SLOG(info, #{
                msg => "unable_to_stop_plugin_apps",
                apps => Left,
                reason => "running_apps_still_depends_on_this_apps"
            }),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

unload_apps_by_name(AppNames) ->
    ?SLOG(info, #{
        msg => "emqx_plugins_unloading_apps",
        apps => AppNames
    }),
    unload_apps(AppNames, running_apps(), loaded_apps()).

%%--------------------------------------------------------------------
%% API for triggering app's callbacks
%%--------------------------------------------------------------------

-spec on_config_changed(name_vsn(), map(), map()) -> ok | {error, term()}.
on_config_changed(NameVsn, OldConf, NewConf) ->
    apply_callback(NameVsn, {on_config_changed, 2}, [OldConf, NewConf]).

-spec on_health_check(name_vsn(), health_check_options()) -> ok | {error, term()}.
on_health_check(NameVsn, Options) ->
    apply_callback(NameVsn, {on_health_check, 1}, [Options]).

-spec on_handle_api_call(name_vsn(), map()) ->
    {ok, pos_integer(), map() | [{term(), iodata()}], term()}
    | {error, term(), iodata()}
    | {error, pos_integer(), map() | [{term(), iodata()}], term()}
    | {error, not_found}
    | {error, term()}.
on_handle_api_call(NameVsn, #{
    method := Method, path := PathRemainder, request := Request, context := Context
}) ->
    apply_api_callback(NameVsn, {on_handle_api_call, 4}, [Method, PathRemainder, Request, Context]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

apply_callback(NameVsn, {FuncName, Arity}, Args) ->
    maybe
        {ok, PluginAppModule} ?= app_module_name(NameVsn),
        ok ?= is_callback_exported(PluginAppModule, FuncName, Arity),
        try erlang:apply(PluginAppModule, FuncName, Args) of
            ok -> ok;
            {error, _} = Error -> Error;
            Other -> {error, {bad_callback_return_value, Other}}
        catch
            Class:Error:Stacktrace ->
                ?SLOG(error, #{
                    msg => "failed_to_apply_plugin_callback",
                    callback => {FuncName, Arity},
                    exception => Class,
                    reason => Error,
                    stacktrace => Stacktrace
                }),
                {error, Error}
        end
    else
        {error, Reason} ->
            ?SLOG(info, #{
                msg => "callback_not_found", callback => {FuncName, Arity}, reason => Reason
            }),
            ok;
        _ ->
            ok
    end.

apply_api_callback(NameVsn, {FuncName, Arity}, Args) ->
    maybe
        {ok, PluginAppModule} ?= app_module_name(NameVsn),
        ok ?= is_callback_exported(PluginAppModule, FuncName, Arity),
        erlang:apply(PluginAppModule, FuncName, Args)
    else
        {error, _Reason} ->
            {error, not_found}
    end.

validate_plugin_app(_AppNameVsn, _LibDir, Error) when Error =/= ok ->
    Error;
validate_plugin_app(AppNameVsn, LibDir, ok) ->
    {AppName, AppVsn} = emqx_plugins_utils:parse_name_vsn(AppNameVsn),
    EbinDir = filename:join([LibDir, AppNameVsn, "ebin"]),
    AppFile = filename:join(EbinDir, atom_to_list(AppName) ++ ".app"),
    case file:consult(AppFile) of
        {ok, [{application, AppName, Props}]} ->
            validate_plugin_app(AppName, AppVsn, EbinDir, AppFile, Props);
        {ok, AppSpec} ->
            {error, #{
                kind => invalid_package,
                msg => "bad_plugin_app_file",
                path => AppFile,
                reason => AppSpec
            }};
        {error, Reason} ->
            {error, #{
                kind => invalid_package,
                msg => "bad_plugin_app_file",
                path => AppFile,
                reason => Reason
            }}
    end.

validate_plugin_app(AppName, AppVsn, EbinDir, AppFile, Props) when is_list(Props) ->
    Vsn = proplists:get_value(vsn, Props, undefined),
    case
        (is_list(Vsn) orelse is_binary(Vsn)) andalso
            emqx_plugins_utils:bin(Vsn) =:= emqx_plugins_utils:bin(AppVsn)
    of
        true ->
            validate_loaded_plugin_app(AppName, EbinDir, Props);
        false ->
            {error, #{
                kind => invalid_package,
                msg => "plugin_app_version_mismatch",
                path => AppFile,
                expected_vsn => AppVsn,
                actual_vsn => Vsn
            }}
    end;
validate_plugin_app(_AppName, _AppVsn, _EbinDir, AppFile, Props) ->
    {error, #{
        kind => invalid_package,
        msg => "bad_plugin_app_file",
        path => AppFile,
        reason => Props
    }}.

validate_loaded_plugin_app(AppName, EbinDir, Props) ->
    case lists:keyfind(AppName, 1, loaded_apps()) of
        false ->
            ok;
        {AppName, _} ->
            ExpectedEbinDir = path_to_list(EbinDir),
            case app_ebin_dir(AppName) of
                ExpectedEbinDir ->
                    ok;
                LoadedEbinDir ->
                    case
                        is_protected_app(AppName) orelse
                            is_shared_plugin_app(AppName, Props, LoadedEbinDir)
                    of
                        true ->
                            ok;
                        false ->
                            {error, #{
                                kind => invalid_package,
                                msg => "plugin_app_loaded_outside_package",
                                name => AppName,
                                expected_ebin => ExpectedEbinDir,
                                loaded_ebin => LoadedEbinDir
                            }}
                    end
            end
    end.

is_shared_plugin_app(AppName, Props, LoadedEbinDir) when is_list(LoadedEbinDir) ->
    InstallDir = filename:absname(emqx_plugins_fs:install_dir()),
    EbinDir = filename:absname(LoadedEbinDir),
    case string:prefix(EbinDir, InstallDir ++ "/") of
        nomatch ->
            false;
        _ ->
            AppFile = filename:join(EbinDir, atom_to_list(AppName) ++ ".app"),
            file:consult(AppFile) =:= {ok, [{application, AppName, Props}]}
    end;
is_shared_plugin_app(_AppName, _Props, _LoadedEbinDir) ->
    false.

app_ebin_dir(AppName) ->
    case code:lib_dir(AppName) of
        {error, _} = Error -> Error;
        LibDir -> filename:join(LibDir, "ebin")
    end.

path_to_list(Path) when is_binary(Path) ->
    binary_to_list(Path);
path_to_list(Path) ->
    Path.

%% Turn the apps that are not loaded yet plus the preflight result into a plan,
%% keeping the order of `rel_apps'.  An app that is already loaded is left
%% alone, exactly like the sequential loader used to do.
-spec plan_load([name_vsn()], file:filename(), preflight(), [{module(), term()}]) ->
    {ok, load_plan()} | {error, map()}.
plan_load([], _LibDir, _Preflight, _LoadedApps) ->
    {ok, []};
plan_load([AppNameVsn | Rest], LibDir, Preflight, LoadedApps) ->
    {AppName, AppVsn} = emqx_plugins_utils:parse_name_vsn(AppNameVsn),
    Ebin = ebin_dir(LibDir, AppNameVsn),
    case lists:keyfind(AppName, 1, LoadedApps) of
        false ->
            case lists:keyfind(AppNameVsn, 1, Preflight) of
                {_, AppSpec, Entries} ->
                    %% The application resource file is the one the preflight
                    %% validated, so it can not change in between.
                    Spec = drop_loaded_included_apps(drop_self_dep(AppSpec), LoadedApps),
                    case plan_load(Rest, LibDir, Preflight, LoadedApps) of
                        {ok, Tail} ->
                            {ok, [
                                #{
                                    app => AppName,
                                    ebin => Ebin,
                                    spec => Spec,
                                    entries => Entries
                                }
                                | Tail
                            ]};
                        {error, _} = Error ->
                            Error
                    end;
                false ->
                    {error, #{
                        msg => "failed_to_load_plugin_app",
                        name => AppName,
                        reason => preflight_result_missing
                    }}
            end;
        {_, Vsn} ->
            case emqx_plugins_utils:bin(Vsn) =:= emqx_plugins_utils:bin(AppVsn) of
                true ->
                    %% already loaded on the exact version
                    ok;
                false ->
                    ?SLOG(warning, #{
                        msg => "plugin_app_already_loaded",
                        name => AppName,
                        loaded_vsn => Vsn,
                        loading_vsn => AppVsn
                    })
            end,
            plan_load(Rest, LibDir, Preflight, LoadedApps)
    end.

%% `application:load/1' recursively loads the applications listed in
%% `included_applications', and it reads their `.app' files from the code path
%% (`application_controller.erl:1332-1343').  Phase A has already put the
%% package's ebin directories there, so an application shipped by the package
%% would be loaded from disk, with the spec `drop_loaded_included_apps/2' was
%% meant to hide.  Loading the package's own applications first, deepest
%% `included_applications' first, makes the controller find them already loaded
%% and never read their file.
order_plan_by_included_apps(Plan) ->
    Names = [App || #{app := App} <- Plan],
    order_plan(Plan, Names, []).

order_plan([], _Names, Acc) ->
    {ok, lists:reverse(Acc)};
order_plan(Pending, Names, Acc) ->
    case lists:splitwith(fun(Entry) -> not plan_deps_loaded(Entry, Names, Acc) end, Pending) of
        {_Before, []} ->
            {error, #{
                msg => "plugin_included_applications_cycle",
                apps => [App || #{app := App} <- Pending]
            }};
        {Before, [Entry | After]} ->
            order_plan(Before ++ After, Names, [Entry | Acc])
    end.

%% Whether every application of the package this one includes has already been
%% scheduled.  Included applications which are not part of the package are not
%% loaded by us (either they are already loaded, or loading fails) and do not
%% constrain the order.
plan_deps_loaded(#{spec := Spec}, Names, Acc) ->
    Scheduled = [App || #{app := App} <- Acc],
    lists:all(
        fun(App) -> not lists:member(App, Names) orelse lists:member(App, Scheduled) end,
        included_apps(Spec)
    ).

included_apps({application, _AppName, Props}) ->
    case proplists:get_value(included_applications, Props, []) of
        Included when is_list(Included) -> Included;
        _ -> []
    end.

%% An application which is included by the plugin but is already loaded before
%% this transaction (typically one the release ships) must not be part of the
%% loaded spec: the application controller skips it while loading anyway, but
%% `application:unload/1' recursively unloads every loaded included application
%% (`application_controller.erl:1346-1355'), with no check that it is running.
%% Keeping it would let the rollback of a rejected package unload a running
%% application and take the node down.
drop_loaded_included_apps({application, AppName, Props} = AppSpec, LoadedApps) ->
    case lists:keyfind(included_applications, 1, Props) of
        {included_applications, Included} when is_list(Included) ->
            Kept = [App || App <- Included, not lists:keymember(App, 1, LoadedApps)],
            case Kept =:= Included of
                true ->
                    AppSpec;
                false ->
                    ?SLOG(info, #{
                        msg => "plugin_included_applications_already_loaded",
                        name => AppName,
                        not_taken_over => Included -- Kept
                    }),
                    {application, AppName,
                        lists:keyreplace(
                            included_applications, 1, Props, {included_applications, Kept}
                        )}
            end;
        _ ->
            AppSpec
    end.

-spec new_tx() -> tx().
new_tx() ->
    #{
        prev_path => code:get_path(),
        added_paths => [],
        loaded_mods => [],
        loaded_apps => []
    }.

%% Publish a plan in three phases, each of them rollback-able:
%%
%% 1. put every ebin on the code path;
%% 2. load every application resource file (`application:load/1' does not load
%%    nor run any beam code, see `application.erl');
%% 3. load the modules themselves.
%%
%% Phases 1 and 2 cannot run plugin code, so every failure they can produce is
%% dealt with before the first module is loaded.  Only phase 3 can run
%% `-on_load' functions.
-spec do_load(load_plan(), tx()) -> {ok, tx()} | {error, map(), tx()}.
do_load(Plan, Tx) ->
    maybe
        {ok, Tx1} ?= add_ebin_paths(Plan, Tx),
        {ok, Tx2} ?= load_app_specs(Plan, Tx1),
        load_beams(Plan, Tx2)
    end.

add_ebin_paths([], Tx) ->
    {ok, Tx};
add_ebin_paths([#{ebin := Ebin} | Rest], Tx) ->
    case code:add_patha(Ebin) of
        true ->
            Added = maps:get(added_paths, Tx),
            add_ebin_paths(Rest, Tx#{added_paths => [Ebin | Added]});
        {error, Reason} ->
            {error,
                #{
                    msg => "failed_to_add_plugin_ebin_to_code_path",
                    ebin => Ebin,
                    reason => Reason
                },
                Tx}
    end.

load_app_specs([], Tx) ->
    {ok, Tx};
load_app_specs([#{app := AppName, spec := Spec} | Rest], Tx) ->
    case application:load(Spec) of
        ok ->
            Loaded = maps:get(loaded_apps, Tx),
            load_app_specs(Rest, Tx#{loaded_apps => [AppName | Loaded]});
        {error, {already_loaded, _}} ->
            %% Not loaded by this transaction: do not unload it on rollback.
            load_app_specs(Rest, Tx);
        {error, Reason} ->
            %% `application:load/1' inserts the application before it loads the
            %% applications it includes, so a failure can leave it behind.
            %% Record it so that the rollback unloads it again.
            Loaded = maps:get(loaded_apps, Tx),
            {error,
                #{
                    msg => "failed_to_load_plugin_app",
                    name => AppName,
                    reason => Reason
                },
                Tx#{loaded_apps => [AppName | Loaded]}}
    end.

load_beams([], Tx) ->
    {ok, Tx};
load_beams([#{app := AppName, ebin := Ebin, entries := Entries} | Rest], Tx) ->
    case publish_entries(Entries, Tx) of
        {ok, Tx1} ->
            ?SLOG(debug, #{
                msg => "plugin_app_loaded",
                name => AppName,
                ebin => Ebin,
                modules => [Module || #{module := Module} <- Entries]
            }),
            load_beams(Rest, Tx1);
        {error, _Reason, _Tx} = Error ->
            Error
    end.

publish_entries([], Tx) ->
    {ok, Tx};
publish_entries([Entry | Rest], Tx) ->
    case publish(Entry) of
        {ok, Ref} ->
            Loaded = maps:get(loaded_mods, Tx),
            publish_entries(Rest, Tx#{loaded_mods => [Ref | Loaded]});
        {error, Reason} ->
            {error, Reason, Tx}
    end.

%% Load the very bytes that were preflighted, so that the file can not change
%% between the check and the load.
-spec publish(preflight_entry()) -> {ok, loaded_module_ref()} | {error, map()}.
publish(#{module := Module, beam := Beam, bin := Bin}) ->
    Prev = previous_code(Module),
    case code:load_binary(Module, Beam, Bin) of
        {module, Module} ->
            %% Best effort: drop the version this one just replaced.  Note that
            %% `code:load_binary/3' itself purges pre-existing old code (with
            %% `Purge = true').
            _ = code:soft_purge(Module),
            {ok, {Module, Prev, same_file(Prev, Beam)}};
        {error, Reason} ->
            %% A load which fails never becomes current: `code:load_binary/3'
            %% purges only the old code before loading, and a failure such as an
            %% `-on_load' function returning an error aborts the load with the
            %% module still loaded as it was (verified on OTP 27.3.4.2).  There
            %% is thus nothing to put back for this module, which is why no
            %% entry is added to `loaded_mods'.
            {error, #{
                msg => "failed_to_load_plugin_beam",
                module => Module,
                path => Beam,
                reason => Reason
            }}
    end.

%% The code `Module' was running from before this transaction, or `non_existing'
%% when it was not loaded.  `code:which/1' alone can not be used: it also
%% searches the code path for a module which is not loaded, and phase 1 has
%% already put the package's own ebin there, so it would report the package
%% beam as if it were the previous code of a module which had none.
-spec previous_code(module()) -> which_ret().
previous_code(Module) ->
    case code:is_loaded(Module) of
        {file, _} -> code:which(Module);
        false -> non_existing
    end.

%% Whether the previous code was loaded from the very file this package
%% publishes.  It happens on an in-place reinstall, where the package has
%% already overwritten that file: its bytes are then the package's, and the
%% previous code can not be read back from it.
%%
%% Path equality is a conservative proxy: it also matches a reinstall of an
%% unchanged package, whose file still holds the previous bytes.  Reloading is
%% not an option in that case either, because the same path may hold the
%% package's bytes instead, and OTP has no API to read the previous in-memory
%% code back (`code:get_object_code/1' reads the file).  Such a module is
%% therefore unloaded rather than restored, and logged as
%% `plugin_module_not_restored' rather than reported as a rollback failure.
same_file(Prev, Beam) when is_list(Prev); is_binary(Prev) ->
    filename:absname(path_to_list(Prev)) =:= filename:absname(path_to_list(Beam));
same_file(_Prev, _Beam) ->
    false.

%% A declared emqx_plugins dependency is satisfied by construction:
%% emqx_plugins is always running when a plugin is started. Drop it from the
%% app spec so packages built for releases where the dependency deadlocked
%% the boot keep working.
drop_self_dep({application, AppName, Props} = AppSpec) ->
    Deps = proplists:get_value(applications, Props, []),
    case lists:member(emqx_plugins, Deps) of
        true ->
            ?SLOG(info, #{
                msg => "plugin_app_declares_emqx_plugins_dependency",
                name => AppName,
                hint =>
                    "remove emqx_plugins from the plugin application's"
                    " dependencies (mix.exs for mix-built plugins)"
            }),
            Deps1 = {applications, lists:delete(emqx_plugins, Deps)},
            {application, AppName, lists:keyreplace(applications, 1, Props, Deps1)};
        false ->
            AppSpec
    end.

%%--------------------------------------------------------------------
%% Rollback
%%--------------------------------------------------------------------

%% Undo what the publishing phase did, in reverse publishing order.  Every step
%% is attempted even when an earlier one failed; the failed steps are returned
%% so that the caller can report them.
-spec rollback(tx()) -> [map()].
rollback(Tx) ->
    ModuleErrors = rollback_modules(maps:get(loaded_mods, Tx, []), []),
    AppErrors = rollback_apps(maps:get(loaded_apps, Tx, []), []),
    PathErrors = rollback_paths(
        maps:get(added_paths, Tx, []),
        maps:get(prev_path, Tx, undefined)
    ),
    ModuleErrors ++ AppErrors ++ PathErrors.

rollback_modules([], Errors) ->
    lists:reverse(Errors);
rollback_modules([{Module, Prev, SameFile} | Rest], Errors) ->
    case restore_module(Module, Prev, SameFile) of
        ok ->
            rollback_modules(Rest, Errors);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "plugin_load_rollback_step_failed",
                step => restore_module,
                module => Module,
                reason => Reason
            }),
            rollback_modules(Rest, [
                #{step => restore_module, module => Module, reason => Reason} | Errors
            ])
    end.

rollback_apps([], Errors) ->
    lists:reverse(Errors);
rollback_apps([App | Rest], Errors) ->
    case application:unload(App) of
        ok ->
            rollback_apps(Rest, Errors);
        {error, {not_loaded, App}} ->
            rollback_apps(Rest, Errors);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "plugin_load_rollback_step_failed",
                step => unload_app,
                app => App,
                reason => Reason
            }),
            rollback_apps(Rest, [#{step => unload_app, app => App, reason => Reason} | Errors])
    end.

rollback_paths([], _PrevPath) ->
    [];
rollback_paths(_AddedPaths, undefined) ->
    [];
rollback_paths(AddedPaths, PrevPath) ->
    %% Only what this transaction added is removed, so code path entries added
    %% concurrently by another load are not discarded.  `code:add_patha/1'
    %% moves an already present directory to the front, which `code:del_path/1'
    %% can not undo; putting the previous path back wholesale is the only way to
    %% restore that order, so it assumes the caller serializes plugin lifecycle
    %% operations (`emqx_plugins_fs:prepare_replacement/1').
    {Reordered, Added} = lists:partition(
        fun(Path) -> lists:member(Path, PrevPath) end, AddedPaths
    ),
    Errors = remove_added_paths(Added) ++ restore_reordered_paths(Reordered, PrevPath),
    lists:foreach(
        fun(Error) ->
            ?SLOG(error, Error#{msg => "plugin_load_rollback_step_failed"})
        end,
        Errors
    ),
    Errors.

remove_added_paths(Added) ->
    lists:filtermap(
        fun(Path) ->
            case code:del_path(Path) of
                {error, Reason} ->
                    {true, #{step => remove_code_path, path => Path, reason => Reason}};
                _Removed ->
                    %% `true', or `false' when the directory is not on the path
                    %% any more (removed concurrently, or listed twice): either
                    %% way it is no longer there, which is all that is wanted.
                    false
            end
        end,
        Added
    ).

restore_reordered_paths([], _PrevPath) ->
    [];
restore_reordered_paths(_Reordered, PrevPath) ->
    case code:set_path(PrevPath) of
        true ->
            [];
        {error, Reason} ->
            [#{step => restore_code_path, reason => Reason}]
    end.

%% Put back the code which `publish/1' replaced.
-spec restore_module(module(), which_ret(), boolean()) -> ok | {error, term()}.
restore_module(Module, Prev, true) when is_list(Prev); is_binary(Prev) ->
    %% The previous code was loaded from the very file this package published,
    %% so that file holds the package's bytes now: reloading it would keep the
    %% rejected code.  Unloading is the only safe option left.  This is an
    %% expected outcome, not a failed rollback: it is logged here and not
    %% reported in `rollback_errors'.
    drop_module(Module),
    ?SLOG(warning, #{
        msg => "plugin_module_not_restored",
        module => Module,
        previous_file => Prev,
        hint =>
            "the module was unloaded instead of restored because its previous"
            " code was loaded from the file this package overwrote; it is"
            " loaded again from disk on the next install or start"
    }),
    ok;
restore_module(Module, Prev, false) when is_list(Prev); is_binary(Prev) ->
    %% `code:load_abs/1' does not purge (`Purge = false') and fails with
    %% `{error, not_purged}' when the old slot is taken, so clear it first.
    %% NOTE: loading the previous version runs its `-on_load' function again.
    %% There is no API to make the code which the publishing phase replaced
    %% current without loading it, so a non-idempotent initializer can run
    %% twice here; if it fails, the previous version is reported as
    %% `restore_failed' and dropped instead of leaving the rejected code.
    _ = code:soft_purge(Module),
    PrevFile = filename:rootname(path_to_list(Prev)),
    case code:load_abs(PrevFile) of
        {module, Module} ->
            _ = code:soft_purge(Module),
            ok;
        {error, Reason1} ->
            %% The old code is still referenced: fall back to a hard purge.
            %% That purge can kill processes still executing that version, but
            %% leaving the rejected code current would be worse.
            _ = code:purge(Module),
            case code:load_abs(PrevFile) of
                {module, Module} ->
                    ok;
                {error, Reason2} ->
                    %% The previous version can not be put back (for example
                    %% its directory was already purged).  Then the code of the
                    %% rejected package must not stay loaded either.
                    drop_module(Module),
                    {error, {restore_failed, Prev, Reason1, Reason2}}
            end
    end;
restore_module(Module, _Prev, _SameFile) ->
    %% The module was not loaded from a file before: unloading is enough.
    drop_module(Module).

-spec drop_module(module()) -> ok.
drop_module(Module) ->
    %% `code:delete/1' makes the current code old and `code:purge/1' removes old
    %% code; both are needed.  Each is done twice because the first `delete'
    %% fails when there was already old code to purge.  `code:purge/1' removes
    %% the old code even when processes are still executing it (it kills them),
    %% so no failure can be left to report here; a purge which had to kill
    %% processes is the documented `Purge = true' behaviour of this module.
    _ = code:delete(Module),
    _ = code:purge(Module),
    _ = code:delete(Module),
    _ = code:purge(Module),
    ok.

-spec add_rollback_errors(map(), [map()]) -> map().
add_rollback_errors(Reason, []) ->
    Reason;
add_rollback_errors(Reason, RollbackErrors) ->
    Reason#{rollback_errors => RollbackErrors}.

%%--------------------------------------------------------------------
%% Beam preflight
%%
%% Nothing here loads a module, runs an `-on_load' function or changes the code
%% server: only `file:read_file/1', `beam_lib' and `erlang:prepare_loading/2'
%% are used.
%%--------------------------------------------------------------------

-spec preflight_beams([name_vsn()], file:filename()) -> {ok, preflight()} | {error, map()}.
preflight_beams(Apps, LibDir) ->
    preflight_beams(Apps, LibDir, #{}, []).

preflight_beams([], _LibDir, _Seen, Acc) ->
    {ok, lists:reverse(Acc)};
preflight_beams([AppNameVsn | Rest], LibDir, Seen, Acc) ->
    case preflight_app_beams(AppNameVsn, LibDir) of
        {ok, AppSpec, Entries} ->
            case check_no_duplicates(Entries, Seen) of
                ok ->
                    Seen1 = lists:foldl(
                        fun(#{module := Module}, S) -> S#{Module => true} end, Seen, Entries
                    ),
                    preflight_beams(Rest, LibDir, Seen1, [
                        {AppNameVsn, AppSpec, Entries} | Acc
                    ]);
                {error, Error} ->
                    log_preflight_failure(Error),
                    {error, Error}
            end;
        {error, Error} ->
            log_preflight_failure(Error),
            {error, Error}
    end.

check_no_duplicates(Entries, Seen) ->
    check_no_duplicates(Entries, Seen, #{}).

check_no_duplicates([], _Seen, _This) ->
    ok;
check_no_duplicates([#{module := Module} | Rest], Seen, This) ->
    case is_map_key(Module, Seen) orelse is_map_key(Module, This) of
        true ->
            {error, preflight_error("duplicate_plugin_module", #{module => Module})};
        false ->
            check_no_duplicates(Rest, Seen, This#{Module => true})
    end.

preflight_app_beams(AppNameVsn, LibDir) ->
    {AppName, AppVsn} = emqx_plugins_utils:parse_name_vsn(AppNameVsn),
    EbinDir = ebin_dir(LibDir, AppNameVsn),
    AppFile = filename:join(EbinDir, atom_to_list(AppName) ++ ".app"),
    case file:consult(AppFile) of
        {ok, [{application, AppName, Props}]} ->
            case preflight_app_beams(AppNameVsn, AppName, AppVsn, EbinDir, Props) of
                {ok, Entries} -> {ok, {application, AppName, Props}, Entries};
                {error, _} = Error -> Error
            end;
        {ok, Other} ->
            {error, preflight_error("bad_plugin_app_file", #{path => AppFile, reason => Other})};
        {error, Reason} ->
            {error, preflight_error("bad_plugin_app_file", #{path => AppFile, reason => Reason})}
    end.

preflight_app_beams(AppNameVsn, AppName, _AppVsn, EbinDir, Props) when is_list(Props) ->
    Beams = lists:sort(filelib:wildcard(filename:join([EbinDir, "*.beam"]))),
    Declared = proplists:get_value(modules, Props, undefined),
    case check_declared_modules(Beams, Declared, AppNameVsn, AppName, EbinDir) of
        {ok, DeclaredModules} ->
            check_beams(Beams, AppNameVsn, DeclaredModules, Props);
        {error, _} = Error ->
            Error
    end;
preflight_app_beams(AppNameVsn, AppName, _AppVsn, _EbinDir, Props) ->
    {error,
        preflight_error("bad_plugin_app_file", #{
            name_vsn => AppNameVsn, app => AppName, reason => Props
        })}.

%% A package whose ebin holds no beam at all may legitimately not declare
%% `modules' at all.  A declaration which is there but malformed is rejected in
%% any case, and as soon as there is a beam to load the declaration must be a
%% usable list covering every one of them.
check_declared_modules(Beams, Declared, AppNameVsn, AppName, EbinDir) ->
    case is_module_list(Declared) of
        false when Beams =:= [] andalso Declared =:= undefined ->
            {ok, []};
        false ->
            {error,
                preflight_error("plugin_app_modules_not_declared", #{
                    name_vsn => AppNameVsn, app => AppName, ebin => EbinDir
                })};
        true ->
            case missing_module_beams(Declared, EbinDir) of
                [] ->
                    {ok, Declared};
                [Missing | _] ->
                    {error,
                        preflight_error("plugin_app_beam_missing", #{
                            name_vsn => AppNameVsn,
                            app => AppName,
                            module => Missing,
                            ebin => EbinDir
                        })}
            end
    end.

is_module_list(Modules) ->
    is_list(Modules) andalso lists:all(fun is_atom/1, Modules).

missing_module_beams(Modules, EbinDir) ->
    [
        Module
     || Module <- Modules,
        not filelib:is_regular(filename:join(EbinDir, atom_to_list(Module) ++ ".beam"))
    ].

check_beams([], _AppNameVsn, _DeclaredModules, _Props) ->
    {ok, []};
check_beams([Beam | Rest], AppNameVsn, DeclaredModules, Props) ->
    case check_beam(Beam, AppNameVsn, DeclaredModules, Props) of
        {ok, Entry} ->
            case check_beams(Rest, AppNameVsn, DeclaredModules, Props) of
                {ok, Entries} -> {ok, [Entry | Entries]};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

-spec check_beam(file:filename(), name_vsn(), [module()], list()) ->
    {ok, preflight_entry()} | {error, map()}.
check_beam(Beam, AppNameVsn, DeclaredModules, Props) ->
    maybe
        {ok, Bin} ?= read_beam(Beam, AppNameVsn),
        {ok, EmbeddedModule} ?= parse_beam(Beam, Bin, AppNameVsn),
        ok ?= check_beam_module_name(Beam, EmbeddedModule, AppNameVsn),
        ok ?= check_declared_module(Beam, EmbeddedModule, DeclaredModules, AppNameVsn),
        ok ?= check_beam_loadable(Beam, Bin, EmbeddedModule, AppNameVsn),
        {AppName, _AppVsn} = emqx_plugins_utils:parse_name_vsn(AppNameVsn),
        ok ?=
            check_ownership(
                EmbeddedModule, filename:dirname(Beam), AppName, Props, Beam, AppNameVsn
            ),
        {ok, #{module => EmbeddedModule, beam => Beam, bin => Bin}}
    end.

read_beam(Beam, AppNameVsn) ->
    case file:read_file(Beam) of
        {ok, Bin} ->
            {ok, Bin};
        {error, Reason} ->
            {error,
                preflight_error("plugin_beam_unreadable", #{
                    name_vsn => AppNameVsn, path => Beam, reason => Reason
                })}
    end.

%% `beam_lib:info/1' reads the module name from the `Atom'/`AtU8' chunk and
%% lists every chunk with its position and size.  Its result is not wrapped in
%% an `ok' tuple.
parse_beam(Beam, Bin, AppNameVsn) ->
    case beam_lib:info(Bin) of
        {error, beam_lib, Reason} ->
            {error, classify_beam_info_error(Beam, Bin, AppNameVsn, Reason)};
        Info when is_list(Info) ->
            Module = proplists:get_value(module, Info),
            Chunks = proplists:get_value(chunks, Info, []),
            case check_beam_chunks(Beam, Bin, AppNameVsn, Chunks) of
                ok -> {ok, Module};
                {error, _} = Error -> Error
            end
    end.

classify_beam_info_error(Beam, Bin, AppNameVsn, {not_a_beam_file, _Source}) ->
    %% `beam_lib' reports the whole input as the source when it was given a
    %% binary, which must not end up in the log or in the API response.
    preflight_error("plugin_beam_unparsable", #{
        name_vsn => AppNameVsn,
        path => Beam,
        reason => {not_a_beam_file, {binary, byte_size(Bin)}}
    });
classify_beam_info_error(Beam, Bin, AppNameVsn, Reason) ->
    %% `beam_lib' puts the whole input in the source position of the other
    %% errors as well (`filename/1' returns a binary as is), so it is replaced by
    %% its size here too: only the file name and the size may reach the log or
    %% the API response.
    BinSize = byte_size(Bin),
    {Msg, Scrubbed} =
        case Reason of
            {chunk_too_big, _Source, ChunkId, Size, Len} ->
                {"plugin_beam_truncated", {chunk_too_big, {binary, BinSize}, ChunkId, Size, Len}};
            {invalid_beam_file, _Source, Pos} ->
                {"plugin_beam_unparsable", {invalid_beam_file, {binary, BinSize}, Pos}};
            {missing_chunk, _Source, ChunkId} ->
                {"plugin_beam_unparsable", {missing_chunk, {binary, BinSize}, ChunkId}};
            _ ->
                {"plugin_beam_unparsable", Reason}
        end,
    preflight_error(Msg, #{name_vsn => AppNameVsn, path => Beam, reason => Scrubbed}).

%% `beam_lib' locates chunks through the chunk table without validating that
%% they fit in the file, so a truncated beam can look fine to it (and even to
%% `chunks/2').  Check the bounds and the chunks a loadable module needs.
check_beam_chunks(Beam, Bin, AppNameVsn, Chunks) ->
    HasAtomChunk = has_chunk("Atom", Chunks) orelse has_chunk("AtU8", Chunks),
    HasCodeChunk = has_chunk("Code", Chunks),
    InBounds = chunk_table_within_file(Bin, Chunks),
    case HasAtomChunk andalso HasCodeChunk andalso InBounds of
        true ->
            ok;
        false ->
            {error,
                preflight_error("plugin_beam_truncated", #{
                    name_vsn => AppNameVsn, path => Beam, chunks => Chunks
                })}
    end.

%% `beam_lib' uncompresses a compressed beam before it locates the chunks, so
%% for those the reported positions are relative to the uncompressed bytes and
%% can not be compared with the size of the file.
chunk_table_within_file(<<"FOR1", _/binary>> = Bin, Chunks) ->
    FileSize = byte_size(Bin),
    lists:all(fun({_Id, Pos, Size}) -> Pos + Size =< FileSize end, Chunks);
chunk_table_within_file(_Bin, _Chunks) ->
    true.

has_chunk(Id, Chunks) ->
    lists:keymember(Id, 1, Chunks).

%% Compare the embedded module name with the file name without creating an atom
%% for the latter.
check_beam_module_name(Beam, EmbeddedModule, AppNameVsn) ->
    Base = filename:basename(Beam, ".beam"),
    case atom_to_binary(EmbeddedModule, utf8) =:= filename_to_binary(Base) of
        true ->
            ok;
        false ->
            {error,
                preflight_error("plugin_beam_module_name_mismatch", #{
                    name_vsn => AppNameVsn,
                    path => Beam,
                    module => EmbeddedModule,
                    hint =>
                        <<
                            "Rebuild the plugin package: every beam file must contain the module "
                            "named after the file"
                        >>
                })}
    end.

check_declared_module(Beam, Module, DeclaredModules, AppNameVsn) ->
    case lists:member(Module, DeclaredModules) of
        true ->
            ok;
        false ->
            {error,
                preflight_error("plugin_app_module_not_declared", #{
                    name_vsn => AppNameVsn, path => Beam, module => Module
                })}
    end.

%% `erlang:prepare_loading/2' is the front end used by `code:load_file/1' and
%% `code:load_binary/3'.  The prepared code is thrown away: `finish_loading/1' is
%% never called, so this neither loads the module nor runs its `-on_load'.
check_beam_loadable(Beam, Bin, Module, AppNameVsn) ->
    case erlang:prepare_loading(Module, Bin) of
        {error, Reason} ->
            {error,
                preflight_error("plugin_beam_not_loadable", #{
                    name_vsn => AppNameVsn, path => Beam, module => Module, reason => Reason
                })};
        _Prepared ->
            ok
    end.

%% Reject a module which would replace a module that is not this plugin's own.
%% Reject a module which would replace a module that is not this plugin's own.
%%
%% Only loaded code is protected.  A module which is merely resolvable on the
%% code path is not in use, and its beam can be a plugin's own build output:
%% the applications under `plugins/' are built into the same `_build' tree as
%% the release applications, so rejecting them would refuse to install a valid
%% development plugin.  Loading the package's own bytes for such a name is what
%% the loader did before this check existed.
-spec check_ownership(module(), file:filename(), atom(), list(), file:filename(), name_vsn()) ->
    ok | {error, map()}.
check_ownership(Module, EbinDir, AppName, Props, Beam, AppNameVsn) ->
    case code:is_loaded(Module) of
        false ->
            ok;
        {file, preloaded} ->
            {error, beam_conflict(Module, preloaded_module, Beam, AppNameVsn)};
        {file, cover_compiled} ->
            {error, beam_conflict(Module, cover_compiled_module, Beam, AppNameVsn)};
        {file, File} when is_list(File); is_binary(File) ->
            case code:is_sticky(Module) of
                true ->
                    %% kernel / stdlib / compiler
                    {error, beam_conflict(Module, sticky_module, File, AppNameVsn)};
                false ->
                    check_beam_owner(Module, File, EbinDir, AppName, Props, AppNameVsn)
            end;
        {file, Other} ->
            {error, beam_conflict(Module, unknown_load_source, Other, AppNameVsn)}
    end.

check_beam_owner(Module, File, EbinDir, AppName, Props, AppNameVsn) ->
    FileEbin = filename:dirname(filename:absname(File)),
    case FileEbin =:= filename:absname(EbinDir) of
        true ->
            %% The package is reloading its own module.
            ok;
        false ->
            check_foreign_beam_owner(Module, File, FileEbin, EbinDir, AppName, Props, AppNameVsn)
    end.

check_foreign_beam_owner(Module, File, FileEbin, EbinDir, AppName, Props, AppNameVsn) ->
    case plugin_name_of(FileEbin) of
        {ok, PluginName} ->
            check_plugin_beam_owner(
                Module, File, FileEbin, EbinDir, AppName, Props, AppNameVsn, PluginName
            );
        error ->
            check_preloaded_own_app(Module, File, FileEbin, AppName, AppNameVsn)
    end.

%% The file is not below a plugin installation directory.  It can still be one
%% of the package's own applications: `emqx.ct' loads the modules of the project
%% under test from the build tree, so a development plugin finds the modules of
%% its own applications already loaded.  Only an application which is not loaded
%% is accepted -- a loaded application belongs to the release, and its modules
%% must not be replaced.
check_preloaded_own_app(Module, File, FileEbin, AppName, AppNameVsn) ->
    AppLibDir = filename:dirname(FileEbin),
    case app_lib_dir_matches(AppLibDir, AppName) andalso not app_is_loaded(AppName) of
        true ->
            ?SLOG(info, #{
                msg => "plugin_module_reloaded_from_own_preloaded_app",
                module => Module,
                loaded_from => File
            }),
            ok;
        false ->
            check_outside_plugins(Module, File, AppName, AppNameVsn)
    end.

app_lib_dir_matches(AppLibDir, AppName) ->
    Name = atom_to_list(AppName),
    Base = path_to_list(filename:basename(AppLibDir)),
    Base =:= Name orelse lists:prefix(Name ++ "-", Base).

app_is_loaded(AppName) ->
    lists:keymember(AppName, 1, application:loaded_applications()).

check_plugin_beam_owner(Module, File, FileEbin, EbinDir, AppName, Props, AppNameVsn, PluginName) ->
    case plugin_name_of(filename:absname(EbinDir)) of
        %% Another version of the same plugin: on upgrade the modules of the
        %% version being replaced stay in the code server (`unload' only soft
        %% purges).  Keeping the `PluginName' match is what allows it.
        {ok, PluginName} ->
            ?SLOG(info, #{
                msg => "plugin_module_reloaded_from_another_version",
                module => Module,
                loaded_from => File
            }),
            ok;
        _ ->
            check_other_plugin_beam(Module, File, FileEbin, AppName, Props, AppNameVsn)
    end.

check_other_plugin_beam(Module, File, FileEbin, AppName, Props, AppNameVsn) ->
    case filelib:is_regular(File) of
        false ->
            %% The plugin which used to own this module is gone from disk: its
            %% module is a leftover in the code server, and its `.app' can not
            %% be compared any more.  Let the new owner replace it.
            ?SLOG(info, #{
                msg => "plugin_module_reloaded_from_removed_plugin",
                module => Module,
                loaded_from => File
            }),
            ok;
        true ->
            shared_or_conflict(Module, FileEbin, AppName, Props, File, AppNameVsn)
    end.

check_outside_plugins(Module, File, AppName, AppNameVsn) ->
    %% Not a plugin below the configured install directory: an EMQX or OTP
    %% module.  The applications which Elixir plugins share with the release
    %% (`elixir' and `iex') are the exception: the release already loads them.
    case is_protected_app(AppName) andalso is_protected_module(File) of
        true ->
            ?SLOG(info, #{
                msg => "plugin_module_reloaded_from_protected_app",
                module => Module,
                loaded_from => File
            }),
            ok;
        false ->
            case filelib:is_regular(File) of
                false ->
                    %% The file is gone: this is a leftover in the code server
                    %% of a plugin which no longer exists on disk (its directory
                    %% was purged, or it was installed under an install
                    %% directory this configuration does not use any more).
                    %% There is nothing left to protect and no `.app' left to
                    %% compare, so let the new owner replace it.
                    ?SLOG(info, #{
                        msg => "plugin_module_reloaded_from_removed_plugin",
                        module => Module,
                        loaded_from => File
                    }),
                    ok;
                true ->
                    {error, beam_conflict(Module, loaded_outside_plugins, File, AppNameVsn)}
            end
    end.

shared_or_conflict(Module, FileEbin, AppName, Props, File, AppNameVsn) ->
    case is_shared_plugin_app(AppName, Props, FileEbin) of
        true ->
            %% Another plugin installs the very same app spec: it is shared.
            ok;
        false ->
            {error, beam_conflict(Module, loaded_from_other_plugin, File, AppNameVsn)}
    end.

%% Whether a loaded module comes from one of the applications which are shared
%% between the release and Elixir plugins (`<lib>/<app>-<vsn>/ebin' or
%% `<lib>/<app>/ebin'), or from the protocol implementations Elixir consolidates
%% into a `consolidated' directory.
is_protected_module(File) when is_list(File); is_binary(File) ->
    case lists:reverse(filename:split(filename:absname(File))) of
        [_Beam, "ebin", LibDir | _] ->
            lists:any(fun(App) -> is_app_lib_dir(LibDir, App) end, protected_apps());
        [_Beam, "consolidated" | _] ->
            true;
        _ ->
            false
    end;
is_protected_module(_File) ->
    false.

is_app_lib_dir(LibDir, App) ->
    AppName = atom_to_list(App),
    LibName = path_to_list(filename:basename(LibDir)),
    LibName =:= AppName orelse lists:prefix(AppName ++ "-", LibName).

%% The name of the plugin an ebin directory belongs to, with the version
%% stripped.  `<install>/foo-1.0.0/foo-1.0.0/ebin' and
%% `<install>/foo-2.0.0/foo-2.0.0/ebin' both give `foo'.
%%
%% The install directory is configurable, and a module can stay loaded from a
%% plugin installation made under another one (a test work directory, or a
%% previous configuration).  Such a directory is therefore also recognised by
%% its layout `<plugin-dir>/<app-dir>/ebin' when it carries the plugin manifest;
%% OTP and release applications live under a `lib' directory, which has no
%% manifest, so they are not mistaken for plugins.
plugin_name_of(EbinDir) ->
    case plugin_name_in_install_dir(EbinDir) of
        {ok, _} = Ok ->
            Ok;
        error ->
            plugin_name_from_manifest(EbinDir)
    end.

plugin_name_in_install_dir(Dir) ->
    Parts = filename:split(filename:absname(Dir)),
    InstallParts = filename:split(
        filename:absname(path_to_list(emqx_plugins_fs:install_dir()))
    ),
    case lists:prefix(InstallParts, Parts) of
        true ->
            case lists:nthtail(length(InstallParts), Parts) of
                [PluginDir | _] -> plugin_name_from_dir(PluginDir);
                _ -> error
            end;
        false ->
            error
    end.

plugin_name_from_manifest(EbinDir) ->
    %% `<plugin-dir>/<app-dir>/ebin'
    PluginDir = filename:dirname(filename:dirname(filename:absname(EbinDir))),
    case filelib:is_regular(filename:join(PluginDir, "release.json")) of
        true -> plugin_name_from_dir(filename:basename(PluginDir));
        false -> error
    end.

plugin_name_from_dir(PluginDir) ->
    try emqx_plugins_utils:parse_name_vsn(PluginDir) of
        {Name, _Vsn} -> {ok, emqx_plugins_utils:bin(Name)}
    catch
        _:_ -> error
    end.

beam_conflict(Module, Conflict, LoadedFrom, AppNameVsn) ->
    #{
        kind => invalid_package,
        msg => "plugin_beam_load_conflict",
        name_vsn => AppNameVsn,
        module => Module,
        conflict => Conflict,
        loaded_from => LoadedFrom,
        hint =>
            <<
                "A plugin must not replace modules it does not own; rebuild the package so that "
                "its module names do not collide with EMQX or with other plugins"
            >>
    }.

preflight_error(Msg, Fields) ->
    maps:merge(#{kind => invalid_package, msg => Msg}, Fields).

log_preflight_failure(Error) ->
    SubMsg = maps:get(msg, Error, undefined),
    ?SLOG(warning, (maps:without([msg], Error))#{
        msg => "plugin_beam_preflight_failed",
        sub_msg => SubMsg
    }).

filename_to_binary(Name) when is_binary(Name) ->
    Name;
filename_to_binary(Name) when is_list(Name) ->
    try
        list_to_binary(Name)
    catch
        _:_ -> unicode:characters_to_binary(Name)
    end.

ebin_dir(LibDir, AppNameVsn) ->
    filename:join([path_to_list(LibDir), path_to_list(AppNameVsn), "ebin"]).

%% During node boot, plugin apps are started after all EMQX applications
%% (tail of emqx_machine_boot:ensure_apps_started/0), so a plugin may declare
%% any EMQX application as a dependency.
start_app(App) ->
    case run_with_timeout(application, ensure_all_started, [App], 10_000) of
        {ok, {ok, Started}} ->
            case Started =/= [] of
                true -> ?SLOG(debug, #{msg => "started_plugin_apps", apps => Started});
                false -> ok
            end;
        {ok, {error, Reason}} ->
            {error, #{
                msg => "failed_to_start_app",
                app => App,
                reason => Reason
            }};
        {error, timeout} ->
            {error, #{
                msg => "failed_to_start_plugin_app",
                app => App,
                reason => timeout,
                not_running_deps => not_running_deps(App),
                hint =>
                    "every application the plugin declares as a dependency"
                    " (mix.exs for mix-built plugins) must be part of the"
                    " EMQX release or bundled with the plugin package"
            }}
    end.

not_running_deps(App) ->
    case application:get_key(App, applications) of
        {ok, Deps} ->
            Running = [N || {N, _} <- running_apps()],
            [Dep || Dep <- Deps, not lists:member(Dep, Running)];
        undefined ->
            []
    end.

%% On one hand, Elixir plugins might include Elixir itself, when targetting a non-Elixir
%% EMQX release.  If, on the other hand, the EMQX release already includes Elixir, we
%% shouldn't stop Elixir nor IEx.
-ifdef(EMQX_ELIXIR).
is_protected_app(elixir) -> true;
is_protected_app(iex) -> true;
is_protected_app(_) -> false.

protected_apps() -> [elixir, iex].

parse_name_vsn_for_stopping(NameVsn) ->
    {AppName, _AppVsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    case is_protected_app(AppName) of
        true ->
            false;
        false ->
            {true, AppName}
    end.
%% ELSE ifdef(EMQX_ELIXIR)
-else.
is_protected_app(_) -> false.

protected_apps() -> [].

parse_name_vsn_for_stopping(NameVsn) ->
    {AppName, _AppVsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    {true, AppName}.
%% END ifdef(EMQX_ELIXIR)
-endif.

stop_apps(Apps) ->
    RunningApps = running_apps(),
    case do_stop_apps(Apps, [], RunningApps) of
        %% all stopped
        {ok, []} -> {ok, []};
        %% no progress
        {ok, Remain} when Remain =:= Apps -> {ok, Apps};
        %% try again
        {ok, Remain} -> stop_apps(Remain);
        {error, Reason} -> {error, Reason}
    end.

do_stop_apps([], Remain, _AllApps) ->
    {ok, lists:reverse(Remain)};
do_stop_apps([App | Apps], Remain, RunningApps) ->
    case is_needed_by_any(App, RunningApps) of
        true ->
            do_stop_apps(Apps, [App | Remain], RunningApps);
        false ->
            case stop_app(App) of
                ok ->
                    do_stop_apps(Apps, Remain, RunningApps);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

unload_apps([], _RunningApps, _LoadedApps) ->
    ok;
unload_apps([App | Apps], RunningApps, LoadedApps) ->
    _ =
        case app_running_status(App, undefined, RunningApps, LoadedApps) of
            running ->
                ?SLOG(warning, #{msg => "emqx_plugins_cannot_unload_running_app", app => App});
            loaded ->
                ?SLOG(debug, #{msg => "emqx_plugins_unloading_loaded_app", app => App}),
                ok = unload_modules_and_app(App);
            stopped ->
                ?SLOG(debug, #{msg => "emqx_plugins_app_already_unloaded", app => App}),
                ok
        end,
    unload_apps(Apps, RunningApps, LoadedApps).

app_running_status(AppName, AppVsn, RunningApps, LoadedApps) ->
    case lists:keyfind(AppName, 1, LoadedApps) of
        {AppName, LoadedVsn} ->
            case same_app_vsn(AppVsn, LoadedVsn) of
                true -> loaded_app_status(AppName, AppVsn, RunningApps);
                false -> stopped
            end;
        false ->
            stopped
    end.

loaded_app_status(AppName, AppVsn, RunningApps) ->
    case lists:keyfind(AppName, 1, RunningApps) of
        {AppName, RunningVsn} ->
            case same_app_vsn(AppVsn, RunningVsn) of
                true -> running;
                false -> loaded
            end;
        _ ->
            loaded
    end.

stop_app(App) ->
    case application:stop(App) of
        ok ->
            ?SLOG(debug, #{msg => "emqx_plugins_stop_plugin_successfully", app => App}),
            ok;
        {error, {not_started, App}} ->
            ?SLOG(debug, #{msg => "emqx_plugins_plugin_not_started", app => App}),
            ok;
        {error, Reason} ->
            {error, #{msg => "emqx_plugins_failed_to_stop_app", app => App, reason => Reason}}
    end.

unload_modules_and_app(App) ->
    case application:get_key(App, modules) of
        {ok, Modules} ->
            ?SLOG(debug, #{msg => "emqx_plugins_purging_modules", app => App, modules => Modules}),
            lists:foreach(fun code:soft_purge/1, Modules);
        _ ->
            ok
    end,
    Result = application:unload(App),
    ?SLOG(debug, #{msg => "emqx_plugins_unloaded_app", app => App, result => Result}),
    ok.

is_needed_by_any(AppToStop, RunningApps) ->
    lists:any(
        fun({RunningApp, _RunningAppVsn}) ->
            is_needed_by(AppToStop, RunningApp)
        end,
        RunningApps
    ).

is_needed_by(AppToStop, AppToStop) ->
    false;
is_needed_by(AppToStop, RunningApp) ->
    case application:get_key(RunningApp, applications) of
        {ok, Deps} -> lists:member(AppToStop, Deps);
        undefined -> false
    end.

running_apps() ->
    lists:map(
        fun({N, _, V}) ->
            {N, V}
        end,
        application:which_applications(infinity)
    ).

loaded_apps() ->
    lists:map(
        fun({N, _, V}) ->
            {N, V}
        end,
        application:loaded_applications()
    ).

run_with_timeout(Module, Function, Args, Timeout) ->
    Self = self(),
    Fun = fun() ->
        Result = apply(Module, Function, Args),
        Self ! {self(), Result}
    end,
    Pid = spawn(Fun),
    TimerRef = erlang:send_after(Timeout, self(), {timeout, Pid}),
    receive
        {Pid, Result} ->
            _ = erlang:cancel_timer(TimerRef),
            {ok, Result};
        {timeout, Pid} ->
            exit(Pid, kill),
            {error, timeout}
    end.

app_module_name(NameVsn) ->
    {AppName, _} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    case
        emqx_utils:safe_to_existing_atom(
            <<(emqx_plugins_utils:bin(AppName))/binary, "_app">>
        )
    of
        {ok, AppModule} ->
            {ok, AppModule};
        {error, Reason} ->
            {error, {undefined_app_module, AppName, Reason}}
    end.

is_callback_exported(AppModule, FuncName, Arity) ->
    case erlang:function_exported(AppModule, FuncName, Arity) of
        true -> ok;
        false -> {error, {callback_not_exported, AppModule, FuncName, Arity}}
    end.

primary_app_name_vsn(PluginName, Apps) ->
    PluginNameBin = emqx_plugins_utils:bin(PluginName),
    Pred = fun(AppNameVsn) ->
        emqx_plugins_utils:plugin_name(AppNameVsn) =:= PluginNameBin
    end,
    case lists:search(Pred, Apps) of
        {value, PluginAppNameVsn} ->
            emqx_plugins_utils:parse_name_vsn(PluginAppNameVsn);
        false ->
            emqx_plugins_utils:parse_name_vsn(hd(Apps))
    end.

same_app_vsn(undefined, _LoadedVsn) ->
    true;
same_app_vsn(AppVsn, LoadedVsn) ->
    emqx_plugins_utils:bin(AppVsn) =:= emqx_plugins_utils:bin(LoadedVsn).

-ifdef(TEST).

app_running_status_test_() ->
    [
        ?_assertEqual(
            running,
            app_running_status(demo, "1.0.0", [{demo, "1.0.0"}], [{demo, "1.0.0"}])
        ),
        ?_assertEqual(
            loaded,
            app_running_status(demo, "1.0.0", [], [{demo, "1.0.0"}])
        ),
        ?_assertEqual(
            stopped,
            app_running_status(demo, "1.0.0", [{demo, "2.0.0"}], [{demo, "2.0.0"}])
        ),
        ?_assertEqual(
            stopped,
            app_running_status(demo, "1.0.0", [], [{demo, "2.0.0"}])
        ),
        ?_assertEqual(
            {demo, "2.0.0"},
            primary_app_name_vsn(<<"demo">>, [<<"dep-1.0.0">>, <<"demo-2.0.0">>])
        ),
        ?_assertEqual(
            {dep, "1.0.0"},
            primary_app_name_vsn(<<"demo">>, [<<"dep-1.0.0">>])
        )
    ].

-ifdef(EMQX_ELIXIR).
validate_loaded_protected_app_test() ->
    WasLoaded = lists:keymember(iex, 1, loaded_apps()),
    case application:load(iex) of
        ok -> ok;
        {error, {already_loaded, iex}} -> ok
    end,
    try
        ?assertEqual(ok, validate_loaded_plugin_app(iex, "/outside/plugin/package", []))
    after
        case WasLoaded of
            true -> ok;
            false -> application:unload(iex)
        end
    end.
-endif.

-endif.
