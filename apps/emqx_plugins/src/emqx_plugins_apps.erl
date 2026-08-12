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
    running_status/1
]).

%% Triggering app's callbacks
-export([
    on_config_changed/3,
    on_health_check/2,
    on_handle_api_call/2
]).

-type health_check_options() :: #{}.

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
    AppsToStop = lists:filtermap(fun parse_name_vsn_for_stopping/1, Apps),
    case stop_apps(AppsToStop) of
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

-spec load(emqx_plugins_info:t(), file:filename()) -> ok | {error, term()}.
load(#{rel_apps := Apps}, LibDir) ->
    LoadedApps = loaded_apps(),
    %% load plugin apps and beam code
    try
        lists:foreach(
            fun(AppNameVsn) ->
                {AppName, AppVsn} = emqx_plugins_utils:parse_name_vsn(AppNameVsn),
                EbinDir = filename:join([LibDir, AppNameVsn, "ebin"]),
                case load_plugin_app(AppName, AppVsn, EbinDir, LoadedApps) of
                    ok -> ok;
                    {error, Reason} -> throw(Reason)
                end
            end,
            Apps
        )
    catch
        throw:Reason ->
            {error, Reason}
    end.

-spec validate(emqx_plugins_info:t(), file:filename()) -> ok | {error, map()}.
validate(#{rel_apps := Apps}, LibDir) ->
    lists:foldl(fun(App, Acc) -> validate_plugin_app(App, LibDir, Acc) end, ok, Apps).

-spec unload(emqx_plugins_info:t()) -> ok | {error, term()}.
unload(#{rel_apps := Apps}) ->
    RunningApps = running_apps(),
    LoadedApps = loaded_apps(),
    AppsForUnload = lists:filtermap(fun parse_name_vsn_for_stopping/1, Apps),
    ?SLOG(info, #{
        msg => "emqx_plugins_unloading_apps",
        apps => AppsForUnload
    }),
    unload_apps(AppsForUnload, RunningApps, LoadedApps).

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

load_plugin_app(AppName, AppVsn, Ebin, LoadedApps) ->
    case lists:keyfind(AppName, 1, LoadedApps) of
        false ->
            do_load_plugin_app(AppName, Ebin);
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
                    }),
                    ok
            end
    end.

do_load_plugin_app(AppName, Ebin) when is_binary(Ebin) ->
    do_load_plugin_app(AppName, binary_to_list(Ebin));
do_load_plugin_app(AppName, Ebin) ->
    _ = code:add_patha(Ebin),
    Modules = filelib:wildcard(filename:join([Ebin, "*.beam"])),
    maybe
        ok ?= load_modules(Modules),
        {ok, AppSpec} ?= read_app_spec(AppName, Ebin),
        ok ?= application:load(drop_self_dep(AppSpec))
    else
        {error, {already_loaded, _}} ->
            ok;
        {error, Reason} ->
            {error, #{
                msg => "failed_to_load_plugin_app",
                name => AppName,
                reason => Reason
            }}
    end.

read_app_spec(AppName, Ebin) ->
    AppFile = filename:join(Ebin, atom_to_list(AppName) ++ ".app"),
    case file:consult(AppFile) of
        {ok, [{application, AppName, Props}]} ->
            {ok, {application, AppName, Props}};
        {ok, Other} ->
            {error, {bad_app_file, AppFile, Other}};
        {error, Reason} ->
            {error, {bad_app_file, AppFile, Reason}}
    end.

%% Plugin apps are started from within the emqx_plugins application's own
%% start/2. A plugin that declares emqx_plugins in its applications list makes
%% ensure_all_started/1 wait for emqx_plugins to finish starting, which cannot
%% happen until the plugin start returns: plugin start times out on every node
%% boot. The dependency is satisfied by construction (emqx_plugins is running
%% or starting whenever a plugin is started), so drop it from the app spec.
drop_self_dep({application, AppName, Props} = AppSpec) ->
    Deps = proplists:get_value(applications, Props, []),
    case lists:member(emqx_plugins, Deps) of
        true ->
            ?SLOG(warning, #{
                msg => "plugin_app_declares_emqx_plugins_dependency",
                name => AppName,
                hint =>
                    "remove emqx_plugins from the applications list"
                    " in the plugin app's .app.src file"
            }),
            Deps1 = {applications, lists:delete(emqx_plugins, Deps)},
            {application, AppName, lists:keyreplace(applications, 1, Props, Deps1)};
        false ->
            AppSpec
    end.

load_modules([]) ->
    ok;
load_modules([BeamFile | Modules]) ->
    Module = list_to_atom(filename:basename(BeamFile, ".beam")),
    _ = code:purge(Module),
    case code:load_file(Module) of
        {module, _} ->
            load_modules(Modules);
        {error, Reason} ->
            {error, #{msg => "failed_to_load_plugin_beam", path => BeamFile, reason => Reason}}
    end.

%% Plugin apps are started while the emqx_plugins application itself is
%% starting during node boot. Applications a plugin declares in its .app.src
%% must be running by then; EMQX apps that start after emqx_plugins (for
%% example emqx_management) cannot be declared as plugin dependencies.
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
                    "all applications a plugin declares in its .app.src"
                    " must be running before plugins start during node boot"
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
