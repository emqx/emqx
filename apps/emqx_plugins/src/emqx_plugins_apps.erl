%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_apps).

%% Plugin's app lifecycle
-export([
    stop/1,
    start/2,
    running_status/1
]).

%% Triggering app's callbacks
-export([
    on_config_changed/3
]).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-spec running_status(name_vsn()) -> running | loaded | stopped.
running_status(NameVsn) ->
    {AppName, _AppVsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    case application:get_key(AppName, vsn) of
        {ok, _} ->
            case lists:keyfind(AppName, 1, running_apps()) of
                {AppName, _} -> running;
                _ -> loaded
            end;
        undefined ->
            stopped
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
            ?SLOG(warning, #{
                msg => "unabled_to_stop_plugin_apps",
                apps => Left,
                reason => "running_apps_still_depends_on_this_apps"
            }),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-spec start(emqx_plugins_info:t(), file:filename()) -> ok | {error, term()}.
start(#{rel_apps := Apps}, LibDir) ->
    RunningApps = running_apps(),
    %% load plugin apps and beam code
    try
        AppNames =
            lists:map(
                fun(AppNameVsn) ->
                    {AppName, AppVsn} = emqx_plugins_utils:parse_name_vsn(AppNameVsn),
                    EbinDir = filename:join([LibDir, AppNameVsn, "ebin"]),
                    case load_plugin_app(AppName, AppVsn, EbinDir, RunningApps) of
                        ok -> AppName;
                        {error, Reason} -> throw(Reason)
                    end
                end,
                Apps
            ),
        ok = lists:foreach(
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

%% @doc Call plugin's callback on_config_changed/2
on_config_changed(NameVsn, OldConf, NewConf) ->
    FuncName = on_config_changed,
    maybe
        {ok, PluginAppModule} ?= app_module_name(NameVsn),
        ok ?= is_callback_exported(PluginAppModule, FuncName, 2),
        try erlang:apply(PluginAppModule, FuncName, [OldConf, NewConf]) of
            _ -> ok
        catch
            Class:CatchReason:Stacktrace ->
                ?SLOG(error, #{
                    msg => "failed_to_call_on_config_changed",
                    exception => Class,
                    reason => CatchReason,
                    stacktrace => Stacktrace
                }),
                ok
        end
    else
        {error, Reason} ->
            ?SLOG(info, #{msg => "failed_to_call_on_config_changed", reason => Reason});
        _ ->
            ok
    end.

load_plugin_app(AppName, AppVsn, Ebin, RunningApps) ->
    case lists:keyfind(AppName, 1, RunningApps) of
        false ->
            do_load_plugin_app(AppName, Ebin);
        {_, Vsn} ->
            case bin(Vsn) =:= bin(AppVsn) of
                true ->
                    %% already started on the exact version
                    ok;
                false ->
                    %% running but a different version
                    ?SLOG(warning, #{
                        msg => "plugin_app_already_running",
                        name => AppName,
                        running_vsn => Vsn,
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
        ok ?= application:load(AppName)
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
        {error, Reason} ->
            {error, #{
                msg => "failed_to_start_plugin_app",
                app => App,
                reason => Reason
            }}
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

stop_app(App) ->
    case application:stop(App) of
        ok ->
            ?SLOG(debug, #{msg => "stop_plugin_successfully", app => App}),
            ok = unload_module_and_app(App);
        {error, {not_started, App}} ->
            ?SLOG(debug, #{msg => "plugin_not_started", app => App}),
            ok = unload_module_and_app(App);
        {error, Reason} ->
            {error, #{msg => "failed_to_stop_app", app => App, reason => Reason}}
    end.

unload_module_and_app(App) ->
    case application:get_key(App, modules) of
        {ok, Modules} ->
            lists:foreach(fun code:soft_purge/1, Modules);
        _ ->
            ok
    end,
    _ = application:unload(App),
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
    case emqx_utils:safe_to_existing_atom(<<(bin(AppName))/binary, "_app">>) of
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

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.
