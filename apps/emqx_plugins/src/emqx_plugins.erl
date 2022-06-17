%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    ensure_installed/1,
    ensure_uninstalled/1,
    ensure_enabled/1,
    ensure_enabled/2,
    ensure_disabled/1,
    purge/1,
    delete_package/1
]).

-export([
    ensure_started/0,
    ensure_started/1,
    ensure_stopped/0,
    ensure_stopped/1,
    restart/1,
    list/0,
    describe/1,
    parse_name_vsn/1
]).

-export([
    get_config/2,
    put_config/2
]).

%% internal
-export([do_ensure_started/1]).
-export([
    install_dir/0
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_plugins.hrl").

%% "my_plugin-0.1.0"
-type name_vsn() :: binary() | string().
%% the parse result of the JSON info file
-type plugin() :: map().
-type position() :: no_move | front | rear | {before, name_vsn()} | {behind, name_vsn()}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Describe a plugin.
-spec describe(name_vsn()) -> {ok, plugin()} | {error, any()}.
describe(NameVsn) -> read_plugin(NameVsn, #{fill_readme => true}).

%% @doc Install a .tar.gz package placed in install_dir.
-spec ensure_installed(name_vsn()) -> ok | {error, any()}.
ensure_installed(NameVsn) ->
    case read_plugin(NameVsn, #{}) of
        {ok, _} ->
            ok;
        {error, _} ->
            ok = purge(NameVsn),
            do_ensure_installed(NameVsn)
    end.

do_ensure_installed(NameVsn) ->
    TarGz = pkg_file(NameVsn),
    case erl_tar:extract(TarGz, [{cwd, install_dir()}, compressed]) of
        ok ->
            case read_plugin(NameVsn, #{}) of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, Reason#{msg => "failed_to_read_after_install"}),
                    _ = ensure_uninstalled(NameVsn),
                    {error, Reason}
            end;
        {error, {_, enoent}} ->
            {error, #{
                reason => "failed_to_extract_plugin_package",
                path => TarGz,
                return => not_found
            }};
        {error, Reason} ->
            {error, #{
                reason => "bad_plugin_package",
                path => TarGz,
                return => Reason
            }}
    end.

%% @doc Ensure files and directories for the given plugin are delete.
%% If a plugin is running, or enabled, error is returned.
-spec ensure_uninstalled(name_vsn()) -> ok | {error, any()}.
ensure_uninstalled(NameVsn) ->
    case read_plugin(NameVsn, #{}) of
        {ok, #{running_status := RunningSt}} when RunningSt =/= stopped ->
            {error, #{
                reason => "bad_plugin_running_status",
                hint => "stop_the_plugin_first"
            }};
        {ok, #{config_status := enabled}} ->
            {error, #{
                reason => "bad_plugin_config_status",
                hint => "disable_the_plugin_first"
            }};
        _ ->
            purge(NameVsn),
            ensure_delete(NameVsn)
    end.

ensure_delete(NameVsn0) ->
    NameVsn = bin(NameVsn0),
    List = configured(),
    put_configured(lists:filter(fun(#{name_vsn := N1}) -> bin(N1) =/= NameVsn end, List)),
    ok.

%% @doc Ensure a plugin is enabled to the end of the plugins list.
-spec ensure_enabled(name_vsn()) -> ok | {error, any()}.
ensure_enabled(NameVsn) ->
    ensure_enabled(NameVsn, no_move).

%% @doc Ensure a plugin is enabled at the given position of the plugin list.
-spec ensure_enabled(name_vsn(), position()) -> ok | {error, any()}.
ensure_enabled(NameVsn, Position) ->
    ensure_state(NameVsn, Position, true).

%% @doc Ensure a plugin is disabled.
-spec ensure_disabled(name_vsn()) -> ok | {error, any()}.
ensure_disabled(NameVsn) ->
    ensure_state(NameVsn, no_move, false).

ensure_state(NameVsn, Position, State) when is_binary(NameVsn) ->
    ensure_state(binary_to_list(NameVsn), Position, State);
ensure_state(NameVsn, Position, State) ->
    case read_plugin(NameVsn, #{}) of
        {ok, _} ->
            Item = #{
                name_vsn => NameVsn,
                enable => State
            },
            tryit("ensure_state", fun() -> ensure_configured(Item, Position) end);
        {error, Reason} ->
            {error, Reason}
    end.

ensure_configured(#{name_vsn := NameVsn} = Item, Position) ->
    Configured = configured(),
    SplitFun = fun(#{name_vsn := Nv}) -> bin(Nv) =/= bin(NameVsn) end,
    {Front, Rear} = lists:splitwith(SplitFun, Configured),
    NewConfigured =
        case Rear of
            [_ | More] when Position =:= no_move ->
                Front ++ [Item | More];
            [_ | More] ->
                add_new_configured(Front ++ More, Position, Item);
            [] ->
                add_new_configured(Configured, Position, Item)
        end,
    ok = put_configured(NewConfigured).

add_new_configured(Configured, no_move, Item) ->
    %% default to rear
    add_new_configured(Configured, rear, Item);
add_new_configured(Configured, front, Item) ->
    [Item | Configured];
add_new_configured(Configured, rear, Item) ->
    Configured ++ [Item];
add_new_configured(Configured, {Action, NameVsn}, Item) ->
    SplitFun = fun(#{name_vsn := Nv}) -> bin(Nv) =/= bin(NameVsn) end,
    {Front, Rear} = lists:splitwith(SplitFun, Configured),
    Rear =:= [] andalso
        throw(#{
            error => "position_anchor_plugin_not_configured",
            hint => "maybe_install_and_configure",
            name_vsn => NameVsn
        }),
    case Action of
        before ->
            Front ++ [Item | Rear];
        behind ->
            [Anchor | Rear0] = Rear,
            Front ++ [Anchor, Item | Rear0]
    end.

%% @doc Delete the package file.
-spec delete_package(name_vsn()) -> ok.
delete_package(NameVsn) ->
    File = pkg_file(NameVsn),
    case file:delete(File) of
        ok ->
            ?SLOG(info, #{msg => "purged_plugin_dir", path => File}),
            ok;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_delete_package_file",
                path => File,
                reason => Reason
            }),
            {error, Reason}
    end.

%% @doc Delete extracted dir
%% In case one lib is shared by multiple plugins.
%% it might be the case that purging one plugin's install dir
%% will cause deletion of loaded beams.
%% It should not be a problem, because shared lib should
%% reside in all the plugin install dirs.
-spec purge(name_vsn()) -> ok.
purge(NameVsn) ->
    Dir = dir(NameVsn),
    case file:del_dir_r(Dir) of
        ok ->
            ?SLOG(info, #{msg => "purged_plugin_dir", dir => Dir});
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_purge_plugin_dir",
                dir => Dir,
                reason => Reason
            }),
            {error, Reason}
    end.

%% @doc Start all configured plugins are started.
-spec ensure_started() -> ok.
ensure_started() ->
    ok = for_plugins(fun ?MODULE:do_ensure_started/1).

%% @doc Start a plugin from Management API or CLI.
%% the input is a <name>-<vsn> string.
-spec ensure_started(name_vsn()) -> ok | {error, term()}.
ensure_started(NameVsn) ->
    case do_ensure_started(NameVsn) of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(alert, #{
                msg => "failed_to_start_plugin",
                reason => Reason
            }),
            {error, Reason}
    end.

%% @doc Stop all plugins before broker stops.
-spec ensure_stopped() -> ok.
ensure_stopped() ->
    for_plugins(fun ?MODULE:ensure_stopped/1).

%% @doc Stop a plugin from Management API or CLI.
-spec ensure_stopped(name_vsn()) -> ok | {error, term()}.
ensure_stopped(NameVsn) ->
    tryit(
        "stop_plugin",
        fun() ->
            Plugin = do_read_plugin(NameVsn),
            ensure_apps_stopped(Plugin)
        end
    ).

%% @doc Stop and then start the plugin.
restart(NameVsn) ->
    case ensure_stopped(NameVsn) of
        ok -> ensure_started(NameVsn);
        {error, Reason} -> {error, Reason}
    end.

%% @doc List all installed plugins.
%% Including the ones that are installed, but not enabled in config.
-spec list() -> [plugin()].
list() ->
    Pattern = filename:join([install_dir(), "*", "release.json"]),
    All = lists:filtermap(
        fun(JsonFile) ->
            case read_plugin({file, JsonFile}, #{}) of
                {ok, Info} ->
                    {true, Info};
                {error, Reason} ->
                    ?SLOG(warning, Reason),
                    false
            end
        end,
        filelib:wildcard(Pattern)
    ),
    list(configured(), All).

%% Make sure configured ones are ordered in front.
list([], All) ->
    All;
list([#{name_vsn := NameVsn} | Rest], All) ->
    SplitF = fun(#{<<"name">> := Name, <<"rel_vsn">> := Vsn}) ->
        bin([Name, "-", Vsn]) =/= bin(NameVsn)
    end,
    case lists:splitwith(SplitF, All) of
        {_, []} ->
            ?SLOG(warning, #{
                msg => "configured_plugin_not_installed",
                name_vsn => NameVsn
            }),
            list(Rest, All);
        {Front, [I | Rear]} ->
            [I | list(Rest, Front ++ Rear)]
    end.

do_ensure_started(NameVsn) ->
    tryit(
        "start_plugins",
        fun() ->
            Plugin = do_read_plugin(NameVsn),
            ok = load_code_start_apps(NameVsn, Plugin)
        end
    ).

%% try the function, catch 'throw' exceptions as normal 'error' return
%% other exceptions with stacktrace logged.
tryit(WhichOp, F) ->
    try
        F()
    catch
        throw:Reason ->
            %% thrown exceptions are known errors
            %% translate to a return value without stacktrace
            {error, Reason};
        error:Reason:Stacktrace ->
            %% unexpected errors, log stacktrace
            ?SLOG(warning, #{
                msg => "plugin_op_failed",
                which_op => WhichOp,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            {error, {failed, WhichOp}}
    end.

%% read plugin info from the JSON file
%% returns {ok, Info} or {error, Reason}
read_plugin(NameVsn, Options) ->
    tryit(
        "read_plugin_info",
        fun() -> {ok, do_read_plugin(NameVsn, Options)} end
    ).

do_read_plugin(Plugin) -> do_read_plugin(Plugin, #{}).

do_read_plugin({file, InfoFile}, Options) ->
    [_, NameVsn | _] = lists:reverse(filename:split(InfoFile)),
    case hocon:load(InfoFile, #{format => richmap}) of
        {ok, RichMap} ->
            Info0 = check_plugin(hocon_maps:ensure_plain(RichMap), NameVsn, InfoFile),
            Info1 = plugins_readme(NameVsn, Options, Info0),
            plugin_status(NameVsn, Info1);
        {error, Reason} ->
            throw(#{
                error => "bad_info_file",
                path => InfoFile,
                return => Reason
            })
    end;
do_read_plugin(NameVsn, Options) ->
    do_read_plugin({file, info_file(NameVsn)}, Options).

plugins_readme(NameVsn, #{fill_readme := true}, Info) ->
    case file:read_file(readme_file(NameVsn)) of
        {ok, Bin} -> Info#{readme => Bin};
        _ -> Info#{readme => <<>>}
    end;
plugins_readme(_NameVsn, _Options, Info) ->
    Info.

plugin_status(NameVsn, Info) ->
    {ok, AppName, _AppVsn} = parse_name_vsn(NameVsn),
    RunningSt =
        case application:get_key(AppName, vsn) of
            {ok, _} ->
                case lists:keyfind(AppName, 1, running_apps()) of
                    {AppName, _} -> running;
                    _ -> loaded
                end;
            undefined ->
                stopped
        end,
    Configured = lists:filtermap(
        fun(#{name_vsn := Nv, enable := St}) ->
            case bin(Nv) =:= bin(NameVsn) of
                true -> {true, St};
                false -> false
            end
        end,
        configured()
    ),
    ConfSt =
        case Configured of
            [] -> not_configured;
            [true] -> enabled;
            [false] -> disabled
        end,
    Info#{
        running_status => RunningSt,
        config_status => ConfSt
    }.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.

check_plugin(
    #{
        <<"name">> := Name,
        <<"rel_vsn">> := Vsn,
        <<"rel_apps">> := Apps,
        <<"description">> := _
    } = Info,
    NameVsn,
    File
) ->
    case bin(NameVsn) =:= bin([Name, "-", Vsn]) of
        true ->
            try
                %% assert
                [_ | _] = Apps,
                %% validate if the list is all <app>-<vsn> strings
                lists:foreach(fun(App) -> {ok, _, _} = parse_name_vsn(App) end, Apps)
            catch
                _:_ ->
                    throw(#{
                        error => "bad_rel_apps",
                        rel_apps => Apps,
                        hint => "A non-empty string list of app_name-app_vsn format"
                    })
            end,
            Info;
        false ->
            throw(#{
                error => "name_vsn_mismatch",
                name_vsn => NameVsn,
                path => File,
                name => Name,
                rel_vsn => Vsn
            })
    end;
check_plugin(_What, NameVsn, File) ->
    throw(#{
        error => "bad_info_file_content",
        mandatory_fields => [rel_vsn, name, rel_apps, description],
        name_vsn => NameVsn,
        path => File
    }).

load_code_start_apps(RelNameVsn, #{<<"rel_apps">> := Apps}) ->
    LibDir = filename:join([install_dir(), RelNameVsn]),
    RunningApps = running_apps(),
    %% load plugin apps and beam code
    AppNames =
        lists:map(
            fun(AppNameVsn) ->
                {ok, AppName, AppVsn} = parse_name_vsn(AppNameVsn),
                EbinDir = filename:join([LibDir, AppNameVsn, "ebin"]),
                ok = load_plugin_app(AppName, AppVsn, EbinDir, RunningApps),
                AppName
            end,
            Apps
        ),
    lists:foreach(fun start_app/1, AppNames).

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
                    })
            end
    end.

do_load_plugin_app(AppName, Ebin) when is_binary(Ebin) ->
    do_load_plugin_app(AppName, binary_to_list(Ebin));
do_load_plugin_app(AppName, Ebin) ->
    _ = code:add_patha(Ebin),
    Modules = filelib:wildcard(filename:join([Ebin, "*.beam"])),
    lists:foreach(
        fun(BeamFile) ->
            Module = list_to_atom(filename:basename(BeamFile, ".beam")),
            case code:load_file(Module) of
                {module, _} ->
                    ok;
                {error, Reason} ->
                    throw(#{
                        error => "failed_to_load_plugin_beam",
                        path => BeamFile,
                        reason => Reason
                    })
            end
        end,
        Modules
    ),
    case application:load(AppName) of
        ok ->
            ok;
        {error, {already_loaded, _}} ->
            ok;
        {error, Reason} ->
            throw(#{
                error => "failed_to_load_plugin_app",
                name => AppName,
                reason => Reason
            })
    end.

start_app(App) ->
    case application:ensure_all_started(App) of
        {ok, Started} ->
            case Started =/= [] of
                true -> ?SLOG(debug, #{msg => "started_plugin_apps", apps => Started});
                false -> ok
            end,
            ?SLOG(debug, #{msg => "started_plugin_app", app => App}),
            ok;
        {error, {ErrApp, Reason}} ->
            throw(#{
                error => "failed_to_start_plugin_app",
                app => App,
                err_app => ErrApp,
                reason => Reason
            })
    end.

%% Stop all apps installed by the plugin package,
%% but not the ones shared with others.
ensure_apps_stopped(#{<<"rel_apps">> := Apps}) ->
    %% load plugin apps and beam code
    AppsToStop =
        lists:map(
            fun(NameVsn) ->
                {ok, AppName, _AppVsn} = parse_name_vsn(NameVsn),
                AppName
            end,
            Apps
        ),
    case tryit("stop_apps", fun() -> stop_apps(AppsToStop) end) of
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

stop_apps(Apps) ->
    RunningApps = running_apps(),
    case do_stop_apps(Apps, [], RunningApps) of
        %% all stopped
        {ok, []} -> {ok, []};
        %% no progress
        {ok, Remain} when Remain =:= Apps -> {ok, Apps};
        %% try again
        {ok, Remain} -> stop_apps(Remain)
    end.

do_stop_apps([], Remain, _AllApps) ->
    {ok, lists:reverse(Remain)};
do_stop_apps([App | Apps], Remain, RunningApps) ->
    case is_needed_by_any(App, RunningApps) of
        true ->
            do_stop_apps(Apps, [App | Remain], RunningApps);
        false ->
            ok = stop_app(App),
            do_stop_apps(Apps, Remain, RunningApps)
    end.

stop_app(App) ->
    case application:stop(App) of
        ok ->
            ?SLOG(debug, #{msg => "stop_plugin_successfully", app => App}),
            ok = unload_moudle_and_app(App);
        {error, {not_started, App}} ->
            ?SLOG(debug, #{msg => "plugin_not_started", app => App}),
            ok = unload_moudle_and_app(App);
        {error, Reason} ->
            throw(#{error => "failed_to_stop_app", app => App, reason => Reason})
    end.

unload_moudle_and_app(App) ->
    case application:get_key(App, modules) of
        {ok, Modules} -> lists:foreach(fun code:soft_purge/1, Modules);
        _ -> ok
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

put_config(Key, Value) when is_atom(Key) ->
    put_config([Key], Value);
put_config(Path, Values) when is_list(Path) ->
    Opts = #{rawconf_with_defaults => true, override_to => cluster},
    %% Already in cluster_rpc, don't use emqx_conf:update, dead calls
    case emqx:update_config([?CONF_ROOT | Path], bin_key(Values), Opts) of
        {ok, _} -> ok;
        Error -> Error
    end.

bin_key(Map) when is_map(Map) ->
    maps:fold(fun(K, V, Acc) -> Acc#{bin(K) => V} end, #{}, Map);
bin_key(List = [#{} | _]) ->
    lists:map(fun(M) -> bin_key(M) end, List);
bin_key(Term) ->
    Term.

get_config(Key, Default) when is_atom(Key) ->
    get_config([Key], Default);
get_config(Path, Default) ->
    emqx_conf:get([?CONF_ROOT | Path], Default).

install_dir() -> get_config(install_dir, "").

put_configured(Configured) ->
    ok = put_config(states, bin_key(Configured)).

configured() ->
    get_config(states, []).

for_plugins(ActionFun) ->
    case lists:flatmap(fun(I) -> for_plugin(I, ActionFun) end, configured()) of
        [] -> ok;
        Errors -> erlang:error(#{function => ActionFun, errors => Errors})
    end.

for_plugin(#{name_vsn := NameVsn, enable := true}, Fun) ->
    case Fun(NameVsn) of
        ok -> [];
        {error, Reason} -> [{NameVsn, Reason}]
    end;
for_plugin(#{name_vsn := NameVsn, enable := false}, _Fun) ->
    ?SLOG(debug, #{
        msg => "plugin_disabled",
        name_vsn => NameVsn
    }),
    [].

parse_name_vsn(NameVsn) when is_binary(NameVsn) ->
    parse_name_vsn(binary_to_list(NameVsn));
parse_name_vsn(NameVsn) when is_list(NameVsn) ->
    case lists:splitwith(fun(X) -> X =/= $- end, NameVsn) of
        {AppName, [$- | Vsn]} -> {ok, list_to_atom(AppName), Vsn};
        _ -> {error, "bad_name_vsn"}
    end.

pkg_file(NameVsn) ->
    filename:join([install_dir(), bin([NameVsn, ".tar.gz"])]).

dir(NameVsn) ->
    filename:join([install_dir(), NameVsn]).

info_file(NameVsn) ->
    filename:join([dir(NameVsn), "release.json"]).

readme_file(NameVsn) ->
    filename:join([dir(NameVsn), "README.md"]).

running_apps() ->
    lists:map(
        fun({N, _, V}) ->
            {N, V}
        end,
        application:which_applications(infinity)
    ).
