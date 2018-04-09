%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-include("emqx.hrl").

-export([init/0]).

-export([load/0, unload/0]).

-export([load/1, unload/1]).

-export([list/0]).

-export([load_expand_plugin/1]).

%% @doc Init plugins' config
-spec(init() -> ok).
init() ->
    case emqx_conf:get_env(plugins_etc_dir) of
        {ok, PluginsEtc} ->
            CfgFiles = [filename:join(PluginsEtc, File) ||
                          File <- filelib:wildcard("*.config", PluginsEtc)],
            lists:foreach(fun init_config/1, CfgFiles);
        undefined ->
            ok
    end.

init_config(CfgFile) ->
    {ok, [AppsEnv]} = file:consult(CfgFile),
    lists:foreach(fun({AppName, Envs}) ->
                      [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
                  end, AppsEnv).

%% @doc Load all plugins when the broker started.
-spec(load() -> list() | {error, term()}).
load() ->
    load_expand_plugins(),
    case emqx_conf:get_env(plugins_loaded_file) of
        {ok, File} ->
            ensure_file(File),
            with_loaded_file(File, fun(Names) -> load_plugins(Names, false) end);
        undefined ->
            %% No plugins available
            ignore
    end.

load_expand_plugins() ->
    case emqx_conf:get_env(expand_plugins_dir) of
        {ok, Dir} ->
            PluginsDir = filelib:wildcard("*", Dir),
            lists:foreach(fun(PluginDir) ->
                case filelib:is_dir(Dir ++ PluginDir) of
                    true  -> load_expand_plugin(Dir ++ PluginDir);
                    false -> ok
                end
            end, PluginsDir);
        _ -> ok
    end.

load_expand_plugin(PluginDir) ->
    init_expand_plugin_config(PluginDir),
    Ebin = PluginDir ++ "/ebin",
    code:add_patha(Ebin),
    Modules = filelib:wildcard(Ebin ++ "/*.beam"),
    lists:foreach(fun(Mod) ->
        Module = list_to_atom(filename:basename(Mod, ".beam")),
        code:load_file(Module)
    end, Modules),
    case filelib:wildcard(Ebin ++ "/*.app") of
        [App|_] -> application:load(list_to_atom(filename:basename(App, ".app")));
        _ -> emqx_log:error("App file cannot be found."),
             {error, load_app_fail}
    end.

init_expand_plugin_config(PluginDir) ->
    Priv = PluginDir ++ "/priv",
    Etc  = PluginDir ++ "/etc",
    Schema = filelib:wildcard(Priv ++ "/*.schema"),
    Conf = case filelib:wildcard(Etc ++ "/*.conf") of
        [] -> [];
        [Conf1] -> cuttlefish_conf:file(Conf1)
    end,
    AppsEnv = cuttlefish_generator:map(cuttlefish_schema:files(Schema), Conf),
    lists:foreach(fun({AppName, Envs}) ->
        [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
    end, AppsEnv).

get_expand_plugin_config() ->
    case emqx_conf:get_env(expand_plugins_dir) of
        {ok, Dir} ->
            PluginsDir = filelib:wildcard("*", Dir),
            lists:foldl(fun(PluginDir, Acc) ->
                case filelib:is_dir(Dir ++ PluginDir) of
                    true  ->
                        Etc  = Dir ++ PluginDir ++ "/etc",
                        case filelib:wildcard("*.{conf,config}", Etc) of
                            [] -> Acc;
                            [Conf] -> [Conf | Acc]
                        end;
                    false ->
                        Acc
                end
            end, [], PluginsDir);
        _ -> ok
    end.


ensure_file(File) ->
    case filelib:is_file(File) of false -> write_loaded([]); true -> ok end.

with_loaded_file(File, SuccFun) ->
    case read_loaded(File) of
        {ok, Names} ->
            SuccFun(Names);
        {error, Error} ->
            emqx_log:error("[Plugins] Failed to read: ~p, error: ~p", [File, Error]),
            {error, Error}
    end.

load_plugins(Names, Persistent) ->
    Plugins = list(), NotFound = Names -- names(Plugins),
    case NotFound of
        []       -> ok;
        NotFound -> emqx_log:error("[Plugins] Cannot find plugins: ~p", [NotFound])
    end,
    NeedToLoad = Names -- NotFound -- names(started_app),
    [load_plugin(find_plugin(Name, Plugins), Persistent) || Name <- NeedToLoad].

%% @doc Unload all plugins before broker stopped.
-spec(unload() -> list() | {error, term()}).
unload() ->
    case emqx_conf:get_env(plugins_loaded_file) of
        {ok, File} ->
            with_loaded_file(File, fun stop_plugins/1);
        undefined ->
            ignore
    end.

%% stop plugins
stop_plugins(Names) ->
    [stop_app(App) || App <- Names].

%% @doc List all available plugins
-spec(list() -> [plugin()]).
list() ->
    case emqx_conf:get_env(plugins_etc_dir) of
        {ok, PluginsEtc} ->
            CfgFiles = filelib:wildcard("*.{conf,config}", PluginsEtc) ++ get_expand_plugin_config(),
            Plugins = [plugin(CfgFile) || CfgFile <- CfgFiles],
            StartedApps = names(started_app),
            lists:map(fun(Plugin = #plugin{name = Name}) ->
                          case lists:member(Name, StartedApps) of
                              true  -> Plugin#plugin{active = true};
                              false -> Plugin
                          end
                      end, Plugins);
        undefined ->
            []
    end.

plugin(CfgFile) ->
    AppName = app_name(CfgFile),
    {ok, Attrs} = application:get_all_key(AppName),
    Ver = proplists:get_value(vsn, Attrs, "0"),
    Descr = proplists:get_value(description, Attrs, ""),
    #plugin{name = AppName, version = Ver, descr = Descr}.

%% @doc Load a Plugin
-spec(load(atom()) -> ok | {error, term()}).
load(PluginName) when is_atom(PluginName) ->
    case lists:member(PluginName, names(started_app)) of
        true ->
            emqx_log:error("[Plugins] Plugin ~s is already started", [PluginName]),
            {error, already_started};
        false ->
            case find_plugin(PluginName) of
                false ->
                    emqx_log:error("[Plugins] Plugin ~s not found", [PluginName]),
                    {error, not_found};
                Plugin ->
                    load_plugin(Plugin, true)
            end
    end.

load_plugin(#plugin{name = Name}, Persistent) ->
    case load_app(Name) of
        ok ->
            start_app(Name, fun(App) -> plugin_loaded(App, Persistent) end);
        {error, Error} ->
            {error, Error}
    end.

load_app(App) ->
    case application:load(App) of
        ok ->
            ok;
        {error, {already_loaded, App}} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

start_app(App, SuccFun) ->
    case application:ensure_all_started(App) of
        {ok, Started} ->
            emqx_log:info("Started Apps: ~p", [Started]),
            emqx_log:info("Load plugin ~s successfully", [App]),
            SuccFun(App),
            {ok, Started};
        {error, {ErrApp, Reason}} ->
            emqx_log:error("Load plugin ~s error, cannot start app ~s for ~p", [App, ErrApp, Reason]),
            {error, {ErrApp, Reason}}
    end.

find_plugin(Name) ->
    find_plugin(Name, list()).

find_plugin(Name, Plugins) ->
    lists:keyfind(Name, 2, Plugins). 

%% @doc UnLoad a Plugin
-spec(unload(atom()) -> ok | {error, term()}).
unload(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, names(started_app)), lists:member(PluginName, names(plugin))} of
        {true, true} ->
            unload_plugin(PluginName, true);
        {false, _} ->
            emqx_log:error("Plugin ~s is not started", [PluginName]),
            {error, not_started};
        {true, false} ->
            emqx_log:error("~s is not a plugin, cannot unload it", [PluginName]),
            {error, not_found}
    end.

unload_plugin(App, Persistent) ->
    case stop_app(App) of
        ok ->
            plugin_unloaded(App, Persistent), ok;
        {error, Reason} ->
            {error, Reason}
    end.
    
stop_app(App) ->
    case application:stop(App) of
        ok ->
            emqx_log:info("Stop plugin ~s successfully", [App]), ok;
        {error, {not_started, App}} ->
            emqx_log:error("Plugin ~s is not started", [App]), ok;
        {error, Reason} ->
            emqx_log:error("Stop plugin ~s error: ~p", [App]), {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

app_name(File) ->
    [AppName | _] = string:tokens(File, "."), list_to_atom(AppName).

names(plugin) ->
    names(list());

names(started_app) ->
    [Name || {Name, _Descr, _Ver} <- application:which_applications()];

names(Plugins) ->
    [Name || #plugin{name = Name} <- Plugins].

plugin_loaded(_Name, false) ->
    ok;
plugin_loaded(Name, true) ->
    case read_loaded() of
        {ok, Names} ->
            case lists:member(Name, Names) of
                false ->
                    %% write file if plugin is loaded
                    write_loaded(lists:append(Names, [Name]));
                true ->
                    ignore
            end;
        {error, Error} ->
            emqx_log:error("Cannot read loaded plugins: ~p", [Error])
    end.

plugin_unloaded(_Name, false) ->
    ok;
plugin_unloaded(Name, true) ->
    case read_loaded() of
        {ok, Names} ->
            case lists:member(Name, Names) of
                true ->
                    write_loaded(lists:delete(Name, Names));
                false ->
                    emqx_log:error("Cannot find ~s in loaded_file", [Name])
            end;
        {error, Error} ->
            emqx_log:error("Cannot read loaded_plugins: ~p", [Error])
    end.

read_loaded() ->
    case emqx_conf:get_env(plugins_loaded_file) of
        {ok, File} -> read_loaded(File);
        undefined  -> {error, not_found}
    end.

read_loaded(File) -> file:consult(File).

write_loaded(AppNames) ->
    {ok, File} = emqx_conf:get_env(plugins_loaded_file),
    case file:open(File, [binary, write]) of
        {ok, Fd} ->
            lists:foreach(fun(Name) ->
                file:write(Fd, iolist_to_binary(io_lib:format("~s.~n", [Name])))
            end, AppNames);
        {error, Error} ->
            emqx_log:error("Open File ~p Error: ~p", [File, Error]),
            {error, Error}
    end.

