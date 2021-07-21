%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("logger.hrl").

-logger_header("[Plugins]").

-export([ load/0
        , load/1
        , unload/0
        , unload/1
        , reload/1
        , list/0
        , find_plugin/1
        ]).

-export([funlog/2]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Load all plugins when the broker started.
-spec(load() -> ok | ignore | {error, term()}).
load() ->
    ok = load_ext_plugins(emqx_config:get([plugins, expand_plugins_dir])).

%% @doc Load a Plugin
-spec(load(atom()) -> ok | {error, term()}).
load(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, names(plugin)), lists:member(PluginName, names(started_app))} of
        {false, _} ->
            ?LOG(alert, "Plugin ~s not found, cannot load it", [PluginName]),
            {error, not_found};
        {_, true} ->
            ?LOG(notice, "Plugin ~s is already started", [PluginName]),
            {error, already_started};
        {_, false} ->
            load_plugin(PluginName)
    end.

%% @doc Unload all plugins before broker stopped.
-spec(unload() -> ok).
unload() ->
    stop_plugins(list()).

%% @doc UnLoad a Plugin
-spec(unload(atom()) -> ok | {error, term()}).
unload(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, names(plugin)), lists:member(PluginName, names(started_app))} of
        {false, _} ->
            ?LOG(error, "Plugin ~s is not found, cannot unload it", [PluginName]),
            {error, not_found};
        {_, false} ->
            ?LOG(error, "Plugin ~s is not started", [PluginName]),
            {error, not_started};
        {_, _} ->
            unload_plugin(PluginName)
    end.

reload(PluginName) when is_atom(PluginName)->
    case {lists:member(PluginName, names(plugin)), lists:member(PluginName, names(started_app))} of
        {false, _} ->
            ?LOG(error, "Plugin ~s is not found, cannot reload it", [PluginName]),
            {error, not_found};
        {_, false} ->
            load(PluginName);
        {_, true} ->
            case unload(PluginName) of
                ok -> load(PluginName);
                {error, Reason} -> {error, Reason}
            end
    end.

%% @doc List all available plugins
-spec(list() -> [emqx_types:plugin()]).
list() ->
    StartedApps = names(started_app),
    lists:map(fun({Name, _, _}) ->
        Plugin = plugin(Name),
        case lists:member(Name, StartedApps) of
            true  -> Plugin#plugin{active = true};
            false -> Plugin
        end
    end, lists:sort(ekka_boot:all_module_attributes(emqx_plugin))).

find_plugin(Name) ->
    find_plugin(Name, list()).

find_plugin(Name, Plugins) ->
    lists:keyfind(Name, 2, Plugins).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% load external plugins which are placed in etc/plugins dir
load_ext_plugins(undefined) -> ok;
load_ext_plugins(Dir) ->
    lists:foreach(
        fun(Plugin) ->
                PluginDir = filename:join(Dir, Plugin),
                case filelib:is_dir(PluginDir) of
                    true  -> load_ext_plugin(PluginDir);
                    false -> ok
                end
        end, filelib:wildcard("*", Dir)).

load_ext_plugin(PluginDir) ->
    ?LOG(debug, "loading_extra_plugin: ~s", [PluginDir]),
    Ebin = filename:join([PluginDir, "ebin"]),
    AppFile = filename:join([Ebin, "*.app"]),
    AppName = case filelib:wildcard(AppFile) of
                  [App] ->
                      list_to_atom(filename:basename(App, ".app"));
                  [] ->
                      ?LOG(alert, "plugin_app_file_not_found: ~s", [AppFile]),
                      error({plugin_app_file_not_found, AppFile})
              end,
    ok = load_plugin_app(AppName, Ebin).
    % try
    %     ok = generate_configs(AppName, PluginDir)
    % catch
    %     throw : {conf_file_not_found, ConfFile} ->
    %         %% this is maybe a dependency of an external plugin
    %         ?LOG(debug, "config_load_error_ignored for app=~p, path=~s", [AppName, ConfFile]),
    %         ok
    % end.

load_plugin_app(AppName, Ebin) ->
    _ = code:add_patha(Ebin),
    Modules = filelib:wildcard(filename:join([Ebin, "*.beam"])),
    lists:foreach(
        fun(BeamFile) ->
                Module = list_to_atom(filename:basename(BeamFile, ".beam")),
                case code:load_file(Module) of
                    {module, _} -> ok;
                    {error, Reason} -> error({failed_to_load_plugin_beam, BeamFile, Reason})
                end
        end, Modules),
    case application:load(AppName) of
        ok -> ok;
        {error, {already_loaded, _}} -> ok
    end.

%% Stop plugins
stop_plugins(Plugins) ->
    _ = [stop_app(Plugin#plugin.name) || Plugin <- Plugins],
    ok.

plugin(AppName) ->
    case application:get_all_key(AppName) of
        {ok, Attrs} ->
            Descr = proplists:get_value(description, Attrs, ""),
            #plugin{name = AppName, descr = Descr};
        undefined -> error({plugin_not_found, AppName})
    end.

load_plugin(Name) ->
    try
        case load_app(Name) of
            ok ->
                start_app(Name);
            {error, Error0} ->
                {error, Error0}
        end
    catch _ : Error : Stacktrace ->
        ?LOG(alert, "Plugin ~s load failed with ~p", [Name, {Error, Stacktrace}]),
        {error, parse_config_file_failed}
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

start_app(App) ->
    case application:ensure_all_started(App) of
        {ok, Started} ->
            ?LOG(info, "Started plugins: ~p", [Started]),
            ?LOG(info, "Load plugin ~s successfully", [App]),
            ok;
        {error, {ErrApp, Reason}} ->
            ?LOG(error, "Load plugin ~s failed, cannot start plugin ~s for ~0p", [App, ErrApp, Reason]),
            {error, {ErrApp, Reason}}
    end.

unload_plugin(App) ->
    case stop_app(App) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

stop_app(App) ->
    case application:stop(App) of
        ok ->
            ?LOG(info, "Stop plugin ~s successfully", [App]), ok;
        {error, {not_started, App}} ->
            ?LOG(error, "Plugin ~s is not started", [App]), ok;
        {error, Reason} ->
            ?LOG(error, "Stop plugin ~s error: ~p", [App]), {error, Reason}
    end.

names(plugin) ->
    names(list());

names(started_app) ->
    [Name || {Name, _Descr, _Ver} <- application:which_applications()];

names(Plugins) ->
    [Name || #plugin{name = Name} <- Plugins].

funlog(Key, Value) ->
    ?LOG(info, "~s = ~p", [string:join(Key, "."), Value]).
