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

-export([init/0]).

-export([ load/0
        , load/1
        , unload/0
        , unload/1
        , reload/1
        , list/0
        , find_plugin/1
        , generate_configs/1
        , apply_configs/1
        ]).

-export([funlog/2]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-dialyzer({no_match, [ plugin_loaded/2
                     , plugin_unloaded/2
                     ]}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Init plugins' config
-spec(init() -> ok).
init() ->
    case emqx:get_env(plugins_etc_dir) of
        undefined  -> ok;
        PluginsEtc ->
            CfgFiles = [filename:join(PluginsEtc, File) ||
                        File <- filelib:wildcard("*.config", PluginsEtc)],
            lists:foreach(fun init_config/1, CfgFiles)
    end.

%% @doc Load all plugins when the broker started.
-spec(load() -> ok | ignore | {error, term()}).
load() ->
    ok = load_ext_plugins(emqx:get_env(expand_plugins_dir)),
    case emqx:get_env(plugins_loaded_file) of
        undefined -> ignore; %% No plugins available
        File ->
            _ = ensure_file(File),
            with_loaded_file(File, fun(Names) -> load_plugins(Names, false) end)
    end.

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
            load_plugin(PluginName, true)
    end.

%% @doc Unload all plugins before broker stopped.
-spec(unload() -> list() | {error, term()}).
unload() ->
    case emqx:get_env(plugins_loaded_file) of
        undefined -> ignore;
        File ->
            with_loaded_file(File, fun stop_plugins/1)
    end.

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
            unload_plugin(PluginName, true)
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
    lists:map(fun({Name, _, [Type| _]}) ->
        Plugin = plugin(Name, Type),
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

init_config(CfgFile) ->
    {ok, [AppsEnv]} = file:consult(CfgFile),
    lists:foreach(fun({App, Envs}) ->
                      [application:set_env(App, Par, Val) || {Par, Val} <- Envs]
                  end, AppsEnv).

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
    ok = load_plugin_app(AppName, Ebin),
    try
        ok = generate_configs(AppName, PluginDir)
    catch
        throw : {conf_file_not_found, ConfFile} ->
            %% this is maybe a dependency of an external plugin
            ?LOG(debug, "config_load_error_ignored for app=~p, path=~s", [AppName, ConfFile]),
            ok
    end.

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

ensure_file(File) ->
    case filelib:is_file(File) of false -> write_loaded([]); true -> ok end.

with_loaded_file(File, SuccFun) ->
    case read_loaded(File) of
        {ok, Names0} ->
            Names = filter_plugins(Names0),
            SuccFun(Names);
        {error, Error} ->
            ?LOG(alert, "Failed to read: ~p, error: ~p", [File, Error]),
            {error, Error}
    end.

filter_plugins(Names) ->
    lists:filtermap(fun(Name1) when is_atom(Name1) -> {true, Name1};
                       ({Name1, true}) -> {true, Name1};
                       ({_Name1, false}) -> false
                    end, Names).

load_plugins(Names, Persistent) ->
    Plugins = list(),
    NotFound = Names -- names(Plugins),
    case NotFound of
        []       -> ok;
        NotFound -> ?LOG(alert, "cannot_find_plugins: ~p", [NotFound])
    end,
    NeedToLoad = Names -- NotFound -- names(started_app),
    lists:foreach(fun(Name) ->
                      Plugin = find_plugin(Name, Plugins),
                      load_plugin(Plugin#plugin.name, Persistent)
                  end, NeedToLoad).

%% Stop plugins
stop_plugins(Names) ->
    _ = [stop_app(App) || App <- Names],
    ok.

plugin(AppName, Type) ->
    case application:get_all_key(AppName) of
        {ok, Attrs} ->
            Descr = proplists:get_value(description, Attrs, ""),
            #plugin{name = AppName, descr = Descr, type = plugin_type(Type)};
        undefined -> error({plugin_not_found, AppName})
    end.

load_plugin(Name, Persistent) ->
    try
        ok = ?MODULE:generate_configs(Name),
        case load_app(Name) of
            ok ->
                start_app(Name, fun(App) -> plugin_loaded(App, Persistent) end);
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

start_app(App, SuccFun) ->
    case application:ensure_all_started(App) of
        {ok, Started} ->
            ?LOG(info, "Started plugins: ~p", [Started]),
            ?LOG(info, "Load plugin ~s successfully", [App]),
            _ = SuccFun(App),
            ok;
        {error, {ErrApp, Reason}} ->
            ?LOG(error, "Load plugin ~s failed, cannot start plugin ~s for ~0p", [App, ErrApp, Reason]),
            {error, {ErrApp, Reason}}
    end.

unload_plugin(App, Persistent) ->
    case stop_app(App) of
        ok ->
            _ = plugin_unloaded(App, Persistent), ok;
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

plugin_loaded(_Name, false) ->
    ok;
plugin_loaded(Name, true) ->
    case read_loaded() of
        {ok, Names} ->
            case lists:member(Name, Names) of
                false ->
                    %% write file if plugin is loaded
                    write_loaded(lists:append(Names, [{Name, true}]));
                true ->
                    ignore
            end;
        {error, Error} ->
            ?LOG(error, "Cannot read loaded plugins: ~p", [Error])
    end.

plugin_unloaded(_Name, false) ->
    ok;
plugin_unloaded(Name, true) ->
    case read_loaded() of
        {ok, Names0} ->
            Names = filter_plugins(Names0),
            case lists:member(Name, Names) of
                true ->
                    write_loaded(lists:delete(Name, Names));
                false ->
                    ?LOG(error, "Cannot find ~s in loaded_file", [Name])
            end;
        {error, Error} ->
            ?LOG(error, "Cannot read loaded_plugins: ~p", [Error])
    end.

read_loaded() ->
    case emqx:get_env(plugins_loaded_file) of
        undefined -> {error, not_found};
        File      -> read_loaded(File)
    end.

read_loaded(File) -> file:consult(File).

write_loaded(AppNames) ->
    FilePath = emqx:get_env(plugins_loaded_file),
    case file:write_file(FilePath, [io_lib:format("~p.~n", [Name]) || Name <- AppNames]) of
        ok -> ok;
        {error, Error} ->
            ?LOG(error, "Write File ~p Error: ~p", [FilePath, Error]),
            {error, Error}
    end.

plugin_type(auth) -> auth;
plugin_type(protocol) -> protocol;
plugin_type(backend) -> backend;
plugin_type(bridge) -> bridge;
plugin_type(_) -> feature.


funlog(Key, Value) ->
    ?LOG(info, "~s = ~p", [string:join(Key, "."), Value]).

generate_configs(App) ->
    PluginConfDir = emqx:get_env(plugins_etc_dir),
    PluginSchemaDir = code:priv_dir(App),
    generate_configs(App, PluginConfDir, PluginSchemaDir).

generate_configs(App, PluginDir) ->
    PluginConfDir = filename:join([PluginDir, "etc"]),
    PluginSchemaDir = filename:join([PluginDir, "priv"]),
    generate_configs(App, PluginConfDir, PluginSchemaDir).

generate_configs(App, PluginConfDir, PluginSchemaDir) ->
    ConfigFile = filename:join([PluginConfDir, App]) ++ ".config",
    case filelib:is_file(ConfigFile) of
        true ->
            {ok, [Configs]} = file:consult(ConfigFile),
            apply_configs(Configs);
        false ->
            SchemaFile = filename:join([PluginSchemaDir, App]) ++ ".schema",
            case filelib:is_file(SchemaFile) of
                true ->
                    AppsEnv = do_generate_configs(App),
                    apply_configs(AppsEnv);
                false ->
                    SchemaMod = lists:concat([App, "_schema"]),
                    ConfName = filename:join([PluginConfDir, App]) ++ ".conf",
                    SchemaFile1 = filename:join([code:lib_dir(App), "ebin", SchemaMod]) ++ ".beam",
                    do_generate_hocon_configs(App, ConfName, SchemaFile1)
            end
    end.

do_generate_configs(App) ->
    Name1 = filename:join([emqx:get_env(plugins_etc_dir), App]) ++ ".conf",
    Name2 = filename:join([code:lib_dir(App), "etc", App]) ++ ".conf",
    ConfFile = case {filelib:is_file(Name1), filelib:is_file(Name2)} of
                   {true, _} -> Name1;
                   {false, true} -> Name2;
                   {false, false} -> error({config_not_found, [Name1, Name2]})
               end,
    SchemaFile = filename:join([code:priv_dir(App), App]) ++ ".schema",
    case filelib:is_file(SchemaFile) of
        true ->
            Schema = cuttlefish_schema:files([SchemaFile]),
            Conf = cuttlefish_conf:file(ConfFile),
            cuttlefish_generator:map(Schema, Conf, undefined, fun ?MODULE:funlog/2);
        false ->
            error({schema_not_found, SchemaFile})
    end.

do_generate_hocon_configs(App, ConfName, SchemaFile) ->
    SchemaMod = lists:concat([App, "_schema"]),
    case {filelib:is_file(ConfName), filelib:is_file(SchemaFile)} of
        {true, true} ->
            {ok, RawConfig} = hocon:load(ConfName, #{format => richmap}),
            Config = hocon_schema:check(list_to_atom(SchemaMod), RawConfig, #{atom_key => true,
                                                                              return_plain => true}),
            emqx_config_handler:update_config(emqx_config_handler, Config);
        {true, false} ->
            error({schema_not_found, [SchemaFile]});
        {false, true} ->
            error({config_not_found, [ConfName]});
        {false, false} ->
            error({conf_and_schema_not_found, [ConfName, SchemaFile]})
    end.

apply_configs([]) ->
    ok;
apply_configs([{App, Config} | More]) ->
    lists:foreach(fun({Key, _}) -> application:unset_env(App, Key) end, application:get_all_env(App)),
    lists:foreach(fun({Key, Val}) -> application:set_env(App, Key, Val) end, Config),
    apply_configs(More).
