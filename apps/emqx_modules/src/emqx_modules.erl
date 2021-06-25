%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules).

-include_lib("emqx/include/logger.hrl").

-logger_header("[Modules]").

-export([ list/0
        , load/0
        , load/1
        , unload/0
        , unload/1
        , reload/1
        , find_module/1
        , load_module/2
        ]).

-export([cli/1]).

%% @doc List all available plugins
-spec(list() -> [{atom(), boolean()}]).
list() ->
    ets:tab2list(?MODULE).

%% @doc Load all the extended modules.
-spec(load() -> ok).
load() ->
    case emqx:get_env(modules_loaded_file) of
        undefined -> ok;
        File ->
            load_modules(File)
    end.

load(ModuleName) ->
    case find_module(ModuleName) of
        [] ->
            ?LOG(alert, "Module ~s not found, cannot load it", [ModuleName]),
            {error, not_found};
        [{ModuleName, true}] ->
            ?LOG(notice, "Module ~s is already started", [ModuleName]),
            {error, already_started};
        [{ModuleName, false}] ->
            emqx_modules:load_module(ModuleName, true)
    end.

%% @doc Unload all the extended modules.
-spec(unload() -> ok).
unload() ->
    case emqx:get_env(modules_loaded_file) of
        undefined -> ignore;
        File ->
            unload_modules(File)
    end.

unload(ModuleName) ->
    case find_module(ModuleName) of
        [] ->
            ?LOG(alert, "Module ~s not found, cannot load it", [ModuleName]),
            {error, not_found};
        [{ModuleName, false}] ->
            ?LOG(error, "Module ~s is not started", [ModuleName]),
            {error, not_started};
        [{ModuleName, true}] ->
            unload_module(ModuleName, true)
    end.

-spec(reload(module()) -> ok | ignore | {error, any()}).
reload(_) ->
    ignore.

find_module(ModuleName) ->
    ets:lookup(?MODULE, ModuleName).

filter_module(ModuleNames) ->
    filter_module(ModuleNames, emqx:get_env(modules, [])).
filter_module([], Acc) ->
    Acc;
filter_module([{ModuleName, true} | ModuleNames], Acc) ->
    filter_module(ModuleNames, lists:keydelete(ModuleName, 1, Acc));
filter_module([{_, false} | ModuleNames], Acc) ->
    filter_module(ModuleNames, Acc).

load_modules(File) ->
    case file:consult(File) of
        {ok, ModuleNames} ->
            lists:foreach(fun({ModuleName, _}) ->
                ets:insert(?MODULE, {ModuleName, false})
            end, filter_module(ModuleNames)),
            lists:foreach(fun load_module/1, ModuleNames);
        {error, Error} ->
            ?LOG(alert, "Failed to read: ~p, error: ~p", [File, Error])
    end.

load_module({ModuleName, true}) ->
    emqx_modules:load_module(ModuleName, false);
load_module({ModuleName, false}) ->
    ets:insert(?MODULE, {ModuleName, false});
load_module(ModuleName) ->
    load_module({ModuleName, true}).

load_module(ModuleName, Persistent) ->
    Modules = emqx:get_env(modules, []),
    Env = proplists:get_value(ModuleName, Modules, undefined),
    case ModuleName:load(Env) of
        ok ->
            ets:insert(?MODULE, {ModuleName, true}),
            ok = write_loaded(Persistent),
            ?LOG(info, "Load ~s module successfully.", [ModuleName]);
        {error, Error} ->
            ?LOG(error, "Load module ~s failed, cannot load for ~0p", [ModuleName, Error]),
            {error, Error}
    end.

unload_modules(File) ->
    case file:consult(File) of
        {ok, ModuleNames} ->
            lists:foreach(fun unload_module/1, ModuleNames);
        {error, Error} ->
            ?LOG(alert, "Failed to read: ~p, error: ~p", [File, Error])
    end.
unload_module({ModuleName, true}) ->
    unload_module(ModuleName, false);
unload_module({ModuleName, false}) ->
    ets:insert(?MODULE, {ModuleName, false});
unload_module(ModuleName) ->
    unload_module({ModuleName, true}).

unload_module(ModuleName, Persistent) ->
    Modules = emqx:get_env(modules, []),
    Env = proplists:get_value(ModuleName, Modules, undefined),
    case ModuleName:unload(Env) of
        ok ->
            ets:insert(?MODULE, {ModuleName, false}),
            ok = write_loaded(Persistent),
            ?LOG(info, "Unload ~s module successfully.", [ModuleName]);
        {error, Error} ->
            ?LOG(error, "Unload module ~s failed, cannot unload for ~0p", [ModuleName, Error])
    end.

write_loaded(true) ->
    FilePath = emqx:get_env(modules_loaded_file),
    case file:write_file(FilePath, [io_lib:format("~p.~n", [Name]) || Name <- list()]) of
        ok -> ok;
        {error, Error} ->
            ?LOG(error, "Write File ~p Error: ~p", [FilePath, Error]),
            ok
    end;
write_loaded(false) -> ok.

%%--------------------------------------------------------------------
%% @doc Modules Command
cli(["list"]) ->
    lists:foreach(fun({Name, Active}) -> 
                    emqx_ctl:print("Module(~s, description=~s, active=~s)~n",
                        [Name, Name:description(), Active])
                  end, emqx_modules:list());

cli(["load", Name]) ->
    case emqx_modules:load(list_to_atom(Name)) of
        ok ->
            emqx_ctl:print("Module ~s loaded successfully.~n", [Name]);
        {error, Reason}   ->
            emqx_ctl:print("Load module ~s error: ~p.~n", [Name, Reason])
    end;

cli(["unload", Name]) ->
    case emqx_modules:unload(list_to_atom(Name)) of
        ok ->
            emqx_ctl:print("Module ~s unloaded successfully.~n", [Name]);
        {error, Reason} ->
            emqx_ctl:print("Unload module ~s error: ~p.~n", [Name, Reason])
    end;

cli(["reload", Name]) ->
    emqx_ctl:print("Module: ~p does not need to be reloaded.~n", [Name]);

cli(_) ->
    emqx_ctl:usage([{"modules list",            "Show loaded modules"},
                    {"modules load <Module>",   "Load module"},
                    {"modules unload <Module>", "Unload module"},
                    {"modules reload <Module>", "Reload module"}
                   ]).
