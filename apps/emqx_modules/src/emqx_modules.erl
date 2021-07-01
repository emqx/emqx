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
        , load/2
        , unload/0
        , unload/1
        , reload/1
        , find_module/1
        ]).

-export([cli/1]).

%% @doc List all available plugins
-spec(list() -> [{atom(), boolean()}]).
list() ->
    persistent_term:get(?MODULE, []).

%% @doc Load all the extended modules.
-spec(load() -> ok).
load() ->
    Modules = emqx_config:get([emqx_modules, modules], []),
    lists:foreach(fun(#{type := Module, enable := Enable} = Config) ->
        case Enable of
            true ->
                load(name(Module), maps:without([type, enable], Config));
            false ->
                ok
        end
    end, Modules).

load(Module, Env) ->
    ModuleName = name(Module),
    case find_module(ModuleName) of
        false ->
            load_mod(ModuleName, Env);
        true ->
            ?LOG(notice, "Module ~s is already started", [ModuleName]),
            {error, already_started}
    end.

%% @doc Unload all the extended modules.
-spec(unload() -> ok).
unload() ->
    Modules = emqx_config:get([emqx_modules, modules], []),
    lists:foreach(fun(#{type := Module, enable := Enable}) ->
        case Enable of
            true ->
                unload_mod(name(Module));
            false ->
                ok
        end
    end, Modules).

unload(ModuleName) ->
    case find_module(ModuleName) of
        false ->
            ?LOG(alert, "Module ~s not found, cannot load it", [ModuleName]),
            {error, not_started};
        true ->
            unload_mod(ModuleName)
    end.

-spec(reload(module()) -> ok | ignore | {error, any()}).
reload(_) ->
    ignore.

find_module(ModuleName) ->
    lists:member(ModuleName, persistent_term:get(?MODULE, [])).

load_mod(ModuleName, Env) ->
    case ModuleName:load(Env) of
        ok ->
            Modules = persistent_term:get(?MODULE, []),
            persistent_term:put(?MODULE, [ModuleName| Modules]),
            ?LOG(info, "Load ~s module successfully.", [ModuleName]);
        {error, Error} ->
            ?LOG(error, "Load module ~s failed, cannot load for ~0p", [ModuleName, Error]),
            {error, Error}
    end.

unload_mod(ModuleName) ->
    case ModuleName:unload(#{}) of
        ok ->
            Modules = persistent_term:get(?MODULE, []),
            persistent_term:put(?MODULE, Modules -- [ModuleName]),
            ?LOG(info, "Unload ~s module successfully.", [ModuleName]);
        {error, Error} ->
            ?LOG(error, "Unload module ~s failed, cannot unload for ~0p", [ModuleName, Error])
    end.

%%--------------------------------------------------------------------
%% @doc Modules Command
cli(["list"]) ->
    lists:foreach(fun(Name) ->
                    emqx_ctl:print("Module(~s, description=~s)~n",
                        [Name, Name:description()])
                  end, emqx_modules:list());

cli(["load", Name]) ->
    case emqx_modules:load(list_to_atom(Name), #{}) of
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

name(delayed) -> emqx_mod_delayed;
name(presence) -> emqx_mod_presence;
name(recon) -> emqx_mod_recon;
name(rewrite) -> emqx_mod_rewrite;
name(topic_metrics) -> emqx_mod_topic_metrics;
name(Name) -> Name.
