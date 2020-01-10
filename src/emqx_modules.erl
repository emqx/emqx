%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("logger.hrl").

-logger_header("[Modules]").

-export([ load/0
        , unload/0
        ]).

%% @doc Load all the extended modules.
-spec(load() -> ok).
load() ->
    ok = emqx_mod_acl_internal:load([]),
    lists:foreach(fun load/1, modules()).

load({Mod, Env}) ->
    ok = Mod:load(Env),
    ?LOG(info, "Load ~s module successfully.", [Mod]).

modules() -> emqx:get_env(modules, []).

%% @doc Unload all the extended modules.
-spec(unload() -> ok).
unload() ->
    ok = emqx_mod_acl_internal:unload([]),
    lists:foreach(fun unload/1, modules()).

unload({Mod, Env}) ->
    Mod:unload(Env).

