%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules).

-include("logger.hrl").

-export([ load/0
        , unload/0
        ]).

-spec(load() -> ok).
load() ->
    ok = emqx_mod_acl_internal:load([]),
    lists:foreach(
      fun({Mod, Env}) ->
        ok = Mod:load(Env),
        ?LOG(info, "[Modules] Load ~s module successfully.", [Mod])
      end, emqx_config:get_env(modules, [])).

-spec(unload() -> ok).
unload() ->
    ok = emqx_mod_acl_internal:unload([]),
    lists:foreach(
      fun({Mod, Env}) ->
          Mod:unload(Env) end,
      emqx_config:get_env(modules, [])).

