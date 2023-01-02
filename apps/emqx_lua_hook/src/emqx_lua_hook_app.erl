%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_lua_hook_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([ start/2
        , stop/1
        , prep_stop/1
        ]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_lua_hook_sup:start_link(),
    emqx_lua_hook:load_scripts(),
    emqx_lua_hook_cli:load(),
    {ok, Sup}.

prep_stop(State) ->
    emqx_lua_hook:unload_scripts(),
    emqx_lua_hook_cli:unload(),
    State.

stop(_State) ->
    ok.
