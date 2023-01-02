%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules_app).

-behaviour(application).

-export([start/2]).

-export([stop/1]).

start(_Type, _Args) ->
    % the configs for emqx_modules is so far still in emqx application
    % Ensure it's loaded
    _ = application:load(emqx),
    {ok, Pid} = emqx_mod_sup:start_link(),
    ok = emqx_modules:load(),
    emqx_ctl:register_command(modules, {emqx_modules, cli}, []),
    {ok, Pid}.

stop(_State) ->
    emqx_ctl:unregister_command(modules),
    emqx_modules:unload().
