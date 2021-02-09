%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-emqx_plugin(?MODULE).

-export([start/2]).

-export([stop/1]).

-define(APP, emqx_modules).

start(_Type, _Args) ->
    % the configs for emqx_modules is so far still in emqx application
    % Ensure it's loaded
    application:load(emqx),
    ok = load_app_env(),
    {ok, Pid} = emqx_mod_sup:start_link(),
    ok = emqx_modules:load(),
    {ok, Pid}.

stop(_State) ->
    emqx_modules:unload().

load_app_env() ->
    Schema = filename:join([code:priv_dir(?APP), "emqx_modules.schema"]),
    Conf1 = filename:join([code:lib_dir(?APP), "etc", "emqx_modules.conf"]),
    Conf2 = filename:join([emqx:get_env(plugins_etc_dir), "emqx_modules.conf"]),
    [ConfFile | _] = lists:filter(fun filelib:is_regular/1, [Conf1, Conf2]),
    Conf = cuttlefish_conf:file(ConfFile),
    AppEnv = cuttlefish_generator:map(cuttlefish_schema:files([Schema]), Conf),
    lists:foreach(fun({AppName, Envs}) ->
        [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
    end, AppEnv).

