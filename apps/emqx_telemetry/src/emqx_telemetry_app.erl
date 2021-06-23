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

-module(emqx_telemetry_app).

-behaviour(application).

-define(APP, emqx_telemetry).

-export([ start/2
        , stop/1
        ]).

start(_Type, _Args) ->
    %% TODO
    %% After the relevant code for building hocon configuration will be deleted
    %% Get the configuration using emqx_config:get
    ConfFile = filename:join([emqx:get_env(plugins_etc_dir), ?APP]) ++ ".conf",
    {ok, RawConfig} = hocon:load(ConfFile),
    Config = hocon_schema:check_plain(emqx_telemetry_schema, RawConfig, #{atom_key => true}),
    emqx_config_handler:update_config(emqx_config_handler, Config),
    Enabled = emqx_config:get([?APP, enabled], true),
    emqx_telemetry_sup:start_link(Enabled).

stop(_State) ->
    emqx_ctl:unregister_command(telemetry),
    ok.

