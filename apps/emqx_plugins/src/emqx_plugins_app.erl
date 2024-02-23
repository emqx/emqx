%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins_app).

-behaviour(application).

-include("emqx_plugins.hrl").

-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    %% load all pre-configured
    ok = emqx_plugins:ensure_started(),
    {ok, Sup} = emqx_plugins_sup:start_link(),
    ok = emqx_config_handler:add_handler([?CONF_ROOT], emqx_plugins),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_config_handler:remove_handler([?CONF_ROOT]),
    ok.
