%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_prometheus_app).

-behaviour(application).

-include("emqx_prometheus.hrl").

%% Application callbacks
-export([
    start/2,
    stop/1
]).

start(_StartType, _StartArgs) ->
    Res = emqx_prometheus_sup:start_link(),
    emqx_prometheus_config:add_handler(),
    Res.

stop(_State) ->
    emqx_prometheus_config:remove_handler(),
    ok.
