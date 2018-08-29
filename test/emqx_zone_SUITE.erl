%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_zone_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").

all() -> [t_set_get_env].

t_set_get_env(_) ->
    {ok, _} = emqx_zone:start_link(),
    ok = emqx_zone:set_env(china, language, chinese),
    timer:sleep(100),   % make sure set_env/3 is okay
    chinese = emqx_zone:get_env(china, language),
    cn470 = emqx_zone:get_env(china, ism_band, cn470),
    undefined = emqx_zone:get_env(undefined, delay),
    500 = emqx_zone:get_env(undefined, delay, 500).

