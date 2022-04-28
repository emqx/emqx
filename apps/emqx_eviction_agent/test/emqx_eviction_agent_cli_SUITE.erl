%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_eviction_agent_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_eviction_agent]),
    Config.

end_per_suite(Config) ->
    _ = emqx_eviction_agent:disable(foo),
    emqx_ct_helpers:stop_apps([emqx_eviction_agent]),
    Config.

t_status(_Config) ->
    %% usage
    ok = emqx_eviction_agent_cli:cli(["foobar"]),

    %% status
    ok = emqx_eviction_agent_cli:cli(["status"]),

    ok = emqx_eviction_agent:enable(foo, undefined),

    %% status
    ok = emqx_eviction_agent_cli:cli(["status"]).
