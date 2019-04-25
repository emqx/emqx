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

-module(emqx_rpc_SUITE).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).
-define(MASTER, 'emqxct@127.0.0.1').

all() -> [t_rpc].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_rpc(_) ->
    60000 = emqx_rpc:call(?MASTER, timer, seconds, [60]),
    {badrpc, _} = emqx_rpc:call(?MASTER, os, test, []),
    {_, []} = emqx_rpc:multicall([?MASTER, ?MASTER], os, timestamp, []).
