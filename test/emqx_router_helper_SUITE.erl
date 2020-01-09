%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_router_helper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(ROUTER_HELPER, emqx_router_helper).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_monitor(_) ->
    ok = emqx_router_helper:monitor({undefined, node()}),
    emqx_router_helper:monitor(undefined).

t_mnesia(_) ->
    ?ROUTER_HELPER ! {mnesia_table_event, {delete, {emqx_routing_node, node()}, undefined}},
    ?ROUTER_HELPER ! {mnesia_table_event, testing},
    ?ROUTER_HELPER ! {mnesia_table_event, {write, {emqx_routing_node, node()}, undefined}},
    ?ROUTER_HELPER ! {membership, testing},
    ?ROUTER_HELPER ! {membership, {mnesia, down, node()}},
    ct:sleep(200).

t_message(_) ->
    ?ROUTER_HELPER ! testing,
    gen_server:cast(?ROUTER_HELPER, testing),
    gen_server:call(?ROUTER_HELPER, testing).
