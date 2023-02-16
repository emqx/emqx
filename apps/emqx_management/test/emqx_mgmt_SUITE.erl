%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite([emqx_conf, emqx_management]),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite([emqx_management, emqx_conf]).

init_per_testcase(TestCase, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TestCase, Config).

end_per_testcase(TestCase, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TestCase, Config).

t_list_nodes(init, Config) ->
    meck:expect(
        mria_mnesia,
        cluster_nodes,
        fun
            (running) -> [node()];
            (stopped) -> ['stopped@node']
        end
    ),
    Config;
t_list_nodes('end', _Config) ->
    meck:unload(mria_mnesia).

t_list_nodes(_) ->
    NodeInfos = emqx_mgmt:list_nodes(),
    Node = node(),
    ?assertMatch(
        [
            {Node, #{node := Node, node_status := 'running'}},
            {'stopped@node', #{node := 'stopped@node', node_status := 'stopped'}}
        ],
        NodeInfos
    ).
