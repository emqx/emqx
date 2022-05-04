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

-module(emqx_conf_app_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_conf.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_copy_conf_override_on_restarts(_Config) ->
    net_kernel:start(['master@127.0.0.1', longnames]),
    ct:timetrap({seconds, 120}),
    snabbkaffe:fix_ct_logging(),
    Cluster = cluster([core, core, core]),

    %% 1. Start all nodes
    Nodes = start_cluster(Cluster),
    try
        assert_config_load_done(Nodes),

        %% 2. Stop each in order.
        stop_cluster(Nodes),

        %% 3. Restart nodes in the same order.  This should not
        %% crash and eventually all nodes should be ready.
        start_cluster_async(Cluster),

        timer:sleep(15_000),

        assert_config_load_done(Nodes),

        ok
    after
        stop_cluster(Nodes)
    end.

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

assert_config_load_done(Nodes) ->
    lists:foreach(
        fun(Node) ->
            Done = rpc:call(Node, emqx_app, get_init_config_load_done, []),
            ?assert(Done, #{node => Node})
        end,
        Nodes
    ).

stop_cluster(Nodes) ->
    [emqx_common_test_helpers:stop_slave(Node) || Node <- Nodes].

start_cluster(Specs) ->
    [emqx_common_test_helpers:start_slave(Name, Opts) || {Name, Opts} <- Specs].

start_cluster_async(Specs) ->
    [
        begin
            Opts1 = maps:remove(join_to, Opts),
            spawn_link(fun() -> emqx_common_test_helpers:start_slave(Name, Opts1) end),
            timer:sleep(7_000)
        end
     || {Name, Opts} <- Specs
    ].

cluster(Specs) ->
    Env = [
        {emqx, init_config_load_done, false},
        {emqx, boot_modules, []}
    ],
    emqx_common_test_helpers:emqx_cluster(Specs, [
        {env, Env},
        {apps, [emqx_conf]},
        {load_schema, false},
        {join_to, false},
        {env_handler, fun
            (emqx) ->
                application:set_env(emqx, boot_modules, []),
                ok;
            (_) ->
                ok
        end}
    ]).
