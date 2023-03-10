%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_storage_fs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [
        {group, cluster}
    ].

-define(CLUSTER_CASES, [t_multinode_ready_transfers]).

groups() ->
    [
        {cluster, [sequence], ?CLUSTER_CASES}
    ].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_ft], set_special_configs(Config)),
    Config.
end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_ft]),
    ok.

set_special_configs(Config) ->
    fun
        (emqx_ft) ->
            emqx_ft_test_helpers:load_config(#{
                storage => emqx_ft_test_helpers:local_storage(Config)
            });
        (_) ->
            ok
    end.

init_per_testcase(Case, Config) ->
    [{tc, Case} | Config].
end_per_testcase(_Case, _Config) ->
    ok.

init_per_group(cluster, Config) ->
    Node = emqx_ft_test_helpers:start_additional_node(Config, emqx_ft_storage_fs1),
    [{additional_node, Node} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(cluster, Config) ->
    ok = emqx_ft_test_helpers:stop_additional_node(?config(additional_node, Config));
end_per_group(_Group, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_multinode_ready_transfers(Config) ->
    Node1 = ?config(additional_node, Config),
    ok = emqx_ft_test_helpers:upload_file(<<"c/1">>, <<"f:1">>, "fn1", <<"data">>, Node1),

    Node2 = node(),
    ok = emqx_ft_test_helpers:upload_file(<<"c/2">>, <<"f:2">>, "fn2", <<"data">>, Node2),

    ?assertMatch(
        [
            #{transfer := {<<"c/1">>, <<"f:1">>}, name := "fn1"},
            #{transfer := {<<"c/2">>, <<"f:2">>}, name := "fn2"}
        ],
        lists:sort(list_exports(Config))
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_id(Config) ->
    atom_to_binary(?config(tc, Config), utf8).

storage(Config) ->
    #{
        type => local,
        root => emqx_ft_test_helpers:root(Config, node(), ["transfers"]),
        exporter => #{
            type => local,
            root => emqx_ft_test_helpers:root(Config, node(), ["exports"])
        }
    }.

list_exports(Config) ->
    {ok, Exports} = emqx_ft_storage_fs:exports(storage(Config)),
    Exports.
