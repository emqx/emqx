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

-module(emqx_ft_storage_fs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [
        {group, cluster}
    ].

-define(CLUSTER_CASES, [t_multinode_exports]).

groups() ->
    [
        {cluster, [sequence], ?CLUSTER_CASES}
    ].

init_per_suite(Config) ->
    Storage = emqx_ft_test_helpers:local_storage(Config),
    Apps = emqx_cth_suite:start(
        [
            {emqx_ft, #{config => emqx_ft_test_helpers:config(Storage)}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(Case, Config) ->
    [{tc, Case} | Config].
end_per_testcase(_Case, _Config) ->
    ok.

init_per_group(Group = cluster, Config) ->
    WorkDir = filename:join(?config(priv_dir, Config), Group),
    Apps = [
        {emqx_conf, #{start => false}},
        {emqx_ft, "file_transfer { enable = true, storage.local { enable = true } }"}
    ],
    Nodes = emqx_cth_cluster:start(
        [
            {emqx_ft_storage_fs1, #{apps => Apps, join_to => node()}},
            {emqx_ft_storage_fs2, #{apps => Apps, join_to => node()}}
        ],
        #{work_dir => WorkDir}
    ),
    [{cluster, Nodes} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(cluster, Config) ->
    ok = emqx_cth_suite:stop(?config(cluster, Config));
end_per_group(_Group, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_multinode_exports(Config) ->
    [Node1, Node2 | _] = ?config(cluster, Config),
    ok = emqx_ft_test_helpers:upload_file(sync, <<"c/1">>, <<"f:1">>, "fn1", <<"data">>, Node1),
    ok = emqx_ft_test_helpers:upload_file(sync, <<"c/2">>, <<"f:2">>, "fn2", <<"data">>, Node2),
    ?assertMatch(
        [
            #{transfer := {<<"c/1">>, <<"f:1">>}, name := "fn1"},
            #{transfer := {<<"c/2">>, <<"f:2">>}, name := "fn2"}
        ],
        lists:sort(list_files(Config))
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_id(Config) ->
    atom_to_binary(?config(tc, Config), utf8).

storage(Config) ->
    RawConfig = #{<<"storage">> => emqx_ft_test_helpers:local_storage(Config)},
    #{storage := #{local := Storage}} = emqx_ft_schema:translate(RawConfig),
    Storage.

list_files(Config) ->
    {ok, #{items := Files}} = emqx_ft_storage_fs:files(storage(Config), #{}),
    Files.
