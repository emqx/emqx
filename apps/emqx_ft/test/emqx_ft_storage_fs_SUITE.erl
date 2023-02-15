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

-define(assertInclude(Pattern, List),
    ?assert(
        lists:any(
            fun
                (Pattern) -> true;
                (_) -> false
            end,
            List
        )
    )
).

all() ->
    [
        {group, single_node},
        {group, cluster}
    ].

-define(CLUSTER_CASES, [t_multinode_ready_transfers]).

groups() ->
    [
        {single_node, [sequence], emqx_common_test_helpers:all(?MODULE) -- ?CLUSTER_CASES},
        {cluster, [sequence], ?CLUSTER_CASES}
    ].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_ft], set_special_configs(Config)),
    ok = emqx_common_test_helpers:set_gen_rpc_stateless(),
    Config.
end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_ft, emqx_conf]),
    ok.

set_special_configs(Config) ->
    fun
        (emqx_ft) ->
            ok = emqx_config:put([file_transfer, storage], #{
                type => local, root => emqx_ft_test_helpers:ft_root(Config, node())
            });
        (_) ->
            ok
    end.

init_per_testcase(Case, Config) ->
    [{tc, Case} | Config].
end_per_testcase(_Case, _Config) ->
    ok.

init_per_group(cluster, Config) ->
    Node = emqx_ft_test_helpers:start_additional_node(Config, test2),
    [{additional_node, Node} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(cluster, Config) ->
    ok = emqx_ft_test_helpers:stop_additional_node(Config);
end_per_group(_Group, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_invalid_ready_transfer_id(Config) ->
    ?assertMatch(
        {error, _},
        emqx_ft_storage_fs:get_ready_transfer(storage(Config), #{
            <<"clientid">> => client_id(Config),
            <<"fileid">> => <<"fileid">>,
            <<"node">> => atom_to_binary('nonexistent@127.0.0.1')
        })
    ),
    ?assertMatch(
        {error, _},
        emqx_ft_storage_fs:get_ready_transfer(storage(Config), #{
            <<"clientid">> => client_id(Config),
            <<"fileid">> => <<"fileid">>,
            <<"node">> => <<"nonexistent_as_atom@127.0.0.1">>
        })
    ),
    ?assertMatch(
        {error, _},
        emqx_ft_storage_fs:get_ready_transfer(storage(Config), #{
            <<"clientid">> => client_id(Config),
            <<"fileid">> => <<"nonexistent_file">>,
            <<"node">> => node()
        })
    ).

t_multinode_ready_transfers(Config) ->
    Node1 = ?config(additional_node, Config),
    ok = emqx_ft_test_helpers:upload_file(<<"c1">>, <<"f1">>, <<"data">>, Node1),

    Node2 = node(),
    ok = emqx_ft_test_helpers:upload_file(<<"c2">>, <<"f2">>, <<"data">>, Node2),

    ?assertInclude(
        #{<<"clientid">> := <<"c1">>, <<"fileid">> := <<"f1">>},
        ready_transfer_ids(Config)
    ),

    ?assertInclude(
        #{<<"clientid">> := <<"c2">>, <<"fileid">> := <<"f2">>},
        ready_transfer_ids(Config)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_id(Config) ->
    atom_to_binary(?config(tc, Config), utf8).

storage(Config) ->
    #{
        type => local,
        root => ft_root(Config)
    }.

ft_root(Config) ->
    emqx_ft_test_helpers:ft_root(Config, node()).

ready_transfer_ids(Config) ->
    {ok, ReadyTransfers} = emqx_ft_storage_fs:ready_transfers(storage(Config)),
    {ReadyTransferIds, _} = lists:unzip(ReadyTransfers),
    ReadyTransferIds.
