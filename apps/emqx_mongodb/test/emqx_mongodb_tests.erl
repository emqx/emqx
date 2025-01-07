%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mongodb_tests).

-include_lib("eunit/include/eunit.hrl").

srv_record_test() ->
    with_dns_mock(
        fun normal_dns_resolution_mock/2,
        fun() ->
            Single = single_config(),
            Rs = simple_rs_config(),
            Hosts = [
                <<"cluster0-shard-00-02.zkemc.mongodb.net:27017">>,
                <<"cluster0-shard-00-01.zkemc.mongodb.net:27017">>,
                <<"cluster0-shard-00-00.zkemc.mongodb.net:27017">>
            ],
            ?assertMatch(
                #{
                    hosts := Hosts,
                    auth_source := <<"admin">>
                },
                resolve(Single)
            ),
            ?assertMatch(
                #{
                    hosts := Hosts,
                    auth_source := <<"admin">>,
                    replica_set_name := <<"atlas-wrnled-shard-0">>
                },
                resolve(Rs)
            ),
            ok
        end
    ).

empty_srv_record_test() ->
    with_dns_mock(
        bad_srv_record_mock(_DnsResolution = []),
        fun() ->
            ?assertThrow(#{reason := "failed_to_resolve_srv_record"}, resolve(simple_rs_config()))
        end
    ).

empty_txt_record_test() ->
    with_dns_mock(
        bad_txt_record_mock(_DnsResolution = []),
        fun() ->
            Config = resolve(single_config()),
            ?assertNot(maps:is_key(auth_source, Config)),
            ?assertNot(maps:is_key(replica_set_name, Config)),
            ok
        end
    ).

multiple_txt_records_test() ->
    with_dns_mock(
        bad_txt_record_mock(_DnsResolution = [1, 2]),
        fun() ->
            ?assertThrow(#{reason := "multiple_txt_records"}, resolve(simple_rs_config()))
        end
    ).

bad_query_string_test() ->
    with_dns_mock(
        bad_txt_record_mock(_DnsResolution = [["%-111"]]),
        fun() ->
            ?assertThrow(#{reason := "bad_txt_record_resolution"}, resolve(simple_rs_config()))
        end
    ).

resolve(Config) ->
    emqx_mongodb:maybe_resolve_srv_and_txt_records(Config).

checked_config(Hocon) ->
    {ok, Config} = hocon:binary(Hocon),
    hocon_tconf:check_plain(
        emqx_mongodb,
        #{<<"config">> => Config},
        #{atom_key => true}
    ).

simple_rs_config() ->
    #{config := Rs} = checked_config(
        "mongo_type = rs\n"
        "servers = \"cluster0.zkemc.mongodb.net:27017\"\n"
        "srv_record = true\n"
        "database = foobar\n"
        "replica_set_name = configured_replicaset_name\n"
    ),
    Rs.

single_config() ->
    #{config := Single} = checked_config(
        "mongo_type = single\n"
        "server = \"cluster0.zkemc.mongodb.net:27017,cluster0.zkemc.mongodb.net:27017\"\n"
        "srv_record = true\n"
        "database = foobar\n"
    ),
    Single.

normal_srv_resolution() ->
    [
        {0, 0, 27017, "cluster0-shard-00-02.zkemc.mongodb.net"},
        {0, 0, 27017, "cluster0-shard-00-01.zkemc.mongodb.net"},
        {0, 0, 27017, "cluster0-shard-00-00.zkemc.mongodb.net"}
    ].

normal_txt_resolution() ->
    [["authSource=admin&replicaSet=atlas-wrnled-shard-0"]].

normal_dns_resolution_mock("_mongodb._tcp.cluster0.zkemc.mongodb.net", srv) ->
    normal_srv_resolution();
normal_dns_resolution_mock("cluster0.zkemc.mongodb.net", txt) ->
    normal_txt_resolution().

bad_srv_record_mock(DnsResolution) ->
    fun("_mongodb._tcp.cluster0.zkemc.mongodb.net", srv) ->
        DnsResolution
    end.

bad_txt_record_mock(DnsResolution) ->
    fun
        ("_mongodb._tcp.cluster0.zkemc.mongodb.net", srv) ->
            normal_srv_resolution();
        ("cluster0.zkemc.mongodb.net", txt) ->
            DnsResolution
    end.

with_dns_mock(MockFn, TestFn) ->
    meck:new(emqx_connector_lib, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_connector_lib, resolve_dns, MockFn),
    try
        TestFn()
    after
        meck:unload(emqx_connector_lib)
    end,
    ok.
