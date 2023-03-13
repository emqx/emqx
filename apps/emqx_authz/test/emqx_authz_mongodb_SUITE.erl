%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_mongodb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

all() ->
    [
        {group, mongo},
        {group, mongo_v5}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {mongo, TCs},
        {mongo_v5, TCs}
    ].

init_per_group(Group, Config) ->
    ok = stop_apps([emqx_resource]),
    {MongoHost, MongoPort} = address(Group),
    case emqx_common_test_helpers:is_tcp_server_available(MongoHost, MongoPort) of
        true ->
            ok = emqx_common_test_helpers:start_apps(
                [emqx_conf, emqx_authz],
                fun set_special_configs/1
            ),
            ok = start_apps([emqx_resource]),
            [{mongo_host, MongoHost}, {mongo_port, MongoPort} | Config];
        false ->
            {skip, no_mongo}
    end.

end_per_group(_Group, _Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    ok = stop_apps([emqx_resource]),
    ok = emqx_common_test_helpers:stop_apps([emqx_authz]).

set_special_configs(emqx_authz) ->
    ok = emqx_authz_test_lib:reset_authorizers();
set_special_configs(_) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, Pid} = mc_worker_api:connect(mongo_config(Config)),
    ok = emqx_authz_test_lib:reset_authorizers(),
    [{mongo_client, Pid} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = reset_samples(?config(mongo_client, Config)),
    ok = mc_worker_api:disconnect(?config(mongo_client, Config)).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_topic_rules(Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    SetupFun = fun(Samples) ->
        setup_client_samples(Config, ClientInfo, Samples)
    end,

    ok = emqx_authz_test_lib:test_no_topic_rules(ClientInfo, SetupFun),

    ok = emqx_authz_test_lib:test_allow_topic_rules(ClientInfo, SetupFun),

    ok = emqx_authz_test_lib:test_deny_topic_rules(ClientInfo, SetupFun).

t_complex_filter(Config) ->
    %% atom and string values also supported
    ClientInfo = #{
        clientid => clientid,
        username => "username",
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    Samples = [
        #{
            <<"x">> => #{
                <<"u">> => <<"username">>,
                <<"c">> => [#{<<"c">> => <<"clientid">>}],
                <<"y">> => 1
            },
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"publish">>,
            <<"topics">> => [<<"t">>]
        }
    ],

    ok = setup_samples(Config, Samples),
    ok = setup_config(
        Config,
        #{
            <<"filter">> => #{
                <<"x">> => #{
                    <<"u">> => <<"${username}">>,
                    <<"c">> => [#{<<"c">> => <<"${clientid}">>}],
                    <<"y">> => 1
                }
            }
        }
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [{allow, publish, <<"t">>}]
    ).

t_mongo_error(Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ok = setup_samples(Config, []),
    ok = setup_config(
        Config,
        #{<<"filter">> => #{<<"$badoperator">> => <<"$badoperator">>}}
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [{deny, publish, <<"t">>}]
    ).

t_lookups(Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        cn => <<"cn">>,
        dn => <<"dn">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ByClientid = #{
        <<"clientid">> => <<"clientid">>,
        <<"topics">> => [<<"a">>],
        <<"action">> => <<"all">>,
        <<"permission">> => <<"allow">>
    },

    ok = setup_samples(Config, [ByClientid]),
    ok = setup_config(
        Config,
        #{<<"filter">> => #{<<"clientid">> => <<"${clientid}">>}}
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ),

    ByPeerhost = #{
        <<"peerhost">> => <<"127.0.0.1">>,
        <<"topics">> => [<<"a">>],
        <<"action">> => <<"all">>,
        <<"permission">> => <<"allow">>
    },

    ok = setup_samples(Config, [ByPeerhost]),
    ok = setup_config(
        Config,
        #{<<"filter">> => #{<<"peerhost">> => <<"${peerhost}">>}}
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ),

    ByCN = #{
        <<"CN">> => <<"cn">>,
        <<"topics">> => [<<"a">>],
        <<"action">> => <<"all">>,
        <<"permission">> => <<"allow">>
    },

    ok = setup_samples(Config, [ByCN]),
    ok = setup_config(
        Config,
        #{<<"filter">> => #{<<"CN">> => ?PH_CERT_CN_NAME}}
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ),

    ByDN = #{
        <<"DN">> => <<"dn">>,
        <<"topics">> => [<<"a">>],
        <<"action">> => <<"all">>,
        <<"permission">> => <<"allow">>
    },

    ok = setup_samples(Config, [ByDN]),
    ok = setup_config(
        Config,
        #{<<"filter">> => #{<<"DN">> => ?PH_CERT_SUBJECT}}
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ).

t_bad_filter(Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        cn => <<"cn">>,
        dn => <<"dn">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ok = setup_config(
        Config,
        #{<<"filter">> => #{<<"$in">> => #{<<"a">> => 1}}}
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {deny, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

populate_records(AclRecords, AdditionalData) ->
    [maps:merge(Record, AdditionalData) || Record <- AclRecords].

setup_samples(Config, AclRecords) ->
    Client = ?config(mongo_client, Config),
    ok = reset_samples(Client),
    {{true, _}, _} = mc_worker_api:insert(Client, <<"acl">>, AclRecords),
    ok.

setup_client_samples(Config, ClientInfo, Samples) ->
    #{username := Username} = ClientInfo,
    Records = lists:map(
        fun(Sample) ->
            #{
                topics := Topics,
                permission := Permission,
                action := Action
            } = Sample,

            #{
                <<"topics">> => Topics,
                <<"permission">> => Permission,
                <<"action">> => Action,
                <<"username">> => Username
            }
        end,
        Samples
    ),
    setup_samples(Config, Records),
    setup_config(Config, #{<<"filter">> => #{<<"username">> => <<"${username}">>}}).

reset_samples(Client) ->
    {true, _} = mc_worker_api:delete(Client, <<"acl">>, #{}),
    ok.

setup_config(Config, SpecialParams) ->
    emqx_authz_test_lib:setup_config(
        raw_mongo_authz_config(Config),
        SpecialParams
    ).

raw_mongo_authz_config(Config) ->
    #{
        <<"type">> => <<"mongodb">>,
        <<"enable">> => <<"true">>,

        <<"mongo_type">> => <<"single">>,
        <<"database">> => <<"mqtt">>,
        <<"collection">> => <<"acl">>,
        <<"server">> => mongo_server(Config),

        <<"filter">> => #{<<"username">> => <<"${username}">>}
    }.

mongo_server(Config) ->
    iolist_to_binary(?config(mongo_host, Config)).

mongo_config(Config) ->
    [
        {database, <<"mqtt">>},
        {host, ?config(mongo_host, Config)},
        {port, ?config(mongo_port, Config)}
    ].

address(mongo) -> {"mongo", ?MONGO_DEFAULT_PORT};
address(mongo_v5) -> {"mongo_v5", ?MONGO_DEFAULT_PORT}.

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
