%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_connector.hrl").
-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(MONGO_HOST, "mongo").
-define(MONGO_CLIENT, 'emqx_authz_mongo_SUITE_client').

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = stop_apps([emqx_resource, emqx_connector]),
    case emqx_common_test_helpers:is_tcp_server_available(?MONGO_HOST, ?MONGO_DEFAULT_PORT) of
        true ->
            ok = emqx_common_test_helpers:start_apps(
                [emqx_conf, emqx_authz],
                fun set_special_configs/1
            ),
            ok = start_apps([emqx_resource, emqx_connector]),
            Config;
        false ->
            {skip, no_mongo}
    end.

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    ok = stop_apps([emqx_resource, emqx_connector]),
    ok = emqx_common_test_helpers:stop_apps([emqx_authz]).

set_special_configs(emqx_authz) ->
    ok = emqx_authz_test_lib:reset_authorizers();
set_special_configs(_) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, _} = mc_worker_api:connect(mongo_config()),
    ok = emqx_authz_test_lib:reset_authorizers(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = reset_samples(),
    ok = mc_worker_api:disconnect(?MONGO_CLIENT).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_topic_rules(_Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ok = emqx_authz_test_lib:test_no_topic_rules(ClientInfo, fun setup_client_samples/2),

    ok = emqx_authz_test_lib:test_allow_topic_rules(ClientInfo, fun setup_client_samples/2),

    ok = emqx_authz_test_lib:test_deny_topic_rules(ClientInfo, fun setup_client_samples/2).

t_complex_filter(_) ->
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

    ok = setup_samples(Samples),
    ok = setup_config(
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

t_mongo_error(_Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ok = setup_samples([]),
    ok = setup_config(
        #{<<"filter">> => #{<<"$badoperator">> => <<"$badoperator">>}}
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [{deny, publish, <<"t">>}]
    ).

t_lookups(_Config) ->
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

    ok = setup_samples([ByClientid]),
    ok = setup_config(
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

    ok = setup_samples([ByPeerhost]),
    ok = setup_config(
        #{<<"filter">> => #{<<"peerhost">> => <<"${peerhost}">>}}
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ).

t_bad_filter(_Config) ->
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

setup_samples(AclRecords) ->
    ok = reset_samples(),
    {{true, _}, _} = mc_worker_api:insert(?MONGO_CLIENT, <<"acl">>, AclRecords),
    ok.

setup_client_samples(ClientInfo, Samples) ->
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
    setup_samples(Records),
    setup_config(#{<<"filter">> => #{<<"username">> => <<"${username}">>}}).

reset_samples() ->
    {true, _} = mc_worker_api:delete(?MONGO_CLIENT, <<"acl">>, #{}),
    ok.

setup_config(SpecialParams) ->
    emqx_authz_test_lib:setup_config(
        raw_mongo_authz_config(),
        SpecialParams
    ).

raw_mongo_authz_config() ->
    #{
        <<"type">> => <<"mongodb">>,
        <<"enable">> => <<"true">>,

        <<"mongo_type">> => <<"single">>,
        <<"database">> => <<"mqtt">>,
        <<"collection">> => <<"acl">>,
        <<"server">> => mongo_server(),

        <<"filter">> => #{<<"username">> => <<"${username}">>}
    }.

mongo_server() ->
    iolist_to_binary(io_lib:format("~s", [?MONGO_HOST])).

mongo_config() ->
    [
        {database, <<"mqtt">>},
        {host, ?MONGO_HOST},
        {port, ?MONGO_DEFAULT_PORT},
        {register, ?MONGO_CLIENT}
    ].

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
