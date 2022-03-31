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

-module(emqx_authz_mysql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_connector.hrl").
-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(MYSQL_HOST, "mysql").
-define(MYSQL_RESOURCE, <<"emqx_authz_mysql_SUITE">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = stop_apps([emqx_resource, emqx_connector]),
    case emqx_common_test_helpers:is_tcp_server_available(?MYSQL_HOST, ?MYSQL_DEFAULT_PORT) of
        true ->
            ok = emqx_common_test_helpers:start_apps(
                [emqx_conf, emqx_authz],
                fun set_special_configs/1
            ),
            ok = start_apps([emqx_resource, emqx_connector]),
            {ok, _} = emqx_resource:create_local(
                ?MYSQL_RESOURCE,
                ?RESOURCE_GROUP,
                emqx_connector_mysql,
                mysql_config(),
                #{}
            ),
            Config;
        false ->
            {skip, no_mysql}
    end.

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    ok = emqx_resource:remove_local(?MYSQL_RESOURCE),
    ok = stop_apps([emqx_resource, emqx_connector]),
    ok = emqx_common_test_helpers:stop_apps([emqx_authz]).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    Config.

set_special_configs(emqx_authz) ->
    ok = emqx_authz_test_lib:reset_authorizers();
set_special_configs(_) ->
    ok.

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

    %% by clientid

    ok = init_table(),
    ok = q(
        <<
            "INSERT INTO acl(clientid, topic, permission, action)"
            "VALUES(?, ?, ?, ?)"
        >>,
        [<<"clientid">>, <<"a">>, <<"allow">>, <<"subscribe">>]
    ),

    ok = setup_config(
        #{
            <<"query">> => <<
                "SELECT permission, action, topic "
                "FROM acl WHERE clientid = ${clientid}"
            >>
        }
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ),

    %% by peerhost

    ok = init_table(),
    ok = q(
        <<
            "INSERT INTO acl(peerhost, topic, permission, action)"
            "VALUES(?, ?, ?, ?)"
        >>,
        [<<"127.0.0.1">>, <<"a">>, <<"allow">>, <<"subscribe">>]
    ),

    ok = setup_config(
        #{
            <<"query">> => <<
                "SELECT permission, action, topic "
                "FROM acl WHERE peerhost = ${peerhost}"
            >>
        }
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ),

    %% by cn

    ok = init_table(),
    ok = q(
        <<
            "INSERT INTO acl(cn, topic, permission, action)"
            "VALUES(?, ?, ?, ?)"
        >>,
        [<<"cn">>, <<"a">>, <<"allow">>, <<"subscribe">>]
    ),

    ok = setup_config(
        #{
            <<"query">> => <<
                "SELECT permission, action, topic "
                "FROM acl WHERE cn = ${cert_common_name}"
            >>
        }
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ),

    %% by dn

    ok = init_table(),
    ok = q(
        <<
            "INSERT INTO acl(dn, topic, permission, action)"
            "VALUES(?, ?, ?, ?)"
        >>,
        [<<"dn">>, <<"a">>, <<"allow">>, <<"subscribe">>]
    ),

    ok = setup_config(
        #{
            <<"query">> => <<
                "SELECT permission, action, topic "
                "FROM acl WHERE dn = ${cert_subject}"
            >>
        }
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ).

t_mysql_error(_Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ok = setup_config(
        #{<<"query">> => <<"SOME INVALID STATEMENT">>}
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [{deny, subscribe, <<"a">>}]
    ).

t_create_invalid(_Config) ->
    BadConfig = maps:merge(
        raw_mysql_authz_config(),
        #{<<"server">> => <<"255.255.255.255:33333">>}
    ),
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [BadConfig]),

    [_] = emqx_authz:lookup().

t_nonbinary_values(_Config) ->
    ClientInfo = #{
        clientid => clientid,
        username => "username",
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ok = init_table(),
    ok = q(
        <<
            "INSERT INTO acl(clientid, username, topic, permission, action)"
            "VALUES(?, ?, ?, ?, ?)"
        >>,
        [<<"clientid">>, <<"username">>, <<"a">>, <<"allow">>, <<"subscribe">>]
    ),

    ok = setup_config(
        #{
            <<"query">> => <<
                "SELECT permission, action, topic "
                "FROM acl WHERE clientid = ${clientid} AND username = ${username}"
            >>
        }
    ),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_mysql_authz_config() ->
    #{
        <<"enable">> => <<"true">>,

        <<"type">> => <<"mysql">>,
        <<"database">> => <<"mqtt">>,
        <<"username">> => <<"root">>,
        <<"password">> => <<"public">>,

        <<"query">> => <<
            "SELECT permission, action, topic "
            "FROM acl WHERE username = ${username}"
        >>,

        <<"server">> => mysql_server()
    }.

q(Sql) ->
    emqx_resource:query(
        ?MYSQL_RESOURCE,
        {sql, Sql}
    ).

q(Sql, Params) ->
    emqx_resource:query(
        ?MYSQL_RESOURCE,
        {sql, Sql, Params}
    ).

init_table() ->
    ok = drop_table(),
    ok = q(
        "CREATE TABLE acl(\n"
        "                       username VARCHAR(255),\n"
        "                       clientid VARCHAR(255),\n"
        "                       peerhost VARCHAR(255),\n"
        "                       cn VARCHAR(255),\n"
        "                       dn VARCHAR(255),\n"
        "                       topic VARCHAR(255),\n"
        "                       permission VARCHAR(255),\n"
        "                       action VARCHAR(255))"
    ).

drop_table() ->
    ok = q("DROP TABLE IF EXISTS acl").

setup_client_samples(ClientInfo, Samples) ->
    #{username := Username} = ClientInfo,
    ok = init_table(),
    ok = lists:foreach(
        fun(#{topics := Topics, permission := Permission, action := Action}) ->
            lists:foreach(
                fun(Topic) ->
                    q(
                        <<
                            "INSERT INTO acl(username, topic, permission, action)"
                            "VALUES(?, ?, ?, ?)"
                        >>,
                        [Username, Topic, Permission, Action]
                    )
                end,
                Topics
            )
        end,
        Samples
    ),
    setup_config(
        #{
            <<"query">> => <<
                "SELECT permission, action, topic "
                "FROM acl WHERE username = ${username}"
            >>
        }
    ).

setup_config(SpecialParams) ->
    emqx_authz_test_lib:setup_config(
        raw_mysql_authz_config(),
        SpecialParams
    ).

mysql_server() ->
    iolist_to_binary(io_lib:format("~s", [?MYSQL_HOST])).

mysql_config() ->
    #{
        auto_reconnect => true,
        database => <<"mqtt">>,
        username => <<"root">>,
        password => <<"public">>,
        pool_size => 8,
        server => {?MYSQL_HOST, ?MYSQL_DEFAULT_PORT},
        ssl => #{enable => false}
    }.

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
