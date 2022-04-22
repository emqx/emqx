%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_connector.hrl").
-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(REDIS_HOST, "redis").
-define(REDIS_RESOURCE, <<"emqx_authz_redis_SUITE">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = stop_apps([emqx_resource, emqx_connector]),
    case emqx_common_test_helpers:is_tcp_server_available(?REDIS_HOST, ?REDIS_DEFAULT_PORT) of
        true ->
            ok = emqx_common_test_helpers:start_apps(
                [emqx_conf, emqx_authz],
                fun set_special_configs/1
            ),
            ok = start_apps([emqx_resource, emqx_connector]),
            {ok, _} = emqx_resource:create_local(
                ?REDIS_RESOURCE,
                ?RESOURCE_GROUP,
                emqx_connector_redis,
                redis_config(),
                #{}
            ),
            Config;
        false ->
            {skip, no_redis}
    end.

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    ok = emqx_resource:remove_local(?REDIS_RESOURCE),
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
%% Tests
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

    ok = emqx_authz_test_lib:test_allow_topic_rules(ClientInfo, fun setup_client_samples/2).

t_lookups(_Config) ->
    ClientInfo = #{
        clientid => <<"client id">>,
        cn => <<"cn">>,
        dn => <<"dn">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ByClientid = #{
        <<"mqtt_user:client id">> =>
            #{<<"a">> => <<"all">>}
    },

    ok = setup_sample(ByClientid),
    ok = setup_config(#{<<"cmd">> => <<"HGETALL mqtt_user:${clientid}">>}),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ),

    ByPeerhost = #{
        <<"mqtt_user:127.0.0.1">> =>
            #{<<"a">> => <<"all">>}
    },

    ok = setup_sample(ByPeerhost),
    ok = setup_config(#{<<"cmd">> => <<"HGETALL mqtt_user:${peerhost}">>}),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ),

    ByCN = #{
        <<"mqtt_user:cn">> =>
            #{<<"a">> => <<"all">>}
    },

    ok = setup_sample(ByCN),
    ok = setup_config(#{<<"cmd">> => <<"HGETALL mqtt_user:${cert_common_name}">>}),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ),

    ByDN = #{
        <<"mqtt_user:dn">> =>
            #{<<"a">> => <<"all">>}
    },

    ok = setup_sample(ByDN),
    ok = setup_config(#{<<"cmd">> => <<"HGETALL mqtt_user:${cert_subject}">>}),

    ok = emqx_authz_test_lib:test_samples(
        ClientInfo,
        [
            {allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}
        ]
    ).

t_create_invalid(_Config) ->
    AuthzConfig = raw_redis_authz_config(),

    InvalidConfigs =
        [
            maps:without([<<"server">>], AuthzConfig),
            AuthzConfig#{<<"server">> => <<"unknownhost:3333">>},
            AuthzConfig#{<<"password">> => <<"wrongpass">>},
            AuthzConfig#{<<"database">> => <<"5678">>}
        ],

    lists:foreach(
        fun(Config) ->
            {ok, _} = emqx_authz:update(?CMD_REPLACE, [Config]),
            [_] = emqx_authz:lookup()
        end,
        InvalidConfigs
    ).

t_redis_error(_Config) ->
    ok = setup_config(#{<<"cmd">> => <<"INVALID COMMAND">>}),

    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    deny = emqx_access_control:authorize(ClientInfo, subscribe, <<"a">>).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

setup_sample(AuthzData) ->
    {ok, _} = q(["FLUSHDB"]),
    ok = lists:foreach(
        fun({Key, Values}) ->
            lists:foreach(
                fun({TopicFilter, Action}) ->
                    q(["HSET", Key, TopicFilter, Action])
                end,
                maps:to_list(Values)
            )
        end,
        maps:to_list(AuthzData)
    ).

setup_client_samples(ClientInfo, Samples) ->
    #{username := Username} = ClientInfo,
    Key = <<"mqtt_user:", Username/binary>>,
    lists:foreach(
        fun(Sample) ->
            #{
                topics := Topics,
                permission := <<"allow">>,
                action := Action
            } = Sample,
            lists:foreach(
                fun(Topic) ->
                    q(["HSET", Key, Topic, Action])
                end,
                Topics
            )
        end,
        Samples
    ),
    setup_config(#{}).

setup_config(SpecialParams) ->
    Config = maps:merge(raw_redis_authz_config(), SpecialParams),
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [Config]),
    ok.

raw_redis_authz_config() ->
    #{
        <<"enable">> => <<"true">>,
        <<"type">> => <<"redis">>,
        <<"redis_type">> => <<"single">>,
        <<"cmd">> => <<"HGETALL mqtt_user:${username}">>,
        <<"database">> => <<"1">>,
        <<"password">> => <<"public">>,
        <<"server">> => redis_server()
    }.

redis_server() ->
    iolist_to_binary(io_lib:format("~s", [?REDIS_HOST])).

q(Command) ->
    emqx_resource:query(
        ?REDIS_RESOURCE,
        {cmd, Command}
    ).

redis_config() ->
    #{
        auto_reconnect => true,
        database => 1,
        pool_size => 8,
        redis_type => single,
        password => "public",
        server => {?REDIS_HOST, ?REDIS_DEFAULT_PORT},
        ssl => #{enable => false}
    }.

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
