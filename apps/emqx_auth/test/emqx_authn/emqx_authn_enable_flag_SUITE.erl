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

-module(emqx_authn_enable_flag_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_authn.hrl").

-define(PATH, [?CONF_NS_ATOM]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth], #{
        work_dir => ?config(priv_dir, Config)
    }),
    ok = emqx_authn_test_lib:register_fake_providers([{password_based, built_in_database}]),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    AuthnConfig = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"clientid">>
    },
    {ok, _} = emqx_conf:update(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthnConfig},
        #{}
    ),
    {ok, _} = emqx_conf:update(
        [listeners, tcp, listener_authn_enabled],
        {create, listener_mqtt_tcp_conf(18830, true)},
        #{}
    ),
    {ok, _} = emqx_conf:update(
        [listeners, tcp, listener_authn_disabled],
        {create, listener_mqtt_tcp_conf(18831, false)},
        #{}
    ),
    Config.

end_per_testcase(_Case, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        ?PATH,
        ?GLOBAL
    ),
    emqx_conf:remove(
        [listeners, tcp, listener_authn_enabled], #{}
    ),
    emqx_conf:remove(
        [listeners, tcp, listener_authn_disabled], #{}
    ),
    Config.

listener_mqtt_tcp_conf(Port, EnableAuthn) ->
    PortS = integer_to_binary(Port),
    #{
        <<"acceptors">> => 16,
        <<"access_rules">> => ["allow all"],
        <<"bind">> => <<"0.0.0.0:", PortS/binary>>,
        <<"max_connections">> => 1024000,
        <<"mountpoint">> => <<>>,
        <<"proxy_protocol">> => false,
        <<"proxy_protocol_timeout">> => <<"3s">>,
        <<"enable_authn">> => EnableAuthn
    }.

t_enable_authn(_Config) ->
    %% enable_authn set to false, we connect successfully
    {ok, ConnPid0} = emqtt:start_link([{port, 18831}, {clientid, <<"clientid">>}]),
    ?assertMatch(
        {ok, _},
        emqtt:connect(ConnPid0)
    ),
    ok = emqtt:disconnect(ConnPid0),

    process_flag(trap_exit, true),

    %% enable_authn set to true, we go to the set up authn and fail
    {ok, ConnPid1} = emqtt:start_link([{port, 18830}, {clientid, <<"clientid">>}]),
    ?assertMatch(
        {error, {malformed_username_or_password, _}},
        emqtt:connect(ConnPid1)
    ),
    ok.
