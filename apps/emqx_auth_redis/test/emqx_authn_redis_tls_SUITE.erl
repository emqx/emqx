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

-module(emqx_authn_redis_tls_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(REDIS_HOST, "redis-tls").
-define(REDIS_TLS_PORT, 6380).

-define(PATH, [authentication]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(_, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    Config.

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_tcp_server_available(?REDIS_HOST, ?REDIS_TLS_PORT) of
        true ->
            Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_redis], #{
                work_dir => ?config(priv_dir, Config)
            }),
            [{apps, Apps} | Config];
        false ->
            {skip, no_redis}
    end.

end_per_suite(Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    ?assertMatch(
        {ok, _},
        create_redis_auth_with_ssl_opts(
            #{
                <<"server_name_indication">> => <<"authn-server">>,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.3">>],
                <<"ciphers">> => [<<"TLS_CHACHA20_POLY1305_SHA256">>]
            }
        )
    ).

t_create_invalid(_Config) ->
    %% invalid server_name
    ?assertMatch(
        {ok, _},
        create_redis_auth_with_ssl_opts(
            #{
                <<"server_name_indication">> => <<"authn-server-unknown-host">>,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.3">>],
                <<"ciphers">> => [<<"TLS_CHACHA20_POLY1305_SHA256">>]
            }
        )
    ),

    %% incompatible versions
    ?assertMatch(
        {error, _},
        create_redis_auth_with_ssl_opts(
            #{
                <<"server_name_indication">> => <<"authn-server">>,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.1">>, <<"tlsv1.2">>]
            }
        )
    ),

    %% incompatible ciphers
    ?assertMatch(
        {error, _},
        create_redis_auth_with_ssl_opts(
            #{
                <<"server_name_indication">> => <<"authn-server">>,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.3">>],
                <<"ciphers">> => [<<"TLS_AES_128_GCM_SHA256">>]
            }
        )
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

create_redis_auth_with_ssl_opts(SpecificSSLOpts) ->
    AuthConfig = raw_redis_auth_config(SpecificSSLOpts),
    emqx:update_config(?PATH, {create_authenticator, ?GLOBAL, AuthConfig}).

raw_redis_auth_config(SpecificSSLOpts) ->
    SSLOpts = maps:merge(
        emqx_authn_test_lib:client_ssl_cert_opts(),
        #{<<"enable">> => <<"true">>}
    ),
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"plain">>,
            <<"salt_position">> => <<"suffix">>
        },
        <<"enable">> => <<"true">>,

        <<"backend">> => <<"redis">>,
        <<"cmd">> => <<"HMGET mqtt_user:${username} password_hash salt is_superuser">>,
        <<"database">> => <<"1">>,
        <<"password">> => <<"public">>,
        <<"server">> => redis_server(),
        <<"redis_type">> => <<"single">>,
        <<"ssl">> => maps:merge(SSLOpts, SpecificSSLOpts)
    }.

redis_server() ->
    iolist_to_binary(io_lib:format("~s:~b", [?REDIS_HOST, ?REDIS_TLS_PORT])).

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
