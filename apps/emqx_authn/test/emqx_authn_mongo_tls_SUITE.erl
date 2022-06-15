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

-module(emqx_authn_mongo_tls_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_authn/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(MONGO_HOST, "mongo-tls").

-define(PATH, [authentication]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    emqx_authentication:initialize_authentication(?GLOBAL, []),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    Config.

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    case emqx_common_test_helpers:is_tcp_server_available(?MONGO_HOST, ?MONGO_DEFAULT_PORT) of
        true ->
            ok = emqx_common_test_helpers:start_apps([emqx_authn]),
            ok = start_apps([emqx_resource, emqx_connector]),
            Config;
        false ->
            {skip, no_mongo}
    end.

end_per_suite(_Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = stop_apps([emqx_resource, emqx_connector]),
    ok = emqx_common_test_helpers:stop_apps([emqx_authn]).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

%% emqx_connector_mongo connects asynchronously,
%% so we check failure/success indirectly (through snabbkaffe).

%% openssl s_client -tls1_2 -cipher ECDHE-RSA-AES256-GCM-SHA384 \
%%   -connect mongo-tls:27017 \
%%   -cert client.crt -key client.key -CAfile ca.crt

t_create(_Config) ->
    ?check_trace(
        create_mongo_auth_with_ssl_opts(
            #{
                <<"server_name_indication">> => <<"authn-server">>,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.2">>],
                <<"ciphers">> => [<<"ECDHE-RSA-AES256-GCM-SHA384">>]
            }
        ),
        fun({ok, _}, Trace) ->
            ?assertMatch(
                [ok | _],
                ?projection(
                    status,
                    ?of_kind(emqx_connector_mongo_health_check, Trace)
                )
            )
        end
    ).

t_create_invalid_server_name(_Config) ->
    ?check_trace(
        create_mongo_auth_with_ssl_opts(
            #{
                <<"server_name_indication">> => <<"authn-server-unknown-host">>,
                <<"verify">> => <<"verify_peer">>
            }
        ),
        fun(_, Trace) ->
            ?assertNotEqual(
                [ok],
                ?projection(
                    status,
                    ?of_kind(emqx_connector_mongo_health_check, Trace)
                )
            )
        end
    ).

%% docker-compose-mongo-single-tls.yaml:
%% --tlsDisabledProtocols TLS1_0,TLS1_1

t_create_invalid_version(_Config) ->
    ?check_trace(
        create_mongo_auth_with_ssl_opts(
            #{
                <<"server_name_indication">> => <<"authn-server">>,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.1">>]
            }
        ),
        fun(_, Trace) ->
            ?assertNotEqual(
                [ok],
                ?projection(
                    status,
                    ?of_kind(emqx_connector_mongo_health_check, Trace)
                )
            )
        end
    ).

%% docker-compose-mongo-single-tls.yaml:
%% --setParameter opensslCipherConfig='HIGH:!EXPORT:!aNULL:!DHE:!kDHE@STRENGTH'

t_invalid_ciphers(_Config) ->
    ?check_trace(
        create_mongo_auth_with_ssl_opts(
            #{
                <<"server_name_indication">> => <<"authn-server">>,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.2">>],
                <<"ciphers">> => [<<"DHE-RSA-AES256-GCM-SHA384">>]
            }
        ),
        fun(_, Trace) ->
            ?assertNotEqual(
                [ok],
                ?projection(
                    status,
                    ?of_kind(emqx_connector_mongo_health_check, Trace)
                )
            )
        end
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

create_mongo_auth_with_ssl_opts(SpecificSSLOpts) ->
    AuthConfig = raw_mongo_auth_config(SpecificSSLOpts),
    Res = emqx:update_config(?PATH, {create_authenticator, ?GLOBAL, AuthConfig}),
    timer:sleep(500),
    Res.

raw_mongo_auth_config(SpecificSSLOpts) ->
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

        <<"backend">> => <<"mongodb">>,
        <<"pool_size">> => 2,
        <<"mongo_type">> => <<"single">>,
        <<"database">> => <<"mqtt">>,
        <<"collection">> => <<"users">>,
        <<"server">> => mongo_server(),
        <<"w_mode">> => <<"unsafe">>,

        <<"filter">> => #{<<"username">> => <<"${username}">>},
        <<"password_hash_field">> => <<"password_hash">>,
        <<"salt_field">> => <<"salt">>,
        <<"is_superuser_field">> => <<"is_superuser">>,
        <<"topology">> => #{
            <<"server_selection_timeout_ms">> => <<"10000ms">>
        },

        <<"ssl">> => maps:merge(SSLOpts, SpecificSSLOpts)
    }.

mongo_server() ->
    iolist_to_binary(io_lib:format("~s", [?MONGO_HOST])).

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
