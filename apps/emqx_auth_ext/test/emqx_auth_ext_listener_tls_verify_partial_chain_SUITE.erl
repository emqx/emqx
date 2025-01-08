%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_auth_ext_listener_tls_verify_partial_chain_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(
    emqx_test_tls_certs_helper,
    [
        emqx_start_listener/4,
        fail_when_ssl_error/1,
        fail_when_no_ssl_alert/2,
        generate_tls_certs/1,
        select_free_port/1
    ]
).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    generate_tls_certs(Config),
    %% injection happens when module is loaded.
    code:load_file(emqx_auth_ext),
    Apps = emqx_cth_suite:start(
        [{emqx, #{override_env => [{boot_modules, [broker]}]}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{ssl_config, ssl_config_verify_partial_chain()}, {suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

t_conn_success_with_server_intermediate_cacert_and_client_cert(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate1.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client1.key")},
            {certfile, filename:join(DataDir, "client1.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ssl:close(Socket).

t_conn_success_with_intermediate_cacert_bundle(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "server1-intermediate1-bundle.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client1.key")},
            {certfile, filename:join(DataDir, "client1.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ssl:close(Socket).

t_conn_success_with_renewed_intermediate_cacert(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate1_renewed.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client1.key")},
            {certfile, filename:join(DataDir, "client1.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ssl:close(Socket).

t_conn_fail_with_renewed_intermediate_cacert_and_client_using_old_complete_bundle(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate2_renewed.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-complete-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_fail_with_renewed_intermediate_cacert_and_client_using_old_bundle(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate2_renewed.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_success_with_old_and_renewed_intermediate_cacert_and_client_provides_renewed_client_cert(
    Config
) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate2_renewed_old-bundle.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")},
                    {partial_chain, two_cacerts_from_cacertfile}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2_renewed.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ssl:close(Socket).

%% Note, this is good to have for usecase coverage
t_conn_success_with_new_intermediate_cacert_and_client_provides_renewed_client_cert_signed_by_old_intermediate(
    Config
) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate2_renewed.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2_renewed.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ssl:close(Socket).

%% @doc server should build a partial_chain with old version of ca cert.
t_conn_success_with_old_and_renewed_intermediate_cacert_and_client_provides_client_cert(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate2_renewed_old-bundle.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")},
                    {partial_chain, two_cacerts_from_cacertfile}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ssl:close(Socket).

%% @doc verify when config does not allow two versions of certs from same trusted CA.
t_conn_fail_with_renewed_and_old_intermediate_cacert_and_client_using_old_bundle(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate2_renewed_old-bundle.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

%% @doc verify when config (two_cacerts_from_cacertfile) allows two versions of certs from same trusted CA.
t_001_conn_success_with_old_and_renewed_intermediate_cacert_bundle_and_client_using_old_bundle(
    Config
) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate2_renewed_old-bundle.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")},
                    {partial_chain, two_cacerts_from_cacertfile}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ssl:close(Socket).

%% @doc: verify even if listener has old/new intermediate2 certs,
%%       client1 should not able to connect with old intermediate2 cert.
%%  In this case, listener verify_fun returns {trusted_ca, Oldintermediate2Cert} but
%%  OTP should still fail the validation since the client1 cert is not signed by
%%  Oldintermediate2Cert (trusted CA cert).
%% @end
t_conn_fail_with_old_and_renewed_intermediate_cacert_bundle_and_client_using_all_CAcerts(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate2_renewed_old-bundle.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")},
                    {partial_chain, two_cacerts_from_cacertfile}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client1.key")},
            {certfile, filename:join(DataDir, "all-CAcerts-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_fail_with_renewed_intermediate_cacert_other_client(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate1_renewed.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_fail_with_intermediate_cacert_bundle_but_incorrect_order(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate1-server1-bundle.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client1.key")},
            {certfile, filename:join(DataDir, "client1.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_fail_when_singed_by_other_intermediate_ca(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate1.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_success_with_complete_chain_that_server_root_cacert_and_client_complete_cert_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "root.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-complete-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_fail_with_other_client_complete_cert_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate1.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-complete-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_fail_with_server_intermediate_and_other_client_complete_cert_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate1-root-bundle.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-complete-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_success_with_server_intermediate_cacert_and_client_complete_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate2.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-complete-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_fail_with_server_intermediate_chain_and_client_other_incomplete_cert_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate1.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_fail_with_server_intermediate_and_other_client_root_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate1.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-root-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_success_with_server_intermediate_and_client_root_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate2.pem")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-root-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

%% @doc once rootCA cert present in cacertfile, sibling CA signed Client cert could connect.
t_conn_success_with_server_all_CA_bundle_and_client_root_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "all-CAcerts-bundle.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-root-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_fail_with_server_two_IA_bundle_and_client_root_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "two-intermediates-bundle.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-root-bundle.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_fail_with_server_partial_chain_false_intermediate_cacert_and_client_cert(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "intermediate1.pem")},
                    {certfile, filename:join(DataDir, "server1.pem")},
                    {keyfile, filename:join(DataDir, "server1.key")},
                    {partial_chain, false}
                ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client1.key")},
            {certfile, filename:join(DataDir, "client1.pem")}
            | client_default_tls_opts()
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_error_handling_invalid_cacertfile(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% trigger error
    Options = [
        {ssl_options,
            ?config(ssl_config, Config) ++
                [
                    {cacertfile, filename:join(DataDir, "server2.key")},
                    {certfile, filename:join(DataDir, "server2.pem")},
                    {keyfile, filename:join(DataDir, "server2.key")}
                ]}
    ],
    ?assertException(
        throw,
        {error, rootfun_trusted_ca_from_cacertfile},
        emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options)
    ).

ssl_config_verify_partial_chain() ->
    [
        {verify, verify_peer},
        {fail_if_no_peer_cert, true},
        {partial_chain, true}
    ].

client_default_tls_opts() ->
    [
        {versions, ['tlsv1.2']},
        {verify, verify_none}
    ].
