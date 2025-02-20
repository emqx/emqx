%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_ext_listener_tls_verify_chain_SUITE).

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
    [{ssl_config, ssl_config_verify_peer()}, {suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

t_conn_fail_with_intermediate_ca_cert(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options, [
            {cacertfile, filename:join(DataDir, "intermediate1.pem")},
            {certfile, filename:join(DataDir, "server1.pem")},
            {keyfile, filename:join(DataDir, "server1.key")}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client1.key")},
            {certfile, filename:join(DataDir, "client1.pem")},
            {verify, verify_none}
        ],
        1000
    ),

    fail_when_no_ssl_alert(Socket, unknown_ca),
    ok = ssl:close(Socket).

t_conn_fail_with_other_intermediate_ca_cert(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options, [
            {cacertfile, filename:join(DataDir, "intermediate1.pem")},
            {certfile, filename:join(DataDir, "server1.pem")},
            {keyfile, filename:join(DataDir, "server1.key")}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2.pem")},
            {verify, verify_none}
        ],
        1000
    ),

    fail_when_no_ssl_alert(Socket, unknown_ca),
    ok = ssl:close(Socket).

t_conn_success_with_server_client_composed_complete_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% Server has root ca cert
    Options = [
        {ssl_options, [
            {cacertfile, filename:join(DataDir, "root.pem")},
            {certfile, filename:join(DataDir, "server2.pem")},
            {keyfile, filename:join(DataDir, "server2.key")}
            | ?config(ssl_config, Config)
        ]}
    ],
    %% Client has complete chain
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")},
            {verify, verify_none}
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_success_with_other_signed_client_composed_complete_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% Server has root ca cert
    Options = [
        {ssl_options, [
            {cacertfile, filename:join(DataDir, "root.pem")},
            {certfile, filename:join(DataDir, "server1.pem")},
            {keyfile, filename:join(DataDir, "server1.key")}
            | ?config(ssl_config, Config)
        ]}
    ],
    %% Client has partial_chain
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")},
            {verify, verify_none}
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_success_with_renewed_intermediate_root_bundle(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% Server has root ca cert
    Options = [
        {ssl_options, [
            {cacertfile, filename:join(DataDir, "intermediate1_renewed-root-bundle.pem")},
            {certfile, filename:join(DataDir, "server1.pem")},
            {keyfile, filename:join(DataDir, "server1.key")}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client1.key")},
            {certfile, filename:join(DataDir, "client1.pem")},
            {verify, verify_none}
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_success_with_client_complete_cert_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options, [
            {cacertfile, filename:join(DataDir, "root.pem")},
            {certfile, filename:join(DataDir, "server2.pem")},
            {keyfile, filename:join(DataDir, "server2.key")}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-complete-bundle.pem")},
            {verify, verify_none}
        ],
        1000
    ),
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_fail_with_server_partial_chain(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% imcomplete at server side
    Options = [
        {ssl_options, [
            {cacertfile, filename:join(DataDir, "intermediate2.pem")},
            {certfile, filename:join(DataDir, "server2.pem")},
            {keyfile, filename:join(DataDir, "server2.key")}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-complete-bundle.pem")},
            {versions, ['tlsv1.2']},
            {verify, verify_none}
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

t_conn_fail_without_root_cacert(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options, [
            {cacertfile, filename:join(DataDir, "intermediate2.pem")},
            {certfile, filename:join(DataDir, "server2.pem")},
            {keyfile, filename:join(DataDir, "server2.key")}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    Res = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, filename:join(DataDir, "client2.key")},
            {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")},
            %% stick to tlsv1.2 for consistent error message
            {versions, ['tlsv1.2']},
            {cacertfile, filename:join(DataDir, "intermediate2.pem")}
        ],
        1000
    ),
    fail_when_no_ssl_alert(Res, unknown_ca).

ssl_config_verify_peer() ->
    [
        {verify, verify_peer},
        {fail_if_no_peer_cert, true}
    ].
