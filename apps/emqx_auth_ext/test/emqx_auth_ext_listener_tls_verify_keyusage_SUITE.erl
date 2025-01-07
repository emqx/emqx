%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_ext_listener_tls_verify_keyusage_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(
    emqx_test_tls_certs_helper,
    [
        fail_when_ssl_error/1,
        fail_when_no_ssl_alert/2,
        generate_tls_certs/1,
        gen_host_cert/4,
        emqx_start_listener/4,
        select_free_port/1
    ]
).

all() ->
    [
        {group, full_chain},
        {group, partial_chain}
    ].

all_tc() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [
        {partial_chain, [], all_tc()},
        {full_chain, [], all_tc()}
    ].

init_per_suite(Config) ->
    generate_tls_certs(Config),
    %% injection happens when module is loaded.
    code:load_file(emqx_auth_ext),
    Apps = emqx_cth_suite:start(
        [{emqx, #{override_env => [{boot_modules, [broker]}]}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(full_chain, Config) ->
    [{ssl_config, ssl_config_verify_peer_full_chain(Config)} | Config];
init_per_group(partial_chain, Config) ->
    [{ssl_config, ssl_config_verify_peer_partial_chain(Config)} | Config];
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

t_conn_success_verify_peer_ext_key_usage_unset(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% Given listener keyusage unset
    Options = [{ssl_options, ?config(ssl_config, Config)}],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    %% when client connect with cert without keyusage ext
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
    %% Then connection success
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_success_verify_peer_ext_key_usage_undefined(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% Give listener keyusage is set to undefined
    Options = [
        {ssl_options, [
            {verify_peer_ext_key_usage, undefined}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    %% when client connect with cert without keyusages ext
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
    %% Then connection success
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_success_verify_peer_ext_key_usage_matched_predefined(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% Give listener keyusage is set to clientAuth
    Options = [
        {ssl_options, [
            {verify_peer_ext_key_usage, "clientAuth"}
            | ?config(ssl_config, Config)
        ]}
    ],

    %% When client cert has clientAuth that is matched
    gen_client_cert_ext_keyusage(?FUNCTION_NAME, "intermediate1", DataDir, "clientAuth"),
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, client_key_file(DataDir, ?FUNCTION_NAME)},
            {certfile, client_pem_file(DataDir, ?FUNCTION_NAME)},
            {verify, verify_none}
        ],
        1000
    ),
    %% Then connection success
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_success_verify_peer_ext_key_usage_matched_raw_oid(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% Give listener keyusage is set to raw OID

    %% from OTP-PUB-KEY.hrl
    Options = [
        {ssl_options, [
            {verify_peer_ext_key_usage, "OID:1.3.6.1.5.5.7.3.2"}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    %% When client cert has keyusage and matched.
    gen_client_cert_ext_keyusage(?FUNCTION_NAME, "intermediate1", DataDir, "clientAuth"),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, client_key_file(DataDir, ?FUNCTION_NAME)},
            {certfile, client_pem_file(DataDir, ?FUNCTION_NAME)},
            {verify, verify_none}
        ],
        1000
    ),
    %% Then connection success
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_success_verify_peer_ext_key_usage_matched_ordered_list(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),

    %% Give listener keyusage is clientAuth,serverAuth
    Options = [
        {ssl_options, [
            {verify_peer_ext_key_usage, "clientAuth,serverAuth"}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    %% When client cert has the same keyusage ext list
    gen_client_cert_ext_keyusage(?FUNCTION_NAME, "intermediate1", DataDir, "clientAuth,serverAuth"),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, client_key_file(DataDir, ?FUNCTION_NAME)},
            {certfile, client_pem_file(DataDir, ?FUNCTION_NAME)},
            {verify, verify_none}
        ],
        1000
    ),
    %% Then connection success
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_success_verify_peer_ext_key_usage_matched_unordered_list(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% Give listener keyusage is clientAuth,serverAuth
    Options = [
        {ssl_options, [
            {verify_peer_ext_key_usage, "serverAuth,clientAuth"}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    %% When client cert has the same keyusage ext list but different order
    gen_client_cert_ext_keyusage(?FUNCTION_NAME, "intermediate1", DataDir, "clientAuth,serverAuth"),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, client_key_file(DataDir, ?FUNCTION_NAME)},
            {certfile, client_pem_file(DataDir, ?FUNCTION_NAME)},
            {verify, verify_none}
        ],
        1000
    ),
    %% Then connection success
    fail_when_ssl_error(Socket),
    ok = ssl:close(Socket).

t_conn_fail_verify_peer_ext_key_usage_unmatched_raw_oid(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% Give listener keyusage is using OID
    Options = [
        {ssl_options, [
            {verify_peer_ext_key_usage, "OID:1.3.6.1.5.5.7.3.1"}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),

    %% When client cert has the keyusage but not matching OID
    gen_client_cert_ext_keyusage(?FUNCTION_NAME, "intermediate1", DataDir, "clientAuth"),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, client_key_file(DataDir, ?FUNCTION_NAME)},
            {certfile, client_pem_file(DataDir, ?FUNCTION_NAME)},
            {verify, verify_none}
        ],
        1000
    ),

    %% Then connecion should fail.
    fail_when_no_ssl_alert(Socket, handshake_failure),
    ok = ssl:close(Socket).

t_conn_fail_verify_peer_ext_key_usage_empty_str(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    Options = [
        {ssl_options, [
            {verify_peer_ext_key_usage, ""}
            | ?config(ssl_config, Config)
        ]}
    ],
    %% Give listener keyusage is empty string
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    %% When client connect with cert without keyusage
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
    %% Then connecion should fail.
    fail_when_no_ssl_alert(Socket, handshake_failure),
    ok = ssl:close(Socket).

t_conn_fail_client_keyusage_unmatch(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),

    %% Give listener keyusage is clientAuth
    Options = [
        {ssl_options, [
            {verify_peer_ext_key_usage, "clientAuth"}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    %% When client connect with mismatch cert keyusage = codeSigning
    gen_client_cert_ext_keyusage(?FUNCTION_NAME, "intermediate1", DataDir, "codeSigning"),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        Port,
        [
            {keyfile, client_key_file(DataDir, ?FUNCTION_NAME)},
            {certfile, client_pem_file(DataDir, ?FUNCTION_NAME)},
            {verify, verify_none}
        ],
        1000
    ),
    %% Then connecion should fail.
    fail_when_no_ssl_alert(Socket, handshake_failure),
    ok = ssl:close(Socket).

t_conn_fail_client_keyusage_incomplete(Config) ->
    Port = select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    %% Give listener keyusage is codeSigning,clientAuth
    Options = [
        {ssl_options, [
            {verify_peer_ext_key_usage,
                "serverAuth,clientAuth,codeSigning,emailProtection,timeStamping,ocspSigning"}
            | ?config(ssl_config, Config)
        ]}
    ],
    emqx_start_listener(?FUNCTION_NAME, ssl, Port, Options),
    %% When client connect with cert keyusage = clientAuth
    gen_client_cert_ext_keyusage(?FUNCTION_NAME, "intermediate1", DataDir, "codeSigning"),
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
    %% Then connection should fail
    fail_when_no_ssl_alert(Socket, handshake_failure),
    ok = ssl:close(Socket).

%%%
%%% Helpers
%%%
gen_client_cert_ext_keyusage(Name, CA, DataDir, Usage) when is_atom(Name) ->
    gen_client_cert_ext_keyusage(atom_to_list(Name), CA, DataDir, Usage);
gen_client_cert_ext_keyusage(Name, CA, DataDir, Usage) ->
    gen_host_cert(Name, CA, DataDir, #{ext => "extendedKeyUsage=" ++ Usage}).

client_key_file(DataDir, Name) ->
    filename:join(DataDir, Name) ++ ".key".

client_pem_file(DataDir, Name) ->
    filename:join(DataDir, Name) ++ ".pem".

ssl_config_verify_peer_full_chain(Config) ->
    [
        {cacertfile, filename:join(?config(data_dir, Config), "intermediate1-root-bundle.pem")}
        | ssl_config_verify_peer(Config)
    ].
ssl_config_verify_peer_partial_chain(Config) ->
    [
        {cacertfile, filename:join(?config(data_dir, Config), "intermediate1.pem")},
        {partial_chain, true}
        | ssl_config_verify_peer(Config)
    ].

ssl_config_verify_peer(Config) ->
    DataDir = ?config(data_dir, Config),
    [
        {verify, verify_peer},
        {fail_if_no_peer_cert, true},
        {keyfile, filename:join(DataDir, "server1.key")},
        {certfile, filename:join(DataDir, "server1.pem")}
        %% , {log_level, debug}
    ].
