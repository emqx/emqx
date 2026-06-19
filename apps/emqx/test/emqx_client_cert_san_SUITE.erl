%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_client_cert_san_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

all() ->
    [
        t_cert_san_as_client_attrs,
        t_invalid_cert_san_rejected,
        t_empty_cert_san_do_not_set_attr
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx,
                "listeners.tcp.default.enable = false\n"
                "listeners.ssl.default.enable = false\n"
                "listeners.ws.default.enable = false\n"
                "listeners.wss.default.enable = false\n"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], []),
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], []),
    ok.

t_cert_san_as_client_attrs(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {Port, SslOpts, ListenerConf} = start_san_listener(
        ?FUNCTION_NAME,
        Config,
        [
            {dns, "example.com"},
            {dns, "www.example.com"},
            {ip, {192, 168, 1, 100}},
            {ip, {16#2001, 16#0db8, 0, 0, 0, 0, 0, 1}},
            {email, "ops@example.com"},
            {uri, "spiffe://example.com/client"}
        ]
    ),
    {ok, DNS} = emqx_variform:compile("nth(1, cert_san.dns)"),
    {ok, AllDNS} = emqx_variform:compile("join_to_string(',', cert_san.dns)"),
    {ok, IP} = emqx_variform:compile("nth(1, cert_san.ip)"),
    {ok, IPv6} = emqx_variform:compile("nth(2, cert_san.ip)"),
    {ok, Email} = emqx_variform:compile("nth(1, cert_san.email)"),
    {ok, URI} = emqx_variform:compile("nth(1, cert_san.uri)"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{expression => DNS, set_as_attr => <<"san_dns">>},
        #{expression => AllDNS, set_as_attr => <<"san_dns_all">>},
        #{expression => IP, set_as_attr => <<"san_ip">>},
        #{expression => IPv6, set_as_attr => <<"san_ipv6">>},
        #{expression => Email, set_as_attr => <<"san_email">>},
        #{expression => URI, set_as_attr => <<"san_uri">>}
    ]),
    try
        {ok, Client} = emqtt:start_link([
            {host, "127.0.0.1"},
            {clientid, ClientId},
            {port, Port},
            {ssl, true},
            {ssl_opts, SslOpts}
        ]),
        {ok, _} = emqtt:connect(Client),
        ChanInfo = get_chan_info(ClientId),
        ?assertMatch(
            #{
                clientinfo := #{
                    client_attrs := #{
                        <<"san_dns">> := <<"example.com">>,
                        <<"san_dns_all">> := <<"example.com,www.example.com">>,
                        <<"san_ip">> := <<"192.168.1.100">>,
                        <<"san_ipv6">> := <<"2001:db8::1">>,
                        <<"san_email">> := <<"ops@example.com">>,
                        <<"san_uri">> := <<"spiffe://example.com/client">>
                    }
                }
            },
            ChanInfo
        ),
        #{clientinfo := ClientInfo} = ChanInfo,
        ?assertNot(maps:is_key(cert_san, ClientInfo)),
        emqtt:disconnect(Client)
    after
        stop_san_listener(?FUNCTION_NAME, ListenerConf)
    end.

t_invalid_cert_san_rejected(Config) ->
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {Port, SslOpts, ListenerConf} = start_san_listener(
        ?FUNCTION_NAME,
        Config,
        [{dns, "tenant-a\r\nX-Override-Result: allow"}]
    ),
    {ok, DNS} = emqx_variform:compile("nth(1, cert_san.dns)"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{expression => DNS, set_as_attr => <<"san_dns">>}
    ]),
    try
        {ok, Client} = emqtt:start_link([
            {host, "127.0.0.1"},
            {clientid, ClientId},
            {port, Port},
            {ssl, true},
            {ssl_opts, SslOpts}
        ]),
        case catch emqtt:connect(Client) of
            {error, _} ->
                ok;
            {'EXIT', _} ->
                ok;
            {ok, _} ->
                emqtt:disconnect(Client),
                ct:fail(invalid_cert_san_accepted)
        end
    after
        stop_san_listener(?FUNCTION_NAME, ListenerConf)
    end.

t_empty_cert_san_do_not_set_attr(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {Port, SslOpts, ListenerConf} = start_san_listener(?FUNCTION_NAME, Config, []),
    {ok, DNS} = emqx_variform:compile("nth(1, cert_san.dns)"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{expression => DNS, set_as_attr => <<"san_dns">>}
    ]),
    try
        {ok, Client} = emqtt:start_link([
            {host, "127.0.0.1"},
            {clientid, ClientId},
            {port, Port},
            {ssl, true},
            {ssl_opts, SslOpts}
        ]),
        {ok, _} = emqtt:connect(Client),
        ChanInfo = get_chan_info(ClientId),
        ?assertMatch(#{clientinfo := #{client_attrs := #{}}}, ChanInfo),
        #{clientinfo := ClientInfo} = ChanInfo,
        ?assertNot(maps:is_key(cert_san, ClientInfo)),
        emqtt:disconnect(Client)
    after
        stop_san_listener(?FUNCTION_NAME, ListenerConf)
    end.

start_san_listener(Name, Config, ClientSANList) ->
    CertDir = filename:join(?config(priv_dir, Config), atom_to_list(Name)),
    ok = filelib:ensure_dir(filename:join(CertDir, "dummy")),
    CertKeyRoot = emqx_cth_tls:gen_cert(#{key => ec, issuer => root}),
    CertKeyServer = emqx_cth_tls:gen_cert(#{
        key => ec,
        issuer => CertKeyRoot,
        subject => #{name => "localhost"},
        extensions => #{subject_alt_name => [{ip, {127, 0, 0, 1}}]}
    }),
    ClientExtensions =
        case ClientSANList of
            [] -> #{};
            [_ | _] -> #{subject_alt_name => ClientSANList}
        end,
    CertKeyClient = emqx_cth_tls:gen_cert(#{
        key => ec,
        issuer => CertKeyRoot,
        subject => #{name => "san-client"},
        extensions => ClientExtensions
    }),
    {CACertFile, _} = emqx_cth_tls:write_cert(CertDir, "ca", CertKeyRoot),
    {ServerCertFile, ServerKeyFile} = emqx_cth_tls:write_cert(CertDir, "server", CertKeyServer),
    {ClientCertFile, ClientKeyFile} = emqx_cth_tls:write_cert(CertDir, "client", CertKeyClient),
    Port = emqx_common_test_helpers:select_free_port(ssl),
    ListenerConf = #{
        enable => true,
        bind => {{127, 0, 0, 1}, Port},
        mountpoint => <<>>,
        zone => default,
        tcp_options => #{active_n => 100},
        ssl_options => #{
            cacertfile => CACertFile,
            certfile => ServerCertFile,
            keyfile => ServerKeyFile,
            verify => verify_peer,
            fail_if_no_peer_cert => true
        }
    },
    ok = emqx_config:put_listener_conf(ssl, Name, [], ListenerConf),
    ok = emqx_listeners:start_listener(ssl, Name, ListenerConf),
    SslOpts =
        emqx_common_test_helpers:ssl_verify_fun_allow_any_host() ++
            [
                {cacertfile, CACertFile},
                {certfile, ClientCertFile},
                {keyfile, ClientKeyFile}
            ],
    {Port, SslOpts, ListenerConf}.

stop_san_listener(Name, ListenerConf) ->
    _ = emqx_listeners:stop_listener(ssl, Name, ListenerConf),
    ok.

get_chan_info(ClientId) ->
    ?retry(
        3_000,
        100,
        #{} = emqx_cm:get_chan_info(ClientId)
    ).
