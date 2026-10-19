%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_listeners_limits_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    [{group, Group} || {Group, _} <- groups()].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE) -- tls_only_cases(),
    [
        {tcp, TCs},
        {ws, TCs},
        {wss, TCs ++ tls_only_cases()},
        {ssl, tls_only_cases()}
    ].

tls_only_cases() ->
    [
        t_refused_before_tls_handshake_max_conns,
        t_refused_before_tls_handshake_max_conn_rate,
        t_tls_handshake_timeout
    ].

init_per_suite(Config0) ->
    Config = generate_tls_certs(Config0),
    WorkDir = emqx_cth_suite:work_dir(Config),
    Apps = emqx_cth_suite:start([emqx], #{work_dir => WorkDir}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_group(Group, Config) ->
    [{proto, Group} | Config].

end_per_group(_Group, Config) ->
    Config.

%% Verify "total number of connections" limit is respected per listener.
%% Connections over limit are expected to be quickly rejected.
t_max_conns(Config) ->
    Type = ?config(proto, Config),
    Name = ?FUNCTION_NAME,
    ListenerId = emqx_listeners:listener_id(Type, Name),
    MaxConns = 8,
    Port = emqx_common_test_helpers:select_free_port(tcp),
    LConf = listener_config(
        Type,
        #{
            <<"bind">> => format_bind({"127.0.0.1", Port}),
            <<"max_connections">> => MaxConns
        },
        Config
    ),
    with_listener(Type, Name, LConf, fun() ->
        %% Verify the listener is aware of the limit:
        ?assertEqual(
            MaxConns,
            emqx_listeners:max_conns(ListenerId, {{127, 0, 0, 1}, Port})
        ),
        %% Spawn `MaxConns` connections:
        Clients = [emqtt_connect("127.0.0.1", Port, Config) || _ <- lists:seq(1, MaxConns)],
        ?assertEqual(
            [pong || _ <- Clients],
            [emqtt:ping(C) || C <- Clients]
        ),
        %% One more client:
        assert_connect_refused("127.0.0.1", Port, Config),
        %% Cleanup:
        lists:foreach(fun emqtt:disconnect/1, Clients),
        %% One more client is now allowed:
        ExtraClient = emqtt_connect("127.0.0.1", Port, Config),
        pong = emqtt:ping(ExtraClient),
        ok = emqtt:disconnect(ExtraClient)
    end).

%% Verify connection rate limit is respected per listener.
%% The limit for the whole listnener, regardless of configured number of acceptors.
%% Connections exceeding specified connection rate are expected to be quickly rejected.
t_max_conn_rate(Config) ->
    Type = ?config(proto, Config),
    Name = ?FUNCTION_NAME,
    Port = emqx_common_test_helpers:select_free_port(tcp),
    LConf = listener_config(
        Type,
        #{
            <<"bind">> => format_bind({"127.0.0.1", Port}),
            <<"max_conn_rate">> => <<"5/1000ms">>
        },
        Config
    ),
    with_listener(Type, Name, LConf, fun() ->
        %% Spawn 5 connections, exhausting the rate limit:
        Clients = emqx_utils:pmap(
            fun(_) -> emqtt_connect("127.0.0.1", Port, Config) end,
            lists:seq(1, 5)
        ),
        ?assertEqual(
            [pong || _ <- Clients],
            [emqtt:ping(C) || C <- Clients]
        ),
        %% One more client:
        assert_connect_refused("127.0.0.1", Port, Config),
        %% Wait for listener to cool down:
        ok = timer:sleep(600),
        %% Few more clients should be allowed now:
        ExtraClients = emqx_utils:pmap(
            fun(_) -> emqtt_connect("127.0.0.1", Port, Config) end,
            lists:seq(1, 3)
        ),
        ?assertEqual(
            [pong || _ <- Clients],
            [emqtt:ping(C) || C <- Clients]
        ),
        %% Cleanup:
        lists:foreach(fun emqtt:disconnect/1, Clients ++ ExtraClients)
    end).

%% Verify connection rate limit takes effect as soon as it's updated.
t_max_conn_rate_update(Config) ->
    Type = ?config(proto, Config),
    Name = ?FUNCTION_NAME,
    Port = emqx_common_test_helpers:select_free_port(tcp),
    LConf = listener_config(
        Type,
        #{
            <<"bind">> => format_bind({"127.0.0.1", Port}),
            <<"max_conn_rate">> => <<"1/1s">>,
            <<"max_conn_burst">> => <<"1/1m">>
        },
        Config
    ),
    with_listener(Type, Name, LConf, fun() ->
        %% Spawn connections using the regular capacity plus configured burst capacity.
        Clients1 = emqx_utils:pmap(
            fun(_) -> emqtt_connect("127.0.0.1", Port, Config) end,
            [1, 2]
        ),
        ?assertEqual([pong], lists:usort([emqtt:ping(C) || C <- Clients1])),
        %% One more client, both rate and burst limits are exhausted:
        assert_connect_refused("127.0.0.1", Port, Config),
        %% Wait for listener to cool down:
        ok = timer:sleep(1000),
        %% One more client should be allowed now, more bursts are not allowed:
        Client2 = emqtt_connect("127.0.0.1", Port, Config),
        ?assertEqual(pong, emqtt:ping(Client2)),
        assert_connect_refused("127.0.0.1", Port, Config),
        %% Update the limit, allowing for much higher bursts:
        ?assertMatch(
            {ok, _},
            emqx:update_config(
                [listeners, Type, Name],
                {update, #{<<"max_conn_burst">> => <<"20/1m">>}}
            )
        ),
        %% Connection burst should be allowed right after:
        Clients3 = emqx_utils:pmap(
            fun(_) -> emqtt_connect("127.0.0.1", Port, Config) end,
            lists:seq(1, 10)
        ),
        ?assertEqual([pong], lists:usort([emqtt:ping(C) || C <- Clients3])),
        %% Cleanup:
        lists:foreach(fun emqtt:disconnect/1, Clients1 ++ [Client2] ++ Clients3)
    end).

%% Verify message rate limit is respected per listener connection.
t_message_rate(Config) ->
    Type = ?config(proto, Config),
    Name = ?FUNCTION_NAME,
    Port = emqx_common_test_helpers:select_free_port(tcp),
    LConf = listener_config(
        Type,
        #{
            <<"bind">> => format_bind({"127.0.0.1", Port}),
            <<"messages_rate">> => <<"1/s">>,
            <<"messages_burst">> => <<"4/m">>
        },
        Config
    ),
    with_listener(Type, Name, LConf, fun() ->
        %% Spawn 3 connections:
        Clients =
            [C1, C2, C3] = emqx_utils:pmap(
                fun(_) -> emqtt_connect("127.0.0.1", Port, Config) end,
                [1, 2, 3]
            ),
        %% Shoot messages using the regular capacity plus configured burst capacity.
        Topic = <<"t/msgrate">>,
        ok = emqx_broker:subscribe(Topic),
        _ = emqx_utils:pmap(
            fun(C) ->
                [
                    {ok, #{reason_code := ?RC_SUCCESS}} = emqtt:publish(C, Topic, Payload, qos1)
                 || Payload <- [<<"M1">>, <<"M2">>, <<"M3">>, <<"M4">>, <<"M5">>]
                ]
            end,
            [C1, C2]
        ),
        %% Each of them should hit the limit, while the third is still unlimited:
        {ok, #{reason_code := ?RC_QUOTA_EXCEEDED}} = emqtt:publish(C1, Topic, <<"X1">>, qos1),
        {ok, #{reason_code := ?RC_QUOTA_EXCEEDED}} = emqtt:publish(C2, Topic, <<"X1">>, qos1),
        {ok, #{reason_code := ?RC_SUCCESS}} = emqtt:publish(C3, Topic, <<"X1">>, qos1),
        %% Cooldown:
        ok = timer:sleep(1_200),
        %% One more message should be allowed for first 2 clients:
        {ok, #{reason_code := ?RC_SUCCESS}} = emqtt:publish(C1, Topic, <<"X2">>, qos1),
        {ok, #{reason_code := ?RC_SUCCESS}} = emqtt:publish(C2, Topic, <<"X2">>, qos1),
        {ok, #{reason_code := ?RC_SUCCESS}} = emqtt:publish(C3, Topic, <<"X2">>, qos1),
        {ok, #{reason_code := ?RC_QUOTA_EXCEEDED}} = emqtt:publish(C1, Topic, <<"X3">>, qos1),
        {ok, #{reason_code := ?RC_QUOTA_EXCEEDED}} = emqtt:publish(C2, Topic, <<"X3">>, qos1),
        {ok, #{reason_code := ?RC_SUCCESS}} = emqtt:publish(C3, Topic, <<"X3">>, qos1),
        %% Cleanup:
        lists:foreach(fun emqtt:disconnect/1, Clients)
    end).

-doc """
Verify that a TLS-based listener closes each connection that `max_connections`
refuses before the TLS handshake.
""".
t_refused_before_tls_handshake_max_conns(Config) ->
    test_refused_before_tls_handshake(
        ?FUNCTION_NAME, #{<<"max_connections">> => 1}, Config
    ).

-doc """
Verify that a TLS-based listener closes each connection that `max_conn_rate`
refuses before the TLS handshake.
""".
t_refused_before_tls_handshake_max_conn_rate(Config) ->
    test_refused_before_tls_handshake(
        ?FUNCTION_NAME, #{<<"max_conn_rate">> => <<"1/10s">>}, Config
    ).

-doc """
Verify that a TLS-based listener closes each accepted connection that does not
finish the TLS handshake in time.
""".
t_tls_handshake_timeout(Config) ->
    Type = ?config(proto, Config),
    Name = ?FUNCTION_NAME,
    Port = emqx_common_test_helpers:select_free_port(tcp),
    LConf0 = listener_config(
        Type, #{<<"bind">> => format_bind({"127.0.0.1", Port})}, Config
    ),
    LConf = emqx_utils_maps:deep_put(
        [<<"ssl_options">>, <<"handshake_timeout">>], LConf0, <<"1s">>
    ),
    ClientHello = capture_client_hello(),
    with_listener(Type, Name, LConf, fun() ->
        Baseline = count_tls_resources(),
        %% Half of the clients send a ClientHello, the others send nothing.
        %% None of them finishes the handshake. The WSS listener uses the
        %% default 15 s handshake timeout.
        Sockets = lists:map(
            fun(I) ->
                {ok, Socket} = gen_tcp:connect(
                    {127, 0, 0, 1}, Port, [binary, {active, false}]
                ),
                _ = (I rem 2 =:= 0) andalso gen_tcp:send(Socket, ClientHello),
                Socket
            end,
            lists:seq(1, 6)
        ),
        ?assertEqual(
            [closed || _ <- Sockets],
            [wait_closed(Socket) || Socket <- Sockets]
        ),
        lists:foreach(fun gen_tcp:close/1, Sockets),
        ?retry(100, 30, ?assertEqual(Baseline, count_tls_resources()))
    end).

test_refused_before_tls_handshake(Name, Limits, Config) ->
    Type = ?config(proto, Config),
    Port = emqx_common_test_helpers:select_free_port(tcp),
    LConf = listener_config(
        Type,
        Limits#{<<"bind">> => format_bind({"127.0.0.1", Port})},
        Config
    ),
    ClientHello = capture_client_hello(),
    with_listener(Type, Name, LConf, fun() ->
        %% This client takes the only connection slot, or the only token:
        Client = emqtt_connect("127.0.0.1", Port, Config),
        pong = emqtt:ping(Client),
        Baseline = count_tls_resources(),
        %% The listener refuses all of these before the TLS handshake.
        %% Half of them send a ClientHello, the others send nothing.
        Sockets = lists:map(
            fun(I) ->
                {ok, Socket} = gen_tcp:connect(
                    {127, 0, 0, 1}, Port, [binary, {active, false}]
                ),
                _ = (I rem 2 =:= 0) andalso gen_tcp:send(Socket, ClientHello),
                Socket
            end,
            lists:seq(1, 10)
        ),
        %% The listener closes each refused socket, possibly after a TLS alert.
        ?assertEqual(
            [closed || _ <- Sockets],
            [wait_closed(Socket) || Socket <- Sockets]
        ),
        lists:foreach(fun gen_tcp:close/1, Sockets),
        ?retry(100, 30, ?assertEqual(Baseline, count_tls_resources())),
        ok = emqtt:disconnect(Client)
    end).

wait_closed(Socket) ->
    case gen_tcp:recv(Socket, 0, 20_000) of
        {ok, _Data} -> wait_closed(Socket);
        {error, Reason} -> Reason
    end.

%% Count TLS connection processes and TCP ports of this node.
count_tls_resources() ->
    TLSConns = proplists:get_value(active, supervisor:count_children(tls_connection_sup)),
    TCPPorts = [P || P <- erlang:ports(), erlang:port_info(P, name) =:= {name, "tcp_inet"}],
    #{tls_connections => TLSConns, tcp_ports => length(TCPPorts)}.

capture_client_hello() ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {ip, {127, 0, 0, 1}}]),
    {ok, {_, Port}} = inet:sockname(LSock),
    {Pid, MRef} = spawn_monitor(fun() ->
        ssl:connect({127, 0, 0, 1}, Port, [{verify, verify_none}], 1000)
    end),
    {ok, Socket} = gen_tcp:accept(LSock, 1000),
    {ok, ClientHello} = gen_tcp:recv(Socket, 0, 1000),
    ok = gen_tcp:close(Socket),
    ok = gen_tcp:close(LSock),
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    end,
    ClientHello.

assert_connect_refused(Host, Port, Config) ->
    Type = ?config(proto, Config),
    try emqtt_connect(Host, Port, Config) of
        Client -> error({"Connection accepted over capacity", Client})
    catch
        error:closed when Type == tcp -> ok;
        error:{closed, _} when Type == tcp -> ok;
        error:tcp_closed when Type == tcp -> ok;
        error:{ws_upgrade_failed, closed} when Type == ws -> ok;
        error:{ws_upgrade_failed, {error, closed}} when Type == ws -> ok;
        %% The throttled listener can close the pending connection while the
        %% client writes the upgrade request. The client then reports a
        %% socket-level einval instead of closed.
        error:{ws_upgrade_failed, {error, einval}} when Type == ws -> ok;
        error:{down, {shutdown, closed}} when Type == wss -> ok
    end.

with_listener(Type, Name, Config, Then) ->
    {ok, _} = emqx:update_config([listeners, Type, Name], {create, Config}),
    try
        Then()
    after
        emqx_listeners:stop_listener(emqx_listeners:listener_id(Type, Name)),
        emqx:remove_config([listeners, Type, Name])
    end.

format_bind(Bind) ->
    iolist_to_binary(emqx_listeners:format_bind(Bind)).

emqtt_connect(Host, Port, Config) ->
    case ?config(proto, Config) of
        tcp -> emqtt_connect_tcp(Host, Port);
        ws -> emqtt_connect_ws(Host, Port);
        wss -> emqtt_connect_wss(Host, Port, client_ssl_opts(Config));
        ssl -> emqtt_connect_ssl(Host, Port, client_ssl_opts(Config))
    end.

emqtt_connect_tcp(Host, Port) ->
    emqtt_do_connect(fun emqtt:connect/1, #{
        hosts => [{Host, Port}],
        proto_ver => v5,
        connect_timeout => 1
    }).

emqtt_connect_ws(Host, Port) ->
    emqtt_do_connect(fun emqtt:ws_connect/1, #{
        hosts => [{Host, Port}],
        proto_ver => v5,
        connect_timeout => 1
    }).

emqtt_connect_ssl(Host, Port, SSLOpts) ->
    emqtt_do_connect(fun emqtt:connect/1, #{
        hosts => [{Host, Port}],
        proto_ver => v5,
        connect_timeout => 1,
        ssl => true,
        ssl_opts => SSLOpts
    }).

emqtt_connect_wss(Host, Port, SSLOpts) ->
    emqtt_do_connect(fun emqtt:ws_connect/1, #{
        hosts => [{Host, Port}],
        proto_ver => v5,
        connect_timeout => 1,
        ws_transport_options => [
            {protocols, [http]},
            {transport, tls},
            {tls_opts, SSLOpts}
        ]
    }).

emqtt_do_connect(Connect, Opts) ->
    case emqtt:start_link(Opts) of
        {ok, Client} ->
            true = erlang:unlink(Client),
            case Connect(Client) of
                {ok, _} -> Client;
                {error, Reason} -> error(Reason, [Opts])
            end;
        {error, Reason} ->
            error(Reason, [Opts])
    end.

generate_tls_certs(Config) ->
    PrivDir = ?config(priv_dir, Config),
    CertDir = filename:join(PrivDir, "tls"),
    ok = file:make_dir(CertDir),
    CertKeyRoot = emqx_cth_tls:gen_cert(#{key => ec, issuer => root}),
    CertKeyServer = emqx_cth_tls:gen_cert(#{
        key => ec,
        issuer => CertKeyRoot,
        extensions => #{subject_alt_name => [{ip, {127, 0, 0, 1}}]}
    }),
    {CertfileCA, _} = emqx_cth_tls:write_cert(CertDir, CertKeyRoot),
    {Certfile, Keyfile} = emqx_cth_tls:write_cert(CertDir, CertKeyServer),
    [
        {suite_tls_certs, #{
            cacertfile => CertfileCA,
            certfile => Certfile,
            keyfile => Keyfile
        }}
        | Config
    ].

listener_config(tcp, Config, _CTConfig) ->
    Config;
listener_config(ws, Config, _CTConfig) ->
    Config;
listener_config(Type, Config, CTConfig) when Type =:= wss; Type =:= ssl ->
    Certs = ?config(suite_tls_certs, CTConfig),
    SSLOpts = emqx_utils_maps:binary_key_map(Certs),
    Config#{
        <<"ssl_options">> => SSLOpts#{
            <<"verify">> => <<"verify_none">>
        }
    }.

client_ssl_opts(CTConfig) ->
    #{cacertfile := CertfileCA} = ?config(suite_tls_certs, CTConfig),
    [
        {cacertfile, CertfileCA},
        {verify, verify_peer}
    ].
