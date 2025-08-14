%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_listeners_limits_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [{group, Group} || {Group, _} <- groups()].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [{Proto, TCs} || Proto <- [tcp, ws, wss]].

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
            <<"acceptors">> => 1,
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

t_max_conn_rate(Config) ->
    Type = ?config(proto, Config),
    Name = ?FUNCTION_NAME,
    Port = emqx_common_test_helpers:select_free_port(tcp),
    LConf = listener_config(
        Type,
        #{
            <<"bind">> => format_bind({"127.0.0.1", Port}),
            <<"acceptors">> => 1,
            <<"max_conn_rate">> => <<"5/500ms">>
        },
        Config
    ),
    with_listener(Type, Name, LConf, fun() ->
        %% Spawn 5 connections, exhausting the rate limit:
        Clients = [emqtt_connect("127.0.0.1", Port, Config) || _ <- lists:seq(1, 5)],
        ?assertEqual(
            [pong || _ <- Clients],
            [emqtt:ping(C) || C <- Clients]
        ),
        %% One more client:
        assert_connect_refused("127.0.0.1", Port, Config),
        %% Wait for listener to cool down:
        ok = timer:sleep(500),
        %% Few more clients should be allowed now:
        ExtraClients = [emqtt_connect("127.0.0.1", Port, Config) || _ <- lists:seq(1, 5)],
        ?assertEqual(
            [pong || _ <- Clients],
            [emqtt:ping(C) || C <- Clients]
        ),
        %% Cleanup:
        lists:foreach(fun emqtt:disconnect/1, Clients ++ ExtraClients)
    end).

assert_connect_refused(Host, Port, Config) ->
    Type = ?config(proto, Config),
    try emqtt_connect(Host, Port, Config) of
        Client -> error({"Connection accepted over capacity", Client})
    catch
        error:{tcp_closed, _} when Type == tcp -> ok;
        error:{ws_upgrade_failed, closed} when Type == ws -> ok;
        error:timeout when Type == wss -> ok
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
        wss -> emqtt_connect_wss(Host, Port, client_ssl_opts(Config))
    end.

emqtt_connect_tcp(Host, Port) ->
    emqtt_do_connect(fun emqtt:connect/1, #{
        hosts => [{Host, Port}],
        connect_timeout => 1
    }).

emqtt_connect_ws(Host, Port) ->
    emqtt_do_connect(fun emqtt:ws_connect/1, #{
        hosts => [{Host, Port}],
        connect_timeout => 1
    }).

emqtt_connect_wss(Host, Port, SSLOpts) ->
    emqtt_do_connect(fun emqtt:ws_connect/1, #{
        hosts => [{Host, Port}],
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
listener_config(wss, Config, CTConfig) ->
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
