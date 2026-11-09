%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_ws_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [{group, Group} || Group <- [ws_binary, ws_text, wss_binary, wss_text]].

groups() ->
    [
        {Group, [], emqx_common_test_helpers:all(?MODULE)}
     || {group, Group} <- all()
    ].

init_per_suite(Config) ->
    %% These cases are about WebSocket framing, and connect without credentials.
    %% The default profile on this branch is `hardened', where
    %% `authn_not_configured' denies, so pin `legacy' to keep the subject of the
    %% suite its framing rather than its authentication.
    ok = emqx_common_test_helpers:set_security_profile(legacy),
    application:load(emqx_gateway_nats),
    [WsPort, WssPort] = emqx_common_test_helpers:select_free_ports(tcp, 2),
    Listeners = #{
        ws => #{default => #{bind => WsPort}},
        wss => #{
            default => #{
                bind => WssPort,
                ssl_options => #{
                    cacertfile => cert_path("cacert.pem"),
                    certfile => cert_path("cert.pem"),
                    keyfile => cert_path("key.pem")
                }
            }
        }
    },
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{config => #{gateway => #{nats => #{listeners => Listeners}}}}},
            emqx_gateway
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps}, {ws_port, WsPort}, {wss_port, WssPort} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_common_test_helpers:clear_security_profile().

init_per_group(Group, Config) ->
    {Port, Options, FrameType} =
        case Group of
            ws_binary -> {?config(ws_port, Config), #{}, binary};
            ws_text -> {?config(ws_port, Config), #{}, text};
            wss_binary -> {?config(wss_port, Config), tls_options(), binary};
            wss_text -> {?config(wss_port, Config), tls_options(), text}
        end,
    [{port, Port}, {conn_options, Options}, {frame_type, FrameType} | Config].

end_per_group(_Group, _Config) ->
    ok.

tls_options() ->
    #{transport => tls, tls_opts => [{verify, verify_none}]}.

%% `apps/emqx/etc/certs' holds only a README on this branch, so the `wss'
%% listener takes the generated test certificates, the way the other suites in
%% this application do.
cert_path(Name) ->
    emqx_common_test_helpers:test_cert(Name).

-doc "Commands span WebSocket messages.".
t_split_command(Config) ->
    with_connection(Config, fun(Conn, Stream, Type) ->
        send(Conn, Stream, Type, <<"PI">>),
        send(Conn, Stream, Type, <<"NG\r">>),
        send(Conn, Stream, Type, <<"\n">>),
        ?assertEqual(<<"PONG\r\n">>, recv(Conn, Stream)),
        ping(Conn, Stream, Type)
    end).

-doc "PUB headers, payloads, and terminators span WebSocket messages.".
t_split_payload(Config) ->
    with_connection(Config, fun(Conn, Stream, Type) ->
        send(Conn, Stream, Type, <<"SUB ws.partial 1\r\n">>),
        ping(Conn, Stream, Type),
        lists:foreach(
            fun(Fragment) -> send(Conn, Stream, Type, Fragment) end,
            [<<"PUB ws.par">>, <<"tial 5\r\nhe">>, <<"llo\r">>, <<"\n">>]
        ),
        ?assertEqual(<<"MSG ws.partial 1 5\r\nhello\r\n">>, recv(Conn, Stream)),
        ping(Conn, Stream, Type)
    end).

-doc "A complete command is dispatched before an incomplete one.".
t_complete_then_partial(Config) ->
    with_connection(Config, fun(Conn, Stream, Type) ->
        send(Conn, Stream, Type, <<"PING\r\nPI">>),
        ?assertEqual(<<"PONG\r\n">>, recv(Conn, Stream)),
        send(Conn, Stream, Type, <<"NG\r\n">>),
        ?assertEqual(<<"PONG\r\n">>, recv(Conn, Stream)),
        ping(Conn, Stream, Type)
    end).

with_connection(Config, Fun) ->
    Options = maps:merge(#{retry => 0, protocols => [http]}, ?config(conn_options, Config)),
    {ok, Conn} = gun:open("127.0.0.1", ?config(port, Config), Options),
    try
        {ok, _} = gun:await_up(Conn),
        Stream = gun:ws_upgrade(Conn, "/"),
        receive
            {gun_upgrade, Conn, Stream, [<<"websocket">>], _} ->
                ok;
            {gun_response, Conn, Stream, _, Status, Headers} ->
                ct:fail({websocket_upgrade_failed, Status, Headers});
            {gun_error, Conn, Stream, Reason} ->
                ct:fail({websocket_upgrade_failed, Reason})
        after 5000 ->
            ct:fail(websocket_upgrade_timeout)
        end,
        ?assertMatch(<<"INFO ", _/binary>>, recv(Conn, Stream)),
        Type = ?config(frame_type, Config),
        send(Conn, Stream, Type, <<"CONNECT {\"verbose\":false}\r\nPING\r\n">>),
        ?assertEqual(<<"PONG\r\n">>, recv(Conn, Stream)),
        Fun(Conn, Stream, Type)
    after
        gun:shutdown(Conn)
    end.

send(Conn, Stream, Type, Data) ->
    ok = gun:ws_send(Conn, Stream, {Type, Data}).

ping(Conn, Stream, Type) ->
    send(Conn, Stream, Type, <<"PING\r\n">>),
    ?assertEqual(<<"PONG\r\n">>, recv(Conn, Stream)).

recv(Conn, Stream) ->
    receive
        {gun_ws, Conn, Stream, {Type, Data}} when Type =:= binary; Type =:= text ->
            Data;
        {gun_ws, Conn, Stream, Close} ->
            ct:fail({unexpected_ws_frame, Close});
        {gun_down, Conn, _, Reason, _} ->
            ct:fail({connection_closed, Reason})
    after 5000 ->
        ct:fail(receive_timeout)
    end.
