%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_nats.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% CT Callbacks
%%--------------------------------------------------------------------

all() ->
    [{group, emqx_gateway}, {group, nats_server}].

groups() ->
    CTs = emqx_common_test_helpers:all(?MODULE),
    [
        {emqx_gateway, [], emqx_group_members()},
        {nats_server, [], nats_group_members()},
        {tcp, [], CTs},
        {ws, [], CTs},
        {wss, [], CTs},
        {ssl, [], CTs}
    ].

init_per_suite(Config) ->
    application:load(emqx_gateway_nats),
    TcpPort = tcp_port(emqx),
    WsPort = ws_port(emqx),
    WssPort = wss_port(emqx),
    SslPort = ssl_port(emqx),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, conf_default(TcpPort, WsPort, WssPort, SslPort)},
            emqx_gateway,
            emqx_auth,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    case lists:keyfind(suite_apps, 1, Config) of
        {suite_apps, _} ->
            emqx_common_test_http:delete_default_app(),
            emqx_cth_suite:stop(?config(suite_apps, Config)),
            ok;
        false ->
            ok
    end.

init_per_group(emqx_gateway, Config) ->
    [{target, emqx} | Config];
init_per_group(nats_server, Config) ->
    case ensure_nats_server_available() of
        ok ->
            [{target, nats} | Config];
        {skip, Reason} ->
            {skip, Reason}
    end;
init_per_group(tcp, Config) ->
    Target = target_from(Config),
    BaseOpts = default_tcp_client_opts(Target),
    group_config(Target, tcp, BaseOpts) ++ Config;
init_per_group(ws, Config) ->
    Target = target_from(Config),
    case ws_port(Target) of
        undefined ->
            {skip, "WS listener not configured"};
        _ ->
            BaseOpts = default_ws_client_opts(Target),
            group_config(Target, ws, BaseOpts) ++ Config
    end;
init_per_group(wss, Config) ->
    Target = target_from(Config),
    case wss_port(Target) of
        undefined ->
            {skip, "WSS listener not configured"};
        _ ->
            BaseOpts = default_wss_client_opts(Target),
            group_config(Target, wss, BaseOpts) ++ Config
    end;
init_per_group(ssl, Config) ->
    Target = target_from(Config),
    case ssl_port(Target) of
        undefined ->
            {skip, "SSL listener not configured"};
        _ ->
            BaseOpts = default_ssl_client_opts(Target),
            group_config(Target, ssl, BaseOpts) ++ Config
    end.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    case should_skip(TestCase, target_from(Config)) of
        {skip, Reason} ->
            {skip, Reason};
        ok ->
            emqx_common_test_helpers:init_per_testcase(?MODULE, TestCase, Config)
    end.

end_per_testcase(TestCase, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TestCase, Config).

%%--------------------------------------------------------------------
%% Helper Functions
%%--------------------------------------------------------------------

target() ->
    emqx.

target_from(Config) ->
    case lists:keyfind(target, 1, Config) of
        {target, Target} -> Target;
        false -> target()
    end.

emqx_group_members() ->
    [{group, tcp}, {group, ws}, {group, wss}, {group, ssl}].

nats_group_members() ->
    [{group, tcp}, {group, ws}, {group, wss}, {group, ssl}].

env_str(Key, Default) ->
    case os:getenv(Key) of
        false -> Default;
        "" -> Default;
        Val -> Val
    end.

env_int(Key, Default) ->
    case os:getenv(Key) of
        false -> Default;
        "" -> Default;
        Val -> list_to_integer(Val)
    end.

default_host(Target) ->
    default_host_for_target(Target).

default_host_for_target(emqx) ->
    "127.0.0.1";
default_host_for_target(nats) ->
    "toxiproxy".

has_scheme(Host) ->
    lists:prefix("tcp://", Host) orelse lists:prefix("ssl://", Host) orelse
        lists:prefix("ws://", Host) orelse lists:prefix("wss://", Host).

ensure_scheme(Host, Scheme) ->
    case has_scheme(Host) of
        true -> Host;
        false -> Scheme ++ "://" ++ Host
    end.

tcp_host(Target) ->
    Host0 = default_host(Target),
    ensure_scheme(Host0, "tcp").

ws_host(Target) ->
    Host0 = default_host(Target),
    ensure_scheme(Host0, "ws").

tcp_port(_Target) ->
    4222.

ws_port(Target) ->
    case Target of
        emqx -> 4223;
        nats -> 9222
    end.

wss_host(Target) ->
    Host0 = default_wss_host(Target),
    ensure_scheme(Host0, "wss").

default_wss_host(emqx) ->
    "127.0.0.1";
default_wss_host(nats) ->
    "toxiproxy".

wss_port(Target) ->
    case Target of
        emqx -> 4224;
        nats -> 9322
    end.

ssl_host(Target) ->
    Host0 = default_host(Target),
    ensure_scheme(Host0, "ssl").

ssl_port(Target) ->
    case Target of
        emqx -> 4225;
        nats -> 4422
    end.

noauth_tcp_host() ->
    Host0 = env_str("NATS_NOAUTH_TCP_HOST", "nats-noauth"),
    ensure_scheme(Host0, "tcp").

noauth_ws_host() ->
    Host0 = env_str("NATS_NOAUTH_WS_HOST", "nats-noauth"),
    ensure_scheme(Host0, "ws").

noauth_wss_host() ->
    Host0 = env_str("NATS_NOAUTH_WSS_HOST", "nats-tls-noauth"),
    ensure_scheme(Host0, "wss").

noauth_ssl_host() ->
    Host0 = env_str("NATS_NOAUTH_SSL_HOST", "nats-tls-noauth"),
    ensure_scheme(Host0, "ssl").

noauth_tcp_port() ->
    env_int("NATS_NOAUTH_TCP_PORT", 4222).

noauth_ws_port() ->
    env_int("NATS_NOAUTH_WS_PORT", 9222).

noauth_wss_port() ->
    env_int("NATS_NOAUTH_WSS_PORT", 9322).

noauth_ssl_port() ->
    env_int("NATS_NOAUTH_SSL_PORT", 4422).

default_tcp_client_opts(Target) ->
    maybe_add_nats_auth(
        Target,
        #{
            host => tcp_host(Target),
            port => tcp_port(Target),
            verbose => false
        }
    ).

default_tcp_client_opts_noauth(nats) ->
    #{
        host => noauth_tcp_host(),
        port => noauth_tcp_port(),
        verbose => false
    };
default_tcp_client_opts_noauth(emqx) ->
    default_tcp_client_opts(emqx).

default_ws_client_opts(Target) ->
    maybe_add_nats_auth(
        Target,
        #{
            host => ws_host(Target),
            port => ws_port(Target),
            verbose => false
        }
    ).

default_ws_client_opts_noauth(nats) ->
    #{
        host => noauth_ws_host(),
        port => noauth_ws_port(),
        verbose => false
    };
default_ws_client_opts_noauth(emqx) ->
    default_ws_client_opts(emqx).

default_wss_client_opts(Target) ->
    maybe_add_nats_auth(
        Target,
        #{
            host => wss_host(Target),
            port => wss_port(Target),
            verbose => false,
            ssl_opts => #{verify => verify_none}
        }
    ).

default_wss_client_opts_noauth(nats) ->
    #{
        host => noauth_wss_host(),
        port => noauth_wss_port(),
        verbose => false,
        ssl_opts => #{verify => verify_none}
    };
default_wss_client_opts_noauth(emqx) ->
    default_wss_client_opts(emqx).

default_ssl_client_opts(emqx) ->
    #{
        host => ssl_host(emqx),
        port => ssl_port(emqx),
        verbose => false,
        ssl_opts => #{verify => verify_none}
    };
default_ssl_client_opts(nats) ->
    maybe_add_nats_auth(
        nats,
        #{
            host => ssl_starttls_host(),
            port => ssl_port(nats),
            verbose => false,
            starttls => true,
            ssl_opts => #{verify => verify_none}
        }
    ).

default_ssl_client_opts_noauth(emqx) ->
    default_ssl_client_opts(emqx);
default_ssl_client_opts_noauth(nats) ->
    #{
        host => ssl_noauth_starttls_host(),
        port => noauth_ssl_port(),
        verbose => false,
        starttls => true,
        ssl_opts => #{verify => verify_none}
    }.

maybe_add_nats_auth(nats, Opts) ->
    Opts#{
        user => auth_user(),
        pass => auth_pass()
    };
maybe_add_nats_auth(emqx, Opts) ->
    Opts.

ssl_starttls_host() ->
    Host0 = default_host(nats),
    "tcp://" ++ strip_scheme(Host0).

ssl_noauth_starttls_host() ->
    Host0 = env_str("NATS_NOAUTH_SSL_HOST", "nats-tls-noauth"),
    "tcp://" ++ strip_scheme(Host0).

conf_default(TcpPort, WsPort, WssPort, SslPort) ->
    iolist_to_binary(
        [
            "\n",
            "gateway.nats {\n",
            "  default_heartbeat_interval = 2s\n",
            "  heartbeat_wait_timeout = 1s\n",
            "  protocol {\n",
            "    max_payload_size = 1024\n",
            "  }\n",
            "  listeners.tcp.default {\n",
            "    bind = ",
            integer_to_list(TcpPort),
            "\n",
            "  }\n",
            ws_section(WsPort),
            wss_section(WssPort),
            ssl_section(SslPort),
            "}\n"
        ]
    ).

ws_section(undefined) ->
    [];
ws_section(Port) ->
    [
        "  listeners.ws.default {\n",
        "    bind = ",
        integer_to_list(Port),
        "\n",
        "  }\n"
    ].

wss_section(undefined) ->
    [];
wss_section(Port) ->
    [
        "  listeners.wss.default {\n",
        "    bind = ",
        integer_to_list(Port),
        "\n",
        "    ssl_options {\n",
        "      cacertfile = \"",
        cert_path("cacert.pem"),
        "\"\n",
        "      certfile = \"",
        cert_path("cert.pem"),
        "\"\n",
        "      keyfile = \"",
        cert_path("key.pem"),
        "\"\n",
        "    }\n",
        "  }\n"
    ].

ssl_section(undefined) ->
    [];
ssl_section(Port) ->
    [
        "  listeners.ssl.default {\n",
        "    bind = ",
        integer_to_list(Port),
        "\n",
        "    ssl_options {\n",
        "      cacertfile = \"",
        cert_path("cacert.pem"),
        "\"\n",
        "      certfile = \"",
        cert_path("cert.pem"),
        "\"\n",
        "      keyfile = \"",
        cert_path("key.pem"),
        "\"\n",
        "    }\n",
        "  }\n"
    ].

cert_path(Name) ->
    filename:join([code:lib_dir(emqx), "etc", "certs", Name]).

should_skip(TestCase, nats) ->
    case lists:member(TestCase, nats_only_skips()) of
        true -> {skip, "EMQX-only or requires EMQX management/auth"};
        false -> ok
    end;
should_skip(_TestCase, emqx) ->
    ok.

nats_only_skips() ->
    [].

ensure_nats_server_available() ->
    Target = nats,
    Host = strip_scheme(tcp_host(Target)),
    Port = tcp_port(Target),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            ok;
        false ->
            ct:fail({no_nats_server, Host, Port})
    end.

strip_scheme("tcp://" ++ Host) ->
    Host;
strip_scheme("ssl://" ++ Host) ->
    Host;
strip_scheme("ws://" ++ Host) ->
    Host;
strip_scheme("wss://" ++ Host) ->
    Host;
strip_scheme(Host) ->
    Host.

enable_auth() ->
    emqx_gateway_test_utils:enable_gateway_auth(<<"nats">>).

disable_auth() ->
    emqx_gateway_test_utils:disable_gateway_auth(<<"nats">>).

create_test_user() ->
    User = #{
        user_id => auth_user(),
        password => auth_pass(),
        is_superuser => false
    },
    emqx_gateway_test_utils:add_gateway_auth_user(<<"nats">>, User).

delete_test_user() ->
    emqx_gateway_test_utils:delete_gateway_auth_user(<<"nats">>, auth_user()).

allow_pubsub_all() ->
    emqx_gateway_test_utils:update_authz_file_rule(
        <<
            "\n"
            "        {allow,all}.\n"
            "    "
        >>
    ).

deny_pubsub_all() ->
    emqx_gateway_test_utils:update_authz_file_rule(
        <<
            "\n"
            "        {deny,all}.\n"
            "    "
        >>
    ).

auth_user() ->
    <<"test_user">>.

auth_pass() ->
    <<"password">>.

authz_deny_user() ->
    <<"deny_user">>.

authz_deny_pass() ->
    <<"deny_password">>.

strip_creds(Opts) ->
    maps:remove(pass, maps:remove(user, Opts)).

auth_enabled_opts(_Target, BaseOpts) ->
    BaseOpts#{
        user => auth_user(),
        pass => auth_pass()
    }.

auth_disabled_opts(emqx, _Group, BaseOpts) ->
    strip_creds(BaseOpts);
auth_disabled_opts(nats, tcp, _BaseOpts) ->
    default_tcp_client_opts_noauth(nats);
auth_disabled_opts(nats, ws, _BaseOpts) ->
    default_ws_client_opts_noauth(nats);
auth_disabled_opts(nats, wss, _BaseOpts) ->
    default_wss_client_opts_noauth(nats);
auth_disabled_opts(nats, ssl, _BaseOpts) ->
    default_ssl_client_opts_noauth(nats).

authz_allow_opts(Target, BaseOpts) ->
    auth_enabled_opts(Target, BaseOpts).

authz_deny_opts(emqx, BaseOpts) ->
    auth_enabled_opts(emqx, BaseOpts);
authz_deny_opts(nats, BaseOpts) ->
    BaseOpts#{
        user => authz_deny_user(),
        pass => authz_deny_pass()
    }.

apply_authz_deny(Config) ->
    case target_from(Config) of
        emqx ->
            deny_pubsub_all();
        nats ->
            ok
    end.

apply_authz_allow(Config) ->
    case target_from(Config) of
        emqx ->
            allow_pubsub_all();
        nats ->
            ok
    end.

auth_setup(Config) ->
    case target_from(Config) of
        emqx ->
            safe_delete_test_user(),
            ok = enable_auth(),
            ok = create_test_user();
        nats ->
            ok
    end,
    Config.

auth_cleanup(Config) ->
    case target_from(Config) of
        emqx ->
            safe_delete_test_user(),
            _ = disable_auth(),
            ok;
        nats ->
            ok
    end,
    Config.

authz_cleanup(Config) ->
    case target_from(Config) of
        emqx ->
            allow_pubsub_all();
        nats ->
            ok
    end,
    auth_cleanup(Config).

group_config(Target, GroupName, BaseOpts) ->
    AuthEnabled = auth_enabled_opts(Target, BaseOpts),
    AuthDisabled = auth_disabled_opts(Target, GroupName, BaseOpts),
    AuthzAllow = authz_allow_opts(Target, BaseOpts),
    AuthzDeny = authz_deny_opts(Target, BaseOpts),
    [
        {client_opts, BaseOpts},
        {auth_enabled_opts, AuthEnabled},
        {auth_disabled_opts, AuthDisabled},
        {authz_allow_opts, AuthzAllow},
        {authz_deny_opts, AuthzDeny},
        {group_name, GroupName}
    ].

safe_delete_test_user() ->
    try
        delete_test_user()
    catch
        _:_ ->
            ok
    end.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connect(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:ping(Client),
    {ok, Msgs2} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_PONG}], Msgs2),
    emqx_nats_client:stop(Client).

t_verbose_mode(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),

    %% Test INFO message
    {ok, [InfoMsg]} = emqx_nats_client:receive_message(Client),
    assert_info_message(InfoMsg),

    %% Test CONNECT with verbose mode
    ok = emqx_nats_client:connect(Client),
    {ok, [ConnectAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        ConnectAck
    ),

    %% Test PING/PONG
    ok = emqx_nats_client:ping(Client),
    {ok, [PongMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_PONG
        },
        PongMsg
    ),

    %% Test SUBSCRIBE
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    {ok, [SubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        SubAck
    ),

    %% Test PUBLISH and message delivery
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello">>),
    {ok, [PubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        PubAck
    ),

    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),

    %% Test PUBLISH with reply-to
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"reply-to">>, <<"with reply">>),
    {ok, [PubAck2]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        PubAck2
    ),

    {ok, [Msg2]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo">>,
                sid := <<"sid-1">>,
                payload := <<"with reply">>
            }
        },
        Msg2
    ),

    %% Test UNSUBSCRIBE
    ok = emqx_nats_client:unsubscribe(Client, <<"sid-1">>),
    {ok, [UnsubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        UnsubAck
    ),

    emqx_nats_client:stop(Client).

t_echo_disabled_no_self_delivery(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),

    ok = emqx_nats_client:connect(Client, #{echo => false}),
    recv_ok_frame(Client),

    ok = emqx_nats_client:subscribe(Client, <<"echo.subject">>, <<"sid-1">>),
    recv_ok_frame(Client),

    ok = emqx_nats_client:publish(Client, <<"echo.subject">>, <<"payload">>),
    recv_ok_frame(Client),

    assert_no_message(Client, <<"echo.subject">>, 1000),

    emqx_nats_client:stop(Client).

t_ping_pong(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:ping(Client),
    {ok, Msgs2} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_PONG}], Msgs2),
    emqx_nats_client:stop(Client).

t_subscribe(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    emqx_nats_client:stop(Client).

t_subscribe_invalid_subject(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),

    ok = send_raw_sub(Client, <<"foo..bar">>, <<"sid-1">>),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    assert_protocol_error(Msgs),

    emqx_nats_client:stop(Client).

t_publish(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello">>),
    emqx_nats_client:stop(Client).

t_publish_wildcard_subject(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),

    ok = send_raw_pub(Client, <<"foo.*">>, <<"hello">>),
    recv_ok_frame(Client),

    emqx_nats_client:stop(Client).

t_publish_exceed_max_payload(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),

    Payload = binary:copy(<<"x">>, 2048),
    ok = emqx_nats_client:publish(Client, <<"foo">>, Payload),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    assert_protocol_error(Msgs),

    emqx_nats_client:stop(Client).

t_receive_message(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello">>),
    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),
    emqx_nats_client:stop(Client).

't_receive_message_with_wildcard_*'(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo.*">>, <<"sid-1">>),
    ok = emqx_nats_client:publish(Client, <<"foo.bar">>, <<"hello">>),
    Msg = recv_non_ping_frame(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo.bar">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),
    emqx_nats_client:stop(Client).

't_receive_message_with_wildcard_>'(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo.>">>, <<"sid-1">>),
    ok = emqx_nats_client:publish(Client, <<"foo.bar.baz">>, <<"hello">>),
    Msg = recv_non_ping_frame(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo.bar.baz">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),
    emqx_nats_client:stop(Client).

't_receive_message_with_wildcard_combined_*_>'(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"*.bar.>">>, <<"sid-1">>),
    ok = emqx_nats_client:publish(Client, <<"foo.bar.cato.delta">>, <<"hello">>),
    Msg = recv_non_ping_frame(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo.bar.cato.delta">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),
    emqx_nats_client:stop(Client).

t_unsubscribe(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    ok = emqx_nats_client:unsubscribe(Client, <<"sid-1">>),
    emqx_nats_client:stop(Client).

t_unsubscribe_with_max_msgs(Config) ->
    BaseOpts = ?config(client_opts, Config),
    ClientOpts = maps:merge(BaseOpts, #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),

    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),

    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    recv_ok_frame(Client),

    ok = emqx_nats_client:unsubscribe(Client, <<"sid-1">>, 1),
    recv_ok_frame(Client),

    {ok, Publisher} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Publisher),
    ok = emqx_nats_client:connect(Publisher),
    recv_ok_frame(Publisher),

    ok = emqx_nats_client:publish(Publisher, <<"foo">>, <<"hello1">>),
    recv_ok_frame(Publisher),

    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo">>,
                sid := <<"sid-1">>,
                payload := <<"hello1">>
            }
        },
        Msg
    ),

    ok = emqx_nats_client:publish(Publisher, <<"foo">>, <<"hello2">>),
    recv_ok_frame(Publisher),

    {ok, []} = emqx_nats_client:receive_message(Client, 1000),

    emqx_nats_client:stop(Publisher),
    emqx_nats_client:stop(Client).

t_queue_group(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Client1} = emqx_nats_client:start_link(ClientOpts),
    {ok, Client2} = emqx_nats_client:start_link(ClientOpts),
    {ok, Publisher} = emqx_nats_client:start_link(ClientOpts),

    %% Connect both clients
    {ok, [_]} = emqx_nats_client:receive_message(Client1),
    {ok, [_]} = emqx_nats_client:receive_message(Client2),
    {ok, [_]} = emqx_nats_client:receive_message(Publisher),
    ok = emqx_nats_client:connect(Client1),
    ok = emqx_nats_client:connect(Client2),
    ok = emqx_nats_client:connect(Publisher),
    {ok, [_]} = emqx_nats_client:receive_message(Client1),
    {ok, [_]} = emqx_nats_client:receive_message(Client2),
    {ok, [_]} = emqx_nats_client:receive_message(Publisher),

    %% Subscribe to the same queue group
    ok = emqx_nats_client:subscribe(Client1, <<"foo">>, <<"sid-1">>, <<"group-1">>),
    ok = emqx_nats_client:subscribe(Client2, <<"foo">>, <<"sid-2">>, <<"group-1">>),
    {ok, [_]} = emqx_nats_client:receive_message(Client1),
    {ok, [_]} = emqx_nats_client:receive_message(Client2),

    %% Publish messages via NATS
    ok = emqx_nats_client:publish(Publisher, <<"foo">>, <<"msgs1">>),
    recv_ok_frame(Publisher),
    ok = emqx_nats_client:publish(Publisher, <<"foo">>, <<"msgs2">>),
    recv_ok_frame(Publisher),

    %% Receive messages - only one client should receive each message
    {ok, Msgs1} = emqx_nats_client:receive_message(Client1),
    {ok, Msgs2} = emqx_nats_client:receive_message(Client2),

    ?assertEqual(2, length(Msgs1 ++ Msgs2)),

    emqx_nats_client:stop(Client1),
    emqx_nats_client:stop(Client2),
    emqx_nats_client:stop(Publisher).

t_queue_group_with_empty_string(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Publisher} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Publisher),

    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Publisher),
    {ok, [_]} = emqx_nats_client:receive_message(Publisher),

    %% Subscribe to the empty string queue group treated as no queue group
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>, <<>>),
    recv_ok_frame(Client),

    ok = emqx_nats_client:publish(Publisher, <<"foo">>, <<"hello">>),
    recv_ok_frame(Publisher),

    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),

    emqx_nats_client:stop(Publisher),
    emqx_nats_client:stop(Client).

t_reply_to(Config) ->
    ClientOpts = maps:merge(?config(client_opts, Config), #{verbose => true}),
    {ok, Publisher} = emqx_nats_client:start_link(ClientOpts),
    {ok, Subscriber} = emqx_nats_client:start_link(ClientOpts),

    %% Connect both clients
    {ok, [_]} = emqx_nats_client:receive_message(Publisher),
    {ok, [_]} = emqx_nats_client:receive_message(Subscriber),
    ok = emqx_nats_client:connect(Publisher),
    ok = emqx_nats_client:connect(Subscriber),
    {ok, [_]} = emqx_nats_client:receive_message(Publisher),
    {ok, [_]} = emqx_nats_client:receive_message(Subscriber),

    %% Subscribe to the subject
    ok = emqx_nats_client:subscribe(Subscriber, <<"test.subject">>, <<"sid-1">>),
    {ok, [_]} = emqx_nats_client:receive_message(Subscriber),

    %% Publish message with reply-to
    ReplyTo = <<"reply.subject">>,
    Payload = <<"test payload">>,
    ok = emqx_nats_client:publish(Publisher, <<"test.subject">>, ReplyTo, Payload),
    {ok, [_]} = emqx_nats_client:receive_message(Publisher),

    %% Receive message and verify reply-to
    {ok, [Msg]} = emqx_nats_client:receive_message(Subscriber),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"test.subject">>,
                sid := <<"sid-1">>,
                reply_to := ReplyTo,
                payload := Payload
            }
        },
        Msg
    ),

    emqx_nats_client:stop(Publisher),
    emqx_nats_client:stop(Subscriber).

t_reply_to_with_no_responders(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            verbose => true,
            no_responders => true,
            headers => true
        }
    ),

    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),

    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),

    ok = emqx_nats_client:subscribe(Client, <<"reply.*">>, <<"sid-1">>),
    recv_ok_frame(Client),

    ok = emqx_nats_client:publish(
        Client, <<"test.subject">>, <<"reply.subject">>, <<"test payload">>
    ),
    recv_ok_frame(Client),

    Msg = recv_non_ping_frame(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_HMSG,
            message = #{
                subject := <<"reply.subject">>,
                sid := <<"sid-1">>,
                headers := #{<<"code">> := 503}
            }
        },
        Msg
    ),
    emqx_nats_client:stop(Client).

t_no_responders_must_work_with_headers(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            verbose => true,
            no_responders => true,
            headers => false
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [ConnectAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR
        },
        ConnectAck
    ),
    emqx_nats_client:stop(Client).

t_auth_success(init, Config) ->
    auth_setup(Config);
t_auth_success('end', Config) ->
    auth_cleanup(Config).

t_auth_success(Config) ->
    ClientOpts = maps:merge(?config(auth_enabled_opts, Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    InfoMsg = recv_info_frame(Client),
    assert_auth_required(InfoMsg, true),

    %% Connect with credentials
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),

    %% Test basic operations after successful authentication
    ok = emqx_nats_client:ping(Client),
    {ok, [PongMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_PONG
        },
        PongMsg
    ),

    emqx_nats_client:stop(Client).

t_auth_failure(init, Config) ->
    auth_setup(Config);
t_auth_failure('end', Config) ->
    auth_cleanup(Config).

t_auth_failure(Config) ->
    ClientOpts = maps:merge(
        ?config(auth_enabled_opts, Config),
        #{pass => <<"wrong_password">>, verbose => true}
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    InfoMsg = recv_info_frame(Client),
    assert_auth_required(InfoMsg, true),

    %% Connect with wrong credentials
    ok = emqx_nats_client:connect(Client),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    assert_auth_failed(Msgs),

    emqx_nats_client:stop(Client).

t_auth_dynamic_enable_disable(init, Config) ->
    auth_cleanup(Config);
t_auth_dynamic_enable_disable('end', Config) ->
    auth_cleanup(Config).

t_auth_dynamic_enable_disable(Config) ->
    ClientOptsNoAuth = maps:merge(?config(auth_disabled_opts, Config), #{verbose => true}),
    ClientOptsAuthNoCred = maps:merge(strip_creds(?config(auth_enabled_opts, Config)), #{
        verbose => true
    }),

    %% Start with auth disabled
    {ok, Client1} = emqx_nats_client:start_link(ClientOptsNoAuth),
    InfoMsg1 = recv_info_frame(Client1),
    assert_auth_required(InfoMsg1, false),

    %% Connect without credentials (should succeed)
    ok = emqx_nats_client:connect(Client1),
    recv_ok_frame(Client1),
    emqx_nats_client:stop(Client1),

    %% Enable auth and create test user
    _ = auth_setup(Config),
    {ok, Client2} = emqx_nats_client:start_link(ClientOptsAuthNoCred),
    InfoMsg2 = recv_info_frame(Client2),
    assert_auth_required(InfoMsg2, true),

    %% Try to connect without credentials (should fail)
    ok = emqx_nats_client:connect(Client2),
    {ok, Msgs2} = emqx_nats_client:receive_message(Client2),
    assert_auth_failed(Msgs2),
    emqx_nats_client:stop(Client2),

    %% Disable auth again
    _ = auth_cleanup(Config),
    {ok, Client3} = emqx_nats_client:start_link(ClientOptsNoAuth),
    InfoMsg3 = recv_info_frame(Client3),
    assert_auth_required(InfoMsg3, false),

    %% Connect without credentials (should succeed again)
    ok = emqx_nats_client:connect(Client3),
    recv_ok_frame(Client3),
    emqx_nats_client:stop(Client3).

t_publish_authz(init, Config) ->
    auth_setup(Config);
t_publish_authz('end', Config) ->
    authz_cleanup(Config).

t_publish_authz(Config) ->
    ok = apply_authz_deny(Config),
    DenyOpts = maps:merge(?config(authz_deny_opts, Config), #{verbose => true}),
    {ok, DenyClient} = emqx_nats_client:start_link(DenyOpts),
    recv_info_frame(DenyClient),
    ok = emqx_nats_client:connect(DenyClient),
    recv_ok_frame(DenyClient),

    %% Test denied topic (should fail)
    ok = emqx_nats_client:publish(DenyClient, <<"test.topic">>, <<"test message">>),
    {ok, [ErrorMsg1]} = emqx_nats_client:receive_message(DenyClient),
    assert_permissions_violation(ErrorMsg1, publish, <<"test.topic">>),
    emqx_nats_client:stop(DenyClient),

    ok = apply_authz_allow(Config),
    AllowOpts = maps:merge(?config(authz_allow_opts, Config), #{verbose => true}),
    {ok, AllowClient} = emqx_nats_client:start_link(AllowOpts),
    recv_info_frame(AllowClient),
    ok = emqx_nats_client:connect(AllowClient),
    recv_ok_frame(AllowClient),

    %% Test allowed topic (should succeed)
    ok = emqx_nats_client:publish(AllowClient, <<"test.topic">>, <<"test message">>),
    {ok, [PubAck]} = emqx_nats_client:receive_message(AllowClient),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        PubAck
    ),

    emqx_nats_client:stop(AllowClient),
    ok.

t_subscribe_authz(init, Config) ->
    auth_setup(Config);
t_subscribe_authz('end', Config) ->
    authz_cleanup(Config).

t_subscribe_authz(Config) ->
    ok = apply_authz_deny(Config),
    DenyOpts = maps:merge(?config(authz_deny_opts, Config), #{verbose => true}),
    {ok, DenyClient} = emqx_nats_client:start_link(DenyOpts),
    recv_info_frame(DenyClient),
    ok = emqx_nats_client:connect(DenyClient),
    recv_ok_frame(DenyClient),

    %% Test denied subscription (should fail)
    ok = emqx_nats_client:subscribe(DenyClient, <<"test.topic">>, <<"sid-1">>),
    {ok, [ErrorMsg1]} = emqx_nats_client:receive_message(DenyClient),
    assert_permissions_violation(ErrorMsg1, subscribe, <<"test.topic">>),
    emqx_nats_client:stop(DenyClient),

    ok = apply_authz_allow(Config),
    AllowOpts = maps:merge(?config(authz_allow_opts, Config), #{verbose => true}),
    {ok, AllowClient} = emqx_nats_client:start_link(AllowOpts),
    recv_info_frame(AllowClient),
    ok = emqx_nats_client:connect(AllowClient),
    recv_ok_frame(AllowClient),

    %% Test allowed subscription (should succeed)
    ok = emqx_nats_client:subscribe(AllowClient, <<"test.topic">>, <<"sid-1">>),
    {ok, [SubAck]} = emqx_nats_client:receive_message(AllowClient),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        SubAck
    ),

    emqx_nats_client:stop(AllowClient),
    ok.

t_optional_connect_request(Config) ->
    ClientOpts = maps:merge(
        ?config(auth_disabled_opts, Config),
        #{
            verbose => true
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),

    emqx_nats_client:subscribe(Client, <<"test.topic">>, <<"sid-1">>),
    recv_ok_frame(Client),

    emqx_nats_client:publish(Client, <<"test.topic">>, <<"test message">>),
    recv_ok_frame(Client),

    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = _Message
        },
        Msg
    ),

    emqx_nats_client:unsubscribe(Client, <<"sid-1">>),
    recv_ok_frame(Client),

    emqx_nats_client:stop(Client).

t_optional_connect_request_only_work_authn_disabled(init, Config) ->
    auth_setup(Config);
t_optional_connect_request_only_work_authn_disabled('end', Config) ->
    auth_cleanup(Config).

t_optional_connect_request_only_work_authn_disabled(Config) ->
    ClientOpts = maps:merge(
        strip_creds(?config(auth_enabled_opts, Config)),
        #{
            verbose => true
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),

    ok = emqx_nats_client:publish(Client, <<"test.topic">>, <<"test message">>),
    {ok, [ErrorMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR,
            message = _
        },
        ErrorMsg
    ),

    emqx_nats_client:stop(Client),
    ok.

t_server_to_client_ping(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            verbose => true,
            auto_respond_ping => false
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),

    %% waiting ping message
    {ok, [PingMsg]} = emqx_nats_client:receive_message(Client, 1, 5000),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_PING
        },
        PingMsg
    ),
    %% waiting for timeout and disconnect
    {ok, [DisconnectMsg]} = emqx_nats_client:receive_message(Client, 1, 5000),
    case DisconnectMsg of
        #nats_frame{operation = ?OP_ERR} ->
            ok;
        tcp_closed ->
            ok;
        _ ->
            ?assertMatch(#nats_frame{operation = ?OP_ERR}, DisconnectMsg)
    end,
    emqx_nats_client:stop(Client).

t_invalid_frame(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            verbose => true
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),

    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    ok = emqx_nats_client:send_invalid_frame(Client, <<"invalid frame">>),
    {ok, [ErrorMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR
        },
        ErrorMsg
    ),
    emqx_nats_client:stop(Client).

%%--------------------------------------------------------------------
%% Utils

recv_ok_frame(Client) ->
    {ok, [Frame]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        Frame
    ).

recv_info_frame(Client) ->
    {ok, [Frame]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_INFO
        },
        Frame
    ),
    Frame.

recv_non_ping_frame(Client) ->
    {ok, [Frame]} = emqx_nats_client:receive_message(Client),
    case Frame of
        #nats_frame{operation = ?OP_PING} ->
            recv_non_ping_frame(Client);
        _ ->
            Frame
    end.

assert_info_message(#nats_frame{operation = ?OP_INFO, message = Message}) ->
    ?assert(is_map(Message)),
    ?assert(maps:is_key(<<"version">>, Message)),
    ?assert(maps:is_key(<<"max_payload">>, Message)).

assert_auth_required(#nats_frame{operation = ?OP_INFO, message = Message}, Expected) ->
    ?assert(is_map(Message)),
    ?assertEqual(Expected, maps:get(<<"auth_required">>, Message, false)).

assert_auth_failed(Msgs) ->
    case Msgs of
        [#nats_frame{operation = ?OP_ERR} | _] ->
            ok;
        [tcp_closed | _] ->
            ok;
        _ ->
            ?assertMatch([#nats_frame{operation = ?OP_ERR}], Msgs)
    end.

assert_protocol_error(Msgs) ->
    case
        lists:any(
            fun
                (#nats_frame{operation = ?OP_ERR}) -> true;
                (tcp_closed) -> true;
                (_) -> false
            end,
            Msgs
        )
    of
        true -> ok;
        false -> ?assertMatch([#nats_frame{operation = ?OP_ERR} | _], Msgs)
    end.

send_raw_sub(Client, Subject, Sid) ->
    Data = iolist_to_binary([
        "SUB ",
        Subject,
        " ",
        Sid,
        "\r\n"
    ]),
    emqx_nats_client:send_invalid_frame(Client, Data).

send_raw_pub(Client, Subject, Payload) ->
    PayloadSize = integer_to_list(byte_size(Payload)),
    Data = iolist_to_binary([
        "PUB ",
        Subject,
        " ",
        PayloadSize,
        "\r\n",
        Payload,
        "\r\n"
    ]),
    emqx_nats_client:send_invalid_frame(Client, Data).

assert_permissions_violation(#nats_frame{operation = ?OP_ERR, message = Msg}, Kind, Subject) ->
    Normalized = normalize_violation_msg(Msg),
    ExpectedPrefix =
        case Kind of
            publish ->
                <<"Permissions Violation for Publish to">>;
            subscribe ->
                <<"Permissions Violation for Subscription to">>
        end,
    ?assertMatch({_, _}, binary:match(Normalized, ExpectedPrefix)),
    ?assertMatch({_, _}, binary:match(Normalized, Subject));
assert_permissions_violation(Other, _Kind, _Subject) ->
    ?assertMatch(#nats_frame{operation = ?OP_ERR}, Other).

normalize_violation_msg(Msg) when is_binary(Msg) ->
    Str0 = binary_to_list(Msg),
    Str1 = string:replace(Str0, "\\\"", "\"", all),
    Str2 = string:trim(Str1, both, "'\""),
    list_to_binary(Str2);
normalize_violation_msg(Msg) ->
    Msg.

assert_no_message(Client, Subject, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    assert_no_message_loop(Client, Subject, Deadline).

assert_no_message_loop(Client, Subject, Deadline) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true ->
            ok;
        false ->
            {ok, Msgs} = emqx_nats_client:receive_message(Client, 1, 200),
            case Msgs of
                [] ->
                    assert_no_message_loop(Client, Subject, Deadline);
                [#nats_frame{operation = ?OP_MSG, message = #{subject := Subject}}] ->
                    ct:fail({unexpected_message, Subject});
                [#nats_frame{operation = ?OP_MSG, message = #{subject := Other}}] ->
                    ct:fail({unexpected_message, Other});
                [tcp_closed] ->
                    ct:fail(tcp_closed);
                [_Other] ->
                    assert_no_message_loop(Client, Subject, Deadline)
            end
    end.
