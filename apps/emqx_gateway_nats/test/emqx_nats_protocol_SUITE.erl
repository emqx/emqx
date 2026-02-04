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
    CoreTCs = core_tests(CTs),
    [
        {emqx_gateway, [], emqx_group_members()},
        {nats_server, [], nats_group_members()},
        {tcp_all, [], CTs},
        {ws_all, [], CTs},
        {wss_all, [], CTs},
        {ssl_all, [], CTs},
        {tcp_core, [], CoreTCs},
        {ws_core, [], CoreTCs},
        {wss_core, [], CoreTCs},
        {ssl_core, [], CoreTCs}
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
init_per_group(tcp_all, Config) ->
    Target = target_from(Config),
    [{client_opts, default_tcp_client_opts(Target)}, {group_name, tcp} | Config];
init_per_group(tcp_core, Config) ->
    Target = target_from(Config),
    [{client_opts, default_tcp_client_opts(Target)}, {group_name, tcp} | Config];
init_per_group(ws_all, Config) ->
    Target = target_from(Config),
    case ws_port(Target) of
        undefined ->
            {skip, "WS listener not configured"};
        _ ->
            [{client_opts, default_ws_client_opts(Target)}, {group_name, ws} | Config]
    end;
init_per_group(ws_core, Config) ->
    Target = target_from(Config),
    case ws_port(Target) of
        undefined ->
            {skip, "WS listener not configured"};
        _ ->
            [{client_opts, default_ws_client_opts(Target)}, {group_name, ws} | Config]
    end;
init_per_group(wss_all, Config) ->
    Target = target_from(Config),
    case wss_port(Target) of
        undefined ->
            {skip, "WSS listener not configured"};
        _ ->
            [{client_opts, default_wss_client_opts(Target)}, {group_name, wss} | Config]
    end;
init_per_group(wss_core, Config) ->
    Target = target_from(Config),
    case wss_port(Target) of
        undefined ->
            {skip, "WSS listener not configured"};
        _ ->
            [{client_opts, default_wss_client_opts(Target)}, {group_name, wss} | Config]
    end;
init_per_group(ssl_all, Config) ->
    Target = target_from(Config),
    case ssl_port(Target) of
        undefined ->
            {skip, "SSL listener not configured"};
        _ ->
            [{client_opts, default_ssl_client_opts(Target)}, {group_name, ssl} | Config]
    end;
init_per_group(ssl_core, Config) ->
    Target = target_from(Config),
    case ssl_port(Target) of
        undefined ->
            {skip, "SSL listener not configured"};
        _ ->
            [{client_opts, default_ssl_client_opts(Target)}, {group_name, ssl} | Config]
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
    [{group, tcp_all}, {group, ws_all}, {group, wss_all}, {group, ssl_all}].

nats_group_members() ->
    [{group, tcp_core}, {group, ws_core}, {group, wss_core}, {group, ssl_core}].

core_tests(AllTCs) ->
    [TC || TC <- AllTCs, not lists:member(TC, nats_only_skips())].

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
    case os:getenv("NATS_HOST") of
        false -> default_host_for_target(Target);
        "" -> default_host_for_target(Target);
        Val -> Val
    end.

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
    Host0 = env_str("NATS_TCP_HOST", default_host(Target)),
    ensure_scheme(Host0, "tcp").

ws_host(Target) ->
    Host0 = env_str("NATS_WS_HOST", default_host(Target)),
    ensure_scheme(Host0, "ws").

tcp_port(_Target) ->
    env_int("NATS_TCP_PORT", 4222).

ws_port(Target) ->
    case os:getenv("NATS_WS_PORT") of
        false ->
            case Target of
                emqx -> 4223;
                nats -> 9222
            end;
        "" ->
            case Target of
                emqx -> 4223;
                nats -> 9222
            end;
        "0" ->
            undefined;
        Val ->
            list_to_integer(Val)
    end.

wss_host(Target) ->
    Host0 = env_str("NATS_WSS_HOST", default_wss_host(Target)),
    ensure_scheme(Host0, "wss").

default_wss_host(emqx) ->
    "127.0.0.1";
default_wss_host(nats) ->
    "toxiproxy".

wss_port(Target) ->
    case os:getenv("NATS_WSS_PORT") of
        false ->
            case Target of
                emqx -> 4224;
                nats -> 9322
            end;
        "" ->
            case Target of
                emqx -> 4224;
                nats -> 9322
            end;
        "0" ->
            undefined;
        Val ->
            list_to_integer(Val)
    end.

ssl_host(Target) ->
    Host0 = env_str("NATS_SSL_HOST", default_host(Target)),
    ensure_scheme(Host0, "ssl").

ssl_port(Target) ->
    case os:getenv("NATS_SSL_PORT") of
        false ->
            case Target of
                emqx -> 4225;
                nats -> 4422
            end;
        "" ->
            case Target of
                emqx -> 4225;
                nats -> 4422
            end;
        "0" ->
            undefined;
        Val ->
            list_to_integer(Val)
    end.

default_tcp_client_opts(Target) ->
    maybe_add_nats_auth(
        Target,
        #{
            host => tcp_host(Target),
            port => tcp_port(Target),
            verbose => false
        }
    ).

default_ws_client_opts(Target) ->
    maybe_add_nats_auth(
        Target,
        #{
            host => ws_host(Target),
            port => ws_port(Target),
            verbose => false
        }
    ).

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

maybe_add_nats_auth(nats, Opts) ->
    Opts#{
        user => <<"test_user">>,
        pass => <<"password">>
    };
maybe_add_nats_auth(emqx, Opts) ->
    Opts.

ssl_starttls_host() ->
    Host0 = env_str("NATS_SSL_HOST", default_host(nats)),
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
    [
        t_auth_dynamic_enable_disable,
        t_publish_authz,
        t_subscribe_authz,
        t_optional_connect_request,
        t_optional_connect_request_only_work_authn_disabled,
        t_server_to_client_ping
    ].

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
        user_id => <<"test_user">>,
        password => <<"password">>,
        is_superuser => false
    },
    emqx_gateway_test_utils:add_gateway_auth_user(<<"nats">>, User).

delete_test_user() ->
    emqx_gateway_test_utils:delete_gateway_auth_user(<<"nats">>, <<"test_user">>).

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

auth_client_opts(Config, Pass) ->
    maps:merge(
        ?config(client_opts, Config),
        #{
            user => auth_user(),
            pass => Pass,
            verbose => true
        }
    ).

client_opts_no_creds(Config) ->
    maps:remove(pass, maps:remove(user, ?config(client_opts, Config))).

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

t_publish(Config) ->
    ClientOpts = ?config(client_opts, Config),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello">>),
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
    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
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
    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
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
    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
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

    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
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
    ClientOpts = auth_client_opts(Config, auth_pass()),
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
    ClientOpts = auth_client_opts(Config, <<"wrong_password">>),
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
    ClientOptsNoCred = maps:merge(client_opts_no_creds(Config), #{verbose => true}),

    %% Start with auth disabled
    {ok, Client1} = emqx_nats_client:start_link(ClientOptsNoCred),
    InfoMsg1 = recv_info_frame(Client1),
    assert_auth_required(InfoMsg1, false),

    %% Connect without credentials (should succeed)
    ok = emqx_nats_client:connect(Client1),
    recv_ok_frame(Client1),
    emqx_nats_client:stop(Client1),

    %% Enable auth and create test user
    ok = enable_auth(),
    ok = create_test_user(),
    {ok, Client2} = emqx_nats_client:start_link(ClientOptsNoCred),
    InfoMsg2 = recv_info_frame(Client2),
    assert_auth_required(InfoMsg2, true),

    %% Try to connect without credentials (should fail)
    ok = emqx_nats_client:connect(Client2),
    {ok, Msgs2} = emqx_nats_client:receive_message(Client2),
    assert_auth_failed(Msgs2),
    emqx_nats_client:stop(Client2),

    %% Disable auth again
    ok = delete_test_user(),
    ok = disable_auth(),
    {ok, Client3} = emqx_nats_client:start_link(ClientOptsNoCred),
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
    %% Enable authorization with deny all first
    ok = deny_pubsub_all(),

    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => auth_user(),
            pass => auth_pass(),
            verbose => true
        }
    ),

    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    %% Test denied topic (should fail)
    ok = emqx_nats_client:publish(Client, <<"test.topic">>, <<"test message">>),
    {ok, [ErrorMsg1]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR,
            message = <<"Permissions Violation for Publish to test.topic">>
        },
        ErrorMsg1
    ),

    %% Allow all publish operations
    ok = allow_pubsub_all(),

    %% Test allowed topic (should succeed)
    ok = emqx_nats_client:publish(Client, <<"test.topic">>, <<"test message">>),
    {ok, [PubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        PubAck
    ),

    emqx_nats_client:stop(Client),
    ok.

t_subscribe_authz(init, Config) ->
    auth_setup(Config);
t_subscribe_authz('end', Config) ->
    authz_cleanup(Config).

t_subscribe_authz(Config) ->
    %% Enable authorization with deny all first
    ok = deny_pubsub_all(),

    ClientOpts = maps:merge(
        ?config(client_opts, Config),
        #{
            user => auth_user(),
            pass => auth_pass(),
            verbose => true
        }
    ),

    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [_]} = emqx_nats_client:receive_message(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [_]} = emqx_nats_client:receive_message(Client),

    %% Test denied subscription (should fail)
    ok = emqx_nats_client:subscribe(Client, <<"test.topic">>, <<"sid-1">>),
    {ok, [ErrorMsg1]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR,
            message = <<"Permissions Violation for Subscription to test.topic">>
        },
        ErrorMsg1
    ),

    %% Allow all subscribe operations
    ok = allow_pubsub_all(),

    %% Test allowed subscription (should succeed)
    ok = emqx_nats_client:subscribe(Client, <<"test.topic">>, <<"sid-1">>),
    {ok, [SubAck]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_OK
        },
        SubAck
    ),

    emqx_nats_client:stop(Client),
    ok.

t_optional_connect_request(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
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
    case target_from(Config) of
        emqx ->
            ok = enable_auth(),
            Config;
        nats ->
            Config
    end;
t_optional_connect_request_only_work_authn_disabled('end', Config) ->
    auth_cleanup(Config).

t_optional_connect_request_only_work_authn_disabled(Config) ->
    ClientOpts = maps:merge(
        ?config(client_opts, Config),
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
    timer:sleep(2000),
    {ok, [PingMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_PING
        },
        PingMsg
    ),
    %% waiting for timeout and disconnect
    timer:sleep(1500),
    {ok, [DisconnectMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_ERR
        },
        DisconnectMsg
    ),
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
