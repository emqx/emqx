%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exproto_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(
    emqx_exproto_echo_svr,
    [
        frame_connect/2,
        frame_connack/1,
        frame_publish/3,
        frame_raw_publish/3,
        frame_puback/1,
        frame_subscribe/2,
        frame_suback/1,
        frame_unsubscribe/1,
        frame_unsuback/1,
        frame_disconnect/0
    ]
).

-define(TCPOPTS, [binary, {active, false}]).
-define(DTLSOPTS, [binary, {active, false}, {protocol, dtls}]).

%%--------------------------------------------------------------------
-define(CONF_DEFAULT, <<
    "\n"
    "gateway.exproto {\n"
    "  server.bind = 9100,\n"
    "  handler.address = \"http://127.0.0.1:9001\"\n"
    "  handler.service_name = \"ConnectionHandler\"\n"
    "  listeners.tcp.default {\n"
    "    bind = 7993,\n"
    "    acceptors = 8\n"
    "  }\n"
    "}\n"
>>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [
        {group, tcp_listener},
        {group, ssl_listener},
        {group, udp_listener},
        {group, dtls_listener},
        {group, https_grpc_server},
        {group, streaming_connection_handler},
        {group, hostname_grpc_server}
    ].

suite() ->
    [{timetrap, {seconds, 30}}].

groups() ->
    MainCases = [
        t_keepalive_timeout,
        t_mountpoint_echo,
        t_raw_publish,
        t_auth_deny,
        t_acl_deny,
        t_auth_expire,
        t_hook_connected_disconnected,
        t_hook_session_subscribed_unsubscribed,
        t_hook_message_delivered
    ],
    [
        {tcp_listener, [sequence], MainCases},
        {ssl_listener, [sequence], MainCases},
        {udp_listener, [sequence], MainCases},
        {dtls_listener, [sequence], MainCases},
        {streaming_connection_handler, [sequence], MainCases},
        {https_grpc_server, [sequence], MainCases},
        {hostname_grpc_server, [sequence], MainCases}
    ].

init_per_group(GrpName, Cfg) when
    GrpName == tcp_listener;
    GrpName == ssl_listener;
    GrpName == udp_listener;
    GrpName == dtls_listener
->
    LisType =
        case GrpName of
            tcp_listener -> tcp;
            ssl_listener -> ssl;
            udp_listener -> udp;
            dtls_listener -> dtls
        end,
    init_per_group(GrpName, LisType, 'ConnectionUnaryHandler', http, Cfg);
init_per_group(https_grpc_server = GrpName, Cfg) ->
    init_per_group(GrpName, tcp, 'ConnectionUnaryHandler', https, Cfg);
init_per_group(streaming_connection_handler = GrpName, Cfg) ->
    init_per_group(GrpName, tcp, 'ConnectionHandler', http, Cfg);
init_per_group(GrpName, Cfg) ->
    init_per_group(GrpName, tcp, 'ConnectionUnaryHandler', http, Cfg).

init_per_group(GrpName, LisType, ServiceName, Scheme, Cfg) ->
    Svrs = emqx_exproto_echo_svr:start(Scheme),
    Addrs = lists:flatten(
        io_lib:format("~s://~s:9001", [
            Scheme,
            case GrpName of
                hostname_grpc_server ->
                    "localhost";
                _ ->
                    "127.0.0.1"
            end
        ])
    ),
    GWConfig = #{
        server => #{bind => 9100},
        idle_timeout => 5000,
        mountpoint => <<"ct/">>,
        handler => #{
            address => Addrs,
            service_name => ServiceName,
            ssl_options => #{enable => Scheme == https}
        },
        listeners => listener_confs(LisType)
    },
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_auth,
            {emqx_gateway, #{
                config =>
                    #{gateway => #{exproto => GWConfig}}
            }},
            emqx_gateway_exproto
        ],
        #{work_dir => emqx_cth_suite:work_dir(Cfg)}
    ),
    [
        {servers, Svrs},
        {apps, Apps},
        {listener_type, LisType},
        {service_name, ServiceName},
        {grpc_client_scheme, Scheme}
        | Cfg
    ].

end_per_group(_, Cfg) ->
    ok = emqx_cth_suite:stop(proplists:get_value(apps, Cfg)),
    emqx_exproto_echo_svr:stop(proplists:get_value(servers, Cfg)).

init_per_testcase(TestCase, Cfg) when
    TestCase == t_enter_passive_mode
->
    snabbkaffe:start_trace(),
    case proplists:get_value(listener_type, Cfg) of
        udp -> {skip, ignore};
        _ -> Cfg
    end;
init_per_testcase(_TestCase, Cfg) ->
    snabbkaffe:start_trace(),
    Cfg.

end_per_testcase(_TestCase, _Cfg) ->
    snabbkaffe:stop(),
    ok.

listener_confs(Type) ->
    Default = #{
        bind => 7993,
        max_connections => 64,
        access_rules => ["allow all"]
    },
    #{Type => #{'default' => maps:merge(Default, socketopts(Type))}}.

default_config() ->
    ?CONF_DEFAULT.

%%--------------------------------------------------------------------
%% Tests cases
%%--------------------------------------------------------------------

t_mountpoint_echo(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => <<"test_client_1">>,
        %% deperated since v5.1.0, and this value will be ignored
        mountpoint => <<"deperated/">>
    },
    Password = <<"123456">>,

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    SubBin = frame_subscribe(<<"t/dn">>, 1),
    SubAckBin = frame_suback(0),

    send(Sock, SubBin),
    {ok, SubAckBin} = recv(Sock, 5000),

    emqx:publish(emqx_message:make(<<"ct/t/dn">>, <<"echo">>)),
    PubBin1 = frame_publish(<<"t/dn">>, 0, <<"echo">>),
    {ok, PubBin1} = recv(Sock, 5000),

    PubBin2 = frame_publish(<<"t/up">>, 0, <<"echo">>),
    PubAckBin = frame_puback(0),

    emqx:subscribe(<<"ct/t/up">>),

    send(Sock, PubBin2),
    {ok, PubAckBin} = recv(Sock, 5000),

    receive
        {deliver, _, _} -> ok
    after 1000 ->
        error(echo_not_running)
    end,
    close(Sock).

t_raw_publish(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => <<"test_client_1">>,
        mountpoint => <<>>
    },
    Password = <<"123456">>,

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    PubBin2 = frame_raw_publish(<<"t/up">>, 0, <<"echo">>),
    PubAckBin = frame_puback(0),

    %% mountpoint is not used in raw publish
    emqx:subscribe(<<"t/up">>),

    send(Sock, PubBin2),
    {ok, PubAckBin} = recv(Sock, 5000),

    receive
        {deliver, _, _} -> ok
    after 1000 ->
        error(echo_not_running)
    end,
    close(Sock).

t_auth_deny(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => <<"test_client_1">>
    },
    Password = <<"123456">>,

    ok = meck:new(emqx_gateway_ctx, [passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_gateway_ctx,
        authenticate,
        fun(_, _) -> {error, ?RC_NOT_AUTHORIZED} end
    ),

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(1),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    SockType =/= udp andalso
        begin
            {error, closed} = recv(Sock, 5000)
        end,
    meck:unload([emqx_gateway_ctx]).

t_auth_expire(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => <<"test_client_1">>
    },
    Password = <<"123456">>,

    ok = meck:new(emqx_access_control, [passthrough, no_history]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) ->
            {ok, #{is_superuser => false, expire_at => erlang:system_time(millisecond) + 500}}
        end
    ),

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    ?assertWaitEvent(
        begin
            send(Sock, ConnBin),
            {ok, ConnAckBin} = recv(Sock, 5000)
        end,
        #{
            ?snk_kind := conn_process_terminated,
            clientid := <<"test_client_1">>,
            reason := {shutdown, expired}
        },
        5000
    ).

t_acl_deny(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => <<"test_client_1">>
    },
    Password = <<"123456">>,

    ok = meck:new(emqx_gateway_ctx, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_gateway_ctx, authorize, fun(_, _, _, _) -> deny end),

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    SubBin = frame_subscribe(<<"t/#">>, 1),
    SubAckBin = frame_suback(1),

    send(Sock, SubBin),
    {ok, SubAckBin} = recv(Sock, 5000),

    emqx:publish(emqx_message:make(<<"ct/t/dn">>, <<"echo">>)),

    PubBin = frame_publish(<<"t/dn">>, 0, <<"echo">>),
    PubBinFailedAck = frame_puback(1),
    PubBinSuccesAck = frame_puback(0),

    send(Sock, PubBin),
    {ok, PubBinFailedAck} = recv(Sock, 5000),

    meck:unload([emqx_gateway_ctx]),

    send(Sock, PubBin),
    {ok, PubBinSuccesAck} = recv(Sock, 5000),
    close(Sock).

t_keepalive_timeout(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    ClientId1 = <<"keepalive_test_client1">>,
    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => ClientId1,
        keepalive => 5
    },
    Password = <<"123456">>,

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock),

    case SockType of
        udp ->
            %% another udp client should not affect the first
            %% udp client keepalive check
            timer:sleep(4000),
            Sock2 = open(SockType),
            ConnBin2 = frame_connect(
                Client#{clientid => <<"keepalive_test_client2">>},
                Password
            ),
            send(Sock2, ConnBin2),
            %% first client will be keepalive timeouted in 6s
            ?assertMatch(
                {ok, #{
                    clientid := ClientId1,
                    reason := {shutdown, keepalive_timeout}
                }},
                ?block_until(#{?snk_kind := conn_process_terminated}, 8000)
            );
        _ ->
            ?assertMatch(
                {ok, #{
                    clientid := ClientId1,
                    reason := {shutdown, keepalive_timeout}
                }},
                ?block_until(#{?snk_kind := conn_process_terminated}, 12000)
            ),
            Trace = snabbkaffe:collect_trace(),
            %% conn process should be terminated
            ?assertEqual(1, length(?of_kind(conn_process_terminated, Trace))),
            %% socket port should be closed
            ?assertEqual({error, closed}, recv(Sock, 5000))
    end.

t_hook_connected_disconnected(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => <<"test_client_1">>
    },
    Password = <<"123456">>,

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    Parent = self(),
    emqx_hooks:add('client.connect', {?MODULE, hook_fun0, [Parent]}, 1000),
    emqx_hooks:add('client.connected', {?MODULE, hook_fun1, [Parent]}, 1000),
    emqx_hooks:add('client.disconnected', {?MODULE, hook_fun2, [Parent]}, 1000),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    receive
        connect -> ok
    after 1000 ->
        error(hook_is_not_running)
    end,

    receive
        connected -> ok
    after 1000 ->
        error(hook_is_not_running)
    end,

    DisconnectBin = frame_disconnect(),
    send(Sock, DisconnectBin),

    receive
        disconnected -> ok
    after 1000 ->
        error(hook_is_not_running)
    end,

    SockType =/= udp andalso
        begin
            {error, closed} = recv(Sock, 5000)
        end,
    emqx_hooks:del('client.connect', {?MODULE, hook_fun0}),
    emqx_hooks:del('client.connected', {?MODULE, hook_fun1}),
    emqx_hooks:del('client.disconnected', {?MODULE, hook_fun2}).

t_hook_session_subscribed_unsubscribed(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => <<"test_client_1">>
    },
    Password = <<"123456">>,

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    Parent = self(),
    emqx_hooks:add('session.subscribed', {?MODULE, hook_fun3, [Parent]}, 1000),
    emqx_hooks:add('session.unsubscribed', {?MODULE, hook_fun4, [Parent]}, 1000),

    SubBin = frame_subscribe(<<"t/#">>, 1),
    SubAckBin = frame_suback(0),

    send(Sock, SubBin),
    {ok, SubAckBin} = recv(Sock, 5000),

    receive
        subscribed -> ok
    after 1000 ->
        error(hook_is_not_running)
    end,

    UnsubBin = frame_unsubscribe(<<"t/#">>),
    UnsubAckBin = frame_unsuback(0),

    send(Sock, UnsubBin),
    {ok, UnsubAckBin} = recv(Sock, 5000),

    receive
        unsubscribed -> ok
    after 1000 ->
        error(hook_is_not_running)
    end,

    send(Sock, frame_disconnect()),

    close(Sock),
    emqx_hooks:del('session.subscribed', {?MODULE, hook_fun3}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, hook_fun4}).

t_hook_message_delivered(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => <<"test_client_1">>
    },
    Password = <<"123456">>,

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    SubBin = frame_subscribe(<<"t/#">>, 1),
    SubAckBin = frame_suback(0),

    send(Sock, SubBin),
    {ok, SubAckBin} = recv(Sock, 5000),

    emqx_hooks:add('message.delivered', {?MODULE, hook_fun5, []}, 1000),

    emqx:publish(emqx_message:make(<<"ct/t/dn">>, <<"1">>)),
    PubBin1 = frame_publish(<<"t/dn">>, 0, <<"2">>),
    {ok, PubBin1} = recv(Sock, 5000),

    close(Sock),
    emqx_hooks:del('message.delivered', {?MODULE, hook_fun5}).

t_idle_timeout(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    %% need to create udp client by sending something
    case SockType of
        udp ->
            %% nothing to do
            ok = meck:new(emqx_exproto_gcli, [passthrough, no_history]),
            ok = meck:expect(
                emqx_exproto_gcli,
                async_call,
                fun(FunName, _Req, _GClient) ->
                    self() ! {hreply, FunName, ok},
                    ok
                end
            ),
            %% send request, but nobody can respond to it
            ClientId = <<"idle_test_client1">>,
            Client = #{
                proto_name => <<"demo">>,
                proto_ver => <<"v0.1">>,
                clientid => ClientId,
                keepalive => 5
            },
            Password = <<"123456">>,
            ConnBin = frame_connect(Client, Password),
            send(Sock, ConnBin),
            ?assertMatch(
                {ok, #{reason := {shutdown, idle_timeout}}},
                ?block_until(#{?snk_kind := conn_process_terminated}, 10000)
            ),
            ok = meck:unload(emqx_exproto_gcli);
        _ ->
            ?assertMatch(
                {ok, #{reason := {shutdown, idle_timeout}}},
                ?block_until(#{?snk_kind := conn_process_terminated}, 10000)
            )
    end.

%%--------------------------------------------------------------------
%% Utils

hook_fun0(_, _, Parent) ->
    Parent ! connect,
    ok.

hook_fun1(_, _, Parent) ->
    Parent ! connected,
    ok.
hook_fun2(_, _, _, Parent) ->
    Parent ! disconnected,
    ok.

hook_fun3(_, _, _, Parent) ->
    Parent ! subscribed,
    ok.
hook_fun4(_, _, _, Parent) ->
    Parent ! unsubscribed,
    ok.

hook_fun5(_, Msg) -> {ok, Msg#message{payload = <<"2">>}}.

rand_bytes() ->
    crypto:strong_rand_bytes(rand:uniform(256)).

%%--------------------------------------------------------------------
%% Sock funcs

open(tcp) ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", 7993, ?TCPOPTS),
    {tcp, Sock};
open(udp) ->
    {ok, Sock} = gen_udp:open(0, ?TCPOPTS),
    {udp, Sock};
open(ssl) ->
    SslOpts = client_ssl_opts(),
    {ok, SslSock} = ssl:connect("127.0.0.1", 7993, ?TCPOPTS ++ SslOpts),
    {ssl, SslSock};
open(dtls) ->
    SslOpts = client_ssl_opts(),
    {ok, SslSock} = ssl:connect("127.0.0.1", 7993, ?DTLSOPTS ++ SslOpts),
    {dtls, SslSock}.

send({tcp, Sock}, Bin) ->
    gen_tcp:send(Sock, Bin);
send({udp, Sock}, Bin) ->
    gen_udp:send(Sock, "127.0.0.1", 7993, Bin);
send({ssl, Sock}, Bin) ->
    ssl:send(Sock, Bin);
send({dtls, Sock}, Bin) ->
    ssl:send(Sock, Bin).

recv(Sock) ->
    recv(Sock, infinity).

recv({tcp, Sock}, Ts) ->
    gen_tcp:recv(Sock, 0, Ts);
recv({udp, Sock}, Ts) ->
    {ok, {_, _, Bin}} = gen_udp:recv(Sock, 0, Ts),
    {ok, Bin};
recv({ssl, Sock}, Ts) ->
    ssl:recv(Sock, 0, Ts);
recv({dtls, Sock}, Ts) ->
    ssl:recv(Sock, 0, Ts).

close({tcp, Sock}) ->
    gen_tcp:close(Sock);
close({udp, Sock}) ->
    gen_udp:close(Sock);
close({ssl, Sock}) ->
    ssl:close(Sock);
close({dtls, Sock}) ->
    ssl:close(Sock).

%%--------------------------------------------------------------------
%% Server-Opts

socketopts(tcp) ->
    #{
        acceptors => 8,
        tcp_options => tcp_opts()
    };
socketopts(ssl) ->
    #{
        acceptors => 8,
        tcp_options => tcp_opts(),
        ssl_options => ssl_opts()
    };
socketopts(udp) ->
    #{udp_options => udp_opts()};
socketopts(dtls) ->
    #{
        acceptors => 8,
        udp_options => udp_opts(),
        dtls_options => dtls_opts()
    }.

tcp_opts() ->
    maps:merge(
        udp_opts(),
        #{
            send_timeout => 15000,
            send_timeout_close => true,
            backlog => 100,
            nodelay => true
        }
    ).

udp_opts() ->
    #{
        %% NOTE
        %% Making those too small will lead to inability to accept connections.
        recbuf => 2048,
        sndbuf => 2048,
        buffer => 2048,
        reuseaddr => true
    }.

ssl_opts() ->
    Certs = certs("key.pem", "cert.pem", "cacert.pem"),
    maps:merge(
        Certs,
        #{
            versions => emqx_tls_lib:available_versions(tls),
            ciphers => [],
            verify => verify_peer,
            fail_if_no_peer_cert => true,
            secure_renegotiate => false,
            reuse_sessions => true,
            honor_cipher_order => true
        }
    ).

dtls_opts() ->
    maps:merge(ssl_opts(), #{versions => ['dtlsv1.2', 'dtlsv1']}).

%%--------------------------------------------------------------------
%% Client-Opts

client_ssl_opts() ->
    OptsWithCerts = certs("client-key.pem", "client-cert.pem", "cacert.pem"),
    [{verify, verify_none} | maps:to_list(OptsWithCerts)].

certs(Key, Cert, CACert) ->
    CertsPath = emqx_common_test_helpers:deps_path(emqx, "etc/certs"),
    #{
        keyfile => filename:join([CertsPath, Key]),
        certfile => filename:join([CertsPath, Cert]),
        cacertfile => filename:join([CertsPath, CACert])
    }.
