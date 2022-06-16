%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx_hooks.hrl").

-import(
    emqx_exproto_echo_svr,
    [
        frame_connect/2,
        frame_connack/1,
        frame_publish/3,
        frame_puback/1,
        frame_subscribe/2,
        frame_suback/1,
        frame_unsubscribe/1,
        frame_unsuback/1,
        frame_disconnect/0
    ]
).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(TCPOPTS, [binary, {active, false}]).
-define(DTLSOPTS, [binary, {active, false}, {protocol, dtls}]).

%%--------------------------------------------------------------------
-define(CONF_DEFAULT, <<
    "\n"
    "gateway.exproto {\n"
    "  server.bind = 9100,\n"
    "  handler.address = \"http://127.0.0.1:9001\"\n"
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
    [{group, Name} || Name <- metrics()].

groups() ->
    Cases = emqx_common_test_helpers:all(?MODULE),
    [{Name, Cases} || Name <- metrics()].

%% @private
metrics() ->
    [tcp, ssl, udp, dtls].

init_per_group(GrpName, Cfg) ->
    put(grpname, GrpName),
    Svrs = emqx_exproto_echo_svr:start(),
    emqx_common_test_helpers:start_apps([emqx_authn, emqx_gateway], fun set_special_cfg/1),
    [{servers, Svrs}, {listener_type, GrpName} | Cfg].

end_per_group(_, Cfg) ->
    emqx_config:erase(gateway),
    emqx_common_test_helpers:stop_apps([emqx_gateway, emqx_authn]),
    emqx_exproto_echo_svr:stop(proplists:get_value(servers, Cfg)).

set_special_cfg(emqx_gateway) ->
    LisType = get(grpname),
    emqx_config:put(
        [gateway, exproto],
        #{
            server => #{bind => 9100},
            handler => #{address => "http://127.0.0.1:9001"},
            listeners => listener_confs(LisType)
        }
    );
set_special_cfg(_App) ->
    ok.

listener_confs(Type) ->
    Default = #{bind => 7993, acceptors => 8},
    #{Type => #{'default' => maps:merge(Default, socketopts(Type))}}.

default_config() ->
    ?CONF_DEFAULT.

%%--------------------------------------------------------------------
%% Tests cases
%%--------------------------------------------------------------------

t_start_stop(_) ->
    ok.

t_mountpoint_echo(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => <<"test_client_1">>,
        mountpoint => <<"ct/">>
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

    emqx:publish(emqx_message:make(<<"t/dn">>, <<"echo">>)),

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

    Client = #{
        proto_name => <<"demo">>,
        proto_ver => <<"v0.1">>,
        clientid => <<"test_client_1">>,
        keepalive => 2
    },
    Password = <<"123456">>,

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    DisconnectBin = frame_disconnect(),
    {ok, DisconnectBin} = recv(Sock, 10000),

    SockType =/= udp andalso
        begin
            {error, closed} = recv(Sock, 5000)
        end,
    ok.

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
    emqx_hooks:add('client.connected', {?MODULE, hook_fun1, [Parent]}, 1000),
    emqx_hooks:add('client.disconnected', {?MODULE, hook_fun2, [Parent]}, 1000),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

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

    emqx:publish(emqx_message:make(<<"t/dn">>, <<"1">>)),
    PubBin1 = frame_publish(<<"t/dn">>, 0, <<"2">>),
    {ok, PubBin1} = recv(Sock, 5000),

    close(Sock),
    emqx_hooks:del('message.delivered', {?MODULE, hook_fun5}).

%%--------------------------------------------------------------------
%% Utils

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
    SslOpts = maps:to_list(client_ssl_opts()),
    {ok, SslSock} = ssl:connect("127.0.0.1", 7993, ?TCPOPTS ++ SslOpts),
    {ssl, SslSock};
open(dtls) ->
    SslOpts = maps:to_list(client_ssl_opts()),
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
    #{tcp => tcp_opts()};
socketopts(ssl) ->
    #{
        tcp => tcp_opts(),
        ssl => ssl_opts()
    };
socketopts(udp) ->
    #{udp => udp_opts()};
socketopts(dtls) ->
    #{
        udp => udp_opts(),
        dtls => dtls_opts()
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
        recbuf => 1024,
        sndbuf => 1024,
        buffer => 1024,
        reuseaddr => true
    }.

ssl_opts() ->
    Certs = certs("key.pem", "cert.pem", "cacert.pem"),
    maps:merge(
        Certs,
        #{
            versions => emqx_tls_lib:default_versions(),
            ciphers => emqx_tls_lib:default_ciphers(),
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
    certs("client-key.pem", "client-cert.pem", "cacert.pem").

certs(Key, Cert, CACert) ->
    CertsPath = emqx_common_test_helpers:deps_path(emqx, "etc/certs"),
    #{
        keyfile => filename:join([CertsPath, Key]),
        certfile => filename:join([CertsPath, Cert]),
        cacertfile => filename:join([CertsPath, CACert])
    }.
