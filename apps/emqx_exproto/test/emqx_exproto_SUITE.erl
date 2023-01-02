%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(emqx_exproto_echo_svr,
        [ frame_connect/2
        , frame_connack/1
        , frame_publish/3
        , frame_puback/1
        , frame_subscribe/2
        , frame_suback/1
        , frame_unsubscribe/1
        , frame_unsuback/1
        , frame_disconnect/0
        ]).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(TCPOPTS, [binary, {active, false}]).
-define(DTLSOPTS, [binary, {active, false}, {protocol, dtls}]).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [{group, Name} || Name  <- metrics()].

groups() ->
    Cases = emqx_ct:all(?MODULE),
    [{Name, Cases} || Name <- metrics()].

%% @private
metrics() ->
    [tcp, ssl, udp, dtls].

init_per_group(GrpName, Cfg) ->
    put(grpname, GrpName),
    Svrs = emqx_exproto_echo_svr:start(),
    emqx_ct_helpers:start_apps([emqx_exproto], fun set_special_cfg/1),
    emqx_logger:set_log_level(debug),
    [{servers, Svrs}, {listener_type, GrpName} | Cfg].

end_per_group(_, Cfg) ->
    emqx_ct_helpers:stop_apps([emqx_exproto]),
    emqx_exproto_echo_svr:stop(proplists:get_value(servers, Cfg)).

set_special_cfg(emqx_exproto) ->
    LisType = get(grpname),
    Listeners = application:get_env(emqx_exproto, listeners, []),
    SockOpts = socketopts(LisType),
    UpgradeOpts = fun(Opts) ->
                      Opts2 = lists:keydelete(tcp_options, 1, Opts),
                      Opts3 = lists:keydelete(ssl_options, 1, Opts2),
                      Opts4 = lists:keydelete(udp_options, 1, Opts3),
                      Opts5 = lists:keydelete(dtls_options, 1, Opts4),
                      SockOpts ++ Opts5
                  end,
    NListeners = [{Proto, LisType, LisOn, UpgradeOpts(Opts)}
                  || {Proto, _Type, LisOn, Opts} <- Listeners],
    application:set_env(emqx_exproto, listeners, NListeners);
set_special_cfg(emqx) ->
    application:set_env(emqx, allow_anonymous, true),
    application:set_env(emqx, enable_acl_cache, false),
    ok.

%%--------------------------------------------------------------------
%% Tests cases
%%--------------------------------------------------------------------

t_start_stop(_) ->
    ok.

t_mountpoint_echo(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{proto_name => <<"demo">>,
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

    Client = #{proto_name => <<"demo">>,
               proto_ver => <<"v0.1">>,
               clientid => <<"test_client_1">>
              },
    Password = <<"123456">>,

    ok = meck:new(emqx_access_control, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_access_control, authenticate,
                     fun(_) -> {error, ?RC_NOT_AUTHORIZED} end),

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(1),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    SockType =/= udp andalso begin
        {error, closed} = recv(Sock, 5000)
    end,
    meck:unload([emqx_access_control]).

t_acl_deny(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{proto_name => <<"demo">>,
               proto_ver => <<"v0.1">>,
               clientid => <<"test_client_1">>
              },
    Password = <<"123456">>,

    ok = meck:new(emqx_access_control, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_access_control, check_acl, fun(_, _, _) -> deny end),

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

    meck:unload([emqx_access_control]),

    send(Sock, PubBin),
    {ok, PubBinSuccesAck} = recv(Sock, 5000),
    close(Sock).

t_keepalive_timeout(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{proto_name => <<"demo">>,
               proto_ver => <<"v0.1">>,
               clientid => <<"test_client_1">>,
               keepalive => 2
              },
    Password = <<"123456">>,

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    %% Timed out connections are closed immediately,
    %% so there may not be a disconnect message here
    %%DisconnectBin = frame_disconnect(),
    %%{ok, DisconnectBin} = recv(Sock, 10000),

    SockType =/= udp andalso begin
        {error, closed} = recv(Sock, 10000)
    end, ok.

t_hook_connected_disconnected(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{proto_name => <<"demo">>,
               proto_ver => <<"v0.1">>,
               clientid => <<"test_client_1">>
              },
    Password = <<"123456">>,

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    Parent = self(),
    HookFun1 = fun(_, _) -> Parent ! connected, ok end,
    HookFun2 = fun(_, _, _) -> Parent ! disconnected, ok end,
    emqx:hook('client.connected', HookFun1),
    emqx:hook('client.disconnected', HookFun2),

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

    SockType =/= udp andalso begin
        {error, closed} = recv(Sock, 5000)
    end,
    emqx:unhook('client.connected', HookFun1),
    emqx:unhook('client.disconnected', HookFun2).

t_hook_session_subscribed_unsubscribed(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{proto_name => <<"demo">>,
               proto_ver => <<"v0.1">>,
               clientid => <<"test_client_1">>
              },
    Password = <<"123456">>,

    ConnBin = frame_connect(Client, Password),
    ConnAckBin = frame_connack(0),

    send(Sock, ConnBin),
    {ok, ConnAckBin} = recv(Sock, 5000),

    Parent = self(),
    HookFun1 = fun(_, _, _) -> Parent ! subscribed, ok end,
    HookFun2 = fun(_, _, _) -> Parent ! unsubscribed, ok end,
    emqx:hook('session.subscribed', HookFun1),
    emqx:hook('session.unsubscribed', HookFun2),

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
    emqx:unhook('session.subscribed', HookFun1),
    emqx:unhook('session.unsubscribed', HookFun2).

t_hook_message_delivered(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Sock = open(SockType),

    Client = #{proto_name => <<"demo">>,
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

    HookFun1 = fun(_, Msg) -> {ok, Msg#message{payload = <<"2">>}} end,
    emqx:hook('message.delivered', HookFun1),

    emqx:publish(emqx_message:make(<<"t/dn">>, <<"1">>)),
    PubBin1 = frame_publish(<<"t/dn">>, 0, <<"2">>),
    {ok, PubBin1} = recv(Sock, 5000),

    close(Sock),
    emqx:unhook('message.delivered', HookFun1).

%%--------------------------------------------------------------------
%% Utils

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
    [{tcp_options, tcp_opts()}];
socketopts(ssl) ->
    [{tcp_options, tcp_opts()},
     {ssl_options, ssl_opts()}];
socketopts(udp) ->
    [{udp_options, udp_opts()}];
socketopts(dtls) ->
    [{udp_options, udp_opts()},
     {dtls_options, dtls_opts()}].

tcp_opts() ->
    [{send_timeout, 15000},
     {send_timeout_close, true},
     {backlog, 100},
     {nodelay, true} | udp_opts()].

udp_opts() ->
    [{recbuf, 1024},
     {sndbuf, 1024},
     {buffer, 1024},
     {reuseaddr, true}].

ssl_opts() ->
    Certs = certs("key.pem", "cert.pem", "cacert.pem"),
    [{versions, emqx_tls_lib:default_versions()},
     {ciphers, emqx_tls_lib:default_ciphers()},
     {verify, verify_peer},
     {fail_if_no_peer_cert, true},
     {secure_renegotiate, false},
     {reuse_sessions, true},
     {honor_cipher_order, true}]++Certs.

dtls_opts() ->
    Opts = ssl_opts(),
    lists:keyreplace(versions, 1, Opts, {versions, ['dtlsv1.2', 'dtlsv1']}).

%%--------------------------------------------------------------------
%% Client-Opts

client_ssl_opts() ->
    certs( "client-key.pem", "client-cert.pem", "cacert.pem" ).

certs( Key, Cert, CACert ) ->
    CertsPath = emqx_ct_helpers:deps_path(emqx, "etc/certs"),
    [ { keyfile,    filename:join([ CertsPath, Key    ]) },
      { certfile,   filename:join([ CertsPath, Cert   ]) },
      { cacertfile, filename:join([ CertsPath, CACert ]) } ].

