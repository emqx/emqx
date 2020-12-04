%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").

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
    [ list_to_atom(X ++ "_" ++ Y)
      || X <- ["python3", "java"], Y <- ["tcp", "ssl", "udp", "dtls"]].

init_per_group(GrpName, Config) ->
    [Lang, LisType] = [list_to_atom(X) || X <- string:tokens(atom_to_list(GrpName), "_")],
    put(grpname, {Lang, LisType}),
    emqx_ct_helpers:start_apps([emqx_exproto], fun set_sepecial_cfg/1),
    [{driver_type, Lang},
     {listener_type, LisType} | Config].

end_per_group(_, _) ->
    emqx_ct_helpers:stop_apps([emqx_exproto]).

set_sepecial_cfg(emqx_exproto) ->
    {Lang, LisType} = get(grpname),
    Path = emqx_ct_helpers:deps_path(emqx_exproto, "example/"),
    Listeners = application:get_env(emqx_exproto, listeners, []),
    Driver = compile(Lang, Path),
    SockOpts = socketopts(LisType),
    UpgradeOpts = fun(Opts) ->
                      Opts1 = lists:keydelete(driver, 1, Opts),
                      Opts2 = lists:keydelete(tcp_options, 1, Opts1),
                      Opts3 = lists:keydelete(ssl_options, 1, Opts2),
                      Opts4 = lists:keydelete(udp_options, 1, Opts3),
                      Opts5 = lists:keydelete(dtls_options, 1, Opts4),
                      Driver ++ SockOpts ++ Opts5
                  end,
    NListeners = [{Proto, LisType, LisOn, UpgradeOpts(Opts)}
                  || {Proto, _Type, LisOn, Opts} <- Listeners],
    application:set_env(emqx_exproto, listeners, NListeners);
set_sepecial_cfg(_App) ->
    ok.

compile(java, Path) ->
    ErlPortJar = emqx_ct_helpers:deps_path(erlport, "priv/java/_pkgs/erlport.jar"),
    ct:pal(os:cmd(lists:concat(["cd ", Path, " && ",
                                "rm -rf Main.class State.class && ",
                                "javac -cp ", ErlPortJar, " Main.java"]))),
    [{driver, [{type, java}, {path, Path}, {cbm, 'Main'}]}];
compile(python3, Path) ->
    [{driver, [{type, python3}, {path, Path}, {cbm, main}]}].

%%--------------------------------------------------------------------
%% Tests cases
%%--------------------------------------------------------------------

t_start_stop(_) ->
    ok.

t_echo(Cfg) ->
    SockType = proplists:get_value(listener_type, Cfg),
    Bin = rand_bytes(),

    Sock = open(SockType),

    send(Sock, Bin),

    {ok, Bin} = recv(Sock, byte_size(Bin), 5000),

    %% pubsub echo
    emqx:subscribe(<<"t/#">>),
    emqx:publish(emqx_message:make(<<"t/dn">>, <<"echo">>)),
    First = receive {_, _, X} -> X#message.payload end,
    First = receive {_, _, Y} -> Y#message.payload end,

    close(Sock).

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

recv({tcp, Sock}, Size, Ts) ->
    gen_tcp:recv(Sock, Size, Ts);
recv({udp, Sock}, Size, Ts) ->
    {ok, {_, _, Bin}} = gen_udp:recv(Sock, Size, Ts),
    {ok, Bin};
recv({ssl, Sock}, Size, Ts) ->
    ssl:recv(Sock, Size, Ts);
recv({dtls, Sock}, Size, Ts) ->
    ssl:recv(Sock, Size, Ts).

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
    Path = emqx_ct_helpers:deps_path(emqx, "etc/certs"),
    [{versions, ['tlsv1.2','tlsv1.1',tlsv1]},
     {ciphers, ciphers()},
     {keyfile, Path ++ "/key.pem"},
     {certfile, Path ++ "/cert.pem"},
     {cacertfile, Path ++ "/cacert.pem"},
     {verify, verify_peer},
     {fail_if_no_peer_cert, true},
     {secure_renegotiate, false},
     {reuse_sessions, true},
     {honor_cipher_order, true}].

dtls_opts() ->
    Opts = ssl_opts(),
    lists:keyreplace(versions, 1, Opts, {versions, ['dtlsv1.2', 'dtlsv1']}).

ciphers() ->
    proplists:get_value(ciphers, emqx_ct_helpers:client_ssl()).

%%--------------------------------------------------------------------
%% Client-Opts

client_ssl_opts() ->
    Path = emqx_ct_helpers:deps_path(emqx, "etc/certs"),
    [{keyfile, Path ++ "/client-key.pem"},
     {certfile, Path ++ "/client-cert.pem"},
     {cacertfile, Path ++ "/cacert.pem"}].
