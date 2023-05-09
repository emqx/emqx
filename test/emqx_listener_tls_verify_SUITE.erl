%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_listener_tls_verify_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_test_tls_certs_helper, [ gen_ca/2
                                    , gen_host_cert/3
                                    ]).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    generate_tls_certs(Config),
    application:ensure_all_started(esockd),
    Config.

end_per_suite(_Config) ->
    application:stop(esockd).

t_tls_conn_success_when_partial_chain_enabled_with_intermediate_ca_cert(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, cacert_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client1.key")},
                                                    {certfile,  filename:join(DataDir, "client1.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ssl:close(Socket).

t_tls_conn_success_when_partial_chain_enabled_with_intermediate_cacert_bundle(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, cacert_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "server1-intermediate1-bundle.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client1.key")},
                                                    {certfile,  filename:join(DataDir, "client1.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ssl:close(Socket).

t_tls_conn_fail_when_partial_chain_enabled_with_intermediate_cacert_bundle2(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, cacert_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate1-server1-bundle.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client1.key")},
                                                    {certfile,  filename:join(DataDir, "client1.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ssl:close(Socket).

t_tls_conn_fail_when_partial_chain_disabled_with_intermediate_ca_cert(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client1.key")},
                                                    {certfile,  filename:join(DataDir, "client1.pem")}
                                                   ], 1000),

  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).


t_tls_conn_fail_when_partial_chain_disabled_with_other_intermediate_ca_cert(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2.pem")}
                                                   ], 1000),

  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_tls_conn_fail_when_partial_chain_enabled_with_other_intermediate_ca_cert(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, cacert_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_tls_conn_success_when_partial_chain_enabled_root_ca_with_complete_cert_chain(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, cacert_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "root.pem")}
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).

t_tls_conn_success_when_partial_chain_disabled_with_intermediate_ca_cert(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client1.key")},
                                                    {certfile,  filename:join(DataDir, "client1.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_tls_conn_success_when_partial_chain_disabled_with_broken_cert_chain_other_intermediate(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  %% Server has root ca cert
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "root.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  %% Client has complete chain
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile, filename:join(DataDir, "client2.key")},
                                                    {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).

t_tls_conn_fail_when_partial_chain_enabled_with_other_complete_cert_chain(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, cacert_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_tls_conn_success_when_partial_chain_enabled_with_complete_cert_chain(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, cacert_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate2.pem")}
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).

t_tls_conn_success_when_partial_chain_disabled_with_complete_cert_chain_client(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "root.pem")}
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).


t_tls_conn_fail_when_partial_chain_disabled_with_incomplete_cert_chain_server(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "intermediate2.pem")} %% imcomplete at server side
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).


t_tls_conn_fail_when_partial_chain_enabled_with_imcomplete_cert_chain(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, cacert_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-intermediate2-bundle.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_tls_conn_fail_when_partial_chain_disabled_with_incomplete_cert_chain(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-intermediate2-bundle.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

generate_tls_certs(Config) ->
  DataDir = ?config(data_dir, Config),
  gen_ca(DataDir, "root"),
  gen_host_cert("intermediate1", "root", DataDir),
  gen_host_cert("intermediate2", "root", DataDir),
  gen_host_cert("server1", "intermediate1", DataDir),
  gen_host_cert("client1", "intermediate1", DataDir),
  gen_host_cert("server2", "intermediate2", DataDir),
  gen_host_cert("client2", "intermediate2", DataDir),
  os:cmd(io_lib:format("cat ~p ~p ~p > ~p", [filename:join(DataDir, "client2.pem"),
                                             filename:join(DataDir, "intermediate2.pem"),
                                             filename:join(DataDir, "root.pem"),
                                             filename:join(DataDir, "client2-complete-bundle.pem")
                                            ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "client2.pem"),
                                          filename:join(DataDir, "intermediate2.pem"),
                                          filename:join(DataDir, "client2-intermediate2-bundle.pem")
                                         ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "server1.pem"),
                                          filename:join(DataDir, "intermediate1.pem"),
                                          filename:join(DataDir, "server1-intermediate1-bundle.pem")
                                         ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "intermediate1.pem"),
                                          filename:join(DataDir, "server1.pem"),
                                          filename:join(DataDir, "intermediate1-server1-bundle.pem")
                                         ])).

fail_when_ssl_error(Socket) ->
  receive
    {ssl_error, Socket, _} ->
      ct:fail("Handshake failed!")
  after 1000 ->
      ok
  end.

fail_when_no_ssl_alert(Socket, Alert) ->
  receive
    {ssl_error, Socket, {tls_alert, {Alert, AlertInfo}}} ->
        ct:pal("alert info: ~p~n", [AlertInfo]);
    {ssl_error, Socket, Other} ->
        ct:fail("recv unexpected ssl_error: ~p~n", [Other])
  after 1000 ->
      ct:fail("No expected alert: ~p from Socket: ~p ", [Alert, Socket])
  end.
