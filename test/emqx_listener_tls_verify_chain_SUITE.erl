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
-module(emqx_listener_tls_verify_chain_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_test_tls_certs_helper, [ fail_when_ssl_error/1
                                    , fail_when_no_ssl_alert/2
                                    , generate_tls_certs/1
                                    ]).


all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    generate_tls_certs(Config),
    application:ensure_all_started(esockd),
    [{ssl_config, ssl_config_verify_peer()} | Config].

end_per_suite(_Config) ->
    application:stop(esockd).

t_conn_fail_with_intermediate_ca_cert(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           | ?config(ssl_config, Config)
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client1.key")},
                                                    {certfile,  filename:join(DataDir, "client1.pem")}
                                                   ], 1000),

  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).


t_conn_fail_with_other_intermediate_ca_cert(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           | ?config(ssl_config, Config)
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2.pem")}
                                                   ], 1000),

  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_conn_success_with_server_client_composed_complete_chain(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  %% Server has root ca cert
  Options = [{ssl_options, [ {cacertfile, filename:join(DataDir, "root.pem")}
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           | ?config(ssl_config, Config)
                           ]}],
  %% Client has complete chain
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile, filename:join(DataDir, "client2.key")},
                                                    {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).

t_conn_success_with_other_signed_client_composed_complete_chain(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  %% Server has root ca cert
  Options = [{ssl_options, [ {cacertfile, filename:join(DataDir, "root.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           | ?config(ssl_config, Config)
                           ]}],
  %% Client has partial_chain
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile, filename:join(DataDir, "client2.key")},
                                                    {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).

t_conn_success_with_renewed_intermediate_root_bundle(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  %% Server has root ca cert
  Options = [{ssl_options, [ {cacertfile, filename:join(DataDir, "intermediate1_renewed-root-bundle.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           | ?config(ssl_config, Config)
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile, filename:join(DataDir, "client1.key")},
                                                    {certfile, filename:join(DataDir, "client1.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).

t_conn_success_with_client_complete_cert_chain(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {cacertfile, filename:join(DataDir, "root.pem")}
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           | ?config(ssl_config, Config)
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).

t_conn_fail_with_server_partial_chain(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [{cacertfile, filename:join(DataDir, "intermediate2.pem")} %% imcomplete at server side
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           | ?config(ssl_config, Config)
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_conn_fail_without_root_cacert(Config) ->
  Port = emqx_test_tls_certs_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {cacertfile, filename:join(DataDir, "intermediate2.pem")}
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           | ?config(ssl_config, Config)
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-intermediate2-bundle.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

ssl_config_verify_peer() ->
  [ {verify, verify_peer}
  , {fail_if_no_peer_cert, true}
  ].
