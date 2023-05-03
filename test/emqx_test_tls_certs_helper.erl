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

-module(emqx_test_tls_certs_helper).
-export([ gen_ca/2
        , gen_host_cert/3
        , gen_host_cert/4

        , select_free_port/1
        , generate_tls_certs/1

        , fail_when_ssl_error/1
        , fail_when_ssl_error/2
        , fail_when_no_ssl_alert/2
        , fail_when_no_ssl_alert/3


        ]).

-include_lib("common_test/include/ct.hrl").

%%-------------------------------------------------------------------------------
%% TLS certs
%%-------------------------------------------------------------------------------
gen_ca(Path, Name) ->
    %% Generate ca.pem and ca.key which will be used to generate certs
    %% for hosts server and clients
    ECKeyFile = eckey_name(Path),
    filelib:ensure_dir(ECKeyFile),
    os:cmd("openssl ecparam -name secp256r1 > " ++ ECKeyFile),
    Cmd = lists:flatten(
        io_lib:format(
            "openssl req -new -x509 -nodes "
            "-newkey ec:~s "
            "-keyout ~s -out ~s -days 3650 "
            "-addext basicConstraints=CA:TRUE "
            "-subj \"/C=SE/O=TEST CA\"",
            [
                ECKeyFile,
                ca_key_name(Path, Name),
                ca_cert_name(Path, Name)
            ]
        )
    ),
    os:cmd(Cmd).

ca_cert_name(Path, Name) ->
    filename(Path, "~s.pem", [Name]).
ca_key_name(Path, Name) ->
    filename(Path, "~s.key", [Name]).

eckey_name(Path) ->
    filename(Path, "ec.key", []).

gen_host_cert(H, CaName, Path) ->
    gen_host_cert(H, CaName, Path, #{}).

gen_host_cert(H, CaName, Path, Opts) ->
    ECKeyFile = eckey_name(Path),
    CN = str(H),
    HKey = filename(Path, "~s.key", [H]),
    HCSR = filename(Path, "~s.csr", [H]),
    HCSR2 = filename(Path, "~s.csr", [H]),
    HPEM = filename(Path, "~s.pem", [H]),
    HPEM2 = filename(Path, "~s_renewed.pem", [H]),
    HEXT = filename(Path, "~s.extfile", [H]),
    PasswordArg =
        case maps:get(password, Opts, undefined) of
            undefined ->
                " -nodes ";
            Password ->
                io_lib:format(" -passout pass:'~s' ", [Password])
        end,

    create_file(
      HEXT,
      "keyUsage=digitalSignature,keyAgreement,keyCertSign\n"
      "basicConstraints=CA:TRUE \n"
      "~s \n"
      "subjectAltName=DNS:~s\n",
      [maps:get(ext, Opts, ""), CN]
    ),

    CSR_Cmd = csr_cmd(PasswordArg, ECKeyFile, HKey, HCSR, CN),
    CSR_Cmd2 = csr_cmd(PasswordArg, ECKeyFile, HKey, HCSR2, CN),

    CERT_Cmd = cert_sign_cmd(HEXT, HCSR, ca_cert_name(Path, CaName), ca_key_name(Path, CaName), HPEM),
    %% 2nd cert for testing renewed cert.
    CERT_Cmd2 = cert_sign_cmd(HEXT, HCSR2, ca_cert_name(Path, CaName), ca_key_name(Path, CaName), HPEM2),
    ct:pal(os:cmd(CSR_Cmd)),
    ct:pal(os:cmd(CSR_Cmd2)),
    ct:pal(os:cmd(CERT_Cmd)),
    ct:pal(os:cmd(CERT_Cmd2)),
    file:delete(HEXT).

cert_sign_cmd(ExtFile, CSRFile, CACert, CAKey, OutputCert)->
    lists:flatten(
            io_lib:format(
                "openssl x509 -req "
                "-extfile ~s "
                "-in ~s -CA ~s -CAkey ~s -CAcreateserial "
                "-out ~s -days 500",
                [
                    ExtFile,
                    CSRFile,
                    CACert,
                    CAKey,
                    OutputCert
                ]
            )
        ).

csr_cmd(PasswordArg, ECKeyFile, HKey, HCSR, CN) ->
    lists:flatten(
      io_lib:format(
        "openssl req -new ~s -newkey ec:~s "
        "-keyout ~s -out ~s "
        "-addext \"subjectAltName=DNS:~s\" "
        "-addext basicConstraints=CA:TRUE "
        "-addext keyUsage=digitalSignature,keyAgreement,keyCertSign "
        "-subj \"/C=SE/O=TEST/CN=~s\"",
        [PasswordArg, ECKeyFile, HKey, HCSR, CN, CN]
       )
     ).

filename(Path, F, A) ->
    filename:join(Path, str(io_lib:format(F, A))).

str(Arg) ->
    binary_to_list(iolist_to_binary(Arg)).

create_file(Filename, Fmt, Args) ->
    filelib:ensure_dir(Filename),
    {ok, F} = file:open(Filename, [write]),
    try
        io:format(F, Fmt, Args)
    after
        file:close(F)
    end,
    ok.

%% @doc get unused port from OS
-spec select_free_port(tcp | udp | ssl | quic) -> inets:port_number().
select_free_port(tcp) ->
    select_free_port(gen_tcp, listen);
select_free_port(udp) ->
    select_free_port(gen_udp, open);
select_free_port(ssl) ->
    select_free_port(tcp);
select_free_port(quic) ->
    select_free_port(udp).

select_free_port(GenModule, Fun) when
    GenModule == gen_tcp orelse
        GenModule == gen_udp
->
    {ok, S} = GenModule:Fun(0, [{reuseaddr, true}]),
    {ok, Port} = inet:port(S),
    ok = GenModule:close(S),
    case os:type() of
        {unix, darwin} ->
            %% in MacOS, still get address_in_use after close port
            timer:sleep(500);
        _ ->
            skip
    end,
    ct:pal("Select free OS port: ~p", [Port]),
    Port.

%% @doc fail the test if ssl_error recvd
%%      post check for success conn establishment
fail_when_ssl_error(Socket) ->
    fail_when_ssl_error(Socket, 1000).
fail_when_ssl_error(Socket, Timeout) ->
  receive
    {ssl_error, Socket, _} ->
      ct:fail("Handshake failed!")
  after Timeout ->
      ok
  end.

%% @doc fail the test if no ssl_error recvd
fail_when_no_ssl_alert(Socket, Alert) ->
    fail_when_no_ssl_alert(Socket, Alert, 1000).
fail_when_no_ssl_alert(Socket, Alert, Timeout) ->
  receive
    {ssl_error, Socket, {tls_alert, {Alert, AlertInfo}}} ->
        ct:pal("alert info: ~p~n", [AlertInfo]);
    {ssl_error, Socket, Other} ->
        ct:fail("recv unexpected ssl_error: ~p~n", [Other])
  after Timeout ->
      ct:fail("No expected alert: ~p from Socket: ~p ", [Alert, Socket])
  end.

%% @doc Generate TLS cert chain for tests
generate_tls_certs(Config) ->
  DataDir = ?config(data_dir, Config),
  gen_ca(DataDir, "root"),
  gen_host_cert("intermediate1", "root", DataDir),
  gen_host_cert("intermediate2", "root", DataDir),
  gen_host_cert("server1", "intermediate1", DataDir),
  gen_host_cert("client1", "intermediate1", DataDir),
  gen_host_cert("server2", "intermediate2", DataDir),
  gen_host_cert("client2", "intermediate2", DataDir),

  %% Build bundles below
  os:cmd(io_lib:format("cat ~p ~p ~p > ~p", [filename:join(DataDir, "client2.pem"),
                                             filename:join(DataDir, "intermediate2.pem"),
                                             filename:join(DataDir, "root.pem"),
                                             filename:join(DataDir, "client2-complete-bundle.pem")
                                            ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "client2.pem"),
                                          filename:join(DataDir, "intermediate2.pem"),
                                          filename:join(DataDir, "client2-intermediate2-bundle.pem")
                                         ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "client2.pem"),
                                          filename:join(DataDir, "root.pem"),
                                          filename:join(DataDir, "client2-root-bundle.pem")
                                         ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "server1.pem"),
                                          filename:join(DataDir, "intermediate1.pem"),
                                          filename:join(DataDir, "server1-intermediate1-bundle.pem")
                                         ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "intermediate1.pem"),
                                          filename:join(DataDir, "server1.pem"),
                                          filename:join(DataDir, "intermediate1-server1-bundle.pem")
                                         ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "intermediate1_renewed.pem"),
                                          filename:join(DataDir, "root.pem"),
                                          filename:join(DataDir, "intermediate1_renewed-root-bundle.pem")
                                         ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "intermediate2.pem"),
                                          filename:join(DataDir, "intermediate2_renewed.pem"),
                                          filename:join(DataDir, "intermediate2_renewed_old-bundle.pem")
                                         ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "intermediate1.pem"),
                                          filename:join(DataDir, "root.pem"),
                                          filename:join(DataDir, "intermediate1-root-bundle.pem")
                                         ])),
  os:cmd(io_lib:format("cat ~p ~p ~p > ~p", [filename:join(DataDir, "root.pem"),
                                             filename:join(DataDir, "intermediate2.pem"),
                                             filename:join(DataDir, "intermediate1.pem"),
                                             filename:join(DataDir, "all-CAcerts-bundle.pem")
                                            ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "intermediate2.pem"),
                                          filename:join(DataDir, "intermediate1.pem"),
                                          filename:join(DataDir, "two-intermediates-bundle.pem")
                                         ])).
