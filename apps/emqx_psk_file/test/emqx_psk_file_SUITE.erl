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

-module(emqx_psk_file_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_psk_file], fun set_special_confs/1),
    Config.

end_per_suite(_) ->
    emqx_ct_helpers:stop_apps([emqx_psk_file]).

set_special_confs(emqx) ->
    emqx_ct_helpers:change_emqx_opts(
      ssl_twoway, [{ssl_options,
                    [{versions, ['tlsv1.2','tlsv1.1', tlsv1]},
                     {ciphers, psk_ciphers()},
                     {user_lookup_fun,{fun emqx_psk:lookup/3,<<>>}}
                    ]
                   }]),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins"));
set_special_confs(emqx_psk_file) ->
    Path = emqx_ct_helpers:deps_path(emqx_psk_file, "etc/psk.txt"),
    application:set_env(emqx_psk_file, path, Path),
    application:set_env(emqx_psk_file, delimiter, ":");
set_special_confs(_App) ->
    ok.

%%--------------------------------------------------------------------
%% cases

t_psk_loaded(_) ->
    ?assertEqual({stop, <<"1234">>},
                  emqx_psk_file:on_psk_lookup(<<"client1">>, undefined)).

t_psk_ciphers(_) ->
    lists:foreach(fun(Cipher) ->
        {ok, C} = do_emqtt_connect(Cipher),
        emqtt:disconnect(C)
    end, psk_ciphers()).

do_emqtt_connect(Cipher) ->
    {ok, C} = emqtt:start_link(
                [{proto_ver, v5},
                 {port, 8883},
                 {ssl, true},
                 {ssl_opts, ssl_opts(Cipher)}
                ]),
    {ok, _} = emqtt:connect(C),
    {ok, C}.

psk_ciphers() ->
    ["RSA-PSK-AES256-GCM-SHA384","RSA-PSK-AES256-CBC-SHA384",
     "RSA-PSK-AES128-GCM-SHA256","RSA-PSK-AES128-CBC-SHA256",
     "RSA-PSK-AES256-CBC-SHA","RSA-PSK-AES128-CBC-SHA",
     "PSK-AES256-GCM-SHA384","PSK-AES128-GCM-SHA256",
     "PSK-AES256-CBC-SHA384","PSK-AES256-CBC-SHA",
     "PSK-AES128-CBC-SHA256","PSK-AES128-CBC-SHA"].

ssl_opts(Cipher) ->
    TlsFile = fun(Name) ->
        emqx_ct_helpers:app_path(
          emqx,
          filename:join(["etc", "certs", Name]))
    end,
    [{cacertfile, TlsFile("cacert.pem")},
     {certfile, TlsFile("client-cert.pem")},
     {keyfile, TlsFile("client-key.pem")},
     {verify, verify_peer},
     {server_name_indication, disable},
     {protocol, tls},
     {versions, ['tlsv1.2', 'tlsv1.1']},
     {psk_identity, "client1"},
     {user_lookup_fun, {fun ?MODULE:on_psk_client_lookup/3, #{}}},
     {ciphers, [Cipher]}
    ].

on_psk_client_lookup(psk, _PSKId, _UserState) ->
    {stop, Psk} = emqx_psk_file:on_psk_lookup(<<"client1">>, undefined),
    {ok, Psk}.
