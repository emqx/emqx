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

-module(emqx_authn_https_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(PATH, [?CONF_NS_ATOM]).

-define(HTTPS_PORT, 32334).
-define(HTTPS_PATH, "/auth").
-define(CREDENTIALS, #{
    username => <<"plain">>,
    password => <<"plain">>,
    listener => 'tcp:default',
    protocol => mqtt
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_common_test_helpers:start_apps([emqx_authn]),
    application:ensure_all_started(cowboy),
    Config.

end_per_suite(_) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    emqx_common_test_helpers:stop_apps([emqx_authn]),
    application:stop(cowboy),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    {ok, _} = emqx_authn_http_test_server:start_link(?HTTPS_PORT, ?HTTPS_PATH, server_ssl_opts()),
    ok = emqx_authn_http_test_server:set_handler(fun cowboy_handler/2),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_authn_http_test_server:stop().

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    {ok, _} = create_https_auth_with_ssl_opts(
        #{
            <<"server_name_indication">> => <<"authn-server">>,
            <<"verify">> => <<"verify_peer">>,
            <<"versions">> => [<<"tlsv1.2">>],
            <<"ciphers">> => [<<"ECDHE-RSA-AES256-GCM-SHA384">>]
        }
    ),

    ?assertMatch(
        {ok, _},
        emqx_access_control:authenticate(?CREDENTIALS)
    ).

t_create_invalid_domain(_Config) ->
    {ok, _} = create_https_auth_with_ssl_opts(
        #{
            <<"server_name_indication">> => <<"authn-server-unknown-host">>,
            <<"verify">> => <<"verify_peer">>,
            <<"versions">> => [<<"tlsv1.2">>],
            <<"ciphers">> => [<<"ECDHE-RSA-AES256-GCM-SHA384">>]
        }
    ),

    ?assertEqual(
        {error, not_authorized},
        emqx_access_control:authenticate(?CREDENTIALS)
    ).

t_create_invalid_version(_Config) ->
    {ok, _} = create_https_auth_with_ssl_opts(
        #{
            <<"server_name_indication">> => <<"authn-server">>,
            <<"verify">> => <<"verify_peer">>,
            <<"versions">> => [<<"tlsv1.1">>]
        }
    ),

    ?assertEqual(
        {error, not_authorized},
        emqx_access_control:authenticate(?CREDENTIALS)
    ).

t_create_invalid_ciphers(_Config) ->
    {ok, _} = create_https_auth_with_ssl_opts(
        #{
            <<"server_name_indication">> => <<"authn-server">>,
            <<"verify">> => <<"verify_peer">>,
            <<"versions">> => [<<"tlsv1.2">>],
            <<"ciphers">> => [<<"ECDHE-ECDSA-AES256-SHA384">>]
        }
    ),

    ?assertEqual(
        {error, not_authorized},
        emqx_access_control:authenticate(?CREDENTIALS)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

create_https_auth_with_ssl_opts(SpecificSSLOpts) ->
    AuthConfig = raw_https_auth_config(SpecificSSLOpts),
    emqx:update_config(?PATH, {create_authenticator, ?GLOBAL, AuthConfig}).

raw_https_auth_config(SpecificSSLOpts) ->
    SSLOpts = maps:merge(
        emqx_authn_test_lib:client_ssl_cert_opts(),
        #{<<"enable">> => <<"true">>}
    ),
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"enable">> => <<"true">>,

        <<"backend">> => <<"http">>,
        <<"method">> => <<"get">>,
        <<"url">> => <<"https://127.0.0.1:32334/auth">>,
        <<"body">> => #{<<"username">> => ?PH_USERNAME, <<"password">> => ?PH_PASSWORD},
        <<"headers">> => #{<<"X-Test-Header">> => <<"Test Value">>},
        <<"ssl">> => maps:merge(SSLOpts, SpecificSSLOpts)
    }.

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).

cert_path(FileName) ->
    Dir = code:lib_dir(emqx_authn, test),
    filename:join([Dir, <<"data/certs">>, FileName]).

cowboy_handler(Req0, State) ->
    Req = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        jiffy:encode(#{result => allow, is_superuser => false}),
        Req0
    ),
    {ok, Req, State}.

server_ssl_opts() ->
    [
        {keyfile, cert_path("server.key")},
        {certfile, cert_path("server.crt")},
        {cacertfile, cert_path("ca.crt")},
        {verify, verify_none},
        {versions, ['tlsv1.2', 'tlsv1.3']},
        {ciphers, ["ECDHE-RSA-AES256-GCM-SHA384", "TLS_CHACHA20_POLY1305_SHA256"]}
    ].
