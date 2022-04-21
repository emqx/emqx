
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

-module(emqx_resource_ssl).

-export([ convert_certs/2
        , convert_certs/3
        , clear_certs/2
        ]).

convert_certs(ResId, NewConfig) ->
    convert_certs(ResId, NewConfig, #{}).

convert_certs(ResId, NewConfig, OldConfig) ->
    OldSSL = drop_invalid_certs(maps:get(ssl, OldConfig, undefined)),
    NewSSL = drop_invalid_certs(maps:get(ssl, NewConfig, undefined)),
    CertsDir = cert_dir(ResId),
    case emqx_tls_lib:ensure_ssl_files(CertsDir, NewSSL) of
        {ok, NewSSL1} ->
            ok = emqx_tls_lib:delete_ssl_files(CertsDir, NewSSL1, OldSSL),
            {ok, new_ssl_config(NewConfig, NewSSL1)};
        {error, Reason} ->
            {error, {bad_ssl_config, Reason}}
    end.

clear_certs(ResId, Config) ->
    OldSSL = drop_invalid_certs(maps:get(ssl, Config, undefined)),
    ok = emqx_tls_lib:delete_ssl_files(cert_dir(ResId), undefined, OldSSL).

cert_dir(ResId) ->
    filename:join(["resources", ResId]).

new_ssl_config(Config, undefined) -> Config;
new_ssl_config(Config, SSL) -> Config#{ssl => SSL}.

drop_invalid_certs(undefined) -> undefined;
drop_invalid_certs(SSL) -> emqx_tls_lib:drop_invalid_certs(SSL).
