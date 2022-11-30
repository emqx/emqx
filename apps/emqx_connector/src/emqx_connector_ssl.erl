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

-module(emqx_connector_ssl).

-include_lib("emqx/include/logger.hrl").

-export([
    convert_certs/2,
    clear_certs/2,
    try_clear_certs/3
]).

convert_certs(RltvDir, #{<<"ssl">> := SSL} = Config) ->
    new_ssl_config(RltvDir, Config, SSL);
convert_certs(RltvDir, #{ssl := SSL} = Config) ->
    new_ssl_config(RltvDir, Config, SSL);
%% for bridges use connector name
convert_certs(_RltvDir, Config) ->
    {ok, Config}.

clear_certs(RltvDir, Config) ->
    clear_certs2(RltvDir, normalize_key_to_bin(Config)).

clear_certs2(RltvDir, #{<<"ssl">> := OldSSL} = _Config) ->
    ok = emqx_tls_lib:delete_ssl_files(RltvDir, undefined, OldSSL);
clear_certs2(_RltvDir, _) ->
    ok.

try_clear_certs(RltvDir, NewConf, undefined) ->
    try_clear_certs(RltvDir, NewConf, #{});
try_clear_certs(RltvDir, NewConf, OldConf) ->
    NewSSL = maps:get(<<"ssl">>, normalize_key_to_bin(NewConf), #{}),
    OldSSL = maps:get(<<"ssl">>, normalize_key_to_bin(OldConf), #{}),
    try_clear_certs2(RltvDir, NewSSL, OldSSL).

try_clear_certs2(RltvDir, NewSSL, OldSSL) ->
    ok = emqx_tls_lib:delete_ssl_files(RltvDir, NewSSL, OldSSL).

new_ssl_config(RltvDir, Config, SSL) ->
    case emqx_tls_lib:ensure_ssl_files(RltvDir, SSL) of
        {ok, NewSSL} ->
            {ok, new_ssl_config(Config, NewSSL)};
        {error, Reason} ->
            {error, {bad_ssl_config, Reason}}
    end.

new_ssl_config(#{ssl := _} = Config, NewSSL) ->
    Config#{ssl => NewSSL};
new_ssl_config(#{<<"ssl">> := _} = Config, NewSSL) ->
    Config#{<<"ssl">> => NewSSL};
new_ssl_config(Config, _NewSSL) ->
    Config.

normalize_key_to_bin(Map) ->
    emqx_map_lib:binary_key_map(Map).
