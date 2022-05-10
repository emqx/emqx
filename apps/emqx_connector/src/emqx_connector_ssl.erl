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

-export([
    convert_certs/2,
    clear_certs/2
]).

convert_certs(RltvDir, NewConfig) ->
    NewSSL = map_get_oneof([<<"ssl">>, ssl], NewConfig, undefined),
    case emqx_tls_lib:ensure_ssl_files(RltvDir, NewSSL) of
        {ok, NewSSL1} ->
            {ok, new_ssl_config(NewConfig, NewSSL1)};
        {error, Reason} ->
            {error, {bad_ssl_config, Reason}}
    end.

clear_certs(_RltvDir, undefined) ->
    ok;
clear_certs(RltvDir, Config) ->
    OldSSL = map_get_oneof([<<"ssl">>, ssl], Config, undefined),
    ok = emqx_tls_lib:delete_ssl_files(RltvDir, undefined, OldSSL).

new_ssl_config(Config, undefined) -> Config;
new_ssl_config(Config, #{<<"enable">> := _} = SSL) -> Config#{<<"ssl">> => SSL};
new_ssl_config(Config, #{enable := _} = SSL) -> Config#{ssl => SSL}.

map_get_oneof([], _Map, Default) ->
    Default;
map_get_oneof([Key | Keys], Map, Default) ->
    case maps:find(Key, Map) of
        error ->
            map_get_oneof(Keys, Map, Default);
        {ok, Value} ->
            Value
    end.
