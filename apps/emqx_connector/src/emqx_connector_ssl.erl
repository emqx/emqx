%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    convert_certs/2
]).

convert_certs(RltvDir, #{<<"ssl">> := SSL} = Config) ->
    new_ssl_config(RltvDir, Config, SSL);
convert_certs(RltvDir, #{ssl := SSL} = Config) ->
    new_ssl_config(RltvDir, Config, SSL);
%% for bridges use connector name
convert_certs(_RltvDir, Config) ->
    {ok, Config}.

new_ssl_config(RltvDir, Config, SSL) ->
    case emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(RltvDir, SSL) of
        {ok, NewSSL} ->
            {ok, new_ssl_config(Config, NewSSL)};
        {error, Reason} ->
            {error, map_bad_ssl_error(Reason)}
    end.

new_ssl_config(#{connector := Connector} = Config, NewSSL) ->
    Config#{connector => Connector#{ssl => NewSSL}};
new_ssl_config(#{<<"connector">> := Connector} = Config, NewSSL) ->
    Config#{<<"connector">> => Connector#{<<"ssl">> => NewSSL}};
new_ssl_config(#{ssl := _} = Config, NewSSL) ->
    Config#{ssl => NewSSL};
new_ssl_config(#{<<"ssl">> := _} = Config, NewSSL) ->
    Config#{<<"ssl">> => NewSSL};
new_ssl_config(Config, _NewSSL) ->
    Config.

map_bad_ssl_error(#{
    pem_check := NotPem,
    file_path := FilePath,
    which_option := Field
}) ->
    #{
        kind => validation_error,
        reason => <<"bad_ssl_config">>,
        bad_field => Field,
        file_path => FilePath,
        details => emqx_utils:format(
            "Failed to access certificate / key file: ~s",
            [emqx_utils:explain_posix(NotPem)]
        )
    };
map_bad_ssl_error(#{which_option := Field, reason := Reason}) ->
    #{
        kind => validation_error,
        reason => <<"bad_ssl_config">>,
        bad_field => Field,
        details => Reason
    };
map_bad_ssl_error(TLSLibError) ->
    #{
        kind => validation_error,
        reason => <<"bad_ssl_config">>,
        details => TLSLibError
    }.
