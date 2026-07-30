%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_ssl).

-include_lib("emqx/include/logger.hrl").

-export([
    convert_certs/2
]).

convert_certs(RltvDir, Config0) ->
    case convert_ssl_certs(RltvDir, Config0) of
        {ok, Config} ->
            convert_oauth2_certs(RltvDir, Config);
        {error, _} = Error ->
            Error
    end.

convert_ssl_certs(RltvDir, #{<<"ssl">> := SSL} = Config) ->
    new_ssl_config(RltvDir, Config, SSL);
convert_ssl_certs(RltvDir, #{ssl := SSL} = Config) ->
    new_ssl_config(RltvDir, Config, SSL);
%% for bridges use connector name
convert_ssl_certs(_RltvDir, Config) ->
    {ok, Config}.

convert_oauth2_certs(RltvDir, #{<<"oauth2">> := OAuth2} = Config) when is_map(OAuth2) ->
    case convert_ssl_certs(filename:join(RltvDir, "oauth2"), OAuth2) of
        {ok, NewOAuth2} ->
            {ok, Config#{<<"oauth2">> := NewOAuth2}};
        {error, _} = Error ->
            Error
    end;
convert_oauth2_certs(RltvDir, #{oauth2 := OAuth2} = Config) when is_map(OAuth2) ->
    case convert_ssl_certs(filename:join(RltvDir, "oauth2"), OAuth2) of
        {ok, NewOAuth2} ->
            {ok, Config#{oauth2 := NewOAuth2}};
        {error, _} = Error ->
            Error
    end;
convert_oauth2_certs(_RltvDir, Config) ->
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
