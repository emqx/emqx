%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    SanitizedSSL = maybe_drop_blank_certs_for_verify_none(SSL),
    case emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(RltvDir, SanitizedSSL) of
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

maybe_drop_blank_certs_for_verify_none(SSL) ->
    case normalize_verify(get_ssl_verify(SSL)) of
        verify_none ->
            maps:filter(
                fun(K, V) ->
                    not (is_cert_key(K) andalso is_blank_or_undefined(V))
                end,
                SSL
            );
        _ ->
            SSL
    end.

get_ssl_verify(SSL) ->
    case maps:find(verify, SSL) of
        {ok, Verify} ->
            Verify;
        error ->
            case maps:find(<<"verify">>, SSL) of
                {ok, Verify} -> Verify;
                error -> undefined
            end
    end.

normalize_verify(verify_none) ->
    verify_none;
normalize_verify(<<"verify_none">>) ->
    verify_none;
normalize_verify("verify_none") ->
    verify_none;
normalize_verify(verify_peer) ->
    verify_peer;
normalize_verify(<<"verify_peer">>) ->
    verify_peer;
normalize_verify("verify_peer") ->
    verify_peer;
normalize_verify(_) ->
    undefined.

is_cert_key(cacertfile) ->
    true;
is_cert_key(certfile) ->
    true;
is_cert_key(keyfile) ->
    true;
is_cert_key(<<"cacertfile">>) ->
    true;
is_cert_key(<<"certfile">>) ->
    true;
is_cert_key(<<"keyfile">>) ->
    true;
is_cert_key(_) ->
    false.

is_blank_or_undefined(undefined) ->
    true;
is_blank_or_undefined(null) ->
    true;
is_blank_or_undefined(V) when is_binary(V) ->
    byte_size(V) =:= 0;
is_blank_or_undefined(V) when is_list(V) ->
    V =:= [];
is_blank_or_undefined(_) ->
    false.

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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

convert_certs_verify_none_blank_paths_pass_test() ->
    Config = #{
        ssl => #{
            enable => true,
            verify => verify_none,
            cacertfile => <<>>,
            certfile => <<>>,
            keyfile => <<>>
        }
    },
    ?assertMatch(
        {ok, #{ssl := #{verify := verify_none}}},
        convert_certs("dummy-connector", Config)
    ).

maybe_drop_blank_certs_for_verify_none_with_atom_keys_test() ->
    SSL = #{
        enable => true,
        verify => verify_none,
        cacertfile => <<>>,
        certfile => undefined,
        keyfile => []
    },
    ?assertEqual(
        #{
            enable => true,
            verify => verify_none
        },
        maybe_drop_blank_certs_for_verify_none(SSL)
    ).

maybe_drop_blank_certs_for_verify_none_with_binary_keys_test() ->
    SSL = #{
        <<"enable">> => true,
        <<"verify">> => <<"verify_none">>,
        <<"cacertfile">> => <<>>,
        <<"certfile">> => <<>>,
        <<"keyfile">> => <<>>
    },
    ?assertEqual(
        #{
            <<"enable">> => true,
            <<"verify">> => <<"verify_none">>
        },
        maybe_drop_blank_certs_for_verify_none(SSL)
    ).

convert_certs_verify_peer_blank_paths_fail_test() ->
    Config = #{
        ssl => #{
            enable => true,
            verify => verify_peer,
            cacertfile => <<>>,
            certfile => <<>>,
            keyfile => <<>>
        }
    },
    ?assertMatch(
        {error, #{reason := <<"bad_ssl_config">>}},
        convert_certs("dummy-connector", Config)
    ).

convert_certs_verify_none_non_blank_invalid_path_fail_test() ->
    Config = #{
        ssl => #{
            enable => true,
            verify => verify_none,
            cacertfile => <<"/definitely/not/exist-ca.pem">>
        }
    },
    ?assertMatch(
        {error, #{reason := <<"bad_ssl_config">>}},
        convert_certs("dummy-connector", Config)
    ).

-endif.
