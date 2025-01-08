%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_schema_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_minimal_config(_Config) ->
    ?assertMatch(
        #{
            bucket := "bucket",
            host := "s3.us-east-1.endpoint.com",
            port := 443,
            access_method := path,
            min_part_size := 5242880,
            transport_options :=
                #{
                    connect_timeout := 15000,
                    pool_size := 8,
                    ssl := #{enable := false}
                }
        },
        emqx_s3_schema:translate(#{
            <<"bucket">> => <<"bucket">>,
            <<"host">> => <<"s3.us-east-1.endpoint.com">>,
            <<"port">> => 443
        })
    ).

t_full_config(_Config) ->
    ?assertMatch(
        #{
            access_key_id := "access_key_id",
            acl := public_read,
            bucket := "bucket",
            host := "s3.us-east-1.endpoint.com",
            min_part_size := 10485760,
            port := 443,
            access_method := vhost,
            secret_access_key := Secret,
            transport_options :=
                #{
                    connect_timeout := 30000,
                    headers := #{<<"x-amz-acl">> := <<"public-read">>},
                    max_retries := 3,
                    pool_size := 10,
                    pool_type := random,
                    request_timeout := 10000,
                    ssl :=
                        #{
                            cacertfile := <<"cacertfile.crt">>,
                            certfile := <<"server.crt">>,
                            ciphers := ["ECDHE-RSA-AES256-GCM-SHA384"],
                            depth := 10,
                            enable := true,
                            keyfile := <<"server.key">>,
                            reuse_sessions := true,
                            secure_renegotiate := true,
                            server_name_indication := "some-host",
                            verify := verify_peer,
                            versions := ['tlsv1.2']
                        }
                }
        } when is_function(Secret),
        emqx_s3_schema:translate(#{
            <<"access_key_id">> => <<"access_key_id">>,
            <<"secret_access_key">> => <<"secret_access_key">>,
            <<"bucket">> => <<"bucket">>,
            <<"host">> => <<"s3.us-east-1.endpoint.com">>,
            <<"port">> => 443,
            <<"access_method">> => <<"vhost">>,
            <<"min_part_size">> => <<"10mb">>,
            <<"acl">> => <<"public_read">>,
            <<"transport_options">> => #{
                <<"connect_timeout">> => <<"30s">>,
                <<"pool_size">> => 10,
                <<"pool_type">> => <<"random">>,
                <<"ssl">> => #{
                    <<"enable">> => true,
                    <<"keyfile">> => <<"server.key">>,
                    <<"certfile">> => <<"server.crt">>,
                    <<"cacertfile">> => <<"cacertfile.crt">>,
                    <<"server_name_indication">> => <<"some-host">>,
                    <<"verify">> => <<"verify_peer">>,
                    <<"versions">> => [<<"tlsv1.2">>],
                    <<"ciphers">> => [<<"ECDHE-RSA-AES256-GCM-SHA384">>]
                },
                <<"request_timeout">> => <<"10s">>,
                <<"max_retries">> => 3,
                <<"headers">> => #{
                    <<"x-amz-acl">> => <<"public-read">>
                }
            }
        })
    ).

t_sensitive_config_hidden(_Config) ->
    ?assertMatch(
        #{
            access_key_id := "access_key_id",
            secret_access_key := <<"******">>
        },
        emqx_s3_schema:translate(
            #{
                <<"bucket">> => <<"bucket">>,
                <<"host">> => <<"s3.us-east-1.endpoint.com">>,
                <<"port">> => 443,
                <<"access_key_id">> => <<"access_key_id">>,
                <<"secret_access_key">> => <<"secret_access_key">>
            },
            % NOTE: this is what Config API handler is doing
            #{obfuscate_sensitive_values => true}
        )
    ).

t_sensitive_config_no_leak(_Config) ->
    ?assertThrow(
        {emqx_s3_schema, [
            Error = #{
                kind := validation_error,
                path := "s3.secret_access_key",
                reason := invalid_type
            }
        ]} when map_size(Error) == 3,
        emqx_s3_schema:translate(
            #{
                <<"bucket">> => <<"bucket">>,
                <<"host">> => <<"s3.us-east-1.endpoint.com">>,
                <<"port">> => 443,
                <<"access_key_id">> => <<"access_key_id">>,
                <<"secret_access_key">> => #{<<"1">> => <<"secret_access_key">>}
            }
        )
    ).

t_invalid_limits(_Config) ->
    ?assertException(
        throw,
        {emqx_s3_schema, [#{kind := validation_error, path := "s3.min_part_size"}]},
        emqx_s3_schema:translate(#{
            <<"bucket">> => <<"bucket">>,
            <<"host">> => <<"s3.us-east-1.endpoint.com">>,
            <<"port">> => 443,
            <<"min_part_size">> => <<"1mb">>
        })
    ),

    ?assertException(
        throw,
        {emqx_s3_schema, [#{kind := validation_error, path := "s3.min_part_size"}]},
        emqx_s3_schema:translate(#{
            <<"bucket">> => <<"bucket">>,
            <<"host">> => <<"s3.us-east-1.endpoint.com">>,
            <<"port">> => 443,
            <<"min_part_size">> => <<"100000gb">>
        })
    ),

    ?assertException(
        throw,
        {emqx_s3_schema, [#{kind := validation_error, path := "s3.max_part_size"}]},
        emqx_s3_schema:translate(#{
            <<"bucket">> => <<"bucket">>,
            <<"host">> => <<"s3.us-east-1.endpoint.com">>,
            <<"port">> => 443,
            <<"max_part_size">> => <<"1mb">>
        })
    ),

    ?assertException(
        throw,
        {emqx_s3_schema, [#{kind := validation_error, path := "s3.max_part_size"}]},
        emqx_s3_schema:translate(#{
            <<"bucket">> => <<"bucket">>,
            <<"host">> => <<"s3.us-east-1.endpoint.com">>,
            <<"port">> => 443,
            <<"max_part_size">> => <<"100000gb">>
        })
    ).
