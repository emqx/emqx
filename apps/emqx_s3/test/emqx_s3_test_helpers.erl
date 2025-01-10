%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_test_helpers).

-compile(nowarn_export_all).
-compile(export_all).

-define(ACCESS_KEY_ID, "minioadmin").
-define(SECRET_ACCESS_KEY, "minioadmin").

-define(TOXIPROXY_HOST, "toxiproxy").
-define(TOXIPROXY_PORT, 8474).

-define(MINIO_HOST, "minio.net").
-define(MINIO_PORT, 9000).

-define(TCP_HOST, ?TOXIPROXY_HOST).
-define(TCP_PORT, 19000).
-define(TLS_HOST, ?TOXIPROXY_HOST).
-define(TLS_PORT, 19100).

-include_lib("erlcloud/include/erlcloud_aws.hrl").

-export([
    aws_config/1,
    base_raw_config/1,
    base_config/1,

    unique_key/0,
    unique_bucket/0,

    with_failure/3
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

aws_config(tcp) ->
    aws_config(tcp, ?TCP_HOST, ?TCP_PORT);
aws_config(tls) ->
    aws_config(tls, ?TLS_HOST, ?TLS_PORT);
aws_config({tcp, direct}) ->
    aws_config(tcp, ?MINIO_HOST, ?MINIO_PORT).

aws_config(tcp, Host, Port) ->
    erlcloud_s3_new(
        ?ACCESS_KEY_ID,
        ?SECRET_ACCESS_KEY,
        Host,
        Port,
        "http://"
    );
aws_config(tls, Host, Port) ->
    erlcloud_s3_new(
        ?ACCESS_KEY_ID,
        ?SECRET_ACCESS_KEY,
        Host,
        Port,
        "https://"
    ).

base_raw_config(tcp) ->
    #{
        <<"bucket">> => <<"bucket">>,
        <<"access_key_id">> => bin(?ACCESS_KEY_ID),
        <<"secret_access_key">> => bin(?SECRET_ACCESS_KEY),
        <<"host">> => ?TCP_HOST,
        <<"port">> => ?TCP_PORT,
        <<"max_part_size">> => <<"10MB">>,
        <<"transport_options">> =>
            #{
                <<"request_timeout">> => <<"2s">>
            }
    };
base_raw_config(tls) ->
    #{
        <<"bucket">> => <<"bucket">>,
        <<"access_key_id">> => bin(?ACCESS_KEY_ID),
        <<"secret_access_key">> => bin(?SECRET_ACCESS_KEY),
        <<"host">> => ?TLS_HOST,
        <<"port">> => ?TLS_PORT,
        <<"max_part_size">> => <<"10MB">>,
        <<"transport_options">> =>
            #{
                <<"request_timeout">> => <<"2s">>,
                <<"ssl">> => #{
                    <<"enable">> => true,
                    <<"cacertfile">> => bin(cert_path("ca.crt")),
                    <<"server_name_indication">> => <<"authn-server">>,
                    <<"verify">> => <<"verify_peer">>
                }
            }
    };
base_raw_config({tcp, direct}) ->
    maps:merge(
        base_raw_config(tcp),
        #{
            <<"host">> => ?MINIO_HOST,
            <<"port">> => ?MINIO_PORT
        }
    ).

base_config(ConnType) ->
    emqx_s3_schema:translate(base_raw_config(ConnType)).

unique_key() ->
    "key-" ++ integer_to_list(erlang:system_time(millisecond)) ++ "-" ++
        integer_to_list(erlang:unique_integer([positive])).

unique_bucket() ->
    "bucket-" ++ integer_to_list(erlang:system_time(millisecond)) ++ "-" ++
        integer_to_list(erlang:unique_integer([positive])).

with_failure(_ConnType, httpc_500, Fun) ->
    try
        meck:new(hackney, [passthrough, no_history]),
        meck:expect(hackney, request, fun(_, _, _, _, _) -> {ok, 500, []} end),
        Fun()
    after
        meck:unload(hackney)
    end;
with_failure(ConnType, FailureType, Fun) ->
    emqx_common_test_helpers:with_failure(
        FailureType,
        toxproxy_name(ConnType),
        ?TOXIPROXY_HOST,
        ?TOXIPROXY_PORT,
        Fun
    ).

%%--------------------------------------------------------------------

recreate_bucket(Bucket, AwsConfig) ->
    ok = delete_bucket(Bucket, AwsConfig),
    ok = erlcloud_s3:create_bucket(Bucket, AwsConfig).

delete_bucket(Bucket, AwsConfig) ->
    try erlcloud_s3:list_objects(Bucket, AwsConfig) of
        List ->
            Contents = proplists:get_value(contents, List),
            ok = lists:foreach(
                fun(Object) ->
                    Key = emqx_s3_client:erlcloud_key(proplists:get_value(key, Object)),
                    _Ok = erlcloud_s3:delete_object(Bucket, Key, AwsConfig)
                end,
                Contents
            ),
            ok = erlcloud_s3:delete_bucket(Bucket, AwsConfig)
    catch
        error:{aws_error, _NotFound} ->
            ok
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

toxproxy_name(tcp) -> "minio_tcp";
toxproxy_name(tls) -> "minio_tls".

cert_path(FileName) ->
    Dir = code:lib_dir(emqx_s3),
    filename:join([Dir, <<"test">>, <<"certs">>, FileName]).

bin(String) when is_list(String) -> list_to_binary(String);
bin(Binary) when is_binary(Binary) -> Binary.

erlcloud_s3_new(AccessKeyId, SecretAccessKey, Host, Port, Scheme) ->
    AwsConfig = erlcloud_s3:new(AccessKeyId, SecretAccessKey, Host, Port),
    AwsConfig#aws_config{
        s3_scheme = Scheme,
        s3_bucket_access_method = path
    }.
