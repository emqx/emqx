%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_test_helpers).

-compile(nowarn_export_all).
-compile(export_all).

-define(ACCESS_KEY_ID, "minioadmin").
-define(SECRET_ACCESS_KEY, "minioadmin").

-define(TOXIPROXY_HOST, "toxiproxy").
-define(TOXIPROXY_PORT, 8474).

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
    aws_config(tls, ?TLS_HOST, ?TLS_PORT).

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
    }.

base_config(ConnType) ->
    emqx_s3_schema:translate(base_raw_config(ConnType)).

unique_key() ->
    "key-" ++ integer_to_list(erlang:system_time(millisecond)) ++ "-" ++
        integer_to_list(erlang:unique_integer([positive])).

unique_bucket() ->
    "bucket-" ++ integer_to_list(erlang:system_time(millisecond)) ++ "-" ++
        integer_to_list(erlang:unique_integer([positive])).

with_failure(_ConnType, ehttpc_500, Fun) ->
    try
        meck:new(ehttpc, [passthrough, no_history]),
        meck:expect(ehttpc, request, fun(_, _, _, _, _) -> {ok, 500, []} end),
        Fun()
    after
        meck:unload(ehttpc)
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
%% Internal functions
%%--------------------------------------------------------------------

toxproxy_name(tcp) -> "minio_tcp";
toxproxy_name(tls) -> "minio_tls".

cert_path(FileName) ->
    Dir = code:lib_dir(emqx_s3, test),
    filename:join([Dir, <<"certs">>, FileName]).

bin(String) when is_list(String) -> list_to_binary(String);
bin(Binary) when is_binary(Binary) -> Binary.

erlcloud_s3_new(AccessKeyId, SecretAccessKey, Host, Port, Scheme) ->
    AwsConfig = erlcloud_s3:new(AccessKeyId, SecretAccessKey, Host, Port),
    AwsConfig#aws_config{
        s3_scheme = Scheme,
        s3_bucket_access_method = path
    }.
