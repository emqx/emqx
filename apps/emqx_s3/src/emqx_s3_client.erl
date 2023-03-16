%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_client).

-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-export([
    create/1,

    put_object/3,

    start_multipart/2,
    upload_part/5,
    complete_multipart/4,
    abort_multipart/3,
    list/2,

    format/1
]).

-export_type([client/0]).

-type s3_bucket_acl() ::
    private
    | public_read
    | public_read_write
    | authenticated_read
    | bucket_owner_read
    | bucket_owner_full_control.

-type headers() :: #{binary() => binary()}.

-type key() :: string().
-type part_number() :: non_neg_integer().
-type upload_id() :: string().
-type etag() :: string().

-type upload_options() :: list({acl, s3_bucket_acl()}).

-opaque client() :: #{
    aws_config := aws_config(),
    options := upload_options(),
    bucket := string(),
    headers := headers()
}.

-type config() :: #{
    scheme := string(),
    host := string(),
    port := part_number(),
    bucket := string(),
    headers := headers(),
    acl := s3_bucket_acl(),
    access_key_id := string() | undefined,
    secret_access_key := string() | undefined,
    http_pool := ecpool:pool_name(),
    request_timeout := timeout()
}.

-type s3_options() :: list({string(), string()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create(config()) -> client().
create(Config) ->
    #{
        aws_config => aws_config(Config),
        upload_options => upload_options(Config),
        bucket => maps:get(bucket, Config),
        headers => headers(Config)
    }.

-spec put_object(client(), key(), iodata()) -> ok_or_error(term()).
put_object(
    #{bucket := Bucket, upload_options := Options, headers := Headers, aws_config := AwsConfig},
    Key,
    Value
) ->
    try erlcloud_s3:put_object(Bucket, Key, Value, Options, Headers, AwsConfig) of
        Props when is_list(Props) ->
            ok
    catch
        error:{aws_error, Reason} ->
            ?SLOG(debug, #{msg => "put_object_fail", key => Key, reason => Reason}),
            {error, Reason}
    end.

-spec start_multipart(client(), key()) -> ok_or_error(upload_id(), term()).
start_multipart(
    #{bucket := Bucket, upload_options := Options, headers := Headers, aws_config := AwsConfig},
    Key
) ->
    case erlcloud_s3:start_multipart(Bucket, Key, Options, Headers, AwsConfig) of
        {ok, Props} ->
            {ok, proplists:get_value(uploadId, Props)};
        {error, Reason} ->
            ?SLOG(debug, #{msg => "start_multipart_fail", key => Key, reason => Reason}),
            {error, Reason}
    end.

-spec upload_part(client(), key(), upload_id(), part_number(), iodata()) ->
    ok_or_error(etag(), term()).
upload_part(
    #{bucket := Bucket, headers := Headers, aws_config := AwsConfig},
    Key,
    UploadId,
    PartNumber,
    Value
) ->
    case erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNumber, Value, Headers, AwsConfig) of
        {ok, Props} ->
            {ok, proplists:get_value(etag, Props)};
        {error, Reason} ->
            ?SLOG(debug, #{msg => "upload_part_fail", key => Key, reason => Reason}),
            {error, Reason}
    end.

-spec complete_multipart(client(), key(), upload_id(), [etag()]) -> ok_or_error(term()).
complete_multipart(
    #{bucket := Bucket, headers := Headers, aws_config := AwsConfig}, Key, UploadId, ETags
) ->
    case erlcloud_s3:complete_multipart(Bucket, Key, UploadId, ETags, Headers, AwsConfig) of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(debug, #{msg => "complete_multipart_fail", key => Key, reason => Reason}),
            {error, Reason}
    end.

-spec abort_multipart(client(), key(), upload_id()) -> ok_or_error(term()).
abort_multipart(#{bucket := Bucket, headers := Headers, aws_config := AwsConfig}, Key, UploadId) ->
    case erlcloud_s3:abort_multipart(Bucket, Key, UploadId, [], Headers, AwsConfig) of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(debug, #{msg => "abort_multipart_fail", key => Key, reason => Reason}),
            {error, Reason}
    end.

-spec list(client(), s3_options()) -> ok_or_error(term()).
list(#{bucket := Bucket, aws_config := AwsConfig}, Options) ->
    try
        {ok, erlcloud_s3:list_objects(Bucket, Options, AwsConfig)}
    catch
        error:{aws_error, Reason} ->
            ?SLOG(debug, #{msg => "list_objects_fail", bucket => Bucket, reason => Reason}),
            {error, Reason}
    end.

-spec format(client()) -> term().
format(#{aws_config := AwsConfig} = Client) ->
    Client#{aws_config => AwsConfig#aws_config{secret_access_key = "***"}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

upload_options(Config) ->
    [
        {acl, maps:get(acl, Config)}
    ].

headers(#{headers := Headers}) ->
    maps:to_list(Headers).

aws_config(#{
    scheme := Scheme,
    host := Host,
    port := Port,
    headers := Headers,
    access_key_id := AccessKeyId,
    secret_access_key := SecretAccessKey,
    http_pool := HttpPool,
    request_timeout := Timeout
}) ->
    #aws_config{
        s3_scheme = Scheme,
        s3_host = Host,
        s3_port = Port,
        s3_bucket_access_method = path,

        access_key_id = AccessKeyId,
        secret_access_key = SecretAccessKey,

        http_client = request_fun(Headers, HttpPool),
        timeout = Timeout
    }.

-type http_headers() :: [{binary(), binary()}].
-type http_pool() :: term().

-spec request_fun(http_headers(), http_pool()) -> erlcloud_httpc:request_fun().
request_fun(CustomHeaders, HttpPool) ->
    fun(Url, Method, Headers, Body, Timeout, _Config) ->
        with_path_and_query_only(Url, fun(PathQuery) ->
            JoinedHeaders = join_headers(Headers, CustomHeaders),
            Request = make_request(Method, PathQuery, JoinedHeaders, Body),
            ehttpc_request(HttpPool, Method, Request, Timeout)
        end)
    end.

ehttpc_request(HttpPool, Method, Request, Timeout) ->
    try ehttpc:request(HttpPool, Method, Request, Timeout) of
        {ok, StatusCode, RespHeaders} ->
            {ok, {{StatusCode, undefined}, string_headers(RespHeaders), undefined}};
        {ok, StatusCode, RespHeaders, RespBody} ->
            {ok, {{StatusCode, undefined}, string_headers(RespHeaders), RespBody}};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "s3_ehttpc_request_fail",
                reason => Reason,
                timeout => Timeout,
                pool => HttpPool,
                method => Method
            }),
            {error, Reason}
    catch
        error:badarg ->
            ?SLOG(error, #{
                msg => "s3_ehttpc_request_fail",
                reason => badarg,
                timeout => Timeout,
                pool => HttpPool,
                method => Method
            }),
            {error, no_ehttpc_pool};
        error:Reason ->
            ?SLOG(error, #{
                msg => "s3_ehttpc_request_fail",
                reason => Reason,
                timeout => Timeout,
                pool => HttpPool,
                method => Method
            }),
            {error, Reason}
    end.

-define(IS_BODY_EMPTY(Body), (Body =:= undefined orelse Body =:= <<>>)).
-define(NEEDS_BODY(Method), (Method =:= get orelse Method =:= head orelse Method =:= delete)).

make_request(Method, PathQuery, Headers, Body) when
    ?IS_BODY_EMPTY(Body) andalso ?NEEDS_BODY(Method)
->
    {PathQuery, Headers};
make_request(_Method, PathQuery, Headers, Body) when ?IS_BODY_EMPTY(Body) ->
    {PathQuery, [{<<"content-length">>, <<"0">>} | Headers], <<>>};
make_request(_Method, PathQuery, Headers, Body) ->
    {PathQuery, Headers, Body}.

format_request({PathQuery, Headers, _Body}) -> {PathQuery, Headers, <<"...">>}.

join_headers(Headers, CustomHeaders) ->
    MapHeaders = lists:foldl(
        fun({K, V}, MHeaders) ->
            maps:put(to_binary(K), V, MHeaders)
        end,
        #{},
        Headers ++ maps:to_list(CustomHeaders)
    ),
    maps:to_list(MapHeaders).

with_path_and_query_only(Url, Fun) ->
    case string:split(Url, "//", leading) of
        [_Scheme, UrlRem] ->
            case string:split(UrlRem, "/", leading) of
                [_HostPort, PathQuery] ->
                    Fun([$/ | PathQuery]);
                _ ->
                    {error, {invalid_url, Url}}
            end;
        _ ->
            {error, {invalid_url, Url}}
    end.

to_binary(Val) when is_list(Val) -> list_to_binary(Val);
to_binary(Val) when is_binary(Val) -> Val.

string_headers(Hdrs) ->
    [{string:to_lower(to_list_string(K)), to_list_string(V)} || {K, V} <- Hdrs].

to_list_string(Val) when is_binary(Val) ->
    binary_to_list(Val);
to_list_string(Val) when is_list(Val) ->
    Val.
