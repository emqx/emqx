%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_client).

-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-export([
    create/2,

    put_object/3,
    put_object/4,

    start_multipart/3,
    upload_part/5,
    complete_multipart/4,
    abort_multipart/3,
    list/2,
    uri/2,

    format/1,
    format_request/1
]).

%% For connectors
-export([aws_config/1]).

-export_type([
    client/0,
    headers/0,
    bucket/0,
    key/0,
    upload_options/0,
    upload_id/0,
    etag/0,
    part_number/0,
    config/0
]).

-type headers() :: #{binary() | string() => iodata()}.
-type erlcloud_headers() :: list({string(), iodata()}).

-type bucket() :: string().
-type key() :: string().
-type part_number() :: non_neg_integer().
-type upload_id() :: string().
-type etag() :: string().
-type http_pool() :: ehttpc:pool_name().
-type pool_type() :: random | hash.

-opaque client() :: #{
    aws_config := aws_config(),
    bucket := bucket(),
    headers := erlcloud_headers(),
    url_expire_time := non_neg_integer(),
    pool_type := pool_type()
}.

-type config() :: #{
    scheme := string(),
    host := string(),
    port := part_number(),
    headers := headers(),
    url_expire_time := pos_integer(),
    access_key_id := string() | undefined,
    secret_access_key := emqx_secret:t(string()) | undefined,
    http_pool := http_pool(),
    pool_type := pool_type(),
    request_timeout := timeout() | undefined,
    max_retries := non_neg_integer() | undefined
}.

-type upload_options() :: #{
    acl => emqx_s3:acl() | undefined,
    headers => headers()
}.

-type s3_options() :: proplists:proplist().

-define(DEFAULT_REQUEST_TIMEOUT, 30000).
-define(DEFAULT_MAX_RETRIES, 2).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create(bucket(), config()) -> client().
create(Bucket, Config) ->
    #{
        aws_config => aws_config(Config),
        bucket => Bucket,
        url_expire_time => maps:get(url_expire_time, Config),
        headers => headers(Config),
        pool_type => maps:get(pool_type, Config)
    }.

-spec put_object(client(), key(), iodata()) -> ok_or_error(term()).
put_object(Client, Key, Value) ->
    put_object(Client, Key, #{}, Value).

-spec put_object(client(), key(), upload_options(), iodata()) -> ok_or_error(term()).
put_object(
    #{bucket := Bucket, headers := BaseHeaders, aws_config := AwsConfig = #aws_config{}},
    Key,
    UploadOpts,
    Content
) ->
    ECKey = erlcloud_key(Key),
    ECOpts = erlcloud_upload_options(UploadOpts),
    Headers = join_headers(BaseHeaders, maps:get(headers, UploadOpts, undefined)),
    try erlcloud_s3:put_object(Bucket, ECKey, Content, ECOpts, Headers, AwsConfig) of
        Props when is_list(Props) ->
            ok
    catch
        error:{aws_error, Reason} ->
            ?SLOG(debug, #{msg => "put_object_fail", key => Key, reason => Reason}),
            {error, Reason}
    end;
put_object(#{aws_config := {error, Reason}}, _Key, _UploadOpts, _Content) ->
    {error, {config_error, Reason}}.

-spec start_multipart(client(), key(), upload_options()) -> ok_or_error(upload_id(), term()).
start_multipart(
    #{bucket := Bucket, headers := BaseHeaders, aws_config := AwsConfig = #aws_config{}},
    Key,
    UploadOpts
) ->
    ECKey = erlcloud_key(Key),
    ECOpts = erlcloud_upload_options(UploadOpts),
    Headers = join_headers(BaseHeaders, maps:get(headers, UploadOpts, undefined)),
    case erlcloud_s3:start_multipart(Bucket, ECKey, ECOpts, Headers, AwsConfig) of
        {ok, Props} ->
            UploadId = response_property('uploadId', Props),
            ?tp(s3_client_multipart_started, #{
                bucket => Bucket,
                key => Key,
                upload_id => UploadId
            }),
            {ok, UploadId};
        {error, Reason} ->
            ?SLOG(debug, #{msg => "start_multipart_fail", key => Key, reason => Reason}),
            {error, Reason}
    end;
start_multipart(#{aws_config := {error, Reason}}, _Key, _UploadOpts) ->
    {error, {config_error, Reason}}.

-spec upload_part(client(), key(), upload_id(), part_number(), iodata()) ->
    ok_or_error(etag(), term()).
upload_part(
    #{bucket := Bucket, headers := Headers, aws_config := AwsConfig = #aws_config{}},
    Key,
    UploadId,
    PartNumber,
    Value
) ->
    case
        erlcloud_s3:upload_part(
            Bucket, erlcloud_key(Key), UploadId, PartNumber, Value, Headers, AwsConfig
        )
    of
        {ok, Props} ->
            {ok, response_property(etag, Props)};
        {error, Reason} ->
            ?SLOG(debug, #{msg => "upload_part_fail", key => Key, reason => Reason}),
            {error, Reason}
    end;
upload_part(#{aws_config := {error, Reason}}, _Key, _UploadId, _PartNumber, _Value) ->
    {error, {config_error, Reason}}.

-spec complete_multipart(client(), key(), upload_id(), [etag()]) -> ok_or_error(term()).
complete_multipart(
    #{bucket := Bucket, headers := Headers, aws_config := AwsConfig = #aws_config{}},
    Key,
    UploadId,
    ETags
) ->
    case
        erlcloud_s3:complete_multipart(
            Bucket, erlcloud_key(Key), UploadId, ETags, Headers, AwsConfig
        )
    of
        ok ->
            ?tp(s3_client_multipart_completed, #{
                bucket => Bucket,
                key => Key,
                upload_id => UploadId
            }),
            ok;
        {error, Reason} ->
            ?SLOG(debug, #{msg => "complete_multipart_fail", key => Key, reason => Reason}),
            {error, Reason}
    end;
complete_multipart(#{aws_config := {error, Reason}}, _Key, _UploadId, _ETags) ->
    {error, {config_error, Reason}}.

-spec abort_multipart(client(), key(), upload_id()) -> ok_or_error(term()).
abort_multipart(
    #{bucket := Bucket, headers := Headers, aws_config := AwsConfig = #aws_config{}},
    Key,
    UploadId
) ->
    case erlcloud_s3:abort_multipart(Bucket, erlcloud_key(Key), UploadId, [], Headers, AwsConfig) of
        ok ->
            ?tp(s3_client_multipart_aborted, #{
                bucket => Bucket,
                key => Key,
                upload_id => UploadId
            }),
            ok;
        {error, Reason} ->
            ?SLOG(debug, #{msg => "abort_multipart_fail", key => Key, reason => Reason}),
            {error, Reason}
    end;
abort_multipart(#{aws_config := {error, Reason}}, _Key, _UploadId) ->
    {error, {config_error, Reason}}.

-spec list(client(), s3_options()) -> ok_or_error(proplists:proplist(), term()).
list(#{bucket := Bucket, aws_config := AwsConfig = #aws_config{}}, Options) ->
    try erlcloud_s3:list_objects(Bucket, Options, AwsConfig) of
        Result -> {ok, Result}
    catch
        error:{aws_error, Reason} ->
            ?SLOG(debug, #{msg => "list_objects_fail", bucket => Bucket, reason => Reason}),
            {error, Reason}
    end;
list(#{aws_config := {error, Reason}}, _Options) ->
    {error, {config_error, Reason}}.

-spec uri(client(), key()) -> iodata().
uri(#{bucket := Bucket, aws_config := AwsConfig, url_expire_time := ExpireTime}, Key) ->
    erlcloud_s3:make_presigned_v4_url(ExpireTime, Bucket, get, erlcloud_key(Key), [], AwsConfig).

-spec format(client()) -> term().
format(#{aws_config := AwsConfig} = Client) ->
    Client#{aws_config => AwsConfig#aws_config{secret_access_key = "***"}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

erlcloud_upload_options(#{acl := Acl}) when Acl =/= undefined ->
    [
        {acl, Acl}
    ];
erlcloud_upload_options(#{}) ->
    [].

headers(#{headers := Headers}) ->
    headers_user_to_erlcloud_request(Headers);
headers(#{}) ->
    [].

aws_config(
    Config = #{
        scheme := Scheme,
        host := Host,
        port := Port,
        request_timeout := Timeout
    }
) ->
    ensure_aws_credentials(Config, #aws_config{
        s3_scheme = Scheme,
        s3_host = Host,
        s3_port = Port,
        s3_bucket_access_method = path,
        s3_bucket_after_host = true,

        %% This value will be transparently passed to ehttpc
        timeout = with_default(Timeout, ?DEFAULT_REQUEST_TIMEOUT),
        %% We rely on retry mechanism of ehttpc
        retry_num = 1
    }).

ensure_aws_credentials(
    Config = #{
        access_key_id := undefined,
        secret_access_key := undefined
    },
    AWSConfigIn
) ->
    %% NOTE
    %% Try to fetch credentials from the runtime environment (i.e. EC2 instance metadata).
    %% Doing it before changing HTTP client to the one pinned to the S3 hostname.
    case erlcloud_aws:update_config(AWSConfigIn) of
        {ok, AWSConfig} ->
            ensure_ehttpc_client(Config, AWSConfig);
        {error, Reason} ->
            {error, {failed_to_obtain_credentials, Reason}}
    end;
ensure_aws_credentials(
    Config = #{
        access_key_id := AccessKeyId,
        secret_access_key := SecretAccessKey
    },
    AWSConfigIn
) ->
    ensure_ehttpc_client(Config, AWSConfigIn#aws_config{
        access_key_id = AccessKeyId,
        secret_access_key = emqx_secret:unwrap(SecretAccessKey)
    }).

ensure_ehttpc_client(
    #{
        http_pool := HttpPool,
        pool_type := PoolType,
        max_retries := MaxRetries
    },
    AWSConfig
) ->
    AWSConfig#aws_config{
        http_client = request_fun(
            HttpPool, PoolType, with_default(MaxRetries, ?DEFAULT_MAX_RETRIES)
        )
    }.

-spec request_fun(http_pool(), pool_type(), non_neg_integer()) -> erlcloud_httpc:request_fun().
request_fun(HttpPool, PoolType, MaxRetries) ->
    fun(Url, Method, Headers, Body, Timeout, _Config) ->
        with_path_and_query_only(Url, fun(PathQuery) ->
            Request = make_request(
                Method, PathQuery, headers_erlcloud_request_to_ehttpc(Headers), Body
            ),
            case pick_worker_safe(HttpPool, PoolType) of
                {ok, Worker} ->
                    ehttpc_request(Worker, Method, Request, Timeout, MaxRetries);
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "s3_request_fun_fail",
                        reason => Reason,
                        http_pool => HttpPool,
                        pool_type => PoolType,
                        method => Method,
                        request => Request,
                        timeout => Timeout,
                        max_retries => MaxRetries
                    }),
                    {error, Reason}
            end
        end)
    end.

ehttpc_request(Worker, Method, Request, Timeout, MaxRetries) ->
    try timer:tc(fun() -> ehttpc:request(Worker, Method, Request, Timeout, MaxRetries) end) of
        {Time, {ok, StatusCode, RespHeaders}} ->
            ?SLOG(debug, #{
                msg => "s3_ehttpc_request_ok",
                status_code => StatusCode,
                headers => RespHeaders,
                time => Time
            }),
            {ok, {
                {StatusCode, undefined}, headers_ehttpc_to_erlcloud_response(RespHeaders), undefined
            }};
        {Time, {ok, StatusCode, RespHeaders, RespBody}} ->
            ?SLOG(debug, #{
                msg => "s3_ehttpc_request_ok",
                status_code => StatusCode,
                headers => RespHeaders,
                body => RespBody,
                time => Time
            }),
            {ok, {
                {StatusCode, undefined}, headers_ehttpc_to_erlcloud_response(RespHeaders), RespBody
            }};
        {Time, {error, Reason}} ->
            ?SLOG(warning, #{
                msg => "s3_ehttpc_request_fail",
                reason => Reason,
                timeout => Timeout,
                worker => Worker,
                method => Method,
                time => Time
            }),
            {error, Reason}
    catch
        error:badarg ->
            ?SLOG(warning, #{
                msg => "s3_ehttpc_request_fail",
                reason => badarg,
                timeout => Timeout,
                worker => Worker,
                method => Method
            }),
            {error, no_ehttpc_pool};
        error:Reason ->
            ?SLOG(warning, #{
                msg => "s3_ehttpc_request_fail",
                reason => Reason,
                timeout => Timeout,
                worker => Worker,
                method => Method
            }),
            {error, Reason}
    end.

pick_worker_safe(HttpPool, PoolType) ->
    try
        {ok, pick_worker(HttpPool, PoolType)}
    catch
        error:badarg ->
            {error, no_ehttpc_pool}
    end.

pick_worker(HttpPool, random) ->
    ehttpc_pool:pick_worker(HttpPool);
pick_worker(HttpPool, hash) ->
    ehttpc_pool:pick_worker(HttpPool, self()).

-define(IS_BODY_EMPTY(Body), (Body =:= undefined orelse Body =:= <<>>)).
-define(NEEDS_NO_BODY(Method), (Method =:= get orelse Method =:= head orelse Method =:= delete)).

make_request(Method, PathQuery, Headers, Body) when
    ?IS_BODY_EMPTY(Body) andalso ?NEEDS_NO_BODY(Method)
->
    {PathQuery, Headers};
make_request(_Method, PathQuery, Headers, Body) when ?IS_BODY_EMPTY(Body) ->
    {PathQuery, [{<<"content-length">>, <<"0">>} | Headers], <<>>};
make_request(_Method, PathQuery, Headers, Body) ->
    {PathQuery, Headers, Body}.

format_request({PathQuery, Headers, _Body}) -> {PathQuery, Headers, <<"...">>};
format_request({PathQuery, Headers}) -> {PathQuery, Headers}.

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

%% We need some header conversions to tie the emqx_s3, erlcloud and ehttpc APIs together.

%% The request header flow is:

%% UserHeaders -> [emqx_s3_client API] -> ErlcloudRequestHeaders0 ->
%% -> [erlcloud API] -> ErlcloudRequestHeaders1 -> [emqx_s3_client injected request_fun] ->
%% -> EhttpcRequestHeaders -> [ehttpc API]

%% The response header flow is:

%% [ehttpc API] -> EhttpcResponseHeaders -> [emqx_s3_client injected request_fun] ->
%% -> ErlcloudResponseHeaders0 -> [erlcloud API] -> [emqx_s3_client API]

%% UserHeders (emqx_s3 API headers) are maps with string/binary keys.
%% ErlcloudRequestHeaders are lists of tuples with string keys and iodata values
%% ErlcloudResponseHeders are lists of tuples with lower case string keys and iodata values.
%% EhttpcHeaders are lists of tuples with binary keys and iodata values.

%% Users provide headers as a map, but erlcloud expects a list of tuples with string keys and values.
headers_user_to_erlcloud_request(UserHeaders) ->
    [{string:to_lower(to_list_string(K)), V} || {K, V} <- maps:to_list(UserHeaders)].

%% Ehttpc returns operates on headers as a list of tuples with binary keys.
%% Erlcloud expects a list of tuples with string values and lowcase string keys
%% from the underlying http library.
headers_ehttpc_to_erlcloud_response(EhttpcHeaders) ->
    [{string:to_lower(to_list_string(K)), to_list_string(V)} || {K, V} <- EhttpcHeaders].

%% Ehttpc expects a list of tuples with binary keys.
%% Erlcloud provides a list of tuples with string keys.
headers_erlcloud_request_to_ehttpc(ErlcloudHeaders) ->
    [{to_binary(K), V} || {K, V} <- ErlcloudHeaders].

join_headers(ErlcloudHeaders, undefined) ->
    ErlcloudHeaders;
join_headers(ErlcloudHeaders, UserSpecialHeaders) ->
    ErlcloudHeaders ++ headers_user_to_erlcloud_request(UserSpecialHeaders).

to_binary(Val) when is_list(Val) -> list_to_binary(Val);
to_binary(Val) when is_binary(Val) -> Val.

to_list_string(Val) when is_binary(Val) ->
    binary_to_list(Val);
to_list_string(Val) when is_atom(Val) ->
    atom_to_list(Val);
to_list_string(Val) when is_list(Val) ->
    Val.

erlcloud_key(Characters) ->
    binary_to_list(unicode:characters_to_binary(Characters)).

response_property(Name, Props) ->
    case proplists:get_value(Name, Props) of
        undefined ->
            %% This schould not happen for valid S3 implementations
            ?SLOG(error, #{
                msg => "missing_s3_response_property",
                name => Name,
                props => Props
            }),
            error({missing_s3_response_property, Name});
        Value ->
            Value
    end.

with_default(undefined, Default) -> Default;
with_default(Value, _Default) -> Value.
