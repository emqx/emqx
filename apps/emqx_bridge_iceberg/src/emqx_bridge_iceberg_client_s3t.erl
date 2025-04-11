%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iceberg_client_s3t).

%% API
-export([
    new/1,
    load_table/3,
    update_table/4
]).

%% Internal exports; only for tests/debugging
-export([do_request/2]).

-export_type([
    t/0
]).

-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_bridge_iceberg.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(SERVICE, s3tables).
-define(SERVICE_STR, "s3tables").

%% 30 s
-define(DEFAULT_REQUEST_TIMEOUT, 30_000).

-define(aws_config, aws_config).
-define(account_id, account_id).
-define(bucket, bucket).
-define(base_uri, base_uri).
-define(base_path, base_path).
-define(host, host).
-define(method, method).
-define(headers, headers).
-define(query_params, query_params).
-define(path_parts, path_parts).
-define(payload, payload).

-type client_opts() :: #{
    access_key_id := string(),
    secret_access_key := emqx_secret:t(binary()),
    base_endpoint := string(),
    bucket := string(),
    account_id => string(),
    request_timeout => pos_integer(),
    any() => term()
}.

-opaque t() :: #{
    ?aws_config := aws_config(),
    ?host := string(),
    ?base_uri := string(),
    ?base_path := [string()],
    ?account_id := string(),
    ?bucket := string()
}.

-type namespace() :: string() | binary().
-type table() :: string() | binary().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec new(client_opts()) -> {ok, t()} | {error, term()}.
new(Params) ->
    #{
        access_key_id := AccessKeyId,
        secret_access_key := SecretAccessKey0,
        base_endpoint := BaseEndpoint,
        bucket := Bucket
    } = Params,
    RequestTimeout = maps:get(request_timeout, Params, ?DEFAULT_REQUEST_TIMEOUT),
    SecretAccessKey1 = emqx_secret:unwrap(SecretAccessKey0),
    AWSConfig0 = erlcloud_config:new(str(AccessKeyId), str(SecretAccessKey1)),
    AWSConfig = AWSConfig0#aws_config{
        timeout = RequestTimeout,
        %% todo: make configurable?
        retry_num = 3
    },
    maybe
        {ok, #{
            host := Host,
            base_uri := BaseURI,
            base_path := BasePath
        }} ?= emqx_bridge_iceberg_connector_schema:parse_base_endpoint(BaseEndpoint),
        {ok, AccountId} ?= infer_account_id(Params, AWSConfig),
        {ok, #{
            ?aws_config => AWSConfig,
            ?host => Host,
            ?base_uri => BaseURI,
            ?base_path => BasePath,
            ?account_id => AccountId,
            ?bucket => Bucket
        }}
    end.

-spec load_table(t(), namespace(), table()) -> {ok, map()} | {error, term()}.
load_table(Client, Namespace, Table) ->
    Method = get,
    Payload = <<"">>,
    Context = #{
        ?method => Method,
        ?path_parts => [arn, "namespaces", Namespace, "tables", Table],
        ?query_params => [],
        ?payload => Payload
    },
    case do_request(Client, Context) of
        {ok, LoadedTable} ->
            {ok, LoadedTable};
        {error, {http_error, 404, _StatusStr, _Body, _Headers}} ->
            {error, not_found};
        Error ->
            Error
    end.

-spec update_table(t(), namespace(), table(), map()) ->
    {ok, term()}
    | {error, conflict}
    | {error, term()}.
update_table(Client, Namespace, Table, Payload) ->
    Method = post,
    Context = #{
        ?method => Method,
        ?path_parts => [arn, "namespaces", Namespace, "tables", Table],
        ?query_params => [],
        ?payload => emqx_utils_json:encode(Payload)
    },
    case do_request(Client, Context) of
        {ok, Result} ->
            {ok, Result};
        {error, {http_error, 409, _StatusStr, Body, _Headers}} ->
            ?SLOG(debug, #{
                msg => "iceberg_commit_conflict",
                body => Body
            }),
            {error, conflict};
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

do_request(Client, Context) ->
    #{?aws_config := AWSConfig} = Client,
    #{
        ?method := Method,
        ?payload := Payload
    } = Context,
    Headers = sign_request(Client, Context),
    URI = mk_uri(Client, Context),
    AWSRequest = #aws_request{
        service = ?SERVICE,
        method = Method,
        uri = URI,
        request_headers = Headers,
        request_body = Payload
    },
    AWSResponse = erlcloud_retry:request(AWSConfig, AWSRequest, fun aws_result_fn/1),
    map_aws_response(AWSResponse).

map_aws_response(AWSResponse) ->
    maybe
        {ok, {_RespHeaders, Body}} ?= erlcloud_aws:request_to_return(AWSResponse),
        Response =
            case emqx_utils_json:safe_decode(Body) of
                {ok, Decoded} -> Decoded;
                {error, _} -> Body
            end,
        {ok, Response}
    else
        Error -> map_socket_errors(Error)
    end.

aws_result_fn(#aws_request{response_type = ok} = Request) ->
    Request;
aws_result_fn(
    #aws_request{
        response_type = error,
        error_type = aws,
        response_status = Status
    } = Request
) when
    Status >= 500
->
    Request#aws_request{should_retry = true};
aws_result_fn(#aws_request{response_type = error, error_type = aws} = Request) ->
    Request#aws_request{should_retry = false}.

sign_request(Client, Context) ->
    #{
        ?host := Host,
        ?aws_config := AWSConfig
    } = Client,
    Region = erlcloud_aws:aws_region_from_host(Host),
    #{
        ?method := Method,
        ?payload := Payload
    } = Context,
    QueryParams = maps:get(?query_params, Context, []),
    Headers0 = maps:get(?headers, Context, []),
    Headers = [{"host", Host} | Headers0],
    %% Note: even though the path may contain an already encoded ARNs, we have to encode
    %% it _again_ here for signing.  i.e., colons in the parts which already are `%3A`
    %% become `%253A` for signing...
    Path = mk_path(sign, Client, Context),
    Date = erlcloud_aws:iso_8601_basic_time(),
    erlcloud_aws:sign_v4(
        Method,
        Path,
        AWSConfig,
        Headers,
        Payload,
        Region,
        ?SERVICE_STR,
        QueryParams,
        Date
    ).

-spec mk_uri(_Client, _Context) -> _.
mk_uri(Client, Context) ->
    #{?base_uri := BaseURI} = Client,
    Path = mk_path(request, Client, Context),
    QueryParams0 = maps:get(?query_params, Context, []),
    QueryParams =
        case QueryParams0 of
            [] -> [];
            _ -> ["?", QueryParams0]
        end,
    lists:flatten([BaseURI, Path, QueryParams]).

mk_path(Kind, Client, Context) ->
    #{?base_path := BasePath} = Client,
    #{?path_parts := PathParts0} = Context,
    PathParts =
        lists:map(
            fun
                (arn) ->
                    mk_arn(Kind, Client);
                (Part) ->
                    Part
            end,
            PathParts0
        ),
    ["/", lists:join("/", BasePath ++ PathParts)].

mk_arn(pure, Client) ->
    #{
        ?host := Host,
        ?account_id := AccountId,
        ?bucket := Bucket
    } = Client,
    Region = erlcloud_aws:aws_region_from_host(Host),
    lists:flatten(
        io_lib:format(
            "arn:aws:s3tables:~s:~s:bucket/~s",
            [Region, AccountId, Bucket]
        )
    );
mk_arn(request, Client) ->
    ARN = mk_arn(pure, Client),
    erlcloud_http:url_encode(ARN);
mk_arn(sign, Client) ->
    ARN = mk_arn(request, Client),
    erlcloud_http:url_encode(ARN).

infer_account_id(#{account_id := AccountId}, _AWSConfig) when AccountId /= undefined ->
    {ok, str(AccountId)};
infer_account_id(_Params, AWSConfig) ->
    %% Should we try replace `sts_host` with inferred region from base endpoint for faster
    %% request?
    try erlcloud_sts:get_caller_identity(AWSConfig) of
        {ok, Props} ->
            {account, AccountId} = lists:keyfind(account, 1, Props),
            {ok, AccountId}
    catch
        Kind:Error:Stacktrace ->
            ?SLOG(warning, #{
                msg => "failed_to_infer_account_id",
                connector_type => ?CONNECTOR_TYPE,
                reason => {Kind, Error},
                stacktrace => Stacktrace
            }),
            {error, {failed_to_infer_account_id, {Kind, Error}}}
    end.

str(X) -> emqx_utils_conv:str(X).

map_socket_errors({error, {socket_error, timeout = Reason}}) ->
    {error, Reason};
map_socket_errors({error, {socket_error, {econnrefused = Reason, _Stacktrace}}}) ->
    {error, Reason};
map_socket_errors(Error) ->
    Error.
