%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_msk_iam_authn).

%% API
-export([token_callback/1, token_callback/2, mk_token_callback/1, generate_token/1]).

-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("emqx/include/logger.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(SERVICE, "kafka-cluster").
%% erlcloud_aws:base16(erlcloud_util:sha256(""))
-define(EMPTY_PAYLOAD_HASH, <<"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855">>).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

token_callback(Context) ->
    token_callback(Context, _Opts = #{}).

token_callback(_Context, Opts) ->
    try
        Token = generate_token(Opts),
        {ok, #{token => Token}}
    catch
        throw:#{msg := _} = LogCtx ->
            ?SLOG(warning, LogCtx),
            {error, LogCtx};
        Kind:Reason:Stacktrace ->
            ?SLOG(warning, #{
                msg => "kafka_msk_token_exception",
                kind => Kind,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, failed_to_fetch_token}
    end.

mk_token_callback(#{} = Opts) ->
    fun(Context) -> token_callback(Context, Opts) end.

generate_token(Opts0) ->
    Opts1 = maps:with([endpoint], Opts0),
    Opts2 = emqx_utils_maps:update_if_present(endpoint, fun str/1, Opts1),
    AWSConfig = load_config(Opts2),
    Region0 =
        case Opts0 of
            #{region := Region1} ->
                Region1;
            _ ->
                get_metadata("placement/region", AWSConfig, Opts2, "fetch_region")
        end,
    Region = str(Region0),
    Date = erlcloud_aws:iso_8601_basic_time(),
    Service = ?SERVICE,
    Credential = erlcloud_aws:credential(AWSConfig, Date, Region, Service),
    Method = get,
    Host = lists:flatten(io_lib:format("kafka.~s.amazonaws.com", [Region])),
    SecurityToken = AWSConfig#aws_config.security_token,
    QueryParamsToSign = [
        {"Action", "kafka-cluster:Connect"},
        {"X-Amz-Security-Token", SecurityToken},
        {"X-Amz-Algorithm", "AWS4-HMAC-SHA256"},
        {"X-Amz-Credential", Credential},
        {"X-Amz-Date", Date},
        {"X-Amz-Expires", "900"},
        {"X-Amz-SignedHeaders", "host"}
    ],
    PayloadHash = ?EMPTY_PAYLOAD_HASH,
    Path = "/",
    Headers = [{"Host", Host}],
    Context = #{
        config => AWSConfig,
        method => Method,
        date => Date,
        region => Region,
        path => Path,
        query_params => QueryParamsToSign,
        headers => Headers,
        payload => PayloadHash
    },
    Signature = signature(Context),
    ExtraQueryParams = [
        {"User-Agent", "emqx/" ++ emqx_release:version()}
    ],
    QueryParams = lists:flatten([
        QueryParamsToSign,
        [{"X-Amz-Signature", Signature}],
        ExtraQueryParams
    ]),
    QS = erlcloud_http:make_query_string(QueryParams, no_assignment),
    URI = iolist_to_binary(["https://", Host, "?", QS]),
    base64:encode(URI, #{padding => false, mode => urlsafe}).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

load_config(Opts0) ->
    AWSConfig0 = erlcloud_aws:default_config(),
    Token = get_session_token(AWSConfig0, Opts0),
    Opts = Opts0#{session_token => Token},
    Body0 = get_metadata("iam/security-credentials/", AWSConfig0, Opts, "list_roles"),
    [Role0 | _] = binary:split(Body0, <<$\n>>),
    Role = binary_to_list(Role0),
    Body1 = get_metadata(
        "iam/security-credentials/" ++ Role, AWSConfig0, Opts, "get_security_token"
    ),
    #{
        <<"AccessKeyId">> := AccessKeyIdBin,
        <<"SecretAccessKey">> := SecretAccessKeyBin,
        <<"Token">> := SecurityTokenBin
    } = emqx_utils_json:decode(Body1),
    maybe
        <<"">> ?= SecurityTokenBin,
        %% likely an ill-configured credential helper process
        throw(#{
            msg => "kafka_msk_credential_endpoint_returned_empty_token",
            opts => emqx_utils:redact(Opts0)
        })
    end,
    AWSConfig0#aws_config{
        access_key_id = binary_to_list(AccessKeyIdBin),
        secret_access_key = binary_to_list(SecretAccessKeyBin),
        security_token = binary_to_list(SecurityTokenBin)
    }.

signature(Context) ->
    #{
        config := AWSConfig,
        method := Method,
        date := Date,
        region := Region,
        path := Path,
        query_params := QueryParams,
        headers := Headers,
        payload := Payload
    } = Context,
    Service = ?SERVICE,
    CredScope = erlcloud_aws:credential_scope(Date, Region, Service),
    {CanonicalReq, _SignedHeaders} =
        erlcloud_aws:canonical_request(Method, Path, QueryParams, Headers, Payload),
    ToSign = erlcloud_aws:to_sign(Date, CredScope, CanonicalReq),
    SigningKey = erlcloud_aws:signing_key(AWSConfig, Date, Region, Service),
    [Res] = erlcloud_aws:base16(erlcloud_util:sha256_mac(SigningKey, ToSign)),
    Res.

str(X) -> emqx_utils_conv:str(X).

get_session_token(AWSConfig, Opts) ->
    case erlcloud_ec2_meta:get_metadata_v2_session_token(AWSConfig, Opts) of
        {ok, Token} ->
            Token;
        {error, Reason} ->
            throw(#{
                msg => "kafka_msk_failed_to_fetch_session_token",
                reason => Reason,
                opts => emqx_utils:redact(Opts)
            })
    end.

get_metadata(Path, AWSConfig, Opts, Step) ->
    case erlcloud_ec2_meta:get_instance_metadata_v2(Path, AWSConfig, Opts) of
        {ok, Token} ->
            Token;
        {error, Reason} ->
            throw(#{
                msg => "kafka_msk_failed_to_" ++ Step,
                reason => Reason,
                opts => emqx_utils:redact(maps:without([security_token], Opts))
            })
    end.
