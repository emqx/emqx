%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kinesis_sts).

-moduledoc """
Obtains temporary credentials with the STS `AssumeRole` action.

The caller owns the returned credentials and decides when to refresh them.  Nothing is cached
here, so two callers with the same role and different base credentials never share the result.
""".

-include_lib("erlcloud/include/erlcloud_aws.hrl").

-export([
    new/3,
    assume_role/2,
    default_endpoint/1
]).

-export_type([t/0, credentials/0]).

-define(API_VERSION, "2011-06-15").
-define(SESSION_NAME, "emqx-kinesis").
-define(DURATION_SECONDS, 3600).
-define(DEFAULT_PORT, 443).
-define(GREGORIAN_EPOCH_OFFSET, 62167219200).

-type t() :: #{
    role_arn := string(),
    external_id := string() | undefined,
    scheme := string(),
    host := string(),
    port := inet:port_number()
}.

-type credentials() :: #{
    access_key_id := string(),
    secret_access_key := string(),
    session_token := string(),
    %% Seconds since the Unix epoch.
    expiration := integer()
}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc """
Creates the STS options for a role.

`Endpoint` is the URL of the STS endpoint, for example `https://sts.us-east-1.amazonaws.com`.
""".
-spec new(binary(), binary() | undefined, binary()) -> t().
new(RoleArn, ExternalId, Endpoint) ->
    #{scheme := Scheme, hostname := Host, port := Port} =
        emqx_schema:parse_server(
            Endpoint,
            #{default_port => ?DEFAULT_PORT, supported_schemes => ["http", "https"]}
        ),
    #{
        role_arn => to_str(RoleArn),
        external_id => maybe_str(ExternalId),
        scheme => Scheme,
        host => Host,
        port => Port
    }.

-doc """
Calls `AssumeRole` and signs the request with the resolved base credentials in `BaseConfig`.
""".
-spec assume_role(t(), aws_config()) ->
    {ok, credentials()} | {error, {failed_to_assume_role, term()}}.
assume_role(Opts, BaseConfig) ->
    #{role_arn := RoleArn, external_id := ExternalId, scheme := Scheme, host := Host, port := Port} =
        Opts,
    Params =
        [
            {"Action", "AssumeRole"},
            {"Version", ?API_VERSION},
            {"RoleArn", RoleArn},
            {"RoleSessionName", ?SESSION_NAME},
            {"DurationSeconds", ?DURATION_SECONDS}
        ] ++ [{"ExternalId", ExternalId} || ExternalId =/= undefined],
    try erlcloud_aws:aws_request_xml4(post, Scheme, Host, Port, "/", Params, "sts", BaseConfig) of
        {ok, Xml} ->
            parse_credentials(Xml);
        {error, Reason} ->
            {error, {failed_to_assume_role, format_error(Reason)}}
    catch
        Class:Reason ->
            {error, {failed_to_assume_role, {Class, Reason}}}
    end.

-doc """
Returns the regional STS endpoint for the region of a Kinesis endpoint host.
""".
-spec default_endpoint(string()) -> binary().
default_endpoint(KinesisHost) ->
    Region = erlcloud_aws:aws_region_from_host(KinesisHost),
    Suffix =
        case Region of
            "cn-" ++ _ -> ".amazonaws.com.cn";
            _ -> ".amazonaws.com"
        end,
    iolist_to_binary(["https://sts.", Region, Suffix]).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

parse_credentials(Xml) ->
    Get = fun(Name) ->
        erlcloud_xml:get_text("AssumeRoleResult/Credentials/" ++ Name, Xml, undefined)
    end,
    Expiration = erlcloud_xml:get_time("AssumeRoleResult/Credentials/Expiration", Xml),
    case {Get("AccessKeyId"), Get("SecretAccessKey"), Get("SessionToken"), Expiration} of
        {KeyId, Secret, Token, {{_, _, _}, {_, _, _}} = DateTime} when
            is_list(KeyId), is_list(Secret), is_list(Token)
        ->
            {ok, #{
                access_key_id => KeyId,
                secret_access_key => Secret,
                session_token => Token,
                expiration =>
                    calendar:datetime_to_gregorian_seconds(DateTime) - ?GREGORIAN_EPOCH_OFFSET
            }};
        _ ->
            {error, {failed_to_assume_role, invalid_response}}
    end.

%% An STS error response has this shape:
%% `<ErrorResponse><Error><Code>..</Code><Message>..</Message></Error></ErrorResponse>'
format_error({http_error, Status, _StatusLine, Body}) ->
    try xmerl_scan:string(binary_to_list(Body), [{quiet, true}]) of
        {Doc, _} ->
            #{
                http_status => Status,
                code => list_to_binary(erlcloud_xml:get_text("Error/Code", Doc)),
                message => unicode:characters_to_binary(erlcloud_xml:get_text("Error/Message", Doc))
            }
    catch
        _:_ ->
            #{http_status => Status, body => Body}
    end;
format_error({socket_error, Reason}) ->
    Reason;
format_error(Reason) ->
    Reason.

maybe_str(undefined) -> undefined;
maybe_str(<<>>) -> undefined;
maybe_str(Bin) -> to_str(Bin).

to_str(Bin) when is_binary(Bin) -> binary_to_list(Bin);
to_str(List) when is_list(List) -> List.
