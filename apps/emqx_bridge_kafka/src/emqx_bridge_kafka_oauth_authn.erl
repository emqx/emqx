%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_oauth_authn).

%% API
-export([mk_token_callback/1]).

-include_lib("emqx/include/logger.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

mk_token_callback(#{grant_type := client_credentials} = Opts) ->
    #{
        endpoint_uri := URI,
        client_id := ClientId,
        client_secret := ClientSecret
    } = Opts,
    fun(_Context) ->
        Scope = maps:get(scope, Opts, undefined),
        Params = lists:flatten([
            {"grant_type", "client_credentials"},
            {"client_id", ClientId},
            {"client_secret", emqx_secret:term(ClientSecret)},
            [{"scope", Scope} || Scope /= undefined]
        ]),
        Body = uri_string:compose_query(Params),
        Timeout = maps:get(timeout, Opts, 30_000),
        ConnectTimeout = maps:get(connect_timeout, Opts, 15_000),
        Resp = httpc:request(
            post,
            {str(URI), _Headers = [], "application/x-www-form-urlencoded", Body},
            [
                {timeout, Timeout},
                {connect_timeout, ConnectTimeout}
            ],
            [{body_format, binary}]
        ),
        case Resp of
            {ok, {{_, 200, _}, _, RespBody}} ->
                case emqx_utils_json:safe_decode(RespBody) of
                    {ok, #{<<"access_token">> := Token}} ->
                        {ok, #{token => Token}};
                    {ok, BadResp} ->
                        {error, {bad_token_response, BadResp}};
                    {error, Reason} ->
                        {error, {bad_token_response, Reason}}
                end;
            {ok, {{_, Status, _}, Headers, BadResp}} ->
                Details = #{status => Status, headers => Headers, body => BadResp},
                {error, {bad_token_response, Details}};
            {ok, {{_, Status, _}, BadResp}} ->
                Details = #{status => Status, headers => [], body => BadResp},
                {error, {bad_token_response, Details}};
            {ok, BadResp} ->
                {error, {bad_token_response, BadResp}};
            {error, Reason} ->
                {error, {failed_to_fetch_token, Reason}}
        end
    end.

str(X) -> emqx_utils_conv:str(X).
