%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_oauth_authn).

%% API
-export([mk_token_callback/1]).

%% Internal exports (only for mocking/tests)
-export([do_request/1]).

-include_lib("jose/include/jose_jwt.hrl").
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
        client_id := TokenClientId,
        client_secret := TokenClientSecret
    } = Opts,
    RefreshFn = fun() ->
        Scope = maps:get(scope, Opts, undefined),
        Params = lists:flatten([
            {"grant_type", "client_credentials"},
            {"client_id", TokenClientId},
            {"client_secret", emqx_secret:term(TokenClientSecret)},
            [{"scope", Scope} || Scope /= undefined]
        ]),
        Body = uri_string:compose_query(Params),
        Timeout = maps:get(timeout, Opts, 30_000),
        ConnectTimeout = maps:get(connect_timeout, Opts, 15_000),
        Resp = ?MODULE:do_request(#{
            uri => URI,
            body => Body,
            timeout => Timeout,
            connect_timeout => ConnectTimeout
        }),
        case Resp of
            {ok, {{_, 200, _}, _, RespBody}} ->
                case emqx_utils_json:safe_decode(RespBody) of
                    {ok, #{<<"access_token">> := Token, <<"expires_in">> := ExpiryS}} ->
                        ExpiryMS = erlang:convert_time_unit(ExpiryS, second, millisecond),
                        {ok, ExpiryMS, Token};
                    {ok, #{<<"access_token">> := Token}} ->
                        ExpiryMS = get_expiry_ms(Token),
                        {ok, ExpiryMS, Token};
                    {ok, BadResp} ->
                        {error, {bad_token_response, BadResp}};
                    {error, Reason} ->
                        {error, {bad_token_response, Reason}}
                end;
            {ok, {{_, Status, _}, Headers, BadResp}} ->
                Details = #{status => Status, headers => Headers, body => BadResp},
                {error, {bad_token_response, Details}};
            {error, Reason} ->
                {error, {failed_to_fetch_token, Reason}}
        end
    end,
    fun(#{client_id := KafkaClientId} = _Context) ->
        case emqx_bridge_kafka_token_cache:get_or_refresh(KafkaClientId, RefreshFn) of
            {ok, Token} ->
                {ok, #{token => Token}};
            {error, Reason} ->
                {error, Reason}
        end
    end.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%% Only exposed for mocking/tests
do_request(Params) ->
    #{
        uri := URI,
        timeout := Timeout,
        connect_timeout := ConnectTimeout,
        body := Body
    } = Params,
    httpc:request(
        post,
        {str(URI), _Headers = [], "application/x-www-form-urlencoded", Body},
        [
            {timeout, Timeout},
            {connect_timeout, ConnectTimeout}
        ],
        [{body_format, binary}]
    ).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

str(X) -> emqx_utils_conv:str(X).

now_ms() ->
    erlang:system_time(millisecond).

get_expiry_ms(Token) ->
    try jose_jwt:peek(Token) of
        #jose_jwt{fields = #{<<"exp">> := ExpS}} ->
            ExpMS = erlang:convert_time_unit(ExpS, second, millisecond),
            max(0, ExpMS - now_ms());
        _ ->
            %% Malformed token?
            timer:seconds(15)
    catch
        _:_ ->
            %% Malformed token.
            timer:seconds(15)
    end.
