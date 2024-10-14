%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 8月 2023 上午10:50
%%%-------------------------------------------------------------------
-module(alinkdata_lanzun_gnss).
-author("yqfclid").

-export([
    get_username_password/1,
    get_token/1,
    fresh_token/1,
    get_access_token/2,
    get_data/4
]).


-ehttpd_router(alinkdata_yinshi).

%%%===================================================================
%%% API
%%%===================================================================
get_username_password(ProductId) ->
    case alinkdata_dao:query_no_count('QUERY_product', #{<<"id">> => ProductId}) of
        {ok, [#{<<"config">> := Config}]} ->
            ConfigMap =
                case catch jiffy:decode(Config, [return_maps]) of
                    {'EXIT', _} ->
                        [];
                    M ->
                        M
                end,
            Username = find_key_from_config_list(<<"username">>, ConfigMap, <<>>),
            Password = find_key_from_config_list(<<"password">>, ConfigMap, <<>>),
            {ok, Username, Password};
        {error, Reason} ->
            {error, Reason}
    end.



get_token(ProductId) ->
    ProductIdB = alinkutil_type:to_binary(ProductId),
    Key = <<"lanzun_token_", ProductIdB/binary>>,
    case ets:lookup(alinkdata_wechat, Key) of
        [] ->
            fresh_token(ProductId);
        [{_, AccessToken}] ->
            {ok, AccessToken}
    end.


fresh_token(ProductId) ->
    ProductIdB = alinkutil_type:to_binary(ProductId),
    Key = <<"lanzun_token_", ProductIdB/binary>>,
    case get_username_password(ProductId) of
        {ok, Username, Password} ->
            case get_access_token(Username, Password) of
                {ok, AccessToken} ->
                    ets:insert(alinkdata_wechat, {Key, AccessToken}),
                    {ok, AccessToken};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

get_access_token(Username, Password) ->
    Url = <<"https://iot.lzkjiot.com/api/auth/login">>,
    Body = jiffy:encode(#{<<"username">> => Username, <<"password">> => Password}),
    case http_request(Url, Body) of
        {ok, #{<<"token">> := Token}} ->
            {ok, Token};
        {error, Reason} ->
            {error, Reason}
    end.



get_data(ProductId, DeviceId, Start, End) ->
    F =
        fun(Token) ->
            Headers = [{<<"Authorization">>, <<"Bearer ", Token/binary>>}],
            StartB = alinkutil_type:to_binary(Start),
            EndB = alinkutil_type:to_binary(End),
            Url = <<"https://iot.lzkjiot.com/api/plugins/telemetry/DEVICE/", DeviceId/binary,
                    "/values/timeseries?keys=X,XI,Y,YI,H,HI&startTs=",
                    StartB/binary, "&endTs=", EndB/binary>>,
            http_request(get, Url, Headers, <<>>)
        end,
    request_with_token(ProductId, F).
%%%===================================================================
%%% Internal functions
%%%===================================================================
request_with_token(ProductId, Fun) ->
    case get_token(ProductId) of
        {ok, Token} ->
            case Fun(Token) of
                {error, {http, Code}} = Err->
                    case lists:member(Code, [401]) of
                        true ->
                            case fresh_token(ProductId) of
                                {ok, NewToken} ->
                                    Fun(NewToken);
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        false ->
                            Err
                    end;
                Other ->
                    Other
            end;
        {error, Reason} ->
            {error, Reason}
    end.



http_request(Url, Body) ->
    http_request(Url, [], Body).

http_request(Url, Headers, Body) ->
    http_request(post, Url, Headers, Body).



http_request(Method, Url, Headers, Body) ->
    case alinkdata_http:request(Method, Url, Headers, Body, [{ssl_options, [{depth, 2}]}]) of
        {ok, 200, _RespHeaders, RespBody} ->
            {ok, jiffy:decode(RespBody, [return_maps])};
        {ok, Code, _RespHeader, _RespBody} ->
            {error, {http, Code}};
        {error, Reason} ->
            {error, Reason}
    end.



find_key_from_config_list(_Name, [], Default) ->
    Default;
find_key_from_config_list(Name, [#{<<"name">> := Name, <<"value">> := Value}|_], _Default) ->
    Value;
find_key_from_config_list(Name, [_|T], Default) ->
    find_key_from_config_list(Name, T, Default).

