%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 16. 6月 2023 下午1:30
%%%-------------------------------------------------------------------
-module(alinkdata_yinshi).

%% API
-export([
    route/2,
    init/2
]).

-export([
    get_key_secert/1,
    get_token/1,
    fresh_token/1,
    get_access_token/2,
    get_live_address/2,
    get_device_live_address/2,
    device_info/2,
    device_list/1,
    device_list/2,
    get_access_token_raw_return/2
]).


-ehttpd_router(alinkdata_yinshi).

%%%===================================================================
%%% API
%%%===================================================================
route(_, _) ->
    [{"/camera", ?MODULE, []}].

init(Req0, Opts) ->
    logger:debug("receive yinshi reqest: ~p", [Req0]),
    Method = cowboy_req:method(Req0),
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    CameraType = proplists:get_value(<<"type">>, cowboy_req:parse_qs(Req0), undefined),
    Reply = handle_req(Method, CameraType, Body, Req1),
    {ok, Reply, Opts}.

%%handle_req(<<"POST">>, RawData, Req) ->
%%    {XmlElement, _} = xmerl_scan:string(alinkutil_type:to_list(RawData)),
%%    WechatEvent = parse_wechat_xml(XmlElement),
%%    alinkdata_wechat:handle_event(WechatEvent),
%%    cowboy_req:reply(200, headers(), <<"success">>, Req);
handle_req(<<"POST">>, <<"yinshi">>, Body, Req) ->
    case jiffy:decode(Body, [return_maps]) of
        #{
            <<"body">> := #{
                <<"devSerial">> := DevSerial,
                <<"alarmTime">> := AlarmTime,
                <<"alarmType">> := AlarmType,
                <<"pictureList">> := PictureList
            },
            <<"header">> := #{
                <<"messageId">> := MessageId,
                <<"type">> := Type
            }
        } when Type =:= <<"ys.alarm">> ->
            Sql = <<"select addr, product from sys_device where config like '%", DevSerial/binary, "%'">>,
            case alinkdata:query_mysql_format_map(default, Sql) of
                {ok, [#{<<"addr">> := Addr, <<"product">> := Product}]} ->
                    PictureL = jiffy:encode(lists:map(fun(#{<<"url">> := Url}) -> Url end, PictureList)),
                    Time = format_ts(AlarmTime),
                    Data =
                        #{
                            <<"ts">> => Time,
                            <<"alarmType">> => #{<<"value">> => AlarmType},
                            <<"pictureList">> => #{<<"value">> => PictureL}
                        },
                    alinkalarm_consumer:message_publish(Product, Addr, Data);
                _ ->
                    ok
            end,
            cowboy_req:reply(200, headers(), jiffy:encode(#{<<"messageId">> => MessageId}), Req);
        #{<<"header">> := #{<<"messageId">> := MessageId}} ->
            cowboy_req:reply(200, headers(), jiffy:encode(#{<<"messageId">> => MessageId}), Req)
    end;
handle_req(_, _, _, Req) ->
    cowboy_req:reply(200, headers(), <<"ok">>, Req).

headers() ->
    #{<<"content-type">> => <<"text/plain; charset=utf-8">>,
        <<"Access-Control-Allow-Origin">> => <<"*">>,
        <<"Access-Control-Allow-Credentials">> => <<"true">>,
        <<"Access-Control-Allow-Headers">> => <<"Origin,Content-Type,Accept,token,X-Requested-With">>}.




get_key_secert(ProductId) ->
    case alinkdata_dao:query_no_count('QUERY_product', #{<<"id">> => ProductId}) of
        {ok, [#{<<"pk">> := AppKey, <<"ps">> := AppSecret}]} ->
                {ok, AppKey, AppSecret};
        {error, Reason} ->
            {error, Reason}
    end.



get_token(ProductId) ->
    ProductIdB = alinkutil_type:to_binary(ProductId),
    Key = <<"yinshi_access_token_", ProductIdB/binary>>,
    case ets:lookup(alinkdata_wechat, Key) of
        [] ->
            fresh_token(ProductId);
        [{_, AccessToken}] ->
            {ok, AccessToken}
    end.


fresh_token(ProductId) ->
    ProductIdB = alinkutil_type:to_binary(ProductId),
    Key = <<"yinshi_access_token_", ProductIdB/binary>>,
    case get_key_secert(ProductId) of
        {ok, AppKey, AppSecret} ->
            case get_access_token(AppKey, AppSecret) of
                {ok, AccessToken} ->
                    ets:insert(alinkdata_wechat, {Key, AccessToken}),
                    {ok, AccessToken};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

get_access_token(AppKey, AppSecret) ->
    Url = <<"https://open.ys7.com/api/lapp/token/get">>,
    Body = build_body([{<<"appKey">>,AppKey}, {<<"appSecret">>, AppSecret}]),
    case http_request(Url, Body) of
        {ok, #{<<"accessToken">> := AccessToken}} ->
            {ok, AccessToken};
        {error, Reason} ->
            {error, Reason}
    end.


get_access_token_raw_return(AppKey, AppSecret) ->
    Url = <<"https://open.ys7.com/api/lapp/token/get">>,
    Body = build_body([{<<"appKey">>,AppKey}, {<<"appSecret">>, AppSecret}]),
    case http_request(Url, Body) of
        {ok, Data} ->
            {ok, Data};
        {error, Reason} ->
            {error, Reason}
    end.

get_device_live_address(ProductId, DeviceId) ->
    get_live_address(ProductId, #{<<"deviceSerial">> => DeviceId, <<"protocol">> => 2}).


get_live_address(ProductId, Params) ->
    F =
        fun(Token) ->
            Url = <<"https://open.ys7.com/api/lapp/v2/live/address/get">>,
            NParams =Params#{<<"accessToken">> => Token},
            case http_request(Url, build_body(NParams)) of
                {ok, #{<<"url">> := LiveUrl}} ->
                    {ok, LiveUrl};
                {error, Reason} ->
                    {error, Reason}
            end
        end,
    request_with_token(ProductId, F).



device_info(ProductId, DeviceSerial) ->
    F =
        fun(Token) ->
            Url = <<"https://open.ys7.com/api/lapp/device/info">>,
            Params = #{<<"accessToken">> => Token, <<"deviceSerial">> => DeviceSerial},
            http_request(Url, build_body(Params))
        end,
    request_with_token(ProductId, F).


device_list(ProductId) ->
    device_list(ProductId, #{}).

device_list(ProductId, RParams) ->
    F =
        fun(Token) ->
            Url = <<"https://open.ys7.com/api/lapp/device/list">>,
            Params = #{
                <<"accessToken">> => Token,
                <<"pageStart">> => maps:get(<<"pageNum">>, RParams, 0),
                <<"pageSize">> => maps:get(<<"pageSize">>, RParams, 10)
            },
            http_request(Url, build_body(Params))
        end,
    request_with_token(ProductId, F).
%%%===================================================================
%%% Internal functions
%%%===================================================================
request_with_token(ProductId, Fun) ->
    case get_token(ProductId) of
        {ok, Token} ->
            case Fun(Token) of
                {error, {yinshi, Code}} = Err->
                    case lists:member(Code, [<<"10002">>]) of
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




build_body(Params) when is_list(Params)->
    Body =
        lists:foldl(
            fun({K, V}, Acc) ->
                KB = alinkutil_type:to_binary(K),
                VB = alinkutil_type:to_binary(V),
                case Acc of
                    <<>> ->
                        <<KB/binary, "=", VB/binary>>;
                    _ ->
                        <<Acc/binary, "&", KB/binary, "=", VB/binary>>
                end
        end, <<>>, Params),
    Body;
build_body(Params) when is_map(Params)->
    Body =
        maps:fold(
            fun(K, V, Acc) ->
                KB = alinkutil_type:to_binary(K),
                VB = alinkutil_type:to_binary(V),
                case Acc of
                    <<>> ->
                        <<KB/binary, "=", VB/binary>>;
                    _ ->
                        <<Acc/binary, "&", KB/binary, "=", VB/binary>>
                end
            end, <<>>, Params),
    Body.


http_request(Url, Body) ->
    Headers = [{<<"Content-Type">>, <<"application/x-www-form-urlencoded">>}],
    http_request(Url, Headers, Body).

http_request(Url, Headers, Body) ->
    http_request(post, Url, Headers, Body).



http_request(Method, Url, Headers, Body) ->
    case alinkdata_http:request(Method, Url, Headers, Body, [{ssl_options, [{depth, 2}]}]) of
        {ok, 200, _RespHeaders, RespBody} ->
            case jiffy:decode(RespBody, [return_maps]) of
                #{
                    <<"code">> := Code,
                    <<"data">> := Data
                } when Code =:= <<"200">> ->
                    {ok, Data};
                #{<<"code">> := Code} ->
                    {error, {yinshi, Code}};
                Err ->
                    {error, Err}
            end;
        {ok, Code, _RespHeader, _RespBody} ->
            {error, {http, Code}};
        {error, Reason} ->
            {error, Reason}
    end.



get_key_from_config(Name, Config)->
    case lists:member(Config, [null, undefined, <<>>]) of
        true ->
            undefined;
        false ->
            find_key_from_config_list(Name, jiffy:decode(Config, [return_maps]))
    end.


find_key_from_config_list(_Name, []) ->
    undefined;
find_key_from_config_list(Name, [#{<<"name">> := Name, <<"value">> := Value}|_]) ->
    Value;
find_key_from_config_list(Name, [_|T]) ->
    find_key_from_config_list(Name, T).


format_ts(<<Y:4/binary, "-", Mon:2/binary, "-", D:2/binary, "T", H:2/binary, ":", M:2/binary, ":", S:2/binary, _/binary>>) ->
    YI = binary_to_integer(Y),
    MonI = binary_to_integer(Mon),
    DI = binary_to_integer(D),
    HI = binary_to_integer(H),
    MI = binary_to_integer(M),
    SI = binary_to_integer(S),
    calendar:datetime_to_gregorian_seconds({{YI, MonI, DI}, {HI, MI, SI}}) - 3600 * 8 -
        calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}});
format_ts(_) ->
    erlang:system_time(second).



