%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 28. 2月 2023 上午12:50
%%%-------------------------------------------------------------------
-module(alinkdata_wechat_server).
-author("yqfclid").

-include_lib("xmerl/include/xmerl.hrl").

-ehttpd_router(alinkdata_wechat_server).

%% API
-export([
    route/2,
    init/2
]).

%%%===================================================================
%%% API
%%%===================================================================
route(_, _) ->
    [{"/alinkiot", alinkdata_wechat_server, []}].


init(Req0, Opts) ->
    logger:debug("receive wechat reqest: ~p", [Req0]),
    Method = cowboy_req:method(Req0),
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    Reply = handle_req(Method, Body, Req1),
    {ok, Reply, Opts}.
%%%===================================================================
%%% Internal functions
%%%===================================================================
handle_req(<<"GET">>, _Body, Req) ->
    Infos = cowboy_req:parse_qs(Req),
    EchoStr = proplists:get_value(<<"echostr">>, Infos, <<>>),
    cowboy_req:reply(200, headers(), EchoStr, Req);
handle_req(<<"POST">>, RawData, Req) ->
    {XmlElement, _} = xmerl_scan:string(alinkutil_type:to_list(RawData)),
    WechatEvent = parse_wechat_xml(XmlElement),
    alinkdata_wechat:handle_event(WechatEvent),
    cowboy_req:reply(200, headers(), <<"success">>, Req);
handle_req(_, _, Req) ->
    invalid_request(Req).




headers() ->
    #{<<"content-type">> => <<"text/plain; charset=utf-8">>,
        <<"Access-Control-Allow-Origin">> => <<"*">>,
        <<"Access-Control-Allow-Credentials">> => <<"true">>,
        <<"Access-Control-Allow-Headers">> => <<"Origin,Content-Type,Accept,token,X-Requested-With">>}.


invalid_request(Req) ->
    cowboy_req:reply(400, headers(), <<"invalid request">>, Req).



parse_wechat_xml(#xmlElement{content = Contents}) ->
    lists:foldl(
        fun(#xmlElement{name = K, content = [#xmlText{value = V}|_]}, Acc) ->
            Acc#{alinkutil_type:to_binary(K) => alinkutil_type:to_binary(V)};
           (_, Acc) ->
            Acc
    end, #{}, Contents).
