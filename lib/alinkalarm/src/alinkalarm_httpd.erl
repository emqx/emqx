%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 16. 3月 2023 下午11:45
%%%-------------------------------------------------------------------
-module(alinkalarm_httpd).
-author("yqfclid").

-ehttpd_router(alinkalarm_httpd).
%% API
-export([
    route/2,
    init/2
]).

%%%===================================================================
%%% API
%%%===================================================================
route(_, _) ->
    [{"/iotapi/alinkalarm/test", ?MODULE, []}].


init(Req0, Opts) ->
    logger:debug("receive wechat reqest: ~p", [Req0]),
    Method = cowboy_req:method(Req0),
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    Reply = handle_req(Method, Body, Req1),
    {ok, Reply, Opts}.
%%%===================================================================
%%% Internal functions
%%%===================================================================
handle_req(<<"POST">>, RawData, Req) ->
    #{
        <<"stat">> := Stat,
        <<"deviceAddr">> := DeviceAddr,
        <<"productId">> := ProductId
    } = jiffy:decode(RawData, [return_maps]),
    alinkalarm:test(DeviceAddr, ProductId, Stat),
    cowboy_req:reply(200, headers(), <<"ok">>, Req);
handle_req(_, _, Req) ->
    invalid_request(Req).




headers() ->
    #{<<"content-type">> => <<"text/plain; charset=utf-8">>,
        <<"Access-Control-Allow-Origin">> => <<"*">>,
        <<"Access-Control-Allow-Credentials">> => <<"true">>,
        <<"Access-Control-Allow-Headers">> => <<"Origin,Content-Type,Accept,token,X-Requested-With">>}.


invalid_request(Req) ->
    cowboy_req:reply(400, headers(), <<"invalid request">>, Req).

