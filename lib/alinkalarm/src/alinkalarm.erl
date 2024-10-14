%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023
%%% @doc
%%%
%%% @end
%%% Created : 06. 3月 2023 下午1:54
%%%-------------------------------------------------------------------
-module(alinkalarm).
-author("yqfclid").

%% API
-export([
    start/0,
    test/3
]).

%%%===================================================================
%%% API
%%%===================================================================
start() ->
    application:ensure_all_started(alinkalarm).

test(DeviceAddr, ProductId, Stat) ->
    MsgStat = #{
        <<"stat">> => Stat,
        <<"deviceAddr">> => DeviceAddr,
        <<"productId">> => ProductId
    },
    alinkalarm_handler:handle(MsgStat).
%%%===================================================================
%%% Application callbacks
%%%===================================================================
