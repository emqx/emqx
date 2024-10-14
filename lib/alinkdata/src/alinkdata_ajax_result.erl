%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 11. 8月 2022 上午12:47
%%%-------------------------------------------------------------------
-module(alinkdata_ajax_result).
-author("yqfclid").

%% API
-export([
    success_result/0,
    success_result/1,
    success_result/2,
    error_result/0,
    error_result/1,
    error_result/2,
    build/3
]).


-define(CODE_TAG, <<"code">>).
-define(MSG_TAG, <<"msg">>).
-define(DATA_TAG, <<"data">>).

-define(SUCCESS_MSG, <<"操作成功"/utf8>>).
-define(ERROR_MSG, <<"操作失败"/utf8>>).

-define(SUCCESS_CODE, 200).
-define(ERROR_CODE, 500).

%%%===================================================================
%%% API
%%%===================================================================
success_result() ->
    success_result(undefined).

success_result(Data) ->
    success_result(?SUCCESS_MSG, Data).


success_result(Msg, Data) ->
    build(?SUCCESS_CODE, Msg, Data).

error_result() ->
    error_result(?ERROR_MSG).

error_result(Msg) ->
    error_result(?ERROR_CODE, Msg).

error_result(Code, Msg) ->
    #{
        ?CODE_TAG => Code,
        ?MSG_TAG => Msg
    }.

build(Code, Msg, undefined) ->
    #{
        ?CODE_TAG => Code,
        ?MSG_TAG => Msg
    };
build(Code, Msg, Data) ->
    #{
        ?CODE_TAG => Code,
        ?MSG_TAG => Msg,
        ?DATA_TAG => Data
    }.
%%%===================================================================
%%% Internal functions
%%%===================================================================