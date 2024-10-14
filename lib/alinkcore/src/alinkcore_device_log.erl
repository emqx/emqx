%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 24. 4月 2023 下午2:05
%%%-------------------------------------------------------------------
-module(alinkcore_device_log).

%% API
-export([
    auth/6,
    up_data/4,
    up_data/5,
    down_data/4,
    down_data/5,
    disconnected/4
]).

%%%===================================================================
%%% API
%%%===================================================================
auth(Addr, ProductId, Data, Result, Reason, Ip) ->
    case Result of
        <<"1">> ->
            alinkcore_device_event:online(Addr);
        _ ->
            ok
    end,
    Log =
        #{
            <<"addr">> => alinkutil_type:to_binary(Addr),
            <<"productId">> => ProductId,
            <<"data">> => Data,
            <<"result">> => Result,
            <<"reason">> => alinkutil_type:to_binary(Reason),
            <<"time">> => erlang:system_time(second),
            <<"event">> => <<"auth">>,
            <<"ip">> => Ip
        },
    record(Log).

up_data(Addr, ProductId, Data, Ip) ->
    up_data(Addr, ProductId, Data, Ip, binary).

up_data(Addr, ProductId, Data, Ip, DataType) ->
    Log =
        #{
            <<"addr">> => alinkutil_type:to_binary(Addr),
            <<"productId">> => ProductId,
            <<"data">> => format_data(Data, DataType),
            <<"result">> => <<"1">>,
            <<"reason">> => <<>>,
            <<"time">> => erlang:system_time(second),
            <<"event">> => <<"up_data">>,
            <<"ip">> => Ip
        },
    record(Log).



down_data(Addr, ProductId, Data, Ip) ->
    down_data(Addr, ProductId, Data, Ip, binary).

down_data(Addr, ProductId, Data, Ip, DataType) ->
    Log =
        #{
            <<"addr">> => alinkutil_type:to_binary(Addr),
            <<"productId">> => ProductId,
            <<"data">> => format_data(Data, DataType),
            <<"result">> => <<"1">>,
            <<"reason">> => <<>>,
            <<"time">> => erlang:system_time(second),
            <<"event">> => <<"down_data">>,
            <<"ip">> => Ip
        },
    record(Log).


disconnected(Addr, ProductId, Reason, Ip) ->
    alinkcore_device_event:offline(Addr, Reason),
    Log =
        #{
            <<"addr">> => alinkutil_type:to_binary(Addr),
            <<"productId">> => ProductId,
            <<"data">> => <<>>,
            <<"result">> => <<"1">>,
            <<"reason">> => alinkutil_type:to_binary(Reason),
            <<"time">> => erlang:system_time(second),
            <<"event">> => <<"logout">>,
            <<"ip">> => Ip
        },
    record(Log).


record(Log) ->
    case catch alinkdata_hooks:run('device.log', [Log]) of
        ok ->
            ok;
        Err ->
            logger:error("run hook device log ~p failed :~p", [Log, Err])
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================
format_data(Data, hex) ->
    DataB = alinkutil_type:to_binary(Data),
    case catch binary:encode_hex(DataB) of
        {'EXIT', Reason} ->
            logger:error("encode data ~p to hex failed ~p", [DataB, Reason]),
            <<>>;
        Hex ->
            Hex
    end;
format_data(Data, _DataType) ->
    alinkutil_type:to_binary(Data).