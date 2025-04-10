%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_validator).

-export([
    min/2,
    max/2,
    not_empty/1
]).

max(Type, Max) ->
    limit(Type, '=<', Max).

min(Type, Min) ->
    limit(Type, '>=', Min).

not_empty(ErrMsg0) ->
    ErrMsg =
        try
            lists:flatten(ErrMsg0)
        catch
            _:_ ->
                ErrMsg0
        end,
    fun
        (undefined) -> {error, ErrMsg};
        (<<>>) -> {error, ErrMsg};
        ("") -> {error, ErrMsg};
        (_) -> ok
    end.

limit(Type, Op, Expected) ->
    L = len(Type),
    fun(Value) ->
        Got = L(Value),
        return(
            erlang:Op(Got, Expected),
            err_limit({Type, {Op, Expected}, {got, Got}})
        )
    end.

len(array) -> fun erlang:length/1;
len(string) -> fun string:length/1;
len(_Type) -> fun(Val) -> Val end.

err_limit({Type, {Op, Expected}, {got, Got}}) ->
    Msg = io_lib:format("Expect the ~ts value ~ts ~p but got: ~p", [Type, Op, Expected, Got]),
    lists:flatten(Msg).

return(true, _) -> ok;
return(false, Error) -> {error, Error}.
