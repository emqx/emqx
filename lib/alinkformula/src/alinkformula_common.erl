%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2024, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 25. 8月 2024 下午4:47
%%%-------------------------------------------------------------------
-module(alinkformula_common).
-author("yqfclid").

%% API
-export([
    add_x0/4,
    diff_x0/4
]).

add_x0(_Thing, #{<<"config">> := Config}, _, V) when is_float(V) orelse is_integer(V)->
    lists:foldl(
        fun(#{<<"name">> := Name, <<"value">> := Value}, Acc) ->
            case Name =:= <<"x0">> of
                true when is_binary(Value) ->
                    Acc + binary_to_integer(Value);
                true ->
                    Acc + Value;
                _ ->
                    Acc
            end
        end, V, Config).


diff_x0(_Thing, #{<<"config">> := Config}, _, V) when is_float(V) orelse is_integer(V)->
    lists:foldl(
        fun(#{<<"name">> := Name, <<"value">> := Value}, Acc) ->
            case Name =:= <<"h0">> of
                true when is_binary(Value) ->
                    VNum =
                        case binary:match(Value, <<".">>) of
                            nomatch ->
                                binary_to_integer(Value);
                            _ ->
                                binary_to_float(Value)
                        end,
                    Acc - VNum;
                true ->
                    Acc - Value;
                _ ->
                    Acc
            end
        end, V, Config).