%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 13. 4月 2023 下午11:32
%%%-------------------------------------------------------------------
-module(alinkiot_funs).

%% API
-export([
    to_string/4,
    add_x0/4,
    diff_h0/4
]).
%%%===================================================================
%%% API
%%%===================================================================
to_string(_, _, _, V) when is_integer(V) ->
    integer_to_binary(V);
to_string(_, _, _, V) when is_float(V) ->
    float_to_binary(V);
to_string(_, _, _, V) ->
    V.

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


diff_h0(_Thing, #{<<"config">> := Config}, _, V) when is_float(V) orelse is_integer(V)->
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

%%%===================================================================
%%% Internal functions
%%%===================================================================
