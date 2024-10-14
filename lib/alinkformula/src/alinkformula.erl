%%%-------------------------------------------------------------------
%%% @author uzyiot
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. 9月 2024 23:56
%%%-------------------------------------------------------------------
-module(alinkformula).

%% API
-export([
    execute/3
]).

execute(#{<<"thing">> := Things} = Product, Device, OrigData) ->
    lists:foldl(
        fun(Thing, Acc) ->
            case get_formula(Thing, OrigData) of
                undefined ->
                    Acc;
                Formula ->
                    apply_formula(Formula, Thing, Product, Device, OrigData)
            end
        end, OrigData, Things).


get_formula(#{<<"name">> := Name} = Thing, OrigData) ->
    case maps:get(Name, OrigData, undefined) of
        #{<<"value">> := <<"alink_ignore", _/binary>>} ->
            undefined;
        _ ->
            case maps:get(<<"formula">>, Thing, undefined) of
                <<>> -> undefined;
                Other -> Other
            end
    end.

apply_formula(<<"add_x0">>, _Thing, Product, Device, OrigData) ->
    _Config = get_config(Product, Device),
    OrigData;
apply_formula(_, _, _, _, OrigData) ->
    OrigData.

get_config(Product, Device) ->
    Config1 = maps:get(<<"config">>, Product, #{}),
    Config2 = maps:get(<<"config">>, Device, #{}),
    maps:merge(Config1, Config2).
