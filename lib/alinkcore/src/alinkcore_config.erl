%%%-------------------------------------------------------------------
%%% @author weixingzheng
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. 7月 2024 23:38
%%%-------------------------------------------------------------------
-module(alinkcore_config).
-author("weixingzheng").

%% API
-export([find_by_name/2, find_by_type/2]).


find_by_type(Type, Opts) ->
    Query = #{
        <<"where">> => #{
            <<"config_key">> => Type,
            <<"config_type">> => <<"N">>
        }
    },
    format_result(alinkdata_mysql:query(default, <<"sys_config">>, Query), Opts).


find_by_name(Name, Opts) ->
    Query = #{
        <<"where">> => #{
            <<"config_name">> => Name
        }
    },
    format_result(alinkdata_mysql:query(default, <<"sys_config">>, Query), Opts).



format_result({error, Reason}, _Opts) -> {error, Reason};
format_result({ok, Rows}, Opts) ->
    Format = proplists:get_value(format, Opts, string),
    {ok, [ format_row(Format, Rows) || Rows <- Rows]}.

format_row(json, #{ <<"config_value">> := Value } = Item) ->
    Item1 = maps:without([<<"config_value">>], Item),
    maps:merge(Item1, jiffy:decode(Value, [return_maps]));
format_row(_, Item) -> Item.



