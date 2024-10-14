%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2023 上午12:56
%%%-------------------------------------------------------------------
-module(alinkiot_tdengine_common).
-author("yqfclid").

%% API
-export([
    to_sql_bin/1,
    transform_field_type/1,
    to_td_field_name/1,
    from_td_field_name/1,
    rows_from_td_field_name/1,
    thing_field_precision_type/1
]).

%%%===================================================================
%%% API
%%%===================================================================
to_sql_bin(V) when is_binary(V)->
    <<"'", V/binary, "'">>;
to_sql_bin(V) ->
    alinkutil_type:to_binary(V).




thing_field_precision_type(#{<<"type">> := Type} = Thing) when Type =:= <<"int">>
                                            orelse Type =:= <<"integer">> ->
    Check =
        {
            maps:get(<<"precision">>, Thing, 0),
            maps:get(<<"offset">>, Thing, 0),
            maps:get(<<"rate">>, Thing, 1),
            maps:get(<<"formula">>, Thing, undefined)
        },
    case Check of
        {0, 0, Rate, undefined} when is_integer(Rate)->
            Type;
        _ ->
            <<"float">>
    end;
thing_field_precision_type(#{<<"type">> := Type}) ->
    Type.

transform_field_type(<<"time">>) ->
    <<"TIMESTAMP">>;
transform_field_type(<<"int">>) ->
    <<"INT">>;
transform_field_type(<<"integer">>) ->
    <<"INT">>;
transform_field_type(<<"float">>) ->
    <<"FLOAT">>;
transform_field_type(<<"string">>) ->
    <<"VARCHAR(100)">>;
transform_field_type(<<"array">>) ->
    <<"VARCHAR(4096)">>;
transform_field_type(Type) ->
    Type.

to_td_field_name(<<"alink_", _/binary>> = F) ->
    F;
to_td_field_name(F) ->
    <<"alink_", F/binary>>.


from_td_field_name(<<"alink_", F/binary>>) ->
    F;
from_td_field_name(F) ->
    F.


rows_from_td_field_name(Rows) ->
    lists:map(fun row_from_td_field_name/1, Rows).


row_from_td_field_name(Row) ->
    maps:fold(
        fun(K, V, Acc) ->
            NK = from_td_field_name(K),
            Acc#{NK => V}
    end, #{}, Row).
%%%===================================================================
%%% Internal functions
%%%===================================================================