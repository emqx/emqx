-module(alinkdata_formater).
-include("alinkdata.hrl").

%% API
-export([save/2, format_tree/2, format_field/4, to_field/2, format_type/1]).


save(Table, Map) ->
    application:set_env(alinkdata, Table, Map).

format_tree(<<"dept">>, #{
    <<"deptId">> := Id,
    <<"deptName">> := Name,
    <<"parentId">> := ParentId
}) ->
    #{
        <<"label">> => Name,
        <<"id">> => Id,
        <<"pid">> => ParentId
    };
format_tree(<<"menu">>, #{
    <<"menuId">> := Id,
    <<"menuName">> := Name,
    <<"parentId">> := ParentId
}) ->
    #{
        <<"label">> => Name,
        <<"id">> => Id,
        <<"pid">> => ParentId
    }.

format_field(Table, Field, Value, Acc) ->
    Maps = application:get_env(alinkdata, Table, []),
    case keyfind(Field, 1, Maps) of
        false -> Acc;
        NewField ->
            Acc#{
                NewField => Value
            }
    end.

to_field(Table, Args) ->
    Maps = application:get_env(alinkdata, Table, []),
    maps:fold(
        fun(Field, Value, {Acc, Where}) ->
            case keyfind(Field, 2, Maps) of
                false ->
                    {Acc#{ Field => Value }, Where};
                Key ->
                    {Acc, Where#{Key => Value}}
            end
        end, {#{}, #{}}, Args).

format_type(<<"datetime">>) ->
    <<"string">>;
format_type(<<"text">>) ->
    <<"string">>;
format_type(<<"varchar", _/binary>>) ->
    <<"string">>;
format_type(<<"char", _/binary>>) ->
    <<"string">>;
format_type(<<"int", _/binary>>) ->
    <<"integer">>;
format_type(<<"longblob">>) ->
    <<"string">>;
format_type(<<"bit(1)">>) ->
    <<"boolean">>;
format_type(<<"bigint", _/binary>>) ->
    <<"integer">>;
format_type(<<"tinyint", _/binary>>) ->
    <<"integer">>;
format_type(Type) ->
    Type.

keyfind(_, _, undefined) -> false;
keyfind(Key, Idx, List) ->
    case lists:keyfind(Key, Idx, List) of
        false -> false;
        {_, Value} when Idx == 1 -> Value;
        {Value, _} when Idx == 2 -> Value
    end.
