-module(alinkdata_mysql).

%% API
-export([query/3, query/4, delete/3, update/4, create/3, get_schema/2, save/1]).

save(Pool) ->
    Format =
        fun(#{ <<"fields">> := Fields} = Info) ->
            {Id, NewFields} = format_field(Fields, [], undefined),
            case Id of
                undefined ->
                    Info#{
                        <<"fields">> => NewFields
                    };
                _ ->
                    Info#{
                        <<"id">> => Id,
                        <<"fields">> => NewFields
                    }
            end
        end,
    case get_schema(Pool, Format) of
        {error, Reason} ->
            {error, Reason};
        {ok, Tables} ->
            Path = code:priv_dir(alinkdata),
            Dir = filename:join([Path, "table", "tables.json"]),
            file:write_file(Dir, jiffy:encode(Tables))
    end.


format_field([], Acc, Alias) -> {Alias, Acc};
format_field([#{<<"name">> := Name} = Field | Fields], Acc, Alias0) ->
    Alias = to_camelcase(Name),
    NewField = Field#{<<"alias">> => Alias},
    case is_key_field(Field) of
        true ->
            format_field(Fields, [NewField | Acc], Alias);
        false ->
            format_field(Fields, [NewField | Acc], Alias0)
    end.


query(Pool, Table, Query) ->
    Format = fun(Key, Value, Acc) -> Acc#{ Key => Value } end,
    query(Pool, Table, Query, Format).


query(Pool, Table, Query, Format) ->
    PageSize = maps:get(<<"pageSize">>, Query, 100),
    Keys = maps:get(<<"keys">>, Query, undefined),
    Where = maps:get(<<"where">>, Query, #{}),
    SQL1 = format_select(Table, Keys),
    SQL2 = format_where(Where),
    SQL4 = format_order(maps:get(<<"order">>, Query, undefined)),
    PageNum = maps:get(<<"pageNum">>, Query, undefined),
    SQL3 = format_limit(PageNum, PageSize),
    SQL = case PageNum of
              undefined ->
                  <<SQL1/binary, SQL2/binary, SQL3/binary, SQL4/binary, ";">>;
              _ ->
                  SQL5 = <<"SELECT COUNT(*) FROM ", Table/binary, SQL2/binary, ";">>,
                  <<SQL5/binary, SQL1/binary, SQL2/binary, SQL3/binary, SQL4/binary, ";">>
          end,
    case alinkdata:query_from_mysql(Pool, binary_to_list(SQL)) of
        {ok, [{_, [[Count]]}, {Fields, Rows}]} ->
            {ok, Count, format_result(Fields, Rows, Format)};
        {ok, Fields, Rows} ->
            {ok, format_result(Fields, Rows, Format)};
        {error, Reason} ->
            logger:error("[SQL ERROR]~p,~p", [SQL, Reason]),
            {error, Reason}
    end.


delete(Pool, Table, #{ <<"where">> := Where }) ->
    SQL1 = format_where(Where),
    SQL = <<"DELETE FROM ", Table/binary, SQL1/binary, ";">>,
    alinkdata:query_from_mysql(Pool, binary_to_list(SQL)).


update(Pool, Table, Where, Args) ->
    SQL1 = alinkdata_utils:join(",", format_update(Args)),
    SQL2 = format_where(Where),
    SQL = lists:concat(["UPDATE ", binary_to_list(Table), " SET ", SQL1, binary_to_list(SQL2), ";"]),
    alinkdata:query_from_mysql(Pool, SQL).


create(Pool, Table, Args) ->
    {Fields0, Values0} =
        maps:fold(
            fun(Field, Value, {Fields, Values}) ->
                {Field1, Value1} = format_create(Field, Value),
                {[Field1 | Fields], [Value1 | Values]}
            end, {[], []}, Args),
    Fields = alinkdata_utils:join(",", Fields0),
    Values = alinkdata_utils:join(",", Values0),
    SQL = lists:concat(["INSERT INTO ", binary_to_list(Table), "(", Fields, ") VALUES (", Values, ");"]),
    alinkdata:query_from_mysql(Pool, SQL).


get_schema(Pool, Format) ->
    case alinkdata:query_from_mysql(Pool, "show tables;") of
        {ok, _, Tables} ->
            SQL = list_to_binary([binary_to_list(<<"desc ", Table/binary, ";">>) || [Table] <- Tables]),
            case alinkdata:query_from_mysql(Pool, SQL) of
                {error, Reason} ->
                    {error, Reason};
                {ok, DataSet} ->
                    {ok, get_schema(Tables, DataSet, Format, [])}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

get_schema([], [], _, Acc) -> Acc;
get_schema([[TableName] | Tables], [{_, Fields} | DataSet], Format, Acc) ->
    NewFields =
        lists:foldl(
            fun(Field, Acc1) ->
                [create_field(Field) | Acc1]
            end, [], Fields),
    Info = #{
        <<"tableName">> => TableName,
        <<"fields">> => NewFields
    },
    get_schema(Tables, DataSet, Format, [Format(Info) | Acc]).

create_field([FieldName, Type, IsNull, PRI, Default, AutoIncrement]) ->
    #{
        <<"name">> => FieldName,
        <<"type">> => Type,
        <<"required">> => PRI == <<"PRI">>,
        <<"isnull">> => IsNull == <<"YES">>,
        <<"default">> => Default,
        <<"auto_increment">> => AutoIncrement == <<"auto_increment">>
    }.


format_create(Field, Value) when is_binary(Value) ->
    {Field, <<"'", Value/binary, "'">>};
format_create(Field, Value) when is_integer(Value) ->
    {Field, integer_to_binary(Value)};
format_create(Field, Value) ->
    {Field, Value}.


format_update(Args) ->
    maps:fold(
        fun(Field, Value, Acc) ->
            case format_update(Field, Value) of
                <<>> -> Acc;
                Where -> [Where | Acc]
            end
        end, [], Args).

format_update(_, <<>>) -> <<>>;
format_update(Field, Value) when is_binary(Value) ->
    <<Field/binary, "='", Value/binary, "'">>;
format_update(Field, Value) when is_integer(Value) ->
    Value1 = integer_to_binary(Value),
    <<Field/binary, "=", Value1/binary>>;
format_update(Field, Value) ->
    list_to_binary(lists:concat([binary_to_list(Field), "=", Value])).


format_select(Table, Keys) when Keys == <<>>; Keys == undefined ->
    <<"SELECT * FROM ", Table/binary>>;
format_select(Table, Keys) ->
    <<"SELECT ", Keys/binary, " FROM ", Table/binary>>.

format_where(Where) ->
    List =
        maps:fold(
            fun(Key, Value, Acc) ->
                [format_where_value(Key, Value) | Acc]
            end, [], Where),
    case alinkdata_utils:join(" and ", List) of
        "" -> <<>>;
        S -> list_to_binary(" WHERE " ++ S)
    end.



format_where_value(Key, Value) when is_list(Value) ->
    ValueBin0 =
        lists:foldl(
            fun(V, Acc) ->
                VB = alinkdata_utils:to_binary(V),
                <<Acc/binary, "'", VB/binary, "',">>
        end, <<>>, Value),
    ValueBin1 =
        case byte_size(ValueBin0) of
            0 ->
                <<>>;
            _ ->
                binary:part(ValueBin0, {0, byte_size(ValueBin0) - 1})
        end,
    <<Key/binary, " in (", ValueBin1/binary, ")">>;
format_where_value(Key, Value) when is_binary(Value) ->
    <<Key/binary, "='", Value/binary, "'">>;
format_where_value(Key, Value) when is_integer(Value) ->
    V = integer_to_binary(Value),
    <<Key/binary, "=", V/binary, "">>.

format_limit(PageNum, undefined) ->
    format_limit(PageNum, 100);
format_limit(undefined, PageSize) ->
    list_to_binary(lists:concat([" LIMIT ", PageSize]));
format_limit(PageNum, PageSize) ->
    list_to_binary(lists:concat([" LIMIT ", (PageNum - 1) * PageSize, ",", PageSize])).

format_order(undefined) -> <<>>;
format_order(Order) ->
    format_order(binary:split(Order, <<",">>, [global]), <<"ORDER BY ">>).

format_order([Field], Acc) ->
    Field1 = format_order1(Field),
    <<Acc/binary, Field1/binary>>;
format_order([Field | Other], Acc) ->
    Field1 = format_order1(Field),
    format_order(Other, <<Acc/binary, Field1/binary, ",">>).

format_order1(<<"-", Field/binary>>) ->
    <<Field/binary, " DESC">>;
format_order1(<<"+", Field/binary>>) ->
    <<Field/binary, " AES,">>;
format_order1(Field) ->
    <<Field/binary, " AES">>.


format_result(Fields, Rows, Format) ->
    format_result(Fields, Rows, Format, []).

format_result(_, [], _, Acc) -> Acc;
format_result(Fields, [Row | Rows], Format, Acc) ->
    Row1 = format_row(Fields, Row, Format, #{}),
    format_result(Fields, Rows, Format, lists:append(Acc, [Row1])).

format_row([], [], _, Acc) -> Acc;
format_row([Field | Fields], [Value | Values], Format, Acc) ->
    Acc1 = format_value(Field, Value, Format, Acc),
    format_row(Fields, Values, Format, Acc1).

format_value(Field, {{Y, N, D}, {H, M, S}}, Format, Acc) ->
    Time = list_to_binary((lists:concat([Y, "-", N, "-", D, " ", H, ":", M, ":", S]))),
    Format(Field, Time, Acc);
format_value(Field, Value, Format, Acc) ->
    Format(Field, Value, Acc).

to_camelcase(Field) ->
    case binary:split(Field, <<"_">>, [global]) of
        [Field] ->
            Field;
        [Field1 | Fields] ->
            UpperFirst =
                fun(<<A:1/bytes, L/binary>>) ->
                    A1 = string:uppercase(A),
                    binary_to_list(<<A1/binary, L/binary>>)
                end,
            list_to_binary(lists:concat([binary_to_list(Field1) | [UpperFirst(F) || F <- Fields]]))
    end.

is_key_field(#{
    <<"required">> := true,
    <<"auto_increment">> := true
}) ->
    true;
is_key_field(_) -> false.

