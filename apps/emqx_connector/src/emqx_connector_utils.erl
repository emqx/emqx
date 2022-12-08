-module(emqx_connector_utils).

-export([split_insert_sql/1]).

%% SQL = <<"INSERT INTO \"abc\" (c1,c2,c3) VALUES (${1}, ${1}, ${1})">>
split_insert_sql(SQL) ->
    case re:split(SQL, "((?i)values)", [{return, binary}]) of
        [Part1, _, Part3] ->
            case string:trim(Part1, leading) of
                <<"insert", _/binary>> = InsertSQL ->
                    {ok, {InsertSQL, Part3}};
                <<"INSERT", _/binary>> = InsertSQL ->
                    {ok, {InsertSQL, Part3}};
                _ ->
                    {error, not_insert_sql}
            end;
        _ ->
            {error, not_insert_sql}
    end.
