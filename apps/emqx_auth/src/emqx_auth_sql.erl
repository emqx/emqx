%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_sql).

-moduledoc """
Checks of the SQL queries that the authentication and authorization backends
send to a database.
""".

-export([validate_read_only/2]).

-export_type([dialect/0]).

-type dialect() :: mysql | pgsql.

-type token() :: {word, binary()} | lparen | semicolon | literal | other.

-define(WRITE_KEYWORDS, [
    <<"insert">>,
    <<"update">>,
    <<"delete">>,
    <<"replace">>,
    <<"merge">>,
    <<"upsert">>,
    <<"truncate">>,
    <<"drop">>,
    <<"alter">>,
    <<"create">>,
    <<"into">>
]).

-define(IS_WORD_START(C),
    ((C >= $a andalso C =< $z) orelse (C >= $A andalso C =< $Z) orelse C =:= $_ orelse C >= 128)
).
-define(IS_WORD_CHAR(C), (?IS_WORD_START(C) orelse (C >= $0 andalso C =< $9) orelse C =:= $$)).

-doc """
Return `ok` when the query is one statement that does not change data.

The check skips comments, string literals and quoted identifiers. It rejects
the query when it finds a keyword that changes data, for example `INSERT`,
`UPDATE`, `DELETE` or `SELECT ... INTO`. A keyword that is followed by `(` is a
function call, for example the `REPLACE()` string function, and is accepted.
`UPDATE` after `FOR` or `KEY` is a row lock clause, and is accepted.
""".
-spec validate_read_only(unicode:chardata(), dialect()) -> ok | {error, binary()}.
validate_read_only(Query, Dialect) ->
    case unicode:characters_to_binary(Query) of
        Bin when is_binary(Bin) ->
            Tokens = tokens(Bin, Dialect, []),
            maybe
                ok ?= check_single_statement(Tokens),
                ok ?= check_no_write_keyword(Tokens, undefined)
            end;
        _ ->
            {error, <<"The query is not a valid UTF-8 string">>}
    end.

%% Only empty statements can follow the first semicolon.
check_single_statement(Tokens) ->
    case lists:dropwhile(fun(T) -> T =/= semicolon end, Tokens) of
        [] ->
            ok;
        [semicolon | Rest] ->
            case lists:all(fun(T) -> T =:= semicolon end, Rest) of
                true -> ok;
                false -> {error, <<"The query must contain only one SQL statement">>}
            end
    end.

check_no_write_keyword([], _Prev) ->
    ok;
check_no_write_keyword([{word, Word} = Token | Rest], Prev) ->
    case is_write_keyword(Word, Prev, Rest) of
        true ->
            {error,
                <<"The query must not change data. It contains the keyword ",
                    (string:uppercase(Word))/binary>>};
        false ->
            check_no_write_keyword(Rest, Token)
    end;
check_no_write_keyword([Token | Rest], _Prev) ->
    check_no_write_keyword(Rest, Token).

is_write_keyword(_Word, _Prev, [lparen | _]) ->
    false;
is_write_keyword(<<"update">>, {word, Prev}, _Next) when Prev =:= <<"for">>; Prev =:= <<"key">> ->
    false;
is_write_keyword(Word, _Prev, _Next) ->
    lists:member(Word, ?WRITE_KEYWORDS).

-spec tokens(binary(), dialect(), [token()]) -> [token()].
tokens(<<>>, _Dialect, Acc) ->
    lists:reverse(Acc);
tokens(<<"--", C, Rest/binary>>, mysql, Acc) when C =< $\s ->
    tokens(skip_line(Rest), mysql, Acc);
tokens(<<"--">>, mysql, Acc) ->
    lists:reverse(Acc);
tokens(<<"--", Rest/binary>>, pgsql, Acc) ->
    tokens(skip_line(Rest), pgsql, Acc);
tokens(<<"#", Rest/binary>>, mysql, Acc) ->
    tokens(skip_line(Rest), mysql, Acc);
tokens(<<"/*!", Rest/binary>>, mysql, Acc) ->
    %% MySQL runs the content of a `/*! ... */` comment as SQL.
    tokens(skip_digits(Rest), mysql, Acc);
tokens(<<"/*", Rest/binary>>, Dialect, Acc) ->
    tokens(skip_block_comment(Rest), Dialect, Acc);
tokens(<<"'", Rest/binary>>, Dialect, Acc) ->
    tokens(skip_quoted($', Rest, Dialect =:= mysql), Dialect, [literal | Acc]);
tokens(<<"\"", Rest/binary>>, Dialect, Acc) ->
    tokens(skip_quoted($", Rest, Dialect =:= mysql), Dialect, [literal | Acc]);
tokens(<<"`", Rest/binary>>, mysql, Acc) ->
    tokens(skip_quoted($`, Rest, false), mysql, [literal | Acc]);
tokens(<<"$", Rest/binary>>, pgsql, Acc) ->
    case dollar_quote_tag(Rest) of
        {ok, Delimiter, Rest1} ->
            tokens(skip_past(Delimiter, Rest1), pgsql, [literal | Acc]);
        error ->
            tokens(Rest, pgsql, [other | Acc])
    end;
tokens(<<"(", Rest/binary>>, Dialect, Acc) ->
    tokens(Rest, Dialect, [lparen | Acc]);
tokens(<<";", Rest/binary>>, Dialect, Acc) ->
    tokens(Rest, Dialect, [semicolon | Acc]);
tokens(<<C, _/binary>> = Bin, Dialect, Acc) when ?IS_WORD_START(C) ->
    {Word0, Rest} = take_word(Bin, <<>>),
    Word = string:lowercase(Word0),
    case {Dialect, Word, Rest} of
        {pgsql, <<"e">>, <<"'", Rest1/binary>>} ->
            %% PostgreSQL escape string: E'...'
            tokens(skip_quoted($', Rest1, true), pgsql, [literal | Acc]);
        _ ->
            tokens(Rest, Dialect, [{word, Word} | Acc])
    end;
tokens(<<C, Rest/binary>>, Dialect, Acc) when C =< $\s ->
    tokens(Rest, Dialect, Acc);
tokens(<<_, Rest/binary>>, Dialect, Acc) ->
    tokens(Rest, Dialect, [other | Acc]).

take_word(<<C, Rest/binary>>, Acc) when ?IS_WORD_CHAR(C) ->
    take_word(Rest, <<Acc/binary, C>>);
take_word(Rest, Acc) ->
    {Acc, Rest}.

skip_line(Bin) ->
    case binary:match(Bin, <<"\n">>) of
        {Pos, 1} -> binary:part(Bin, Pos + 1, byte_size(Bin) - Pos - 1);
        nomatch -> <<>>
    end.

skip_digits(<<C, Rest/binary>>) when C >= $0, C =< $9 ->
    skip_digits(Rest);
skip_digits(Rest) ->
    Rest.

skip_block_comment(Bin) ->
    skip_past(<<"*/">>, Bin).

skip_past(Delimiter, Bin) ->
    case binary:match(Bin, Delimiter) of
        {Pos, Len} -> binary:part(Bin, Pos + Len, byte_size(Bin) - Pos - Len);
        nomatch -> <<>>
    end.

skip_quoted(_Quote, <<>>, _BackslashEscapes) ->
    <<>>;
skip_quoted(Quote, <<$\\, _, Rest/binary>>, true) ->
    skip_quoted(Quote, Rest, true);
skip_quoted(Quote, <<Quote, Quote, Rest/binary>>, BackslashEscapes) ->
    skip_quoted(Quote, Rest, BackslashEscapes);
skip_quoted(Quote, <<Quote, Rest/binary>>, _BackslashEscapes) ->
    Rest;
skip_quoted(Quote, <<_, Rest/binary>>, BackslashEscapes) ->
    skip_quoted(Quote, Rest, BackslashEscapes).

%% PostgreSQL dollar quote: `$$...$$` or `$tag$...$tag$`.
dollar_quote_tag(Bin) ->
    case re:run(Bin, <<"^([A-Za-z_][A-Za-z0-9_]*)?\\$">>, [{capture, [0, 1], binary}]) of
        {match, [Matched, Tag]} ->
            Rest = binary:part(Bin, byte_size(Matched), byte_size(Bin) - byte_size(Matched)),
            {ok, <<"$", Tag/binary, "$">>, Rest};
        nomatch ->
            error
    end.
