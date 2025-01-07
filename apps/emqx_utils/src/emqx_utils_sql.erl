%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_utils_sql).

-export([get_statement_type/1]).
-export([split_insert/1]).

-export([to_sql_value/1]).
-export([to_sql_string/2]).

-export([escape_sql/1]).
-export([escape_cql/1]).
-export([escape_mysql/1]).
-export([escape_snowflake/1]).

-export_type([value/0]).

-type statement_type() :: select | insert | delete | update.
-type value() :: null | binary() | number() | boolean() | [value()].

%% The type Copied from stdlib/src/re.erl to compatibility with OTP 26
%% Since `re:mp()` exported after OTP 27
-type mp() :: {re_pattern, _, _, _, _}.

-define(INSERT_RE_MP_KEY, {?MODULE, insert_re_mp}).
-define(INSERT_RE_BIN, <<
    %% case-insensitive
    "(?i)^\\s*"
    %% Group-1: insert into, table name and columns (when existed).
    %% All space characters suffixed to <TABLE_NAME> will be kept
    %% `INSERT INTO <TABLE_NAME> [(<COLUMN>, ..)]`
    "(insert\\s+into\\s+[^\\s\\(\\)]+\\s*(?:\\([^\\)]*\\))?)"
    %% Keyword: `VALUES`
    "\\s*values\\s*"
    %% Group-2: literals value(s) or placeholder(s) with round brackets.
    %% And the sub-pattern in brackets does not do any capturing
    %% `([<VALUE> | <PLACEHOLDER>], ..])`
    "(\\((?:[^()]++|(?2))*\\))"
    %% Group-3: match the `ON conflict` or `ON dulicate` clause
    "\\s*(?:on\\s+(\\S+.*\\S))?"
    %% Match trailings spaces
    "\\s*$"
>>).

-define(HEX_RE_MP_KEY, {?MODULE, hex_re_mp}).
-define(HEX_RE_BIN, <<"^[0-9a-fA-F]+$">>).

-dialyzer({no_improper_lists, [escape_mysql/4, escape_prepend/4]}).

-on_load(on_load/0).

on_load() ->
    ok = put_insert_mp(),
    ok = put_hex_re_mp().

put_insert_mp() ->
    persistent_term:put(?INSERT_RE_MP_KEY, re:compile(?INSERT_RE_BIN)),
    ok.

-spec get_insert_mp() -> {ok, mp()}.
get_insert_mp() ->
    case persistent_term:get(?INSERT_RE_MP_KEY, undefined) of
        undefined ->
            ok = put_insert_mp(),
            get_insert_mp();
        {ok, MP} ->
            {ok, MP}
    end.

put_hex_re_mp() ->
    persistent_term:put(?HEX_RE_MP_KEY, re:compile(?HEX_RE_BIN)),
    ok.

-spec get_hex_re_mp() -> {ok, mp()}.
get_hex_re_mp() ->
    case persistent_term:get(?HEX_RE_MP_KEY, undefined) of
        undefined ->
            ok = put_hex_re_mp(),
            get_hex_re_mp();
        {ok, MP} ->
            {ok, MP}
    end.

-spec get_statement_type(iodata()) -> statement_type() | {error, unknown}.
get_statement_type(Query) ->
    KnownTypes = #{
        <<"select">> => select,
        <<"insert">> => insert,
        <<"update">> => update,
        <<"delete">> => delete
    },
    case re:run(Query, <<"^\\s*([a-zA-Z]+)">>, [{capture, all_but_first, binary}]) of
        {match, [Token]} ->
            maps:get(string:lowercase(Token), KnownTypes, {error, unknown});
        _ ->
            {error, unknown}
    end.

%% @doc Parse an INSERT SQL statement into its INSERT part and the VALUES part.
%%
%% SQL = "INSERT INTO \"abc\" (c1, c2, c3) VALUES (${a}, ${b}, ${c.prop})"
%%   {ok, {<<"INSERT INTO \"abc\" (c1, c2, c3)">>, <<"(${a}, ${b}, ${c.prop})">>}}
%%
%% SQL = "INSERT INTO \"abc\" (c1, c2, c3) VALUES (${a}, ${b}, ${c.prop}) ON DUPLICATE KEY UPDATE c1 = ${a}"
%%   {ok, {<<"INSERT INTO \"abc\" (c1, c2, c3)">>, <<"(${a}, ${b}, ${c.prop})">>,
%%         <<"DUPLICATE KEY UPDATE c1 = ${a}">>}}
-spec split_insert(iodata()) ->
    {ok, {binary(), binary()} | {binary(), binary(), binary()}} | {error, term()}.
split_insert(SQL) ->
    {ok, MP} = get_insert_mp(),
    try
        case re:run(SQL, MP, [{capture, all_but_first, binary}]) of
            {match, [InsertInto, ValuesTemplate]} ->
                {ok, {assert_no_vars(InsertInto), ValuesTemplate}};
            {match, [InsertInto, ValuesTemplate, OnClause]} ->
                {ok, {assert_no_vars(InsertInto), ValuesTemplate, assert_no_vars(OnClause)}};
            nomatch ->
                {error, <<"Not an INSERT statement or incorrect SQL syntax">>}
        end
    catch
        throw:{placeholders_not_allowed, _Str} ->
            {error, <<"Placeholders are only allowed in VALUES part">>}
    end.

assert_no_vars(Str) ->
    case emqx_template_sql:has_placeholder(Str) of
        true -> throw({placeholders_not_allowed, Str});
        false -> Str
    end.

%% @doc Convert an Erlang term to a value that can be used primarily in
%% prepared SQL statements.
-spec to_sql_value(term()) -> value().
to_sql_value(undefined) -> null;
to_sql_value(List) when is_list(List) -> List;
to_sql_value(Bin) when is_binary(Bin) -> Bin;
to_sql_value(Num) when is_number(Num) -> Num;
to_sql_value(Bool) when is_boolean(Bool) -> Bool;
to_sql_value(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
to_sql_value(Map) when is_map(Map) -> emqx_utils_json:encode(Map).

%% @doc Convert an Erlang term to a string that can be interpolated in literal
%% SQL statements. The value is escaped if necessary.
-spec to_sql_string(term(), Options) -> unicode:chardata() when
    Options :: #{
        escaping => mysql | sql | cql | sqlserver,
        undefined => null | unicode:chardata()
    }.
to_sql_string(undefined, #{undefined := Str} = Opts) when Str =/= null ->
    to_sql_string(Str, Opts);
to_sql_string(undefined, #{}) ->
    <<"NULL">>;
to_sql_string(String, #{escaping := mysql}) when is_binary(String) ->
    try
        escape_mysql(String)
    catch
        throw:invalid_utf8 ->
            [<<"0x">>, binary:encode_hex(String)]
    end;
to_sql_string(Term, #{escaping := mysql}) ->
    maybe_escape(Term, fun escape_mysql/1);
to_sql_string(Term, #{escaping := cql}) ->
    maybe_escape(Term, fun escape_cql/1);
to_sql_string(Term, #{escaping := sqlserver}) ->
    maybe_escape(Term, fun escape_sqlserver/1);
to_sql_string(Term, #{}) ->
    maybe_escape(Term, fun escape_sql/1).

-spec maybe_escape(_Value, fun((binary()) -> iodata())) -> unicode:chardata().
maybe_escape(Str, EscapeFun) when is_binary(Str) ->
    EscapeFun(Str);
maybe_escape(Str, EscapeFun) when is_list(Str) ->
    case unicode:characters_to_binary(Str) of
        Bin when is_binary(Bin) ->
            EscapeFun(Bin);
        Otherwise ->
            error(Otherwise)
    end;
maybe_escape(Val, EscapeFun) when is_atom(Val) orelse is_map(Val) ->
    EscapeFun(emqx_template:to_string(Val));
maybe_escape(Val, _EscapeFun) ->
    emqx_template:to_string(Val).

-spec escape_sql(binary()) -> iodata().
escape_sql(S) ->
    % NOTE
    % This is a bit misleading: currently, escaping logic in `escape_sql/1` likely
    % won't work with pgsql since it does not support C-style escapes by default.
    % https://www.postgresql.org/docs/14/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS
    ES = binary:replace(S, [<<"\\">>, <<"'">>], <<"\\">>, [global, {insert_replaced, 1}]),
    [$', ES, $'].

-spec escape_cql(binary()) -> iodata().
escape_cql(S) ->
    ES = binary:replace(S, <<"'">>, <<"'">>, [global, {insert_replaced, 1}]),
    [$', ES, $'].

-spec escape_mysql(binary()) -> iodata().
escape_mysql(S0) ->
    % https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
    [$', escape_mysql(S0, 0, 0, S0), $'].

-spec escape_snowflake(binary()) -> iodata().
escape_snowflake(S) ->
    ES = binary:replace(S, <<"\"">>, <<"\"">>, [global, {insert_replaced, 1}]),
    [$", ES, $"].

escape_sqlserver(<<"0x", Rest/binary>> = S) ->
    {ok, MP} = get_hex_re_mp(),
    case re:run(Rest, MP, []) of
        {match, _} ->
            [S];
        _ ->
            escape_sql(S)
    end;
escape_sqlserver(S) ->
    escape_sql(S).

%% NOTE
%% This thing looks more complicated than needed because it's optimized for as few
%% intermediate memory (re)allocations as possible.
escape_mysql(<<$', Rest/binary>>, I, Run, Src) ->
    escape_prepend(I, Run, Src, [<<"\\'">> | escape_mysql(Rest, I + Run + 1, 0, Src)]);
escape_mysql(<<$\\, Rest/binary>>, I, Run, Src) ->
    escape_prepend(I, Run, Src, [<<"\\\\">> | escape_mysql(Rest, I + Run + 1, 0, Src)]);
escape_mysql(<<0, Rest/binary>>, I, Run, Src) ->
    escape_prepend(I, Run, Src, [<<"\\0">> | escape_mysql(Rest, I + Run + 1, 0, Src)]);
escape_mysql(<<_/utf8, Rest/binary>> = S, I, Run, Src) ->
    CWidth = byte_size(S) - byte_size(Rest),
    escape_mysql(Rest, I, Run + CWidth, Src);
escape_mysql(<<>>, 0, _, Src) ->
    Src;
escape_mysql(<<>>, I, Run, Src) ->
    binary:part(Src, I, Run);
escape_mysql(_, _I, _Run, _Src) ->
    throw(invalid_utf8).

escape_prepend(_RunI, 0, _Src, Tail) ->
    Tail;
escape_prepend(I, Run, Src, Tail) ->
    [binary:part(Src, I, Run) | Tail].
