%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This lexer recognizes the restricted MySQL INSERT INTO VALUES token set.
%% Leex selects the longest prefix. Rule order resolves equal-length matches:
%% https://github.com/erlang/otp/blob/OTP-26.2.5.14/lib/parsetools/src/leex.erl#L1333-L1365
%% https://github.com/erlang/otp/blob/OTP-26.2.5.14/lib/parsetools/src/leex.erl#L1142-L1163
%% https://github.com/erlang/otp/blob/OTP-26.2.5.14/lib/parsetools/src/leex.erl#L1270-L1278

Definitions.

%% MySQL's state map classifies vertical tab as whitespace:
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/strings/sql_chars.cc#L86-L130
WS              = [\s\t\r\n\v\f]+
%% MySQL permits extended bytes. Reject leading $ because MySQL 8.4 dispatches it to dollar quoting:
%% https://dev.mysql.com/doc/refman/8.4/en/identifiers.html
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L2158-L2190
ID_START        = [A-Za-z_\200-\377]
ID_CONTINUE     = [A-Za-z0-9_\$\200-\377]
IDENTIFIER      = {ID_START}{ID_CONTINUE}*
%% emqx_template placeholder envelope; parse_placeholder/1 applies local validation:
%% See emqx_template:parse/1 in apps/emqx_utils/src/emqx_template.erl.
PLACEHOLDER     = \$\{[A-Za-z0-9_.]*\}
%% https://github.com/mysql/mysql-server/blob/0a7df2e4693d8f10901a26034ae6257699356e30/sql/sql_lex.cc#L1620-L1772
%% https://github.com/mysql/mysql-server/blob/0a7df2e4693d8f10901a26034ae6257699356e30/sql/sql_lex.cc#L2025-L2033
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L1674-L1826
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L2089-L2097
NUMBER          = (([0-9]+(\.[0-9]*)?)|(\.[0-9]+))([eE][+-]?[0-9]+)?
%% Only lowercase 0x is HEX_NUM. Reject the uppercase identifier-path form:
%% https://github.com/mysql/mysql-server/blob/0a7df2e4693d8f10901a26034ae6257699356e30/sql/sql_lex.cc#L1620-L1707
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L1674-L1761
HEX_NUMBER      = 0x[0-9A-Fa-f]+
%% Both versions route x/X to quoted-hex scanning and require an even hex-digit count:
%% https://github.com/mysql/mysql-server/blob/0a7df2e4693d8f10901a26034ae6257699356e30/mysys/sql_chars.cc#L125-L128
%% https://github.com/mysql/mysql-server/blob/0a7df2e4693d8f10901a26034ae6257699356e30/sql/sql_lex.cc#L1774-L1785
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/strings/sql_chars.cc#L124-L127
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L1828-L1839
HEX_STRING      = [xX]'([0-9A-Fa-f][0-9A-Fa-f])*'
%% The connector clears ANSI_QUOTES and NO_BACKSLASH_ESCAPES for every session.
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L1023-L1127
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L1763-L1801
SQ_STRING       = '([^\000'\\]|\\(.|\n)|'')*'
%% '
DQ_STRING       = "([^\000"\\]|\\(.|\n)|"")*"
%% "
%% Backtick identifiers use doubled backticks and stop before NUL:
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L1763-L1801
BT_IDENTIFIER   = `([^\000`]|``)*`

Rules.

{WS}            : skip_token.
%% Reject all MySQL comment openers. '--' is intentionally over-rejected:
%% https://dev.mysql.com/doc/refman/8.4/en/comments.html
--               : {error, {comments_not_allowed, TokenLine}}.
\/\*             : {error, {comments_not_allowed, TokenLine}}.
\#               : {error, {comments_not_allowed, TokenLine}}.
{PLACEHOLDER}   : placeholder(TokenChars, TokenLine).
{HEX_STRING}    : {token, {hex_string, TokenLine, to_binary(TokenChars)}}.
{HEX_NUMBER}    : {token, {hex_number, TokenLine, to_binary(TokenChars)}}.
{SQ_STRING}     : {token, {string, TokenLine, to_binary(TokenChars)}}.
{DQ_STRING}     : {token, {string, TokenLine, to_binary(TokenChars)}}.
{BT_IDENTIFIER} : {token, {bt_identifier, TokenLine, to_binary(TokenChars)}}.
{NUMBER}        : {token, {number, TokenLine, to_binary(TokenChars)}}.
{IDENTIFIER}    : identifier(TokenChars, TokenLine).
%% MySQL source for the supported one-byte punctuation and operator forms:
%% https://github.com/mysql/mysql-server/blob/0a7df2e4693d8f10901a26034ae6257699356e30/sql/sql_lex.cc#L1426-L1469
%% https://github.com/mysql/mysql-server/blob/0a7df2e4693d8f10901a26034ae6257699356e30/sql/sql_lex.cc#L1799-L2033
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L1487-L1530
%% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L1853-L2097
\(              : {token, {'(', TokenLine}}.
\)              : {token, {')', TokenLine}}.
,               : {token, {',', TokenLine}}.
\.              : {token, {'.', TokenLine}}.
;               : {token, {';', TokenLine}}.
<=>             : {token, {'<=>', TokenLine}}.
>=              : {token, {'>=', TokenLine}}.
<=              : {token, {'<=', TokenLine}}.
<>              : {token, {'<>', TokenLine}}.
!=              : {token, {'!=', TokenLine}}.
=               : {token, {'=', TokenLine}}.
>               : {token, {'>', TokenLine}}.
<               : {token, {'<', TokenLine}}.
\+              : {token, {'+', TokenLine}}.
-               : {token, {'-', TokenLine}}.
\*              : {token, {'*', TokenLine}}.
\/              : {token, {'/', TokenLine}}.
\%              : {token, {'%', TokenLine}}.
'               : {error, {unterminated_single_quoted_string, TokenLine}}.
%% '
"               : {error, {unterminated_double_quoted_token, TokenLine}}.
%% "
`               : {error, {unterminated_backtick_identifier, TokenLine}}.
\$              : {error, {invalid_placeholder, TokenLine}}.
.               : {error, {unsupported_token, TokenLine, to_binary(TokenChars)}}.

Erlang code.

to_binary(Chars) ->
    list_to_binary(Chars).

placeholder(Chars, Line) ->
    Bin = to_binary(Chars),
    case emqx_mysql_sql:parse_placeholder(Bin) of
        {ok, Placeholder} -> {token, {placeholder, Line, Placeholder}};
        {error, _} -> {error, {invalid_placeholder, Line, Bin}}
    end.

identifier(Chars, Line) ->
    %% MySQL performs keyword lookup after scanning the complete identifier:
    %% https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/sql/sql_lex.cc#L1558-L1619
    Bin = to_binary(Chars),
    case string:lowercase(Bin) of
        <<"insert">> -> {token, {insert, Line}};
        <<"into">> -> {token, {into, Line}};
        <<"values">> -> {token, {values, Line, Bin}};
        <<"on">> -> {token, {on, Line}};
        <<"duplicate">> -> {token, {duplicate, Line}};
        <<"key">> -> {token, {key, Line, Bin}};
        <<"update">> -> {token, {update, Line, Bin}};
        <<"as">> -> {token, {as, Line}};
        <<"null">> -> {token, {null, Line}};
        <<"true">> -> {token, {true, Line}};
        <<"false">> -> {token, {false, Line}};
        <<"default">> -> {token, {default, Line}};
        <<"case">> -> {token, {case_kw, Line}};
        <<"when">> -> {token, {when_kw, Line}};
        <<"then">> -> {token, {then_kw, Line}};
        <<"else">> -> {token, {else_kw, Line}};
        <<"end">> -> {token, {end_kw, Line}};
        <<"is">> -> {token, {is_kw, Line}};
        <<"and">> -> {token, {and_kw, Line}};
        <<"or">> -> {token, {or_kw, Line}};
        <<"not">> -> {token, {not_kw, Line}};
        _ -> {token, {identifier, Line, Bin}}
    end.
