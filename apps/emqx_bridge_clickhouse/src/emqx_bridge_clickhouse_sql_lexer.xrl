%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This lexer recognizes the restricted ClickHouse INSERT template token set.

Definitions.

WS              = [\s\t\r\n\f]+
ID_START        = [A-Za-z_\200-\377]
ID_CONTINUE     = [A-Za-z0-9_\200-\377]
IDENTIFIER      = {ID_START}{ID_CONTINUE}*
PLACEHOLDER     = \$\{[A-Za-z0-9_.]*\}
NUMBER          = (([0-9]+(\.[0-9]*)?)|(\.[0-9]+))([eE][+-]?[0-9]+)?
SQ_STRING       = '([^'\\]|\\.|'')*'
DQ_STRING       = "([^"\\]|\\.|"")*"
BT_IDENTIFIER   = `([^`\\]|\\.|``)*`

Rules.

{WS}            : skip_token.
--               : {error, {comments_not_allowed, TokenLine}}.
\/\*             : {error, {comments_not_allowed, TokenLine}}.
\/\/             : {error, {comments_not_allowed, TokenLine}}.
\#[\s!]          : {error, {comments_not_allowed, TokenLine}}.
\$[^\{]          : {error, {unsupported_clickhouse_syntax, TokenLine}}.
{PLACEHOLDER}   : placeholder(TokenChars, TokenLine).
{SQ_STRING}     : {token, {sq_string, TokenLine, to_binary(TokenChars)}}.
{DQ_STRING}     : {token, {dq_string, TokenLine, to_binary(TokenChars)}}.
{BT_IDENTIFIER} : {token, {bt_identifier, TokenLine, to_binary(TokenChars)}}.
{NUMBER}        : {token, {number, TokenLine, to_binary(TokenChars)}}.
{IDENTIFIER}    : identifier(TokenChars, TokenLine).
\(              : {token, {'(', TokenLine}}.
\)              : {token, {')', TokenLine}}.
\[              : {token, {'[', TokenLine}}.
\]              : {token, {']', TokenLine}}.
\{              : {token, {'{', TokenLine}}.
\}              : {token, {'}', TokenLine}}.
,               : {token, {',', TokenLine}}.
:               : {token, {':', TokenLine}}.
\.              : {token, {'.', TokenLine}}.
;               : {token, {';', TokenLine}}.
\+              : {token, {'+', TokenLine}}.
-               : {token, {'-', TokenLine}}.
\*              : {token, {'*', TokenLine}}.
\/              : {token, {'/', TokenLine}}.
\%              : {token, {'%', TokenLine}}.
'               : {error, {unterminated_string, TokenLine}}.
"               : {error, {unterminated_quoted_identifier, TokenLine}}.
`               : {error, {unterminated_backtick_identifier, TokenLine}}.
\$              : {error, {invalid_placeholder, TokenLine}}.
.               : {error, {unsupported_token, TokenLine, to_binary(TokenChars)}}.

Erlang code.

to_binary(Chars) ->
    list_to_binary(Chars).

placeholder(Chars, Line) ->
    Bin = to_binary(Chars),
    case emqx_bridge_clickhouse_sql:parse_placeholder(Bin) of
        {ok, Placeholder} -> {token, {placeholder, Line, Placeholder}};
        {error, _} -> {error, {invalid_placeholder, Line, Bin}}
    end.

identifier(Chars, Line) ->
    Bin = to_binary(Chars),
    case string:lowercase(Bin) of
        <<"insert">> -> {token, {insert, Line}};
        <<"into">> -> {token, {into, Line}};
        <<"table">> -> {token, {table, Line}};
        <<"values">> -> {token, {values, Line}};
        <<"format">> -> {token, {format, Line}};
        <<"except">> -> {token, {except, Line}};
        <<"null">> -> {token, {null, Line}};
        <<"true">> -> {token, {true, Line}};
        <<"false">> -> {token, {false, Line}};
        _ -> {token, {identifier, Line, Bin}}
    end.
